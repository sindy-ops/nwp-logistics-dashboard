"""
Queue Health — Gorgias Unassigned view snapshot.

Env:
    GORGIAS_AUTH_TOKEN  — "email:token"
    SLACK_NWP_TOKEN     — bot token (only if --slack)
    UNASSIGNED_CHANNEL  — fallback channel id

Args:
    --slack           post summary to Slack
    --channel <id>    override channel id

Output: unassigned_data.json in CWD (= repo root when run from GitHub Actions).
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _gorgias import get as gorgias_get, post as gorgias_post  # noqa: E402

UNASSIGNED_VIEW_ID = 1616299


def fetch_response_time_overview():
    """Pull last 24h overview stats. Returns dict with median_first_response_time
    (seconds) and median_resolution_time (seconds). Safe-failing."""
    try:
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        now = _dt.now(_tz.utc)
        start = (now - _td(days=1)).isoformat()
        end = now.isoformat()
        body = {"filters": {"period": {"start_datetime": start, "end_datetime": end}}}
        d = gorgias_post("/api/stats/overview", body)
        vals = {item["name"]: item.get("value") or 0 for item in d["data"]["data"]}
        return {
            "median_first_response_sec": vals.get("median_first_response_time"),
            "median_resolution_sec": vals.get("median_resolution_time"),
            "total_messages_sent_24h": vals.get("total_messages_sent"),
            "total_messages_received_24h": vals.get("total_messages_received"),
            "total_new_tickets_24h": vals.get("total_new_tickets"),
        }
    except Exception as e:
        print(f"  (response-time stats skipped: {e})", flush=True)
        return {}


def slack_post(channel, text):
    token = os.environ.get("SLACK_NWP_TOKEN")
    if not token:
        print("  (Slack skipped — SLACK_NWP_TOKEN not set)", flush=True)
        return False
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps({"channel": channel, "text": text}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            if not data.get("ok"):
                print(f"  Slack error: {data.get('error')}", flush=True)
                return False
            return True
    except Exception as e:
        print(f"  Slack error: {e}", flush=True)
        return False


def scan():
    """
    Scan Gorgias Unassigned view. For each ticket:
      - WAITING: customer's last message is the latest event on the ticket
        (last_received_message_datetime >= last_message_datetime). Customer
        still needs a response.
      - ANSWERED: someone (agent or bot) has already replied. Ticket is just
        in the Unassigned view because no one formally took ownership.

    Age buckets + over-Nd counts apply ONLY to WAITING tickets — that's the
    real SLA metric.
    """
    now = datetime.now(timezone.utc)
    next_url = f"/api/views/{UNASSIGNED_VIEW_ID}/items?limit=100"

    total = 0
    answered = 0
    waiting = 0
    waiting_over = {"1d": 0, "3d": 0, "7d": 0, "30d": 0}
    oldest_dt = None
    oldest_id = None
    by_bucket = {}
    pages = 0

    while next_url:
        pages += 1
        data = gorgias_get(next_url)
        items = data.get("data", [])
        if not items:
            break
        for t in items:
            total += 1
            lrm = t.get("last_received_message_datetime")
            lm = t.get("last_message_datetime")
            if not lrm:
                # Edge case: no customer message at all. Skip from "waiting".
                answered += 1
                continue
            # Customer is waiting iff their message is the last event on the ticket.
            # (last_received == last_message means they are the most recent author.)
            is_waiting = not lm or lrm >= lm
            if not is_waiting:
                answered += 1
                continue
            waiting += 1
            c = datetime.fromisoformat(lrm.replace("Z", "+00:00"))
            age = (now - c).total_seconds() / 86400
            if age >= 1:  waiting_over["1d"]  += 1
            if age >= 3:  waiting_over["3d"]  += 1
            if age >= 7:  waiting_over["7d"]  += 1
            if age >= 30: waiting_over["30d"] += 1
            if oldest_dt is None or c < oldest_dt:
                oldest_dt = c
                oldest_id = t["id"]
            if   age < 1:  b = "0-1d"
            elif age < 3:  b = "1-3d"
            elif age < 7:  b = "3-7d"
            elif age < 14: b = "7-14d"
            elif age < 30: b = "14-30d"
            else:          b = "30d+"
            by_bucket[b] = by_bucket.get(b, 0) + 1
        next_url = data.get("meta", {}).get("next_items")
        if pages % 10 == 0:
            print(f"  page {pages}: total={total} waiting={waiting} answered={answered}", flush=True)
        time.sleep(0.15)

    order = ["0-1d", "1-3d", "3-7d", "7-14d", "14-30d", "30d+"]
    buckets = [{"label": b, "count": by_bucket.get(b, 0)} for b in order]
    # Also pull first-response time / 24h overview for an SLA side-metric.
    rt = fetch_response_time_overview()
    return {
        "generated": now.isoformat(),
        "age_metric": "waiting = last_received_message_datetime >= last_message_datetime",
        "total": total,
        "waiting": waiting,
        "answered": answered,
        "over_1d": waiting_over["1d"],
        "over_3d": waiting_over["3d"],
        "over_7d": waiting_over["7d"],
        "over_30d": waiting_over["30d"],
        "oldest_days": (now - oldest_dt).days if oldest_dt else 0,
        "oldest_date": oldest_dt.date().isoformat() if oldest_dt else None,
        "oldest_ticket_id": oldest_id,
        "buckets": buckets,
        "response_time_24h": rt,
    }


def fmt_duration(sec):
    if not sec:
        return "—"
    sec = int(sec)
    h, r = divmod(sec, 3600)
    m = r // 60
    if h >= 24:
        d = h // 24
        h = h % 24
        return f"{d}d {h}h"
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def summary(s):
    pct_of = lambda n, d: int(round(n / max(1, d) * 100))
    waiting = s["waiting"]
    under_24h = waiting - s["over_1d"]
    rt = s.get("response_time_24h") or {}
    first_resp = fmt_duration(rt.get("median_first_response_sec"))
    resolved = fmt_duration(rt.get("median_resolution_sec"))
    return (
        "*Gorgias Unassigned — Queue Health*\n"
        "_SLA = client waiting for response over 24h. "
        "Answered tickets are excluded._\n\n"
        f"Total unassigned view: *{s['total']:,}*\n"
        f"  Already answered (agent/bot replied): {s['answered']:,}\n"
        f"  :hourglass_flowing_sand: Waiting for response: *{waiting:,}*\n\n"
        f":white_check_mark: Under 24h: *{under_24h:,}* ({pct_of(under_24h, waiting)}% of waiting)\n"
        f":warning: Over 24h: *{s['over_1d']:,}* ({pct_of(s['over_1d'], waiting)}% of waiting)\n"
        f":rotating_light: Over 3 days: *{s['over_3d']:,}*\n"
        f":rotating_light: Over 7 days: *{s['over_7d']:,}*\n\n"
        f"Oldest waiting: *{s['oldest_days']}d* "
        f"(ticket #{s['oldest_ticket_id']}, client wrote {s['oldest_date']})\n"
        f":stopwatch: Median first response (24h): *{first_resp}*  "
        f":white_check_mark: Median resolution: {resolved}\n"
        "Dashboard: https://dashboard.nw-project.com/cs.html"
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--slack", action="store_true")
    p.add_argument("--channel", default=os.environ.get("UNASSIGNED_CHANNEL", ""))
    args = p.parse_args()

    print("Scanning Gorgias Unassigned view...", flush=True)
    stats = scan()

    out = os.path.join(os.getcwd(), "unassigned_data.json")
    with open(out, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Wrote {out}", flush=True)
    print()
    print(summary(stats).replace("*", ""))

    if args.slack:
        if not args.channel:
            print("ERROR: --slack requires --channel or UNASSIGNED_CHANNEL env", file=sys.stderr)
            sys.exit(2)
        ok = slack_post(args.channel, summary(stats))
        print(f"\nSlack posted to {args.channel}: {ok}", flush=True)


if __name__ == "__main__":
    main()
