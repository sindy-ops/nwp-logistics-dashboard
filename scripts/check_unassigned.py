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
from _gorgias import get as gorgias_get  # noqa: E402

UNASSIGNED_VIEW_ID = 1616299


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
    now = datetime.now(timezone.utc)
    next_url = f"/api/views/{UNASSIGNED_VIEW_ID}/items?limit=100"
    totals = {"total": 0, "over_1d": 0, "over_3d": 0, "over_7d": 0, "over_30d": 0}
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
            ref = t.get("last_received_message_datetime") or t.get("created_datetime")
            if not ref:
                continue
            c = datetime.fromisoformat(ref.replace("Z", "+00:00"))
            age = (now - c).total_seconds() / 86400
            totals["total"] += 1
            if age >= 1:  totals["over_1d"]  += 1
            if age >= 3:  totals["over_3d"]  += 1
            if age >= 7:  totals["over_7d"]  += 1
            if age >= 30: totals["over_30d"] += 1
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
            print(f"  page {pages}: total={totals['total']}", flush=True)
        time.sleep(0.15)

    order = ["0-1d", "1-3d", "3-7d", "7-14d", "14-30d", "30d+"]
    buckets = [{"label": b, "count": by_bucket.get(b, 0)} for b in order]
    return {
        "generated": now.isoformat(),
        "age_metric": "last_received_message_datetime",
        **totals,
        "oldest_days": (now - oldest_dt).days if oldest_dt else 0,
        "oldest_date": oldest_dt.date().isoformat() if oldest_dt else None,
        "oldest_ticket_id": oldest_id,
        "buckets": buckets,
    }


def summary(s):
    pct = lambda n: int(round(n / max(1, s["total"]) * 100))
    under = s["total"] - s["over_1d"]
    return (
        "*Gorgias Unassigned — Queue Health*\n"
        "_Age = time since last message from client. Target: under 24h._\n\n"
        f"Total unassigned: *{s['total']:,}*\n"
        f":white_check_mark: Under 24h: *{under:,}* ({pct(under)}%)\n"
        f":warning: Over 24h: *{s['over_1d']:,}* ({pct(s['over_1d'])}%)\n"
        f":rotating_light: Over 3 days: *{s['over_3d']:,}* ({pct(s['over_3d'])}%)\n"
        f":rotating_light: Over 7 days: *{s['over_7d']:,}* ({pct(s['over_7d'])}%)\n\n"
        f"Oldest client msg: *{s['oldest_days']}d ago* "
        f"(ticket #{s['oldest_ticket_id']}, {s['oldest_date']})\n"
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
