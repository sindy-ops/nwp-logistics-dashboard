"""
Refresh Volume data — combined fetch + aggregate.

Calls Gorgias /api/stats per day for 90 days (2 stats/day = 180 calls).
Writes cs_data.json to CWD.

Env: GORGIAS_AUTH_TOKEN
"""

import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _gorgias import post as gorgias_post  # noqa: E402

WINDOW_DAYS = 90

EXCLUDED_NAMES = {
    "Unassigned", "John", "Adam", "Beverly", "Bianca", "Reiner Partosa",
    "HiRelief Support", "Gorgias Support Agent", "Matteo", "Dario Ziarati",
    "Glenda",
    "Gorgias Bot", "Gorgias Help Center Bot", "Gorgias Helpdesk Bot",
    "Gorgias Workflows Bot", "Gorgias Convert Bot", "Gorgias Contact Form Bot",
    "AI Agent Bot", "Gorgias Mobile Bot",
    "Gorgias Help Center- Bot", "Gorgias Helpdesk- Bot",
}

NAME_OVERRIDES = {
    # optional; display-name cleanups if needed
}


def fetch_day(d):
    start = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc).isoformat()
    end = datetime.combine(d, datetime.max.time(), tzinfo=timezone.utc).replace(microsecond=0).isoformat()
    body = {"filters": {"period": {"start_datetime": start, "end_datetime": end}}}
    ov = gorgias_post("/api/stats/overview", body)
    us = gorgias_post("/api/stats/users-performance", body)
    return {"date": d.isoformat(), "overview": ov, "users": us}


def parse_overview(ov):
    return {item["name"]: item.get("value") or 0 for item in ov["data"]["data"]}


def parse_users(us):
    axes = us["data"]["data"]["axes"]["x"]
    col = {a["name"]: i for i, a in enumerate(axes)}
    rows = []
    for line in us["data"]["data"]["lines"]:
        agent = line[col["Agent"]]["value"]
        rows.append({
            "name": agent.get("name"),
            "id": agent.get("id"),
            "messages_sent": line[col["Messages sent"]]["value"] or 0,
        })
    return rows


def main():
    today = datetime.now(timezone.utc).date()
    days = sorted({today - timedelta(days=i) for i in range(WINDOW_DAYS)})
    print(f"Fetching {len(days)} days: {days[0]} → {days[-1]}", flush=True)

    results = [None] * len(days)
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(fetch_day, d): i for i, d in enumerate(days)}
        done = 0
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                results[i] = fut.result()
            except Exception as e:
                print(f"  day {days[i]} ERROR: {e}", flush=True)
                results[i] = {"date": days[i].isoformat(), "error": str(e)}
            done += 1
            if done % 10 == 0 or done == len(days):
                print(f"  {done}/{len(days)}", flush=True)

    day_strs = [r["date"] for r in results]
    customer_msgs = []
    new_tickets = []
    agent_day = defaultdict(dict)

    for r in results:
        date = r["date"]
        if "error" in r:
            customer_msgs.append(0)
            new_tickets.append(0)
            continue
        ov = parse_overview(r["overview"])
        customer_msgs.append(ov.get("total_messages_received", 0))
        new_tickets.append(ov.get("total_new_tickets", 0))
        for row in parse_users(r["users"]):
            if row["name"] in EXCLUDED_NAMES:
                continue
            agent_day[row["name"]][date] = row["messages_sent"]

    agents = []
    for name, dm in agent_day.items():
        total = sum(dm.values())
        if total == 0:
            continue
        agents.append({
            "name": NAME_OVERRIDES.get(name, name),
            "total": total,
            "per_day": {d: dm.get(d, 0) for d in day_strs},
        })
    agents.sort(key=lambda r: -r["total"])

    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_days": len(day_strs),
            "window_start": day_strs[0],
            "window_end": day_strs[-1],
            "total_customer_msgs": sum(customer_msgs),
            "total_new_tickets": sum(new_tickets),
            "total_agent_replies": sum(a["total"] for a in agents),
            "agent_count": len(agents),
        },
        "days": day_strs,
        "customer_msgs_per_day": customer_msgs,
        "new_tickets_per_day": new_tickets,
        "agents": agents,
    }

    out_path = os.path.join(os.getcwd(), "cs_data.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Wrote {out_path}")
    print(f"  customer msgs: {sum(customer_msgs):,}")
    print(f"  new tickets:   {sum(new_tickets):,}")
    print(f"  agent replies: {sum(a['total'] for a in agents):,} ({len(agents)} agents)")


if __name__ == "__main__":
    main()
