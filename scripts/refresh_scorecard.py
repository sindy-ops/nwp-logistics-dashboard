"""
Refresh Score Card data — multi-week history.

Fetches tickets created in the last 30 days (one pass), then for each configured
week, filters to that week's range, samples up to 50 closed tickets per agent,
fetches their messages, and scores against the 12Q/100pt rubric.

Output: scorecard_data.json with shape:
    {
      "generated": "...",
      "category_labels": {...},
      "sections": {...},
      "weeks": [
        {
          "label": "Week of Apr 13",
          "start": "2026-04-13",
          "end": "2026-04-19",
          "total_tickets": 311,
          "agents": [ {agent, avg_total, section_scores, ...}, ... ]
        },
        ...
      ],
      "current_week_index": 1     // which week to show by default
    }

Runtime: ~10-12 min for 2 weeks (most of the time is messages fetch, which is shared).
Env: GORGIAS_AUTH_TOKEN
"""

import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _gorgias import get as gorgias_get  # noqa: E402

SAMPLE_CAP = 50
# Fetch window must be at least as wide as the oldest week we score.
FETCH_WINDOW_DAYS = 30


def compute_weeks():
    """Return list of (label, start_date, end_date) tuples — most recent last.
    Mondays-to-Sundays. Generates the last 2 COMPLETED weeks (current week skipped
    because it's partial)."""
    today = datetime.now(timezone.utc).date()
    # Find the Monday of the current week
    this_monday = today - timedelta(days=today.weekday())
    weeks = []
    for offset in (2, 1):  # 2 weeks ago, then 1 week ago (most recent last)
        monday = this_monday - timedelta(weeks=offset)
        sunday = monday + timedelta(days=6)
        label = f"Week of {monday:%b %d} – {sunday:%b %d}"
        weeks.append((label, monday, sunday))
    return weeks


TARGET_AGENTS = {
    "abegail@nw-project.com": "Abby",
    "aga@nw-project.com": "Aga",
    "alatiada@nw-project.com": "Andrea",
    "almira@nw-project.com": "Almira",
    "asaguid@nw-project.com": "Anna Pauline",
    "betty@nw-project.com": "Betty",
    "charlottemae@nw-project.com": "Charlotte Mae",
    "cvergara@nw-project.com": "Charles",
    "daniel-angelo@nw-project.com": "Daniel",
    "daryl@nw-project.com": "Daryl",
    "dasentista@nw-project.com": "Star",
    "djanest@nw-project.com": "Djanest Son",
    "elaine-aguirre@nw-project.com": "Elaine",
    "ianna@nw-project.com": "Ianna",
    "jasonkeith@nw-project.com": "Jason Keith",
    "jasonsalas@nw-project.com": "Jason",
    "jerome@nw-project.com": "Jerome",
    "jmarimon@nw-project.com": "Jessa",
    "julie@nw-project.com": "Julie",
    "kbenitua@nw-project.com": "Katherine",
    "lorenzo@nw-project.com": "Lorenzo",
    "lyn@nw-project.com": "Lyn",
    "marco@nw-project.com": "Marco",
    "marcus@nw-project.com": "Marcus",
    "marquisa@nw-project.com": "Marquisa",
    "mbresenio@nw-project.com": "Michael Vincent",
    "menriquez@nw-project.com": "Marianne",
    "nerissa@nw-project.com": "Nerissa",
    "ralph@nw-project.com": "Ralph",
    "reymon@nw-project.com": "Reymon",
    "rosalei@nw-project.com": "Rosalei",
    "sandra@nw-project.com": "Sandra",
    "stephanie-juanir@nw-project.com": "Stephanie",
    "warlito@nw-project.com": "Warlito",
    "yfabiana@nw-project.com": "Yvonne",
}

INQUIRY_TAGS = {
    "cancellation request", "return request", "duplicate order", "chargeback",
    "change - address", "change shipping address", "edit-address",
    "declined-payment", "missed call", "rebill reminder",
}
RESOLUTION_TAGS = {
    "subs_cancel", "subs:move_date_offer", "cancel_u3h:subs_reason",
    "resolved", "refunded", "replacement sent", "return",
}
RETENTION_TRIGGER_TAGS = {"cancellation request", "return request", "subs_cancel"}
MACRO_SIGNATURES = [
    "thank you for reaching out", "we have successfully received your inquiry",
    "our team is currently reviewing", "we appreciate your patience",
    "please allow us", "rest assured",
    "is there anything else i can help you with", "have a wonderful day",
]
RETENTION_KEYWORDS = [
    "discount", "% off", "coupon", "free", "offer", "save", "keep",
    "extend", "pause", "skip", "move your", "next shipment",
    "would you like", "we can offer", "as a valued customer",
    "one-time", "credit", "partial refund",
]
PERSONALIZATION_SIGNALS = [
    r"#\w{2,}", r"NR\d+",
    r"\b(jar|cream|gum|fortify|relief)\b",
    r"\$\d+",
    r"\b(your order|your account|your subscription)\b",
]
CATEGORY = {
    "solution_accuracy":   ("A1. Solution Accuracy", 10),
    "retention_sop":       ("A2. Retention SOP", 15),
    "tool_usage":          ("A3. Tool Usage", 10),
    "inquiry_tagging":     ("B1. Inquiry Tagging", 5),
    "resolution_tagging":  ("B2. Resolution Tagging", 5),
    "internal_notes":      ("B3. Internal Notes", 10),
    "macro_customization": ("B4. Macro Customization", 10),
    "tone_empathy":        ("C1. Tone & Empathy", 10),
    "clarity_mechanics":   ("C2. Clarity & Mechanics", 10),
    "opening_closing":     ("C3. Opening/Closing", 5),
    "fcr":                 ("D1. FCR", 5),
    "conciseness":         ("D2. Conciseness", 5),
}
SECTIONS = {
    "A. Technical Accuracy & Retention": ["solution_accuracy", "retention_sop", "tool_usage"],
    "B. Documentation & Tagging": ["inquiry_tagging", "resolution_tagging", "internal_notes", "macro_customization"],
    "C. Communication & Voice": ["tone_empathy", "clarity_mechanics", "opening_closing"],
    "D. Efficiency": ["fcr", "conciseness"],
}


def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def normalize_email(e):
    if not e:
        return None
    e = e.lower()
    if "-deleted-" in e:
        e = e.split("-deleted-", 1)[0]
    return e


def fetch_ticket_list(cutoff):
    """Paginate tickets by created_datetime desc until we pass cutoff."""
    kept = []
    cursor = None
    batch = 0
    scanned = 0
    while True:
        batch += 1
        qs = "limit=100&order_by=created_datetime:desc"
        if cursor:
            qs += f"&cursor={cursor}"
        d = gorgias_get(f"/api/tickets?{qs}")
        page = d.get("data", [])
        if not page:
            break
        oldest = None
        for t in page:
            c = parse_dt(t.get("created_datetime"))
            if not c:
                continue
            oldest = c
            if c < cutoff:
                continue
            if t.get("status") != "closed":
                continue
            au = t.get("assignee_user") or {}
            email = normalize_email(au.get("email"))
            if email not in TARGET_AGENTS:
                continue
            t["__agent_name"] = TARGET_AGENTS[email]
            kept.append(t)
        scanned += len(page)
        if batch % 20 == 0:
            print(f"  tix batch {batch}: scanned={scanned} kept={len(kept)}", flush=True)
        if oldest and oldest < cutoff:
            break
        cursor = d.get("meta", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.2)
    print(f"  tix scan done: {scanned} scanned, {len(kept)} matched", flush=True)
    return kept


def sample_for_week(tickets, start_dt, end_dt):
    """Filter to week range and sample up to SAMPLE_CAP per agent."""
    in_week = [t for t in tickets if start_dt <= parse_dt(t["created_datetime"]) < end_dt]
    by = {}
    for t in in_week:
        by.setdefault(t["__agent_name"], []).append(t)
    out = []
    for agent, tix in by.items():
        random.seed(42 + hash(agent))
        out.extend(random.sample(tix, min(SAMPLE_CAP, len(tix))))
    return out, by


def fetch_messages(ids):
    results = {}

    def _one(tid):
        try:
            return tid, gorgias_get(f"/api/tickets/{tid}/messages?limit=100").get("data", [])
        except Exception as e:
            return tid, {"__error__": str(e)}

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(_one, tid) for tid in ids]
        done = 0
        for fut in as_completed(futs):
            tid, msgs = fut.result()
            results[tid] = msgs
            done += 1
            if done % 100 == 0 or done == len(ids):
                print(f"  msgs: {done}/{len(ids)}", flush=True)
    return results


def score_ticket(ticket):
    tags = set(t.lower() for t in ticket.get("tags", []))
    msgs = ticket.get("messages", [])
    agent_msgs = [m for m in msgs if m.get("from_agent") and not m.get("is_internal")]
    internal = [m for m in msgs if m.get("is_internal")]
    agent_text = " ".join(m.get("body_text", "") or "" for m in agent_msgs).strip()
    lower = agent_text.lower()

    s = {}
    if not agent_msgs or not agent_text:
        s["solution_accuracy"] = 0
    else:
        x = 5
        specs = sum(1 for p in PERSONALIZATION_SIGNALS if re.search(p, agent_text, re.I))
        if specs >= 2:   x += 3
        elif specs >= 1: x += 2
        w = len(agent_text.split())
        if 20 <= w <= 300: x += 2
        elif w > 10:       x += 1
        s["solution_accuracy"] = min(x, 10)
    is_ret = bool(tags & RETENTION_TRIGGER_TAGS)
    if not is_ret:
        s["retention_sop"] = 15
    else:
        sig = sum(1 for kw in RETENTION_KEYWORDS if kw in lower)
        s["retention_sop"] = 15 if sig >= 3 else 8 if sig >= 1 else 0
    tool_kw = ["shopify", "checkout champ", "gorgias", "airwallex", "solvpath",
               "refund", "cancelled", "adjusted", "updated", "processed",
               "subscription", "order #", "tracking"]
    all_text = lower + " " + " ".join((m.get("body_text", "") or "").lower() for m in internal)
    ev = sum(1 for kw in tool_kw if kw in all_text)
    s["tool_usage"] = 10 if ev >= 3 else 7 if ev >= 2 else 5 if ev >= 1 else 3
    if tags & INQUIRY_TAGS:
        s["inquiry_tagging"] = 5
    elif tags:
        s["inquiry_tagging"] = 3
    else:
        s["inquiry_tagging"] = 0
    s["resolution_tagging"] = 5 if (tags & RESOLUTION_TAGS) else 0
    if internal:
        nt = " ".join((m.get("body_text", "") or "") for m in internal)
        nw = len(nt.split())
        s["internal_notes"] = 10 if nw >= 15 else 6 if nw >= 5 else 3
    else:
        s["internal_notes"] = 0
    if not agent_text:
        s["macro_customization"] = 0
    else:
        mm = sum(1 for p in MACRO_SIGNATURES if p in lower)
        hp = sum(1 for p in PERSONALIZATION_SIGNALS if re.search(p, agent_text, re.I))
        if mm >= 3 and hp == 0: s["macro_customization"] = 2
        elif mm >= 2 and hp >= 1: s["macro_customization"] = 7
        elif hp >= 2: s["macro_customization"] = 10
        elif hp >= 1: s["macro_customization"] = 7
        else:         s["macro_customization"] = 5
    if not agent_text:
        s["tone_empathy"] = s["clarity_mechanics"] = s["opening_closing"] = 0
    else:
        empathy = ["understand","sorry","apologize","appreciate","thank you",
                   "happy to help","glad","certainly","absolutely","of course",
                   "i understand","completely understand","concern"]
        ec = sum(1 for w in empathy if w in lower)
        s["tone_empathy"] = 10 if ec >= 3 else 8 if ec >= 2 else 6 if ec >= 1 else 3
        issues = 0
        if re.search(r'[A-Z]{10,}', agent_text): issues += 1
        if len(agent_text.split()) < 10 and ticket.get("messages_count", 0) > 2: issues += 1
        if agent_text.count("!!!") > 0: issues += 1
        s["clarity_mechanics"] = max(10 - (issues * 3), 4)
        hg = bool(re.search(r'(hi |hello |dear |good (morning|afternoon|evening)|hey )', lower[:100]))
        hc = bool(re.search(
            r'(best regards|warm regards|sincerely|thank you|let (me|us) know|'
            r"don.t hesitate|reach out|any.*(question|concern)|have a (great|wonderful|nice))",
            lower[-200:]))
        s["opening_closing"] = (2 if hg else 0) + (3 if hc else 0)
    nr = len(agent_msgs)
    s["fcr"] = 5 if nr == 1 else 3 if nr == 2 else 1 if nr > 2 else 0
    if not agent_text:
        s["conciseness"] = 0
    else:
        wc = len(agent_text.split())
        if 20 <= wc <= 150:    s["conciseness"] = 5
        elif 150 < wc <= 250:  s["conciseness"] = 4
        elif wc < 20:          s["conciseness"] = 3
        else:                  s["conciseness"] = 2
    return s, is_ret


def to_conv(tickets, msgs_by_tid):
    convs = []
    for t in tickets:
        msgs = msgs_by_tid.get(t["id"], [])
        if not isinstance(msgs, list):
            continue
        cm = []
        for m in msgs:
            cm.append({
                "from_agent": m.get("from_agent", False),
                "body_text": m.get("body_text"),
                "is_internal": not m.get("public", True),
            })
        convs.append({
            "id": t["id"],
            "agent_name": t["__agent_name"],
            "tags": [tag.get("name", "") for tag in (t.get("tags") or [])],
            "messages_count": t.get("messages_count", len(cm)),
            "messages": cm,
        })
    return convs


def aggregate(scored_rows):
    by_agent = defaultdict(list)
    for r in scored_rows:
        by_agent[r["agent"]].append(r)
    out = []
    for agent, items in by_agent.items():
        avg_total = round(sum(r["total"] for r in items) / len(items), 1)
        cat = {k: round(sum(r["scores"][k] for r in items) / len(items), 1) for k in CATEGORY}
        secs = {}
        for sec, keys in SECTIONS.items():
            mx = sum(CATEGORY[k][1] for k in keys)
            avg = sum(cat[k] for k in keys)
            secs[sec] = {"score": round(avg, 1), "max": mx, "pct": round(avg / mx * 100, 1)}
        has_ret = any(r["is_retention"] for r in items)
        weakest = sorted(
            [(CATEGORY[k][0], cat[k], CATEGORY[k][1]) for k in CATEGORY
             if not (k == "retention_sop" and not has_ret)],
            key=lambda x: x[1] / x[2]
        )[:3]
        out.append({
            "agent": agent,
            "tickets_scored": len(items),
            "avg_total": avg_total,
            "category_scores": cat,
            "section_scores": secs,
            "weakest_areas": [{"label": n, "avg": v, "max": mx} for n, v, mx in weakest],
            "retention_tickets": sum(1 for r in items if r["is_retention"]),
            "retention_attempted": sum(1 for r in items if r["is_retention"] and r["scores"]["retention_sop"] >= 8),
        })
    out.sort(key=lambda r: -r["avg_total"])
    return out


def process_week(all_tickets, label, start_date, end_date):
    """Sample + fetch messages + score for tickets in [start_date, end_date]."""
    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    print(f"\n{label}  ({start_date} → {end_date})", flush=True)

    sampled, _ = sample_for_week(all_tickets, start_dt, end_dt)
    ids = [t["id"] for t in sampled]
    agents_count = len(set(t["__agent_name"] for t in sampled))
    print(f"  sample: {len(ids)} tickets, {agents_count} agents", flush=True)
    if not ids:
        return {
            "label": label, "start": start_date.isoformat(), "end": end_date.isoformat(),
            "total_tickets": 0, "agents": [],
        }
    msgs = fetch_messages(ids)
    convs = to_conv(sampled, msgs)
    scored = []
    for t in convs:
        sc, is_ret = score_ticket(t)
        scored.append({
            "agent": t["agent_name"],
            "is_retention": is_ret,
            "scores": sc,
            "total": sum(sc.values()),
        })
    agents = aggregate(scored)
    return {
        "label": label,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "total_tickets": len(scored),
        "agents": agents,
    }


def main():
    fetch_cutoff = datetime.now(timezone.utc) - timedelta(days=FETCH_WINDOW_DAYS)
    print(f"Fetch cutoff: {fetch_cutoff.isoformat()} ({FETCH_WINDOW_DAYS}d)", flush=True)

    print("[1/3] Scanning ticket list (one pass)...", flush=True)
    all_tickets = fetch_ticket_list(fetch_cutoff)

    weeks_to_process = compute_weeks()
    print(f"[2/3] Processing {len(weeks_to_process)} weeks...", flush=True)

    weeks_out = []
    for label, start, end in weeks_to_process:
        weeks_out.append(process_week(all_tickets, label, start, end))

    out = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "category_labels": {k: {"label": v[0], "max": v[1]} for k, v in CATEGORY.items()},
        "sections": {s: {"keys": ks, "max": sum(CATEGORY[k][1] for k in ks)} for s, ks in SECTIONS.items()},
        "weeks": weeks_out,
        "current_week_index": len(weeks_out) - 1,  # default to most recent
    }

    out_path = os.path.join(os.getcwd(), "scorecard_data.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

    print(f"\n[3/3] Wrote {out_path}")
    for w in weeks_out:
        top = w["agents"][0]["agent"] if w["agents"] else "(none)"
        top_score = w["agents"][0]["avg_total"] if w["agents"] else 0
        print(f"  {w['label']:30s}  {w['total_tickets']:>4} tix  top: {top} ({top_score})")


if __name__ == "__main__":
    main()
