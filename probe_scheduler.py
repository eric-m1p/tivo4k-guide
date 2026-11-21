#!/usr/bin/env python3
"""Budgeted source-health scheduler: which URLs get probed this run.

Every number comes from the feed policy's ``probe`` block (feed_policy.json):

- a global page-probe budget per hour, scaled to the time since the last run;
- events inside a live window (``live_before_min`` before start to
  ``live_after_min`` after end) get their ``top_n`` best sources probed every run;
- 24/7 channels are a sparse rotating sample: each URL at most once every
  ``always_on_every_hours``, oldest first;
- leftover budget goes to in-window sources below the top N, then to upcoming
  events (soonest first), each also at most once every ``always_on_every_hours``.

``plan_probes`` is passed to ``source_health.apply_source_health(planner=...)``;
it returns the page-probe list and the playback-probe priority order, and
records what it picked in the state (``probed``: url hash -> last probe time).
"""

from __future__ import annotations

import hashlib

import source_health as sh

DEFAULTS = {
    "budget_per_hour": 1200,
    "live_before_min": 30,
    "live_after_min": 15,
    "top_n": 4,
    "always_on_every_hours": 6,
}
DEFAULT_EVENT_HOURS = 4  # an event with no ends_at is treated as this long


def url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]


def run_budget(policy: dict, state: dict, now: int) -> int:
    """budget_per_hour scaled to the time since the last run (15 min .. 1 h)."""
    per_hour = int(policy.get("budget_per_hour", DEFAULTS["budget_per_hour"]))
    last = int(state.get("last_probe_run") or 0)
    hours = 0.5 if last <= 0 else min(1.0, max(0.25, (now - last) / 3600))
    return max(0, round(per_hour * hours))


def in_window(event: dict, now: int, before_min: int, after_min: int) -> bool:
    status = str(event.get("status") or "")
    if status == "live":
        return True
    if status != "future":
        return False
    starts = int(event.get("starts_at") or 0)
    if starts <= 0:
        return False
    ends = int(event.get("ends_at") or 0) or starts + DEFAULT_EVENT_HOURS * 3600
    return starts - before_min * 60 <= now <= ends + after_min * 60


def plan_probes(events: list[dict], state: dict, now: int, *,
                policy: dict | None = None, blocklist: dict | None = None) -> dict:
    p = {**DEFAULTS, **(policy or {})}
    top_n = max(1, int(p["top_n"]))
    every = max(1, int(p["always_on_every_hours"])) * 3600
    before, after = int(p["live_before_min"]), int(p["live_after_min"])
    budget = run_budget(p, state, now)

    probed: dict = state.setdefault("probed", {})
    keep = max(every, 24 * 3600)
    for key, ts in list(probed.items()):
        if now - int(ts or 0) > keep:
            probed.pop(key, None)

    def due(url: str) -> bool:
        return now - int(probed.get(url_key(url)) or 0) >= every

    def ranked(event: dict) -> list[str]:
        return [
            str(src.get("url") or "").strip()
            for src in sh.ranked_sources(event.get("sources") or [], state, now, blocklist=blocklist)
        ]

    window = sorted(
        (e for e in events if in_window(e, now, before, after)),
        key=lambda e: (0 if e.get("status") == "live" else 1, int(e.get("starts_at") or 0)),
    )
    window_ranked = [ranked(e) for e in window]
    tier1 = [url for urls in window_ranked for url in urls[:top_n]]

    always = []
    for event in events:
        if event.get("status") == "always_live":
            always += [url for url in ranked(event) if due(url)]
    always.sort(key=lambda url: int(probed.get(url_key(url)) or 0))

    spill = []
    depth = max((len(urls) for urls in window_ranked), default=0)
    for rank in range(top_n, depth):  # interleave: every event's 5th source, then 6th, ...
        spill += [urls[rank] for urls in window_ranked if rank < len(urls) and due(urls[rank])]
    upcoming = sorted(
        (e for e in events if e.get("status") == "future" and not in_window(e, now, before, after)),
        key=lambda e: int(e.get("starts_at") or 0),
    )
    for event in upcoming:
        spill += [url for url in ranked(event)[:top_n] if due(url)]

    page: list[str] = []
    seen: set[str] = set()
    counts = {"window_events": len(window), "live_window": 0, "always_on": 0, "spill": 0}
    for tier, urls in (("live_window", tier1), ("always_on", always), ("spill", spill)):
        for url in urls:
            if len(page) >= budget:
                break
            if not url.startswith("https://") or url in seen:
                continue
            seen.add(url)
            page.append(url)
            counts[tier] += 1

    for url in page:
        probed[url_key(url)] = now
    state["last_probe_run"] = now
    counts["budget"] = budget
    print(
        f"[schedule] budget={budget} window_events={counts['window_events']} "
        f"live={counts['live_window']} always_on={counts['always_on']} spill={counts['spill']}",
        flush=True,
    )
    # Playback checks follow the same priority: live events first.
    return {"page": page, "playback": page, "counts": counts}
