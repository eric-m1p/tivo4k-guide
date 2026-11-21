#!/usr/bin/env python3
"""Live-sports scraper plugins: one module per site, run through one harness.

Each plugin is a ``sites_<id>.py`` module with ``ORIGIN``, ``BASE`` and
``fetch(limit=0) -> list[dict]`` (feed dicts), plus pure parse functions that
``test_site_snapshots.py`` runs against saved pages in ``snapshots/<id>/``.
StreamCorner is the same idea with a JSON API: ``streamcorner_api.py``
(fetched by distill_events.py's ``--streamcorner`` path, snapshot-tested too).

The harness (``run``) adds what every site needs:

- **page counters** (``live_sites_common.FETCH_STATS``) so a run can tell
  *down* (nothing loads, or the fetch raised) from *layout* (pages load but
  nothing parses — the site changed its markup);
- **backoff**: after ``backoff_after_runs`` down runs in a row a site is skipped
  for 1, 2, 4 … runs (30 min each), capped at ``backoff_max_min``; the first
  run with rows resets it. Layout breaks don't back off (the site is up and
  the next run should notice a fix);
- the verdict goes to ``site_canary.update(scrape=...)``, which names a layout
  change in its alert.

State lives in source_health.json under ``scrapers.<id>``; numbers come from
the feed policy's ``canary`` block.
"""
from __future__ import annotations

import importlib
import time

import live_sites_common as C

PLUGINS = ("isportsurge", "buffstreams", "totalsporteki", "roxiestreams")
DEFAULTS = {"backoff_after_runs": 2, "backoff_max_min": 240}
RUN_MIN = 30  # events workflow cadence; one backoff step


def module_for(site: str):
    return importlib.import_module(f"sites_{site}")


def verdict(rows: int, pages: dict, error: str = "") -> str:
    """ok | layout | down | empty (no rows, no page signal either way)."""
    if rows > 0:
        return "ok"
    if pages.get("ok", 0) > 0:
        return "layout"
    if error or pages.get("fail", 0) > 0 or pages.get("throttled", 0) > 0:
        return "down"
    return "empty"


def should_skip(entry: dict, now: int) -> bool:
    return int(entry.get("skip_until") or 0) > now


def record(entry: dict, outcome: str, now: int, policy: dict) -> None:
    p = {**DEFAULTS, **(policy or {})}
    entry["last_run"] = now
    entry["last_verdict"] = outcome
    if outcome == "ok":
        entry["down_runs"] = 0
        entry.pop("skip_until", None)
        entry["last_ok"] = now
        return
    if outcome != "down":
        entry["down_runs"] = 0
        entry.pop("skip_until", None)
        return
    entry["down_runs"] = int(entry.get("down_runs") or 0) + 1
    over = entry["down_runs"] - int(p["backoff_after_runs"])
    if over >= 0:
        wait = min(int(p["backoff_max_min"]), RUN_MIN * (2 ** over))
        entry["skip_until"] = now + wait * 60


def run(site: str, state: dict, now: int, policy: dict | None = None, limit: int = 0) -> tuple[list[dict], dict]:
    """Fetch one site through the harness -> (feed rows, scrape stats)."""
    entry = state.setdefault("scrapers", {}).setdefault(site, {})
    if should_skip(entry, now):
        left = (int(entry["skip_until"]) - now) // 60
        print(f"[sites] {site}: backing off ({entry.get('down_runs')} down runs; {left} min left)", flush=True)
        return [], {"skipped": True, "verdict": "skipped", "down_runs": entry.get("down_runs", 0)}
    started = time.monotonic()
    C.begin_site(site)
    rows: list[dict] = []
    error = ""
    try:
        rows = module_for(site).fetch(limit=limit) or []
    except Exception as exc:  # noqa: BLE001 - one bad site must not break the feed
        error = f"{type(exc).__name__}: {str(exc)[:160]}"
        print(f"[sites] {site}: FAILED ({error})", flush=True)
    finally:
        pages = C.end_site()
    outcome = verdict(len(rows), pages, error)
    record(entry, outcome, now, policy or {})
    stats = {
        "rows": len(rows), "pages": pages, "verdict": outcome, "error": error,
        "seconds": round(time.monotonic() - started, 1), "down_runs": entry.get("down_runs", 0),
    }
    entry["last_stats"] = {k: v for k, v in stats.items() if k != "down_runs"}
    if outcome == "layout":
        print(f"[sites] {site}: {pages.get('ok')} pages loaded but 0 rows parsed — layout change?", flush=True)
    return rows, stats
