#!/usr/bin/env python3
"""Persistent source health for the Stream Sites TV feed.

The feed frequently carries 10-60 mirrors for one event. Most are duplicates or
dead hosts, and trying them in order wastes the first minutes of playback. This
module:

- probes candidate embed URLs before events.json is published;
- keeps a committed history (`source_health.json`) of ok/fail streaks;
- ranks healthy, higher-quality mirrors first and caps each event at
  `MAX_SOURCES_PER_EVENT`;
- quarantines a URL after repeated failures and retries it after a cooldown.

Probing is deliberately conservative: if more than half of a CI run's probes
fail (for example the runner's IP is blocked by several embed hosts), failure
results are not applied, so a bad network path cannot gut the feed.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

DEFAULT_STATE = "source_health.json"
MAX_SOURCES_PER_EVENT = 8
MAX_SOURCES_247 = 4
PROBE_BUDGET = 3000
PROBE_WORKERS = 32
PROBE_TIMEOUT = 8
CANDIDATE_POOL = 16
DISABLE_STREAK = 3
DISABLE_TOTAL = 8
QUARANTINE_HOURS = (6, 12, 24, 72)
KEEP_DAYS = 30
FAILURE_TRUST_THRESHOLD = 0.65

PROBE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/138.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://streamcorner.cloud/",
}


def _now() -> int:
    return int(time.time())


def quality_rank(source: dict) -> int:
    """0 = 4K/1080p, 1 = HD/720p, 2 = unknown, 3 = SD. Matches the app."""
    label = str(source.get("label") or "").lower()
    url = str(source.get("url") or "").lower()
    if (
        "_540p" in url or "_480p" in url or "_360p" in url
        or "540p" in label or "480p" in label or "360p" in label
        or label.startswith("sd") or " sd" in label or "sd |" in label
    ):
        return 3
    if any(k in label or k in url for k in ("4k", "uhd", "2160p", "1080p", "fhd")):
        return 0
    if (
        label.startswith("hd") or " hd" in label or "hd |" in label
        or "primary hd" in label or "720p" in label or "720p" in url
    ):
        return 1
    return 2


def _entry(state: dict, url: str) -> dict:
    entries = state.setdefault("sources", {})
    entry = entries.get(url)
    if not isinstance(entry, dict):
        entry = {}
        entries[url] = entry
    entry.setdefault("ok", 0)
    entry.setdefault("fail", 0)
    entry.setdefault("streak", 0)
    entry.setdefault("last_ok", 0)
    entry.setdefault("last_fail", 0)
    entry.setdefault("disabled_until", 0)
    entry.setdefault("quarantines", 0)
    return entry


def load_state(path: str | os.PathLike) -> dict:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("sources"), dict):
            return raw
    except Exception:
        pass
    return {"version": 1, "updated_at": 0, "sources": {}}


def save_state(path: str | os.PathLike, state: dict, stats: dict) -> None:
    now = _now()
    state["version"] = 1
    state["updated_at"] = now
    state["stats"] = stats
    # Drop old, healthy URLs but keep anything currently quarantined.
    cutoff = now - KEEP_DAYS * 86400
    entries = state.get("sources", {})
    for url, entry in list(entries.items()):
        last = max(int(entry.get("last_ok") or 0), int(entry.get("last_fail") or 0))
        if (int(entry.get("fail") or 0) <= 0 and int(entry.get("disabled_until") or 0) <= now):
            entries.pop(url, None)
        elif last < cutoff and int(entry.get("disabled_until") or 0) <= now:
            entries.pop(url, None)
    # Hard cap keeps the committed JSON small even after months of churn.
    if len(entries) > 8000:
        ranked = sorted(
            entries.items(),
            key=lambda kv: (int(kv[1].get("disabled_until") or 0), int(kv[1].get("last_fail") or 0)),
            reverse=True,
        )
        state["sources"] = dict(ranked[:8000])
    tmp = str(path) + ".tmp"
    Path(tmp).write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def is_disabled(entry: dict, now: int) -> bool:
    return int(entry.get("disabled_until") or 0) > now


def _health_rank(entry: dict) -> tuple:
    return (
        1 if int(entry.get("streak") or 0) >= DISABLE_STREAK else 0,
        int(entry.get("streak") or 0),
        int(entry.get("fail") or 0),
        -int(entry.get("ok") or 0),
    )


def ranked_sources(
    sources: list[dict],
    state: dict,
    now: int,
    *,
    include_disabled: bool = False,
) -> list[dict]:
    entries = state.get("sources", {})
    out = []
    seen = set()
    for index, src in enumerate(sources or []):
        url = str(src.get("url") or "").strip()
        if not url.startswith("https://") or url in seen:
            continue
        entry = entries.get(url, {})
        if not include_disabled and is_disabled(entry, now):
            continue
        seen.add(url)
        out.append((src, index, entry))
    out.sort(key=lambda item: (
        quality_rank(item[0]),
        _health_rank(item[2]),
        item[1],
    ))
    return [item[0] for item in out]


def _probe_one(url: str) -> tuple[str, bool, str]:
    try:
        import requests  # imported lazily so --no-probe-sources has no dependency
    except Exception as exc:  # pragma: no cover - configuration error
        return url, False, "requests missing: " + str(exc)[:60]

    try:
        with requests.get(
            url,
            headers=PROBE_HEADERS,
            timeout=(PROBE_TIMEOUT, PROBE_TIMEOUT),
            stream=True,
            allow_redirects=True,
        ) as response:
            code = response.status_code
            try:
                next(response.iter_content(1024), b"")
            except Exception:
                pass
            return url, 200 <= code < 400, str(code)
    except Exception as exc:
        return url, False, type(exc).__name__


def probe_sources(urls: list[str], workers: int = PROBE_WORKERS) -> dict[str, tuple[bool, str]]:
    results: dict[str, tuple[bool, str]] = {}
    if not urls:
        return results
    workers = max(1, min(workers, 64))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_probe_one, url): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                _, ok, detail = future.result()
            except Exception as exc:
                ok, detail = False, type(exc).__name__
            results[url] = (ok, detail)
    return results


def _record(state: dict, url: str, ok: bool, detail: str, now: int, trust_failures: bool) -> None:
    entries = state.setdefault("sources", {})
    if ok:
        # A working URL needs no history. Keeping only problem URLs makes the
        # committed JSON small and its diffs readable.
        entries.pop(url, None)
        return
    if not trust_failures:
        return
    entry = _entry(state, url)
    entry["last_status"] = str(detail)[:24]
    entry["fail"] = min(int(entry.get("fail") or 0) + 1, 9999)
    entry["streak"] = min(int(entry.get("streak") or 0) + 1, 99)
    entry["last_fail"] = now
    if int(entry["streak"]) >= DISABLE_STREAK or int(entry["fail"]) >= DISABLE_TOTAL:
        q = min(int(entry.get("quarantines") or 0), len(QUARANTINE_HOURS) - 1)
        entry["disabled_until"] = now + QUARANTINE_HOURS[q] * 3600
        entry["quarantines"] = min(q + 1, len(QUARANTINE_HOURS) - 1)


def apply_source_health(
    events: list[dict],
    state_path: str | os.PathLike = DEFAULT_STATE,
    *,
    max_sources: int = MAX_SOURCES_PER_EVENT,
    probe_budget: int = PROBE_BUDGET,
    do_probe: bool = True,
    workers: int = PROBE_WORKERS,
) -> dict:
    """Probe, rank and trim sources on feed-shaped event dictionaries."""
    now = _now()
    state = load_state(state_path)
    entries = state.setdefault("sources", {})

    candidates_by_event: list[list[dict]] = []
    unique: list[str] = []
    seen: set[str] = set()
    for event in events:
        limit = MAX_SOURCES_247 if event.get("status") == "always_live" else max(CANDIDATE_POOL, max_sources * 2)
        pool = ranked_sources(event.get("sources") or [], state, now)[:limit]
        candidates_by_event.append(pool)
        for src in pool:
            url = str(src.get("url") or "").strip()
            if url and url not in seen and len(unique) < probe_budget:
                seen.add(url)
                unique.append(url)

    results: dict[str, tuple[bool, str]] = {}
    trusted = True
    if do_probe and unique:
        results = probe_sources(unique, workers=workers)
        checked = len(results)
        failed = sum(1 for ok, _ in results.values() if not ok)
        failure_rate = failed / checked if checked else 0.0
        trusted = checked < 20 or failure_rate <= FAILURE_TRUST_THRESHOLD
        if not trusted:
            print(
                f"[health] WARNING: {failed}/{checked} probes failed ({failure_rate:.0%}); "
                "ignoring failures this run (runner network looks blocked)",
                flush=True,
            )
        for url, (ok, detail) in results.items():
            _record(state, url, ok, detail, now, trust_failures=trusted)

    trimmed = 0
    empty = 0
    for index, event in enumerate(events):
        limit = MAX_SOURCES_247 if event.get("status") == "always_live" else max_sources
        source_list = event.get("sources") or []
        ranked = ranked_sources(source_list, state, now)
        selected = []
        for src in ranked:
            url = str(src.get("url") or "").strip()
            entry = entries.get(url, {})
            if is_disabled(entry, now):
                continue
            probe = results.get(url)
            if probe is not None and not probe[0]:
                # A failed probe is a demotion. Keep it only as a last-resort
                # mirror when there is nothing healthier and it has not been
                # repeatedly failing.
                if int(entry.get("streak") or 0) >= 2 or int(entry.get("fail") or 0) >= 3:
                    continue
                selected.append((False, src))
                continue
            selected.append((True, src))

        # Healthy/probe-ok first, then quality/health rank (already sorted).
        selected.sort(key=lambda item: (0 if item[0] else 1,))
        final = [src for _, src in selected[:limit]]
        if len(final) != len(source_list):
            trimmed += 1
        if not final:
            empty += 1
        event["sources"] = final

    stats = {
        "probed": len(results),
        "probe_ok": sum(1 for ok, _ in results.values() if ok),
        "probe_failed": sum(1 for ok, _ in results.values() if not ok),
        "trusted": trusted,
        "trimmed_events": trimmed,
        "empty_events": empty,
        "max_sources": max_sources,
    }
    if do_probe:
        save_state(state_path, state, stats)
    print(
        "[health] "
        + (f"probed={stats['probed']} ok={stats['probe_ok']} failed={stats['probe_failed']} "
           if do_probe else "probe=off ")
        + f"trimmed={trimmed} empty={empty} cap={max_sources}",
        flush=True,
    )
    return stats
