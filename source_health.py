#!/usr/bin/env python3
"""Persistent source health for the Stream Sites TV feed.

The feed frequently carries 10-60 mirrors for one event. Most are duplicates or
dead hosts, and trying them in order wastes the first minutes of playback. This
module:

- probes candidate embed URLs before events.json is published;
- keeps a committed history (`source_health.json`) of ok/fail streaks;
- ranks healthy, higher-quality mirrors first and caps each event at
  `MAX_SOURCES_PER_EVENT`;
- quarantines a URL after repeated failures and retries it after a cooldown;
- permanently blocks URLs after definitive 404/410s or too many failed cycles,
  plus an optional manual `source_blocklist.json`.

Probing is deliberately conservative: if more than half of a CI run's probes
fail (for example the runner's IP is blocked by several embed hosts), failure
results are not applied, so a bad network path cannot gut the feed.
"""

from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

DEFAULT_STATE = "source_health.json"
DEFAULT_BLOCKLIST = "source_blocklist.json"
MAX_SOURCES_PER_EVENT = 8
MAX_SOURCES_247 = 4
PROBE_BUDGET = 3000
PROBE_WORKERS = 32
PROBE_TIMEOUT = 8
PROBE_BODY_LIMIT = 64 * 1024
MANIFEST_LIMIT = 96 * 1024
CANDIDATE_POOL = 16
DISABLE_STREAK = 3
DISABLE_TOTAL = 8
QUARANTINE_HOURS = (6, 12, 24, 72)
KEEP_DAYS = 30
FAILURE_TRUST_THRESHOLD = 0.65
DEFINITIVE_STATUSES = {"404", "410"}
PERMANENT_FAIL_TOTAL = 6
PERMANENT_QUARANTINES = 2

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
    height = int(source.get("height") or 0)
    if height >= 1080:
        return 0
    if height >= 720:
        return 1
    if height > 0:
        return 3
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


def _migrate_entry(entry: dict) -> None:
    """Backfill block state for files written before permanent blocks existed."""
    if not isinstance(entry, dict):
        return
    entry.setdefault("dead_fails", 0)
    entry.setdefault("blocked", False)
    status = str(entry.get("last_status") or "")
    fail = int(entry.get("fail") or 0)
    if status in DEFINITIVE_STATUSES and fail >= 2:
        entry["dead_fails"] = max(int(entry.get("dead_fails") or 0), fail)
    if int(entry.get("dead_fails") or 0) >= 2:
        entry["blocked"] = True
        entry.setdefault("blocked_reason", f"HTTP {status} x{entry['dead_fails']}")
    elif fail >= PERMANENT_FAIL_TOTAL and int(entry.get("quarantines") or 0) >= PERMANENT_QUARANTINES:
        entry["blocked"] = True
        entry.setdefault("blocked_reason", f"failed {fail}x across {entry['quarantines']} quarantines")
    if entry.get("blocked"):
        entry["disabled_until"] = 0


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
    entry.setdefault("dead_fails", 0)
    entry.setdefault("blocked", False)
    entry.setdefault("height", 0)
    entry.setdefault("bitrate", 0)
    return entry


def load_state(path: str | os.PathLike) -> dict:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("sources"), dict):
            for entry in raw["sources"].values():
                _migrate_entry(entry)
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
        if entry.get("blocked"):
            continue
        last = max(int(entry.get("last_ok") or 0), int(entry.get("last_fail") or 0))
        if (int(entry.get("height") or 0) > 0):
            continue
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
    return bool(entry.get("blocked")) or int(entry.get("disabled_until") or 0) > now


def is_blocked(entry: dict) -> bool:
    return bool(entry.get("blocked"))


def load_blocklist(path: str | os.PathLike | None) -> dict:
    """Manual permanent denies: {"urls": [...], "hosts": [...]} (lists or maps)."""
    result = {"urls": set(), "hosts": set(), "reasons": {}}
    if not path:
        return result
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return result
    for key, bucket in (("urls", "urls"), ("hosts", "hosts")):
        values = raw.get(bucket)
        if isinstance(values, dict):
            for item, reason in values.items():
                result[key].add(str(item).strip())
                result["reasons"][str(item).strip()] = str(reason)
        elif isinstance(values, list):
            for item in values:
                result[key].add(str(item).strip())
    result["urls"].discard("")
    result["hosts"].discard("")
    return result


def host_of(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def is_manually_blocked(url: str, blocklist: dict) -> bool:
    if not blocklist:
        return False
    return url in blocklist.get("urls", set()) or host_of(url) in blocklist.get("hosts", set())


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
    blocklist: dict | None = None,
) -> list[dict]:
    entries = state.get("sources", {})
    blocklist = blocklist or {"urls": set(), "hosts": set()}
    out = []
    seen = set()
    for index, src in enumerate(sources or []):
        url = str(src.get("url") or "").strip()
        if not url.startswith("https://") or url in seen:
            continue
        entry = entries.get(url, {})
        if not src.get("height") and int(entry.get("height") or 0) > 0:
            src = dict(src)
            src["height"] = int(entry.get("height") or 0)
            src["bitrate"] = int(entry.get("bitrate") or 0)
            src["verified"] = True
        if is_manually_blocked(url, blocklist):
            continue
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


def _read_limited(response, limit: int) -> bytes:
    chunks = []
    total = 0
    try:
        for chunk in response.iter_content(8192):
            if not chunk:
                continue
            chunks.append(chunk)
            total += len(chunk)
            if total >= limit:
                break
    except Exception:
        pass
    return b"".join(chunks)[:limit]


def _manifest_from_text(text: str) -> str:
    text = text.replace("\\/", "/")
    match = re.search(r'https?://[^\s"\'<>]+?\.(?:m3u8|mpd)(?:\?[^\s"\'<>]*)?', text, re.I)
    return match.group(0) if match else ""


def _quality_from_manifest(text: str) -> tuple[int, int]:
    height = 0
    bitrate = 0
    for match in re.finditer(r"RESOLUTION=(\d+)x(\d+)", text, re.I):
        height = max(height, int(match.group(2)))
    for match in re.finditer(r"BANDWIDTH=(\d+)", text, re.I):
        bitrate = max(bitrate, int(match.group(1)))
    for tag in re.findall(r"<Representation\b[^>]*>", text, re.I):
        h = re.search(r'height="(\d+)"', tag, re.I)
        b = re.search(r'bandwidth="(\d+)"', tag, re.I)
        if h:
            height = max(height, int(h.group(1)))
        if b:
            bitrate = max(bitrate, int(b.group(1)))
    return height, bitrate


def _quality_meta(url: str, text: str, headers: dict, timeout: int) -> dict:
    height = 0
    bitrate = 0
    manifest = ""
    lowered = url.lower().split("?")[0]
    if lowered.endswith((".m3u8", ".mpd")):
        manifest = url
        height, bitrate = _quality_from_manifest(text)
    else:
        found = _manifest_from_text(text)
        if found:
            manifest = found
            try:
                import requests
                with requests.get(
                    found,
                    headers={**headers, "Referer": url},
                    timeout=(timeout, timeout),
                    stream=True,
                    allow_redirects=True,
                ) as response:
                    if 200 <= response.status_code < 400:
                        body = _read_limited(response, MANIFEST_LIMIT).decode("utf-8", "replace")
                        height, bitrate = _quality_from_manifest(body)
            except Exception:
                pass
    return {
        "height": height,
        "bitrate": bitrate,
        "manifest": manifest[:400],
        "verified": height > 0,
        "quality": _quality_label(height),
    }


def _quality_label(height: int) -> str:
    if height >= 2160:
        return "4K"
    if height >= 1080:
        return "1080p"
    if height >= 720:
        return "720p"
    if height >= 480:
        return "480p"
    if height > 0:
        return "SD"
    return ""


def _probe_one(url: str, timeout: int = PROBE_TIMEOUT) -> tuple[str, bool, str, int, dict]:
    started = time.monotonic()
    try:
        import requests  # imported lazily so --no-probe-sources has no dependency
    except Exception as exc:  # pragma: no cover - configuration error
        return url, False, "requests missing: " + str(exc)[:60], 0, {}

    try:
        with requests.get(
            url,
            headers=PROBE_HEADERS,
            timeout=(timeout, timeout),
            stream=True,
            allow_redirects=True,
        ) as response:
            code = response.status_code
            ctype = response.headers.get("content-type", "")
            body = _read_limited(response, PROBE_BODY_LIMIT)
            text = body.decode("utf-8", "replace")
            meta = _quality_meta(url, text, PROBE_HEADERS, timeout)
            meta["content_type"] = ctype[:80]
            latency_ms = int((time.monotonic() - started) * 1000)
            return url, 200 <= code < 400, str(code), latency_ms, meta
    except Exception as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return url, False, type(exc).__name__, latency_ms, {}


def probe_sources(
    urls: list[str], workers: int = PROBE_WORKERS, timeout: int = PROBE_TIMEOUT
) -> dict[str, tuple[bool, str]]:
    detailed = probe_sources_detailed(urls, workers=workers, timeout=timeout)
    return {url: (item["ok"], item["status"]) for url, item in detailed.items()}


def probe_sources_detailed(
    urls: list[str], workers: int = PROBE_WORKERS, timeout: int = PROBE_TIMEOUT
) -> dict[str, dict]:
    """Probe URLs and return status plus latency; used by the rack/report CLI."""
    results: dict[str, dict] = {}
    if not urls:
        return results
    workers = max(1, min(workers, 64))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_probe_one, url, timeout): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                _, ok, detail, latency_ms, meta = future.result()
            except Exception as exc:
                ok, detail, latency_ms, meta = False, type(exc).__name__, 0, {}
            status = str(detail)
            results[url] = {
                "ok": ok,
                "status": status,
                "latency_ms": latency_ms,
                "error": "" if status.isdigit() else status,
                "height": int(meta.get("height") or 0),
                "bitrate": int(meta.get("bitrate") or 0),
                "manifest": str(meta.get("manifest") or ""),
                "verified": bool(meta.get("verified")),
                "quality": str(meta.get("quality") or ""),
                "content_type": str(meta.get("content_type") or ""),
            }
    return results


def _record(state: dict, url: str, ok: bool, detail: str, now: int,
            trust_failures: bool, height: int = 0, bitrate: int = 0,
            manifest: str = "") -> None:
    entries = state.setdefault("sources", {})
    if ok:
        entry = entries.get(url)
        if int(height or 0) > 0:
            entry = _entry(state, url)
            entry["height"] = int(height)
            entry["bitrate"] = int(bitrate or 0)
            if manifest:
                entry["manifest"] = manifest[:300]
            entry["verified"] = True
            entry["fail"] = 0
            entry["streak"] = 0
            entry["disabled_until"] = 0
            entry["blocked"] = False
            entry.pop("blocked_reason", None)
            entry.pop("blocked_at", None)
            if not entry.get("last_ok"):
                entry["last_ok"] = now
            return
        if entry and int(entry.get("height") or 0) > 0:
            entry["fail"] = 0
            entry["streak"] = 0
            entry["disabled_until"] = 0
            return
        # A working URL with no measured quality needs no history.
        entries.pop(url, None)
        return
    if not trust_failures:
        return
    entry = _entry(state, url)
    status = str(detail)[:24]
    entry["last_status"] = status
    entry["fail"] = min(int(entry.get("fail") or 0) + 1, 9999)
    entry["streak"] = min(int(entry.get("streak") or 0) + 1, 99)
    entry["last_fail"] = now
    if status in DEFINITIVE_STATUSES:
        entry["dead_fails"] = min(int(entry.get("dead_fails") or 0) + 1, 99)
    if int(entry.get("dead_fails") or 0) >= 2:
        entry["blocked"] = True
        entry["blocked_reason"] = f"HTTP {status} x{entry['dead_fails']}"
        entry["blocked_at"] = now
        entry["disabled_until"] = 0
        return
    if int(entry["streak"]) >= DISABLE_STREAK or int(entry["fail"]) >= DISABLE_TOTAL:
        q = min(int(entry.get("quarantines") or 0), len(QUARANTINE_HOURS) - 1)
        entry["disabled_until"] = now + QUARANTINE_HOURS[q] * 3600
        entry["quarantines"] = min(q + 1, len(QUARANTINE_HOURS) - 1)
    if int(entry.get("fail") or 0) >= PERMANENT_FAIL_TOTAL and int(entry.get("quarantines") or 0) >= PERMANENT_QUARANTINES:
        entry["blocked"] = True
        entry["blocked_reason"] = f"failed {entry['fail']}x across {entry['quarantines']} quarantines"
        entry["blocked_at"] = now
        entry["disabled_until"] = 0


def apply_source_health(
    events: list[dict],
    state_path: str | os.PathLike = DEFAULT_STATE,
    *,
    max_sources: int = MAX_SOURCES_PER_EVENT,
    probe_budget: int = PROBE_BUDGET,
    do_probe: bool = True,
    workers: int = PROBE_WORKERS,
    blocklist_path: str | os.PathLike | None = None,
    probe_all: bool = False,
) -> dict:
    """Probe, rank and trim sources on feed-shaped event dictionaries."""
    now = _now()
    state = load_state(state_path)
    entries = state.setdefault("sources", {})
    blocklist = load_blocklist(blocklist_path)

    candidates_by_event: list[list[dict]] = []
    unique: list[str] = []
    seen: set[str] = set()
    all_unique: set[str] = set()
    raw_unique: set[str] = set()
    blocked_in_feed: set[str] = set()
    quarantined_in_feed: set[str] = set()
    for event in events:
        for src in event.get("sources") or []:
            url = str(src.get("url") or "").strip()
            if not url.startswith("https://"):
                continue
            raw_unique.add(url)
            entry = entries.get(url, {})
            if is_manually_blocked(url, blocklist) or is_blocked(entry):
                blocked_in_feed.add(url)
            elif is_disabled(entry, now):
                quarantined_in_feed.add(url)
        limit = MAX_SOURCES_247 if event.get("status") == "always_live" else max(CANDIDATE_POOL, max_sources * 2)
        ranked = ranked_sources(event.get("sources") or [], state, now, blocklist=blocklist)
        pool = ranked if probe_all else ranked[:limit]
        candidates_by_event.append(pool)
        for src in pool:
            url = str(src.get("url") or "").strip()
            if not url:
                continue
            all_unique.add(url)
            if url not in seen and len(unique) < probe_budget:
                seen.add(url)
                unique.append(url)

    results: dict[str, tuple[bool, str]] = {}
    trusted = True
    detailed: dict[str, dict] = {}
    if do_probe and unique:
        detailed = probe_sources_detailed(unique, workers=workers)
        results = {url: (info["ok"], info["status"]) for url, info in detailed.items()}
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
        for url, info in detailed.items():
            _record(
                state, url, bool(info["ok"]), str(info["status"]), now,
                trust_failures=trusted,
                height=int(info.get("height") or 0),
                bitrate=int(info.get("bitrate") or 0),
                manifest=str(info.get("manifest") or ""),
            )
        if not trusted:
            # Do not demote anything when the runner's probes look blocked.
            results = {url: value for url, value in results.items() if value[0]}

    trimmed = 0
    empty = 0
    for index, event in enumerate(events):
        limit = MAX_SOURCES_247 if event.get("status") == "always_live" else max_sources
        source_list = event.get("sources") or []
        for src in source_list:
            url = str(src.get("url") or "").strip()
            info = detailed.get(url)
            entry = entries.get(url, {})
            if info:
                if int(info.get("height") or 0) > 0:
                    src["height"] = int(info["height"])
                    src["bitrate"] = int(info.get("bitrate") or 0)
                    src["verified"] = True
                    src["quality"] = str(info.get("quality") or "")
            elif int(entry.get("height") or 0) > 0:
                src["height"] = int(entry["height"])
                src["bitrate"] = int(entry.get("bitrate") or 0)
                src["verified"] = True
        ranked = ranked_sources(source_list, state, now, blocklist=blocklist)
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
        "sources_raw": len(raw_unique),
        "sources_seen": len(all_unique),
        "sources_blocked": len(blocked_in_feed),
        "sources_quarantined": len(quarantined_in_feed),
        "unprobed": max(0, len(all_unique) - len(set(unique))),
        "probe_all": probe_all,
        "blocked": sum(1 for entry in entries.values() if entry.get("blocked")),
        "manual_url_blocks": len(blocklist.get("urls", set())),
        "manual_host_blocks": len(blocklist.get("hosts", set())),
    }
    if do_probe:
        save_state(state_path, state, stats)
    print(
        "[health] "
        + (f"probed={stats['probed']} ok={stats['probe_ok']} failed={stats['probe_failed']} "
           if do_probe else "probe=off ")
        + f"raw={stats['sources_raw']} seen={stats['sources_seen']} "
          f"blocked={stats['sources_blocked']} quarantined={stats['sources_quarantined']} "
          f"unprobed={stats['unprobed']} "
          f"trimmed={trimmed} empty={empty} cap={max_sources} "
          f"blocked_state={stats['blocked']} manual={stats['manual_url_blocks']}u/{stats['manual_host_blocks']}h",
        flush=True,
    )
    return stats
