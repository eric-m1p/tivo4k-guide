#!/usr/bin/env python3
"""Persistent source health for the Slang feed.

The feed frequently carries 10-60 mirrors for one event. Most are duplicates or
dead hosts, and trying them in order wastes the first minutes of playback. This
module:

- probes candidate embed URLs before events.json is published;
- keeps a committed history (`source_health.json`) of time-decayed URL,
  stream-family, and host reliability plus vantage streaks;
- ranks healthy, higher-quality mirrors first with host-interleaved failover
  order and caps each event at `MAX_SOURCES_PER_EVENT`;
- quarantines a URL after repeated failures and retries it after a cooldown;
- permanently blocks URLs after definitive 404/410s or too many failed cycles,
  plus an optional manual `source_blocklist.json`.

Probing is deliberately conservative: if more than half of a CI run's probes
fail (for example the runner's IP is blocked by several embed hosts), failure
results are not applied, so a bad network path cannot gut the feed.
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urljoin

DEFAULT_STATE = "source_health.json"
DEFAULT_BLOCKLIST = "source_blocklist.json"
MAX_SOURCES_PER_EVENT = 12
MAX_SOURCES_247 = 4
PROBE_BUDGET = 3000
PROBE_WORKERS = 32
PROBE_TIMEOUT = 8
PROBE_BODY_LIMIT = 64 * 1024
MANIFEST_LIMIT = 96 * 1024
CANDIDATE_POOL = 16
# Weak mirrors (dead now, measured or labelled below 720p, or mostly failing)
# only fill an event up to this many.
MIN_SOURCES_PER_EVENT = 3
LOW_QUALITY_HEIGHT = 720
DISABLE_STREAK = 3
DISABLE_TOTAL = 8
QUARANTINE_HOURS = (6, 12, 24, 72)
# Source health v2+: a quarantine needs DISABLE_STREAK failures in a row from one
# vantage point (the CI runner, or one device) or failures from QUARANTINE_VANTAGES
# vantage points within FAIL_MEMORY_HOURS. App playback outweighs probes.
QUARANTINE_VANTAGES = 2
FAIL_MEMORY_HOURS = 24
FAIL_OMIT = 3
APP_WEIGHT = 3.0
SCAN_WEIGHT = 0.5
HOST_WEIGHT = 2.0
FAMILY_WEIGHT = 1.0
PAGE_WEIGHT = 0.35
PLAYBACK_WEIGHT = 1.0
HOST_HALF_LIFE_DAYS = 7.0
URL_HALF_LIFE_HOURS = 48.0
CI_VANTAGE = "ci"
# The runner was refused, which says nothing about viewers: never a failure on
# its own, only alongside a recent app failure.
SERVER_BLOCKED_STATUSES = {"401", "403", "407", "429", "451", "manifest-403"}


def _load_policy_thresholds() -> None:
    """Thresholds ship in feed_policy.json (the feed's policy block); constants above are fallbacks."""
    global MIN_SOURCES_PER_EVENT, LOW_QUALITY_HEIGHT, DISABLE_STREAK, DISABLE_TOTAL, QUARANTINE_HOURS
    global QUARANTINE_VANTAGES, FAIL_MEMORY_HOURS, FAIL_OMIT, APP_WEIGHT, SCAN_WEIGHT
    global HOST_HALF_LIFE_DAYS, URL_HALF_LIFE_HOURS, MAX_SOURCES_PER_EVENT, MAX_SOURCES_247
    here = os.path.dirname(os.path.abspath(__file__))
    policy = None
    # guide-repo keeps the file next to this module; the iptv/ copy reads guide-repo's.
    for path in (os.path.join(here, "feed_policy.json"), os.path.join(here, "guide-repo", "feed_policy.json")):
        try:
            with open(path, encoding="utf-8") as f:
                policy = json.load(f)["policy"]
            break
        except (OSError, ValueError, KeyError):
            continue
    if policy is None:
        return
    weak, health = policy.get("weak_source") or {}, policy.get("health") or {}
    MIN_SOURCES_PER_EVENT = int(weak.get("min_sources", MIN_SOURCES_PER_EVENT))
    LOW_QUALITY_HEIGHT = int(weak.get("min_height", LOW_QUALITY_HEIGHT))
    DISABLE_STREAK = int(health.get("quarantine_streak", DISABLE_STREAK))
    DISABLE_TOTAL = int(health.get("fail_total_disable", DISABLE_TOTAL))
    QUARANTINE_HOURS = tuple(int(h) for h in health.get("quarantine_hours") or QUARANTINE_HOURS)
    QUARANTINE_VANTAGES = int(health.get("quarantine_vantages", QUARANTINE_VANTAGES))
    FAIL_MEMORY_HOURS = int(health.get("fail_memory_hours", FAIL_MEMORY_HOURS))
    FAIL_OMIT = int(health.get("fail_omit", FAIL_OMIT))
    APP_WEIGHT = float(health.get("app_weight", APP_WEIGHT))
    SCAN_WEIGHT = float(health.get("scan_weight", SCAN_WEIGHT))
    HOST_HALF_LIFE_DAYS = float(health.get("host_half_life_days", HOST_HALF_LIFE_DAYS))
    URL_HALF_LIFE_HOURS = float(health.get("url_half_life_hours", URL_HALF_LIFE_HOURS))
    if "max_sources_per_event" in health:
        MAX_SOURCES_PER_EVENT = int(health["max_sources_per_event"])
    if "max_sources_247" in health:
        MAX_SOURCES_247 = int(health["max_sources_247"])


_load_policy_thresholds()
KEEP_DAYS = 30
FAILURE_TRUST_THRESHOLD = 0.65
DEFINITIVE_STATUSES = {"404", "410"}
MANIFEST_DEAD_STATUSES = {"403", "404", "410"}
PERMANENT_FAIL_TOTAL = 6
PERMANENT_QUARANTINES = 2
# Headless-browser quality pass. Static HTML only sees the outer embed page; most
# players build their manifest at runtime, so a real browser is the only reliable
# way to measure height/bitrate from CI. The budget keeps a full feed run inside
# the workflow's time limit; URLs already measured are remembered in state.
QUALITY_BROWSER_BUDGET = 40
QUALITY_BROWSER_WORKERS = 4
QUALITY_BROWSER_TIMEOUT = 14
QUALITY_BROWSER_SETTLE_MS = 700

QUALITY_TOKEN_RE = re.compile(
    r"(?<![a-z0-9])(4k|uhd|fhd|hd|2160p?|1080p?|720p?|540p|480p|360p|sd)(?![a-z0-9])",
    re.I,
)


def _quality_tokens(text: str) -> set[str]:
    return {m.group(1).lower() for m in QUALITY_TOKEN_RE.finditer(text or "")}


PROBE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/138.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://streamcorner.cloud/",
}


def _iso_epoch(ts: str) -> int:
    """'2026-11-21T06:38:51.399Z' -> epoch seconds (0 if unparseable)."""
    import datetime

    try:
        return int(datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


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
    label_tokens = _quality_tokens(label)
    url_tokens = _quality_tokens(url)
    tokens = label_tokens | url_tokens
    if tokens & {"540p", "480p", "360p"} or "sd" in label_tokens:
        return 3
    if tokens & {"4k", "uhd", "fhd", "2160p", "1080p"}:
        return 0
    if tokens & {"720p"} or "hd" in label_tokens:
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
    if "vantages" not in entry and int(entry.get("streak") or 0) > 0:
        entry["vantages"] = {CI_VANTAGE: {
            "streak": int(entry.get("streak") or 0),
            "last_fail": int(entry.get("last_fail") or 0),
            "last_ok": int(entry.get("last_ok") or 0),
        }}


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


def _forgive_manifest_failures(state: dict) -> int:
    """One-time: undo quarantines/blocks that only playlist-fetch failures caused.

    Before 2026-11-21 a 403/404 on the playlist counted as a probe failure (two
    404s = permanent block). Those fetches were wrong (scrambled tokens, worker
    players), so sources whose last verdict was one and that no app has failed
    get a clean slate.
    """
    done = state.setdefault("migrations", [])
    if "manifest_soft" in done:
        return 0
    forgiven = 0
    for entry in (state.get("sources") or {}).values():
        if not isinstance(entry, dict) or not str(entry.get("last_status") or "").startswith("manifest-"):
            continue
        if int(entry.get("app_fail") or 0):
            continue
        if entry.get("blocked") and not str(entry.get("blocked_reason") or "").startswith("HTTP manifest-"):
            continue
        for key in ("fail", "streak", "dead_fails", "quarantines", "disabled_until"):
            entry[key] = 0
        entry["blocked"] = False
        for key in ("blocked_reason", "blocked_at", "vantages"):
            entry.pop(key, None)
        forgiven += 1
    done.append("manifest_soft")
    if forgiven:
        print(f"[health] migration: cleared {forgiven} sources judged only by playlist-fetch failures", flush=True)
    return forgiven


def load_state(path: str | os.PathLike) -> dict:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("sources"), dict):
            for entry in raw["sources"].values():
                _migrate_entry(entry)
            _forgive_manifest_failures(raw)
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
        last = max(
            int(entry.get("last_ok") or 0),
            int(entry.get("last_fail") or 0),
            int(entry.get("app_last_ok") or 0),
            int(entry.get("app_last_fail") or 0),
            int(entry.get("last_play") or 0),
            int(entry.get("last_play_fail") or 0),
            int(entry.get("scan_last") or 0),
        )
        if last < cutoff and int(entry.get("disabled_until") or 0) <= now:
            entries.pop(url, None)
        elif (int(entry.get("height") or 0) <= 0
              and int(entry.get("fail") or 0) <= 0
              and not any(
                  float(entry.get(k) or 0) > 0
                  for k in ("app_ok", "app_fail", "play_ok", "play_fail", "scan_ok", "scan_fail")
              )
              and int(entry.get("disabled_until") or 0) <= now):
            entries.pop(url, None)
    # Hard cap keeps the committed JSON small even after months of churn.
    if len(entries) > 8000:
        ranked = sorted(
            entries.items(),
            key=lambda kv: (
                1 if kv[1].get("blocked") else 0,
                int(kv[1].get("disabled_until") or 0),
                1 if int(kv[1].get("height") or 0) > 0 else 0,
                int(kv[1].get("last_ok") or 0),
                int(kv[1].get("last_fail") or 0),
            ),
            reverse=True,
        )
        state["sources"] = dict(ranked[:8000])
    # Prune stale stream families and long-idle hosts.
    families = state.get("families")
    if isinstance(families, dict):
        for fam, bucket in list(families.items()):
            if not isinstance(bucket, dict) or _bucket_last_ts(bucket) < cutoff:
                families.pop(fam, None)
        if len(families) > 2000:
            top_fam = sorted(
                families.items(),
                key=lambda kv: _bucket_last_ts(kv[1]),
                reverse=True,
            )
            state["families"] = dict(top_fam[:2000])
    hosts = state.get("hosts")
    if isinstance(hosts, dict):
        host_cutoff = now - max(KEEP_DAYS, 90) * 86400
        for host, bucket in list(hosts.items()):
            if not isinstance(bucket, dict):
                hosts.pop(host, None)
                continue
            ts = _bucket_last_ts(bucket)
            if ts > 0 and ts < host_cutoff:
                hosts.pop(host, None)
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


_DATE_SEG_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")
_HEX_TOK_RE = re.compile(r"^[a-f0-9_-]{16,}$", re.I)
_LONG_TOK_RE = re.compile(r"^[A-Za-z0-9_-]{24,}$")


def url_family(url: str) -> str:
    """Canonical stream family `(host, normalized_path_prefix)` for ephemeral URLs.

    Strips query strings and collapses numeric IDs, dates, matchup slugs, and
    opaque tokens so newly minted mirrors on the same player template inherit
    their family's reliability record immediately.
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url or "")
        host = (parsed.hostname or "").lower()
        if not host:
            return ""
        segs = [s for s in (parsed.path or "").split("/") if s]
    except Exception:
        return ""
    norm: list[str] = []
    for seg in segs[:3]:
        low = seg.lower()
        if _DATE_SEG_RE.match(low):
            norm.append("{date}")
        elif low.isdigit():
            norm.append("{n}")
        elif "-vs-" in low or low.count("-") >= 3:
            norm.append("{slug}")
        elif _HEX_TOK_RE.match(low) or _LONG_TOK_RE.match(seg):
            norm.append("{tok}")
        else:
            norm.append(low)
    return host + ("/" + "/".join(norm) if norm else "")


def _bucket_last_ts(bucket: dict) -> int:
    if not isinstance(bucket, dict):
        return 0
    ts = max(
        int(bucket.get("updated_at") or 0),
        int(bucket.get("last_ok") or 0),
        int(bucket.get("last_fail") or 0),
    )
    if ts > 0:
        return ts
    seen = bucket.get("last_seen")
    return _iso_epoch(seen) if seen else 0


def _decay_factor(last_ts: int, now: int, half_life_sec: float) -> float:
    if last_ts <= 0 or now <= 0 or now <= last_ts or half_life_sec <= 0:
        return 1.0
    age = now - last_ts
    if age > half_life_sec * 30:
        return 0.0
    return 0.5 ** (age / half_life_sec)


def _decayed_pair(
    ok: float,
    fail: float,
    last_ok: int,
    last_fail: int,
    now: int,
    half_life_sec: float,
    fallback_ts: int = 0,
) -> tuple[float, float]:
    """Apply exponential time decay to an (ok, fail) count pair."""
    if ok <= 0 and fail <= 0:
        return 0.0, 0.0
    if now <= 0 or half_life_sec <= 0:
        return float(ok), float(fail)
    ok_ts = last_ok or fallback_ts
    fail_ts = last_fail or fallback_ts
    d_ok = float(ok) * _decay_factor(ok_ts, now, half_life_sec) if ok > 0 else 0.0
    d_fail = float(fail) * _decay_factor(fail_ts, now, half_life_sec) if fail > 0 else 0.0
    return d_ok, d_fail


def _update_decayed_bucket(
    bucket: dict,
    ok_delta: float,
    fail_delta: float,
    when: int,
    half_life_sec: float,
) -> None:
    """Decay existing counts in a host/family bucket to `when`, then add deltas."""
    prev_ts = _bucket_last_ts(bucket)
    prev_ok = float(bucket.get("ok") or 0.0)
    prev_fail = float(bucket.get("fail") or 0.0)
    if prev_ts > 0 and when > prev_ts and half_life_sec > 0:
        f = _decay_factor(prev_ts, when, half_life_sec)
        prev_ok *= f
        prev_fail *= f
    bucket["ok"] = round(prev_ok + max(0.0, ok_delta), 3)
    bucket["fail"] = round(prev_fail + max(0.0, fail_delta), 3)
    bucket["updated_at"] = max(prev_ts, when)
    if ok_delta > 0:
        bucket["last_ok"] = max(int(bucket.get("last_ok") or 0), when)
    if fail_delta > 0:
        bucket["last_fail"] = max(int(bucket.get("last_fail") or 0), when)


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


def _host_rank(state: dict, url: str) -> tuple:
    hosts = state.get("hosts") or {}
    info = hosts.get(host_of(url))
    if not isinstance(info, dict):
        return (0, 0.0, 0)
    ok = float(info.get("ok") or 0)
    fail = float(info.get("fail") or 0)
    total = ok + fail
    rate = (ok / total) if total >= 3 else 0.5
    height = int(info.get("height") or 0)
    return (
        -(1 if height >= 1080 else 0),
        -round(rate, 2),
        -height,
    )


def _host_probe_stats(detailed: dict[str, dict]) -> dict[str, dict]:
    """Per-run host pass rate and p50 latency from the liveness probes."""
    stats: dict[str, dict] = {}
    for url, info in (detailed or {}).items():
        host = host_of(url)
        if not host:
            continue
        bucket = stats.setdefault(host, {"ok": 0, "total": 0, "latencies": []})
        bucket["total"] += 1
        if info.get("ok"):
            bucket["ok"] += 1
            latency = int(info.get("latency_ms") or 0)
            if latency > 0:
                bucket["latencies"].append(latency)
    for bucket in stats.values():
        latencies = sorted(bucket.pop("latencies"))
        bucket["p50_ms"] = latencies[len(latencies) // 2] if latencies else 0
        bucket["rate"] = (bucket["ok"] + 1) / (bucket["total"] + 2)
    return stats


def _quality_score(src: dict, height: int) -> int:
    """0-5 quality score: measured height, with a one-tier penalty for guesses.

    A measured 480p is real but is not automatically better than an unmeasured
    "HD" label; a measured 720p is. This keeps a known-low stream from jumping
    ahead of a plausible higher-quality mirror just because it was measured.
    """
    if height >= 1080:
        return 5
    if height >= 720:
        return 4
    if height >= 480:
        return 2
    if height > 0:
        return 1
    return {0: 4, 1: 3, 2: 1, 3: 0}.get(quality_rank(src), 1)


def _reliability_tier(score: float) -> int:
    """0 = proven good, 1 = neutral/unknown, 2 = poor reliability."""
    if score >= 0.65:
        return 0
    if score >= 0.35:
        return 1
    return 2


def _selection_key(
    src: dict,
    entry: dict,
    probe_info: dict | None,
    probe_ok: bool | None,
    host_stats: dict[str, dict],
    state: dict,
    index: int,
    now: int | None = None,
) -> tuple:
    """Unified source ranking key: reliability tier first, then quality, fresh liveness, speed.

    A failed page probe always drops behind live/unprobed sources, but a plain
    HTTP 200 on an outer HTML wrapper never outranks proven real-device playback
    history.
    """
    url = str(src.get("url") or "")
    host = host_of(url)
    height = int(src.get("height") or 0)
    bitrate = int(src.get("bitrate") or 0)
    if probe_info:
        if height <= 0:
            height = int(probe_info.get("height") or 0)
        if bitrate <= 0:
            bitrate = int(probe_info.get("bitrate") or 0)
    if height <= 0:
        height = int(entry.get("height") or 0)
    if bitrate <= 0:
        bitrate = int(entry.get("bitrate") or 0)
    fps = float(entry.get("fps") or 0.0)
    latency = int((probe_info or {}).get("latency_ms") or 0) if probe_ok else 10**9
    score = health_score(entry, state, url, now=now)
    if host_stats.get(host):
        # This run's host page-pass rate nudges sources mildly; never enough to
        # rescue a host with bad real playback history.
        score = round(score * 0.90 + float(host_stats[host].get("rate", 0.5)) * 0.10, 4)
    return (
        1 if probe_ok is False else 0,
        _reliability_tier(score),
        -_quality_score(src, height),
        -min(height, 4320),
        0 if fps >= 48.0 else 1,
        0 if (src.get("verified") or entry.get("verified")) else 1,
        0 if probe_ok is True else 1,
        -score,
        -min(bitrate, 50_000_000),
        latency,
        index,
    )


def _host_is_chronically_bad(state: dict | None, url: str, now: int) -> bool:
    """True when real device playback shows this host almost never plays."""
    if not state or not url:
        return False
    bucket = (state.get("hosts") or {}).get(host_of(url))
    if not isinstance(bucket, dict):
        return False
    ok, fail = _decayed_pair(
        float(bucket.get("ok") or 0),
        float(bucket.get("fail") or 0),
        int(bucket.get("last_ok") or 0),
        int(bucket.get("last_fail") or 0),
        now,
        HOST_HALF_LIFE_DAYS * 86400.0,
        fallback_ts=_bucket_last_ts(bucket),
    )
    total = ok + fail
    return fail >= 5.0 and total >= 5.0 and (ok / total) < 0.20


def is_weak_source(
    src: dict,
    entry: dict,
    probe_ok: bool | None,
    state: dict | None = None,
    now: int | None = None,
) -> bool:
    """A mirror only worth publishing when an event has little else."""
    if probe_ok is False:
        return True
    height = int(src.get("height") or entry.get("height") or 0)
    if 0 < height < LOW_QUALITY_HEIGHT:
        return True
    if height <= 0 and quality_rank(src) == 3:
        return True
    app_fail = float(entry.get("app_fail") or 0)
    app_ok = float(entry.get("app_ok") or 0)
    if app_fail >= FAIL_OMIT and app_fail > app_ok:
        return True
    if int(entry.get("play_fail") or 0) >= 3 and not int(entry.get("play_ok") or 0) and app_ok <= 0:
        return True
    fail = int(entry.get("fail") or 0)
    ok_count = int(entry.get("ok") or 0)
    if fail >= 3 and fail > ok_count and app_ok <= 0:
        return True
    if app_ok <= 0 and state is not None:
        if _host_is_chronically_bad(state, str(src.get("url") or ""), now if now is not None else _now()):
            return True
    return False


def _interleave_by_host(pool: list[dict]) -> list[dict]:
    """Round-robin wave ordering across hosts while preserving best-first order.

    Wave 0 takes the highest-ranked source from each distinct host (in order of
    each host's best source), Wave 1 takes the 2nd source from each host, etc.
    Slot #1 is always the single best source overall, and slots #2..#k hit
    independent CDNs before repeating any host.
    """
    by_host: dict[str, list[dict]] = {}
    host_order: list[str] = []
    for item in pool:
        host = item.get("host") or host_of(str(item["src"].get("url") or "")) or ""
        if host not in by_host:
            by_host[host] = []
            host_order.append(host)
        by_host[host].append(item)
    out: list[dict] = []
    depth = max((len(v) for v in by_host.values()), default=0)
    for wave in range(depth):
        for host in host_order:
            bucket = by_host[host]
            if wave < len(bucket):
                out.append(bucket[wave])
    return out


def _select_best_sources(candidates: list[dict], limit: int | None) -> list[dict]:
    """Select up to `limit` sources in host-interleaved failover order.

    Non-bad sources (proven healthy or neutral, not page-failed) always come
    before poor-reliability or page-failed mirrors. Within each band, mirrors
    are interleaved in round-robin waves across distinct hosts so one dead or
    ISP-blocked host cannot dominate the top of the failover stack.
    """
    if not candidates:
        return []
    if limit is not None and limit <= 0:
        return []
    ordered = sorted(candidates, key=lambda item: item["key"])
    # key[0] == 0 means not page-failed; key[1] <= 1 means healthy or neutral reliability tier.
    primary = [item for item in ordered if item["key"][0] == 0 and item["key"][1] <= 1]
    secondary = [item for item in ordered if not (item["key"][0] == 0 and item["key"][1] <= 1)]
    selected: list[dict] = []
    selected_urls: set[str] = set()
    for pool in (primary, secondary):
        for item in _interleave_by_host(pool):
            if limit is not None and len(selected) >= limit:
                return selected
            src = item["src"]
            url = str(src.get("url") or "")
            if not url or url in selected_urls:
                continue
            selected.append(src)
            selected_urls.add(url)
    return selected


def ranked_sources(
    sources: list[dict],
    state: dict,
    now: int,
    *,
    include_disabled: bool = False,
    blocklist: dict | None = None,
) -> list[dict]:
    """Rank sources using the unified `_selection_key` (shared with scheduler and publisher)."""
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
        key = _selection_key(src, entry, None, None, {}, state, index, now=now)
        out.append((key, src))
    out.sort(key=lambda item: item[0])
    return [item[1] for item in out]


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


def _b64_decode(text: str) -> str:
    value = (text or "").strip()
    if not value:
        return ""
    value = value.replace("-", "+").replace("_", "/")
    value += "=" * (-len(value) % 4)
    try:
        return base64.b64decode(value).decode("utf-8", "replace").replace("\\/", "/")
    except Exception:
        return ""


def _decode_js_concat(text: str) -> str:
    """Decode `var u = f(a)+f(b)+...` pages (cdnlivetv and friends).

    Each fragment is a base64 literal assigned to a variable; the player joins
    the decoded pieces in expression order. Only the expression that references
    at least two variables is considered, which avoids ad/config strings.
    """
    variables = dict(re.findall(r"var\s+(\w+)\s*=\s*'([^']*)'", text))
    if not variables:
        return ""
    for match in re.finditer(
        r"var\s+\w+\s*=\s*((?:\w+\(\w+\)\s*(?:\+\s*)?){2,});", text
    ):
        pieces = []
        for _, name in re.findall(r"(\w+)\((\w+)\)", match.group(1)):
            decoded = _b64_decode(variables.get(name, ""))
            if decoded:
                pieces.append(decoded)
        candidate = "".join(pieces)
        if ".m3u8" in candidate or ".mpd" in candidate:
            return candidate
    return ""


_CHAR_ARRAY_RE = re.compile(r'(\w+)\s*=\s*\[\s*(\[\s*\d+\s*,\s*"[^"]+"\s*\](?:\s*,\s*\[\s*\d+\s*,\s*"[^"]+"\s*\])*)\s*\]\s*;')
_CHAR_KEY_RE = re.compile(r"var\s+k\s*=\s*(\w+)\(\)\s*\+\s*(\w+)\(\)")


def _decode_char_arrays(text: str) -> str:
    """Decode `String.fromCharCode(parseInt(atob(v).replace(/\\D/g,''))-k)` pages (streamxhd).

    The page stores shuffled `[index, base64]` pairs per string (stream URL,
    ad payloads...); each array is followed by its own `var k=f()+g()` whose
    two functions return the constants to subtract. Returns every decoded
    string, newline-separated, for the manifest regex to search.
    """
    out = []
    for match in _CHAR_ARRAY_RE.finditer(text):
        pairs = re.findall(r'\[\s*(\d+)\s*,\s*"([^"]+)"\s*\]', match.group(2))
        if len(pairs) < 8:
            continue
        kexpr = _CHAR_KEY_RE.search(text, match.end(), match.end() + 400)
        if not kexpr:
            continue
        total = 0
        for name in kexpr.groups():
            const = re.search(r"function\s+" + re.escape(name) + r"\s*\(\)\s*\{\s*return\s+(\d+)", text)
            total += int(const.group(1)) if const else 0
        if total <= 0:
            continue
        chars = []
        for _, encoded in sorted(pairs, key=lambda item: int(item[0])):
            digits = "".join(ch for ch in _b64_decode(encoded) if ch.isdigit())
            if digits and 0 < int(digits) - total < 0x110000:
                chars.append(chr(int(digits) - total))
        out.append("".join(chars))
    return "\n".join(out)


def _manifest_from_text(text: str) -> str:
    text = text.replace("\\/", "/")
    match = re.search(r'https?://[^\s"\'<>]+?\.(?:m3u8|mpd)(?:\?[^\s"\'<>]*)?', text, re.I)
    if match:
        return match.group(0)
    for b64 in re.findall(r'atob\(\s*["\']([A-Za-z0-9+/=_-]{16,})["\']\s*\)', text):
        decoded = _b64_decode(b64)
        m = re.search(r'https?://[^\s"\'<>]+?\.(?:m3u8|mpd)(?:\?[^\s"\'<>]*)?', decoded, re.I)
        if m:
            return m.group(0)
    for decoded in (_decode_js_concat(text), _decode_char_arrays(text)):
        m = re.search(r'https?://[^\s"\'<>]+?\.(?:m3u8|mpd)(?:\?[^\s"\'<>]*)?', decoded, re.I)
        if m:
            return m.group(0)
    return ""


def _iframe_urls(text: str, base_url: str) -> list[str]:
    out = []
    for m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', text, re.I):
        candidate = m.group(1).strip()
        if candidate.startswith("//"):
            candidate = "https:" + candidate
        elif candidate.startswith("/"):
            candidate = urljoin(base_url, candidate)
        if candidate.startswith("https://") and candidate not in out:
            out.append(candidate)
    return out


def _site_player_url(text: str) -> str:
    """Build the posamari/JW player URL used by the embedsports -> frame chain."""
    config_match = re.search(r"const\s+siteConfig\s*=\s*(\{.*?\});", text, re.S)
    attributes = re.search(
        r'data-pldm="([^"]+)".*?data-pid="([^"]+)".*?data-play="([^"]+)"'
        r'.*?data-player="([^"]+)".*?data-gc="([^"]+)"',
        text,
        re.S,
    )
    if not config_match or not attributes:
        return ""
    try:
        config = json.loads(config_match.group(1))
    except Exception:
        return ""
    host, pid, play, player, category = attributes.groups()
    if "/" in host or " " in host:
        return ""
    params = {
        "pid": pid,
        "v": play,
        "player": player,
        "csrf": str(config.get("csrf") or ""),
        "csrf_ip": str(config.get("csrf_ip") or ""),
        "expires": str(config.get("sec_expires") or ""),
        "sec_hash": str(config.get("sec_hash") or ""),
    }
    return f"https://{host}/sd0embed/{quote(category, safe='')}?{urlencode(params)}"


def _json_object_at(text: str, start: int) -> str:
    index = text.find("{", start)
    if index < 0:
        return ""
    depth = 0
    in_string = False
    escaped = False
    for position in range(index, len(text)):
        char = text[position]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[index:position + 1]
    return ""


def _player_config_quality(text: str, url: str, headers: dict, timeout: int) -> dict:
    """Read quality hints from static player configs (posamari/JW, matchora)."""
    result: dict = {}
    blobs = []
    for match in re.finditer(
        r'window\.SecKeyPlayerConfig\[["\'][^"\']+["\']\]\s*=', text
    ):
        blob = _json_object_at(text, match.end())
        if blob:
            blobs.append(blob)
    if not blobs:
        for match in re.finditer(r"window\.SecKeyPlayerConfig\s*=\s*", text):
            blob = _json_object_at(text, match.end())
            if blob:
                blobs.append(blob)
    for blob in blobs:
        try:
            config = json.loads(blob)
        except Exception:
            continue
        for player in (
            config.values() if any(isinstance(value, dict) for value in config.values())
            else [config]
        ):
            if not isinstance(player, dict):
                continue
            stream_type = str(player.get("stream_type") or "").upper()
            is_sd = int(player.get("is_sd") or 0)
            height = 0
            if "2160" in stream_type or "4K" in stream_type or "UHD" in stream_type:
                height = 2160
            elif "1080" in stream_type or "FHD" in stream_type:
                height = 1080
            elif "720" in stream_type or "HD" in stream_type:
                height = 720
            elif "480" in stream_type or "SD" in stream_type or is_sd:
                height = 480
            source = str(player.get("urlSource") or "")
            if height:
                result["height"] = max(int(result.get("height") or 0), height)
            if source:
                result["manifest"] = source[:400]
                if source.lower().endswith((".m3u8", ".mpd")):
                    try:
                        import requests
                        with requests.get(
                            source, headers={**headers, "Referer": url},
                            timeout=(timeout, timeout), stream=True,
                            allow_redirects=True,
                        ) as response:
                            if 200 <= response.status_code < 400:
                                body = _read_limited(response, MANIFEST_LIMIT).decode("utf-8", "replace")
                                h, b = _quality_from_manifest(body)
                                if h > 0:
                                    result["height"] = h
                                    result["bitrate"] = b
                    except Exception:
                        pass
    return result


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


# Players whose playlist arrives through a signed worker channel (edgestream's
# CdnDelivery behind streamec/embedcenter): never fetchable outside a browser.
_BROWSER_ONLY_MARKERS = ("/sh/access/", "new CdnDelivery(")


def _browser_only_player(text: str) -> bool:
    return any(marker in (text or "") for marker in _BROWSER_ONLY_MARKERS)


def _origin(url: str) -> str:
    from urllib.parse import urlparse

    parts = urlparse(url)
    return f"{parts.scheme}://{parts.netloc}" if parts.scheme and parts.netloc else ""


def _fetch_manifest(url: str, page_url: str, headers: dict, timeout: int) -> tuple[str, str]:
    """GET a playlist the way the player's XHR does: page origin as Referer plus Origin.

    Falls back to the full page URL as Referer (some CDNs check the path).
    Returns (status, body); body is "" unless the response was 2xx/3xx.
    """
    import requests

    origin = _origin(page_url)
    attempts = [{**headers, "Referer": origin + "/", "Origin": origin}] if origin else []
    attempts.append({**headers, "Referer": page_url})
    status = ""
    for attempt in attempts:
        try:
            with requests.get(url, headers=attempt, timeout=(timeout, timeout),
                              stream=True, allow_redirects=True) as response:
                status = str(response.status_code)
                if 200 <= response.status_code < 400:
                    return status, _read_limited(response, MANIFEST_LIMIT).decode("utf-8", "replace")
                if response.status_code not in (401, 403):
                    return status, ""
        except Exception:
            continue
    return status, ""


def _quality_meta(url: str, text: str, headers: dict, timeout: int,
                  depth: int = 0, seen: set[str] | None = None) -> dict:
    height = 0
    bitrate = 0
    manifest = ""
    manifest_status = ""
    seen = seen if seen is not None else set()
    if url in seen:
        return {
            "height": 0, "bitrate": 0, "manifest": "", "manifest_status": "",
            "verified": False, "quality": "",
        }
    seen.add(url)
    lowered = url.lower().split("?")[0]
    if lowered.endswith((".m3u8", ".mpd")):
        manifest = url
        height, bitrate = _quality_from_manifest(text)
    else:
        found = _manifest_from_text(text)
        manifest_referer = url
        found_text = text
        if not found and depth < 3:
            for iframe_url in _iframe_urls(text, url)[:4]:
                if iframe_url in seen:
                    continue
                try:
                    import requests
                    with requests.get(
                        iframe_url,
                        headers={**headers, "Referer": url},
                        timeout=(timeout, timeout),
                        stream=True,
                        allow_redirects=True,
                    ) as iframe_resp:
                        if 200 <= iframe_resp.status_code < 400:
                            iframe_text = _read_limited(iframe_resp, PROBE_BODY_LIMIT).decode("utf-8", "replace")
                            found = _manifest_from_text(iframe_text)
                            if found:
                                manifest_referer = iframe_url
                                found_text = iframe_text
                                break
                            player_url = _site_player_url(iframe_text)
                            if player_url:
                                with requests.get(
                                    player_url,
                                    headers={**headers, "Referer": iframe_url},
                                    timeout=(timeout, timeout),
                                    stream=True,
                                    allow_redirects=True,
                                ) as player_resp:
                                    if 200 <= player_resp.status_code < 400:
                                        player_text = _read_limited(
                                            player_resp, PROBE_BODY_LIMIT
                                        ).decode("utf-8", "replace")
                                        player = _player_config_quality(
                                            player_text, player_url, headers, timeout
                                        )
                                        if player.get("height", 0) > 0:
                                            return {
                                                "height": int(player["height"]),
                                                "bitrate": int(player.get("bitrate") or 0),
                                                "manifest": str(player.get("manifest") or player_url)[:400],
                                                "manifest_status": "",
                                                "verified": True,
                                                "quality": _quality_label(int(player["height"])),
                                            }
                            nested = _quality_meta(
                                iframe_url, iframe_text, headers, timeout, depth + 1, seen
                            )
                            if nested.get("height", 0) > 0:
                                return {**nested, "manifest": nested.get("manifest") or iframe_url}
                            if nested.get("manifest"):
                                if not manifest:
                                    manifest = nested["manifest"]
                                if nested.get("manifest_status") and not manifest_status:
                                    manifest_status = nested["manifest_status"]
                except Exception:
                    pass
        player = _player_config_quality(text, url, headers, timeout)
        if player.get("height", 0) <= 0:
            player_url = _site_player_url(text)
            if player_url:
                try:
                    import requests
                    with requests.get(
                        player_url,
                        headers={**headers, "Referer": url},
                        timeout=(timeout, timeout),
                        stream=True,
                        allow_redirects=True,
                    ) as player_resp:
                        if 200 <= player_resp.status_code < 400:
                            player_text = _read_limited(
                                player_resp, PROBE_BODY_LIMIT
                            ).decode("utf-8", "replace")
                            player = _player_config_quality(
                                player_text, player_url, headers, timeout
                            )
                except Exception:
                    pass
        if player.get("height", 0) > 0:
            height = int(player["height"])
            bitrate = max(bitrate, int(player.get("bitrate") or 0))
            manifest = manifest or str(player.get("manifest") or "")
        if found and _browser_only_player(found_text):
            # The player gets its playlist through a signed, IP-bound JS worker;
            # a direct fetch is always refused. Only the playback check can tell.
            manifest = manifest or found
        elif found:
            manifest = manifest or found
            status, body = _fetch_manifest(found, manifest_referer, headers, timeout)
            manifest_status = status
            if body:
                parsed_height, parsed_bitrate = _quality_from_manifest(body)
                if parsed_height > 0:
                    height, bitrate = parsed_height, parsed_bitrate
    return {
        "height": height,
        "bitrate": bitrate,
        "manifest": manifest[:400],
        "manifest_status": manifest_status,
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


_PLACEHOLDER_HOSTS = ("aurasportsplayer.cfd", "embedme.cfd", "embedsports.me")


def _detect_placeholder_embed(url: str, text: str, timeout: int) -> str:
    """Follow nested embed iframes (aurasportsplayer -> embedme -> embedsports) for placeholders."""
    low_url = url.lower()
    if not any(h in low_url for h in _PLACEHOLDER_HOSTS):
        return ""
    import requests

    cur_url = url
    cur_text = text
    for _ in range(2):
        low = cur_text.lower()
        if "data-pldm=" in low or ".m3u8" in low or ".mpd" in low:
            return ""
        if (
            'id="headlinetext">starting soon' in low
            or "no streams found" in low
            or "stream offline" in low
        ):
            return "starting_soon"
        m = re.search(
            r'<iframe[^>]+src=["\'](https://[^"\']+)["\']',
            cur_text,
            re.I,
        )
        if not m:
            break
        nxt = m.group(1).strip()
        if not any(h in nxt.lower() for h in _PLACEHOLDER_HOSTS):
            break
        try:
            headers = dict(PROBE_HEADERS)
            headers["Referer"] = cur_url
            resp = requests.get(nxt, headers=headers, timeout=(timeout, timeout), allow_redirects=True)
            if resp.status_code >= 400:
                return ""
            cur_url = nxt
            cur_text = resp.text[:PROBE_BODY_LIMIT]
        except Exception:
            return ""
    low = cur_text.lower()
    if "data-pldm=" not in low and ".m3u8" not in low and ".mpd" not in low:
        if (
            'id="headlinetext">starting soon' in low
            or "no streams found" in low
            or "stream offline" in low
        ):
            return "starting_soon"
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
            if url.lower().split("?")[0].endswith((".m3u8", ".mpd")):
                meta.setdefault("manifest_status", str(code))
            latency_ms = int((time.monotonic() - started) * 1000)
            ok = 200 <= code < 400
            status_label = str(code)
            if ok:
                placeholder = _detect_placeholder_embed(url, text, timeout)
                if placeholder:
                    ok = False
                    status_label = placeholder
            return url, ok, status_label, latency_ms, meta
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
                "manifest_status": str(meta.get("manifest_status") or ""),
                "verified": bool(meta.get("verified")),
                "quality": str(meta.get("quality") or ""),
                "content_type": str(meta.get("content_type") or ""),
            }
    return results


def apply_manifest_failures(results: dict[str, dict], trusted: bool = True) -> int:
    """Mark sources whose playlist fetch failed (403/404/410). A hint, not a failure.

    The page check's own playlist fetch is unreliable: tokens are IP-bound or
    hidden in scrambled JS, some players use a worker channel. So the page stays
    ok; the stale measured quality is dropped and the playback check (and the
    apps) decide. Returns how many were marked.
    """
    changed = 0
    for info in results.values():
        if not info.get("ok") or int(info.get("height") or 0) > 0:
            continue
        status = str(info.get("manifest_status") or "")
        if status in MANIFEST_DEAD_STATUSES:
            info["manifest_dead"] = status
            changed += 1
    return changed


def _looks_like_manifest(url: str) -> bool:
    lowered = url.lower()
    return ".m3u8" in lowered or ".mpd" in lowered or "manifest.ts" in lowered


def _browser_launch(playwright):
    """Launch a real browser. Use installed Chrome first, then Playwright's build."""
    for options in ({"channel": "chrome"}, {}):
        try:
            return playwright.chromium.launch(
                headless=True, args=["--autoplay-policy=no-user-gesture-required"], **options
            )
        except Exception:
            continue
    return None


def _browser_fetch(ctx, url: str, referer: str, timeout: int) -> str:
    try:
        response = ctx.request.get(
            url,
            headers={"Referer": referer, "User-Agent": PROBE_HEADERS["User-Agent"]},
            timeout=timeout * 1000,
        )
        if response.ok:
            return response.text()[:MANIFEST_LIMIT]
    except Exception:
        pass
    try:
        import requests
        with requests.get(
            url, headers={**PROBE_HEADERS, "Referer": referer},
            timeout=(timeout, timeout), stream=True, allow_redirects=True,
        ) as response:
            if 200 <= response.status_code < 400:
                return _read_limited(response, MANIFEST_LIMIT).decode("utf-8", "replace")
    except Exception:
        pass
    return ""


def _height_from_stream_type(player: dict) -> int:
    stream_type = str(player.get("stream_type") or "").upper()
    is_sd = int(player.get("is_sd") or 0)
    if "2160" in stream_type or "4K" in stream_type or "UHD" in stream_type:
        return 2160
    if "1080" in stream_type or "FHD" in stream_type:
        return 1080
    if "720" in stream_type or "HD" in stream_type:
        return 720
    if "480" in stream_type or "SD" in stream_type or is_sd:
        return 480
    return 0


_VIDEO_STATE_JS = (
    "() => [...document.querySelectorAll('video')].map(v => [v.videoHeight || 0,"
    " v.currentTime || 0, v.readyState || 0,"
    " v.getVideoPlaybackQuality ? v.getVideoPlaybackQuality().totalVideoFrames : 0])"
)
_FORCE_PLAY_JS = (
    "() => document.querySelectorAll('video').forEach(v => {"
    " v.muted = true; const p = v.play(); if (p && p.catch) p.catch(() => {}); })"
)
PLAYBACK_MIN_FRAMES = 12
PLAYBACK_MIN_SECONDS = 1.0


def _wrapper_url(url: str, home: str) -> str:
    """The probe page lives on the site's own origin, so referrer and ancestor checks pass."""
    from urllib.parse import urlparse

    base = urlparse(home or "")
    if base.scheme != "https" or not base.hostname:
        base = urlparse(url)
    return f"https://{base.netloc}/__slang_probe__"


def _browser_probe_one(ctx, url: str, timeout: int, home: str = "") -> dict:
    """Playback check: does the embed actually decode frames, and at what height?

    The embed is loaded in an iframe on a page served (via request routing) from
    the site's own origin, so anti-embed and referrer checks see the site they
    expect. Players that wait for a gesture get a click and a muted play().
    `playing` means video frames advanced; `height` is the best of the decoded
    height and the master playlist's top rendition (ABR starts low).
    """
    wrapper = _wrapper_url(url, home)
    page = ctx.new_page()
    manifests: list[str] = []

    def on_response(response) -> None:
        try:
            ctype = (response.headers.get("content-type") or "").lower()
        except Exception:
            ctype = ""
        if _looks_like_manifest(response.url) or "mpegurl" in ctype or "dash+xml" in ctype:
            if response.url not in manifests and len(manifests) < 6:
                manifests.append(response.url)

    page.on("response", on_response)
    page.route(wrapper, lambda route: route.fulfill(
        status=200,
        content_type="text/html",
        body=(
            '<html><body style="margin:0;background:#000">'
            f'<iframe src="{html.escape(url)}" style="width:1280px;height:720px;border:0" '
            'allow="autoplay; fullscreen; encrypted-media; picture-in-picture"></iframe></body></html>'
        ),
    ))
    observed_height = 0
    playing = False
    saw_video = False
    first: dict[int, tuple[float, int]] = {}
    try:
        page.goto(wrapper, wait_until="domcontentloaded", timeout=max(5, timeout) * 1000)
        started = time.monotonic()
        deadline = started + max(4, timeout)
        nudges = (2.5, 6.0)
        nudged = 0
        playing_since = 0.0
        while time.monotonic() < deadline:
            page.wait_for_timeout(QUALITY_BROWSER_SETTLE_MS)
            for index, frame in enumerate(page.frames):
                try:
                    videos = frame.evaluate(_VIDEO_STATE_JS)
                except Exception:
                    continue
                for vi, (height, current, ready, frames) in enumerate(videos or []):
                    saw_video = True
                    key = index * 16 + vi
                    base = first.setdefault(key, (float(current or 0), int(frames or 0)))
                    advanced = (
                        int(frames or 0) - base[1] >= PLAYBACK_MIN_FRAMES
                        or (float(current or 0) - base[0] >= PLAYBACK_MIN_SECONDS and int(ready or 0) >= 2)
                    )
                    if advanced:
                        if not playing:
                            playing_since = time.monotonic()
                        playing = True
                        observed_height = max(observed_height, int(height or 0))
            if playing and time.monotonic() - playing_since >= 2.5:
                break  # let ABR step up once, then stop
            elapsed = time.monotonic() - started
            if not playing and nudged < len(nudges) and elapsed >= nudges[nudged]:
                nudged += 1
                try:
                    page.mouse.click(640, 360)
                except Exception:
                    pass
                for frame in page.frames:
                    try:
                        frame.evaluate(_FORCE_PLAY_JS)
                    except Exception:
                        pass
    except Exception:
        pass
    finally:
        try:
            page.close()
        except Exception:
            pass

    manifest = ""
    manifest_height = 0
    bitrate = 0
    for candidate in manifests[:3]:
        body = _browser_fetch(ctx, candidate, url, timeout)
        if not body:
            continue
        h, b = _quality_from_manifest(body)
        if not manifest:
            manifest = candidate
        if h > manifest_height:
            manifest_height, bitrate, manifest = h, b, candidate
    # A master playlist's top rendition only counts when something actually played.
    height = max(observed_height, manifest_height) if playing else 0
    return {
        "playing": playing,
        "status": "playing" if playing else ("no-playback" if saw_video else "no-video"),
        "height": height,
        "bitrate": bitrate if playing else 0,
        "manifest": manifest[:400],
        "verified": playing and height > 0,
        "quality": _quality_label(height),
    }


def _browser_worker(chunk: list[tuple[str, str]], timeout: int) -> dict[str, dict]:
    results: dict[str, dict] = {}
    if not chunk:
        return results
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return results
    try:
        with sync_playwright() as playwright:
            browser = _browser_launch(playwright)
            if browser is None:
                return results
            try:
                for url, home in chunk:
                    ctx = browser.new_context(
                        user_agent=PROBE_HEADERS["User-Agent"],
                        viewport={"width": 1280, "height": 720},
                        locale="en-US",
                    )
                    # Pop-unders opened by the click are ads; close them at once.
                    ctx.on("page", lambda extra: extra.close() if extra.opener() else None)
                    try:
                        results[url] = _browser_probe_one(ctx, url, timeout, home)
                    except Exception:
                        pass
                    finally:
                        ctx.close()
            finally:
                browser.close()
    except Exception:
        pass
    return results


def probe_playback(
    urls: list[str],
    *,
    homes: dict[str, str] | None = None,
    budget: int = QUALITY_BROWSER_BUDGET,
    workers: int = QUALITY_BROWSER_WORKERS,
    timeout: int = QUALITY_BROWSER_TIMEOUT,
) -> dict[str, dict]:
    """Level-2 probe: real playback in headless Chrome (frames advancing + height).

    `homes` maps a URL to its site's home page (the probe page is served from
    that origin). Returns one entry per probed URL with `playing`, `status`,
    `height`, `bitrate`, `manifest`. A URL missing from the result was not
    checked (browser unavailable), which is not a failure.
    """
    homes = homes or {}
    selected = [str(url).strip() for url in urls if str(url or "").startswith("https://")]
    if budget > 0:
        selected = selected[:budget]
    if not selected:
        return {}
    workers = max(1, min(int(workers or 1), 8))
    workers = min(workers, len(selected))
    # Round-robin chunks so each worker gets a mix of sites (one slow site cannot stall a worker).
    chunks = [[(url, homes.get(url, "")) for url in selected[i::workers]] for i in range(workers)]
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futures = [pool.submit(_browser_worker, chunk, timeout) for chunk in chunks]
        for future in as_completed(futures):
            try:
                results.update(future.result())
            except Exception:
                pass
    return results


# Old name, kept for source_health_report.py.
probe_quality_browser = probe_playback


def _vantage(entry: dict, name: str) -> dict:
    vantages = entry.setdefault("vantages", {})
    v = vantages.get(name)
    if not isinstance(v, dict):
        v = {"streak": 0, "last_fail": 0, "last_ok": 0}
        vantages[name] = v
    return v


def _recent_app_failure(entry: dict, now: int) -> bool:
    memory = FAIL_MEMORY_HOURS * 3600
    return any(
        name != CI_VANTAGE and int(v.get("streak") or 0) > 0 and now - int(v.get("last_fail") or 0) <= memory
        for name, v in (entry.get("vantages") or {}).items()
        if isinstance(v, dict)
    )


def _sync_streak(entry: dict, now: int) -> None:
    """entry["streak"] = the worst current streak among vantages that failed recently."""
    memory = FAIL_MEMORY_HOURS * 3600
    entry["streak"] = max(
        [int(v.get("streak") or 0) for v in (entry.get("vantages") or {}).values()
         if isinstance(v, dict) and now - int(v.get("last_fail") or 0) <= memory] or [0]
    )


def _quarantine(entry: dict, now: int) -> None:
    if is_disabled(entry, now):
        return
    q = min(int(entry.get("quarantines") or 0), len(QUARANTINE_HOURS) - 1)
    entry["disabled_until"] = now + QUARANTINE_HOURS[q] * 3600
    entry["quarantines"] = min(q + 1, len(QUARANTINE_HOURS) - 1)


def should_quarantine(entry: dict, now: int) -> bool:
    """Failures from QUARANTINE_VANTAGES vantage points, or DISABLE_STREAK in a row from one."""
    memory = FAIL_MEMORY_HOURS * 3600
    failing = [
        v for v in (entry.get("vantages") or {}).values()
        if isinstance(v, dict) and int(v.get("streak") or 0) > 0 and now - int(v.get("last_fail") or 0) <= memory
    ]
    return (
        any(int(v.get("streak") or 0) >= DISABLE_STREAK for v in failing)
        or len(failing) >= QUARANTINE_VANTAGES
    )


def record_vantage(entry: dict, vantage: str, ok: bool, when: int, now: int | None = None) -> None:
    """One playback/probe verdict from one vantage point (the CI runner or a device)."""
    now = now or when
    v = _vantage(entry, vantage)
    if ok:
        v["streak"] = 0
        v["last_ok"] = max(int(v.get("last_ok") or 0), when)
    else:
        v["streak"] = min(int(v.get("streak") or 0) + 1, 99)
        v["last_fail"] = max(int(v.get("last_fail") or 0), when)
    _sync_streak(entry, now)
    if not ok and should_quarantine(entry, now):
        _quarantine(entry, now)


def _halve_if_large(entry: dict, ok_key: str, fail_key: str, cap: int = 40) -> None:
    """Keep counts recent: once a pair passes `cap`, halve both."""
    if float(entry.get(ok_key) or 0) + float(entry.get(fail_key) or 0) > cap:
        entry[ok_key] = round(float(entry.get(ok_key) or 0) / 2.0, 3)
        entry[fail_key] = round(float(entry.get(fail_key) or 0) / 2.0, 3)


def _qoe_credits(
    ok: bool,
    *,
    reason: str = "",
    rebuffer_count: int = 0,
    audio_ok: bool | None = None,
) -> tuple[float, float]:
    """Translate a playback verdict + QoE hints into (ok_credit, fail_credit)."""
    if not ok:
        return 0.0, 1.0
    if reason == "video_no_audio" or audio_ok is False:
        return 0.4, 0.6
    if int(rebuffer_count or 0) >= 3:
        return 0.5, 0.5
    return 1.0, 0.0


def record_app_result(
    state: dict,
    url: str,
    device: str,
    ok: bool,
    when: int,
    now: int,
    height: int = 0,
    fps: float = 0.0,
    kbps: int = 0,
    *,
    reason: str = "",
    watch_sec: int = 0,
    rebuffer_count: int = 0,
    audio_ok: bool | None = None,
) -> None:
    """An app's real playback verdict: the primary health signal."""
    entry = _entry(state, url)
    ok_credit, fail_credit = _qoe_credits(
        ok, reason=reason, rebuffer_count=rebuffer_count, audio_ok=audio_ok,
    )
    if ok_credit > 0:
        entry["app_ok"] = round(float(entry.get("app_ok") or 0) + ok_credit, 3)
        entry["app_last_ok"] = max(int(entry.get("app_last_ok") or 0), when)
        entry["last_ok"] = max(int(entry.get("last_ok") or 0), when)
    if fail_credit > 0:
        entry["app_fail"] = round(float(entry.get("app_fail") or 0) + fail_credit, 3)
        entry["app_last_fail"] = max(int(entry.get("app_last_fail") or 0), when)
    if ok:
        if height >= 480:
            entry["verified"] = True
        # It plays for a viewer: lift any quarantine or automatic block.
        entry["disabled_until"] = 0
        if entry.get("blocked"):
            entry["blocked"] = False
            entry["dead_fails"] = 0
            entry.pop("blocked_reason", None)
            entry.pop("blocked_at", None)
    _halve_if_large(entry, "app_ok", "app_fail")
    if height > 0:
        entry["height"] = max(int(entry.get("height") or 0), height)
    if fps > 0:
        entry["fps"] = max(float(entry.get("fps") or 0.0), round(fps, 1))
    if kbps > 0:
        entry["bitrate"] = max(int(entry.get("bitrate") or 0), kbps * 1000)
    fam = url_family(url)
    if fam:
        fb = state.setdefault("families", {}).setdefault(fam, {"ok": 0.0, "fail": 0.0})
        _update_decayed_bucket(fb, ok_credit, fail_credit, when, HOST_HALF_LIFE_DAYS * 86400.0)
        if height > 0:
            fb["height"] = max(int(fb.get("height") or 0), height)
    record_vantage(entry, device or "app", ok, when, now)


def record_scan_result(
    state: dict,
    url: str,
    ok: bool,
    when: int,
    now: int,
    height: int = 0,
    fps: float = 0.0,
    kbps: int = 0,
) -> None:
    """Background on-device probe verdict (`source_scan`).

    Weighs much less than real user playback (`SCAN_WEIGHT`) and never triggers
    quarantine streaks so background timeouts on a busy TV cannot kill streams.
    """
    entry = _entry(state, url)
    if ok:
        entry["scan_ok"] = round(float(entry.get("scan_ok") or 0) + 1.0, 3)
        if height >= 480:
            entry["verified"] = True
    else:
        entry["scan_fail"] = round(float(entry.get("scan_fail") or 0) + 1.0, 3)
    entry["scan_last"] = max(int(entry.get("scan_last") or 0), when)
    _halve_if_large(entry, "scan_ok", "scan_fail", cap=20)
    if ok:
        if height > 0:
            entry["height"] = max(int(entry.get("height") or 0), height)
        if fps > 0:
            entry["fps"] = max(float(entry.get("fps") or 0.0), round(fps, 1))
        if kbps > 0:
            entry["bitrate"] = max(int(entry.get("bitrate") or 0), kbps * 1000)


def record_playback(state: dict, url: str, result: dict, now: int) -> None:
    """Level-2 probe verdict. A no-playback result lowers the score but cannot quarantine:
    from a datacenter it may only mean the runner is refused."""
    entry = _entry(state, url)
    if result.get("playing"):
        entry["play_ok"] = int(entry.get("play_ok") or 0) + 1
        entry["last_play"] = now
        height = int(result.get("height") or 0)
        if height > 0:
            entry["height"] = height
            entry["bitrate"] = int(result.get("bitrate") or 0)
            entry["verified"] = True
        if result.get("manifest"):
            entry["manifest"] = str(result["manifest"])[:300]
        record_vantage(entry, CI_VANTAGE, True, now)
    else:
        entry["play_fail"] = int(entry.get("play_fail") or 0) + 1
        entry["last_play_fail"] = now
    _halve_if_large(entry, "play_ok", "play_fail")


def health_score(
    entry: dict,
    state: dict | None = None,
    url: str = "",
    now: int | None = None,
) -> float:
    """0..1 empirical Bayes reliability score with exponential time decay.

    Hierarchy: real app playback > host & stream-family priors > CI playback
    probes > background scans > outer-HTML page checks > neutral prior (0.5).
    """
    def part(ok: float, fail: float, weight: float, damp: float = 1.0) -> tuple[float, float]:
        n = ok + fail
        if n <= 0:
            return 0.5, 0.0
        return (ok + 1.0) / (n + 2.0), weight * n / (n + damp)

    ref_now = _now() if now is None else now
    url_hl = URL_HALF_LIFE_HOURS * 3600.0
    host_hl = HOST_HALF_LIFE_DAYS * 86400.0
    entry = entry or {}

    app_ok, app_fail = _decayed_pair(
        float(entry.get("app_ok") or 0), float(entry.get("app_fail") or 0),
        int(entry.get("app_last_ok") or 0), int(entry.get("app_last_fail") or 0),
        ref_now, url_hl,
    )
    scan_ok, scan_fail = _decayed_pair(
        float(entry.get("scan_ok") or 0), float(entry.get("scan_fail") or 0),
        int(entry.get("scan_last") or 0), int(entry.get("scan_last") or 0),
        ref_now, url_hl,
    )
    play_ok, play_fail = _decayed_pair(
        float(entry.get("play_ok") or 0), float(entry.get("play_fail") or 0),
        int(entry.get("last_play") or 0), int(entry.get("last_play_fail") or 0),
        ref_now, url_hl,
    )
    page_ok, page_fail = _decayed_pair(
        float(entry.get("ok") or 0), float(entry.get("fail") or 0),
        int(entry.get("last_ok") or 0), int(entry.get("last_fail") or 0),
        ref_now, url_hl,
    )

    parts = [
        part(app_ok, app_fail, APP_WEIGHT, 1.0),
        part(play_ok, play_fail, PLAYBACK_WEIGHT, 1.0),
        part(scan_ok, scan_fail, SCAN_WEIGHT, 2.0),
        part(page_ok, page_fail, PAGE_WEIGHT, 1.5),
        (0.5, 0.5),  # neutral prior
    ]
    if url and isinstance(state, dict):
        fam = url_family(url)
        fam_bucket = (state.get("families") or {}).get(fam) if fam else None
        if isinstance(fam_bucket, dict):
            f_ok, f_fail = _decayed_pair(
                float(fam_bucket.get("ok") or 0), float(fam_bucket.get("fail") or 0),
                int(fam_bucket.get("last_ok") or 0), int(fam_bucket.get("last_fail") or 0),
                ref_now, host_hl, fallback_ts=_bucket_last_ts(fam_bucket),
            )
            parts.append(part(f_ok, f_fail, FAMILY_WEIGHT, 3.0))
        host_bucket = (state.get("hosts") or {}).get(host_of(url))
        if isinstance(host_bucket, dict):
            h_ok, h_fail = _decayed_pair(
                float(host_bucket.get("ok") or 0), float(host_bucket.get("fail") or 0),
                int(host_bucket.get("last_ok") or 0), int(host_bucket.get("last_fail") or 0),
                ref_now, host_hl, fallback_ts=_bucket_last_ts(host_bucket),
            )
            parts.append(part(h_ok, h_fail, HOST_WEIGHT, 3.0))

    total = sum(w for _, w in parts)
    return round(sum(r * w for r, w in parts) / total, 4) if total else 0.5


def _record(state: dict, url: str, ok: bool, detail: str, now: int,
            trust_failures: bool, height: int = 0, bitrate: int = 0,
            manifest: str = "", manifest_dead: str = "") -> None:
    """Quick page check from the CI runner (vantage "ci")."""
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
            entry["disabled_until"] = 0
            entry["blocked"] = False
            entry.pop("blocked_reason", None)
            entry.pop("blocked_at", None)
        if entry is None and manifest_dead:
            entry = _entry(state, url)
        if entry is None:
            return  # a working URL with no history needs no entry
        entry["ok"] = min(int(entry.get("ok") or 0) + 1, 9999)
        entry["last_ok"] = now
        entry["last_status"] = str(detail)[:24]
        if manifest_dead:
            # The playlist URL no longer answers: drop a stale measurement, count nothing.
            entry["last_status"] = f"manifest-{manifest_dead}"[:24]
            if not int(height or 0):
                entry["height"] = 0
                entry["bitrate"] = 0
                entry["verified"] = False
        _halve_if_large(entry, "ok", "fail")
        record_vantage(entry, CI_VANTAGE, True, now)
        return
    if not trust_failures:
        return
    status = str(detail)[:24]
    if status in SERVER_BLOCKED_STATUSES:
        entry = entries.get(url)
        if entry is None or not _recent_app_failure(entry, now):
            # Refused from the server only: note it, never count it.
            entry = _entry(state, url)
            entry["last_status"] = status
            entry["server_blocked"] = min(int(entry.get("server_blocked") or 0) + 1, 9999)
            entry["last_server_block"] = now
            return
    entry = _entry(state, url)
    entry["last_status"] = status
    if status.startswith("manifest-"):
        # The stream URL itself is gone/forbidden; drop the stale quality so it
        # stops ranking on a measurement that no longer plays.
        entry["height"] = 0
        entry["bitrate"] = 0
        entry["verified"] = False
    entry["fail"] = min(int(entry.get("fail") or 0) + 1, 9999)
    entry["last_fail"] = now
    manifest_code = status.split("-", 1)[1] if status.startswith("manifest-") else ""
    if status in DEFINITIVE_STATUSES or manifest_code in DEFINITIVE_STATUSES:
        entry["dead_fails"] = min(int(entry.get("dead_fails") or 0) + 1, 99)
    if int(entry.get("dead_fails") or 0) >= 2:
        entry["blocked"] = True
        entry["blocked_reason"] = f"HTTP {status} x{entry['dead_fails']}"
        entry["blocked_at"] = now
        entry["disabled_until"] = 0
        return
    record_vantage(entry, CI_VANTAGE, False, now)
    if int(entry.get("fail") or 0) >= PERMANENT_FAIL_TOTAL and int(entry.get("quarantines") or 0) >= PERMANENT_QUARANTINES:
        entry["blocked"] = True
        entry["blocked_reason"] = f"failed {entry['fail']}x across {entry['quarantines']} quarantines"
        entry["blocked_at"] = now
        entry["disabled_until"] = 0


def _channel_url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def _record_channel_app_result(state: dict, url: str, ok: bool, when: int) -> None:
    """Fold real TV/app playback outcomes into tracked Live TV channel URLs."""
    ch_urls = (state.get("channels") or {}).get("urls")
    if not isinstance(ch_urls, dict) or not url:
        return
    rec = ch_urls.get(_channel_url_key(url))
    if not isinstance(rec, dict):
        return
    if ok:
        rec["app_ok"] = int(rec.get("app_ok") or 0) + 1
        rec["app_last_ok"] = max(int(rec.get("app_last_ok") or 0), when)
        rec["app_fail_streak"] = 0
        rec["fail_streak"] = 0
        rec["demoted"] = False
    else:
        rec["app_fail"] = int(rec.get("app_fail") or 0) + 1
        rec["app_last_fail"] = max(int(rec.get("app_last_fail") or 0), when)
        rec["app_fail_streak"] = int(rec.get("app_fail_streak") or 0) + 1
        if rec["app_fail_streak"] >= 3:
            rec["demoted"] = True


def ingest_telemetry_rows(
    state: dict,
    files: dict[str, list[dict]],
    now: int | None = None,
) -> dict:
    """Fold parsed per-file telemetry rows into `state` using per-file cursors.

    Accepts both `url` (desktop Slang) and `source_url` (Android TV Slang),
    prefers unique `device_id` over `device` for vantage tracking, separates
    real `source_result` playback from background `source_scan`, and decays
    host/family buckets over `HOST_HALF_LIFE_DAYS`.
    """
    ref_now = _now() if now is None else now
    legacy_ts = str(state.get("last_telemetry_ts") or "")
    first_migration = "telemetry_cursors" not in state
    cursors: dict = state.setdefault("telemetry_cursors", {})
    hosts = state.setdefault("hosts", {})
    max_global_ts = legacy_ts
    processed = 0
    half_life_sec = HOST_HALF_LIFE_DAYS * 86400.0

    for name, rows in files.items():
        # Legacy migration (once): seed cursors for already-existing files from
        # `last_telemetry_ts` so old host counts are not double-counted. After
        # migration, every new device/day file starts from "" so delayed flushes
        # are never dropped by another device's clock.
        if name in cursors:
            file_cursor = str(cursors[name] or "")
        elif first_migration and legacy_ts:
            file_cursor = legacy_ts
        else:
            file_cursor = ""
        file_max = file_cursor
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = str(row.get("ts") or "")
            if ts and file_cursor and ts <= file_cursor:
                continue
            if ts > file_max:
                file_max = ts
            if ts > max_global_ts:
                max_global_ts = ts
            rtype = row.get("type")
            url = str(row.get("url") or row.get("source_url") or "").strip()
            host = str(row.get("source_host") or row.get("host") or host_of(url)).strip().lower()
            if not host:
                continue
            when = _iso_epoch(ts) or ref_now
            bucket = hosts.setdefault(host, {"ok": 0.0, "fail": 0.0, "height": 0, "stream_kbps": 0, "last_seen": ""})
            if rtype == "source_result":
                ok_val = bool(row.get("ok"))
                reason = str(row.get("reason") or "")
                rebuffer_count = int(row.get("rebuffer_count") or 0)
                audio_ok = row.get("audio_ok") if isinstance(row.get("audio_ok"), bool) else None
                ok_credit, fail_credit = _qoe_credits(
                    ok_val, reason=reason, rebuffer_count=rebuffer_count, audio_ok=audio_ok,
                )
                _update_decayed_bucket(bucket, ok_credit, fail_credit, when, half_life_sec)
                h = int(row.get("height") or 0)
                fps = float(row.get("fps") or 0.0)
                kbps = int(row.get("bitrate_kbps") or row.get("stream_kbps") or 0)
                if h > 0:
                    bucket["height"] = max(int(bucket.get("height") or 0), h)
                if kbps > 0:
                    bucket["stream_kbps"] = max(int(bucket.get("stream_kbps") or 0), kbps)
                device = str(row.get("device_id") or row.get("device") or "app").strip()
                if url.startswith("https://"):
                    record_app_result(
                        state, url, device, ok_val, when, ref_now,
                        height=h, fps=fps, kbps=kbps,
                        reason=reason,
                        watch_sec=int(row.get("watch_sec") or 0),
                        rebuffer_count=rebuffer_count,
                        audio_ok=audio_ok,
                    )
                    _record_channel_app_result(state, url, ok_val, when)
                processed += 1
            elif rtype == "source_scan":
                ok_val = bool(row.get("ok"))
                h = int(row.get("height") or 0)
                fps = float(row.get("fps") or 0.0)
                kbps = int(row.get("bitrate_kbps") or row.get("stream_kbps") or 0)
                if ok_val:
                    if h > 0:
                        bucket["height"] = max(int(bucket.get("height") or 0), h)
                    if kbps > 0:
                        bucket["stream_kbps"] = max(int(bucket.get("stream_kbps") or 0), kbps)
                if url.startswith("https://"):
                    record_scan_result(state, url, ok_val, when, ref_now, height=h, fps=fps, kbps=kbps)
                processed += 1
            elif rtype in ("first_frame", "source_quality"):
                h = int(row.get("height") or 0)
                kbps = int(row.get("stream_kbps") or 0)
                if h > 0:
                    bucket["height"] = max(int(bucket.get("height") or 0), h)
                if kbps > 0:
                    bucket["stream_kbps"] = max(int(bucket.get("stream_kbps") or 0), kbps)
                processed += 1
            if ts and ts > str(bucket.get("last_seen") or ""):
                bucket["last_seen"] = ts
        if file_max:
            cursors[name] = file_max

    # Drop cursors for files deleted from the telemetry branch.
    for old_name in list(cursors.keys()):
        if old_name not in files:
            cursors.pop(old_name, None)
    if max_global_ts:
        state["last_telemetry_ts"] = max_global_ts
    return {"rows": processed, "hosts": len(hosts)}


def ingest_telemetry_from_git(
    state_path: str | os.PathLike = DEFAULT_STATE,
    repo_dir: str | os.PathLike = ".",
    ref: str = "origin/telemetry",
) -> dict:
    """Fold on-device playback telemetry (host/family/URL reliability + resolution) into state."""
    import subprocess

    try:
        names = subprocess.check_output(
            ["git", "-C", str(repo_dir), "ls-tree", "-r", "--name-only", ref, "--", "telemetry"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
    except Exception:
        return {"rows": 0, "hosts": 0}

    state = load_state(state_path)
    parsed_files: dict[str, list[dict]] = {}
    for name in names:
        if not name.endswith(".jsonl"):
            continue
        try:
            text = subprocess.check_output(
                ["git", "-C", str(repo_dir), "show", f"{ref}:{name}"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            continue
        rows: list[dict] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
        parsed_files[name] = rows

    result = ingest_telemetry_rows(state, parsed_files)
    if result["rows"] > 0 or state.get("telemetry_cursors"):
        save_state(state_path, state, state.get("stats") or {})
    return result


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
    quality_budget: int = QUALITY_BROWSER_BUDGET,
    quality_workers: int = QUALITY_BROWSER_WORKERS,
    quality_timeout: int = QUALITY_BROWSER_TIMEOUT,
    planner=None,
    homes: dict[str, str] | None = None,
    site_of: dict[str, str] | None = None,
    playback_recheck_min: int = 90,
) -> dict:
    """Probe, rank and trim sources on feed-shaped event dictionaries.

    With a ``planner`` (probe_scheduler.plan_probes) only the URLs it picks are
    page-probed, and its priority order drives the playback checks
    (``quality_budget`` per run, 0 = off). Without one, the legacy behaviour:
    the top candidates of every event (or all of them with ``probe_all``).
    ``homes`` maps URL -> site home page (playback probe origin); ``site_of``
    maps URL -> site id for the per-site run counts the canary reads.
    """
    now = _now()
    state = load_state(state_path)
    entries = state.setdefault("sources", {})
    blocklist = load_blocklist(blocklist_path)
    homes = homes or {}
    site_of = site_of or {}
    plan = None
    if do_probe and planner is not None and not probe_all:
        plan = planner(events, state, now, blocklist=blocklist)

    candidates_by_event: list[list[dict]] = []
    unique: list[str] = []
    seen: set[str] = set()
    all_unique: set[str] = set()
    raw_unique: set[str] = set()
    blocked_in_feed: set[str] = set()
    quarantined_in_feed: set[str] = set()
    non_future_urls: set[str] = set()
    for event in events:
        ev_status = str(event.get("status") or "")
        for src in event.get("sources") or []:
            url = str(src.get("url") or "").strip()
            if not url.startswith("https://"):
                continue
            if ev_status != "future":
                non_future_urls.add(url)
            raw_unique.add(url)
            entry = entries.get(url, {})
            if is_manually_blocked(url, blocklist) or is_blocked(entry):
                blocked_in_feed.add(url)
            elif is_disabled(entry, now):
                quarantined_in_feed.add(url)
        ranked = ranked_sources(event.get("sources") or [], state, now, blocklist=blocklist)
        if max_sources <= 0:
            # 0 = no cap: rank everything and let the quality/probe verdict drop the bad ones.
            pool = ranked
        else:
            limit = MAX_SOURCES_247 if event.get("status") == "always_live" else max(CANDIDATE_POOL, max_sources * 2)
            pool = ranked if probe_all else ranked[:limit]
        candidates_by_event.append(pool)
        for src in pool:
            url = str(src.get("url") or "").strip()
            if not url:
                continue
            all_unique.add(url)
            if plan is None and url not in seen and len(unique) < probe_budget:
                seen.add(url)
                unique.append(url)
    if plan is not None:
        unique = list(plan["page"])

    results: dict[str, tuple[bool, str]] = {}
    trusted = True
    detailed: dict[str, dict] = {}
    quality_stats = {
        "quality_probed": 0,
        "quality_browser_measured": 0,
        "quality_manifests": 0,
        "playback_ok": 0,
    }
    host_stats: dict[str, dict] = {}
    manifest_failures = 0
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
        # Playlist fetch failures are only hints; the playback check below goes
        # to those sources first, since the page check could not judge them.
        manifest_failures = apply_manifest_failures(detailed, trusted=trusted)
        # Level 2: real playback in a browser for page-ok URLs, in priority
        # order, skipping ones verified playing within playback_recheck_min.
        if quality_budget > 0:
            order = list(plan["playback"]) if plan is not None else unique
            recheck = max(0, int(playback_recheck_min)) * 60
            browser_urls = [
                url for url in order
                if detailed.get(url, {}).get("ok")
                and now - int(entries.get(url, {}).get("last_play") or 0) >= recheck
            ]
            browser_urls.sort(key=lambda url: 0 if detailed[url].get("manifest_dead") else 1)
            if plan is None and len(browser_urls) > quality_budget:
                # Rotate the window so successive runs cover the whole catalogue.
                offset = now % len(browser_urls)
                browser_urls = browser_urls[offset:] + browser_urls[:offset]
            if browser_urls:
                measured = probe_playback(
                    browser_urls,
                    homes=homes,
                    budget=quality_budget,
                    workers=quality_workers,
                    timeout=quality_timeout,
                )
                quality_stats["quality_probed"] = len(measured)
                for url, quality in measured.items():
                    record_playback(state, url, quality, now)
                    info = detailed.get(url)
                    if info is None:
                        continue
                    info["playing"] = bool(quality.get("playing"))
                    if quality.get("playing"):
                        quality_stats["playback_ok"] += 1
                    if quality.get("manifest"):
                        quality_stats["quality_manifests"] += 1
                        info["manifest"] = str(quality["manifest"])
                    if int(quality.get("height") or 0) > 0:
                        quality_stats["quality_browser_measured"] += 1
                        info["height"] = int(quality["height"])
                        info["bitrate"] = int(quality.get("bitrate") or 0)
                        info["verified"] = True
                        info["quality"] = str(quality.get("quality") or "")
        for url, info in detailed.items():
            if not info["ok"] and info.get("status") == "starting_soon" and url not in non_future_urls:
                # Upcoming games legitimately show "Starting Soon" before kickoff.
                continue
            _record(
                state, url, bool(info["ok"]), str(info["status"]), now,
                trust_failures=trusted,
                height=int(info.get("height") or 0),
                bitrate=int(info.get("bitrate") or 0),
                manifest=str(info.get("manifest") or ""),
                manifest_dead=str(info.get("manifest_dead") or ""),
            )
        if not trusted:
            # Do not demote anything when the runner's probes look blocked.
            results = {url: value for url, value in results.items() if value[0]}
        host_stats = _host_probe_stats(detailed)

    trimmed = 0
    empty = 0
    weak_dropped = 0
    weak_kept = 0
    selected_sources = 0
    multi_host_events = 0
    for index, event in enumerate(events):
        if max_sources <= 0:
            limit = None  # publish every mirror that survives ranking + probing
        else:
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
        candidates = []
        for index, src in enumerate(ranked):
            url = str(src.get("url") or "").strip()
            entry = entries.get(url, {})
            if is_disabled(entry, now):
                continue
            probe = results.get(url)
            probe_ok = probe[0] if probe is not None else None
            if probe_ok is False and (
                int(entry.get("streak") or 0) >= 2 or int(entry.get("fail") or 0) >= 3
            ):
                # Repeatedly failing mirrors are not worth a fallback slot.
                continue
            candidates.append({
                "src": src,
                "host": host_of(url),
                "ok": probe_ok,
                "weak": is_weak_source(src, entry, probe_ok, state=state, now=now),
                "key": _selection_key(
                    src, entry, detailed.get(url), probe_ok, host_stats, state, index, now=now
                ),
            })
        # Weak mirrors (low res, failing, or chronically dead host) only fill thin events.
        good = [c for c in candidates if not c["weak"]]
        weak = sorted((c for c in candidates if c["weak"]), key=lambda c: c["key"])
        keep = max(0, MIN_SOURCES_PER_EVENT - len(good))
        weak_dropped += max(0, len(weak) - keep)
        weak_kept += min(len(weak), keep)
        candidates = good + weak[:keep]
        final = _select_best_sources(candidates, limit)
        if len(final) != len(source_list):
            trimmed += 1
        if not final:
            empty += 1
        else:
            selected_sources += len(final)
            if len({host_of(str(src.get("url") or "")) for src in final}) > 1:
                multi_host_events += 1
        event["sources"] = final

    site_probe: dict[str, dict] = {}
    for url, info in detailed.items():
        site = site_of.get(url)
        if not site:
            continue
        bucket = site_probe.setdefault(site, {"probed": 0, "ok": 0, "server_blocked": 0, "play_tried": 0, "played": 0})
        bucket["probed"] += 1
        if info.get("ok"):
            bucket["ok"] += 1
        elif str(info.get("status") or "") in SERVER_BLOCKED_STATUSES:
            bucket["server_blocked"] += 1
        if "playing" in info:
            bucket["play_tried"] += 1
            bucket["played"] += 1 if info["playing"] else 0

    stats = {
        "probed": len(results),
        "playback_ok": quality_stats["playback_ok"],
        "scheduled": (plan or {}).get("counts", {}),
        "site_probe": site_probe,
        "probe_ok": sum(1 for ok, _ in results.values() if ok),
        "probe_failed": sum(1 for ok, _ in results.values() if not ok),
        "trusted": trusted,
        "quality_probed": quality_stats["quality_probed"],
        "quality_measured": sum(
            1 for info in detailed.values() if int(info.get("height") or 0) > 0
        ),
        "quality_browser_measured": quality_stats["quality_browser_measured"],
        "quality_manifests": sum(
            1 for info in detailed.values() if str(info.get("manifest") or "")
        ),
        "manifest_failed": manifest_failures,
        "trimmed_events": trimmed,
        "weak_dropped": weak_dropped,
        "weak_kept": weak_kept,
        "empty_events": empty,
        "selected_sources": selected_sources,
        "multi_host_events": multi_host_events,
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
          f"quality={stats['quality_measured']} measured "
          f"playback={stats['playback_ok']}/{stats['quality_probed']} "
          f"manifests={stats['quality_manifests']} manifest_dead={stats['manifest_failed']} "
          f"trimmed={trimmed} empty={empty} selected={selected_sources} "
          f"weak_dropped={weak_dropped} weak_kept={weak_kept} "
          f"multi_host={multi_host_events} cap={max_sources} "
          f"blocked_state={stats['blocked']} manual={stats['manual_url_blocks']}u/{stats['manual_host_blocks']}h",
        flush=True,
    )
    return stats
