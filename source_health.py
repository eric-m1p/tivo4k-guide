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

import base64
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
MANIFEST_DEAD_STATUSES = {"403", "404", "410"}
PERMANENT_FAIL_TOTAL = 6
PERMANENT_QUARANTINES = 2
# Headless-browser quality pass. Static HTML only sees the outer embed page; most
# players build their manifest at runtime, so a real browser is the only reliable
# way to measure height/bitrate from CI. The budget keeps a full feed run inside
# the workflow's time limit; URLs already measured are remembered in state.
QUALITY_BROWSER_BUDGET = 120
QUALITY_BROWSER_WORKERS = 4
QUALITY_BROWSER_TIMEOUT = 10
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
        if last < cutoff and int(entry.get("disabled_until") or 0) <= now:
            entries.pop(url, None)
        elif (int(entry.get("height") or 0) <= 0
              and int(entry.get("fail") or 0) <= 0
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


def _host_rank(state: dict, url: str) -> tuple:
    hosts = state.get("hosts") or {}
    info = hosts.get(host_of(url))
    if not isinstance(info, dict):
        return (0, 0.0, 0)
    ok = int(info.get("ok") or 0)
    fail = int(info.get("fail") or 0)
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


def _host_reliability(state: dict, host: str, run_stats: dict[str, dict]) -> float:
    run = run_stats.get(host) or {}
    run_rate = float(run.get("rate", 0.5))
    telemetry = (state.get("hosts") or {}).get(host)
    if isinstance(telemetry, dict):
        ok = int(telemetry.get("ok") or 0)
        fail = int(telemetry.get("fail") or 0)
        telemetry_rate = (ok + 1) / (ok + fail + 2)
        return round(run_rate * 0.7 + telemetry_rate * 0.3, 3)
    return round(run_rate, 3)


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


def _selection_key(
    src: dict,
    entry: dict,
    probe_info: dict | None,
    probe_ok: bool | None,
    host_stats: dict[str, dict],
    state: dict,
    index: int,
) -> tuple:
    """Order trusted live sources before guesses, then quality, reliability, speed."""
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
    reliability = _host_reliability(state, host, host_stats)
    streak = int(entry.get("streak") or 0)
    fail = int(entry.get("fail") or 0)
    ok_count = int(entry.get("ok") or 0)
    health_penalty = min(streak, 6) * 12 + min(fail, 12) * 3 - min(ok_count, 30)
    return (
        0 if probe_ok is True else (1 if probe_ok is None else 2),
        -_quality_score(src, height),
        -min(height, 4320),
        0 if fps >= 48.0 else 1,
        0 if (src.get("verified") or entry.get("verified")) else 1,
        -reliability,
        -min(bitrate, 50_000_000),
        latency,
        health_penalty,
        -min(ok_count, 50),
        index,
    )


def _select_best_sources(candidates: list[dict], limit: int) -> list[dict]:
    """Greedy best-N with host diversity, then fill from the same hosts.

    Liveness always wins: every probe-ok source outranks unprobed/failed
    mirrors. Within that tier, the best source on each host is taken first so
    one CDN outage cannot take out the whole failover stack; the remaining
    slots are then filled with the best duplicates.
    """
    if not candidates or limit <= 0:
        return []
    ordered = sorted(candidates, key=lambda item: item["key"])
    healthy = [item for item in ordered if item["ok"] is True]
    unprobed = [item for item in ordered if item["ok"] is None]
    failed = [item for item in ordered if item["ok"] is False]
    selected: list[dict] = []
    selected_urls: set[str] = set()
    used_hosts: set[str] = set()

    def diverse(pool: list[dict]) -> None:
        for item in pool:
            if len(selected) >= limit:
                return
            host = item["host"]
            if host in used_hosts:
                continue
            src = item["src"]
            url = str(src.get("url") or "")
            if url in selected_urls:
                continue
            selected.append(src)
            selected_urls.add(url)
            used_hosts.add(host)

    def fill(pool: list[dict]) -> None:
        for item in pool:
            if len(selected) >= limit:
                return
            src = item["src"]
            url = str(src.get("url") or "")
            if url in selected_urls:
                continue
            selected.append(src)
            selected_urls.add(url)

    for pool in (healthy, unprobed, failed):
        diverse(pool)
        fill(pool)
        if len(selected) >= limit:
            break
    # Keep the diverse membership but play the best source first: a lower-quality
    # mirror on a new host is a fallback, not something to try before a better
    # duplicate on a host that already answered.
    key_by_url = {str(item["src"].get("url") or ""): item["key"] for item in candidates}
    selected.sort(key=lambda src: key_by_url.get(str(src.get("url") or ""), ()))
    return selected[:limit]


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
        0 if item[0].get("verified") else 1,
        -int(item[0].get("bitrate") or 0),
        _health_rank(item[2]),
        _host_rank(state, str(item[0].get("url") or "")),
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


def _decode_char_arrays(text: str) -> str:
    """Decode `String.fromCharCode(parseInt(atob(v))-k)` pages (streamxhd).

    The page stores shuffled `[index, base64]` pairs and subtracts the sum of
    two constant functions from the digits left after decoding. Rebuild the
    string and let the normal manifest regex find the URL.
    """
    kexpr = re.search(r"var\s+k\s*=\s*(\w+)\(\)\+(\w+)\(\)", text)
    if not kexpr:
        return ""
    total = 0
    for name in kexpr.groups():
        const = re.search(r"function\s+" + re.escape(name) + r"\(\)\{return\s+(\d+)", text)
        if const:
            total += int(const.group(1))
    if total <= 0:
        return ""
    for array in re.findall(r"=\[\[(.*?)\];", text, re.S):
        pairs = re.findall(r'\[(\d+),"([^"]+)"\]', array)
        if len(pairs) < 8:
            continue
        chars = []
        for _, encoded in sorted(pairs, key=lambda item: int(item[0])):
            digits = "".join(ch for ch in _b64_decode(encoded) if ch.isdigit())
            if not digits:
                continue
            code = int(digits) - total
            if 32 <= code < 127:
                chars.append(chr(code))
        candidate = "".join(chars)
        if ".m3u8" in candidate or ".mpd" in candidate:
            return candidate
    return ""


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
        if found:
            manifest = manifest or found
            try:
                import requests
                with requests.get(
                    found,
                    headers={**headers, "Referer": manifest_referer},
                    timeout=(timeout, timeout),
                    stream=True,
                    allow_redirects=True,
                ) as response:
                    manifest_status = str(response.status_code)
                    if 200 <= response.status_code < 400:
                        body = _read_limited(response, MANIFEST_LIMIT).decode("utf-8", "replace")
                        parsed_height, parsed_bitrate = _quality_from_manifest(body)
                        if parsed_height > 0:
                            height, bitrate = parsed_height, parsed_bitrate
            except Exception:
                pass
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
    """Turn a dead manifest (403/404/410) into a probe failure.

    The outer embed page often stays 200 while the tokenized m3u8 it points at
    has expired. Without this, a source measured earlier keeps its height and
    clears its fail streak forever even though playback gets a 403.
    """
    if not trusted:
        return 0
    changed = 0
    for info in results.values():
        if not info.get("ok"):
            continue
        if int(info.get("height") or 0) > 0:
            continue
        status = str(info.get("manifest_status") or "")
        if status in MANIFEST_DEAD_STATUSES:
            info["ok"] = False
            info["status"] = f"manifest-{status}"
            changed += 1
    return changed


def _looks_like_manifest(url: str) -> bool:
    lowered = url.lower()
    return ".m3u8" in lowered or ".mpd" in lowered or "manifest.ts" in lowered


def _browser_launch(playwright):
    """Launch a real browser. Use installed Chrome first, then Playwright's build."""
    for options in ({"channel": "chrome"}, {}):
        try:
            return playwright.chromium.launch(headless=True, **options)
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


def _browser_probe_one(ctx, url: str, timeout: int) -> dict:
    """Load an embed inside an iframe (so anti-embed checks pass) and measure quality."""
    page = ctx.new_page()
    manifests: list[str] = []
    page.on(
        "request",
        lambda request: manifests.append(request.url)
        if _looks_like_manifest(request.url) else None,
    )
    height = 0
    bitrate = 0
    manifest = ""
    try:
        page.set_content(
            '<html><body style="margin:0;background:#000">'
            f'<iframe src="{html.escape(url)}" style="width:1280px;height:720px;border:0" '
            'allow="autoplay; fullscreen; picture-in-picture"></iframe></body></html>'
        )
        deadline = time.monotonic() + max(2, timeout)
        while time.monotonic() < deadline:
            page.wait_for_timeout(QUALITY_BROWSER_SETTLE_MS)
            for frame in page.frames:
                if frame is page.main_frame:
                    continue
                try:
                    config = frame.evaluate("() => window.SecKeyPlayerConfig || null")
                except Exception:
                    config = None
                if isinstance(config, dict):
                    for player in config.values():
                        if not isinstance(player, dict):
                            continue
                        candidate = _height_from_stream_type(player)
                        if candidate:
                            height = max(height, candidate)
                        source = str(player.get("urlSource") or "")
                        if source and _looks_like_manifest(source) and not manifest:
                            manifest = source[:400]
                try:
                    dimensions = frame.evaluate(
                        "() => { const v = document.querySelector('video');"
                        " return v && v.videoWidth ? [v.videoWidth, v.videoHeight] : null; }"
                    )
                except Exception:
                    dimensions = None
                if isinstance(dimensions, list) and len(dimensions) == 2:
                    height = max(height, int(dimensions[1] or 0))
            if manifests or height:
                page.wait_for_timeout(400)
                break
    except Exception:
        pass
    finally:
        try:
            page.close()
        except Exception:
            pass

    for candidate in manifests:
        body = _browser_fetch(ctx, candidate, url, timeout)
        if not body:
            continue
        h, b = _quality_from_manifest(body)
        if h > 0:
            height, bitrate, manifest = h, b, candidate
            break
        if not manifest:
            manifest = candidate[:400]
    return {
        "height": height,
        "bitrate": bitrate,
        "manifest": manifest[:400],
        "verified": height > 0,
        "quality": _quality_label(height),
    }


def _browser_worker(chunk: list[str], timeout: int) -> dict[str, dict]:
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
                for url in chunk:
                    ctx = browser.new_context(
                        user_agent=PROBE_HEADERS["User-Agent"],
                        viewport={"width": 1280, "height": 720},
                        locale="en-US",
                    )
                    try:
                        results[url] = _browser_probe_one(ctx, url, timeout)
                    finally:
                        ctx.close()
            finally:
                browser.close()
    except Exception:
        pass
    return results


def probe_quality_browser(
    urls: list[str],
    *,
    budget: int = QUALITY_BROWSER_BUDGET,
    workers: int = QUALITY_BROWSER_WORKERS,
    timeout: int = QUALITY_BROWSER_TIMEOUT,
) -> dict[str, dict]:
    """Measure real stream height/bitrate with a headless browser.

    Static HTTP probes only see the outer embed page; providers build their
    playlist in JavaScript. Each URL is loaded inside an iframe so the site's
    anti-embed checks pass, then m3u8/mpd requests, the player config and the
    video element are inspected. Returns one entry per probed URL; a `height`
    of 0 means "no hint found", not that the source is dead.
    """
    selected = [str(url).strip() for url in urls if str(url or "").startswith("https://")]
    if budget > 0:
        selected = selected[:budget]
    if not selected:
        return {}
    workers = max(1, min(int(workers or 1), 8))
    workers = min(workers, len(selected))
    chunk_size = max(1, (len(selected) + workers - 1) // workers)
    chunks = [selected[i:i + chunk_size] for i in range(0, len(selected), chunk_size)]
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futures = [pool.submit(_browser_worker, chunk, timeout) for chunk in chunks]
        for future in as_completed(futures):
            try:
                results.update(future.result())
            except Exception:
                pass
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
    if status.startswith("manifest-"):
        # The stream URL itself is gone/forbidden; drop the stale quality so it
        # stops ranking on a measurement that no longer plays.
        entry["height"] = 0
        entry["bitrate"] = 0
        entry["verified"] = False
    entry["fail"] = min(int(entry.get("fail") or 0) + 1, 9999)
    entry["streak"] = min(int(entry.get("streak") or 0) + 1, 99)
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
    if int(entry["streak"]) >= DISABLE_STREAK or int(entry["fail"]) >= DISABLE_TOTAL:
        q = min(int(entry.get("quarantines") or 0), len(QUARANTINE_HOURS) - 1)
        entry["disabled_until"] = now + QUARANTINE_HOURS[q] * 3600
        entry["quarantines"] = min(q + 1, len(QUARANTINE_HOURS) - 1)
    if int(entry.get("fail") or 0) >= PERMANENT_FAIL_TOTAL and int(entry.get("quarantines") or 0) >= PERMANENT_QUARANTINES:
        entry["blocked"] = True
        entry["blocked_reason"] = f"failed {entry['fail']}x across {entry['quarantines']} quarantines"
        entry["blocked_at"] = now
        entry["disabled_until"] = 0


def ingest_telemetry_from_git(
    state_path: str | os.PathLike = DEFAULT_STATE,
    repo_dir: str | os.PathLike = ".",
    ref: str = "origin/telemetry",
) -> dict:
    """Fold on-device playback telemetry (host success rate + resolution) into state."""
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
    last_ts = str(state.get("last_telemetry_ts") or "")
    max_ts = last_ts
    hosts = state.setdefault("hosts", {})
    sources_map = state.setdefault("sources", {})
    processed = 0

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
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            ts = str(row.get("ts") or "")
            if ts and last_ts and ts <= last_ts:
                continue
            if ts > max_ts:
                max_ts = ts
            rtype = row.get("type")
            url = str(row.get("url") or "").strip()
            host = str(row.get("source_host") or row.get("host") or host_of(url)).strip().lower()
            if not host:
                continue
            bucket = hosts.setdefault(host, {"ok": 0, "fail": 0, "height": 0, "stream_kbps": 0, "last_seen": ""})
            if rtype in ("source_result", "source_scan"):
                ok_val = bool(row.get("ok"))
                if ok_val:
                    bucket["ok"] = int(bucket.get("ok") or 0) + 1
                else:
                    bucket["fail"] = int(bucket.get("fail") or 0) + 1
                h = int(row.get("height") or 0)
                fps = float(row.get("fps") or 0.0)
                kbps = int(row.get("bitrate_kbps") or row.get("stream_kbps") or 0)
                if h > 0:
                    bucket["height"] = max(int(bucket.get("height") or 0), h)
                if kbps > 0:
                    bucket["stream_kbps"] = max(int(bucket.get("stream_kbps") or 0), kbps)
                if url:
                    entry = sources_map.setdefault(url, {"ok": 0, "fail": 0, "streak": 0})
                    if ok_val:
                        entry["ok"] = int(entry.get("ok") or 0) + 1
                        entry["streak"] = 0
                        if h >= 480:
                            entry["verified"] = True
                    else:
                        entry["fail"] = int(entry.get("fail") or 0) + 1
                        entry["streak"] = int(entry.get("streak") or 0) + 1
                    if h > 0:
                        entry["height"] = max(int(entry.get("height") or 0), h)
                    if fps > 0:
                        entry["fps"] = max(float(entry.get("fps") or 0.0), round(fps, 1))
                    if kbps > 0:
                        entry["bitrate"] = max(int(entry.get("bitrate") or 0), kbps * 1000)
                processed += 1
            elif rtype in ("first_frame", "source_quality"):
                h = int(row.get("height") or 0)
                kbps = int(row.get("stream_kbps") or 0)
                if h > 0:
                    bucket["height"] = max(int(bucket.get("height") or 0), h)
                if kbps > 0:
                    bucket["stream_kbps"] = max(int(bucket.get("stream_kbps") or 0), kbps)
                processed += 1
            if ts:
                bucket["last_seen"] = ts

    if processed > 0:
        state["last_telemetry_ts"] = max_ts
        save_state(state_path, state, state.get("stats") or {})
    return {"rows": processed, "hosts": len(hosts)}


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
    quality_stats = {
        "quality_probed": 0,
        "quality_browser_measured": 0,
        "quality_manifests": 0,
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
        # Static HTML only sees the outer embed. Measure the live URLs that still
        # have no height with a real browser (bounded, and only unmeasured URLs).
        if quality_budget > 0:
            browser_urls = [
                url for url in unique
                if detailed.get(url, {}).get("ok")
                and int(detailed.get(url, {}).get("height") or 0) <= 0
                and int(entries.get(url, {}).get("height") or 0) <= 0
            ]
            if browser_urls:
                if len(browser_urls) > quality_budget:
                    # Rotate the window so successive runs eventually measure the
                    # whole catalogue instead of re-probing the same first batch.
                    offset = now % len(browser_urls)
                    browser_urls = browser_urls[offset:] + browser_urls[:offset]
                measured = probe_quality_browser(
                    browser_urls,
                    budget=quality_budget,
                    workers=quality_workers,
                    timeout=quality_timeout,
                )
                quality_stats["quality_probed"] = len(measured)
                for url, quality in measured.items():
                    info = detailed.get(url)
                    if info is None:
                        continue
                    if quality.get("manifest"):
                        quality_stats["quality_manifests"] += 1
                        info["manifest"] = str(quality["manifest"])
                    if int(quality.get("height") or 0) > 0:
                        quality_stats["quality_browser_measured"] += 1
                        info["height"] = int(quality["height"])
                        info["bitrate"] = int(quality.get("bitrate") or 0)
                        info["verified"] = True
                        info["quality"] = str(quality.get("quality") or "")
        manifest_failures = apply_manifest_failures(detailed, trusted=trusted)
        if manifest_failures:
            results = {url: (info["ok"], info["status"]) for url, info in detailed.items()}
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
            )
        if not trusted:
            # Do not demote anything when the runner's probes look blocked.
            results = {url: value for url, value in results.items() if value[0]}
        host_stats = _host_probe_stats(detailed)

    trimmed = 0
    empty = 0
    selected_sources = 0
    multi_host_events = 0
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
                "key": _selection_key(
                    src, entry, detailed.get(url), probe_ok, host_stats, state, index
                ),
            })
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

    stats = {
        "probed": len(results),
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
          f"browser={stats['quality_browser_measured']}/{stats['quality_probed']} "
          f"manifests={stats['quality_manifests']} manifest_dead={stats['manifest_failed']} "
          f"trimmed={trimmed} empty={empty} selected={selected_sources} "
          f"multi_host={multi_host_events} cap={max_sources} "
          f"blocked_state={stats['blocked']} manual={stats['manual_url_blocks']}u/{stats['manual_host_blocks']}h",
        flush=True,
    )
    return stats
