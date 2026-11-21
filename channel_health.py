#!/usr/bin/env python3
"""Sparse health checks for the Live TV channel list (Step D).

Each run probes a small rotating sample of the stream URLs in live_channels.json
(published by the local playlist build, iptv/refresh.sh), oldest check first,
main streams before backups, each URL at most once per ``every_hours``.

A probe follows the HLS playlist down to the first media segment, like the
playlist builder's own health check. Results go to source_health.json under
``channels.urls`` (keyed by sha1(url)[:16], the same key build_playlist.py
uses); a URL with ``demote_after_fails`` failures in a row is marked
``demoted`` and sorts after the healthy sources of its channel. live_channels.json
is rewritten in that order, so the apps' first choice is the healthiest stream
between local rebuilds. Rules live in feed_policy.json ``policy.probe.channels``.

    python3 channel_health.py                     # one scheduled run
    python3 channel_health.py --dry-run           # show the sample, probe nothing
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULTS = {"enabled": True, "per_run": 40, "every_hours": 24, "demote_after_fails": 3, "timeout_sec": 8}
UA = "Mozilla/5.0 (Linux; Android 12; TV) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
CTX = ssl.create_default_context()


def url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def load_json(path: str, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def write_json(path: str, doc, **kw) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, **kw)
        fh.write("\n")
    os.replace(tmp, path)


def policy_from(feed_policy: dict) -> dict:
    block = ((feed_policy.get("policy") or {}).get("probe") or {}).get("channels") or {}
    return {**DEFAULTS, **{k: v for k, v in block.items() if not k.startswith("_")}}


# ---------------------------------------------------------------- planning
def plan(channels: list[dict], urls: dict, now: int, policy: dict) -> list[dict]:
    """Pick this run's sample: due URLs, oldest check first, main streams first."""
    every = max(1, int(policy["every_hours"])) * 3600
    due = []
    for ch in channels:
        for rank, src in enumerate(ch.get("sources") or []):
            url = str(src.get("url") or "")
            if not url.startswith(("http://", "https://")):
                continue
            last = int((urls.get(url_key(url)) or {}).get("last_probe") or 0)
            if now - last >= every:
                due.append((last, min(rank, 1), ch.get("id", ""), src))
    due.sort(key=lambda t: (t[0], t[1]))
    seen, out = set(), []
    for _, _, cid, src in due:
        if src["url"] in seen:
            continue
        seen.add(src["url"])
        out.append({**src, "channel": cid})
        if len(out) >= int(policy["per_run"]):
            break
    return out


# ---------------------------------------------------------------- probing
def _fetch(url: str, headers: dict, limit: int, timeout: int):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return r.geturl(), r.status, r.read(limit)


def probe(src: dict, timeout: int = 8) -> tuple[bool, str]:
    """(ok, detail): the HLS playlist resolves down to a downloadable media segment."""
    headers = {"User-Agent": src.get("user_agent") or UA}
    if src.get("referrer"):
        headers["Referer"] = src["referrer"]
    url = src["url"]
    try:
        for _ in range(3):  # master -> variant -> (rarely) another level
            final, status, body = _fetch(url, headers, 262144, timeout)
            text = body.decode("utf-8", "replace")
            if not text.lstrip().startswith("#EXTM3U"):
                ok = status == 200 and len(body) > 1000 and not text.lstrip().startswith("<")
                return ok, "media bytes" if ok else f"not a playlist (HTTP {status})"
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            uris = [ln for ln in lines if not ln.startswith("#")]
            if not uris:
                return False, "empty playlist"
            nxt = urllib.parse.urljoin(final, uris[0])
            if "#EXTINF" in text:
                _, st, seg = _fetch(nxt, dict(headers, Range="bytes=0-2047"), 2048, timeout)
                return (st in (200, 206) and len(seg) > 0), f"segment HTTP {st}"
            url = nxt
        return False, "too many playlist levels"
    except Exception as exc:  # noqa: BLE001 - any failure is a failed probe
        return False, f"{type(exc).__name__}: {str(exc)[:120]}"


# ---------------------------------------------------------------- recording
def record(urls: dict, src: dict, ok: bool, detail: str, now: int, threshold: int) -> None:
    rec = urls.setdefault(url_key(src["url"]), {})
    rec["channel"] = src.get("channel", "")
    rec["last_probe"] = now
    rec["detail"] = detail
    if ok:
        rec["ok"] = int(rec.get("ok") or 0) + 1
        rec["last_ok"] = now
        rec["fail_streak"] = 0
    else:
        rec["fail"] = int(rec.get("fail") or 0) + 1
        rec["last_fail"] = now
        rec["fail_streak"] = int(rec.get("fail_streak") or 0) + 1
    rec["demoted"] = int(rec.get("fail_streak") or 0) >= threshold


def reorder(channels: list[dict], urls: dict) -> int:
    """Demoted sources go after healthy ones (stable); a channel with every source demoted
    gets ``down: true``. Returns how many channels changed (order or down flag)."""
    changed = 0
    for ch in channels:
        srcs = ch.get("sources") or []
        new = sorted(srcs, key=lambda s: bool((urls.get(url_key(s.get("url", ""))) or {}).get("demoted")))
        if [s.get("url") for s in new] != [s.get("url") for s in srcs]:
            ch["sources"] = new
            changed += 1
        down = bool(srcs) and all((urls.get(url_key(s.get("url", ""))) or {}).get("demoted") for s in srcs)
        if down != bool(ch.get("down")):
            changed += 1
        if down:
            ch["down"] = True
        else:
            ch.pop("down", None)
    return changed


def prune(urls: dict, channels: list[dict]) -> int:
    live = {url_key(s.get("url", "")) for ch in channels for s in ch.get("sources") or []}
    gone = [k for k in urls if k not in live]
    for k in gone:
        urls.pop(k, None)
    return len(gone)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--channels", default=os.path.join(HERE, "live_channels.json"))
    ap.add_argument("--state", default=os.path.join(HERE, "source_health.json"))
    ap.add_argument("--policy", default=os.path.join(HERE, "feed_policy.json"))
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    policy = policy_from(load_json(args.policy, {}))
    if not policy["enabled"]:
        print("[channels] disabled by policy.probe.channels.enabled")
        return 0
    doc = load_json(args.channels, None)
    if not doc or not doc.get("channels"):
        print(f"[channels] no channel list at {args.channels}; nothing to check")
        return 0
    channels = doc["channels"]
    state = load_json(args.state, {"version": 1, "sources": {}})
    block = state.setdefault("channels", {})
    urls = block.setdefault("urls", {})
    now = int(time.time())

    sample = plan(channels, urls, now, policy)
    if args.dry_run:
        for src in sample:
            print(f"{src['channel']:<32} {src['url']}")
        print(f"[channels] would probe {len(sample)} of "
              f"{sum(len(c.get('sources') or []) for c in channels)} stream URLs")
        return 0

    timeout = int(policy["timeout_sec"])
    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        results = list(ex.map(lambda s: probe(s, timeout), sample))
    threshold = max(1, int(policy["demote_after_fails"]))
    for src, (ok, detail) in zip(sample, results):
        record(urls, src, ok, detail, now, threshold)
    pruned = prune(urls, channels)
    changed = reorder(channels, urls)
    block["last_run"] = now
    block["stats"] = {
        "probed": len(sample), "ok": sum(1 for ok, _ in results if ok),
        "demoted": sum(1 for r in urls.values() if r.get("demoted")),
        "down_channels": sum(1 for c in channels if c.get("down")),
        "tracked": len(urls), "pruned": pruned, "reordered": changed,
    }
    write_json(args.state, state, indent=2, sort_keys=True)
    if changed:  # only rewrite (and commit) the list when the order or a down flag moved
        write_json(args.channels, doc, separators=(",", ":"))
    s = block["stats"]
    print(f"[channels] probed {s['probed']} ok {s['ok']}; demoted {s['demoted']}, "
          f"channels down {s['down_channels']}, reordered {s['reordered']}, tracked {s['tracked']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
