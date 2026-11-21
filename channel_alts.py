#!/usr/bin/env python3
"""Backup streams for the Stream Sites TV channel catalogue (Step D).

The TV app's channels are Sling Freestream channels (app_channels.json). This
matches them by name to the Live TV playlist (live_channels.json, ranked and
health-ordered by channel_health.py) and writes the free HLS mirrors the app
falls back to when the Sling stream will not start:

    {"v": 1, "generated": <epoch s>,
     "ch": {"<sling channel id>": [["https://…m3u8", "<referer>", <height>], ...]}}

Only plain HTTPS .m3u8 URLs that need no custom User-Agent are kept (ExoPlayer
plays them with a Referer header only); demoted URLs and channels marked down
are left out. Matching lives here, not in the app (feed contract rule).

    python3 channel_alts.py            # writes channel_alts.json if it changed
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MAX_PER_CHANNEL = 3


def norm_name(name: str) -> str:
    """Same normalisation as the playlist builder's dedupe key, plus & -> and."""
    n = re.sub(r"\(.*?\)|\[.*?\]", "", (name or "").lower()).replace("&", "and")
    n = re.sub(r"\b(hd|uhd|4k|tv|channel|east|west|us|usa)\b", "", n)
    return re.sub(r"[^a-z0-9]", "", n)


def url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def usable(src: dict, health: dict) -> bool:
    url = str(src.get("url") or "")
    if not url.startswith("https://") or ".m3u8" not in url.lower() or src.get("user_agent"):
        return False
    return not (health.get(url_key(url)) or {}).get("demoted")


def build(app_channels: list[dict], live: list[dict], health: dict) -> dict:
    by_name: dict[str, dict] = {}
    for ch in live:
        if ch.get("local") or ch.get("down"):
            continue  # local affiliates never match a national Sling channel
        by_name.setdefault(norm_name(ch.get("name", "")), ch)
    out = {}
    for app in app_channels:
        key = norm_name(app.get("name", ""))
        ch = by_name.get(key) if key else None
        if not ch or not app.get("id"):
            continue
        rows = [[s["url"], s.get("referrer") or "", int(s.get("height") or 0)]
                for s in ch.get("sources") or [] if usable(s, health)][:MAX_PER_CHANNEL]
        if rows:
            out[app["id"]] = rows
    return out


def load(path: str, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--app-channels", default=os.path.join(HERE, "app_channels.json"))
    ap.add_argument("--channels", default=os.path.join(HERE, "live_channels.json"))
    ap.add_argument("--state", default=os.path.join(HERE, "source_health.json"))
    ap.add_argument("--out", default=os.path.join(HERE, "channel_alts.json"))
    args = ap.parse_args()

    live = (load(args.channels, {}) or {}).get("channels") or []
    app = load(args.app_channels, [])
    if not live or not app:
        print("[alts] missing live_channels.json or app_channels.json; nothing written")
        return 0
    health = ((load(args.state, {}) or {}).get("channels") or {}).get("urls") or {}
    ch = build(app, live, health)
    old = load(args.out, {})
    if old.get("ch") == ch:
        print(f"[alts] unchanged: {len(ch)} of {len(app)} app channels have backups")
        return 0
    tmp = args.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"v": 1, "generated": int(time.time()), "ch": ch}, fh, separators=(",", ":"))
        fh.write("\n")
    os.replace(tmp, args.out)
    print(f"[alts] wrote {len(ch)} of {len(app)} app channels with backups "
          f"({sum(len(v) for v in ch.values())} streams)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
