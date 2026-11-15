#!/usr/bin/env python3
"""Build the compact Now/Next guide that Stream Sites TV downloads.

Reuses freestream_extract.py's anonymous-token bootstrap (headless Chrome via
Playwright) and its schedule parser, but skips the per-channel stream-auth
calls: the guide only needs /fluid-guide/schedules.

Output (default guide-repo/channel_guide.json), ~150 KB:

    {"v": 1, "generated": <epoch s>,
     "ch": {"<channel guid>": [[start, end, "Title", "Episode"], ...]}}

Times are epoch seconds (UTC). Each channel carries the schedule window the
API returns (about 6 hours from now), so rebuild at least every ~3 hours.

    .venv/bin/python build_channel_guide.py                  # all app channels
    .venv/bin/python build_channel_guide.py --limit 10       # quick check
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import freestream_extract as fx  # noqa: E402

APP_CHANNELS = HERE.parent / "stream-sites-tv" / "res" / "raw" / "channels.json"
if not APP_CHANNELS.is_file():  # running inside the tivo4k-guide repo
    APP_CHANNELS = HERE / "app_channels.json"

# Mirror ChannelCatalogue.isEnglish(): the app hides these categories except
# for the English-language channels below, so don't fetch schedules for them.
FOREIGN_CATEGORIES = {"international", "international freestream", "español", "espanol"}
ENGLISH_EXCEPTIONS = {
    "al jazeera english", "france 24 (english)", "euronews", "nhk world-japan",
    "tvp world", "ndtv world", "india today international", "mirror now",
    "republic tv", "noursat english", "bein sports xtra", "death valley days",
}


def app_channel_ids() -> set[str]:
    ids = set()
    for c in json.loads(APP_CHANNELS.read_text(encoding="utf-8")):
        cat = (c.get("category") or "").strip().lower()
        name = (c.get("name") or "").strip().lower()
        if cat in FOREIGN_CATEGORIES and name not in ENGLISH_EXCEPTIONS:
            continue
        if c.get("id"):
            ids.add(c["id"])
    return ids


def to_epoch(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value / 1000) if value > 10**11 else int(value)
    s = str(value).strip()
    if s.isdigit():
        return to_epoch(int(s))
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def episode_of(prog: dict) -> str:
    d = prog.get("description") or ""
    # Keep a short episode line only when the description looks like one.
    m = re.match(r'^\s*(S\d+\s*E\d+[^.\n]{0,60})', d)
    return m.group(1).strip() if m else ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    default_out = HERE / "guide-repo" / "channel_guide.json"
    if not default_out.parent.is_dir():
        default_out = HERE / "channel_guide.json"
    ap.add_argument("--out", default=str(default_out))
    ap.add_argument("--limit", type=int, default=0, help="only the first N channels")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--headed", action="store_true")
    ar = ap.parse_args()

    import requests

    wanted = app_channel_ids()
    print(f"[1/3] app channels needing a guide: {len(wanted)}")
    print("[2/3] booting the web player for an anonymous API token ...")
    state = asyncio.run(fx.bootstrap(headed=ar.headed))
    catalogue = [c for c in fx.parse_catalogue(state["guide"]) if c["id"] in wanted]
    if ar.limit:
        catalogue = catalogue[: ar.limit]
    print(f"      token ok; {len(catalogue)} channels to fetch")

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/138.0.0.0"

    def fetch(c):
        return c["id"], fx.fetch_schedule(session, state, c.get("schedule_url"))

    def to_rows(progs):
        rows = []
        for p in progs:
            s, e = to_epoch(p.get("start")), to_epoch(p.get("end"))
            if s is None or e is None or e <= s:
                continue
            rows.append([s, e, p["title"], episode_of(p)])
        return rows

    t0 = time.time()
    out: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=max(1, ar.workers)) as pool:
        for i, (cid, progs) in enumerate(pool.map(fetch, catalogue), 1):
            rows = to_rows(progs)
            if rows:
                out[cid] = rows
            if i % 50 == 0 or i == len(catalogue):
                print(f"      {i}/{len(catalogue)}")

    # Bursts can get a run of channels refused (seen in CI: whole categories
    # empty). Retry the empty ones slowly, one at a time.
    for attempt in (1, 2):
        missing = [c for c in catalogue if c["id"] not in out]
        if not missing:
            break
        print(f"      retry {attempt}: {len(missing)} channels without listings")
        time.sleep(5 * attempt)
        for c in missing:
            rows = to_rows(fetch(c)[1])
            if rows:
                out[c["id"]] = rows
            time.sleep(0.4 * attempt)

    doc = {"v": 1, "generated": int(time.time()), "ch": out}
    path = Path(ar.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    spans = [r[-1][1] - r[0][0] for r in out.values()]
    print(f"[3/3] wrote {path} ({path.stat().st_size // 1024} KB): "
          f"{len(out)}/{len(catalogue)} channels with listings, "
          f"median window {sorted(spans)[len(spans) // 2] // 3600 if spans else 0} h, "
          f"{time.time() - t0:.0f}s")
    return 0 if out else 1


if __name__ == "__main__":
    sys.exit(main())
