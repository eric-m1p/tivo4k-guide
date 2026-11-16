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


XMLTV_FEEDS = [
    "https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz",
]

# Map normalized always_live network titles -> candidate XMLTV display-name/id substrings
NETWORK_ALIASES = {
    "amc": ["amc hd", "amc"],
    "bbcamerica": ["bbc america hd", "bbc america"],
    "cnn": ["cnn hd", "cnn"],
    "foxnews": ["fox news channel hd", "fox news channel", "fox news"],
    "hbocomedy": ["hbo comedy hd", "hbo comedy"],
    "hbodrama": ["hbo signature hd", "hbo drama", "hbo2 hd"],
    "hboeast": ["hbo hd", "hbo east", "hbo"],
    "hbohits": ["hbo hits", "hbo zone"],
    "hbolatino": ["hbo latino hd", "hbo latino"],
    "hbomovies": ["hbo2 hd", "hbo family"],
    "hbowest": ["hbo (pacific)", "hbo west"],
    "nbcknbc": ["knbc", "nbc 4", "nbc los angeles"],
    "nbcsbayarea": ["nbc sports bay area"],
    "nbcsboston": ["nbc sports boston"],
    "nbcscalifornia": ["nbc sports california"],
    "nbcsphiladelphia": ["nbc sports philadelphia"],
    "nflnetwork": ["nfl network hd", "nfl network"],
    "nhlnetwork": ["nhl network hd", "nhl network"],
    "premiersports1uk": ["premier sports 1 hd", "premier sports 1"],
    "foxcricket": ["fox cricket"],
    "foxfooty": ["fox footy"],
    "foxleague": ["fox league"],
    "skysportsgolf": ["sky sports golf"],
    "skysportsracing": ["sky sports racing"],
    "willow": ["willow hd", "willow cricket", "willow"],
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _parse_xmltv_time(raw: str) -> int | None:
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in ("%Y%m%d%H%M%S %z", "%Y%m%d%H%M %z", "%Y%m%d%H%M%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return int(dt.timestamp())
        except ValueError:
            continue
    return None


def fetch_network_epg(events_path: Path, now_sec: int) -> dict[str, list]:
    """Pull XMLTV schedules for always_live networks in events.json."""
    if not events_path.is_file():
        return {}
    try:
        evs = json.loads(events_path.read_text(encoding="utf-8")).get("events") or []
    except Exception:
        return {}
    wanted_slugs: set[str] = set()
    for e in evs:
        if e.get("status") == "always_live" and (e.get("league") or "").lower() != "24/7 show":
            slug = _norm(e.get("title") or "")
            if slug:
                wanted_slugs.add(slug)
    if not wanted_slugs:
        return {}

    import gzip
    import urllib.request
    import xml.etree.ElementTree as ET

    win_start = now_sec - 3600
    win_end = now_sec + 8 * 3600
    matched: dict[str, list] = {}

    for url in XMLTV_FEEDS:
        remaining = [s for s in wanted_slugs if f"net:{s}" not in matched]
        if not remaining:
            break
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            raw = urllib.request.urlopen(req, timeout=20).read()
            root = ET.fromstring(gzip.decompress(raw))
        except Exception as exc:
            print(f"      xmltv skip {url}: {exc}")
            continue

        cid_by_slug: dict[str, str] = {}
        ch_names: dict[str, str] = {}
        for ch in root.findall("channel"):
            cid = ch.attrib.get("id", "")
            names = [dn.text or "" for dn in ch.findall("display-name")]
            ch_names[cid] = (" ".join(names) + " " + cid).lower()

        for slug in remaining:
            aliases = NETWORK_ALIASES.get(slug, [slug])
            for alias in aliases:
                for cid, hay in ch_names.items():
                    if alias in hay:
                        cid_by_slug[cid] = slug
                        break
                if slug in cid_by_slug.values():
                    break

        if not cid_by_slug:
            continue

        progs_by_cid: dict[str, list] = {}
        for prog in root.findall("programme"):
            cid = prog.attrib.get("channel", "")
            if cid not in cid_by_slug:
                continue
            s = _parse_xmltv_time(prog.attrib.get("start", ""))
            e = _parse_xmltv_time(prog.attrib.get("stop", ""))
            if s is None or e is None or e <= s or e < win_start or s > win_end:
                continue
            t_el = prog.find("title")
            title = (t_el.text or "").strip() if t_el is not None else ""
            if not title:
                continue
            sub_el = prog.find("sub-title")
            ep = (sub_el.text or "").strip()[:60] if sub_el is not None else ""
            progs_by_cid.setdefault(cid, []).append([s, e, title, ep])

        for cid, slug in cid_by_slug.items():
            rows = sorted(progs_by_cid.get(cid, []), key=lambda r: r[0])
            if rows:
                matched[f"net:{slug}"] = rows

    return matched


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

    net_epg = fetch_network_epg(HERE / "events.json", int(time.time()))
    if net_epg:
        out.update(net_epg)
        print(f"      added {len(net_epg)} network EPG schedules from XMLTV")

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
