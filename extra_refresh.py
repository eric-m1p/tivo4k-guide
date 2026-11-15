#!/usr/bin/env python3
"""
Scheduled refresh for the /extra feed (CI entrypoint).

1. sweep https://streamcorner.cloud/extra  -> extra_events.json
2. re-resolve streams only for events that are live / about to start and still
   have no working manifest (sources appear close to game time)
3. verify manifests, rebuild extra.m3u
4. leave extra_streams.json / extra.m3u changed only when something moved

Designed to run every 30 min from GitHub Actions; a full re-scrape of all 702
events is not needed each time.

Usage:
    python3 extra_refresh.py                 # window -3h .. +6h
    python3 extra_refresh.py --max 300
    python3 extra_refresh.py --all           # re-resolve everything (slow)
"""

import argparse
import asyncio
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from extra_scrape import BASE, collect_cards, launch

HERE = Path(__file__).parent
EVENTS = HERE / "extra_events.json"
STREAMS = HERE / "extra_streams.json"
M3U = HERE / "extra.m3u"

PACIFIC = ZoneInfo("America/Los_Angeles")
WORKERS = 8


# --------------------------------------------------------------------------- #
def parse_when(text: str):
    """'11/15/26 10:00 AM' (Pacific) -> aware datetime, or None."""
    if not text:
        return None
    try:
        naive = datetime.strptime(" ".join(text.split()), "%m/%d/%y %I:%M %p")
        return naive.replace(tzinfo=PACIFIC)
    except Exception:
        return None


def wanted(rec, now, hours_before=3, hours_after=6):
    when = parse_when(rec.get("time", ""))
    if when is None:
        return False
    return (now - timedelta(hours=hours_before)) <= when <= (now + timedelta(hours=hours_after))


def playable(rec):
    return bool(rec.get("playable"))


# --------------------------------------------------------------------------- #
async def scrape_targets(targets):
    """Resolve embed + manifest for each target, 8 pages in parallel."""
    results = {}
    if not targets:
        return results
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        b = await launch(p)
        ctx = await b.new_context(
            locale="en-US", viewport={"width": 1400, "height": 900},
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/138.0.0.0 Safari/537.36"))
        from extra_scrape import BLOCK

        async def route(r):
            if any(k in r.request.url for k in BLOCK):
                return await r.abort()
            await r.continue_()

        await ctx.route("**/*", route)
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        queue = asyncio.Queue()
        for t in targets:
            queue.put_nowait(t)
        done = [0]
        total = len(targets)

        async def worker():
            pg = await ctx.new_page()
            media = []
            pg.on("request", lambda r: media.append(r.url)
                  if (".m3u8" in r.url or ".mpd" in r.url) else None)
            while True:
                try:
                    ev = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                rec = dict(ev)
                media.clear()
                try:
                    await pg.goto(ev["page"], wait_until="domcontentloaded", timeout=30000)
                    await pg.wait_for_timeout(3000)
                    try:
                        await pg.click("button:has-text('Play')", timeout=3500)
                    except Exception:
                        pass
                    await pg.wait_for_timeout(4000)
                    frames = await pg.evaluate(
                        "Array.from(document.querySelectorAll('iframe')).map(f=>f.src)")
                    rec["embed"] = next((f for f in frames if "sc-chat-apple" not in f), "")
                    rec["m3u8"] = sorted({u for u in media if "sc-chat" not in u})
                except Exception as e:
                    rec["error"] = str(e)[:120]
                results[rec["id"]] = rec
                done[0] += 1
                if done[0] % 25 == 0:
                    print(f"  resolved {done[0]}/{total}", flush=True)

        await asyncio.gather(*[worker() for _ in range(WORKERS)])
        await b.close()
    return results


def verify(records):
    """Fill manifest/manifest_status/playable using a quick HTTP probe."""
    urls = [(r["id"], u) for r in records for u in (r.get("m3u8") or [])]
    if not urls:
        return

    def probe(item):
        _, u = item
        try:
            return u, subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "8",
                 "-A", "Mozilla/5.0", "-H", "Referer: https://streamcorner.cloud/", u],
                capture_output=True, text=True, timeout=12).stdout.strip()
        except Exception:
            return u, "ERR"

    with ThreadPoolExecutor(max_workers=10) as ex:
        codes = dict(ex.map(probe, urls))

    for r in records:
        best = next((u for u in (r.get("m3u8") or []) if codes.get(u) == "200"), None)
        if best is None and r.get("m3u8"):
            best = r["m3u8"][0]
        r["manifest"] = best or ""
        r["manifest_status"] = codes.get(best, "none") if best else "none"
        r["playable"] = r["manifest_status"] == "200"


def write_outputs(events, records):
    EVENTS.write_text(json.dumps(events, indent=2, ensure_ascii=False), encoding="utf-8")
    rows = sorted(records.values(), key=lambda r: (r.get("time") or "", r.get("title") or ""))
    STREAMS.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = ["#EXTM3U"]
    for r in rows:
        if not r.get("playable"):
            continue
        name = (r.get("title") or "Event").replace(",", " ")
        league = (r.get("league") or "Extra").replace('"', "")
        lines.append(f'#EXTINF:-1 tvg-id="{r["id"]}" group-title="{league}",{name}')
        lines.append(r["manifest"])
    M3U.write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=300, help="cap events resolved per run")
    ap.add_argument("--all", action="store_true", help="re-resolve every event")
    ap.add_argument("--cards-only", action="store_true")
    args = ap.parse_args()

    from playwright.async_api import async_playwright

    print("[1/4] sweeping /extra ...", flush=True)
    async with async_playwright() as p:
        b = await launch(p)
        ctx = await b.new_context(
            locale="en-US", viewport={"width": 1400, "height": 900},
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/138.0.0.0 Safari/537.36"))
        from extra_scrape import BLOCK
        async def route(r):
            if any(k in r.request.url for k in BLOCK):
                return await r.abort()
            await r.continue_()
        await ctx.route("**/*", route)
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        pg = await ctx.new_page()
        cards = await collect_cards(pg)
        await b.close()

    print(f"      {len(cards)} events", flush=True)
    if args.cards_only:
        EVENTS.write_text(json.dumps(cards, indent=2, ensure_ascii=False), encoding="utf-8")
        print("wrote", EVENTS, flush=True)
        return 0

    records = {}
    if STREAMS.exists():
        try:
            records = {r["id"]: r for r in json.loads(STREAMS.read_text())}
        except Exception:
            records = {}

    now = datetime.now(timezone.utc)
    targets = []
    for c in cards:
        old = records.get(c["id"], {})
        merged = {**old, **c}
        records[c["id"]] = merged
        if args.all:
            targets.append(merged)
        elif not playable(merged) and wanted(merged, now):
            targets.append(merged)
    targets = targets[: args.max]
    print(f"[2/4] {len(targets)} events need a source (window -3h..+6h)", flush=True)

    if targets:
        got = await scrape_targets(targets)
        records.update(got)
    print("[3/4] verifying manifests ...", flush=True)
    verify(list(records.values()))
    print("[4/4] writing outputs ...", flush=True)
    write_outputs(cards, records)

    live = sum(1 for r in records.values() if r.get("live"))
    ready = sum(1 for r in records.values() if r.get("playable"))
    print(f"done: {len(cards)} events | {live} live | {ready} playable", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
