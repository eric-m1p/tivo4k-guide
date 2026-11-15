#!/usr/bin/env python3
"""
StreamCorner /extra scraper.

The site ships `disable-devtool` from jsDelivr, which detects CDP and bounces the
page to google.com.  Blocking that one script (plus the popunder/ad junk) lets a
normal headless Chrome drive it.

Per event card: page -> Play -> embed iframe -> tokenised HLS request.

Outputs (merged per batch):
    extra_streams.json   canonical records
    extra.m3u            playlist from resolved m3u8 URLs (short-lived tokens)

Usage:
    python3 extra_scrape.py --cards-only
    python3 extra_scrape.py --offset 0  --limit 5
    python3 extra_scrape.py --offset 5  --limit 5
"""

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path

from playwright.async_api import async_playwright

BASE = "https://streamcorner.cloud"
OUT_JSON = Path(__file__).with_name("extra_streams.json")
OUT_M3U = Path(__file__).with_name("extra.m3u")

BLOCK = (
    "disable-devtool", "popa_corner_cloud", "bootstrap.min.js", "rapidadexchange",
    "cdn4ads", "ssfustmve", "grzrttparohgrl", "vcarto.min.js", "mvpapyxbiivhb",
    "cloudflareinsights", "google.com",
)


async def launch(p):
    """CI runners may not have branded Chrome; fall back to bundled Chromium."""
    args = ["--disable-blink-features=AutomationControlled"]
    try:
        return await p.chromium.launch(headless=True, channel="chrome", args=args)
    except Exception:
        return await p.chromium.launch(headless=True, args=args)


async def new_page(p):
    browser = await launch(p)
    ctx = await browser.new_context(
        locale="en-US", viewport={"width": 1500, "height": 950},
        user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/138.0.0.0 Safari/537.36"))

    async def route(r):
        if any(k in r.request.url for k in BLOCK):
            return await r.abort()
        await r.continue_()

    await ctx.route("**/*", route)
    await ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
    return browser, await ctx.new_page()


CARD_JS = """() => Array.from(document.querySelectorAll('a[href*="/stream/extra/"]')).map(a=>{
    const h3=a.querySelector('h3');
    const sp=a.querySelector('span[class*=text-amber]');
    const time=[...a.querySelectorAll('span')].map(s=>s.textContent.trim())
        .find(t=>/^\\d+\\/\\d+\\/\\d+\\s+\\d+:\\d+\\s*(AM|PM)$/.test(t))||'';
    return {href:a.getAttribute('href'), title:h3?h3.textContent.trim():'',
            league:sp?sp.textContent.trim():'', time,
            live:((a.className||'')+'').includes('live-pulse')};
})"""


async def expand(pg):
    """The list seeds 50 cards; the rest load behind a 'SHOW N MORE' div."""
    last = 0
    # wait for the control to render
    for _ in range(30):
        has = await pg.evaluate("""() => [...document.querySelectorAll('button,a,div,span')]
            .some(e=>/SHOW\\s+\\d+\\s+MORE/i.test(e.textContent||'') && e.children.length<=1)""")
        if has:
            break
        await pg.wait_for_timeout(500)
    print("  expand: control visible =", has, flush=True)
    for _ in range(20):
        n = await pg.evaluate("document.querySelectorAll('a[href*=\"/stream/extra/\"]').length")
        clicked = await pg.evaluate("""() => {
            const el=[...document.querySelectorAll('button,a,div,span')]
                .find(e=>/SHOW\\s+\\d+\\s+MORE/i.test(e.textContent||'') && e.children.length<=1);
            if(!el) return false;
            el.scrollIntoView({block:'center'});
            el.click();
            return true;
        }""")
        if not clicked:
            break
        await pg.wait_for_timeout(2500)
        now = await pg.evaluate("document.querySelectorAll('a[href*=\"/stream/extra/\"]').length")
        if now == n and now == last:
            break
        last = now
    await pg.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await pg.wait_for_timeout(1500)


async def collect_cards(pg):
    """Expand + scroll the infinite feed until no new cards appear."""
    await pg.goto(BASE + "/extra", wait_until="domcontentloaded", timeout=45000)
    await pg.wait_for_timeout(7000)
    found = {}
    for _round in range(25):
        before = len(found)
        sh = await pg.evaluate("document.body.scrollHeight")
        y = 0
        while y <= sh:
            await pg.evaluate(f"window.scrollTo(0,{y})")
            await pg.wait_for_timeout(220)
            for c in await pg.evaluate(CARD_JS):
                if c.get("href"):
                    found[c["href"]] = c
            sh = await pg.evaluate("document.body.scrollHeight")
            y += 900
        clicked = await pg.evaluate("""() => {
            const b=[...document.querySelectorAll('button')]
                .find(x=>/^\\s*show\\s+\\d+\\s+more\\s*$/i.test(x.textContent||''));
            if(!b) return false;
            b.scrollIntoView({block:'center'}); b.click(); return true;
        }""")
        if clicked:
            await pg.wait_for_timeout(3000)
        if len(found) == before and not clicked:
            break
    out = []
    for href, c in found.items():
        c["id"] = href.rsplit("/", 1)[-1]
        c["page"] = BASE + href
        out.append(c)
    return out


def load_existing():
    if OUT_JSON.exists():
        try:
            return {r["id"]: r for r in json.loads(OUT_JSON.read_text())}
        except Exception:
            pass
    return {}


def write_all(records):
    rows = sorted(records.values(), key=lambda r: (r.get("time") or "", r.get("title") or ""))
    OUT_JSON.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = ["#EXTM3U"]
    for r in rows:
        url = (r.get("m3u8") or [""])[0]
        if not url:
            continue
        name = (r.get("title") or "Event").replace(",", " ")
        lines.append(f'#EXTINF:-1 tvg-id="{r.get("id","")}" group-title="'
                     f'{(r.get("league") or "Extra").replace(chr(34), "")}",{name}')
        lines.append(url)
    OUT_M3U.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def scrape(args):
    records = load_existing()
    async with async_playwright() as p:
        browser, pg = await new_page(p)
        media = []
        pg.on("request", lambda r: media.append(r.url)
              if (".m3u8" in r.url or ".mpd" in r.url) else None)

        print("loading /extra ...", flush=True)
        cards = await collect_cards(pg)
        print(f"cards: {len(cards)}", flush=True)
        if args.cards_only:
            for c in cards:
                records.setdefault(c["id"], c)
            write_all(records)
            print(f"wrote {OUT_JSON} ({len(records)} records)", flush=True)
            await browser.close()
            return

        batch = cards[args.offset:args.offset + args.limit]
        print(f"batch: {args.offset}..{args.offset + len(batch)} of {len(cards)}", flush=True)
        for i, c in enumerate(batch, 1):
            rec = dict(records.get(c["id"], {}))
            rec.update({k: c[k] for k in ("id", "page", "title", "league", "time")})
            media.clear()
            try:
                await pg.goto(c["page"], wait_until="domcontentloaded", timeout=35000)
                await pg.wait_for_timeout(3500)
                txt = await pg.evaluate("document.body.innerText")
                m = re.search(r"\b(\d+)\s*/\s*(\d+)\b", txt or "")
                rec["source_count"] = int(m.group(2)) if m else 0
                clicked = False
                for sel in ("button:has-text('Play')", "text=Play"):
                    try:
                        await pg.click(sel, timeout=4500)
                        clicked = True
                        break
                    except Exception:
                        pass
                await pg.wait_for_timeout(4500)
                frames = await pg.evaluate(
                    "Array.from(document.querySelectorAll('iframe')).map(f=>f.src)")
                rec["embed"] = next((f for f in frames if "sc-chat-apple" not in f), "")
                rec["m3u8"] = sorted({u for u in media if "sc-chat" not in u})
                rec["clicked"] = clicked
            except Exception as e:
                rec["error"] = str(e)[:120]
            records[rec["id"]] = rec
            write_all(records)  # checkpoint every card
            print(f"[{i}/{len(batch)}] {rec.get('league','')} | "
                  f"{(rec.get('title') or '')[:38]} | src={rec.get('source_count',0)} | "
                  f"embed={'Y' if rec.get('embed') else 'N'} | "
                  f"m3u8={len(rec.get('m3u8') or [])}", flush=True)
        await browser.close()
    print(f"done; {OUT_JSON} has {len(records)} records", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--cards-only", action="store_true")
    scrape(asyncio.run, ap.parse_args()) if False else asyncio.run(scrape(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
