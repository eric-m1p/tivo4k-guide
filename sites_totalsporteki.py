#!/usr/bin/env python3
"""totalsporteki.live extractor.

The site is a Next.js App Router build.  The homepage server-renders every fixture
as an ``<a class="game-card">`` with the UTC kickoff in ``<time dateTime>`` and a
``data-status`` of ``Live`` / ``Upcoming``.  Sources are *not* in the card: each
event page renders one or more ``Watch`` buttons whose React ``onClick`` does

    e.iframe_link ? window.open(e.streaming_link + "/stream/" + e.slug)
                  : window.open(e.streaming_link)

so the only reliable way to obtain the target URL is to invoke that prop in a real
browser.  (A synthetic ``.click()`` gets swallowed by the ad-overlay layer.)

Each opened URL points at ``https://topstream.help/?p=<id>``, which embeds a single
``<iframe src="https://topstreamsz.shop/tvplayer/stream-<n>.php">``; that iframe URL
is emitted as the embed source (falling back to the topstream URL when the page has
no iframe).

    python3 sites_totalsporteki.py --out /tmp/tsk.json --limit 10

Notes for maintainers:
  * kickoff stamps are naive UTC -- do NOT feed them through ``C.et_to_epoch``;
  * ``page.click()``/``el.click()`` is hijacked by ad overlays, call
    ``el[__reactProps*].onClick(...)`` instead;
  * ``topstream.help`` and the ``tiestep.top`` / ``topstreamsz.shop`` player hosts
    rotate; never hardcode them beyond the fallback;
  * the RSC payload (``self.__next_f``) also carries ``streaming_link`` values and is
    used only as a fallback when no Watch button yields a URL.
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import html as html_lib
import json
import os
import re
import sys
import urllib.parse
from datetime import datetime, timezone

import live_sites_common as C

BASE = "https://totalsporteki.live"
ORIGIN = "totalsporteki"
MAX_EVENTS = 40
CHROME = (
    "/Users/r00t/Library/Caches/ms-playwright/"
    "chromium_headless_shell-1248/chrome-headless-shell-mac-arm64/chrome-headless-shell"
)
CATEGORY_PAGES = (
    "soccer-streams", "nba-streams", "nfl-streams", "mlb-streams", "nhl-streams",
    "ufc-streams", "wwe-streams", "boxing-streams", "cricket-streams",
    "f1-streams", "motogp-streams", "tennis-streams", "rugby-streams",
)
LIVE_BADGES = {"live", "in progress", "inprogress", "playing"}
OPEN_BADGES = {"live", "upcoming", "scheduled", "not started", "in progress", "inprogress", "playing"}

CARD_RE = re.compile(
    r'<a\b[^>]*href="(https://totalsporteki\.live/[a-z0-9][a-z0-9-]*-\d{6,})"'
    r'[^>]*class="game-card"[^>]*>(.*?)</a>',
    re.S | re.I,
)
TEAM_RE = re.compile(r'class="game-team-name"[^>]*>([^<]*)<', re.I)
ALT_RE = re.compile(r'\balt="([^"]+)"', re.I)
STATUS_RE = re.compile(r'data-status="([^"]*)"', re.I)
TIME_RE = re.compile(r'dateTime="([^"]+)"', re.I)
IFRAME_RE = re.compile(r"<iframe\b[^>]*?\bsrc\s*=\s*([\"'])(.*?)\1", re.I | re.S)
RSC_LINK_RE = re.compile(r'"streaming_link":"(https?:[^"]+)"')

# The league badge in the JSON-LD ("sport") is a competition, not a sport.  Map the
# obvious ones and treat the rest of the football pyramid as Soccer.
LEAGUE_SPORT = {
    "nba": "NBA", "nfl": "NFL", "mlb": "MLB", "nhl": "NHL", "ufc": "UFC",
    "wwe": "WWE", "boxing": "Boxing", "f1": "F1", "formula1": "F1",
    "motogp": "MotoGP", "tennis": "Tennis", "cricket": "Cricket",
    "rugby": "Rugby", "soccer": "Soccer", "football": "Soccer", "mma": "MMA",
    "ncaaf": "NCAAF", "ncaab": "NCAAB", "wnba": "WNBA",
}
SOCCER_HINTS = (
    "league", "liga", "serie", "bundesliga", "ligue", "premier", "championship",
    "mls", "eredivisie", "primeira", "superlig", "cup", "copa", "uefa", "concacaf",
    "libertadores", "fa", "carabao", "trophy", "division",
)

CAPTURE_JS = r"""
window.__tskOpened = [];
(function () {
  const orig = window.open;
  window.open = function (u) {
    try { if (u) window.__tskOpened.push(String(u)); } catch (e) {}
    return null;  // never actually spawn tabs
  };
  window.__tskOrigOpen = orig;
})();
"""

# Fires every Watch button's React onClick directly (a real .click() is hijacked by
# the ad-overlay layer) and harvests popup URLs + per-row channel names.
# Re-arm the popup capture at evaluate time too: an ad script can reassign window.open
# after the init script runs, and a missed capture means zero sources for that event.
EXTRACT_JS = r"""
() => {
  const data = {opened: [], rsc: [], ld: [], title: "", watch: 0, props: 0, errors: 0, channels: []};
  data.title = (document.title || "").slice(0, 200);
  if (!Array.isArray(window.__tskOpened)) window.__tskOpened = [];
  window.open = function (u) {
    try { if (u) window.__tskOpened.push(String(u)); } catch (e) {}
    return null;
  };
  for (const s of document.querySelectorAll('script[type="application/ld+json"]')) {
    try { data.ld.push(s.textContent || ""); } catch (e) {}
  }
  try {
    const payload = (self.__next_f || []).map(p => (p && p[1]) || "").join("");
    const re = /"streaming_link":"(https?:[^"]+)"/g;
    let m;
    while ((m = re.exec(payload))) data.rsc.push(m[1].replace(/\\u0026/g, "&"));
  } catch (e) {}

  const btns = Array.from(document.querySelectorAll("button"))
    .filter(b => (b.textContent || "").trim() === "Watch");
  data.watch = btns.length;
  for (const b of btns) {
    let ch = "";
    try {
      const tr = b.closest("tr");
      const tds = tr ? tr.querySelectorAll("td") : [];
      if (tds.length > 1) ch = (tds[1].textContent || "").trim();
    } catch (e) {}
    data.channels.push(ch);
    const key = Object.keys(b).find(k => k.startsWith("__reactProps"));
    if (!key) continue;
    const props = b[key];
    if (!props || typeof props.onClick !== "function") continue;
    data.props += 1;
    try {
      props.onClick({preventDefault() {}, stopPropagation() {}, currentTarget: b, target: b});
    } catch (e) { data.errors += 1; }
  }
  data.opened = (window.__tskOpened || []).slice();
  return data;
}
"""


# --------------------------------------------------------------------------- utils


def _utc_epoch(text: str) -> int:
    """``2026-11-21T01:00`` (naive UTC on this site) -> epoch seconds."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})", text or "")
    if not m:
        return 0
    y, mo, d, hh, mm = (int(x) for x in m.groups())
    return int(datetime(y, mo, d, hh, mm, tzinfo=timezone.utc).timestamp())


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", html_lib.unescape(re.sub(r"<[^>]+>", " ", text or ""))).strip()


def _title_from(block: str) -> str:
    names = [_clean(n) for n in TEAM_RE.findall(block)]
    names = [n for n in names if n]
    if len(names) >= 2:
        return f"{names[0]} vs {names[1]}"
    if names:
        return names[0]
    alt = ALT_RE.search(block)
    return _clean(alt.group(1)) if alt else ""


def _cards(page_html: str) -> list[dict]:
    """Parse every ``game-card`` anchor; first occurrence of a URL wins."""
    seen: set[str] = set()
    out: list[dict] = []
    for m in CARD_RE.finditer(page_html):
        url, block = m.group(1), m.group(2)
        if url in seen:
            continue
        seen.add(url)
        title = _title_from(block)
        if len(title) < 4:
            continue
        st = STATUS_RE.search(block)
        tm = TIME_RE.search(block)
        starts_at = _utc_epoch(tm.group(1)) if tm else 0
        slug = url.rsplit("/", 1)[-1]
        out.append({
            "url": url,
            "slug": slug,
            "title": title,
            "starts_at": starts_at,
            "status": (st.group(1) if st else "").strip(),
        })
    return out


def _sport_for(league: str) -> str:
    key = re.sub(r"[^a-z0-9]", "", (league or "").lower())
    if key in LEAGUE_SPORT:
        return LEAGUE_SPORT[key]
    low = (league or "").lower()
    if any(h in low.replace("-", " ") for h in SOCCER_HINTS):
        return "Soccer"
    return C.sport_label(league) if league else "Sports"


def _first_iframe(page_html: str) -> str:
    for m in IFRAME_RE.finditer(page_html):
        src = html_lib.unescape(m.group(2) or "").strip()
        if not src or src.startswith(("about:", "javascript:", "data:", "blob:")):
            continue
        if src.startswith("//"):
            src = "https:" + src
        return urllib.parse.urljoin("https://topstream.help/", src)
    return ""


def _ld_event(blobs: list[str]) -> dict:
    """Pull the SportsEvent JSON-LD node (title, league, start, poster)."""
    for blob in blobs:
        try:
            rows = json.loads(blob)
        except (ValueError, TypeError):
            continue
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict) or row.get("@type") != "SportsEvent":
                continue
            image = row.get("image")
            if isinstance(image, list):
                image = image[0] if image else ""
            poster = image if isinstance(image, str) and "logo." not in image.lower() else ""
            return {
                "title": _clean(str(row.get("name") or "")),
                "league": _clean(str(row.get("sport") or "")),
                "starts_at": _utc_epoch(str(row.get("startDate") or "")),
                "poster": poster,
            }
    return {}


def _blocked(resource_type: str) -> bool:
    return resource_type in {"image", "font", "media", "stylesheet"}


def _chrome_path() -> str | None:
    """Prefer the pre-downloaded headless shell (bundled browser may be missing)."""
    if os.path.exists(CHROME):
        return CHROME
    hits = sorted(glob.glob(os.path.expanduser(
        "~/Library/Caches/ms-playwright/chromium_headless_shell-*/"
        "chrome-headless-shell-mac-*/chrome-headless-shell"
    )))
    if hits:
        return hits[-1]
    print(f"[{ORIGIN}] WARNING: {CHROME} not found; using Playwright's bundled Chromium",
          file=sys.stderr)
    return None


# ------------------------------------------------------------------------- browser


async def _render_pages(items: list[dict], concurrency: int, timeout_ms: int) -> dict[str, dict]:
    """Render event pages concurrently in ONE Chromium, harvesting popup URLs."""
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover - environment guard
        raise RuntimeError(f"playwright is required: {exc}") from exc

    results: dict[str, dict] = {}

    async def route_filter(route):
        try:
            if _blocked(route.request.resource_type):
                await route.abort()
            else:
                await route.continue_()
        except Exception:
            pass

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            executable_path=_chrome_path(),
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--mute-audio",
                "--disable-gpu",
            ],
        )
        try:
            sem = asyncio.Semaphore(max(1, concurrency))

            async def one(item: dict) -> None:
                url = item["url"]
                async with sem:
                    ctx = None
                    data: dict = {"ok": False, "opened": [], "rsc": [], "ld": [], "watch": 0}
                    try:
                        ctx = await browser.new_context(
                            user_agent=C.SITE_UA,
                            viewport={"width": 1280, "height": 900},
                            java_script_enabled=True,
                        )
                        await ctx.add_init_script(CAPTURE_JS)
                        page = await ctx.new_page()
                        page.set_default_timeout(timeout_ms)
                        await page.route("**/*", route_filter)
                        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                        try:
                            await page.wait_for_selector("button", timeout=6000)
                        except Exception:
                            pass
                        data = await page.evaluate(EXTRACT_JS) or data
                        data["ok"] = True
                    except Exception as exc:  # keep the run alive, count the failure
                        data["error"] = f"{type(exc).__name__}: {exc}"[:180]
                    finally:
                        if ctx is not None:
                            try:
                                await ctx.close()
                            except Exception:
                                pass
                    results[url] = data

            await asyncio.gather(*(one(i) for i in items))
        finally:
            try:
                await browser.close()
            except Exception:
                pass
    return results


# ----------------------------------------------------------------------------- fetch


def fetch(limit: int = 0, concurrency: int = 3, timeout: int = 45) -> list[dict]:
    when = C.now_sec()
    home = C.get(BASE + "/")
    cards = _cards(home)
    live_cards = [c for c in cards if c["status"].lower() in LIVE_BADGES]
    open_cards = [c for c in cards if c["status"].lower() in OPEN_BADGES]
    # A stale badge is still a hint: drop clearly finished cards that are not marked live.
    open_cards = [
        c for c in open_cards
        if c["status"].lower() in LIVE_BADGES or not c["starts_at"] or c["starts_at"] + 6 * 3600 >= when
    ]
    # Live first, then the next fixtures by kickoff.
    open_cards.sort(key=lambda c: (c["status"].lower() not in LIVE_BADGES, c["starts_at"] or 0))
    cap = limit if limit and limit > 0 else MAX_EVENTS
    # Only live cards expose streams: an upcoming page renders a placeholder Watch row
    # with no sources, so opening it just burns browser time (verified: 6 wasted pages).
    selected = [c for c in open_cards if c["status"].lower() in LIVE_BADGES][:cap]
    print(
        f"[{ORIGIN}] cards: {len(cards)} ({len(live_cards)} live, "
        f"{len(open_cards) - len(live_cards)} upcoming) -> opening {len(selected)} event pages",
        flush=True,
    )
    if not selected:
        return []

    rendered = asyncio.run(_render_pages(selected, concurrency, timeout * 1000))
    ok = sum(1 for d in rendered.values() if d.get("ok"))
    print(f"[{ORIGIN}] event pages rendered: {ok}/{len(selected)} "
          f"(failed {len(selected) - ok})", flush=True)

    # Per-event popup URLs (or the RSC fallback when no button yielded anything).
    pending: list[tuple[dict, list[str], str]] = []
    watch_total = 0
    for card in selected:
        data = rendered.get(card["url"], {})
        opened = [u for u in data.get("opened", []) if u.startswith("http")]
        if not opened:
            opened = [u for u in data.get("rsc", []) if u.startswith("http")]
            opened = [html_lib.unescape(u).replace("\\u0026", "&") for u in opened]
        opened = list(dict.fromkeys(opened))
        watch_total += len(data.get("opened", []))
        ld = data.get("ld") or []
        meta = _ld_event(ld) if ld else {}
        card["meta"] = meta
        pending.append((card, opened, card["url"]))
    print(f"[{ORIGIN}] watch urls: {watch_total}", flush=True)

    # Resolve every topstream page in one HTTP burst.
    all_links = [u for _, links, _ in pending for u in links]
    pages = C.get_many(all_links, workers=8, referer=BASE + "/")
    print(f"[{ORIGIN}] topstream pages fetched: "
          f"{sum(1 for v in pages.values() if v)}/{len(all_links)}", flush=True)

    events = []
    resolved = fallbacks = 0
    for card, links, referer in pending:
        meta = card.get("meta") or {}
        embeds: list[str] = []
        for link in links:
            embed = _first_iframe(pages.get(link, ""))
            if embed:
                resolved += 1
            else:
                embed = link  # no iframe -> use the topstream page itself
                fallbacks += 1
            embeds.append(embed)
        # Several stream rows often resolve to the same player URL: dedupe first so the
        # labels stay consecutive ("Stream 1..N") instead of leaving gaps.
        sources = [
            C.source(ORIGIN, f"Stream {idx}", url, "embed", BASE + "/")
            for idx, url in enumerate(dict.fromkeys(embeds), 1)
            if url.startswith("http")
        ]
        if not sources:
            continue

        league = meta.get("league") or ""
        sport = _sport_for(league)
        title = meta.get("title") or card["title"]
        starts_at = meta.get("starts_at") or card["starts_at"]
        is_live = card["status"].lower() in LIVE_BADGES
        # build_event drops anything past its duration; trust the site's LIVE badge and
        # extend the window so an over-running card is not silently discarded.
        ends_at = max(starts_at + C.sport_duration(f"{sport} {league}"), when + 3600) if is_live else 0
        ev = C.build_event(
            ORIGIN,
            title,
            sport,
            league or sport,
            starts_at,
            C.dedupe_sources(sources),
            ends_at=ends_at,
            poster=meta.get("poster") or "",
            when=when,
        )
        if ev is None:
            continue
        events.append(ev)

    print(f"[{ORIGIN}] iframes resolved: {resolved} (fallback {fallbacks})", flush=True)
    return C.publish(events)


def main() -> int:
    ap = argparse.ArgumentParser(description="totalsporteki.live live event extractor")
    ap.add_argument("--out", default="", help="write feed JSON here")
    ap.add_argument("--limit", type=int, default=0, help=f"max event pages to open (default {MAX_EVENTS})")
    ap.add_argument("--concurrency", type=int, default=3, help="parallel event pages (default 3)")
    ap.add_argument("--timeout", type=int, default=45, help="per-page timeout seconds (default 45)")
    args = ap.parse_args()
    C.dump(fetch(limit=args.limit, concurrency=args.concurrency, timeout=args.timeout), ORIGIN, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
