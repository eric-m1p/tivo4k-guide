#!/usr/bin/env python3
"""isportsurge.ws extractor.

The site is a server-rendered sportsurge template.  Category pages
(``/<sport>/livestreams<N>``) list one ``/event/<sport>/<slug>-live-streaming-links``
page per fixture, and each event page:

  * carries ``<h1>Team A vs Team B</h1>`` + an ``Event Information`` dl with
    ``Date: 2026-11-20 20:00ET`` and ``League: Boxing``;
  * renders one ``<div onclick="window.changeStream(60522)">Server1</div>`` per
    working mirror, and the page's own player does
    ``iframe.src = 'https://gooz.aapmains.net/new-stream-embed/' + streamId``.

So the source list is the set of gooz embed URLs, no browser required.

    python3 sites_isportsurge.py --out /tmp/iss.json
"""

from __future__ import annotations

import argparse
import re
import sys

import live_sites_common as C

BASE = "https://isportsurge.ws"
EMBED = "https://gooz.aapmains.net/new-stream-embed/{}"
ORIGIN = "isportsurge"
MAX_EVENTS = 320


def _event_urls(html: str) -> list[str]:
    """Both page families carry the player: /event/… for combat sports, /watch/… for leagues."""
    urls = re.findall(r'href="(https://isportsurge\.ws/(?:event|watch)/[^"]+)"', html)
    return list(dict.fromkeys(urls))


def _category_pages(html: str) -> list[str]:
    pages = re.findall(r'href="(https://isportsurge\.ws/[a-z0-9]+/livestreams\d*)"', html)
    return list(dict.fromkeys(pages))


def _parse_event(html: str, url: str) -> dict | None:
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S | re.I)
    if not m:
        return None
    title = re.sub(r"<[^>]+>|\s+", " ", m.group(1)).strip()
    sport = re.sub(r"^.*?/(?:event|watch)/", "", url).split("/", 1)[0]

    date_m = re.search(r"Date:\s*(?:</dt>)?\s*(?:<[^>]+>)*\s*(\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}\s*(?:ET|EST|EDT)?)", html, re.I)
    starts_at = C.et_to_epoch(date_m.group(1)) if date_m else 0
    if not starts_at:
        gm = re.search(r'dateTime="([^"]+)"', html)
        if gm:
            starts_at = C.et_to_epoch(gm.group(1))

    league_m = re.search(r"League:\s*(?:</dt>)?\s*(?:<[^>]+>)*\s*([^<]{2,60})", html, re.I)
    league = (league_m.group(1).strip() if league_m else "") or C.sport_label(sport)

    sources = []
    for sm in re.finditer(r'changeStream\((\d+)\)[^>]*>\s*(?:Server\s*(\d+)|([^<]{0,24}))', html):
        sid, num = sm.group(1), sm.group(2)
        label = f"Server {num}" if num else (sm.group(3).strip() or "Server")
        sources.append(C.source(ORIGIN, label, EMBED.format(sid), "embed", BASE + "/"))
    if not sources:  # some pages use data attributes instead of inline handlers
        for sm in re.finditer(r'data-stream(?:-id)?="(\d+)"', html):
            sources.append(C.source(ORIGIN, "Server", EMBED.format(sm.group(1)), "embed", BASE + "/"))

    return {
        "title": title,
        "sport": sport,
        "league": league,
        "starts_at": starts_at,
        "sources": C.dedupe_sources(sources),
        "url": url,
    }


def fetch(limit: int = 0, workers: int = 10) -> list[dict]:
    home = C.get(BASE + "/")
    pages = _category_pages(home) or [
        f"{BASE}/{s}/livestreams" for s in ("nba", "nfl", "nhl", "mlb", "cfb", "soccer", "ufc", "boxing", "wwe", "f1")
    ]
    event_urls = _event_urls(home)
    for html in C.get_many(pages, workers=workers, referer=BASE + "/").values():
        event_urls.extend(_event_urls(html))
    event_urls = list(dict.fromkeys(event_urls))
    cap = limit or MAX_EVENTS
    event_urls = event_urls[:cap]
    print(f"[isportsurge] {len(pages)} category pages -> {len(event_urls)} event pages", flush=True)

    rows = C.get_many(event_urls, workers=workers, referer=BASE + "/")
    when = C.now_sec()
    events = []
    for url, html in rows.items():
        if not html:
            continue
        row = _parse_event(html, url)
        if not row:
            continue
        badge = "In Progress" in html or "LIVE" in html[:8000]
        ev = C.build_event(
            ORIGIN,
            row["title"],
            C.sport_label(row["sport"]),
            row["league"],
            row["starts_at"],
            row["sources"],
            poster="",
            when=when,
        )
        if ev is None:
            continue
        if badge and ev.status == "live" and not ev.time_label.endswith("In Progress"):
            ev.time_label = (ev.time_label + " • In Progress").strip()
        events.append(ev)
    return C.publish(events)


def main() -> int:
    ap = argparse.ArgumentParser(description="isportsurge.ws live event extractor")
    ap.add_argument("--out", default="")
    ap.add_argument("--limit", type=int, default=0, help="max event pages to fetch")
    args = ap.parse_args()
    C.dump(fetch(limit=args.limit), ORIGIN, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
