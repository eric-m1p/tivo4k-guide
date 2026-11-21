#!/usr/bin/env python3
"""roxiestreams.info extractor.

The site is a small server-rendered Bootstrap listing site (one static HTML file per
fixture).  Category pages (``/soccer``, ``/nba``, ``/nfl`` ...) contain a plain table::

    <tr>
      <td><a href="https://roxiestreams.info/nba-streams-1">Charlotte Hornets vs Atlanta Hawks</a></td>
      <td class="event-start-time">November 20, 2026 4:00 PM</td>
      <td><span class="countdown-timer"></span></td>
    </tr>

The countdown is filled client-side, so the *only* server-side time signal is that
``event-start-time`` string -- it carries no timezone at all.  The column header even
says "Start Time (Local)", i.e. it is baked into the static HTML from the operator's own
clock.  We therefore parse it as **US Pacific** (see ``DEFAULT_TZ_OFFSET``; the Bundesliga /
NBA / NHL kickoffs only line up under PT), keep that
assumption overridable with ``--tz-offset``, and soften the ended-check with
``TZ_FUZZ_SEC`` so a 3h zone error cannot hide a live game.

Each fixture page is a bare player shell: no heading, no date, only stream buttons::

    <button class="streambutton"
            onclick="showPlayer('clappr', getRandomStream('nba.m3u8', 'admin2'))">Stream 1</button>

and the inline helper (verbatim from the page)::

    var domains = []                                    // filled from domainsz77.txt
    function getRandomStream(streamPath, subdomain = 'admin2') {
      if (domains.length === 0)
        return `https://${subdomain}.shadow-ran.online/${streamPath}`;   // fallback
      const randomDomain = domains[Math.floor(Math.random() * domains.length)];
      return `https://${subdomain}.${randomDomain}/${streamPath}`;
    }

So the real media URL is ``https://<sub>.<domain-from-domainsz77.txt>/<path>``; we
expand *every* listed domain instead of picking one at random (the whole point is to
give the player failover mirrors), and fall back to ``shadow-ran.online`` only when the
domain file is empty.  ``<sub>`` varies per page (``admin2``, ``tedesco``, ..., and
occasionally ``admin``) and is read out of each call, defaulting to ``admin2`` for the
one-argument variant.  Note the ``shadow-ran.online`` fallback currently serves an HTML
landing page, not an HLS manifest, so it is only emitted when the pool is empty.

These are genuine HLS manifests (``application/vnd.apple.mpegurl``), so sources are
emitted with ``kind="hls"`` and the app plays them natively instead of in a WebView.

    python3 sites_roxiestreams.py --out /tmp/rxs.json
    python3 sites_roxiestreams.py --limit 20 --tz-offset -8   # if the site is PT after all
"""

from __future__ import annotations

import argparse
import html as html_mod
import re
import sys
from datetime import datetime, timedelta, timezone

import live_sites_common as C

BASE = "https://roxiestreams.info"
ORIGIN = "roxiestreams"
DOMAINS_FILE = "domainsz77.txt"
FALLBACK_DOMAIN = "shadow-ran.online"  # only used when domainsz77.txt yields nothing
DEFAULT_SUB = "admin2"
MAX_EVENTS = 150
WORKERS = 8

# The table timestamps ("November 20, 2026 4:00 PM") carry no zone and the page has no
# server-side clock anchor, so they are assumed to be **US Eastern (EST, UTC-5)** as the
# default.  Observed slate, however, fits US Pacific exactly (Bundesliga 11:30 AM, NBA
# 4:00/4:30/5:00 PM, NHL 10:00 AM/12:30 PM/3:00 PM all land on standard ET slots under
# -8), so --tz-offset -8 is worth trying if the feed looks shifted.
DEFAULT_TZ_OFFSET = -8  # the column is the site's local time; verified against CET kickoffs

# The timestamp zone is genuinely unknown, so an event is not declared "ended" until it
# has been over for this long.  A 3h zone error then never hides a live game (a 4:00 PM
# stamp read as ET instead of PT is only 3h off), while feeds still prune after ~6h.
TZ_FUZZ_SEC = 6 * 3600

# Real navigation categories.
CATEGORY_PAGES = ("soccer", "mlb", "nba", "nfl", "nhl", "fighting", "motorsports")
# Pages that look like categories but are actually single-fixture player shells linked
# from the tables above (kept as crawl seeds so a lone fixture is still picked up).
EXTRA_SEEDS = ("f1", "wwe-streams", "soccer-streams-7")

DATE_FORMATS = (
    "%B %d, %Y %I:%M %p",  # November 20, 2026 4:00 PM
    "%b %d, %Y %I:%M %p",  # Nov 20, 2026 3:00 PM
    "%B %d, %Y %H:%M",
    "%b %d, %Y %H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
)

_STREAM_CALL = re.compile(
    r"""getRandomStream\(\s*['"]([^'"]+)['"]\s*       # 1: media path, e.g. nba.m3u8
        (?:,\s*['"]([^'"]*)['"]\s*)?                  # 2: optional subdomain
        \)""",
    re.X,
)


def _text(fragment: str) -> str:
    """Strip tags/entities and collapse whitespace."""
    plain = re.sub(r"<[^>]+>", " ", fragment or "")
    return re.sub(r"\s+", " ", html_mod.unescape(plain)).strip()


def parse_date(raw: str, tz_offset: int = DEFAULT_TZ_OFFSET) -> int:
    """'November 20, 2026 4:00 PM' -> epoch seconds, or 0 when unparseable."""
    text = _text(raw)
    if not text:
        return 0
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
        except ValueError:
            continue
        tz = timezone(timedelta(hours=tz_offset))
        return int(dt.replace(tzinfo=tz).timestamp())
    return 0


def _domains() -> list[str]:
    """Fetch domainsz77.txt once per run -> clean host list (may be empty)."""
    raw = C.get(f"{BASE}/{DOMAINS_FILE}", referer=BASE + "/")
    hosts: list[str] = []
    for line in (raw or "").splitlines():
        host = line.strip().strip("'\"").strip()
        if not host or host.startswith("#"):
            continue
        host = re.sub(r"^[a-zA-Z]+://", "", host).split("/")[0].strip()
        if host and "." in host:
            hosts.append(host)
    hosts = list(dict.fromkeys(hosts))
    if not hosts:
        print(
            f"[{ORIGIN}] WARNING domainsz77.txt empty/unreachable; "
            f"falling back to {FALLBACK_DOMAIN}",
            file=sys.stderr,
        )
    return hosts


def _is_listing_url(url: str) -> bool:
    """True for the nav category pages themselves (`/nba`, `/nfl`, ...)."""
    path = url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if not path.startswith(BASE):
        return False
    tail = path[len(BASE):].strip("/")
    return tail in CATEGORY_PAGES or tail == ""


def _parse_rows(html: str, page_slug: str, tz_offset: int) -> tuple[list[dict], int]:
    """Every fixture row on a listing page -> ([{url,title,starts_at,sport,listed_under}], skipped)."""
    rows: list[dict] = []
    skipped = 0
    for chunk in re.split(r"(?i)<tr\b", html)[1:]:
        href = re.search(r"""(?i)href=["'](https?://[^"']*/[^"'#]+)["']""", chunk)
        if not href:
            continue
        url = href.group(1).strip()
        if url.rstrip("/") == BASE:
            continue
        if _is_listing_url(url):
            # The homepage "featured" table links category pages alongside fixtures.
            continue
        # Title: the anchor text right after that href (the Event column).
        title_m = re.search(
            r"""href=["']""" + re.escape(url) + r"""["'][^>]*>(.*?)</a>""", chunk, re.S | re.I
        )
        title = _text(title_m.group(1)) if title_m else ""
        date_m = re.search(
            r"""(?i)class=["'][^"']*event-start-time[^"']*["'][^>]*>(.*?)</td>""", chunk, re.S
        )
        if not date_m:
            continue  # header/footer chunk, not a fixture row
        starts_at = parse_date(date_m.group(1), tz_offset)
        if not title or not starts_at:
            # A row without a usable title/time is skipped individually -- never the run.
            skipped += 1
            continue
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        sport = re.split(r"-streams?", slug, maxsplit=1)[0] or page_slug
        rows.append(
            {
                "url": url,
                "title": title,
                "starts_at": starts_at,
                "sport": sport,
                "listed_under": page_slug,
            }
        )
    return rows, skipped


def _stream_calls(html: str) -> list[tuple[str, str]]:
    """All distinct (path, subdomain) pairs in getRandomStream(...) calls.

    The function *definition* is naturally excluded because its first argument is not
    quoted.  Both ``getRandomStream('nba.m3u8', 'admin2')`` and the auto-start
    ``getRandomStream('nba.m3u8')`` (subdomain defaults to admin2) are matched.
    """
    calls: list[tuple[str, str]] = []
    for m in _STREAM_CALL.finditer(html or ""):
        path = (m.group(1) or "").strip()
        if not path:
            continue
        sub = (m.group(2) or "").strip() or DEFAULT_SUB
        pair = (path, sub)
        if pair not in calls:
            calls.append(pair)
    return calls


def _button_labels(html: str) -> dict[str, str]:
    """{'nba.m3u8': 'Stream 1'} from the stream buttons that wrap a call."""
    labels: dict[str, str] = {}
    pattern = r"(?is)<(button|a)\b[^>]*class=[\"'][^\"']*streambutton[^\"']*[\"'][^>]*>(.*?)</\1>"
    for bm in re.finditer(pattern, html or ""):
        tag, inner = bm.group(0), bm.group(2)
        label = _text(inner)
        for m in _STREAM_CALL.finditer(tag):
            path = (m.group(1) or "").strip()
            if path and label and path not in labels:
                labels[path] = label
    return labels


def _sources_for(html: str, domains: list[str]) -> list:
    """Expand every getRandomStream call over the domain pool -> hls sources."""
    calls = _stream_calls(html)
    labels = _button_labels(html)
    pool = domains or [FALLBACK_DOMAIN]
    subs = {sub for _, sub in calls}
    out = []
    for idx, (path, sub) in enumerate(calls, 1):
        base_label = labels.get(path) or f"Stream {idx}"
        if len(subs) > 1:  # same path on two subdomains -> keep the labels distinct
            base_label = f"{base_label} · {sub}"
        rel = path.lstrip("/")
        for domain in pool:
            url = f"https://{sub}.{domain}/{rel}"
            label = base_label if len(pool) == 1 else f"{base_label} · {domain}"
            ext = rel.rsplit("?", 1)[0].lower()
            kind = "hls" if ext.endswith((".m3u8", ".mpd")) else "embed"
            out.append(C.source(ORIGIN, label, url, kind, BASE + "/"))
    return out


def fetch(limit: int = 0, workers: int = WORKERS, tz_offset: int = DEFAULT_TZ_OFFSET) -> list[dict]:
    """Crawl listings -> fixture pages -> HLS source list, in feed-dict shape."""
    seeds = [BASE + "/"] + [f"{BASE}/{s}" for s in CATEGORY_PAGES + EXTRA_SEEDS]
    listing = C.get_many(seeds, workers=workers, referer=BASE + "/")

    fixtures: dict[str, dict] = {}
    skipped_dates = 0
    for url in seeds:
        slug = url.rstrip("/").rsplit("/", 1)[-1] or "home"
        page_html = listing.get(url) or ""
        if not page_html:
            continue
        rows, skipped = _parse_rows(page_html, slug, tz_offset)
        skipped_dates += skipped
        for row in rows:
            fixtures.setdefault(row["url"], row)  # first listing wins (rows repeat)

    domains = _domains()
    game_urls = list(fixtures)
    cap = limit or MAX_EVENTS
    game_urls = game_urls[:cap]
    print(
        f"[{ORIGIN}] {len(listing)} seeds -> {len(fixtures)} fixtures"
        f" ({len(game_urls)} capped, {skipped_dates} skipped for bad date),"
        f" domain pool={domains or [FALLBACK_DOMAIN + ' (fallback)']}",
        flush=True,
    )

    pages = C.get_many(game_urls, workers=workers, referer=BASE + "/")
    when = C.now_sec()
    events = []
    empty = 0
    for url in game_urls:
        row = fixtures[url]
        html = pages.get(url) or ""
        sources = C.dedupe_sources(_sources_for(html, domains)) if html else []
        if not sources:
            empty += 1
        category = C.sport_label(row["sport"])
        ends_at = row["starts_at"] + C.sport_duration(category) + TZ_FUZZ_SEC
        ev = C.build_event(
            ORIGIN,
            row["title"],
            category,
            category,
            row["starts_at"],
            sources,
            ends_at=ends_at,
            when=when,
        )
        if ev is None:  # already finished
            continue
        events.append(ev)

    live = sum(1 for e in events if e.status == "live")
    print(
        f"[{ORIGIN}] {len(events)} events kept ({live} live/{len(events) - live} future),"
        f" {empty} fixture pages without streams",
        flush=True,
    )
    # C.publish() sorts and keeps future fixtures even without a source (only finished /
    # empty non-future cards are dropped), so a not-yet-populated game page still shows.
    return C.publish(events)


def main() -> int:
    ap = argparse.ArgumentParser(description="roxiestreams.info live event extractor")
    ap.add_argument("--out", default="", help="write feed JSON here")
    ap.add_argument("--limit", type=int, default=0, help=f"max fixture pages (default {MAX_EVENTS})")
    ap.add_argument("--workers", type=int, default=WORKERS, help="concurrent fetches (default 8)")
    ap.add_argument(
        "--tz-offset",
        type=int,
        default=DEFAULT_TZ_OFFSET,
        help="UTC offset of the listing timestamps (default -5 = US Eastern)",
    )
    args = ap.parse_args()
    C.dump(fetch(limit=args.limit, workers=args.workers, tz_offset=args.tz_offset), ORIGIN, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
