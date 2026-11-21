#!/usr/bin/env python3
"""buffstreams (mybuffstreams.plus) extractor.

Same server-rendered sportsurge-family template as ``sites_isportsurge.py`` but a
different domain and a different URL/timing scheme:

  * Listing pages (``/nbastreams2``, ``/ncaastreams``, ``/soccer-live-streams``, ...)
    render one ``<a class="competition" title="Buffstreams A vs B" href="...">`` row
    per fixture.  ``<span class="name">`` holds the two sides and the middle cell is
    either ``<span class="competition-cell-status ...><b>In Progress</b></span>`` or
    ``<time datetime="2026-11-21 02:30:00">02:30 AM</time>``.
  * Boxing/MMA/WWE/motorsport rows use the compact variant
    ``<a class="d-block competition p-4" href="/title-game/<sport>/<slug>">``
    whose body is ``Title - 08:00 PM ET`` plus ``<small>1 hour ago</small>``.
  * Player pages are ``/<sport>/<slug>/<numeric-id>`` (or ``/title-game/...``) and
    carry ``<h1>Team A vs Team B</h1>``, one
    ``onclick="window.changeStream(60522)">Server1</div>`` per working mirror, and
    ``var countDownDate = 1795217400 * 1000`` (an exact UTC Unix start stamp).
    The page's own player does
    ``iframe.src = 'https://gooz.aapmains.net/new-stream-embed/' + streamId``,
    so the sources are plain gooz embed URLs — no browser needed.

Timing notes (verified against live pages, 2026-11-21):

  * ``countDownDate`` is the authoritative start (UTC epoch).  It is present on
    every player page, including rows the listing shows as ``In Progress``.
  * The listing ``<time datetime>`` attribute is **UTC wall clock**, not ET — it
    matches ``countDownDate`` to the second (e.g. ``2026-11-21 02:30:00`` <->
    ``1795228200``).  So it is parsed as UTC, *not* through ``C.et_to_epoch``.
  * Only the compact ``/title-game/`` rows print an explicit ``HH:MM AM ET``; those
    are converted assuming US Eastern (zoneinfo ``America/New_York`` when tzdata is
    available, otherwise a fixed -4:00 as the task suggests), then snapped to the
    ``N hours/days from now`` label on the row.

    python3 sites_buffstreams.py --out /tmp/bfs.json [--limit 200]
"""

from __future__ import annotations

import argparse
import calendar
import html as html_lib
import re
import sys
from datetime import datetime, timedelta, timezone

import live_sites_common as C
import streamcorner_api as sc

BASE = "https://mybuffstreams.plus"
EMBED = "https://gooz.aapmains.net/new-stream-embed/{}"
ORIGIN = "buffstreams"
MAX_EVENTS = 200  # polite cap on player-page fetches per run

# (listing path, category used when the URL's sport segment is not a known sport)
LISTING_PAGES: list[tuple[str, str]] = [
    ("/nbastreams2", "NBA"),
    ("/nflstreams2", "NFL"),
    ("/nhlstreams2", "NHL"),
    ("/mlb-live-streams", "MLB"),
    ("/cfbstreams2", "NCAAF"),
    ("/ncaastreams", "NCAA"),
    ("/wnbastreams", "WNBA"),
    ("/boxingstreams2", "Boxing"),
    ("/mmastreams2", "MMA"),
    ("/wwestreams", "WWE"),
    ("/f1streams2", "F1"),
    ("/soccer-live-streams", "Soccer"),
    ("/", ""),  # homepage already aggregates everything
]

# The soccer/racing listing URLs use the competition as the sport segment, so
# sport_label() cannot map them; keep a readable name for the league field.
EXTRA_LEAGUES = {
    "laliga": "La Liga",
    "seriea": "Serie A",
    "premierleague": "Premier League",
    "bundesliga": "Bundesliga",
    "ligue1": "Ligue 1",
    "ncaawomen": "NCAA Women",
    "ncaam": "NCAAM",
    "formula1": "Formula 1",
    "nascarcupseries": "NASCAR Cup Series",
}

# Segments that are not in C.SPORT_ALIASES but still deserve a real category.
CATEGORY_HINTS = {
    "ncaawomen": "NCAA Women",
    "formula1": "F1",
    "motogp": "Motorsports",
    "moto2": "Motorsports",
    "moto3": "Motorsports",
    "nascarcupseries": "Motorsports",
}

_ANCHOR_RE = re.compile(r"<a\b[^>]*\bclass=\"[^\"]*competition[^\"]*\"[^>]*>", re.I)
_TIME_RE = re.compile(r"<time\s+datetime=\"([^\"]+)\"[^>]*>([^<]*)</time>", re.I)
_NAME_RE = re.compile(r"class=\"name\">([^<]*)</span>", re.I)
_STATUS_RE = re.compile(r"competition-cell-status[^\"']*\"[^>]*>\s*<b>([^<]{0,32})</b>", re.I)
_REL_RE = re.compile(r"(\d+)\s*(second|minute|hour|day|week)s?\s+(ago|from now)", re.I)
_CLOCK_ET_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*(?:ET|EST|EDT)\b", re.I)
_PAGE_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


# --------------------------------------------------------------------------- helpers
def _attr(tag: str, name: str) -> str:
    m = re.search(r"\b%s=\"([^\"]*)\"" % re.escape(name), tag or "", re.I)
    return m.group(1).strip() if m else ""


def _text(html: str) -> str:
    plain = re.sub(r"<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", html_lib.unescape(plain)).strip()


def _et_tz():
    """US Eastern; falls back to a fixed -4:00 when tzdata is unavailable."""
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("America/New_York")
    except Exception:  # pragma: no cover - tzdata missing / zoneinfo absent
        return timezone(timedelta(hours=-4))


_ET = _et_tz()


def _et_epoch(y: int, mo: int, d: int, hh: int, mm: int) -> int:
    try:
        return int(datetime(y, mo, d, hh, mm, tzinfo=_ET).timestamp())
    except Exception:
        return 0


def _utc_epoch(stamp: str) -> int:
    """'2026-11-21 02:30:00' from a listing <time> is UTC wall clock."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})", stamp or "")
    if not m:
        return 0
    y, mo, d, hh, mm = (int(x) for x in m.groups())
    try:
        return calendar.timegm(datetime(y, mo, d, hh, mm).timetuple())
    except Exception:
        return 0


def _clean_title(text: str) -> str:
    """Strip the site's decorations so the same fixture keys identically everywhere."""
    title = _text(text)
    title = re.sub(r"^Buffstreams\s+", "", title, flags=re.I)
    title = re.sub(r"\s*[-|]?\s*Live Streams?( Links)?\s*$", "", title, flags=re.I)
    title = re.sub(r"\s*[-|]\s*Buffstreams.*$", "", title, flags=re.I)
    return re.sub(r"\s+", " ", title).strip()


def _sport_segment(url: str) -> str:
    path = url.split("mybuffstreams.plus/", 1)[-1].split("?", 1)[0].strip("/")
    parts = [p for p in path.split("/") if p]
    if not parts:
        return ""
    if parts[0] == "title-game" and len(parts) > 1:
        return parts[1]
    return parts[0]


def _league_label(segment: str) -> str:
    label = C.sport_label(segment)
    key = re.sub(r"[^a-z0-9]", "", (segment or "").lower())
    return EXTRA_LEAGUES.get(key, label)


def _row_clock_et(text: str) -> int:
    """'08:00 PM ET' -> seconds-of-day (0 when absent)."""
    m = _CLOCK_ET_RE.search(text or "")
    if not m:
        return 0
    hh, mm, ampm = int(m.group(1)) % 12, int(m.group(2)), m.group(3).upper()
    return (hh + (12 if ampm == "PM" else 0)) * 3600 + mm * 60


def _rel_delta(text: str) -> int:
    """'1 hour ago' / '6 days from now' -> seconds offset from now (0 if absent)."""
    m = _REL_RE.search(text or "")
    if not m:
        return 0
    n, unit, direction = int(m.group(1)), m.group(2).lower(), m.group(3).lower()
    scale = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}[unit]
    return -n * scale if direction == "ago" else n * scale


def _from_relative(clock_sod: int, rel: int, now: int) -> int:
    """Combine an ET clock time with the row's 'N hours from now' hint.

    Snap to the ET calendar day of (now + rel) and check the neighbours, keeping the
    candidate closest to the hinted instant — that kills the ±1 day rounding error.
    """
    if not clock_sod:
        return 0
    target = now + rel
    base = datetime.fromtimestamp(target, _ET)
    best, best_delta = 0, None
    for shift in (-1, 0, 1):
        day = base + timedelta(days=shift)
        cand = _et_epoch(day.year, day.month, day.day, clock_sod // 3600, (clock_sod % 3600) // 60)
        if not cand:
            continue
        diff = abs(cand - target)
        if best_delta is None or diff < best_delta:
            best, best_delta = cand, diff
    return best


# --------------------------------------------------------------------------- parsing
def _rows(html: str) -> list[dict]:
    """Every listing row on a page: title, url, status/date hints, order."""
    out: list[dict] = []
    for m in _ANCHOR_RE.finditer(html or ""):
        end = html.find("</a>", m.end())
        if end < 0:
            continue
        body = html[m.end():end]
        url = _attr(m.group(0), "href")
        if not url.startswith("http"):
            continue
        title = _attr(m.group(0), "title")
        tm = _TIME_RE.search(body)
        names = [_text(n) for n in _NAME_RE.findall(body)]
        status = (_text(_STATUS_RE.search(body).group(1)) if _STATUS_RE.search(body) else "")
        plain = _text(body)
        if not title:  # compact /title-game/ rows carry the title in the body text
            title = plain.split(" - ", 1)[0]
        if not names and " vs " in plain:
            first = plain.split(" - ", 1)[0]
            if " vs " in first:
                names = [first.split(" vs ", 1)[0].strip(), first.split(" vs ", 1)[1].strip()]
        out.append(
            {
                "url": url,
                "title": title,
                "names": names[:2],
                "status": status,
                "dt": tm.group(1).strip() if tm else "",
                "clock": tm.group(2).strip() if tm else "",
                "plain": plain,
                "rel": _rel_delta(plain),
                "live": "in progress" in (status or plain).lower(),
            }
        )
    return out


def _parse_player(html: str) -> dict:
    h1 = ""
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html or "", re.S | re.I)
    if m:
        h1 = _clean_title(m.group(1))

    sources = []
    for sm in re.finditer(r"changeStream\((\d+)\)[^>]*>\s*(?:Server\s*(\d+)|([^<]{0,24}))", html or ""):
        sid, num = sm.group(1), sm.group(2)
        label = f"Server {num}" if num else (_text(sm.group(3)) or "Server")
        sources.append(C.source(ORIGIN, label, EMBED.format(sid), "embed", BASE + "/"))
    if not sources:  # no mirror buttons: fall back to the preloaded iframe stream
        for i, sid in enumerate(dict.fromkeys(re.findall(r"new-stream-embed/(\d+)", html or "")), 1):
            sources.append(C.source(ORIGIN, f"Server {i}", EMBED.format(sid), "embed", BASE + "/"))

    cd = re.search(r"countDownDate\s*=\s*(\d+)\s*\*\s*1000", html or "")
    title_date = 0
    tm = re.search(r"<title>(.*?)</title>", html or "", re.S | re.I)
    if tm:
        dm = _PAGE_DATE_RE.search(tm.group(1))
        if dm:
            title_date = _et_epoch(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)), 0, 0)
    return {
        "h1": h1,
        "sources": sources,
        "countdown": int(cd.group(1)) if cd else 0,
        "page_date": title_date,
    }


def _starts_at(row: dict, player: dict, now: int) -> int:
    # 1) exact epoch on the player page (most reliable; UTC)
    if player["countdown"] > 0:
        return player["countdown"]
    # 2) listing <time datetime> — UTC wall clock, verified against countDownDate
    if row["dt"]:
        stamp = _utc_epoch(row["dt"])
        if stamp:
            return stamp
    # 3) explicit 'HH:MM AM ET' + 'N hours from now' (compact /title-game/ rows)
    clock = _row_clock_et(row["plain"])
    if clock:
        stamp = _from_relative(clock, row["rel"], now)
        if stamp:
            return stamp
        # 4) no relative hint: combine the player-page date with the listing time
        if player["page_date"]:
            day = datetime.fromtimestamp(player["page_date"], _ET)
            stamp = _et_epoch(day.year, day.month, day.day, clock // 3600, (clock % 3600) // 60)
            if stamp:
                return stamp
    # 5) nothing datable at all -> 0 (live/unknown, distiller sorts it as live)
    return 0


# --------------------------------------------------------------------------- fetch
def fetch(limit: int = 0, workers: int = 10) -> list[dict]:
    now = C.now_sec()
    listing_urls = [BASE + p for p, _ in LISTING_PAGES]
    pages = C.get_many(listing_urls, workers=8, referer=BASE + "/")

    group: dict[str, str] = dict(LISTING_PAGES)
    rows: list[dict] = []
    for url in listing_urls:
        html = pages.get(url, "")
        if not html:
            continue
        for row in _rows(html):
            row["fallback_category"] = group.get(url.split("mybuffstreams.plus", 1)[-1] or "/", "")
            rows.append(row)

    # one player page per canonical URL (homepage duplicates the listing pages)
    by_url: dict[str, dict] = {}
    for row in rows:
        key = row["url"].rstrip("/")
        prev = by_url.get(key)
        if prev is None:
            by_url[key] = row
            continue
        # prefer the richer row (a date/status beats nothing)
        if not prev["dt"] and not prev["live"] and (row["dt"] or row["live"]):
            by_url[key] = row
        elif row["names"] and not by_url[key]["names"]:
            by_url[key]["names"] = row["names"]
    cap = limit or MAX_EVENTS
    player_urls = list(by_url)[:cap]
    print(f"[buffstreams] {len(pages)} listing pages -> {len(rows)} rows -> {len(player_urls)} player pages", flush=True)

    fetched = C.get_many(player_urls, workers=workers, referer=BASE + "/")
    events: list[sc.Event] = []
    for url in player_urls:
        html = fetched.get(url, "")
        if not html:
            continue
        row = by_url[url]
        player = _parse_player(html)
        title = player["h1"] or _clean_title(row["title"])
        if not title and row["names"]:
            title = " vs ".join(row["names"])
        if len(title) < 4:
            continue
        segment = _sport_segment(url)
        seg_key = re.sub(r"[^a-z0-9]", "", segment.lower())
        label = C.sport_label(segment)
        # known sport segment -> that sport; competition-named segment (soccer league,
        # motorsport series) -> hint, else the listing page's sport.
        category = CATEGORY_HINTS.get(seg_key) or (
            label if seg_key in C.SPORT_ALIASES else (row.get("fallback_category") or label or "Sports")
        )
        league = _league_label(segment)
        starts_at = _starts_at(row, player, now)
        sources = C.dedupe_sources(player["sources"])

        ev = C.build_event(ORIGIN, title, category, league, starts_at, sources, when=now)
        if ev is None:
            # a row the site still badges "In Progress" can outrun the duration guess;
            # keep it as a live card with an unknown start rather than losing it
            if not row["live"]:
                continue
            ev = C.build_event(ORIGIN, title, category, league, 0, sources, when=now)
            if ev is None:
                continue
        if row["live"] and ev.status == "live" and "n progress" not in ev.time_label:
            ev.time_label = (ev.time_label + " • In Progress").strip()
        events.append(ev)

    return _finalize(events)


def _finalize(events: list[sc.Event]) -> list[dict]:
    """Merge duplicate keys, then publish sourced events + keep unsourced future cards."""
    merged: dict[str, sc.Event] = {}
    for ev in events:
        prev = merged.get(ev.canonical_key)
        if prev is None:
            merged[ev.canonical_key] = ev
            continue
        prev.sources = C.dedupe_sources(prev.sources + ev.sources)
        if not prev.starts_at and ev.starts_at:
            prev.starts_at, prev.ends_at, prev.time_label = ev.starts_at, ev.ends_at, ev.time_label
    unique = list(merged.values())

    sourced = [e for e in unique if e.sources]
    feed = C.publish(sourced)  # sorted feed dicts, per the shared helper
    seen = {d["canonical_key"] for d in feed}
    # C.publish() drops source-less events; upcoming cards are still useful, so add
    # the ones that have not already been published (distiller accepts them).
    for ev in unique:
        if ev.sources or ev.canonical_key in seen or ev.status != "future":
            continue
        feed.append(sc.to_feed_dict(ev))
        seen.add(ev.canonical_key)
    rank = {"live": 0, "future": 1, "always_live": 2}
    feed.sort(key=lambda d: (rank.get(d["status"], 3), d["starts_at"], d["title"].lower()))
    return feed


def main() -> int:
    ap = argparse.ArgumentParser(description="mybuffstreams.plus live event extractor")
    ap.add_argument("--out", default="")
    ap.add_argument("--limit", type=int, default=0, help="max player pages to fetch")
    args = ap.parse_args()
    C.dump(fetch(limit=args.limit), ORIGIN, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
