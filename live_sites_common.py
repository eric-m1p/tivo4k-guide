"""Shared helpers for the extra live-stream sites (sportsurge/buffstreams/totalsporteki/roxie).

Every site module exposes ``fetch(limit=0) -> list[dict]`` returning feed dicts in the
same shape ``distill_events.py`` writes (canonical_key/title/.../sources).  The app
reads those dicts and treats ``.m3u8``/``.mpd`` URLs as native ExoPlayer sources and
everything else as an embed page in the WebView, so ``kind`` is informational.

No third-party deps: urllib + regex only.  Playwright is imported lazily by the
sites that need a rendered page.
"""

from __future__ import annotations

import concurrent.futures as cf
import gzip
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import streamcorner_api as sc  # noqa: E402  (Event/Source/canonical_key live there)

SITE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_CTX = __import__("ssl").create_default_context()
_CTX.check_hostname = False
_CTX.verify_mode = __import__("ssl").CERT_NONE

SPORT_ALIASES = {
    "nba": "NBA", "wnba": "WNBA", "nfl": "NFL", "nhl": "NHL", "mlb": "MLB",
    "cfb": "NCAAF", "ncaa": "NCAA", "ncaaf": "NCAAF", "soccer": "Soccer",
    "football": "Soccer", "boxing": "Boxing", "mma": "MMA", "ufc": "UFC",
    "wwe": "WWE", "f1": "F1", "motogp": "MotoGP", "tennis": "Tennis",
    "cricket": "Cricket", "rugby": "Rugby", "golf": "Golf", "motorsports": "Motorsports",
}


def now_sec() -> int:
    return int(time.time())


def get(url: str, referer: str = "", timeout: int = 25, retries: int = 2) -> str:
    """GET a page as text. Retries transient failures; returns '' on hard failure."""
    headers = {
        "User-Agent": SITE_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
    }
    if referer:
        headers["Referer"] = referer
    last = ""
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as r:
                data = r.read()
                if r.headers.get("Content-Encoding", "").lower() == "gzip":
                    data = gzip.decompress(data)
                return data.decode("utf-8", "replace")
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            last = str(exc)
            time.sleep(0.8 * (attempt + 1))
    if last:
        print(f"[sites] GET failed {url[:90]}: {last[:80]}", file=sys.stderr)
    return ""


def get_many(urls: list[str], workers: int = 8, referer: str = "") -> dict[str, str]:
    """Fetch many pages concurrently -> {url: html}."""
    out: dict[str, str] = {}
    if not urls:
        return out
    with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        fut = {ex.submit(get, u, referer): u for u in dict.fromkeys(urls)}
        for f in cf.as_completed(fut):
            out[fut[f]] = f.result() or ""
    return out


# The app groups by the same category words the StreamCorner feed already uses, so map the
# site-native sport labels ("NBA", "NCAAF", …) onto them instead of shipping "Nba"/"Ncaaf".
APP_CATEGORIES = {
    "NBA": "Basketball", "WNBA": "Basketball", "NFL": "American Football",
    "NCAAF": "American Football", "NCAA": "American Football", "NHL": "Ice Hockey",
    "MLB": "Baseball", "UFC": "Fighting", "MMA": "Fighting", "BOXING": "Fighting",
    "WWE": "Wrestling", "F1": "Motorsports", "MOTOGP": "Motorsports",
    "SOCCER": "Football", "FOOTBALL": "Football", "TENNIS": "Tennis",
    "CRICKET": "Cricket", "RUGBY": "Rugby", "GOLF": "Golf", "MOTORSPORTS": "Motorsports",
}


def app_category(raw: str) -> str:
    key = (raw or "").strip().upper()
    if key in APP_CATEGORIES:
        return APP_CATEGORIES[key]
    return sc.pretty_category(raw or "Sports")


def source(origin: str, label: str, url: str, kind: str = "embed", referer: str = "") -> sc.Source:
    return sc.Source(origin=origin.upper(), label=label or "Source", url=url, kind=kind, referer=referer)


def sport_label(raw: str) -> str:
    key = re.sub(r"[^a-z0-9]", "", (raw or "").lower())
    return SPORT_ALIASES.get(key, (raw or "Sports").title())


def status_for(starts_at: int, ends_at: int, when: int) -> str:
    if starts_at <= 0:
        return "live"
    if when < starts_at:
        return "future"
    if ends_at and when <= ends_at:
        return "live"
    return "ended"


def sport_duration(category: str) -> int:
    c = (category or "").lower()
    if any(k in c for k in ("nfl", "ncaaf", "cfb", "american football")):
        return 4 * 3600
    if "nba" in c or "nhl" in c or "wnba" in c:
        return 3 * 3600
    if "mlb" in c:
        return 3 * 3600
    if "soccer" in c:
        return 150 * 60
    if "ufc" in c or "mma" in c or "boxing" in c or "wwe" in c:
        return 3 * 3600
    return 3 * 3600


def build_event(
    origin: str,
    title: str,
    category: str,
    league: str,
    starts_at: int,
    sources: list[sc.Source],
    *,
    ends_at: int = 0,
    poster: str = "",
    score: str = "",
    always_live: bool = False,
    when: int | None = None,
) -> sc.Event | None:
    """Normalize one row; returns None when the fixture already finished."""
    when = when or now_sec()
    title = re.sub(r"\s+", " ", (title or "")).strip()
    if len(title) < 4:
        return None
    if ends_at <= 0 and starts_at > 0:
        ends_at = starts_at + sport_duration(category + " " + league)
    status = "always_live" if always_live else status_for(starts_at, ends_at, when)
    if status == "ended" and not always_live:
        return None
    key = sc.canonical_key("ev", title, starts_at, always_live, when)
    return sc.Event(
        canonical_key=key,
        title=title,
        category=app_category(category or "Sports"),
        league=league or "",
        status=status,
        starts_at=starts_at,
        ends_at=ends_at,
        time_label="24/7" if always_live else ("LIVE" if status == "live" else sc._future_label(starts_at, when)),
        score_summary=score or "",
        poster_url=poster or "",
        origins=[origin.upper()],
        sources=sources,
        updated_at=when,
    )


# One physical feed can be published by several sites under different URLs (the same
# gooz stream id, the same topstream player, the same HLS channel path on mirror hosts).
# Identity is the *feed*, never the event: two different fixtures pointing at the same
# channel are still one feed, while two events stay two events.
_IDENT_PATTERNS = (
    (r"gooz\.aapmains\.net/new-stream-embed/(\d+)", "gooz:{}"),
    (r"topstream\.help/(?:\?p=|source)([A-Za-z0-9_-]+)", "topstream:{}"),
    # stream-N.php clones on different hosts wrap different upstream players, so
    # only the original topstreamsz host is treated as one feed.
    (r"topstreamsz\.[a-z]+/tvplayer/stream-(\d+)\.php", "tvplayer:{}"),
    (r"tiestep\.top/e/([A-Za-z0-9_-]+)", "tiestep:{}"),
)


def stream_identity(url: str) -> str:
    """Stable key for the underlying feed, so mirror URLs collapse to one source."""
    raw = (url or "").strip()
    if not raw.lower().startswith(("http://", "https://")):
        return "raw:" + raw
    low = raw.lower()
    for pattern, fmt in _IDENT_PATTERNS:
        m = re.search(pattern, low)
        if m:
            return fmt.format(m.group(1))
    parts = urllib.parse.urlsplit(low)
    path = parts.path.rstrip("/")
    # Keep host and query: mirror hosts are the failover stack, and many players
    # pick the channel by query string (?id=31, ?ch=18, ?p=5582).
    ident = parts.netloc + path
    if parts.query:
        ident += "?" + parts.query
    return "url:" + ident


def dedupe_sources(sources: list[sc.Source]) -> list[sc.Source]:
    seen: set[str] = set()
    out: list[sc.Source] = []
    for s in sources:
        if not s.url or not s.url.startswith("http"):
            continue
        ident = stream_identity(s.url)
        if ident in seen:
            continue
        seen.add(ident)
        out.append(s)
    return out


def publish(events: list[sc.Event]) -> list[dict]:
    """Sort + drop empties, then emit the feed dict shape."""
    # Future cards may legitimately have no source yet (the distiller and app both
    # support that); everything else needs at least one playable URL.
    clean = [e for e in events if e and (e.sources or e.status == "future")]
    clean.sort(key=sc.sort_key)
    return [sc.to_feed_dict(e) for e in clean]


def et_to_epoch(text: str, default_day: datetime | None = None) -> int:
    """Parse the '2026-11-20 20:00ET' style stamps these sites print."""
    text = (text or "").strip()
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})\s*(ET|EST|EDT|UTC|PT|PST|PDT)?", text, re.I)
    if m:
        y, mo, d, hh, mm, tz = m.groups()
        dt = datetime(int(y), int(mo), int(d), int(hh), int(mm))
        zone = (tz or "ET").upper()
        fixed = {"EST": -5, "EDT": -4, "PST": -8, "PDT": -7, "UTC": 0}
        if zone in fixed:
            tzinfo = timezone(timedelta(hours=fixed[zone]))
        else:  # bare ET/PT follow daylight saving
            try:
                from zoneinfo import ZoneInfo
                tzinfo = ZoneInfo("America/Los_Angeles" if zone == "PT" else "America/New_York")
            except Exception:
                tzinfo = timezone(timedelta(hours=-8 if zone == "PT" else -5))
        return int(dt.replace(tzinfo=tzinfo).timestamp())
    if default_day:
        return int(default_day.replace(tzinfo=timezone.utc).timestamp())
    return 0


def dump(events: list[dict], origin: str, path: str = "") -> None:
    counts: dict[str, int] = {}
    for e in events:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    srcs = sum(len(e["sources"]) for e in events)
    print(f"[{origin}] {len(events)} events {counts}, {srcs} sources")
    for e in events[:8]:
        print(f"  [{e['status']:11}] {e['time_label']:26} | {e['title'][:60]} ({len(e['sources'])} src)")
    if path:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"origin": origin, "total": len(events), "events": events}, f, indent=1)
        print(f"  -> {path}")
