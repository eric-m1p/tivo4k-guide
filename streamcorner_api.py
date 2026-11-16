#!/usr/bin/env python3
"""StreamCorner provider API client (all feeds, no per-event page scraping).

Why this exists
---------------
StreamCorner's SPA does not fetch data with plain HTTP. It boots a module worker
(`assets/<hash>.js`), which POSTs a signed envelope to a pool of
`data.*.workers.dev/corner?p=<provider>` origins and decrypts the reply with the
per-session signature. Claude's `/extra` scraper drove 700 event pages in a
browser; this client instead instantiates the site's own worker on a static
same-origin page, so the worker performs the signed request and decryption for
us. It is the API path the endpoints doc described, without re-implementing the
cipher or the signature.

The same worker returns provider lists *and* provider detail (which contains
the `streams[].embed_url` values the player uses).
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BASE = "https://streamcorner.cloud"
MANIFEST_URL = BASE + "/manifest.json"
WORKER_ASSET = "Cp8qs7nj.js"  # content-hashed; override with --worker-asset

# Hosts seen serving /corner successfully. The app itself shuffles five per call;
# several public origins are frequently down, so give the worker a wide choice.
HOSTS = [
    "data.gigav.workers.dev",
    "data.yedmzoa.workers.dev",
    "data.ngagzipx.workers.dev",
    "data.miopks.workers.dev",
    "data.l0o1afmju0.workers.dev",
    "data.nibflolsi9.workers.dev",
    "data.5j181.workers.dev",
    "data.rim1043.workers.dev",
    "data.kuig2.workers.dev",
    "data.senbon001.workers.dev",
    "data.senbon001-2.workers.dev",
    "data.senbon002.workers.dev",
    "data.senbon003.workers.dev",
    "data.kageyoshi001.workers.dev",
    "data.silentbyte125.workers.dev",
    "data.stealthwolf798-69b.workers.dev",
    "data.redjoy256.workers.dev",
    "data.anonfox144.workers.dev",
    "data.cripw4lk000.workers.dev",
    "data.phamviet444.workers.dev",
    "data.kanghaerin444.workers.dev",
    "data.minjikim444.workers.dev",
    "data.leehyein444.workers.dev",
    "data.daniellemarsh444.workers.dev",
]

# provider id -> field that identifies one item in provider-detail
ID_FIELD = {
    "nba": "game_id",
    "wnba": "game_id",
}

# Providers whose cards are just events; `extra` is a dict of numbered buckets.
LIST_PROVIDERS = [
    "nfl",
    "nba",
    "unl",
    "alpha",
    "beta",
    "charlie",
    "001",
    "extra",
    "admin",
    "channels",
]

PROVIDER_NAMES = {
    "nfl": "NFL / CFB",
    "nba": "NBA",
    "wnba": "WNBA",
    "unl": "UEFA Nations League",
    "alpha": "Soccer ALPHA",
    "beta": "BETA",
    "charlie": "Soccer CHARLIE",
    "001": "Feed 001",
    "extra": "EXTRA",
    "admin": "24/7 Admin",
    "channels": "24/7 Channels",
}

CREATE_WORKER_JS = r"""
async ({asset, sig}) => {
  const w = new Worker(location.origin + '/assets/' + asset + '#' + sig,
                       {type: 'module', name: 'provider-data'});
  self.__scWorker = w;
  return true;
}
"""


class StreamCornerError(RuntimeError):
    pass


@dataclass
class Source:
    origin: str
    label: str
    url: str
    kind: str = "embed"
    referer: str = BASE + "/"


@dataclass
class Event:
    canonical_key: str
    title: str
    category: str
    league: str
    status: str
    starts_at: int
    ends_at: int
    time_label: str
    score_summary: str
    poster_url: str
    origins: list[str]
    sources: list[Source] = field(default_factory=list)
    updated_at: int = 0


class WorkerClient:
    """Owns one page and one provider-data worker on streamcorner.cloud."""

    def __init__(self, page, worker_asset: str = WORKER_ASSET):
        self.page = page
        self.worker_asset = worker_asset
        self._seq = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._sig = ""
        self._ready = False

    async def start(self) -> None:
        self._sig = "".join(random.choice("0123456789abcdef") for _ in range(32))
        await self.page.goto(MANIFEST_URL, wait_until="domcontentloaded", timeout=45000)
        await self.page.evaluate(CREATE_WORKER_JS, {"asset": self.worker_asset, "sig": self._sig})
        await self.page.evaluate(
            """() => {
                const w = self.__scWorker;
                self.__scOnMessage = (e) => {
                    const d = e.data || {};
                    if (d.requestId != null && self.__scPending && self.__scPending[d.requestId]) {
                        self.__scPending[d.requestId](d);
                        delete self.__scPending[d.requestId];
                    }
                };
                self.__scPending = {};
                w.addEventListener('message', self.__scOnMessage);
                self.__scPost = (payload) => w.postMessage(payload);
            }"""
        )
        # Expose a promise-based bridge that returns the worker's decoded message.
        await self.page.evaluate(
            """() => {
                self.__scCall = (payload) => new Promise((resolve, reject) => {
                    const id = payload.requestId;
                    self.__scPending[id] = resolve;
                    try { self.__scPost(payload); }
                    catch (e) { delete self.__scPending[id]; reject(e); }
                    setTimeout(() => {
                        if (self.__scPending[id]) { delete self.__scPending[id]; resolve({requestId:id, error:'timeout'}); }
                    }, 30000);
                });
            }"""
        )
        self._ready = True

    async def call(self, operation: str, provider: str, item_id: str | None = None) -> Any:
        if not self._ready:
            await self.start()
        self._seq += 1
        request_id = self._seq
        urls = []
        for host in random.sample(HOSTS, 5):
            url = f"https://{host}/corner?p={provider}"
            if item_id:
                url += "&id=" + item_id
            urls.append(url)
        payload = {
            "requestId": request_id,
            "operation": operation,
            "requestUrls": urls,
            "providerId": provider,
            "providerName": PROVIDER_NAMES.get(provider, provider.upper()),
            "_ed23bd83ad9570445752283e": self._sig,
        }
        reply = await self.page.evaluate("p => self.__scCall(p)", payload)
        if not isinstance(reply, dict):
            raise StreamCornerError(f"{provider}: invalid worker reply {reply!r}")
        if reply.get("error"):
            raise StreamCornerError(f"{provider}: {reply['error']}")
        data = reply.get("data")
        # provider-list replies are {requestId, data}; provider-detail replies too.
        if isinstance(data, dict) and "data" in data and set(data) <= {"requestId", "data", "error"}:
            data = data["data"]
        return data

    async def provider_list(self, provider: str) -> Any:
        return await self.call("provider-list", provider)

    async def provider_detail(self, provider: str, item_id: str) -> dict:
        data = await self.call("provider-detail", provider, item_id)
        if isinstance(data, dict) and "streams" not in data and "data" in data:
            data = data["data"]
        return data if isinstance(data, dict) else {}


# --------------------------------------------------------------------------- #
# normalization
# --------------------------------------------------------------------------- #
STOPWORDS = {
    "vs", "v", "at", "the", "fc", "cf", "sc", "afc", "ac", "cd", "fk",
    "live", "stream", "game", "match", "channel", "hd", "sd", "tv",
    "24", "7", "watch", "now",
}

TITLE_ALIASES = {
    "czech republic": "czech",
    "czechia": "czech",
    "türkiye": "turkey",
    "turkiye": "turkey",
    "bosnia and herzegovina": "bosnia",
    "bosnia-herzegovina": "bosnia",
    "north macedonia": "macedonia",
    "republic of ireland": "ireland",
    "cote d'ivoire": "ivory coast",
    "côte d'ivoire": "ivory coast",
    "korea republic": "south korea",
    "korea dpr": "north korea",
    "trinidad and tobago": "trinidad",
    "united states": "usa",
}


SPORT_DURATION_SEC = {
    "AMERICAN FOOTBALL": 4 * 3600,
    "NFL": 4 * 3600,
    "BASKETBALL": 3 * 3600,
    "NBA": 3 * 3600,
    "BASEBALL": 3 * 3600,
    "ICE HOCKEY": 3 * 3600,
    "NHL": 3 * 3600,
    "FOOTBALL": 2 * 3600 + 1800,
    "SOCCER": 2 * 3600 + 1800,
    "CRICKET": 5 * 3600,
    "TENNIS": 3 * 3600,
}


def match_tokens(text: str) -> list[str]:
    lowered = (text or "").lower()
    for alias, canonical in TITLE_ALIASES.items():
        if alias in lowered:
            lowered = lowered.replace(alias, canonical)
    cleaned = re.sub(r"[^a-z0-9 ]", " ", lowered)
    out = []
    for part in cleaned.split():
        if len(part) >= 2 and part not in STOPWORDS and part not in out:
            out.append(part)
    return out


def canonical_key(prefix: str, title: str, starts_at: int, always_live: bool, now_sec: int) -> str:
    tokens = match_tokens(title)
    if always_live:
        if tokens:
            return "ch:" + "_".join(tokens[:5])
        return f"{prefix}:{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')}"
    if len(tokens) >= 2:
        day_bucket = starts_at // 86400 if starts_at > 0 else now_sec // 86400
        return f"ev:{day_bucket}:{'_'.join(sorted(tokens)[:6])}"
    return f"{prefix}:{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')}"


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _iso_to_epoch(value: Any) -> int:
    if not value:
        return 0
    text = str(value).strip().replace("Z", "+00:00")
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    return 0


def event_start(item: dict) -> int:
    for key in ("start_time", "timestamp", "game_timestamp"):
        value = item.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return _iso_to_epoch(item.get("time_utc"))


def event_end(item: dict, start: int) -> int:
    value = item.get("end_time")
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    if start <= 0:
        return 0
    category = str(item.get("category") or "").upper()
    league = str(item.get("league") or "").upper()
    for needle, duration in SPORT_DURATION_SEC.items():
        if needle in category or needle in league:
            return start + duration
    return start + 3 * 3600


def is_247(item: dict) -> bool:
    text = " ".join(str(item.get(k) or "") for k in ("category", "league", "time_et", "event_name")).upper()
    if "24/7" in text:
        return True
    if str(item.get("time_et") or "").upper() == "LIVE" and event_start(item) <= 0:
        return True
    return False


def normalize_title(title: str) -> str:
    title = re.sub(r"^24/7\s+", "", (title or "").strip(), flags=re.I)
    fixes = {
        "spongebob squarepants": "SpongeBob SquarePants",
        "family guy": "Family Guy",
        "south park": "South Park",
        "the simpsons": "The Simpsons",
        "cnn": "CNN",
        "fox news": "Fox News",
    }
    return fixes.get(title.lower(), title)


CATEGORY_ALIASES = {
    "uefa nations": "Soccer",
    "fifa": "Soccer",
    "iihf world": "Ice Hockey",
    "hockey": "Ice Hockey",
    "croatia": "Volleyball",
    "poland": "Volleyball",
    "women": "Soccer",
    "sports essentials": "Sports",
    "others": "Other",
    "fighting": "Fighting",
    "mma": "Fighting",
    "boxing": "Fighting",
    "ufc": "Fighting",
}


def pretty_category(category: str) -> str:
    text = (category or "").strip()
    if not text:
        return "Sports"
    cleaned = " ".join(part.capitalize() for part in text.replace("_", " ").split())
    return CATEGORY_ALIASES.get(cleaned.lower(), cleaned)


def normalize_always_live(title: str, category: str, league: str) -> tuple[str, str, str]:
    title = normalize_title(title)
    low = title.lower()
    if low in {"family guy", "south park", "spongebob squarepants", "the simpsons", "cows"}:
        return title, "Entertainment", "24/7 Show"
    if low in {"cnn", "fox news"}:
        return title, "News", "News Network"
    if low in {"sky sports racing", "rally tv"}:
        return title, "Motorsports", "Sports Network"
    cat = pretty_category(category)
    if cat.upper() in {"24/7 STREAMS", "24/7 CHANNEL", "24/7"}:
        cat = "Entertainment"
    lg = (league or "").strip() or ("24/7 Channel" if cat == "Entertainment" else "Sports Network")
    return title, cat, lg


def score_summary(item: dict, provider: str) -> str:
    if provider != "nba":
        return ""
    home = str(item.get("home_team") or "").strip()
    away = str(item.get("away_team") or "").strip()
    hs = _as_int(item.get("home_score"))
    aw = _as_int(item.get("away_score"))
    label = str(item.get("time") or "").strip()
    if not home or not away:
        return ""
    if label.lower() == "final" or (hs or aw):
        return f"{away} {aw} - {hs} {home}"
    return f"{away} @ {home}"


def item_to_event(item: dict, provider: str, now_sec: int) -> Event | None:
    title = str(item.get("event_name") or "").strip()
    if not title:
        home = str(item.get("home_team") or "").strip()
        away = str(item.get("away_team") or "").strip()
        title = f"{away} at {home}".strip() if home or away else ""
    if not title:
        return None

    always_live = is_247(item)
    start = 0 if always_live else event_start(item)
    end = 0 if always_live else event_end(item, start)
    category = pretty_category(str(item.get("category") or ""))
    league = str(item.get("league") or "").strip() or category
    poster = str(item.get("poster") or item.get("hero_thumbnail") or "").strip()
    summary = score_summary(item, provider)

    if always_live:
        title, category, league = normalize_always_live(title, category, league)
        status = "always_live"
        time_label = "24/7"
    else:
        # NBA's `time` says Final for completed games.
        if str(item.get("time") or "").strip().lower() == "final" or "final" in summary.lower():
            return None
        if end > 0 and now_sec > end + 1800:
            return None
        status = "future" if start > now_sec else "live"
        time_label = "LIVE" if status == "live" else _future_label(start, now_sec)

    key = canonical_key(provider, title, start, always_live, now_sec)
    return Event(
        canonical_key=key,
        title=title,
        category=category,
        league=league,
        status=status,
        starts_at=start,
        ends_at=end,
        time_label=time_label,
        score_summary=summary,
        poster_url=poster,
        origins=[provider],
        updated_at=now_sec,
    )


def _future_label(starts_at: int, now_sec: int) -> str:
    if starts_at <= 0:
        return "Upcoming"
    dt = datetime.fromtimestamp(starts_at, timezone.utc)
    label = dt.strftime("%a %-I:%M %p UTC")
    delta = starts_at - now_sec
    if delta <= 0:
        return label
    mins = delta // 60
    if mins < 60:
        return f"{label} (in {mins}m)"
    hours, rem = divmod(mins, 60)
    if hours < 24:
        return f"{label} (in {hours}h {rem}m)"
    return label


def stream_sources(detail: dict, provider: str) -> list[Source]:
    out: list[Source] = []
    seen: set[str] = set()
    for entry in detail.get("streams") or []:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("embed_url") or entry.get("url") or "").strip()
        if not url.startswith("https://") or url in seen:
            continue
        seen.add(url)
        label = str(entry.get("source_name") or "Stream").strip() or "Stream"
        out.append(Source(
            origin=provider.upper(),
            label=label,
            url=url,
            kind="embed",
            referer=BASE + "/",
        ))
    return out


def list_items(provider: str, payload: Any) -> list[dict]:
    if provider == "extra" and isinstance(payload, dict):
        merged: list[dict] = []
        for bucket in sorted(payload):
            rows = payload.get(bucket)
            if isinstance(rows, list):
                merged.extend(r for r in rows if isinstance(r, dict))
        return merged
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [r for r in payload["data"] if isinstance(r, dict)]
    return []


def item_id(provider: str, item: dict) -> str:
    field = ID_FIELD.get(provider, "stream_id")
    return str(item.get(field) or item.get("stream_id") or item.get("game_id") or "").strip()


# --------------------------------------------------------------------------- #
# top-level fetch
# --------------------------------------------------------------------------- #
async def fetch_provider(client: WorkerClient, provider: str, detail_limit: int = 0) -> list[Event]:
    now_sec = int(time.time())
    payload = await client.provider_list(provider)
    pairs: list[tuple[Event, dict]] = []
    for item in list_items(provider, payload):
        event = item_to_event(item, provider, now_sec)
        if event is not None:
            pairs.append((event, item))

    if detail_limit:
        pairs = pairs[:detail_limit]
    sem = asyncio.Semaphore(8)

    async def _fetch_detail(event: Event, item: dict) -> None:
        ident = item_id(provider, item)
        if not ident:
            return
        async with sem:
            try:
                detail = await client.provider_detail(provider, ident)
            except Exception:
                return
        sources = stream_sources(detail, provider)
        if sources:
            event.sources = sources
            if detail.get("poster"):
                event.poster_url = str(detail["poster"]).strip() or event.poster_url

    await asyncio.gather(*[_fetch_detail(event, item) for event, item in pairs])
    return [event for event, _ in pairs]


async def fetch_all(
    providers: list[str] | None = None,
    concurrency: int = 4,
    worker_asset: str = WORKER_ASSET,
) -> dict[str, list[Event]]:
    from playwright.async_api import async_playwright

    providers = providers or LIST_PROVIDERS
    out: dict[str, list[Event]] = {}

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True, channel="chrome")
        except Exception:
            browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            locale="en-US",
            viewport={"width": 1280, "height": 800},
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/138.0.0.0 Safari/537.36"),
        )
        # The static manifest page keeps us same-origin without booting the SPA
        # (whose anti-devtool script navigates automation away).
        clients = []
        for _ in range(max(1, concurrency)):
            page = await ctx.new_page()
            client = WorkerClient(page, worker_asset=worker_asset)
            await client.start()
            clients.append(client)

        queue = asyncio.Queue()
        for provider in providers:
            queue.put_nowait(provider)

        async def worker(client: WorkerClient):
            while True:
                try:
                    provider = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    events = await fetch_provider(client, provider)
                    out[provider] = events
                    print(f"  {provider}: {len(events)} events", flush=True)
                except Exception as exc:
                    print(f"  {provider}: ERROR {exc}", flush=True)
                    out[provider] = []

        await asyncio.gather(*[worker(c) for c in clients])
        await browser.close()
    return out


def _absorb(existing: Event, event: Event) -> None:
    by_url = {s.url: s for s in existing.sources}
    for source in event.sources:
        by_url.setdefault(source.url, source)
    existing.sources = list(by_url.values())
    # Longer titles usually carry the round/match qualifier; keep that one.
    if len(event.title) > len(existing.title):
        existing.title = event.title
    if event.category and event.category.lower() not in {"", "sports", "other"}:
        existing.category = event.category
    if event.league and event.league.lower() not in {"", "sports", "other"}:
        existing.league = event.league
    if not existing.poster_url and event.poster_url:
        existing.poster_url = event.poster_url
    if event.score_summary:
        existing.score_summary = event.score_summary
    if event.starts_at and not existing.starts_at:
        existing.starts_at = event.starts_at
        existing.ends_at = event.ends_at
    origins = list(existing.origins)
    for origin in event.origins:
        if origin not in origins:
            origins.append(origin)
    existing.origins = origins
    # live wins over future; always_live wins over both
    rank = {"always_live": 3, "live": 2, "future": 1}
    if rank.get(event.status, 0) > rank.get(existing.status, 0):
        existing.status = event.status


def _title_tokens(event: Event) -> set[str]:
    return set(match_tokens(event.title))


def _same_match(a: Event, b: Event) -> bool:
    """Fuzzy de-dupe for the same fixture described differently by providers."""
    if a.status != b.status or a.status == "always_live":
        return False
    if not a.starts_at or not b.starts_at:
        return False
    if abs(a.starts_at - b.starts_at) > 3600:
        return False
    ta, tb = _title_tokens(a), _title_tokens(b)
    common = ta & tb
    if len(common) < 2:
        return False
    union = ta | tb
    return bool(union) and (len(common) / len(union)) >= 0.5


def merge_events(base: list[Event], extra: list[Event]) -> list[Event]:
    merged: dict[str, Event] = {}
    for event in list(base) + list(extra):
        existing = merged.get(event.canonical_key)
        if existing is None:
            merged[event.canonical_key] = event
        else:
            _absorb(existing, event)

    # Canonical keys miss translated/sponsored titles ("NBA Basketball: …",
    # "Baskonia vs Real Madrid Baloncesto", "#7 Texas vs. Baylor"). A second
    # pass merges same-time fixtures whose title tokens substantially overlap.
    kept: list[Event] = []
    for event in sorted(
        merged.values(),
        key=lambda e: (e.status, e.starts_at, len(e.sources)),
    ):
        match = next((candidate for candidate in kept if _same_match(candidate, event)), None)
        if match is None:
            kept.append(event)
        else:
            _absorb(match, event)
            if match.canonical_key != event.canonical_key:
                merged.pop(event.canonical_key, None)
    return kept


def sort_key(event: Event):
    rank = {"live": 0, "future": 1, "always_live": 2}
    r = rank.get(event.status, 3)
    if event.status == "future":
        return (r, event.starts_at, event.title.lower(), event.canonical_key)
    if event.status == "always_live":
        channel_rank = {"Sports Network": 0, "News Network": 1, "24/7 Show": 2}
        return (r, channel_rank.get(event.league, 3), event.title.lower(), event.canonical_key)
    return (r, 0, event.title.lower(), event.canonical_key)


def to_feed_dict(event: Event) -> dict:
    return {
        "canonical_key": event.canonical_key,
        "title": event.title,
        "category": event.category,
        "league": event.league,
        "status": event.status,
        "starts_at": event.starts_at,
        "ends_at": event.ends_at,
        "time_label": event.time_label,
        "score_summary": event.score_summary,
        "poster_url": event.poster_url,
        "origins": event.origins,
        "sources": [
            {
                "origin": s.origin,
                "label": s.label,
                "url": s.url,
                "kind": s.kind,
                "referer": s.referer,
            }
            for s in event.sources
        ],
        "updated_at": event.updated_at,
    }


async def _main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Fetch every StreamCorner provider feed via the site worker")
    ap.add_argument("--out", default="streamcorner_events.json")
    ap.add_argument("--providers", default=",".join(LIST_PROVIDERS))
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--worker-asset", default=WORKER_ASSET)
    ap.add_argument("--detail-limit", type=int, default=0, help="debug: only resolve N details per provider")
    args = ap.parse_args()

    providers = [p.strip() for p in args.providers.split(",") if p.strip()]
    print(f"fetching {len(providers)} providers via the StreamCorner worker ...", flush=True)
    result = await fetch_all(providers, concurrency=args.concurrency, worker_asset=args.worker_asset)
    events: list[Event] = []
    for rows in result.values():
        events.extend(rows)
    events = sorted(merge_events([], events), key=sort_key)
    now_sec = int(time.time())
    payload = {
        "synced_at": now_sec,
        "synced_at_iso": datetime.fromtimestamp(now_sec, timezone.utc).isoformat(),
        "total": len(events),
        "providers": {p: len(r) for p, r in result.items()},
        "events": [to_feed_dict(e) for e in events],
    }
    Path(args.out).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    counts: dict[str, int] = {}
    for event in events:
        counts[event.status] = counts.get(event.status, 0) + 1
    print(f"wrote {args.out}: {len(events)} events {counts}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
