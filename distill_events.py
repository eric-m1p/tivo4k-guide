#!/usr/bin/env python3
"""Cloud event & channel distiller for Stream Sites TV.

Pulls live, upcoming, and 24/7 embed streams from api.ppv.st and live score
summaries from streambuddy.cc, deduplicates by stream ID and canonical event
key, and writes a clean events.json feed consumed directly by the TV app.

With `--streamcorner` it also calls every StreamCorner provider feed through the
site's own provider-data worker (see streamcorner_api.py), then merges the two
catalogues. That covers the /extra cards as well as NFL, NBA, UEFA Nations
League, ALPHA, BETA, CHARLIE, #001, and the 24/7 admin/channel lists. Future
events are kept even before a source appears, so the app's Upcoming tab is not
empty; later runs fill in `streams[].embed_url` when the provider publishes it.
"""

import argparse
import functools
import asyncio
import datetime
import json
import os
import re
import sys
import time
import urllib.request

PPV_URL = "https://api.ppv.st/api/streams"
SCORES_URL = "https://www.streambuddy.cc/api/scores"
DEFAULT_REFERER = "https://www.streambuddy.cc/"

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


GENERIC_LABELS = {"", "stream", "sports", "other"}


def fetch_json(url: str, timeout: int = 15):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


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
            return f"ch:{'_'.join(tokens[:5])}"
        return f"{prefix}:{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')}"
    if len(tokens) >= 2:
        day_bucket = starts_at // 86400 if starts_at > 0 else now_sec // 86400
        return f"ev:{day_bucket}:{'_'.join(sorted(tokens)[:6])}"
    return f"{prefix}:{re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')}"


def effective_status(always_live: bool, starts_at: int, ends_at: int, now_sec: int) -> str:
    if always_live:
        return "always_live"
    if starts_at > 0:
        if ends_at > 0 and now_sec > ends_at + 1800:
            return "ended"
        if now_sec < starts_at:
            return "future"
        return "live"
    return "live"


def parse_scores(raw_scores) -> list[dict]:
    entries = []
    if not isinstance(raw_scores, list):
        return entries
    for obj in raw_scores:
        if not isinstance(obj, dict):
            continue
        teams = obj.get("teams") or []
        tokens = set()
        summary = ""
        if len(teams) >= 2:
            away, home = teams[0] or {}, teams[1] or {}
            a_abbr = (away.get("abbr") or "").strip()
            h_abbr = (home.get("abbr") or "").strip()
            tokens.update(match_tokens(away.get("name", "")))
            tokens.update(match_tokens(home.get("name", "")))
            if a_abbr:
                tokens.add(a_abbr.lower())
            if h_abbr:
                tokens.add(h_abbr.lower())
            status = obj.get("status", "")
            clock = (obj.get("clock") or "").strip()
            if status == "pre":
                a_rec = f" ({away['record']})" if away.get("record") else ""
                h_rec = f" ({home['record']})" if home.get("record") else ""
                summary = f"{a_abbr}{a_rec} @ {h_abbr}{h_rec}"
            else:
                summary = f"{a_abbr} {away.get('score', 0)} - {h_abbr} {home.get('score', 0)}"
            if clock:
                summary += f" • {clock}"
            entries.append(
                {
                    "league": (obj.get("league") or "").lower(),
                    "start_ms": int(obj.get("start") or 0),
                    "summary": summary,
                    "is_final": status.lower() == "post" or "final" in clock.lower(),
                    "tokens": tokens,
                }
            )
    return entries


def find_score(name: str, tag: str, starts_at: int, scores: list[dict]) -> tuple[str, bool]:
    if not scores or starts_at <= 0:
        return "", False
    tokens = match_tokens(name)
    if len(tokens) < 2:
        return "", False
    start_ms = starts_at * 1000
    low_tag = (tag or "").lower()
    best = None
    best_diff = 10**18
    for sc in scores:
        if low_tag and sc["league"] and sc["league"] not in low_tag:
            continue
        diff = abs(sc["start_ms"] - start_ms)
        if diff > 3 * 3600 * 1000:
            continue
        hits = sum(1 for t in tokens if t in sc["tokens"])
        if hits >= 2 and diff < best_diff:
            best = sc
            best_diff = diff
    if best:
        return best["summary"], bool(best.get("is_final"))
    return "", False


def format_time_label(status: str, starts_at: int, now_sec: int) -> str:
    if status == "always_live":
        return "24/7"
    if status == "live":
        return "LIVE"
    if starts_at <= 0:
        return "Upcoming"
    dt = datetime.datetime.fromtimestamp(starts_at, datetime.timezone.utc)
    clock = dt.strftime("%a %-I:%M %p UTC")
    delta = starts_at - now_sec
    if delta <= 0:
        return clock
    mins = delta // 60
    if mins < 60:
        return f"{clock} (in {mins}m)"
    hours, rem = divmod(mins, 60)
    if hours < 24:
        return f"{clock} (in {hours}h {rem}m)"
    return clock


def _is_generic(val: str) -> bool:
    return (val or "").strip().lower() in GENERIC_LABELS


def _source_quality_rank(src: dict) -> int:
    label = (src.get("label") or "").lower()
    url = (src.get("url") or "").lower()
    if (
        "_540p" in url
        or "_480p" in url
        or "_360p" in url
        or "540p" in label
        or "480p" in label
        or "360p" in label
        or label.startswith("sd")
        or " sd" in label
        or "sd |" in label
    ):
        return 3
    if any(k in label or k in url for k in ("4k", "uhd", "2160p", "1080p", "fhd")):
        return 0
    if (
        label.startswith("hd")
        or " hd" in label
        or "hd |" in label
        or "primary hd" in label
        or "720p" in label
        or "720p" in url
    ):
        return 1
    return 2


def _merge_events(existing: dict, incoming: dict) -> dict:
    seen_urls = {src["url"]: src for src in existing["sources"]}
    merged_sources = list(existing["sources"])
    for src in incoming["sources"]:
        if src["url"] not in seen_urls:
            seen_urls[src["url"]] = src
            merged_sources.append(src)
    merged_sources.sort(key=_source_quality_rank)

    if existing["status"] == "always_live" or incoming["status"] == "always_live":
        status = "always_live"
    elif existing["status"] == "live" or incoming["status"] == "live":
        status = "live"
    else:
        status = incoming["status"]

    starts_at = incoming["starts_at"] or existing["starts_at"]
    ends_at = incoming["ends_at"] or existing["ends_at"]
    category = incoming["category"] if not _is_generic(incoming["category"]) else existing["category"]
    league = incoming["league"] if not _is_generic(incoming["league"]) else existing["league"]

    return {
        "canonical_key": existing["canonical_key"],
        "title": incoming["title"] if len(incoming["title"]) >= len(existing["title"]) else existing["title"],
        "category": category,
        "league": league,
        "status": status,
        "starts_at": starts_at,
        "ends_at": ends_at,
        "starts_at_iso": incoming["starts_at_iso"] or existing["starts_at_iso"],
        "time_label": incoming["time_label"] or existing["time_label"],
        "score_summary": incoming["score_summary"] or existing["score_summary"],
        "poster_url": incoming["poster_url"] or existing["poster_url"],
        "origins": sorted(set(existing["origins"]) | set(incoming["origins"])),
        "sources": merged_sources,
        "updated_at": max(existing["updated_at"], incoming["updated_at"]),
    }


def _extract_sources(stream_obj: dict, tag: str) -> list[dict]:
    sources = []
    seen_urls = set()

    primary_url = (stream_obj.get("iframe") or "").strip()
    if primary_url:
        seen_urls.add(primary_url)
        primary_label = (stream_obj.get("source_tag") or "").strip()
        if not primary_label:
            primary_label = f"{tag} Primary" if tag else "Primary HD"
        sources.append(
            {
                "origin": "PPV",
                "label": primary_label,
                "url": primary_url,
                "kind": "embed",
                "referer": DEFAULT_REFERER,
            }
        )

    for idx, sub in enumerate(stream_obj.get("substreams") or []):
        if not isinstance(sub, dict):
            continue
        sub_url = (sub.get("iframe") or "").strip()
        if not sub_url or sub_url in seen_urls:
            continue
        seen_urls.add(sub_url)
        sub_label = (sub.get("source_tag") or "").strip() or f"Backup {idx + 1}"
        sources.append(
            {
                "origin": "PPV",
                "label": sub_label,
                "url": sub_url,
                "kind": "embed",
                "referer": DEFAULT_REFERER,
            }
        )

    sources.sort(key=_source_quality_rank)
    return sources


SHOWS_247 = {
    "family guy",
    "south park",
    "spongebob squarepants",
    "the simpsons",
    "cows",
}
NEWS_247 = {"cnn", "fox news"}
MOTOR_247 = {"sky sports racing", "rally tv"}
CHANNEL_RANK = {"Sports Network": 0, "News Network": 1, "24/7 Show": 2}


CHANNEL_TITLE_TOKENS = (
    "nfl network",
    "nba tv",
    "nhl network",
    "mlb network",
    "golf channel",
    "tennis channel",
    "sec network",
    "acc network",
    "big ten network",
    "cbs sports network",
    "fox cricket",
    "fox footy",
    "fox league",
    "sky sports ",
    "premier sports",
)


def is_channel_like_title(title: str) -> bool:
    low = (title or "").strip().lower()
    if not low or "redzone" in low or "red zone" in low:
        return False
    return any(tok in low for tok in CHANNEL_TITLE_TOKENS)


def normalize_always_live(title: str, category: str, league: str) -> tuple[str, str, str]:
    clean_title = re.sub(r"^24/7\s+", "", (title or "").strip(), flags=re.I).strip()
    low = clean_title.lower()
    if "nfl network" in low:
        return "NFL Network", "Sports", "Sports Network"
    if low == "spongebob squarepants":
        clean_title = "SpongeBob SquarePants"
    cat = (category or "").strip()
    lg = (league or "").strip()
    if low in SHOWS_247:
        return clean_title, "Entertainment", "24/7 Show"
    if low in NEWS_247:
        return clean_title, "News", "News Network"
    if low in MOTOR_247:
        return clean_title, "Motorsports", "Sports Network"
    if cat.lower() in ("", "24/7 streams", "24/7 channel", "24/7"):
        cat = "Sports"
    if lg.lower() in ("", "24/7 channel", "24/7 streams", "24/7"):
        lg = "Sports Network"
    return clean_title, cat, lg


def distill_once() -> tuple[list[dict], int]:
    wall_now = int(time.time())
    ppv = fetch_json(PPV_URL)
    feed_ts = int(ppv.get("timestamp") or wall_now)
    now_sec = max(wall_now, feed_ts) if abs(feed_ts - wall_now) > 300 else wall_now

    try:
        scores = parse_scores(fetch_json(SCORES_URL, timeout=10))
    except Exception:
        scores = []

    by_stream_id: dict[int, dict] = {}
    anonymous_records: list[dict] = []

    for cat_obj in ppv.get("streams") or []:
        if not isinstance(cat_obj, dict):
            continue
        cat_name = (cat_obj.get("category") or "Sports").strip() or "Sports"
        for s in cat_obj.get("streams") or []:
            if not isinstance(s, dict):
                continue
            name = (s.get("name") or "").strip()
            if not name:
                continue

            tag = (s.get("tag") or "").strip()
            category = (s.get("category_name") or cat_name).strip() or cat_name
            always_live = bool(s.get("always_live")) or (
                "24/7" in name.lower() or "24/7" in cat_name.lower() or "24/7" in tag.lower()
            )
            starts_at = int(s.get("starts_at") or 0)
            ends_at = int(s.get("ends_at") or 0)
            status = effective_status(always_live, starts_at, ends_at, now_sec)
            if status == "ended":
                continue

            score_summary, is_final = (
                ("", False) if always_live else find_score(name, tag, starts_at, scores)
            )
            if not always_live and (is_final or "final" in score_summary.lower()):
                continue

            sources = _extract_sources(s, tag)
            if not sources:
                continue

            league = tag or category
            if always_live:
                name, category, league = normalize_always_live(name, category, league)

            key = canonical_key("ppv", name, starts_at, always_live, now_sec)
            starts_iso = (
                datetime.datetime.fromtimestamp(starts_at, datetime.timezone.utc).isoformat()
                if starts_at > 0
                else None
            )
            record = {
                "canonical_key": key,
                "title": name,
                "category": category,
                "league": league,
                "status": status,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "starts_at_iso": starts_iso,
                "time_label": format_time_label(status, starts_at, now_sec),
                "score_summary": score_summary,
                "poster_url": (s.get("poster") or "").strip(),
                "origins": ["ppv"],
                "sources": sources,
                "updated_at": now_sec,
            }

            stream_id = int(s.get("id") or 0)
            if stream_id > 0:
                if stream_id in by_stream_id:
                    by_stream_id[stream_id] = _merge_events(by_stream_id[stream_id], record)
                else:
                    by_stream_id[stream_id] = record
            else:
                anonymous_records.append(record)

    merged: dict[str, dict] = {}
    for rec in list(by_stream_id.values()) + anonymous_records:
        key = rec["canonical_key"]
        if key in merged:
            merged[key] = _merge_events(merged[key], rec)
        else:
            merged[key] = rec

    rank = {"live": 0, "future": 1, "always_live": 2}

    def sort_key(e: dict):
        st = e["status"]
        r = rank.get(st, 3)
        if st == "future":
            return (r, 0, e["starts_at"], e["title"].lower(), e["canonical_key"])
        if st == "always_live":
            return (r, CHANNEL_RANK.get(e["league"], 3), 0, e["title"].lower(), e["canonical_key"])
        return (r, 0, 0, e["title"].lower(), e["canonical_key"])

    events = sorted(merged.values(), key=sort_key)
    return events, now_sec


def _event_from_feed_dict(record: dict, now_sec: int):
    import streamcorner_api as sc

    return sc.Event(
        canonical_key=record["canonical_key"],
        title=record.get("title", ""),
        category=sc.pretty_category(record.get("category", "Sports")),
        league=record.get("league", ""),
        status=record.get("status", "live"),
        starts_at=int(record.get("starts_at") or 0),
        ends_at=int(record.get("ends_at") or 0),
        time_label=record.get("time_label", ""),
        score_summary=record.get("score_summary", ""),
        poster_url=record.get("poster_url", ""),
        origins=list(record.get("origins") or []),
        sources=[
            sc.Source(
                origin=str(src.get("origin") or ""),
                label=str(src.get("label") or "Source"),
                url=str(src.get("url") or ""),
                kind=str(src.get("kind") or "embed"),
                referer=str(src.get("referer") or sc.BASE + "/"),
            )
            for src in (record.get("sources") or [])
            if src.get("url")
        ],
        updated_at=int(record.get("updated_at") or now_sec),
    )


def _feed_dict_from_event(event, now_sec: int) -> dict:
    starts_iso = (
        datetime.datetime.fromtimestamp(event.starts_at, datetime.timezone.utc).isoformat()
        if event.starts_at > 0
        else None
    )
    return {
        "canonical_key": event.canonical_key,
        "title": event.title,
        "category": event.category,
        "league": event.league,
        "status": event.status,
        "starts_at": event.starts_at,
        "ends_at": event.ends_at,
        "starts_at_iso": starts_iso,
        "time_label": event.time_label,
        "score_summary": event.score_summary,
        "poster_url": event.poster_url,
        "origins": event.origins,
        "sources": [
            {
                "origin": src.origin,
                "label": src.label,
                "url": src.url,
                "kind": src.kind,
                "referer": src.referer,
            }
            for src in event.sources
        ],
        "updated_at": event.updated_at or now_sec,
    }


def merge_streamcorner_feed(
    events: list[dict],
    now_sec: int,
    providers: list[str] | None = None,
    concurrency: int = 4,
    worker_asset: str | None = None,
) -> tuple[list[dict], dict]:
    """Fetch every StreamCorner provider and merge it into the PPV records."""
    import streamcorner_api as sc

    print(f"[extra] fetching StreamCorner providers (concurrency={concurrency}) ...", flush=True)
    result = asyncio.run(
        sc.fetch_all(
            providers=providers,
            concurrency=concurrency,
            worker_asset=worker_asset or None,  # None: discover from the live site
        )
    )
    sc_events = [event for rows in result.values() for event in rows]
    ppv_events = [_event_from_feed_dict(record, now_sec) for record in events]
    merged = sc.merge_events(ppv_events, sc_events)
    merged.sort(key=sc.sort_key)
    counts = {provider: len(rows) for provider, rows in result.items()}
    print(
        "[extra] " + ", ".join(f"{p}={n}" for p, n in counts.items())
        + f" -> {len(sc_events)} provider rows, {len(merged)} merged events",
        flush=True,
    )
    out_dicts = []
    for event in merged:
        if not any(ch.isalnum() for ch in (event.title or "")):
            continue
        if event.status != "always_live" and is_channel_like_title(event.title):
            event.status = "always_live"
            event.title, event.category, event.league = normalize_always_live(
                event.title, event.category, event.league
            )
            event.canonical_key = canonical_key("ev", event.title, event.starts_at, True, now_sec)
            event.time_label = "24/7"
        out_dicts.append(_feed_dict_from_event(event, now_sec))
    return out_dicts, counts


def merge_site_feed(
    events: list[dict],
    now_sec: int,
    sites: list[str],
    limit: int = 0,
) -> tuple[list[dict], dict]:
    """Fetch the extra live-stream sites (sportsurge/buffstreams/…) and merge them in.

    Each ``sites_<name>.py`` module exposes ``fetch(limit=0) -> list[dict]`` in the
    same feed shape this script writes; a failure in one site must never abort the
    rest of the refresh.
    """
    import importlib

    import streamcorner_api as sc

    site_events: list = []
    counts: dict[str, int] = {}
    for name in sites:
        name = name.strip().lower()
        if not name:
            continue
        try:
            module = importlib.import_module(f"sites_{name}")
            rows = module.fetch(limit=limit) or []
        except Exception as exc:  # noqa: BLE001 - one bad site must not break the feed
            print(f"[sites] {name}: FAILED ({exc})", file=sys.stderr)
            counts[name] = 0
            continue
        counts[name] = len(rows)
        site_events.extend(_event_from_feed_dict(record, now_sec) for record in rows)

    if not site_events:
        return events, counts

    ppv_events = [_event_from_feed_dict(record, now_sec) for record in events]
    merged = sc.merge_events(ppv_events, site_events)
    merged.sort(key=sc.sort_key)
    print(
        "[sites] " + ", ".join(f"{p}={n}" for p, n in counts.items())
        + f" -> {len(site_events)} site rows, {len(merged)} merged events",
        flush=True,
    )
    out_dicts = []
    for event in merged:
        if not any(ch.isalnum() for ch in (event.title or "")):
            continue
        if event.status != "always_live" and is_channel_like_title(event.title):
            event.status = "always_live"
            event.title, event.category, event.league = normalize_always_live(
                event.title, event.category, event.league
            )
            event.canonical_key = canonical_key("ev", event.title, event.starts_at, True, now_sec)
            event.time_label = "24/7"
        out_dicts.append(_feed_dict_from_event(event, now_sec))
    return out_dicts, counts


def dedupe_event_sources(events: list[dict]) -> list[dict]:
    """Drop per-event mirrors that resolve to the same feed.

    Different sites publish the same physical stream (same gooz id / topstream player /
    HLS channel) under different URLs.  Identity is the feed, not the event: events are
    never merged or dropped here, only the duplicate URLs inside each event's source list.
    """
    try:
        import live_sites_common as sites_common
        ident = sites_common.stream_identity
    except Exception:  # noqa: BLE001 - keep the feed building even without the helper
        def ident(url: str) -> str:
            return (url or "").split("?", 1)[0].rstrip("/").lower()

    removed = 0
    for record in events:
        seen: set[str] = set()
        kept: list[dict] = []
        for src in record.get("sources") or []:
            url = str(src.get("url") or "")
            key = ident(url)
            if not url or key in seen:
                removed += 1
                continue
            seen.add(key)
            kept.append(src)
        record["sources"] = kept
    if removed:
        print(f"[dedupe] dropped {removed} duplicate source URLs (same feed)", flush=True)
    return events


def save_json(events: list[dict], now_sec: int, json_path: str, policy: dict | None = None) -> None:
    """Write the v2 feed (events + sites/policy/config). Refuses to publish an invalid feed."""
    import feed_v2

    payload = feed_v2.finalize(events, now_sec, policy)
    errors = feed_v2.validate(payload)
    if errors:
        for err in errors[:20]:
            print(f"[schema] {err}", file=sys.stderr)
        raise SystemExit(f"events.json failed schema v{feed_v2.SCHEMA_VERSION} ({len(errors)} errors); not written")
    tmp_path = f"{json_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp_path, json_path)


def print_summary(events: list[dict], status_filter: str, sport_filter: str) -> None:
    counts = {"live": 0, "future": 0, "always_live": 0}
    filtered = []
    for e in events:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
        if status_filter != "all" and e["status"] != status_filter:
            continue
        if sport_filter:
            needle = sport_filter.lower()
            if needle not in e["category"].lower() and needle not in e["league"].lower():
                continue
        filtered.append(e)

    print(
        f"Distilled {len(events)} events "
        f"(live={counts['live']}, future={counts['future']}, always_live={counts['always_live']})"
    )
    for e in filtered[:25]:
        score = f" | {e['score_summary']}" if e["score_summary"] else ""
        print(f"  [{e['status']:11}] {e['time_label']:28} | {e['league']:14} | {e['title']} ({len(e['sources'])} src){score}")
    if len(filtered) > 25:
        print(f"  ... and {len(filtered) - 25} more")


def main() -> int:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description="Distill live/upcoming/24-7 embed events to events.json")
    parser.add_argument("--json", default=os.path.join(base_dir, "events.json"), help="Output JSON path")
    parser.add_argument("--status", default="all", choices=["all", "live", "future", "always_live"])
    parser.add_argument("--sport", default="")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing JSON")
    parser.add_argument(
        "--streamcorner",
        action="store_true",
        help="also fetch every StreamCorner provider feed and merge it in",
    )
    parser.add_argument(
        "--sites",
        default="",
        help=(
            "comma-separated extra live-stream sites to merge in: "
            "isportsurge,buffstreams,totalsporteki,roxiestreams"
        ),
    )
    parser.add_argument(
        "--sites-limit",
        type=int,
        default=0,
        help="cap how many events each extra site pulls (0 = site default)",
    )
    parser.add_argument(
        "--sc-providers",
        default="",
        help="comma-separated StreamCorner provider ids (default: all)",
    )
    parser.add_argument("--sc-concurrency", type=int, default=4, help="parallel StreamCorner workers")
    parser.add_argument("--sc-worker-asset", default="", help="override the site worker asset filename")
    parser.add_argument(
        "--probe-sources",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="probe source URLs before publishing and cap each event's list (default: on)",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=0,
        help="max sources kept per event (0 = no cap: ranked by measured quality, dead mirrors dropped)",
    )
    parser.add_argument(
        "--scoreboards",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="match ESPN scoreboards for scores, team logos and start times (default: on)",
    )
    parser.add_argument("--probe-budget", type=int, default=3000, help="URL cap for the legacy --probe-all path (the scheduler uses policy.probe.budget_per_hour)")
    parser.add_argument("--probe-workers", type=int, default=32)
    parser.add_argument(
        "--quality-budget",
        type=int,
        default=None,
        help="playback checks per run (0 = off; default: policy.probe.playback)",
    )
    parser.add_argument("--quality-workers", type=int, default=4)
    parser.add_argument("--quality-timeout", type=int, default=None, help="default: policy.probe.playback.timeout_sec")
    parser.add_argument(
        "--probe-all",
        action="store_true",
        help="legacy: probe every source up to --probe-budget instead of the policy.probe scheduler",
    )
    parser.add_argument(
        "--source-health",
        default=os.path.join(base_dir, "source_health.json"),
        help="persistent source health file",
    )
    parser.add_argument(
        "--source-blocklist",
        default=os.path.join(base_dir, "source_blocklist.json"),
        help='manual permanent denies: {"urls": {...}, "hosts": {...}}',
    )
    args = parser.parse_args()

    now_sec = int(time.time())
    try:
        events, now_sec = distill_once()
    except Exception as exc:
        if not args.streamcorner:
            raise
        print(f"Warning: PPV feed failed ({exc}); continuing with StreamCorner only", file=sys.stderr)
        events = []
    ppv_rows = len(events)

    sc_counts = None
    if args.streamcorner:
        providers = [p.strip() for p in args.sc_providers.split(",") if p.strip()] or None
        try:
            events, sc_counts = merge_streamcorner_feed(
                events,
                now_sec,
                providers=providers,
                concurrency=args.sc_concurrency,
                worker_asset=args.sc_worker_asset or None,
            )
        except Exception as exc:  # noqa: BLE001 - a broken site must not stop the feed; the canary sees 0 rows
            print(f"Warning: StreamCorner failed ({exc}); continuing without it", file=sys.stderr)
            sc_counts = {}

    site_counts = None
    if args.sites:
        sites = [s.strip() for s in args.sites.split(",") if s.strip()]
        events, site_counts = merge_site_feed(events, now_sec, sites, limit=args.sites_limit)

    events = dedupe_event_sources(events)

    try:
        import feed_v2
        try:
            import live_sites_common as sites_common
            ident = sites_common.stream_identity
        except Exception:  # noqa: BLE001
            ident = None
        events = feed_v2.enrich(
            events, now_sec, scoreboards=args.scoreboards, time_label=format_time_label, ident=ident
        )
    except Exception as exc:  # noqa: BLE001 - never lose the feed over the enrich pass
        print(f"Warning: v2 enrich pass failed ({exc}); publishing unmerged events", file=sys.stderr)

    if not events:
        print("Error: upstream feed returned 0 active events", file=sys.stderr)
        return 1

    import feed_v2
    policy = feed_v2.load_policy()
    probe_policy = policy["policy"].get("probe") or {}
    playback_policy = probe_policy.get("playback") or {}
    origin_site = feed_v2.site_index(policy)
    site_of: dict[str, str] = {}
    homes: dict[str, str] = {}
    for event in events:
        for src in event.get("sources") or []:
            url = str(src.get("url") or "").strip()
            origin = str(src.get("origin") or "").upper()
            site = origin_site.get(origin, origin.lower())
            site_of.setdefault(url, site)
            home = (policy["sites"].get(site) or {}).get("home") or src.get("referer") or ""
            homes.setdefault(url, home)

    do_probe = args.probe_sources and not args.dry_run
    try:
        import source_health
        if do_probe:
            try:
                tstats = source_health.ingest_telemetry_from_git(args.source_health, base_dir)
                if tstats.get("rows", 0) > 0:
                    print(
                        f"[health] telemetry: +{tstats['rows']} rows across {tstats['hosts']} hosts",
                        flush=True,
                    )
            except Exception as exc:
                print(f"Warning: telemetry ingest skipped ({exc})", file=sys.stderr)
        planner = None
        if not args.probe_all:
            import probe_scheduler
            planner = functools.partial(probe_scheduler.plan_probes, policy=probe_policy)
        quality_budget = args.quality_budget
        if quality_budget is None:
            enabled = playback_policy.get("enabled", True)
            quality_budget = int(playback_policy.get("budget_per_run", 40)) if enabled else 0
        health_stats = source_health.apply_source_health(
            events,
            args.source_health,
            max_sources=max(0, args.max_sources),
            probe_budget=max(0, args.probe_budget),
            do_probe=do_probe,
            workers=max(1, args.probe_workers),
            blocklist_path=args.source_blocklist,
            probe_all=args.probe_all,
            quality_budget=max(0, quality_budget),
            quality_workers=max(1, args.quality_workers),
            quality_timeout=max(2, args.quality_timeout or int(playback_policy.get("timeout_sec", 14))),
            planner=planner,
            homes=homes,
            site_of=site_of,
            playback_recheck_min=int(playback_policy.get("recheck_min", 90)),
        )
    except Exception as exc:
        print(f"Warning: source health pass failed ({exc}); publishing untrimmed sources", file=sys.stderr)
        health_stats = {}

    if not args.dry_run:
        try:
            import site_canary
            import source_health
            canary_policy = policy["policy"].get("canary") or {}
            state = source_health.load_state(args.source_health)
            rows = site_canary.site_rows(policy, ppv_rows, sc_counts, site_counts)
            alerts = site_canary.update(
                state, rows, health_stats.get("site_probe") or {},
                site_canary.app_by_site(state, site_of, now_sec), now_sec, canary_policy,
            )
            source_health.save_state(args.source_health, state, state.get("stats") or {})
            site_canary.overlay(policy, state)
            site_canary.send_alerts(
                alerts, state, canary_policy,
                {k: v.get("name", k) for k, v in policy["sites"].items()},
            )
        except Exception as exc:  # noqa: BLE001 - the canary must never block the feed
            print(f"Warning: site canary skipped ({exc})", file=sys.stderr)

    if not args.dry_run:
        save_json(events, now_sec, args.json, policy)
    print_summary(events, args.status, args.sport)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
