#!/usr/bin/env python3
"""Cloud event & channel distiller for Stream Sites TV.

Pulls live, upcoming, and 24/7 embed streams from api.ppv.st and live score
summaries from streambuddy.cc, deduplicates by stream ID and canonical event
key, and writes a clean events.json feed consumed directly by the TV app.
"""

import argparse
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
    cleaned = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
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
                "tokens": tokens,
            }
        )
    return entries


def find_score(name: str, tag: str, starts_at: int, scores: list[dict]) -> str:
    if not scores or starts_at <= 0:
        return ""
    tokens = match_tokens(name)
    if len(tokens) < 2:
        return ""
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
    return best["summary"] if best else ""


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


def _merge_events(existing: dict, incoming: dict) -> dict:
    seen_urls = {src["url"]: src for src in existing["sources"]}
    merged_sources = list(existing["sources"])
    for src in incoming["sources"]:
        if src["url"] not in seen_urls:
            seen_urls[src["url"]] = src
            merged_sources.append(src)

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

    return sources


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

            always_live = bool(s.get("always_live"))
            starts_at = int(s.get("starts_at") or 0)
            ends_at = int(s.get("ends_at") or 0)
            status = effective_status(always_live, starts_at, ends_at, now_sec)
            if status == "ended":
                continue

            tag = (s.get("tag") or "").strip()
            category = (s.get("category_name") or cat_name).strip() or cat_name
            sources = _extract_sources(s, tag)
            if not sources:
                continue

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
                "league": tag or category,
                "status": status,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "starts_at_iso": starts_iso,
                "time_label": format_time_label(status, starts_at, now_sec),
                "score_summary": find_score(name, tag, starts_at, scores),
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
    events = sorted(
        merged.values(),
        key=lambda e: (
            rank.get(e["status"], 3),
            e["starts_at"] if e["status"] == "future" else 0,
            e["title"].lower(),
            e["canonical_key"],
        ),
    )
    return events, now_sec


def save_json(events: list[dict], now_sec: int, json_path: str) -> None:
    payload = {
        "synced_at": now_sec,
        "synced_at_iso": datetime.datetime.fromtimestamp(now_sec, datetime.timezone.utc).isoformat(),
        "total": len(events),
        "events": events,
    }
    tmp_path = f"{json_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
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
    args = parser.parse_args()

    events, now_sec = distill_once()
    if not events:
        print("Error: upstream feed returned 0 active events", file=sys.stderr)
        return 1
    if not args.dry_run:
        save_json(events, now_sec, args.json)
    print_summary(events, args.status, args.sport)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
