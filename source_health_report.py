#!/usr/bin/env python3
"""Probe the event feed and rack/stack the good sources.

This is the manual companion to the automated feed pass. It reads an existing
`events.json`, probes each event's mirrors (HTTP + latency), ranks the working
ones by quality then speed, and prints a per-event stack plus a host rack.

Examples:
    python3 source_health_report.py --feed guide-repo/events.json --limit 10
    python3 source_health_report.py --event "Lakers" --show-bad
    python3 source_health_report.py --status live --json /tmp/health.json
    python3 source_health_report.py --feed events.json --apply-out /tmp/events.probed.json
    python3 source_health_report.py --feed events.json --write-health

By default it does NOT modify `source_health.json` or the feed.
"""

from __future__ import annotations

import argparse
import datetime
import json
import statistics
import sys
import time
from pathlib import Path

import source_health as sh

STATUS_ORDER = {"live": 0, "future": 1, "always_live": 2}
QUALITY_LABEL = {0: "4K/1080p", 1: "HD/720p", 2: "unknown", 3: "SD"}


def load_feed(path: str | Path) -> tuple[dict, list[dict]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return {"events": raw}, raw
    if not isinstance(raw, dict) or not isinstance(raw.get("events"), list):
        raise SystemExit(f"{path}: expected an events.json object or a list of events")
    return raw, raw["events"]


def _source_label(src: dict) -> str:
    return str(src.get("label") or src.get("origin") or "Source").strip() or "Source"


def _short_url(url: str) -> str:
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.hostname or ""
        path = parsed.path.rstrip("/")
        if len(path) > 42:
            path = path[:39] + "..."
        return host + path
    except Exception:
        return url[:70]


def _sort_stack(rows: list[dict]) -> list[dict]:
    rank = {0: 0, 1: 1, 2: 2, 3: 3}
    return sorted(
        rows,
        key=lambda row: (
            0 if row["ok"] else 1,
            rank.get(row["quality"], 9),
            row["latency_ms"] if row["ok"] else 10**9,
            row["fails"],
            row["label"].lower(),
        ),
    )


def _host_rack(rows: list[dict]) -> list[dict]:
    by_host: dict[str, dict] = {}
    for row in rows:
        host = row["host"] or "(invalid)"
        bucket = by_host.setdefault(host, {"host": host, "probes": 0, "ok": 0, "fail": 0, "latencies": []})
        bucket["probes"] += 1
        if row["ok"]:
            bucket["ok"] += 1
            bucket["latencies"].append(row["latency_ms"])
        else:
            bucket["fail"] += 1
    out = []
    for bucket in by_host.values():
        latencies = bucket.pop("latencies")
        bucket["pass_rate"] = round(bucket["ok"] / bucket["probes"], 3) if bucket["probes"] else 0.0
        bucket["p50_ms"] = int(statistics.median(latencies)) if latencies else None
        bucket["avg_ms"] = int(sum(latencies) / len(latencies)) if latencies else None
        out.append(bucket)
    out.sort(key=lambda b: (-b["pass_rate"], b["p50_ms"] if b["p50_ms"] is not None else 10**9, -b["probes"], b["host"]))
    return out


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell).replace("|", "\\|") for cell in row) + " |")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe and rack/stack event sources")
    ap.add_argument("--feed", default="events.json", help="events.json to inspect")
    ap.add_argument("--status", default="live,future,always_live", help="comma-separated statuses")
    ap.add_argument("--event", default="", help="only events whose title contains this text")
    ap.add_argument("--limit", type=int, default=0, help="max events to probe (0 = all)")
    ap.add_argument("--show", type=int, default=12, help="events to print on the console")
    ap.add_argument("--pool", type=int, default=24, help="candidate sources examined per event")
    ap.add_argument("--probe-all", action="store_true", help="examine every source, ignoring --pool")
    ap.add_argument("--max-sources", type=int, default=8, help="healthy sources kept by --apply-out")
    ap.add_argument("--budget", type=int, default=2500, help="max unique URLs probed")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--timeout", type=int, default=8)
    ap.add_argument("--health", default="source_health.json", help="probe history file")
    ap.add_argument("--blocklist", default="source_blocklist.json", help="manual URL/host denies")
    ap.add_argument("--show-bad", action="store_true", help="include failed mirrors in the stack")
    ap.add_argument("--include-blocked", action="store_true", help="inspect quarantined/blocked mirrors")
    ap.add_argument("--write-health", action="store_true", help="record probe results in --health")
    ap.add_argument("--json", default="", help="write a machine-readable report")
    ap.add_argument("--markdown", default="", help="write a Markdown report")
    ap.add_argument("--apply-out", default="", help="write a reordered/trimmed feed (does not touch feed)")
    args = ap.parse_args()

    payload, events = load_feed(args.feed)
    wanted = {s.strip() for s in args.status.split(",") if s.strip()}
    now = int(time.time())

    feed_path = Path(args.feed)
    health_path = Path(args.health)
    blocklist_path = Path(args.blocklist)
    if args.health == "source_health.json" and feed_path.parent != Path("."):
        health_path = feed_path.with_name("source_health.json")
    if args.blocklist == "source_blocklist.json" and feed_path.parent != Path("."):
        blocklist_path = feed_path.with_name("source_blocklist.json")

    state = sh.load_state(health_path)
    entries = state.setdefault("sources", {})
    blocklist = sh.load_blocklist(blocklist_path)

    selected = []
    for event in events:
        if event.get("status") not in wanted:
            continue
        if args.event and args.event.lower() not in str(event.get("title", "")).lower():
            continue
        selected.append(event)
    selected.sort(key=lambda e: (STATUS_ORDER.get(e.get("status"), 9), int(e.get("starts_at") or 0), e.get("title", "")))
    if args.limit > 0:
        selected = selected[: args.limit]
    if not selected:
        print("No matching events.", file=sys.stderr)
        return 1

    candidates_by_event: list[list[dict]] = []
    unique: list[str] = []
    seen: set[str] = set()
    for event in selected:
        sources = sh.ranked_sources(
            event.get("sources") or [],
            state,
            now,
            include_disabled=args.include_blocked,
            blocklist=blocklist,
        )
        if not args.probe_all:
            sources = sources[: max(1, args.pool)]
        candidates_by_event.append(sources)
        for src in sources:
            url = str(src.get("url") or "").strip()
            if url and url not in seen and len(unique) < args.budget:
                seen.add(url)
                unique.append(url)

    print(f"probing {len(unique)} sources across {len(selected)} events ...", flush=True)
    results = sh.probe_sources_detailed(unique, workers=args.workers, timeout=args.timeout)

    if args.write_health:
        for url, result in results.items():
            sh._record(
                state, url, bool(result["ok"]), str(result["status"]), now,
                trust_failures=True,
                height=int(result.get("height") or 0),
                bitrate=int(result.get("bitrate") or 0),
                manifest=str(result.get("manifest") or ""),
            )
        sh.save_state(health_path, state, {
            "probed": len(results),
            "probe_ok": sum(1 for r in results.values() if r["ok"]),
            "probe_failed": sum(1 for r in results.values() if not r["ok"]),
            "trusted": True,
            "manual_report": True,
        })
        print(f"updated {health_path}", flush=True)

    event_reports = []
    flat_rows: list[dict] = []
    for event, sources in zip(selected, candidates_by_event):
        rows = []
        skipped = 0
        for src in sources:
            url = str(src.get("url") or "").strip()
            entry = entries.get(url, {})
            if sh.is_disabled(entry, now) and not args.include_blocked:
                skipped += 1
                continue
            probe = results.get(url)
            if probe is None:
                skipped += 1
                continue
            height = int(probe.get("height") or 0)
            quality_src = {"height": height, "label": src.get("label"), "url": url}
            rows.append({
                "label": _source_label(src),
                "url": url,
                "host": sh.host_of(url),
                "origin": str(src.get("origin") or ""),
                "referer": str(src.get("referer") or "https://streamcorner.cloud/"),
                "quality": sh.quality_rank(quality_src),
                "quality_text": sh._quality_label(height) or QUALITY_LABEL.get(sh.quality_rank(quality_src), "?"),
                "height": height,
                "bitrate": int(probe.get("bitrate") or 0),
                "verified": bool(probe.get("verified")),
                "manifest": str(probe.get("manifest") or ""),
                "ok": bool(probe["ok"]),
                "status": str(probe["status"]),
                "latency_ms": int(probe.get("latency_ms") or 0),
                "fails": int(entry.get("fail") or 0),
                "streak": int(entry.get("streak") or 0),
                "blocked": bool(entry.get("blocked")),
                "disabled": sh.is_disabled(entry, now),
                "probe_error": str(probe.get("error") or ""),
            })
        stack = _sort_stack(rows)
        for index, row in enumerate(stack, 1):
            row["rank"] = index
        flat_rows.extend(stack)
        healthy = [row for row in stack if row["ok"]]
        event_reports.append({
            "canonical_key": event.get("canonical_key", ""),
            "title": event.get("title", ""),
            "status": event.get("status", ""),
            "league": event.get("league", ""),
            "starts_at": event.get("starts_at", 0),
            "time_label": event.get("time_label", ""),
            "healthy": len(healthy),
            "failed": len(stack) - len(healthy),
            "skipped_blocked": skipped,
            "stack": stack,
        })

    hosts = _host_rack(flat_rows)
    probed = len(results)
    ok_count = sum(1 for result in results.values() if result["ok"])
    summary = {
        "generated_at": now,
        "generated_at_iso": datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat(),
        "feed": str(args.feed),
        "events": len(selected),
        "sources_examined": len(flat_rows),
        "probed": probed,
        "ok": ok_count,
        "failed": probed - ok_count,
        "pass_rate": round(ok_count / probed, 3) if probed else 0.0,
        "blocked_urls": sum(1 for entry in entries.values() if entry.get("blocked")),
        "manual_urls": len(blocklist.get("urls", set())),
        "manual_hosts": len(blocklist.get("hosts", set())),
    }

    print(
        f"events={summary['events']} sources={summary['sources_examined']} "
        f"probed={summary['probed']} ok={summary['ok']} failed={summary['failed']} "
        f"pass={summary['pass_rate']:.0%}"
    )
    print()
    for report in event_reports[: max(0, args.show)]:
        print(f"{report['status'].upper():11} {report['title']}  [{report['healthy']}/{report['healthy'] + report['failed']} healthy]")
        for row in report["stack"]:
            if not row["ok"] and not args.show_bad:
                continue
            mark = "OK  " if row["ok"] else "FAIL"
            latency = f"{row['latency_ms']:>5}ms" if row["ok"] else "     —"
            print(
                f"  #{row['rank']:<2} {mark} {row['quality_text']:9} "
                f"{latency}  {row['label'][:36]:36}  {_short_url(row['url'])}"
            )
        print()

    print("HOST RACK")
    print(f"{'host':38} {'probes':>6} {'ok':>4} {'fail':>4} {'pass':>5} {'p50':>6}")
    for bucket in hosts[:30]:
        p50 = f"{bucket['p50_ms']}ms" if bucket["p50_ms"] is not None else "—"
        print(
            f"{bucket['host'][:38]:38} {bucket['probes']:>6} {bucket['ok']:>4} "
            f"{bucket['fail']:>4} {bucket['pass_rate']:>5.0%} {p50:>6}"
        )

    if args.json:
        Path(args.json).write_text(json.dumps({
            "summary": summary,
            "hosts": hosts,
            "events": event_reports,
        }, indent=2) + "\n", encoding="utf-8")
        print(f"\nwrote {args.json}")

    if args.markdown:
        lines = [
            "# Source health report",
            "",
            f"- Feed: `{args.feed}`",
            f"- Generated: {summary['generated_at_iso']}",
            f"- Events: **{summary['events']}**, sources examined: **{summary['sources_examined']}**",
            f"- Probes: **{summary['probed']}** — {summary['ok']} OK / {summary['failed']} failed "
            f"({summary['pass_rate']:.0%} pass)",
            "",
            "## Host rack",
            "",
            _md_table(
                ["#", "Host", "Probes", "OK", "Fail", "Pass", "p50"],
                [
                    [i, b["host"], b["probes"], b["ok"], b["fail"], f"{b['pass_rate']:.0%}",
                     f"{b['p50_ms']} ms" if b["p50_ms"] is not None else "—"]
                    for i, b in enumerate(hosts[:50], 1)
                ],
            ),
            "",
            "## Event stacks",
            "",
        ]
        for report in event_reports:
            lines.append(f"### {report['title']}")
            lines.append("")
            lines.append(
                f"`{report['status']}` · {report['league']} · "
                f"**{report['healthy']} healthy / {report['failed']} failed**"
            )
            lines.append("")
            lines.append(_md_table(
                ["#", "State", "Quality", "Latency", "Source", "Host", "Fails"],
                [
                    [row["rank"], "OK" if row["ok"] else row["status"],
                     row.get("quality_text", QUALITY_LABEL.get(row["quality"], "?")),
                     f"{row['latency_ms']} ms" if row["ok"] else "—",
                     row["label"], row["host"], row["fails"]]
                    for row in report["stack"] if row["ok"] or args.show_bad
                ],
            ))
            lines.append("")
        Path(args.markdown).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"wrote {args.markdown}")

    if args.apply_out:
        output = dict(payload)
        by_key = {report["canonical_key"]: report for report in event_reports}
        for event in payload["events"]:
            report = by_key.get(event.get("canonical_key", ""))
            if not report:
                continue
            keep = []
            for row in report["stack"]:
                if not row["ok"]:
                    continue
                keep.append({
                    "origin": row["origin"],
                    "label": row["label"],
                    "url": row["url"],
                    "kind": "embed",
                    "referer": row.get("referer", "https://streamcorner.cloud/"),
                    "height": row.get("height", 0),
                    "bitrate": row.get("bitrate", 0),
                    "verified": bool(row.get("verified")),
                    "quality": row.get("quality_text", ""),
                })
                if len(keep) >= max(1, args.max_sources):
                    break
            event["sources"] = keep
        output["source_report_at"] = now
        Path(args.apply_out).write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {args.apply_out} (max {args.max_sources} healthy sources/event)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
