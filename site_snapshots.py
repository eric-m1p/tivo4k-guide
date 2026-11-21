#!/usr/bin/env python3
"""Record and replay scraper plugin runs (saved page snapshots).

``record`` runs a plugin's real ``fetch(limit=N)`` against the live site and
saves every page it loaded (plus totalsporteki's rendered-page results, or
StreamCorner's worker responses), the clock, and the plugin's output to
``snapshots/<site>.json.gz``. ``replay`` serves those pages back with the
clock frozen and returns the output again, so ``test_site_snapshots.py`` can
check that the parsers still turn the same pages into the same rows.

When a site changes its layout (the canary says "layout change likely"):
record a fresh snapshot, fix the parser until the new output looks right,
record again, and commit the snapshot with the fix.

    python3 site_snapshots.py record isportsurge [--limit 4]
    python3 site_snapshots.py record streamcorner [--providers nfl,alpha]
    python3 site_snapshots.py check            # replay every snapshot, report diffs
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import re
import sys
from contextlib import contextmanager
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
SNAP_DIR = os.path.join(HERE, "snapshots")

import live_sites_common as C  # noqa: E402
import site_plugins  # noqa: E402
import streamcorner_api as sc  # noqa: E402

SITES = site_plugins.PLUGINS + ("streamcorner",)


def path_for(site: str) -> str:
    return os.path.join(SNAP_DIR, f"{site}.json.gz")


def load(site: str) -> dict:
    with gzip.open(path_for(site), "rt", encoding="utf-8") as fh:
        return json.load(fh)


def save(site: str, snap: dict) -> None:
    os.makedirs(SNAP_DIR, exist_ok=True)
    with gzip.open(path_for(site), "wt", encoding="utf-8", compresslevel=9) as fh:
        json.dump(snap, fh, ensure_ascii=False, sort_keys=True)


def canonical(rows: list[dict]) -> list[dict]:
    """Order-independent form: fetch order varies with thread timing."""
    return sorted(json.loads(json.dumps(rows)), key=lambda r: json.dumps(r, sort_keys=True))


# ---------------------------------------------------------------- page sites
@contextmanager
def _frozen(now: int):
    with mock.patch.object(C, "now_sec", lambda: now):
        yield


def record_site(site: str, limit: int) -> dict:
    module = site_plugins.module_for(site)
    pages: dict[str, str] = {}
    real_get = C.get

    def recording_get(url, referer="", timeout=25, retries=2):
        html = real_get(url, referer=referer, timeout=timeout, retries=retries)
        pages[url] = html
        return html

    render: dict = {}
    patches = [mock.patch.object(C, "get", recording_get)]
    if hasattr(module, "_render_pages"):
        real_render = module._render_pages

        async def recording_render(items, *args, **kwargs):
            out = await real_render(items, *args, **kwargs)
            render.update(out)
            return out
        patches.append(mock.patch.object(module, "_render_pages", recording_render))
    now = C.now_sec()
    with patches[0], (patches[1] if len(patches) > 1 else _null()), _frozen(now):
        rows = module.fetch(limit=limit)
    return {"site": site, "now": now, "limit": limit, "pages": pages, "render": render, "expected": rows}


@contextmanager
def _null():
    yield


def replay_site(snap: dict) -> list[dict]:
    module = site_plugins.module_for(snap["site"])
    pages = snap["pages"]

    def replay_get(url, referer="", timeout=25, retries=2):
        return pages.get(url, "")

    async def replay_render(items, *args, **kwargs):
        return {k: v for k, v in snap.get("render", {}).items()}

    with mock.patch.object(C, "get", replay_get), _frozen(int(snap["now"])):
        if hasattr(module, "_render_pages"):
            with mock.patch.object(module, "_render_pages", replay_render):
                return module.fetch(limit=snap["limit"])
        return module.fetch(limit=snap["limit"])


_STRIPS = (
    # (name, regex) tried in order; a strip is kept only if the replayed output is unchanged
    ("style+svg", re.compile(r"(<style\b[^>]*>).*?(</style>)|(<svg\b[^>]*>).*?(</svg>)", re.S | re.I)),
    ("scripts", re.compile(r"(<script\b[^>]*>).*?(</script>)", re.S | re.I)),
    ("comments", re.compile(r"<!--.*?-->", re.S)),
)


def _strip(html: str, rx: re.Pattern) -> str:
    return rx.sub(lambda m: "".join(g for g in m.groups() if g and g.startswith("<")) if m.groups() else "", html)


def minimize(snap: dict) -> list[str]:
    """Shrink saved pages without changing what the plugin makes of them."""
    want = canonical(snap["expected"])
    kept = []
    for name, rx in _STRIPS:
        trial = {**snap, "pages": {u: _strip(h, rx) for u, h in snap["pages"].items()}}
        try:
            same = canonical(replay_site(trial)) == want
        except Exception:  # noqa: BLE001
            same = False
        if same:
            snap["pages"] = trial["pages"]
            kept.append(name)
    return kept


# ---------------------------------------------------------------- StreamCorner
def record_streamcorner(providers: list[str]) -> dict:
    lists: dict = {}
    details: dict = {}
    real_list, real_detail = sc.WorkerClient.provider_list, sc.WorkerClient.provider_detail

    async def rec_list(self, provider):
        payload = await real_list(self, provider)
        lists[provider] = payload
        return payload

    async def rec_detail(self, provider, ident):
        detail = await real_detail(self, provider, ident)
        details.setdefault(provider, {})[str(ident)] = detail
        return detail

    now = int(__import__("time").time())
    with mock.patch.object(sc.WorkerClient, "provider_list", rec_list), \
            mock.patch.object(sc.WorkerClient, "provider_detail", rec_detail), \
            mock.patch.object(sc.time, "time", lambda: now):
        result = asyncio.run(sc.fetch_all(providers=providers, concurrency=1))
    expected = {p: [sc.to_feed_dict(e) for e in events] for p, events in result.items()}
    return {"site": "streamcorner", "now": now, "lists": lists, "details": details, "expected": expected}


class _ReplayClient:
    def __init__(self, snap: dict):
        self.snap = snap

    async def provider_list(self, provider):
        return self.snap["lists"].get(provider)

    async def provider_detail(self, provider, ident):
        detail = (self.snap["details"].get(provider) or {}).get(str(ident))
        if detail is None:
            raise KeyError(ident)
        return detail


def replay_streamcorner(snap: dict) -> dict:
    client = _ReplayClient(snap)
    out = {}
    with mock.patch.object(sc.time, "time", lambda: int(snap["now"])):
        for provider in snap["lists"]:
            events = asyncio.run(sc.fetch_provider(client, provider))
            out[provider] = [sc.to_feed_dict(e) for e in events]
    return out


# ---------------------------------------------------------------- CLI
def replay(site: str):
    snap = load(site)
    return snap, (replay_streamcorner(snap) if site == "streamcorner" else replay_site(snap))


def matches(site: str) -> tuple[bool, str]:
    snap, got = replay(site)
    if site == "streamcorner":
        exp = {p: canonical(v) for p, v in snap["expected"].items()}
        got = {p: canonical(v) for p, v in got.items()}
        n = sum(len(v) for v in exp.values())
    else:
        exp, got = canonical(snap["expected"]), canonical(got)
        n = len(exp)
    return exp == got, f"{site}: {n} expected rows"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    rec = sub.add_parser("record")
    rec.add_argument("site", choices=SITES)
    rec.add_argument("--limit", type=int, default=4)
    rec.add_argument("--providers", default="nfl,alpha")
    sub.add_parser("check")
    args = ap.parse_args()

    if args.cmd == "record":
        if args.site == "streamcorner":
            snap = record_streamcorner([p.strip() for p in args.providers.split(",") if p.strip()])
            rows = sum(len(v) for v in snap["expected"].values())
        else:
            snap = record_site(args.site, args.limit)
            rows = len(snap["expected"])
            stripped = minimize(snap)
            if stripped:
                print(f"[snapshot] stripped {', '.join(stripped)} (output unchanged)")
        save(args.site, snap)
        size = os.path.getsize(path_for(args.site)) // 1024
        print(f"[snapshot] {args.site}: {rows} rows -> {path_for(args.site)} ({size} KB)")
        if rows == 0:
            print("[snapshot] warning: 0 rows recorded; the test would pin an empty result", file=sys.stderr)
        return 0

    bad = 0
    for site in SITES:
        if not os.path.exists(path_for(site)):
            print(f"{site}: no snapshot")
            continue
        ok, note = matches(site)
        bad += not ok
        print(("ok   " if ok else "DIFF ") + note)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
