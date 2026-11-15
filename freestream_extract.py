#!/usr/bin/env python3
"""
Freestream channel extractor (working version)

The public site is a React Router SSR shell.  All data comes
from the CMW API at https://p-cmwnext-fast.movetv.com and is authenticated with a
short-lived "prospect" (anonymous) JWT issued by POST /cmw/v1/client/jwt, which
is itself OAuth-1.0a signed with a consumer secret loaded at runtime from an
encrypted config bundle.

Rather than re-implement that handshake, this tool boots the real app in a
headless Chrome (Playwright), borrows the Bearer token + request headers the app
obtains for free, then talks to the API directly.

Outputs
    freestream_channels.json  channel catalogue (+ EPG if --epg)
    freestream.m3u           channel metadata playlist (see DRM note)
    freestream.xml           XMLTV guide (with --epg)

DRM note
    Every live Freestream channel is delivered as CENC/SAMPLE-AES-CTR
    (Widevine + PlayReady).  The master manifest is fetchable, but media
    segments return "DRM required".  The .m3u therefore references the SSAI
    manifest and will only play in a Widevine-capable player/browser -- not in
    VLC/TiviMate/ffmpeg.  Channel metadata and EPG are fully usable.

Usage
    python3 freestream_extract.py                 # channels + m3u
    python3 freestream_extract.py --epg           # + XMLTV (slow: 1 req/channel)
    python3 freestream_extract.py --epg --channels 40
    python3 freestream_extract.py --headed        # watch the bootstrap
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

CMW = "https://p-cmwnext-fast.movetv.com"
STREAMAUTH = "https://p-streamauth.movetv.com/stream/auth"
QVT = ("https://cbd46b77.cdn.cms.movetv.com/playermetadata/sling/v1/api/"
       "channels/{guid}/schedule/now/playback_info.qvt")
HOME = "https://watch.sling.com/dashboard/home"

HEADER_KEYS = {
    "client-config", "client-version", "player-version", "client-analytics-id",
    "time-zone-id", "timezone", "dma", "geo-zipcode", "platform-variant",
    "features", "response-config", "page_size", "device-model",
}


# --------------------------------------------------------------------------- #
# bootstrap: borrow an anonymous JWT from the real app
# --------------------------------------------------------------------------- #
async def bootstrap(headed: bool = False, timeout: int = 90):
    from playwright.async_api import async_playwright

    state: dict = {"token": None, "headers": {}, "guide": None, "guide_headers": {}}

    async with async_playwright() as p:
        launch = {"headless": not headed}
        try:
            browser = await p.chromium.launch(channel="chrome", **launch)
        except Exception:
            browser = await p.chromium.launch(**launch)

        ctx = await browser.new_context(
            locale="en-US", timezone_id="America/Los_Angeles",
            viewport={"width": 1600, "height": 1000},
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/138.0.0.0 Safari/537.36"),
        )
        page = await ctx.new_page()

        async def on_request(req):
            if "cmwnext" not in req.url or state["token"]:
                return
            auth = req.headers.get("authorization") or ""
            if auth.startswith("Bearer "):
                state["token"] = auth
                state["headers"] = {
                    k: v for k, v in req.headers.items()
                    if k.lower() in HEADER_KEYS
                }

        async def on_response(resp):
            if resp.url.endswith("/fluid-guide") and resp.status == 200 and state["guide"] is None:
                try:
                    state["guide"] = await resp.text()
                    state["guide_headers"] = {
                        k: v for k, v in resp.request.headers.items()
                        if k.lower() in HEADER_KEYS
                    }
                except Exception:
                    pass

        page.on("request", lambda r: asyncio.create_task(on_request(r)))
        page.on("response", lambda r: asyncio.create_task(on_response(r)))

        await page.goto(HOME, wait_until="domcontentloaded", timeout=timeout * 1000)

        deadline = time.time() + timeout
        while time.time() < deadline and not state["token"]:
            await page.wait_for_timeout(300)
        if not state["token"]:
            await browser.close()
            raise RuntimeError("never saw an anonymous CMW token (page blocked?)")

        # dismiss consent so the Guide tab is clickable
        for sel in ("button:has-text('Accept')", "text=Accept"):
            try:
                await page.click(sel, timeout=2000)
                break
            except Exception:
                pass

        # open Guide -> app requests /fluid-guide for us
        try:
            await page.click("text=Guide", timeout=8000)
        except Exception:
            pass
        while time.time() < deadline and state["guide"] is None:
            await page.wait_for_timeout(300)

        await browser.close()

    if not state["guide"]:
        raise RuntimeError("could not capture /fluid-guide payload")

    state["headers"] = {**state["headers"], **state["guide_headers"]}
    try:
        adobe = json.loads(state["guide"]).get("adobe", {})
        state["profile_guid"] = adobe.get("ProfileGuid") or adobe.get("AccountGuid") or ""
    except Exception:
        state["profile_guid"] = ""
    state["zip"] = state["headers"].get("geo-zipcode", "91605")
    return state


# --------------------------------------------------------------------------- #
# API helpers (replay with the borrowed token)
# --------------------------------------------------------------------------- #
def api_headers(state: dict) -> dict:
    h = dict(state["headers"])
    h["authorization"] = state["token"]
    h.setdefault("content-type", "application/json; charset=UTF-8")
    h.setdefault("origin", "https://watch.sling.com")
    h.setdefault("referer", "https://watch.sling.com/")
    return h


def parse_catalogue(guide_json: str) -> list[dict]:
    doc = json.loads(guide_json)
    tiles = []
    for ribbon in doc.get("ribbons", []):
        tiles.extend(ribbon.get("tiles", []))

    out, category = [], "Uncategorized"
    for tile in tiles:
        fmt = tile.get("format")
        if fmt == "DIVIDER_FILTER":
            category = tile.get("title") or category
            continue
        if fmt != "CHANNEL_CARD":
            continue
        meta = tile.get("meta_block") or {}
        img = tile.get("image") or meta.get("image") or {}
        out.append({
            "id": tile.get("id"),
            "name": tile.get("tts") or meta.get("title"),
            "description": (meta.get("description") or "").strip(),
            "logo": img.get("url") or "",
            "category": category,
            "channel_type": tile.get("channel_type"),
            "schedule_url": tile.get("schedule_href"),
        })
    # de-dupe by guid
    seen, uniq = set(), []
    for c in out:
        if c["id"] and c["id"] not in seen:
            seen.add(c["id"])
            uniq.append(c)
    return uniq


def resolve_stream(session, state, guid: str) -> dict | None:
    body = {
        "os_version": "10.15.7", "device_name": "browser", "os_name": "macOS",
        "brand": "sling", "subscriber_id": state.get("profile_guid", ""),
        "qvt": QVT.format(guid=guid), "drm": "none",
        "location_zip": state.get("zip", "91605"),
        "account_status": "prospect", "device_model": "Chrome",
        "support_mode": "false", "profile_type": "Admin",
        "ad_tracking_allowed": "false",
    }
    try:
        r = session.post(STREAMAUTH, json=body, headers={
            "authorization": state["token"],
            "content-type": "application/json",
            "origin": "https://watch.sling.com",
            "referer": "https://watch.sling.com/",
        }, timeout=25)
        if r.status_code != 200:
            return None
        j = r.json()
        return {
            "manifest_url": j.get("m3u8_url"),
            "ssai_manifest": j.get("ssai_manifest"),
            "dash_manifest": j.get("dash_manifest_url"),
            "widevine_license": j.get("widevine_license_url"),
            "is_free": (j.get("qvt") or {}).get("is_free"),
            "plan_type": _plan(j.get("ssai_manifest", "")),
        }
    except Exception:
        return None


def _plan(url: str) -> str | None:
    m = re.search(r"[?&]plan_type=([^&]+)", url or "")
    return m.group(1) if m else None


def fetch_schedule(session, state, url: str) -> list[dict]:
    if not url:
        return []
    try:
        r = session.get(url, headers=api_headers(state), timeout=25)
        if r.status_code != 200:
            return []
        doc = r.json()
    except Exception:
        return []

    progs = []
    for ribbon in doc.get("ribbons", []):
        for tile in ribbon.get("tiles", []):
            title = tile.get("title") or tile.get("tts")
            start, end = tile.get("start_time"), tile.get("end_time")
            if not title or not start:
                continue
            mb = tile.get("meta_block") or {}
            progs.append({
                "title": title,
                "description": (mb.get("description") or "").strip(),
                "start": start,
                "end": end,
            })
    progs.sort(key=lambda p: p["start"])
    return progs


# --------------------------------------------------------------------------- #
# writers
# --------------------------------------------------------------------------- #
def write_m3u(channels, path: Path):
    lines = ["#EXTM3U"]
    for c in channels:
        name = (c.get("name") or "Unknown").replace(",", " ")
        lines.append(
            f'#EXTINF:-1 tvg-id="{c["id"]}" tvg-logo="{c.get("logo","")}" '
            f'group-title="{c.get("category","")}",{name}'
        )
        url = c.get("ssai_manifest") or c.get("manifest_url")
        if url:
            lines.append(url)
        else:
            lines.append("# no anonymous manifest")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def xml_escape(s: str) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def write_xmltv(channels, path: Path):
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<tv generator-info-name="freestream">']
    for c in channels:
        out.append(f'  <channel id="{xml_escape(c["id"])}">')
        out.append(f'    <display-name>{xml_escape(c.get("name"))}</display-name>')
        if c.get("logo"):
            out.append(f'    <icon src="{xml_escape(c["logo"])}"/>')
        out.append("  </channel>")
    for c in channels:
        for p in c.get("programs", []):
            try:
                start = datetime.fromtimestamp(int(p["start"]), tz=timezone.utc)
                stop = (datetime.fromtimestamp(int(p["end"]), tz=timezone.utc)
                        if p.get("end") else start)
            except Exception:
                continue
            out.append(
                f'  <programme start="{start:%Y%m%d%H%M%S} +0000" '
                f'stop="{stop:%Y%m%d%H%M%S} +0000" channel="{xml_escape(c["id"])}">'
            )
            out.append(f'    <title>{xml_escape(p["title"])}</title>')
            if p.get("description"):
                out.append(f'    <desc>{xml_escape(p["description"])}</desc>')
            out.append("  </programme>")
    out.append("</tv>")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="Extract Freestream channel catalogue/EPG")
    ap.add_argument("--epg", action="store_true", help="also fetch schedules -> XMLTV")
    ap.add_argument("--channels", type=int, default=0, help="limit channels processed")
    ap.add_argument("--headed", action="store_true", help="show the browser")
    ap.add_argument("--outdir", default=".", help="output directory")
    ar = ap.parse_args()

    try:
        import requests  # noqa: F401
    except ImportError:
        print("need: pip install requests playwright", file=sys.stderr)
        return 1

    import requests

    print("[1/3] booting the web player for an anonymous API token ...")
    state = asyncio.run(bootstrap(headed=ar.headed))
    print("      token acquired")

    channels = parse_catalogue(state["guide"])
    print(f"[2/3] /fluid-guide -> {len(channels)} channels")
    if ar.channels:
        channels = channels[:ar.channels]

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/138.0.0.0"

    print("[3/3] resolving anonymous manifests ...")
    for i, c in enumerate(channels, 1):
        info = resolve_stream(session, state, c["id"])
        if info:
            c.update(info)
        if ar.epg:
            c["programs"] = fetch_schedule(session, state, c.get("schedule_url"))
        if i % 25 == 0 or i == len(channels):
            print(f"      {i}/{len(channels)}")

    outdir = Path(ar.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    jpath = outdir / "freestream_channels.json"
    mpath = outdir / "freestream.m3u"
    jpath.write_text(json.dumps(channels, indent=2, ensure_ascii=False), encoding="utf-8")
    write_m3u(channels, mpath)
    print(f"wrote {jpath} and {mpath}")

    if ar.epg:
        xpath = outdir / "freestream.xml"
        write_xmltv(channels, xpath)
        print(f"wrote {xpath}")

    free = sum(1 for c in channels if c.get("is_free"))
    with_manifest = sum(1 for c in channels if c.get("manifest_url"))
    print(f"\nsummary: {len(channels)} channels | {with_manifest} manifests | {free} flagged free")
    print("note: media is Widevine/PlayReady CENC -> not playable in VLC/TiviMate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
