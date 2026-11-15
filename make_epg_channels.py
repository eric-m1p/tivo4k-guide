#!/usr/bin/env python3
"""
Write epg-grabber/tivo4k.channels.xml: one guide source per playlist channel
that isn't already covered by the Pluto/Samsung guides from i.mjh.nz.

xmltv_id is set to the playlist's exact tvg-id so TiviMate matches it.
"""
import json
import os
import re
import sys
from collections import defaultdict
from xml.sax.saxutils import escape, quoteattr

HERE = os.path.dirname(os.path.abspath(__file__))
M3U = os.path.join(HERE, "tivo4k.m3u")
GUIDES = os.path.join(HERE, ".cache", "guides.json")
GRABBER = os.path.join(HERE, "epg-grabber")
OUT = os.path.join(GRABBER, "tivo4k.channels.xml")

# Sites tried first: fast JSON APIs that rarely block, then big listings sites.
PREFERRED = [
    "i.mjh.nz", "plex.tv", "pluto.tv", "xumo.tv", "distro.tv", "watch.whaletvplus.com",
    "tvpassport.com", "tvguide.com", "ontvtonight.com",
    "sky.com", "freeview.co.uk", "mytelly.co.uk", "tvireland.ie", "virgintvgo.virginmedia.com",
    "tvtv.us",  # returned nothing in Nov 2026; kept last in case it recovers
]
# --fallback: only channels listed in .cache/epg_empty.txt (no programmes on the
# last grab), each from its next-best site; written to tivo4k.fallback.channels.xml
FALLBACK = "--fallback" in sys.argv
SKIP = {a for a in sys.argv[1:] if not a.startswith("--")}  # site names to avoid
EMPTY = os.path.join(HERE, ".cache", "epg_empty.txt")
if FALLBACK:
    OUT = os.path.join(GRABBER, "tivo4k.fallback.channels.xml")


def main():
    guides = json.load(open(GUIDES))
    by = defaultdict(list)
    for g in guides:
        if g["channel"]:
            by[(g["channel"], g["feed"])].append(g)
            by[(g["channel"], None)].append(g)

    have_site = {d for d in os.listdir(os.path.join(GRABBER, "sites"))}
    failed = {}
    if FALLBACK:
        import re as _re
        main_xml = open(os.path.join(GRABBER, "tivo4k.channels.xml")).read()
        empty = set(open(EMPTY).read().split())
        for m in _re.finditer(r'site="([^"]+)" lang="[^"]*" xmltv_id="([^"]+)"', main_xml):
            if m.group(2) in empty:
                failed[m.group(2)] = m.group(1)

    def score(g):
        s = g["site"]
        return (PREFERRED.index(s) if s in PREFERRED else len(PREFERRED), g["lang"] != "en")

    rows, missing = [], 0
    for line in open(M3U):
        if not line.startswith("#EXTINF"):
            continue
        tvg = re.search(r'tvg-id="([^"]*)"', line).group(1)
        name = re.search(r'tvg-name="([^"]*)"', line).group(1)
        if re.fullmatch(r"[0-9a-f]{24}", tvg) or "@" not in tvg and not tvg.endswith(tuple("abcdefghijklmnopqrstuvwxyz")):
            continue  # Pluto / Samsung ids, covered by i.mjh.nz
        if FALLBACK and tvg not in failed:
            continue
        ch, _, feed = tvg.partition("@")
        opts = [g for g in (by.get((ch, feed or None)) or by.get((ch, None)) or [])
                if g["site"] in have_site and g["site"] not in SKIP and g["site"] != failed.get(tvg)]
        if not opts:
            missing += 1
            continue
        g = sorted(opts, key=score)[0]
        rows.append((g["site"], g["lang"], tvg, g["site_id"], name))

    rows.sort()
    with open(OUT, "w") as fh:
        fh.write('<?xml version="1.0" encoding="UTF-8"?>\n<channels>\n')
        for site, lang, tvg, sid, name in rows:
            fh.write(f"  <channel site={quoteattr(site)} lang={quoteattr(lang)} xmltv_id={quoteattr(tvg)} "
                     f"site_id={quoteattr(sid)}>{escape(name)}</channel>\n")
        fh.write("</channels>\n")
    sites = defaultdict(int)
    for r in rows:
        sites[r[0]] += 1
    print(f"{len(rows)} channels -> {OUT} ({missing} without any guide source)")
    print(dict(sorted(sites.items(), key=lambda kv: -kv[1])))


if __name__ == "__main__":
    main()
