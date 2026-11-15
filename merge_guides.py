#!/usr/bin/env python3
"""Merge guide.xml + guide.fallback.xml (+ Pluto/Samsung listings from i.mjh.nz
for playlist channels) into one self-contained guide.xml / guide.xml.gz.

Channels that have programmes in the main guide keep them; the fallback guide
only fills channels that came back empty. Also writes .cache/epg_empty.txt
(still-empty channel ids) for the next fallback pass.
"""
import gzip
import io
import os
import urllib.request
import re
import xml.etree.ElementTree as ET
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REMOTE = ["https://i.mjh.nz/PlutoTV/all.xml.gz", "https://i.mjh.nz/SamsungTVPlus/all.xml.gz"]
MAIN, FB = os.path.join(HERE, "guide.xml"), os.path.join(HERE, "guide.fallback.xml")


def main():
    root = ET.parse(MAIN).getroot()
    have = Counter(p.get("channel") for p in root.iter("programme"))
    if os.path.exists(FB):
        fb = ET.parse(FB).getroot()
        known = {c.get("id") for c in root.iter("channel")}
        fill = {p.get("channel") for p in fb.iter("programme")} - {k for k, v in have.items() if v}
        for c in fb.iter("channel"):
            if c.get("id") in fill and c.get("id") not in known:
                root.insert(0, c)
        for p in fb.iter("programme"):
            if p.get("channel") in fill:
                root.append(p)
        have = Counter(p.get("channel") for p in root.iter("programme"))

    # Pluto / Samsung listings for channels in the playlist
    ids = set(re.findall(r'tvg-id="([^"]+)"', open(os.path.join(HERE, "tivo4k.m3u")).read()))
    known = {c.get("id") for c in root.iter("channel")}
    # Channels still empty whose Pluto/Samsung twin has listings: borrow them,
    # renamed to the playlist's tvg-id (e.g. Golazo streams from CBS but Pluto has its guide).
    import json
    alias = {}
    for g in json.load(open(os.path.join(HERE, ".cache", "guides.json"))):
        sid = g["site_id"]
        twin = sid.split("#", 1)[1] if g["site"] == "i.mjh.nz" and sid.startswith(("PlutoTV/", "SamsungTVPlus/")) else (
            sid if g["site"] == "pluto.tv" else None)
        if not twin or not g["channel"]:
            continue
        for tid in ids:
            if (tid == g["channel"] or tid.startswith(g["channel"] + "@")) and not have[tid]:
                alias.setdefault(twin, tid)
    for url in REMOTE:
        raw = urllib.request.urlopen(url, timeout=120).read()
        for _, el in ET.iterparse(io.BytesIO(gzip.decompress(raw))):
            if el.tag == "channel" and el.get("id") in ids and el.get("id") not in known and not have[el.get("id")]:
                root.insert(0, el); known.add(el.get("id"))
            elif el.tag == "programme" and el.get("channel") in ids and not have[el.get("channel")]:
                root.append(el)
            elif el.tag == "programme" and el.get("channel") in alias:
                el.set("channel", alias[el.get("channel")]); root.append(el)
            elif el.tag == "channel" and el.get("id") in alias and alias[el.get("id")] not in known:
                el.set("id", alias[el.get("id")]); known.add(el.get("id")); root.insert(0, el)
    have = Counter(p.get("channel") for p in root.iter("programme"))

    data = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    with open(MAIN, "wb") as fh:
        fh.write(data)
    with gzip.open(MAIN + ".gz", "wb") as fh:
        fh.write(data)

    xml = open(os.path.join(HERE, "epg-grabber", "tivo4k.channels.xml")).read()
    wanted = re.findall(r'xmltv_id="([^"]+)"', xml)
    empty = [w for w in wanted if not have[w]]
    with open(os.path.join(HERE, ".cache", "epg_empty.txt"), "w") as fh:
        fh.write("\n".join(empty))
    covered = sum(1 for i in ids if have[i])
    print(f"playlist coverage: {covered}/{len(ids)} channels have listings")
    print(f"guide.xml: {sum(1 for v in have.values() if v)} channels with listings, "
          f"{sum(have.values())} programmes; {len(empty)} mapped channels still empty")


if __name__ == "__main__":
    main()
