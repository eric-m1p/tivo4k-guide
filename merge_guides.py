#!/usr/bin/env python3
"""Merge fresh guide listings for the current playlist and write XMLTV."""

from collections import defaultdict
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import re
import sys
from tempfile import NamedTemporaryFile
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # support python -I
from supplemental_epg import (
    add_remote_sources,
    choose_schedules,
    collect_programmes,
    current_ids,
    fill_gaps,
    iter_xmltv,
)

CACHE = HERE / ".cache"
MAIN = HERE / "guide.xml"
FALLBACK = HERE / "guide.fallback.xml"
PUBLISHED = HERE / "guide.xml.gz"
PLAYLIST = HERE / "tivo4k_all.m3u"
SELECTED = HERE / "tivo4k.m3u"
NATIVE_PROVIDERS = ("PlutoTV", "SamsungTVPlus")


def playlist_channels(path):
    channels = {}
    with path.open(encoding="utf-8") as playlist:
        for line in playlist:
            if line.startswith("#EXTINF"):
                attrs = dict(re.findall(r'([a-z-]+)="([^"]*)"', line))
                if attrs.get("tvg-id"):
                    channels[attrs["tvg-id"]] = attrs.get("tvg-name", attrs["tvg-id"])
    return channels


def record_empty(covered):
    mapped = ET.parse(HERE / "epg-grabber/tivo4k.channels.xml").getroot()
    empty = [channel.get("xmltv_id") for channel in mapped if channel.get("xmltv_id") not in covered]
    (CACHE / "epg_empty.txt").write_text("\n".join(empty), encoding="utf-8")
    return len(empty)


def keep_playlist_entries(root, channels):
    root[:] = [
        element for element in root
        if (element.tag != "channel" or element.get("id") in channels)
        and (element.tag != "programme" or element.get("channel") in channels)
    ]


def remote_sources(channels):
    with (HERE / "epg_sources.json").open(encoding="utf-8") as file:
        configured = {source["url"]: source for source in json.load(file)}
    native = {}
    for provider in NATIVE_PROVIDERS:
        url = f"https://i.mjh.nz/{provider}/all.xml.gz"
        source = configured.pop(url, {"url": url, "channels": {}})
        for channel in channels:
            if re.fullmatch(r"[A-Za-z0-9]+", channel):
                source["channels"][channel] = [channel]
        native[provider] = source

    aliases = defaultdict(list)
    for channel in channels:
        base, _, feed = channel.partition("@")
        aliases[base].append((channel, feed or None))
    with (CACHE / "guides.json").open(encoding="utf-8") as file:
        for guide in json.load(file):
            if guide["site"] == "pluto.tv":
                provider, source_id = "PlutoTV", guide["site_id"]
            elif guide["site"] == "i.mjh.nz":
                prefix, separator, source_id = guide["site_id"].partition("#")
                provider = prefix.split("/", 1)[0]
                if not separator or provider not in native:
                    continue
            else:
                continue
            for channel, feed in aliases.get(guide["channel"], []):
                if guide["feed"] in (None, feed):
                    ids = native[provider]["channels"].setdefault(channel, [])
                    if source_id not in ids:
                        ids.append(source_id)
    return [*native.values(), *configured.values()]


def add_previous(root, path, channels, covered, now):
    if not path.exists():
        return 0
    missing = channels.keys() - covered
    if not missing:
        return 0
    try:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rb") as file:
            programmes = collect_programmes(iter_xmltv(file), missing, now)
    except (OSError, EOFError, ET.ParseError, ValueError) as error:
        print(f"Could not reuse {path.name}: {error}")
        return 0
    schedules = choose_schedules(programmes, {channel: [channel] for channel in missing}, now)
    fill_gaps(root, schedules, channels, covered)
    return len(schedules)


def write_guide(root):
    data = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    outputs = ((MAIN, data), (PUBLISHED, gzip.compress(data, mtime=0)))
    for path, content in outputs:
        with NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as file:
            staged = Path(file.name)
            try:
                file.write(content)
                file.close()
                staged.chmod(0o644)
                os.replace(staged, path)
            finally:
                staged.unlink(missing_ok=True)


def main():
    now = datetime.now(timezone.utc)
    root = ET.parse(MAIN).getroot()
    if root.tag != "tv":
        raise ValueError("primary guide is not XMLTV")
    covered = current_ids(root, now)
    if "--find-empty" in sys.argv:
        print(f"{record_empty(covered)} mapped channels need a fallback source")
        return

    channels = playlist_channels(PLAYLIST if PLAYLIST.exists() else SELECTED)
    selected = playlist_channels(SELECTED)
    if not channels:
        raise ValueError("playlist has no guide IDs")
    keep_playlist_entries(root, channels)
    print(f"Primary coverage in the next 24 hours: {len(covered & channels.keys())}/{len(channels)}")
    print(f"Fallback grabber filled {add_previous(root, FALLBACK, channels, covered, now)} channel gaps")
    add_remote_sources(root, remote_sources(channels), channels, covered, now)
    print(f"Retained {add_previous(root, PUBLISHED, channels, covered, now)} still-current channels from the previous guide")
    if not covered & channels.keys():
        raise ValueError("no current programmes for the playlist")
    write_guide(root)
    empty = record_empty(covered)
    print(f"Guide coverage in the next 24 hours: {len(covered & channels.keys())}/{len(channels)}")
    if selected.keys() != channels.keys():
        print(f"Selected TiVo channels with listings: {len(covered & selected.keys())}/{len(selected)}")
    print(f"Mapped channels still empty: {empty}")


if __name__ == "__main__":
    main()
