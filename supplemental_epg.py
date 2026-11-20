"""Load and merge XMLTV schedules for playlist channels with a guide gap."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import gzip
from http.client import HTTPException
import io
import json
import re
import urllib.request
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

COVERAGE_WINDOW = timedelta(hours=24)
SOURCE_WINDOW = timedelta(days=3)
PLACEHOLDERS = {"to be announced", "tba", "no information available", "no programming information", "off air"}


def xmltv_time(value):
    if not value or not re.match(r"^\d{14}(?:\s|$|Z)", value):
        return None
    timestamp = f"{value[:8]}T{value[8:14]}{value[14:].strip() or '+0000'}"
    try:
        return datetime.fromisoformat(timestamp)
    except ValueError:
        return None


def programme_times(programme):
    title = (programme.findtext("title") or "").strip()
    if not title or title.casefold() in PLACEHOLDERS:
        return None
    start = xmltv_time(programme.get("start"))
    stop = xmltv_time(programme.get("stop"))
    if start is None or stop is None or stop <= start:
        return None
    return start, stop


def current_ids(root, now, window=COVERAGE_WINDOW):
    """IDs with a programme on now or starting inside the requested window."""
    covered = set()
    cutoff = now + window
    for programme in root.iter("programme"):
        channel = programme.get("channel")
        if channel in covered:
            continue
        times = programme_times(programme)
        if times and times[0] <= cutoff and times[1] > now:
            covered.add(channel)
    return covered


def collect_programmes(programmes, wanted, now):
    result = defaultdict(dict)
    cutoff = now + SOURCE_WINDOW
    for programme in programmes:
        channel = programme.get("channel")
        if channel not in wanted:
            continue
        times = programme_times(programme)
        if times and times[0] <= cutoff and times[1] > now:
            result[channel].setdefault(times[0], programme)
    return result


def iter_xmltv(stream):
    """Stream top-level programmes without retaining the unneeded parts of the XML."""
    events = iter(ET.iterparse(stream, events=("start", "end")))
    _, root = next(events)
    if root.tag != "tv":
        raise ValueError("expected an XMLTV guide")
    for event, element in events:
        if event == "end" and element.tag in ("programme", "channel"):
            if element.tag == "programme":
                yield element
            root.remove(element)


def c31_programmes(stream):
    body = stream.read(2 * 1024 * 1024).decode("utf-8")
    _, marker, data = body.partition("const c31Data=")
    if not marker:
        raise ValueError("C31 guide has no programme data")
    rows, _ = json.JSONDecoder().raw_decode(data.lstrip())
    melbourne = ZoneInfo("Australia/Melbourne")
    starts = [
        datetime.fromisoformat(f"{row['date']}T{row['time']}").replace(tzinfo=melbourne)
        for row in rows
    ]
    if starts != sorted(set(starts)):
        raise ValueError("C31 guide is out of order")
    for row, start, stop in zip(rows, starts, starts[1:]):
        programme = ET.Element("programme", {
            "channel": "C31 Melbourne TV Guide",
            "start": start.strftime("%Y%m%d%H%M%S %z"),
            "stop": stop.strftime("%Y%m%d%H%M%S %z"),
        })
        ET.SubElement(programme, "title", lang="en").text = row["program"]
        if row.get("description"):
            ET.SubElement(programme, "desc", lang="en").text = row["description"]
        if row.get("genre"):
            ET.SubElement(programme, "category", lang="en").text = row["genre"]
        yield programme


def fetch_programmes(source, wanted, now):
    req = urllib.request.Request(source["url"], headers={"User-Agent": "tivo4k-guide/1.0"})
    with urllib.request.urlopen(req, timeout=45) as response:
        if source.get("format") == "c31":
            return collect_programmes(c31_programmes(response), wanted, now)
        with io.BufferedReader(response) as stream:
            if stream.peek(2).startswith(b"\x1f\x8b"):
                with gzip.GzipFile(fileobj=stream) as uncompressed:
                    return collect_programmes(iter_xmltv(uncompressed), wanted, now)
            return collect_programmes(iter_xmltv(stream), wanted, now)


def choose_schedules(programmes, mappings, now):
    selected = {}
    cutoff = now + COVERAGE_WINDOW
    for channel, source_ids in mappings.items():
        for source_id in source_ids:
            entries = programmes.get(source_id, {})
            if any(start <= cutoff for start in entries):
                selected[channel] = entries
                break
    return selected


def fill_gaps(root, schedules, channels, covered):
    """Replace stale entries only for channels with a usable replacement."""
    if not schedules:
        return
    root[:] = [
        element for element in root
        if element.tag != "programme" or element.get("channel") not in schedules
    ]
    existing_channels = root.findall("channel")
    known = {channel.get("id") for channel in existing_channels}
    position = len(existing_channels)
    for channel, programmes in sorted(schedules.items()):
        if channel not in known:
            entry = ET.Element("channel", {"id": channel})
            ET.SubElement(entry, "display-name").text = channels[channel]
            root.insert(position, entry)
            position += 1
        for _, programme in sorted(programmes.items()):
            entry = ET.SubElement(root, "programme", {**programme.attrib, "channel": channel})
            entry.extend(programme)
    covered.update(schedules)


def add_remote_sources(root, sources, channels, covered, now):
    """Fetch each configured feed once. Earlier sources take precedence."""
    pending = []
    for source in sources:
        overrides = set(source.get("override_channels", []))
        force_all = bool(source.get("override"))
        mappings = {
            channel: ids
            for channel, ids in source["channels"].items()
            if channel in channels and (force_all or channel in overrides or channel not in covered)
        }
        if mappings:
            pending.append((source, mappings, force_all, overrides))

    def load(item):
        source, mappings, _, _ = item
        wanted = {source_id for ids in mappings.values() for source_id in ids}
        try:
            return fetch_programmes(source, wanted, now)
        except (OSError, HTTPException, EOFError, ValueError, ET.ParseError, KeyError) as error:
            print(f"Guide source unavailable ({source['url']}): {error}")
            return {}

    with ThreadPoolExecutor(max_workers=4) as pool:
        for (source, mappings, force_all, overrides), programmes in zip(pending, pool.map(load, pending)):
            remaining = {
                channel: ids
                for channel, ids in mappings.items()
                if force_all or channel in overrides or channel not in covered
            }
            schedules = choose_schedules(programmes, remaining, now)
            fill_gaps(root, schedules, channels, covered)
            print(f"{source['url']}: filled {len(schedules)}/{len(remaining)} channel gaps")
