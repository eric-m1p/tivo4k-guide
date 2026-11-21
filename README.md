# tivo4k-guide

XMLTV TV guide (`guide.xml.gz`) for a personal IPTV playlist built from the
public [iptv-org](https://github.com/iptv-org) index. Listings come from
iptv-org/epg grabbers and i.mjh.nz (Pluto TV, Samsung TV Plus).

Guide URL for TiviMate and other players:

    https://raw.githubusercontent.com/eric-m1p/tivo4k-guide/main/guide.xml.gz

Live/upcoming sports & 24/7 channels feed for Stream Sites TV:

    https://raw.githubusercontent.com/eric-m1p/tivo4k-guide/main/events.json

## How it updates

### Live Events Feed (`events.json`)

A GitHub Actions job (`.github/workflows/events.yml`) runs `distill_events.py`
every 30 minutes directly from GitHub's runners (pulling upstream APIs and
committing `events.json` without any local machine in the loop).

Run it by hand from the Actions tab ("Refresh Live Events Feed", then "Run workflow").

#### Feed contract v2

`events.json` follows `schema/events.v2.schema.json` (`schema_version: 2`). v2 only
adds fields, so v1 readers keep working. The pipeline decides; the apps render:

- `feed_v2.py` runs after the providers are merged: cleans titles, merges duplicate
  rows of one fixture (`aliases` keeps the merged keys), sets `sport`, `group`,
  `detail` and `icon`, pretty-prints league codes, moves 24/7 sources labeled as
  another show, moves mirrors whose URL names another game, flags a channel stream
  attached to two overlapping games (`shared_channel`, ranked last), and matches
  ESPN scoreboards (`teams`, `espn`, live scores, start-time check, finished games
  dropped). After the source-health pass it maps each source to its `site` and
  wraps everything with `sites`, `policy` and `config`.
- `feed_policy.json` holds the admin hooks: sport groups and their order, health
  and weak-source thresholds (`source_health.py` reads them too), site and feature
  kill switches, hidden groups (global and per device), and the oldest app build
  that reads the feed.
- `distill_events.py` refuses to write a feed that fails the schema.
  `python3 feed_v2.py validate [path-or-url]` checks one by hand;
  `python3 -m unittest test_feed_v2` runs the rules' tests (CI runs both).

### TV Guide (`guide.xml.gz`)

A GitHub Actions job (`.github/workflows/guide.yml`) rebuilds the guide every
day around 2-3 AM Pacific and commits it when it changes. It uses:

- `tivo4k.m3u`: the playlist's channel lines only (names, guide IDs, logos; no stream links)
- `make_epg_channels.py`: picks a listings source per channel from the iptv-org index
- `merge_guides.py`: merges grabbed listings with Pluto TV / Samsung TV Plus guides
- `epg_sources.json`: matches playlist IDs to channel IDs from supplemental public schedules
- `supplemental_epg.py`: fetches and checks the supplemental programmes before merging

The rebuild counts a channel as covered when it has a programme airing now or
starting in the next 24 hours. Supplemental schedules fill gaps after the main
grabbers. If a source fails, the rebuild reuses any listings in the previous
guide that are still current.

Run it by hand from the Actions tab ("Rebuild TV guide", then "Run workflow").
