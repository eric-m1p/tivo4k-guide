# tivo4k-guide

XMLTV TV guide (`guide.xml.gz`) for a personal IPTV playlist built from the
public [iptv-org](https://github.com/iptv-org) index. Listings come from
iptv-org/epg grabbers and i.mjh.nz (Pluto TV, Samsung TV Plus).

Guide URL for TiviMate and other players:

    https://raw.githubusercontent.com/eric-m1p/tivo4k-guide/main/guide.xml.gz

## How it updates

A GitHub Actions job (`.github/workflows/guide.yml`) rebuilds the guide every
day around 2-3 AM Pacific and commits it when it changes. It uses:

- `tivo4k.m3u`: the playlist's channel lines only (names, guide IDs, logos; no stream links)
- `make_epg_channels.py`: picks a listings source per channel from the iptv-org index
- `merge_guides.py`: merges grabbed listings with Pluto TV / Samsung TV Plus guides

Run it by hand from the Actions tab ("Rebuild TV guide", then "Run workflow").
