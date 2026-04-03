# Last.fm Dashboard

Static dashboard generator inspired by the Foursquare dashboard layout, fed from public Last.fm profile pages.

## Features

- Scrapes Last.fm public pages (overview, artists, albums, tracks, weekly report)
- Enriches albums with playback previews via Spotify API (optional)
- Outputs a single self-contained `index.html` + `lastfm_data.json`
- Dark / light theme toggle with localStorage persistence
- Live search/filter on all ranked lists
- Fully keyboard-navigable and screen-reader accessible (ARIA)
- Mobile-friendly (44 px touch targets, responsive layout)

## Requirements

```
Python 3.10+
beautifulsoup4      # recommended — pip install beautifulsoup4
                    # (falls back to regex parsing without it)
```

## Setup

### Spotify enrichment (optional)

Create a [Spotify app](https://developer.spotify.com/dashboard) and export the credentials as environment variables before running:

```bash
export SPOTIFY_CLIENT_ID=your_client_id
export SPOTIFY_CLIENT_SECRET=your_client_secret
```

Without these, the dashboard is generated without album preview URLs.

### Last.fm API incremental accuracy (recommended)

Create a Last.fm API key at [Last.fm API account](https://www.last.fm/api/account/create) and set:

```bash
export LASTFM_API_KEY=your_lastfm_api_key
```

Then run incremental sync before generating the dashboard:

```bash
python sync_lastfm_api.py --user TOOUUR
python build_lastfm_dashboard.py --user TOOUUR --api-aggregates raw_snapshot/api_aggregates.json
```

This keeps a persistent scrobble cache in `raw_snapshot/api_scrobbles_cache.json` and updates only the delta on each run.
It also writes:
- `raw_snapshot/all_scrobbles_api.json` (full normalized searchable scrobbles export)
- `raw_snapshot/scrobbles_diff.json` (diff report vs existing export when `--compare-with` is set)

In GitHub Actions, incremental state is persisted with `actions/cache` (the runner filesystem is ephemeral).

If you have an export such as `lastfmstats-TOOUUR.json` (or `recenttracks-*.json`), it can be used as a bootstrap dataset on the first run:

```bash
python sync_lastfm_api.py --user TOOUUR --seed-export lastfmstats-TOOUUR.json
```

After that, each run increments from new API scrobbles and deduplicates against the seeded cache.

To compare API/full cache against your existing export:

```bash
python sync_lastfm_api.py \
  --user TOOUUR \
  --compare-with lastfmstats-TOOUUR.json \
  --diff-output raw_snapshot/scrobbles_diff.json
```

## Usage

### Generate from live Last.fm pages

```bash
python build_lastfm_dashboard.py --user TOOUUR --save-raw-dir raw_snapshot
```

Outputs `index.html` and `lastfm_data.json` in the current directory.

### Generate from a saved HTML snapshot

```bash
python build_lastfm_dashboard.py --user TOOUUR --input-dir raw_snapshot
```

Does not require network access.

### All CLI flags

| Flag | Default | Description |
|---|---|---|
| `--user USER` | *(required)* | Last.fm username |
| `--input-dir DIR` | — | Use saved HTML snapshot instead of fetching |
| `--save-raw-dir DIR` | — | Save fetched HTML pages to this directory |
| `--output FILE` | `index.html` | Output HTML path |
| `--json-output FILE` | `lastfm_data.json` | Output JSON path |
| `--template FILE` | `template.html` (next to script) | HTML template path |
| `--cache-max-age-days N` | `30` | Stale-cache threshold for album pages |
| `--refresh-cache` | false | Ignore existing album page cache |
| `-v` / `--verbose` | false | Debug-level logging |

## Architecture

```
build_lastfm_dashboard.py   # scraper + data builder
template.html               # Jinja-free HTML template (placeholders: __USERNAME__, __DATA_JSON__)
```

`render_dashboard_html()` does a plain string substitution of `__USERNAME__` and `__DATA_JSON__` in `template.html`, so the template has zero Python dependencies and can be edited freely.

## Data schema (lastfm_data.json)

```jsonc
{
  "username": "TOOUUR",
  "scrobbles": 123456,
  "scrobbling_since": "Jan 2010",
  "recent_tracks": [ { "name": "", "artist": "", "time": "" } ],
  "top_tracks":    [ { "rank": 1, "name": "", "artist": "", "plays": 0 } ],
  "top_albums":    [ { "rank": 1, "name": "", "artist": "", "plays": 0,
                       "preview_url": "", "image_url": "" } ],
  "top_artists":   [ { "rank": 1, "name": "", "plays": 0 } ],
  "weekly_report": { "week": "", "total": 0, "top_track": "", "top_artist": "" }
}
```

## Future

- Combine with Foursquare check-in data to correlate venue visits with listening history (timestamp join on `lastfmstats-TOOUUR.json` scrobbles and Foursquare export)
