#!/usr/bin/env python3
"""Incrementally sync Last.fm scrobbles via API and compute dashboard aggregates."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

LASTFM_API_BASE = "https://ws.audioscrobbler.com/2.0/"


def parse_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    digits = re.sub(r"[^0-9]", "", str(value))
    return int(digits) if digits else default


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _field_text(raw: Any) -> str:
    if isinstance(raw, dict):
        return _normalize_text(str(raw.get("#text") or raw.get("name") or ""))
    return _normalize_text(str(raw or ""))


def _lastfm_music_url(*parts: str) -> str:
    encoded = [urllib.parse.quote((p or "").strip(), safe="") for p in parts if (p or "").strip()]
    return f"https://www.last.fm/music/{'/'.join(encoded)}" if encoded else ""


def request_json(url: str, timeout: int = 30, max_retries: int = 5) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json,text/javascript,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(body)
                if isinstance(payload, dict):
                    return payload
                raise RuntimeError("Unexpected JSON payload type")
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                delay = 2 ** attempt
                log.warning(
                    "HTTP %d for %s, retrying in %.1fs (attempt %d/%d)",
                    exc.code,
                    url,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay)
                last_exc = exc
                continue
            raise
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            if attempt < max_retries - 1:
                delay = 2 ** attempt
                log.warning("Network/json error for %s, retrying in %.1fs: %s", url, delay, exc)
                time.sleep(delay)
                last_exc = exc
                continue
            raise
    raise RuntimeError(f"All retries failed for URL: {url}") from last_exc


def lastfm_api_call(
    method: str,
    api_key: str,
    params: dict[str, Any],
    timeout: int = 30,
    max_retries: int = 5,
) -> dict[str, Any]:
    payload = {
        "method": method,
        "api_key": api_key,
        "format": "json",
        **params,
    }

    for attempt in range(max_retries):
        query = urllib.parse.urlencode(payload)
        url = f"{LASTFM_API_BASE}?{query}"
        data = request_json(url, timeout=timeout, max_retries=max_retries)
        err_code = parse_int(data.get("error"), 0)
        if not err_code:
            return data

        retriable = {11, 16, 29}
        message = str(data.get("message") or "unknown error")
        if err_code in retriable and attempt < max_retries - 1:
            delay = 2 ** attempt
            log.warning(
                "Last.fm API error %d (%s), retrying in %.1fs (attempt %d/%d)",
                err_code,
                message,
                delay,
                attempt + 1,
                max_retries,
            )
            time.sleep(delay)
            continue
        raise RuntimeError(f"Last.fm API error {err_code}: {message}")

    raise RuntimeError(f"Last.fm API call failed after retries: {method}")


def normalize_scrobble(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    attr = raw.get("@attr") if isinstance(raw.get("@attr"), dict) else {}
    if str(attr.get("nowplaying") or "").lower() == "true":
        return None

    date_raw = raw.get("date") if isinstance(raw.get("date"), dict) else {}
    uts = parse_int(date_raw.get("uts"), 0)
    if not uts:
        return None

    artist = _field_text(raw.get("artist"))
    track = _field_text(raw.get("name"))
    album = _field_text(raw.get("album"))
    if not track:
        return None

    track_url = str(raw.get("url") or "").strip()
    if track_url and track_url.startswith("/"):
        track_url = "https://www.last.fm" + track_url
    if not track_url:
        track_url = _lastfm_music_url(artist, track)

    return {
        "uts": uts,
        "track": track,
        "artist": artist,
        "album": album,
        "track_url": track_url,
    }


def scrobble_key(item: dict[str, Any]) -> tuple[int, str, str, str]:
    return (
        parse_int(item.get("uts"), 0),
        _normalize_text(str(item.get("artist") or "")).lower(),
        _normalize_text(str(item.get("track") or "")).lower(),
        _normalize_text(str(item.get("album") or "")).lower(),
    )


def fetch_recent_tracks_incremental(
    username: str,
    api_key: str,
    from_uts: int,
    limit: int = 200,
    max_pages: int = 0,
    delay_ms: int = 120,
    timeout: int = 30,
) -> tuple[list[dict[str, Any]], int]:
    page = 1
    total_pages = 1
    output: list[dict[str, Any]] = []

    while True:
        params: dict[str, Any] = {
            "user": username,
            "limit": max(1, min(limit, 200)),
            "page": page,
            "extended": 0,
        }
        if from_uts > 0:
            params["from"] = from_uts

        payload = lastfm_api_call(
            "user.getrecenttracks",
            api_key,
            params,
            timeout=timeout,
        )

        recent = payload.get("recenttracks") if isinstance(payload.get("recenttracks"), dict) else {}
        rows = recent.get("track")
        attr = recent.get("@attr") if isinstance(recent.get("@attr"), dict) else {}
        total_pages = max(1, parse_int(attr.get("totalPages"), 1))

        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            rows = []

        normalized_count = 0
        for row in rows:
            norm = normalize_scrobble(row)
            if norm:
                output.append(norm)
                normalized_count += 1

        if page == 1:
            total_items = parse_int(attr.get("total"), 0)
            log.info(
                "API window: total=%d, totalPages=%d, from_uts=%d",
                total_items,
                total_pages,
                from_uts,
            )

        if page % 25 == 0:
            log.info("Fetched page %d/%d (new scrobbles in page=%d)", page, total_pages, normalized_count)

        if max_pages > 0 and page >= max_pages:
            break
        if page >= total_pages:
            break

        page += 1
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    return output, page


def load_cache(path: Path, username: str) -> dict[str, Any]:
    default = {
        "username": username,
        "updated_at_utc": "",
        "last_synced_uts": 0,
        "scrobbles": [],
    }
    if not path.exists():
        return default

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Could not read cache %s: %s", path, exc)
        return default

    if not isinstance(raw, dict):
        return default

    cached_user = str(raw.get("username") or "").strip()
    if cached_user and cached_user.lower() != username.lower():
        log.warning("Cache user mismatch (%s != %s), rebuilding cache", cached_user, username)
        return default

    rows = raw.get("scrobbles")
    if not isinstance(rows, list):
        rows = []

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        uts = parse_int(row.get("uts"), 0)
        track = _normalize_text(str(row.get("track") or ""))
        if not uts or not track:
            continue
        normalized_rows.append(
            {
                "uts": uts,
                "track": track,
                "artist": _normalize_text(str(row.get("artist") or "")),
                "album": _normalize_text(str(row.get("album") or "")),
                "track_url": str(row.get("track_url") or "").strip(),
            }
        )

    last_synced = parse_int(raw.get("last_synced_uts"), 0)
    if normalized_rows:
        max_uts = max(parse_int(r.get("uts"), 0) for r in normalized_rows)
        if max_uts > last_synced:
            last_synced = max_uts

    normalized_rows.sort(key=lambda x: parse_int(x.get("uts"), 0), reverse=True)
    return {
        "username": username,
        "updated_at_utc": str(raw.get("updated_at_utc") or ""),
        "last_synced_uts": last_synced,
        "scrobbles": normalized_rows,
    }


def parse_uts(value: Any) -> int:
    if isinstance(value, (int, float)):
        uts = int(value)
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return 0
        if s.isdigit():
            uts = int(s)
        else:
            try:
                uts = int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
            except ValueError:
                return 0
    elif isinstance(value, dict):
        if "uts" in value:
            return parse_uts(value.get("uts"))
        return 0
    else:
        return 0

    # Handle millisecond timestamps.
    if uts > 10_000_000_000:
        uts //= 1000
    return uts if uts > 0 else 0


def _extract_seed_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("recenttracks"), dict):
        tracks = payload["recenttracks"].get("track")
        if isinstance(tracks, dict):
            return [tracks]
        if isinstance(tracks, list):
            return [item for item in tracks if isinstance(item, dict)]

    for key in ("scrobbles", "recent_tracks", "tracks", "items", "data"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]

    for value in payload.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_seed_rows(value)
            if nested:
                return nested

    return []


def normalize_seed_scrobble(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    attr = raw.get("@attr") if isinstance(raw.get("@attr"), dict) else {}
    if str(attr.get("nowplaying") or "").lower() == "true":
        return None

    track_obj = raw.get("track") if isinstance(raw.get("track"), dict) else {}
    date_obj = raw.get("date") if isinstance(raw.get("date"), dict) else {}

    uts = 0
    for candidate in (
        raw.get("uts"),
        raw.get("timestamp"),
        raw.get("time"),
        raw.get("played_at"),
        raw.get("date_uts"),
        date_obj.get("uts"),
        raw.get("date"),
    ):
        uts = parse_uts(candidate)
        if uts:
            break
    if not uts:
        return None

    track = _field_text(raw.get("name") or raw.get("track_name") or raw.get("title") or raw.get("track"))
    if not track:
        track = _field_text(track_obj.get("name") or track_obj.get("title"))
    artist = _field_text(raw.get("artist_name") or raw.get("artist") or raw.get("artistName"))
    if not artist:
        artist = _field_text(track_obj.get("artist"))
    album = _field_text(raw.get("album_name") or raw.get("album") or raw.get("albumName"))
    if not album:
        album = _field_text(track_obj.get("album"))
    if not track:
        return None

    track_url = str(
        raw.get("track_url")
        or raw.get("url")
        or raw.get("lastfm_url")
        or raw.get("lastfmUrl")
        or track_obj.get("url")
        or ""
    ).strip()
    if track_url.startswith("/"):
        track_url = "https://www.last.fm" + track_url
    if not track_url:
        track_url = _lastfm_music_url(artist, track)

    return {
        "uts": uts,
        "track": track,
        "artist": artist,
        "album": album,
        "track_url": track_url,
    }


def load_seed_scrobbles(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        log.warning("Seed export file not found: %s", path)
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Could not read seed export %s: %s", path, exc)
        return []

    rows = _extract_seed_rows(payload)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = normalize_seed_scrobble(row)
        if item:
            normalized.append(item)

    normalized.sort(key=lambda x: parse_int(x.get("uts"), 0), reverse=True)
    log.info(
        "Loaded seed export %s: raw_rows=%d normalized_scrobbles=%d",
        path,
        len(rows),
        len(normalized),
    )
    return normalized


def discover_seed_export(username: str, root: Path) -> Path | None:
    lower_user = username.lower()
    pattern_candidates = [
        *root.glob("lastfmstats-*.json"),
        *root.glob("recenttracks-*.json"),
    ]
    candidates = sorted(pattern_candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None

    for path in candidates:
        if lower_user in path.name.lower():
            return path
    return candidates[0]


def merge_scrobbles(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    merged = list(existing)
    seen = {scrobble_key(item): idx for idx, item in enumerate(merged)}
    added = 0
    updated = 0

    for item in incoming:
        key = scrobble_key(item)
        idx = seen.get(key)
        if idx is None:
            seen[key] = len(merged)
            merged.append(item)
            added += 1
            continue

        current = merged[idx]
        if not current.get("track_url") and item.get("track_url"):
            current["track_url"] = item["track_url"]
            updated += 1

    merged.sort(key=lambda x: parse_int(x.get("uts"), 0), reverse=True)
    return merged, {"added": added, "updated": updated}


def build_listening_clock(scrobbles: list[dict[str, Any]], cutoff_uts: int) -> list[dict[str, Any]]:
    by_hour = [0] * 24
    for row in scrobbles:
        uts = parse_int(row.get("uts"), 0)
        if uts < cutoff_uts:
            continue
        hour = dt.datetime.fromtimestamp(uts, tz=dt.timezone.utc).hour
        by_hour[hour] += 1

    result: list[dict[str, Any]] = []
    for h in range(24):
        label = f"{h:02d}:00"
        result.append(
            {
                "hour": label,
                "hour24": h,
                "hour24_label": label,
                "scrobbles": by_hour[h],
            }
        )
    return result


def build_aggregates(
    scrobbles: list[dict[str, Any]],
    top_limit: int = 20,
    recent_limit: int = 40,
) -> dict[str, Any]:
    rows = sorted(scrobbles, key=lambda x: parse_int(x.get("uts"), 0), reverse=True)
    artist_counter: Counter[str] = Counter()
    album_counter: Counter[tuple[str, str]] = Counter()
    track_counter: Counter[tuple[str, str]] = Counter()

    track_meta: dict[tuple[str, str], dict[str, str]] = {}
    album_meta: dict[tuple[str, str], dict[str, str]] = {}

    for row in rows:
        artist = _normalize_text(str(row.get("artist") or ""))
        album = _normalize_text(str(row.get("album") or ""))
        track = _normalize_text(str(row.get("track") or ""))
        track_url = str(row.get("track_url") or "").strip()

        if artist:
            artist_counter[artist] += 1
        if album:
            album_key = (album, artist)
            album_counter[album_key] += 1
            if album_key not in album_meta:
                album_meta[album_key] = {
                    "url": _lastfm_music_url(artist, album),
                    "artist_url": _lastfm_music_url(artist),
                }

        track_key = (track, artist)
        track_counter[track_key] += 1
        if track_key not in track_meta:
            track_meta[track_key] = {
                "url": track_url or _lastfm_music_url(artist, track),
                "artist_url": _lastfm_music_url(artist),
            }

    top_artists = [
        {
            "rank": i,
            "name": name,
            "scrobbles": count,
            "url": _lastfm_music_url(name),
            "youtube_url": "",
            "spotify_type": "artist",
            "spotify_id": "",
            "spotify_uri": "",
            "spotify_url": "",
        }
        for i, (name, count) in enumerate(
            sorted(artist_counter.items(), key=lambda x: (-x[1], x[0].lower()))[:top_limit],
            start=1,
        )
    ]

    top_albums = []
    for i, ((album, artist), count) in enumerate(
        sorted(album_counter.items(), key=lambda x: (-x[1], x[0][0].lower(), x[0][1].lower()))[:top_limit],
        start=1,
    ):
        meta = album_meta.get((album, artist), {})
        top_albums.append(
            {
                "rank": i,
                "name": album,
                "artist": artist,
                "scrobbles": count,
                "url": str(meta.get("url") or ""),
                "artist_url": str(meta.get("artist_url") or ""),
                "youtube_url": "",
                "spotify_type": "album",
                "spotify_id": "",
                "spotify_uri": "",
                "spotify_url": "",
            }
        )

    top_tracks = []
    for i, ((track, artist), count) in enumerate(
        sorted(track_counter.items(), key=lambda x: (-x[1], x[0][0].lower(), x[0][1].lower()))[:top_limit],
        start=1,
    ):
        meta = track_meta.get((track, artist), {})
        top_tracks.append(
            {
                "rank": i,
                "name": track,
                "artist": artist,
                "scrobbles": count,
                "url": str(meta.get("url") or ""),
                "artist_url": str(meta.get("artist_url") or ""),
                "youtube_url": "",
                "spotify_type": "track",
                "spotify_id": "",
                "spotify_uri": "",
                "spotify_url": "",
            }
        )

    recent_tracks = []
    for row in rows[:recent_limit]:
        recent_tracks.append(
            {
                "track": str(row.get("track") or ""),
                "track_url": str(row.get("track_url") or ""),
                "artist": str(row.get("artist") or ""),
                "artist_url": _lastfm_music_url(str(row.get("artist") or "")),
                "timestamp": parse_int(row.get("uts"), 0),
                "youtube_url": "",
                "spotify_type": "track",
                "spotify_id": "",
                "spotify_uri": "",
                "spotify_url": "",
            }
        )

    now_uts = int(time.time())
    week_cutoff = now_uts - 7 * 86400
    scrobbles_this_week = sum(1 for row in rows if parse_int(row.get("uts"), 0) >= week_cutoff)

    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "lastfm_api_incremental",
        "profile": {
            "total_scrobbles": len(rows),
            "total_artists": len(artist_counter),
        },
        "weekly_report": {
            "scrobbles_this_week": scrobbles_this_week,
            "listening_clock": build_listening_clock(rows, week_cutoff),
        },
        "top_artists": top_artists,
        "top_albums": top_albums,
        "top_tracks": top_tracks,
        "recent_tracks": recent_tracks,
    }


def to_full_export(username: str, scrobbles: list[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(scrobbles, key=lambda x: parse_int(x.get("uts"), 0), reverse=True)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        uts = parse_int(row.get("uts"), 0)
        export_rows.append(
            {
                "track": str(row.get("track") or ""),
                "artist": str(row.get("artist") or ""),
                "album": str(row.get("album") or ""),
                "albumId": "",
                "date": uts * 1000 if uts else 0,
            }
        )
    return {"username": username, "scrobbles": export_rows}


def _normalize_comp_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    uts = parse_uts(raw.get("date") or raw.get("uts") or raw.get("timestamp") or raw.get("time"))
    if not uts:
        return None
    track = _normalize_text(str(raw.get("track") or raw.get("name") or raw.get("title") or ""))
    artist = _normalize_text(str(raw.get("artist") or raw.get("artist_name") or ""))
    album = _normalize_text(str(raw.get("album") or raw.get("album_name") or ""))
    if not track:
        return None
    return {"uts": uts, "track": track, "artist": artist, "album": album}


def _comp_key(item: dict[str, Any]) -> tuple[int, str, str, str]:
    return (
        parse_int(item.get("uts"), 0),
        _normalize_text(str(item.get("artist") or "")).lower(),
        _normalize_text(str(item.get("track") or "")).lower(),
        _normalize_text(str(item.get("album") or "")).lower(),
    )


def compare_with_existing_export(
    merged_scrobbles: list[dict[str, Any]],
    existing_export_path: Path,
    sample_size: int = 20,
) -> dict[str, Any]:
    try:
        existing_payload = json.loads(existing_export_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Could not read existing export {existing_export_path}: {exc}") from exc

    existing_raw = existing_payload.get("scrobbles") if isinstance(existing_payload, dict) else []
    if not isinstance(existing_raw, list):
        existing_raw = []

    existing_norm: list[dict[str, Any]] = []
    for row in existing_raw:
        norm = _normalize_comp_row(row if isinstance(row, dict) else {})
        if norm:
            existing_norm.append(norm)

    merged_norm = [
        {
            "uts": parse_int(row.get("uts"), 0),
            "track": _normalize_text(str(row.get("track") or "")),
            "artist": _normalize_text(str(row.get("artist") or "")),
            "album": _normalize_text(str(row.get("album") or "")),
        }
        for row in merged_scrobbles
        if isinstance(row, dict) and parse_int(row.get("uts"), 0) > 0 and str(row.get("track") or "").strip()
    ]

    existing_counter = Counter(_comp_key(x) for x in existing_norm)
    merged_counter = Counter(_comp_key(x) for x in merged_norm)
    existing_keys = set(existing_counter.keys())
    merged_keys = set(merged_counter.keys())

    only_api_keys = merged_keys - existing_keys
    only_existing_keys = existing_keys - merged_keys
    shared_keys = merged_keys & existing_keys

    merged_map = {_comp_key(x): x for x in merged_norm}
    existing_map = {_comp_key(x): x for x in existing_norm}

    def _render_row(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "uts": parse_int(item.get("uts"), 0),
            "date_utc": dt.datetime.fromtimestamp(parse_int(item.get("uts"), 0), tz=dt.timezone.utc).isoformat(),
            "track": str(item.get("track") or ""),
            "artist": str(item.get("artist") or ""),
            "album": str(item.get("album") or ""),
        }

    only_api_rows = sorted(
        (_render_row(merged_map[k]) for k in only_api_keys if k in merged_map),
        key=lambda x: x["uts"],
        reverse=True,
    )
    only_existing_rows = sorted(
        (_render_row(existing_map[k]) for k in only_existing_keys if k in existing_map),
        key=lambda x: x["uts"],
        reverse=True,
    )

    merged_latest = max((parse_int(x.get("uts"), 0) for x in merged_norm), default=0)
    existing_latest = max((parse_int(x.get("uts"), 0) for x in existing_norm), default=0)
    merged_oldest = min((parse_int(x.get("uts"), 0) for x in merged_norm), default=0)
    existing_oldest = min((parse_int(x.get("uts"), 0) for x in existing_norm), default=0)

    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "existing_export": str(existing_export_path),
        "api_total_rows": len(merged_norm),
        "existing_total_rows": len(existing_norm),
        "api_unique_rows": len(merged_keys),
        "existing_unique_rows": len(existing_keys),
        "api_duplicate_rows": len(merged_norm) - len(merged_keys),
        "existing_duplicate_rows": len(existing_norm) - len(existing_keys),
        "shared_rows": len(shared_keys),
        "only_api_rows": len(only_api_keys),
        "only_existing_rows": len(only_existing_keys),
        "api_latest_uts": merged_latest,
        "existing_latest_uts": existing_latest,
        "api_oldest_uts": merged_oldest,
        "existing_oldest_uts": existing_oldest,
        "sample_only_api": only_api_rows[:sample_size],
        "sample_only_existing": only_existing_rows[:sample_size],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Incrementally sync Last.fm scrobbles and build aggregates.")
    parser.add_argument("--user", default="TOOUUR", help="Last.fm username")
    parser.add_argument("--cache", default="raw_snapshot/api_scrobbles_cache.json", help="Path to persistent scrobble cache")
    parser.add_argument("--aggregates", default="raw_snapshot/api_aggregates.json", help="Path to aggregate output JSON")
    parser.add_argument("--lookback-hours", type=int, default=72, help="Overlap window in hours for incremental sync")
    parser.add_argument("--limit", type=int, default=200, help="Last.fm page size (max 200)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages per run (0 = all pages)")
    parser.add_argument("--delay-ms", type=int, default=120, help="Delay between page requests in milliseconds")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument(
        "--full-export",
        default="raw_snapshot/all_scrobbles_api.json",
        help="Path to full exported scrobbles JSON",
    )
    parser.add_argument(
        "--skip-full-export",
        action="store_true",
        help="Skip writing full exported scrobbles JSON",
    )
    parser.add_argument(
        "--compare-with",
        default="",
        help="Optional existing export JSON to compare against",
    )
    parser.add_argument(
        "--diff-output",
        default="raw_snapshot/scrobbles_diff.json",
        help="Path to diff output JSON when --compare-with is set",
    )
    parser.add_argument(
        "--seed-export",
        default="",
        help="Optional seed JSON export path (used when cache is empty)",
    )
    parser.add_argument(
        "--skip-seed-export",
        action="store_true",
        help="Do not import seed export even when cache is empty",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cache_path = Path(args.cache)
    aggregates_path = Path(args.aggregates)
    cache = load_cache(cache_path, args.user)
    seed_added = 0

    if not args.skip_seed_export and not cache.get("scrobbles"):
        seed_path: Path | None
        if args.seed_export:
            seed_path = Path(args.seed_export)
        else:
            seed_path = discover_seed_export(args.user, Path.cwd())

        if seed_path:
            seed_rows = load_seed_scrobbles(seed_path)
            if seed_rows:
                merged_seed, seed_stats = merge_scrobbles(cache.get("scrobbles", []), seed_rows)
                cache["scrobbles"] = merged_seed
                cache["last_synced_uts"] = max((parse_int(row.get("uts"), 0) for row in merged_seed), default=0)
                seed_added = seed_stats["added"]
                log.info("Seed import applied: +%d scrobbles from %s", seed_added, seed_path)
        elif args.seed_export:
            log.warning("Configured seed export does not exist: %s", args.seed_export)

    api_key = os.environ.get("LASTFM_API_KEY", "").strip()
    incoming: list[dict[str, Any]] = []
    pages = 0

    last_synced = parse_int(cache.get("last_synced_uts"), 0)
    overlap = max(0, args.lookback_hours) * 3600
    from_uts = max(0, last_synced - overlap) if last_synced else 0

    if api_key:
        if last_synced:
            log.info(
                "Incremental sync from uts=%d (last_synced=%d, overlap=%dh)",
                from_uts,
                last_synced,
                args.lookback_hours,
            )
        else:
            log.info("No existing cache found. Performing initial API bootstrap sync.")

        incoming, pages = fetch_recent_tracks_incremental(
            username=args.user,
            api_key=api_key,
            from_uts=from_uts,
            limit=args.limit,
            max_pages=args.max_pages,
            delay_ms=args.delay_ms,
            timeout=args.timeout,
        )
    else:
        log.warning("LASTFM_API_KEY is not set. API fetch skipped; using cached/seed data only.")

    merged, merge_stats = merge_scrobbles(cache.get("scrobbles", []), incoming)
    last_synced_uts = max((parse_int(row.get("uts"), 0) for row in merged), default=0)

    cache_payload = {
        "username": args.user,
        "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "last_synced_uts": last_synced_uts,
        "scrobbles": merged,
    }
    write_json(cache_path, cache_payload)

    aggregates = build_aggregates(merged, top_limit=50, recent_limit=40)
    aggregates["sync"] = {
        "pages_fetched": pages,
        "api_fetch_enabled": bool(api_key),
        "fetched_scrobbles": len(incoming),
        "seed_imported_scrobbles": seed_added,
        "added_scrobbles": merge_stats["added"],
        "updated_scrobbles": merge_stats["updated"],
        "cache_size": len(merged),
        "from_uts": from_uts,
        "last_synced_uts": last_synced_uts,
    }
    write_json(aggregates_path, aggregates)

    if not args.skip_full_export:
        full_export = to_full_export(args.user, merged)
        write_json(Path(args.full_export), full_export)
        log.info("Full export written: %s", Path(args.full_export).resolve())

    if args.compare_with:
        compare_path = Path(args.compare_with)
        if compare_path.exists():
            diff = compare_with_existing_export(merged, compare_path)
            write_json(Path(args.diff_output), diff)
            log.info(
                "Diff written: %s (only_api=%d, only_existing=%d, shared=%d)",
                Path(args.diff_output).resolve(),
                parse_int(diff.get("only_api_rows"), 0),
                parse_int(diff.get("only_existing_rows"), 0),
                parse_int(diff.get("shared_rows"), 0),
            )
        else:
            log.warning("Compare file not found: %s", compare_path)

    log.info(
        "Sync complete: fetched=%d added=%d updated=%d total=%d",
        len(incoming),
        merge_stats["added"],
        merge_stats["updated"],
        len(merged),
    )
    log.info("Cache written: %s", cache_path.resolve())
    log.info("Aggregates written: %s", aggregates_path.resolve())


if __name__ == "__main__":
    main()
