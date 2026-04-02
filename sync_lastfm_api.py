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
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    api_key = os.environ.get("LASTFM_API_KEY", "").strip()
    if not api_key:
        log.warning("LASTFM_API_KEY is not set. Skipping API sync.")
        return

    cache_path = Path(args.cache)
    aggregates_path = Path(args.aggregates)

    cache = load_cache(cache_path, args.user)
    last_synced = parse_int(cache.get("last_synced_uts"), 0)
    overlap = max(0, args.lookback_hours) * 3600
    from_uts = max(0, last_synced - overlap) if last_synced else 0

    if last_synced:
        log.info("Incremental sync from uts=%d (last_synced=%d, overlap=%dh)", from_uts, last_synced, args.lookback_hours)
    else:
        log.info("No existing cache found. Performing initial bootstrap sync.")

    incoming, pages = fetch_recent_tracks_incremental(
        username=args.user,
        api_key=api_key,
        from_uts=from_uts,
        limit=args.limit,
        max_pages=args.max_pages,
        delay_ms=args.delay_ms,
        timeout=args.timeout,
    )

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
        "fetched_scrobbles": len(incoming),
        "added_scrobbles": merge_stats["added"],
        "updated_scrobbles": merge_stats["updated"],
        "cache_size": len(merged),
        "from_uts": from_uts,
        "last_synced_uts": last_synced_uts,
    }
    write_json(aggregates_path, aggregates)

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
