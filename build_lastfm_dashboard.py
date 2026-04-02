#!/usr/bin/env python3
"""
Build a static Last.fm dashboard page for a public user profile.

The script can either:
1) Fetch live public pages from last.fm, or
2) Read a local HTML snapshot directory.

Spotify credentials must be set as environment variables:
    SPOTIFY_CLIENT_ID
    SPOTIFY_CLIENT_SECRET
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, TypedDict

try:
    from bs4 import BeautifulSoup
    _BS4 = True
except ImportError:
    _BS4 = False

log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_SPOTIFY_LAST_CALL: float = 0.0
_SPOTIFY_MIN_INTERVAL = 0.13  # ~7 req/s max


# ── TypedDicts ────────────────────────────────────────────────────────────────

class TrackItem(TypedDict, total=False):
    rank: int
    name: str
    url: str
    scrobbles: int
    artist: str
    artist_url: str
    youtube_url: str
    spotify_type: str
    spotify_id: str
    spotify_uri: str
    spotify_url: str
    spotify_preview_url: str
    album_tracks: list[dict[str, Any]]


class AlbumItem(TypedDict, total=False):
    rank: int
    name: str
    url: str
    scrobbles: int
    artist: str
    artist_url: str
    youtube_url: str
    spotify_type: str
    spotify_id: str
    spotify_uri: str
    spotify_url: str
    album_tracks: list[dict[str, Any]]


class ArtistItem(TypedDict, total=False):
    rank: int
    name: str
    url: str
    scrobbles: int
    youtube_url: str
    spotify_type: str
    spotify_id: str
    spotify_uri: str
    spotify_url: str


class ProfileData(TypedDict):
    description: str
    scrobbling_since: str
    total_scrobbles: int
    total_artists: int
    loved_tracks: int


class WeeklyReport(TypedDict):
    scrobbles_this_week: int
    listening_clock: list[dict[str, Any]]


class DashboardData(TypedDict, total=False):
    username: str
    generated_at_utc: str
    profile: ProfileData
    weekly_report: WeeklyReport
    top_artists: list[ArtistItem]
    top_albums: list[AlbumItem]
    top_tracks: list[TrackItem]
    recent_tracks: list[dict[str, Any]]
    spotify_enrichment: dict[str, int]


# ── Network helpers ───────────────────────────────────────────────────────────

def fetch_url(url: str, timeout: int = 30, max_retries: int = 3) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                delay = 2.0 ** attempt
                log.warning(
                    "HTTP %d for %s – retrying in %.1fs (attempt %d/%d)",
                    exc.code, url, delay, attempt + 1, max_retries,
                )
                time.sleep(delay)
                last_exc = exc
            else:
                raise
        except (urllib.error.URLError, OSError) as exc:
            if attempt < max_retries - 1:
                delay = 2.0 ** attempt
                log.warning("Network error for %s – retrying in %.1fs: %s", url, delay, exc)
                time.sleep(delay)
                last_exc = exc
            else:
                raise
    raise RuntimeError(f"All {max_retries} attempts failed for {url}") from last_exc


def _spotify_throttle() -> None:
    global _SPOTIFY_LAST_CALL
    elapsed = time.monotonic() - _SPOTIFY_LAST_CALL
    if elapsed < _SPOTIFY_MIN_INTERVAL:
        time.sleep(_SPOTIFY_MIN_INTERVAL - elapsed)
    _SPOTIFY_LAST_CALL = time.monotonic()


# ── Utilities ─────────────────────────────────────────────────────────────────

def strip_tags(value: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", value, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_int(value: str | None, default: int = 0) -> int:
    if value is None:
        return default
    digits = re.sub(r"[^0-9]", "", str(value))
    return int(digits) if digits else default


def extract_spotify_url(raw_html: str) -> str:
    direct = re.search(r'data-spotify-url="([^"]+)"', raw_html, flags=re.I)
    if direct:
        return html.unescape(direct.group(1))
    link = re.search(
        r"https://open\.spotify\.com/(?:intl-[a-z-]+/)?(?:track|album|artist)/[A-Za-z0-9]+",
        raw_html,
        flags=re.I,
    )
    return html.unescape(link.group(0)) if link else ""


def spotify_extract_id(url_or_uri: str, entity_type: str) -> str:
    if not url_or_uri:
        return ""
    m = re.search(rf"spotify:{re.escape(entity_type)}:([A-Za-z0-9]+)", url_or_uri)
    if m:
        return m.group(1)
    m = re.search(
        rf"open\.spotify\.com/(?:intl-[a-z-]+/)?{re.escape(entity_type)}/([A-Za-z0-9]+)",
        url_or_uri,
        flags=re.I,
    )
    return m.group(1) if m else ""


# ── Spotify API ───────────────────────────────────────────────────────────────

def spotify_get_access_token(client_id: str, client_secret: str, timeout: int = 30) -> str:
    payload = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode("ascii")
    req = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=payload,
        headers={
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": DEFAULT_USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
            return str(data.get("access_token") or "")
    except Exception as exc:
        log.error("Spotify token request failed: %s", exc)
        return ""


def spotify_search_first(
    token: str,
    query: str,
    entity_type: str,
    timeout: int = 25,
    _retry: int = 0,
) -> dict[str, Any]:
    _spotify_throttle()
    params = urllib.parse.urlencode({"q": query, "type": entity_type, "limit": 1})
    req = urllib.request.Request(
        f"https://api.spotify.com/v1/search?{params}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        if exc.code == 429 and _retry < 3:
            retry_after = int(exc.headers.get("Retry-After", "5"))
            log.warning("Spotify rate-limited – sleeping %ds", retry_after)
            time.sleep(retry_after)
            return spotify_search_first(token, query, entity_type, timeout, _retry + 1)
        log.warning("Spotify search HTTP %d for query %r", exc.code, query)
        return {}
    except Exception as exc:
        log.warning("Spotify search failed for %r: %s", query, exc)
        return {}

    bucket = data.get(f"{entity_type}s", {})
    items = bucket.get("items") if isinstance(bucket, dict) else []
    if not isinstance(items, list) or not items:
        return {}
    return items[0] if isinstance(items[0], dict) else {}


def apply_spotify_fields(target: dict[str, Any], entity_type: str, hit: dict[str, Any]) -> bool:
    if not hit:
        return False
    spotify_id = str(hit.get("id") or "")
    spotify_uri = str(hit.get("uri") or "")
    spotify_url = str((hit.get("external_urls") or {}).get("spotify") or "")
    if not spotify_id:
        spotify_id = spotify_extract_id(spotify_uri or spotify_url, entity_type)
    if not spotify_uri and spotify_id:
        spotify_uri = f"spotify:{entity_type}:{spotify_id}"
    if not spotify_url and spotify_id:
        spotify_url = f"https://open.spotify.com/{entity_type}/{spotify_id}"
    if not spotify_id:
        return False
    target["spotify_type"] = entity_type
    target["spotify_id"] = spotify_id
    target["spotify_uri"] = spotify_uri
    target["spotify_url"] = spotify_url
    if preview := hit.get("preview_url"):
        target["spotify_preview_url"] = str(preview)
    return True


def enrich_spotify_playback(
    data: dict[str, Any], client_id: str, client_secret: str
) -> dict[str, int]:
    token = spotify_get_access_token(client_id, client_secret)
    if not token:
        log.error("Could not obtain Spotify access token – skipping enrichment")
        return {"tracks": 0, "albums": 0, "artists": 0}

    stats = {"tracks": 0, "albums": 0, "artists": 0}
    track_cache: dict[tuple[str, str], dict[str, Any]] = {}
    album_cache: dict[tuple[str, str], dict[str, Any]] = {}
    artist_cache: dict[str, dict[str, Any]] = {}

    def enrich_track(item: dict[str, Any], title_key: str = "name") -> None:
        title = str(item.get(title_key) or item.get("track") or "").strip()
        artist = str(item.get("artist") or "").strip()
        if not title or item.get("spotify_id"):
            return
        key = (title.lower(), artist.lower())
        hit = track_cache.get(key)
        if hit is None:
            q = f'track:"{title}"' + (f' artist:"{artist}"' if artist else "")
            hit = spotify_search_first(token, q, "track")
            if not hit and artist:
                hit = spotify_search_first(token, f"{artist} {title}", "track")
            track_cache[key] = hit or {}
        if apply_spotify_fields(item, "track", hit or {}):
            stats["tracks"] += 1

    for track in data.get("top_tracks", []):
        enrich_track(track, title_key="name")
    for track in data.get("recent_tracks", []):
        enrich_track(track, title_key="track")

    for album in data.get("top_albums", []):
        name = str(album.get("name") or "").strip()
        artist = str(album.get("artist") or "").strip()
        if not name or album.get("spotify_id"):
            continue
        key = (name.lower(), artist.lower())
        hit = album_cache.get(key)
        if hit is None:
            q = f'album:"{name}"' + (f' artist:"{artist}"' if artist else "")
            hit = spotify_search_first(token, q, "album")
            if not hit and artist:
                hit = spotify_search_first(token, f"{artist} {name}", "album")
            album_cache[key] = hit or {}
        if apply_spotify_fields(album, "album", hit or {}):
            stats["albums"] += 1

    for artist in data.get("top_artists", []):
        name = str(artist.get("name") or "").strip()
        if not name or artist.get("spotify_id"):
            continue
        hit = artist_cache.get(name.lower())
        if hit is None:
            hit = spotify_search_first(token, f'artist:"{name}"', "artist")
            if not hit:
                hit = spotify_search_first(token, name, "artist")
            artist_cache[name.lower()] = hit or {}
        if apply_spotify_fields(artist, "artist", hit or {}):
            stats["artists"] += 1

    log.info(
        "Spotify enrichment: tracks=%d albums=%d artists=%d",
        stats["tracks"], stats["albums"], stats["artists"],
    )
    return stats


# ── HTML parsing (BeautifulSoup with regex fallback) ──────────────────────────

def _soup(html_text: str) -> "BeautifulSoup":
    return BeautifulSoup(html_text, "html.parser")


def extract_meta_description(overview_html: str) -> str:
    if _BS4:
        tag = _soup(overview_html).find("meta", {"name": "description"})
        return (tag.get("content") or "").strip() if tag else ""  # type: ignore[union-attr]
    m = re.search(r'<meta name="description" content="([^"]+)"', overview_html)
    return html.unescape(m.group(1)).strip() if m else ""


def extract_scrobbling_since(overview_html: str) -> str:
    if _BS4:
        tag = _soup(overview_html).find(class_="header-scrobble-since")
        if not tag:
            return ""
        text = tag.get_text(" ", strip=True).lstrip("•").strip()
        return re.sub(r"^scrobbling since\s+", "", text, flags=re.I)
    m = re.search(r'header-scrobble-since">([^<]+)<', overview_html)
    if not m:
        return ""
    text = html.unescape(m.group(1)).strip().lstrip("•").strip()
    return re.sub(r"^scrobbling since\s+", "", text, flags=re.I)


def extract_header_number(overview_html: str, label: str) -> int:
    if _BS4:
        soup = _soup(overview_html)
        for h4 in soup.find_all("h4", class_="header-metadata-title"):
            if h4.get_text(strip=True).lower() == label.lower():
                li = h4.find_parent("li")
                if li:
                    val = re.search(r"[0-9][0-9,]*", li.get_text())
                    return parse_int(val.group(0) if val else "0")
        return 0
    m = re.search(
        rf'<h4 class="header-metadata-title">\s*{re.escape(label)}\s*</h4>(.*?)</li>',
        overview_html,
        flags=re.S | re.I,
    )
    if not m:
        return 0
    n = re.search(r">\s*([0-9][0-9,]*)\s*<", m.group(1))
    return parse_int(n.group(1) if n else "0")


def extract_recent_tracks(overview_html: str, limit: int = 40) -> list[dict[str, Any]]:
    if _BS4:
        soup = _soup(overview_html)
        rows = soup.find_all("tr", attrs={"data-scrobble-row": True})
        results: list[dict[str, Any]] = []
        for row in rows:
            name_cell = row.find(class_="chartlist-name")
            artist_cell = row.find(class_="chartlist-artist")
            if not name_cell:
                continue
            track_a = name_cell.find("a")
            artist_a = artist_cell.find("a") if artist_cell else None
            track_name = track_a.get_text(strip=True) if track_a else ""
            track_href = html.unescape(track_a.get("href", "")) if track_a else ""
            artist_name = artist_a.get_text(strip=True) if artist_a else ""
            artist_href = html.unescape(artist_a.get("href", "")) if artist_a else ""
            ts = parse_int(row.get("data-timestamp") or "0")
            yt_url = html.unescape(row.get("data-youtube-url") or "")
            row_str = str(row)
            spotify_url = extract_spotify_url(row_str)
            spotify_id = spotify_extract_id(spotify_url, "track")
            results.append({
                "track": track_name,
                "track_url": "https://www.last.fm" + track_href,
                "artist": artist_name,
                "artist_url": ("https://www.last.fm" + artist_href) if artist_href else "",
                "timestamp": ts,
                "youtube_url": yt_url,
                "spotify_type": "track",
                "spotify_id": spotify_id,
                "spotify_uri": f"spotify:track:{spotify_id}" if spotify_id else "",
                "spotify_url": spotify_url,
            })
            if len(results) >= limit:
                break
        return results

    # regex fallback
    rows_raw = re.findall(r"(<tr[^>]*data-scrobble-row.*?</tr>)", overview_html, flags=re.S)
    results = []
    for row in rows_raw:
        track_m = re.search(
            r'class="chartlist-name"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            row, flags=re.S,
        )
        artist_m = re.search(
            r'class="chartlist-artist"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            row, flags=re.S,
        )
        ts_m = re.search(r'data-timestamp="([0-9]+)"', row)
        if not track_m:
            continue
        track_name = strip_tags(track_m.group(2))
        artist_name = strip_tags(artist_m.group(2)) if artist_m else ""
        ts = parse_int(ts_m.group(1) if ts_m else "0")
        yt_m = re.search(r'data-youtube-url="([^"]+)"', row)
        youtube_url = html.unescape(yt_m.group(1)) if yt_m else ""
        spotify_url = extract_spotify_url(row)
        spotify_id = spotify_extract_id(spotify_url, "track")
        results.append({
            "track": track_name,
            "track_url": "https://www.last.fm" + html.unescape(track_m.group(1)),
            "artist": artist_name,
            "artist_url": (
                "https://www.last.fm" + html.unescape(artist_m.group(1)) if artist_m else ""
            ),
            "timestamp": ts,
            "youtube_url": youtube_url,
            "spotify_type": "track",
            "spotify_id": spotify_id,
            "spotify_uri": f"spotify:track:{spotify_id}" if spotify_id else "",
            "spotify_url": spotify_url,
        })
        if len(results) >= limit:
            break
    return results


def extract_chart_rows(
    chart_html: str,
    include_artist: bool,
    entity_type: str,
) -> list[dict[str, Any]]:
    if _BS4:
        soup = _soup(chart_html)
        rows = soup.find_all("tr", class_="chartlist-row")
        results: list[dict[str, Any]] = []
        for row in rows:
            idx_td = row.find(class_="chartlist-index")
            name_cell = row.find(class_="chartlist-name")
            count_tag = row.find(attrs={"data-stat-value": True})
            if not (idx_td and name_cell and count_tag):
                continue
            name_a = name_cell.find("a")
            artist_name = ""
            artist_url = ""
            if include_artist:
                artist_cell = row.find(class_="chartlist-artist")
                if artist_cell:
                    artist_a = artist_cell.find("a")
                    if artist_a:
                        artist_name = artist_a.get_text(strip=True)
                        artist_url = "https://www.last.fm" + html.unescape(
                            artist_a.get("href", "")
                        )
            row_str = str(row)
            yt_m = re.search(r'data-youtube-url="([^"]+)"', row_str)
            youtube_url = html.unescape(yt_m.group(1)) if yt_m else ""
            spotify_url = extract_spotify_url(row_str)
            spotify_id = spotify_extract_id(spotify_url, entity_type)
            results.append({
                "rank": parse_int(idx_td.get_text(strip=True)),
                "name": name_a.get_text(strip=True) if name_a else "",
                "url": (
                    "https://www.last.fm" + html.unescape(name_a.get("href", ""))
                    if name_a else ""
                ),
                "scrobbles": parse_int(count_tag.get("data-stat-value", "0")),
                "artist": artist_name,
                "artist_url": artist_url,
                "youtube_url": youtube_url,
                "spotify_type": entity_type,
                "spotify_id": spotify_id,
                "spotify_uri": f"spotify:{entity_type}:{spotify_id}" if spotify_id else "",
                "spotify_url": spotify_url,
            })
        return results

    # regex fallback
    rows_raw = re.findall(r"(<tr[^>]*chartlist-row.*?</tr>)", chart_html, flags=re.S)
    results = []
    for row in rows_raw:
        idx_m = re.search(r'class="chartlist-index">\s*([0-9]+)\s*</td>', row, flags=re.S)
        name_m = re.search(
            r'class="chartlist-name"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            row, flags=re.S,
        )
        count_m = re.search(r'data-stat-value="([0-9]+)"', row)
        if not (idx_m and name_m and count_m):
            continue
        artist_name = ""
        artist_url = ""
        if include_artist:
            artist_m = re.search(
                r'class="chartlist-artist"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                row, flags=re.S,
            )
            if artist_m:
                artist_name = strip_tags(artist_m.group(2))
                artist_url = "https://www.last.fm" + html.unescape(artist_m.group(1))
        yt_m = re.search(r'data-youtube-url="([^"]+)"', row)
        youtube_url = html.unescape(yt_m.group(1)) if yt_m else ""
        spotify_url = extract_spotify_url(row)
        spotify_id = spotify_extract_id(spotify_url, entity_type)
        results.append({
            "rank": parse_int(idx_m.group(1)),
            "name": strip_tags(name_m.group(2)),
            "url": "https://www.last.fm" + html.unescape(name_m.group(1)),
            "scrobbles": parse_int(count_m.group(1)),
            "artist": artist_name,
            "artist_url": artist_url,
            "youtube_url": youtube_url,
            "spotify_type": entity_type,
            "spotify_id": spotify_id,
            "spotify_uri": f"spotify:{entity_type}:{spotify_id}" if spotify_id else "",
            "spotify_url": spotify_url,
        })
    return results


def extract_weekly_total(report_html: str) -> int:
    if _BS4:
        soup = _soup(report_html)
        tag = soup.find(class_=re.compile(r"report-headline-total"))
        if tag:
            m = re.search(r"[0-9,]+", tag.get_text())
            return parse_int(m.group(0) if m else "0")
        return 0
    m = re.search(
        r'report-headline-total[^>]*>\s*([0-9,]+)\s*scrobbles', report_html, flags=re.I
    )
    return parse_int(m.group(1) if m else "0")


def extract_listening_clock(report_html: str) -> list[dict[str, Any]]:
    # Attribute-value scan works well regardless of bs4
    matches = re.findall(
        r'data-listening-clock-tooltip="([^"]+?)<br/>\s*([0-9,]+)\s*scrobbles"',
        report_html,
        flags=re.I,
    )

    def to_hour24(label: str) -> int:
        m = re.match(r"^\s*([0-9]{1,2}):[0-9]{2}\s*([ap]m)\s*$", label, flags=re.I)
        if not m:
            return -1
        h = int(m.group(1))
        ap = m.group(2).lower()
        if ap == "am":
            return 0 if h == 12 else h
        return 12 if h == 12 else h + 12

    result: list[dict[str, Any]] = []
    for hour_label, count in matches:
        normalized = html.unescape(hour_label).strip()
        h24 = to_hour24(normalized)
        result.append({
            "hour": normalized,
            "hour24": h24,
            "hour24_label": f"{h24:02d}:00" if h24 >= 0 else normalized,
            "scrobbles": parse_int(count),
        })
    result.sort(key=lambda x: (x.get("hour24", -1), x.get("hour", "")))
    return result


def extract_album_tracks(album_html: str, artist_hint: str = "") -> list[dict[str, Any]]:
    if _BS4:
        soup = _soup(album_html)
        rows = soup.find_all("tr", class_="chartlist-row")
        tracks: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for row in rows:
            name_cell = row.find(class_="chartlist-name")
            if not name_cell:
                continue
            name_a = name_cell.find("a")
            title = name_a.get_text(strip=True) if name_a else ""
            if not title:
                continue
            row_str = str(row)
            yt_m = re.search(r'data-youtube-url="([^"]+)"', row_str)
            youtube_url = html.unescape(yt_m.group(1)) if yt_m else ""
            spotify_url = extract_spotify_url(row_str)
            spotify_id = spotify_extract_id(spotify_url, "track")
            key = (title.lower(), youtube_url, spotify_id)
            if key in seen:
                continue
            seen.add(key)
            tracks.append({
                "name": title,
                "artist": artist_hint,
                "youtube_url": youtube_url,
                "spotify_type": "track",
                "spotify_id": spotify_id,
                "spotify_uri": f"spotify:track:{spotify_id}" if spotify_id else "",
                "spotify_url": spotify_url,
            })
        return tracks

    rows_raw = re.findall(r"(<tr[^>]*chartlist-row.*?</tr>)", album_html, flags=re.S)
    tracks = []
    seen_set: set[tuple[str, str, str]] = set()
    for row in rows_raw:
        name_m = re.search(
            r'class="chartlist-name"[^>]*>.*?<a[^>]*>(.*?)</a>', row, flags=re.S
        )
        if not name_m:
            continue
        title = strip_tags(name_m.group(1))
        if not title:
            continue
        yt_m = re.search(r'data-youtube-url="([^"]+)"', row)
        youtube_url = html.unescape(yt_m.group(1)) if yt_m else ""
        spotify_url = extract_spotify_url(row)
        spotify_id = spotify_extract_id(spotify_url, "track")
        key = (title.lower(), youtube_url, spotify_id)
        if key in seen_set:
            continue
        seen_set.add(key)
        tracks.append({
            "name": title,
            "artist": artist_hint,
            "youtube_url": youtube_url,
            "spotify_type": "track",
            "spotify_id": spotify_id,
            "spotify_uri": f"spotify:track:{spotify_id}" if spotify_id else "",
            "spotify_url": spotify_url,
        })
    return tracks


# ── Album page cache ──────────────────────────────────────────────────────────

def _cache_is_fresh(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return False
    age_secs = time.time() - path.stat().st_mtime
    return age_secs < max_age_days * 86400


def enrich_album_playback(
    data: dict[str, Any],
    album_cache_dir: Path,
    fetch_album_pages: bool = True,
    cache_max_age_days: int = 30,
    refresh_cache: bool = False,
) -> None:
    album_cache_dir.mkdir(parents=True, exist_ok=True)

    def cache_path_for(album: dict[str, Any]) -> Path:
        rank = int(album.get("rank") or 0)
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", f"{rank}_{album.get('name', 'album')}".strip())
        return album_cache_dir / f"{safe[:80]}.html"

    for album in data.get("top_albums", []):
        album["album_tracks"] = []
        album_url = album.get("url", "").strip()
        if not album_url:
            continue

        cache_file = cache_path_for(album)
        html_text = ""

        if not refresh_cache and _cache_is_fresh(cache_file, cache_max_age_days):
            try:
                html_text = cache_file.read_text(encoding="utf-8")
                log.debug("Album cache hit: %s", cache_file.name)
            except OSError:
                html_text = ""
        elif refresh_cache and cache_file.exists():
            log.debug("Cache refresh requested for: %s", cache_file.name)

        if not html_text and fetch_album_pages:
            try:
                html_text = fetch_url(album_url, timeout=25)
                cache_file.write_text(html_text, encoding="utf-8")
                log.debug("Fetched and cached: %s", album_url)
            except Exception as exc:
                log.warning("Could not fetch album page %s: %s", album_url, exc)

        if html_text:
            tracks = extract_album_tracks(html_text, artist_hint=album.get("artist", ""))
            if tracks:
                album["album_tracks"] = tracks


# ── Page I/O ──────────────────────────────────────────────────────────────────

def save_raw_pages(raw_dir: Path, pages: dict[str, str]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for key, content in pages.items():
        (raw_dir / f"{key}.html").write_text(content, encoding="utf-8")
    log.info("Raw pages saved to %s", raw_dir)


def load_raw_pages(raw_dir: Path) -> dict[str, str]:
    keys = ("overview", "artists", "albums", "tracks", "report")
    pages: dict[str, str] = {}
    for key in keys:
        path = raw_dir / f"{key}.html"
        if not path.exists():
            raise FileNotFoundError(f"Missing snapshot file: {path}")
        pages[key] = path.read_text(encoding="utf-8")
    log.info("Loaded %d snapshot pages from %s", len(pages), raw_dir)
    return pages


# ── Data assembly ─────────────────────────────────────────────────────────────

def build_data(username: str, pages: dict[str, str]) -> dict[str, Any]:
    overview = pages["overview"]

    top_artists = extract_chart_rows(pages["artists"], include_artist=False, entity_type="artist")
    top_albums = extract_chart_rows(pages["albums"], include_artist=True, entity_type="album")
    top_tracks = extract_chart_rows(pages["tracks"], include_artist=True, entity_type="track")
    recent_tracks = extract_recent_tracks(overview, limit=40)
    clock = extract_listening_clock(pages["report"])

    log.info(
        "Extracted: %d artists, %d albums, %d tracks, %d recent",
        len(top_artists), len(top_albums), len(top_tracks), len(recent_tracks),
    )

    return {
        "username": username,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "profile": {
            "description": extract_meta_description(overview),
            "scrobbling_since": extract_scrobbling_since(overview),
            "total_scrobbles": extract_header_number(overview, "Scrobbles"),
            "total_artists": extract_header_number(overview, "Artists"),
            "loved_tracks": extract_header_number(overview, "Loved tracks"),
        },
        "weekly_report": {
            "scrobbles_this_week": extract_weekly_total(pages["report"]),
            "listening_clock": clock,
        },
        "top_artists": top_artists[:20],
        "top_albums": top_albums[:20],
        "top_tracks": top_tracks[:20],
        "recent_tracks": recent_tracks,
    }


# ── Rendering ─────────────────────────────────────────────────────────────────

def _norm_key(*parts: str) -> str:
    normalized = [re.sub(r"\s+", " ", (p or "").strip()).lower() for p in parts]
    return "|".join(normalized)


def _safe_int(value: Any, default: int = 0) -> int:
    return value if isinstance(value, int) else parse_int(str(value), default)


def _apply_seed_fields(target: dict[str, Any], seed: dict[str, Any], fields: tuple[str, ...]) -> None:
    for field in fields:
        if not target.get(field) and seed.get(field):
            target[field] = seed[field]


def merge_api_aggregates(
    data: dict[str, Any],
    api_data: dict[str, Any],
    top_limit: int = 20,
    recent_limit: int = 40,
) -> None:
    if not isinstance(api_data, dict):
        return

    profile = data.setdefault("profile", {})
    api_profile = api_data.get("profile") if isinstance(api_data.get("profile"), dict) else {}
    total_scrobbles = _safe_int(api_profile.get("total_scrobbles"), 0)
    total_artists = _safe_int(api_profile.get("total_artists"), 0)
    if total_scrobbles > 0:
        profile["total_scrobbles"] = total_scrobbles
    if total_artists > 0:
        profile["total_artists"] = total_artists

    weekly_report = api_data.get("weekly_report") if isinstance(api_data.get("weekly_report"), dict) else {}
    if weekly_report:
        target_weekly = data.setdefault("weekly_report", {})
        if "scrobbles_this_week" in weekly_report:
            target_weekly["scrobbles_this_week"] = _safe_int(weekly_report.get("scrobbles_this_week"), 0)
        if isinstance(weekly_report.get("listening_clock"), list):
            target_weekly["listening_clock"] = weekly_report["listening_clock"]

    seeded_tracks = {
        _norm_key(str(item.get("name") or ""), str(item.get("artist") or "")): item
        for item in data.get("top_tracks", [])
        if isinstance(item, dict)
    }
    seeded_albums = {
        _norm_key(str(item.get("name") or ""), str(item.get("artist") or "")): item
        for item in data.get("top_albums", [])
        if isinstance(item, dict)
    }
    seeded_artists = {
        _norm_key(str(item.get("name") or "")): item
        for item in data.get("top_artists", [])
        if isinstance(item, dict)
    }

    def merge_ranked_rows(
        incoming: list[dict[str, Any]],
        seeded: dict[str, dict[str, Any]],
        key_fn: Any,
        seed_fields: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        for idx, row in enumerate(incoming[:top_limit], start=1):
            item = dict(row)
            item["rank"] = idx
            seed = seeded.get(key_fn(item))
            if seed:
                _apply_seed_fields(item, seed, seed_fields)
            merged.append(item)
        return merged

    api_tracks = api_data.get("top_tracks") if isinstance(api_data.get("top_tracks"), list) else []
    if api_tracks:
        data["top_tracks"] = merge_ranked_rows(
            [x for x in api_tracks if isinstance(x, dict)],
            seeded_tracks,
            key_fn=lambda x: _norm_key(str(x.get("name") or ""), str(x.get("artist") or "")),
            seed_fields=("url", "artist_url", "youtube_url", "spotify_type", "spotify_id", "spotify_uri", "spotify_url"),
        )

    api_albums = api_data.get("top_albums") if isinstance(api_data.get("top_albums"), list) else []
    if api_albums:
        data["top_albums"] = merge_ranked_rows(
            [x for x in api_albums if isinstance(x, dict)],
            seeded_albums,
            key_fn=lambda x: _norm_key(str(x.get("name") or ""), str(x.get("artist") or "")),
            seed_fields=("url", "artist_url", "youtube_url", "spotify_type", "spotify_id", "spotify_uri", "spotify_url", "album_tracks"),
        )

    api_artists = api_data.get("top_artists") if isinstance(api_data.get("top_artists"), list) else []
    if api_artists:
        data["top_artists"] = merge_ranked_rows(
            [x for x in api_artists if isinstance(x, dict)],
            seeded_artists,
            key_fn=lambda x: _norm_key(str(x.get("name") or "")),
            seed_fields=("url", "youtube_url", "spotify_type", "spotify_id", "spotify_uri", "spotify_url"),
        )

    api_recent = api_data.get("recent_tracks") if isinstance(api_data.get("recent_tracks"), list) else []
    if api_recent:
        data["recent_tracks"] = [x for x in api_recent if isinstance(x, dict)][:recent_limit]

    if isinstance(api_data.get("sync"), dict):
        data["api_sync"] = api_data["sync"]


def render_dashboard_html(data: dict[str, Any], template_path: Path | None = None) -> str:
    if template_path is None:
        template_path = Path(__file__).parent / "template.html"
    if not template_path.exists():
        raise FileNotFoundError(
            f"Template not found: {template_path}\n"
            "Ensure template.html is in the same directory as this script."
        )
    template = template_path.read_text(encoding="utf-8")
    data_json = json.dumps(data, ensure_ascii=False)
    return (
        template
        .replace("__USERNAME__", data["username"])
        .replace("__DATA_JSON__", data_json)
    )


def build_live_pages(username: str) -> dict[str, str]:
    base = f"https://www.last.fm/user/{username}"
    log.info("Fetching live pages for user: %s", username)
    return {
        "overview": fetch_url(base),
        "artists": fetch_url(f"{base}/library/artists"),
        "albums": fetch_url(f"{base}/library/albums"),
        "tracks": fetch_url(f"{base}/library/tracks"),
        "report": fetch_url(f"{base}/listening-report"),
    }


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Build a static Last.fm dashboard page.")
    parser.add_argument("--user", default="TOOUUR", help="Last.fm username")
    parser.add_argument("--output", default="index.html", help="Output HTML path")
    parser.add_argument("--json-output", default="lastfm_data.json", help="Parsed data JSON path")
    parser.add_argument(
        "--input-dir",
        default="",
        help="Directory containing overview/artists/albums/tracks/report.html snapshots",
    )
    parser.add_argument(
        "--save-raw-dir",
        default="",
        help="If set, save downloaded HTML files to this directory",
    )
    parser.add_argument(
        "--skip-album-track-fetch",
        action="store_true",
        help="Do not fetch album pages for random-track playback enrichment",
    )
    parser.add_argument(
        "--skip-spotify-enrich",
        action="store_true",
        help="Skip Spotify lookup even if credentials are set",
    )
    parser.add_argument(
        "--cache-max-age-days",
        type=int,
        default=30,
        metavar="DAYS",
        help="Album page cache TTL in days (default: 30)",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Force re-fetch all cached album pages",
    )
    parser.add_argument(
        "--api-aggregates",
        default="raw_snapshot/api_aggregates.json",
        help="Optional Last.fm API aggregate JSON to merge into dashboard data",
    )
    parser.add_argument(
        "--skip-api-aggregates",
        action="store_true",
        help="Do not merge local API aggregate JSON into dashboard payload",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not _BS4:
        log.warning(
            "beautifulsoup4 is not installed – using fragile regex parsing. "
            "Install with: pip install beautifulsoup4"
        )

    # Spotify credentials from environment only (never CLI args)
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        log.info(
            "Spotify credentials not found (SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET). "
            "Spotify embed enrichment will be skipped."
        )

    # Fetch or load snapshot pages
    if args.input_dir:
        pages = load_raw_pages(Path(args.input_dir))
    else:
        pages = build_live_pages(args.user)
        if args.save_raw_dir:
            save_raw_pages(Path(args.save_raw_dir), pages)

    data = build_data(args.user, pages)

    if not args.skip_api_aggregates:
        api_aggregates_path = Path(args.api_aggregates)
        if api_aggregates_path.exists():
            try:
                api_aggregates = json.loads(api_aggregates_path.read_text(encoding="utf-8"))
                merge_api_aggregates(data, api_aggregates)
                log.info("Merged API aggregates from %s", api_aggregates_path)
            except Exception as exc:
                log.warning("Could not merge API aggregates %s: %s", api_aggregates_path, exc)

    # Resolve album cache directory
    if args.input_dir:
        cache_dir = Path(args.input_dir) / "album_pages"
    elif args.save_raw_dir:
        cache_dir = Path(args.save_raw_dir) / "album_pages"
    else:
        cache_dir = Path("raw_snapshot/album_pages")

    enrich_album_playback(
        data,
        album_cache_dir=cache_dir,
        fetch_album_pages=not args.skip_album_track_fetch,
        cache_max_age_days=args.cache_max_age_days,
        refresh_cache=args.refresh_cache,
    )

    # Spotify enrichment
    spotify_stats: dict[str, int] = {"tracks": 0, "albums": 0, "artists": 0}
    if not args.skip_spotify_enrich and client_id and client_secret:
        spotify_stats = enrich_spotify_playback(data, client_id, client_secret)
    data["spotify_enrichment"] = spotify_stats

    # Write outputs
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_dashboard_html(data), encoding="utf-8")

    json_path = Path(args.json_output)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info("Dashboard written: %s", output_path.resolve())
    log.info("Data JSON written: %s", json_path.resolve())
    log.info(
        "Summary: scrobbles=%d  artists=%d  loved=%d  recent=%d  "
        "spotify_tracks=%d  spotify_albums=%d  spotify_artists=%d",
        data["profile"]["total_scrobbles"],
        data["profile"]["total_artists"],
        data["profile"]["loved_tracks"],
        len(data["recent_tracks"]),
        spotify_stats["tracks"],
        spotify_stats["albums"],
        spotify_stats["artists"],
    )


if __name__ == "__main__":
    main()
