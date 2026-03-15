"""
Video discovery for @thechoirsource pipeline.
Searches YouTube channels and search terms for candidate choir videos.

YouTube Data API v3 quota cost estimate for a single weekly run:
  - 12 channels × channels.list (1 unit each)  = 12 units
  - 12 channels × playlistItems.list (1 unit)   = 12 units
  - 8 search terms × search.list (100 units)    = 800 units
  Total: ~824 units per run (well within the 10,000 daily quota for weekly runs)
"""

import logging
import math
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

MAX_RESULTS = 7  # Top N videos to return after scoring
MIN_VIEWS = 500
MIN_DURATION_SECONDS = 60
MAX_DURATION_SECONDS = 30 * 60  # 30 minutes
RECENCY_DECAY_PER_DAY = 0.03
CHANNEL_MAX_RECENT = 5
SEARCH_MAX_RESULTS = 10
LOOKBACK_DAYS = 30


def _parse_duration(iso_duration: str) -> int:
    """Parse ISO 8601 duration string (e.g. PT8M30S) to seconds."""
    import re
    match = re.match(
        r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?",
        iso_duration or "",
    )
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _parse_youtube_id(url: str) -> str | None:
    """Extract video ID from a YouTube URL."""
    try:
        parsed = urlparse(url)
        if parsed.hostname in ("youtu.be",):
            return parsed.path.lstrip("/")
        if parsed.hostname in ("www.youtube.com", "youtube.com", "m.youtube.com"):
            qs = parse_qs(parsed.query)
            if "v" in qs:
                return qs["v"][0]
            # Handle /shorts/ and /embed/ paths
            parts = parsed.path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] in ("shorts", "embed", "v"):
                return parts[1]
    except Exception:
        pass
    return None


def _score_video(view_count: int, published_at: str, now: datetime) -> float:
    """Score = log(view_count) * recency_weight."""
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        days_old = max(0, (now - pub).total_seconds() / 86400)
    except Exception:
        days_old = LOOKBACK_DAYS

    recency = max(0.0, 1.0 - RECENCY_DECAY_PER_DAY * days_old)
    views = max(1, view_count)
    return math.log(views) * recency


def _build_item_from_api(video_data: dict, channel_name: str, source: str) -> dict:
    snippet = video_data.get("snippet", {})
    details = video_data.get("contentDetails", {})
    stats = video_data.get("statistics", {})

    return {
        "youtube_id": video_data.get("id") or snippet.get("videoId", ""),
        "title": snippet.get("title", ""),
        "channel_name": channel_name or snippet.get("channelTitle", ""),
        "channel_id": snippet.get("channelId", ""),
        "description": snippet.get("description", "")[:1000],
        "duration_seconds": _parse_duration(details.get("duration", "")),
        "view_count": int(stats.get("viewCount", 0)),
        "published_at": snippet.get("publishedAt", ""),
        "url": f"https://www.youtube.com/watch?v={video_data.get('id', '')}",
        "source": source,
    }


def _fetch_channel_videos(youtube, channel_id: str, channel_name: str, published_after: str) -> list:
    """Fetch recent uploads from a channel's uploads playlist."""
    try:
        # Step 1: get the uploads playlist ID
        ch_resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id,
        ).execute()
        items = ch_resp.get("items", [])
        if not items:
            logger.warning("No channel found for ID: %s", channel_id)
            return []
        playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # Step 2: fetch recent playlist items
        pl_resp = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=CHANNEL_MAX_RECENT * 3,  # fetch extra to allow filtering
        ).execute()
        pl_items = pl_resp.get("items", [])

        video_ids = [
            item["contentDetails"]["videoId"]
            for item in pl_items
            if item.get("contentDetails", {}).get("videoId")
        ]
        if not video_ids:
            return []

        # Step 3: batch-fetch video details
        detail_resp = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids),
        ).execute()

        results = []
        for v in detail_resp.get("items", []):
            item = _build_item_from_api(v, channel_name, "channel")
            pub = item["published_at"]
            if pub < published_after:
                continue
            results.append(item)

        return results[:CHANNEL_MAX_RECENT]

    except Exception as exc:
        logger.warning("Error fetching channel %s (%s): %s", channel_name, channel_id, exc)
        return []


def _fetch_search_results(youtube, term: str, published_after: str) -> list:
    """Search YouTube for a term and return candidate video dicts."""
    try:
        search_resp = youtube.search().list(
            part="id,snippet",
            q=term,
            type="video",
            order="viewCount",
            publishedAfter=published_after,
            videoDuration="medium",  # 4–20 minutes
            maxResults=SEARCH_MAX_RESULTS,
            relevanceLanguage="en",
        ).execute()

        video_ids = [
            item["id"]["videoId"]
            for item in search_resp.get("items", [])
            if item.get("id", {}).get("kind") == "youtube#video"
        ]
        if not video_ids:
            return []

        detail_resp = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids),
        ).execute()

        results = []
        for v in detail_resp.get("items", []):
            item = _build_item_from_api(v, v.get("snippet", {}).get("channelTitle", ""), "search")
            results.append(item)

        return results

    except Exception as exc:
        logger.warning("Error searching for '%s': %s", term, exc)
        return []


def discover_videos(config, queue_manager, manual_urls: list = None) -> list:
    """
    Discovers candidate choir videos from YouTube.

    Sources:
    1. Curated channels (recent uploads in the past 30 days)
    2. Search terms (view-count ranked, past 30 days)
    3. Manual URLs (always included, bypasses filters)

    Returns top MAX_RESULTS videos sorted by combined recency+views score.
    """
    if config.is_mock_mode():
        from pipeline.mock import MockYouTubeAPI
        mock = MockYouTubeAPI()
        candidates = mock.search()
        logger.info("Mock mode: returning %d mock videos", len(candidates))
    else:
        candidates = _discover_from_youtube(config, manual_urls)

    existing_ids = queue_manager.get_all_youtube_ids()
    now = datetime.now(tz=timezone.utc)

    # Handle manual URLs (add without filtering, except dedup)
    manual_items = []
    if manual_urls and not config.is_mock_mode():
        for url in manual_urls:
            vid_id = _parse_youtube_id(url.strip())
            if vid_id and vid_id not in existing_ids:
                manual_items.append({
                    "youtube_id": vid_id,
                    "title": url,
                    "channel_name": "",
                    "channel_id": "",
                    "description": "",
                    "duration_seconds": 0,
                    "view_count": 0,
                    "published_at": now.isoformat(),
                    "url": url,
                    "source": "manual",
                })

    # Deduplicate + filter
    seen = set()
    filtered = []
    for item in candidates:
        vid_id = item["youtube_id"]
        if not vid_id or vid_id in seen:
            continue
        if vid_id in existing_ids:
            logger.debug("Skipping already-queued video: %s", vid_id)
            continue
        if item["source"] != "manual":
            dur = item["duration_seconds"]
            if dur and dur < MIN_DURATION_SECONDS:
                logger.debug("Skipping short video %s (%ds)", vid_id, dur)
                continue
            if dur and dur > MAX_DURATION_SECONDS:
                logger.debug("Skipping long video %s (%ds)", vid_id, dur)
                continue
            if item["view_count"] < MIN_VIEWS:
                logger.debug("Skipping low-view video %s (%d views)", vid_id, item["view_count"])
                continue
        seen.add(vid_id)
        filtered.append(item)

    # Score and rank
    for item in filtered:
        item["_score"] = _score_video(item["view_count"], item["published_at"], now)

    filtered.sort(key=lambda x: x.get("_score", 0), reverse=True)

    # Remove internal score key
    result = []
    for item in (manual_items + filtered)[:MAX_RESULTS]:
        item.pop("_score", None)
        result.append(item)

    logger.info("Discovered %d candidate videos", len(result))
    return result


def _discover_from_youtube(config, manual_urls: list = None) -> list:
    from googleapiclient.discovery import build

    youtube = build("youtube", "v3", developerKey=config.youtube_api_key)

    now = datetime.now(tz=timezone.utc)
    lookback = now - timedelta(days=LOOKBACK_DAYS)
    published_after = lookback.strftime("%Y-%m-%dT%H:%M:%SZ")

    candidates = []

    # Channel uploads
    for channel in config.channels:
        channel_id = channel.get("id", "")
        channel_name = channel.get("name", "")
        if channel_id.startswith("PLACEHOLDER"):
            logger.warning("Skipping placeholder channel ID for: %s", channel_name)
            continue
        logger.info("Fetching recent uploads from channel: %s", channel_name)
        videos = _fetch_channel_videos(youtube, channel_id, channel_name, published_after)
        candidates.extend(videos)

    # Search terms
    for term in config.search_terms:
        logger.info("Searching for: %s", term)
        videos = _fetch_search_results(youtube, term, published_after)
        candidates.extend(videos)

    return candidates
