"""
Social media publisher for @thechoirsource pipeline.
Posts clips to Instagram, Facebook, and TikTok.
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)

GRAPH_API_VERSION = "v21.0"
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
TIKTOK_POST_URL = "https://open.tiktokapis.com/v2/post/publish/video/init/"

MAX_POLL_ATTEMPTS = 60
POLL_INTERVAL_SECONDS = 10


def publish_to_instagram(clip_url: str, caption: str, config) -> dict:
    """
    Posts a Reel to Instagram via Meta Graph API.
    """
    if config.is_mock_mode():
        from pipeline.mock import MockPublisher
        return MockPublisher().publish_instagram(clip_url, caption)

    ig_user_id = config.meta_ig_user_id
    token = config.meta_access_token

    try:
        # Step 1: Create media container
        create_resp = requests.post(
            f"{GRAPH_BASE}/{ig_user_id}/media",
            data={
                "video_url": clip_url,
                "caption": caption,
                "media_type": "REELS",
                "share_to_feed": "true",
                "access_token": token,
            },
            timeout=30,
        )
        create_resp.raise_for_status()
        container_id = create_resp.json().get("id")
        if not container_id:
            return _error_result("instagram", "No container ID returned")

        # Step 2: Poll until ready
        for attempt in range(MAX_POLL_ATTEMPTS):
            status_resp = requests.get(
                f"{GRAPH_BASE}/{container_id}",
                params={"fields": "status_code", "access_token": token},
                timeout=30,
            )
            status_resp.raise_for_status()
            status_code = status_resp.json().get("status_code")

            if status_code == "FINISHED":
                break
            elif status_code == "ERROR":
                return _error_result("instagram", f"Container status ERROR after {attempt+1} polls")
            elif attempt < MAX_POLL_ATTEMPTS - 1:
                time.sleep(POLL_INTERVAL_SECONDS)
        else:
            return _error_result("instagram", "Container never reached FINISHED state")

        # Step 3: Publish
        pub_resp = requests.post(
            f"{GRAPH_BASE}/{ig_user_id}/media_publish",
            data={"creation_id": container_id, "access_token": token},
            timeout=30,
        )
        pub_resp.raise_for_status()
        post_id = pub_resp.json().get("id")
        logger.info("Published to Instagram: %s", post_id)
        return {"platform": "instagram", "success": True, "post_id": post_id, "error": None}

    except requests.RequestException as exc:
        return _error_result("instagram", str(exc))


def publish_to_facebook(clip_url: str, caption: str, config) -> dict:
    """
    Posts a Reel to Facebook Page via Graph API.
    """
    if config.is_mock_mode():
        from pipeline.mock import MockPublisher
        return MockPublisher().publish_facebook(clip_url, caption)

    page_id = config.meta_page_id
    token = config.meta_access_token

    try:
        # Step 1: Initialise upload
        init_resp = requests.post(
            f"{GRAPH_BASE}/{page_id}/video_reels",
            data={"upload_phase": "start", "access_token": token},
            timeout=30,
        )
        init_resp.raise_for_status()
        video_id = init_resp.json().get("video_id")
        if not video_id:
            return _error_result("facebook", "No video_id returned from init")

        # Step 2: Upload video (provide URL for pull-based upload)
        upload_resp = requests.post(
            f"https://rupload.facebook.com/video-upload/{GRAPH_API_VERSION}/{video_id}",
            headers={
                "Authorization": f"OAuth {token}",
                "file_url": clip_url,
            },
            timeout=60,
        )
        upload_resp.raise_for_status()

        # Step 3: Finish and publish
        finish_resp = requests.post(
            f"{GRAPH_BASE}/{page_id}/video_reels",
            data={
                "upload_phase": "finish",
                "video_id": video_id,
                "description": caption,
                "access_token": token,
            },
            timeout=30,
        )
        finish_resp.raise_for_status()
        post_id = finish_resp.json().get("post_id") or video_id
        logger.info("Published to Facebook: %s", post_id)
        return {"platform": "facebook", "success": True, "post_id": post_id, "error": None}

    except requests.RequestException as exc:
        return _error_result("facebook", str(exc))


def publish_to_tiktok(clip_url: str, caption: str, config) -> dict:
    """
    Posts a video to TikTok via Content Posting API v2.
    NOTE: Requires TikTok app review and approved Content Posting API access.
    """
    if config.is_mock_mode():
        from pipeline.mock import MockPublisher
        return MockPublisher().publish_tiktok(clip_url, caption)

    token = config.tiktok_access_token

    try:
        resp = requests.post(
            TIKTOK_POST_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={
                "post_info": {
                    "title": caption[:2200],
                    "privacy_level": "PUBLIC_TO_EVERYONE",
                    "disable_duet": False,
                    "disable_stitch": False,
                    "disable_comment": False,
                },
                "source_info": {
                    "source": "PULL_FROM_URL",
                    "video_url": clip_url,
                },
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        post_id = data.get("data", {}).get("publish_id")
        logger.info("Published to TikTok: %s", post_id)
        return {"platform": "tiktok", "success": True, "post_id": post_id, "error": None}

    except requests.RequestException as exc:
        return _error_result("tiktok", str(exc))


def publish_clip(approved_item: dict, config) -> dict:
    """
    Publishes a single approved clip to all configured platforms.

    Returns: {youtube_id, results: [...], all_success: bool}
    """
    youtube_id = approved_item["youtube_id"]
    selected_rank = approved_item.get("selected_clip_rank", 1)
    caption = approved_item.get("caption", "")
    hashtags = approved_item.get("hashtags", "")

    # Find the R2 URL for the selected clip
    clips = approved_item.get("clips", [])
    r2_url = None
    for clip in clips:
        if clip.get("rank") == selected_rank:
            r2_url = clip.get("r2_url")
            break

    if not r2_url:
        logger.error("No R2 URL found for %s rank %d", youtube_id, selected_rank)
        return {
            "youtube_id": youtube_id,
            "results": [{"platform": "all", "success": False, "error": "No R2 URL found"}],
            "all_success": False,
        }

    full_caption = caption + "\n\n" + hashtags if hashtags else caption
    platforms = config.platforms

    platform_functions = {
        "instagram_reels": publish_to_instagram,
        "facebook_reels": publish_to_facebook,
        "tiktok": publish_to_tiktok,
    }

    results = []
    for platform in platforms:
        fn = platform_functions.get(platform)
        if fn is None:
            logger.warning("Unknown platform: %s", platform)
            continue
        try:
            result = fn(r2_url, full_caption, config)
        except Exception as exc:
            logger.error("Unexpected error publishing to %s: %s", platform, exc)
            result = _error_result(platform, str(exc))
        results.append(result)
        if not result["success"]:
            logger.warning("Failed to publish to %s: %s", platform, result.get("error"))

    all_success = all(r["success"] for r in results)
    return {
        "youtube_id": youtube_id,
        "results": results,
        "all_success": all_success,
    }


def _error_result(platform: str, error: str) -> dict:
    return {"platform": platform, "success": False, "post_id": None, "error": error}
