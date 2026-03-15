"""
Orchestrator for the @thechoirsource weekly pipeline.

Usage:
    python -m pipeline.run                              # Full pipeline (discover + process)
    python -m pipeline.run --manual-urls URL1,URL2     # Add manual URLs
    python -m pipeline.run --mock                      # Run in mock mode (no API calls)
    python -m pipeline.run --approve YT_ID RANK CAP TAGS  # Approve a video
    python -m pipeline.run --reject YT_ID               # Reject a video
    python -m pipeline.run --publish                    # Publish due items
"""

import argparse
import logging
import os
import random
import shutil
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from pipeline.audio_analysis import analyse_audio
from pipeline.caption_overlay import overlay_captions
from pipeline.config import Config
from pipeline.crop_portrait import crop_to_portrait
from pipeline.discover import discover_videos
from pipeline.download import download_batch
from pipeline.generate_copy import generate_post_copy
from pipeline.metadata_parser import parse_metadata
from pipeline.publish import publish_clip
from pipeline.queue_manager import QueueManager
from pipeline.upload_r2 import cleanup_old_clips, delete_clips_for_video, upload_clips

logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "/tmp/thechoirsource/downloads"
CLIPS_DIR = "/tmp/thechoirsource/clips"
TMP_ROOT = "/tmp/thechoirsource"


def setup_logging():
    """Configure logging to stdout with timestamps. GitHub Actions captures stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _process_single_video(video: dict, config: Config) -> dict | None:
    """
    Process one video through the full pipeline.
    Returns a queue item dict on success, or None on failure.
    """
    youtube_id = video["youtube_id"]
    title = video["title"]
    logger.info("=== Processing %s: %s ===", youtube_id, title[:80])

    try:
        # Step 1: Download
        download_results = download_batch([youtube_id], config)
        dl = download_results[0]
        if not dl["success"]:
            logger.error("Download failed for %s: %s", youtube_id, dl.get("error_message"))
            return None
        local_path = dl["local_path"]

        # Step 2: Audio analysis → 2-3 clip candidates
        clips = analyse_audio(local_path, config)
        if not clips:
            logger.error("Audio analysis returned no clips for %s", youtube_id)
            return None
        logger.info("Found %d clip candidates for %s", len(clips), youtube_id)

        # Step 3: Parse metadata
        metadata = parse_metadata(
            title=title,
            description=video.get("description", ""),
            channel_name=video.get("channel_name", ""),
            config=config,
        )
        metadata.update({
            "title": title,
            "channel_name": video.get("channel_name", ""),
            "description": video.get("description", ""),
        })

        # Step 4: Generate post copy
        copy_data = generate_post_copy(metadata, config)

        # Step 5: Crop and overlay captions for each clip candidate
        clip_paths = []
        for clip in clips:
            try:
                clip_path = crop_to_portrait(local_path, clip, youtube_id, config)
                clip_path = overlay_captions(clip_path, metadata, config)
                clip_paths.append({
                    "youtube_id": youtube_id,
                    "rank": clip["rank"],
                    "local_path": clip_path,
                })
            except Exception as exc:
                logger.error(
                    "Failed to process clip %d for %s: %s",
                    clip["rank"], youtube_id, exc
                )

        if not clip_paths:
            logger.error("All clip processing failed for %s", youtube_id)
            return None

        # Step 6: Upload clips to R2
        upload_results = upload_clips(clip_paths, config)
        if not upload_results:
            logger.error("Upload failed for all clips of %s", youtube_id)
            return None

        # Build clips list for queue item
        processed_clips = []
        clip_by_rank = {c["rank"]: c for c in clips}
        for upload in upload_results:
            rank = upload["rank"]
            clip_info = clip_by_rank.get(rank, {})
            processed_clips.append({
                "rank": rank,
                "start_seconds": clip_info.get("start_seconds", 0),
                "end_seconds": clip_info.get("end_seconds", 0),
                "duration_seconds": clip_info.get("duration_seconds", 0),
                "contrast_score": clip_info.get("contrast_score", 0),
                "r2_url": upload["r2_url"],
            })

        queue_item = {
            "youtube_id": youtube_id,
            "title": title,
            "channel_name": video.get("channel_name", ""),
            "channel_id": video.get("channel_id", ""),
            "published_at": video.get("published_at", ""),
            "view_count": video.get("view_count", 0),
            "url": video.get("url", ""),
            "source": video.get("source", ""),
            "piece_title": metadata.get("piece_title", ""),
            "composer": metadata.get("composer", ""),
            "ensemble_name": metadata.get("ensemble_name", ""),
            "caption": copy_data["caption"],
            "hashtags": copy_data["hashtags_string"],
            "full_post_text": copy_data["full_post_text"],
            "clips": processed_clips,
            "added_at": datetime.now(tz=timezone.utc).isoformat(),
            "status": "pending",
        }

        logger.info("Successfully processed %s (%d clips)", youtube_id, len(processed_clips))
        return queue_item

    except Exception as exc:
        logger.error(
            "Unexpected error processing %s: %s\n%s",
            youtube_id, exc, traceback.format_exc()
        )
        return None


def run_weekly_pipeline(config: Config, manual_urls: list = None):
    """
    Full weekly pipeline: discover → download → analyse → crop → overlay → copy → upload → queue.
    """
    logger.info("Starting weekly pipeline (mock=%s)", config.is_mock_mode())

    queue_manager = QueueManager(queue_dir=str(config.queue_dir))

    # Discover videos
    try:
        videos = discover_videos(config, queue_manager, manual_urls=manual_urls)
    except Exception as exc:
        logger.error("Discovery failed: %s\n%s", exc, traceback.format_exc())
        sys.exit(1)

    if not videos:
        logger.warning("No new videos discovered. Exiting.")
        return

    logger.info("Discovered %d candidate videos", len(videos))

    # Process each video
    successful_items = []
    failed = []

    for video in videos:
        item = _process_single_video(video, config)
        if item is not None:
            successful_items.append(item)
        else:
            failed.append(video["youtube_id"])

    logger.info(
        "Pipeline complete: %d/%d videos processed successfully. %d failures.",
        len(successful_items), len(videos), len(failed)
    )
    if failed:
        logger.warning("Failed videos: %s", ", ".join(failed))

    if successful_items:
        queue_manager.add_pending(successful_items)
        logger.info("Added %d items to pending queue", len(successful_items))

    # Cleanup old R2 clips
    try:
        cleanup_old_clips(config, days=30)
    except Exception as exc:
        logger.warning("R2 cleanup failed (non-fatal): %s", exc)

    # Cleanup temp files (skip in mock mode to allow test caching of synthetic videos)
    if not config.is_mock_mode():
        try:
            if os.path.exists(TMP_ROOT):
                shutil.rmtree(TMP_ROOT, ignore_errors=True)
                logger.info("Cleaned up temp directory: %s", TMP_ROOT)
        except Exception as exc:
            logger.warning("Temp cleanup failed: %s", exc)

    if len(successful_items) == 0 and len(videos) > 0:
        logger.error("Zero videos succeeded — exiting with error code 1")
        sys.exit(1)


def _calculate_scheduled_at(config: Config, queue_manager: QueueManager) -> str:
    """
    Calculate the next available posting time.
    Schedules the day after the latest approved item, within the posting window.
    """
    tz = ZoneInfo(config.posting_timezone)
    approved = queue_manager.get_approved()

    if approved:
        # Find the latest scheduled date
        latest_str = max(item.get("scheduled_at", "") for item in approved)
        try:
            latest = datetime.fromisoformat(latest_str)
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=tz)
            base_date = (latest + timedelta(days=1)).date()
        except ValueError:
            base_date = (datetime.now(tz=tz) + timedelta(days=1)).date()
    else:
        base_date = (datetime.now(tz=tz) + timedelta(days=1)).date()

    # Random time within posting window
    start_hour = config.posting_window_start
    end_hour = config.posting_window_end
    random_minute = random.randint(0, 59)
    random_hour = random.randint(start_hour, end_hour - 1)

    scheduled = datetime(
        base_date.year, base_date.month, base_date.day,
        random_hour, random_minute, 0,
        tzinfo=tz,
    )
    return scheduled.isoformat()


def run_approval(
    config: Config,
    youtube_id: str,
    selected_rank: int,
    caption: str,
    hashtags: str,
):
    """Approve a video: move from pending to approved with scheduled time."""
    queue_manager = QueueManager(queue_dir=str(config.queue_dir))
    scheduled_at = _calculate_scheduled_at(config, queue_manager)

    success = queue_manager.approve(
        youtube_id=youtube_id,
        selected_clip_rank=selected_rank,
        edited_caption=caption,
        edited_hashtags=hashtags,
        scheduled_at=scheduled_at,
    )
    if not success:
        logger.error("Failed to approve %s — not found in pending queue", youtube_id)
        sys.exit(1)

    # Clean up non-selected clips from R2
    try:
        delete_clips_for_video(youtube_id, config, exclude_rank=selected_rank)
    except Exception as exc:
        logger.warning("Failed to clean up non-selected clips: %s", exc)

    logger.info("Approved %s, scheduled at %s", youtube_id, scheduled_at)


def run_rejection(config: Config, youtube_id: str):
    """Reject a video: move from pending to archive, delete all clips from R2."""
    queue_manager = QueueManager(queue_dir=str(config.queue_dir))
    success = queue_manager.reject(youtube_id)
    if not success:
        logger.error("Failed to reject %s — not found in pending queue", youtube_id)
        sys.exit(1)

    # Delete all clips from R2
    try:
        delete_clips_for_video(youtube_id, config)
    except Exception as exc:
        logger.warning("Failed to delete clips from R2: %s", exc)

    logger.info("Rejected %s", youtube_id)


def run_publish(config: Config):
    """Publish any approved items that are due."""
    queue_manager = QueueManager(queue_dir=str(config.queue_dir))
    tz = ZoneInfo(config.posting_timezone)
    now = datetime.now(tz=tz)

    due_items = queue_manager.get_due_for_posting(now)
    if not due_items:
        logger.info("No items due for posting at %s", now.isoformat())
        return

    logger.info("Found %d item(s) due for posting", len(due_items))

    for item in due_items:
        youtube_id = item["youtube_id"]
        try:
            result = publish_clip(item, config)
            if result["all_success"]:
                queue_manager.mark_posted(youtube_id)
                logger.info("Published and marked as posted: %s", youtube_id)
            else:
                # Partial success — still mark as posted to avoid re-posting to successful platforms
                failed_platforms = [
                    r["platform"] for r in result["results"] if not r["success"]
                ]
                logger.warning(
                    "Partially published %s — failed on: %s. Marking as posted anyway.",
                    youtube_id, ", ".join(failed_platforms)
                )
                queue_manager.mark_posted(youtube_id)
        except Exception as exc:
            logger.error("Failed to publish %s: %s\n%s", youtube_id, exc, traceback.format_exc())


def main():
    parser = argparse.ArgumentParser(description="@thechoirsource pipeline orchestrator")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode (no API calls)")
    parser.add_argument(
        "--manual-urls", type=str, metavar="URLS",
        help="Comma-separated YouTube URLs to add manually"
    )
    parser.add_argument(
        "--approve", nargs=4,
        metavar=("YT_ID", "RANK", "CAPTION", "HASHTAGS"),
        help="Approve a pending video"
    )
    parser.add_argument("--reject", type=str, metavar="YT_ID", help="Reject a pending video")
    parser.add_argument("--publish", action="store_true", help="Publish due approved items")
    args = parser.parse_args()

    setup_logging()

    # Set MOCK_MODE env var if --mock flag is passed
    if args.mock:
        os.environ["MOCK_MODE"] = "true"

    config = Config()

    # Validate config (skip for mock mode — mocks don't need real keys)
    if not config.is_mock_mode():
        missing = config.validate()
        if missing:
            logger.error("Missing required config: %s", ", ".join(missing))
            sys.exit(1)

    if args.approve:
        yt_id, rank_str, caption, hashtags = args.approve
        run_approval(config, yt_id, int(rank_str), caption, hashtags)
    elif args.reject:
        run_rejection(config, args.reject)
    elif args.publish:
        run_publish(config)
    else:
        manual = [u.strip() for u in args.manual_urls.split(",")] if args.manual_urls else None
        run_weekly_pipeline(config, manual_urls=manual)


if __name__ == "__main__":
    main()
