"""
YouTube video downloader for @thechoirsource pipeline.
Uses yt-dlp to download videos, with a mock mode for testing.
"""

import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_DIR = "/tmp/thechoirsource/downloads"
MIN_FILE_SIZE = 1024 * 1024  # 1 MB


def download_video(youtube_id: str, config) -> dict:
    """
    Downloads a YouTube video using yt-dlp.

    Returns: {youtube_id, local_path, success, error_message}
    """
    if config.is_mock_mode():
        from pipeline.mock import MockDownloader
        mock = MockDownloader()
        return mock.generate(youtube_id, OUTPUT_DIR)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{youtube_id}.mp4")

    # Skip re-download if file already exists and is large enough
    if os.path.exists(output_path) and os.path.getsize(output_path) >= MIN_FILE_SIZE:
        logger.info("Video already downloaded: %s", output_path)
        return {"youtube_id": youtube_id, "local_path": output_path, "success": True}

    url = f"https://www.youtube.com/watch?v={youtube_id}"
    output_template = os.path.join(OUTPUT_DIR, f"{youtube_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--format", "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
        "--merge-output-format", "mp4",
        "--output", output_template,
        "--no-playlist",
        "--socket-timeout", "300",
        "--quiet",
        "--no-warnings",
        url,
    ]

    logger.info("Downloading %s with yt-dlp", youtube_id)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=360,  # 6 min timeout
        )
    except subprocess.TimeoutExpired:
        return {
            "youtube_id": youtube_id,
            "local_path": None,
            "success": False,
            "error_message": "Download timed out after 360 seconds",
        }

    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        logger.error("yt-dlp failed for %s: %s", youtube_id, err[:500])
        return {
            "youtube_id": youtube_id,
            "local_path": None,
            "success": False,
            "error_message": err[:500],
        }

    # yt-dlp may have used a different extension if merge wasn't needed
    # Check for the mp4 file
    if not os.path.exists(output_path):
        # Try to find any file with this youtube_id
        parent = Path(OUTPUT_DIR)
        matches = list(parent.glob(f"{youtube_id}.*"))
        if matches:
            output_path = str(matches[0])
        else:
            return {
                "youtube_id": youtube_id,
                "local_path": None,
                "success": False,
                "error_message": "Output file not found after download",
            }

    size = os.path.getsize(output_path)
    if size < MIN_FILE_SIZE:
        return {
            "youtube_id": youtube_id,
            "local_path": None,
            "success": False,
            "error_message": f"Downloaded file too small ({size} bytes) — may be corrupted",
        }

    logger.info("Downloaded %s → %s (%d MB)", youtube_id, output_path, size // (1024 * 1024))
    return {"youtube_id": youtube_id, "local_path": output_path, "success": True}


def download_batch(youtube_ids: list, config) -> list:
    """Downloads multiple videos sequentially. Returns list of results.
    Continues past failures (logs and skips)."""
    results = []
    for youtube_id in youtube_ids:
        try:
            result = download_video(youtube_id, config)
        except Exception as exc:
            logger.error("Unexpected error downloading %s: %s", youtube_id, exc)
            result = {
                "youtube_id": youtube_id,
                "local_path": None,
                "success": False,
                "error_message": str(exc),
            }
        results.append(result)
    return results
