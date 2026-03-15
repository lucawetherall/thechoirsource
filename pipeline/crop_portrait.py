"""
Portrait crop for @thechoirsource pipeline.
Trims a video clip and crops from landscape to 9:16 portrait for social media.
"""

import logging
import os
import subprocess

logger = logging.getLogger(__name__)

OUTPUT_DIR = "/tmp/thechoirsource/clips"


def _get_video_dimensions(video_path: str) -> tuple:
    """Return (width, height) of video using ffprobe."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=s=x:p=0",
        video_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        return (1920, 1080)  # assume landscape as fallback
    try:
        w, h = result.stdout.strip().split("x")
        return int(w), int(h)
    except ValueError:
        return (1920, 1080)


def crop_to_portrait(video_path: str, clip: dict, youtube_id: str, config=None) -> str:
    """
    Trims video to the clip window and crops from landscape to 9:16 portrait.

    Args:
        video_path: path to the full downloaded video
        clip: dict with {rank, start_seconds, end_seconds}
        youtube_id: for naming the output file
        config: Config object (unused currently, reserved for future config)

    Returns: path to the output clip file
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    rank = clip["rank"]
    start = clip["start_seconds"]
    duration = clip["end_seconds"] - clip["start_seconds"]
    output_path = os.path.join(OUTPUT_DIR, f"{youtube_id}_clip{rank}.mp4")

    width, height = _get_video_dimensions(video_path)
    is_portrait = width <= height

    if is_portrait:
        # Already portrait/square — just scale and pad
        video_filter = (
            "scale=1080:1920:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2"
        )
    else:
        # Landscape → 9:16 centre crop then scale/pad
        video_filter = (
            "crop=ih*9/16:ih:(iw-ih*9/16)/2:0,"
            "scale=1080:1920:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2"
        )

    # Fade in/out
    fade_out_start = max(0, duration - 0.5)
    audio_filter = (
        f"afade=t=in:st=0:d=0.5,"
        f"afade=t=out:st={fade_out_start:.3f}:d=0.5"
    )

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),         # fast seek (before -i)
        "-i", video_path,
        "-t", str(duration),
        "-vf", video_filter,
        "-af", audio_filter,
        "-c:v", "libx264",
        "-crf", "23",
        "-preset", "fast",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        output_path,
    ]

    logger.info("Cropping clip %d for %s (%.1fs–%.1fs)", rank, youtube_id, start, clip["end_seconds"])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"FFmpeg crop failed for {youtube_id} clip {rank}:\n{result.stderr[-2000:]}"
        )

    if not os.path.exists(output_path):
        raise RuntimeError(f"Output file not found after crop: {output_path}")

    logger.info("Crop complete: %s", output_path)
    return output_path
