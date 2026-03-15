"""
Cloudflare R2 upload for @thechoirsource pipeline.
Uses boto3 (S3-compatible API) to upload clips to R2.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _get_r2_client(config):
    """Create and return a boto3 S3 client configured for Cloudflare R2."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=f"https://{config.r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=config.r2_access_key_id,
        aws_secret_access_key=config.r2_secret_access_key,
    )


def _clip_key(youtube_id: str, rank: int) -> str:
    return f"clips/{youtube_id}/{youtube_id}_clip{rank}.mp4"


def _clip_url(youtube_id: str, rank: int, config) -> str:
    return f"{config.r2_public_url.rstrip('/')}/{_clip_key(youtube_id, rank)}"


def upload_clips(clip_paths: list, config) -> list:
    """
    Uploads processed clips to Cloudflare R2.

    Args:
        clip_paths: list of {youtube_id, rank, local_path}
        config: Config object with R2 credentials

    Returns: list of {youtube_id, rank, r2_url}
    """
    if config.is_mock_mode():
        from pipeline.mock import MockR2
        mock = MockR2()
        results = []
        for clip in clip_paths:
            if not clip.get("local_path") or not os.path.exists(clip["local_path"]):
                logger.warning("Skipping upload — file not found: %s", clip.get("local_path"))
                continue
            key = _clip_key(clip["youtube_id"], clip["rank"])
            url = mock.upload(clip["local_path"], key)
            results.append({
                "youtube_id": clip["youtube_id"],
                "rank": clip["rank"],
                "r2_url": url,
            })
        return results

    client = _get_r2_client(config)
    results = []

    for clip in clip_paths:
        local_path = clip.get("local_path")
        if not local_path or not os.path.exists(local_path):
            logger.warning("Skipping upload — file not found: %s", local_path)
            continue

        youtube_id = clip["youtube_id"]
        rank = clip["rank"]
        key = _clip_key(youtube_id, rank)

        try:
            logger.info("Uploading %s to R2 key: %s", local_path, key)
            with open(local_path, "rb") as f:
                client.put_object(
                    Bucket=config.r2_bucket_name,
                    Key=key,
                    Body=f,
                    ContentType="video/mp4",
                )
            url = _clip_url(youtube_id, rank, config)
            results.append({"youtube_id": youtube_id, "rank": rank, "r2_url": url})
            logger.info("Uploaded: %s", url)
        except Exception as exc:
            logger.error("Failed to upload %s: %s", local_path, exc)

    return results


def cleanup_old_clips(config, days: int = 30):
    """Deletes clips older than `days` from R2."""
    if config.is_mock_mode():
        from pipeline.mock import MockR2
        MockR2().cleanup(older_than_days=days)
        return

    client = _get_r2_client(config)
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=config.r2_bucket_name, Prefix="clips/")

        to_delete = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["LastModified"] < cutoff:
                    to_delete.append({"Key": obj["Key"]})

        if not to_delete:
            logger.info("No old clips to clean up")
            return

        # Batch delete (max 1000 per call)
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i: i + 1000]
            client.delete_objects(
                Bucket=config.r2_bucket_name,
                Delete={"Objects": batch, "Quiet": True},
            )
        logger.info("Cleaned up %d old clips from R2", len(to_delete))

    except Exception as exc:
        logger.error("Failed to clean up old clips: %s", exc)


def delete_clips_for_video(youtube_id: str, config, exclude_rank: int = None):
    """Deletes all clips for a youtube_id from R2, optionally keeping one rank."""
    if config.is_mock_mode():
        from pipeline.mock import MockR2
        mock = MockR2()
        for rank in range(1, 4):
            if exclude_rank is not None and rank == exclude_rank:
                continue
            mock.delete(_clip_key(youtube_id, rank))
        return

    client = _get_r2_client(config)
    prefix = f"clips/{youtube_id}/"

    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=config.r2_bucket_name, Prefix=prefix)

        to_delete = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if exclude_rank is not None:
                    # Keep the selected rank
                    keep_key = _clip_key(youtube_id, exclude_rank)
                    if key == keep_key:
                        continue
                to_delete.append({"Key": key})

        if not to_delete:
            return

        client.delete_objects(
            Bucket=config.r2_bucket_name,
            Delete={"Objects": to_delete, "Quiet": True},
        )
        logger.info("Deleted %d clip(s) for %s (kept rank %s)", len(to_delete), youtube_id, exclude_rank)

    except Exception as exc:
        logger.error("Failed to delete clips for %s: %s", youtube_id, exc)
