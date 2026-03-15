"""
Queue manager for @thechoirsource pipeline.
All JSON queue file operations: pending, approved, archive.
"""

import fcntl
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

PENDING_FILE = "pending.json"
APPROVED_FILE = "approved.json"
ARCHIVE_FILE = "archive.json"


class QueueManager:
    def __init__(self, queue_dir: str = "queue"):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)

        self._pending: list = []
        self._approved: list = []
        self._archive: list = []
        self._load()

    # ------------------------------------------------------------------
    # Internal load/save
    # ------------------------------------------------------------------

    def _load_file(self, filename: str) -> list:
        path = self.queue_dir / filename
        if not path.exists():
            logger.warning("Queue file not found: %s — treating as empty", path)
            return []
        try:
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                return []
            data = json.loads(text)
            if not isinstance(data, list):
                logger.warning("Queue file %s does not contain a list — treating as empty", path)
                return []
            return data
        except json.JSONDecodeError as exc:
            logger.warning("Malformed JSON in %s: %s — treating as empty", path, exc)
            return []

    def _load(self):
        self._pending = self._load_file(PENDING_FILE)
        self._approved = self._load_file(APPROVED_FILE)
        self._archive = self._load_file(ARCHIVE_FILE)

    def _save(self):
        """Write all three JSON files atomically (write to .tmp then rename)."""
        self._save_file(PENDING_FILE, self._pending)
        self._save_file(APPROVED_FILE, self._approved)
        self._save_file(ARCHIVE_FILE, self._archive)

    def _save_file(self, filename: str, data: list):
        path = self.queue_dir / filename
        tmp_path = path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                # Acquire exclusive lock for the duration of the write
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f, fcntl.LOCK_UN)
            os.replace(tmp_path, path)  # atomic rename
        except Exception as exc:
            logger.error("Failed to save queue file %s: %s", path, exc)
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_pending(self) -> list:
        return list(self._pending)

    def get_approved(self) -> list:
        """Returns all approved items, sorted by scheduled_at ascending."""
        items = list(self._approved)
        items.sort(key=lambda x: x.get("scheduled_at", ""))
        return items

    def get_archive(self) -> list:
        return list(self._archive)

    def get_all_youtube_ids(self) -> set:
        """Returns all youtube_ids across all three queues. Used for dedup."""
        ids = set()
        for item in self._pending + self._approved + self._archive:
            if "youtube_id" in item:
                ids.add(item["youtube_id"])
        return ids

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def add_pending(self, items: list):
        """Appends items to pending queue. Saves to disk."""
        self._pending.extend(items)
        self._save()
        logger.info("Added %d items to pending queue", len(items))

    def approve(
        self,
        youtube_id: str,
        selected_clip_rank: int,
        edited_caption: str,
        edited_hashtags: str,
        scheduled_at: str,
    ) -> bool:
        """
        Moves item from pending to approved.
        Updates caption/hashtags/clip selection.
        Returns False if youtube_id not found in pending.
        """
        item = self._find_and_remove(self._pending, youtube_id)
        if item is None:
            logger.warning("approve: youtube_id %s not found in pending", youtube_id)
            return False

        item["selected_clip_rank"] = selected_clip_rank
        item["caption"] = edited_caption
        item["hashtags"] = edited_hashtags
        item["scheduled_at"] = scheduled_at
        item["approved_at"] = datetime.now(tz=timezone.utc).isoformat()
        item["status"] = "approved"

        self._approved.append(item)
        self._save()
        logger.info("Approved %s, scheduled at %s", youtube_id, scheduled_at)
        return True

    def reject(self, youtube_id: str) -> bool:
        """Moves item from pending to archive with status='rejected'."""
        item = self._find_and_remove(self._pending, youtube_id)
        if item is None:
            logger.warning("reject: youtube_id %s not found in pending", youtube_id)
            return False

        item["status"] = "rejected"
        item["rejected_at"] = datetime.now(tz=timezone.utc).isoformat()
        self._archive.append(item)
        self._save()
        logger.info("Rejected %s", youtube_id)
        return True

    def mark_posted(self, youtube_id: str) -> bool:
        """Moves item from approved to archive with status='posted' and posted_at."""
        item = self._find_and_remove(self._approved, youtube_id)
        if item is None:
            logger.warning("mark_posted: youtube_id %s not found in approved", youtube_id)
            return False

        item["status"] = "posted"
        item["posted_at"] = datetime.now(tz=timezone.utc).isoformat()
        self._archive.append(item)
        self._save()
        logger.info("Marked %s as posted", youtube_id)
        return True

    def get_due_for_posting(self, now: datetime) -> list:
        """Returns approved items where scheduled_at <= now."""
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        due = []
        for item in self._approved:
            scheduled_str = item.get("scheduled_at", "")
            if not scheduled_str:
                continue
            try:
                scheduled = datetime.fromisoformat(scheduled_str)
                if scheduled.tzinfo is None:
                    scheduled = scheduled.replace(tzinfo=timezone.utc)
                if scheduled <= now:
                    due.append(item)
            except ValueError as exc:
                logger.warning(
                    "Invalid scheduled_at for %s: %s (%s)",
                    item.get("youtube_id"), scheduled_str, exc
                )
        return due

    def reload(self):
        """Reload data from disk (useful if files were modified externally)."""
        self._load()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_and_remove(lst: list, youtube_id: str) -> Optional[dict]:
        for i, item in enumerate(lst):
            if item.get("youtube_id") == youtube_id:
                return lst.pop(i)
        return None
