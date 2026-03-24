"""
Mock/stub implementations of all external services.
Used when MOCK_MODE=true is set, allowing the pipeline to run without API keys.
"""

import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mock YouTube API
# ---------------------------------------------------------------------------

MOCK_VIDEOS = [
    {
        "youtube_id": "mock_vid_001",
        "title": "Allegri - Miserere mei, Deus | Choir of King's College, Cambridge",
        "channel_name": "King's College Cambridge",
        "channel_id": "UC9qIt1B9hELcfJdOMUmBLTQ",
        "description": "A breathtaking performance of Allegri's Miserere from King's College Chapel.",
        "duration_seconds": 510,
        "view_count": 154200,
        "published_at": "2026-03-10T14:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_001",
        "source": "channel",
    },
    {
        "youtube_id": "mock_vid_002",
        "title": "The Sixteen: Palestrina — Sicut Cervus",
        "channel_name": "The Sixteen",
        "channel_id": "PLACEHOLDER_CHANNEL_ID_the_sixteen",
        "description": "Harry Christophers conducts The Sixteen in this serene motet.",
        "duration_seconds": 315,
        "view_count": 89300,
        "published_at": "2026-03-08T10:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_002",
        "source": "search",
    },
    {
        "youtube_id": "mock_vid_003",
        "title": "Evensong from Westminster Abbey, 10 March 2026",
        "channel_name": "Westminster Abbey",
        "channel_id": "UCOchT7ZJ4TXe3stdLW1rofA",
        "description": "Choral Evensong sung by the Choir of Westminster Abbey.",
        "duration_seconds": 3600,
        "view_count": 42100,
        "published_at": "2026-03-10T18:30:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_003",
        "source": "channel",
    },
    {
        "youtube_id": "mock_vid_004",
        "title": "VOCES8: 'O Magnum Mysterium' (Lauridsen)",
        "channel_name": "VOCES8",
        "channel_id": "UCpbz4KBCNrKyoDNBDGQoAbg",
        "description": "VOCES8 perform Morten Lauridsen's luminous O Magnum Mysterium.",
        "duration_seconds": 420,
        "view_count": 231400,
        "published_at": "2026-03-05T12:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_004",
        "source": "search",
    },
    {
        "youtube_id": "mock_vid_005",
        "title": "Stanford Magnificat in G — Choir of St John's College, Cambridge",
        "channel_name": "St John's College Cambridge",
        "channel_id": "UCFMJNSNAMbRQJX0EiPJP0rg",
        "description": "The Choir of St John's College performs Stanford's beloved Magnificat.",
        "duration_seconds": 600,
        "view_count": 67800,
        "published_at": "2026-03-07T09:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_005",
        "source": "channel",
    },
    {
        "youtube_id": "mock_vid_006",
        "title": "J.S. Bach: Jesu, meine Freude BWV 227 | Netherlands Bach Society",
        "channel_name": "Netherlands Bach Society",
        "channel_id": "UC_mock_bach_society",
        "description": "All of Bach project: BWV 227 performed live.",
        "duration_seconds": 1800,
        "view_count": 95600,
        "published_at": "2026-03-06T16:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_006",
        "source": "search",
    },
    {
        "youtube_id": "mock_vid_007",
        "title": "Choral Evensong live from York Minster",
        "channel_name": "York Minster",
        "channel_id": "UC_mock_york_minster",
        "description": "BBC Radio 3 broadcast of Choral Evensong from York Minster.",
        "duration_seconds": 2700,
        "view_count": 18900,
        "published_at": "2026-03-11T17:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_007",
        "source": "search",
    },
    {
        "youtube_id": "mock_vid_008",
        "title": "Ola Gjeilo - Northern Lights (Aurora Borealis) | Tenebrae",
        "channel_name": "Tenebrae Choir",
        "channel_id": "PLACEHOLDER_CHANNEL_ID_tenebrae",
        "description": "Tenebrae perform Ola Gjeilo's hauntingly beautiful Northern Lights.",
        "duration_seconds": 480,
        "view_count": 312000,
        "published_at": "2026-03-04T11:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_008",
        "source": "search",
    },
    {
        "youtube_id": "mock_vid_009",
        "title": "Tallis - Spem in Alium | The Tallis Scholars",
        "channel_name": "The Tallis Scholars",
        "channel_id": "PLACEHOLDER_CHANNEL_ID_tallis_scholars",
        "description": "Thomas Tallis' extraordinary 40-part motet performed by The Tallis Scholars.",
        "duration_seconds": 540,
        "view_count": 445000,
        "published_at": "2026-03-01T15:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_009",
        "source": "search",
    },
    {
        "youtube_id": "mock_vid_010",
        "title": "Britten - A Ceremony of Carols | Trinity College Cambridge",
        "channel_name": "Trinity College Cambridge",
        "channel_id": "PLACEHOLDER_CHANNEL_ID_trinity_cambridge",
        "description": "The Choir of Trinity College Cambridge performs Britten's Ceremony of Carols.",
        "duration_seconds": 1200,
        "view_count": 28500,
        "published_at": "2026-03-09T13:00:00Z",
        "url": "https://www.youtube.com/watch?v=mock_vid_010",
        "source": "channel",
    },
]


class MockYouTubeAPI:
    """Returns a fixed set of fake video results with realistic metadata."""

    def search(self, **kwargs) -> list:
        return MOCK_VIDEOS[:7]

    def get_channel_videos(self, channel_id: str, **kwargs) -> list:
        return [v for v in MOCK_VIDEOS if v["channel_id"] == channel_id][:3]


# ---------------------------------------------------------------------------
# Mock Downloader
# ---------------------------------------------------------------------------

class MockDownloader:
    """
    Generates a synthetic test video using FFmpeg's built-in signal generators.
    Falls back to generating a minimal valid MP4 with WAV audio using pure Python
    if FFmpeg is not available.

    FFmpeg version has varied audio dynamics so the audio analysis algorithm can
    find interesting segments:
      0-30s:  quiet sine wave (low amplitude)
      30-35s: sudden loud burst (multiple frequencies, high amplitude)
      35-60s: medium amplitude with gradual crescendo
      60-65s: silence
      65-90s: another loud, complex section
      90-120s: quiet fade out
    """

    def generate(self, youtube_id: str, output_dir: str) -> dict:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{youtube_id}.mp4")

        if os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            logger.info("Mock video already exists at %s", output_path)
            return {"youtube_id": youtube_id, "local_path": output_path, "success": True}

        # Try FFmpeg first (produces high-quality test videos with dynamic audio)
        if shutil.which("ffmpeg"):
            result = self._generate_with_ffmpeg(youtube_id, output_path)
            if result["success"]:
                return result
            logger.warning("FFmpeg generation failed, falling back to pure-Python generator")

        # Fallback: generate a minimal WAV file with dynamic audio using numpy/scipy.
        # The pipeline's audio_analysis module works on extracted WAV audio, so this
        # is sufficient for testing even though it won't have a video track.
        return self._generate_fallback(youtube_id, output_path, output_dir)

    def _generate_fallback(self, youtube_id: str, output_path: str, output_dir: str) -> dict:
        """Generate a minimal MP4-like file with a WAV audio track using pure Python."""
        import math
        import struct
        import wave

        logger.info("Generating fallback test video (pure Python) for %s", youtube_id)

        # Generate a 120-second WAV with dynamic contrast sections
        sample_rate = 22050
        duration = 120
        n_samples = sample_rate * duration
        samples = []

        for i in range(n_samples):
            t = i / sample_rate
            # Section-based amplitude to create dynamic contrast
            if t < 30:
                amp = 0.05  # quiet
            elif t < 35:
                amp = 0.8  # loud burst
            elif t < 60:
                amp = 0.1 + (t - 35) / 25 * 0.4  # crescendo
            elif t < 65:
                amp = 0.02  # near silence
            elif t < 90:
                amp = 0.7  # second loud section
            else:
                amp = max(0.02, 0.3 - (t - 90) / 30 * 0.28)  # fade out

            # Mix sine waves for richer sound in loud sections
            val = amp * math.sin(2 * math.pi * 440 * t)
            if amp > 0.3:
                val += amp * 0.5 * math.sin(2 * math.pi * 880 * t)
                val += amp * 0.3 * math.sin(2 * math.pi * 1320 * t)

            samples.append(max(-1.0, min(1.0, val)))

        # Write WAV file
        wav_path = os.path.join(output_dir, f"{youtube_id}.wav")
        with wave.open(wav_path, "w") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)  # 16-bit
            wf.setframerate(sample_rate)
            for s in samples:
                wf.writeframes(struct.pack("<h", int(s * 32767)))

        # If ffmpeg is available, wrap in a proper MP4 container
        if shutil.which("ffmpeg"):
            cmd = [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", "color=c=blue:size=320x180:rate=1:duration=120",
                "-i", wav_path,
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "51",
                "-c:a", "aac", "-b:a", "32k",
                "-pix_fmt", "yuv420p", "-shortest",
                output_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0 and os.path.exists(output_path):
                os.unlink(wav_path)
                logger.info("Fallback video generated: %s", output_path)
                return {"youtube_id": youtube_id, "local_path": output_path, "success": True}

        # No ffmpeg at all — use the WAV directly as the "video" file.
        # The pipeline's audio_analysis extracts audio via ffmpeg anyway, but
        # the mock can at least provide a valid audio file for unit tests.
        # Rename to .mp4 extension so file discovery works.
        if os.path.exists(wav_path):
            shutil.move(wav_path, output_path)
            logger.info("Fallback WAV-as-MP4 generated: %s (no ffmpeg available)", output_path)
            return {"youtube_id": youtube_id, "local_path": output_path, "success": True}

        return {
            "youtube_id": youtube_id,
            "local_path": None,
            "success": False,
            "error_message": "Failed to generate fallback test file",
        }

    def _generate_with_ffmpeg(self, youtube_id: str, output_path: str) -> dict:
        """Generate a full test video with dynamic audio using FFmpeg."""
        logger.info("Generating synthetic test video at %s", output_path)

        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "color=c=blue:size=1920x1080:rate=25:duration=120",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=120:sample_rate=22050",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=120:sample_rate=22050",
            "-f", "lavfi", "-i", "sine=frequency=880:duration=120:sample_rate=22050",
            "-f", "lavfi", "-i", "sine=frequency=1320:duration=120:sample_rate=22050",
            "-f", "lavfi", "-i", "sine=frequency=330:duration=120:sample_rate=22050",
            "-f", "lavfi", "-i", "sine=frequency=660:duration=120:sample_rate=22050",
            "-f", "lavfi", "-i", "sine=frequency=990:duration=120:sample_rate=22050",
            "-filter_complex",
            (
                "[1]volume=0.05[quiet];"
                "[2]volume=enable='between(t,30,35)':volume=0.8:eval=frame[burst_a];"
                "[3]volume=enable='between(t,30,35)':volume=0.8:eval=frame[burst_b];"
                "[4]volume=enable='between(t,30,35)':volume=0.8:eval=frame[burst_c];"
                "[1]volume=enable='between(t,35,60)':volume='0.1+(t-35)/25*0.4':eval=frame[cresc];"
                "[5]volume=enable='between(t,65,90)':volume=0.7:eval=frame[loud2_a];"
                "[6]volume=enable='between(t,65,90)':volume=0.7:eval=frame[loud2_b];"
                "[7]volume=enable='between(t,65,90)':volume=0.7:eval=frame[loud2_c];"
                "[1]volume=enable='between(t,90,120)':volume='0.3-(t-90)/30*0.28':eval=frame[fadeout];"
                "[quiet][burst_a][burst_b][burst_c][cresc][loud2_a][loud2_b][loud2_c][fadeout]"
                "amix=inputs=9:normalize=0[audio]"
            ),
            "-map", "0:v",
            "-map", "[audio]",
            "-t", "120",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "35",
            "-c:a", "aac", "-b:a", "64k",
            "-pix_fmt", "yuv420p",
            output_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("FFmpeg failed to generate mock video: %s", result.stderr[-2000:])
            return {
                "youtube_id": youtube_id,
                "local_path": None,
                "success": False,
                "error_message": result.stderr[-500:],
            }

        if not os.path.exists(output_path) or os.path.getsize(output_path) < 1024:
            return {
                "youtube_id": youtube_id,
                "local_path": None,
                "success": False,
                "error_message": "Output file missing or too small",
            }

        logger.info("Mock video generated: %s (%d bytes)", output_path, os.path.getsize(output_path))
        return {"youtube_id": youtube_id, "local_path": output_path, "success": True}


# ---------------------------------------------------------------------------
# Mock Anthropic API
# ---------------------------------------------------------------------------

class MockAnthropicAPI:
    """Returns plausible caption text and hashtags without calling the API."""

    def generate_copy(self, metadata: dict) -> dict:
        piece = metadata.get("piece_title", "this piece")
        ensemble = metadata.get("ensemble_name", "this ensemble")
        composer = metadata.get("composer", "")

        ctas = [
            "Turn up the volume.",
            "Close your eyes and listen.",
            "Let this wash over you.",
            "Headphones on.",
            "This one's for your soul.",
        ]
        import hashlib
        cta_index = int(hashlib.md5(piece.encode()).hexdigest(), 16) % len(ctas)
        cta = ctas[cta_index]

        if composer:
            caption = (
                f"{ensemble} performing {composer}'s {piece}. "
                f"Few things in music match the human voice in perfect harmony. {cta}"
            )
        else:
            caption = (
                f"{ensemble} in {piece}. "
                f"The kind of music that makes you stop and just listen. {cta}"
            )

        hashtags = [
            "#choir", "#choralmusic", "#classicalmusic", "#sacredmusic",
            "#choirtok", "#choirsource", "#choral", "#singing",
            "#cathedral", "#classicalmusiclovers",
        ]

        hashtags_string = " ".join(hashtags)
        return {
            "caption": caption,
            "hashtags": hashtags,
            "hashtags_string": hashtags_string,
            "full_post_text": caption + "\n\n" + hashtags_string,
        }

    def parse_metadata(self, title: str, description: str, channel_name: str) -> dict:
        # Delegate to rule-based parser
        from pipeline.metadata_parser import _rule_based_parse
        return _rule_based_parse(title, channel_name)


# ---------------------------------------------------------------------------
# Mock R2
# ---------------------------------------------------------------------------

class MockR2:
    """Writes files to a local directory instead of uploading to Cloudflare R2."""

    def __init__(self, base_dir: str = None):
        self.base_dir = Path(base_dir) if base_dir else Path(tempfile.gettempdir()) / "mock_r2"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def upload(self, local_path: str, key: str, content_type: str = "video/mp4") -> str:
        dest = self.base_dir / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)
        url = f"file://{dest}"
        logger.info("MockR2: uploaded %s → %s", local_path, url)
        return url

    def delete(self, key: str) -> bool:
        target = self.base_dir / key
        if target.exists():
            target.unlink()
            logger.info("MockR2: deleted %s", target)
            return True
        return False

    def list_objects(self, prefix: str = "") -> list:
        results = []
        for path in self.base_dir.rglob("*"):
            if path.is_file():
                rel = str(path.relative_to(self.base_dir))
                if rel.startswith(prefix):
                    results.append({
                        "Key": rel,
                        "LastModified": datetime.fromtimestamp(
                            path.stat().st_mtime, tz=timezone.utc
                        ),
                        "Size": path.stat().st_size,
                    })
        return results

    def cleanup(self, older_than_days: int = 30):
        from datetime import timedelta
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=older_than_days)
        deleted = 0
        for path in list(self.base_dir.rglob("*")):
            if path.is_file():
                mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    path.unlink()
                    deleted += 1
        logger.info("MockR2: cleaned up %d old files", deleted)


# ---------------------------------------------------------------------------
# Mock Publisher
# ---------------------------------------------------------------------------

class MockPublisher:
    """Logs what would be posted to each platform. Returns success statuses."""

    def publish_instagram(self, clip_url: str, caption: str) -> dict:
        logger.info(
            "MockPublisher [Instagram] Would post clip: %s\nCaption: %.100s...",
            clip_url, caption
        )
        return {
            "platform": "instagram",
            "success": True,
            "post_id": "mock_ig_post_12345",
            "error": None,
        }

    def publish_facebook(self, clip_url: str, caption: str) -> dict:
        logger.info(
            "MockPublisher [Facebook] Would post clip: %s\nCaption: %.100s...",
            clip_url, caption
        )
        return {
            "platform": "facebook",
            "success": True,
            "post_id": "mock_fb_post_67890",
            "error": None,
        }

    def publish_tiktok(self, clip_url: str, caption: str) -> dict:
        logger.info(
            "MockPublisher [TikTok] Would post clip: %s\nCaption: %.100s...",
            clip_url, caption
        )
        return {
            "platform": "tiktok",
            "success": True,
            "post_id": "mock_tt_post_11111",
            "error": None,
        }
