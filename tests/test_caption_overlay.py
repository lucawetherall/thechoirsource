"""Tests for pipeline/caption_overlay.py"""

import os
import shutil
import subprocess
import tempfile

import pytest

from pipeline.caption_overlay import overlay_captions, _truncate, _resolve_font, _escape_ffmpeg_text


# ------------------------------------------------------------------
# Unit tests
# ------------------------------------------------------------------

def test_truncate_short_text():
    assert _truncate("Short text", 38) == "Short text"


def test_truncate_long_text():
    text = "A" * 50
    result = _truncate(text, 38)
    assert len(result) == 38
    assert result.endswith("…")


def test_truncate_exact_length():
    text = "A" * 38
    assert _truncate(text, 38) == text


def test_escape_ffmpeg_text_colon():
    assert "\\:" in _escape_ffmpeg_text("Hello: World")


def test_escape_ffmpeg_text_quote():
    result = _escape_ffmpeg_text("it's fine")
    # Should not contain unescaped single quote
    assert "\\'" in result or result.count("'") == 0 or "'\\''" in result


def test_resolve_font_fallback(tmp_path):
    """If brand font doesn't exist, should return fallback or None."""
    result = _resolve_font(str(tmp_path / "nonexistent.ttf"))
    # Either None or a path to an existing font
    if result is not None:
        assert os.path.exists(result), f"Resolved font path {result} does not exist"


# ------------------------------------------------------------------
# Integration tests
# ------------------------------------------------------------------

class MockConfig:
    @property
    def brand(self):
        return {
            "font_file": "assets/fonts/Montserrat-SemiBold.ttf",  # won't exist — use fallback
            "font_size": 42,
            "font_colour": "#FFFFFF",
            "shadow_colour": "#000000",
            "shadow_x": 2,
            "shadow_y": 2,
            "position": "bottom",
            "margin_bottom": 150,
            "margin_sides": 60,
            "line_spacing": 14,
            "background_box": True,
            "background_colour": "#00000099",
            "background_padding": 20,
            "watermark_text": "@thechoirsource",
            "watermark_font_size": 28,
            "watermark_position": "top_right",
            "watermark_margin": 30,
            "watermark_opacity": 0.85,
        }


@pytest.fixture(scope="module")
def portrait_clip(tmp_path_factory):
    """Generate a short 1080x1920 portrait video clip for overlay testing."""
    tmp = tmp_path_factory.mktemp("overlay_test")
    path = str(tmp / "test_clip.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", "color=c=darkblue:size=1080x1920:rate=25:duration=8",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=8:sample_rate=22050",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "40",
        "-c:a", "aac", "-b:a", "32k",
        "-pix_fmt", "yuv420p",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, f"FFmpeg failed: {result.stderr.decode()[-500:]}"
    return path


def copy_clip(src: str, dst_dir: str, name: str) -> str:
    """Copy a clip to a temp dir for testing (so original is preserved)."""
    dst = os.path.join(dst_dir, name)
    shutil.copy2(src, dst)
    return dst


def test_overlay_runs_without_error(portrait_clip, tmp_path):
    clip = copy_clip(portrait_clip, str(tmp_path), "overlay_test_1.mp4")
    metadata = {
        "piece_title": "Miserere mei, Deus",
        "composer": "Gregorio Allegri",
        "ensemble_name": "Choir of King's College, Cambridge",
    }
    config = MockConfig()
    result_path = overlay_captions(clip, metadata, config)
    assert os.path.exists(result_path)
    assert os.path.getsize(result_path) > 1024


def test_overlay_with_partial_metadata(portrait_clip, tmp_path):
    """Should not crash when some metadata fields are empty."""
    clip = copy_clip(portrait_clip, str(tmp_path), "overlay_test_2.mp4")
    metadata = {
        "piece_title": "Evensong",
        "composer": "",
        "ensemble_name": "Westminster Abbey",
    }
    config = MockConfig()
    result_path = overlay_captions(clip, metadata, config)
    assert os.path.exists(result_path)


def test_overlay_with_empty_metadata(portrait_clip, tmp_path):
    """Should not crash with all-empty metadata."""
    clip = copy_clip(portrait_clip, str(tmp_path), "overlay_test_3.mp4")
    metadata = {"piece_title": "", "composer": "", "ensemble_name": ""}
    config = MockConfig()
    result_path = overlay_captions(clip, metadata, config)
    assert os.path.exists(result_path)


def test_overlay_with_long_metadata(portrait_clip, tmp_path):
    """Long text should be truncated without crashing."""
    clip = copy_clip(portrait_clip, str(tmp_path), "overlay_test_4.mp4")
    metadata = {
        "piece_title": "A Very Long Piece Name That Exceeds The Maximum Character Count Limit",
        "composer": "Johannes Chrysostomus Wolfgangus Theophilus Mozart",
        "ensemble_name": "The Choir of the Cathedral of St. Mary the Virgin, Edinburgh",
    }
    config = MockConfig()
    result_path = overlay_captions(clip, metadata, config)
    assert os.path.exists(result_path)


def test_overlay_returns_same_path(portrait_clip, tmp_path):
    """overlay_captions should return the same path as the input."""
    clip = copy_clip(portrait_clip, str(tmp_path), "overlay_test_5.mp4")
    metadata = {"piece_title": "Test", "composer": "Test", "ensemble_name": "Test"}
    config = MockConfig()
    result_path = overlay_captions(clip, metadata, config)
    assert result_path == clip


def test_overlay_output_is_valid_video(portrait_clip, tmp_path):
    """FFprobe should confirm the output is a valid video."""
    clip = copy_clip(portrait_clip, str(tmp_path), "overlay_test_6.mp4")
    metadata = {"piece_title": "O Magnum Mysterium", "composer": "Lauridsen", "ensemble_name": "VOCES8"}
    config = MockConfig()
    result_path = overlay_captions(clip, metadata, config)

    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-select_streams", "v:0",
         "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0", result_path],
        capture_output=True, text=True
    )
    assert probe.returncode == 0
    assert "x" in probe.stdout.strip()
