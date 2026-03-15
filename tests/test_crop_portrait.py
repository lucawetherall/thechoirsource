"""Tests for pipeline/crop_portrait.py"""

import os
import subprocess
import tempfile

import pytest

from pipeline.crop_portrait import crop_to_portrait, _get_video_dimensions


@pytest.fixture(scope="module")
def landscape_video(tmp_path_factory):
    """Generate a 10-second synthetic 1920x1080 landscape video."""
    tmp = tmp_path_factory.mktemp("crop_test")
    path = str(tmp / "landscape.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", "color=c=red:size=1920x1080:rate=25:duration=10",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=10:sample_rate=22050",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "40",
        "-c:a", "aac", "-b:a", "32k",
        "-pix_fmt", "yuv420p",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, f"FFmpeg failed: {result.stderr.decode()[-500:]}"
    return path


@pytest.fixture(scope="module")
def portrait_video(tmp_path_factory):
    """Generate a 10-second synthetic 1080x1920 portrait video."""
    tmp = tmp_path_factory.mktemp("crop_test_portrait")
    path = str(tmp / "portrait.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", "color=c=green:size=1080x1920:rate=25:duration=10",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=10:sample_rate=22050",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "40",
        "-c:a", "aac", "-b:a", "32k",
        "-pix_fmt", "yuv420p",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, f"FFmpeg failed: {result.stderr.decode()[-500:]}"
    return path


def get_resolution(path: str) -> tuple:
    """Use ffprobe to get (width, height) of the video."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=s=x:p=0",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    w, h = result.stdout.strip().split("x")
    return int(w), int(h)


def get_duration(path: str) -> float:
    """Use ffprobe to get video duration."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-show_entries", "format=duration",
        "-of", "csv=p=0",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return float(result.stdout.strip())


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

def test_get_video_dimensions_landscape(landscape_video):
    w, h = _get_video_dimensions(landscape_video)
    assert w == 1920
    assert h == 1080


def test_get_video_dimensions_portrait(portrait_video):
    w, h = _get_video_dimensions(portrait_video)
    assert w == 1080
    assert h == 1920


def test_crop_landscape_to_portrait(landscape_video, tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.crop_portrait.OUTPUT_DIR", str(tmp_path))
    clip = {"rank": 1, "start_seconds": 1.0, "end_seconds": 8.0}
    output_path = crop_to_portrait(landscape_video, clip, "test_land", config=None)

    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) > 1024

    w, h = get_resolution(output_path)
    assert w == 1080, f"Expected width 1080, got {w}"
    assert h == 1920, f"Expected height 1920, got {h}"


def test_crop_duration_accurate(landscape_video, tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.crop_portrait.OUTPUT_DIR", str(tmp_path))
    clip = {"rank": 2, "start_seconds": 0.0, "end_seconds": 5.0}
    output_path = crop_to_portrait(landscape_video, clip, "test_dur", config=None)
    dur = get_duration(output_path)
    assert abs(dur - 5.0) < 1.0, f"Expected ~5s clip, got {dur:.1f}s"


def test_crop_portrait_input(portrait_video, tmp_path, monkeypatch):
    """Portrait input should be handled without crop filter."""
    monkeypatch.setattr("pipeline.crop_portrait.OUTPUT_DIR", str(tmp_path))
    clip = {"rank": 1, "start_seconds": 1.0, "end_seconds": 8.0}
    output_path = crop_to_portrait(portrait_video, clip, "test_port", config=None)

    assert os.path.exists(output_path)
    w, h = get_resolution(output_path)
    assert w == 1080
    assert h == 1920


def test_crop_ranks_produce_different_files(landscape_video, tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.crop_portrait.OUTPUT_DIR", str(tmp_path))
    clip1 = {"rank": 1, "start_seconds": 0.0, "end_seconds": 5.0}
    clip2 = {"rank": 2, "start_seconds": 2.0, "end_seconds": 7.0}
    p1 = crop_to_portrait(landscape_video, clip1, "test_ranks", config=None)
    p2 = crop_to_portrait(landscape_video, clip2, "test_ranks", config=None)
    assert p1 != p2
    assert os.path.exists(p1)
    assert os.path.exists(p2)
