"""
Tests for pipeline/audio_analysis.py

Generates a synthetic audio file programmatically using numpy + soundfile.
Does NOT require any external video files for pure audio tests.
For tests requiring a video file, generates a short FFmpeg test video.
"""

import os
import subprocess
import tempfile

import numpy as np
import pytest
import soundfile as sf

from pipeline.audio_analysis import analyse_audio, _compute_rms, _normalise

SAMPLE_RATE = 22050
DURATION = 120  # seconds


def make_synthetic_audio(path: str):
    """
    Generate a 120-second synthetic audio file with varying dynamics:
      0–30s:  quiet sine at 440 Hz, amplitude 0.05
      30–35s: loud burst — sum of 440+880+1320 Hz, amplitude 0.8
      35–60s: crescendo 440 Hz, amplitude 0.1 → 0.5
      60–65s: silence
      65–90s: loud — sum of 330+660+990 Hz, amplitude 0.7
      90–120s: fade out 440 Hz, amplitude 0.3 → 0.02
    """
    t = np.linspace(0, DURATION, SAMPLE_RATE * DURATION, endpoint=False)
    audio = np.zeros(len(t))

    def idx(start_s, end_s):
        return slice(int(start_s * SAMPLE_RATE), int(end_s * SAMPLE_RATE))

    # 0–30s: quiet
    audio[idx(0, 30)] += 0.05 * np.sin(2 * np.pi * 440 * t[idx(0, 30)])

    # 30–35s: loud burst
    audio[idx(30, 35)] += 0.8 * np.sin(2 * np.pi * 440 * t[idx(30, 35)])
    audio[idx(30, 35)] += 0.8 * np.sin(2 * np.pi * 880 * t[idx(30, 35)])
    audio[idx(30, 35)] += 0.8 * np.sin(2 * np.pi * 1320 * t[idx(30, 35)])

    # 35–60s: crescendo
    ramp = np.linspace(0.1, 0.5, int(25 * SAMPLE_RATE))
    audio[idx(35, 60)] += ramp * np.sin(2 * np.pi * 440 * t[idx(35, 60)])

    # 60–65s: silence (already zero)

    # 65–90s: loud second section
    audio[idx(65, 90)] += 0.7 * np.sin(2 * np.pi * 330 * t[idx(65, 90)])
    audio[idx(65, 90)] += 0.7 * np.sin(2 * np.pi * 660 * t[idx(65, 90)])
    audio[idx(65, 90)] += 0.7 * np.sin(2 * np.pi * 990 * t[idx(65, 90)])

    # 90–120s: fade out
    ramp_down = np.linspace(0.3, 0.02, int(30 * SAMPLE_RATE))
    audio[idx(90, 120)] += ramp_down * np.sin(2 * np.pi * 440 * t[idx(90, 120)])

    # Clip to [-1, 1]
    audio = np.clip(audio, -1.0, 1.0)
    sf.write(path, audio, SAMPLE_RATE)


def make_flat_audio(path: str):
    """Generate a constant-amplitude audio file."""
    t = np.linspace(0, DURATION, SAMPLE_RATE * DURATION, endpoint=False)
    audio = 0.3 * np.sin(2 * np.pi * 440 * t)
    sf.write(path, audio, SAMPLE_RATE)


def make_video_from_audio(audio_path: str, video_path: str):
    """Wrap an audio file in a video container using FFmpeg."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"color=c=black:size=320x240:rate=25:duration={DURATION}",
        "-i", audio_path,
        "-shortest",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "40",
        "-c:a", "aac", "-b:a", "32k",
        video_path,
    ]
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0, f"FFmpeg failed: {result.stderr.decode()[-1000:]}"


@pytest.fixture(scope="module")
def dynamic_video(tmp_path_factory):
    """Generate a dynamic test video once for the module."""
    tmp = tmp_path_factory.mktemp("audio_test")
    audio_path = str(tmp / "dynamic.wav")
    video_path = str(tmp / "dynamic.mp4")
    make_synthetic_audio(audio_path)
    make_video_from_audio(audio_path, video_path)
    return video_path


@pytest.fixture(scope="module")
def flat_video(tmp_path_factory):
    """Generate a flat-audio test video once for the module."""
    tmp = tmp_path_factory.mktemp("flat_test")
    audio_path = str(tmp / "flat.wav")
    video_path = str(tmp / "flat.mp4")
    make_flat_audio(audio_path)
    make_video_from_audio(audio_path, video_path)
    return video_path


# ------------------------------------------------------------------
# Unit tests
# ------------------------------------------------------------------

def test_normalise_basic():
    arr = np.array([0.0, 1.0, 2.0, 3.0])
    n = _normalise(arr)
    assert n[0] == pytest.approx(0.0)
    assert n[-1] == pytest.approx(1.0)


def test_normalise_flat():
    arr = np.ones(10) * 0.5
    n = _normalise(arr)
    assert np.all(n == 0.0)


def test_compute_rms_nonzero():
    audio = np.random.randn(SAMPLE_RATE * 5).astype(np.float32)
    rms = _compute_rms(audio)
    assert len(rms) > 0
    assert rms.mean() > 0


# ------------------------------------------------------------------
# Integration tests with dynamic video
# ------------------------------------------------------------------

def test_analyse_returns_candidates(dynamic_video):
    candidates = analyse_audio(dynamic_video)
    assert len(candidates) >= 2
    assert len(candidates) <= 3


def test_analyse_ranks_are_ordered(dynamic_video):
    candidates = analyse_audio(dynamic_video)
    ranks = [c["rank"] for c in candidates]
    assert ranks == list(range(1, len(candidates) + 1))


def test_analyse_clip_lengths_in_range(dynamic_video):
    candidates = analyse_audio(dynamic_video)
    for c in candidates:
        assert 15 <= c["duration_seconds"] <= 40, (
            f"Clip duration {c['duration_seconds']}s out of [15,40] range"
        )


def test_analyse_times_within_track(dynamic_video):
    candidates = analyse_audio(dynamic_video)
    for c in candidates:
        assert 0 <= c["start_seconds"] <= DURATION
        assert 0 < c["end_seconds"] <= DURATION
        assert c["start_seconds"] < c["end_seconds"]


def test_analyse_no_excessive_overlap(dynamic_video):
    """Candidates must not overlap by more than 5 seconds."""
    from pipeline.audio_analysis import MIN_OVERLAP_SECONDS
    candidates = analyse_audio(dynamic_video)
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a = candidates[i]
            b = candidates[j]
            overlap = min(a["end_seconds"], b["end_seconds"]) - max(a["start_seconds"], b["start_seconds"])
            assert overlap <= MIN_OVERLAP_SECONDS + 1.0, (
                f"Candidates {i+1} and {j+1} overlap by {overlap:.1f}s"
            )


def test_analyse_top_candidate_brackets_burst(dynamic_video):
    """The top candidate should overlap with the 30–35s burst region."""
    candidates = analyse_audio(dynamic_video)
    top = candidates[0]
    burst_start, burst_end = 27.0, 40.0  # expanded window
    overlap = min(top["end_seconds"], burst_end) - max(top["start_seconds"], burst_start)
    assert overlap > 0, (
        f"Top candidate [{top['start_seconds']:.1f}s–{top['end_seconds']:.1f}s] "
        f"does not overlap with burst region [27–40s]"
    )


def test_analyse_second_candidate_brackets_second_section(dynamic_video):
    """The second candidate should overlap with the 65–90s loud section."""
    candidates = analyse_audio(dynamic_video)
    if len(candidates) < 2:
        pytest.skip("Only 1 candidate returned")
    second = candidates[1]
    section_start, section_end = 60.0, 95.0
    overlap = min(second["end_seconds"], section_end) - max(second["start_seconds"], section_start)
    assert overlap > 0, (
        f"Second candidate [{second['start_seconds']:.1f}s–{second['end_seconds']:.1f}s] "
        f"does not overlap with loud section [60–95s]"
    )


def test_analyse_has_contrast_scores(dynamic_video):
    candidates = analyse_audio(dynamic_video)
    for c in candidates:
        assert 0.0 <= c["contrast_score"] <= 1.0


# ------------------------------------------------------------------
# Edge case: flat audio
# ------------------------------------------------------------------

def test_analyse_flat_audio_returns_candidates(flat_video):
    candidates = analyse_audio(flat_video)
    assert len(candidates) >= 1


def test_analyse_flat_audio_clip_length_valid(flat_video):
    candidates = analyse_audio(flat_video)
    for c in candidates:
        assert c["duration_seconds"] > 0
        assert c["start_seconds"] < c["end_seconds"]
