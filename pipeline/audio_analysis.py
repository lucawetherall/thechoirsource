"""
Audio analysis for @thechoirsource pipeline.
Finds the 2-3 most dynamically exciting clip windows in a video's audio track.
"""

import logging
import os
import subprocess
import tempfile

import librosa
import numpy as np

logger = logging.getLogger(__name__)

SAMPLE_RATE = 22050
FRAME_LENGTH = 2048
HOP_LENGTH = 512
FRAMES_PER_SEC = SAMPLE_RATE / HOP_LENGTH  # ~43 fps
SMOOTH_WINDOW = 22  # ~0.5 seconds
STEP_SECONDS = 2
CANDIDATE_WINDOW_LENGTHS = [20, 25, 30, 35]  # seconds
MIN_OVERLAP_SECONDS = 5
MAX_CLIPS = 3
FLAT_AUDIO_THRESHOLD_RATIO = 0.01  # std < 1% of mean → flat
LOCAL_MIN_LOOKBACK_SECONDS = 3
LOCAL_MIN_PERCENTILE = 30
FADE_PADDING_SECONDS = 0.5


def _extract_audio(video_path: str) -> str:
    """Extract mono WAV from video. Returns path to temp WAV file."""
    tmp_wav = tempfile.mktemp(suffix=".wav", prefix="choirsrc_")
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-ac", "1",
        "-ar", str(SAMPLE_RATE),
        "-vn",
        "-f", "wav",
        tmp_wav,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"FFmpeg audio extraction failed for {video_path}:\n{result.stderr[-2000:]}"
        )
    return tmp_wav


def _compute_rms(audio: np.ndarray) -> np.ndarray:
    """Compute smoothed RMS energy envelope."""
    rms = librosa.feature.rms(
        y=audio,
        frame_length=FRAME_LENGTH,
        hop_length=HOP_LENGTH,
    ).squeeze()

    # Smooth with moving average
    kernel = np.ones(SMOOTH_WINDOW) / SMOOTH_WINDOW
    rms_smooth = np.convolve(rms, kernel, mode="same")
    return rms_smooth


def _normalise(arr: np.ndarray) -> np.ndarray:
    """Min-max normalise to [0, 1]. If range is zero, return zeros."""
    mn, mx = arr.min(), arr.max()
    if mx == mn:
        return np.zeros_like(arr)
    return (arr - mn) / (mx - mn)


def _score_windows(rms: np.ndarray, total_duration: float) -> list:
    """
    Slide windows of varying lengths over the RMS envelope and score each.

    Returns list of (start_frame, end_frame, window_seconds, combined_score).
    """
    n_frames = len(rms)
    step_frames = int(STEP_SECONDS * FRAMES_PER_SEC)

    all_scores = []

    for win_secs in CANDIDATE_WINDOW_LENGTHS:
        win_frames = int(win_secs * FRAMES_PER_SEC)
        if win_frames >= n_frames:
            continue

        range_scores = []
        deriv_scores = []
        positions = []

        for start in range(0, n_frames - win_frames, step_frames):
            window = rms[start: start + win_frames]
            range_score = float(window.max() - window.min())
            deriv_score = float(np.sum(np.abs(np.diff(window))) / win_frames)
            range_scores.append(range_score)
            deriv_scores.append(deriv_score)
            positions.append((start, start + win_frames, win_secs))

        if not positions:
            continue

        range_arr = np.array(range_scores)
        deriv_arr = np.array(deriv_scores)
        norm_range = _normalise(range_arr)
        norm_deriv = _normalise(deriv_arr)
        combined = 0.4 * norm_range + 0.6 * norm_deriv

        for i, (start_f, end_f, ws) in enumerate(positions):
            all_scores.append((start_f, end_f, ws, float(combined[i])))

    return all_scores


def _snap_to_local_min(start_frame: int, rms: np.ndarray) -> int:
    """Search backwards up to LOCAL_MIN_LOOKBACK_SECONDS for a local RMS minimum."""
    lookback_frames = int(LOCAL_MIN_LOOKBACK_SECONDS * FRAMES_PER_SEC)
    search_start = max(0, start_frame - lookback_frames)

    threshold = np.percentile(rms, LOCAL_MIN_PERCENTILE)
    best_frame = start_frame

    for i in range(start_frame, search_start - 1, -1):
        if rms[i] < threshold:
            # Check for local minimum
            prev_ok = (i == 0) or (rms[i] < rms[i - 1])
            next_ok = (i == len(rms) - 1) or (rms[i] < rms[i + 1])
            if prev_ok and next_ok:
                best_frame = i
                break

    return best_frame


def analyse_audio(video_path: str, config=None) -> list:
    """
    Finds the 2-3 most dynamically exciting 15-40 second windows in the audio.

    Returns list of 2-3 dicts sorted by rank (1=best):
    {rank, start_seconds, end_seconds, duration_seconds, contrast_score}
    """
    wav_path = None
    try:
        wav_path = _extract_audio(video_path)
        audio, sr = librosa.load(wav_path, sr=SAMPLE_RATE, mono=True)
        total_duration = len(audio) / sr

        # Edge case: very short audio
        if total_duration < 40:
            logger.info("Short audio (%.1fs) — returning single candidate covering full track", total_duration)
            return [{
                "rank": 1,
                "start_seconds": 0.0,
                "end_seconds": round(total_duration, 2),
                "duration_seconds": round(total_duration, 2),
                "contrast_score": 1.0,
            }]

        rms = _compute_rms(audio)

        # Edge case: dynamically flat audio
        rms_mean = rms.mean()
        rms_std = rms.std()
        is_flat = rms_mean > 0 and rms_std < FLAT_AUDIO_THRESHOLD_RATIO * rms_mean

        if is_flat:
            logger.info("Flat audio detected — selecting by highest absolute mean RMS")
            candidates = _select_flat_audio_candidates(rms, total_duration)
        else:
            candidates = _select_dynamic_candidates(rms, total_duration)

        return candidates

    finally:
        if wav_path and os.path.exists(wav_path):
            os.unlink(wav_path)


def _select_dynamic_candidates(rms: np.ndarray, total_duration: float) -> list:
    """Select non-overlapping high-contrast windows."""
    all_scores = _score_windows(rms, total_duration)
    if not all_scores:
        return []

    # Sort by score descending, tiebreak by later position
    all_scores.sort(key=lambda x: (x[3], x[0]), reverse=True)

    selected = []
    for start_f, end_f, win_secs, score in all_scores:
        # Check overlap with already selected
        overlapping = False
        for sel_start, sel_end, *_ in selected:
            overlap = min(end_f, sel_end) - max(start_f, sel_start)
            overlap_seconds = overlap / FRAMES_PER_SEC
            if overlap_seconds > MIN_OVERLAP_SECONDS:
                overlapping = True
                break
        if overlapping:
            continue

        # Snap start to nearest local RMS minimum
        snapped_start = _snap_to_local_min(start_f, rms)
        # Apply fade-in padding
        pad_frames = int(FADE_PADDING_SECONDS * FRAMES_PER_SEC)
        snapped_start = max(0, snapped_start - pad_frames)

        selected.append((snapped_start, end_f, win_secs, score))

        if len(selected) == MAX_CLIPS:
            break

    # Tiebreaking: if top scores within 10%, prefer later tracks
    if len(selected) >= 2:
        top_score = selected[0][3]
        tied = [s for s in selected if s[3] >= top_score * 0.9]
        if len(tied) > 1:
            tied.sort(key=lambda x: x[0])  # sort by position (later = higher index)
            selected = tied + [s for s in selected if s not in tied]

    results = []
    for rank, (start_f, end_f, win_secs, score) in enumerate(selected, 1):
        start_secs = start_f / FRAMES_PER_SEC
        end_secs = min(end_f / FRAMES_PER_SEC, total_duration)
        results.append({
            "rank": rank,
            "start_seconds": round(start_secs, 2),
            "end_seconds": round(end_secs, 2),
            "duration_seconds": round(end_secs - start_secs, 2),
            "contrast_score": round(score, 4),
        })

    return results


def _select_flat_audio_candidates(rms: np.ndarray, total_duration: float) -> list:
    """For flat audio, select windows with highest absolute mean RMS."""
    n_frames = len(rms)
    win_frames = int(30 * FRAMES_PER_SEC)
    step_frames = int(STEP_SECONDS * FRAMES_PER_SEC)

    if win_frames >= n_frames:
        return [{
            "rank": 1,
            "start_seconds": 0.0,
            "end_seconds": round(total_duration, 2),
            "duration_seconds": round(total_duration, 2),
            "contrast_score": 0.5,
        }]

    windows = []
    for start in range(0, n_frames - win_frames, step_frames):
        mean_rms = float(rms[start: start + win_frames].mean())
        windows.append((start, start + win_frames, mean_rms))

    windows.sort(key=lambda x: x[2], reverse=True)

    selected = []
    for start_f, end_f, mean_rms in windows:
        overlapping = any(
            (min(end_f, s[1]) - max(start_f, s[0])) / FRAMES_PER_SEC > MIN_OVERLAP_SECONDS
            for s in selected
        )
        if overlapping:
            continue
        selected.append((start_f, end_f, mean_rms))
        if len(selected) == MAX_CLIPS:
            break

    max_rms = selected[0][2] if selected else 1.0
    results = []
    for rank, (start_f, end_f, mean_rms) in enumerate(selected, 1):
        start_secs = start_f / FRAMES_PER_SEC
        end_secs = min(end_f / FRAMES_PER_SEC, total_duration)
        results.append({
            "rank": rank,
            "start_seconds": round(start_secs, 2),
            "end_seconds": round(end_secs, 2),
            "duration_seconds": round(end_secs - start_secs, 2),
            "contrast_score": round(mean_rms / max_rms, 4) if max_rms > 0 else 0.5,
        })

    return results
