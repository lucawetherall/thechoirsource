"""
Caption overlay for @thechoirsource pipeline.
Overlays branded text on video clips using FFmpeg drawtext filter.
"""

import logging
import os
import shutil
import subprocess
import tempfile

logger = logging.getLogger(__name__)

MAX_LINE_CHARS = 38
FALLBACK_FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


def _truncate(text: str, max_chars: int = MAX_LINE_CHARS) -> str:
    """Truncate text to max_chars, appending '…' if needed."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 1] + "…"


def _resolve_font(font_file: str) -> str:
    """
    Return the font file path to use. Falls back to system font if brand font is missing.
    """
    if os.path.exists(font_file):
        return font_file

    logger.warning(
        "Brand font not found at %s, using DejaVu Sans Bold as fallback", font_file
    )
    if os.path.exists(FALLBACK_FONT):
        return FALLBACK_FONT

    # Try to find any DejaVu bold font
    for path in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",  # macOS
    ]:
        if os.path.exists(path):
            return path

    # Last resort: return None and let FFmpeg use its default
    logger.warning("No fallback font found — FFmpeg will attempt to use its default font")
    return None


def _escape_ffmpeg_text(text: str) -> str:
    """Escape special characters for FFmpeg drawtext."""
    # Escape single quotes, colons, and backslashes
    text = text.replace("\\", "\\\\")
    text = text.replace("'", "'\\''")
    text = text.replace(":", "\\:")
    return text


def _build_drawtext(text: str, font_file: str, font_size: int, colour: str,
                    x_expr: str, y_expr: str, shadow_x: int, shadow_y: int,
                    box: bool = True, box_colour: str = "black@0.6",
                    box_padding: int = 24) -> str:
    """Build a single FFmpeg drawtext filter string."""
    escaped = _escape_ffmpeg_text(text)
    font_part = f":fontfile='{font_file}'" if font_file else ""
    box_part = (
        f":box=1:boxcolor={box_colour}:boxborderw={box_padding}"
        if box else ""
    )
    return (
        f"drawtext=text='{escaped}'"
        f"{font_part}"
        f":fontsize={font_size}"
        f":fontcolor={colour.lstrip('#')}"
        f":x={x_expr}"
        f":y={y_expr}"
        f":shadowx={shadow_x}"
        f":shadowy={shadow_y}"
        f":shadowcolor=black"
        f"{box_part}"
    )


def overlay_captions(clip_path: str, metadata: dict, config) -> str:
    """
    Overlays branded text on the clip video.

    Reads styling from config (brand.yml).
    On-screen text (bottom of frame): piece_title, composer, ensemble_name.
    Watermark (top right): "@thechoirsource"

    Returns: the clip path (same as input — overwrites the file).
    """
    brand = config.brand
    font_file = _resolve_font(brand.get("font_file", "assets/fonts/Montserrat-SemiBold.ttf"))
    font_size = int(brand.get("font_size", 42))
    font_colour = brand.get("font_colour", "#FFFFFF")
    shadow_x = int(brand.get("shadow_x", 2))
    shadow_y = int(brand.get("shadow_y", 2))
    margin_bottom = int(brand.get("margin_bottom", 200))
    line_spacing = int(brand.get("line_spacing", 14))
    box_padding = int(brand.get("background_padding", 24))
    watermark_text = brand.get("watermark_text", "@thechoirsource")
    watermark_size = int(brand.get("watermark_font_size", 28))
    watermark_margin = int(brand.get("watermark_margin", 30))
    wm_opacity = float(brand.get("watermark_opacity", 0.85))
    wm_colour = f"white@{wm_opacity}"

    # Build lines from metadata
    lines = []
    piece = _truncate(metadata.get("piece_title", ""))
    composer = _truncate(metadata.get("composer", ""))
    ensemble = _truncate(metadata.get("ensemble_name", ""))

    if piece:
        lines.append(piece)
    if composer:
        lines.append(composer)
    if ensemble:
        lines.append(ensemble)

    # Calculate y positions from bottom
    # Total height of text block
    num_lines = len(lines)
    line_height = font_size + line_spacing
    block_height = num_lines * line_height

    # Position: start from margin_bottom, lines stack upward
    # y for first line (top of block) = h - margin_bottom - block_height
    base_y = f"h-{margin_bottom + block_height}"

    filters = []

    # Main caption lines
    for i, line_text in enumerate(lines):
        y_expr = f"h-{margin_bottom + block_height - i * line_height}"
        x_expr = "(w-tw)/2"  # horizontally centred
        dt = _build_drawtext(
            text=line_text,
            font_file=font_file,
            font_size=font_size,
            colour=font_colour,
            x_expr=x_expr,
            y_expr=y_expr,
            shadow_x=shadow_x,
            shadow_y=shadow_y,
            box=True,
            box_colour=f"black@0.6",
            box_padding=box_padding,
        )
        filters.append(dt)

    # Watermark (top right)
    wm_x = f"w-tw-{watermark_margin}"
    wm_y = str(watermark_margin)
    wm_dt = _build_drawtext(
        text=watermark_text,
        font_file=font_file,
        font_size=watermark_size,
        colour=wm_colour,
        x_expr=wm_x,
        y_expr=wm_y,
        shadow_x=shadow_x,
        shadow_y=shadow_y,
        box=False,
    )
    filters.append(wm_dt)

    vf = ",".join(filters)

    # Write to temp file then replace
    tmp_out = clip_path + ".captioned.mp4"
    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", clip_path,
            "-vf", vf,
            "-c:v", "libx264",
            "-crf", "23",
            "-preset", "fast",
            "-c:a", "aac",
            "-b:a", "128k",
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            tmp_out,
        ]

        logger.info("Overlaying captions on %s", clip_path)
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"FFmpeg caption overlay failed:\n{result.stderr[-2000:]}"
            )

        # Atomic replace
        shutil.move(tmp_out, clip_path)
        logger.info("Caption overlay complete: %s", clip_path)
        return clip_path

    except Exception:
        if os.path.exists(tmp_out):
            os.unlink(tmp_out)
        raise
