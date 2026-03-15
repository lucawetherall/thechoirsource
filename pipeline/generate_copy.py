"""
Post copy generation for @thechoirsource pipeline.
Generates Instagram/TikTok captions with hashtags using the Claude API.
"""

import json
import logging
import re

logger = logging.getLogger(__name__)

FIXED_HASHTAGS = [
    "#choir", "#choralmusic", "#classicalmusic", "#sacredmusic",
    "#choirtok", "#choirsource",
]

SYSTEM_PROMPT = """\
You generate short social media captions for @thechoirsource, an account that shares beautiful professional choir performances. Your captions are warm, reverent, and accessible — never stuffy or academic. You make people want to stop scrolling and listen.

Rules:
- 2–4 sentences maximum
- Credit the performing ensemble by name
- Never mention YouTube or link to the source
- Maximum 1 emoji, or none — the music speaks for itself
- End with a subtle call to action: something like 'Turn up the volume.', 'Close your eyes and listen.', 'Let this wash over you.', 'Headphones on.', or similar. Vary these — never repeat the same CTA twice in a row.
- Do not use the word 'ethereal' or 'transcendent'. These are overused.
- Respond with ONLY a JSON object, no markdown, no preamble:
  {"caption": "...", "hashtags": ["...", ...]}"""


def _build_user_prompt(metadata: dict) -> str:
    piece = metadata.get("piece_title", "")
    composer = metadata.get("composer", "")
    ensemble = metadata.get("ensemble_name", "")
    title = metadata.get("title", piece)

    return (
        f"Write a caption for this video:\n"
        f"Piece: {piece}\n"
        f"Composer: {composer}\n"
        f"Ensemble: {ensemble}\n"
        f"Original title: {title}\n\n"
        f"Also generate 8–12 relevant hashtags. Include these fixed tags:\n"
        f"{' '.join(FIXED_HASHTAGS)}\n"
        f"Add 4–6 dynamic tags based on the specific piece, composer, ensemble, "
        f"tradition, or genre."
    )


def _mock_generate(metadata: dict) -> dict:
    """Template-based caption generation for mock mode."""
    from pipeline.mock import MockAnthropicAPI
    mock = MockAnthropicAPI()
    return mock.generate_copy(metadata)


def generate_post_copy(metadata: dict, config) -> dict:
    """
    Generates an Instagram/TikTok caption with hashtags.

    Returns: {caption, hashtags, hashtags_string, full_post_text}
    """
    if config.is_mock_mode():
        return _mock_generate(metadata)

    return _generate_with_claude(metadata, config)


def _generate_with_claude(metadata: dict, config) -> dict:
    """Call Claude API to generate caption and hashtags."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=config.anthropic_api_key)

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=512,
            messages=[{"role": "user", "content": _build_user_prompt(metadata)}],
            system=SYSTEM_PROMPT,
        )

        raw = response.content[0].text.strip()
        # Strip markdown fences if present
        raw = re.sub(r"^```json?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        data = json.loads(raw)
        caption = str(data.get("caption", ""))
        hashtags = [str(h) for h in data.get("hashtags", FIXED_HASHTAGS)]

        # Ensure fixed hashtags are always included
        for tag in FIXED_HASHTAGS:
            if tag not in hashtags:
                hashtags.append(tag)

        hashtags_string = " ".join(hashtags)
        full_post_text = caption + "\n\n" + hashtags_string

        return {
            "caption": caption,
            "hashtags": hashtags,
            "hashtags_string": hashtags_string,
            "full_post_text": full_post_text,
        }

    except (json.JSONDecodeError, KeyError, IndexError) as exc:
        logger.warning("Failed to parse Claude copy response: %s", exc)
        return _fallback_copy(metadata)
    except Exception as exc:
        logger.error("Claude API call failed for copy generation: %s", exc)
        return _fallback_copy(metadata)


def _fallback_copy(metadata: dict) -> dict:
    """Fallback if Claude API call fails."""
    ensemble = metadata.get("ensemble_name", "this ensemble")
    piece = metadata.get("piece_title", "this piece")
    caption = (
        f"A beautiful performance of {piece} by {ensemble}. "
        f"Let this wash over you."
    )
    hashtags = FIXED_HASHTAGS + ["#choral", "#singing"]
    hashtags_string = " ".join(hashtags)
    return {
        "caption": caption,
        "hashtags": hashtags,
        "hashtags_string": hashtags_string,
        "full_post_text": caption + "\n\n" + hashtags_string,
    }
