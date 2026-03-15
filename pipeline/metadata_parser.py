"""
Metadata parser for @thechoirsource pipeline.
Extracts piece title, composer, and ensemble from YouTube video titles.
Uses Claude API in production, rule-based parsing in mock mode.
"""

import json
import logging
import re

logger = logging.getLogger(__name__)

# Common separators used in choir video titles
SEPARATORS = [" | ", " — ", " - ", ": ", " – "]


def _rule_based_parse(title: str, channel_name: str = "") -> dict:
    """
    Rule-based parser for common choir video title formats.
    Handles patterns like:
      "Allegri - Miserere mei, Deus | Choir of King's College, Cambridge"
      "The Sixteen: Palestrina — Sicut Cervus"
      "Evensong from Westminster Abbey, 10 March 2026"
      "VOCES8: 'O Magnum Mysterium' (Lauridsen)"
      "Stanford Magnificat in G — Choir of St John's College, Cambridge"
      "J.S. Bach: Jesu, meine Freude BWV 227 | Netherlands Bach Society"
      "Choral Evensong live from York Minster"
      "Ola Gjeilo - Northern Lights (Aurora Borealis) | Tenebrae"
    """
    piece_title = ""
    composer = ""
    ensemble_name = ""

    # First, handle the common "X | Ensemble" pattern — extract ensemble first
    if " | " in title:
        main, ensemble_raw = title.rsplit(" | ", 1)
        ensemble_name = ensemble_raw.strip()
        # Now parse main for composer and piece
        for inner_sep in [" — ", " – ", " - ", ": "]:
            if inner_sep in main:
                left, right = main.split(inner_sep, 1)
                if _looks_like_composer(left):
                    composer = left.strip()
                    piece_title = _clean_piece(right.strip())
                else:
                    piece_title = _clean_piece(left.strip())
                break
        else:
            piece_title = _clean_piece(main.strip())
    else:
        # No pipe — try other separators
        for sep in SEPARATORS:
            if sep not in title:
                continue
            parts = [p.strip() for p in title.split(sep)]
            if len(parts) < 2:
                continue
            first = parts[0]
            last = parts[-1]

            # Pattern: "Ensemble: Composer — Piece"
            if sep == ": ":
                potential_ensemble = first
                rest = sep.join(parts[1:])
                if _looks_like_composer(potential_ensemble) and not ensemble_name:
                    # First part is composer, not ensemble
                    composer = potential_ensemble
                    for inner_sep in [" — ", " – ", " - "]:
                        if inner_sep in rest:
                            p, _ = rest.split(inner_sep, 1)
                            piece_title = _clean_piece(p.strip())
                            break
                    else:
                        piece_title = _clean_piece(rest.strip())
                else:
                    ensemble_name = potential_ensemble
                    for inner_sep in [" — ", " – ", " - "]:
                        if inner_sep in rest:
                            c, p = rest.split(inner_sep, 1)
                            if _looks_like_composer(c):
                                composer = c.strip()
                                piece_title = _clean_piece(p.strip())
                            else:
                                piece_title = _clean_piece(c.strip())
                            break
                    else:
                        piece_title = _clean_piece(rest.strip())
                break

            # Default: "A - B" where A might be composer or piece
            elif _looks_like_composer(first) and not ensemble_name:
                composer = first
                piece_title = _clean_piece(parts[1] if len(parts) > 1 else last)
                break
            else:
                piece_title = _clean_piece(first)
                ensemble_name = last
                break

    # Extract composer from parentheses if not yet found: "Piece (Composer)"
    if not composer and piece_title:
        paren_match = re.search(r"\(([^)]+)\)$", piece_title)
        if paren_match:
            potential = paren_match.group(1)
            if _looks_like_composer(potential):
                composer = potential
                piece_title = piece_title[:paren_match.start()].strip()

    # Clean up BWV/K numbers and trailing parenthetical info from piece title
    piece_title = re.sub(r"\s+BWV\s+\d+", "", piece_title).strip()
    piece_title = _clean_piece(piece_title)

    # Remove quotes from piece title
    piece_title = piece_title.strip("'\"''\u201c\u201d")

    # Fallback for ensemble
    if not ensemble_name and channel_name:
        ensemble_name = channel_name

    # Fallback: if nothing parsed, use full title as piece
    if not piece_title:
        piece_title = title

    return {
        "piece_title": piece_title.strip(),
        "composer": composer.strip(),
        "ensemble_name": ensemble_name.strip(),
    }


def _looks_like_composer(text: str) -> bool:
    """Heuristic: short text that looks like a person's name."""
    text = text.strip()
    # Short enough to be a name (< 40 chars)
    if len(text) > 40:
        return False
    # Contains at least one word
    words = text.split()
    if not words:
        return False
    # All words start with capital or are abbreviations like "J.S."
    for word in words:
        clean = word.rstrip(".,")
        if not clean or not (clean[0].isupper() or re.match(r"^[A-Z]\.", clean)):
            return False
    # Doesn't look like an ensemble name (choir/cathedral/abbey etc.)
    ensemble_keywords = {"choir", "choral", "cathedral", "abbey", "college", "minster",
                         "ensemble", "consort", "society", "singers", "voices", "chapel"}
    if any(kw in text.lower() for kw in ensemble_keywords):
        return False
    return True


def _clean_piece(text: str) -> str:
    """Remove trailing date patterns and other noise from piece titles."""
    # Remove trailing date like ", 10 March 2026" or "live from ..."
    text = re.sub(r",?\s*\d{1,2}\s+\w+\s+\d{4}$", "", text)
    text = re.sub(r"\s*live from.*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def parse_metadata(title: str, description: str, channel_name: str, config) -> dict:
    """
    Extracts structured metadata from YouTube video title and description.

    In mock mode: uses rule-based parser.
    In production: calls Claude API.

    Returns: {piece_title: str, composer: str, ensemble_name: str}
    """
    if config.is_mock_mode():
        result = _rule_based_parse(title, channel_name)
        logger.debug("Mock metadata parse: %s", result)
        return result

    return _parse_with_claude(title, description, channel_name, config)


def _parse_with_claude(title: str, description: str, channel_name: str, config) -> dict:
    """Call Claude API to parse metadata from title/description."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=config.anthropic_api_key)

        system_msg = (
            "You extract structured metadata from choir music YouTube video titles and descriptions. "
            "Respond with ONLY a JSON object, no markdown, no preamble:\n"
            '{"piece_title": "...", "composer": "...", "ensemble_name": "..."}\n\n'
            "If a field cannot be determined, use empty string \"\".\n"
            "composer should be just the composer's name (e.g. 'Gregorio Allegri', not 'by Allegri').\n"
            "piece_title should be the musical work's name without composer prefix.\n"
            "ensemble_name should be the performing group's name."
        )

        user_msg = (
            f"Extract metadata from this video:\n"
            f"Title: {title}\n"
            f"Channel: {channel_name}\n"
            f"Description (first 500 chars): {description[:500]}"
        )

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=256,
            messages=[{"role": "user", "content": user_msg}],
            system=system_msg,
        )

        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```json?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        data = json.loads(raw)
        return {
            "piece_title": str(data.get("piece_title", title)),
            "composer": str(data.get("composer", "")),
            "ensemble_name": str(data.get("ensemble_name", channel_name)),
        }

    except (json.JSONDecodeError, KeyError, IndexError) as exc:
        logger.warning("Failed to parse Claude metadata response: %s", exc)
        return {
            "piece_title": title,
            "composer": "",
            "ensemble_name": channel_name,
        }
    except Exception as exc:
        logger.error("Claude API call failed for metadata: %s", exc)
        return {
            "piece_title": title,
            "composer": "",
            "ensemble_name": channel_name,
        }
