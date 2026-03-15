"""
Configuration loader for @thechoirsource pipeline.
Loads YAML config files and environment variables.
"""

import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Determine project root (two levels up from this file)
PROJECT_ROOT = Path(__file__).parent.parent


class Config:
    """Central configuration object for the pipeline."""

    def __init__(self, config_dir: str = None, env_file: str = None):
        # Load .env file (if present) before reading env vars.
        # GitHub Actions injects secrets directly as env vars, so .env is optional.
        env_path = Path(env_file) if env_file else PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            load_dotenv()  # will silently no-op if no .env

        self._config_dir = Path(config_dir) if config_dir else PROJECT_ROOT / "config"
        self._queue_dir = PROJECT_ROOT / "queue"

        self._channels = self._load_yaml("channels.yml", default={"channels": []})
        self._search_terms = self._load_yaml("search_terms.yml", default={"search_terms": []})
        self._brand = self._load_yaml("brand.yml", default={})
        self._schedule = self._load_yaml("schedule.yml", default={})

    # ------------------------------------------------------------------
    # YAML helpers
    # ------------------------------------------------------------------

    def _load_yaml(self, filename: str, default: dict = None) -> dict:
        path = self._config_dir / filename
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return data if data is not None else (default or {})
        except FileNotFoundError:
            logger.warning("Config file not found: %s — using defaults", path)
            return default or {}
        except yaml.YAMLError as exc:
            logger.warning("YAML parse error in %s: %s — using defaults", path, exc)
            return default or {}

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def channels(self) -> list:
        return self._channels.get("channels", [])

    @property
    def search_terms(self) -> list:
        return self._search_terms.get("search_terms", [])

    @property
    def brand(self) -> dict:
        return self._brand

    @property
    def schedule(self) -> dict:
        return self._schedule

    @property
    def queue_dir(self) -> Path:
        return self._queue_dir

    @property
    def project_root(self) -> Path:
        return PROJECT_ROOT

    # ------------------------------------------------------------------
    # Environment variable accessors (with defaults / warnings)
    # ------------------------------------------------------------------

    def _env(self, key: str, default: str = None) -> str:
        value = os.environ.get(key, default)
        if value is None:
            logger.warning("Environment variable %s is not set", key)
        return value

    @property
    def youtube_api_key(self) -> str:
        return self._env("YOUTUBE_API_KEY")

    @property
    def anthropic_api_key(self) -> str:
        return self._env("ANTHROPIC_API_KEY")

    @property
    def r2_access_key_id(self) -> str:
        return self._env("R2_ACCESS_KEY_ID")

    @property
    def r2_secret_access_key(self) -> str:
        return self._env("R2_SECRET_ACCESS_KEY")

    @property
    def r2_account_id(self) -> str:
        return self._env("R2_ACCOUNT_ID")

    @property
    def r2_bucket_name(self) -> str:
        return self._env("R2_BUCKET_NAME", "thechoirsource-clips")

    @property
    def r2_public_url(self) -> str:
        return self._env("R2_PUBLIC_URL", "")

    @property
    def meta_access_token(self) -> str:
        return self._env("META_ACCESS_TOKEN")

    @property
    def meta_ig_user_id(self) -> str:
        return self._env("META_IG_USER_ID")

    @property
    def meta_page_id(self) -> str:
        return self._env("META_PAGE_ID")

    @property
    def tiktok_access_token(self) -> str:
        return self._env("TIKTOK_ACCESS_TOKEN")

    @property
    def dashboard_secret(self) -> str:
        return self._env("DASHBOARD_SECRET")

    @property
    def gh_pat_token(self) -> str:
        return self._env("GH_PAT_TOKEN")

    @property
    def gh_owner(self) -> str:
        return self._env("GH_OWNER")

    @property
    def gh_repo(self) -> str:
        return self._env("GH_REPO", "thechoirsource")

    # ------------------------------------------------------------------
    # Brand / schedule convenience accessors
    # ------------------------------------------------------------------

    @property
    def font_file(self) -> str:
        return self.brand.get("font_file", "assets/fonts/Montserrat-SemiBold.ttf")

    @property
    def posting_timezone(self) -> str:
        return self.schedule.get("timezone", "Europe/London")

    @property
    def posting_window_start(self) -> int:
        return self.schedule.get("posting_window_start_hour", 18)

    @property
    def posting_window_end(self) -> int:
        return self.schedule.get("posting_window_end_hour", 20)

    @property
    def platforms(self) -> list:
        return self.schedule.get("platforms", ["instagram_reels", "facebook_reels", "tiktok"])

    # ------------------------------------------------------------------
    # Mock mode
    # ------------------------------------------------------------------

    def is_mock_mode(self) -> bool:
        """Returns True if MOCK_MODE=true is set in the environment."""
        return os.environ.get("MOCK_MODE", "").lower() in ("true", "1", "yes")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> list:
        """Check all required env vars. Returns list of missing variable names."""
        required = [
            "YOUTUBE_API_KEY",
            "ANTHROPIC_API_KEY",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_ACCOUNT_ID",
            "R2_BUCKET_NAME",
            "R2_PUBLIC_URL",
            "META_ACCESS_TOKEN",
            "META_IG_USER_ID",
            "META_PAGE_ID",
            "TIKTOK_ACCESS_TOKEN",
            "DASHBOARD_SECRET",
            "GH_PAT_TOKEN",
            "GH_OWNER",
        ]
        missing = [k for k in required if not os.environ.get(k)]
        return missing
