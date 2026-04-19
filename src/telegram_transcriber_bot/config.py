from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Settings:
    telegram_bot_token: str
    data_dir: Path
    allowed_user_ids: tuple[int, ...]
    whisper_model: str
    whisper_device: str
    whisper_compute_type: str
    report_prompt_suffix: str
    media_group_window_seconds: float
    youtube_languages: tuple[str, ...]


def load_settings(base_dir: Path | None = None) -> Settings:
    root_dir = Path(base_dir or Path.cwd())
    load_dotenv(root_dir / ".env")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    data_dir = Path(os.getenv("DATA_DIR", root_dir / ".data"))
    allowed_user_ids = tuple(
        int(part.strip()) for part in os.getenv("ALLOWED_USER_IDS", "").split(",") if part.strip()
    )
    languages = tuple(
        part.strip() for part in os.getenv("YOUTUBE_TRANSCRIPT_LANGUAGES", "ru,en").split(",") if part.strip()
    )
    return Settings(
        telegram_bot_token=token,
        data_dir=data_dir,
        allowed_user_ids=allowed_user_ids,
        whisper_model=os.getenv("WHISPER_MODEL", "turbo"),
        whisper_device=os.getenv("WHISPER_DEVICE", "auto"),
        whisper_compute_type=os.getenv("WHISPER_COMPUTE_TYPE", "default"),
        report_prompt_suffix=os.getenv("REPORT_PROMPT_SUFFIX", "").strip(),
        media_group_window_seconds=float(os.getenv("MEDIA_GROUP_WINDOW_SECONDS", "2.5")),
        youtube_languages=languages or ("ru", "en"),
    )
