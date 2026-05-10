from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class TelegramAdapterSettings:
    telegram_bot_token: str
    allowed_user_ids: tuple[int, ...]


def load_settings(base_dir: Path | None = None, *, env: Mapping[str, str] | None = None) -> TelegramAdapterSettings:
    root_dir = Path(base_dir or Path.cwd())
    values = os.environ if env is None else env
    if env is None:
        load_dotenv(root_dir / ".env")

    token = values.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    allowed_user_ids = tuple(
        int(part.strip()) for part in values.get("ALLOWED_USER_IDS", "").split(",") if part.strip()
    )
    return TelegramAdapterSettings(telegram_bot_token=token, allowed_user_ids=allowed_user_ids)
