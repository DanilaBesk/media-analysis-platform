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
    telegram_bot_api_base_url: str | None = None
    telegram_bot_api_local_mode: bool = False
    telegram_bot_api_required: bool = False


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
    bot_api_base_url = values.get("TELEGRAM_BOT_API_BASE_URL", "").strip().rstrip("/") or None
    bot_api_local_mode_raw = values.get("TELEGRAM_BOT_API_IS_LOCAL")
    if bot_api_local_mode_raw is None:
        bot_api_local_mode_raw = values.get("TELEGRAM_BOT_API_LOCAL_MODE")
    bot_api_local_mode = _parse_bool(
        bot_api_local_mode_raw,
        default=bot_api_base_url is not None,
        name="TELEGRAM_BOT_API_IS_LOCAL",
    )
    bot_api_required = _parse_bool(
        values.get("TELEGRAM_BOT_API_REQUIRED"),
        default=False,
        name="TELEGRAM_BOT_API_REQUIRED",
    )
    if bot_api_required and bot_api_base_url is None:
        raise RuntimeError("TELEGRAM_BOT_API_BASE_URL is required when TELEGRAM_BOT_API_REQUIRED=true")
    if bot_api_required and not bot_api_local_mode:
        raise RuntimeError("TELEGRAM_BOT_API_IS_LOCAL must be true when TELEGRAM_BOT_API_REQUIRED=true")
    return TelegramAdapterSettings(
        telegram_bot_token=token,
        allowed_user_ids=allowed_user_ids,
        telegram_bot_api_base_url=bot_api_base_url,
        telegram_bot_api_local_mode=bot_api_local_mode,
        telegram_bot_api_required=bot_api_required,
    )


def _parse_bool(value: str | None, *, default: bool, name: str) -> bool:
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean value")
