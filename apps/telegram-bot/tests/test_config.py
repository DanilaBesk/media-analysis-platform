from __future__ import annotations

from pathlib import Path

import pytest

from telegram_adapter.config import load_settings


def test_load_settings_requires_local_bot_api_when_marked_required() -> None:
    with pytest.raises(RuntimeError, match="TELEGRAM_BOT_API_BASE_URL is required"):
        load_settings(
            Path("/tmp/runtime"),
            env={
                "TELEGRAM_BOT_TOKEN": "123:ABC",
                "TELEGRAM_BOT_API_REQUIRED": "true",
            },
        )


def test_load_settings_requires_local_mode_when_bot_api_is_required() -> None:
    with pytest.raises(RuntimeError, match="TELEGRAM_BOT_API_IS_LOCAL must be true"):
        load_settings(
            Path("/tmp/runtime"),
            env={
                "TELEGRAM_BOT_TOKEN": "123:ABC",
                "TELEGRAM_BOT_API_REQUIRED": "true",
                "TELEGRAM_BOT_API_BASE_URL": "http://telegram-bot-api:8081",
                "TELEGRAM_BOT_API_IS_LOCAL": "false",
            },
        )


def test_load_settings_accepts_required_local_bot_api() -> None:
    settings = load_settings(
        Path("/tmp/runtime"),
        env={
            "TELEGRAM_BOT_TOKEN": "123:ABC",
            "TELEGRAM_BOT_API_REQUIRED": "true",
            "TELEGRAM_BOT_API_BASE_URL": "http://telegram-bot-api:8081",
            "TELEGRAM_BOT_API_IS_LOCAL": "true",
        },
    )

    assert settings.telegram_bot_api_base_url == "http://telegram-bot-api:8081"
    assert settings.telegram_bot_api_local_mode is True
