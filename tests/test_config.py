from __future__ import annotations

from pathlib import Path

import pytest

from telegram_transcriber_bot.config import load_settings


def test_load_settings_reads_env_file_and_defaults(tmp_path: Path, monkeypatch) -> None:
    for key in (
        "TELEGRAM_BOT_TOKEN",
        "DATA_DIR",
        "WHISPER_MODEL",
        "WHISPER_DEVICE",
        "WHISPER_COMPUTE_TYPE",
        "REPORT_PROMPT_SUFFIX",
        "MEDIA_GROUP_WINDOW_SECONDS",
        "YOUTUBE_TRANSCRIPT_LANGUAGES",
    ):
        monkeypatch.delenv(key, raising=False)

    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "TELEGRAM_BOT_TOKEN=token-from-env",
                "DATA_DIR=custom-data",
                "REPORT_PROMPT_SUFFIX=extra instructions",
                "MEDIA_GROUP_WINDOW_SECONDS=3.5",
                "YOUTUBE_TRANSCRIPT_LANGUAGES=de,en",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(tmp_path)

    assert settings.telegram_bot_token == "token-from-env"
    assert settings.data_dir == Path("custom-data")
    assert settings.allowed_user_ids == ()
    assert settings.report_prompt_suffix == "extra instructions"
    assert settings.media_group_window_seconds == 3.5
    assert settings.youtube_languages == ("de", "en")
    assert settings.whisper_model == "turbo"


def test_load_settings_requires_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    (tmp_path / ".env").write_text("", encoding="utf-8")

    with pytest.raises(RuntimeError, match="TELEGRAM_BOT_TOKEN"):
        load_settings(tmp_path)


def test_load_settings_parses_allowed_user_ids(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    (tmp_path / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=token\nALLOWED_USER_IDS=1, 2,3\n",
        encoding="utf-8",
    )

    settings = load_settings(tmp_path)

    assert settings.allowed_user_ids == (1, 2, 3)
