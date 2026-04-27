# FILE: apps/telegram-bot/tests/test_i18n_service.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove the packet-local Telegram i18n foundation resolves supported locales, formats catalog text, and shapes localized command metadata.
# SCOPE: Locale normalization and fallback resolution, localized text formatting, and aiogram command helper output.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import pytest

from telegram_adapter.i18n import (
    DEFAULT_LOCALE,
    TelegramCommandKey,
    TelegramLocaleService,
    TelegramTextKey,
    build_localized_commands,
)


def test_normalize_locale_supports_telegram_variants_and_falls_back_to_default() -> None:
    service = TelegramLocaleService()

    assert service.normalize_locale("ru") == "ru"
    assert service.normalize_locale("ru_RU") == "ru"
    assert service.normalize_locale("EN-us") == "en"
    assert service.normalize_locale("de-DE") == DEFAULT_LOCALE
    assert service.normalize_locale(None) == DEFAULT_LOCALE
    assert service.normalize_locale("   ") == DEFAULT_LOCALE


def test_resolve_locale_prefers_explicit_user_locale_before_fallbacks() -> None:
    service = TelegramLocaleService(default_locale="en")

    assert service.resolve_locale(user_locale="ru-RU", chat_locale="en-US") == "ru"
    assert service.resolve_locale(user_locale=None, chat_locale="en-US") == "en"
    assert service.resolve_locale(user_locale=None, chat_locale=None) == "en"


def test_text_formats_catalog_values_with_typed_keys() -> None:
    service = TelegramLocaleService()

    assert service.text(TelegramTextKey.START_PROMPT, locale="en") == (
        "Send a voice message, audio, video, document, or a link. "
        "In collection mode I gather several sources and run one shared transcript."
    )
    assert service.text(TelegramTextKey.BASKET_SUMMARY_ADDED, locale="ru", count=3) == "Добавлено: 3"


def test_text_raises_for_missing_format_arguments() -> None:
    service = TelegramLocaleService()

    with pytest.raises(ValueError, match="count"):
        service.text(TelegramTextKey.BASKET_SUMMARY_ADDED, locale="en")


def test_build_localized_commands_returns_stable_registry_with_localized_descriptions() -> None:
    commands = build_localized_commands("en")

    assert [command.command for command in commands] == [key.value for key in TelegramCommandKey]
    assert [command.description for command in commands] == [
        "Open the menu",
        "How to use the bot",
        "Turn collection mode on or off",
        "Show the current collection",
        "Clear the collection",
    ]
