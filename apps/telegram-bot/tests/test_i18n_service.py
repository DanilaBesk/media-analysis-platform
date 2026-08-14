# FILE: apps/telegram-bot/tests/test_i18n_service.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove the packet-local Telegram i18n foundation resolves supported locales, formats catalog text, and shapes localized command metadata.
# SCOPE: Locale normalization and fallback resolution, localized text formatting, and aiogram command helper output.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_MODULE_MAP
#   test_normalize_locale_supports_telegram_variants_and_falls_back_to_default - Verify locale normalization and fallback.
#   test_resolve_locale_prefers_explicit_user_locale_before_fallbacks - Verify locale resolution precedence.
#   test_text_formats_catalog_values_with_typed_keys - Verify typed translation formatting.
#   test_text_raises_for_missing_format_arguments - Verify missing format argument failures.
#   test_build_localized_commands_returns_stable_registry_with_localized_descriptions - Verify localized command metadata.
#   test_catalog_returns_locale_specific_mapping - Verify locale-specific catalog lookup.
# END_MODULE_MAP

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
        "Send text, links, photos, videos, or documents. Everything accepted lands in inbox first."
    )
    assert service.text(TelegramTextKey.INBOX_SUMMARY_ADDED, locale="ru", count=3) == "Принято: 3"


def test_text_raises_for_missing_format_arguments() -> None:
    service = TelegramLocaleService()

    with pytest.raises(ValueError, match="count"):
        service.text(TelegramTextKey.INBOX_SUMMARY_ADDED, locale="en")


def test_build_localized_commands_returns_stable_registry_with_localized_descriptions() -> None:
    commands = build_localized_commands("en")

    assert [command.command for command in commands] == [key.value for key in TelegramCommandKey]
    assert [command.description for command in commands] == [
        "Open inbox",
        "How to manage inbox",
        "Show current inbox",
    ]


def test_catalog_returns_locale_specific_mapping() -> None:
    service = TelegramLocaleService()

    catalog = service.catalog("en-US")

    assert catalog[TelegramTextKey.START_PROMPT].startswith("Send text, links")
