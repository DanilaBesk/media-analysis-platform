# FILE: apps/telegram-bot/tests/test_i18n_catalogs.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove the packet-local Telegram i18n catalogs stay structurally aligned across supported locales.
# SCOPE: Supported-locale validation, keyset parity, and placeholder-shape validation for translations.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import pytest

from telegram_adapter.i18n import DEFAULT_LOCALE, SUPPORTED_LOCALES, TelegramTextKey
from telegram_adapter.i18n.catalogs import TRANSLATION_CATALOGS
from telegram_adapter.i18n.validation import CatalogValidationError, validate_translation_catalogs


def test_translation_catalogs_cover_every_supported_locale_and_key() -> None:
    assert set(TRANSLATION_CATALOGS) == set(SUPPORTED_LOCALES)

    for locale in SUPPORTED_LOCALES:
        assert set(TRANSLATION_CATALOGS[locale]) == set(TelegramTextKey)

    validate_translation_catalogs(TRANSLATION_CATALOGS, default_locale=DEFAULT_LOCALE)


def test_validate_translation_catalogs_rejects_placeholder_mismatch() -> None:
    broken_catalogs = {
        "ru": dict(TRANSLATION_CATALOGS["ru"]),
        "en": dict(TRANSLATION_CATALOGS["en"]),
    }
    broken_catalogs["en"][TelegramTextKey.BASKET_SUMMARY_ADDED] = "Added: {items}"

    with pytest.raises(CatalogValidationError, match="BASKET_SUMMARY_ADDED"):
        validate_translation_catalogs(broken_catalogs, default_locale=DEFAULT_LOCALE)
