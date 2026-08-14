# FILE: apps/telegram-bot/src/telegram_adapter/i18n/__init__.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Expose the packet-local Telegram i18n foundation for later runtime migration.
# SCOPE: Re-export typed keys, locale constants, catalogs, validation, locale service, and aiogram command helpers.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: BARREL
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_MODULE_MAP
#   export-catalogs - Re-export translation catalogs and locale constants.
#   export-keys - Re-export typed Telegram text and command keys.
#   export-service - Re-export locale resolution and validation helpers.
#   export-commands - Re-export localized command metadata helpers.
# END_MODULE_MAP

from telegram_adapter.i18n.catalogs import TRANSLATION_CATALOGS
from telegram_adapter.i18n.commands import COMMAND_SPECS, LocalizedCommandSpec, build_localized_commands
from telegram_adapter.i18n.keys import (
    DEFAULT_LOCALE,
    SUPPORTED_LOCALES,
    SupportedLocale,
    TelegramCommandKey,
    TelegramTextKey,
)
from telegram_adapter.i18n.service import TelegramLocaleService
from telegram_adapter.i18n.validation import CatalogValidationError, validate_translation_catalogs

__all__ = [
    "COMMAND_SPECS",
    "DEFAULT_LOCALE",
    "SUPPORTED_LOCALES",
    "CatalogValidationError",
    "LocalizedCommandSpec",
    "SupportedLocale",
    "TRANSLATION_CATALOGS",
    "TelegramCommandKey",
    "TelegramLocaleService",
    "TelegramTextKey",
    "build_localized_commands",
    "validate_translation_catalogs",
]
