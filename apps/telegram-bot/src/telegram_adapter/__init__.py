# FILE: apps/telegram-bot/src/telegram_adapter/__init__.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Expose the packet-local Telegram inbox adapter HTTP client, gateway, and bot runtime.
# SCOPE: Re-export thin final API client and gateway surfaces used by the Telegram adapter packet.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: SCRIPT
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the packet-local Telegram adapter package exports.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   export-api-client - Re-export the thin HTTP API client surface for Telegram flows.
#   export-inbox-gateway - Re-export the gateway that maps Telegram updates to inbox, selection, and run calls.
# END_MODULE_MAP

from telegram_adapter.api_client import (
    TelegramApiClient,
    TelegramApiClientError,
)
from telegram_adapter.bot import TelegramInboxApp
from telegram_adapter.errors import TelegramUserError, TelegramUserErrorCode
from telegram_adapter.gateway import InboxStatus, IngressRecord, TelegramFileInput, TelegramInboxGateway
from telegram_adapter.i18n import (
    DEFAULT_LOCALE,
    SUPPORTED_LOCALES,
    TelegramCommandKey,
    TelegramLocaleService,
    TelegramTextKey,
    build_localized_commands,
)

__all__ = [
    "DEFAULT_LOCALE",
    "SUPPORTED_LOCALES",
    "TelegramApiClient",
    "TelegramApiClientError",
    "TelegramCommandKey",
    "TelegramFileInput",
    "TelegramInboxApp",
    "TelegramInboxGateway",
    "TelegramLocaleService",
    "TelegramTextKey",
    "TelegramUserError",
    "TelegramUserErrorCode",
    "InboxStatus",
    "IngressRecord",
    "build_localized_commands",
]
