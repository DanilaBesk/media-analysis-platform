# FILE: apps/telegram-bot/src/telegram_adapter/__init__.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Expose the packet-local Telegram adapter HTTP client and polling gateway without introducing a shared SDK extraction.
# SCOPE: Re-export the thin API client and API-backed processing gateway used by the Telegram adapter packet.
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
#   export-processing-gateway - Re-export the polling gateway that turns API jobs into Telegram-sendable artifacts.
# END_MODULE_MAP

from telegram_adapter.api_client import (
    TelegramApiClient,
    TelegramApiClientError,
    UploadFilePart,
)
from telegram_adapter.gateway import TelegramApiProcessingGateway

__all__ = [
    "TelegramApiClient",
    "TelegramApiClientError",
    "TelegramApiProcessingGateway",
    "UploadFilePart",
]
