# FILE: apps/telegram-bot/src/telegram_adapter/__main__.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the compose-ready executable launcher for the thin Telegram adapter package.
# SCOPE: Runtime env normalization, API client bootstrap, readiness logging, and long-running adapter process lifecycle.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP, M-INFRA-COMPOSE
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: SCRIPT
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the compose-ready Telegram adapter launcher with placeholder-token health mode.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   launch-telegram-adapter - Normalize runtime env, construct the API client boundary, and keep the adapter process healthy.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping

from telegram_adapter.api_client import TelegramApiClient


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_LAUNCH_TELEGRAM_ADAPTER = "[TelegramAdapter][main][BLOCK_LAUNCH_TELEGRAM_ADAPTER]"


def main(env: Mapping[str, str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    values = os.environ if env is None else env
    api_base_url = values.get("API_BASE_URL", "http://api:8080").strip() or "http://api:8080"
    token = values.get("TELEGRAM_BOT_TOKEN", "").strip()
    TelegramApiClient(api_base_url)
    mode = "token_configured" if token and token != "replace-me" else "token_placeholder"
    _LOGGER.info("%s api_base_url=%s mode=%s", _LOG_MARKER_LAUNCH_TELEGRAM_ADAPTER, api_base_url, mode)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    raise SystemExit(main())
