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
#   LAST_CHANGE: v2.0.0 - Start the always-on inbox adapter over final API state.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   launch-telegram-adapter - Normalize runtime env, construct the API client boundary, and keep the adapter process healthy.
# END_MODULE_MAP

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from collections.abc import Mapping

from telegram_adapter.api_client import TelegramApiClient
from telegram_adapter.bot import TelegramInboxApp
from telegram_adapter.config import load_settings
from telegram_adapter.gateway import TelegramInboxGateway


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_LAUNCH_TELEGRAM_ADAPTER = "[TelegramAdapter][main][BLOCK_LAUNCH_TELEGRAM_ADAPTER]"


async def _run(env: Mapping[str, str] | None = None) -> None:
    values = os.environ if env is None else env
    settings_base_dir = Path(values.get("SETTINGS_BASE_DIR", "/workspace"))
    settings = load_settings(settings_base_dir, env=None if env is None else values)
    api_base_url = values.get("API_BASE_URL", "").strip() or "http://api:8080"
    api_client = TelegramApiClient(api_base_url)
    gateway = TelegramInboxGateway(api_client)
    app = TelegramInboxApp(settings, gateway)
    _LOGGER.info("%s api_base_url=%s mode=token_configured", _LOG_MARKER_LAUNCH_TELEGRAM_ADAPTER, api_base_url)
    await app.run()


def main(env: Mapping[str, str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(_run(env))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
