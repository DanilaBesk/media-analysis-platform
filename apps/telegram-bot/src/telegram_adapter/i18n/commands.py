# FILE: apps/telegram-bot/src/telegram_adapter/i18n/commands.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Build localized aiogram command metadata from the packet-local Telegram i18n foundation.
# SCOPE: Stable command registry and helper for per-locale BotCommand construction without full bot runtime integration.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

from dataclasses import dataclass

from aiogram.types import BotCommand

from telegram_adapter.i18n.keys import TelegramCommandKey, TelegramTextKey
from telegram_adapter.i18n.service import TelegramLocaleService


@dataclass(frozen=True, slots=True)
class LocalizedCommandSpec:
    command_key: TelegramCommandKey
    description_key: TelegramTextKey


COMMAND_SPECS: tuple[LocalizedCommandSpec, ...] = (
    LocalizedCommandSpec(TelegramCommandKey.START, TelegramTextKey.COMMAND_START_DESCRIPTION),
    LocalizedCommandSpec(TelegramCommandKey.HELP, TelegramTextKey.COMMAND_HELP_DESCRIPTION),
    LocalizedCommandSpec(TelegramCommandKey.BATCH, TelegramTextKey.COMMAND_BATCH_DESCRIPTION),
    LocalizedCommandSpec(TelegramCommandKey.BASKET, TelegramTextKey.COMMAND_BASKET_DESCRIPTION),
    LocalizedCommandSpec(TelegramCommandKey.CLEAR, TelegramTextKey.COMMAND_CLEAR_DESCRIPTION),
)


def build_localized_commands(
    locale: str | None,
    *,
    locale_service: TelegramLocaleService | None = None,
) -> tuple[BotCommand, ...]:
    service = locale_service or TelegramLocaleService()
    return tuple(
        BotCommand(
            command=spec.command_key.value,
            description=service.text(spec.description_key, locale=locale),
        )
        for spec in COMMAND_SPECS
    )
