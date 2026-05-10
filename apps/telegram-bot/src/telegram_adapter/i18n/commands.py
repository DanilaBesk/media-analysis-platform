# FILE: apps/telegram-bot/src/telegram_adapter/i18n/commands.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Build localized aiogram command metadata for the final Telegram inbox adapter.
# SCOPE: Stable command registry for start, help, and inbox restore commands.
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
    LocalizedCommandSpec(TelegramCommandKey.INBOX, TelegramTextKey.COMMAND_INBOX_DESCRIPTION),
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
