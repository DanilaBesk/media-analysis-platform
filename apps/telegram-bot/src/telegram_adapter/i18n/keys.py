# FILE: apps/telegram-bot/src/telegram_adapter/i18n/keys.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Define supported locale constants and message keys for the final Telegram inbox adapter.
# SCOPE: Command descriptions and short status/action texts for inbox, selection, run, diagnostics, and access flows.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

from enum import StrEnum
from typing import Literal, TypeAlias

SupportedLocale: TypeAlias = Literal["ru", "en"]

DEFAULT_LOCALE: SupportedLocale = "ru"
SUPPORTED_LOCALES: tuple[SupportedLocale, ...] = ("ru", "en")
SUPPORTED_LOCALE_SET = frozenset(SUPPORTED_LOCALES)


class TelegramTextKey(StrEnum):
    COMMAND_START_DESCRIPTION = "command.start.description"
    COMMAND_HELP_DESCRIPTION = "command.help.description"
    COMMAND_INBOX_DESCRIPTION = "command.inbox.description"
    START_PROMPT = "start.prompt"
    HELP_MENU = "help.menu"
    INBOX_SUMMARY_ADDED = "inbox.summary.added"
    INBOX_SUMMARY_HEADER = "inbox.summary.header"
    INBOX_SUMMARY_ITEM = "inbox.summary.item"
    INBOX_REJECTED_ITEM = "inbox.rejected.item"
    INBOX_EMPTY = "inbox.empty"
    INBOX_BUTTON_REFRESH = "inbox.button.refresh"
    INBOX_BUTTON_NEXT_PAGE = "inbox.button.next_page"
    INBOX_BUTTON_REMOVE = "inbox.button.remove"
    INBOX_BUTTON_START_ANALYSIS = "inbox.button.start_analysis"
    RUN_QUEUED = "run.queued"
    RUN_ACTIVE_HEADER = "run.active.header"
    RUN_ACTIVE_ITEM = "run.active.item"
    ACCESS_DENIED = "access.denied"


class TelegramCommandKey(StrEnum):
    START = "start"
    HELP = "help"
    INBOX = "inbox"
