# FILE: apps/telegram-bot/src/telegram_adapter/i18n/catalogs.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Hold RU/EN translations for the final Telegram inbox adapter.
# SCOPE: Compact command, status, and inline action text keyed by the typed Telegram registry.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

from typing import TypeAlias

from telegram_adapter.i18n.keys import SupportedLocale, TelegramTextKey

TranslationCatalog: TypeAlias = dict[TelegramTextKey, str]
TranslationCatalogs: TypeAlias = dict[SupportedLocale, TranslationCatalog]

TRANSLATION_CATALOGS: TranslationCatalogs = {
    "ru": {
        TelegramTextKey.COMMAND_START_DESCRIPTION: "Открыть inbox",
        TelegramTextKey.COMMAND_HELP_DESCRIPTION: "Как управлять inbox",
        TelegramTextKey.COMMAND_INBOX_DESCRIPTION: "Показать текущий inbox",
        TelegramTextKey.START_PROMPT: (
            "Отправьте текст, ссылку, фото, видео или документ. Всё принятое сначала попадает в inbox."
        ),
        TelegramTextKey.HELP_MENU: (
            "/inbox - восстановить текущий inbox\n"
            "Кнопками можно обновить статус, открыть следующую страницу, убрать элемент и запустить анализ."
        ),
        TelegramTextKey.INBOX_SUMMARY_ADDED: "Принято: {count}",
        TelegramTextKey.INBOX_SUMMARY_HEADER: "Inbox ({count}):",
        TelegramTextKey.INBOX_SUMMARY_ITEM: "{index}. {label}",
        TelegramTextKey.INBOX_REJECTED_ITEM: "Отклонено: {label} ({reason})",
        TelegramTextKey.INBOX_EMPTY: "Inbox пока пуст.",
        TelegramTextKey.INBOX_BUTTON_REFRESH: "Обновить",
        TelegramTextKey.INBOX_BUTTON_NEXT_PAGE: "Дальше",
        TelegramTextKey.INBOX_BUTTON_REMOVE: "Убрать {index}",
        TelegramTextKey.INBOX_BUTTON_START_ANALYSIS: "Запустить анализ",
        TelegramTextKey.RUN_QUEUED: "Анализ запущен: {run_id}",
        TelegramTextKey.RUN_ACTIVE_HEADER: "Активные запуски:",
        TelegramTextKey.RUN_ACTIVE_ITEM: "- {run_id}: {status}",
        TelegramTextKey.ACCESS_DENIED: "Доступ к этому боту ограничен.",
    },
    "en": {
        TelegramTextKey.COMMAND_START_DESCRIPTION: "Open inbox",
        TelegramTextKey.COMMAND_HELP_DESCRIPTION: "How to manage inbox",
        TelegramTextKey.COMMAND_INBOX_DESCRIPTION: "Show current inbox",
        TelegramTextKey.START_PROMPT: "Send text, links, photos, videos, or documents. Everything accepted lands in inbox first.",
        TelegramTextKey.HELP_MENU: (
            "/inbox - restore the current inbox\n"
            "Use the buttons to refresh status, open the next page, remove an item, and start analysis."
        ),
        TelegramTextKey.INBOX_SUMMARY_ADDED: "Accepted: {count}",
        TelegramTextKey.INBOX_SUMMARY_HEADER: "Inbox ({count}):",
        TelegramTextKey.INBOX_SUMMARY_ITEM: "{index}. {label}",
        TelegramTextKey.INBOX_REJECTED_ITEM: "Rejected: {label} ({reason})",
        TelegramTextKey.INBOX_EMPTY: "Inbox is empty.",
        TelegramTextKey.INBOX_BUTTON_REFRESH: "Refresh",
        TelegramTextKey.INBOX_BUTTON_NEXT_PAGE: "Next",
        TelegramTextKey.INBOX_BUTTON_REMOVE: "Remove {index}",
        TelegramTextKey.INBOX_BUTTON_START_ANALYSIS: "Start analysis",
        TelegramTextKey.RUN_QUEUED: "Run queued: {run_id}",
        TelegramTextKey.RUN_ACTIVE_HEADER: "Active runs:",
        TelegramTextKey.RUN_ACTIVE_ITEM: "- {run_id}: {status}",
        TelegramTextKey.ACCESS_DENIED: "Access to this bot is restricted.",
    },
}
