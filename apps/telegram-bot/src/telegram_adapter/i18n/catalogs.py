# FILE: apps/telegram-bot/src/telegram_adapter/i18n/catalogs.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Hold the packet-local RU/EN translation catalogs for the Telegram adapter i18n foundation.
# SCOPE: Typed translation catalogs keyed by the shared Telegram text registry.
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
        TelegramTextKey.COMMAND_START_DESCRIPTION: "Открыть меню",
        TelegramTextKey.COMMAND_HELP_DESCRIPTION: "Как пользоваться ботом",
        TelegramTextKey.COMMAND_BATCH_DESCRIPTION: "Включить или выключить режим подборки",
        TelegramTextKey.COMMAND_BASKET_DESCRIPTION: "Показать текущую подборку",
        TelegramTextKey.COMMAND_CLEAR_DESCRIPTION: "Очистить подборку",
        TelegramTextKey.START_PROMPT: (
            "Отправьте голосовое, аудио, видео, документ или ссылку. "
            "В режиме подборки я собираю несколько источников и запускаю одну общую расшифровку."
        ),
        TelegramTextKey.HELP_MENU: (
            "Команды доступны в меню Telegram рядом с полем ввода:\n"
            "/start - открыть меню\n"
            "/help - как пользоваться ботом\n"
            "/batch - включить или выключить режим подборки\n"
            "/basket - показать текущую подборку\n"
            "/clear - очистить подборку\n\n"
            "Отправьте ссылку, голосовое, аудио, видео или документ. "
            "В режиме подборки бот собирает несколько источников, а затем запускает одну общую расшифровку. "
            "Кнопками можно убрать отдельные элементы, очистить подборку или запустить обработку."
        ),
        TelegramTextKey.MODE_STATUS_ENABLED: (
            "Режим подборки включён. Новые источники будут добавляться в одну подборку."
        ),
        TelegramTextKey.MODE_STATUS_DISABLED: (
            "Режим подборки выключен. Один источник я обработаю сразу, а при нескольких предложу выбрать один."
        ),
        TelegramTextKey.MODE_BUTTON_ENABLED: "Режим подборки: включён",
        TelegramTextKey.MODE_BUTTON_DISABLED: "Режим подборки: выключен",
        TelegramTextKey.BASKET_EMPTY: "Подборка пока пуста.",
        TelegramTextKey.BASKET_CLEARED: "Подборка очищена.",
        TelegramTextKey.BASKET_CLEAR_FAILED: "Не удалось очистить подборку: {error}",
        TelegramTextKey.BASKET_UPDATE_FAILED: "Не удалось обновить подборку: {error}",
        TelegramTextKey.BASKET_UNSUPPORTED_SOURCES: (
            "Не нашёл поддерживаемых источников. Поддерживаются ссылки на YouTube, распознаваемые ссылки и медиа "
            "или документы из Telegram."
        ),
        TelegramTextKey.BASKET_REJECTED_LINKS_HEADER: "Пропущенные неподдерживаемые ссылки:",
        TelegramTextKey.BASKET_SINGLE_MODE_PROCESS_NOW: (
            "Режим подборки выключен. Запускаю обработку выбранного источника."
        ),
        TelegramTextKey.BASKET_SINGLE_MODE_SELECT_ONE: (
            "Режим подборки выключен. Выберите один источник для обработки."
        ),
        TelegramTextKey.BASKET_SUMMARY_ADDED: "Добавлено: {count}",
        TelegramTextKey.BASKET_SUMMARY_HEADER: "Подборка ({count}):",
        TelegramTextKey.BASKET_SUMMARY_ITEM: "{index}. {label}",
        TelegramTextKey.BASKET_SUMMARY_FOOTER: "После запуска вы получите одну общую расшифровку.",
        TelegramTextKey.BASKET_BUTTON_PROCESS: "Запустить обработку",
        TelegramTextKey.BASKET_BUTTON_CLEAR: "Очистить подборку",
        TelegramTextKey.BASKET_BUTTON_REMOVE: "Убрать {index}",
        TelegramTextKey.BASKET_ACTION_INVALID: "Некорректное действие для подборки.",
        TelegramTextKey.BASKET_START_ACK: "Запускаю обработку подборки...",
        TelegramTextKey.BASKET_REMOVE_ACK: "Источник удалён из подборки.",
        TelegramTextKey.BASKET_STALE_BUTTON: "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
        TelegramTextKey.BASKET_VERSION_CONFLICT: "Подборка изменилась. Откройте /basket, чтобы увидеть актуальное состояние.",
        TelegramTextKey.SELECTION_INVALID: "Некорректный выбор источника.",
        TelegramTextKey.SELECTION_MISSING: "Источник больше недоступен.",
        TelegramTextKey.SELECTION_ACK: "Запускаю обработку...",
        TelegramTextKey.PROCESSING_STATUS_MEDIA: (
            "Обрабатываю источник: {label}\n\n"
            "Скачиваю файл из Telegram и запускаю расшифровку. Это может занять несколько минут."
        ),
        TelegramTextKey.PROCESSING_STATUS_LINK: (
            "Обрабатываю источник: {label}\n\n"
            "Проверяю доступный текст и при необходимости запускаю расшифровку."
        ),
        TelegramTextKey.PROCESSING_STATUS_GROUP: (
            "Обрабатываю подборку ({count}).\n\n"
            "Скачиваю файлы из Telegram и отправляю весь набор на одну общую обработку. Это может занять несколько минут."
        ),
        TelegramTextKey.PROCESSING_STATUS_BASKET: (
            "Обрабатываю подборку ({count}).\n\n"
            "Отправляю собранные источники на одну общую обработку. Это может занять несколько минут."
        ),
        TelegramTextKey.PROCESSING_FAILED_SOURCE: "Не удалось обработать источник: {error}",
        TelegramTextKey.PROCESSING_FAILED_GROUP: "Не удалось обработать источники: {error}",
        TelegramTextKey.PROCESSING_FAILED_BASKET: "Не удалось обработать подборку: {error}",
        TelegramTextKey.PROCESSING_DONE: "Расшифровка готова.",
        TelegramTextKey.PROCESSING_DONE_BASKET: "Общая расшифровка готова.",
        TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_SINGLE: "Готовая расшифровка в виде текстового файла.",
        TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_BASKET: "Готовая общая расшифровка в виде текстового файла.",
        TelegramTextKey.TRANSCRIPT_TEXT_UNAVAILABLE: (
            "Расшифровка готова, но текстовый файл сейчас недоступен."
        ),
        TelegramTextKey.RESULT_BUTTON_SEGMENTS: "Текст по частям",
        TelegramTextKey.RESULT_BUTTON_REPORT: "Подготовить отчёт",
        TelegramTextKey.JOB_INVALID: "Некорректный идентификатор задачи.",
        TelegramTextKey.TRANSCRIPT_ARTIFACTS_NOT_FOUND: "Файлы результата не найдены.",
        TelegramTextKey.TRANSCRIPT_ACK: "Отправляю расшифровку по частям...",
        TelegramTextKey.TRANSCRIPT_SEGMENTS_CAPTION: "Расшифровка по частям в формате Markdown.",
        TelegramTextKey.REPORT_LOCKED: "Отчёт уже формируется. Подождите немного.",
        TelegramTextKey.REPORT_ACK: "Запускаю подготовку отчёта...",
        TelegramTextKey.REPORT_STATUS: "Готовлю отчёт. Это может занять несколько минут.",
        TelegramTextKey.REPORT_FAILED: "Не удалось подготовить отчёт: {error}",
        TelegramTextKey.REPORT_READY: "Отчёт готов.",
        TelegramTextKey.REPORT_MISSING: "Отчёт не был сохранён.",
        TelegramTextKey.REPORT_MISSING_JOB_ID: (
            "Отчёт сохранён, но для следующего шага не хватает идентификатора."
        ),
        TelegramTextKey.REPORT_CAPTION: "Отчёт в формате Markdown.",
        TelegramTextKey.REPORT_BUTTON_DEEP_RESEARCH: "Подготовить расширенный обзор",
        TelegramTextKey.DEEP_RESEARCH_LOCKED: "Расширенный обзор уже формируется. Подождите немного.",
        TelegramTextKey.DEEP_RESEARCH_ACK: "Запускаю расширенный обзор...",
        TelegramTextKey.DEEP_RESEARCH_STATUS: (
            "Готовлю расширенный обзор. Это может занять больше времени, чем обычный отчёт."
        ),
        TelegramTextKey.DEEP_RESEARCH_FAILED: "Не удалось подготовить расширенный обзор: {error}",
        TelegramTextKey.DEEP_RESEARCH_READY: "Расширенный обзор готов.",
        TelegramTextKey.DEEP_RESEARCH_CAPTION: "Расширенный обзор в формате Markdown.",
        TelegramTextKey.ACCESS_DENIED: "Доступ к этому боту ограничен.",
        TelegramTextKey.SOURCE_LABEL_UNKNOWN: "Неизвестный источник",
        TelegramTextKey.SOURCE_LABEL_TELEGRAM_AUDIO: "Аудиофайл из Telegram",
        TelegramTextKey.SOURCE_LABEL_TELEGRAM_VIDEO: "Видеофайл из Telegram",
    },
    "en": {
        TelegramTextKey.COMMAND_START_DESCRIPTION: "Open the menu",
        TelegramTextKey.COMMAND_HELP_DESCRIPTION: "How to use the bot",
        TelegramTextKey.COMMAND_BATCH_DESCRIPTION: "Turn collection mode on or off",
        TelegramTextKey.COMMAND_BASKET_DESCRIPTION: "Show the current collection",
        TelegramTextKey.COMMAND_CLEAR_DESCRIPTION: "Clear the collection",
        TelegramTextKey.START_PROMPT: (
            "Send a voice message, audio, video, document, or a link. "
            "In collection mode I gather several sources and run one shared transcript."
        ),
        TelegramTextKey.HELP_MENU: (
            "Telegram commands are available from the menu next to the input field:\n"
            "/start - open the menu\n"
            "/help - how to use the bot\n"
            "/batch - turn collection mode on or off\n"
            "/basket - show the current collection\n"
            "/clear - clear the collection\n\n"
            "Send a link, voice message, audio, video, or document. "
            "In collection mode the bot gathers several sources, then runs one shared transcript. "
            "Buttons let you remove items, clear the collection, or start processing."
        ),
        TelegramTextKey.MODE_STATUS_ENABLED: "Collection mode is on. New sources will be added to one collection.",
        TelegramTextKey.MODE_STATUS_DISABLED: (
            "Collection mode is off. I will process one source right away, and ask you to choose when there are several."
        ),
        TelegramTextKey.MODE_BUTTON_ENABLED: "Collection mode: on",
        TelegramTextKey.MODE_BUTTON_DISABLED: "Collection mode: off",
        TelegramTextKey.BASKET_EMPTY: "The collection is empty.",
        TelegramTextKey.BASKET_CLEARED: "The collection has been cleared.",
        TelegramTextKey.BASKET_CLEAR_FAILED: "Couldn't clear the collection: {error}",
        TelegramTextKey.BASKET_UPDATE_FAILED: "Couldn't update the collection: {error}",
        TelegramTextKey.BASKET_UNSUPPORTED_SOURCES: (
            "I couldn't find any supported sources. Supported inputs include YouTube links, recognized links, and "
            "Telegram media or documents."
        ),
        TelegramTextKey.BASKET_REJECTED_LINKS_HEADER: "Skipped unsupported links:",
        TelegramTextKey.BASKET_SINGLE_MODE_PROCESS_NOW: "Collection mode is off. Starting processing for the selected source.",
        TelegramTextKey.BASKET_SINGLE_MODE_SELECT_ONE: "Collection mode is off. Choose one source to process.",
        TelegramTextKey.BASKET_SUMMARY_ADDED: "Added: {count}",
        TelegramTextKey.BASKET_SUMMARY_HEADER: "Collection ({count}):",
        TelegramTextKey.BASKET_SUMMARY_ITEM: "{index}. {label}",
        TelegramTextKey.BASKET_SUMMARY_FOOTER: "When you start it, you will receive one shared transcript.",
        TelegramTextKey.BASKET_BUTTON_PROCESS: "Start processing",
        TelegramTextKey.BASKET_BUTTON_CLEAR: "Clear collection",
        TelegramTextKey.BASKET_BUTTON_REMOVE: "Remove {index}",
        TelegramTextKey.BASKET_ACTION_INVALID: "Invalid action for this collection.",
        TelegramTextKey.BASKET_START_ACK: "Starting collection processing...",
        TelegramTextKey.BASKET_REMOVE_ACK: "The source was removed from the collection.",
        TelegramTextKey.BASKET_STALE_BUTTON: "This button is outdated. Open /basket or send a new source.",
        TelegramTextKey.BASKET_VERSION_CONFLICT: "The collection changed. Open /basket to see the latest state.",
        TelegramTextKey.SELECTION_INVALID: "Invalid source selection.",
        TelegramTextKey.SELECTION_MISSING: "That source is no longer available.",
        TelegramTextKey.SELECTION_ACK: "Starting processing...",
        TelegramTextKey.PROCESSING_STATUS_MEDIA: (
            "Processing source: {label}\n\n"
            "Downloading the file from Telegram and starting the transcript. This can take a few minutes."
        ),
        TelegramTextKey.PROCESSING_STATUS_LINK: (
            "Processing source: {label}\n\n"
            "Checking for available text and starting a transcript if needed."
        ),
        TelegramTextKey.PROCESSING_STATUS_GROUP: (
            "Processing collection ({count}).\n\n"
            "Downloading Telegram files and sending the whole set for one shared run. This can take a few minutes."
        ),
        TelegramTextKey.PROCESSING_STATUS_BASKET: (
            "Processing collection ({count}).\n\n"
            "Sending the collected sources for one shared run. This can take a few minutes."
        ),
        TelegramTextKey.PROCESSING_FAILED_SOURCE: "Couldn't process the source: {error}",
        TelegramTextKey.PROCESSING_FAILED_GROUP: "Couldn't process the sources: {error}",
        TelegramTextKey.PROCESSING_FAILED_BASKET: "Couldn't process the collection: {error}",
        TelegramTextKey.PROCESSING_DONE: "The transcript is ready.",
        TelegramTextKey.PROCESSING_DONE_BASKET: "The shared transcript is ready.",
        TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_SINGLE: "The finished transcript as a text file.",
        TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_BASKET: "The finished shared transcript as a text file.",
        TelegramTextKey.TRANSCRIPT_TEXT_UNAVAILABLE: "The transcript is ready, but the text file is unavailable right now.",
        TelegramTextKey.RESULT_BUTTON_SEGMENTS: "Segmented transcript",
        TelegramTextKey.RESULT_BUTTON_REPORT: "Prepare report",
        TelegramTextKey.JOB_INVALID: "Invalid job identifier.",
        TelegramTextKey.TRANSCRIPT_ARTIFACTS_NOT_FOUND: "Result files were not found.",
        TelegramTextKey.TRANSCRIPT_ACK: "Sending the segmented transcript...",
        TelegramTextKey.TRANSCRIPT_SEGMENTS_CAPTION: "Segmented transcript in Markdown format.",
        TelegramTextKey.REPORT_LOCKED: "A report is already being prepared. Please wait.",
        TelegramTextKey.REPORT_ACK: "Starting report preparation...",
        TelegramTextKey.REPORT_STATUS: "Preparing the report. This can take a few minutes.",
        TelegramTextKey.REPORT_FAILED: "Couldn't prepare the report: {error}",
        TelegramTextKey.REPORT_READY: "The report is ready.",
        TelegramTextKey.REPORT_MISSING: "The report was not saved.",
        TelegramTextKey.REPORT_MISSING_JOB_ID: "The report was saved, but the next step is unavailable because its identifier is missing.",
        TelegramTextKey.REPORT_CAPTION: "Report in Markdown format.",
        TelegramTextKey.REPORT_BUTTON_DEEP_RESEARCH: "Prepare expanded research",
        TelegramTextKey.DEEP_RESEARCH_LOCKED: "Expanded research is already being prepared. Please wait.",
        TelegramTextKey.DEEP_RESEARCH_ACK: "Starting expanded research...",
        TelegramTextKey.DEEP_RESEARCH_STATUS: (
            "Preparing expanded research. This may take longer than a regular report."
        ),
        TelegramTextKey.DEEP_RESEARCH_FAILED: "Couldn't prepare expanded research: {error}",
        TelegramTextKey.DEEP_RESEARCH_READY: "Expanded research is ready.",
        TelegramTextKey.DEEP_RESEARCH_CAPTION: "Expanded research in Markdown format.",
        TelegramTextKey.ACCESS_DENIED: "Access to this bot is restricted.",
        TelegramTextKey.SOURCE_LABEL_UNKNOWN: "Unknown source",
        TelegramTextKey.SOURCE_LABEL_TELEGRAM_AUDIO: "Telegram audio file",
        TelegramTextKey.SOURCE_LABEL_TELEGRAM_VIDEO: "Telegram video file",
    },
}
