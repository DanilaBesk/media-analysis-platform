# FILE: apps/telegram-bot/src/telegram_adapter/i18n/keys.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Define the typed locale and message-key registry for the packet-local Telegram i18n foundation.
# SCOPE: Supported locale constants plus text and command key registries used by catalog, formatter, and command helpers.
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
    COMMAND_BATCH_DESCRIPTION = "command.batch.description"
    COMMAND_BASKET_DESCRIPTION = "command.basket.description"
    COMMAND_CLEAR_DESCRIPTION = "command.clear.description"
    START_PROMPT = "start.prompt"
    HELP_MENU = "help.menu"
    MODE_STATUS_ENABLED = "mode.status.enabled"
    MODE_STATUS_DISABLED = "mode.status.disabled"
    MODE_BUTTON_ENABLED = "mode.button.enabled"
    MODE_BUTTON_DISABLED = "mode.button.disabled"
    BASKET_EMPTY = "basket.empty"
    BASKET_CLEARED = "basket.cleared"
    BASKET_CLEAR_FAILED = "basket.clear.failed"
    BASKET_UPDATE_FAILED = "basket.update.failed"
    BASKET_UNSUPPORTED_SOURCES = "basket.unsupported.sources"
    BASKET_REJECTED_LINKS_HEADER = "basket.rejected_links.header"
    BASKET_SINGLE_MODE_PROCESS_NOW = "basket.single_mode.process_now"
    BASKET_SINGLE_MODE_SELECT_ONE = "basket.single_mode.select_one"
    BASKET_SUMMARY_ADDED = "basket.summary.added"
    BASKET_SUMMARY_HEADER = "basket.summary.header"
    BASKET_SUMMARY_ITEM = "basket.summary.item"
    BASKET_SUMMARY_FOOTER = "basket.summary.footer"
    BASKET_BUTTON_PROCESS = "basket.button.process"
    BASKET_BUTTON_CLEAR = "basket.button.clear"
    BASKET_BUTTON_REMOVE = "basket.button.remove"
    BASKET_ACTION_INVALID = "basket.action.invalid"
    BASKET_START_ACK = "basket.start.ack"
    BASKET_REMOVE_ACK = "basket.remove.ack"
    BASKET_STALE_BUTTON = "basket.stale_button"
    BASKET_VERSION_CONFLICT = "basket.version_conflict"
    SELECTION_INVALID = "selection.invalid"
    SELECTION_MISSING = "selection.missing"
    SELECTION_ACK = "selection.ack"
    PROCESSING_STATUS_MEDIA = "processing.status.media"
    PROCESSING_STATUS_LINK = "processing.status.link"
    PROCESSING_STATUS_GROUP = "processing.status.group"
    PROCESSING_STATUS_BASKET = "processing.status.basket"
    PROCESSING_FAILED_SOURCE = "processing.failed.source"
    PROCESSING_FAILED_GROUP = "processing.failed.group"
    PROCESSING_FAILED_BASKET = "processing.failed.basket"
    PROCESSING_DONE = "processing.done"
    PROCESSING_DONE_BASKET = "processing.done.basket"
    TRANSCRIPT_FALLBACK_CAPTION_SINGLE = "transcript.fallback_caption.single"
    TRANSCRIPT_FALLBACK_CAPTION_BASKET = "transcript.fallback_caption.basket"
    TRANSCRIPT_TEXT_UNAVAILABLE = "transcript.text_unavailable"
    RESULT_BUTTON_SEGMENTS = "result.button.segments"
    RESULT_BUTTON_REPORT = "result.button.report"
    JOB_INVALID = "job.invalid"
    TRANSCRIPT_ARTIFACTS_NOT_FOUND = "transcript.artifacts_not_found"
    TRANSCRIPT_ACK = "transcript.ack"
    TRANSCRIPT_SEGMENTS_CAPTION = "transcript.segments.caption"
    REPORT_LOCKED = "report.locked"
    REPORT_ACK = "report.ack"
    REPORT_STATUS = "report.status"
    REPORT_FAILED = "report.failed"
    REPORT_READY = "report.ready"
    REPORT_MISSING = "report.missing"
    REPORT_MISSING_JOB_ID = "report.missing_job_id"
    REPORT_CAPTION = "report.caption"
    REPORT_BUTTON_DEEP_RESEARCH = "report.button.deep_research"
    DEEP_RESEARCH_LOCKED = "deep_research.locked"
    DEEP_RESEARCH_ACK = "deep_research.ack"
    DEEP_RESEARCH_STATUS = "deep_research.status"
    DEEP_RESEARCH_FAILED = "deep_research.failed"
    DEEP_RESEARCH_READY = "deep_research.ready"
    DEEP_RESEARCH_CAPTION = "deep_research.caption"
    ACCESS_DENIED = "access.denied"
    SOURCE_LABEL_UNKNOWN = "source_label.unknown"
    SOURCE_LABEL_TELEGRAM_AUDIO = "source_label.telegram_audio"
    SOURCE_LABEL_TELEGRAM_VIDEO = "source_label.telegram_video"


class TelegramCommandKey(StrEnum):
    START = "start"
    HELP = "help"
    BATCH = "batch"
    BASKET = "basket"
    CLEAR = "clear"
