# FILE: apps/telegram-bot/src/telegram_adapter/bot.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Run the always-on Telegram inbox client over API-owned media, selection, and analysis_run state.
# SCOPE: Aiogram handlers for message ingestion, status refresh, paging, item removal, run start, and restart-safe restore.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP, M-CONTRACTS
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import asyncio
import base64
import logging
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, BinaryIO
from urllib.parse import unquote, urlparse
from urllib.request import urlopen
from uuid import UUID

from aiogram import Bot, Dispatcher, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramConflictError,
    TelegramEntityTooLarge,
    TelegramForbiddenError,
    TelegramMigrateToChat,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from aiogram.filters import Command
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
    InputFile,
)

from telegram_adapter.api_client import TelegramApiClientError
from telegram_adapter.errors import (
    TelegramUserError,
    TelegramUserErrorCode,
    rejected_reason_text,
    safe_callback_answer,
    user_error_text,
)
from telegram_adapter.gateway import (
    ANALYSIS_TASK_SURFACE,
    ACTIVE_RUN_STATUSES,
    CANCELABLE_RUN_STATUSES,
    CURRENT_MATERIALS_PANEL,
    EXPORT_TASK_SURFACE,
    InboxStatus,
    IngressRecord,
    RESULT_ARTIFACT_SURFACE,
    TERMINAL_RUN_STATUSES,
    TERMINAL_EXPORT_STATUSES,
    TelegramFileInput,
    TelegramInboxGateway,
)
from telegram_adapter.i18n import DEFAULT_LOCALE, TelegramLocaleService, TelegramTextKey, build_localized_commands
from telegram_adapter.policy import TelegramChatScope
from telegram_adapter.presentation import render_material_summary_lines

JsonObject = dict[str, Any]
_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_TELEGRAM_HANDLER_ERROR = "[TelegramAdapter][bot][BLOCK_HANDLE_TELEGRAM_HANDLER_ERROR]"
_LOG_MARKER_TELEGRAM_SURFACE_FAILURE = "[TelegramAdapter][bot][BLOCK_HANDLE_TELEGRAM_SURFACE_FAILURE]"
_LOG_MARKER_TELEGRAM_POLLING_STATE = "[TelegramAdapter][bot][BLOCK_TRACK_TELEGRAM_POLLING_STATE]"
_AUTO_DELIVER_RUN_STATUSES = {"succeeded", "partially_succeeded"}
_BUFFER_LINK_PREVIEW_OPTIONS = LinkPreviewOptions(is_disabled=True)
_MAX_EXPORT_DELIVERY_BYTES = 2 * 1024 * 1024 * 1024


class _TemporaryInputFile(InputFile):
    def __init__(self, handle: BinaryIO, *, filename: str) -> None:
        super().__init__(filename=filename)
        self.handle = handle

    async def read(self, _bot: Bot):
        while chunk := await asyncio.to_thread(self.handle.read, self.chunk_size):
            yield chunk


@dataclass(slots=True)
class _PageState:
    current_cursor: str | None = None
    previous_cursors: list[str | None] = field(default_factory=list)
    next_cursor: str | None = None
    selection: JsonObject | None = None
    screen: str = "main"
    focused_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class _AutoDeliveryResult:
    status: InboxStatus
    delivered: bool
    result_message_id: int | None = None


@dataclass(frozen=True, slots=True)
class _ResultDeliveryResult:
    notice: str
    show_alert: bool
    message_id: int | None = None


CALLBACK_NAMESPACE = "ib"


@dataclass(frozen=True, slots=True)
class _TelegramSurfaceErrorClassification:
    classification: str
    lifecycle_reason: str | None
    fatal: bool = False


class _TelegramSurfaceDeliveryFailure(RuntimeError):
    pass


def _create_bot(settings: Any) -> Bot:
    bot_api_base_url = str(getattr(settings, "telegram_bot_api_base_url", "") or "").strip()
    if not bot_api_base_url:
        return Bot(settings.telegram_bot_token)
    api = TelegramAPIServer.from_base(
        bot_api_base_url,
        is_local=bool(getattr(settings, "telegram_bot_api_local_mode", False)),
    )
    return Bot(settings.telegram_bot_token, session=AiohttpSession(api=api))


class TelegramInboxApp:
    def __init__(self, settings: Any, gateway: TelegramInboxGateway, bot: Bot | None = None) -> None:
        self.settings = settings
        self.gateway = gateway
        self.bot = bot or _create_bot(settings)
        self.dispatcher = Dispatcher()
        self.router = Router(name="telegram-inbox")
        self.locale_service = TelegramLocaleService()
        self.status_message_ids: dict[tuple[int, int | None], int] = {}
        self.status_update_locks: dict[tuple[int, int | None], asyncio.Lock] = {}
        self.page_states: dict[tuple[int, int | None], _PageState] = {}
        self.run_status_poll_attempts = 3
        self.run_status_poll_delay_seconds = 0.2
        self.run_status_follow_attempts = 120
        self.export_status_follow_attempts = 3600
        self.run_status_follow_delay_seconds = 2.0
        self.run_watch_tasks: dict[tuple[int, int | None], asyncio.Task[None]] = {}
        self.export_watch_tasks: dict[str, asyncio.Task[None]] = {}
        self.export_selections: dict[tuple[int, int | None], JsonObject] = {}
        self.inbound_status_burst_until: dict[tuple[int, int | None], float] = {}
        self.inbound_status_burst_window_seconds = 5.0
        self._monotonic = time.monotonic
        self._sleep = asyncio.sleep
        self._register_handlers()
        self.dispatcher.include_router(self.router)

    async def run(self) -> None:
        polling_monitor = _TelegramPollingMonitor()
        dispatcher_logger = logging.getLogger("aiogram.dispatcher")
        dispatcher_logger.addHandler(polling_monitor)
        for locale in ("ru", "en"):
            await self.bot.set_my_commands(
                list(build_localized_commands(locale, locale_service=self.locale_service)),
                language_code=locale,
            )
        await self._recover_active_channel_surfaces()
        try:
            await self.dispatcher.start_polling(self.bot)
        finally:
            dispatcher_logger.removeHandler(polling_monitor)

    def _register_handlers(self) -> None:
        self.router.message.register(self._handle_start, Command("start"))
        self.router.message.register(self._handle_help, Command("help"))
        self.router.message.register(self._handle_inbox, Command("inbox"))
        self.router.message.register(self._handle_any_message)
        self.router.callback_query.register(
            self._handle_status_callback,
            lambda call: bool(call.data and call.data.startswith(f"{CALLBACK_NAMESPACE}:")),
        )

    async def _handle_start(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        await message.answer(_start_text())
        await self._send_or_edit_status(message)

    async def _handle_help(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        await message.answer(_help_text())
        await self._send_or_edit_status(message)

    async def _handle_inbox(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        await self._send_or_edit_status(message)

    async def _handle_any_message(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        channel_identity = self._channel_identity_from_message(message)
        pending_status_sent = False
        files: list[TelegramFileInput] = []
        try:
            pending_status_sent = await self._send_pending_file_ingest_status(message, _message_files(message))
            files = await self._download_message_files(message)
            records = await asyncio.to_thread(
                self.gateway.add_message_inputs,
                channel_identity=channel_identity,
                text=_message_text(message),
                files=files,
                message_id=message.message_id,
            )
        except Exception as exc:
            normalized = _normalize_message_error(exc)
            _log_handler_exception("message_ingest", exc, normalized=normalized, message=message)
            await self._answer_message_error(message, normalized)
            return
        finally:
            for file_input in files:
                _close_file_input(file_input)
        await self._send_or_edit_status(message, rejected=[record for record in records if record.status == "rejected"], post_ingest_records=records, fresh_for_inbound_burst=not pending_status_sent)

    async def _download_message_files(self, message: Message) -> list[TelegramFileInput]:
        hydrated: list[TelegramFileInput] = []
        try:
            for file_input in _message_files(message):
                telegram_file = await self.bot.get_file(file_input.file_id)
                destination = tempfile.TemporaryFile(prefix="telegram-upload-", suffix=".tmp")
                try:
                    await self.bot.download_file(telegram_file.file_path, destination=destination)
                    destination.flush()
                    destination.seek(0, 2)
                    size_bytes = destination.tell()
                    destination.seek(0)
                except Exception:
                    destination.close()
                    raise
                if size_bytes <= 0:
                    destination.close()
                    raise RuntimeError("telegram_file_download_failed")
                hydrated.append(replace(file_input, file_handle=destination, size_bytes=size_bytes))
        except Exception:
            for file_input in hydrated:
                _close_file_input(file_input)
            raise
        return hydrated

    async def _send_pending_file_ingest_status(
        self,
        message: Message,
        file_inputs: Iterable[TelegramFileInput],
    ) -> bool:
        pending_files = list(file_inputs)
        if not pending_files:
            return False
        key = self._scope_from_message(message).state_key
        text = _pending_file_ingest_status_text(pending_files)
        lock = self.status_update_locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = self._monotonic()
            prefer_edit = now < self.inbound_status_burst_until.get(key, 0.0)
            self.inbound_status_burst_until[key] = now + self.inbound_status_burst_window_seconds
            previous_message_id = self.status_message_ids.get(key) if prefer_edit else None
            if previous_message_id is not None:
                try:
                    await self.bot.edit_message_text(
                        text,
                        chat_id=message.chat.id,
                        message_id=previous_message_id,
                        reply_markup=None,
                        link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
                    )
                    self.status_message_ids[key] = previous_message_id
                    return True
                except TelegramBadRequest as error:
                    if "message is not modified" in str(error).lower():
                        self.status_message_ids[key] = previous_message_id
                        return True
                    self.status_message_ids.pop(key, None)
                except Exception:
                    self.status_message_ids.pop(key, None)
            sent = await message.answer(text, link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS)
            self.status_message_ids[key] = sent.message_id
            return True

    async def _handle_status_callback(self, callback: CallbackQuery) -> None:
        if not await self._ensure_callback_allowed(callback):
            return
        if callback.message is None:
            await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
            return
        channel_identity = self._channel_identity_from_callback(callback)
        data = callback.data or ""
        key = self._state_key_from_callback(callback)
        page_state = self.page_states.get(key)
        try:
            action, tokens = _parse_callback_payload(data)
            if action == "rf":
                status = self.gateway.restore_status(channel_identity=channel_identity)
                selection = page_state.selection if page_state else None
                screen = page_state.screen if page_state else "main"
                focused_run_id = page_state.focused_run_id if page_state else None
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=selection,
                    screen=screen,
                    focused_run_id=focused_run_id,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Состояние обновлено")
                return
            if action == "mt":
                self.export_selections.pop(key, None)
                status = self.gateway.restore_status(channel_identity=channel_identity)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="materials",
                    focused_run_id=page_state.focused_run_id if page_state else None,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Открыт список материалов")
                return
            if action == "mn":
                self.export_selections.pop(key, None)
                status = self.gateway.restore_status(channel_identity=channel_identity)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="main",
                    focused_run_id=page_state.focused_run_id if page_state else None,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Открыта главная карточка")
                return
            if action == "pn":
                if page_state is None:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                if not page_state.next_cursor:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                cursor = page_state.next_cursor
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=cursor)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=cursor,
                    previous_cursors=[*page_state.previous_cursors, page_state.current_cursor],
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=page_state.focused_run_id,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Открыта следующая страница")
                return
            if action == "pp":
                if page_state is None:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                if not page_state.previous_cursors:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                cursor = page_state.previous_cursors[-1]
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=cursor)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=cursor,
                    previous_cursors=page_state.previous_cursors[:-1],
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=page_state.focused_run_id,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Открыта предыдущая страница")
                return
            if action == "rm":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                media_asset_id = _decode_callback_token(tokens[2])
                status = self.gateway.remove_collection_item(
                    channel_identity=channel_identity,
                    collection_id=collection_id,
                    media_asset_id=media_asset_id,
                    expected_version=expected_version,
                    cursor=page_state.current_cursor if page_state else None,
                )
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor if page_state else None,
                    previous_cursors=page_state.previous_cursors if page_state else [],
                    selection=page_state.selection if page_state else None,
                    screen=page_state.screen if page_state else "materials",
                    focused_run_id=page_state.focused_run_id if page_state else None,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Материал убран")
                return
            if action == "ex":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                media_asset_id = _decode_callback_token(tokens[2])
                status = self.gateway.restore_status(
                    channel_identity=channel_identity,
                    cursor=page_state.current_cursor if page_state else None,
                )
                item = next((item for item in status.items if str(item.get("media_asset_id") or "") == media_asset_id), None)
                if item is None or not _is_youtube_export_item(item):
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                self.export_selections[key] = {
                    "collection_id": collection_id,
                    "expected_version": expected_version,
                    "media_asset_id": media_asset_id,
                    "mode": "youtube",
                }
                await self._edit_callback_status(callback, status, prefix="Скачать с YouTube\nВыбери формат.\n\n")
                await callback.answer("Выбери формат")
                return
            if action in {"ea", "eq"}:
                collection_id = _decode_callback_token(tokens[0])
                _decode_callback_version(tokens[1])
                media_asset_id = _decode_callback_token(tokens[2])
                if action == "ea":
                    operation, variant = "video_to_audio", {"audio_bitrate_kbps": 192}
                else:
                    quality = _decode_callback_token(tokens[3])
                    operation, variant = "youtube_video", {"video_quality": quality}
                job = self.gateway.create_export_job(
                    channel_identity=channel_identity,
                    collection_id=collection_id,
                    media_asset_id=media_asset_id,
                    operation=operation,
                    variant=variant,
                    action_id=str(callback.id),
                )
                self.export_selections.pop(key, None)
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=page_state.current_cursor if page_state else None)
                self._set_page_state(
                    key, status, current_cursor=page_state.current_cursor if page_state else None,
                    previous_cursors=page_state.previous_cursors if page_state else [], selection=None,
                    screen=page_state.screen if page_state else "main", focused_run_id=page_state.focused_run_id if page_state else None,
                )
                surface = await self._anchor_export_task_surface(
                    channel_identity=channel_identity,
                    export_job=job,
                    chat_id=callback.message.chat.id,
                )
                self._schedule_export_status_tracking(
                    channel_identity=channel_identity, export_job_id=str(job["export_job_id"]), chat_id=callback.message.chat.id,
                    surface=surface,
                )
                await self._edit_callback_status(callback, status, prefix="Экспорт запущен.\n\n")
                await callback.answer("Экспорт запущен")
                return
            if action == "ey":
                selection = self.export_selections.get(key)
                if selection is None:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                media_asset_id = str(selection["media_asset_id"])
                job = self.gateway.create_export_job(
                    channel_identity=channel_identity,
                    collection_id=str(selection["collection_id"]),
                    media_asset_id=media_asset_id,
                    operation="youtube_audio",
                    variant={"audio_bitrate_kbps": 192},
                    action_id=str(callback.id),
                )
                self.export_selections.pop(key, None)
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=page_state.current_cursor if page_state else None)
                surface = await self._anchor_export_task_surface(
                    channel_identity=channel_identity,
                    export_job=job,
                    chat_id=callback.message.chat.id,
                )
                self._schedule_export_status_tracking(
                    channel_identity=channel_identity, export_job_id=str(job["export_job_id"]), chat_id=callback.message.chat.id,
                    surface=surface,
                )
                await self._edit_callback_status(callback, status, prefix="Экспорт запущен.\n\n")
                await callback.answer("Экспорт аудио запущен")
                return
            if action == "ev":
                selection = self.export_selections.get(key)
                if selection is None or selection.get("mode") != "youtube":
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                selection["mode"] = "video_quality"
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=page_state.current_cursor if page_state else None)
                await self._edit_callback_status(callback, status, prefix="Скачать видео с YouTube\nВыбери качество.\n\n")
                await callback.answer("Выбери качество")
                return
            if action == "cl":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                current_cursor = _decode_optional_callback_token(tokens[2])
                status = self.gateway.clear_visible_items(
                    channel_identity=channel_identity,
                    collection_id=collection_id,
                    expected_version=expected_version,
                    cursor=current_cursor,
                )
                previous_cursors = page_state.previous_cursors if page_state else []
                if not status.items and current_cursor is not None and previous_cursors:
                    current_cursor = previous_cursors[-1]
                    previous_cursors = previous_cursors[:-1]
                    status = self.gateway.restore_status(channel_identity=channel_identity, cursor=current_cursor)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=current_cursor,
                    previous_cursors=previous_cursors,
                    selection=page_state.selection if page_state else None,
                    screen=page_state.screen if page_state else "materials",
                    focused_run_id=page_state.focused_run_id if page_state else None,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Видимые материалы убраны")
                return
            if action == "rl":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                status = self.gateway.remove_latest_collection_item(
                    channel_identity=channel_identity,
                    collection_id=collection_id,
                    expected_version=expected_version,
                    cursor=page_state.current_cursor if page_state else None,
                )
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor if page_state else None,
                    previous_cursors=page_state.previous_cursors if page_state else [],
                    selection=page_state.selection if page_state else None,
                    screen=page_state.screen if page_state else "materials",
                    focused_run_id=page_state.focused_run_id if page_state else None,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Последний материал убран")
                return
            if action == "sl":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                status, prefix, answer_text, run_id, terminal_status, reused_transcript = await self._start_analysis_from_collection(
                    channel_identity=channel_identity,
                    collection_id=collection_id,
                    expected_version=expected_version,
                )
                delivered = False
                result_message_id = None
                if terminal_status in _AUTO_DELIVER_RUN_STATUSES and run_id:
                    run_version = _analysis_run_version(status, run_id)
                    if run_version is not None:
                        delivery = await self._auto_deliver_and_maybe_clear_collection(
                            channel_identity=channel_identity,
                            analysis_run_id=run_id,
                            expected_version=run_version,
                            chat_id=callback.message.chat.id,
                            message=callback.message,
                            allow_repeat_delivery=reused_transcript,
                        )
                        status = delivery.status
                        delivered = delivery.delivered
                        result_message_id = delivery.result_message_id
                        if reused_transcript and delivered:
                            answer_text = "Транскрипт отправлен файлом"
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="main",
                    focused_run_id=None if delivered else run_id,
                )
                task_surface = None
                if terminal_status is None and run_id:
                    task_surface = self._persist_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run=_run_for_id(status, run_id) or {"analysis_run_id": run_id},
                        state=self.page_states.get(key, _PageState()),
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                    )
                    self._schedule_run_status_tracking(
                        key=key,
                        channel_identity=channel_identity,
                        analysis_run_id=run_id,
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                        surface=task_surface,
                    )
                elif terminal_status is not None and run_id:
                    self._try_finish_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run_id=run_id,
                        surface=task_surface,
                    )
                if result_message_id is not None:
                    await self._reanchor_current_status_after_result(
                        key=key,
                        channel_identity=channel_identity,
                        chat_id=callback.message.chat.id,
                        previous_message_id=callback.message.message_id,
                        status=status,
                        prefix=prefix,
                    )
                else:
                    await self._edit_callback_status(callback, status, prefix=prefix)
                await callback.answer(answer_text)
                return
            if action == "rn":
                if len(tokens) >= 2:
                    collection_id = _decode_callback_token(tokens[0])
                    expected_version = _decode_callback_version(tokens[1])
                    status, prefix, answer_text, run_id, terminal_status, reused_transcript = await self._start_analysis_from_collection(
                        channel_identity=channel_identity,
                        collection_id=collection_id,
                        expected_version=expected_version,
                    )
                else:
                    selection_snapshot_id = _decode_callback_token(tokens[0])
                    run = self.gateway.start_analysis(channel_identity=channel_identity, selection_snapshot_id=selection_snapshot_id)
                    status, prefix, answer_text, run_id, terminal_status = await self._resolve_run_start_status(
                        channel_identity=channel_identity,
                        run=run,
                    )
                    reused_transcript = False
                delivered = False
                result_message_id = None
                if terminal_status in _AUTO_DELIVER_RUN_STATUSES and run_id:
                    run_version = _analysis_run_version(status, run_id)
                    if run_version is not None:
                        delivery = await self._auto_deliver_and_maybe_clear_collection(
                            channel_identity=channel_identity,
                            analysis_run_id=run_id,
                            expected_version=run_version,
                            chat_id=callback.message.chat.id,
                            message=callback.message,
                            allow_repeat_delivery=reused_transcript,
                        )
                        status = delivery.status
                        delivered = delivery.delivered
                        result_message_id = delivery.result_message_id
                        if reused_transcript and delivered:
                            answer_text = "Транскрипт отправлен файлом"
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="main",
                    focused_run_id=None if delivered else run_id,
                )
                task_surface = None
                if terminal_status is None and run_id:
                    task_surface = self._persist_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run=_run_for_id(status, run_id) or {"analysis_run_id": run_id},
                        state=self.page_states.get(key, _PageState()),
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                    )
                    self._schedule_run_status_tracking(
                        key=key,
                        channel_identity=channel_identity,
                        analysis_run_id=run_id,
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                        surface=task_surface,
                    )
                elif terminal_status is not None and run_id:
                    self._try_finish_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run_id=run_id,
                        surface=task_surface,
                    )
                if result_message_id is not None:
                    await self._reanchor_current_status_after_result(
                        key=key,
                        channel_identity=channel_identity,
                        chat_id=callback.message.chat.id,
                        previous_message_id=callback.message.message_id,
                        status=status,
                        prefix=prefix,
                    )
                else:
                    await self._edit_callback_status(callback, status, prefix=prefix)
                await callback.answer(answer_text)
                return
            if action == "ar":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                if page_state is None or page_state.focused_run_id != analysis_run_id:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                result_delivery = await self._deliver_run_result(
                    channel_identity=channel_identity,
                    analysis_run_id=analysis_run_id,
                    expected_version=expected_version,
                    message=callback.message,
                )
                cursor = page_state.current_cursor if page_state else None
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=cursor)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=cursor,
                    previous_cursors=page_state.previous_cursors,
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=page_state.focused_run_id,
                )
                if result_delivery.message_id is not None:
                    self._try_finish_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run_id=analysis_run_id,
                    )
                    await self._reanchor_current_status_after_result(
                        key=key,
                        channel_identity=channel_identity,
                        chat_id=callback.message.chat.id,
                        previous_message_id=callback.message.message_id,
                        status=status,
                    )
                else:
                    await self._edit_callback_status(callback, status)
                await callback.answer(result_delivery.notice, show_alert=result_delivery.show_alert)
                return
            if action == "cn":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                if page_state is None:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                if page_state.focused_run_id and page_state.focused_run_id != analysis_run_id:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                if not page_state.focused_run_id:
                    current_status = self.gateway.restore_status(channel_identity=channel_identity, cursor=page_state.current_cursor)
                    if _active_run_for_focus(current_status, analysis_run_id) is None:
                        await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                        return
                status = self.gateway.cancel_analysis_run(
                    channel_identity=channel_identity,
                    analysis_run_id=analysis_run_id,
                    expected_version=expected_version,
                    message="Canceled from Telegram inline button",
                )
                self._cancel_run_status_tracking(key)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor,
                    previous_cursors=page_state.previous_cursors,
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=analysis_run_id,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Обработка отменена")
                return
            if action == "dg":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                if page_state is None or page_state.focused_run_id != analysis_run_id:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                diagnostics = self.gateway.list_run_diagnostics(
                    channel_identity=channel_identity,
                    analysis_run_id=analysis_run_id,
                    expected_version=expected_version,
                )
                status = self.gateway.restore_status(channel_identity=channel_identity, cursor=page_state.current_cursor if page_state else None)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor if page_state else None,
                    previous_cursors=page_state.previous_cursors if page_state else [],
                    selection=page_state.selection if page_state else None,
                    screen=page_state.screen if page_state else "main",
                    focused_run_id=page_state.focused_run_id if page_state else None,
                )
                await self._edit_callback_status(
                    callback,
                    status,
                    prefix=_detail_prefix(
                        title="Диагностика",
                        lines=[f"- {_diagnostic_label(diagnostic)}" for diagnostic in diagnostics] or ["- Диагностики пока нет."],
                    ),
                )
                await callback.answer("Открыта диагностика")
                return
        except Exception as exc:
            normalized = _normalize_callback_error(exc)
            _log_handler_exception("callback_action", exc, normalized=normalized, callback=callback)
            await self._answer_callback_error(callback, normalized)
            return
        await self._answer_callback_error(callback, TelegramUserErrorCode.UNKNOWN_ACTION)

    async def _send_or_edit_status(
        self,
        message: Message,
        *,
        rejected: list[IngressRecord] | None = None,
        prefer_edit: bool = True,
        post_ingest_records: list[IngressRecord] | None = None,
        fresh_for_inbound_burst: bool = False,
        _lock_scope: bool = True,
    ) -> bool:
        key = self._scope_from_message(message).state_key
        if _lock_scope:
            lock = self.status_update_locks.setdefault(key, asyncio.Lock())
            async with lock:
                return await self._send_or_edit_status(
                    message,
                    rejected=rejected,
                    prefer_edit=prefer_edit,
                    post_ingest_records=post_ingest_records,
                    fresh_for_inbound_burst=fresh_for_inbound_burst,
                    _lock_scope=False,
                )
        if fresh_for_inbound_burst:
            now = self._monotonic()
            if now >= self.inbound_status_burst_until.get(key, 0.0):
                prefer_edit = False
            self.inbound_status_burst_until[key] = now + self.inbound_status_burst_window_seconds
        channel_identity = self._channel_identity_from_message(message)
        try:
            status = self.gateway.restore_status(channel_identity=channel_identity, rejected=rejected)
        except Exception as exc:
            normalized = _normalize_message_error(exc)
            _log_handler_exception("status_refresh", exc, normalized=normalized, message=message)
            if _has_accepted_ingress(post_ingest_records):
                await self._answer_post_ingest_refresh_failure(message, post_ingest_records or [])
                return False
            await self._answer_message_error(message, normalized)
            return False
        text = render_status_text(status)
        self._set_page_state(
            key,
            status,
            current_cursor=None,
            previous_cursors=[],
            selection=None,
            screen="main",
            focused_run_id=None,
        )
        state = self.page_states.get(key, _PageState())
        markup = build_status_keyboard(
            status,
            can_go_back=False,
            current_cursor=None,
            selection=None,
            screen="main",
            focused_run_id=None,
        )
        current_surface = self._find_current_materials_surface_or_none(channel_identity=channel_identity, scope="status_surface_lookup")
        previous_message_id = self.status_message_ids.get(key) or _surface_message_id(current_surface)
        if prefer_edit and previous_message_id is not None:
            try:
                await self.bot.edit_message_text(
                    text,
                    chat_id=message.chat.id,
                    message_id=previous_message_id,
                    reply_markup=markup,
                    link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
                )
                self.status_message_ids[key] = previous_message_id
                self._try_persist_current_materials_surface(
                    channel_identity=channel_identity,
                    status=status,
                    state=state,
                    chat_id=message.chat.id,
                    message_id=previous_message_id,
                    surface=current_surface,
                )
                return True
            except TelegramBadRequest as error:
                if "message is not modified" in str(error).lower():
                    self.status_message_ids[key] = previous_message_id
                    self._try_persist_current_materials_surface(
                        channel_identity=channel_identity,
                        status=status,
                        state=state,
                        chat_id=message.chat.id,
                        message_id=previous_message_id,
                        surface=current_surface,
                    )
                    return True
                if current_surface is not None:
                    self._try_supersede_channel_surface(
                        surface=current_surface,
                        reason="message_not_editable",
                        actor_id="telegram_adapter",
                        metadata={"chat_id": message.chat.id, "message_id": previous_message_id},
                    )
                self.status_message_ids.pop(key, None)
            except Exception:
                if current_surface is not None:
                    self._try_supersede_channel_surface(
                        surface=current_surface,
                        reason="message_not_editable",
                        actor_id="telegram_adapter",
                        metadata={"chat_id": message.chat.id, "message_id": previous_message_id},
                    )
                self.status_message_ids.pop(key, None)
        sent = await message.answer(
            text,
            reply_markup=markup,
            link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
        )
        self.status_message_ids[key] = sent.message_id
        self._try_persist_current_materials_surface(
            channel_identity=channel_identity,
            status=status,
            state=state,
            chat_id=message.chat.id,
            message_id=sent.message_id,
            surface=None,
        )
        return True

    async def _deliver_run_result(
        self,
        *,
        channel_identity: JsonObject,
        analysis_run_id: str,
        expected_version: int,
        message: Message | None = None,
        chat_id: int | None = None,
        failure_surface: JsonObject | None = None,
        raise_on_surface_failure: bool = False,
        allow_repeat_delivery: bool = False,
    ) -> _ResultDeliveryResult:
        if message is None and chat_id is None:
            return _ResultDeliveryResult("Готовый транскрипт пока недоступен.", True)
        artifacts = self.gateway.list_run_artifacts(
            channel_identity=channel_identity,
            analysis_run_id=analysis_run_id,
            expected_version=expected_version,
        )
        selected = _select_transcript_artifact(artifacts)
        if selected is None:
            return _ResultDeliveryResult("Готовый транскрипт пока недоступен.", True)
        existing_surface = self.gateway.find_result_artifact_surface(
            channel_identity=channel_identity,
            artifact_id=str(selected["artifact_id"]),
        )
        if existing_surface is not None and not allow_repeat_delivery:
            if _surface_address(existing_surface) is not None:
                return _ResultDeliveryResult("Транскрипт уже отправлен в чат.", True)
            self._try_supersede_channel_surface(
                surface=existing_surface,
                reason="result_surface_missing_telegram_address",
                actor_id="telegram_adapter",
                metadata={
                    "analysis_run_id": analysis_run_id,
                    "artifact_id": str(selected["artifact_id"]),
                },
            )
        access = self.gateway.api_client.get_internal_artifact_download_access(artifact_id=str(selected["artifact_id"]))
        download_url = _artifact_download_url(access)
        if not download_url:
            return _ResultDeliveryResult("Готовый транскрипт пока недоступен.", True)
        artifact = dict(selected)
        artifact["download"] = access.get("download")
        if access.get("mime_type"):
            artifact["content_type"] = access["mime_type"]
        if access.get("filename"):
            artifact["filename"] = access["filename"]
        content = self._download_artifact_bytes(download_url)
        document = BufferedInputFile(content, filename=_artifact_filename(artifact))
        try:
            if message is not None:
                sent = await message.answer_document(document)
                target_chat_id = message.chat.id
            else:
                sent = await self.bot.send_document(chat_id=chat_id, document=document)
                target_chat_id = chat_id
        except TelegramAPIError as error:
            classification = self._handle_telegram_surface_error(
                surface=failure_surface,
                error=error,
                operation="send_document",
                scope="result_delivery",
                chat_id=message.chat.id if message is not None else chat_id,
            )
            if raise_on_surface_failure and classification.lifecycle_reason is not None:
                raise _TelegramSurfaceDeliveryFailure(classification.classification) from error
            return _ResultDeliveryResult("Готовый транскрипт пока недоступен.", True)
        self._persist_result_artifact_surface(
            channel_identity=channel_identity,
            artifact=artifact,
            chat_id=target_chat_id,
            message_id=sent.message_id,
            delivery_mode="document",
        )
        return _ResultDeliveryResult("Транскрипт отправлен файлом", False, sent.message_id)

    def _download_artifact_bytes(self, download_url: str) -> bytes:
        with urlopen(download_url, timeout=30) as response:
            content = response.read()
        if not content:
            raise RuntimeError("artifact_download_failed")
        return content

    async def _start_analysis_from_collection(
        self,
        *,
        channel_identity: JsonObject,
        collection_id: str,
        expected_version: int,
    ) -> tuple[InboxStatus, str, str, str | None, str | None, bool]:
        processing = self.gateway.start_collection_processing_run(
            channel_identity=channel_identity,
            collection_id=collection_id,
            expected_version=expected_version,
        )
        run = processing["analysis_run"]
        status, prefix, answer_text, run_id, terminal_status = await self._resolve_run_start_status(
            channel_identity=channel_identity,
            run=run,
        )
        return status, prefix, answer_text, run_id, terminal_status, False

    async def _resolve_run_start_status(
        self,
        *,
        channel_identity: JsonObject,
        run: JsonObject,
    ) -> tuple[InboxStatus, str, str, str | None, str | None]:
        run_id = str(run.get("analysis_run_id") or "")
        for attempt in range(self.run_status_poll_attempts):
            latest = self.gateway.get_run_status(channel_identity=channel_identity, analysis_run_id=run_id)
            status_name = str(latest.get("status") or "")
            if status_name in TERMINAL_RUN_STATUSES:
                status = self.gateway.restore_status(channel_identity=channel_identity)
                return (
                    status,
                    f"Обработка: {_run_status_text(status_name)}\n\n",
                    f"Обработка: {_run_status_text(status_name)}",
                    run_id or None,
                    status_name,
                )
            if attempt + 1 < self.run_status_poll_attempts:
                await self._sleep(self.run_status_poll_delay_seconds)
        status = self.gateway.restore_status(channel_identity=channel_identity)
        return (
            status,
            "Обработка запущена.\n"
            "Карточка обновится автоматически.\n\n",
            "Обработка запущена",
            run_id or None,
            None,
        )

    async def _auto_deliver_and_maybe_clear_collection(
        self,
        *,
        channel_identity: JsonObject,
        analysis_run_id: str,
        expected_version: int,
        chat_id: int,
        message: Message | None = None,
        cursor: str | None = None,
        surface: JsonObject | None = None,
        raise_on_surface_failure: bool = False,
        allow_repeat_delivery: bool = False,
    ) -> _AutoDeliveryResult:
        status = self.gateway.restore_status(channel_identity=channel_identity, cursor=cursor)
        result_delivery = await self._deliver_run_result(
            channel_identity=channel_identity,
            analysis_run_id=analysis_run_id,
            expected_version=expected_version,
            message=message,
            chat_id=chat_id,
            failure_surface=surface,
            raise_on_surface_failure=raise_on_surface_failure,
            allow_repeat_delivery=allow_repeat_delivery,
        )
        delivered = (not result_delivery.show_alert) or result_delivery.notice == "Транскрипт уже отправлен в чат."
        return _AutoDeliveryResult(
            status=status,
            delivered=delivered,
            result_message_id=result_delivery.message_id,
        )

    def _schedule_run_status_tracking(
        self,
        *,
        key: tuple[int, int | None],
        channel_identity: JsonObject,
        analysis_run_id: str,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
    ) -> None:
        existing = self.run_watch_tasks.pop(key, None)
        if existing is not None:
            existing.cancel()
        self.run_watch_tasks[key] = asyncio.create_task(
            self._track_run_status_until_terminal(
                key=key,
                channel_identity=channel_identity,
                analysis_run_id=analysis_run_id,
                chat_id=chat_id,
                message_id=message_id,
                surface=surface,
            )
        )

    def _cancel_run_status_tracking(self, key: tuple[int, int | None]) -> None:
        existing = self.run_watch_tasks.pop(key, None)
        if existing is not None:
            existing.cancel()

    async def _track_run_status_until_terminal(
        self,
        *,
        key: tuple[int, int | None],
        channel_identity: JsonObject,
        analysis_run_id: str,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
        ) -> None:
        status_edit_retry_after_until = 0.0
        try:
            for _ in range(self.run_status_follow_attempts):
                await self._sleep(self.run_status_follow_delay_seconds)
                latest = self.gateway.get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)
                page_state = self.page_states.get(key, _PageState())
                current_cursor = page_state.current_cursor if page_state.screen == "materials" else None
                previous_cursors = page_state.previous_cursors if page_state.screen == "materials" else []
                latest_status = str(latest.get("status") or "")
                delivered = False
                result_message_id = None
                if latest_status in _AUTO_DELIVER_RUN_STATUSES:
                    delivery = await self._auto_deliver_and_maybe_clear_collection(
                        channel_identity=channel_identity,
                        analysis_run_id=analysis_run_id,
                        expected_version=int(latest.get("version") or 0),
                        chat_id=chat_id,
                        cursor=current_cursor,
                        surface=surface,
                        raise_on_surface_failure=True,
                    )
                    status = delivery.status
                    delivered = delivery.delivered
                    result_message_id = delivery.result_message_id
                else:
                    status = self.gateway.restore_status(channel_identity=channel_identity, cursor=current_cursor)
                focused_run_id = page_state.focused_run_id or analysis_run_id
                if delivered:
                    focused_run_id = None
                self._set_page_state(
                    key,
                    status,
                    current_cursor=current_cursor,
                    previous_cursors=previous_cursors,
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=focused_run_id,
                )
                updated_state = self.page_states.get(key, _PageState())
                if latest_status in TERMINAL_RUN_STATUSES and result_message_id is not None:
                    await self._reanchor_current_status_after_result(
                        key=key,
                        channel_identity=channel_identity,
                        chat_id=chat_id,
                        previous_message_id=message_id,
                        status=status,
                    )
                    self._try_finish_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run_id=analysis_run_id,
                        surface=surface,
                    )
                    return
                if self._monotonic() >= status_edit_retry_after_until:
                    try:
                        await self._edit_status_message_via_bot(
                            chat_id=chat_id,
                            message_id=message_id,
                            status=status,
                            state=updated_state,
                        )
                    except TelegramRetryAfter as exc:
                        status_edit_retry_after_until = self._monotonic() + max(float(exc.retry_after), 0.0)
                        _LOGGER.warning(
                            "run status edit rate-limited for %s; continuing watcher retry_after=%s",
                            analysis_run_id,
                            exc.retry_after,
                        )
                    else:
                        if latest_status not in TERMINAL_RUN_STATUSES:
                            surface = self._persist_analysis_task_surface(
                                channel_identity=channel_identity,
                                analysis_run=latest,
                                state=updated_state,
                                chat_id=chat_id,
                                message_id=message_id,
                            )
                if latest_status in TERMINAL_RUN_STATUSES:
                    self._try_finish_analysis_task_surface(
                        channel_identity=channel_identity,
                        analysis_run_id=analysis_run_id,
                        surface=surface,
                    )
                    return
        except asyncio.CancelledError:
            raise
        except _TelegramSurfaceDeliveryFailure as exc:
            _LOGGER.warning("run status tracking stopped after surface delivery failure for %s: %s", analysis_run_id, exc)
        except Exception as exc:
            _LOGGER.warning("run status tracking failed for %s: %s", analysis_run_id, exc)
        finally:
            current = self.run_watch_tasks.get(key)
            if current is asyncio.current_task():
                self.run_watch_tasks.pop(key, None)

    def _schedule_export_status_tracking(
        self,
        *,
        channel_identity: JsonObject,
        export_job_id: str,
        chat_id: int,
        surface: JsonObject | None,
    ) -> None:
        existing = self.export_watch_tasks.pop(export_job_id, None)
        if existing is not None:
            existing.cancel()
        self.export_watch_tasks[export_job_id] = asyncio.create_task(
            self._track_export_status_until_terminal(
                channel_identity=channel_identity,
                export_job_id=export_job_id,
                chat_id=chat_id,
                surface=surface,
            )
        )

    async def _track_export_status_until_terminal(
        self,
        *,
        channel_identity: JsonObject,
        export_job_id: str,
        chat_id: int,
        surface: JsonObject | None,
    ) -> None:
        try:
            for _ in range(self.export_status_follow_attempts):
                job = self.gateway.get_export_job(channel_identity=channel_identity, export_job_id=export_job_id)
                job_status = str(job.get("status") or "")
                refreshed_surface = await self._refresh_export_task_status(
                    channel_identity=channel_identity,
                    export_job=job,
                    surface=surface,
                )
                if refreshed_surface is not None:
                    surface = refreshed_surface
                if job_status == "succeeded":
                    try:
                        await self._deliver_export_result(
                            channel_identity=channel_identity,
                            export_job_id=export_job_id,
                            chat_id=chat_id,
                        )
                    except Exception as exc:
                        _LOGGER.warning("export delivery attempt failed for %s: %s", export_job_id, exc)
                        await self._sleep(self.run_status_follow_delay_seconds)
                        continue
                    self._try_finish_export_task_surface(
                        channel_identity=channel_identity, export_job_id=export_job_id, surface=surface,
                    )
                    return
                if job_status in TERMINAL_EXPORT_STATUSES:
                    self._try_finish_export_task_surface(
                        channel_identity=channel_identity, export_job_id=export_job_id, surface=surface,
                    )
                    return
                await self._sleep(self.run_status_follow_delay_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOGGER.warning("export status tracking failed for %s: %s", export_job_id, exc)
        finally:
            current = self.export_watch_tasks.get(export_job_id)
            if current is asyncio.current_task():
                self.export_watch_tasks.pop(export_job_id, None)

    async def _refresh_export_task_status(
        self,
        *,
        channel_identity: JsonObject,
        export_job: JsonObject,
        surface: JsonObject | None,
    ) -> JsonObject | None:
        address = _surface_address(surface) if surface is not None else None
        if address is None:
            return surface
        try:
            await self.bot.edit_message_text(
                _render_export_task_text(export_job),
                chat_id=address[0],
                message_id=address[1],
                reply_markup=None,
                link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
            )
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise
        return self._persist_export_task_surface(
            channel_identity=channel_identity,
            export_job=export_job,
            chat_id=address[0],
            message_id=address[1],
        )

    async def _deliver_export_result(
        self,
        *,
        channel_identity: JsonObject,
        export_job_id: str,
        chat_id: int,
    ) -> None:
        lease_owner = f"telegram-adapter:{channel_identity['external_account_ref']}"
        claim = self.gateway.claim_export_delivery(
            channel_identity=channel_identity, export_job_id=export_job_id, lease_owner=lease_owner,
        )
        try:
            download = self.gateway.get_internal_export_download(
                channel_identity=channel_identity, export_job_id=export_job_id,
            )
            size_bytes = int(download.get("size_bytes") or 0)
            if size_bytes <= 0 or size_bytes > _MAX_EXPORT_DELIVERY_BYTES:
                raise RuntimeError("export_delivery_size_invalid")
            content = await asyncio.to_thread(
                self._download_artifact_file, str(download["url"]), size_bytes,
            )
            try:
                await self.bot.send_document(
                    chat_id=chat_id,
                    document=_TemporaryInputFile(content, filename=str(download["filename"])),
                    caption="Экспорт готов",
                )
            finally:
                content.close()
        except Exception as exc:
            try:
                self.gateway.fail_export_delivery(
                    channel_identity=channel_identity,
                    export_job_id=export_job_id,
                    claim=claim,
                    failure_code="telegram_delivery_failed",
                )
            except Exception as fail_exc:
                _LOGGER.warning("export delivery failure could not be recorded for %s: %s", export_job_id, fail_exc)
            raise exc
        self.gateway.acknowledge_export_delivery(
            channel_identity=channel_identity, export_job_id=export_job_id, claim=claim,
        )

    def _download_artifact_file(self, download_url: str, expected_size: int) -> BinaryIO:
        destination = tempfile.TemporaryFile(mode="w+b")
        total = 0
        try:
            with urlopen(download_url, timeout=30) as response:
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > expected_size or total > _MAX_EXPORT_DELIVERY_BYTES:
                        raise RuntimeError("artifact_download_size_mismatch")
                    destination.write(chunk)
            if total != expected_size:
                raise RuntimeError("artifact_download_size_mismatch")
            destination.seek(0)
            return destination
        except Exception:
            destination.close()
            raise

    def _persist_export_task_surface(
        self, *, channel_identity: JsonObject, export_job: JsonObject, chat_id: int, message_id: int
    ) -> JsonObject:
        return self.gateway.upsert_export_task_surface(
            channel_identity=channel_identity,
            export_job=export_job,
            address=_telegram_surface_address(chat_id=chat_id, message_id=message_id),
            display_state={
                "export_job_id": export_job.get("export_job_id"),
                "export_status": export_job.get("status"),
            },
        )

    async def _anchor_export_task_surface(
        self,
        *,
        channel_identity: JsonObject,
        export_job: JsonObject,
        chat_id: int,
        force_new: bool = False,
    ) -> JsonObject:
        if not force_new:
            existing = self.gateway.find_export_task_surface(
                channel_identity=channel_identity,
                export_job_id=str(export_job["export_job_id"]),
            )
            if existing is not None and _surface_address(existing) is not None:
                return existing
        sent = await self.bot.send_message(
            chat_id=chat_id,
            text=_render_export_task_text(export_job),
            link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
        )
        return self._persist_export_task_surface(
            channel_identity=channel_identity,
            export_job=export_job,
            chat_id=chat_id,
            message_id=sent.message_id,
        )

    def _try_finish_export_task_surface(
        self, *, channel_identity: JsonObject, export_job_id: str, surface: JsonObject | None = None
    ) -> JsonObject | None:
        active_surface = surface or self.gateway.find_export_task_surface(
            channel_identity=channel_identity, export_job_id=export_job_id,
        )
        if active_surface is None or active_surface.get("lifecycle_status") != "active":
            return None
        return self._try_supersede_channel_surface(
            surface=active_surface,
            reason="export_job_terminal",
            actor_id="telegram_adapter",
            metadata={"export_job_id": export_job_id},
        )

    async def _edit_callback_status(
        self,
        callback: CallbackQuery,
        status: InboxStatus,
        *,
        prefix: str = "",
    ) -> None:
        if callback.message is None:
            return
        key = self._state_key_from_callback(callback)
        state = self.page_states.get(key, _PageState())
        channel_identity = self._channel_identity_from_callback(callback)
        try:
            await callback.message.edit_text(
                prefix + render_status_text(status, selection=state.selection, screen=state.screen),
                reply_markup=build_status_keyboard(
                    status,
                    can_go_back=bool(state.previous_cursors),
                    current_cursor=state.current_cursor,
                    selection=state.selection,
                    screen=state.screen,
                    focused_run_id=state.focused_run_id,
                    export_selection=self.export_selections.get(key),
                ),
                link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
            )
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise
        self.status_message_ids[key] = callback.message.message_id
        self._try_persist_current_materials_surface(
            channel_identity=channel_identity,
            status=status,
            state=state,
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
        )

    async def _edit_status_message_via_bot(
        self,
        *,
        chat_id: int,
        message_id: int,
        status: InboxStatus,
        state: _PageState | None = None,
    ) -> None:
        page_state = state or _PageState()
        try:
            await self.bot.edit_message_text(
                render_status_text(status, selection=page_state.selection, screen=page_state.screen),
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=build_status_keyboard(
                    status,
                    can_go_back=bool(page_state.previous_cursors),
                    current_cursor=page_state.current_cursor,
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=page_state.focused_run_id,
                    export_selection=None,
                ),
                link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
            )
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise

    async def _reanchor_current_status_after_result(
        self,
        *,
        key: tuple[int, int | None],
        channel_identity: JsonObject,
        chat_id: int,
        previous_message_id: int,
        status: InboxStatus,
        prefix: str = "",
    ) -> bool:
        state = self.page_states.get(key, _PageState())
        text = prefix + render_status_text(status, selection=state.selection, screen=state.screen)
        markup = build_status_keyboard(
            status,
            can_go_back=bool(state.previous_cursors),
            current_cursor=state.current_cursor,
            selection=state.selection,
            screen=state.screen,
            focused_run_id=state.focused_run_id,
            export_selection=self.export_selections.get(key),
        )
        lock = self.status_update_locks.setdefault(key, asyncio.Lock())
        async with lock:
            try:
                sent = await self.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=markup,
                    link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
                )
            except TelegramAPIError as error:
                _LOGGER.warning(
                    "%s scope=terminal_result_reanchor operation=send chat_id=%s error_type=%s error=%s",
                    _LOG_MARKER_TELEGRAM_SURFACE_FAILURE,
                    chat_id,
                    type(error).__name__,
                    error,
                )
                try:
                    await self.bot.edit_message_text(
                        text,
                        chat_id=chat_id,
                        message_id=previous_message_id,
                        reply_markup=markup,
                        link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
                    )
                except TelegramAPIError as edit_error:
                    _LOGGER.warning(
                        "%s scope=terminal_result_reanchor operation=fallback_edit chat_id=%s message_id=%s "
                        "error_type=%s error=%s",
                        _LOG_MARKER_TELEGRAM_SURFACE_FAILURE,
                        chat_id,
                        previous_message_id,
                        type(edit_error).__name__,
                        edit_error,
                    )
                    return False
                self.status_message_ids[key] = previous_message_id
                return False

            self.status_message_ids[key] = sent.message_id
            current_surface = self._find_current_materials_surface_or_none(
                channel_identity=channel_identity,
                scope="terminal_result_reanchor",
            )
            self._try_persist_current_materials_surface(
                channel_identity=channel_identity,
                status=status,
                state=state,
                chat_id=chat_id,
                message_id=sent.message_id,
                surface=current_surface,
            )
            if previous_message_id != sent.message_id:
                await self._retire_previous_status_message(
                    chat_id=chat_id,
                    message_id=previous_message_id,
                    fallback_text=text,
                )
            return True

    async def _retire_previous_status_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        fallback_text: str,
    ) -> None:
        try:
            await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
            return
        except TelegramAPIError as error:
            _LOGGER.warning(
                "%s scope=terminal_result_reanchor operation=delete_previous chat_id=%s message_id=%s "
                "error_type=%s error=%s",
                _LOG_MARKER_TELEGRAM_SURFACE_FAILURE,
                chat_id,
                message_id,
                type(error).__name__,
                error,
            )
        try:
            await self.bot.edit_message_text(
                fallback_text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=None,
                link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
            )
        except TelegramAPIError as error:
            _LOGGER.warning(
                "%s scope=terminal_result_reanchor operation=retire_fallback_edit chat_id=%s message_id=%s "
                "error_type=%s error=%s",
                _LOG_MARKER_TELEGRAM_SURFACE_FAILURE,
                chat_id,
                message_id,
                type(error).__name__,
                error,
            )

    async def _recover_active_channel_surfaces(self) -> None:
        for account in self.gateway.list_channel_accounts():
            if account.get("channel") != "telegram" or account.get("status") != "active":
                continue
            channel_identity = _channel_identity_from_channel_account(account)
            if channel_identity is None:
                continue
            surfaces = self.gateway.list_active_channel_surfaces(
                channel_account_id=str(account["channel_account_id"]),
                page_size=100,
            )
            for surface in surfaces:
                if surface.get("surface_type") == CURRENT_MATERIALS_PANEL:
                    try:
                        await self._recover_current_materials_surface(channel_identity=channel_identity, surface=surface)
                    except TelegramAPIError as error:
                        self._handle_telegram_surface_error(
                            surface=surface,
                            error=error,
                            operation="recover",
                            scope="current_materials_recovery",
                        )
            for surface in surfaces:
                if surface.get("surface_type") == ANALYSIS_TASK_SURFACE:
                    await self._recover_analysis_task_surface(channel_identity=channel_identity, surface=surface)
                if surface.get("surface_type") == EXPORT_TASK_SURFACE:
                    await self._recover_export_task_surface(channel_identity=channel_identity, surface=surface)

    async def _recover_export_task_surface(self, *, channel_identity: JsonObject, surface: JsonObject) -> None:
        export_job_id = _surface_subject_id(surface, subject_type="export_job", role="primary")
        address = _surface_address(surface)
        if not export_job_id or address is None:
            return
        job = self.gateway.get_export_job(channel_identity=channel_identity, export_job_id=export_job_id)
        status = str(job.get("status") or "")
        if status in TERMINAL_EXPORT_STATUSES and status != "succeeded":
            self._try_finish_export_task_surface(
                channel_identity=channel_identity, export_job_id=export_job_id, surface=surface,
            )
            return
        current_materials = self._find_current_materials_surface_or_none(
            channel_identity=channel_identity,
            scope="export_task_recovery",
        )
        if _surface_address(current_materials) == address:
            surface = await self._anchor_export_task_surface(
                channel_identity=channel_identity,
                export_job=job,
                chat_id=address[0],
                force_new=True,
            )
            address = _surface_address(surface)
            if address is None:
                return
        self._schedule_export_status_tracking(
            channel_identity=channel_identity,
            export_job_id=export_job_id,
            chat_id=address[0],
            surface=surface,
        )

    async def _recover_current_materials_surface(self, *, channel_identity: JsonObject, surface: JsonObject) -> None:
        address = _surface_address(surface)
        if address is None:
            return
        chat_id, message_id = address
        display_state = _surface_display_state(surface)
        state = _page_state_from_display_state(display_state)
        status = self.gateway.restore_status(channel_identity=channel_identity, cursor=state.current_cursor if state.screen == "materials" else None)
        key = _state_key_from_channel_identity(channel_identity)
        if key is None:
            return
        self._set_page_state(
            key,
            status,
            current_cursor=state.current_cursor,
            previous_cursors=state.previous_cursors,
            selection=None,
            screen=state.screen,
            focused_run_id=state.focused_run_id,
        )
        self.status_message_ids[key] = message_id
        recovered_state = self.page_states.get(key, state)
        try:
            await self._edit_status_message_via_bot(
                chat_id=chat_id,
                message_id=message_id,
                status=status,
                state=recovered_state,
            )
            self._persist_current_materials_surface(
                channel_identity=channel_identity,
                status=status,
                state=recovered_state,
                chat_id=chat_id,
                message_id=message_id,
                surface=surface,
            )
        except TelegramAPIError as error:
            classification = self._handle_telegram_surface_error(
                surface=surface,
                error=error,
                operation="edit",
                scope="current_materials_recovery",
                chat_id=chat_id,
                message_id=message_id,
            )
            if classification.lifecycle_reason != "telegram_message_unavailable":
                self.status_message_ids.pop(key, None)
                return
            try:
                sent = await self.bot.send_message(
                    chat_id=chat_id,
                    text=render_status_text(status, selection=recovered_state.selection, screen=recovered_state.screen),
                    reply_markup=build_status_keyboard(
                        status,
                        can_go_back=bool(recovered_state.previous_cursors),
                        current_cursor=recovered_state.current_cursor,
                        selection=recovered_state.selection,
                        screen=recovered_state.screen,
                        focused_run_id=recovered_state.focused_run_id,
                        export_selection=None,
                    ),
                    link_preview_options=_BUFFER_LINK_PREVIEW_OPTIONS,
                )
            except TelegramAPIError as send_error:
                self._handle_telegram_surface_error(
                    surface=None,
                    error=send_error,
                    operation="send",
                    scope="current_materials_recovery",
                    chat_id=chat_id,
                    message_id=message_id,
                    replacement_attempted=True,
                )
                self.status_message_ids.pop(key, None)
                return
            self.status_message_ids[key] = sent.message_id
            self._persist_current_materials_surface(
                channel_identity=channel_identity,
                status=status,
                state=recovered_state,
                chat_id=chat_id,
                message_id=sent.message_id,
                surface=None,
            )

    async def _recover_analysis_task_surface(self, *, channel_identity: JsonObject, surface: JsonObject) -> None:
        run_id = _surface_subject_id(surface, subject_type="analysis_run", role="primary")
        address = _surface_address(surface)
        key = _state_key_from_channel_identity(channel_identity)
        if not run_id or address is None or key is None:
            return
        latest = self.gateway.get_run_status(channel_identity=channel_identity, analysis_run_id=run_id)
        if str(latest.get("status") or "") not in ACTIVE_RUN_STATUSES:
            self._try_finish_analysis_task_surface(
                channel_identity=channel_identity,
                analysis_run_id=run_id,
                surface=surface,
            )
            chat_id, message_id = address
            current_message_id = self.status_message_ids.get(key)
            status = self.gateway.restore_status(channel_identity=channel_identity)
            if current_message_id is None:
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="main",
                    focused_run_id=None,
                )
                await self._reanchor_current_status_after_result(
                    key=key,
                    channel_identity=channel_identity,
                    chat_id=chat_id,
                    previous_message_id=message_id,
                    status=status,
                )
            elif current_message_id != message_id:
                await self._retire_previous_status_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    fallback_text=render_status_text(status),
                )
            return
        chat_id, message_id = address
        display_state = _surface_display_state(surface)
        state = _page_state_from_display_state(display_state, focused_run_id=run_id)
        status = self.gateway.restore_status(channel_identity=channel_identity, cursor=state.current_cursor if state.screen == "materials" else None)
        self._set_page_state(
            key,
            status,
            current_cursor=state.current_cursor,
            previous_cursors=state.previous_cursors,
            selection=None,
            screen=state.screen,
            focused_run_id=run_id,
        )
        self.status_message_ids[key] = message_id
        self._schedule_run_status_tracking(
            key=key,
            channel_identity=channel_identity,
            analysis_run_id=run_id,
            chat_id=chat_id,
            message_id=message_id,
            surface=surface,
        )

    def _persist_current_materials_surface(
        self,
        *,
        channel_identity: JsonObject,
        status: InboxStatus,
        state: _PageState,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
    ) -> JsonObject:
        display_state = _status_surface_display_state(status, state)
        address = _telegram_surface_address(chat_id=chat_id, message_id=message_id)
        existing = surface if surface is not None else self.gateway.find_current_materials_surface(channel_identity=channel_identity)
        if existing is not None and _surface_address_matches(existing, chat_id=chat_id, message_id=message_id):
            try:
                return self.gateway.replace_channel_surface_display_state(
                    surface=existing,
                    display_state=display_state,
                    actor_id="telegram_adapter",
                )
            except TelegramApiClientError as error:
                if error.status not in {404, 409}:
                    raise
        return self.gateway.upsert_current_materials_surface(
            channel_identity=channel_identity,
            address=address,
            display_state=display_state,
            collection=status.collection,
        )

    def _find_current_materials_surface_or_none(self, *, channel_identity: JsonObject, scope: str) -> JsonObject | None:
        try:
            return self.gateway.find_current_materials_surface(channel_identity=channel_identity)
        except TelegramApiClientError as error:
            _LOGGER.warning(
                "%s scope=%s channel_surface_lookup_failed api_status=%s api_code=%s",
                _LOG_MARKER_TELEGRAM_HANDLER_ERROR,
                scope,
                error.status,
                error.code,
            )
            return None

    def _try_persist_current_materials_surface(
        self,
        *,
        channel_identity: JsonObject,
        status: InboxStatus,
        state: _PageState,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
    ) -> JsonObject | None:
        try:
            return self._persist_current_materials_surface(
                channel_identity=channel_identity,
                status=status,
                state=state,
                chat_id=chat_id,
                message_id=message_id,
                surface=surface,
            )
        except TelegramApiClientError as error:
            _LOGGER.warning(
                "%s scope=current_materials_surface_persist api_status=%s api_code=%s chat_id=%s message_id=%s",
                _LOG_MARKER_TELEGRAM_HANDLER_ERROR,
                error.status,
                error.code,
                chat_id,
                message_id,
            )
            return None

    def _try_supersede_channel_surface(
        self,
        *,
        surface: JsonObject,
        reason: str,
        actor_id: str | None = None,
        metadata: JsonObject | None = None,
    ) -> JsonObject | None:
        try:
            return self.gateway.supersede_channel_surface(
                surface=surface,
                reason=reason,
                actor_id=actor_id,
                metadata=metadata,
            )
        except TelegramApiClientError as error:
            _LOGGER.warning(
                "%s scope=channel_surface_supersede api_status=%s api_code=%s surface_id=%s",
                _LOG_MARKER_TELEGRAM_HANDLER_ERROR,
                error.status,
                error.code,
                surface.get("channel_surface_id"),
            )
            return None

    def _try_finish_analysis_task_surface(
        self,
        *,
        channel_identity: JsonObject,
        analysis_run_id: str,
        surface: JsonObject | None = None,
    ) -> JsonObject | None:
        try:
            active_surface = surface or self.gateway.find_analysis_task_surface(
                channel_identity=channel_identity,
                analysis_run_id=analysis_run_id,
            )
        except TelegramApiClientError as error:
            _LOGGER.warning(
                "%s scope=analysis_task_surface_terminal_lookup api_status=%s api_code=%s analysis_run_id=%s",
                _LOG_MARKER_TELEGRAM_HANDLER_ERROR,
                error.status,
                error.code,
                analysis_run_id,
            )
            return None
        if active_surface is None or active_surface.get("lifecycle_status") != "active":
            return None
        return self._try_supersede_channel_surface(
            surface=active_surface,
            reason="analysis_run_terminal",
            actor_id="telegram_adapter",
            metadata={"analysis_run_id": analysis_run_id},
        )

    def _handle_telegram_surface_error(
        self,
        *,
        surface: JsonObject | None,
        error: TelegramAPIError,
        operation: str,
        scope: str,
        chat_id: int | None = None,
        message_id: int | None = None,
        replacement_attempted: bool = False,
    ) -> _TelegramSurfaceErrorClassification:
        classification = _classify_telegram_surface_error(error)
        if classification.fatal:
            raise error
        if chat_id is None or message_id is None:
            address = _surface_address(surface) if surface is not None else None
            if address is not None:
                chat_id = chat_id if chat_id is not None else address[0]
                message_id = message_id if message_id is not None else address[1]

        superseded = False
        metadata: JsonObject = {
            "scope": scope,
            "operation": operation,
            "classification": classification.classification,
            "telegram_error_type": type(error).__name__,
            "telegram_error": str(error),
            "replacement_attempted": replacement_attempted,
        }
        if chat_id is not None:
            metadata["chat_id"] = chat_id
        if message_id is not None:
            metadata["message_id"] = message_id

        if surface is not None and classification.lifecycle_reason is not None:
            superseded = self._try_supersede_channel_surface(
                surface=surface,
                reason=classification.lifecycle_reason,
                actor_id="telegram_adapter",
                metadata=metadata,
            ) is not None

        _LOGGER.warning(
            "%s scope=%s operation=%s classification=%s surface_id=%s surface_type=%s "
            "surface_key=%s chat_id=%s message_id=%s telegram_error_type=%s "
            "lifecycle_reason=%s superseded=%s replacement_attempted=%s",
            _LOG_MARKER_TELEGRAM_SURFACE_FAILURE,
            scope,
            operation,
            classification.classification,
            surface.get("channel_surface_id") if surface is not None else None,
            surface.get("surface_type") if surface is not None else None,
            surface.get("surface_key") if surface is not None else None,
            chat_id,
            message_id,
            type(error).__name__,
            classification.lifecycle_reason,
            superseded,
            replacement_attempted,
        )
        return classification

    def _persist_analysis_task_surface(
        self,
        *,
        channel_identity: JsonObject,
        analysis_run: JsonObject,
        state: _PageState,
        chat_id: int,
        message_id: int,
    ) -> JsonObject:
        display_state = _run_surface_display_state(analysis_run, state)
        return self.gateway.upsert_analysis_task_surface(
            channel_identity=channel_identity,
            analysis_run=analysis_run,
            address=_telegram_surface_address(chat_id=chat_id, message_id=message_id),
            display_state=display_state,
        )

    def _persist_result_artifact_surface(
        self,
        *,
        channel_identity: JsonObject,
        artifact: JsonObject,
        chat_id: int | None,
        message_id: int,
        delivery_mode: str,
    ) -> JsonObject:
        if chat_id is None:
            raise RuntimeError("telegram_result_chat_missing")
        return self.gateway.upsert_result_artifact_surface(
            channel_identity=channel_identity,
            artifact=artifact,
            address=_telegram_surface_address(chat_id=chat_id, message_id=message_id),
            display_state={
                "analysis_run_id": artifact.get("analysis_run_id"),
                "artifact_id": artifact.get("artifact_id"),
                "delivery_mode": delivery_mode,
                "kind": artifact.get("kind"),
            },
        )

    def _state_key_from_callback(self, callback: CallbackQuery) -> tuple[int, int | None]:
        return self._scope_from_callback(callback).state_key

    def _set_page_state(
        self,
        key: tuple[int, int | None],
        status: InboxStatus,
        *,
        current_cursor: str | None,
        previous_cursors: list[str | None],
        selection: JsonObject | None,
        screen: str = "main",
        focused_run_id: str | None = None,
    ) -> None:
        self.page_states[key] = _PageState(
            current_cursor=current_cursor,
            previous_cursors=list(previous_cursors),
            next_cursor=status.page.get("next_cursor") or None,
            selection=selection,
            screen=screen,
            focused_run_id=focused_run_id,
        )

    def _channel_identity_from_message(self, message: Message) -> JsonObject:
        return self._scope_from_message(message).channel_identity

    def _channel_identity_from_callback(self, callback: CallbackQuery) -> JsonObject:
        return self._scope_from_callback(callback).channel_identity

    def _scope_from_message(self, message: Message) -> TelegramChatScope:
        return self.gateway.scope_for(
            chat_id=message.chat.id,
            user_id=message.from_user.id if message.from_user else None,
            chat_type=_chat_type(message.chat),
            message_thread_id=getattr(message, "message_thread_id", None),
        )

    def _scope_from_callback(self, callback: CallbackQuery) -> TelegramChatScope:
        chat_id = callback.message.chat.id if callback.message else callback.from_user.id
        return self.gateway.scope_for(
            chat_id=chat_id,
            user_id=callback.from_user.id if callback.from_user else None,
            chat_type=_chat_type(callback.message.chat) if callback.message else "private",
            message_thread_id=getattr(callback.message, "message_thread_id", None) if callback.message else None,
        )

    async def _ensure_message_allowed(self, message: Message) -> bool:
        user_id = message.from_user.id if message.from_user else None
        if user_id is not None and self.settings.allowed_user_ids and user_id not in self.settings.allowed_user_ids:
            await message.answer(self.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=DEFAULT_LOCALE))
            return False
        try:
            self._scope_from_message(message)
        except TelegramUserError as exc:
            await self._answer_message_error(message, exc)
            return False
        return True

    async def _ensure_callback_allowed(self, callback: CallbackQuery) -> bool:
        user_id = callback.from_user.id if callback.from_user else None
        if user_id is not None and self.settings.allowed_user_ids and user_id not in self.settings.allowed_user_ids:
            await callback.answer(self.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=DEFAULT_LOCALE), show_alert=True)
            return False
        try:
            self._scope_from_callback(callback)
        except TelegramUserError as exc:
            await self._answer_callback_error(callback, exc)
            return False
        return True

    async def _answer_message_error(self, message: Message, error: BaseException | TelegramUserErrorCode) -> None:
        await message.answer(user_error_text(error))

    async def _answer_post_ingest_refresh_failure(self, message: Message, records: list[IngressRecord]) -> None:
        accepted = [record for record in records if record.status == "accepted"]
        rejected = [record for record in records if record.status == "rejected"]
        if len(accepted) == 1:
            lines = ["Материал сохранён в inbox на сервере."]
        else:
            lines = [f"Материалы сохранены в inbox на сервере: {len(accepted)}."]
        lines.append("Карточку не удалось обновить. Откройте /inbox через минуту.")
        for record in rejected:
            lines.append(f"Отклонено: {record.label} ({rejected_reason_text(record.reason)})")
        await message.answer("\n".join(lines))

    async def _answer_callback_error(self, callback: CallbackQuery, error: BaseException | TelegramUserErrorCode) -> None:
        await callback.answer(**safe_callback_answer(error))


def _has_accepted_ingress(records: list[IngressRecord] | None) -> bool:
    return any(record.status == "accepted" for record in records or [])


def _pending_file_ingest_status_text(file_inputs: list[TelegramFileInput]) -> str:
    lines = ["Обработка", f"Материалов: {len(file_inputs)}"]
    lines.extend(render_material_summary_lines([_pending_file_status_item(file_input) for file_input in file_inputs]))
    lines.append("")
    lines.append(f"Статус: получаем {_pending_file_kind_label(file_inputs)} из Telegram")
    return "\n".join(lines)


def _pending_file_status_item(file_input: TelegramFileInput) -> JsonObject:
    item: JsonObject = {
        "kind": file_input.kind,
        "display_name": file_input.file_name or _kind_text(file_input.kind),
        "metadata": {},
    }
    metadata = item["metadata"]
    if file_input.size_bytes is not None:
        metadata["size_bytes"] = file_input.size_bytes
    if file_input.duration_seconds is not None:
        metadata["duration_seconds"] = file_input.duration_seconds
    return item


def _pending_file_kind_label(file_inputs: list[TelegramFileInput]) -> str:
    kinds = {file_input.kind for file_input in file_inputs}
    if len(file_inputs) != 1 or len(kinds) != 1:
        return "материалы"
    kind = next(iter(kinds))
    if kind in {"video", "video_note"}:
        return "видео"
    if kind in {"voice", "audio"}:
        return "аудио"
    if kind == "photo":
        return "фото"
    if kind == "document":
        return "документ"
    return "материал"


def render_status_text(
    status: InboxStatus,
    *,
    selection: JsonObject | None = None,
    screen: str = "main",
) -> str:
    del selection
    lines = ["Обработка" if screen == "main" else "Материалы"]
    lines.append(f"Материалов: {_material_count(status)}")

    if screen == "materials":
        if status.items:
            lines.extend(_materials_screen_lines(status))
        else:
            lines.append("Список пока пуст.")
    elif status.items:
        lines.extend(_main_card_material_lines(status))
    else:
        lines.append("Материалов пока нет. Отправь текст, ссылку, фото, видео или документ.")

    for record in status.rejected:
        lines.append(f"Отклонено: {record.label} ({rejected_reason_text(record.reason)})")
    active_runs_count = len(status.active_runs)
    active_run = _latest_active_run(status)
    if active_run is not None:
        lines.append("")
        if active_runs_count == 1:
            lines.append(f"Активная задача: {_run_status_text(str(active_run.get('status') or 'unknown'))}")
        else:
            lines.append(
                f"Активные задачи: {active_runs_count}; последняя: {_run_status_text(str(active_run.get('status') or 'unknown'))}"
            )
        lines.extend(_active_run_progress_lines(active_run))
    active_export = _latest_active_export(status)
    if active_export is not None:
        lines.append("")
        lines.append(f"Экспорт: {_export_status_text(str(active_export.get('status') or 'unknown'))}")
        stage = _export_stage_text(active_export)
        if stage:
            lines.append(f"Этап: {stage}")
    return "\n".join(lines)


def _active_run_progress_lines(run: JsonObject) -> list[str]:
    lines: list[str] = []
    stage_text = _active_run_stage_text(run)
    if stage_text:
        lines.append(f"Этап: {stage_text}")
    elapsed_text = _active_run_elapsed_text(run)
    if elapsed_text:
        lines.append(f"Прошло: {elapsed_text}")
    return lines


def _active_run_stage_text(run: JsonObject) -> str:
    stage = _active_run_progress_stage(run)
    if stage == "queued":
        return "ожидает очереди"
    if stage == "materializing_sources":
        return "готовим материалы"
    if stage == "transcribing":
        return "транскрибируем аудио"
    if stage == "persisting_artifacts":
        return "сохраняем результат"
    if stage == "running_agent_harness":
        return "готовим отчет"
    if stage == "cancel_requested":
        return "отменяем"
    status = str(run.get("status") or "")
    if status == "queued":
        return "ожидает очереди"
    if status == "cancel_requested":
        return "отменяем"
    if status == "running":
        return "в работе"
    return ""


def _active_run_progress_stage(run: JsonObject) -> str:
    latest_event = run.get("latest_event")
    if isinstance(latest_event, dict):
        payload = latest_event.get("payload")
        if isinstance(payload, dict):
            stage = str(payload.get("progress_stage") or "").strip()
            if stage:
                return stage
    return str(run.get("status") or "").strip()


def _active_run_elapsed_text(run: JsonObject) -> str:
    started_at = _parse_run_datetime(run.get("started_at")) or _parse_run_datetime(run.get("created_at"))
    if started_at is None:
        return ""
    now = datetime.now(timezone.utc)
    elapsed_seconds = int((now - started_at).total_seconds())
    if elapsed_seconds < 0:
        return ""
    return _format_elapsed_seconds(elapsed_seconds)


def _parse_run_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_elapsed_seconds(total_seconds: int) -> str:
    seconds = max(total_seconds, 0)
    if seconds < 60:
        return f"{seconds} сек"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} мин {seconds:02d} сек"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} ч {minutes:02d} мин"


def build_status_keyboard(
    status: InboxStatus,
    *,
    can_go_back: bool = False,
    current_cursor: str | None = None,
    selection: JsonObject | None = None,
    screen: str = "main",
    focused_run_id: str | None = None,
    export_selection: JsonObject | None = None,
) -> InlineKeyboardMarkup:
    del selection
    rows: list[list[InlineKeyboardButton]] = []
    collection_id = str(status.collection.get("collection_id") or "") if status.collection else ""
    collection_version = int(status.collection.get("version") or 0) if status.collection else 0
    processing_button: InlineKeyboardButton | None = None
    focused_active_run = _active_run_for_focus(status, focused_run_id)
    if export_selection is not None:
        rows.extend(_export_selection_rows(export_selection))
        rows.append([InlineKeyboardButton(text="К карточке", callback_data=_callback_payload("mn"))])
        return InlineKeyboardMarkup(inline_keyboard=rows)
    if screen == "main":
        material_count = _material_count(status)
        if material_count and collection_id and focused_active_run is None:
            processing_button = InlineKeyboardButton(
                text=f"Обработать ({material_count})",
                callback_data=_callback_payload(
                    "rn",
                    _encode_callback_token(collection_id),
                    _encode_callback_version(collection_version),
                ),
            )
        rows.append([InlineKeyboardButton(text="Материалы", callback_data=_callback_payload("mt"))])
    else:
        item_rows = [
            _material_action_row(
                item=item,
                index=index,
                collection_id=collection_id,
                collection_version=collection_version,
            )
            for index, item in enumerate(status.items, start=1)
            if item.get("media_asset_id") and collection_id
        ]
        rows.extend(item_rows)
        nav_row: list[InlineKeyboardButton] = []
        if can_go_back:
            nav_row.append(InlineKeyboardButton(text="Назад", callback_data=_callback_payload("pp")))
        next_cursor = status.page.get("next_cursor")
        if next_cursor:
            nav_row.append(InlineKeyboardButton(text="Дальше", callback_data=_callback_payload("pn")))
        if nav_row:
            rows.append(nav_row)
        if status.items and collection_id:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="Убрать последнее",
                        callback_data=_callback_payload(
                            "rl", _encode_callback_token(collection_id), _encode_callback_version(collection_version),
                        ),
                    ),
                    InlineKeyboardButton(
                        text="Очистить видимое",
                        callback_data=_callback_payload(
                            "cl", _encode_callback_token(collection_id), _encode_callback_version(collection_version),
                            _encode_optional_callback_token(current_cursor),
                        ),
                    ),
                ]
            )
        rows.append([InlineKeyboardButton(text="К карточке", callback_data=_callback_payload("mn"))])
    if screen == "main":
        eligible = [item for item in status.items if _export_button_label(item) is not None]
        collection_items = (status.collection or {}).get("items") or []
        only_collection_item_id = str(collection_items[0].get("media_asset_id") or "") if len(collection_items) == 1 else ""
        if (
            len(eligible) == 1
            and len(status.items) == 1
            and len(collection_items) == 1
            and not status.page.get("has_more")
            and str(eligible[0].get("media_asset_id") or "") == only_collection_item_id
            and collection_id
        ):
            item = eligible[0]
            rows.append(_material_action_row(item=item, index=1, collection_id=collection_id, collection_version=collection_version, include_remove=False))
    if screen == "main":
        cancelable_active_run = focused_active_run or _latest_active_run(status)
        latest_result_run = _terminal_run_with_payload(status, status.artifacts_by_run, focused_run_id)
        latest_diagnostics_run = _terminal_run_with_payload(status, status.diagnostics_by_run, focused_run_id)
        if cancelable_active_run is not None and str(cancelable_active_run.get("status") or "") in CANCELABLE_RUN_STATUSES:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="Отмена",
                        callback_data=_callback_payload(
                            "cn",
                            _encode_callback_token(str(cancelable_active_run["analysis_run_id"])),
                            _encode_callback_version(int(cancelable_active_run.get("version") or 0)),
                        ),
                    )
                ]
            )
        result_row: list[InlineKeyboardButton] = []
        if latest_result_run is not None:
            run_id = str(latest_result_run["analysis_run_id"])
            result_row.append(
                InlineKeyboardButton(
                    text="Результат",
                    callback_data=_callback_payload(
                        "ar",
                        _encode_callback_token(run_id),
                        _encode_callback_version(int(latest_result_run.get("version") or 0)),
                    ),
                )
            )
        if latest_diagnostics_run is not None:
            run_id = str(latest_diagnostics_run["analysis_run_id"])
            result_row.append(
                InlineKeyboardButton(
                    text="Диагностика",
                    callback_data=_callback_payload(
                        "dg",
                        _encode_callback_token(run_id),
                        _encode_callback_version(int(latest_diagnostics_run.get("version") or 0)),
                    ),
                )
            )
        if result_row:
            rows.append(result_row)
        if processing_button is not None:
            rows.append([processing_button])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _material_count(status: InboxStatus) -> int:
    if status.collection and isinstance(status.collection.get("items"), list):
        return len(status.collection["items"])
    return len(status.items)


def _main_card_material_lines(status: InboxStatus) -> list[str]:
    lines = render_material_summary_lines(status.items)
    hidden_count = max(_material_count(status) - len(status.items), 0)
    if hidden_count and not any(line.startswith("+ ещё ") for line in lines):
        lines.append(f"+ ещё {hidden_count} материалов")
    return lines


def _is_youtube_export_item(item: JsonObject) -> bool:
    if str(item.get("status") or "") not in {"ready", "available"} or str(item.get("kind") or "") != "url":
        return False
    origin = item.get("origin")
    url = str(origin.get("origin_ref") or "") if isinstance(origin, dict) else ""
    host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


def _export_button_label(item: JsonObject) -> str | None:
    if _is_youtube_export_item(item):
        return "Скачать"
    if str(item.get("status") or "") in {"ready", "available"} and str(item.get("kind") or "") == "video":
        return "В аудио"
    return None


def _material_action_row(
    *, item: JsonObject, index: int, collection_id: str, collection_version: int, include_remove: bool = True
) -> list[InlineKeyboardButton]:
    media_asset_id = str(item["media_asset_id"])
    label = _export_button_label(item)
    row: list[InlineKeyboardButton] = []
    if label == "Скачать":
        row.append(InlineKeyboardButton(
            text="Скачать", callback_data=_callback_payload(
                "ex", _encode_callback_token(collection_id), _encode_callback_version(collection_version),
                _encode_callback_token(media_asset_id),
            ),
        ))
    elif label == "В аудио":
        row.append(InlineKeyboardButton(
            text="В аудио", callback_data=_callback_payload(
                "ea", _encode_callback_token(collection_id), _encode_callback_version(collection_version),
                _encode_callback_token(media_asset_id),
            ),
        ))
    if include_remove:
        row.append(InlineKeyboardButton(
            text="Убрать" if label else f"Убрать {index}",
            callback_data=_callback_payload(
                "rm", _encode_callback_token(collection_id), _encode_callback_version(collection_version),
                _encode_callback_token(media_asset_id),
            ),
        ))
    return row


def _export_selection_rows(selection: JsonObject) -> list[list[InlineKeyboardButton]]:
    if selection.get("mode") == "youtube":
        return [
            [InlineKeyboardButton(text="Аудио", callback_data=_callback_payload("ey"))],
            [InlineKeyboardButton(text="Видео", callback_data=_callback_payload("ev"))],
        ]
    collection_id = _encode_callback_token(str(selection["collection_id"]))
    version = _encode_callback_version(int(selection["expected_version"]))
    media_asset_id = _encode_callback_token(str(selection["media_asset_id"]))
    return [
        [
            InlineKeyboardButton(text="1080p", callback_data=_callback_payload("eq", collection_id, version, media_asset_id, _encode_callback_token("1080p"))),
            InlineKeyboardButton(text="720p", callback_data=_callback_payload("eq", collection_id, version, media_asset_id, _encode_callback_token("720p"))),
        ],
        [InlineKeyboardButton(text="480p", callback_data=_callback_payload("eq", collection_id, version, media_asset_id, _encode_callback_token("480p")))],
    ]


def _latest_active_export(status: InboxStatus) -> JsonObject | None:
    return status.active_exports[0] if status.active_exports else None


def _export_status_text(status: str) -> str:
    return {
        "queued": "в очереди", "claimed": "получен воркером", "running": "в работе", "cancel_requested": "отмена запрошена",
        "succeeded": "готов", "failed": "ошибка", "canceled": "отменён", "expired": "истёк",
    }.get(status, status)


def _export_stage_text(job: JsonObject) -> str:
    progress = job.get("progress")
    if not isinstance(progress, dict):
        return ""
    stage = str(progress.get("stage") or "")
    return {"queued": "ожидает очереди", "resolving": "получаем источник", "exporting": "готовим файл", "delivering": "отправляем в Telegram"}.get(stage, stage)


def _render_export_task_text(job: JsonObject) -> str:
    lines = ["Экспорт", f"Статус: {_export_status_text(str(job.get('status') or 'unknown'))}"]
    stage = _export_stage_text(job)
    if stage:
        lines.append(f"Этап: {stage}")
    return "\n".join(lines)


def _materials_screen_lines(status: InboxStatus) -> list[str]:
    return [
        f"{index}. {line}"
        for index, line in enumerate(render_material_summary_lines(status.items, limit=len(status.items) or 0), start=1)
    ]


def _callback_payload(action: str, *tokens: str) -> str:
    return ":".join((CALLBACK_NAMESPACE, action, *tokens))


def _parse_callback_payload(data: str) -> tuple[str, list[str]]:
    parts = data.split(":")
    if len(parts) < 2 or parts[0] != CALLBACK_NAMESPACE:
        raise TelegramUserError(TelegramUserErrorCode.STALE_ACTION)
    return parts[1], parts[2:]


def _encode_callback_token(value: str) -> str:
    try:
        return "u" + _urlsafe_b64encode(UUID(value).bytes)
    except (ValueError, AttributeError):
        return "b" + _urlsafe_b64encode(value.encode("utf-8"))


def _decode_callback_token(token: str) -> str:
    if token.startswith("u"):
        return str(UUID(bytes=_urlsafe_b64decode(token[1:])))
    if token.startswith("b"):
        return _urlsafe_b64decode(token[1:]).decode("utf-8")
    raise TelegramUserError(TelegramUserErrorCode.STALE_ACTION)


def _encode_optional_callback_token(value: str | None) -> str:
    if value is None:
        return "_"
    return _encode_callback_token(value)


def _decode_optional_callback_token(token: str) -> str | None:
    if token == "_":
        return None
    return _decode_callback_token(token)


def _encode_callback_version(value: int) -> str:
    if value < 0:
        raise TelegramUserError(TelegramUserErrorCode.STALE_ACTION)
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    encoded = ""
    remaining = value
    while remaining:
        remaining, index = divmod(remaining, 36)
        encoded = digits[index] + encoded
    return encoded


def _decode_callback_version(token: str) -> int:
    try:
        return int(token, 36)
    except ValueError as exc:
        raise TelegramUserError(TelegramUserErrorCode.STALE_ACTION) from exc


def _urlsafe_b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _urlsafe_b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _detail_prefix(*, title: str, lines: list[str]) -> str:
    return "\n".join([title, *lines, ""]) + "\n"


def _normalize_callback_error(error: Exception) -> BaseException | TelegramUserErrorCode:
    if isinstance(error, TelegramApiClientError) and error.status in {404, 409}:
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION, detail=error.code)
    if isinstance(error, (IndexError, KeyError, ValueError)):
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION)
    return error


def _normalize_message_error(error: Exception) -> BaseException | TelegramUserErrorCode:
    if isinstance(error, TelegramApiClientError) and error.status in {404, 409}:
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION, detail=error.code)
    if _is_telegram_file_too_big_error(error):
        return TelegramUserError(TelegramUserErrorCode.UNSUPPORTED_INPUT, detail="telegram_file_too_big")
    if isinstance(error, RuntimeError) and str(error) == "telegram_file_download_failed":
        return TelegramUserError(TelegramUserErrorCode.UNSUPPORTED_INPUT, detail="missing_file_content")
    if isinstance(error, RuntimeError) and str(error) in {"slot_not_visible", "slot_missing_media_asset_id", "inbox_empty"}:
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION, detail=str(error))
    return error


def _is_telegram_file_too_big_error(error: BaseException) -> bool:
    if isinstance(error, TelegramEntityTooLarge):
        return True
    return isinstance(error, TelegramBadRequest) and "file is too big" in str(error).lower()


def _log_handler_exception(
    scope: str,
    error: Exception,
    *,
    normalized: BaseException | TelegramUserErrorCode,
    message: Message | None = None,
    callback: CallbackQuery | None = None,
) -> None:
    normalized_error = normalized if isinstance(normalized, BaseException) else TelegramUserError(normalized)
    user_error = normalized_error if isinstance(normalized_error, TelegramUserError) else None
    api_error = error if isinstance(error, TelegramApiClientError) else None
    callback_data = callback.data[:80] if callback and callback.data else None
    _LOGGER.exception(
        "%s scope=%s normalized_code=%s detail=%s error_type=%s api_status=%s api_code=%s chat_id=%s user_id=%s message_id=%s callback_id=%s callback_data=%s",
        _LOG_MARKER_TELEGRAM_HANDLER_ERROR,
        scope,
        user_error.code if user_error else None,
        user_error.detail if user_error else None,
        type(error).__name__,
        api_error.status if api_error else None,
        api_error.code if api_error else None,
        getattr(getattr(message, "chat", None), "id", None)
        or getattr(getattr(getattr(callback, "message", None), "chat", None), "id", None),
        getattr(getattr(message, "from_user", None), "id", None)
        or getattr(getattr(callback, "from_user", None), "id", None),
        getattr(message, "message_id", None)
        or getattr(getattr(callback, "message", None), "message_id", None),
        getattr(callback, "id", None),
        callback_data,
    )


class _TelegramPollingMonitor(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if record.name != "aiogram.dispatcher":
            return
        message = record.getMessage()
        classification = _classify_polling_log_message(message)
        if classification is None:
            return
        level = logging.WARNING if classification == "telegram_upstream_failure" else logging.INFO
        _LOGGER.log(
            level,
            "%s classification=%s aiogram_level=%s message=%s",
            _LOG_MARKER_TELEGRAM_POLLING_STATE,
            classification,
            record.levelname,
            message,
        )


def _classify_polling_log_message(message: str) -> str | None:
    if "Failed to fetch updates -" in message and "TelegramNetworkError" in message:
        return "telegram_upstream_failure"
    if "Connection established" in message:
        return "telegram_upstream_recovered"
    return None


def _message_text(message: Message) -> str | None:
    value = message.text or message.caption
    if value and value.startswith("/"):
        return None
    return value


def _chat_type(chat: Any) -> str:
    value = getattr(chat, "type", "private")
    enum_value = getattr(value, "value", None)
    return str(enum_value or value or "private")


def _close_file_input(file_input: TelegramFileInput) -> None:
    if file_input.file_handle is not None:
        file_input.file_handle.close()
    if file_input.local_path is not None:
        file_input.local_path.unlink(missing_ok=True)


def _message_files(message: Message) -> Iterable[TelegramFileInput]:
    if message.photo:
        photo = message.photo[-1]
        yield TelegramFileInput(
            kind="photo",
            file_id=photo.file_id,
            file_unique_id=photo.file_unique_id,
            size_bytes=photo.file_size,
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )
    if message.video:
        yield TelegramFileInput(
            kind="video",
            file_id=message.video.file_id,
            file_unique_id=message.video.file_unique_id,
            file_name=message.video.file_name,
            content_type=message.video.mime_type,
            size_bytes=message.video.file_size,
            duration_seconds=_telegram_media_duration_seconds(message.video),
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )
    if message.video_note:
        yield TelegramFileInput(
            kind="video",
            file_id=message.video_note.file_id,
            file_unique_id=message.video_note.file_unique_id,
            file_name="telegram-video-note.mp4",
            content_type="video/mp4",
            size_bytes=message.video_note.file_size,
            duration_seconds=_telegram_media_duration_seconds(message.video_note),
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )
    if message.document:
        yield TelegramFileInput(
            kind="document",
            file_id=message.document.file_id,
            file_unique_id=message.document.file_unique_id,
            file_name=message.document.file_name,
            content_type=message.document.mime_type,
            size_bytes=message.document.file_size,
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )
    if message.audio:
        yield TelegramFileInput(
            kind="audio",
            file_id=message.audio.file_id,
            file_unique_id=message.audio.file_unique_id,
            file_name=message.audio.file_name,
            content_type=message.audio.mime_type,
            size_bytes=message.audio.file_size,
            duration_seconds=_telegram_media_duration_seconds(message.audio),
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )
    if message.voice:
        yield TelegramFileInput(
            kind="voice",
            file_id=message.voice.file_id,
            file_unique_id=message.voice.file_unique_id,
            content_type=message.voice.mime_type,
            size_bytes=message.voice.file_size,
            duration_seconds=_telegram_media_duration_seconds(message.voice),
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )


def _telegram_media_duration_seconds(media: Any) -> int | None:
    value = getattr(media, "duration", None)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if value >= 0 else None
    return None


def _item_label(item: JsonObject) -> str:
    display_name = _display_name_text(str(item.get("display_name") or item.get("media_asset_id") or "media"))
    kind = item.get("kind", "media")
    status = item.get("status", "unknown")
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    message_id = metadata.get("message_id")
    message_suffix = f", сообщение {message_id}" if message_id is not None else ""
    return f"{display_name} [{_kind_text(str(kind))}, {_media_status_text(str(status))}{message_suffix}]"


def _artifact_label(artifact: JsonObject) -> str:
    kind = str(artifact.get("kind") or "artifact")
    status = str(artifact.get("status") or "unknown")
    kind_text = _artifact_kind_text(kind).capitalize()
    status_text = _artifact_status_text(status)
    if status_text == "готов":
        return kind_text
    return f"{kind_text} · {status_text}"


def _diagnostic_label(diagnostic: JsonObject) -> str:
    message = str(diagnostic.get("message") or "").strip()
    severity = _diagnostic_severity_text(str(diagnostic.get("severity") or "info"))
    if message:
        return f"{severity}: {message}"
    return severity


def _visible_item_lines(items: list[JsonObject]) -> list[str]:
    lines: list[str] = []
    grouped_slots: dict[str, list[tuple[int, JsonObject]]] = {}
    emitted_groups: set[str] = set()
    for slot, item in enumerate(items, start=1):
        media_group_id = _media_group_id(item)
        if media_group_id:
            grouped_slots.setdefault(media_group_id, []).append((slot, item))

    for slot, item in enumerate(items, start=1):
        media_group_id = _media_group_id(item)
        if not media_group_id:
            lines.append(f"{slot}. {_item_label(item)}")
            continue
        if media_group_id in emitted_groups:
            continue
        emitted_groups.add(media_group_id)
        album_items = grouped_slots[media_group_id]
        lines.append(f"Альбом {media_group_id} ({len(album_items)} шт.)")
        for album_slot, album_item in album_items:
            lines.append(f"{album_slot}. {_item_label(album_item)}")
    return lines


def _media_group_id(item: JsonObject) -> str | None:
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        return None
    media_group_id = metadata.get("media_group_id")
    return str(media_group_id) if media_group_id else None


def _latest_active_run(status: InboxStatus) -> JsonObject | None:
    return status.active_runs[-1] if status.active_runs else None


def _active_run_for_focus(status: InboxStatus, focused_run_id: str | None) -> JsonObject | None:
    if not focused_run_id:
        return None
    return next(
        (
            run
            for run in status.active_runs
            if str(run.get("analysis_run_id") or "") == focused_run_id
        ),
        None,
    )


def _terminal_run_with_payload(
    status: InboxStatus,
    payloads_by_run: dict[str, list[JsonObject]],
    analysis_run_id: str | None,
) -> JsonObject | None:
    if not analysis_run_id:
        return None
    for run in reversed(status.recent_runs):
        if run.get("status") not in TERMINAL_RUN_STATUSES or not run.get("analysis_run_id"):
            continue
        run_id = str(run["analysis_run_id"])
        if run_id != analysis_run_id:
            continue
        if payloads_by_run.get(run_id):
            return run
    return None


def _analysis_run_version(status: InboxStatus, analysis_run_id: str) -> int | None:
    for run in reversed(status.recent_runs):
        if str(run.get("analysis_run_id") or "") != analysis_run_id:
            continue
        version = int(run.get("version") or 0)
        return version if version > 0 else None
    return None


def _run_for_id(status: InboxStatus, analysis_run_id: str) -> JsonObject | None:
    for run in reversed(status.recent_runs):
        if str(run.get("analysis_run_id") or "") == analysis_run_id:
            return run
    return None


def _reusable_transcript_run(reusable: JsonObject) -> JsonObject:
    run = reusable.get("analysis_run")
    if isinstance(run, dict):
        return run
    run_id = str(reusable.get("analysis_run_id") or "").strip()
    version = int(reusable.get("analysis_run_version") or 0)
    return {
        "analysis_run_id": run_id,
        "status": "succeeded",
        "version": version,
    }


def _status_surface_display_state(status: InboxStatus, state: _PageState) -> JsonObject:
    return {
        "screen": state.screen,
        "current_cursor": state.current_cursor,
        "previous_cursors": state.previous_cursors,
        "next_cursor": state.next_cursor,
        "focused_run_id": state.focused_run_id,
        "material_count": _material_count(status),
        "collection_id": status.collection.get("collection_id") if status.collection else None,
        "collection_version": status.collection.get("version") if status.collection else None,
        "active_run_ids": [
            str(run["analysis_run_id"])
            for run in status.active_runs
            if run.get("analysis_run_id")
        ],
    }


def _run_surface_display_state(analysis_run: JsonObject, state: _PageState) -> JsonObject:
    return {
        "analysis_run_id": analysis_run.get("analysis_run_id"),
        "run_status": analysis_run.get("status"),
        "run_version": analysis_run.get("version"),
        "screen": state.screen,
        "current_cursor": state.current_cursor,
        "focused_run_id": state.focused_run_id or analysis_run.get("analysis_run_id"),
    }


def _telegram_surface_address(*, chat_id: int, message_id: int) -> JsonObject:
    return {"chat_id": chat_id, "message_id": message_id}


def _surface_message_id(surface: JsonObject | None) -> int | None:
    if surface is None:
        return None
    address = _surface_address(surface)
    return address[1] if address is not None else None


def _surface_address(surface: JsonObject) -> tuple[int, int] | None:
    address = surface.get("address")
    if not isinstance(address, dict):
        return None
    chat_id = _parse_int(address.get("chat_id"))
    message_id = _parse_int(address.get("message_id"))
    if chat_id is None or message_id is None:
        return None
    return chat_id, message_id


def _surface_address_matches(surface: JsonObject, *, chat_id: int, message_id: int) -> bool:
    address = _surface_address(surface)
    return address == (chat_id, message_id)


def _classify_telegram_surface_error(error: TelegramAPIError) -> _TelegramSurfaceErrorClassification:
    message = str(error).lower()
    if isinstance(error, (TelegramUnauthorizedError, TelegramConflictError)):
        return _TelegramSurfaceErrorClassification("fatal_telegram_runtime_error", None, fatal=True)
    if isinstance(error, (TelegramForbiddenError, TelegramNotFound, TelegramMigrateToChat)):
        return _TelegramSurfaceErrorClassification("telegram_address_unreachable", "telegram_address_unreachable")
    if isinstance(error, TelegramBadRequest):
        if "message is not modified" in message:
            return _TelegramSurfaceErrorClassification("telegram_message_not_modified", None)
        if "chat not found" in message or "bot was blocked" in message or "user is deactivated" in message:
            return _TelegramSurfaceErrorClassification("telegram_address_unreachable", "telegram_address_unreachable")
        if (
            "message to edit not found" in message
            or "message can't be edited" in message
            or "message_id_invalid" in message
            or "message not found" in message
        ):
            return _TelegramSurfaceErrorClassification("telegram_message_unavailable", "telegram_message_unavailable")
        return _TelegramSurfaceErrorClassification("fatal_telegram_bad_request", None, fatal=True)
    if isinstance(error, (TelegramNetworkError, TelegramServerError, TelegramRetryAfter, TelegramEntityTooLarge)):
        return _TelegramSurfaceErrorClassification("transient_telegram_delivery_error", None)
    return _TelegramSurfaceErrorClassification("fatal_telegram_runtime_error", None, fatal=True)


def _surface_display_state(surface: JsonObject) -> JsonObject:
    display_state = surface.get("display_state")
    return display_state if isinstance(display_state, dict) else {}


def _page_state_from_display_state(display_state: JsonObject, *, focused_run_id: str | None = None) -> _PageState:
    previous = display_state.get("previous_cursors")
    return _PageState(
        current_cursor=_optional_str(display_state.get("current_cursor")),
        previous_cursors=[
            _optional_str(cursor)
            for cursor in previous
        ]
        if isinstance(previous, list)
        else [],
        next_cursor=_optional_str(display_state.get("next_cursor")),
        screen=str(display_state.get("screen") or "main"),
        focused_run_id=focused_run_id or _optional_str(display_state.get("focused_run_id")),
    )


def _channel_identity_from_channel_account(account: JsonObject) -> JsonObject | None:
    metadata = account.get("metadata")
    adapter_identity: JsonObject | None = None
    if isinstance(metadata, dict):
        channel_identity = metadata.get("channel_identity")
        if isinstance(channel_identity, dict) and channel_identity.get("channel") and channel_identity.get("external_account_ref"):
            return channel_identity
        if isinstance(metadata.get("adapter_identity"), dict):
            adapter_identity = metadata["adapter_identity"]
    external_account_ref = str(account.get("external_account_ref") or "").strip()
    if not external_account_ref:
        return None
    channel_identity = {"channel": "telegram", "external_account_ref": external_account_ref}
    if adapter_identity:
        channel_identity["adapter_identity"] = adapter_identity
    return channel_identity


def _state_key_from_channel_identity(channel_identity: JsonObject) -> tuple[int, int | None] | None:
    identity = channel_identity.get("adapter_identity")
    if not isinstance(identity, dict):
        return None
    chat_id = _parse_int(identity.get("telegram_chat_id"))
    user_id = _parse_int(identity.get("telegram_user_id"))
    if chat_id is None:
        return None
    return chat_id, user_id


def _surface_subject_id(surface: JsonObject, *, subject_type: str, role: str) -> str | None:
    subjects = surface.get("subjects")
    if not isinstance(subjects, list):
        return None
    for subject in subjects:
        if not isinstance(subject, dict):
            continue
        if subject.get("subject_type") == subject_type and subject.get("subject_role") == role:
            return _optional_str(subject.get("subject_id"))
    return None


def _parse_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _start_text() -> str:
    return "Отправь текст, ссылку, фото, видео, документ или голосовое. Всё сначала попадает во входящие."


def _help_text() -> str:
    return (
        "/inbox - показать текущее состояние входящих\n"
        "Кнопки помогают открыть список материалов, убрать лишнее, запустить обработку и открыть последний результат или диагностику."
    )


def _kind_text(kind: str) -> str:
    return {
        "text": "текст",
        "url": "ссылка",
        "photo": "фото",
        "image": "изображение",
        "video": "видео",
        "document": "документ",
        "audio": "аудио",
        "voice": "голосовое",
        "file": "файл",
        "media": "медиа",
    }.get(kind, kind)


def _media_status_text(status: str) -> str:
    return {
        "ready": "готов",
        "validating": "проверяется",
        "quarantined": "карантин",
        "deleted": "удалён",
        "unknown": "неизвестно",
    }.get(status, status)


def _run_status_text(status: str) -> str:
    return {
        "queued": "в очереди",
        "running": "в работе",
        "cancel_requested": "отмена запрошена",
        "partially_succeeded": "частично готов",
        "succeeded": "успешно",
        "failed": "ошибка",
        "canceled": "отменён",
        "expired": "истёк",
        "unknown": "неизвестно",
    }.get(status, status)


def _artifact_status_text(status: str) -> str:
    return {
        "available": "готов",
        "ready": "готов",
        "pending": "готовится",
        "failed": "ошибка",
        "expired": "истёк",
        "deleted": "удалён",
        "unknown": "неизвестно",
    }.get(status, status)


def _artifact_kind_text(kind: str) -> str:
    return {
        "transcript": "транскрипт",
        "summary": "сводка",
        "report": "отчёт",
        "run_manifest": "манифест запуска",
        "run_diagnostics": "диагностика запуска",
        "artifact": "файл",
    }.get(kind, kind)


def _diagnostic_severity_text(severity: str) -> str:
    return {
        "info": "Инфо",
        "warning": "Предупреждение",
        "error": "Ошибка",
        "critical": "Критично",
    }.get(severity, "Диагностика")


def _display_name_text(value: str) -> str:
    return {
        "Telegram photo": "Фото из Telegram",
        "Telegram image": "Изображение из Telegram",
        "Telegram video": "Видео из Telegram",
        "Telegram document": "Документ из Telegram",
        "Telegram audio": "Аудио из Telegram",
        "Telegram voice": "Голосовое из Telegram",
        "Telegram file": "Файл из Telegram",
        "Telegram media": "Медиа из Telegram",
    }.get(value, value)


def _select_transcript_artifact(artifacts: list[JsonObject]) -> JsonObject | None:
    candidates: list[tuple[int, int, JsonObject]] = []
    for index, artifact in enumerate(artifacts):
        if str(artifact.get("kind") or "") != "transcript":
            continue
        if str(artifact.get("status") or "") not in {"available", "ready"}:
            continue
        artifact_id = str(artifact.get("artifact_id") or "").strip()
        if not artifact_id:
            continue
        candidates.append((_transcript_artifact_rank(str(artifact.get("content_type") or "")), index, artifact))
    if not candidates:
        return None
    return min(candidates, key=lambda item: (item[0], item[1]))[2]


def _transcript_artifact_rank(content_type: str) -> int:
    normalized = content_type.split(";", 1)[0].strip().lower()
    if normalized == "text/plain":
        return 0
    if normalized == "text/markdown":
        return 1
    if normalized == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return 2
    if normalized.startswith("text/"):
        return 3
    return 4


def _artifact_download_url(artifact: JsonObject) -> str | None:
    download = artifact.get("download")
    if not isinstance(download, dict):
        return None
    url = str(download.get("url") or "").strip()
    return url or None


def _artifact_filename(artifact: JsonObject) -> str:
    filename = str(artifact.get("filename") or "").strip()
    if filename:
        return PurePosixPath(filename).name or filename
    object_key = str(artifact.get("object_key") or "").strip()
    if object_key:
        name = PurePosixPath(object_key).name
        if name:
            return name
    download_url = _artifact_download_url(artifact)
    if download_url:
        path = unquote(urlparse(download_url).path)
        name = PurePosixPath(path).name
        if name:
            return name
    content_type = str(artifact.get("content_type") or "").split(";", 1)[0].strip().lower()
    if content_type == "text/plain":
        return "transcript.txt"
    if content_type == "text/markdown":
        return "transcript.md"
    if content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return "transcript.docx"
    return "transcript.bin"
