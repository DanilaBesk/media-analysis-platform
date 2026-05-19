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
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import urlopen
from uuid import UUID

from aiogram import Bot, Dispatcher, Router
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
from aiogram.types import BufferedInputFile, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

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
    InboxStatus,
    IngressRecord,
    RESULT_ARTIFACT_SURFACE,
    TERMINAL_RUN_STATUSES,
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


CALLBACK_NAMESPACE = "ib"


@dataclass(frozen=True, slots=True)
class _TelegramSurfaceErrorClassification:
    classification: str
    lifecycle_reason: str | None
    fatal: bool = False


class _TelegramSurfaceDeliveryFailure(RuntimeError):
    pass


class TelegramInboxApp:
    def __init__(self, settings: Any, gateway: TelegramInboxGateway, bot: Bot | None = None) -> None:
        self.settings = settings
        self.gateway = gateway
        self.bot = bot or Bot(settings.telegram_bot_token)
        self.dispatcher = Dispatcher()
        self.router = Router(name="telegram-inbox")
        self.locale_service = TelegramLocaleService()
        self.status_message_ids: dict[tuple[int, int | None], int] = {}
        self.page_states: dict[tuple[int, int | None], _PageState] = {}
        self.run_status_poll_attempts = 3
        self.run_status_poll_delay_seconds = 0.2
        self.run_status_follow_attempts = 120
        self.run_status_follow_delay_seconds = 2.0
        self.run_watch_tasks: dict[tuple[int, int | None], asyncio.Task[None]] = {}
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
        owner = self._owner_from_message(message)
        try:
            files = await self._download_message_files(message)
            records = self.gateway.add_message_inputs(
                owner=owner,
                text=_message_text(message),
                files=files,
                message_id=message.message_id,
            )
        except Exception as exc:
            normalized = _normalize_message_error(exc)
            _log_handler_exception("message_ingest", exc, normalized=normalized, message=message)
            await self._answer_message_error(message, normalized)
            return
        await self._send_or_edit_status(
            message,
            rejected=[record for record in records if record.status == "rejected"],
            prefer_edit=False,
            post_ingest_records=records,
        )

    async def _download_message_files(self, message: Message) -> list[TelegramFileInput]:
        hydrated: list[TelegramFileInput] = []
        for file_input in _message_files(message):
            telegram_file = await self.bot.get_file(file_input.file_id)
            buffer = BytesIO()
            await self.bot.download_file(telegram_file.file_path, destination=buffer)
            content = buffer.getvalue()
            if not content:
                raise RuntimeError("telegram_file_download_failed")
            hydrated.append(replace(file_input, content=content))
        return hydrated

    async def _handle_status_callback(self, callback: CallbackQuery) -> None:
        if not await self._ensure_callback_allowed(callback):
            return
        if callback.message is None:
            await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
            return
        owner = self._owner_from_callback(callback)
        data = callback.data or ""
        key = self._state_key_from_callback(callback)
        page_state = self.page_states.get(key)
        try:
            action, tokens = _parse_callback_payload(data)
            if action == "rf":
                status = self.gateway.restore_status(owner=owner)
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
                status = self.gateway.restore_status(owner=owner)
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
                status = self.gateway.restore_status(owner=owner)
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
                status = self.gateway.restore_status(owner=owner, cursor=cursor)
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
                status = self.gateway.restore_status(owner=owner, cursor=cursor)
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
                    owner=owner,
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
            if action == "cl":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                current_cursor = _decode_optional_callback_token(tokens[2])
                status = self.gateway.clear_visible_items(
                    owner=owner,
                    collection_id=collection_id,
                    expected_version=expected_version,
                    cursor=current_cursor,
                )
                previous_cursors = page_state.previous_cursors if page_state else []
                if not status.items and current_cursor is not None and previous_cursors:
                    current_cursor = previous_cursors[-1]
                    previous_cursors = previous_cursors[:-1]
                    status = self.gateway.restore_status(owner=owner, cursor=current_cursor)
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
                    owner=owner,
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
                status, prefix, answer_text, run_id, terminal_status = await self._start_analysis_from_collection(
                    owner=owner,
                    collection_id=collection_id,
                    expected_version=expected_version,
                )
                delivered = False
                if terminal_status in _AUTO_DELIVER_RUN_STATUSES and run_id:
                    run_version = _analysis_run_version(status, run_id)
                    if run_version is not None:
                        delivery = await self._auto_deliver_and_maybe_clear_collection(
                            owner=owner,
                            analysis_run_id=run_id,
                            expected_version=run_version,
                            chat_id=callback.message.chat.id,
                            message=callback.message,
                        )
                        status = delivery.status
                        delivered = delivery.delivered
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="main",
                    focused_run_id=None if delivered else run_id,
                )
                await self._edit_callback_status(callback, status, prefix=prefix)
                task_surface = None
                if run_id:
                    task_surface = self._persist_analysis_task_surface(
                        owner=owner,
                        analysis_run=_run_for_id(status, run_id) or {"analysis_run_id": run_id},
                        state=self.page_states.get(key, _PageState()),
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                    )
                if terminal_status is None and run_id:
                    self._schedule_run_status_tracking(
                        key=key,
                        owner=owner,
                        analysis_run_id=run_id,
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                        surface=task_surface,
                    )
                await callback.answer(answer_text)
                return
            if action == "rn":
                if len(tokens) >= 2:
                    collection_id = _decode_callback_token(tokens[0])
                    expected_version = _decode_callback_version(tokens[1])
                    status, prefix, answer_text, run_id, terminal_status = await self._start_analysis_from_collection(
                        owner=owner,
                        collection_id=collection_id,
                        expected_version=expected_version,
                    )
                else:
                    selection_snapshot_id = _decode_callback_token(tokens[0])
                    run = self.gateway.start_analysis(owner=owner, selection_snapshot_id=selection_snapshot_id)
                    status, prefix, answer_text, run_id, terminal_status = await self._resolve_run_start_status(
                        owner=owner,
                        run=run,
                    )
                delivered = False
                if terminal_status in _AUTO_DELIVER_RUN_STATUSES and run_id:
                    run_version = _analysis_run_version(status, run_id)
                    if run_version is not None:
                        delivery = await self._auto_deliver_and_maybe_clear_collection(
                            owner=owner,
                            analysis_run_id=run_id,
                            expected_version=run_version,
                            chat_id=callback.message.chat.id,
                            message=callback.message,
                        )
                        status = delivery.status
                        delivered = delivery.delivered
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=None,
                    screen="main",
                    focused_run_id=None if delivered else run_id,
                )
                await self._edit_callback_status(callback, status, prefix=prefix)
                task_surface = None
                if run_id:
                    task_surface = self._persist_analysis_task_surface(
                        owner=owner,
                        analysis_run=_run_for_id(status, run_id) or {"analysis_run_id": run_id},
                        state=self.page_states.get(key, _PageState()),
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                    )
                if terminal_status is None and run_id:
                    self._schedule_run_status_tracking(
                        key=key,
                        owner=owner,
                        analysis_run_id=run_id,
                        chat_id=callback.message.chat.id,
                        message_id=callback.message.message_id,
                        surface=task_surface,
                    )
                await callback.answer(answer_text)
                return
            if action == "ar":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                if page_state is None or page_state.focused_run_id != analysis_run_id:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                result_notice, show_alert = await self._deliver_run_result(
                    owner=owner,
                    analysis_run_id=analysis_run_id,
                    expected_version=expected_version,
                    message=callback.message,
                )
                cursor = page_state.current_cursor if page_state else None
                status = self.gateway.restore_status(owner=owner, cursor=cursor)
                if not show_alert and status.collection is not None:
                    status = self.gateway.clear_collection(
                        owner=owner,
                        collection_id=str(status.collection["collection_id"]),
                        expected_version=int(status.collection["version"]),
                        cursor=cursor,
                    )
                self._set_page_state(
                    key,
                    status,
                    current_cursor=cursor,
                    previous_cursors=page_state.previous_cursors,
                    selection=page_state.selection,
                    screen=page_state.screen,
                    focused_run_id=page_state.focused_run_id,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer(result_notice, show_alert=show_alert)
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
                    current_status = self.gateway.restore_status(owner=owner, cursor=page_state.current_cursor)
                    if _active_run_for_focus(current_status, analysis_run_id) is None:
                        await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                        return
                status = self.gateway.cancel_analysis_run(
                    owner=owner,
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
                await callback.answer("Транскрибация отменена")
                return
            if action == "dg":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                if page_state is None or page_state.focused_run_id != analysis_run_id:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                diagnostics = self.gateway.list_run_diagnostics(
                    owner=owner,
                    analysis_run_id=analysis_run_id,
                    expected_version=expected_version,
                )
                status = self.gateway.restore_status(owner=owner, cursor=page_state.current_cursor if page_state else None)
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
    ) -> bool:
        owner = self._owner_from_message(message)
        try:
            status = self.gateway.restore_status(owner=owner, rejected=rejected)
        except Exception as exc:
            normalized = _normalize_message_error(exc)
            _log_handler_exception("status_refresh", exc, normalized=normalized, message=message)
            if _has_accepted_ingress(post_ingest_records):
                await self._answer_post_ingest_refresh_failure(message, post_ingest_records or [])
                return False
            await self._answer_message_error(message, normalized)
            return False
        key = self._scope_from_message(message).state_key
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
        current_surface = self._find_current_materials_surface_or_none(owner=owner, scope="status_surface_lookup")
        previous_message_id = self.status_message_ids.get(key) or _surface_message_id(current_surface)
        if prefer_edit and previous_message_id is not None:
            try:
                await self.bot.edit_message_text(
                    text,
                    chat_id=message.chat.id,
                    message_id=previous_message_id,
                    reply_markup=markup,
                )
                self.status_message_ids[key] = previous_message_id
                self._try_persist_current_materials_surface(
                    owner=owner,
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
                        owner=owner,
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
        sent = await message.answer(text, reply_markup=markup)
        self.status_message_ids[key] = sent.message_id
        self._try_persist_current_materials_surface(
            owner=owner,
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
        owner: JsonObject,
        analysis_run_id: str,
        expected_version: int,
        message: Message | None = None,
        chat_id: int | None = None,
        failure_surface: JsonObject | None = None,
        raise_on_surface_failure: bool = False,
    ) -> tuple[str, bool]:
        if message is None and chat_id is None:
            return ("Готовый транскрипт пока недоступен.", True)
        artifacts = self.gateway.list_run_artifacts(
            owner=owner,
            analysis_run_id=analysis_run_id,
            expected_version=expected_version,
        )
        selected = _select_transcript_artifact(artifacts)
        if selected is None:
            return ("Готовый транскрипт пока недоступен.", True)
        existing_surface = self.gateway.find_result_artifact_surface(
            owner=owner,
            artifact_id=str(selected["artifact_id"]),
        )
        if existing_surface is not None:
            if _surface_address(existing_surface) is not None:
                return ("Транскрипт уже отправлен в чат.", True)
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
            return ("Готовый транскрипт пока недоступен.", True)
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
            return ("Готовый транскрипт пока недоступен.", True)
        self._persist_result_artifact_surface(
            owner=owner,
            artifact=artifact,
            chat_id=target_chat_id,
            message_id=sent.message_id,
            delivery_mode="document",
        )
        return ("Транскрипт отправлен файлом", False)

    def _download_artifact_bytes(self, download_url: str) -> bytes:
        with urlopen(download_url, timeout=30) as response:
            content = response.read()
        if not content:
            raise RuntimeError("artifact_download_failed")
        return content

    async def _start_analysis_from_collection(
        self,
        *,
        owner: JsonObject,
        collection_id: str,
        expected_version: int,
    ) -> tuple[InboxStatus, str, str, str | None, str | None]:
        selection = self.gateway.create_selection_snapshot(
            owner=owner,
            collection_id=collection_id,
            expected_version=expected_version,
        )
        run = self.gateway.start_analysis(owner=owner, selection_snapshot_id=str(selection["selection_snapshot_id"]))
        return await self._resolve_run_start_status(owner=owner, run=run)

    async def _resolve_run_start_status(
        self,
        *,
        owner: JsonObject,
        run: JsonObject,
    ) -> tuple[InboxStatus, str, str, str | None, str | None]:
        run_id = str(run.get("analysis_run_id") or "")
        for attempt in range(self.run_status_poll_attempts):
            latest = self.gateway.get_run_status(owner=owner, analysis_run_id=run_id)
            status_name = str(latest.get("status") or "")
            if status_name in TERMINAL_RUN_STATUSES:
                status = self.gateway.restore_status(owner=owner)
                return (
                    status,
                    f"Транскрибация: {_run_status_text(status_name)}\n\n",
                    f"Транскрибация: {_run_status_text(status_name)}",
                    run_id or None,
                    status_name,
                )
            if attempt + 1 < self.run_status_poll_attempts:
                await self._sleep(self.run_status_poll_delay_seconds)
        status = self.gateway.restore_status(owner=owner)
        return (
            status,
            "Транскрибация запущена.\n"
            "Карточка обновится автоматически.\n\n",
            "Транскрибация запущена",
            run_id or None,
            None,
        )

    async def _auto_deliver_and_maybe_clear_collection(
        self,
        *,
        owner: JsonObject,
        analysis_run_id: str,
        expected_version: int,
        chat_id: int,
        message: Message | None = None,
        cursor: str | None = None,
        surface: JsonObject | None = None,
        raise_on_surface_failure: bool = False,
    ) -> _AutoDeliveryResult:
        status = self.gateway.restore_status(owner=owner, cursor=cursor)
        result_notice, show_alert = await self._deliver_run_result(
            owner=owner,
            analysis_run_id=analysis_run_id,
            expected_version=expected_version,
            message=message,
            chat_id=chat_id,
            failure_surface=surface,
            raise_on_surface_failure=raise_on_surface_failure,
        )
        delivered = (not show_alert) or result_notice == "Транскрипт уже отправлен в чат."
        if show_alert or status.collection is None:
            return _AutoDeliveryResult(status=status, delivered=delivered)
        return _AutoDeliveryResult(
            status=self.gateway.clear_collection(
                owner=owner,
                collection_id=str(status.collection["collection_id"]),
                expected_version=int(status.collection["version"]),
                cursor=cursor,
            ),
            delivered=delivered,
        )

    def _schedule_run_status_tracking(
        self,
        *,
        key: tuple[int, int | None],
        owner: JsonObject,
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
                owner=owner,
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
        owner: JsonObject,
        analysis_run_id: str,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
        ) -> None:
        try:
            for _ in range(self.run_status_follow_attempts):
                await self._sleep(self.run_status_follow_delay_seconds)
                latest = self.gateway.get_run_status(owner=owner, analysis_run_id=analysis_run_id)
                page_state = self.page_states.get(key, _PageState())
                current_cursor = page_state.current_cursor if page_state.screen == "materials" else None
                previous_cursors = page_state.previous_cursors if page_state.screen == "materials" else []
                latest_status = str(latest.get("status") or "")
                delivered = False
                if latest_status in _AUTO_DELIVER_RUN_STATUSES:
                    delivery = await self._auto_deliver_and_maybe_clear_collection(
                        owner=owner,
                        analysis_run_id=analysis_run_id,
                        expected_version=int(latest.get("version") or 0),
                        chat_id=chat_id,
                        cursor=current_cursor,
                        surface=surface,
                        raise_on_surface_failure=True,
                    )
                    status = delivery.status
                    delivered = delivery.delivered
                else:
                    status = self.gateway.restore_status(owner=owner, cursor=current_cursor)
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
                await self._edit_status_message_via_bot(
                    chat_id=chat_id,
                    message_id=message_id,
                    status=status,
                    state=updated_state,
                )
                surface = self._persist_analysis_task_surface(
                    owner=owner,
                    analysis_run=latest,
                    state=updated_state,
                    chat_id=chat_id,
                    message_id=message_id,
                )
                if latest_status in TERMINAL_RUN_STATUSES:
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
        owner = self._owner_from_callback(callback)
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
                ),
            )
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise
        self.status_message_ids[key] = callback.message.message_id
        self._try_persist_current_materials_surface(
            owner=owner,
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
                ),
            )
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise

    async def _recover_active_channel_surfaces(self) -> None:
        for account in self.gateway.list_channel_accounts():
            if account.get("channel") != "telegram" or account.get("status") != "active":
                continue
            owner = _owner_from_channel_account(account)
            if owner is None:
                continue
            surfaces = self.gateway.list_active_channel_surfaces(
                channel_account_id=str(account["channel_account_id"]),
                page_size=100,
            )
            for surface in surfaces:
                if surface.get("surface_type") == CURRENT_MATERIALS_PANEL:
                    try:
                        await self._recover_current_materials_surface(owner=owner, surface=surface)
                    except TelegramAPIError as error:
                        self._handle_telegram_surface_error(
                            surface=surface,
                            error=error,
                            operation="recover",
                            scope="current_materials_recovery",
                        )
            for surface in surfaces:
                if surface.get("surface_type") == ANALYSIS_TASK_SURFACE:
                    self._recover_analysis_task_surface(owner=owner, surface=surface)

    async def _recover_current_materials_surface(self, *, owner: JsonObject, surface: JsonObject) -> None:
        address = _surface_address(surface)
        if address is None:
            return
        chat_id, message_id = address
        display_state = _surface_display_state(surface)
        state = _page_state_from_display_state(display_state)
        status = self.gateway.restore_status(owner=owner, cursor=state.current_cursor if state.screen == "materials" else None)
        key = _state_key_from_owner(owner)
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
                owner=owner,
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
                    ),
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
                owner=owner,
                status=status,
                state=recovered_state,
                chat_id=chat_id,
                message_id=sent.message_id,
                surface=None,
            )

    def _recover_analysis_task_surface(self, *, owner: JsonObject, surface: JsonObject) -> None:
        run_id = _surface_subject_id(surface, subject_type="analysis_run", role="primary")
        address = _surface_address(surface)
        key = _state_key_from_owner(owner)
        if not run_id or address is None or key is None:
            return
        latest = self.gateway.get_run_status(owner=owner, analysis_run_id=run_id)
        if str(latest.get("status") or "") not in ACTIVE_RUN_STATUSES:
            return
        chat_id, message_id = address
        display_state = _surface_display_state(surface)
        state = _page_state_from_display_state(display_state, focused_run_id=run_id)
        status = self.gateway.restore_status(owner=owner, cursor=state.current_cursor if state.screen == "materials" else None)
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
            owner=owner,
            analysis_run_id=run_id,
            chat_id=chat_id,
            message_id=message_id,
            surface=surface,
        )

    def _persist_current_materials_surface(
        self,
        *,
        owner: JsonObject,
        status: InboxStatus,
        state: _PageState,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
    ) -> JsonObject:
        display_state = _status_surface_display_state(status, state)
        address = _telegram_surface_address(chat_id=chat_id, message_id=message_id)
        existing = surface if surface is not None else self.gateway.find_current_materials_surface(owner=owner)
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
            owner=owner,
            address=address,
            display_state=display_state,
            collection=status.collection,
        )

    def _find_current_materials_surface_or_none(self, *, owner: JsonObject, scope: str) -> JsonObject | None:
        try:
            return self.gateway.find_current_materials_surface(owner=owner)
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
        owner: JsonObject,
        status: InboxStatus,
        state: _PageState,
        chat_id: int,
        message_id: int,
        surface: JsonObject | None = None,
    ) -> JsonObject | None:
        try:
            return self._persist_current_materials_surface(
                owner=owner,
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
        owner: JsonObject,
        analysis_run: JsonObject,
        state: _PageState,
        chat_id: int,
        message_id: int,
    ) -> JsonObject:
        display_state = _run_surface_display_state(analysis_run, state)
        return self.gateway.upsert_analysis_task_surface(
            owner=owner,
            analysis_run=analysis_run,
            address=_telegram_surface_address(chat_id=chat_id, message_id=message_id),
            display_state=display_state,
        )

    def _persist_result_artifact_surface(
        self,
        *,
        owner: JsonObject,
        artifact: JsonObject,
        chat_id: int | None,
        message_id: int,
        delivery_mode: str,
    ) -> JsonObject:
        if chat_id is None:
            raise RuntimeError("telegram_result_chat_missing")
        return self.gateway.upsert_result_artifact_surface(
            owner=owner,
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

    def _owner_from_message(self, message: Message) -> JsonObject:
        return self._scope_from_message(message).owner

    def _owner_from_callback(self, callback: CallbackQuery) -> JsonObject:
        return self._scope_from_callback(callback).owner

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


def render_status_text(
    status: InboxStatus,
    *,
    selection: JsonObject | None = None,
    screen: str = "main",
) -> str:
    del selection
    lines = ["Транскрибация" if screen == "main" else "Материалы"]
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
    return "\n".join(lines)


def build_status_keyboard(
    status: InboxStatus,
    *,
    can_go_back: bool = False,
    current_cursor: str | None = None,
    selection: JsonObject | None = None,
    screen: str = "main",
    focused_run_id: str | None = None,
) -> InlineKeyboardMarkup:
    del selection
    rows: list[list[InlineKeyboardButton]] = []
    collection_id = str(status.collection.get("collection_id") or "") if status.collection else ""
    collection_version = int(status.collection.get("version") or 0) if status.collection else 0
    transcription_button: InlineKeyboardButton | None = None
    focused_active_run = _active_run_for_focus(status, focused_run_id)
    if screen == "main":
        material_count = _material_count(status)
        if material_count and collection_id and focused_active_run is None:
            transcription_button = InlineKeyboardButton(
                text=f"🎙 Транскрибация ({material_count})",
                callback_data=_callback_payload(
                    "rn",
                    _encode_callback_token(collection_id),
                    _encode_callback_version(collection_version),
                ),
            )
        rows.append([InlineKeyboardButton(text="Материалы", callback_data=_callback_payload("mt"))])
    else:
        remove_buttons = [
            InlineKeyboardButton(
                text=f"Убрать {index}",
                callback_data=_callback_payload(
                    "rm",
                    _encode_callback_token(collection_id),
                    _encode_callback_version(collection_version),
                    _encode_callback_token(str(item["media_asset_id"])),
                ),
            )
            for index, item in enumerate(status.items, start=1)
            if item.get("media_asset_id") and collection_id
        ]
        rows.extend([button] for button in remove_buttons)
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
                            "rl",
                            _encode_callback_token(collection_id),
                            _encode_callback_version(collection_version),
                        ),
                    ),
                    InlineKeyboardButton(
                        text="Очистить видимое",
                        callback_data=_callback_payload(
                            "cl",
                            _encode_callback_token(collection_id),
                            _encode_callback_version(collection_version),
                            _encode_optional_callback_token(current_cursor),
                        ),
                    )
                ]
            )
        rows.append([InlineKeyboardButton(text="К карточке", callback_data=_callback_payload("mn"))])
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
        if transcription_button is not None:
            rows.append([transcription_button])
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
    if isinstance(error, RuntimeError) and str(error) == "telegram_file_download_failed":
        return TelegramUserError(TelegramUserErrorCode.UNSUPPORTED_INPUT, detail="missing_file_content")
    if isinstance(error, RuntimeError) and str(error) in {"slot_not_visible", "slot_missing_media_asset_id", "inbox_empty"}:
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION, detail=str(error))
    return error


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
            caption=message.caption,
            media_group_id=message.media_group_id,
            message_id=message.message_id,
        )


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


def _owner_from_channel_account(account: JsonObject) -> JsonObject | None:
    metadata = account.get("metadata")
    adapter_identity: JsonObject | None = None
    if isinstance(metadata, dict):
        owner = metadata.get("owner")
        if isinstance(owner, dict) and owner.get("owner_type") and owner.get("owner_id"):
            return owner
        if isinstance(metadata.get("adapter_identity"), dict):
            adapter_identity = metadata["adapter_identity"]
    owner_id = str(account.get("external_account_ref") or "").strip()
    if not owner_id:
        return None
    owner = {"owner_type": "telegram", "owner_id": owner_id}
    if adapter_identity:
        owner["adapter_identity"] = adapter_identity
    return owner


def _state_key_from_owner(owner: JsonObject) -> tuple[int, int | None] | None:
    identity = owner.get("adapter_identity")
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
        "Кнопки помогают открыть список материалов, убрать лишнее, запустить транскрибацию и открыть последний результат или диагностику."
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
