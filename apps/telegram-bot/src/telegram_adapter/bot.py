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
from typing import Any
from uuid import UUID

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from telegram_adapter.api_client import TelegramApiClientError
from telegram_adapter.errors import (
    TelegramUserError,
    TelegramUserErrorCode,
    rejected_reason_text,
    safe_callback_answer,
    user_error_text,
)
from telegram_adapter.gateway import (
    InboxStatus,
    IngressRecord,
    TERMINAL_RUN_STATUSES,
    TelegramFileInput,
    TelegramInboxGateway,
)
from telegram_adapter.i18n import DEFAULT_LOCALE, TelegramLocaleService, TelegramTextKey, build_localized_commands
from telegram_adapter.policy import TelegramChatScope

JsonObject = dict[str, Any]
_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_TELEGRAM_HANDLER_ERROR = "[TelegramAdapter][bot][BLOCK_HANDLE_TELEGRAM_HANDLER_ERROR]"
_LOG_MARKER_TELEGRAM_POLLING_STATE = "[TelegramAdapter][bot][BLOCK_TRACK_TELEGRAM_POLLING_STATE]"


@dataclass(slots=True)
class _PageState:
    current_cursor: str | None = None
    previous_cursors: list[str | None] = field(default_factory=list)
    next_cursor: str | None = None
    selection: JsonObject | None = None


CALLBACK_NAMESPACE = "ib"


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
        await self._send_or_edit_status(message, rejected=[record for record in records if record.status == "rejected"])

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
                self._set_page_state(
                    key,
                    status,
                    current_cursor=None,
                    previous_cursors=[],
                    selection=selection,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Refreshed")
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
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Page loaded")
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
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Page loaded")
                return
            if action == "rm":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                media_item_id = _decode_callback_token(tokens[2])
                status = self.gateway.remove_collection_item(
                    owner=owner,
                    collection_id=collection_id,
                    media_item_id=media_item_id,
                    expected_version=expected_version,
                    cursor=page_state.current_cursor if page_state else None,
                )
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor if page_state else None,
                    previous_cursors=page_state.previous_cursors if page_state else [],
                    selection=page_state.selection if page_state else None,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Removed")
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
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Cleared")
                return
            if action == "sl":
                collection_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                selection = self.gateway.create_selection(
                    owner=owner,
                    collection_id=collection_id,
                    expected_version=expected_version,
                )
                status = self.gateway.restore_status(owner=owner, cursor=page_state.current_cursor if page_state else None)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor if page_state else None,
                    previous_cursors=page_state.previous_cursors if page_state else [],
                    selection=selection,
                )
                await self._edit_callback_status(
                    callback,
                    status,
                    prefix=(
                        f"Selection ready: {_short_id(str(selection['selection_id']))} "
                        f"({len(selection.get('items', []))} items)\n"
                        "Use Run selection when you're ready.\n\n"
                    ),
                )
                await callback.answer("Selection created")
                return
            if action == "rn":
                selection_id = _decode_callback_token(tokens[0])
                run = self.gateway.start_analysis(owner=owner, selection_id=selection_id)
                status, prefix, answer_text = await self._resolve_run_start_status(owner=owner, run=run)
                self._set_page_state(key, status, current_cursor=None, previous_cursors=[], selection=None)
                await self._edit_callback_status(
                    callback,
                    status,
                    prefix=prefix,
                )
                await callback.answer(answer_text)
                return
            if action == "ar":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
                artifacts = self.gateway.list_run_artifacts(
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
                )
                await self._edit_callback_status(
                    callback,
                    status,
                    prefix=_detail_prefix(
                        title=f"Artifacts for {_short_id(analysis_run_id)}",
                        lines=[f"- {_artifact_label(artifact)}" for artifact in artifacts] or ["- No artifacts yet."],
                    ),
                )
                await callback.answer("Artifacts loaded")
                return
            if action == "dg":
                analysis_run_id = _decode_callback_token(tokens[0])
                expected_version = _decode_callback_version(tokens[1])
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
                )
                await self._edit_callback_status(
                    callback,
                    status,
                    prefix=_detail_prefix(
                        title=f"Diagnostics for {_short_id(analysis_run_id)}",
                        lines=[f"- {_diagnostic_label(diagnostic)}" for diagnostic in diagnostics] or ["- No diagnostics yet."],
                    ),
                )
                await callback.answer("Diagnostics loaded")
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
    ) -> bool:
        owner = self._owner_from_message(message)
        try:
            status = self.gateway.restore_status(owner=owner, rejected=rejected)
        except Exception as exc:
            normalized = _normalize_message_error(exc)
            _log_handler_exception("status_refresh", exc, normalized=normalized, message=message)
            await self._answer_message_error(message, normalized)
            return False
        key = self._scope_from_message(message).state_key
        existing_state = self.page_states.get(key)
        selection = existing_state.selection if existing_state else None
        text = render_status_text(status, selection=selection)
        self._set_page_state(key, status, current_cursor=None, previous_cursors=[], selection=selection)
        markup = build_status_keyboard(status, can_go_back=False, current_cursor=None, selection=selection)
        previous_message_id = self.status_message_ids.get(key)
        if previous_message_id is not None:
            try:
                await self.bot.edit_message_text(
                    text,
                    chat_id=message.chat.id,
                    message_id=previous_message_id,
                    reply_markup=markup,
                )
                return True
            except Exception:
                self.status_message_ids.pop(key, None)
        sent = await message.answer(text, reply_markup=markup)
        self.status_message_ids[key] = sent.message_id
        return True

    async def _resolve_run_start_status(self, *, owner: JsonObject, run: JsonObject) -> tuple[InboxStatus, str, str]:
        run_id = str(run.get("analysis_run_id") or "")
        for attempt in range(self.run_status_poll_attempts):
            latest = self.gateway.get_run_status(owner=owner, analysis_run_id=run_id)
            status_name = str(latest.get("status") or "")
            if status_name in TERMINAL_RUN_STATUSES:
                status = self.gateway.restore_status(owner=owner)
                return (
                    status,
                    f"Run {status_name}: {_short_id(run_id)}\nOpen artifacts or diagnostics below, or refresh /inbox.\n\n",
                    f"Run {status_name}",
                )
            if attempt + 1 < self.run_status_poll_attempts:
                await self._sleep(self.run_status_poll_delay_seconds)
        status = self.gateway.restore_status(owner=owner)
        return (
            status,
            f"Run queued: {_short_id(run_id)}\nResult will be available later; refresh /inbox any time.\n\n",
            "Run queued",
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
        await callback.message.edit_text(
            prefix + render_status_text(status, selection=state.selection),
            reply_markup=build_status_keyboard(
                status,
                can_go_back=bool(state.previous_cursors),
                current_cursor=state.current_cursor,
                selection=state.selection,
            ),
        )
        self.status_message_ids[key] = callback.message.message_id

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
    ) -> None:
        self.page_states[key] = _PageState(
            current_cursor=current_cursor,
            previous_cursors=list(previous_cursors),
            next_cursor=status.page.get("next_cursor") or None,
            selection=selection,
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

    async def _answer_callback_error(self, callback: CallbackQuery, error: BaseException | TelegramUserErrorCode) -> None:
        await callback.answer(**safe_callback_answer(error))


def render_status_text(status: InboxStatus, *, selection: JsonObject | None = None) -> str:
    lines = ["Inbox"]
    if status.collection:
        lines.append(f"Items: {len(status.collection.get('items', []))} | version {status.collection.get('version', 0)}")
    else:
        lines.append("Items: 0")

    if status.items:
        page_size = status.page.get("page_size") or len(status.items)
        lines.append(f"Visible: {len(status.items)} of page size {page_size}")
        lines.extend(_visible_item_lines(status.items))

    if not status.items:
        lines.append("No inbox items yet.")

    for record in status.rejected:
        lines.append(f"Rejected: {record.label} ({rejected_reason_text(record.reason)})")

    if selection:
        lines.append("")
        lines.append(
            f"Selection ready: {_short_id(str(selection.get('selection_id') or 'selection'))} "
            f"({len(selection.get('items', []))} items)"
        )
        lines.append("Use Run selection to start analysis.")

    if status.active_runs:
        lines.append("")
        lines.append("Active runs:")
        for run in status.active_runs[:3]:
            lines.append(
                f"- {_short_id(run['analysis_run_id'])}: {run.get('status', 'unknown')} "
                "(result available later; refresh /inbox)"
            )

    completed_runs = [
        run
        for run in status.recent_runs
        if run.get("status") in TERMINAL_RUN_STATUSES and run.get("analysis_run_id")
    ]
    if completed_runs:
        lines.append("")
        lines.append("Completed runs:")
        for run in completed_runs[:3]:
            run_id = str(run["analysis_run_id"])
            lines.append(f"- {_short_id(run_id)}: {run.get('status', 'unknown')}")
        lines.append("Use the run buttons below for artifacts and diagnostics.")

    if status.page.get("has_more"):
        lines.append("")
        lines.append("More items are available.")
    return "\n".join(lines)


def build_status_keyboard(
    status: InboxStatus,
    *,
    can_go_back: bool = False,
    current_cursor: str | None = None,
    selection: JsonObject | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="Refresh", callback_data=_callback_payload("rf"))],
    ]
    collection_id = str(status.collection.get("collection_id") or "") if status.collection else ""
    collection_version = int(status.collection.get("version") or 0) if status.collection else 0
    remove_buttons = [
        InlineKeyboardButton(
            text=f"Remove {index}",
            callback_data=_callback_payload(
                "rm",
                _encode_callback_token(collection_id),
                _encode_callback_version(collection_version),
                _encode_callback_token(str(item["media_item_id"])),
            ),
        )
        for index, item in enumerate(status.items, start=1)
        if item.get("media_item_id") and collection_id
    ]
    rows.extend([button] for button in remove_buttons)
    nav_row: list[InlineKeyboardButton] = []
    if can_go_back:
        nav_row.append(InlineKeyboardButton(text="Previous page", callback_data=_callback_payload("pp")))
    next_cursor = status.page.get("next_cursor")
    if next_cursor:
        nav_row.append(InlineKeyboardButton(text="Next page", callback_data=_callback_payload("pn")))
    if nav_row:
        rows.append(nav_row)
    if status.items and collection_id:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Clear visible",
                    callback_data=_callback_payload(
                        "cl",
                        _encode_callback_token(collection_id),
                        _encode_callback_version(collection_version),
                        _encode_optional_callback_token(current_cursor),
                    ),
                )
            ]
        )
    if status.collection and status.collection.get("items") and collection_id:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Create selection",
                    callback_data=_callback_payload(
                        "sl",
                        _encode_callback_token(collection_id),
                        _encode_callback_version(collection_version),
                    ),
                )
            ]
        )
    if selection and selection.get("selection_id"):
        rows.append(
            [
                InlineKeyboardButton(
                    text="Run selection",
                    callback_data=_callback_payload(
                        "rn",
                        _encode_callback_token(str(selection["selection_id"])),
                    ),
                )
            ]
        )
    for run in status.recent_runs:
        if run.get("status") not in TERMINAL_RUN_STATUSES or not run.get("analysis_run_id"):
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"Artifacts {_short_id(str(run['analysis_run_id']))}",
                    callback_data=_callback_payload(
                        "ar",
                        _encode_callback_token(str(run["analysis_run_id"])),
                        _encode_callback_version(int(run.get("version") or 0)),
                    ),
                ),
                InlineKeyboardButton(
                    text=f"Diagnostics {_short_id(str(run['analysis_run_id']))}",
                    callback_data=_callback_payload(
                        "dg",
                        _encode_callback_token(str(run["analysis_run_id"])),
                        _encode_callback_version(int(run.get("version") or 0)),
                    ),
                ),
            ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    if isinstance(error, RuntimeError) and str(error) in {"slot_not_visible", "slot_missing_media_item_id", "inbox_empty"}:
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
    display_name = str(item.get("display_name") or item.get("media_item_id") or "media")
    kind = item.get("kind", "media")
    status = item.get("status", "unknown")
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    message_id = metadata.get("message_id")
    message_suffix = f", message {message_id}" if message_id is not None else ""
    return f"{display_name} [{kind}, {status}{message_suffix}]"


def _artifact_label(artifact: JsonObject) -> str:
    artifact_id = str(artifact.get("artifact_id") or "artifact")
    kind = str(artifact.get("kind") or "artifact")
    status = str(artifact.get("status") or "unknown")
    content_type = artifact.get("content_type")
    content_suffix = f", {content_type}" if content_type else ""
    return f"{_short_id(artifact_id)}: {kind} [{status}{content_suffix}]"


def _diagnostic_label(diagnostic: JsonObject) -> str:
    code = str(diagnostic.get("code") or "diagnostic")
    message = str(diagnostic.get("message") or diagnostic.get("severity") or "recorded")
    return f"{code}: {message}"


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
        lines.append(f"Album {media_group_id} ({len(album_items)} items)")
        for album_slot, album_item in album_items:
            lines.append(f"{album_slot}. {_item_label(album_item)}")
    return lines


def _media_group_id(item: JsonObject) -> str | None:
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        return None
    media_group_id = metadata.get("media_group_id")
    return str(media_group_id) if media_group_id else None


def _short_id(value: str) -> str:
    return value if len(value) <= 12 else f"{value[:8]}...{value[-4:]}"


def _start_text() -> str:
    return "Send text, links, photos, videos, or documents. Everything accepted goes to your inbox first."


def _help_text() -> str:
    return (
        "/inbox - restore the current inbox status\n"
        "Use the inline buttons to refresh, page, remove items, create a selection, and run it."
    )
