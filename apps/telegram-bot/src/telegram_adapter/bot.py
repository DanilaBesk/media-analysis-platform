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

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

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


@dataclass(slots=True)
class _PageState:
    current_cursor: str | None = None
    previous_cursors: list[str | None] = field(default_factory=list)
    next_cursor: str | None = None


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
        self._register_handlers()
        self.dispatcher.include_router(self.router)

    async def run(self) -> None:
        for locale in ("ru", "en"):
            await self.bot.set_my_commands(
                list(build_localized_commands(locale, locale_service=self.locale_service)),
                language_code=locale,
            )
        await self.dispatcher.start_polling(self.bot)

    def _register_handlers(self) -> None:
        self.router.message.register(self._handle_start, Command("start"))
        self.router.message.register(self._handle_help, Command("help"))
        self.router.message.register(self._handle_inbox, Command("inbox"))
        self.router.message.register(self._handle_any_message)
        self.router.callback_query.register(
            self._handle_status_callback,
            lambda call: bool(call.data and call.data.startswith("inbox:")),
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
            records = self.gateway.add_message_inputs(
                owner=owner,
                text=_message_text(message),
                files=list(_message_files(message)),
                message_id=message.message_id,
            )
        except Exception as exc:
            await self._answer_message_error(message, exc)
            return
        await self._send_or_edit_status(message, rejected=[record for record in records if record.status == "rejected"])

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
            if data == "inbox:refresh":
                status = self.gateway.restore_status(owner=owner)
                self._set_page_state(key, status, current_cursor=None, previous_cursors=[])
                await self._edit_callback_status(callback, status)
                await callback.answer("Refreshed")
                return
            if data == "inbox:page:next":
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
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Page loaded")
                return
            if data == "inbox:page:prev":
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
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Page loaded")
                return
            if data.startswith("inbox:remove:"):
                if page_state is None:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                try:
                    slot = int(data.removeprefix("inbox:remove:"))
                except ValueError as exc:
                    raise TelegramUserError(TelegramUserErrorCode.STALE_ACTION) from exc
                status = self.gateway.remove_visible_slot(owner=owner, slot=slot, cursor=page_state.current_cursor)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=page_state.current_cursor,
                    previous_cursors=page_state.previous_cursors,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Removed")
                return
            if data == "inbox:clear":
                if page_state is None:
                    await self._answer_callback_error(callback, TelegramUserErrorCode.STALE_ACTION)
                    return
                status = self.gateway.clear_visible_items(owner=owner, cursor=page_state.current_cursor)
                current_cursor = page_state.current_cursor
                previous_cursors = page_state.previous_cursors
                if not status.items and current_cursor is not None and previous_cursors:
                    current_cursor = previous_cursors[-1]
                    previous_cursors = previous_cursors[:-1]
                    status = self.gateway.restore_status(owner=owner, cursor=current_cursor)
                self._set_page_state(
                    key,
                    status,
                    current_cursor=current_cursor,
                    previous_cursors=previous_cursors,
                )
                await self._edit_callback_status(callback, status)
                await callback.answer("Cleared")
                return
            if data == "inbox:start":
                result = self.gateway.start_analysis(owner=owner)
                status = self.gateway.restore_status(owner=owner)
                self._set_page_state(key, status, current_cursor=None, previous_cursors=[])
                await self._edit_callback_status(
                    callback,
                    status,
                    prefix=(
                        f"Run queued: {_short_id(result.analysis_run['analysis_run_id'])}\n"
                        "Result will be available later; refresh /inbox any time.\n\n"
                    ),
                )
                await callback.answer("Run queued")
                return
        except Exception as exc:
            await self._answer_callback_error(callback, exc)
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
            await self._answer_message_error(message, exc)
            return False
        key = self._scope_from_message(message).state_key
        text = render_status_text(status)
        self._set_page_state(key, status, current_cursor=None, previous_cursors=[])
        markup = build_status_keyboard(status, can_go_back=False)
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
        await callback.message.edit_text(
            prefix + render_status_text(status),
            reply_markup=build_status_keyboard(status, can_go_back=bool(self.page_states.get(key, _PageState()).previous_cursors)),
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
    ) -> None:
        self.page_states[key] = _PageState(
            current_cursor=current_cursor,
            previous_cursors=list(previous_cursors),
            next_cursor=status.page.get("next_cursor") or None,
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


def render_status_text(status: InboxStatus) -> str:
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
            for artifact in status.artifacts_by_run.get(run_id, [])[:3]:
                lines.append(f"  Artifact {_artifact_label(artifact)}")
            for diagnostic in status.diagnostics_by_run.get(run_id, [])[:3]:
                lines.append(f"  Diagnostic {_diagnostic_label(diagnostic)}")

    if status.page.get("has_more"):
        lines.append("")
        lines.append("More items are available.")
    return "\n".join(lines)


def build_status_keyboard(status: InboxStatus, *, can_go_back: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="Refresh", callback_data="inbox:refresh")],
    ]
    remove_buttons = [
        InlineKeyboardButton(
            text=f"Remove {index}",
            callback_data=f"inbox:remove:{index}",
        )
        for index, item in enumerate(status.items, start=1)
        if item.get("media_item_id")
    ]
    rows.extend([button] for button in remove_buttons)
    nav_row: list[InlineKeyboardButton] = []
    if can_go_back:
        nav_row.append(InlineKeyboardButton(text="Previous page", callback_data="inbox:page:prev"))
    next_cursor = status.page.get("next_cursor")
    if next_cursor:
        nav_row.append(InlineKeyboardButton(text="Next page", callback_data="inbox:page:next"))
    if nav_row:
        rows.append(nav_row)
    if status.items:
        rows.append([InlineKeyboardButton(text="Clear visible", callback_data="inbox:clear")])
    if status.collection and status.collection.get("items"):
        rows.append([InlineKeyboardButton(text="Start analysis", callback_data="inbox:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
        "Use the inline buttons to refresh, page, remove items, and start analysis."
    )
