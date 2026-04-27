from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from aiogram import Bot, Dispatcher, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    BotCommandScopeDefault,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardMarkup,
    MenuButtonCommands,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from media_analysis_platform.config import Settings
from transcriber_workers_common.domain import MediaAttachment, ProcessedJob, SourceCandidate
from transcriber_workers_common.media_groups import MediaGroupAccumulator
from transcriber_workers_common.source_labels import humanize_source_label, looks_like_machine_file_name
from transcriber_workers_common.source_extractor import extract_sources

try:
    from telegram_adapter.i18n import DEFAULT_LOCALE, SUPPORTED_LOCALES, SupportedLocale, TelegramLocaleService, TelegramTextKey, build_localized_commands
except ModuleNotFoundError:  # pragma: no cover - workspace fallback for monorepo runtime/tests
    ADAPTER_SRC = Path(__file__).resolve().parents[2] / "apps" / "telegram-bot" / "src"
    if str(ADAPTER_SRC) not in sys.path:
        sys.path.insert(0, str(ADAPTER_SRC))
    from telegram_adapter.i18n import DEFAULT_LOCALE, SUPPORTED_LOCALES, SupportedLocale, TelegramLocaleService, TelegramTextKey, build_localized_commands

LOGGER = logging.getLogger(__name__)
HANDLE_MEDIA_MARKER = "[TelegramAdapter][handleMedia][BLOCK_SUBMIT_AND_WAIT_FOR_JOB]"
TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS = 3500
TELEGRAM_COMMANDS = build_localized_commands(DEFAULT_LOCALE)


class TelegramProcessingGateway(Protocol):
    def process_source(self, source: SourceCandidate) -> ProcessedJob: ...

    def process_source_group(self, sources: list[SourceCandidate]) -> ProcessedJob: ...

    def create_batch_draft(self, *, owner: dict[str, str], display_name: str | None = None) -> dict: ...

    def load_batch_draft(self, *, draft_id: str, owner: dict[str, str]) -> dict: ...

    def add_source_to_draft(self, draft: dict, *, owner: dict[str, str], source: SourceCandidate) -> dict: ...

    def remove_draft_item(
        self,
        *,
        draft_id: str,
        owner: dict[str, str],
        expected_version: int,
        item_id: str,
    ) -> dict: ...

    def clear_batch_draft(self, *, draft_id: str, owner: dict[str, str], expected_version: int) -> dict: ...

    def submit_batch_draft(self, *, draft_id: str, owner: dict[str, str], expected_version: int) -> ProcessedJob: ...

    def load_job(self, job_id: str) -> ProcessedJob: ...

    def ensure_report(self, job_id: str, report_prompt_suffix: str = "") -> ProcessedJob: ...

    def ensure_deep_research(self, job_id: str) -> Path: ...


class CandidateSelectionStore:
    def __init__(self) -> None:
        self._items: dict[str, tuple[int, int | None, list[SourceCandidate]]] = {}

    def save(self, chat_id: int, user_id: int | None, candidates: list[SourceCandidate]) -> str:
        selection_id = uuid4().hex[:12]
        self._items[selection_id] = (chat_id, user_id, candidates)
        return selection_id

    def get(self, selection_id: str, chat_id: int, user_id: int | None, index: int) -> SourceCandidate | None:
        entry = self._items.get(selection_id)
        if entry is None:
            return None
        expected_chat_id, expected_user_id, candidates = entry
        if expected_chat_id != chat_id:
            return None
        if expected_user_id is not None and user_id is not None and expected_user_id != user_id:
            return None
        if index < 0 or index >= len(candidates):
            return None
        return candidates[index]


class BasketStore:
    def __init__(self) -> None:
        self._items: dict[tuple[int, int | None], list[SourceCandidate]] = {}

    def add(self, chat_id: int, user_id: int | None, candidates: list[SourceCandidate]) -> list[SourceCandidate]:
        key = (chat_id, user_id)
        basket = self._items.setdefault(key, [])
        basket.extend(candidates)
        return list(basket)

    def list(self, chat_id: int, user_id: int | None) -> list[SourceCandidate]:
        return list(self._items.get((chat_id, user_id), []))

    def remove(self, chat_id: int, user_id: int | None, index: int) -> SourceCandidate | None:
        basket = self._items.get((chat_id, user_id))
        if basket is None or index < 0 or index >= len(basket):
            return None
        removed = basket.pop(index)
        if not basket:
            self._items.pop((chat_id, user_id), None)
        return removed

    def clear(self, chat_id: int, user_id: int | None) -> None:
        self._items.pop((chat_id, user_id), None)


class BatchModeStore:
    def __init__(self) -> None:
        self._disabled: set[tuple[int, int | None]] = set()

    def is_enabled(self, chat_id: int, user_id: int | None) -> bool:
        return (chat_id, user_id) not in self._disabled

    def set_enabled(self, chat_id: int, user_id: int | None, enabled: bool) -> None:
        key = (chat_id, user_id)
        if enabled:
            self._disabled.discard(key)
        else:
            self._disabled.add(key)

    def toggle(self, chat_id: int, user_id: int | None) -> bool:
        enabled = not self.is_enabled(chat_id, user_id)
        self.set_enabled(chat_id, user_id, enabled)
        return enabled


class TelegramTranscriberApp:
    def __init__(self, settings: Settings, processing_service: TelegramProcessingGateway, bot: Bot | None = None) -> None:
        self.settings = settings
        self.processing_service = processing_service
        self.bot = bot or Bot(settings.telegram_bot_token)
        self.locale_service = TelegramLocaleService()
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.selection_store = CandidateSelectionStore()
        self._current_drafts: dict[tuple[int, int | None], tuple[str, int]] = {}
        self._item_callback_tokens: dict[tuple[str, str], str] = {}
        self.batch_modes = BatchModeStore()
        self.media_groups = MediaGroupAccumulator()
        self.media_group_tasks: dict[tuple[int, str], asyncio.Task[None]] = {}
        self.media_group_text: dict[tuple[int, str], str] = {}
        self.media_group_locales: dict[tuple[int, str], SupportedLocale] = {}
        self.basket_summary_tasks: dict[tuple[int, int | None], asyncio.Task[None]] = {}
        self.basket_summary_added_counts: dict[tuple[int, int | None], int] = {}
        self.basket_summary_locales: dict[tuple[int, int | None], SupportedLocale] = {}
        self.basket_locks: dict[tuple[int, int | None], asyncio.Lock] = {}
        self.report_locks: dict[str, asyncio.Lock] = {}
        self.deep_research_locks: dict[str, asyncio.Lock] = {}
        self._register_handlers()

    async def run(self) -> None:
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        await self._configure_commands()
        await self.dispatcher.start_polling(self.bot)

    def _register_handlers(self) -> None:
        self.dispatcher.include_router(self.router)
        self.router.message.register(self._handle_start, CommandStart())
        self.router.message.register(self._handle_help, Command("help"))
        self.router.message.register(self._handle_batch_command, Command("batch"))
        self.router.message.register(self._handle_basket_command, Command("basket"))
        self.router.message.register(self._handle_clear_command, Command("clear"))
        self.router.message.register(self._handle_message)
        self.router.callback_query.register(
            self._handle_batch_mode_toggle,
            lambda call: bool(call.data and call.data == "mode:batch:toggle"),
        )
        self.router.callback_query.register(self._handle_source_selection, lambda call: bool(call.data and call.data.startswith("pick:")))
        self.router.callback_query.register(
            self._handle_basket_callback,
            lambda call: bool(call.data and (call.data.startswith("basket:") or call.data.startswith("b:"))),
        )
        self.router.callback_query.register(self._handle_get_transcript, lambda call: bool(call.data and call.data.startswith("get:")))
        self.router.callback_query.register(
            self._handle_generate_report, lambda call: bool(call.data and call.data.startswith("report:"))
        )
        self.router.callback_query.register(
            self._handle_deep_research, lambda call: bool(call.data and call.data.startswith("deep:"))
        )

    async def _configure_commands(self) -> None:
        default_scope = BotCommandScopeDefault()
        default_commands = list(build_localized_commands(DEFAULT_LOCALE, locale_service=self.locale_service))
        await self.bot.set_my_commands(default_commands, scope=default_scope)
        for locale in SUPPORTED_LOCALES:
            await self.bot.set_my_commands(
                list(build_localized_commands(locale, locale_service=self.locale_service)),
                scope=default_scope,
                language_code=locale,
            )
        await self.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    def _resolve_message_locale(self, message: Message) -> SupportedLocale:
        return self.locale_service.resolve_locale(
            user_locale=getattr(message.from_user, "language_code", None),
            fallback_locale=DEFAULT_LOCALE,
        )

    def _resolve_callback_locale(self, callback: CallbackQuery) -> SupportedLocale:
        return self.locale_service.resolve_locale(
            user_locale=getattr(callback.from_user, "language_code", None),
            fallback_locale=DEFAULT_LOCALE,
        )

    async def _handle_start(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        locale = self._resolve_message_locale(message)
        user_id = message.from_user.id if message.from_user else None
        draft = await self._load_current_draft(message.chat.id, user_id)
        await message.answer(
            self.locale_service.text(TelegramTextKey.START_PROMPT, locale=locale),
            reply_markup=_build_main_keyboard(
                locale_service=self.locale_service,
                locale=locale,
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                draft=draft,
            ),
        )

    async def _handle_help(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        locale = self._resolve_message_locale(message)
        user_id = message.from_user.id if message.from_user else None
        await self._send_command_menu(chat_id=message.chat.id, user_id=user_id, answer=message.answer, locale=locale)

    async def _handle_batch_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        locale = self._resolve_message_locale(message)
        user_id = message.from_user.id if message.from_user else None
        enabled = self.batch_modes.toggle(message.chat.id, user_id)
        draft = await self._load_current_draft(message.chat.id, user_id)
        await message.answer(
            _build_batch_mode_text(self.locale_service, locale, enabled),
            reply_markup=_build_main_keyboard(
                locale_service=self.locale_service,
                locale=locale,
                batch_enabled=enabled,
                draft=draft,
            ),
        )

    async def _handle_basket_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        locale = self._resolve_message_locale(message)
        user_id = message.from_user.id if message.from_user else None
        async with self._basket_lock(message.chat.id, user_id):
            self._cancel_pending_basket_summary(message.chat.id, user_id)
            draft = await self._load_current_draft(message.chat.id, user_id)
        text = (
            _build_draft_text(self.locale_service, locale, draft)
            if _draft_items(draft)
            else self.locale_service.text(TelegramTextKey.BASKET_EMPTY, locale=locale)
        )
        await message.answer(
            text,
            reply_markup=self._build_draft_keyboard(draft, locale=locale)
            if _draft_items(draft)
            else _build_main_keyboard(
                locale_service=self.locale_service,
                locale=locale,
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                draft=draft,
            ),
        )

    async def _handle_clear_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        locale = self._resolve_message_locale(message)
        user_id = message.from_user.id if message.from_user else None
        async with self._basket_lock(message.chat.id, user_id):
            self._cancel_pending_basket_summary(message.chat.id, user_id)
            owner = _build_telegram_owner(message.chat.id, user_id)
            draft = await self._load_current_draft(message.chat.id, user_id)
            if draft is not None:
                try:
                    draft = await asyncio.to_thread(
                        self.processing_service.clear_batch_draft,
                        draft_id=draft["draft_id"],
                        owner=owner,
                        expected_version=int(draft["version"]),
                    )
                    self._remember_draft(message.chat.id, user_id, draft)
                except Exception as exc:
                    if _is_draft_stale_error(exc):
                        self._forget_draft(message.chat.id, user_id)
                    else:
                        await message.answer(
                            self.locale_service.text(TelegramTextKey.BASKET_CLEAR_FAILED, locale=locale, error=exc)
                        )
                        return
        await message.answer(
            self.locale_service.text(TelegramTextKey.BASKET_CLEARED, locale=locale),
            reply_markup=_build_main_keyboard(
                locale_service=self.locale_service,
                locale=locale,
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                draft=None,
            ),
        )

    async def _handle_message(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        attachments = _extract_attachments(message)
        text = message.text or message.caption or ""
        user_id = message.from_user.id if message.from_user else None
        locale = self._resolve_message_locale(message)

        if _looks_like_command_menu_request(text) and not attachments:
            await self._send_command_menu(chat_id=message.chat.id, user_id=user_id, answer=message.answer, locale=locale)
            return

        if message.media_group_id and attachments:
            key = (message.chat.id, message.media_group_id)
            for attachment in attachments:
                self.media_groups.add(message.chat.id, message.media_group_id, attachment)
            if text and not self.media_group_text.get(key):
                self.media_group_text[key] = text
            self.media_group_locales[key] = locale

            existing = self.media_group_tasks.get(key)
            if existing and not existing.done():
                existing.cancel()
            self.media_group_tasks[key] = asyncio.create_task(
                self._flush_media_group(
                    chat_id=message.chat.id,
                    user_id=user_id,
                    media_group_id=message.media_group_id,
                )
            )
            return

        await self._process_candidate_set(
            chat_id=message.chat.id,
            user_id=user_id,
            text=text,
            attachments=attachments,
            message_id=getattr(message, "message_id", None),
            locale=locale,
        )

    async def _send_command_menu(self, chat_id: int, user_id: int | None, answer, *, locale: SupportedLocale) -> None:
        draft = await self._load_current_draft(chat_id, user_id)
        await answer(
            self.locale_service.text(TelegramTextKey.HELP_MENU, locale=locale),
            reply_markup=_build_main_keyboard(
                locale_service=self.locale_service,
                locale=locale,
                batch_enabled=self.batch_modes.is_enabled(chat_id, user_id),
                draft=draft,
            ),
        )

    async def _flush_media_group(self, chat_id: int, user_id: int | None, media_group_id: str) -> None:
        key = (chat_id, media_group_id)
        try:
            await asyncio.sleep(self.settings.media_group_window_seconds)
        except asyncio.CancelledError:
            return

        attachments = self.media_groups.pop(chat_id, media_group_id)
        text = self.media_group_text.pop(key, "")
        locale = self.media_group_locales.pop(key, DEFAULT_LOCALE)
        self.media_group_tasks.pop(key, None)
        await self._process_candidate_set(
            chat_id=chat_id,
            user_id=user_id,
            text=text,
            attachments=attachments,
            message_id=None,
            locale=locale,
        )

    async def _process_candidate_set(
        self,
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list[MediaAttachment],
        message_id: int | None = None,
        locale: SupportedLocale | None = None,
    ) -> None:
        resolved_locale = self.locale_service.normalize_locale(locale)
        result = extract_sources(text=text, attachments=attachments)
        if not result.candidates:
            message = self.locale_service.text(TelegramTextKey.BASKET_UNSUPPORTED_SOURCES, locale=resolved_locale)
            if result.rejected_urls:
                message = _append_rejected_urls(
                    self.locale_service,
                    resolved_locale,
                    message,
                    result.rejected_urls,
                )
            await self.bot.send_message(chat_id, message)
            return

        candidates = [_with_stable_source_id(candidate, message_id=message_id) for candidate in result.candidates]
        if not self.batch_modes.is_enabled(chat_id, user_id):
            self._cancel_pending_basket_summary(chat_id, user_id)
            if len(candidates) == 1:
                await self.bot.send_message(
                    chat_id,
                    self.locale_service.text(TelegramTextKey.BASKET_SINGLE_MODE_PROCESS_NOW, locale=resolved_locale),
                )
                await self._start_processing(chat_id, candidates[0], locale=resolved_locale)
                return

            selection_id = self.selection_store.save(chat_id=chat_id, user_id=user_id, candidates=candidates)
            message = self.locale_service.text(TelegramTextKey.BASKET_SINGLE_MODE_SELECT_ONE, locale=resolved_locale)
            if result.rejected_urls:
                message = _append_rejected_urls(
                    self.locale_service,
                    resolved_locale,
                    message,
                    result.rejected_urls,
                )
            await self.bot.send_message(
                chat_id,
                message,
                reply_markup=_build_candidate_keyboard(self.locale_service, resolved_locale, selection_id, candidates),
            )
            return

        should_debounce = len(candidates) == 1 and not result.rejected_urls
        send_message: str | None = None
        send_markup = None
        try:
            async with self._basket_lock(chat_id, user_id):
                if should_debounce:
                    self._mark_basket_summary_added(chat_id, user_id, added_count=1, locale=resolved_locale)

                try:
                    draft = await self._add_candidates_to_draft(
                        chat_id=chat_id,
                        user_id=user_id,
                        candidates=candidates,
                    )
                except Exception:
                    if should_debounce:
                        self._cancel_pending_basket_summary(chat_id, user_id)
                    raise
                message = _build_draft_text(self.locale_service, resolved_locale, draft, added_count=len(candidates))
                if result.rejected_urls:
                    message = _append_rejected_urls(
                        self.locale_service,
                        resolved_locale,
                        message,
                        result.rejected_urls,
                    )

                if should_debounce:
                    self.basket_summary_tasks[(chat_id, user_id)] = asyncio.create_task(
                        self._flush_basket_summary(chat_id, user_id)
                    )
                    return

                self._cancel_pending_basket_summary(chat_id, user_id)
                send_message = message
                send_markup = self._build_draft_keyboard(draft, locale=resolved_locale)
        except Exception as exc:
            await self.bot.send_message(
                chat_id,
                self.locale_service.text(TelegramTextKey.BASKET_UPDATE_FAILED, locale=resolved_locale, error=exc),
            )
            return

        if send_message is not None:
            await self.bot.send_message(chat_id, send_message, reply_markup=send_markup)

    async def _add_candidates_to_draft(
        self,
        *,
        chat_id: int,
        user_id: int | None,
        candidates: list[SourceCandidate],
    ) -> dict:
        owner = _build_telegram_owner(chat_id, user_id)
        draft = await self._load_current_draft(chat_id, user_id)
        if draft is None or draft.get("status") != "open":
            draft = await asyncio.to_thread(
                self.processing_service.create_batch_draft,
                owner=owner,
                display_name="telegram_collection",
            )
        for candidate in candidates:
            prepared_candidate = await self._download_attachment_if_needed(candidate)
            draft = await asyncio.to_thread(
                self.processing_service.add_source_to_draft,
                draft,
                owner=owner,
                source=prepared_candidate,
            )
        self._remember_draft(chat_id, user_id, draft)
        return draft

    def _schedule_basket_summary(
        self,
        chat_id: int,
        user_id: int | None,
        *,
        added_count: int,
        locale: SupportedLocale,
    ) -> None:
        self._mark_basket_summary_added(chat_id, user_id, added_count=added_count, locale=locale)
        self.basket_summary_tasks[(chat_id, user_id)] = asyncio.create_task(self._flush_basket_summary(chat_id, user_id))

    def _mark_basket_summary_added(
        self,
        chat_id: int,
        user_id: int | None,
        *,
        added_count: int,
        locale: SupportedLocale,
    ) -> None:
        key = (chat_id, user_id)
        self.basket_summary_added_counts[key] = self.basket_summary_added_counts.get(key, 0) + added_count
        self.basket_summary_locales[key] = locale
        existing = self.basket_summary_tasks.get(key)
        if existing and not existing.done():
            existing.cancel()

    def _cancel_pending_basket_summary(self, chat_id: int, user_id: int | None) -> None:
        key = (chat_id, user_id)
        existing = self.basket_summary_tasks.pop(key, None)
        if existing and not existing.done():
            existing.cancel()
        self.basket_summary_added_counts.pop(key, None)
        self.basket_summary_locales.pop(key, None)

    def _basket_lock(self, chat_id: int, user_id: int | None) -> asyncio.Lock:
        key = (chat_id, user_id)
        lock = self.basket_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self.basket_locks[key] = lock
        return lock

    async def _flush_basket_summary(self, chat_id: int, user_id: int | None) -> None:
        key = (chat_id, user_id)
        try:
            await asyncio.sleep(self.settings.media_group_window_seconds)
        except asyncio.CancelledError:
            return

        async with self._basket_lock(chat_id, user_id):
            self.basket_summary_tasks.pop(key, None)
            added_count = self.basket_summary_added_counts.pop(key, 0)
            locale = self.basket_summary_locales.pop(key, DEFAULT_LOCALE)
            draft = await self._load_current_draft(chat_id, user_id)
        if not _draft_items(draft):
            return
        await self.bot.send_message(
            chat_id,
            _build_draft_text(self.locale_service, locale, draft, added_count=added_count or None),
            reply_markup=self._build_draft_keyboard(draft, locale=locale),
        )

    async def _handle_source_selection(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        locale = self._resolve_callback_locale(callback)

        try:
            _, selection_id, index_raw = callback.data.split(":", 2)
            index = int(index_raw)
        except ValueError:
            await callback.answer(self.locale_service.text(TelegramTextKey.SELECTION_INVALID, locale=locale), show_alert=True)
            return

        candidate = self.selection_store.get(
            selection_id=selection_id,
            chat_id=callback.message.chat.id,
            user_id=callback.from_user.id if callback.from_user else None,
            index=index,
        )
        if candidate is None:
            await callback.answer(self.locale_service.text(TelegramTextKey.SELECTION_MISSING, locale=locale), show_alert=True)
            return

        await callback.answer(self.locale_service.text(TelegramTextKey.SELECTION_ACK, locale=locale))
        await self._start_processing(callback.message.chat.id, candidate, locale=locale)

    async def _handle_batch_mode_toggle(self, callback: CallbackQuery) -> None:
        if callback.message is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return

        locale = self._resolve_callback_locale(callback)
        chat_id = callback.message.chat.id
        user_id = callback.from_user.id if callback.from_user else None
        enabled = self.batch_modes.toggle(chat_id, user_id)
        draft = await self._load_current_draft(chat_id, user_id)
        await _safe_callback_answer(callback, _build_batch_mode_text(self.locale_service, locale, enabled))
        await callback.message.edit_text(
            _build_batch_mode_text(self.locale_service, locale, enabled),
            reply_markup=_build_main_keyboard(
                locale_service=self.locale_service,
                locale=locale,
                batch_enabled=enabled,
                draft=draft,
            ),
        )

    async def _handle_basket_callback(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return

        locale = self._resolve_callback_locale(callback)
        chat_id = callback.message.chat.id
        user_id = callback.from_user.id if callback.from_user else None
        if callback.data.startswith("basket:"):
            await callback.answer(_stale_button_text(self.locale_service, locale), show_alert=True)
            return

        parsed = _parse_draft_callback(callback.data)
        if parsed is None:
            await callback.answer(
                self.locale_service.text(TelegramTextKey.BASKET_ACTION_INVALID, locale=locale),
                show_alert=True,
            )
            return

        command, draft_id, expected_version, token = parsed
        owner = _build_telegram_owner(chat_id, user_id)

        if command == "s":
            async with self._basket_lock(chat_id, user_id):
                self._cancel_pending_basket_summary(chat_id, user_id)
            await _safe_callback_answer(callback, self.locale_service.text(TelegramTextKey.BASKET_START_ACK, locale=locale))
            await self._start_basket_processing(
                chat_id=chat_id,
                user_id=user_id,
                draft_id=draft_id,
                expected_version=expected_version,
                locale=locale,
            )
            return

        if command == "c":
            async with self._basket_lock(chat_id, user_id):
                self._cancel_pending_basket_summary(chat_id, user_id)
                try:
                    draft = await asyncio.to_thread(
                        self.processing_service.clear_batch_draft,
                        draft_id=draft_id,
                        owner=owner,
                        expected_version=expected_version,
                    )
                except Exception as exc:
                    await self._answer_draft_error(callback, exc, locale=locale)
                    return
                self._remember_draft(chat_id, user_id, draft)
            cleared_text = self.locale_service.text(TelegramTextKey.BASKET_CLEARED, locale=locale)
            await _safe_callback_answer(callback, cleared_text)
            await callback.message.edit_text(cleared_text)
            return

        if command == "r" and token:
            item_id = self._item_callback_tokens.get((draft_id, token))
            if item_id is None:
                await callback.answer(_stale_button_text(self.locale_service, locale), show_alert=True)
                return
            async with self._basket_lock(chat_id, user_id):
                self._cancel_pending_basket_summary(chat_id, user_id)
                try:
                    draft = await asyncio.to_thread(
                        self.processing_service.remove_draft_item,
                        draft_id=draft_id,
                        owner=owner,
                        expected_version=expected_version,
                        item_id=item_id,
                    )
                except Exception as exc:
                    await self._answer_draft_error(callback, exc, locale=locale)
                    return
                self._remember_draft(chat_id, user_id, draft)
            await _safe_callback_answer(callback, self.locale_service.text(TelegramTextKey.BASKET_REMOVE_ACK, locale=locale))
            if _draft_items(draft):
                await callback.message.edit_text(
                    _build_draft_text(self.locale_service, locale, draft),
                    reply_markup=self._build_draft_keyboard(draft, locale=locale),
                )
            else:
                await callback.message.edit_text(self.locale_service.text(TelegramTextKey.BASKET_EMPTY, locale=locale))
            return

        await callback.answer(self.locale_service.text(TelegramTextKey.BASKET_ACTION_INVALID, locale=locale), show_alert=True)

    async def _start_processing(self, chat_id: int, candidate: SourceCandidate, *, locale: SupportedLocale) -> None:
        status_message = await self.bot.send_message(
            chat_id,
            _build_processing_status_text(self.locale_service, locale, candidate),
        )
        try:
            prepared_candidate = await self._download_attachment_if_needed(candidate)
            LOGGER.info("%s mode=single source_kind=%s", HANDLE_MEDIA_MARKER, prepared_candidate.kind)
            job = await asyncio.to_thread(self.processing_service.process_source, prepared_candidate)
        except Exception as exc:
            await status_message.edit_text(
                self.locale_service.text(TelegramTextKey.PROCESSING_FAILED_SOURCE, locale=locale, error=exc)
            )
            return

        await status_message.edit_text(self.locale_service.text(TelegramTextKey.PROCESSING_DONE, locale=locale))
        await self._send_transcript_result(
            status_message,
            job,
            fallback_caption=self.locale_service.text(TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_SINGLE, locale=locale),
            locale=locale,
        )

    async def _start_processing_group(
        self,
        chat_id: int,
        candidates: list[SourceCandidate],
        *,
        locale: SupportedLocale,
    ) -> None:
        status_message = await self.bot.send_message(
            chat_id,
            _build_group_processing_status_text(self.locale_service, locale, candidates),
        )
        try:
            prepared_candidates = [await self._download_attachment_if_needed(candidate) for candidate in candidates]
            LOGGER.info("%s mode=combined source_count=%s", HANDLE_MEDIA_MARKER, len(prepared_candidates))
            job = await asyncio.to_thread(self.processing_service.process_source_group, prepared_candidates)
        except Exception as exc:
            await status_message.edit_text(
                self.locale_service.text(TelegramTextKey.PROCESSING_FAILED_GROUP, locale=locale, error=exc)
            )
            return

        await status_message.edit_text(self.locale_service.text(TelegramTextKey.PROCESSING_DONE, locale=locale))
        await self._send_transcript_result(
            status_message,
            job,
            fallback_caption=self.locale_service.text(TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_SINGLE, locale=locale),
            locale=locale,
        )

    async def _start_basket_processing(
        self,
        *,
        chat_id: int,
        user_id: int | None,
        draft_id: str,
        expected_version: int,
        locale: SupportedLocale,
    ) -> None:
        owner = _build_telegram_owner(chat_id, user_id)
        async with self._basket_lock(chat_id, user_id):
            draft = await self._load_current_draft(chat_id, user_id, draft_id=draft_id)
            if draft is not None and not _draft_items(draft):
                await self.bot.send_message(chat_id, self.locale_service.text(TelegramTextKey.BASKET_EMPTY, locale=locale))
                return

            status_message = await self.bot.send_message(
                chat_id,
                _build_draft_processing_status_text(self.locale_service, locale, draft),
            )
            try:
                LOGGER.info("%s mode=batch draft_id=%s expected_version=%s", HANDLE_MEDIA_MARKER, draft_id, expected_version)
                job = await asyncio.to_thread(
                    self.processing_service.submit_batch_draft,
                    draft_id=draft_id,
                    owner=owner,
                    expected_version=expected_version,
                )
            except Exception as exc:
                if _is_draft_stale_error(exc):
                    await status_message.edit_text(_draft_error_text(self.locale_service, locale, exc))
                    return
                await status_message.edit_text(
                    self.locale_service.text(TelegramTextKey.PROCESSING_FAILED_BASKET, locale=locale, error=exc)
                )
                return

            self._forget_draft(chat_id, user_id)
        await status_message.edit_text(self.locale_service.text(TelegramTextKey.PROCESSING_DONE_BASKET, locale=locale))
        await self._send_transcript_result(
            status_message,
            job,
            fallback_caption=self.locale_service.text(TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_BASKET, locale=locale),
            locale=locale,
        )

    async def _send_transcript_result(
        self,
        status_message: Message,
        job: ProcessedJob,
        *,
        fallback_caption: str,
        locale: SupportedLocale,
    ) -> None:
        reply_markup = _build_result_keyboard(self.locale_service, locale, job.job_id)
        text_path = job.transcript.text_path
        if not text_path.is_file():
            await status_message.answer(
                self.locale_service.text(TelegramTextKey.TRANSCRIPT_TEXT_UNAVAILABLE, locale=locale),
                reply_markup=reply_markup,
            )
            return

        try:
            with text_path.open(encoding="utf-8") as transcript_file:
                transcript_text = transcript_file.read(TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS + 1)
        except (OSError, UnicodeDecodeError):
            transcript_text = None

        if (
            transcript_text is not None
            and transcript_text.strip()
            and len(transcript_text) <= TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS
        ):
            try:
                await status_message.answer(transcript_text, reply_markup=reply_markup)
                return
            except TelegramBadRequest:
                pass

        await status_message.answer_document(
            FSInputFile(text_path),
            caption=fallback_caption,
            reply_markup=reply_markup,
        )

    async def _download_attachment_if_needed(self, candidate: SourceCandidate) -> SourceCandidate:
        if candidate.telegram_file_id is None or candidate.local_path is not None:
            return candidate

        incoming_dir = self.settings.data_dir / "incoming"
        incoming_dir.mkdir(parents=True, exist_ok=True)
        suffix = _guess_suffix(candidate)
        destination = incoming_dir / f"{uuid4().hex[:12]}{suffix}"
        await self.bot.download(candidate.telegram_file_id, destination=destination)
        return replace(candidate, local_path=destination)

    async def _load_current_draft(
        self,
        chat_id: int,
        user_id: int | None,
        *,
        draft_id: str | None = None,
    ) -> dict | None:
        pointer = self._current_drafts.get((chat_id, user_id))
        target_draft_id = draft_id or (pointer[0] if pointer else None)
        if target_draft_id is None:
            return None
        try:
            draft = await asyncio.to_thread(
                self.processing_service.load_batch_draft,
                draft_id=target_draft_id,
                owner=_build_telegram_owner(chat_id, user_id),
            )
        except Exception as exc:
            if _is_draft_missing_or_stale_error(exc):
                self._forget_draft(chat_id, user_id)
                return None
            raise
        self._remember_draft(chat_id, user_id, draft)
        return draft

    def _remember_draft(self, chat_id: int, user_id: int | None, draft: dict) -> None:
        draft_id = draft.get("draft_id")
        version = draft.get("version")
        if not isinstance(draft_id, str) or version is None:
            return
        self._current_drafts[(chat_id, user_id)] = (draft_id, int(version))
        if draft.get("status") != "open":
            self._forget_draft(chat_id, user_id)

    def _forget_draft(self, chat_id: int, user_id: int | None) -> None:
        pointer = self._current_drafts.pop((chat_id, user_id), None)
        if pointer is None:
            return
        draft_id, _ = pointer
        for key in [key for key in self._item_callback_tokens if key[0] == draft_id]:
            self._item_callback_tokens.pop(key, None)

    def _build_draft_keyboard(self, draft: dict, *, locale: SupportedLocale) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(
            text=self.locale_service.text(TelegramTextKey.MODE_BUTTON_ENABLED, locale=locale),
            callback_data="mode:batch:toggle",
        )
        draft_id = str(draft["draft_id"])
        version = int(draft["version"])
        builder.button(
            text=self.locale_service.text(TelegramTextKey.BASKET_BUTTON_PROCESS, locale=locale),
            callback_data=f"b:s:{draft_id}:{version}",
        )
        builder.button(
            text=self.locale_service.text(TelegramTextKey.BASKET_BUTTON_CLEAR, locale=locale),
            callback_data=f"b:c:{draft_id}:{version}",
        )
        for index, item in enumerate(_draft_items(draft), start=1):
            item_id = str(item["item_id"])
            token = self._callback_token_for_item(draft_id, item_id)
            builder.button(
                text=self.locale_service.text(TelegramTextKey.BASKET_BUTTON_REMOVE, locale=locale, index=index),
                callback_data=f"b:r:{draft_id}:{version}:{token}",
            )
        builder.adjust(1, 2, *([1] * len(_draft_items(draft))))
        return builder.as_markup()

    def _callback_token_for_item(self, draft_id: str, item_id: str) -> str:
        for (known_draft_id, token), known_item_id in self._item_callback_tokens.items():
            if known_draft_id == draft_id and known_item_id == item_id:
                return token
        token = uuid4().hex[:12]
        self._item_callback_tokens[(draft_id, token)] = item_id
        return token

    async def _answer_draft_error(self, callback: CallbackQuery, exc: Exception, *, locale: SupportedLocale) -> None:
        if _is_draft_stale_error(exc):
            await callback.answer(_draft_error_text(self.locale_service, locale, exc), show_alert=True)
            return
        raise exc

    async def _handle_get_transcript(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        locale = self._resolve_callback_locale(callback)
        try:
            _, job_id = callback.data.split(":", 1)
        except ValueError:
            await callback.answer(self.locale_service.text(TelegramTextKey.JOB_INVALID, locale=locale), show_alert=True)
            return
        try:
            job = await asyncio.to_thread(self.processing_service.load_job, job_id)
        except FileNotFoundError:
            await callback.answer(
                self.locale_service.text(TelegramTextKey.TRANSCRIPT_ARTIFACTS_NOT_FOUND, locale=locale),
                show_alert=True,
            )
            return

        await callback.answer(self.locale_service.text(TelegramTextKey.TRANSCRIPT_ACK, locale=locale))
        await callback.message.answer_document(
            FSInputFile(job.transcript.markdown_path),
            caption=self.locale_service.text(TelegramTextKey.TRANSCRIPT_SEGMENTS_CAPTION, locale=locale),
        )

    async def _handle_generate_report(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        locale = self._resolve_callback_locale(callback)
        try:
            _, job_id = callback.data.split(":", 1)
        except ValueError:
            await callback.answer(self.locale_service.text(TelegramTextKey.JOB_INVALID, locale=locale), show_alert=True)
            return
        lock = self.report_locks.setdefault(job_id, asyncio.Lock())
        if lock.locked():
            await callback.answer(self.locale_service.text(TelegramTextKey.REPORT_LOCKED, locale=locale), show_alert=False)
            return

        await _safe_callback_answer(callback, self.locale_service.text(TelegramTextKey.REPORT_ACK, locale=locale))
        status_message = await callback.message.answer(self.locale_service.text(TelegramTextKey.REPORT_STATUS, locale=locale))
        async with lock:
            try:
                job = await asyncio.to_thread(
                    self.processing_service.ensure_report,
                    job_id,
                    self.settings.report_prompt_suffix,
                )
            except Exception as exc:
                await status_message.edit_text(
                    self.locale_service.text(TelegramTextKey.REPORT_FAILED, locale=locale, error=exc)
                )
                return

        await status_message.edit_text(self.locale_service.text(TelegramTextKey.REPORT_READY, locale=locale))
        if job.report is None:
            await callback.message.answer(self.locale_service.text(TelegramTextKey.REPORT_MISSING, locale=locale))
            return
        if not job.report.job_id:
            await callback.message.answer(
                self.locale_service.text(TelegramTextKey.REPORT_MISSING_JOB_ID, locale=locale)
            )
            return
        await callback.message.answer_document(
            FSInputFile(job.report.markdown_path),
            caption=self.locale_service.text(TelegramTextKey.REPORT_CAPTION, locale=locale),
            reply_markup=_build_deep_research_keyboard(self.locale_service, locale, job.report.job_id),
        )

    async def _handle_deep_research(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        locale = self._resolve_callback_locale(callback)
        try:
            _, job_id = callback.data.split(":", 1)
        except ValueError:
            await callback.answer(self.locale_service.text(TelegramTextKey.JOB_INVALID, locale=locale), show_alert=True)
            return

        lock = self.deep_research_locks.setdefault(job_id, asyncio.Lock())
        if lock.locked():
            await callback.answer(
                self.locale_service.text(TelegramTextKey.DEEP_RESEARCH_LOCKED, locale=locale),
                show_alert=False,
            )
            return

        await _safe_callback_answer(callback, self.locale_service.text(TelegramTextKey.DEEP_RESEARCH_ACK, locale=locale))
        status_message = await callback.message.answer(
            self.locale_service.text(TelegramTextKey.DEEP_RESEARCH_STATUS, locale=locale)
        )
        async with lock:
            try:
                report_path = await asyncio.to_thread(self.processing_service.ensure_deep_research, job_id)
            except Exception as exc:
                await status_message.edit_text(
                    self.locale_service.text(TelegramTextKey.DEEP_RESEARCH_FAILED, locale=locale, error=exc)
                )
                return

        await status_message.edit_text(self.locale_service.text(TelegramTextKey.DEEP_RESEARCH_READY, locale=locale))
        await callback.message.answer_document(
            FSInputFile(report_path),
            caption=self.locale_service.text(TelegramTextKey.DEEP_RESEARCH_CAPTION, locale=locale),
        )

    async def _ensure_message_allowed(self, message: Message) -> bool:
        user_id = message.from_user.id if message.from_user else None
        if self._is_allowed_user(user_id):
            return True
        await message.answer(self.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=self._resolve_message_locale(message)))
        return False

    async def _ensure_callback_allowed(self, callback: CallbackQuery) -> bool:
        user_id = callback.from_user.id if callback.from_user else None
        if self._is_allowed_user(user_id):
            return True
        await callback.answer(
            self.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=self._resolve_callback_locale(callback)),
            show_alert=True,
        )
        return False

    def _is_allowed_user(self, user_id: int | None) -> bool:
        if not self.settings.allowed_user_ids:
            return True
        if user_id is None:
            return False
        return user_id in self.settings.allowed_user_ids


def _build_candidate_keyboard(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    selection_id: str,
    candidates: list[SourceCandidate],
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, candidate in enumerate(candidates):
        builder.button(
            text=_present_source_label_for_telegram(locale_service, locale, candidate.display_name)[:48],
            callback_data=f"pick:{selection_id}:{index}",
        )
    builder.adjust(1)
    return builder.as_markup()


def _build_main_keyboard(
    *,
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    batch_enabled: bool,
    draft: dict | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text=locale_service.text(
            TelegramTextKey.MODE_BUTTON_ENABLED if batch_enabled else TelegramTextKey.MODE_BUTTON_DISABLED,
            locale=locale,
        ),
        callback_data="mode:batch:toggle",
    )
    if _draft_items(draft):
        draft_id = str(draft["draft_id"])
        version = int(draft["version"])
        builder.button(
            text=locale_service.text(TelegramTextKey.BASKET_BUTTON_PROCESS, locale=locale),
            callback_data=f"b:s:{draft_id}:{version}",
        )
        builder.button(
            text=locale_service.text(TelegramTextKey.BASKET_BUTTON_CLEAR, locale=locale),
            callback_data=f"b:c:{draft_id}:{version}",
        )
    builder.adjust(1, 2)
    return builder.as_markup()


def _build_batch_mode_text(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    enabled: bool,
) -> str:
    return locale_service.text(
        TelegramTextKey.MODE_STATUS_ENABLED if enabled else TelegramTextKey.MODE_STATUS_DISABLED,
        locale=locale,
    )


def _build_draft_text(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    draft: dict | None,
    added_count: int | None = None,
) -> str:
    items = _draft_items(draft)
    parts: list[str] = []
    if added_count is not None:
        parts.append(locale_service.text(TelegramTextKey.BASKET_SUMMARY_ADDED, locale=locale, count=added_count))
    parts.append(locale_service.text(TelegramTextKey.BASKET_SUMMARY_HEADER, locale=locale, count=len(items)))
    parts.extend(
        locale_service.text(
            TelegramTextKey.BASKET_SUMMARY_ITEM,
            locale=locale,
            index=index,
            label=_present_source_label_for_telegram(locale_service, locale, _draft_item_display_name(item)),
        )
        for index, item in enumerate(items, start=1)
    )
    parts.append(locale_service.text(TelegramTextKey.BASKET_SUMMARY_FOOTER, locale=locale))
    return "\n".join(parts)


def _draft_items(draft: dict | None) -> list[dict]:
    if not isinstance(draft, dict):
        return []
    items = draft.get("items")
    return items if isinstance(items, list) else []


def _draft_item_display_name(item: dict) -> str:
    source = item.get("source") if isinstance(item, dict) else None
    if isinstance(source, dict):
        display_name = source.get("display_name") or source.get("url") or source.get("original_filename")
        if display_name:
            return str(display_name)
    return str(item.get("source_label") or item.get("item_id") or "")


def _build_telegram_owner(chat_id: int, user_id: int | None) -> dict[str, str]:
    return {
        "owner_type": "telegram",
        "telegram_chat_id": str(chat_id),
        "telegram_user_id": str(user_id if user_id is not None else 0),
    }


def _parse_draft_callback(data: str) -> tuple[str, str, int, str | None] | None:
    parts = data.split(":")
    if len(parts) not in {4, 5} or parts[0] != "b":
        return None
    _, command, draft_id, version_raw, *rest = parts
    if command not in {"s", "c", "r"}:
        return None
    if command == "r" and not rest:
        return None
    if command != "r" and rest:
        return None
    try:
        version = int(version_raw)
    except ValueError:
        return None
    return command, draft_id, version, rest[0] if rest else None


def _stale_button_text(locale_service: TelegramLocaleService, locale: SupportedLocale) -> str:
    return locale_service.text(TelegramTextKey.BASKET_STALE_BUTTON, locale=locale)


def _draft_error_text(locale_service: TelegramLocaleService, locale: SupportedLocale, exc: Exception) -> str:
    code = getattr(exc, "code", None)
    if code == "draft_empty":
        return locale_service.text(TelegramTextKey.BASKET_EMPTY, locale=locale)
    if code == "version_conflict":
        return locale_service.text(TelegramTextKey.BASKET_VERSION_CONFLICT, locale=locale)
    if code in {"draft_submitted", "draft_expired", "draft_canceled", "draft_owner_mismatch", "batch_draft_not_found"}:
        return _stale_button_text(locale_service, locale)
    return locale_service.text(TelegramTextKey.BASKET_UPDATE_FAILED, locale=locale, error=exc)


def _is_draft_stale_error(exc: Exception) -> bool:
    return getattr(exc, "code", None) in {
        "version_conflict",
        "draft_submitted",
        "draft_expired",
        "draft_canceled",
        "draft_owner_mismatch",
        "batch_draft_not_found",
        "draft_empty",
    }


def _is_draft_missing_or_stale_error(exc: Exception) -> bool:
    return getattr(exc, "code", None) in {
        "draft_submitted",
        "draft_expired",
        "draft_canceled",
        "draft_owner_mismatch",
        "batch_draft_not_found",
    }


def _with_stable_source_id(candidate: SourceCandidate, *, message_id: int | None) -> SourceCandidate:
    if candidate.url:
        return replace(candidate, source_id=f"url:{candidate.url}")
    if message_id is not None:
        seed = candidate.file_name or candidate.telegram_file_id or candidate.file_unique_id or candidate.source_id
        return replace(candidate, source_id=f"telegram-message:{message_id}:{seed}")
    if candidate.file_name:
        return replace(candidate, source_id=f"telegram-file-name:{candidate.file_name}")
    if candidate.telegram_file_id:
        return replace(candidate, source_id=f"telegram-file-id:{candidate.telegram_file_id}")
    return candidate


async def _safe_callback_answer(callback: CallbackQuery, text: str, show_alert: bool = False) -> None:
    try:
        await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest as exc:
        description = str(exc).lower()
        if "query is too old" in description or "query id is invalid" in description:
            return
        raise


def _build_result_keyboard(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    job_id: str,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=locale_service.text(TelegramTextKey.RESULT_BUTTON_SEGMENTS, locale=locale), callback_data=f"get:{job_id}")
    builder.button(text=locale_service.text(TelegramTextKey.RESULT_BUTTON_REPORT, locale=locale), callback_data=f"report:{job_id}")
    builder.adjust(1)
    return builder.as_markup()


def _build_processing_status_text(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    candidate: SourceCandidate,
) -> str:
    humanized_source = _present_source_label_for_telegram(locale_service, locale, candidate.display_name)
    if candidate.kind in {"telegram_audio", "telegram_video"}:
        return locale_service.text(
            TelegramTextKey.PROCESSING_STATUS_MEDIA,
            locale=locale,
            label=humanized_source,
        )
    return locale_service.text(
        TelegramTextKey.PROCESSING_STATUS_LINK,
        locale=locale,
        label=humanized_source,
    )


def _build_deep_research_keyboard(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    job_id: str,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text=locale_service.text(TelegramTextKey.REPORT_BUTTON_DEEP_RESEARCH, locale=locale),
        callback_data=f"deep:{job_id}",
    )
    builder.adjust(1)
    return builder.as_markup()


def _build_group_processing_status_text(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    candidates: list[SourceCandidate],
) -> str:
    attachment_count = len(candidates)
    return locale_service.text(
        TelegramTextKey.PROCESSING_STATUS_GROUP,
        locale=locale,
        count=attachment_count,
    )


def _build_draft_processing_status_text(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    draft: dict | None,
) -> str:
    attachment_count = len(_draft_items(draft))
    return locale_service.text(
        TelegramTextKey.PROCESSING_STATUS_BASKET,
        locale=locale,
        count=attachment_count,
    )


def _present_source_label_for_telegram(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    source_label: str,
) -> str:
    normalized = source_label.strip()
    if not normalized:
        return locale_service.text(TelegramTextKey.SOURCE_LABEL_UNKNOWN, locale=locale)

    if normalized.startswith("Audio:"):
        value = normalized.split(":", 1)[1].strip()
        if not value or looks_like_machine_file_name(value):
            return locale_service.text(TelegramTextKey.SOURCE_LABEL_TELEGRAM_AUDIO, locale=locale)
    if normalized.startswith("Video:"):
        value = normalized.split(":", 1)[1].strip()
        if not value or looks_like_machine_file_name(value):
            return locale_service.text(TelegramTextKey.SOURCE_LABEL_TELEGRAM_VIDEO, locale=locale)

    humanized = humanize_source_label(normalized)
    if humanized == locale_service.text(TelegramTextKey.SOURCE_LABEL_UNKNOWN, locale="ru"):
        return locale_service.text(TelegramTextKey.SOURCE_LABEL_UNKNOWN, locale=locale)
    return humanized


def _append_rejected_urls(
    locale_service: TelegramLocaleService,
    locale: SupportedLocale,
    base_message: str,
    rejected_urls: list[str],
) -> str:
    return (
        f"{base_message}\n\n"
        f"{locale_service.text(TelegramTextKey.BASKET_REJECTED_LINKS_HEADER, locale=locale)}\n"
        + "\n".join(f"- {item}" for item in rejected_urls)
    )


def _extract_attachments(message: Message) -> list[MediaAttachment]:
    attachments: list[MediaAttachment] = []

    if message.audio:
        attachments.append(
            MediaAttachment(
                telegram_file_id=message.audio.file_id,
                kind="telegram_audio",
                file_name=message.audio.file_name or f"{message.audio.file_unique_id}.mp3",
                mime_type=message.audio.mime_type,
                file_unique_id=message.audio.file_unique_id,
            )
        )
    if message.voice:
        attachments.append(
            MediaAttachment(
                telegram_file_id=message.voice.file_id,
                kind="telegram_audio",
                file_name=f"{message.voice.file_unique_id}.ogg",
                mime_type="audio/ogg",
                file_unique_id=message.voice.file_unique_id,
            )
        )
    if message.video:
        attachments.append(
            MediaAttachment(
                telegram_file_id=message.video.file_id,
                kind="telegram_video",
                file_name=message.video.file_name or f"{message.video.file_unique_id}.mp4",
                mime_type=message.video.mime_type,
                file_unique_id=message.video.file_unique_id,
            )
        )
    if message.video_note:
        attachments.append(
            MediaAttachment(
                telegram_file_id=message.video_note.file_id,
                kind="telegram_video",
                file_name=f"{message.video_note.file_unique_id}.mp4",
                mime_type="video/mp4",
                file_unique_id=message.video_note.file_unique_id,
            )
        )
    if message.document:
        if message.document.mime_type and message.document.mime_type.startswith("audio/"):
            attachments.append(
                MediaAttachment(
                    telegram_file_id=message.document.file_id,
                    kind="telegram_audio",
                    file_name=message.document.file_name or f"{message.document.file_unique_id}.bin",
                    mime_type=message.document.mime_type,
                    file_unique_id=message.document.file_unique_id,
                )
            )
        elif message.document.mime_type and message.document.mime_type.startswith("video/"):
            attachments.append(
                MediaAttachment(
                    telegram_file_id=message.document.file_id,
                    kind="telegram_video",
                    file_name=message.document.file_name or f"{message.document.file_unique_id}.bin",
                    mime_type=message.document.mime_type,
                    file_unique_id=message.document.file_unique_id,
                )
            )
        else:
            attachments.append(
                MediaAttachment(
                    telegram_file_id=message.document.file_id,
                    kind="telegram_audio",
                    file_name=message.document.file_name or f"{message.document.file_unique_id}.bin",
                    mime_type=message.document.mime_type,
                    file_unique_id=message.document.file_unique_id,
                )
            )

    return attachments


def _looks_like_command_menu_request(text: str) -> bool:
    stripped = text.strip()
    return stripped == "/" or (stripped.startswith("/") and not stripped.startswith("//"))


def _guess_suffix(candidate: SourceCandidate) -> str:
    if candidate.file_name and "." in candidate.file_name:
        return Path(candidate.file_name).suffix
    if candidate.mime_type == "audio/ogg":
        return ".ogg"
    if candidate.mime_type == "audio/mpeg":
        return ".mp3"
    if candidate.kind == "telegram_video":
        return ".mp4"
    return ".bin"
