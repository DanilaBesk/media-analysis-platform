from __future__ import annotations

import asyncio
import logging
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
from transcriber_workers_common.source_labels import humanize_source_label
from transcriber_workers_common.source_extractor import extract_sources

LOGGER = logging.getLogger(__name__)
HANDLE_MEDIA_MARKER = "[TelegramAdapter][handleMedia][BLOCK_SUBMIT_AND_WAIT_FOR_JOB]"
TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS = 3500

TELEGRAM_COMMANDS = (
    BotCommand(command="start", description="Показать основное меню"),
    BotCommand(command="help", description="Как пользоваться ботом"),
    BotCommand(command="batch", description="Включить или выключить batch-режим"),
    BotCommand(command="basket", description="Показать текущую корзину"),
    BotCommand(command="clear", description="Очистить корзину"),
)


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
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.selection_store = CandidateSelectionStore()
        self._current_drafts: dict[tuple[int, int | None], tuple[str, int]] = {}
        self._item_callback_tokens: dict[tuple[str, str], str] = {}
        self.batch_modes = BatchModeStore()
        self.media_groups = MediaGroupAccumulator()
        self.media_group_tasks: dict[tuple[int, str], asyncio.Task[None]] = {}
        self.media_group_text: dict[tuple[int, str], str] = {}
        self.basket_summary_tasks: dict[tuple[int, int | None], asyncio.Task[None]] = {}
        self.basket_summary_added_counts: dict[tuple[int, int | None], int] = {}
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
        await self.bot.set_my_commands(list(TELEGRAM_COMMANDS), scope=BotCommandScopeDefault())
        await self.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    async def _handle_start(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        draft = await self._load_current_draft(message.chat.id, user_id)
        await message.answer(
            "Отправьте voice/audio/video/document или ссылку. В batch-режиме я собираю источники в корзину и запускаю одну общую задачу.",
            reply_markup=_build_main_keyboard(
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                draft=draft,
            ),
        )

    async def _handle_help(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        await self._send_command_menu(chat_id=message.chat.id, user_id=user_id, answer=message.answer)

    async def _handle_batch_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        enabled = self.batch_modes.toggle(message.chat.id, user_id)
        draft = await self._load_current_draft(message.chat.id, user_id)
        await message.answer(
            _build_batch_mode_text(enabled),
            reply_markup=_build_main_keyboard(
                batch_enabled=enabled,
                draft=draft,
            ),
        )

    async def _handle_basket_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        self._cancel_pending_basket_summary(message.chat.id, user_id)
        draft = await self._load_current_draft(message.chat.id, user_id)
        text = _build_draft_text(draft) if _draft_items(draft) else "Корзина пуста."
        await message.answer(
            text,
            reply_markup=self._build_draft_keyboard(draft)
            if _draft_items(draft)
            else _build_main_keyboard(
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                draft=draft,
            ),
        )

    async def _handle_clear_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
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
                    await message.answer(f"Не удалось очистить корзину: {exc}")
                    return
        await message.answer(
            "Корзина очищена.",
            reply_markup=_build_main_keyboard(
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

        if _looks_like_command_menu_request(text) and not attachments:
            await self._send_command_menu(chat_id=message.chat.id, user_id=user_id, answer=message.answer)
            return

        if message.media_group_id and attachments:
            key = (message.chat.id, message.media_group_id)
            for attachment in attachments:
                self.media_groups.add(message.chat.id, message.media_group_id, attachment)
            if text and not self.media_group_text.get(key):
                self.media_group_text[key] = text

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
        )

    async def _send_command_menu(self, chat_id: int, user_id: int | None, answer) -> None:
        draft = await self._load_current_draft(chat_id, user_id)
        await answer(
            "Команды доступны в меню Telegram рядом с полем ввода:\n"
            "/start - основное меню\n"
            "/help - помощь\n"
            "/batch - включить или выключить batch-режим\n"
            "/basket - показать корзину\n"
            "/clear - очистить корзину\n\n"
            "Корзина собирает несколько источников перед общим batch-запуском. "
            "Кнопками можно убрать источник, очистить корзину или запустить одну batch-транскрибацию. "
            "Кнопка batch включает сбор корзины или выключает его для одиночной обработки.",
            reply_markup=_build_main_keyboard(
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
        self.media_group_tasks.pop(key, None)
        await self._process_candidate_set(
            chat_id=chat_id,
            user_id=user_id,
            text=text,
            attachments=attachments,
            message_id=None,
        )

    async def _process_candidate_set(
        self,
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list[MediaAttachment],
        message_id: int | None = None,
    ) -> None:
        result = extract_sources(text=text, attachments=attachments)
        if not result.candidates:
            message = "Не нашёл поддерживаемых источников. Поддерживается YouTube, URL и Telegram media/document."
            if result.rejected_urls:
                message += "\n\nНеподдерживаемые ссылки:\n" + "\n".join(f"- {item}" for item in result.rejected_urls)
            await self.bot.send_message(chat_id, message)
            return

        candidates = [_with_stable_source_id(candidate, message_id=message_id) for candidate in result.candidates]
        if not self.batch_modes.is_enabled(chat_id, user_id):
            self._cancel_pending_basket_summary(chat_id, user_id)
            if len(candidates) == 1:
                await self.bot.send_message(chat_id, "Batch-режим выключен. Запускаю одиночную обработку.")
                await self._start_processing(chat_id, candidates[0])
                return

            selection_id = self.selection_store.save(chat_id=chat_id, user_id=user_id, candidates=candidates)
            message = "Batch-режим выключен. Выберите один источник для обработки:"
            if result.rejected_urls:
                message += "\n\nПропущены неподдерживаемые ссылки:\n" + "\n".join(f"- {item}" for item in result.rejected_urls)
            await self.bot.send_message(chat_id, message, reply_markup=_build_candidate_keyboard(selection_id, candidates))
            return

        should_debounce = len(candidates) == 1 and not result.rejected_urls
        if should_debounce:
            self._mark_basket_summary_added(chat_id, user_id, added_count=1)

        try:
            draft = await self._add_candidates_to_draft(chat_id=chat_id, user_id=user_id, candidates=candidates)
        except Exception as exc:
            if should_debounce:
                self._cancel_pending_basket_summary(chat_id, user_id)
            await self.bot.send_message(chat_id, f"Не удалось обновить корзину: {exc}")
            return

        message = _build_draft_text(draft, added_count=len(candidates))
        if result.rejected_urls:
            message += "\n\nПропущены неподдерживаемые ссылки:\n" + "\n".join(f"- {item}" for item in result.rejected_urls)

        if should_debounce:
            self.basket_summary_tasks[(chat_id, user_id)] = asyncio.create_task(
                self._flush_basket_summary(chat_id, user_id)
            )
            return

        self._cancel_pending_basket_summary(chat_id, user_id)
        await self.bot.send_message(chat_id, message, reply_markup=self._build_draft_keyboard(draft))

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
                display_name="Telegram basket",
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

    def _schedule_basket_summary(self, chat_id: int, user_id: int | None, *, added_count: int) -> None:
        self._mark_basket_summary_added(chat_id, user_id, added_count=added_count)
        self.basket_summary_tasks[(chat_id, user_id)] = asyncio.create_task(self._flush_basket_summary(chat_id, user_id))

    def _mark_basket_summary_added(self, chat_id: int, user_id: int | None, *, added_count: int) -> None:
        key = (chat_id, user_id)
        self.basket_summary_added_counts[key] = self.basket_summary_added_counts.get(key, 0) + added_count
        existing = self.basket_summary_tasks.get(key)
        if existing and not existing.done():
            existing.cancel()

    def _cancel_pending_basket_summary(self, chat_id: int, user_id: int | None) -> None:
        key = (chat_id, user_id)
        existing = self.basket_summary_tasks.pop(key, None)
        if existing and not existing.done():
            existing.cancel()
        self.basket_summary_added_counts.pop(key, None)

    async def _flush_basket_summary(self, chat_id: int, user_id: int | None) -> None:
        key = (chat_id, user_id)
        try:
            await asyncio.sleep(self.settings.media_group_window_seconds)
        except asyncio.CancelledError:
            return

        self.basket_summary_tasks.pop(key, None)
        added_count = self.basket_summary_added_counts.pop(key, 0)
        draft = await self._load_current_draft(chat_id, user_id)
        if not _draft_items(draft):
            return
        await self.bot.send_message(
            chat_id,
            _build_draft_text(draft, added_count=added_count or None),
            reply_markup=self._build_draft_keyboard(draft),
        )

    async def _handle_source_selection(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return

        try:
            _, selection_id, index_raw = callback.data.split(":", 2)
            index = int(index_raw)
        except ValueError:
            await callback.answer("Некорректный выбор источника.", show_alert=True)
            return

        candidate = self.selection_store.get(
            selection_id=selection_id,
            chat_id=callback.message.chat.id,
            user_id=callback.from_user.id if callback.from_user else None,
            index=index,
        )
        if candidate is None:
            await callback.answer("Источник уже недоступен.", show_alert=True)
            return

        await callback.answer("Запускаю обработку...")
        await self._start_processing(callback.message.chat.id, candidate)

    async def _handle_batch_mode_toggle(self, callback: CallbackQuery) -> None:
        if callback.message is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return

        chat_id = callback.message.chat.id
        user_id = callback.from_user.id if callback.from_user else None
        enabled = self.batch_modes.toggle(chat_id, user_id)
        draft = await self._load_current_draft(chat_id, user_id)
        await _safe_callback_answer(callback, _build_batch_mode_text(enabled))
        await callback.message.edit_text(
            _build_batch_mode_text(enabled),
            reply_markup=_build_main_keyboard(batch_enabled=enabled, draft=draft),
        )

    async def _handle_basket_callback(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return

        chat_id = callback.message.chat.id
        user_id = callback.from_user.id if callback.from_user else None
        if callback.data.startswith("basket:"):
            await callback.answer(_stale_button_text(), show_alert=True)
            return

        parsed = _parse_draft_callback(callback.data)
        if parsed is None:
            await callback.answer("Некорректное действие корзины.", show_alert=True)
            return

        command, draft_id, expected_version, token = parsed
        owner = _build_telegram_owner(chat_id, user_id)

        if command == "s":
            self._cancel_pending_basket_summary(chat_id, user_id)
            await _safe_callback_answer(callback, "Запускаю batch-транскрибацию...")
            await self._start_basket_processing(
                chat_id=chat_id,
                user_id=user_id,
                draft_id=draft_id,
                expected_version=expected_version,
            )
            return

        if command == "c":
            self._cancel_pending_basket_summary(chat_id, user_id)
            try:
                draft = await asyncio.to_thread(
                    self.processing_service.clear_batch_draft,
                    draft_id=draft_id,
                    owner=owner,
                    expected_version=expected_version,
                )
            except Exception as exc:
                await self._answer_draft_error(callback, exc)
                return
            self._remember_draft(chat_id, user_id, draft)
            await _safe_callback_answer(callback, "Корзина очищена.")
            await callback.message.edit_text("Корзина очищена.")
            return

        if command == "r" and token:
            item_id = self._item_callback_tokens.get((draft_id, token))
            if item_id is None:
                await callback.answer(_stale_button_text(), show_alert=True)
                return
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
                await self._answer_draft_error(callback, exc)
                return
            self._remember_draft(chat_id, user_id, draft)
            await _safe_callback_answer(callback, "Источник удалён.")
            if _draft_items(draft):
                await callback.message.edit_text(_build_draft_text(draft), reply_markup=self._build_draft_keyboard(draft))
            else:
                await callback.message.edit_text("Корзина пуста.")
            return

        await callback.answer("Некорректное действие корзины.", show_alert=True)

    async def _start_processing(self, chat_id: int, candidate: SourceCandidate) -> None:
        status_message = await self.bot.send_message(chat_id, _build_processing_status_text(candidate))
        try:
            prepared_candidate = await self._download_attachment_if_needed(candidate)
            LOGGER.info("%s mode=single source_kind=%s", HANDLE_MEDIA_MARKER, prepared_candidate.kind)
            job = await asyncio.to_thread(self.processing_service.process_source, prepared_candidate)
        except Exception as exc:
            await status_message.edit_text(f"Не удалось обработать источник: {exc}")
            return

        await status_message.edit_text("Транскрибация готова.")
        await self._send_transcript_result(
            status_message,
            job,
            fallback_caption="Готовая цельная транскрибация без сегментов.",
        )

    async def _start_processing_group(self, chat_id: int, candidates: list[SourceCandidate]) -> None:
        status_message = await self.bot.send_message(chat_id, _build_group_processing_status_text(candidates))
        try:
            prepared_candidates = [await self._download_attachment_if_needed(candidate) for candidate in candidates]
            LOGGER.info("%s mode=combined source_count=%s", HANDLE_MEDIA_MARKER, len(prepared_candidates))
            job = await asyncio.to_thread(self.processing_service.process_source_group, prepared_candidates)
        except Exception as exc:
            await status_message.edit_text(f"Не удалось обработать источники: {exc}")
            return

        await status_message.edit_text("Транскрибация готова.")
        await self._send_transcript_result(
            status_message,
            job,
            fallback_caption="Готовая цельная транскрибация без сегментов.",
        )

    async def _start_basket_processing(
        self,
        *,
        chat_id: int,
        user_id: int | None,
        draft_id: str,
        expected_version: int,
    ) -> None:
        owner = _build_telegram_owner(chat_id, user_id)
        draft = await self._load_current_draft(chat_id, user_id, draft_id=draft_id)
        if draft is not None and not _draft_items(draft):
            await self.bot.send_message(chat_id, "Корзина пуста.")
            return

        status_message = await self.bot.send_message(chat_id, _build_draft_processing_status_text(draft))
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
                await status_message.edit_text(_draft_error_text(exc))
                return
            await status_message.edit_text(f"Не удалось обработать корзину: {exc}")
            return

        self._forget_draft(chat_id, user_id)
        await status_message.edit_text("Пакетная транскрибация готова.")
        await self._send_transcript_result(
            status_message,
            job,
            fallback_caption="Готовая пакетная транскрибация без сегментов.",
        )

    async def _send_transcript_result(self, status_message: Message, job: ProcessedJob, *, fallback_caption: str) -> None:
        reply_markup = _build_result_keyboard(job.job_id)
        text_path = job.transcript.text_path
        if not text_path.is_file():
            await status_message.answer(
                "Транскрибация готова, но текстовый файл артефакта недоступен.",
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

    def _build_draft_keyboard(self, draft: dict) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="Batch: включен", callback_data="mode:batch:toggle")
        draft_id = str(draft["draft_id"])
        version = int(draft["version"])
        builder.button(text="Запустить batch", callback_data=f"b:s:{draft_id}:{version}")
        builder.button(text="Очистить", callback_data=f"b:c:{draft_id}:{version}")
        for index, item in enumerate(_draft_items(draft), start=1):
            item_id = str(item["item_id"])
            token = self._callback_token_for_item(draft_id, item_id)
            builder.button(text=f"Убрать {index}", callback_data=f"b:r:{draft_id}:{version}:{token}")
        builder.adjust(1, 2, *([1] * len(_draft_items(draft))))
        return builder.as_markup()

    def _callback_token_for_item(self, draft_id: str, item_id: str) -> str:
        for (known_draft_id, token), known_item_id in self._item_callback_tokens.items():
            if known_draft_id == draft_id and known_item_id == item_id:
                return token
        token = uuid4().hex[:12]
        self._item_callback_tokens[(draft_id, token)] = item_id
        return token

    async def _answer_draft_error(self, callback: CallbackQuery, exc: Exception) -> None:
        if _is_draft_stale_error(exc):
            await callback.answer(_draft_error_text(exc), show_alert=True)
            return
        raise exc

    async def _handle_get_transcript(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        try:
            _, job_id = callback.data.split(":", 1)
        except ValueError:
            await callback.answer("Некорректный идентификатор задачи.", show_alert=True)
            return
        try:
            job = await asyncio.to_thread(self.processing_service.load_job, job_id)
        except FileNotFoundError:
            await callback.answer("Артефакты не найдены.", show_alert=True)
            return

        await callback.answer("Отправляю транскрибацию по сегментам...")
        await callback.message.answer_document(
            FSInputFile(job.transcript.markdown_path),
            caption="Транскрибация по сегментам в формате Markdown.",
        )

    async def _handle_generate_report(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        try:
            _, job_id = callback.data.split(":", 1)
        except ValueError:
            await callback.answer("Некорректный идентификатор задачи.", show_alert=True)
            return
        lock = self.report_locks.setdefault(job_id, asyncio.Lock())
        if lock.locked():
            await callback.answer("Отчёт уже формируется, дождитесь завершения.", show_alert=False)
            return

        await _safe_callback_answer(callback, "Запускаю генерацию отчёта...")
        status_message = await callback.message.answer("Готовлю исследовательский отчёт через настроенную AI-упряжку...")
        async with lock:
            try:
                job = await asyncio.to_thread(
                    self.processing_service.ensure_report,
                    job_id,
                    self.settings.report_prompt_suffix,
                )
            except Exception as exc:
                await status_message.edit_text(f"Не удалось сформировать отчёт: {exc}")
                return

        await status_message.edit_text("Исследовательский отчёт готов.")
        if job.report is None:
            await callback.message.answer("Отчёт не был сохранён.")
            return
        if not job.report.job_id:
            await callback.message.answer("Отчёт сохранён без идентификатора report job; глубокое исследование недоступно.")
            return
        await callback.message.answer_document(
            FSInputFile(job.report.markdown_path),
            caption="Исследовательский отчёт в формате Markdown.",
            reply_markup=_build_deep_research_keyboard(job.report.job_id),
        )

    async def _handle_deep_research(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return
        try:
            _, job_id = callback.data.split(":", 1)
        except ValueError:
            await callback.answer("Некорректный идентификатор задачи.", show_alert=True)
            return

        lock = self.deep_research_locks.setdefault(job_id, asyncio.Lock())
        if lock.locked():
            await callback.answer("Глубокое исследование уже запущено, дождитесь завершения.", show_alert=False)
            return

        await _safe_callback_answer(callback, "Запускаю глубокое исследование...")
        status_message = await callback.message.answer(
            "Готовлю глубокое исследование по evidence-first пайплайну. Это может занять заметно больше времени."
        )
        async with lock:
            try:
                report_path = await asyncio.to_thread(self.processing_service.ensure_deep_research, job_id)
            except Exception as exc:
                await status_message.edit_text(f"Не удалось запустить глубокое исследование: {exc}")
                return

        await status_message.edit_text("Глубокое исследование готово.")
        await callback.message.answer_document(
            FSInputFile(report_path),
            caption="Глубокое исследование в формате Markdown.",
        )

    async def _ensure_message_allowed(self, message: Message) -> bool:
        user_id = message.from_user.id if message.from_user else None
        if self._is_allowed_user(user_id):
            return True
        await message.answer("Доступ к этому боту ограничен.")
        return False

    async def _ensure_callback_allowed(self, callback: CallbackQuery) -> bool:
        user_id = callback.from_user.id if callback.from_user else None
        if self._is_allowed_user(user_id):
            return True
        await callback.answer("Доступ к этому боту ограничен.", show_alert=True)
        return False

    def _is_allowed_user(self, user_id: int | None) -> bool:
        if not self.settings.allowed_user_ids:
            return True
        if user_id is None:
            return False
        return user_id in self.settings.allowed_user_ids


def _build_candidate_keyboard(selection_id: str, candidates: list[SourceCandidate]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, candidate in enumerate(candidates):
        builder.button(text=candidate.display_name[:48], callback_data=f"pick:{selection_id}:{index}")
    builder.adjust(1)
    return builder.as_markup()


def _build_main_keyboard(*, batch_enabled: bool, draft: dict | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Batch: включен" if batch_enabled else "Batch: выключен",
        callback_data="mode:batch:toggle",
    )
    if _draft_items(draft):
        draft_id = str(draft["draft_id"])
        version = int(draft["version"])
        builder.button(text="Запустить batch", callback_data=f"b:s:{draft_id}:{version}")
        builder.button(text="Очистить корзину", callback_data=f"b:c:{draft_id}:{version}")
    builder.adjust(1, 2)
    return builder.as_markup()


def _build_batch_mode_text(enabled: bool) -> str:
    if enabled:
        return "Batch-режим включен. Новые источники будут добавляться в корзину для одного общего запуска."
    return "Batch-режим выключен. Один источник будет обрабатываться сразу; для нескольких источников появится выбор."


def _build_draft_text(draft: dict | None, added_count: int | None = None) -> str:
    items = _draft_items(draft)
    prefix = f"Добавлено источников: {added_count}.\n" if added_count is not None else ""
    lines = [
        f"{index}. {humanize_source_label(_draft_item_display_name(item))}"
        for index, item in enumerate(items, start=1)
    ]
    return (
        f"{prefix}В корзине {len(items)} источника(ов):\n"
        + "\n".join(lines)
        + "\n\nЗапуск создаст одну batch-задачу в API."
    )


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
    return str(item.get("source_label") or item.get("item_id") or "Источник")


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


def _stale_button_text() -> str:
    return "Эта кнопка устарела. Откройте /basket или отправьте новый источник."


def _draft_error_text(exc: Exception) -> str:
    code = getattr(exc, "code", None)
    if code == "draft_empty":
        return "Корзина пуста."
    if code == "version_conflict":
        return "Корзина изменилась. Откройте актуальную корзину через /basket."
    if code in {"draft_submitted", "draft_expired", "draft_canceled", "draft_owner_mismatch", "batch_draft_not_found"}:
        return _stale_button_text()
    return f"Не удалось обновить корзину: {exc}"


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


def _build_result_keyboard(job_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Получить по сегментам", callback_data=f"get:{job_id}")
    builder.button(text="Создать исследовательский отчет", callback_data=f"report:{job_id}")
    builder.adjust(1)
    return builder.as_markup()


def _build_processing_status_text(candidate: SourceCandidate) -> str:
    humanized_source = humanize_source_label(candidate.display_name)
    if candidate.kind in {"telegram_audio", "telegram_video"}:
        return (
            f"Обрабатываю источник: {humanized_source}\n\n"
            "Скачиваю файл из Telegram и запускаю транскрибацию. Это может занять несколько минут."
        )
    return (
        f"Обрабатываю источник: {humanized_source}\n\n"
        "Проверяю доступные субтитры и при необходимости запущу транскрибацию."
    )


def _build_deep_research_keyboard(job_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Запустить глубокое исследование", callback_data=f"deep:{job_id}")
    builder.adjust(1)
    return builder.as_markup()


def _build_group_processing_status_text(candidates: list[SourceCandidate]) -> str:
    attachment_count = len(candidates)
    return (
        f"Обрабатываю корзину: {attachment_count} источника(ов).\n\n"
        "Скачиваю Telegram-файлы и отправляю весь набор одним batch-запросом в API. Это может занять несколько минут."
    )


def _build_draft_processing_status_text(draft: dict | None) -> str:
    attachment_count = len(_draft_items(draft))
    return (
        f"Обрабатываю корзину: {attachment_count} источника(ов).\n\n"
        "Отправляю API draft на batch-транскрибацию. Это может занять несколько минут."
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
