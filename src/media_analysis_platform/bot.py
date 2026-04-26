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
from aiogram.types import BotCommand, CallbackQuery, FSInputFile, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from media_analysis_platform.config import Settings
from transcriber_workers_common.domain import MediaAttachment, ProcessedJob, SourceCandidate
from transcriber_workers_common.media_groups import MediaGroupAccumulator
from transcriber_workers_common.source_labels import humanize_source_label
from transcriber_workers_common.source_extractor import extract_sources

LOGGER = logging.getLogger(__name__)
HANDLE_MEDIA_MARKER = "[TelegramAdapter][handleMedia][BLOCK_SUBMIT_AND_WAIT_FOR_JOB]"

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
        self.baskets = BasketStore()
        self.batch_modes = BatchModeStore()
        self.media_groups = MediaGroupAccumulator()
        self.media_group_tasks: dict[tuple[int, str], asyncio.Task[None]] = {}
        self.media_group_text: dict[tuple[int, str], str] = {}
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
            self._handle_basket_callback, lambda call: bool(call.data and call.data.startswith("basket:"))
        )
        self.router.callback_query.register(self._handle_get_transcript, lambda call: bool(call.data and call.data.startswith("get:")))
        self.router.callback_query.register(
            self._handle_generate_report, lambda call: bool(call.data and call.data.startswith("report:"))
        )
        self.router.callback_query.register(
            self._handle_deep_research, lambda call: bool(call.data and call.data.startswith("deep:"))
        )

    async def _configure_commands(self) -> None:
        await self.bot.set_my_commands(list(TELEGRAM_COMMANDS))

    async def _handle_start(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Отправьте voice/audio/video/document или ссылку. В batch-режиме я собираю источники в корзину и запускаю одну общую задачу.",
            reply_markup=_build_main_keyboard(
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                basket=self.baskets.list(message.chat.id, user_id),
            ),
        )

    async def _handle_help(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        await message.answer(
            "Поддерживается корзина из voice/audio/video/document, YouTube и URL. "
            "Кнопками можно убрать источник, очистить корзину или запустить одну batch-транскрибацию. "
            "Кнопка batch включает сбор корзины или выключает его для одиночной обработки. "
            "После обработки бот отдаёт txt-артефакт, markdown по кнопке, отчёт и deep research от aggregate root/report job.",
            reply_markup=_build_main_keyboard(
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                basket=self.baskets.list(message.chat.id, user_id),
            ),
        )

    async def _handle_batch_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        enabled = self.batch_modes.toggle(message.chat.id, user_id)
        await message.answer(
            _build_batch_mode_text(enabled),
            reply_markup=_build_main_keyboard(
                batch_enabled=enabled,
                basket=self.baskets.list(message.chat.id, user_id),
            ),
        )

    async def _handle_basket_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        basket = self.baskets.list(message.chat.id, user_id)
        text = _build_basket_text(basket) if basket else "Корзина пуста."
        await message.answer(
            text,
            reply_markup=_build_main_keyboard(
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                basket=basket,
            ),
        )

    async def _handle_clear_command(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        user_id = message.from_user.id if message.from_user else None
        self.baskets.clear(message.chat.id, user_id)
        await message.answer(
            "Корзина очищена.",
            reply_markup=_build_main_keyboard(
                batch_enabled=self.batch_modes.is_enabled(message.chat.id, user_id),
                basket=[],
            ),
        )

    async def _handle_message(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        attachments = _extract_attachments(message)
        text = message.text or message.caption or ""

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
                    user_id=message.from_user.id if message.from_user else None,
                    media_group_id=message.media_group_id,
                )
            )
            return

        await self._process_candidate_set(
            chat_id=message.chat.id,
            user_id=message.from_user.id if message.from_user else None,
            text=text,
            attachments=attachments,
            message_id=getattr(message, "message_id", None),
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

        basket = self.baskets.add(chat_id=chat_id, user_id=user_id, candidates=candidates)
        message = _build_basket_text(basket, added_count=len(candidates))
        if result.rejected_urls:
            message += "\n\nПропущены неподдерживаемые ссылки:\n" + "\n".join(f"- {item}" for item in result.rejected_urls)
        await self.bot.send_message(chat_id, message, reply_markup=_build_basket_keyboard(basket))

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
        basket = self.baskets.list(chat_id, user_id)
        await _safe_callback_answer(callback, _build_batch_mode_text(enabled))
        await callback.message.edit_text(
            _build_batch_mode_text(enabled),
            reply_markup=_build_main_keyboard(batch_enabled=enabled, basket=basket),
        )

    async def _handle_basket_callback(self, callback: CallbackQuery) -> None:
        if callback.message is None or callback.data is None:
            return
        if not await self._ensure_callback_allowed(callback):
            return

        chat_id = callback.message.chat.id
        user_id = callback.from_user.id if callback.from_user else None
        action = callback.data.split(":", 2)
        command = action[1] if len(action) > 1 else ""

        if command == "start":
            await _safe_callback_answer(callback, "Запускаю batch-транскрибацию...")
            await self._start_basket_processing(chat_id=chat_id, user_id=user_id)
            return

        if command == "clear":
            self.baskets.clear(chat_id, user_id)
            await _safe_callback_answer(callback, "Корзина очищена.")
            await callback.message.edit_text("Корзина очищена.")
            return

        if command == "remove" and len(action) == 3:
            try:
                index = int(action[2])
            except ValueError:
                await callback.answer("Некорректный номер источника.", show_alert=True)
                return
            removed = self.baskets.remove(chat_id, user_id, index)
            if removed is None:
                await callback.answer("Источник уже недоступен.", show_alert=True)
                return
            await _safe_callback_answer(callback, "Источник удалён.")
            basket = self.baskets.list(chat_id, user_id)
            if basket:
                await callback.message.edit_text(_build_basket_text(basket), reply_markup=_build_basket_keyboard(basket))
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
        await status_message.answer_document(
            FSInputFile(job.transcript.text_path),
            caption="Готовая цельная транскрибация без сегментов.",
            reply_markup=_build_result_keyboard(job.job_id),
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
        await status_message.answer_document(
            FSInputFile(job.transcript.text_path),
            caption="Готовая цельная транскрибация без сегментов.",
            reply_markup=_build_result_keyboard(job.job_id),
        )

    async def _start_basket_processing(self, chat_id: int, user_id: int | None) -> None:
        candidates = self.baskets.list(chat_id, user_id)
        if not candidates:
            await self.bot.send_message(chat_id, "Корзина пуста.")
            return

        status_message = await self.bot.send_message(chat_id, _build_group_processing_status_text(candidates))
        try:
            prepared_candidates = [await self._download_attachment_if_needed(candidate) for candidate in candidates]
            LOGGER.info("%s mode=batch source_count=%s", HANDLE_MEDIA_MARKER, len(prepared_candidates))
            job = await asyncio.to_thread(self.processing_service.process_source_group, prepared_candidates)
        except Exception as exc:
            await status_message.edit_text(f"Не удалось обработать корзину: {exc}")
            return

        self.baskets.clear(chat_id, user_id)
        await status_message.edit_text("Пакетная транскрибация готова.")
        await status_message.answer_document(
            FSInputFile(job.transcript.text_path),
            caption="Готовая пакетная транскрибация без сегментов.",
            reply_markup=_build_result_keyboard(job.job_id),
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


def _build_main_keyboard(*, batch_enabled: bool, basket: list[SourceCandidate]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Batch: включен" if batch_enabled else "Batch: выключен",
        callback_data="mode:batch:toggle",
    )
    if basket:
        builder.button(text="Запустить batch", callback_data="basket:start")
        builder.button(text="Очистить корзину", callback_data="basket:clear")
    builder.adjust(1, 2)
    return builder.as_markup()


def _build_basket_keyboard(candidates: list[SourceCandidate]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Batch: включен", callback_data="mode:batch:toggle")
    builder.button(text="Запустить batch", callback_data="basket:start")
    builder.button(text="Очистить", callback_data="basket:clear")
    for index, candidate in enumerate(candidates):
        builder.button(text=f"Убрать {index + 1}", callback_data=f"basket:remove:{index}")
    builder.adjust(1, 2, *([1] * len(candidates)))
    return builder.as_markup()


def _build_batch_mode_text(enabled: bool) -> str:
    if enabled:
        return "Batch-режим включен. Новые источники будут добавляться в корзину для одного общего запуска."
    return "Batch-режим выключен. Один источник будет обрабатываться сразу; для нескольких источников появится выбор."


def _build_basket_text(candidates: list[SourceCandidate], added_count: int | None = None) -> str:
    prefix = f"Добавлено источников: {added_count}.\n" if added_count is not None else ""
    lines = [f"{index}. {humanize_source_label(candidate.display_name)}" for index, candidate in enumerate(candidates, start=1)]
    return (
        f"{prefix}В корзине {len(candidates)} источника(ов):\n"
        + "\n".join(lines)
        + "\n\nЗапуск создаст одну batch-задачу в API."
    )


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
