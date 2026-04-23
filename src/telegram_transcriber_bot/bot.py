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
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from telegram_transcriber_bot.config import Settings
from telegram_transcriber_bot.domain import MediaAttachment, ProcessedJob, SourceCandidate
from telegram_transcriber_bot.media_groups import MediaGroupAccumulator
from telegram_transcriber_bot.source_labels import humanize_source_label
from telegram_transcriber_bot.source_extractor import extract_sources

LOGGER = logging.getLogger(__name__)
HANDLE_MEDIA_MARKER = "[TelegramAdapter][handleMedia][BLOCK_SUBMIT_AND_WAIT_FOR_JOB]"


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


class TelegramTranscriberApp:
    def __init__(self, settings: Settings, processing_service: TelegramProcessingGateway, bot: Bot | None = None) -> None:
        self.settings = settings
        self.processing_service = processing_service
        self.bot = bot or Bot(settings.telegram_bot_token)
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.selection_store = CandidateSelectionStore()
        self.media_groups = MediaGroupAccumulator()
        self.media_group_tasks: dict[tuple[int, str], asyncio.Task[None]] = {}
        self.media_group_text: dict[tuple[int, str], str] = {}
        self.report_locks: dict[str, asyncio.Lock] = {}
        self.deep_research_locks: dict[str, asyncio.Lock] = {}
        self._register_handlers()

    async def run(self) -> None:
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        await self.dispatcher.start_polling(self.bot)

    def _register_handlers(self) -> None:
        self.dispatcher.include_router(self.router)
        self.router.message.register(self._handle_start, CommandStart())
        self.router.message.register(self._handle_help, Command("help"))
        self.router.message.register(self._handle_message)
        self.router.callback_query.register(self._handle_source_selection, lambda call: bool(call.data and call.data.startswith("pick:")))
        self.router.callback_query.register(self._handle_get_transcript, lambda call: bool(call.data and call.data.startswith("get:")))
        self.router.callback_query.register(
            self._handle_generate_report, lambda call: bool(call.data and call.data.startswith("report:"))
        )
        self.router.callback_query.register(
            self._handle_deep_research, lambda call: bool(call.data and call.data.startswith("deep:"))
        )

    async def _handle_start(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        await message.answer(
            "Отправьте ссылку на YouTube, текст с одной или несколькими ссылками, либо загрузите аудио/видео."
        )

    async def _handle_help(self, message: Message) -> None:
        if not await self._ensure_message_allowed(message):
            return
        await message.answer(
            "Поддерживается YouTube, прямые Telegram audio/video и текст с несколькими источниками. "
            "После обработки бот сразу отдаёт цельную txt-транскрибацию, по кнопке может прислать markdown-версию по сегментам, "
            "может собрать формальный markdown-отчёт через настроенную AI-упряжку "
            "и затем запустить deep research по evidence-first пайплайну."
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
        await self._process_candidate_set(chat_id=chat_id, user_id=user_id, text=text, attachments=attachments)

    async def _process_candidate_set(
        self,
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list[MediaAttachment],
    ) -> None:
        result = extract_sources(text=text, attachments=attachments)
        if not result.candidates:
            message = "Не нашёл поддерживаемых источников. Поддерживается YouTube и прямые Telegram audio/video."
            if result.rejected_urls:
                message += "\n\nНеподдерживаемые ссылки:\n" + "\n".join(f"- {item}" for item in result.rejected_urls)
            await self.bot.send_message(chat_id, message)
            return

        if attachments and len(attachments) > 1 and not any(candidate.kind == "youtube_url" for candidate in result.candidates):
            await self._start_processing_group(chat_id, result.candidates)
            return

        if len(result.candidates) == 1:
            await self._start_processing(chat_id, result.candidates[0])
            return

        selection_id = self.selection_store.save(chat_id=chat_id, user_id=user_id, candidates=result.candidates)
        keyboard = _build_candidate_keyboard(selection_id, result.candidates)
        message = "Нашёл несколько источников. Выберите, что обработать:"
        if result.rejected_urls:
            message += "\n\nПропущены неподдерживаемые ссылки:\n" + "\n".join(f"- {item}" for item in result.rejected_urls)
        await self.bot.send_message(chat_id, message, reply_markup=keyboard)

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

    async def _download_attachment_if_needed(self, candidate: SourceCandidate) -> SourceCandidate:
        if candidate.telegram_file_id is None:
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
        await callback.message.answer_document(
            FSInputFile(job.report.markdown_path),
            caption="Исследовательский отчёт в формате Markdown.",
            reply_markup=_build_deep_research_keyboard(job.job_id),
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
        f"Обрабатываю {attachment_count} файла(ов) как одну общую транскрибацию.\n\n"
        "Скачиваю файлы из Telegram и отправляю их в общий API-запрос. Это может занять несколько минут."
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
    if message.document and message.document.mime_type:
        if message.document.mime_type.startswith("audio/"):
            attachments.append(
                MediaAttachment(
                    telegram_file_id=message.document.file_id,
                    kind="telegram_audio",
                    file_name=message.document.file_name or f"{message.document.file_unique_id}.bin",
                    mime_type=message.document.mime_type,
                    file_unique_id=message.document.file_unique_id,
                )
            )
        elif message.document.mime_type.startswith("video/"):
            attachments.append(
                MediaAttachment(
                    telegram_file_id=message.document.file_id,
                    kind="telegram_video",
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
