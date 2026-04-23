from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from telegram_transcriber_bot.bot import (
    CandidateSelectionStore,
    HANDLE_MEDIA_MARKER,
    TelegramTranscriberApp,
    _extract_attachments,
    _guess_suffix,
)
from telegram_transcriber_bot.config import Settings
from telegram_transcriber_bot.domain import (
    ProcessedJob,
    ReportArtifacts,
    SourceCandidate,
    TranscriptArtifacts,
)


class FakeStatusMessage:
    def __init__(self, chat_id: int, text: str = "") -> None:
        self.chat = SimpleNamespace(id=chat_id)
        self.text = text
        self.from_user = SimpleNamespace(id=7)
        self.edits: list[dict[str, object]] = []
        self.documents: list[dict[str, object]] = []

    async def edit_text(self, text: str, reply_markup=None) -> None:
        self.edits.append({"text": text, "reply_markup": reply_markup})

    async def answer_document(self, document, caption: str = "", reply_markup=None) -> None:
        self.documents.append({"document": document, "caption": caption, "reply_markup": reply_markup})

    async def answer(self, text: str) -> "FakeStatusMessage":
        status = FakeStatusMessage(self.chat.id, text)
        self.documents.append({"answer_text": text, "status": status})
        return status


class FakeTask:
    def __init__(self, done: bool = False) -> None:
        self._done = done
        self.cancelled = False

    def done(self) -> bool:
        return self._done

    def cancel(self) -> None:
        self.cancelled = True


class FakeBot:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, object]] = []
        self.documents: list[dict[str, object]] = []
        self.downloads: list[dict[str, object]] = []

    async def send_message(self, chat_id: int, text: str, reply_markup=None) -> FakeStatusMessage:
        status = FakeStatusMessage(chat_id=chat_id, text=text)
        self.sent_messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup, "status": status})
        return status

    async def download(self, file_id: str, destination: Path) -> None:
        destination.write_bytes(b"fake-media")
        self.downloads.append({"file_id": file_id, "destination": destination})


class FakeProcessingService:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.processed_sources: list[SourceCandidate] = []
        self.processed_groups: list[list[SourceCandidate]] = []
        self.loaded_job_id: str | None = None
        self.report_job_id: str | None = None
        self.deep_research_job_id: str | None = None
        self.raise_on_load = False
        self.raise_on_report = False
        self.raise_on_deep_research = False
        self.job = _make_job(tmp_path)
        self.deep_research_path = self.job.workspace_dir / "deep_research" / "evidence-research-final-report.md"
        self.deep_research_path.parent.mkdir(parents=True, exist_ok=True)
        self.deep_research_path.write_text("# Deep Research\n", encoding="utf-8")

    def process_source(self, source: SourceCandidate) -> ProcessedJob:
        self.processed_sources.append(source)
        return self.job

    def process_source_group(self, sources: list[SourceCandidate]) -> ProcessedJob:
        self.processed_groups.append(sources)
        return self.job

    def load_job(self, job_id: str) -> ProcessedJob:
        self.loaded_job_id = job_id
        if self.raise_on_load:
            raise FileNotFoundError(job_id)
        return self.job

    def ensure_report(self, job_id: str, report_prompt_suffix: str = "") -> ProcessedJob:
        self.report_job_id = job_id
        if self.raise_on_report:
            raise RuntimeError("boom")
        return self.job

    def ensure_deep_research(self, job_id: str) -> Path:
        self.deep_research_job_id = job_id
        if self.raise_on_deep_research:
            raise RuntimeError("deep boom")
        return self.deep_research_path


class FakeCallback:
    def __init__(self, data: str | None, message: FakeStatusMessage | None, user_id: int = 7) -> None:
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=user_id)
        self.answers: list[dict[str, object]] = []

    async def answer(self, text: str, show_alert: bool = False) -> None:
        self.answers.append({"text": text, "show_alert": show_alert})


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        telegram_bot_token="123456:dummy-token",
        data_dir=tmp_path / ".data",
        allowed_user_ids=(),
        whisper_model="turbo",
        whisper_device="auto",
        whisper_compute_type="default",
        report_prompt_suffix="",
        media_group_window_seconds=0.0,
        youtube_languages=("ru", "en"),
    )


def _make_job(tmp_path: Path) -> ProcessedJob:
    workspace = tmp_path / "job-123"
    workspace.mkdir(parents=True, exist_ok=True)
    transcript_docx = workspace / "transcript.docx"
    transcript_docx.write_bytes(b"docx")
    transcript_md = workspace / "transcript.md"
    transcript_md.write_text("# Transcript\n", encoding="utf-8")
    transcript_txt = workspace / "transcript.txt"
    transcript_txt.write_text("hello\n", encoding="utf-8")
    report_docx = workspace / "report.docx"
    report_docx.write_bytes(b"report")
    report_md = workspace / "report.md"
    report_md.write_text("# Report\n", encoding="utf-8")
    return ProcessedJob(
        job_id="job-123",
        source=SourceCandidate(
            source_id="src-1",
            kind="telegram_audio",
            display_name="Audio: call.ogg",
            url=None,
            telegram_file_id="file-1",
            mime_type="audio/ogg",
            file_name="call.ogg",
        ),
        workspace_dir=workspace,
        transcript=TranscriptArtifacts(
            markdown_path=transcript_md,
            docx_path=transcript_docx,
            text_path=transcript_txt,
        ),
        report=ReportArtifacts(markdown_path=report_md, docx_path=report_docx),
        metadata_path=workspace / "job.json",
    )


def test_candidate_selection_store_restricts_by_chat_and_user() -> None:
    store = CandidateSelectionStore()
    candidates = [
        SourceCandidate(
            source_id="src-1",
            kind="youtube_url",
            display_name="YouTube: demo",
            url="https://youtu.be/demo",
            telegram_file_id=None,
            mime_type=None,
            file_name=None,
        )
    ]
    selection_id = store.save(chat_id=1, user_id=2, candidates=candidates)

    assert store.get(selection_id, chat_id=1, user_id=2, index=0) == candidates[0]
    assert store.get(selection_id, chat_id=99, user_id=2, index=0) is None
    assert store.get(selection_id, chat_id=1, user_id=3, index=0) is None
    assert store.get(selection_id, chat_id=1, user_id=2, index=5) is None


@pytest.mark.asyncio
async def test_process_candidate_set_sends_unsupported_message(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    await app._process_candidate_set(chat_id=10, user_id=11, text="https://example.com", attachments=[])

    assert len(fake_bot.sent_messages) == 1
    assert "Не нашёл поддерживаемых источников" in fake_bot.sent_messages[0]["text"]
    assert "https://example.com" in fake_bot.sent_messages[0]["text"]


@pytest.mark.asyncio
async def test_process_candidate_set_sends_selection_keyboard_for_multiple_sources(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    await app._process_candidate_set(
        chat_id=10,
        user_id=11,
        text="https://youtu.be/one https://youtu.be/two",
        attachments=[],
    )

    payload = fake_bot.sent_messages[0]
    assert "Выберите" in payload["text"]
    keyboard = payload["reply_markup"]
    assert keyboard is not None
    buttons = keyboard.inline_keyboard
    assert len(buttons) == 2
    assert buttons[0][0].text == "YouTube: one"
    assert buttons[1][0].text == "YouTube: two"


@pytest.mark.asyncio
async def test_handle_source_selection_rejects_malformed_callback(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="pick:bad", message=FakeStatusMessage(10))

    await app._handle_source_selection(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Некорректный выбор источника.", "show_alert": True}]


@pytest.mark.asyncio
async def test_start_processing_downloads_attachment_and_updates_status(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-1",
        kind="telegram_audio",
        display_name="Audio: call.ogg",
        url=None,
        telegram_file_id="file-telegram",
        mime_type="audio/ogg",
        file_name="call.ogg",
    )

    await app._start_processing(chat_id=10, candidate=candidate)

    assert fake_bot.downloads
    assert fake_service.processed_sources[0].local_path is not None
    assert fake_service.processed_sources[0].local_path.exists()
    status = fake_bot.sent_messages[0]["status"]
    assert status.edits[-1]["text"] == "Транскрибация готова."
    document_entry = next(item for item in status.documents if "document" in item)
    assert document_entry["document"].path.name == "transcript.txt"
    assert document_entry["caption"] == "Готовая цельная транскрибация без сегментов."
    keyboard = document_entry["reply_markup"]
    assert keyboard.inline_keyboard[0][0].text == "Получить по сегментам"
    assert keyboard.inline_keyboard[1][0].text == "Создать исследовательский отчет"


@pytest.mark.asyncio
async def test_start_processing_emits_required_adapter_marker(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    logs: list[str] = []

    monkeypatch.setattr(
        "telegram_transcriber_bot.bot.LOGGER",
        SimpleNamespace(info=lambda message, *args: logs.append(message % args if args else message)),
    )

    await app._start_processing(chat_id=10, candidate=candidate)

    assert logs == [f"{HANDLE_MEDIA_MARKER} mode=single source_kind=youtube_url"]


@pytest.mark.asyncio
async def test_start_processing_humanizes_machine_like_telegram_audio_name(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-2",
        kind="telegram_audio",
        display_name="Audio: AgADBproAAg_9MUs.ogg",
        url=None,
        telegram_file_id="file-telegram",
        mime_type="audio/ogg",
        file_name="AgADBproAAg_9MUs.ogg",
    )

    await app._start_processing(chat_id=10, candidate=candidate)

    assert fake_bot.sent_messages[0]["text"] == (
        "Обрабатываю источник: Аудиофайл из Telegram\n\n"
        "Скачиваю файл из Telegram и запускаю транскрибацию. Это может занять несколько минут."
    )


@pytest.mark.asyncio
async def test_handle_get_transcript_returns_missing_artifact_error(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    fake_service.raise_on_load = True
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="get:missing-job", message=FakeStatusMessage(10))

    await app._handle_get_transcript(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Артефакты не найдены.", "show_alert": True}]


@pytest.mark.asyncio
async def test_handle_generate_report_sends_waiting_message_when_locked(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    lock = asyncio.Lock()
    await lock.acquire()
    app.report_locks["job-123"] = lock
    callback = FakeCallback(data="report:job-123", message=FakeStatusMessage(10))

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Отчёт уже формируется, дождитесь завершения.", "show_alert": False}]
    lock.release()


@pytest.mark.asyncio
async def test_handle_generate_report_sends_document_on_success(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeStatusMessage(10)
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="report:job-123", message=callback_message)

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Запускаю генерацию отчёта...", "show_alert": False}]
    document_entry = next(item for item in callback_message.documents if "document" in item)
    assert document_entry["document"].path.name == "report.md"
    keyboard = document_entry["reply_markup"]
    assert keyboard.inline_keyboard[0][0].text == "Запустить глубокое исследование"
    assert keyboard.inline_keyboard[0][0].callback_data == "deep:job-123"


def test_extract_attachments_and_guess_suffix() -> None:
    message = SimpleNamespace(
        audio=SimpleNamespace(file_id="a1", file_name="song.mp3", mime_type="audio/mpeg", file_unique_id="ua"),
        voice=SimpleNamespace(file_id="v1", file_unique_id="uv"),
        video=SimpleNamespace(file_id="vv1", file_name=None, mime_type="video/mp4", file_unique_id="uvv"),
        video_note=SimpleNamespace(file_id="vn1", file_unique_id="uvn"),
        document=SimpleNamespace(
            file_id="d1",
            file_name="clip.mov",
            mime_type="video/quicktime",
            file_unique_id="ud1",
        ),
    )

    attachments = _extract_attachments(message)  # type: ignore[arg-type]

    assert [item.kind for item in attachments] == [
        "telegram_audio",
        "telegram_audio",
        "telegram_video",
        "telegram_video",
        "telegram_video",
    ]
    assert _guess_suffix(
        SourceCandidate(
            source_id="src-2",
            kind="telegram_audio",
            display_name="Audio: song.mp3",
            url=None,
            telegram_file_id="a1",
            mime_type="audio/mpeg",
            file_name=None,
        )
    ) == ".mp3"


@pytest.mark.asyncio
async def test_handle_start_and_help_answer_with_guidance(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    start_message = FakeStatusMessage(10)
    help_message = FakeStatusMessage(10)

    await app._handle_start(start_message)  # type: ignore[arg-type]
    await app._handle_help(help_message)  # type: ignore[arg-type]

    assert start_message.documents[0]["answer_text"].startswith("Отправьте ссылку")
    assert "цельную txt-транскрибацию" in help_message.documents[0]["answer_text"]
    assert "по сегментам" in help_message.documents[0]["answer_text"]


@pytest.mark.asyncio
async def test_handle_message_delegates_non_media_input_to_processing(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    calls: list[dict[str, object]] = []

    async def fake_process_candidate_set(chat_id: int, user_id: int | None, text: str, attachments: list) -> None:
        calls.append({"chat_id": chat_id, "user_id": user_id, "text": text, "attachments": attachments})

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    message = SimpleNamespace(
        text="https://youtu.be/demo",
        caption=None,
        media_group_id=None,
        chat=SimpleNamespace(id=10),
        from_user=SimpleNamespace(id=11),
        audio=None,
        voice=None,
        video=None,
        video_note=None,
        document=None,
    )

    await app._handle_message(message)  # type: ignore[arg-type]

    assert calls == [{"chat_id": 10, "user_id": 11, "text": "https://youtu.be/demo", "attachments": []}]


@pytest.mark.asyncio
async def test_handle_message_buffers_media_group_and_cancels_previous_task(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    previous_task = FakeTask(done=False)
    app.media_group_tasks[(10, "group-1")] = previous_task  # type: ignore[assignment]
    scheduled = FakeTask(done=False)
    def fake_create_task(coro):
        coro.close()
        return scheduled

    monkeypatch.setattr("telegram_transcriber_bot.bot.asyncio.create_task", fake_create_task)
    message = SimpleNamespace(
        text=None,
        caption="caption text",
        media_group_id="group-1",
        chat=SimpleNamespace(id=10),
        from_user=SimpleNamespace(id=11),
        audio=None,
        voice=SimpleNamespace(file_id="voice-1", file_unique_id="voice-uniq"),
        video=None,
        video_note=None,
        document=None,
    )

    await app._handle_message(message)  # type: ignore[arg-type]

    assert previous_task.cancelled is True
    assert app.media_group_text[(10, "group-1")] == "caption text"
    buffered = app.media_groups.pop(10, "group-1")
    assert buffered[0].telegram_file_id == "voice-1"


@pytest.mark.asyncio
async def test_flush_media_group_forwards_buffered_payload(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    app.media_groups.add(
        10,
        "group-1",
        attachment=SimpleNamespace(
            telegram_file_id="file-1",
            kind="telegram_audio",
            file_name="call.ogg",
            mime_type="audio/ogg",
            file_unique_id="uniq-1",
        ),
    )
    app.media_group_text[(10, "group-1")] = "caption"
    app.media_group_tasks[(10, "group-1")] = FakeTask(done=False)  # type: ignore[assignment]
    forwarded: list[dict[str, object]] = []

    async def fake_process_candidate_set(chat_id: int, user_id: int | None, text: str, attachments: list) -> None:
        forwarded.append({"chat_id": chat_id, "user_id": user_id, "text": text, "attachments": attachments})

    async def fake_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    monkeypatch.setattr("telegram_transcriber_bot.bot.asyncio.sleep", fake_sleep)

    await app._flush_media_group(chat_id=10, user_id=11, media_group_id="group-1")

    assert forwarded[0]["text"] == "caption"
    assert forwarded[0]["attachments"][0].telegram_file_id == "file-1"
    assert (10, "group-1") not in app.media_group_tasks


@pytest.mark.asyncio
async def test_flush_media_group_stops_on_cancellation(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    called = False

    async def fake_process_candidate_set(chat_id: int, user_id: int | None, text: str, attachments: list) -> None:
        nonlocal called
        called = True

    async def cancelled_sleep(seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    monkeypatch.setattr("telegram_transcriber_bot.bot.asyncio.sleep", cancelled_sleep)

    await app._flush_media_group(chat_id=10, user_id=11, media_group_id="group-1")

    assert called is False


@pytest.mark.asyncio
async def test_process_candidate_set_starts_processing_for_single_source(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    started: list[SourceCandidate] = []

    async def fake_start_processing(chat_id: int, candidate: SourceCandidate) -> None:
        started.append(candidate)

    monkeypatch.setattr(app, "_start_processing", fake_start_processing)

    await app._process_candidate_set(chat_id=10, user_id=11, text="https://youtu.be/demo", attachments=[])

    assert started[0].display_name == "YouTube: demo"


@pytest.mark.asyncio
async def test_process_candidate_set_routes_multiple_attachments_to_combined_submission(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    grouped: list[list[SourceCandidate]] = []

    async def fake_start_processing_group(chat_id: int, candidates: list[SourceCandidate]) -> None:
        grouped.append(candidates)

    monkeypatch.setattr(app, "_start_processing_group", fake_start_processing_group)

    await app._process_candidate_set(
        chat_id=10,
        user_id=11,
        text="",
        attachments=[
            SimpleNamespace(
                telegram_file_id="voice-1",
                kind="telegram_audio",
                file_name="voice-1.ogg",
                mime_type="audio/ogg",
                file_unique_id="uniq-1",
            ),
            SimpleNamespace(
                telegram_file_id="voice-2",
                kind="telegram_audio",
                file_name="voice-2.ogg",
                mime_type="audio/ogg",
                file_unique_id="uniq-2",
            ),
        ],
    )

    assert len(grouped) == 1
    assert [candidate.telegram_file_id for candidate in grouped[0]] == ["voice-1", "voice-2"]


@pytest.mark.asyncio
async def test_process_candidate_set_mentions_rejected_urls_for_multiple_sources(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    await app._process_candidate_set(
        chat_id=10,
        user_id=11,
        text="https://youtu.be/one https://example.com https://youtu.be/two",
        attachments=[],
    )

    assert "Пропущены неподдерживаемые ссылки" in fake_bot.sent_messages[0]["text"]
    assert "https://example.com" in fake_bot.sent_messages[0]["text"]


@pytest.mark.asyncio
async def test_handle_source_selection_handles_missing_candidate(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="pick:missing:0", message=FakeStatusMessage(10))

    await app._handle_source_selection(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Источник уже недоступен.", "show_alert": True}]


@pytest.mark.asyncio
async def test_handle_source_selection_starts_processing_for_valid_candidate(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    selection_id = app.selection_store.save(chat_id=10, user_id=7, candidates=[candidate])
    started: list[tuple[int, SourceCandidate]] = []

    async def fake_start_processing(chat_id: int, candidate: SourceCandidate) -> None:
        started.append((chat_id, candidate))

    monkeypatch.setattr(app, "_start_processing", fake_start_processing)
    callback = FakeCallback(data=f"pick:{selection_id}:0", message=FakeStatusMessage(10))

    await app._handle_source_selection(callback)  # type: ignore[arg-type]

    assert callback.answers[0] == {"text": "Запускаю обработку...", "show_alert": False}
    assert started[0][0] == 10
    assert started[0][1].display_name == "YouTube: demo"


@pytest.mark.asyncio
async def test_start_processing_reports_failure(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)

    def raising_process_source(source: SourceCandidate) -> ProcessedJob:
        raise RuntimeError("processing failed")

    fake_service.process_source = raising_process_source  # type: ignore[assignment]
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    await app._start_processing(chat_id=10, candidate=candidate)

    status = fake_bot.sent_messages[0]["status"]
    assert status.edits[-1]["text"] == "Не удалось обработать источник: processing failed"


@pytest.mark.asyncio
async def test_callback_handlers_ignore_missing_message_or_data(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callbacks = [
        FakeCallback(data=None, message=FakeStatusMessage(10)),
        FakeCallback(data="pick:1:0", message=None),
        FakeCallback(data=None, message=FakeStatusMessage(10)),
        FakeCallback(data="get:job-123", message=None),
        FakeCallback(data=None, message=FakeStatusMessage(10)),
        FakeCallback(data="report:job-123", message=None),
        FakeCallback(data=None, message=FakeStatusMessage(10)),
        FakeCallback(data="deep:job-123", message=None),
    ]

    await app._handle_source_selection(callbacks[0])  # type: ignore[arg-type]
    await app._handle_source_selection(callbacks[1])  # type: ignore[arg-type]
    await app._handle_get_transcript(callbacks[2])  # type: ignore[arg-type]
    await app._handle_get_transcript(callbacks[3])  # type: ignore[arg-type]
    await app._handle_generate_report(callbacks[4])  # type: ignore[arg-type]
    await app._handle_generate_report(callbacks[5])  # type: ignore[arg-type]
    await app._handle_deep_research(callbacks[6])  # type: ignore[arg-type]
    await app._handle_deep_research(callbacks[7])  # type: ignore[arg-type]

    assert all(callback.answers == [] for callback in callbacks)


@pytest.mark.asyncio
async def test_download_attachment_skips_non_telegram_sources(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    result = await app._download_attachment_if_needed(candidate)

    assert result is candidate
    assert fake_bot.downloads == []


@pytest.mark.asyncio
async def test_handle_get_transcript_success_and_malformed_data(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callback_message = FakeStatusMessage(10)
    good_callback = FakeCallback(data="get:job-123", message=callback_message)
    bad_callback = FakeCallback(data="get", message=FakeStatusMessage(10))

    await app._handle_get_transcript(good_callback)  # type: ignore[arg-type]
    await app._handle_get_transcript(bad_callback)  # type: ignore[arg-type]

    assert good_callback.answers[0] == {"text": "Отправляю транскрибацию по сегментам...", "show_alert": False}
    document_entry = next(item for item in callback_message.documents if "document" in item)
    assert document_entry["document"].path.name == "transcript.md"
    assert bad_callback.answers == [{"text": "Некорректный идентификатор задачи.", "show_alert": True}]


@pytest.mark.asyncio
async def test_handle_generate_report_handles_malformed_and_service_errors(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    fake_service.raise_on_report = True
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    callback_message = FakeStatusMessage(10)
    bad_callback = FakeCallback(data="report", message=FakeStatusMessage(10))
    failing_callback = FakeCallback(data="report:job-123", message=callback_message)

    await app._handle_generate_report(bad_callback)  # type: ignore[arg-type]
    await app._handle_generate_report(failing_callback)  # type: ignore[arg-type]

    assert bad_callback.answers == [{"text": "Некорректный идентификатор задачи.", "show_alert": True}]
    status = next(item["status"] for item in callback_message.documents if "status" in item)
    assert status.edits[-1]["text"] == "Не удалось сформировать отчёт: boom"
    assert failing_callback.answers == [{"text": "Запускаю генерацию отчёта...", "show_alert": False}]


@pytest.mark.asyncio
async def test_handle_generate_report_handles_missing_saved_report(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.report = None
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    callback_message = FakeStatusMessage(10)
    callback = FakeCallback(data="report:job-123", message=callback_message)

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Запускаю генерацию отчёта...", "show_alert": False}]
    assert callback_message.documents[-1]["answer_text"] == "Отчёт не был сохранён."


@pytest.mark.asyncio
async def test_safe_callback_answer_ignores_stale_query_and_still_sends_report(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    callback_message = FakeStatusMessage(10)
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    class StaleCallback(FakeCallback):
        async def answer(self, text: str, show_alert: bool = False) -> None:
            raise RuntimeError("query is too old and response timeout expired")

    monkeypatch.setattr("telegram_transcriber_bot.bot.TelegramBadRequest", RuntimeError)
    callback = StaleCallback(data="report:job-123", message=callback_message)

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    document_entry = next(item for item in callback_message.documents if "document" in item)
    assert document_entry["document"].path.name == "report.md"


@pytest.mark.asyncio
async def test_handle_deep_research_sends_waiting_message_when_locked(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    lock = asyncio.Lock()
    await lock.acquire()
    app.deep_research_locks["job-123"] = lock
    callback = FakeCallback(data="deep:job-123", message=FakeStatusMessage(10))

    await app._handle_deep_research(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {"text": "Глубокое исследование уже запущено, дождитесь завершения.", "show_alert": False}
    ]
    lock.release()


@pytest.mark.asyncio
async def test_handle_deep_research_sends_document_on_success(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeStatusMessage(10)
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="deep:job-123", message=callback_message)

    await app._handle_deep_research(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Запускаю глубокое исследование...", "show_alert": False}]
    assert fake_service.deep_research_job_id == "job-123"
    status = next(item["status"] for item in callback_message.documents if "status" in item)
    assert status.edits[-1]["text"] == "Глубокое исследование готово."
    document_entry = next(item for item in callback_message.documents if "document" in item)
    assert document_entry["document"].path.name == "evidence-research-final-report.md"


@pytest.mark.asyncio
async def test_handle_deep_research_handles_malformed_and_service_errors(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    fake_service.raise_on_deep_research = True
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    callback_message = FakeStatusMessage(10)
    bad_callback = FakeCallback(data="deep", message=FakeStatusMessage(10))
    failing_callback = FakeCallback(data="deep:job-123", message=callback_message)

    await app._handle_deep_research(bad_callback)  # type: ignore[arg-type]
    await app._handle_deep_research(failing_callback)  # type: ignore[arg-type]

    assert bad_callback.answers == [{"text": "Некорректный идентификатор задачи.", "show_alert": True}]
    status = next(item["status"] for item in callback_message.documents if "status" in item)
    assert status.edits[-1]["text"] == "Не удалось запустить глубокое исследование: deep boom"
    assert failing_callback.answers == [{"text": "Запускаю глубокое исследование...", "show_alert": False}]


@pytest.mark.asyncio
async def test_run_creates_data_dir_and_starts_polling(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    started: list[object] = []

    async def fake_start_polling(bot) -> None:
        started.append(bot)

    monkeypatch.setattr(app.dispatcher, "start_polling", fake_start_polling)

    await app.run()

    assert app.settings.data_dir.exists()
    assert started == [fake_bot]


@pytest.mark.asyncio
async def test_handle_message_blocks_non_whitelisted_user_before_processing(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    base = make_settings(tmp_path)
    app = TelegramTranscriberApp(
        Settings(
            telegram_bot_token=base.telegram_bot_token,
            data_dir=base.data_dir,
            allowed_user_ids=(1973144093,),
            whisper_model=base.whisper_model,
            whisper_device=base.whisper_device,
            whisper_compute_type=base.whisper_compute_type,
            report_prompt_suffix=base.report_prompt_suffix,
            media_group_window_seconds=base.media_group_window_seconds,
            youtube_languages=base.youtube_languages,
        ),
        FakeProcessingService(tmp_path),
        bot=fake_bot,
    )  # type: ignore[arg-type]
    called = False

    async def fake_process_candidate_set(chat_id: int, user_id: int | None, text: str, attachments: list) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    message = SimpleNamespace(
        text="https://youtu.be/demo",
        caption=None,
        media_group_id=None,
        chat=SimpleNamespace(id=10),
        from_user=SimpleNamespace(id=999),
        audio=None,
        voice=None,
        video=None,
        video_note=None,
        document=None,
        answer=FakeStatusMessage(10).answer,
    )

    await app._handle_message(message)  # type: ignore[arg-type]

    assert called is False


@pytest.mark.asyncio
async def test_message_handlers_block_non_whitelisted_user(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    base = make_settings(tmp_path)
    app = TelegramTranscriberApp(
        Settings(
            telegram_bot_token=base.telegram_bot_token,
            data_dir=base.data_dir,
            allowed_user_ids=(1973144093,),
            whisper_model=base.whisper_model,
            whisper_device=base.whisper_device,
            whisper_compute_type=base.whisper_compute_type,
            report_prompt_suffix=base.report_prompt_suffix,
            media_group_window_seconds=base.media_group_window_seconds,
            youtube_languages=base.youtube_languages,
        ),
        FakeProcessingService(tmp_path),
        bot=fake_bot,
    )  # type: ignore[arg-type]
    message = FakeStatusMessage(10)
    message.from_user = SimpleNamespace(id=999)

    await app._handle_start(message)  # type: ignore[arg-type]

    assert message.documents[0]["answer_text"] == "Доступ к этому боту ограничен."


@pytest.mark.asyncio
async def test_callback_handlers_block_non_whitelisted_user(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    base = make_settings(tmp_path)
    app = TelegramTranscriberApp(
        Settings(
            telegram_bot_token=base.telegram_bot_token,
            data_dir=base.data_dir,
            allowed_user_ids=(1973144093,),
            whisper_model=base.whisper_model,
            whisper_device=base.whisper_device,
            whisper_compute_type=base.whisper_compute_type,
            report_prompt_suffix=base.report_prompt_suffix,
            media_group_window_seconds=base.media_group_window_seconds,
            youtube_languages=base.youtube_languages,
        ),
        FakeProcessingService(tmp_path),
        bot=fake_bot,
    )  # type: ignore[arg-type]
    callback = FakeCallback(data="get:job-123", message=FakeStatusMessage(10), user_id=999)

    await app._handle_get_transcript(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Доступ к этому боту ограничен.", "show_alert": True}]


def test_extract_attachments_handles_audio_documents_and_ignores_other_documents() -> None:
    audio_document_message = SimpleNamespace(
        audio=None,
        voice=None,
        video=None,
        video_note=None,
        document=SimpleNamespace(
            file_id="doc-a1",
            file_name=None,
            mime_type="audio/wav",
            file_unique_id="uniq-a1",
        ),
    )
    unsupported_document_message = SimpleNamespace(
        audio=None,
        voice=None,
        video=None,
        video_note=None,
        document=SimpleNamespace(
            file_id="doc-x1",
            file_name="notes.txt",
            mime_type="text/plain",
            file_unique_id="uniq-x1",
        ),
    )

    audio_attachments = _extract_attachments(audio_document_message)  # type: ignore[arg-type]
    unsupported_attachments = _extract_attachments(unsupported_document_message)  # type: ignore[arg-type]

    assert len(audio_attachments) == 1
    assert audio_attachments[0].kind == "telegram_audio"
    assert audio_attachments[0].file_name == "uniq-a1.bin"
    assert unsupported_attachments == []


def test_guess_suffix_covers_audio_video_and_default_fallbacks() -> None:
    audio = SourceCandidate(
        source_id="src-a",
        kind="telegram_audio",
        display_name="Audio",
        url=None,
        telegram_file_id="a1",
        mime_type="audio/ogg",
        file_name=None,
    )
    video = SourceCandidate(
        source_id="src-v",
        kind="telegram_video",
        display_name="Video",
        url=None,
        telegram_file_id="v1",
        mime_type=None,
        file_name=None,
    )
    unknown = SourceCandidate(
        source_id="src-u",
        kind="telegram_audio",
        display_name="Unknown",
        url=None,
        telegram_file_id="u1",
        mime_type="application/octet-stream",
        file_name=None,
    )

    assert _guess_suffix(audio) == ".ogg"
    assert _guess_suffix(video) == ".mp4"
    assert _guess_suffix(unknown) == ".bin"
