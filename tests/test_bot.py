from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiogram.exceptions import TelegramBadRequest

from media_analysis_platform.bot import (
    BatchModeStore,
    BasketStore,
    CandidateSelectionStore,
    HANDLE_MEDIA_MARKER,
    TELEGRAM_COMMANDS,
    TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS,
    TelegramTranscriberApp,
    _extract_attachments,
    _guess_suffix,
    _looks_like_command_menu_request,
    _with_stable_source_id,
)
from media_analysis_platform.config import Settings
from transcriber_workers_common.domain import (
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
        self.answers: list[dict[str, object]] = []
        self.documents: list[dict[str, object]] = []

    async def edit_text(self, text: str, reply_markup=None) -> None:
        self.edits.append({"text": text, "reply_markup": reply_markup})

    async def answer_document(self, document, caption: str = "", reply_markup=None) -> None:
        assert Path(document.path).exists(), f"document path does not exist: {document.path}"
        self.documents.append({"document": document, "caption": caption, "reply_markup": reply_markup})

    async def answer(self, text: str, reply_markup=None) -> "FakeStatusMessage":
        status = FakeStatusMessage(self.chat.id, text)
        payload = {"answer_text": text, "text": text, "status": status, "reply_markup": reply_markup}
        self.answers.append(payload)
        self.documents.append(payload)
        return status


class InlineRejectedStatusMessage(FakeStatusMessage):
    async def answer(self, text: str, reply_markup=None) -> "FakeStatusMessage":
        raise TelegramBadRequest(method=None, message="message is too long")


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
        self.commands: list[object] = []
        self.command_scopes: list[object] = []
        self.menu_buttons: list[object] = []

    async def send_message(self, chat_id: int, text: str, reply_markup=None) -> FakeStatusMessage:
        status = FakeStatusMessage(chat_id=chat_id, text=text)
        self.sent_messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup, "status": status})
        return status

    async def set_my_commands(self, commands: list[object], scope=None) -> None:
        self.commands = commands
        self.command_scopes.append(scope)

    async def set_chat_menu_button(self, menu_button=None) -> None:
        self.menu_buttons.append(menu_button)

    async def download(self, file_id: str, destination: Path) -> None:
        destination.write_bytes(b"fake-media")
        self.downloads.append({"file_id": file_id, "destination": destination})


class FakeProcessingService:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.processed_sources: list[SourceCandidate] = []
        self.processed_groups: list[list[SourceCandidate]] = []
        self.created_draft_owners: list[dict[str, str]] = []
        self.loaded_drafts: list[dict[str, object]] = []
        self.draft_additions: list[dict[str, object]] = []
        self.draft_removals: list[dict[str, object]] = []
        self.draft_clears: list[dict[str, object]] = []
        self.submitted_drafts: list[dict[str, object]] = []
        self.next_draft_id = "11111111-1111-1111-1111-111111111111"
        self.draft = _make_draft(self.next_draft_id, version=1, items=[])
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

    def create_batch_draft(self, *, owner: dict[str, str], display_name: str | None = None) -> dict:
        self.created_draft_owners.append(owner)
        self.draft = _make_draft(self.next_draft_id, version=1, items=[])
        return self.draft

    def load_batch_draft(self, *, draft_id: str, owner: dict[str, str]) -> dict:
        self.loaded_drafts.append({"draft_id": draft_id, "owner": owner})
        return self.draft

    def add_source_to_draft(self, draft: dict, *, owner: dict[str, str], source: SourceCandidate) -> dict:
        self.draft_additions.append({"draft": draft, "owner": owner, "source": source})
        source_payload: dict[str, object] = {
            "source_kind": source.kind,
            "display_name": source.display_name,
        }
        if source.url:
            source_payload["url"] = source.url
        if source.file_name:
            source_payload["original_filename"] = source.file_name
        item = {
            "item_id": f"22222222-2222-2222-2222-22222222222{len(self.draft['items']) + 1}",
            "position": len(self.draft["items"]),
            "source_label": f"source_{len(self.draft['items']) + 1}",
            "source": source_payload,
        }
        self.draft = _make_draft(
            self.draft["draft_id"],
            version=int(self.draft["version"]) + 1,
            items=[*self.draft["items"], item],
        )
        return self.draft

    def remove_draft_item(
        self,
        *,
        draft_id: str,
        owner: dict[str, str],
        expected_version: int,
        item_id: str,
    ) -> dict:
        self.draft_removals.append(
            {
                "draft_id": draft_id,
                "owner": owner,
                "expected_version": expected_version,
                "item_id": item_id,
            }
        )
        self.draft = _make_draft(draft_id, version=expected_version + 1, items=[])
        return self.draft

    def clear_batch_draft(self, *, draft_id: str, owner: dict[str, str], expected_version: int) -> dict:
        self.draft_clears.append(
            {
                "draft_id": draft_id,
                "owner": owner,
                "expected_version": expected_version,
            }
        )
        self.draft = _make_draft(draft_id, version=expected_version + 1, items=[])
        return self.draft

    def submit_batch_draft(self, *, draft_id: str, owner: dict[str, str], expected_version: int) -> ProcessedJob:
        self.submitted_drafts.append(
            {
                "draft_id": draft_id,
                "owner": owner,
                "expected_version": expected_version,
            }
        )
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


class UniqueDraftProcessingService(FakeProcessingService):
    def __init__(self, tmp_path: Path) -> None:
        super().__init__(tmp_path)
        self._lock = threading.Lock()
        self._draft_counter = 0
        self.drafts: dict[str, dict] = {}

    def create_batch_draft(self, *, owner: dict[str, str], display_name: str | None = None) -> dict:
        with self._lock:
            self._draft_counter += 1
            draft_id = f"11111111-1111-1111-1111-{self._draft_counter:012d}"
            draft = _make_draft(draft_id, version=1, items=[])
            self.drafts[draft_id] = draft
            self.draft = draft
            self.created_draft_owners.append(owner)
            return draft

    def load_batch_draft(self, *, draft_id: str, owner: dict[str, str]) -> dict:
        self.loaded_drafts.append({"draft_id": draft_id, "owner": owner})
        return self.drafts[draft_id]

    def add_source_to_draft(self, draft: dict, *, owner: dict[str, str], source: SourceCandidate) -> dict:
        self.draft_additions.append({"draft": draft, "owner": owner, "source": source})
        with self._lock:
            current = self.drafts[draft["draft_id"]]
            source_payload: dict[str, object] = {
                "source_kind": source.kind,
                "display_name": source.display_name,
            }
            if source.url:
                source_payload["url"] = source.url
            if source.file_name:
                source_payload["original_filename"] = source.file_name
            item = {
                "item_id": f"22222222-2222-2222-2222-22222222222{len(current['items']) + 1}",
                "position": len(current["items"]),
                "source_label": f"source_{len(current['items']) + 1}",
                "source": source_payload,
            }
            next_draft = _make_draft(
                current["draft_id"],
                version=int(current["version"]) + 1,
                items=[*current["items"], item],
            )
            self.drafts[current["draft_id"]] = next_draft
            self.draft = next_draft
            return next_draft


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
        report=ReportArtifacts(job_id="report-job-123", markdown_path=report_md, docx_path=report_docx),
        metadata_path=workspace / "job.json",
    )


def _make_draft(draft_id: str, *, version: int, items: list[dict]) -> dict:
    return {
        "draft_id": draft_id,
        "version": version,
        "owner": {
            "owner_type": "telegram",
            "telegram_chat_id": "10",
            "telegram_user_id": "7",
        },
        "status": "open",
        "items": items,
    }


class DraftConflictError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _document_entries(message: FakeStatusMessage) -> list[dict[str, object]]:
    return [item for item in message.documents if "document" in item]


@pytest.mark.asyncio
async def test_send_transcript_result_delivers_threshold_length_transcript_inline(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.write_text("x" * TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS, encoding="utf-8")
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = FakeStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="fallback")  # type: ignore[arg-type]

    assert len(status.answers) == 1
    assert status.answers[0]["text"] == "x" * TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS
    assert _document_entries(status) == []
    keyboard = status.answers[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "get:job-123"
    assert keyboard.inline_keyboard[1][0].callback_data == "report:job-123"


@pytest.mark.asyncio
async def test_send_transcript_result_falls_back_to_document_when_inline_delivery_is_rejected(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.write_text("short transcript", encoding="utf-8")
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = InlineRejectedStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="fallback")  # type: ignore[arg-type]

    assert status.answers == []
    document_entry = _document_entries(status)[0]
    assert document_entry["document"].path == fake_service.job.transcript.text_path
    assert document_entry["caption"] == "fallback"
    keyboard = document_entry["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "get:job-123"
    assert keyboard.inline_keyboard[1][0].callback_data == "report:job-123"


@pytest.mark.asyncio
async def test_send_transcript_result_falls_back_to_document_for_long_transcript(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.write_text("x" * (TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS + 1), encoding="utf-8")
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = FakeStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="Готовая цельная транскрибация без сегментов.")  # type: ignore[arg-type]

    assert status.answers == []
    document_entry = _document_entries(status)[0]
    assert document_entry["document"].path == fake_service.job.transcript.text_path
    assert document_entry["caption"] == "Готовая цельная транскрибация без сегментов."
    keyboard = document_entry["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "get:job-123"
    assert keyboard.inline_keyboard[1][0].callback_data == "report:job-123"


@pytest.mark.parametrize("content", ["", " \n\t "])
@pytest.mark.asyncio
async def test_send_transcript_result_falls_back_to_document_for_empty_or_whitespace_transcript(
    tmp_path: Path,
    content: str,
) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.write_text(content, encoding="utf-8")
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = FakeStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="fallback")  # type: ignore[arg-type]

    assert status.answers == []
    document_entry = _document_entries(status)[0]
    assert document_entry["document"].path == fake_service.job.transcript.text_path
    assert document_entry["caption"] == "fallback"


@pytest.mark.asyncio
async def test_send_transcript_result_sends_unavailable_message_for_missing_transcript_file(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.unlink()
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = FakeStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="fallback")  # type: ignore[arg-type]

    assert len(status.answers) == 1
    assert status.answers[0]["text"] == "Транскрибация готова, но текстовый файл артефакта недоступен."
    assert _document_entries(status) == []
    keyboard = status.answers[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "get:job-123"
    assert keyboard.inline_keyboard[1][0].callback_data == "report:job-123"


@pytest.mark.asyncio
async def test_send_transcript_result_sends_unavailable_message_for_non_file_transcript_path(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.unlink()
    fake_service.job.transcript.text_path.mkdir()
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = FakeStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="fallback")  # type: ignore[arg-type]

    assert len(status.answers) == 1
    assert status.answers[0]["text"] == "Транскрибация готова, но текстовый файл артефакта недоступен."
    assert _document_entries(status) == []


@pytest.mark.asyncio
async def test_send_transcript_result_falls_back_to_document_for_unreadable_transcript(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    fake_service.job.transcript.text_path.write_bytes(b"\xff")
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    status = FakeStatusMessage(10)

    await app._send_transcript_result(status, fake_service.job, fallback_caption="fallback")  # type: ignore[arg-type]

    assert status.answers == []
    document_entry = _document_entries(status)[0]
    assert document_entry["document"].path == fake_service.job.transcript.text_path
    assert document_entry["caption"] == "fallback"


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


def test_basket_store_add_remove_clear_is_scoped_by_chat_and_user() -> None:
    store = BasketStore()
    candidate = SourceCandidate(
        source_id="url:https://youtu.be/demo",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    store.add(chat_id=1, user_id=2, candidates=[candidate])

    assert store.list(chat_id=1, user_id=2) == [candidate]
    assert store.list(chat_id=1, user_id=3) == []
    assert store.remove(chat_id=1, user_id=2, index=0) == candidate
    assert store.list(chat_id=1, user_id=2) == []
    store.add(chat_id=1, user_id=2, candidates=[candidate])
    store.clear(chat_id=1, user_id=2)
    assert store.list(chat_id=1, user_id=2) == []


def test_batch_mode_store_defaults_to_enabled_and_toggles_by_user() -> None:
    store = BatchModeStore()

    assert store.is_enabled(chat_id=1, user_id=2) is True
    assert store.toggle(chat_id=1, user_id=2) is False
    assert store.is_enabled(chat_id=1, user_id=2) is False
    assert store.is_enabled(chat_id=1, user_id=3) is True
    store.set_enabled(chat_id=1, user_id=2, enabled=True)
    assert store.is_enabled(chat_id=1, user_id=2) is True


def test_stable_source_id_uses_message_id_file_name_file_id_and_url() -> None:
    url_candidate = SourceCandidate(
        source_id="random",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    media_candidate = SourceCandidate(
        source_id="random",
        kind="telegram_audio",
        display_name="Audio: call.ogg",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="call.ogg",
    )
    file_id_candidate = SourceCandidate(
        source_id="random",
        kind="telegram_audio",
        display_name="Audio",
        url=None,
        telegram_file_id="file-2",
        mime_type="audio/ogg",
        file_name=None,
    )

    assert _with_stable_source_id(url_candidate, message_id=10).source_id == "url:https://youtu.be/demo"
    assert _with_stable_source_id(media_candidate, message_id=10).source_id == "telegram-message:10:call.ogg"
    assert _with_stable_source_id(file_id_candidate, message_id=None).source_id == "telegram-file-id:file-2"


@pytest.mark.asyncio
async def test_process_candidate_set_sends_unsupported_message(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    await app._process_candidate_set(chat_id=10, user_id=11, text="https://example.com", attachments=[])

    assert len(fake_bot.sent_messages) == 1
    assert "Не нашёл поддерживаемых источников" in fake_bot.sent_messages[0]["text"]
    assert "https://example.com" in fake_bot.sent_messages[0]["text"]


@pytest.mark.asyncio
async def test_process_candidate_set_adds_multiple_sources_to_api_draft(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]

    await app._process_candidate_set(
        chat_id=10,
        user_id=11,
        text="https://youtu.be/one https://youtu.be/two",
        attachments=[],
        message_id=77,
    )

    payload = fake_bot.sent_messages[0]
    assert "В корзине 2 источника" in payload["text"]
    assert [entry["source"].source_id for entry in fake_service.draft_additions] == [
        "url:https://youtu.be/one",
        "url:https://youtu.be/two",
    ]
    keyboard = payload["reply_markup"]
    assert keyboard is not None
    buttons = keyboard.inline_keyboard
    assert buttons[0][0].text == "Batch: включен"
    assert buttons[1][0].text == "Запустить batch"
    assert buttons[1][1].text == "Очистить"
    assert buttons[2][0].callback_data.startswith("b:r:")


@pytest.mark.asyncio
async def test_configure_commands_registers_public_command_menu(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    await app._configure_commands()

    assert [command.command for command in fake_bot.commands] == [command.command for command in TELEGRAM_COMMANDS]
    assert [command.command for command in fake_bot.commands] == ["start", "help", "batch", "basket", "clear"]
    assert fake_bot.command_scopes[-1].type == "default"
    assert fake_bot.menu_buttons[-1].type == "commands"


@pytest.mark.asyncio
async def test_batch_command_and_toggle_button_switch_mode(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    message = FakeStatusMessage(10)

    await app._handle_batch_command(message)  # type: ignore[arg-type]

    assert app.batch_modes.is_enabled(10, 7) is False
    assert "выключен" in message.documents[-1]["answer_text"]
    keyboard = message.documents[-1]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].text == "Batch: выключен"

    callback = FakeCallback(data="mode:batch:toggle", message=message)
    await app._handle_batch_mode_toggle(callback)  # type: ignore[arg-type]

    assert app.batch_modes.is_enabled(10, 7) is True
    assert callback.answers == [
        {
            "text": "Batch-режим включен. Новые источники будут добавляться в корзину для одного общего запуска.",
            "show_alert": False,
        }
    ]
    assert message.edits[-1]["reply_markup"].inline_keyboard[0][0].text == "Batch: включен"


@pytest.mark.asyncio
async def test_basket_and_clear_commands_show_controls(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    message = FakeStatusMessage(10)
    fake_service.draft = _make_draft(
        fake_service.draft["draft_id"],
        version=2,
        items=[
            {
                "item_id": "22222222-2222-2222-2222-222222222221",
                "position": 0,
                "source_label": "source_1",
                "source": {
                    "source_kind": "youtube_url",
                    "display_name": "YouTube: demo",
                    "url": "https://youtu.be/demo",
                },
            }
        ],
    )
    app._remember_draft(10, 7, fake_service.draft)

    await app._handle_basket_command(message)  # type: ignore[arg-type]

    assert "В корзине 1" in message.documents[-1]["answer_text"]
    assert message.documents[-1]["reply_markup"].inline_keyboard[1][0].callback_data.startswith("b:s:")

    await app._handle_clear_command(message)  # type: ignore[arg-type]

    assert fake_service.draft_clears[-1]["expected_version"] == 2
    assert message.documents[-1]["answer_text"] == "Корзина очищена."


@pytest.mark.asyncio
async def test_batch_disabled_processes_single_source_immediately(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    app.batch_modes.set_enabled(chat_id=10, user_id=11, enabled=False)

    await app._process_candidate_set(chat_id=10, user_id=11, text="https://youtu.be/demo", attachments=[])

    assert fake_service.draft_additions == []
    assert fake_service.processed_sources[0].url == "https://youtu.be/demo"
    assert fake_bot.sent_messages[0]["text"] == "Batch-режим выключен. Запускаю одиночную обработку."


@pytest.mark.asyncio
async def test_batch_disabled_offers_source_selection_for_multiple_sources(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    app.batch_modes.set_enabled(chat_id=10, user_id=11, enabled=False)

    await app._process_candidate_set(
        chat_id=10,
        user_id=11,
        text="https://youtu.be/one https://youtu.be/two",
        attachments=[],
    )

    assert fake_service.draft_additions == []
    payload = fake_bot.sent_messages[0]
    assert "Выберите один источник" in payload["text"]
    keyboard = payload["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data.startswith("pick:")


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
    assert status.answers[0]["text"] == "hello\n"
    assert _document_entries(status) == []
    keyboard = status.answers[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].text == "Получить по сегментам"
    assert keyboard.inline_keyboard[1][0].text == "Создать исследовательский отчет"


@pytest.mark.asyncio
async def test_start_processing_group_delivers_short_transcript_inline(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    candidates = [
        SourceCandidate(
            source_id="src-1",
            kind="youtube_url",
            display_name="YouTube: one",
            url="https://youtu.be/one",
            telegram_file_id=None,
            mime_type=None,
            file_name=None,
        ),
        SourceCandidate(
            source_id="src-2",
            kind="youtube_url",
            display_name="YouTube: two",
            url="https://youtu.be/two",
            telegram_file_id=None,
            mime_type=None,
            file_name=None,
        ),
    ]

    await app._start_processing_group(chat_id=10, candidates=candidates)

    assert fake_service.processed_groups == [candidates]
    status = fake_bot.sent_messages[0]["status"]
    assert status.edits[-1]["text"] == "Транскрибация готова."
    assert status.answers[0]["text"] == "hello\n"
    assert _document_entries(status) == []
    keyboard = status.answers[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "get:job-123"
    assert keyboard.inline_keyboard[1][0].callback_data == "report:job-123"


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
        "media_analysis_platform.bot.LOGGER",
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
    assert keyboard.inline_keyboard[0][0].callback_data == "deep:report-job-123"


@pytest.mark.asyncio
async def test_handle_generate_report_requires_authoritative_report_job_id(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeStatusMessage(10)
    fake_service = FakeProcessingService(tmp_path)
    assert fake_service.job.report is not None
    fake_service.job.report = ReportArtifacts(
        job_id="",
        markdown_path=fake_service.job.report.markdown_path,
        docx_path=fake_service.job.report.docx_path,
    )
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="report:job-123", message=callback_message)

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Запускаю генерацию отчёта...", "show_alert": False}]
    assert len(callback_message.documents) == 2
    assert callback_message.documents[1]["answer_text"] == (
        "Отчёт сохранён без идентификатора report job; глубокое исследование недоступно."
    )


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

    assert start_message.documents[0]["answer_text"].startswith("Отправьте voice/audio/video/document")
    assert "корзина" in help_message.documents[0]["answer_text"].lower()
    assert "batch" in help_message.documents[0]["answer_text"]


def test_slash_text_is_treated_as_command_menu_request() -> None:
    assert _looks_like_command_menu_request("/") is True
    assert _looks_like_command_menu_request("/unknown") is True
    assert _looks_like_command_menu_request(" //not-command") is False
    assert _looks_like_command_menu_request("https://example.com/a/b") is False


@pytest.mark.asyncio
async def test_handle_message_answers_command_menu_for_bare_slash(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    message = SimpleNamespace(
        text="/",
        caption=None,
        media_group_id=None,
        message_id=99,
        chat=SimpleNamespace(id=10),
        from_user=SimpleNamespace(id=11),
        audio=None,
        voice=None,
        video=None,
        video_note=None,
        document=None,
        documents=[],
    )

    async def answer(text: str, reply_markup=None) -> None:
        message.documents.append({"answer_text": text, "reply_markup": reply_markup})

    message.answer = answer

    await app._handle_message(message)  # type: ignore[arg-type]

    assert message.documents[0]["answer_text"].startswith("Команды доступны в меню Telegram")
    keyboard = message.documents[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "mode:batch:toggle"
    assert fake_bot.sent_messages == []


@pytest.mark.asyncio
async def test_handle_message_delegates_non_media_input_to_processing(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    calls: list[dict[str, object]] = []

    async def fake_process_candidate_set(
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list,
        message_id: int | None = None,
    ) -> None:
        calls.append(
            {"chat_id": chat_id, "user_id": user_id, "text": text, "attachments": attachments, "message_id": message_id}
        )

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    message = SimpleNamespace(
        text="https://youtu.be/demo",
        caption=None,
        media_group_id=None,
        message_id=99,
        chat=SimpleNamespace(id=10),
        from_user=SimpleNamespace(id=11),
        audio=None,
        voice=None,
        video=None,
        video_note=None,
        document=None,
    )

    await app._handle_message(message)  # type: ignore[arg-type]

    assert calls == [{"chat_id": 10, "user_id": 11, "text": "https://youtu.be/demo", "attachments": [], "message_id": 99}]


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

    monkeypatch.setattr("media_analysis_platform.bot.asyncio.create_task", fake_create_task)
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

    async def fake_process_candidate_set(
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list,
        message_id: int | None = None,
    ) -> None:
        forwarded.append(
            {"chat_id": chat_id, "user_id": user_id, "text": text, "attachments": attachments, "message_id": message_id}
        )

    async def fake_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    monkeypatch.setattr("media_analysis_platform.bot.asyncio.sleep", fake_sleep)

    await app._flush_media_group(chat_id=10, user_id=11, media_group_id="group-1")

    assert forwarded[0]["text"] == "caption"
    assert forwarded[0]["message_id"] is None
    assert forwarded[0]["attachments"][0].telegram_file_id == "file-1"
    assert (10, "group-1") not in app.media_group_tasks


@pytest.mark.asyncio
async def test_flush_media_group_stops_on_cancellation(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    called = False

    async def fake_process_candidate_set(
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list,
        message_id: int | None = None,
    ) -> None:
        nonlocal called
        called = True

    async def cancelled_sleep(seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    monkeypatch.setattr("media_analysis_platform.bot.asyncio.sleep", cancelled_sleep)

    await app._flush_media_group(chat_id=10, user_id=11, media_group_id="group-1")

    assert called is False


@pytest.mark.asyncio
async def test_process_candidate_set_adds_single_source_to_api_draft(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    scheduled = FakeTask(done=False)

    def fake_create_task(coro):
        coro.close()
        return scheduled

    monkeypatch.setattr("media_analysis_platform.bot.asyncio.create_task", fake_create_task)

    await app._process_candidate_set(chat_id=10, user_id=11, text="https://youtu.be/demo", attachments=[])

    assert fake_service.draft_additions[0]["source"].display_name == "YouTube: demo"
    assert fake_bot.sent_messages == []
    assert app.basket_summary_added_counts[(10, 11)] == 1
    assert app.basket_summary_tasks[(10, 11)] is scheduled


@pytest.mark.asyncio
async def test_forwarded_single_sources_are_coalesced_into_one_basket_summary(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    app.settings = app.settings.__class__(
        telegram_bot_token=app.settings.telegram_bot_token,
        allowed_user_ids=app.settings.allowed_user_ids,
        whisper_model=app.settings.whisper_model,
        whisper_device=app.settings.whisper_device,
        whisper_compute_type=app.settings.whisper_compute_type,
        report_prompt_suffix=app.settings.report_prompt_suffix,
        media_group_window_seconds=0.01,
        data_dir=app.settings.data_dir,
        youtube_languages=app.settings.youtube_languages,
    )

    async def no_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr("media_analysis_platform.bot.asyncio.sleep", no_sleep)

    for index in range(7):
        await app._process_candidate_set(
            chat_id=10,
            user_id=11,
            text="",
            attachments=[
                SimpleNamespace(
                    telegram_file_id=f"voice-{index}",
                    kind="telegram_audio",
                    file_name=f"voice-{index}.ogg",
                    mime_type="audio/ogg",
                    file_unique_id=f"uniq-{index}",
                )
            ],
            message_id=100 + index,
        )

    pending = app.basket_summary_tasks[(10, 11)]
    await pending

    assert len(fake_bot.sent_messages) == 1
    assert "Добавлено источников: 7" in fake_bot.sent_messages[0]["text"]
    assert "В корзине 7 источника(ов)" in fake_bot.sent_messages[0]["text"]
    keyboard = fake_bot.sent_messages[0]["reply_markup"].inline_keyboard
    assert keyboard[0][0].callback_data == "mode:batch:toggle"
    assert keyboard[1][0].callback_data.startswith("b:s:")


@pytest.mark.asyncio
async def test_concurrent_forwarded_single_sources_share_one_api_draft_and_one_summary(
    tmp_path: Path,
) -> None:
    fake_bot = FakeBot()
    fake_service = UniqueDraftProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    app.settings = app.settings.__class__(
        telegram_bot_token=app.settings.telegram_bot_token,
        allowed_user_ids=app.settings.allowed_user_ids,
        whisper_model=app.settings.whisper_model,
        whisper_device=app.settings.whisper_device,
        whisper_compute_type=app.settings.whisper_compute_type,
        report_prompt_suffix=app.settings.report_prompt_suffix,
        media_group_window_seconds=0.01,
        data_dir=app.settings.data_dir,
        youtube_languages=app.settings.youtube_languages,
    )

    await asyncio.gather(
        *[
            app._process_candidate_set(
                chat_id=10,
                user_id=11,
                text="",
                attachments=[
                    SimpleNamespace(
                        telegram_file_id=f"voice-{index}",
                        kind="telegram_audio",
                        file_name=f"voice-{index}.ogg",
                        mime_type="audio/ogg",
                        file_unique_id=f"uniq-{index}",
                    )
                ],
                message_id=100 + index,
            )
            for index in range(7)
        ]
    )

    await asyncio.sleep(0.05)

    assert len(fake_service.drafts) == 1
    draft = next(iter(fake_service.drafts.values()))
    assert len(draft["items"]) == 7
    assert len(fake_bot.sent_messages) == 1
    assert "Добавлено источников: 7" in fake_bot.sent_messages[0]["text"]
    assert "В корзине 7 источника(ов)" in fake_bot.sent_messages[0]["text"]


@pytest.mark.asyncio
async def test_process_candidate_set_adds_multiple_attachments_to_basket(tmp_path: Path, monkeypatch) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]

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

    assert [entry["source"].telegram_file_id for entry in fake_service.draft_additions] == ["voice-1", "voice-2"]


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
async def test_handle_old_basket_remove_and_clear_callbacks_are_stale(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    message = FakeStatusMessage(10)

    remove_callback = FakeCallback(data="basket:remove:0", message=message)
    await app._handle_basket_callback(remove_callback)  # type: ignore[arg-type]

    assert remove_callback.answers == [
        {
            "text": "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
            "show_alert": True,
        }
    ]

    clear_callback = FakeCallback(data="basket:clear", message=message)
    await app._handle_basket_callback(clear_callback)  # type: ignore[arg-type]

    assert clear_callback.answers == [
        {
            "text": "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
            "show_alert": True,
        }
    ]


@pytest.mark.asyncio
async def test_old_static_basket_start_callback_is_rejected_as_stale(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    callback = FakeCallback(data="basket:start", message=FakeStatusMessage(10))

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {
            "text": "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
            "show_alert": True,
        }
    ]
    assert fake_service.processed_groups == []
    assert fake_service.submitted_drafts == []


@pytest.mark.asyncio
async def test_old_basket_start_callback_does_not_mutate_current_api_draft(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    draft = _make_draft(
        fake_service.draft["draft_id"],
        version=3,
        items=[
            {
                "item_id": "22222222-2222-2222-2222-222222222221",
                "position": 0,
                "source_label": "source_1",
                "source": {"source_kind": "youtube_url", "display_name": "YouTube: current", "url": "https://youtu.be/current"},
            }
        ],
    )
    fake_service.draft = draft
    app._remember_draft(10, 7, draft)
    callback = FakeCallback(data="basket:start", message=FakeStatusMessage(10))

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {
            "text": "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
            "show_alert": True,
        }
    ]
    assert fake_service.submitted_drafts == []
    assert fake_service.draft_removals == []
    assert fake_service.draft_clears == []
    assert app._current_drafts[(10, 7)] == (draft["draft_id"], 3)


@pytest.mark.asyncio
async def test_process_candidate_set_adds_sources_to_api_draft_and_renders_compact_callbacks(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]

    await app._process_candidate_set(
        chat_id=10,
        user_id=7,
        text="https://youtu.be/one https://youtu.be/two",
        attachments=[],
    )

    assert fake_service.created_draft_owners == [
        {
            "owner_type": "telegram",
            "telegram_chat_id": "10",
            "telegram_user_id": "7",
        }
    ]
    assert [entry["source"].url for entry in fake_service.draft_additions] == [
        "https://youtu.be/one",
        "https://youtu.be/two",
    ]
    payload = fake_bot.sent_messages[0]
    assert "В корзине 2 источника" in payload["text"]
    keyboard = payload["reply_markup"].inline_keyboard
    callback_data = [button.callback_data for row in keyboard for button in row]
    assert any(data.startswith("b:s:11111111-1111-1111-1111-111111111111:3") for data in callback_data)
    assert any(data.startswith("b:c:11111111-1111-1111-1111-111111111111:3") for data in callback_data)
    assert any(data.startswith("b:r:11111111-1111-1111-1111-111111111111:3:") for data in callback_data)
    assert all(len(data) <= 64 for data in callback_data)


@pytest.mark.asyncio
async def test_new_draft_start_callback_submits_api_draft_not_local_basket(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    draft = fake_service.create_batch_draft(owner={"owner_type": "telegram", "telegram_chat_id": "10", "telegram_user_id": "7"})
    fake_service.draft = _make_draft(
        draft["draft_id"],
        version=2,
        items=[
            {
                "item_id": "22222222-2222-2222-2222-222222222221",
                "position": 0,
                "source_label": "source_1",
                "source": {"source_kind": "youtube_url", "display_name": "YouTube: demo", "url": "https://youtu.be/demo"},
            }
        ],
    )
    callback = FakeCallback(data=f"b:s:{draft['draft_id']}:2", message=FakeStatusMessage(10))

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    assert fake_service.processed_groups == []
    assert fake_service.submitted_drafts == [
        {
            "draft_id": draft["draft_id"],
            "owner": {
                "owner_type": "telegram",
                "telegram_chat_id": "10",
                "telegram_user_id": "7",
            },
            "expected_version": 2,
        }
    ]
    status = fake_bot.sent_messages[0]["status"]
    assert status.edits[-1]["text"] == "Пакетная транскрибация готова."


@pytest.mark.asyncio
async def test_new_draft_remove_callback_without_token_is_clear_and_safe(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    callback = FakeCallback(data=f"b:r:{fake_service.draft['draft_id']}:2", message=FakeStatusMessage(10))

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    assert callback.answers == [{"text": "Некорректное действие корзины.", "show_alert": True}]
    assert fake_service.draft_removals == []
    assert fake_service.draft_clears == []
    assert fake_service.submitted_drafts == []


@pytest.mark.asyncio
async def test_remove_and_clear_draft_callbacks_pass_version_and_show_stale_conflict(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    draft_id = fake_service.draft["draft_id"]
    item_id = "22222222-2222-2222-2222-222222222221"
    app._item_callback_tokens[(draft_id, "itemtok")] = item_id
    message = FakeStatusMessage(10)

    await app._handle_basket_callback(FakeCallback(data=f"b:r:{draft_id}:4:itemtok", message=message))  # type: ignore[arg-type]

    assert fake_service.draft_removals == [
        {
            "draft_id": draft_id,
            "owner": {
                "owner_type": "telegram",
                "telegram_chat_id": "10",
                "telegram_user_id": "7",
            },
            "expected_version": 4,
            "item_id": item_id,
        }
    ]
    assert message.edits[-1]["text"] == "Корзина пуста."

    def conflict_clear(*, draft_id: str, owner: dict[str, str], expected_version: int) -> dict:
        raise DraftConflictError("version_conflict")

    fake_service.clear_batch_draft = conflict_clear  # type: ignore[assignment]
    conflict = FakeCallback(data=f"b:c:{draft_id}:4", message=message)

    await app._handle_basket_callback(conflict)  # type: ignore[arg-type]

    assert conflict.answers == [
        {
            "text": "Корзина изменилась. Откройте актуальную корзину через /basket.",
            "show_alert": True,
        }
    ]


@pytest.mark.asyncio
async def test_remove_and_clear_callbacks_show_stale_after_submitted_conflict(tmp_path: Path) -> None:
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=FakeBot())  # type: ignore[arg-type]
    draft_id = fake_service.draft["draft_id"]
    item_id = "22222222-2222-2222-2222-222222222221"
    app._item_callback_tokens[(draft_id, "itemtok")] = item_id
    message = FakeStatusMessage(10)

    def submitted_remove(*, draft_id: str, owner: dict[str, str], expected_version: int, item_id: str) -> dict:
        raise DraftConflictError("draft_submitted")

    def submitted_clear(*, draft_id: str, owner: dict[str, str], expected_version: int) -> dict:
        raise DraftConflictError("draft_submitted")

    fake_service.remove_draft_item = submitted_remove  # type: ignore[assignment]
    fake_service.clear_batch_draft = submitted_clear  # type: ignore[assignment]

    remove_callback = FakeCallback(data=f"b:r:{draft_id}:4:itemtok", message=message)
    clear_callback = FakeCallback(data=f"b:c:{draft_id}:4", message=message)

    await app._handle_basket_callback(remove_callback)  # type: ignore[arg-type]
    await app._handle_basket_callback(clear_callback)  # type: ignore[arg-type]

    assert remove_callback.answers == [
        {
            "text": "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
            "show_alert": True,
        }
    ]
    assert clear_callback.answers == [
        {
            "text": "Эта кнопка устарела. Откройте /basket или отправьте новый источник.",
            "show_alert": True,
        }
    ]


@pytest.mark.asyncio
async def test_start_draft_callback_owner_mismatch_is_handled_as_stale(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)

    def owner_mismatch_submit(*, draft_id: str, owner: dict[str, str], expected_version: int) -> ProcessedJob:
        raise DraftConflictError("draft_owner_mismatch")

    fake_service.submit_batch_draft = owner_mismatch_submit  # type: ignore[assignment]
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    draft = _make_draft(
        fake_service.draft["draft_id"],
        version=2,
        items=[
            {
                "item_id": "22222222-2222-2222-2222-222222222221",
                "position": 0,
                "source_label": "source_1",
                "source": {"source_kind": "youtube_url", "display_name": "YouTube: demo", "url": "https://youtu.be/demo"},
            }
        ],
    )
    fake_service.draft = draft
    callback = FakeCallback(data=f"b:s:{draft['draft_id']}:2", message=FakeStatusMessage(10), user_id=999)

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    status = fake_bot.sent_messages[0]["status"]
    assert status.edits[-1]["text"] == "Эта кнопка устарела. Откройте /basket или отправьте новый источник."
    assert _document_entries(status) == []


@pytest.mark.asyncio
async def test_start_basket_processing_submits_api_draft_after_mixed_sources(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    fake_service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), fake_service, bot=fake_bot)  # type: ignore[arg-type]
    await app._process_candidate_set(
        chat_id=10,
        user_id=7,
        text="https://youtu.be/demo",
        attachments=[
            SimpleNamespace(
                telegram_file_id="file-telegram",
                kind="telegram_audio",
                file_name="call.ogg",
                mime_type="audio/ogg",
                file_unique_id="call-uniq",
            ),
        ],
        message_id=42,
    )
    callback_data = fake_bot.sent_messages[0]["reply_markup"].inline_keyboard[1][0].callback_data

    await app._handle_basket_callback(FakeCallback(data=callback_data, message=FakeStatusMessage(10)))  # type: ignore[arg-type]

    assert fake_service.processed_sources == []
    assert fake_service.processed_groups == []
    assert fake_service.submitted_drafts[-1]["draft_id"] == fake_service.draft["draft_id"]
    uploaded_source = next(entry["source"] for entry in fake_service.draft_additions if entry["source"].telegram_file_id)
    url_source = next(entry["source"] for entry in fake_service.draft_additions if entry["source"].url)
    assert uploaded_source.local_path is not None
    assert uploaded_source.local_path.exists()
    assert url_source.url == "https://youtu.be/demo"
    status = fake_bot.sent_messages[1]["status"]
    assert status.edits[-1]["text"] == "Пакетная транскрибация готова."
    assert status.answers[0]["text"] == "hello\n"
    assert _document_entries(status) == []
    keyboard = status.answers[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].callback_data == "get:job-123"
    assert keyboard.inline_keyboard[1][0].callback_data == "report:job-123"


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

    monkeypatch.setattr("media_analysis_platform.bot.TelegramBadRequest", RuntimeError)
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

    async def fake_process_candidate_set(
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list,
        message_id: int | None = None,
    ) -> None:
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


def test_extract_attachments_handles_audio_and_generic_documents() -> None:
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
    generic_document_message = SimpleNamespace(
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
    generic_attachments = _extract_attachments(generic_document_message)  # type: ignore[arg-type]

    assert len(audio_attachments) == 1
    assert audio_attachments[0].kind == "telegram_audio"
    assert audio_attachments[0].file_name == "uniq-a1.bin"
    assert len(generic_attachments) == 1
    assert generic_attachments[0].kind == "telegram_audio"
    assert generic_attachments[0].file_name == "notes.txt"


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
