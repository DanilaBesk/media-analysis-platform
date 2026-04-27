from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from media_analysis_platform.bot import (
    TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS,
    TelegramTranscriberApp,
    _build_draft_text,
    _build_processing_status_text,
    _present_source_label_for_telegram,
)
from media_analysis_platform.config import Settings
from telegram_adapter.i18n import TelegramCommandKey, TelegramLocaleService, TelegramTextKey
from telegram_adapter.i18n.catalogs import TRANSLATION_CATALOGS
from transcriber_workers_common.domain import (
    ProcessedJob,
    ReportArtifacts,
    SourceCandidate,
    TranscriptArtifacts,
)
from transcriber_workers_common.source_labels import humanize_source_label


class FakeAnswerMessage:
    def __init__(self, chat_id: int, user_id: int = 7, language_code: str = "ru") -> None:
        self.chat = SimpleNamespace(id=chat_id)
        self.from_user = SimpleNamespace(id=user_id, language_code=language_code)
        self.answers: list[dict[str, object]] = []
        self.edits: list[dict[str, object]] = []
        self.documents: list[dict[str, object]] = []

    async def answer(self, text: str, reply_markup=None) -> "FakeAnswerMessage":
        payload = {"text": text, "reply_markup": reply_markup}
        self.answers.append(payload)
        return self

    async def answer_document(self, document, caption: str = "", reply_markup=None) -> None:
        self.documents.append({"document": document, "caption": caption, "reply_markup": reply_markup})

    async def edit_text(self, text: str, reply_markup=None) -> None:
        self.edits.append({"text": text, "reply_markup": reply_markup})


class FakeBot:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, object]] = []
        self.commands_calls: list[dict[str, object]] = []
        self.menu_buttons: list[object] = []
        self.downloads: list[dict[str, object]] = []

    async def send_message(self, chat_id: int, text: str, reply_markup=None) -> FakeAnswerMessage:
        status = FakeAnswerMessage(chat_id)
        payload = {"chat_id": chat_id, "text": text, "reply_markup": reply_markup, "status": status}
        self.sent_messages.append(payload)
        return status

    async def set_my_commands(self, commands: list[object], scope=None, language_code: str | None = None) -> None:
        self.commands_calls.append({"commands": commands, "scope": scope, "language_code": language_code})

    async def set_chat_menu_button(self, menu_button=None) -> None:
        self.menu_buttons.append(menu_button)

    async def download(self, file_id: str, destination: Path) -> None:
        destination.write_bytes(b"fake-media")
        self.downloads.append({"file_id": file_id, "destination": destination})


class FakeProcessingService:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.job = _make_job(tmp_path)
        self.draft = _make_draft(
            "11111111-1111-1111-1111-111111111111",
            version=1,
            items=[],
        )
        self.draft_additions: list[SourceCandidate] = []
        self.raise_on_report = False
        self.raise_on_deep_research = False

    def process_source(self, source: SourceCandidate) -> ProcessedJob:
        return self.job

    def process_source_group(self, sources: list[SourceCandidate]) -> ProcessedJob:
        return self.job

    def create_batch_draft(self, *, owner: dict[str, str], display_name: str | None = None) -> dict:
        self.draft = _make_draft(self.draft["draft_id"], version=1, items=[])
        return self.draft

    def load_batch_draft(self, *, draft_id: str, owner: dict[str, str]) -> dict:
        return self.draft

    def add_source_to_draft(self, draft: dict, *, owner: dict[str, str], source: SourceCandidate) -> dict:
        self.draft_additions.append(source)
        item_index = len(self.draft["items"]) + 1
        item = {
            "item_id": f"item-{item_index}",
            "position": item_index - 1,
            "source_label": f"source_{item_index}",
            "source": {
                "display_name": source.display_name,
                "url": source.url,
                "original_filename": source.file_name,
            },
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
        self.draft = _make_draft(draft_id, version=expected_version + 1, items=[])
        return self.draft

    def clear_batch_draft(self, *, draft_id: str, owner: dict[str, str], expected_version: int) -> dict:
        self.draft = _make_draft(draft_id, version=expected_version + 1, items=[])
        return self.draft

    def submit_batch_draft(self, *, draft_id: str, owner: dict[str, str], expected_version: int) -> ProcessedJob:
        return self.job

    def load_job(self, job_id: str) -> ProcessedJob:
        return self.job

    def ensure_report(self, job_id: str, report_prompt_suffix: str = "") -> ProcessedJob:
        if self.raise_on_report:
            raise RuntimeError("boom")
        return self.job

    def ensure_deep_research(self, job_id: str) -> Path:
        if self.raise_on_deep_research:
            raise RuntimeError("deep boom")
        report_path = self.job.workspace_dir / "deep-research.md"
        report_path.write_text("# Deep research\n", encoding="utf-8")
        return report_path


class FakeApiError(RuntimeError):
    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


class FakeCallback:
    def __init__(self, *, data: str | None, message: FakeAnswerMessage | None, language_code: str = "ru") -> None:
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=7, language_code=language_code)
        self.answers: list[dict[str, object]] = []

    async def answer(self, text: str, show_alert: bool = False) -> None:
        self.answers.append({"text": text, "show_alert": show_alert})


def make_settings(tmp_path: Path, *, allowed_user_ids: tuple[int, ...] = ()) -> Settings:
    return Settings(
        telegram_bot_token="123456:dummy-token",
        data_dir=tmp_path / ".data",
        allowed_user_ids=allowed_user_ids,
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
    transcript_md = workspace / "transcript.md"
    transcript_md.write_text("# Transcript\n", encoding="utf-8")
    transcript_docx = workspace / "transcript.docx"
    transcript_docx.write_bytes(b"docx")
    transcript_txt = workspace / "transcript.txt"
    transcript_txt.write_text("hello\n", encoding="utf-8")
    report_md = workspace / "report.md"
    report_md.write_text("# Report\n", encoding="utf-8")
    report_docx = workspace / "report.docx"
    report_docx.write_bytes(b"docx")
    return ProcessedJob(
        job_id="job-123",
        source=SourceCandidate(
            source_id="src-1",
            kind="youtube_url",
            display_name="YouTube: demo",
            url="https://youtu.be/demo",
            telegram_file_id=None,
            mime_type=None,
            file_name=None,
        ),
        workspace_dir=workspace,
        transcript=TranscriptArtifacts(
            markdown_path=transcript_md,
            docx_path=transcript_docx,
            text_path=transcript_txt,
        ),
        report=ReportArtifacts(
            job_id="report-job-123",
            markdown_path=report_md,
            docx_path=report_docx,
        ),
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


def _attachment(file_id: str, file_name: str) -> SimpleNamespace:
    return SimpleNamespace(
        telegram_file_id=file_id,
        kind="telegram_audio",
        file_name=file_name,
        mime_type="audio/ogg",
        file_unique_id=file_id,
    )


@pytest.mark.asyncio
async def test_configure_commands_registers_default_and_en_localized_descriptions(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]

    await app._configure_commands()

    assert [call["language_code"] for call in fake_bot.commands_calls] == [None, "ru", "en"]
    for call in fake_bot.commands_calls:
        locale = call["language_code"] or "ru"
        descriptions = [command.description for command in call["commands"]]
        assert descriptions == [
            app.locale_service.text(TelegramTextKey.COMMAND_START_DESCRIPTION, locale=locale),
            app.locale_service.text(TelegramTextKey.COMMAND_HELP_DESCRIPTION, locale=locale),
            app.locale_service.text(TelegramTextKey.COMMAND_BATCH_DESCRIPTION, locale=locale),
            app.locale_service.text(TelegramTextKey.COMMAND_BASKET_DESCRIPTION, locale=locale),
            app.locale_service.text(TelegramTextKey.COMMAND_CLEAR_DESCRIPTION, locale=locale),
        ]
        assert [command.command for command in call["commands"]] == [key.value for key in TelegramCommandKey]


def test_bot_py_does_not_embed_translation_catalog_copy() -> None:
    source = Path(__file__).resolve().parents[1] / "src" / "media_analysis_platform" / "bot.py"
    bot_source = source.read_text(encoding="utf-8")

    for locale_catalog in TRANSLATION_CATALOGS.values():
        for value in locale_catalog.values():
            if len(value) < 12:
                continue
            assert value not in bot_source


def test_present_source_label_localizes_shared_telegram_fallbacks_for_en() -> None:
    service = TelegramLocaleService()
    shared_label = humanize_source_label("Audio: AgADBproAAg_9MUs.ogg")

    assert shared_label == service.text(TelegramTextKey.SOURCE_LABEL_TELEGRAM_AUDIO, locale="ru")
    assert _present_source_label_for_telegram(service, "en", "Audio: AgADBproAAg_9MUs.ogg") == service.text(
        TelegramTextKey.SOURCE_LABEL_TELEGRAM_AUDIO,
        locale="en",
    )


@pytest.mark.asyncio
async def test_handle_start_and_help_are_locale_aware_for_ru_and_en(tmp_path: Path) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    start_message = FakeAnswerMessage(chat_id=10, language_code="en")
    help_message = FakeAnswerMessage(chat_id=10, language_code="ru")

    await app._handle_start(start_message)  # type: ignore[arg-type]
    await app._handle_help(help_message)  # type: ignore[arg-type]

    assert start_message.answers[0]["text"] == app.locale_service.text(TelegramTextKey.START_PROMPT, locale="en")
    assert help_message.answers[0]["text"] == app.locale_service.text(TelegramTextKey.HELP_MENU, locale="ru")


@pytest.mark.asyncio
async def test_single_mode_selection_alerts_use_callback_locale(tmp_path: Path) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    callback = FakeCallback(data="pick:broken", message=FakeAnswerMessage(10), language_code="en")

    await app._handle_source_selection(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {
            "text": app.locale_service.text(TelegramTextKey.SELECTION_INVALID, locale="en"),
            "show_alert": True,
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("locale", ["ru", "en"])
async def test_stale_basket_alert_uses_callback_locale(tmp_path: Path, locale: str) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    callback = FakeCallback(data="basket:start", message=FakeAnswerMessage(10), language_code=locale)

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {
            "text": app.locale_service.text(TelegramTextKey.BASKET_STALE_BUTTON, locale=locale),
            "show_alert": True,
        }
    ]


@pytest.mark.asyncio
async def test_start_processing_uses_en_status_and_result_buttons(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    candidate = SourceCandidate(
        source_id="src-2",
        kind="telegram_audio",
        display_name="Audio: AgADBproAAg_9MUs.ogg",
        url=None,
        telegram_file_id="file-telegram",
        mime_type="audio/ogg",
        file_name="AgADBproAAg_9MUs.ogg",
    )

    await app._start_processing(chat_id=10, candidate=candidate, locale="en")

    status = fake_bot.sent_messages[0]["status"]
    assert fake_bot.sent_messages[0]["text"] == _build_processing_status_text(app.locale_service, "en", candidate)
    assert status.edits[-1]["text"] == app.locale_service.text(TelegramTextKey.PROCESSING_DONE, locale="en")
    assert status.answers[0]["text"] == "hello\n"
    keyboard = status.answers[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].text == app.locale_service.text(
        TelegramTextKey.RESULT_BUTTON_SEGMENTS,
        locale="en",
    )
    assert keyboard.inline_keyboard[1][0].text == app.locale_service.text(
        TelegramTextKey.RESULT_BUTTON_REPORT,
        locale="en",
    )


@pytest.mark.asyncio
async def test_send_transcript_result_uses_locale_specific_fallback_caption(tmp_path: Path) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    app.processing_service.job.transcript.text_path.write_text(  # type: ignore[attr-defined]
        "x" * (TELEGRAM_INLINE_TRANSCRIPT_MAX_CHARS + 1),
        encoding="utf-8",
    )
    status = FakeAnswerMessage(10)

    await app._send_transcript_result(
        status,  # type: ignore[arg-type]
        app.processing_service.job,  # type: ignore[attr-defined]
        fallback_caption=app.locale_service.text(TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_BASKET, locale="en"),
        locale="en",
    )

    assert status.documents[0]["caption"] == app.locale_service.text(
        TelegramTextKey.TRANSCRIPT_FALLBACK_CAPTION_BASKET,
        locale="en",
    )


@pytest.mark.asyncio
async def test_report_flow_uses_locale_specific_en_copy_and_deep_research_button(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeAnswerMessage(10, language_code="en")
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="report:job-123", message=callback_message, language_code="en")

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {"text": app.locale_service.text(TelegramTextKey.REPORT_ACK, locale="en"), "show_alert": False}
    ]
    status = callback_message.answers[0]
    assert status["text"] == app.locale_service.text(TelegramTextKey.REPORT_STATUS, locale="en")
    assert callback_message.edits[-1]["text"] == app.locale_service.text(TelegramTextKey.REPORT_READY, locale="en")
    keyboard = callback_message.documents[0]["reply_markup"]
    assert keyboard.inline_keyboard[0][0].text == app.locale_service.text(
        TelegramTextKey.REPORT_BUTTON_DEEP_RESEARCH,
        locale="en",
    )


@pytest.mark.asyncio
async def test_report_errors_use_locale_specific_copy(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeAnswerMessage(10, language_code="en")
    service = FakeProcessingService(tmp_path)
    service.raise_on_report = True
    app = TelegramTranscriberApp(make_settings(tmp_path), service, bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="report:job-123", message=callback_message, language_code="en")

    await app._handle_generate_report(callback)  # type: ignore[arg-type]

    assert callback_message.edits[-1]["text"] == app.locale_service.text(
        TelegramTextKey.REPORT_FAILED,
        locale="en",
        error=RuntimeError("boom"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("locale", ["ru", "en"])
async def test_delayed_basket_summary_keeps_locale(tmp_path: Path, monkeypatch, locale: str) -> None:
    fake_bot = FakeBot()
    service = FakeProcessingService(tmp_path)
    app = TelegramTranscriberApp(make_settings(tmp_path), service, bot=fake_bot)  # type: ignore[arg-type]
    scheduled: list[object] = []
    original_create_task = asyncio.create_task

    def fake_create_task(coro):
        task = original_create_task(coro)
        scheduled.append(task)
        return task

    async def no_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr("media_analysis_platform.bot.asyncio.create_task", fake_create_task)
    monkeypatch.setattr("media_analysis_platform.bot.asyncio.sleep", no_sleep)

    await app._process_candidate_set(
        chat_id=10,
        user_id=7,
        text="https://youtu.be/demo",
        attachments=[],
        locale=locale,
    )
    await scheduled[0]

    draft = service.draft
    assert fake_bot.sent_messages[0]["text"] == _build_draft_text(
        app.locale_service,
        locale,
        draft,
        added_count=1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("locale", ["ru", "en"])
async def test_media_group_flush_forwards_stored_locale(tmp_path: Path, monkeypatch, locale: str) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    attachment = _attachment("file-1", "voice.ogg")
    app.media_groups.add(10, "group-1", attachment)
    app.media_group_text[(10, "group-1")] = "caption"
    app.media_group_locales[(10, "group-1")] = locale
    forwarded: list[dict[str, object]] = []

    async def fake_process_candidate_set(
        chat_id: int,
        user_id: int | None,
        text: str,
        attachments: list,
        message_id: int | None = None,
        locale: str | None = None,
    ) -> None:
        forwarded.append(
            {
                "chat_id": chat_id,
                "user_id": user_id,
                "text": text,
                "attachments": attachments,
                "message_id": message_id,
                "locale": locale,
            }
        )

    async def no_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(app, "_process_candidate_set", fake_process_candidate_set)
    monkeypatch.setattr("media_analysis_platform.bot.asyncio.sleep", no_sleep)

    await app._flush_media_group(chat_id=10, user_id=7, media_group_id="group-1")

    assert forwarded == [
        {
            "chat_id": 10,
            "user_id": 7,
            "text": "caption",
            "attachments": [attachment],
            "message_id": None,
            "locale": locale,
        }
    ]


@pytest.mark.parametrize("locale", ["ru", "en"])
def test_basket_text_and_keyboard_are_key_driven_with_compact_callbacks(tmp_path: Path, locale: str) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    draft = _make_draft(
        "11111111-1111-1111-1111-111111111111",
        version=3,
        items=[
            {
                "item_id": "item-1",
                "position": 0,
                "source_label": "source_1",
                "source": {"display_name": "Audio: AgADBproAAg_9MUs.ogg"},
            }
        ],
    )

    text = _build_draft_text(app.locale_service, locale, draft, added_count=1)
    keyboard = app._build_draft_keyboard(draft, locale=locale)
    callback_data = [button.callback_data for row in keyboard.inline_keyboard for button in row]

    assert text == "\n".join(
        [
            app.locale_service.text(TelegramTextKey.BASKET_SUMMARY_ADDED, locale=locale, count=1),
            app.locale_service.text(TelegramTextKey.BASKET_SUMMARY_HEADER, locale=locale, count=1),
            app.locale_service.text(
                TelegramTextKey.BASKET_SUMMARY_ITEM,
                locale=locale,
                index=1,
                label=app.locale_service.text(TelegramTextKey.SOURCE_LABEL_TELEGRAM_AUDIO, locale=locale),
            ),
            app.locale_service.text(TelegramTextKey.BASKET_SUMMARY_FOOTER, locale=locale),
        ]
    )
    assert callback_data == [
        "mode:batch:toggle",
        "b:s:11111111-1111-1111-1111-111111111111:3",
        "b:c:11111111-1111-1111-1111-111111111111:3",
        callback_data[3],
    ]
    assert callback_data[3].startswith("b:r:11111111-1111-1111-1111-111111111111:3:")


@pytest.mark.asyncio
async def test_deep_research_flow_uses_locale_specific_en_copy(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeAnswerMessage(10, language_code="en")
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="deep:report-job-123", message=callback_message, language_code="en")

    await app._handle_deep_research(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {"text": app.locale_service.text(TelegramTextKey.DEEP_RESEARCH_ACK, locale="en"), "show_alert": False}
    ]
    status = callback_message.answers[0]
    assert status["text"] == app.locale_service.text(TelegramTextKey.DEEP_RESEARCH_STATUS, locale="en")
    assert callback_message.edits[-1]["text"] == app.locale_service.text(TelegramTextKey.DEEP_RESEARCH_READY, locale="en")
    assert callback_message.documents[0]["caption"] == app.locale_service.text(
        TelegramTextKey.DEEP_RESEARCH_CAPTION,
        locale="en",
    )


@pytest.mark.asyncio
async def test_deep_research_errors_use_locale_specific_copy(tmp_path: Path) -> None:
    fake_bot = FakeBot()
    callback_message = FakeAnswerMessage(10, language_code="en")
    service = FakeProcessingService(tmp_path)
    service.raise_on_deep_research = True
    app = TelegramTranscriberApp(make_settings(tmp_path), service, bot=fake_bot)  # type: ignore[arg-type]
    callback = FakeCallback(data="deep:report-job-123", message=callback_message, language_code="en")

    await app._handle_deep_research(callback)  # type: ignore[arg-type]

    assert callback_message.edits[-1]["text"] == app.locale_service.text(
        TelegramTextKey.DEEP_RESEARCH_FAILED,
        locale="en",
        error=RuntimeError("deep boom"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("locale", ["ru", "en"])
async def test_access_denied_is_locale_aware_for_messages_and_callbacks(tmp_path: Path, locale: str) -> None:
    app = TelegramTranscriberApp(
        make_settings(tmp_path, allowed_user_ids=(42,)),
        FakeProcessingService(tmp_path),
        bot=FakeBot(),
    )  # type: ignore[arg-type]
    message = FakeAnswerMessage(chat_id=10, user_id=7, language_code=locale)
    callback = FakeCallback(
        data="b:c:11111111-1111-1111-1111-111111111111:3",
        message=FakeAnswerMessage(10),
        language_code=locale,
    )

    assert not await app._ensure_message_allowed(message)  # type: ignore[arg-type]
    assert not await app._ensure_callback_allowed(callback)  # type: ignore[arg-type]
    assert message.answers == [
        {"text": app.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=locale), "reply_markup": None}
    ]
    assert callback.answers == [
        {"text": app.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=locale), "show_alert": True}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "expected_key"),
    [
        ("version_conflict", TelegramTextKey.BASKET_VERSION_CONFLICT),
        ("draft_submitted", TelegramTextKey.BASKET_STALE_BUTTON),
    ],
)
@pytest.mark.parametrize("locale", ["ru", "en"])
async def test_basket_callback_stale_api_outcomes_use_locale_specific_copy(
    tmp_path: Path,
    code: str,
    expected_key: TelegramTextKey,
    locale: str,
) -> None:
    app = TelegramTranscriberApp(make_settings(tmp_path), FakeProcessingService(tmp_path), bot=FakeBot())  # type: ignore[arg-type]
    callback = FakeCallback(
        data="b:c:11111111-1111-1111-1111-111111111111:3",
        message=FakeAnswerMessage(10),
        language_code=locale,
    )

    def raise_stale(*, draft_id: str, owner: dict[str, str], expected_version: int) -> dict:
        del draft_id, owner, expected_version
        raise FakeApiError(code)

    app.processing_service.clear_batch_draft = raise_stale  # type: ignore[method-assign]

    await app._handle_basket_callback(callback)  # type: ignore[arg-type]

    assert callback.answers == [
        {
            "text": app.locale_service.text(expected_key, locale=locale),
            "show_alert": True,
        }
    ]
