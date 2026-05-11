from __future__ import annotations

import asyncio
import inspect
import logging
from pathlib import Path
import runpy
from types import SimpleNamespace
from typing import Any

import pytest

from telegram_adapter import __main__ as telegram_main
from telegram_adapter.api_client import TelegramApiClientError
from telegram_adapter.bot import (
    TelegramInboxApp,
    _artifact_label,
    _chat_type,
    _decode_callback_token,
    _decode_callback_version,
    _decode_optional_callback_token,
    _detail_prefix,
    _diagnostic_label,
    _encode_callback_token,
    _encode_callback_version,
    _help_text,
    _media_group_id,
    _message_files,
    _message_text,
    _normalize_callback_error,
    _normalize_message_error,
    _parse_callback_payload,
    _start_text,
    build_status_keyboard,
)
from telegram_adapter.config import TelegramAdapterSettings, load_settings
from telegram_adapter.errors import TelegramUserError, TelegramUserErrorCode, rejected_reason_text
from telegram_adapter.gateway import InboxStatus, IngressRecord, TelegramInboxGateway
from telegram_adapter.i18n import DEFAULT_LOCALE, TelegramTextKey


class FakeFinalApiClient:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []
        self.collection = {
            "collection_id": "inbox-1",
            "version": 1,
            "items": [],
        }
        self.runs: list[dict[str, Any]] = []
        self.selections: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.diagnostics: list[dict[str, Any]] = []
        self.add_requests: list[dict[str, Any]] = []
        self.upload_requests: list[dict[str, Any]] = []
        self.remove_requests: list[dict[str, Any]] = []

    def add_media_item(self, **kwargs) -> dict[str, Any]:
        media_item = {
            "media_item_id": f"media-{len(self.items) + 1}",
            "kind": kwargs["kind"],
            "status": "ready",
            "display_name": kwargs.get("display_name") or kwargs["kind"],
            "source": kwargs["source"],
            "metadata": kwargs.get("metadata") or {},
        }
        self.add_requests.append(kwargs)
        self.items.append(media_item)
        self.collection["items"].append({"media_item_id": media_item["media_item_id"], "position": len(self.items) - 1})
        return media_item

    def upload_media_item(self, **kwargs) -> dict[str, Any]:
        media_item = {
            "media_item_id": f"media-{len(self.items) + 1}",
            "kind": kwargs["kind"],
            "status": "ready",
            "display_name": kwargs.get("display_name") or kwargs.get("file_name") or kwargs["kind"],
            "source": {
                "origin_type": "object",
                "object_key": f"sources/{kwargs['kind']}/{len(self.items) + 1}-{kwargs.get('file_name') or 'upload.bin'}",
                "mime_type": kwargs.get("content_type"),
                "size_bytes": len(kwargs["content"]),
            },
            "metadata": kwargs.get("metadata") or {},
        }
        self.upload_requests.append(kwargs)
        self.items.append(media_item)
        self.collection["items"].append({"media_item_id": media_item["media_item_id"], "position": len(self.items) - 1})
        return media_item

    def list_media_items(self, **kwargs) -> dict[str, Any]:
        page_size = kwargs.get("page_size") or 5
        start = int(kwargs.get("cursor") or 0)
        next_start = start + page_size
        return {
            "items": self.items[start:next_start],
            "page": {
                "page_size": page_size,
                "has_more": len(self.items) > next_start,
                "next_cursor": str(next_start) if len(self.items) > next_start else "",
            },
        }

    def get_inbox_collection(self, **kwargs) -> dict[str, Any]:
        return self.collection

    def remove_collection_item(self, **kwargs) -> dict[str, Any]:
        media_item_id = kwargs["media_item_id"]
        self.remove_requests.append(kwargs)
        self.collection["items"] = [item for item in self.collection["items"] if item["media_item_id"] != media_item_id]
        self.items = [item for item in self.items if item["media_item_id"] != media_item_id]
        self.collection["version"] += 1
        return self.collection

    def create_selection(self, **kwargs) -> dict[str, Any]:
        selection = {
            "selection_id": f"selection-{len(self.selections) + 1}",
            "items": kwargs["items"],
            "source_collection_id": kwargs.get("source_collection_id"),
        }
        self.selections.append(selection)
        return selection

    def create_analysis_run(self, **kwargs) -> dict[str, Any]:
        run = {
            "analysis_run_id": f"run-{len(self.runs) + 1}",
            "selection_id": kwargs["selection_id"],
            "run_type": kwargs["run_type"],
            "status": "queued",
            "version": 1,
        }
        self.runs.append(run)
        return run

    def list_analysis_runs(self, **kwargs) -> dict[str, Any]:
        return {"items": list(self.runs), "page": {"page_size": 10, "has_more": False}}

    def get_analysis_run(self, **kwargs) -> dict[str, Any]:
        analysis_run_id = kwargs["analysis_run_id"]
        return next(run for run in self.runs if run["analysis_run_id"] == analysis_run_id)

    def list_artifacts(self, **kwargs) -> dict[str, Any]:
        analysis_run_id = kwargs.get("analysis_run_id")
        return {
            "items": [
                artifact
                for artifact in self.artifacts
                if not analysis_run_id or artifact["analysis_run_id"] == analysis_run_id
            ],
            "page": {"page_size": 10, "has_more": False},
        }

    def list_diagnostics(self, **kwargs) -> dict[str, Any]:
        subject_type = kwargs.get("subject_type")
        subject_id = kwargs.get("subject_id")
        return {
            "items": [
                diagnostic
                for diagnostic in self.diagnostics
                if (not subject_type or diagnostic.get("subject_type") == subject_type)
                and (not subject_id or diagnostic.get("subject_id") == subject_id)
            ],
            "page": {"page_size": 10, "has_more": False},
        }


class FakeBot:
    def __init__(self, *, file_bytes: dict[str, bytes] | None = None, edit_error: Exception | None = None) -> None:
        self.file_bytes = file_bytes or {}
        self.edit_error = edit_error
        self.set_commands_calls: list[tuple[list[Any], str]] = []
        self.get_file_calls: list[str] = []
        self.download_calls: list[str] = []
        self.edit_calls: list[dict[str, Any]] = []

    async def set_my_commands(self, commands: list[Any], *, language_code: str) -> None:
        self.set_commands_calls.append((commands, language_code))

    async def get_file(self, file_id: str) -> SimpleNamespace:
        self.get_file_calls.append(file_id)
        return SimpleNamespace(file_path=f"remote/{file_id}")

    async def download_file(self, file_path: str, *, destination: Any) -> None:
        self.download_calls.append(file_path)
        destination.write(self.file_bytes.get(file_path, b""))

    async def edit_message_text(self, text: str, *, chat_id: int, message_id: int, reply_markup: Any) -> None:
        if self.edit_error is not None:
            raise self.edit_error
        self.edit_calls.append(
            {
                "text": text,
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": reply_markup,
            }
        )


class FakeMessage:
    def __init__(
        self,
        *,
        text: str | None = None,
        caption: str | None = None,
        from_user_id: int | None = 7,
        message_id: int = 101,
        photo: list[Any] | None = None,
        video: Any | None = None,
        document: Any | None = None,
        audio: Any | None = None,
        voice: Any | None = None,
        media_group_id: str | None = None,
    ) -> None:
        self.chat = SimpleNamespace(id=10, type="private")
        self.from_user = SimpleNamespace(id=from_user_id) if from_user_id is not None else None
        self.message_thread_id = None
        self.text = text
        self.caption = caption
        self.message_id = message_id
        self.photo = photo or []
        self.video = video
        self.document = document
        self.audio = audio
        self.voice = voice
        self.media_group_id = media_group_id
        self.answers: list[dict[str, Any]] = []
        self.edits: list[dict[str, Any]] = []

    async def answer(self, text: str, **kwargs) -> SimpleNamespace:
        self.answers.append({"text": text, **kwargs})
        return SimpleNamespace(message_id=9001)

    async def edit_text(self, text: str, **kwargs) -> None:
        self.edits.append({"text": text, **kwargs})


class FakeCallback:
    def __init__(
        self,
        *,
        data: str,
        message: FakeMessage | None,
        from_user_id: int | None = 7,
    ) -> None:
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=from_user_id) if from_user_id is not None else None
        self.answers: list[dict[str, Any]] = []

    async def answer(self, text: str, show_alert: bool = False) -> None:
        self.answers.append({"text": text, "show_alert": show_alert})


def owner() -> dict[str, Any]:
    return {
        "owner_type": "telegram",
        "owner_id": "chat:10:user:7",
        "adapter_identity": {"telegram_chat_id": "10", "telegram_user_id": "7"},
    }


def make_app(*, page_size: int = 5, bot: FakeBot | None = None) -> tuple[FakeFinalApiClient, TelegramInboxGateway, TelegramInboxApp]:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=page_size)
    app = TelegramInboxApp(
        TelegramAdapterSettings(telegram_bot_token="token", allowed_user_ids=()),
        gateway,
        bot=bot or FakeBot(),
    )
    return api, gateway, app


def status_for(
    gateway: TelegramInboxGateway,
    *,
    rejected: list[IngressRecord] | None = None,
    cursor: str | None = None,
) -> InboxStatus:
    return gateway.restore_status(owner=owner(), rejected=rejected, cursor=cursor)


def test_load_settings_reads_explicit_env_mapping() -> None:
    settings = load_settings(
        Path("/tmp/runtime"),
        env={
            "TELEGRAM_BOT_TOKEN": "  secret-token  ",
            "ALLOWED_USER_IDS": "1, 2,3",
        },
    )

    assert settings.telegram_bot_token == "secret-token"
    assert settings.allowed_user_ids == (1, 2, 3)


def test_load_settings_loads_dotenv_from_base_dir_when_env_is_implicit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    loaded: dict[str, Path] = {}

    def fake_load_dotenv(path: Path) -> None:
        loaded["path"] = path

    monkeypatch.setattr("telegram_adapter.config.load_dotenv", fake_load_dotenv)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "from-os-env")
    monkeypatch.setenv("ALLOWED_USER_IDS", "7")

    settings = load_settings(tmp_path)

    assert settings == TelegramAdapterSettings(telegram_bot_token="from-os-env", allowed_user_ids=(7,))
    assert loaded["path"] == tmp_path / ".env"


def test_load_settings_requires_bot_token() -> None:
    with pytest.raises(RuntimeError, match="TELEGRAM_BOT_TOKEN is required"):
        load_settings(Path("/tmp/runtime"), env={"ALLOWED_USER_IDS": "1"})


def test_run_builds_adapter_dependencies_and_uses_default_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    settings = TelegramAdapterSettings(telegram_bot_token="token", allowed_user_ids=(7,))

    def fake_load_settings(base_dir: Path, *, env: dict[str, str]) -> TelegramAdapterSettings:
        captured["base_dir"] = base_dir
        captured["env"] = env
        return settings

    class FakeApiClient:
        def __init__(self, base_url: str) -> None:
            captured["api_base_url"] = base_url

    class FakeGateway:
        def __init__(self, api_client: FakeApiClient) -> None:
            captured["gateway_api_client"] = api_client

    class FakeApp:
        def __init__(self, app_settings: TelegramAdapterSettings, gateway: FakeGateway) -> None:
            captured["app_settings"] = app_settings
            captured["gateway"] = gateway

        async def run(self) -> None:
            captured["run_called"] = True

    monkeypatch.setattr(telegram_main, "load_settings", fake_load_settings)
    monkeypatch.setattr(telegram_main, "TelegramApiClient", FakeApiClient)
    monkeypatch.setattr(telegram_main, "TelegramInboxGateway", FakeGateway)
    monkeypatch.setattr(telegram_main, "TelegramInboxApp", FakeApp)

    asyncio.run(telegram_main._run({"SETTINGS_BASE_DIR": "/tmp/adapter", "API_BASE_URL": "  "}))

    assert captured["base_dir"] == Path("/tmp/adapter")
    assert captured["env"] == {"SETTINGS_BASE_DIR": "/tmp/adapter", "API_BASE_URL": "  "}
    assert captured["api_base_url"] == "http://api:8080"
    assert captured["app_settings"] is settings
    assert captured["run_called"] is True


def test_main_configures_logging_and_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_basic_config(**kwargs) -> None:
        captured["logging"] = kwargs

    def fake_asyncio_run(coro: Any) -> None:
        captured["saw_coroutine"] = inspect.iscoroutine(coro)
        coro.close()

    monkeypatch.setattr(telegram_main.logging, "basicConfig", fake_basic_config)
    monkeypatch.setattr(telegram_main.asyncio, "run", fake_asyncio_run)

    result = telegram_main.main({"API_BASE_URL": "http://example.test"})

    assert result == 0
    assert captured["saw_coroutine"] is True
    assert captured["logging"]["level"] == telegram_main.logging.INFO


def test_main_returns_zero_on_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_asyncio_run(coro: Any) -> None:
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(telegram_main.asyncio, "run", fake_asyncio_run)

    assert telegram_main.main() == 0


@pytest.mark.asyncio
async def test_run_registers_localized_commands_and_starts_polling() -> None:
    _, _, app = make_app(bot=FakeBot())
    started: dict[str, Any] = {}

    async def fake_start_polling(bot: FakeBot) -> None:
        started["bot"] = bot

    app.dispatcher.start_polling = fake_start_polling  # type: ignore[method-assign]

    await app.run()

    assert [language_code for _, language_code in app.bot.set_commands_calls] == ["ru", "en"]
    assert started["bot"] is app.bot


@pytest.mark.asyncio
async def test_start_help_and_inbox_handlers_answer_and_refresh_status() -> None:
    _, _, app = make_app()
    message = FakeMessage()
    refreshed: list[int] = []

    async def fake_send_or_edit_status(current: FakeMessage, **kwargs) -> bool:
        refreshed.append(current.message_id)
        return True

    app._send_or_edit_status = fake_send_or_edit_status  # type: ignore[method-assign]

    await app._handle_start(message)
    await app._handle_help(message)
    await app._handle_inbox(message)

    assert message.answers[0]["text"] == _start_text()
    assert message.answers[1]["text"] == _help_text()
    assert refreshed == [101, 101, 101]


@pytest.mark.asyncio
async def test_handle_any_message_reports_rejections_and_handler_errors(caplog: pytest.LogCaptureFixture) -> None:
    _, gateway, app = make_app()
    accepted_message = FakeMessage(text="Keep text ftp://bad.example/file", message_id=42)

    await app._handle_any_message(accepted_message)

    assert "Keep text [text, ready, message 42]" in accepted_message.answers[0]["text"]
    assert "Rejected: ftp://bad.example/file" in accepted_message.answers[0]["text"]
    assert app.status_message_ids[(10, 7)] == 9001

    failing_message = FakeMessage(text="hello")

    async def fake_download_message_files(message: FakeMessage) -> list[Any]:
        raise RuntimeError("telegram_file_download_failed")

    app._download_message_files = fake_download_message_files  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        await app._handle_any_message(failing_message)

    assert failing_message.answers[-1]["text"] == (
        "unsupported input: Telegram file content could not be downloaded."
    )
    assert "scope=message_ingest" in caplog.text
    assert "normalized_code=unsupported_input" in caplog.text


@pytest.mark.asyncio
async def test_download_message_files_hydrates_content_and_rejects_empty_download() -> None:
    photo = SimpleNamespace(file_id="photo-1", file_unique_id="photo-u", file_size=10)
    good_bot = FakeBot(file_bytes={"remote/photo-1": b"photo-bytes"})
    _, _, app = make_app(bot=good_bot)
    message = FakeMessage(photo=[photo], caption="caption", message_id=77, media_group_id="grp")

    files = await app._download_message_files(message)

    assert files[0].kind == "photo"
    assert files[0].content == b"photo-bytes"
    assert good_bot.get_file_calls == ["photo-1"]
    assert good_bot.download_calls == ["remote/photo-1"]

    empty_bot = FakeBot(file_bytes={"remote/photo-1": b""})
    _, _, empty_app = make_app(bot=empty_bot)
    with pytest.raises(RuntimeError, match="telegram_file_download_failed"):
        await empty_app._download_message_files(message)


@pytest.mark.asyncio
async def test_send_or_edit_status_prefers_edit_then_falls_back_to_new_message() -> None:
    edit_bot = FakeBot()
    _, gateway, app = make_app(bot=edit_bot)
    gateway.add_text(owner=owner(), text="first item")
    message = FakeMessage()
    app.status_message_ids[(10, 7)] = 5001

    edited = await app._send_or_edit_status(message)

    assert edited is True
    assert edit_bot.edit_calls[0]["message_id"] == 5001
    assert message.answers == []

    failing_bot = FakeBot(edit_error=RuntimeError("stale message"))
    _, failing_gateway, failing_app = make_app(bot=failing_bot)
    failing_gateway.add_text(owner=owner(), text="fallback item")
    fallback_message = FakeMessage()
    failing_app.status_message_ids[(10, 7)] = 5002

    sent = await failing_app._send_or_edit_status(fallback_message)

    assert sent is True
    assert failing_app.status_message_ids[(10, 7)] == 9001
    assert "fallback item" in fallback_message.answers[0]["text"]


@pytest.mark.asyncio
async def test_resolve_run_start_status_keeps_queued_prefix_when_run_stays_active() -> None:
    api, gateway, app = make_app()
    gateway.add_text(owner=owner(), text="queued run")
    status = gateway.restore_status(owner=owner())
    selection = gateway.create_selection(
        owner=owner(),
        collection_id=status.collection["collection_id"],
        expected_version=int(status.collection["version"]),
    )
    run = gateway.start_analysis(owner=owner(), selection_id=selection["selection_id"])

    async def no_sleep(_seconds: float) -> None:
        return None

    app._sleep = no_sleep  # type: ignore[assignment]
    status, prefix, answer_text = await app._resolve_run_start_status(owner=owner(), run=run)

    assert api.runs[0]["status"] == "queued"
    assert answer_text == "Run queued"
    assert prefix.startswith("Run queued:")
    assert status.active_runs[0]["analysis_run_id"] == run["analysis_run_id"]


@pytest.mark.asyncio
async def test_access_checks_cover_allowlist_and_scope_errors() -> None:
    _, _, app = make_app()
    app.settings = TelegramAdapterSettings(telegram_bot_token="token", allowed_user_ids=(100,))

    denied_message = FakeMessage(from_user_id=7)
    allowed = await app._ensure_message_allowed(denied_message)

    assert allowed is False
    assert denied_message.answers[0]["text"] == app.locale_service.text(TelegramTextKey.ACCESS_DENIED, locale=DEFAULT_LOCALE)

    denied_callback = FakeCallback(data="ib:rf", message=FakeMessage(), from_user_id=7)
    callback_allowed = await app._ensure_callback_allowed(denied_callback)

    assert callback_allowed is False
    assert denied_callback.answers[0]["show_alert"] is True

    _, _, scope_app = make_app()

    def raise_scope_error(*args: Any, **kwargs: Any) -> Any:
        raise TelegramUserError(TelegramUserErrorCode.GROUP_NOT_SUPPORTED)

    scope_app.gateway.scope_for = raise_scope_error  # type: ignore[method-assign]
    scope_message = FakeMessage()
    assert await scope_app._ensure_message_allowed(scope_message) is False
    assert "private-chat only" in scope_message.answers[0]["text"]


@pytest.mark.asyncio
async def test_callback_actions_cover_refresh_paging_remove_selection_run_and_details() -> None:
    api, gateway, app = make_app(page_size=1)
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")
    base_message = FakeMessage()

    refresh_status = status_for(gateway)
    app._set_page_state((10, 7), refresh_status, current_cursor=None, previous_cursors=[], selection=None)
    refresh_callback = FakeCallback(data="ib:rf", message=base_message)
    await app._handle_status_callback(refresh_callback)
    assert refresh_callback.answers[-1]["text"] == "Refreshed"

    next_status = status_for(gateway)
    app._set_page_state((10, 7), next_status, current_cursor=None, previous_cursors=[], selection=None)
    next_callback = FakeCallback(data="ib:pn", message=base_message)
    await app._handle_status_callback(next_callback)
    assert next_callback.answers[-1]["text"] == "Page loaded"
    assert app.page_states[(10, 7)].current_cursor == "1"

    previous_callback = FakeCallback(data="ib:pp", message=base_message)
    await app._handle_status_callback(previous_callback)
    assert previous_callback.answers[-1]["text"] == "Page loaded"
    assert app.page_states[(10, 7)].current_cursor is None

    remove_status = status_for(gateway)
    remove_keyboard = build_status_keyboard(remove_status)
    remove_callback_data = next(
        button.callback_data
        for row in remove_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rm:")
    )
    app._set_page_state((10, 7), remove_status, current_cursor=None, previous_cursors=[], selection=None)
    remove_callback = FakeCallback(data=remove_callback_data, message=base_message)
    await app._handle_status_callback(remove_callback)
    assert remove_callback.answers[-1]["text"] == "Removed"
    assert api.remove_requests[-1]["media_item_id"] == "media-1"

    page_two_status = status_for(gateway, cursor="0")
    gateway.add_text(owner=owner(), text="three")
    next_page_status = status_for(gateway, cursor="1")
    clear_keyboard = build_status_keyboard(next_page_status, current_cursor="1")
    clear_callback_data = next(
        button.callback_data
        for row in clear_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cl:")
    )
    app._set_page_state((10, 7), next_page_status, current_cursor="1", previous_cursors=[None], selection=None)
    clear_callback = FakeCallback(data=clear_callback_data, message=base_message)
    await app._handle_status_callback(clear_callback)
    assert clear_callback.answers[-1]["text"] == "Cleared"
    assert app.page_states[(10, 7)].current_cursor is None

    gateway.add_text(owner=owner(), text="selection item 1")
    gateway.add_text(owner=owner(), text="selection item 2")
    selection_status = status_for(gateway)
    selection_keyboard = build_status_keyboard(selection_status)
    selection_callback_data = next(
        button.callback_data
        for row in selection_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:sl:")
    )
    app._set_page_state((10, 7), selection_status, current_cursor=None, previous_cursors=[], selection=None)
    selection_callback = FakeCallback(data=selection_callback_data, message=base_message)
    await app._handle_status_callback(selection_callback)
    assert selection_callback.answers[-1]["text"] == "Selection created"
    selection = app.page_states[(10, 7)].selection
    assert selection is not None

    run_status = status_for(gateway)
    run_keyboard = build_status_keyboard(run_status, selection=selection)
    run_callback_data = next(
        button.callback_data
        for row in run_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rn:")
    )
    app._set_page_state((10, 7), run_status, current_cursor=None, previous_cursors=[], selection=selection)
    run_callback = FakeCallback(data=run_callback_data, message=base_message)

    async def no_sleep(_seconds: float) -> None:
        return None

    original_get_run_status = gateway.get_run_status

    def fast_fail_run_status(*, owner: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = "failed"
        return original_get_run_status(owner=owner, analysis_run_id=analysis_run_id)

    app._sleep = no_sleep  # type: ignore[assignment]
    gateway.get_run_status = fast_fail_run_status  # type: ignore[method-assign]
    await app._handle_status_callback(run_callback)
    assert run_callback.answers[-1]["text"] == "Run failed"
    assert app.page_states[(10, 7)].selection is None
    assert "Run failed:" in base_message.edits[-1]["text"]
    assert "Completed runs:" in base_message.edits[-1]["text"]
    assert "run-1: failed" in base_message.edits[-1]["text"]

    api.runs[0]["status"] = "succeeded"
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
        }
    )
    api.diagnostics.append(
        {
            "diagnostic_id": "diagnostic-1",
            "subject_type": "analysis_run",
            "subject_id": "run-1",
            "severity": "info",
            "code": "worker_note",
            "message": "Saved for later review.",
        }
    )
    completed_status = status_for(gateway)
    completed_keyboard = build_status_keyboard(completed_status)
    details = {
        button.callback_data.split(":")[1]: button.callback_data
        for row in completed_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith(("ib:ar:", "ib:dg:"))
    }
    app._set_page_state((10, 7), completed_status, current_cursor=None, previous_cursors=[], selection=None)
    artifacts_callback = FakeCallback(data=details["ar"], message=base_message)
    diagnostics_callback = FakeCallback(data=details["dg"], message=base_message)
    await app._handle_status_callback(artifacts_callback)
    await app._handle_status_callback(diagnostics_callback)

    assert artifacts_callback.answers[-1]["text"] == "Artifacts loaded"
    assert diagnostics_callback.answers[-1]["text"] == "Diagnostics loaded"
    assert "Artifacts for run-1" in base_message.edits[-2]["text"]
    assert "Diagnostics for run-1" in base_message.edits[-1]["text"]


@pytest.mark.asyncio
async def test_callback_error_paths_cover_stale_unknown_and_normalized_failures() -> None:
    _, gateway, app = make_app()
    gateway.add_text(owner=owner(), text="one")
    message = FakeMessage()

    missing_message_callback = FakeCallback(data="ib:rf", message=None)
    await app._handle_status_callback(missing_message_callback)
    assert missing_message_callback.answers[-1]["show_alert"] is True

    stale_page_callback = FakeCallback(data="ib:pn", message=message)
    await app._handle_status_callback(stale_page_callback)
    assert stale_page_callback.answers[-1]["show_alert"] is True

    stale_previous_callback = FakeCallback(data="ib:pp", message=message)
    await app._handle_status_callback(stale_previous_callback)
    assert stale_previous_callback.answers[-1]["show_alert"] is True

    unknown_callback = FakeCallback(data="ib:zz", message=message)
    await app._handle_status_callback(unknown_callback)
    assert unknown_callback.answers[-1]["show_alert"] is True

    app._set_page_state((10, 7), status_for(gateway), current_cursor=None, previous_cursors=[], selection=None)
    broken_remove = FakeCallback(data="ib:rm", message=message)
    await app._handle_status_callback(broken_remove)
    assert broken_remove.answers[-1]["show_alert"] is True

    no_next_state = status_for(gateway)
    app._set_page_state((10, 7), no_next_state, current_cursor=None, previous_cursors=[], selection=None)
    no_next_callback = FakeCallback(data="ib:pn", message=message)
    await app._handle_status_callback(no_next_callback)
    assert no_next_callback.answers[-1]["show_alert"] is True

    no_previous_callback = FakeCallback(data="ib:pp", message=message)
    await app._handle_status_callback(no_previous_callback)
    assert no_previous_callback.answers[-1]["show_alert"] is True

    blocked_callback = FakeCallback(data="ib:rf", message=message)

    async def deny_callback(_callback: FakeCallback) -> bool:
        return False

    app._ensure_callback_allowed = deny_callback  # type: ignore[method-assign]
    await app._handle_status_callback(blocked_callback)
    assert blocked_callback.answers == []


@pytest.mark.asyncio
async def test_handlers_return_early_and_error_helpers_cover_remaining_branches() -> None:
    _, _, app = make_app()
    message = FakeMessage(text="hello")

    async def deny_message(_message: FakeMessage) -> bool:
        return False

    app._ensure_message_allowed = deny_message  # type: ignore[method-assign]

    await app._handle_start(message)
    await app._handle_help(message)
    await app._handle_inbox(message)
    await app._handle_any_message(message)

    assert message.answers == []

    app.gateway.restore_status = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("restore exploded"))  # type: ignore[method-assign]
    assert await app._send_or_edit_status(FakeMessage()) is False

    await app._edit_callback_status(FakeCallback(data="ib:rf", message=None), status_for(TelegramInboxGateway(FakeFinalApiClient())))

    scope_app = make_app()[2]

    def raise_scope_error(*args: Any, **kwargs: Any) -> Any:
        raise TelegramUserError(TelegramUserErrorCode.GROUP_NOT_SUPPORTED)

    scope_app.gateway.scope_for = raise_scope_error  # type: ignore[method-assign]
    scope_callback = FakeCallback(data="ib:rf", message=FakeMessage())
    assert await scope_app._ensure_callback_allowed(scope_callback) is False
    assert "private-chat only" in scope_callback.answers[0]["text"]


def test_helper_functions_cover_remaining_callback_token_and_error_branches() -> None:
    uuid_value = "11111111-1111-1111-1111-111111111111"
    uuid_token = _encode_callback_token(uuid_value)

    assert _decode_callback_token(uuid_token) == uuid_value
    assert _decode_optional_callback_token("_") is None
    assert _encode_callback_version(0) == "0"
    assert str(TelegramUserError(TelegramUserErrorCode.STALE_ACTION)) == "stale_action"
    assert rejected_reason_text(None).startswith("unsupported input:")

    with pytest.raises(TelegramUserError):
        _decode_callback_version("not-base36")


def test_run_module_executes_script_exit_path(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_asyncio_run(coro: Any) -> None:
        coro.close()

    monkeypatch.setattr(asyncio, "run", fake_asyncio_run)

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("telegram_adapter.__main__", run_name="__main__")

    assert exit_info.value.code == 0


def test_helper_functions_cover_callback_error_normalization_and_message_shapes() -> None:
    photo = SimpleNamespace(file_id="photo-1", file_unique_id="photo-u", file_size=10)
    video = SimpleNamespace(file_id="video-1", file_unique_id="video-u", file_name="clip.mp4", mime_type="video/mp4", file_size=11)
    document = SimpleNamespace(file_id="doc-1", file_unique_id="doc-u", file_name="brief.pdf", mime_type="application/pdf", file_size=12)
    audio = SimpleNamespace(file_id="audio-1", file_unique_id="audio-u", file_name="note.mp3", mime_type="audio/mpeg", file_size=13)
    voice = SimpleNamespace(file_id="voice-1", file_unique_id="voice-u", mime_type="audio/ogg", file_size=14)
    message = FakeMessage(
        text="/inbox",
        caption="caption",
        photo=[photo],
        video=video,
        document=document,
        audio=audio,
        voice=voice,
        media_group_id="album-1",
    )

    files = list(_message_files(message))

    assert _message_text(message) is None
    message.text = None
    assert _message_text(message) == "caption"
    assert [file.kind for file in files] == ["photo", "video", "document", "audio", "voice"]
    assert _chat_type(SimpleNamespace(type=SimpleNamespace(value="supergroup"))) == "supergroup"
    assert _artifact_label({"artifact_id": "artifact-1234567890", "kind": "report", "status": "ready"}) == "artifact...7890: report [ready]"
    assert _diagnostic_label({"severity": "warning"}) == "diagnostic: warning"
    assert _media_group_id({"metadata": "not-a-dict"}) is None
    assert _detail_prefix(title="Artifacts", lines=["- one"]) == "Artifacts\n- one\n\n"
    assert _start_text().startswith("Send text, links")
    assert _help_text().startswith("/inbox")

    normalized_404 = _normalize_callback_error(TelegramApiClientError("/v1", 404, "missing", code="gone"))
    normalized_message_404 = _normalize_message_error(TelegramApiClientError("/v1", 404, "missing", code="gone"))
    normalized_key_error = _normalize_callback_error(KeyError("selection_id"))
    passthrough = _normalize_callback_error(RuntimeError("boom"))

    assert isinstance(normalized_404, TelegramUserError)
    assert normalized_404.code == TelegramUserErrorCode.STALE_ACTION
    assert isinstance(normalized_message_404, TelegramUserError)
    assert normalized_message_404.code == TelegramUserErrorCode.STALE_ACTION
    assert isinstance(normalized_key_error, TelegramUserError)
    assert isinstance(passthrough, RuntimeError)

    with pytest.raises(TelegramUserError):
        _parse_callback_payload("bad")
    with pytest.raises(TelegramUserError):
        _encode_callback_version(-1)
    with pytest.raises(TelegramUserError):
        _decode_callback_token("xinvalid")
