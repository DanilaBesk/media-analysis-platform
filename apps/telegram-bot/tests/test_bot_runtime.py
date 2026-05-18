from __future__ import annotations

import asyncio
import inspect
import logging
from pathlib import Path
import runpy
from types import SimpleNamespace
from typing import Any

import pytest
from aiogram.exceptions import TelegramBadRequest

from telegram_adapter import __main__ as telegram_main
from telegram_adapter.api_client import TelegramApiClientError
from telegram_adapter.bot import (
    TelegramInboxApp,
    _artifact_label,
    _classify_polling_log_message,
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
    _TelegramPollingMonitor,
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
        self.cancel_requests: list[dict[str, Any]] = []
        self.get_artifact_requests: list[str] = []
        self.internal_artifact_download_access_requests: list[str] = []
        self.internal_artifact_download_access: dict[str, dict[str, Any]] = {}
        self.channel_accounts: list[dict[str, Any]] = []
        self.channel_surfaces: list[dict[str, Any]] = []
        self.surface_events: list[dict[str, Any]] = []
        self.replace_surface_requests: list[dict[str, Any]] = []
        self.supersede_surface_requests: list[dict[str, Any]] = []

    def create_media_asset(self, **kwargs) -> dict[str, Any]:
        media_asset = {
            "media_asset_id": f"media-{len(self.items) + 1}",
            "kind": kwargs["kind"],
            "status": "ready",
            "display_name": kwargs.get("display_name") or kwargs["kind"],
            "origin": kwargs["origin"],
            "metadata": kwargs.get("metadata") or {},
        }
        self.add_requests.append(kwargs)
        self.items.append(media_asset)
        self.collection["items"].append({"media_asset_id": media_asset["media_asset_id"], "position": len(self.items) - 1})
        return media_asset

    def upload_media_asset(self, **kwargs) -> dict[str, Any]:
        media_asset = {
            "media_asset_id": f"media-{len(self.items) + 1}",
            "kind": kwargs["kind"],
            "status": "ready",
            "display_name": kwargs.get("display_name") or kwargs.get("file_name") or kwargs["kind"],
            "origin": {
                "origin_type": "upload",
                "origin_ref": f"sources/{kwargs['kind']}/{len(self.items) + 1}-{kwargs.get('file_name') or 'upload.bin'}",
                "object_ref": f"sources/{kwargs['kind']}/{len(self.items) + 1}-{kwargs.get('file_name') or 'upload.bin'}",
                "content_type": kwargs.get("content_type"),
                "size_bytes": len(kwargs["content"]),
            },
            "metadata": kwargs.get("metadata") or {},
        }
        self.upload_requests.append(kwargs)
        self.items.append(media_asset)
        self.collection["items"].append({"media_asset_id": media_asset["media_asset_id"], "position": len(self.items) - 1})
        return media_asset

    def list_media_assets(self, **kwargs) -> dict[str, Any]:
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
        media_asset_id = kwargs["media_asset_id"]
        self.remove_requests.append(kwargs)
        self.collection["items"] = [item for item in self.collection["items"] if item["media_asset_id"] != media_asset_id]
        self.items = [item for item in self.items if item["media_asset_id"] != media_asset_id]
        self.collection["version"] += 1
        return self.collection

    def create_selection_snapshot(self, **kwargs) -> dict[str, Any]:
        selection = {
            "selection_snapshot_id": f"selection-{len(self.selections) + 1}",
            "items": kwargs["items"],
            "source_collection_id": kwargs.get("source_collection_id"),
        }
        self.selections.append(selection)
        return selection

    def create_analysis_run(self, **kwargs) -> dict[str, Any]:
        run = {
            "analysis_run_id": f"run-{len(self.runs) + 1}",
            "selection_snapshot_id": kwargs["selection_snapshot_id"],
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

    def cancel_analysis_run(self, **kwargs) -> dict[str, Any]:
        analysis_run_id = kwargs["analysis_run_id"]
        self.cancel_requests.append(kwargs)
        run = next(run for run in self.runs if run["analysis_run_id"] == analysis_run_id)
        run["status"] = "canceled"
        run["version"] = int(run.get("version") or 0) + 1
        return run

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

    def get_artifact(self, **kwargs) -> dict[str, Any]:
        artifact_id = kwargs["artifact_id"]
        self.get_artifact_requests.append(artifact_id)
        return next(artifact for artifact in self.artifacts if artifact["artifact_id"] == artifact_id)

    def get_internal_artifact_download_access(self, **kwargs) -> dict[str, Any]:
        artifact_id = kwargs["artifact_id"]
        self.internal_artifact_download_access_requests.append(artifact_id)
        if artifact_id in self.internal_artifact_download_access:
            return self.internal_artifact_download_access[artifact_id]
        return self.get_artifact(artifact_id=artifact_id)

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

    def resolve_channel_account(self, **kwargs) -> dict[str, Any]:
        owner_value = kwargs["owner"]
        owner_id = str(owner_value["owner_id"])
        existing = next((account for account in self.channel_accounts if account["external_account_ref"] == owner_id), None)
        if existing is not None:
            return existing
        account = {
            "channel_account_id": f"channel-account-{len(self.channel_accounts) + 1}",
            "channel": "telegram",
            "external_account_ref": owner_id,
            "display_name": owner_id,
            "status": "active",
            "metadata": {"owner": owner_value, "adapter_identity": owner_value.get("adapter_identity", {})},
        }
        self.channel_accounts.append(account)
        return account

    def list_channel_accounts(self, **kwargs) -> dict[str, Any]:
        return {"items": list(self.channel_accounts), "page": {"page_size": kwargs.get("page_size") or 100, "has_more": False}}

    def upsert_channel_surface(self, **kwargs) -> dict[str, Any]:
        existing = next(
            (
                surface
                for surface in self.channel_surfaces
                if surface["channel_account_id"] == kwargs["channel_account_id"]
                and surface["surface_type"] == kwargs["surface_type"]
                and surface["surface_key"] == kwargs["surface_key"]
                and surface["lifecycle_status"] == "active"
            ),
            None,
        )
        if existing is None:
            surface = {
                "channel_surface_id": f"surface-{len(self.channel_surfaces) + 1}",
                "channel_account_id": kwargs["channel_account_id"],
                "channel": "telegram",
                "surface_type": kwargs["surface_type"],
                "surface_key": kwargs["surface_key"],
                "address": kwargs.get("address") or {},
                "address_fingerprint": kwargs.get("address_fingerprint") or "",
                "display_state": kwargs.get("display_state") or {},
                "lifecycle_status": "active",
                "version": 1,
                "subjects": list(kwargs.get("subjects") or []),
            }
            self.channel_surfaces.append(surface)
            return surface
        existing["address"] = kwargs.get("address") or {}
        existing["address_fingerprint"] = kwargs.get("address_fingerprint") or ""
        existing["display_state"] = kwargs.get("display_state") or {}
        existing["version"] = int(existing.get("version") or 0) + 1
        if kwargs.get("subjects"):
            existing["subjects"] = list(kwargs["subjects"])
        return existing

    def list_channel_surfaces(self, **kwargs) -> dict[str, Any]:
        items = [
            surface
            for surface in self.channel_surfaces
            if surface["channel_account_id"] == kwargs["channel_account_id"]
        ]
        if kwargs.get("active_only"):
            items = [surface for surface in items if surface["lifecycle_status"] == "active"]
        if kwargs.get("lifecycle_status"):
            items = [surface for surface in items if surface["lifecycle_status"] == kwargs["lifecycle_status"]]
        if kwargs.get("subject_type") or kwargs.get("subject_id"):
            items = [
                surface
                for surface in items
                if any(
                    subject.get("subject_type") == kwargs.get("subject_type")
                    and subject.get("subject_id") == kwargs.get("subject_id")
                    for subject in surface.get("subjects", [])
                )
            ]
        return {"items": items[: kwargs.get("page_size") or 100], "page": {"has_more": False}}

    def replace_channel_surface_display_state(self, **kwargs) -> dict[str, Any]:
        self.replace_surface_requests.append(kwargs)
        surface = next(surface for surface in self.channel_surfaces if surface["channel_surface_id"] == kwargs["channel_surface_id"])
        surface["display_state"] = kwargs["display_state"]
        surface["version"] = int(surface.get("version") or 0) + 1
        return surface

    def supersede_channel_surface(self, **kwargs) -> dict[str, Any]:
        self.supersede_surface_requests.append(kwargs)
        surface = next(surface for surface in self.channel_surfaces if surface["channel_surface_id"] == kwargs["channel_surface_id"])
        surface["lifecycle_status"] = "superseded"
        event = {
            "channel_surface_event_id": f"surface-event-{len(self.surface_events) + 1}",
            "channel_surface_id": kwargs["channel_surface_id"],
            "event_type": "channel_surface.superseded",
            "reason": kwargs.get("reason"),
        }
        self.surface_events.append(event)
        return event


class FakeBot:
    def __init__(self, *, file_bytes: dict[str, bytes] | None = None, edit_error: Exception | None = None) -> None:
        self.file_bytes = file_bytes or {}
        self.edit_error = edit_error
        self.set_commands_calls: list[tuple[list[Any], str]] = []
        self.get_file_calls: list[str] = []
        self.download_calls: list[str] = []
        self.edit_calls: list[dict[str, Any]] = []
        self.send_message_calls: list[dict[str, Any]] = []
        self.send_document_calls: list[dict[str, Any]] = []

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

    async def send_message(self, chat_id: int, text: str, **kwargs) -> SimpleNamespace:
        self.send_message_calls.append({"chat_id": chat_id, "text": text, **kwargs})
        return SimpleNamespace(message_id=9003)

    async def send_document(self, chat_id: int, document: Any, **kwargs) -> SimpleNamespace:
        self.send_document_calls.append({"chat_id": chat_id, "document": document, **kwargs})
        return SimpleNamespace(message_id=9004)


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
        self.documents: list[dict[str, Any]] = []
        self.edits: list[dict[str, Any]] = []

    async def answer(self, text: str, **kwargs) -> SimpleNamespace:
        self.answers.append({"text": text, **kwargs})
        return SimpleNamespace(message_id=9001)

    async def answer_document(self, document: Any, **kwargs) -> SimpleNamespace:
        self.documents.append({"document": document, **kwargs})
        return SimpleNamespace(message_id=9002)

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


def test_polling_monitor_classifies_upstream_failures_and_recovery(caplog: pytest.LogCaptureFixture) -> None:
    monitor = _TelegramPollingMonitor()
    aiogram_logger = logging.getLogger("aiogram.dispatcher")
    record_failure = logging.LogRecord(
        name="aiogram.dispatcher",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Failed to fetch updates - TelegramNetworkError: HTTP Client says - Request timeout error",
        args=(),
        exc_info=None,
    )
    record_recovered = logging.LogRecord(
        name="aiogram.dispatcher",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Connection established (tryings = 1, bot id = 1)",
        args=(),
        exc_info=None,
    )
    record_unrelated = logging.LogRecord(
        name="aiogram.dispatcher",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Run polling for bot",
        args=(),
        exc_info=None,
    )
    record_other_logger = logging.LogRecord(
        name="telegram_adapter.other",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Failed to fetch updates - TelegramNetworkError: Request timeout error",
        args=(),
        exc_info=None,
    )

    with caplog.at_level(logging.INFO):
        aiogram_logger.addHandler(monitor)
        try:
            monitor.emit(record_failure)
            monitor.emit(record_recovered)
            monitor.emit(record_unrelated)
            monitor.emit(record_other_logger)
        finally:
            aiogram_logger.removeHandler(monitor)

    assert "classification=telegram_upstream_failure" in caplog.text
    assert "classification=telegram_upstream_recovered" in caplog.text
    assert "Run polling for bot" not in caplog.text


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

    assert accepted_message.answers[0]["text"].startswith("Транскрибация\nМатериалов: 1\nТекст: «Keep text»")
    assert "Отклонено: ftp://bad.example/file" in accepted_message.answers[0]["text"]
    assert app.status_message_ids[(10, 7)] == 9001

    failing_message = FakeMessage(text="hello")

    async def fake_download_message_files(message: FakeMessage) -> list[Any]:
        raise RuntimeError("telegram_file_download_failed")

    app._download_message_files = fake_download_message_files  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        await app._handle_any_message(failing_message)

    assert failing_message.answers[-1]["text"] == "неподдерживаемый ввод: не удалось скачать файл из Telegram."
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
async def test_send_or_edit_status_can_force_fresh_reply_for_new_inbound_message() -> None:
    edit_bot = FakeBot()
    _, gateway, app = make_app(bot=edit_bot)
    gateway.add_text(owner=owner(), text="fresh inbound item")
    message = FakeMessage()
    app.status_message_ids[(10, 7)] = 5001

    sent = await app._send_or_edit_status(message, prefer_edit=False)

    assert sent is True
    assert edit_bot.edit_calls == []
    assert app.status_message_ids[(10, 7)] == 9001
    assert "fresh inbound item" in message.answers[0]["text"]


@pytest.mark.asyncio
async def test_post_ingest_refresh_failure_confirms_saved_inbox_without_unavailable_error() -> None:
    api, gateway, app = make_app()
    message = FakeMessage(text="saved before refresh")

    def fail_restore_status(**kwargs: Any) -> InboxStatus:
        raise TelegramApiClientError("/v1/analysis-runs", 0, "Backend is unavailable", code="backend_unavailable")

    gateway.restore_status = fail_restore_status  # type: ignore[method-assign]

    await app._handle_any_message(message)

    assert api.items[0]["display_name"] == "saved before refresh"
    assert "Материал сохранён в inbox на сервере." in message.answers[-1]["text"]
    assert "Сервис временно недоступен" not in message.answers[-1]["text"]


@pytest.mark.asyncio
async def test_status_surface_failure_does_not_block_inbound_status_reply() -> None:
    _, gateway, app = make_app()
    gateway.add_text(owner=owner(), text="surface API is unavailable")
    message = FakeMessage()

    def fail_find_surface(**kwargs: Any) -> dict[str, Any] | None:
        raise TelegramApiClientError(
            "/internal/v1/channel-surfaces",
            0,
            "Backend is unavailable",
            code="backend_unavailable",
        )

    gateway.find_current_materials_surface = fail_find_surface  # type: ignore[method-assign]

    sent = await app._send_or_edit_status(message, prefer_edit=False)

    assert sent is True
    assert "surface API is unavailable" in message.answers[0]["text"]


@pytest.mark.asyncio
async def test_status_surface_supersedes_uneditable_message_and_creates_replacement() -> None:
    edit_error = TelegramBadRequest(
        method=SimpleNamespace(__api_method__="editMessageText"),
        message="message to edit not found",
    )
    api, gateway, app = make_app(bot=FakeBot(edit_error=edit_error))
    gateway.add_text(owner=owner(), text="surface replacement")
    account = api.resolve_channel_account(owner=owner())
    api.channel_surfaces.append(
        {
            "channel_surface_id": "surface-old",
            "channel_account_id": account["channel_account_id"],
            "channel": "telegram",
            "surface_type": "current_materials_panel",
            "surface_key": "current:chat:10:user:7",
            "address": {"chat_id": 10, "message_id": 5002},
            "address_fingerprint": "telegram:10:5002",
            "display_state": {"screen": "main"},
            "lifecycle_status": "active",
            "version": 1,
            "subjects": [],
        }
    )

    sent = await app._send_or_edit_status(FakeMessage())

    active_surfaces = [surface for surface in api.channel_surfaces if surface["lifecycle_status"] == "active"]
    assert sent is True
    assert api.supersede_surface_requests[-1]["channel_surface_id"] == "surface-old"
    assert api.supersede_surface_requests[-1]["reason"] == "message_not_editable"
    assert app.status_message_ids[(10, 7)] == 9001
    assert active_surfaces[-1]["surface_type"] == "current_materials_panel"
    assert active_surfaces[-1]["address"] == {"chat_id": 10, "message_id": 9001}


@pytest.mark.asyncio
async def test_restart_recovery_restores_materials_surface_and_resumes_active_run_watcher() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    gateway.add_text(owner=owner(), text="recover me")
    selection = gateway.create_selection_snapshot(owner=owner(), collection_id="inbox-1", expected_version=1)
    run = gateway.start_analysis(owner=owner(), selection_snapshot_id=selection["selection_snapshot_id"])
    account = api.resolve_channel_account(owner=owner())
    api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="current_materials_panel",
        surface_key="current:chat:10:user:7",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "collection", "subject_id": "inbox-1", "subject_role": "primary"}],
    )
    api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="analysis_task_surface",
        surface_key="analysis_run:run-1",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main", "focused_run_id": run["analysis_run_id"]},
        subjects=[{"subject_type": "analysis_run", "subject_id": run["analysis_run_id"], "subject_role": "primary"}],
    )
    tick = asyncio.Event()

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    app._sleep = gated_sleep  # type: ignore[assignment]

    await app._recover_active_channel_surfaces()

    assert app.status_message_ids[(10, 7)] == 5001
    assert app.page_states[(10, 7)].focused_run_id == run["analysis_run_id"]
    assert (10, 7) in app.run_watch_tasks
    assert app.bot.edit_calls[-1]["message_id"] == 5001
    app._cancel_run_status_tracking((10, 7))
    tick.set()


@pytest.mark.asyncio
async def test_existing_result_surface_prevents_duplicate_delivery_after_restart() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    api.runs.append(
        {
            "analysis_run_id": "run-1",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 1,
        }
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
            "object_key": "artifacts/run-1/transcript/plain/transcript.txt",
        }
    )
    account = api.resolve_channel_account(owner=owner())
    api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="result_artifact_surface",
        surface_key="artifact:artifact-1",
        address={"chat_id": 10, "message_id": 7001},
        address_fingerprint="telegram:10:7001",
        display_state={"delivery_mode": "text"},
        subjects=[{"subject_type": "artifact", "subject_id": "artifact-1", "subject_role": "primary"}],
    )

    notice, show_alert = await app._deliver_run_result(
        owner=owner(),
        analysis_run_id="run-1",
        expected_version=1,
        chat_id=10,
    )

    assert notice == "Транскрипт уже отправлен в чат."
    assert show_alert is True
    assert api.internal_artifact_download_access_requests == []
    assert app.bot.send_message_calls == []


@pytest.mark.asyncio
async def test_stale_result_surface_without_address_does_not_block_delivery() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    api.runs.append(
        {
            "analysis_run_id": "run-1",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 1,
        }
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
            "object_key": "artifacts/run-1/transcript/plain/transcript.txt",
        }
    )
    api.internal_artifact_download_access["artifact-1"] = {
        "artifact_id": "artifact-1",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }
    account = api.resolve_channel_account(owner=owner())
    api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="result_artifact_surface",
        surface_key="artifact:artifact-1",
        address={},
        address_fingerprint="",
        display_state={"delivery_mode": "text"},
        subjects=[
            {
                "subject_type": "artifact",
                "subject_id": "artifact-1",
                "subject_role": "primary",
            }
        ],
    )
    app._download_artifact_bytes = lambda _url: b"Recovered transcript."  # type: ignore[method-assign]

    notice, show_alert = await app._deliver_run_result(
        owner=owner(),
        analysis_run_id="run-1",
        expected_version=1,
        chat_id=10,
    )

    active_surfaces = [
        surface
        for surface in api.channel_surfaces
        if surface["lifecycle_status"] == "active"
    ]
    assert notice == "Транскрипт отправлен в чат"
    assert show_alert is False
    assert api.supersede_surface_requests[-1]["reason"] == "result_surface_missing_telegram_address"
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert app.bot.send_message_calls == [{"chat_id": 10, "text": "Recovered transcript."}]
    assert active_surfaces[-1]["surface_type"] == "result_artifact_surface"
    assert active_surfaces[-1]["address"] == {"chat_id": 10, "message_id": 9003}


@pytest.mark.asyncio
async def test_resolve_run_start_status_keeps_queued_prefix_when_run_stays_active() -> None:
    api, gateway, app = make_app()
    gateway.add_text(owner=owner(), text="queued run")
    status = gateway.restore_status(owner=owner())
    selection = gateway.create_selection_snapshot(
        owner=owner(),
        collection_id=status.collection["collection_id"],
        expected_version=int(status.collection["version"]),
    )
    run = gateway.start_analysis(owner=owner(), selection_snapshot_id=selection["selection_snapshot_id"])

    async def no_sleep(_seconds: float) -> None:
        return None

    app._sleep = no_sleep  # type: ignore[assignment]
    status, prefix, answer_text, track_run_id, terminal_status = await app._resolve_run_start_status(
        owner=owner(),
        run=run,
    )

    assert api.runs[0]["status"] == "queued"
    assert answer_text == "Транскрибация запущена"
    assert prefix.startswith("Транскрибация запущена.")
    assert status.active_runs[0]["analysis_run_id"] == run["analysis_run_id"]
    assert track_run_id == run["analysis_run_id"]
    assert terminal_status is None


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
    assert "только в личном чате" in scope_message.answers[0]["text"]


@pytest.mark.asyncio
async def test_callback_actions_cover_materials_screen_paging_remove_clear_and_back() -> None:
    api, gateway, app = make_app(page_size=1)
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")
    base_message = FakeMessage()

    refresh_status = status_for(gateway)
    app._set_page_state((10, 7), refresh_status, current_cursor=None, previous_cursors=[], selection=None, screen="main")
    refresh_callback = FakeCallback(data="ib:rf", message=base_message)
    await app._handle_status_callback(refresh_callback)
    assert refresh_callback.answers[-1]["text"] == "Состояние обновлено"
    assert "Материалов: 2" in base_message.edits[-1]["text"]
    assert "версия" not in base_message.edits[-1]["text"]

    main_keyboard = build_status_keyboard(refresh_status)
    assert [button.text for button in main_keyboard.inline_keyboard[0]] == ["Материалы"]
    assert [button.text for button in main_keyboard.inline_keyboard[-1]] == ["🎙 Транскрибация (2)"]

    materials_callback = FakeCallback(data="ib:mt", message=base_message)
    await app._handle_status_callback(materials_callback)
    assert materials_callback.answers[-1]["text"] == "Открыт список материалов"
    assert app.page_states[(10, 7)].screen == "materials"
    assert base_message.edits[-1]["text"].startswith("Материалы\nМатериалов: 2\n1. Текст: «one»")

    page_one_status = status_for(gateway)
    app._set_page_state((10, 7), page_one_status, current_cursor=None, previous_cursors=[], selection=None, screen="materials")
    next_callback = FakeCallback(data="ib:pn", message=base_message)
    await app._handle_status_callback(next_callback)
    assert next_callback.answers[-1]["text"] == "Открыта следующая страница"
    assert app.page_states[(10, 7)].current_cursor == "media-1"

    remove_status = status_for(gateway, cursor="media-1")
    remove_keyboard = build_status_keyboard(remove_status, can_go_back=True, current_cursor="media-1", screen="materials")
    remove_callback_data = next(
        button.callback_data
        for row in remove_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rm:")
    )
    app._set_page_state((10, 7), remove_status, current_cursor="media-1", previous_cursors=[None], selection=None, screen="materials")
    remove_callback = FakeCallback(data=remove_callback_data, message=base_message)
    await app._handle_status_callback(remove_callback)
    assert remove_callback.answers[-1]["text"] == "Материал убран"
    assert api.remove_requests[-1]["media_asset_id"] == "media-2"
    assert app.page_states[(10, 7)].screen == "materials"

    gateway.add_text(owner=owner(), text="three")
    next_page_status = status_for(gateway, cursor="media-1")
    clear_keyboard = build_status_keyboard(next_page_status, can_go_back=True, current_cursor="media-1", screen="materials")
    remove_latest_callback_data = next(
        button.callback_data
        for row in clear_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rl:")
    )
    clear_callback_data = next(
        button.callback_data
        for row in clear_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cl:")
    )
    remove_latest_callback = FakeCallback(data=remove_latest_callback_data, message=base_message)
    await app._handle_status_callback(remove_latest_callback)
    assert remove_latest_callback.answers[-1]["text"] == "Последний материал убран"
    assert api.remove_requests[-1]["media_asset_id"] == "media-2"
    assert app.page_states[(10, 7)].screen == "materials"

    back_callback = FakeCallback(data="ib:mn", message=base_message)
    await app._handle_status_callback(back_callback)
    assert back_callback.answers[-1]["text"] == "Открыта главная карточка"
    assert app.page_states[(10, 7)].screen == "main"
    assert base_message.edits[-1]["text"].startswith("Транскрибация\nМатериалов: 1")


@pytest.mark.asyncio
async def test_refresh_callback_tolerates_message_not_modified() -> None:
    api, gateway, app = make_app(page_size=1)
    gateway.add_text(owner=owner(), text="one")
    base_message = FakeMessage()
    original_edit_text = base_message.edit_text

    async def raise_not_modified(text: str, **kwargs: Any) -> None:
        raise TelegramBadRequest(
            method=SimpleNamespace(__api_method__="editMessageText"),
            message="message is not modified: specified new message content and reply markup are exactly the same",
        )

    base_message.edit_text = raise_not_modified  # type: ignore[method-assign]
    refresh_status = status_for(gateway)
    app._set_page_state((10, 7), refresh_status, current_cursor=None, previous_cursors=[], selection=None)

    refresh_callback = FakeCallback(data="ib:rf", message=base_message)
    await app._handle_status_callback(refresh_callback)

    assert refresh_callback.answers[-1]["text"] == "Состояние обновлено"
    base_message.edit_text = original_edit_text  # type: ignore[method-assign]

    gateway.add_text(owner=owner(), text="run item 2")
    run_status = status_for(gateway)
    run_keyboard = build_status_keyboard(run_status)
    run_callback_data = next(
        button.callback_data
        for row in run_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rn:")
    )
    app._set_page_state((10, 7), run_status, current_cursor=None, previous_cursors=[], selection=None, screen="main")
    run_callback = FakeCallback(data=run_callback_data, message=base_message)

    async def no_sleep(_seconds: float) -> None:
        return None

    original_get_run_status = gateway.get_run_status
    statuses = iter(("queued", "running", "succeeded"))

    def staged_run_status(*, owner: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "succeeded")
        return original_get_run_status(owner=owner, analysis_run_id=analysis_run_id)

    app._sleep = no_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 4
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]
    await app._handle_status_callback(run_callback)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert run_callback.answers[-1]["text"] == "Транскрибация запущена"
    assert app.page_states[(10, 7)].selection is None
    assert "Карточка обновится автоматически." in base_message.edits[-1]["text"]

    api.runs[0]["status"] = "succeeded"
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
            "object_key": "artifacts/run-1/transcript/plain/transcript.txt",
            "download": {"url": "https://download.test/transcript.txt"},
        }
    )
    api.internal_artifact_download_access["artifact-1"] = {
        "artifact_id": "artifact-1",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }
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
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.bot.edit_calls
    assert "Последние результаты:" not in app.bot.edit_calls[-1]["text"]
    assert "run-1" not in app.bot.edit_calls[-1]["text"]
    assert "Обновить состояние" not in [button.text for row in app.bot.edit_calls[-1]["reply_markup"].inline_keyboard for button in row]

    completed_status = status_for(gateway)
    completed_keyboard = build_status_keyboard(completed_status, focused_run_id="run-1")
    details = {
        button.callback_data.split(":")[1]: button.callback_data
        for row in completed_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith(("ib:ar:", "ib:dg:"))
    }
    app._set_page_state(
        (10, 7),
        completed_status,
        current_cursor=None,
        previous_cursors=[],
        selection=None,
        screen="main",
        focused_run_id="run-1",
    )
    artifacts_callback = FakeCallback(data=details["ar"], message=base_message)
    diagnostics_callback = FakeCallback(data=details["dg"], message=base_message)
    app._download_artifact_bytes = lambda _url: b"Completed transcript."  # type: ignore[method-assign]
    await app._handle_status_callback(artifacts_callback)
    await app._handle_status_callback(diagnostics_callback)

    assert artifacts_callback.answers[-1]["text"] == "Транскрипт отправлен в чат"
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert api.get_artifact_requests == []
    assert diagnostics_callback.answers[-1]["text"] == "Открыта диагностика"
    assert base_message.answers[-1]["text"] == "Completed transcript."
    assert "run-1" not in base_message.edits[-2]["text"]
    assert "Диагностика" in base_message.edits[-1]["text"]
    assert "run-1" not in base_message.edits[-1]["text"]


@pytest.mark.asyncio
async def test_result_callback_sends_transcript_and_clears_collection_after_success() -> None:
    api, gateway, app = make_app()
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")
    base_message = FakeMessage()
    api.runs.append(
        {
            "analysis_run_id": "run-1",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 1,
        }
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-plain",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain; charset=utf-8",
            "object_key": "artifacts/run-1/transcript/plain/transcript.txt",
            "download": {"url": "https://download.test/transcript.txt"},
        }
    )
    api.internal_artifact_download_access["artifact-plain"] = {
        "artifact_id": "artifact-plain",
        "filename": "transcript.txt",
        "mime_type": "text/plain; charset=utf-8",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }

    completed_status = status_for(gateway)
    completed_keyboard = build_status_keyboard(completed_status, focused_run_id="run-1")
    result_callback_data = next(
        button.callback_data
        for row in completed_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:ar:")
    )
    app._set_page_state(
        (10, 7),
        completed_status,
        current_cursor=None,
        previous_cursors=[],
        selection=None,
        screen="main",
        focused_run_id="run-1",
    )
    app._download_artifact_bytes = lambda _url: b"manual transcript"  # type: ignore[method-assign]

    result_callback = FakeCallback(data=result_callback_data, message=base_message)
    await app._handle_status_callback(result_callback)

    assert result_callback.answers[-1] == {"text": "Транскрипт отправлен в чат", "show_alert": False}
    assert base_message.answers[-1]["text"] == "manual transcript"
    assert api.collection["items"] == []
    assert api.items == []
    assert [request["media_asset_id"] for request in api.remove_requests] == ["media-1", "media-2"]
    assert "Материалов: 0" in base_message.edits[-1]["text"]


@pytest.mark.asyncio
async def test_cancel_callback_cancels_focused_active_run_and_refreshes_card() -> None:
    api, gateway, app = make_app()
    base_message = FakeMessage()
    api.runs.append(
        {
            "analysis_run_id": "run-1",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "running",
            "version": 2,
        }
    )

    active_status = status_for(gateway)
    keyboard = build_status_keyboard(active_status, focused_run_id="run-1")
    cancel_callback_data = next(
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cn:")
    )
    app._set_page_state(
        (10, 7),
        active_status,
        current_cursor=None,
        previous_cursors=[],
        selection=None,
        screen="main",
        focused_run_id="run-1",
    )

    cancel_callback = FakeCallback(data=cancel_callback_data, message=base_message)
    await app._handle_status_callback(cancel_callback)

    assert cancel_callback.answers[-1] == {"text": "Транскрибация отменена", "show_alert": False}
    assert api.cancel_requests == [
        {
            "channel_account_id": "channel-account-1",
            "analysis_run_id": "run-1",
            "message": "Canceled from Telegram inline button",
        }
    ]
    assert api.runs[0]["status"] == "canceled"
    assert "Активная задача" not in base_message.edits[-1]["text"]
    assert "Отмена" not in [button.text for row in base_message.edits[-1]["reply_markup"].inline_keyboard for button in row]
    assert not any(
        button.callback_data.startswith("ib:rn:")
        for row in base_message.edits[-1]["reply_markup"].inline_keyboard
        for button in row
    )


@pytest.mark.asyncio
async def test_unfocused_active_run_can_be_canceled_while_new_transcription_can_start() -> None:
    api, gateway, app = make_app()
    gateway.add_text(owner=owner(), text="new independent material")
    base_message = FakeMessage()
    api.runs.append(
        {
            "analysis_run_id": "run-old",
            "selection_snapshot_id": "selection-old",
            "run_type": "transcription",
            "status": "running",
            "version": 3,
        }
    )

    status = status_for(gateway)
    keyboard = build_status_keyboard(status)
    button_texts = [button.text for row in keyboard.inline_keyboard for button in row]
    cancel_callback_data = next(
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cn:")
    )
    run_callback_data = next(
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rn:")
    )

    assert "Отмена" in button_texts
    assert "🎙 Транскрибация (1)" in button_texts

    app._set_page_state((10, 7), status, current_cursor=None, previous_cursors=[], selection=None, screen="main", focused_run_id=None)
    cancel_callback = FakeCallback(data=cancel_callback_data, message=base_message)
    await app._handle_status_callback(cancel_callback)

    assert cancel_callback.answers[-1] == {"text": "Транскрибация отменена", "show_alert": False}
    assert api.cancel_requests[-1]["analysis_run_id"] == "run-old"
    assert api.runs[0]["status"] == "canceled"

    refreshed_status = status_for(gateway)
    app._set_page_state((10, 7), refreshed_status, current_cursor=None, previous_cursors=[], selection=None, screen="main", focused_run_id=None)
    app.run_status_poll_attempts = 1
    run_callback = FakeCallback(data=run_callback_data, message=base_message)
    await app._handle_status_callback(run_callback)

    assert run_callback.answers[-1]["text"] == "Транскрибация запущена"
    assert [run["analysis_run_id"] for run in api.runs] == ["run-old", "run-2"]


@pytest.mark.asyncio
async def test_cancel_callback_rejects_stale_focus_without_canceling_other_run() -> None:
    api, gateway, app = make_app()
    base_message = FakeMessage()
    api.runs.extend(
        [
            {
                "analysis_run_id": "run-old",
                "selection_snapshot_id": "selection-old",
                "run_type": "transcription",
                "status": "running",
                "version": 1,
            },
            {
                "analysis_run_id": "run-current",
                "selection_snapshot_id": "selection-current",
                "run_type": "transcription",
                "status": "running",
                "version": 2,
            },
        ]
    )

    active_status = status_for(gateway)
    old_keyboard = build_status_keyboard(active_status, focused_run_id="run-old")
    stale_cancel_data = next(
        button.callback_data
        for row in old_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cn:")
    )
    app._set_page_state(
        (10, 7),
        active_status,
        current_cursor=None,
        previous_cursors=[],
        selection=None,
        screen="main",
        focused_run_id="run-current",
    )

    cancel_callback = FakeCallback(data=stale_cancel_data, message=base_message)
    await app._handle_status_callback(cancel_callback)

    assert cancel_callback.answers[-1] == {
        "text": "Эта кнопка устарела. Откройте /inbox ещё раз и повторите действие.",
        "show_alert": True,
    }
    assert api.cancel_requests == []
    assert [run["status"] for run in api.runs] == ["running", "running"]


@pytest.mark.asyncio
async def test_result_callback_sends_transcript_document_when_plain_text_is_too_large() -> None:
    api, gateway, app = make_app()
    base_message = FakeMessage()
    api.runs.append(
        {
            "analysis_run_id": "run-1",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 1,
        }
    )
    api.artifacts.extend(
        [
            {
                "artifact_id": "artifact-docx",
                "analysis_run_id": "run-1",
                "kind": "transcript",
                "status": "available",
                "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "object_key": "artifacts/run-1/transcript/docx/transcript.docx",
                "download": {"url": "https://download.test/transcript.docx"},
            },
            {
                "artifact_id": "artifact-plain",
                "analysis_run_id": "run-1",
                "kind": "transcript",
                "status": "available",
                "content_type": "text/plain; charset=utf-8",
                "object_key": "artifacts/run-1/transcript/plain/transcript.txt",
                "download": {"url": "https://download.test/transcript.txt"},
            },
        ]
    )
    api.internal_artifact_download_access["artifact-plain"] = {
        "artifact_id": "artifact-plain",
        "filename": "transcript.txt",
        "mime_type": "text/plain; charset=utf-8",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }

    completed_status = status_for(gateway)
    completed_keyboard = build_status_keyboard(completed_status, focused_run_id="run-1")
    result_callback_data = next(
        button.callback_data
        for row in completed_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:ar:")
    )
    app._set_page_state(
        (10, 7),
        completed_status,
        current_cursor=None,
        previous_cursors=[],
        selection=None,
        screen="main",
        focused_run_id="run-1",
    )
    app._download_artifact_bytes = lambda _url: ("line\n" * 2000).encode("utf-8")  # type: ignore[method-assign]

    result_callback = FakeCallback(data=result_callback_data, message=base_message)
    await app._handle_status_callback(result_callback)

    assert result_callback.answers[-1]["text"] == "Транскрипт отправлен файлом"
    assert api.internal_artifact_download_access_requests == ["artifact-plain"]
    assert api.get_artifact_requests == []
    assert base_message.answers == []
    assert len(base_message.documents) == 1
    assert base_message.documents[0]["document"].filename == "transcript.txt"
    assert base_message.documents[0]["document"].data.startswith(b"line\nline\n")


@pytest.mark.asyncio
async def test_run_watcher_keeps_materials_screen_stable_during_active_run() -> None:
    api, gateway, app = make_app(page_size=1)
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")
    base_message = FakeMessage()

    run_status = status_for(gateway)
    run_keyboard = build_status_keyboard(run_status)
    run_callback_data = next(
        button.callback_data
        for row in run_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rn:")
    )
    app._set_page_state((10, 7), run_status, current_cursor=None, previous_cursors=[], selection=None, screen="main")
    run_callback = FakeCallback(data=run_callback_data, message=base_message)

    tick = asyncio.Event()
    original_get_run_status = gateway.get_run_status
    statuses = iter(("queued", "running"))

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    def staged_run_status(*, owner: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "running")
        return original_get_run_status(owner=owner, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]

    await app._handle_status_callback(run_callback)
    assert run_callback.answers[-1]["text"] == "Транскрибация запущена"

    materials_callback = FakeCallback(data="ib:mt", message=base_message)
    await app._handle_status_callback(materials_callback)

    assert materials_callback.answers[-1]["text"] == "Открыт список материалов"
    assert app.page_states[(10, 7)].screen == "materials"
    assert base_message.edits[-1]["text"].startswith("Материалы\nМатериалов: 2\n1. Текст: «one»")

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.page_states[(10, 7)].screen == "materials"
    assert app.bot.edit_calls[-1]["text"].startswith("Материалы\nМатериалов: 2\n1. Текст: «one»")


@pytest.mark.asyncio
async def test_run_watcher_auto_delivers_transcript_and_clears_full_collection_after_success() -> None:
    api, gateway, app = make_app(page_size=1, bot=FakeBot())
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")
    base_message = FakeMessage()

    run_status = status_for(gateway)
    run_keyboard = build_status_keyboard(run_status)
    run_callback_data = next(
        button.callback_data
        for row in run_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rn:")
    )
    app._set_page_state((10, 7), run_status, current_cursor=None, previous_cursors=[], selection=None, screen="main")

    tick = asyncio.Event()
    original_get_run_status = gateway.get_run_status
    statuses = iter(("queued", "succeeded"))

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    def staged_run_status(*, owner: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "succeeded")
        return original_get_run_status(owner=owner, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app._download_artifact_bytes = lambda _url: b"transcript ready"  # type: ignore[method-assign]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
            "object_key": "artifacts/run-1/transcript/plain/transcript.txt",
            "download": {"url": "https://download.test/transcript.txt"},
        }
    )
    api.internal_artifact_download_access["artifact-1"] = {
        "artifact_id": "artifact-1",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }

    run_callback = FakeCallback(data=run_callback_data, message=base_message)
    await app._handle_status_callback(run_callback)

    assert run_callback.answers[-1]["text"] == "Транскрибация запущена"

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.bot.send_message_calls == [{"chat_id": 10, "text": "transcript ready"}]
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert api.get_artifact_requests == []
    assert api.collection["items"] == []
    assert api.items == []
    assert [request["media_asset_id"] for request in api.remove_requests] == ["media-1", "media-2"]
    assert "Материалов: 0" in app.bot.edit_calls[-1]["text"]


@pytest.mark.asyncio
async def test_run_watcher_failed_run_preserves_local_inbox() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    gateway.add_text(owner=owner(), text="one")
    base_message = FakeMessage()

    run_status = status_for(gateway)
    run_keyboard = build_status_keyboard(run_status)
    run_callback_data = next(
        button.callback_data
        for row in run_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rn:")
    )
    app._set_page_state((10, 7), run_status, current_cursor=None, previous_cursors=[], selection=None, screen="main")

    tick = asyncio.Event()
    original_get_run_status = gateway.get_run_status
    statuses = iter(("queued", "failed"))

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    def staged_run_status(*, owner: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "failed")
        return original_get_run_status(owner=owner, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]

    run_callback = FakeCallback(data=run_callback_data, message=base_message)
    await app._handle_status_callback(run_callback)

    assert run_callback.answers[-1]["text"] == "Транскрибация запущена"

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.bot.send_message_calls == []
    assert api.collection["items"] == [{"media_asset_id": "media-1", "position": 0}]
    assert api.remove_requests == []
    assert "Материалов: 1" in app.bot.edit_calls[-1]["text"]


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

    app._set_page_state((10, 7), status_for(gateway), current_cursor=None, previous_cursors=[], selection=None, screen="materials")
    broken_remove = FakeCallback(data="ib:rm", message=message)
    await app._handle_status_callback(broken_remove)
    assert broken_remove.answers[-1]["show_alert"] is True

    no_next_state = status_for(gateway)
    app._set_page_state((10, 7), no_next_state, current_cursor=None, previous_cursors=[], selection=None, screen="materials")
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
    assert "только в личном чате" in scope_callback.answers[0]["text"]


def test_helper_functions_cover_remaining_callback_token_and_error_branches() -> None:
    uuid_value = "11111111-1111-1111-1111-111111111111"
    uuid_token = _encode_callback_token(uuid_value)

    assert _decode_callback_token(uuid_token) == uuid_value
    assert _decode_optional_callback_token("_") is None
    assert _encode_callback_version(0) == "0"
    assert str(TelegramUserError(TelegramUserErrorCode.STALE_ACTION)) == "stale_action"
    assert rejected_reason_text(None).startswith("неподдерживаемый ввод:")

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
    assert _artifact_label({"artifact_id": "artifact-1234567890", "kind": "report", "status": "ready"}) == "Отчёт"
    assert _diagnostic_label({"severity": "warning"}) == "Предупреждение"
    assert _media_group_id({"metadata": "not-a-dict"}) is None
    assert _detail_prefix(title="Artifacts", lines=["- one"]) == "Artifacts\n- one\n\n"
    assert _start_text().startswith("Отправь текст, ссылку")
    assert _help_text().startswith("/inbox")

    normalized_404 = _normalize_callback_error(TelegramApiClientError("/v1", 404, "missing", code="gone"))
    normalized_message_404 = _normalize_message_error(TelegramApiClientError("/v1", 404, "missing", code="gone"))
    normalized_message_runtime = _normalize_message_error(RuntimeError("inbox_empty"))
    normalized_key_error = _normalize_callback_error(KeyError("selection_snapshot_id"))
    passthrough = _normalize_callback_error(RuntimeError("boom"))

    assert isinstance(normalized_404, TelegramUserError)
    assert normalized_404.code == TelegramUserErrorCode.STALE_ACTION
    assert isinstance(normalized_message_404, TelegramUserError)
    assert normalized_message_404.code == TelegramUserErrorCode.STALE_ACTION
    assert isinstance(normalized_message_runtime, TelegramUserError)
    assert normalized_message_runtime.code == TelegramUserErrorCode.STALE_ACTION
    assert isinstance(normalized_key_error, TelegramUserError)
    assert isinstance(passthrough, RuntimeError)
    assert _classify_polling_log_message("Failed to fetch updates - TelegramNetworkError: timeout") == "telegram_upstream_failure"
    assert _classify_polling_log_message("Connection established (tryings = 1)") == "telegram_upstream_recovered"
    assert _classify_polling_log_message("Run polling for bot") is None

    with pytest.raises(TelegramUserError):
        _parse_callback_payload("bad")
    with pytest.raises(TelegramUserError):
        _encode_callback_version(-1)
    with pytest.raises(TelegramUserError):
        _decode_callback_token("xinvalid")
