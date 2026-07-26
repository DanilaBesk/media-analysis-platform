from __future__ import annotations

import asyncio
import inspect
import logging
from pathlib import Path
import runpy
import sys
import tempfile
import threading
from types import SimpleNamespace
from typing import Any
import warnings

import pytest
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)

from telegram_adapter import __main__ as telegram_main
from telegram_adapter.api_client import TelegramApiClientError
from telegram_adapter.bot import (
    _PageState,
    TelegramInboxApp,
    _active_run_for_focus,
    _analysis_run_version,
    _artifact_download_url,
    _artifact_filename,
    _artifact_label,
    _classify_telegram_surface_error,
    _classify_polling_log_message,
    _callback_payload,
    _chat_type,
    _decode_callback_token,
    _decode_callback_version,
    _decode_optional_callback_token,
    _detail_prefix,
    _diagnostic_label,
    _display_name_text,
    _encode_callback_token,
    _encode_callback_version,
    _help_text,
    _item_label,
    _kind_text,
    _latest_active_run,
    _media_group_id,
    _media_status_text,
    _message_files,
    _message_text,
    _normalize_callback_error,
    _normalize_message_error,
    _channel_identity_from_channel_account,
    _page_state_from_display_state,
    _parse_callback_payload,
    _run_for_id,
    _run_surface_display_state,
    _select_transcript_artifact,
    _state_key_from_channel_identity,
    _status_surface_display_state,
    _TelegramPollingMonitor,
    _surface_address,
    _surface_address_matches,
    _surface_display_state,
    _surface_message_id,
    _surface_subject_id,
    _start_text,
    _telegram_surface_address,
    _terminal_run_with_payload,
    _transcript_artifact_rank,
    _visible_item_lines,
    build_status_keyboard,
    render_status_text,
)
from telegram_adapter.config import TelegramAdapterSettings, load_settings
from telegram_adapter.errors import TelegramUserError, TelegramUserErrorCode, rejected_reason_text
from telegram_adapter.gateway import (
    InboxStatus,
    IngressRecord,
    TelegramFileInput,
    TelegramInboxGateway,
    youtube_audio_export_ready,
)
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
        self.reusable_transcripts: dict[str, dict[str, Any]] = {}
        self.reusable_transcript_requests: list[dict[str, Any]] = []
        self.channel_accounts: list[dict[str, Any]] = []
        self.channel_surfaces: list[dict[str, Any]] = []
        self.surface_events: list[dict[str, Any]] = []
        self.replace_surface_requests: list[dict[str, Any]] = []
        self.supersede_surface_requests: list[dict[str, Any]] = []
        self.run_events: dict[str, list[dict[str, Any]]] = {}

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
        content = kwargs.get("content")
        local_path = kwargs.get("file_path")
        file_handle = kwargs.get("file_handle")
        if isinstance(content, bytes):
            size_bytes = len(content)
        elif file_handle is not None:
            file_handle.seek(0, 2)
            size_bytes = file_handle.tell()
            file_handle.seek(0)
        else:
            size_bytes = Path(str(local_path)).stat().st_size
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
                "size_bytes": size_bytes,
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

    def start_collection_processing_run(self, **kwargs) -> dict[str, Any]:
        selection = self.create_selection_snapshot(
            channel_account_id=kwargs["channel_account_id"],
            source_collection_id=kwargs["collection_id"],
            items=kwargs["items"],
            option_snapshot=kwargs.get("option_snapshot"),
        )
        run = self.create_analysis_run(
            channel_account_id=kwargs["channel_account_id"],
            selection_snapshot_id=selection["selection_snapshot_id"],
            run_type=kwargs["run_type"],
        )
        captured = {str(item["media_asset_id"]) for item in kwargs["items"]}
        self.collection["items"] = [item for item in self.collection["items"] if str(item["media_asset_id"]) not in captured]
        self.collection["version"] += 1
        return {
            "selection_snapshot": selection,
            "analysis_run": run,
            "detached_media_asset_ids": sorted(captured),
            "collection_version": self.collection["version"],
        }

    def list_analysis_runs(self, **kwargs) -> dict[str, Any]:
        return {"items": list(self.runs), "page": {"page_size": 10, "has_more": False}}

    def get_analysis_run(self, **kwargs) -> dict[str, Any]:
        analysis_run_id = kwargs["analysis_run_id"]
        return next(run for run in self.runs if run["analysis_run_id"] == analysis_run_id)

    def list_analysis_run_events(self, **kwargs) -> dict[str, Any]:
        analysis_run_id = kwargs["analysis_run_id"]
        return {
            "items": list(self.run_events.get(analysis_run_id, [])),
            "page": {"page_size": kwargs.get("page_size") or 10, "has_more": False},
        }

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

    def get_reusable_transcript(self, **kwargs) -> dict[str, Any] | None:
        self.reusable_transcript_requests.append(kwargs)
        return self.reusable_transcripts.get(str(kwargs.get("stored_object_id") or ""))

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
        owner_value = kwargs["channel_identity"]
        external_account_ref = str(owner_value["external_account_ref"])
        existing = next((account for account in self.channel_accounts if account["external_account_ref"] == external_account_ref), None)
        if existing is not None:
            return existing
        account = {
            "channel_account_id": f"channel-account-{len(self.channel_accounts) + 1}",
            "channel": "telegram",
            "external_account_ref": external_account_ref,
            "display_name": external_account_ref,
            "status": "active",
            "metadata": {"channel_identity": owner_value, "adapter_identity": owner_value.get("adapter_identity", {})},
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
    def __init__(
        self,
        *,
        file_bytes: dict[str, bytes] | None = None,
        edit_error: Exception | None = None,
        edit_errors: dict[tuple[int, int], Exception] | None = None,
        send_message_errors: dict[int, Exception] | None = None,
        send_audio_errors: dict[int, Exception] | None = None,
        send_voice_errors: dict[int, Exception] | None = None,
        send_document_errors: dict[int, Exception] | None = None,
    ) -> None:
        self.file_bytes = file_bytes or {}
        self.edit_error = edit_error
        self.edit_errors = edit_errors or {}
        self.send_message_errors = send_message_errors or {}
        self.send_audio_errors = send_audio_errors or {}
        self.send_voice_errors = send_voice_errors or {}
        self.send_document_errors = send_document_errors or {}
        self.set_commands_calls: list[tuple[list[Any], str]] = []
        self.get_file_calls: list[str] = []
        self.download_calls: list[str] = []
        self.edit_calls: list[dict[str, Any]] = []
        self.send_message_calls: list[dict[str, Any]] = []
        self.send_audio_calls: list[dict[str, Any]] = []
        self.send_voice_calls: list[dict[str, Any]] = []
        self.send_document_calls: list[dict[str, Any]] = []
        self.delete_message_calls: list[dict[str, int]] = []
        self.outbound_call_order: list[str] = []

    async def set_my_commands(self, commands: list[Any], *, language_code: str) -> None:
        self.set_commands_calls.append((commands, language_code))

    async def get_file(self, file_id: str) -> SimpleNamespace:
        self.get_file_calls.append(file_id)
        return SimpleNamespace(file_path=f"remote/{file_id}")

    async def download_file(self, file_path: str, *, destination: Any) -> None:
        self.download_calls.append(file_path)
        destination.write(self.file_bytes.get(file_path, b""))

    async def edit_message_text(
        self,
        text: str,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: Any,
        **kwargs: Any,
    ) -> None:
        scoped_error = self.edit_errors.get((chat_id, message_id))
        if scoped_error is not None:
            raise scoped_error
        if self.edit_error is not None:
            raise self.edit_error
        self.edit_calls.append(
            {
                "text": text,
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": reply_markup,
                **kwargs,
            }
        )

    async def send_message(self, chat_id: int, text: str, **kwargs) -> SimpleNamespace:
        scoped_error = self.send_message_errors.get(chat_id)
        if scoped_error is not None:
            raise scoped_error
        self.outbound_call_order.append("message")
        self.send_message_calls.append({"chat_id": chat_id, "text": text, **kwargs})
        return SimpleNamespace(message_id=9003)

    async def send_document(self, chat_id: int, document: Any, **kwargs) -> SimpleNamespace:
        scoped_error = self.send_document_errors.get(chat_id)
        if scoped_error is not None:
            raise scoped_error
        self.outbound_call_order.append("document")
        self.send_document_calls.append({"chat_id": chat_id, "document": document, **kwargs})
        return SimpleNamespace(message_id=9004)

    async def send_audio(self, chat_id: int, audio: Any, **kwargs) -> SimpleNamespace:
        scoped_error = self.send_audio_errors.get(chat_id)
        if scoped_error is not None:
            raise scoped_error
        self.outbound_call_order.append("audio")
        self.send_audio_calls.append({"chat_id": chat_id, "audio": audio, **kwargs})
        return SimpleNamespace(message_id=9005)

    async def send_voice(self, chat_id: int, voice: Any, **kwargs) -> SimpleNamespace:
        scoped_error = self.send_voice_errors.get(chat_id)
        if scoped_error is not None:
            raise scoped_error
        self.outbound_call_order.append("voice")
        self.send_voice_calls.append({"chat_id": chat_id, "voice": voice, **kwargs})
        return SimpleNamespace(message_id=9006)

    async def delete_message(self, chat_id: int, message_id: int) -> None:
        self.outbound_call_order.append("delete")
        self.delete_message_calls.append({"chat_id": chat_id, "message_id": message_id})


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
        video_note: Any | None = None,
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
        self.video_note = video_note
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
        self.id = f"callback-{id(self)}"
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=from_user_id) if from_user_id is not None else None
        self.answers: list[dict[str, Any]] = []

    async def answer(self, text: str, show_alert: bool = False) -> None:
        self.answers.append({"text": text, "show_alert": show_alert})


def channel_identity(chat_id: int = 10, user_id: int | None = 7) -> dict[str, Any]:
    user_suffix = "" if user_id is None else f":user:{user_id}"
    return {
        "channel": "telegram",
        "external_account_ref": f"chat:{chat_id}{user_suffix}",
        "adapter_identity": {
            "telegram_chat_id": str(chat_id),
            "telegram_user_id": "" if user_id is None else str(user_id),
        },
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


def telegram_bad_request(api_method: str, message: str) -> TelegramBadRequest:
    return TelegramBadRequest(method=SimpleNamespace(__api_method__=api_method), message=message)


def telegram_forbidden(api_method: str, message: str) -> TelegramForbiddenError:
    return TelegramForbiddenError(method=SimpleNamespace(__api_method__=api_method), message=message)


def telegram_retry_after(api_method: str, retry_after: int) -> TelegramRetryAfter:
    return TelegramRetryAfter(
        method=SimpleNamespace(__api_method__=api_method),
        message=f"Too Many Requests: retry after {retry_after}",
        retry_after=retry_after,
    )


def status_for(
    gateway: TelegramInboxGateway,
    *,
    rejected: list[IngressRecord] | None = None,
    cursor: str | None = None,
) -> InboxStatus:
    return gateway.restore_status(channel_identity=channel_identity(), rejected=rejected, cursor=cursor)


def test_load_settings_reads_explicit_env_mapping() -> None:
    settings = load_settings(
        Path("/tmp/runtime"),
        env={
            "TELEGRAM_BOT_TOKEN": "  secret-token  ",
            "ALLOWED_USER_IDS": "1, 2,3",
            "TELEGRAM_BOT_API_BASE_URL": "  http://telegram-bot-api:8081/  ",
            "TELEGRAM_BOT_API_IS_LOCAL": "true",
        },
    )

    assert settings.telegram_bot_token == "secret-token"
    assert settings.allowed_user_ids == (1, 2, 3)
    assert settings.telegram_bot_api_base_url == "http://telegram-bot-api:8081"
    assert settings.telegram_bot_api_local_mode is True


def test_load_settings_supports_explicit_non_local_custom_bot_api_server() -> None:
    settings = load_settings(
        Path("/tmp/runtime"),
        env={
            "TELEGRAM_BOT_TOKEN": "secret-token",
            "TELEGRAM_BOT_API_BASE_URL": "http://bot-api-proxy.internal:8081",
            "TELEGRAM_BOT_API_IS_LOCAL": "false",
        },
    )

    assert settings.telegram_bot_api_base_url == "http://bot-api-proxy.internal:8081"
    assert settings.telegram_bot_api_local_mode is False


def test_load_settings_keeps_cloud_bot_api_default_for_compatibility() -> None:
    settings = load_settings(
        Path("/tmp/runtime"),
        env={
            "TELEGRAM_BOT_TOKEN": "secret-token",
        },
    )

    assert settings.telegram_bot_api_base_url is None
    assert settings.telegram_bot_api_local_mode is False


def test_load_settings_reads_local_bot_api_endpoint() -> None:
    settings = load_settings(
        Path("/tmp/runtime"),
        env={
            "TELEGRAM_BOT_TOKEN": "123:ABC",
            "TELEGRAM_BOT_API_BASE_URL": "  http://telegram-bot-api:8081/  ",
            "TELEGRAM_BOT_API_LOCAL_MODE": "true",
        },
    )

    assert settings.telegram_bot_api_base_url == "http://telegram-bot-api:8081"
    assert settings.telegram_bot_api_local_mode is True


def test_message_files_preserves_telegram_voice_duration_for_status_summary() -> None:
    message = FakeMessage(
        voice=SimpleNamespace(
            file_id="voice-file",
            file_unique_id="voice-unique",
            mime_type="audio/ogg",
            file_size=3_639_024,
            duration=966,
        )
    )

    files = list(_message_files(message))

    assert len(files) == 1
    assert files[0].kind == "voice"
    assert files[0].duration_seconds == 966


def test_message_files_accepts_telegram_video_note_as_video_input() -> None:
    message = FakeMessage(
        video_note=SimpleNamespace(
            file_id="round-video-file",
            file_unique_id="round-video-unique",
            file_size=1_234_567,
            duration=25,
        ),
        message_id=555,
    )

    files = list(_message_files(message))

    assert len(files) == 1
    assert files[0].kind == "video"
    assert files[0].file_id == "round-video-file"
    assert files[0].file_unique_id == "round-video-unique"
    assert files[0].file_name == "telegram-video-note.mp4"
    assert files[0].content_type == "video/mp4"
    assert files[0].size_bytes == 1_234_567
    assert files[0].duration_seconds == 25
    assert files[0].message_id == 555


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


@pytest.mark.asyncio
async def test_app_uses_local_telegram_bot_api_session_when_configured() -> None:
    api = FakeFinalApiClient()
    settings = TelegramAdapterSettings(
        telegram_bot_token="123:ABC",
        allowed_user_ids=(),
        telegram_bot_api_base_url="http://telegram-bot-api:8081",
        telegram_bot_api_local_mode=True,
    )
    app = TelegramInboxApp(settings, TelegramInboxGateway(api))

    try:
        assert app.bot.session.api.base == "http://telegram-bot-api:8081/bot{token}/{method}"
        assert app.bot.session.api.file == "http://telegram-bot-api:8081/file/bot{token}/{path}"
        assert app.bot.session.api.is_local is True
    finally:
        await app.bot.session.close()


def test_run_builds_adapter_dependencies_and_uses_default_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    settings = TelegramAdapterSettings(telegram_bot_token="token", allowed_user_ids=(7,))

    def fake_load_settings(base_dir: Path, *, env: dict[str, str]) -> TelegramAdapterSettings:
        captured["base_dir"] = base_dir
        captured["env"] = env
        return settings

    class FakeApiClient:
        def __init__(self, base_url: str, **kwargs: Any) -> None:
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

    assert accepted_message.answers[0]["text"].startswith("Обработка\nМатериалов: 1\nТекст: «Keep text»")
    assert accepted_message.answers[0]["link_preview_options"].is_disabled is True
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
async def test_handle_any_message_shows_pending_card_before_large_media_download_finishes() -> None:
    _, _, app = make_app()
    download_started = asyncio.Event()
    finish_download = asyncio.Event()
    message = FakeMessage(
        video=SimpleNamespace(
            file_id="large-video-file",
            file_unique_id="large-video-unique",
            file_name="CS SEO.mp4",
            mime_type="video/mp4",
            file_size=180_000_000,
            duration=2316,
        ),
        message_id=665,
    )

    async def slow_download_message_files(current: FakeMessage) -> list[TelegramFileInput]:
        download_started.set()
        await finish_download.wait()
        return [
            TelegramFileInput(
                kind="video",
                file_id=current.video.file_id,
                file_unique_id=current.video.file_unique_id,
                file_name=current.video.file_name,
                content_type=current.video.mime_type,
                content=b"video bytes",
                size_bytes=current.video.file_size,
                duration_seconds=current.video.duration,
                message_id=current.message_id,
            )
        ]

    app._download_message_files = slow_download_message_files  # type: ignore[method-assign]

    task = asyncio.create_task(app._handle_any_message(message))
    await download_started.wait()
    await asyncio.sleep(0)

    assert len(message.answers) == 1
    assert message.answers[0]["text"].startswith("Обработка\nМатериалов: 1\nCS SEO.mp4 · 171.7 MB · 38:36")
    assert "Статус: получаем видео из Telegram" in message.answers[0]["text"]
    assert app.status_message_ids[(10, 7)] == 9001

    finish_download.set()
    await task

    assert "Статус: получаем видео из Telegram" not in app.bot.edit_calls[-1]["text"]
    assert app.bot.edit_calls[-1]["message_id"] == 9001
    assert app.bot.edit_calls[-1]["text"].startswith("Обработка\nМатериалов: 1\nCS SEO.mp4 · 11 B · 38:36")


@pytest.mark.asyncio
async def test_handle_any_message_reports_too_large_telegram_file_as_unsupported_input(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class TooLargeFileBot(FakeBot):
        async def get_file(self, file_id: str) -> SimpleNamespace:
            self.get_file_calls.append(file_id)
            raise telegram_bad_request("getFile", "Bad Request: file is too big")

    bot = TooLargeFileBot()
    _, _, app = make_app(bot=bot)
    message = FakeMessage(
        video=SimpleNamespace(
            file_id="video-too-large",
            file_unique_id="video-too-large-unique",
            file_name="long-call.mp4",
            mime_type="video/mp4",
            file_size=125_000_000,
            duration=1501,
        ),
        message_id=664,
    )

    with caplog.at_level(logging.ERROR):
        await app._handle_any_message(message)

    assert message.answers[-1]["text"] == (
        "неподдерживаемый ввод: файл слишком большой для скачивания через Telegram-бот. "
        "Отправьте ссылку на видео или файл меньшего размера."
    )
    assert "Сервис временно недоступен" not in message.answers[-1]["text"]
    assert bot.get_file_calls == ["video-too-large"]
    assert bot.download_calls == []
    assert "scope=message_ingest" in caplog.text
    assert "normalized_code=unsupported_input" in caplog.text
    assert "detail=telegram_file_too_big" in caplog.text


@pytest.mark.asyncio
async def test_download_message_files_uses_anonymous_disk_stream_and_rejects_empty_download() -> None:
    photo = SimpleNamespace(file_id="photo-1", file_unique_id="photo-u", file_size=10)
    good_bot = FakeBot(file_bytes={"remote/photo-1": b"photo-bytes"})
    _, _, app = make_app(bot=good_bot)
    message = FakeMessage(photo=[photo], caption="caption", message_id=77, media_group_id="grp")

    files = await app._download_message_files(message)

    assert files[0].kind == "photo"
    assert files[0].content is None
    assert files[0].local_path is None
    assert files[0].file_handle is not None
    assert files[0].file_handle.read() == b"photo-bytes"
    files[0].file_handle.close()
    assert good_bot.get_file_calls == ["photo-1"]
    assert good_bot.download_calls == ["remote/photo-1"]

    empty_bot = FakeBot(file_bytes={"remote/photo-1": b""})
    _, _, empty_app = make_app(bot=empty_bot)
    with pytest.raises(RuntimeError, match="telegram_file_download_failed"):
        await empty_app._download_message_files(message)


@pytest.mark.asyncio
async def test_file_ingest_uses_anonymous_temp_stream_and_closes_it_after_upload() -> None:
    bot = FakeBot(file_bytes={"remote/video-1": b"v" * 1024 * 1024})
    api, _, app = make_app(bot=bot)
    message = FakeMessage(
        video=SimpleNamespace(
            file_id="video-1", file_unique_id="video-u", file_name="large.mp4", mime_type="video/mp4",
            file_size=1024 * 1024, duration=10,
        ),
    )

    await app._handle_any_message(message)

    request = api.upload_requests[-1]
    assert request["content"] is None
    assert request["file_path"] is None
    assert request["file_handle"].closed


@pytest.mark.asyncio
async def test_export_delivery_sends_authenticated_audio_as_playable_track_then_acks() -> None:
    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []
            self.failures: list[dict[str, Any]] = []

        def claim_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "export.m4a",
                "content_type": "audio/mp4",
                "url": "http://minio/export.m4a",
                "size_bytes": 12,
            }

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            return {}

        def fail_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.failures.append(kwargs)
            return {}

    gateway = ExportGateway()
    bot = FakeBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    delivered_files: list[Any] = []

    def export_file(_url: str, _size: int) -> Any:
        handle = tempfile.TemporaryFile(mode="w+b")
        handle.write(b"export-bytes")
        handle.seek(0)
        delivered_files.append(handle)
        return handle

    app._download_artifact_file = export_file  # type: ignore[method-assign]

    await app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)

    assert len(bot.send_audio_calls) == 1
    assert bot.send_audio_calls[0]["audio"].filename == "export.m4a"
    assert bot.send_audio_calls[0]["caption"] == "Экспорт готов"
    assert bot.send_voice_calls == []
    assert bot.send_document_calls == []
    assert gateway.acks[0]["export_job_id"] == "job-1"
    assert delivered_files[0].closed


@pytest.mark.asyncio
async def test_export_delivery_sends_music_presentation_as_native_audio_card_without_caption() -> None:
    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []

        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "export.m4a",
                "content_type": " Audio/MP4 ; codecs=mp4a.40.2 ",
                "url": "http://minio/export.m4a",
                "size_bytes": 12,
                "presentation": {
                    "kind": "music",
                    "title": "T" * 70,
                    "performer": "P" * 70,
                    "duration_seconds": 183,
                },
            }

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            return {}

        def fail_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("successful delivery must not be failed")

    gateway = ExportGateway()
    bot = FakeBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    delivered_file = tempfile.TemporaryFile(mode="w+b")
    delivered_file.write(b"export-bytes")
    delivered_file.seek(0)
    app._download_artifact_file = lambda _url, _size: delivered_file  # type: ignore[method-assign]

    await app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)

    assert len(bot.send_audio_calls) == 1
    call = bot.send_audio_calls[0]
    assert call["audio"].filename == "export.m4a"
    assert call["title"] == "T" * 64
    assert call["performer"] == "P" * 64
    assert call["duration"] == 183
    assert "caption" not in call
    assert bot.send_voice_calls == []
    assert bot.send_document_calls == []
    assert delivered_file.closed
    assert len(gateway.acks) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("duration_seconds", [0, -1, "not-a-duration", None, True])
async def test_export_delivery_omits_invalid_music_duration_but_keeps_native_card(
    duration_seconds: Any,
) -> None:
    class ExportGateway:
        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "export.mp3",
                "content_type": "audio/mpeg",
                "url": "http://minio/export.mp3",
                "size_bytes": 12,
                "presentation": {
                    "kind": "music",
                    "title": "Title",
                    "performer": "Performer",
                    "duration_seconds": duration_seconds,
                },
            }

        def acknowledge_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {}

        def fail_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("successful delivery must not be failed")

    gateway = ExportGateway()
    bot = FakeBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    delivered_file = tempfile.TemporaryFile(mode="w+b")
    delivered_file.write(b"export-bytes")
    delivered_file.seek(0)
    app._download_artifact_file = lambda _url, _size: delivered_file  # type: ignore[method-assign]

    await app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)

    call = bot.send_audio_calls[0]
    assert call["title"] == "Title"
    assert call["performer"] == "Performer"
    assert "duration" not in call
    assert "caption" not in call
    assert delivered_file.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filename", "content_type"),
    [
        ("voice.ogg", "audio/ogg"),
        ("voice.opus", "audio/opus"),
        ("voice.ogg", "Audio/OGG; codecs=opus"),
        ("voice.ogg", "application/ogg"),
    ],
)
async def test_export_delivery_sends_authenticated_voice_download_as_telegram_voice_and_acks(
    filename: str,
    content_type: str,
) -> None:
    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []

        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": filename,
                "content_type": content_type,
                "url": f"http://minio/{filename}",
                "size_bytes": 12,
            }

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            return {}

        def fail_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("successful delivery must not be failed")

    gateway = ExportGateway()
    bot = FakeBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    delivered_file = tempfile.TemporaryFile(mode="w+b")
    delivered_file.write(b"export-bytes")
    delivered_file.seek(0)
    app._download_artifact_file = lambda _url, _size: delivered_file  # type: ignore[method-assign]

    await app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)

    assert len(bot.send_voice_calls) == 1
    assert bot.send_voice_calls[0]["voice"].filename == filename
    assert bot.send_voice_calls[0]["caption"] == "Экспорт готов"
    assert bot.send_audio_calls == []
    assert bot.send_document_calls == []
    assert delivered_file.closed
    assert len(gateway.acks) == 1


def test_youtube_audio_export_waits_for_stable_music_metadata() -> None:
    item = {
        "metadata": {
            "provider_metadata": {
                "provider": "youtube",
                "title": "Track",
            }
        }
    }
    assert youtube_audio_export_ready(item) is False
    item["metadata"]["provider_metadata"]["performer"] = "Artist"
    assert youtube_audio_export_ready(item) is True


@pytest.mark.asyncio
async def test_export_delivery_keeps_non_audio_download_as_document() -> None:
    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []

        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "export.mp4",
                "content_type": "video/mp4",
                "url": "http://minio/export.mp4",
                "size_bytes": 12,
            }

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            return {}

        def fail_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("successful delivery must not be failed")

    gateway = ExportGateway()
    bot = FakeBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    delivered_file = tempfile.TemporaryFile(mode="w+b")
    delivered_file.write(b"export-bytes")
    delivered_file.seek(0)
    app._download_artifact_file = lambda _url, _size: delivered_file  # type: ignore[method-assign]

    await app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)

    assert bot.send_audio_calls == []
    assert bot.send_voice_calls == []
    assert len(bot.send_document_calls) == 1
    assert bot.send_document_calls[0]["document"].filename == "export.mp4"
    assert bot.send_document_calls[0]["caption"] == "Экспорт готов"
    assert delivered_file.closed
    assert len(gateway.acks) == 1


@pytest.mark.asyncio
async def test_export_delivery_records_voice_send_failure_and_closes_file() -> None:
    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []
            self.failures: list[dict[str, Any]] = []

        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "voice.ogg",
                "content_type": "audio/ogg",
                "url": "http://minio/voice.ogg",
                "size_bytes": 12,
            }

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            return {}

        def fail_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.failures.append(kwargs)
            return {}

    gateway = ExportGateway()
    bot = FakeBot(send_voice_errors={10: RuntimeError("send failed")})
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    delivered_file = tempfile.TemporaryFile(mode="w+b")
    delivered_file.write(b"export-bytes")
    delivered_file.seek(0)
    app._download_artifact_file = lambda _url, _size: delivered_file  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="send failed"):
        await app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)

    assert gateway.acks == []
    assert gateway.failures[0]["failure_code"] == "telegram_delivery_failed"
    assert delivered_file.closed


@pytest.mark.asyncio
async def test_export_delivery_heartbeats_repeatedly_until_blocked_send_finishes() -> None:
    events: list[str] = []

    class ExportGateway:
        def __init__(self) -> None:
            self.claims: list[dict[str, Any]] = []
            self.heartbeats: list[dict[str, Any]] = []
            self.acks: list[dict[str, Any]] = []

        def claim_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.claims.append(kwargs)
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "voice.ogg",
                "content_type": "audio/ogg",
                "url": "http://minio/voice.ogg",
                "size_bytes": 12,
            }

        def heartbeat_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.heartbeats.append(kwargs)
            events.append("heartbeat")
            return kwargs["claim"]

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            events.append("ack")
            return {}

        def fail_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("successful delivery must not be failed")

    class BlockingBot(FakeBot):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = asyncio.Event()
            self.release_send = asyncio.Event()

        async def send_voice(self, chat_id: int, voice: Any, **kwargs: Any) -> SimpleNamespace:
            events.append("send_started")
            self.send_started.set()
            await self.release_send.wait()
            result = await super().send_voice(chat_id, voice, **kwargs)
            events.append("send_finished")
            return result

    gateway = ExportGateway()
    bot = BlockingBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    assert app.export_delivery_lease_seconds == 120
    assert app.export_delivery_heartbeat_interval_seconds == 30.0
    sleep_started: asyncio.Queue[float] = asyncio.Queue()
    sleep_releases: asyncio.Queue[None] = asyncio.Queue()
    sleep_cancellations = 0

    async def controlled_sleep(seconds: float) -> None:
        nonlocal sleep_cancellations
        await sleep_started.put(seconds)
        try:
            await sleep_releases.get()
        except asyncio.CancelledError:
            sleep_cancellations += 1
            raise

    app._sleep = controlled_sleep
    app._download_artifact_file = lambda _url, _size: tempfile.TemporaryFile(mode="w+b")  # type: ignore[method-assign]

    delivery_task = asyncio.create_task(
        app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)
    )
    await bot.send_started.wait()
    simulated_elapsed = 0.0
    for _ in range(5):
        interval = await sleep_started.get()
        assert interval == 30.0
        simulated_elapsed += interval
        await sleep_releases.put(None)
    assert await sleep_started.get() == 30.0

    assert simulated_elapsed > app.export_delivery_lease_seconds
    assert len(gateway.heartbeats) == 5
    assert gateway.acks == []
    bot.release_send.set()
    await delivery_task

    assert gateway.claims[0]["lease_seconds"] == 120
    assert all(call["lease_seconds"] == 120 for call in gateway.heartbeats)
    assert events.index("ack") > events.index("send_finished")
    assert sleep_cancellations == 1


@pytest.mark.asyncio
async def test_export_delivery_stops_without_ack_when_heartbeat_loses_the_fence() -> None:
    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []
            self.failures: list[dict[str, Any]] = []

        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {
                "filename": "voice.ogg",
                "content_type": "audio/ogg",
                "url": "http://minio/voice.ogg",
                "size_bytes": 12,
            }

        def heartbeat_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("stale delivery claim")

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            return {}

        def fail_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.failures.append(kwargs)
            return {}

    class BlockingBot(FakeBot):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = asyncio.Event()
            self.send_cancelled = asyncio.Event()

        async def send_voice(self, chat_id: int, voice: Any, **kwargs: Any) -> SimpleNamespace:
            self.send_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.send_cancelled.set()
                raise
            return await super().send_voice(chat_id, voice, **kwargs)

    gateway = ExportGateway()
    bot = BlockingBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    assert app.export_delivery_heartbeat_interval_seconds == 30.0
    heartbeat_due = asyncio.Event()

    async def controlled_sleep(_seconds: float) -> None:
        await heartbeat_due.wait()

    app._sleep = controlled_sleep
    downloaded_file = tempfile.TemporaryFile(mode="w+b")
    app._download_artifact_file = lambda _url, _size: downloaded_file  # type: ignore[method-assign]
    loop_errors: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    try:
        delivery_task = asyncio.create_task(
            app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)
        )
        await bot.send_started.wait()
        heartbeat_due.set()

        with pytest.raises(RuntimeError, match="stale delivery claim"):
            await delivery_task
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert bot.send_cancelled.is_set()
    assert downloaded_file.closed
    assert gateway.acks == []
    assert gateway.failures[0]["failure_code"] == "telegram_delivery_failed"
    assert loop_errors == []


@pytest.mark.asyncio
async def test_voice_export_delivery_waits_for_inflight_heartbeat_before_ack() -> None:
    events: list[str] = []
    heartbeat_started = threading.Event()
    release_heartbeat = threading.Event()

    class ExportGateway:
        def __init__(self) -> None:
            self.acks: list[dict[str, Any]] = []
            self.failures: list[dict[str, Any]] = []

        def claim_export_delivery(self, **_kwargs: Any) -> dict[str, Any]:
            return {"delivery": {"export_delivery_id": "delivery-1"}, "lease_owner": "adapter", "attempt_token": "t" * 16}

        def get_internal_export_download(self, **_kwargs: Any) -> dict[str, Any]:
            return {"filename": "voice.ogg", "content_type": "audio/ogg", "url": "http://minio/voice.ogg", "size_bytes": 12}

        def heartbeat_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            events.append("heartbeat_started")
            heartbeat_started.set()
            assert release_heartbeat.wait(timeout=5)
            events.append("heartbeat_returned")
            return kwargs["claim"]

        def acknowledge_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.acks.append(kwargs)
            events.append("ack")
            return {}

        def fail_export_delivery(self, **kwargs: Any) -> dict[str, Any]:
            self.failures.append(kwargs)
            return {}

    class ControlledBot(FakeBot):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = asyncio.Event()
            self.release_send = asyncio.Event()
            self.send_finished = asyncio.Event()

        async def send_voice(self, chat_id: int, voice: Any, **kwargs: Any) -> SimpleNamespace:
            self.send_started.set()
            await self.release_send.wait()
            result = await super().send_voice(chat_id, voice, **kwargs)
            events.append("send_finished")
            self.send_finished.set()
            return result

    gateway = ExportGateway()
    bot = ControlledBot()
    app = TelegramInboxApp(SimpleNamespace(allowed_user_ids=set()), gateway, bot=bot)  # type: ignore[arg-type]
    heartbeat_due = asyncio.Event()
    app._sleep = lambda _seconds: heartbeat_due.wait()  # type: ignore[assignment]
    app._download_artifact_file = lambda _url, _size: tempfile.TemporaryFile(mode="w+b")  # type: ignore[method-assign]

    delivery_task = asyncio.create_task(
        app._deliver_export_result(channel_identity=channel_identity(), export_job_id="job-1", chat_id=10)
    )
    await bot.send_started.wait()
    heartbeat_due.set()
    assert await asyncio.to_thread(heartbeat_started.wait, 5)
    bot.release_send.set()
    await bot.send_finished.wait()
    release_heartbeat.set()
    await delivery_task

    assert events.index("heartbeat_returned") < events.index("ack")
    assert len(gateway.acks) == 1
    assert gateway.failures == []


@pytest.mark.asyncio
async def test_export_watcher_retries_delivery_before_finishing_surface() -> None:
    _, gateway, app = make_app()
    delivery_attempts = 0
    finished: list[str] = []

    gateway.get_export_job = lambda **_kwargs: {"export_job_id": "job-1", "status": "succeeded"}  # type: ignore[method-assign]

    async def deliver(**_kwargs: Any) -> None:
        nonlocal delivery_attempts
        delivery_attempts += 1
        if delivery_attempts == 1:
            raise RuntimeError("temporary Telegram failure")

    app._deliver_export_result = deliver  # type: ignore[method-assign]
    app._try_finish_export_task_surface = lambda **kwargs: finished.append(str(kwargs["export_job_id"]))  # type: ignore[method-assign]
    app._sleep = lambda _seconds: asyncio.sleep(0)  # type: ignore[assignment]
    app.run_status_follow_attempts = 1
    app.export_status_follow_attempts = 2

    await app._track_export_status_until_terminal(
        channel_identity=channel_identity(),
        export_job_id="job-1",
        chat_id=10,
        surface=None,
    )

    assert delivery_attempts == 2
    assert finished == ["job-1"]


@pytest.mark.asyncio
async def test_export_watcher_outlives_analysis_polling_budget() -> None:
    _, gateway, app = make_app()
    polls = 0
    delivered: list[str] = []

    def get_export_job(**_kwargs: Any) -> dict[str, Any]:
        nonlocal polls
        polls += 1
        return {"export_job_id": "job-slow", "status": "succeeded" if polls == 122 else "running"}

    gateway.get_export_job = get_export_job  # type: ignore[method-assign]
    app._deliver_export_result = lambda **_kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    app._try_finish_export_task_surface = lambda **kwargs: delivered.append(str(kwargs["export_job_id"]))  # type: ignore[method-assign]
    app._sleep = lambda _seconds: asyncio.sleep(0)  # type: ignore[assignment]
    app.run_status_follow_attempts = 120
    app.export_status_follow_attempts = 122

    await app._track_export_status_until_terminal(
        channel_identity=channel_identity(), export_job_id="job-slow", chat_id=10, surface=None,
    )

    assert polls == 122
    assert delivered == ["job-slow"]


@pytest.mark.asyncio
async def test_export_watcher_finishes_the_latest_persisted_surface_handle() -> None:
    _, gateway, app = make_app()
    initial_surface = {"channel_surface_id": "surface-1", "version": 1, "lifecycle_status": "active"}
    latest_surface = {"channel_surface_id": "surface-1", "version": 2, "lifecycle_status": "active"}
    finished: list[dict[str, Any] | None] = []
    gateway.get_export_job = lambda **_kwargs: {"export_job_id": "job-1", "status": "succeeded"}  # type: ignore[method-assign]

    async def refresh(**_kwargs: Any) -> dict[str, Any]:
        return latest_surface

    app._refresh_export_task_status = refresh  # type: ignore[method-assign]
    app._deliver_export_result = lambda **_kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    app._try_finish_export_task_surface = lambda **kwargs: finished.append(kwargs.get("surface"))  # type: ignore[method-assign]

    await app._track_export_status_until_terminal(
        channel_identity=channel_identity(),
        export_job_id="job-1",
        chat_id=10,
        surface=initial_surface,
    )

    assert finished == [latest_surface]


@pytest.mark.asyncio
async def test_export_callback_survives_unrelated_collection_change_and_anchors_separate_task() -> None:
    api, gateway, app = make_app()
    api.items = [
        {
            "media_asset_id": "video-1",
            "kind": "video",
            "status": "ready",
            "display_name": "clip.mp4",
            "origin": {"origin_type": "upload", "origin_ref": "sources/video/clip.mp4"},
            "metadata": {},
        }
    ]
    api.collection["items"] = [{"media_asset_id": "video-1", "position": 0}]
    api.create_export_job = lambda **_kwargs: {"export_job_id": "job-1", "status": "queued"}  # type: ignore[attr-defined]
    scheduled: list[dict[str, Any]] = []
    app._schedule_export_status_tracking = lambda **kwargs: scheduled.append(kwargs)  # type: ignore[method-assign]
    message = FakeMessage(message_id=101)
    callback = FakeCallback(
        data=_callback_payload(
            "ea",
            _encode_callback_token("inbox-1"),
            _encode_callback_version(1),
            _encode_callback_token("video-1"),
        ),
        message=message,
    )

    api.items.append({"media_asset_id": "text-2", "kind": "text", "status": "ready", "display_name": "later"})
    api.collection["items"].append({"media_asset_id": "text-2", "position": 1})
    api.collection["version"] = 2

    await app._handle_status_callback(callback)

    current_surface = next(surface for surface in api.channel_surfaces if surface["surface_type"] == "current_materials_panel")
    export_surface = next(surface for surface in api.channel_surfaces if surface["surface_type"] == "export_task_surface")
    assert current_surface["address"] == {"chat_id": 10, "message_id": 101}
    assert export_surface["address"] == {"chat_id": 10, "message_id": 9003}
    assert export_surface["address_fingerprint"] == "telegram:10:9003"
    assert scheduled[0]["surface"] is export_surface
    assert app.bot.send_message_calls[0]["text"].startswith("Экспорт")

    await app._handle_status_callback(callback)

    assert len(app.bot.send_message_calls) == 1
    assert scheduled[-1]["surface"] is export_surface


@pytest.mark.asyncio
async def test_export_surface_recovery_reanchors_legacy_shared_panel_address() -> None:
    api, gateway, app = make_app()
    status = status_for(gateway)
    current_surface = gateway.upsert_current_materials_surface(
        channel_identity=channel_identity(),
        address={"kind": "telegram_message", "chat_id": 10, "message_id": 5001},
        display_state=_status_surface_display_state(status, _PageState()),
    )
    export_surface = gateway.upsert_export_task_surface(
        channel_identity=channel_identity(),
        export_job={"export_job_id": "job-1", "status": "running"},
        address={"kind": "telegram_message", "chat_id": 10, "message_id": 5001},
        display_state={"export_job_id": "job-1", "export_status": "running"},
    )
    gateway.get_export_job = lambda **_kwargs: {"export_job_id": "job-1", "status": "running"}  # type: ignore[method-assign]
    scheduled: list[dict[str, Any]] = []
    app._schedule_export_status_tracking = lambda **kwargs: scheduled.append(kwargs)  # type: ignore[method-assign]

    await app._recover_export_task_surface(channel_identity=channel_identity(), surface=export_surface)

    assert current_surface["address"] == {"kind": "telegram_message", "chat_id": 10, "message_id": 5001}
    assert scheduled[0]["surface"]["address"] == {"chat_id": 10, "message_id": 9003}
    assert scheduled[0]["surface"]["address_fingerprint"] == "telegram:10:9003"


@pytest.mark.asyncio
async def test_send_or_edit_status_prefers_edit_then_falls_back_to_new_message() -> None:
    edit_bot = FakeBot()
    _, gateway, app = make_app(bot=edit_bot)
    gateway.add_text(channel_identity=channel_identity(), text="first item")
    message = FakeMessage()
    app.status_message_ids[(10, 7)] = 5001

    edited = await app._send_or_edit_status(message)

    assert edited is True
    assert edit_bot.edit_calls[0]["message_id"] == 5001
    assert message.answers == []

    failing_bot = FakeBot(edit_error=RuntimeError("stale message"))
    _, failing_gateway, failing_app = make_app(bot=failing_bot)
    failing_gateway.add_text(channel_identity=channel_identity(), text="fallback item")
    fallback_message = FakeMessage()
    failing_app.status_message_ids[(10, 7)] = 5002

    sent = await failing_app._send_or_edit_status(fallback_message)

    assert sent is True
    assert failing_app.status_message_ids[(10, 7)] == 9001
    assert "fallback item" in fallback_message.answers[0]["text"]


@pytest.mark.asyncio
async def test_send_or_edit_status_treats_not_modified_as_success() -> None:
    not_modified = telegram_bad_request(
        "editMessageText",
        "Bad Request: message is not modified: specified new message content and reply markup are exactly the same",
    )
    bot = FakeBot(edit_error=not_modified)
    _, gateway, app = make_app(bot=bot)
    gateway.add_text(channel_identity=channel_identity(), text="same status")
    message = FakeMessage()
    app.status_message_ids[(10, 7)] = 5001

    sent = await app._send_or_edit_status(message)

    assert sent is True
    assert app.status_message_ids[(10, 7)] == 5001
    assert message.answers == []


@pytest.mark.asyncio
async def test_send_or_edit_status_can_force_fresh_reply_for_new_inbound_message() -> None:
    edit_bot = FakeBot()
    _, gateway, app = make_app(bot=edit_bot)
    gateway.add_text(channel_identity=channel_identity(), text="fresh inbound item")
    message = FakeMessage()
    app.status_message_ids[(10, 7)] = 5001

    sent = await app._send_or_edit_status(message, prefer_edit=False)

    assert sent is True
    assert edit_bot.edit_calls == []
    assert app.status_message_ids[(10, 7)] == 9001
    assert "fresh inbound item" in message.answers[0]["text"]
    assert message.answers[0]["link_preview_options"].is_disabled is True


@pytest.mark.asyncio
async def test_inbound_message_burst_reuses_one_current_materials_card() -> None:
    class SlowAnswerMessage(FakeMessage):
        async def answer(self, text: str, **kwargs) -> SimpleNamespace:
            await asyncio.sleep(0)
            return await super().answer(text, **kwargs)

    bot = FakeBot()
    api, _, app = make_app(bot=bot)
    messages = [SlowAnswerMessage(text=f"forwarded note {index}", message_id=1000 + index) for index in range(20)]

    await asyncio.gather(*(app._handle_any_message(message) for message in messages))

    answer_count = sum(len(message.answers) for message in messages)
    assert answer_count == 1
    assert len(bot.edit_calls) == 19
    assert bot.edit_calls[-1]["message_id"] == app.status_message_ids[(10, 7)]
    assert "Материалов: 20" in bot.edit_calls[-1]["text"]
    assert bot.edit_calls[-1]["link_preview_options"].is_disabled is True
    active_current_surfaces = [
        surface
        for surface in api.channel_surfaces
        if surface["surface_type"] == "current_materials_panel" and surface["lifecycle_status"] == "active"
    ]
    assert len(active_current_surfaces) == 1
    assert active_current_surfaces[0]["address"] == {"chat_id": 10, "message_id": app.status_message_ids[(10, 7)]}


@pytest.mark.asyncio
async def test_new_inbound_burst_creates_one_fresh_visible_card_after_previous_card() -> None:
    class SequencedAnswerMessage(FakeMessage):
        next_answer_id = 9001

        async def answer(self, text: str, **kwargs) -> SimpleNamespace:
            self.answers.append({"text": text, **kwargs})
            message_id = SequencedAnswerMessage.next_answer_id
            SequencedAnswerMessage.next_answer_id += 1
            return SimpleNamespace(message_id=message_id)

    now = 100.0
    bot = FakeBot()
    api, _, app = make_app(bot=bot)
    app.inbound_status_burst_window_seconds = 10.0
    app._monotonic = lambda: now  # type: ignore[assignment]

    first_burst = [SequencedAnswerMessage(text=f"first burst {index}", message_id=1000 + index) for index in range(3)]
    await asyncio.gather(*(app._handle_any_message(message) for message in first_burst))

    assert sum(len(message.answers) for message in first_burst) == 1
    assert app.status_message_ids[(10, 7)] == 9001

    now = 200.0
    second_burst = [SequencedAnswerMessage(text=f"second burst {index}", message_id=2000 + index) for index in range(20)]
    await asyncio.gather(*(app._handle_any_message(message) for message in second_burst))

    assert sum(len(message.answers) for message in second_burst) == 1
    assert app.status_message_ids[(10, 7)] == 9002
    assert bot.edit_calls[-1]["message_id"] == 9002
    assert "Материалов: 23" in bot.edit_calls[-1]["text"]
    active_current_surfaces = [
        surface
        for surface in api.channel_surfaces
        if surface["surface_type"] == "current_materials_panel" and surface["lifecycle_status"] == "active"
    ]
    assert len(active_current_surfaces) == 1
    assert active_current_surfaces[0]["address"] == {"chat_id": 10, "message_id": 9002}


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
    gateway.add_text(channel_identity=channel_identity(), text="surface API is unavailable")
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
    gateway.add_text(channel_identity=channel_identity(), text="surface replacement")
    account = api.resolve_channel_account(channel_identity=channel_identity())
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
async def test_status_surface_supersedes_current_surface_after_generic_edit_failure() -> None:
    api, gateway, app = make_app(bot=FakeBot(edit_error=RuntimeError("edit transport failed")))
    gateway.add_text(channel_identity=channel_identity(), text="surface fallback")
    account = api.resolve_channel_account(channel_identity=channel_identity())
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

    assert sent is True
    assert api.supersede_surface_requests[-1]["channel_surface_id"] == "surface-old"
    assert api.supersede_surface_requests[-1]["reason"] == "message_not_editable"
    assert app.status_message_ids[(10, 7)] == 9001


@pytest.mark.asyncio
async def test_restart_recovery_restores_materials_surface_and_resumes_active_run_watcher() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    gateway.add_text(channel_identity=channel_identity(), text="recover me")
    selection = gateway.create_selection_snapshot(channel_identity=channel_identity(), collection_id="inbox-1", expected_version=1)
    run = gateway.start_analysis(channel_identity=channel_identity(), selection_snapshot_id=selection["selection_snapshot_id"])
    account = api.resolve_channel_account(channel_identity=channel_identity())
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
async def test_restart_recovery_supersedes_unreachable_surface_and_starts_polling_for_healthy_surfaces(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stale_channel_identity = channel_identity()
    healthy_channel_identity = channel_identity(chat_id=20, user_id=8)
    edit_error = telegram_bad_request("editMessageText", "Bad Request: chat not found")
    send_error = telegram_bad_request("sendMessage", "Bad Request: chat not found")
    bot = FakeBot(
        edit_errors={(10, 5001): edit_error},
        send_message_errors={10: send_error},
    )
    api, gateway, app = make_app(bot=bot)
    gateway.add_text(channel_identity=stale_channel_identity, text="stale surface")
    gateway.add_text(channel_identity=healthy_channel_identity, text="healthy surface")
    stale_account = api.resolve_channel_account(channel_identity=stale_channel_identity)
    healthy_account = api.resolve_channel_account(channel_identity=healthy_channel_identity)
    api.upsert_channel_surface(
        channel_account_id=stale_account["channel_account_id"],
        surface_type="current_materials_panel",
        surface_key="current:chat:10:user:7",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "collection", "subject_id": "inbox-1", "subject_role": "primary"}],
    )
    api.upsert_channel_surface(
        channel_account_id=healthy_account["channel_account_id"],
        surface_type="current_materials_panel",
        surface_key="current:chat:20:user:8",
        address={"chat_id": 20, "message_id": 6001},
        address_fingerprint="telegram:20:6001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "collection", "subject_id": "inbox-1", "subject_role": "primary"}],
    )
    started: dict[str, Any] = {}

    async def fake_start_polling(started_bot: FakeBot) -> None:
        started["bot"] = started_bot

    app.dispatcher.start_polling = fake_start_polling  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING):
        await app.run()

    assert started["bot"] is bot
    assert api.supersede_surface_requests[-1]["channel_surface_id"] == "surface-1"
    assert api.supersede_surface_requests[-1]["reason"] == "telegram_address_unreachable"
    assert api.supersede_surface_requests[-1]["metadata"]["operation"] == "edit"
    assert api.supersede_surface_requests[-1]["metadata"]["chat_id"] == 10
    assert bot.send_message_calls == []
    assert bot.edit_calls[-1]["chat_id"] == 20
    assert bot.edit_calls[-1]["message_id"] == 6001
    assert "BLOCK_HANDLE_TELEGRAM_SURFACE_FAILURE" in caplog.text


@pytest.mark.asyncio
async def test_recover_current_materials_surface_replaces_missing_message() -> None:
    edit_error = telegram_bad_request("editMessageText", "Bad Request: message to edit not found")
    bot = FakeBot(edit_error=edit_error)
    api, gateway, app = make_app(bot=bot)
    gateway.add_text(channel_identity=channel_identity(), text="recover replacement")
    account = api.resolve_channel_account(channel_identity=channel_identity())
    surface = api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="current_materials_panel",
        surface_key="current:chat:10:user:7",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "collection", "subject_id": "inbox-1", "subject_role": "primary"}],
    )

    await app._recover_current_materials_surface(channel_identity=channel_identity(), surface=surface)

    assert api.supersede_surface_requests[-1]["reason"] == "telegram_message_unavailable"
    assert bot.send_message_calls[-1]["chat_id"] == 10
    assert app.status_message_ids[(10, 7)] == 9003
    active_surfaces = [item for item in api.channel_surfaces if item["lifecycle_status"] == "active"]
    assert active_surfaces[-1]["address"] == {"chat_id": 10, "message_id": 9003}


@pytest.mark.asyncio
async def test_recover_current_materials_surface_clears_status_when_replacement_send_fails() -> None:
    edit_error = telegram_bad_request("editMessageText", "Bad Request: message to edit not found")
    send_error = telegram_bad_request("sendMessage", "Bad Request: chat not found")
    bot = FakeBot(edit_error=edit_error, send_message_errors={10: send_error})
    api, gateway, app = make_app(bot=bot)
    gateway.add_text(channel_identity=channel_identity(), text="recover replacement failure")
    account = api.resolve_channel_account(channel_identity=channel_identity())
    surface = api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="current_materials_panel",
        surface_key="current:chat:10:user:7",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "collection", "subject_id": "inbox-1", "subject_role": "primary"}],
    )

    await app._recover_current_materials_surface(channel_identity=channel_identity(), surface=surface)

    assert api.supersede_surface_requests[-1]["reason"] == "telegram_message_unavailable"
    assert app.status_message_ids == {}
    assert bot.send_message_calls == []


@pytest.mark.asyncio
async def test_recover_active_surfaces_skips_invalid_accounts_and_handles_recover_errors() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    gateway.add_text(channel_identity=channel_identity(), text="recover error")
    good_account = api.resolve_channel_account(channel_identity=channel_identity())
    api.channel_accounts.extend(
        [
            {
                "channel_account_id": "skip-channel",
                "channel": "email",
                "external_account_ref": "chat:20:user:8",
                "display_name": "email",
                "status": "active",
                "metadata": {"adapter_identity": {"telegram_chat_id": "20", "telegram_user_id": "8"}},
            },
            {
                "channel_account_id": "skip-status",
                "channel": "telegram",
                "external_account_ref": "chat:21:user:8",
                "display_name": "inactive",
                "status": "disabled",
                "metadata": {"adapter_identity": {"telegram_chat_id": "21", "telegram_user_id": "8"}},
            },
            {
                "channel_account_id": "skip-channel_identity",
                "channel": "telegram",
                "external_account_ref": " ",
                "display_name": "missing channel_identity",
                "status": "active",
                "metadata": {},
            },
        ]
    )
    surface = api.upsert_channel_surface(
        channel_account_id=good_account["channel_account_id"],
        surface_type="current_materials_panel",
        surface_key="current:chat:10:user:7",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "collection", "subject_id": "inbox-1", "subject_role": "primary"}],
    )

    async def fail_recover_current_surface(**kwargs: Any) -> None:
        raise telegram_bad_request("editMessageText", "Bad Request: chat not found")

    app._recover_current_materials_surface = fail_recover_current_surface  # type: ignore[method-assign]

    await app._recover_active_channel_surfaces()

    assert api.supersede_surface_requests[-1]["channel_surface_id"] == surface["channel_surface_id"]
    assert api.supersede_surface_requests[-1]["reason"] == "telegram_address_unreachable"


@pytest.mark.asyncio
async def test_recover_current_materials_surface_ignores_missing_address_and_channel_identity_key() -> None:
    _, _, app = make_app(bot=FakeBot())

    await app._recover_current_materials_surface(channel_identity=channel_identity(), surface={"address": {}, "display_state": {"screen": "main"}})
    await app._recover_current_materials_surface(
        channel_identity={"channel": "telegram", "external_account_ref": "chat:10:user:7"},
        surface={"address": {"chat_id": 10, "message_id": 5001}, "display_state": {"screen": "main"}},
    )

    assert app.status_message_ids == {}
    assert app.bot.edit_calls == []


@pytest.mark.asyncio
async def test_recover_analysis_task_surface_ignores_missing_inputs_and_retires_terminal_runs() -> None:
    api, _, app = make_app(bot=FakeBot())
    await app._recover_analysis_task_surface(
        channel_identity=channel_identity(),
        surface={"address": {"chat_id": 10, "message_id": 5001}, "subjects": []},
    )
    api.runs.append(
        {
            "analysis_run_id": "run-done",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 1,
        }
    )
    account = api.resolve_channel_account(channel_identity=channel_identity())
    terminal_surface = api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="analysis_task_surface",
        surface_key="analysis_run:run-done",
        address={"chat_id": 10, "message_id": 5001},
        address_fingerprint="telegram:10:5001",
        display_state={"screen": "main"},
        subjects=[{"subject_type": "analysis_run", "subject_id": "run-done", "subject_role": "primary"}],
    )
    app.status_message_ids[(10, 7)] = 6001

    await app._recover_analysis_task_surface(
        channel_identity=channel_identity(),
        surface=terminal_surface,
    )

    assert app.run_watch_tasks == {}
    assert terminal_surface["lifecycle_status"] == "superseded"
    assert api.supersede_surface_requests[-1]["reason"] == "analysis_run_terminal"
    assert app.bot.delete_message_calls == [{"chat_id": 10, "message_id": 5001}]


def test_surface_persistence_helpers_cover_conflict_supersede_failure_and_missing_chat() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    gateway.add_text(channel_identity=channel_identity(), text="persist conflict")
    status = status_for(gateway)
    state = _PageState(screen="main")
    surface = {
        "channel_surface_id": "surface-old",
        "address": {"chat_id": 10, "message_id": 5001},
        "version": 2,
    }

    def fail_replace(**kwargs: Any) -> dict[str, Any]:
        raise TelegramApiClientError("/internal/v1/channel-surfaces/surface-old", 409, "conflict", code="version_conflict")

    gateway.replace_channel_surface_display_state = fail_replace  # type: ignore[method-assign]

    persisted = app._persist_current_materials_surface(
        channel_identity=channel_identity(),
        status=status,
        state=state,
        chat_id=10,
        message_id=5001,
        surface=surface,
    )

    assert persisted["surface_type"] == "current_materials_panel"
    assert persisted["address"] == {"chat_id": 10, "message_id": 5001}

    def fail_replace_with_backend_error(**kwargs: Any) -> dict[str, Any]:
        raise TelegramApiClientError("/internal/v1/channel-surfaces/surface-old", 500, "backend", code="backend_error")

    gateway.replace_channel_surface_display_state = fail_replace_with_backend_error  # type: ignore[method-assign]

    with pytest.raises(TelegramApiClientError):
        app._persist_current_materials_surface(
            channel_identity=channel_identity(),
            status=status,
            state=state,
            chat_id=10,
            message_id=5001,
            surface=surface,
        )

    def fail_supersede(**kwargs: Any) -> dict[str, Any]:
        raise TelegramApiClientError("/internal/v1/channel-surfaces/surface-old/events", 500, "boom", code="backend_error")

    gateway.supersede_channel_surface = fail_supersede  # type: ignore[method-assign]

    assert app._try_supersede_channel_surface(surface=surface, reason="test") is None

    with pytest.raises(RuntimeError, match="telegram_result_chat_missing"):
        app._persist_result_artifact_surface(
            channel_identity=channel_identity(),
            artifact={"artifact_id": "artifact-1"},
            chat_id=None,
            message_id=9001,
            delivery_mode="document",
        )


def test_telegram_surface_error_handler_reraises_fatal_errors() -> None:
    _, _, app = make_app(bot=FakeBot())

    with pytest.raises(TelegramUnauthorizedError):
        app._handle_telegram_surface_error(
            surface=None,
            error=TelegramUnauthorizedError(method=SimpleNamespace(__api_method__="sendMessage"), message="unauthorized"),
            operation="send",
            scope="fatal_surface",
        )


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
            "object_key": "run-1/transcript/plain/transcript.txt",
        }
    )
    account = api.resolve_channel_account(channel_identity=channel_identity())
    api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="result_artifact_surface",
        surface_key="artifact:artifact-1",
        address={"chat_id": 10, "message_id": 7001},
        address_fingerprint="telegram:10:7001",
        display_state={"delivery_mode": "text"},
        subjects=[{"subject_type": "artifact", "subject_id": "artifact-1", "subject_role": "primary"}],
    )

    delivery = await app._deliver_run_result(
        channel_identity=channel_identity(),
        analysis_run_id="run-1",
        expected_version=1,
        chat_id=10,
    )

    assert delivery.notice == "Транскрипт уже отправлен в чат."
    assert delivery.show_alert is True
    assert delivery.message_id is None
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
            "object_key": "run-1/transcript/plain/transcript.txt",
        }
    )
    api.internal_artifact_download_access["artifact-1"] = {
        "artifact_id": "artifact-1",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }
    account = api.resolve_channel_account(channel_identity=channel_identity())
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

    delivery = await app._deliver_run_result(
        channel_identity=channel_identity(),
        analysis_run_id="run-1",
        expected_version=1,
        chat_id=10,
    )

    active_surfaces = [
        surface
        for surface in api.channel_surfaces
        if surface["lifecycle_status"] == "active"
    ]
    assert delivery.notice == "Транскрипт отправлен файлом"
    assert delivery.show_alert is False
    assert delivery.message_id == 9004
    assert api.supersede_surface_requests[-1]["reason"] == "result_surface_missing_telegram_address"
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert app.bot.send_message_calls == []
    assert len(app.bot.send_document_calls) == 1
    assert app.bot.send_document_calls[0]["chat_id"] == 10
    assert app.bot.send_document_calls[0]["document"].filename == "transcript.txt"
    assert app.bot.send_document_calls[0]["document"].data == b"Recovered transcript."
    assert active_surfaces[-1]["surface_type"] == "result_artifact_surface"
    assert active_surfaces[-1]["address"] == {"chat_id": 10, "message_id": 9004}


@pytest.mark.asyncio
async def test_addressless_result_surface_failed_send_does_not_create_duplicate_or_clear_collection() -> None:
    send_error = telegram_bad_request("sendDocument", "Bad Request: chat not found")
    api, gateway, app = make_app(bot=FakeBot(send_document_errors={10: send_error}))
    gateway.add_text(channel_identity=channel_identity(), text="keep me until delivery succeeds")
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
            "object_key": "run-1/transcript/plain/transcript.txt",
        }
    )
    api.internal_artifact_download_access["artifact-1"] = {
        "artifact_id": "artifact-1",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
    }
    account = api.resolve_channel_account(channel_identity=channel_identity())
    api.upsert_channel_surface(
        channel_account_id=account["channel_account_id"],
        surface_type="result_artifact_surface",
        surface_key="artifact:artifact-1",
        address={},
        address_fingerprint="",
        display_state={"delivery_mode": "text"},
        subjects=[{"subject_type": "artifact", "subject_id": "artifact-1", "subject_role": "primary"}],
    )
    app._download_artifact_bytes = lambda _url: b"transcript that cannot be delivered"  # type: ignore[method-assign]

    delivery = await app._deliver_run_result(
        channel_identity=channel_identity(),
        analysis_run_id="run-1",
        expected_version=1,
        chat_id=10,
    )

    active_result_surfaces = [
        surface
        for surface in api.channel_surfaces
        if surface["lifecycle_status"] == "active" and surface["surface_type"] == "result_artifact_surface"
    ]
    assert delivery.notice == "Готовый транскрипт пока недоступен."
    assert delivery.show_alert is True
    assert delivery.message_id is None
    assert api.supersede_surface_requests[-1]["reason"] == "result_surface_missing_telegram_address"
    assert active_result_surfaces == []
    assert api.collection["items"] == [{"media_asset_id": "media-1", "position": 0}]
    assert api.remove_requests == []


@pytest.mark.asyncio
async def test_deliver_run_result_requires_destination_and_download_url() -> None:
    api, _, app = make_app(bot=FakeBot())

    delivery = await app._deliver_run_result(
        channel_identity=channel_identity(),
        analysis_run_id="run-1",
        expected_version=1,
    )

    assert delivery.notice == "Готовый транскрипт пока недоступен."
    assert delivery.show_alert is True
    assert delivery.message_id is None
    assert api.internal_artifact_download_access_requests == []

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
            "artifact_id": "artifact-no-url",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
            "object_key": "run-1/transcript/plain/transcript.txt",
        }
    )
    api.internal_artifact_download_access["artifact-no-url"] = {
        "artifact_id": "artifact-no-url",
        "filename": "transcript.txt",
        "mime_type": "text/plain",
        "download": {},
    }

    missing_url_delivery = await app._deliver_run_result(
        channel_identity=channel_identity(),
        analysis_run_id="run-1",
        expected_version=1,
        chat_id=10,
    )

    assert missing_url_delivery.notice == "Готовый транскрипт пока недоступен."
    assert missing_url_delivery.show_alert is True
    assert missing_url_delivery.message_id is None
    assert api.internal_artifact_download_access_requests == ["artifact-no-url"]
    assert app.bot.send_document_calls == []


@pytest.mark.asyncio
async def test_resolve_run_start_status_keeps_queued_prefix_when_run_stays_active() -> None:
    api, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="queued run")
    status = gateway.restore_status(channel_identity=channel_identity())
    selection = gateway.create_selection_snapshot(
        channel_identity=channel_identity(),
        collection_id=status.collection["collection_id"],
        expected_version=int(status.collection["version"]),
    )
    run = gateway.start_analysis(channel_identity=channel_identity(), selection_snapshot_id=selection["selection_snapshot_id"])

    async def no_sleep(_seconds: float) -> None:
        return None

    app._sleep = no_sleep  # type: ignore[assignment]
    status, prefix, answer_text, track_run_id, terminal_status = await app._resolve_run_start_status(
        channel_identity=channel_identity(),
        run=run,
    )

    assert api.runs[0]["status"] == "queued"
    assert answer_text == "Обработка запущена"
    assert prefix.startswith("Обработка запущена.")
    assert status.active_runs[0]["analysis_run_id"] == run["analysis_run_id"]
    assert track_run_id == run["analysis_run_id"]
    assert terminal_status is None


@pytest.mark.asyncio
async def test_resolve_run_start_status_returns_terminal_status_after_initial_poll() -> None:
    api, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="terminal run")
    status = gateway.restore_status(channel_identity=channel_identity())
    selection = gateway.create_selection_snapshot(
        channel_identity=channel_identity(),
        collection_id=status.collection["collection_id"],
        expected_version=int(status.collection["version"]),
    )
    run = gateway.start_analysis(channel_identity=channel_identity(), selection_snapshot_id=selection["selection_snapshot_id"])
    api.runs[0]["status"] = "succeeded"

    status, prefix, answer_text, track_run_id, terminal_status = await app._resolve_run_start_status(
        channel_identity=channel_identity(),
        run=run,
    )

    assert answer_text == "Обработка: успешно"
    assert prefix == "Обработка: успешно\n\n"
    assert status.recent_runs[0]["analysis_run_id"] == run["analysis_run_id"]
    assert track_run_id == run["analysis_run_id"]
    assert terminal_status == "succeeded"


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
    gateway.add_text(channel_identity=channel_identity(), text="one")
    gateway.add_text(channel_identity=channel_identity(), text="two")
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
    assert [button.text for button in main_keyboard.inline_keyboard[-1]] == ["Обработать (2)"]

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

    gateway.add_text(channel_identity=channel_identity(), text="three")
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
    assert base_message.edits[-1]["text"].startswith("Обработка\nМатериалов: 1")


@pytest.mark.asyncio
async def test_callback_materials_previous_page_and_clear_visible_rollback() -> None:
    api, gateway, app = make_app(page_size=1)
    gateway.add_text(channel_identity=channel_identity(), text="one")
    gateway.add_text(channel_identity=channel_identity(), text="two")
    base_message = FakeMessage()

    page_one_status = status_for(gateway)
    app._set_page_state((10, 7), page_one_status, current_cursor=None, previous_cursors=[], selection=None, screen="materials")
    next_callback = FakeCallback(data="ib:pn", message=base_message)
    await app._handle_status_callback(next_callback)

    assert next_callback.answers[-1]["text"] == "Открыта следующая страница"
    assert app.page_states[(10, 7)].current_cursor == "media-1"
    assert app.page_states[(10, 7)].previous_cursors == [None]

    previous_callback = FakeCallback(data="ib:pp", message=base_message)
    await app._handle_status_callback(previous_callback)

    assert previous_callback.answers[-1]["text"] == "Открыта предыдущая страница"
    assert app.page_states[(10, 7)].current_cursor is None
    assert app.page_states[(10, 7)].previous_cursors == []
    assert base_message.edits[-1]["text"].startswith("Материалы\nМатериалов: 2\n1. Текст: «one»")

    page_two_status = status_for(gateway, cursor="media-1")
    clear_keyboard = build_status_keyboard(page_two_status, can_go_back=True, current_cursor="media-1", screen="materials")
    clear_callback_data = next(
        button.callback_data
        for row in clear_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cl:")
    )
    app._set_page_state(
        (10, 7),
        page_two_status,
        current_cursor="media-1",
        previous_cursors=[None],
        selection=None,
        screen="materials",
    )

    clear_callback = FakeCallback(data=clear_callback_data, message=base_message)
    await app._handle_status_callback(clear_callback)

    assert clear_callback.answers[-1]["text"] == "Видимые материалы убраны"
    assert api.remove_requests[-1]["media_asset_id"] == "media-2"
    assert app.page_states[(10, 7)].current_cursor is None
    assert app.page_states[(10, 7)].previous_cursors == []
    assert base_message.edits[-1]["text"].startswith("Материалы\nМатериалов: 1\n1. Текст: «one»")


@pytest.mark.asyncio
async def test_refresh_callback_tolerates_message_not_modified() -> None:
    api, gateway, app = make_app(page_size=1)
    gateway.add_text(channel_identity=channel_identity(), text="one")
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

    gateway.add_text(channel_identity=channel_identity(), text="run item 2")
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

    def staged_run_status(*, channel_identity: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "succeeded")
        return original_get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)

    app._sleep = no_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 4
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]
    await app._handle_status_callback(run_callback)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert run_callback.answers[-1]["text"] == "Обработка запущена"
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
            "object_key": "run-1/transcript/plain/transcript.txt",
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

    assert artifacts_callback.answers[-1]["text"] == "Транскрипт отправлен файлом"
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert api.get_artifact_requests == []
    assert diagnostics_callback.answers[-1]["text"] == "Открыта диагностика"
    assert base_message.answers == []
    assert base_message.documents[-1]["document"].filename == "transcript.txt"
    assert base_message.documents[-1]["document"].data == b"Completed transcript."
    assert "run-1" not in base_message.edits[-2]["text"]
    assert "Диагностика" in base_message.edits[-1]["text"]
    assert "run-1" not in base_message.edits[-1]["text"]


@pytest.mark.asyncio
async def test_result_callback_sends_transcript_and_preserves_current_collection_after_success() -> None:
    api, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="one")
    gateway.add_text(channel_identity=channel_identity(), text="two")
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
            "object_key": "run-1/transcript/plain/transcript.txt",
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

    assert result_callback.answers[-1] == {"text": "Транскрипт отправлен файлом", "show_alert": False}
    assert base_message.answers == []
    assert base_message.documents[-1]["document"].filename == "transcript.txt"
    assert base_message.documents[-1]["document"].data == b"manual transcript"
    assert [item["media_asset_id"] for item in api.collection["items"]] == ["media-1", "media-2"]
    assert [item["media_asset_id"] for item in api.items] == ["media-1", "media-2"]
    assert api.remove_requests == []
    assert "Материалов: 2" in app.bot.send_message_calls[-1]["text"]
    assert app.status_message_ids[(10, 7)] == 9003
    assert app.bot.delete_message_calls == [{"chat_id": 10, "message_id": 101}]


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

    assert cancel_callback.answers[-1] == {"text": "Обработка отменена", "show_alert": False}
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
async def test_unfocused_active_run_can_be_canceled_while_new_processing_can_start() -> None:
    api, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="new independent material")
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
    assert "Обработать (1)" in button_texts

    app._set_page_state((10, 7), status, current_cursor=None, previous_cursors=[], selection=None, screen="main", focused_run_id=None)
    cancel_callback = FakeCallback(data=cancel_callback_data, message=base_message)
    await app._handle_status_callback(cancel_callback)

    assert cancel_callback.answers[-1] == {"text": "Обработка отменена", "show_alert": False}
    assert api.cancel_requests[-1]["analysis_run_id"] == "run-old"
    assert api.runs[0]["status"] == "canceled"

    refreshed_status = status_for(gateway)
    app._set_page_state((10, 7), refreshed_status, current_cursor=None, previous_cursors=[], selection=None, screen="main", focused_run_id=None)
    app.run_status_poll_attempts = 1
    run_callback = FakeCallback(data=run_callback_data, message=base_message)
    await app._handle_status_callback(run_callback)

    assert run_callback.answers[-1]["text"] == "Обработка запущена"
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
                "object_key": "run-1/transcript/docx/transcript.docx",
                "download": {"url": "https://download.test/transcript.docx"},
            },
            {
                "artifact_id": "artifact-plain",
                "analysis_run_id": "run-1",
                "kind": "transcript",
                "status": "available",
                "content_type": "text/plain; charset=utf-8",
                "object_key": "run-1/transcript/plain/transcript.txt",
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
    gateway.add_text(channel_identity=channel_identity(), text="one")
    gateway.add_text(channel_identity=channel_identity(), text="two")
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

    def staged_run_status(*, channel_identity: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "running")
        return original_get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]

    await app._handle_status_callback(run_callback)
    assert run_callback.answers[-1]["text"] == "Обработка запущена"

    materials_callback = FakeCallback(data="ib:mt", message=base_message)
    await app._handle_status_callback(materials_callback)

    assert materials_callback.answers[-1]["text"] == "Открыт список материалов"
    assert app.page_states[(10, 7)].screen == "materials"
    assert base_message.edits[-1]["text"].startswith("Материалы\nМатериалов: 0\nСписок пока пуст.")

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.page_states[(10, 7)].screen == "materials"
    assert app.bot.edit_calls[-1]["text"].startswith("Материалы\nМатериалов: 0\nСписок пока пуст.")


@pytest.mark.asyncio
async def test_collection_and_selection_snapshot_callbacks_start_terminal_runs() -> None:
    api, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="ready to finish")
    status = status_for(gateway)
    base_message = FakeMessage()
    original_start_analysis = gateway.start_analysis
    original_start_processing = api.start_collection_processing_run

    def terminal_start_analysis(**kwargs: Any) -> dict[str, Any]:
        run = original_start_analysis(**kwargs)
        api.runs[-1]["status"] = "succeeded"
        return run

    gateway.start_analysis = terminal_start_analysis  # type: ignore[method-assign]

    def terminal_start_processing(**kwargs: Any) -> dict[str, Any]:
        processing = original_start_processing(**kwargs)
        api.runs[-1]["status"] = "succeeded"
        processing["analysis_run"]["status"] = "succeeded"
        return processing

    api.start_collection_processing_run = terminal_start_processing  # type: ignore[method-assign]
    collection_id = str(status.collection["collection_id"])
    collection_version = int(status.collection["version"])
    app._set_page_state((10, 7), status, current_cursor=None, previous_cursors=[], selection=None, screen="main")

    collection_callback = FakeCallback(
        data=_callback_payload(
            "sl",
            _encode_callback_token(collection_id),
            _encode_callback_version(collection_version),
        ),
        message=base_message,
    )
    await app._handle_status_callback(collection_callback)

    assert collection_callback.answers[-1]["text"] == "Обработка: успешно"
    assert api.runs[-1]["selection_snapshot_id"] == "selection-1"
    assert app.run_watch_tasks == {}

    gateway.add_text(channel_identity=channel_identity(), text="legacy selection")
    legacy_status = status_for(gateway)
    selection = gateway.create_selection_snapshot(
        channel_identity=channel_identity(),
        collection_id=collection_id,
        expected_version=int(legacy_status.collection["version"]),
    )
    selection_callback = FakeCallback(
        data=_callback_payload("rn", _encode_callback_token(str(selection["selection_snapshot_id"]))),
        message=base_message,
    )
    app._set_page_state((10, 7), status_for(gateway), current_cursor=None, previous_cursors=[], selection=None, screen="main")
    await app._handle_status_callback(selection_callback)

    assert selection_callback.answers[-1]["text"] == "Обработка: успешно"
    assert api.runs[-1]["selection_snapshot_id"] == selection["selection_snapshot_id"]
    assert app.run_watch_tasks == {}


@pytest.mark.asyncio
async def test_duplicate_uploaded_media_defers_reuse_to_atomic_api_planning() -> None:
    api, gateway, app = make_app()
    media_asset = api.upload_media_asset(
        channel_account_id="channel-account-1",
        kind="video",
        content=b"same-video",
        file_name="clip.mp4",
        content_type="video/mp4",
        display_name="clip.mp4",
        metadata={"file_unique_id": "telegram-stable-file"},
    )
    media_asset["origin"]["stored_object_id"] = "stored-source-1"
    media_asset["origin"]["checksum"] = "sha256:source"
    api.runs.append(
        {
            "analysis_run_id": "run-reused",
            "selection_snapshot_id": "selection-reused",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 2,
        }
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-reused",
            "analysis_run_id": "run-reused",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain; charset=utf-8",
            "object_key": "run-reused/transcript/plain/transcript.txt",
            "download": {"url": "https://download.test/transcript.txt"},
        }
    )
    api.reusable_transcripts["stored-source-1"] = {
        "analysis_run_id": "run-reused",
        "analysis_run_version": 2,
        "artifact_id": "artifact-reused",
        "analysis_run": dict(api.runs[0]),
        "artifact": dict(api.artifacts[0]),
    }
    api.internal_artifact_download_access["artifact-reused"] = {
        "artifact_id": "artifact-reused",
        "filename": "transcript.txt",
        "mime_type": "text/plain; charset=utf-8",
        "download": {"url": "http://minio:9000/artifacts/run-reused/transcript.txt"},
    }
    api.channel_surfaces.append(
        {
            "channel_surface_id": "surface-old-result",
            "channel_account_id": "channel-account-1",
            "channel": "telegram",
            "surface_type": "result_artifact_surface",
            "surface_key": "artifact:artifact-reused",
            "address": {"chat_id": 10, "message_id": 700},
            "address_fingerprint": "telegram:10:700",
            "display_state": {"artifact_id": "artifact-reused"},
            "lifecycle_status": "active",
            "version": 1,
            "subjects": [{"subject_type": "artifact", "subject_id": "artifact-reused", "subject_role": "primary"}],
        }
    )
    status = status_for(gateway)
    base_message = FakeMessage()
    app._set_page_state((10, 7), status, current_cursor=None, previous_cursors=[], selection=None, screen="main")
    app._download_artifact_bytes = lambda _url: b"cached transcript"  # type: ignore[method-assign]

    callback = FakeCallback(
        data=_callback_payload(
            "rn",
            _encode_callback_token(str(status.collection["collection_id"])),
            _encode_callback_version(int(status.collection["version"])),
        ),
        message=base_message,
    )
    await app._handle_status_callback(callback)

    assert len(api.runs) == 2
    assert api.reusable_transcript_requests == []
    assert callback.answers[-1] == {"text": "Обработка запущена", "show_alert": False}
    assert base_message.documents == []
    assert api.internal_artifact_download_access_requests == []
    assert api.collection["items"] == []
    for task in app.run_watch_tasks.values():
        task.cancel()


@pytest.mark.asyncio
async def test_collection_callback_schedules_tracking_for_active_run() -> None:
    _, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="watch active run")
    status = status_for(gateway)
    base_message = FakeMessage()
    tick = asyncio.Event()

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    app._sleep = gated_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    app._set_page_state((10, 7), status, current_cursor=None, previous_cursors=[], selection=None, screen="main")

    callback = FakeCallback(
        data=_callback_payload(
            "sl",
            _encode_callback_token(str(status.collection["collection_id"])),
            _encode_callback_version(int(status.collection["version"])),
        ),
        message=base_message,
    )
    await app._handle_status_callback(callback)

    assert callback.answers[-1]["text"] == "Обработка запущена"
    assert (10, 7) in app.run_watch_tasks
    app._cancel_run_status_tracking((10, 7))
    tick.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_run_watcher_auto_delivers_transcript_file_and_hides_result_button_after_success() -> None:
    api, gateway, app = make_app(page_size=1, bot=FakeBot())
    gateway.add_text(channel_identity=channel_identity(), text="one")
    gateway.add_text(channel_identity=channel_identity(), text="two")
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

    def staged_run_status(*, channel_identity: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "succeeded")
        return original_get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)

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
            "object_key": "run-1/transcript/plain/transcript.txt",
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

    assert run_callback.answers[-1]["text"] == "Обработка запущена"

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.bot.outbound_call_order[:2] == ["document", "message"]
    assert len(app.bot.send_message_calls) == 1
    assert "Материалов: 0" in app.bot.send_message_calls[0]["text"]
    assert app.status_message_ids[(10, 7)] == 9003
    assert app.bot.delete_message_calls == [{"chat_id": 10, "message_id": 101}]
    assert len(app.bot.send_document_calls) == 1
    assert app.bot.send_document_calls[0]["chat_id"] == 10
    assert app.bot.send_document_calls[0]["document"].filename == "transcript.txt"
    assert app.bot.send_document_calls[0]["document"].data == b"transcript ready"
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert api.get_artifact_requests == []
    assert api.collection["items"] == []
    assert len(api.items) == 2
    assert api.remove_requests == []
    assert "Результат" not in [
        button.text
        for row in app.bot.send_message_calls[-1]["reply_markup"].inline_keyboard
        for button in row
    ]
    task_surface = next(surface for surface in api.channel_surfaces if surface["surface_type"] == "analysis_task_surface")
    assert task_surface["lifecycle_status"] == "superseded"
    assert api.supersede_surface_requests[-1]["reason"] == "analysis_run_terminal"


@pytest.mark.asyncio
async def test_run_watcher_continues_after_status_edit_retry_after_and_delivers_result() -> None:
    class RetryAfterOnceBot(FakeBot):
        def __init__(self) -> None:
            super().__init__()
            self._raised_retry_after = False

        async def edit_message_text(
            self,
            text: str,
            *,
            chat_id: int,
            message_id: int,
            reply_markup: Any,
            **kwargs: Any,
        ) -> None:
            if not self._raised_retry_after:
                self._raised_retry_after = True
                raise telegram_retry_after("editMessageText", 132)
            await super().edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=reply_markup,
                **kwargs,
            )

    api, gateway, app = make_app(page_size=1, bot=RetryAfterOnceBot())
    gateway.add_text(channel_identity=channel_identity(), text="one")
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
    statuses = iter(("queued", "queued", "succeeded"))

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    def staged_run_status(*, channel_identity: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "succeeded")
        return original_get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app._download_artifact_bytes = lambda _url: b"transcript after retry-after"  # type: ignore[method-assign]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 3
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-1",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
            "object_key": "run-1/transcript/plain/transcript.txt",
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

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert len(app.bot.send_document_calls) == 1
    assert app.bot.send_document_calls[0]["document"].data == b"transcript after retry-after"
    assert api.internal_artifact_download_access_requests == ["artifact-1"]
    assert len(app.bot.send_message_calls) == 1
    assert "Материалов: 0" in app.bot.send_message_calls[0]["text"]
    assert app.bot.delete_message_calls == [{"chat_id": 10, "message_id": 101}]


@pytest.mark.asyncio
async def test_run_watcher_supersedes_task_surface_when_auto_delivery_chat_is_unreachable() -> None:
    send_error = telegram_forbidden("sendDocument", "Forbidden: bot was blocked by the user")
    api, gateway, app = make_app(page_size=1, bot=FakeBot(send_document_errors={10: send_error}))
    gateway.add_text(channel_identity=channel_identity(), text="one")
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

    def staged_run_status(*, channel_identity: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "succeeded")
        return original_get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app._download_artifact_bytes = lambda _url: b"transcript cannot be delivered"  # type: ignore[method-assign]
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
            "object_key": "run-1/transcript/plain/transcript.txt",
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

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    superseded_surface_id = api.supersede_surface_requests[-1]["channel_surface_id"]
    superseded_surface = next(surface for surface in api.channel_surfaces if surface["channel_surface_id"] == superseded_surface_id)
    active_result_surfaces = [
        surface
        for surface in api.channel_surfaces
        if surface["lifecycle_status"] == "active" and surface["surface_type"] == "result_artifact_surface"
    ]
    assert app.run_watch_tasks == {}
    assert superseded_surface["surface_type"] == "analysis_task_surface"
    assert api.supersede_surface_requests[-1]["reason"] == "telegram_address_unreachable"
    assert api.supersede_surface_requests[-1]["metadata"]["operation"] == "send_document"
    assert active_result_surfaces == []
    assert api.collection["items"] == []
    assert api.remove_requests == []


@pytest.mark.asyncio
async def test_run_watcher_failed_run_preserves_local_inbox() -> None:
    api, gateway, app = make_app(bot=FakeBot())
    gateway.add_text(channel_identity=channel_identity(), text="one")
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

    def staged_run_status(*, channel_identity: dict[str, Any], analysis_run_id: str) -> dict[str, Any]:
        api.runs[0]["status"] = next(statuses, "failed")
        return original_get_run_status(channel_identity=channel_identity, analysis_run_id=analysis_run_id)

    app._sleep = gated_sleep  # type: ignore[assignment]
    app.run_status_poll_attempts = 1
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    gateway.get_run_status = staged_run_status  # type: ignore[method-assign]

    run_callback = FakeCallback(data=run_callback_data, message=base_message)
    await app._handle_status_callback(run_callback)

    assert run_callback.answers[-1]["text"] == "Обработка запущена"

    tick.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert app.run_watch_tasks == {}
    assert app.bot.send_message_calls == []
    assert api.collection["items"] == []
    assert api.remove_requests == []
    assert "Материалов: 0" in app.bot.edit_calls[-1]["text"]


@pytest.mark.asyncio
async def test_run_watcher_replaces_existing_task_and_logs_unexpected_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _, gateway, app = make_app(bot=FakeBot())
    tick = asyncio.Event()
    key = (10, 7)

    async def gated_sleep(_seconds: float) -> None:
        await tick.wait()

    app._sleep = gated_sleep  # type: ignore[assignment]
    app.run_status_follow_attempts = 1
    app.run_status_follow_delay_seconds = 0
    app._schedule_run_status_tracking(
        key=key,
        channel_identity=channel_identity(),
        analysis_run_id="run-old",
        chat_id=10,
        message_id=5001,
    )
    first_task = app.run_watch_tasks[key]

    app._schedule_run_status_tracking(
        key=key,
        channel_identity=channel_identity(),
        analysis_run_id="run-new",
        chat_id=10,
        message_id=5001,
    )

    await asyncio.sleep(0)
    assert first_task.cancelled()
    app._cancel_run_status_tracking(key)
    tick.set()
    await asyncio.sleep(0)

    async def no_sleep(_seconds: float) -> None:
        return None

    def fail_get_run_status(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("run polling exploded")

    app._sleep = no_sleep  # type: ignore[assignment]
    gateway.get_run_status = fail_get_run_status  # type: ignore[method-assign]
    with caplog.at_level(logging.WARNING):
        task = asyncio.create_task(
            app._track_run_status_until_terminal(
                key=key,
                channel_identity=channel_identity(),
                analysis_run_id="run-new",
                chat_id=10,
                message_id=5001,
            )
        )
        app.run_watch_tasks[key] = task
        await task

    assert key not in app.run_watch_tasks
    assert "run status tracking failed for run-new" in caplog.text


@pytest.mark.asyncio
async def test_callback_error_paths_cover_stale_unknown_and_normalized_failures() -> None:
    _, gateway, app = make_app()
    gateway.add_text(channel_identity=channel_identity(), text="one")
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

    stale_result_callback = FakeCallback(
        data=_callback_payload("ar", _encode_callback_token("run-missing"), _encode_callback_version(1)),
        message=message,
    )
    await app._handle_status_callback(stale_result_callback)
    assert stale_result_callback.answers[-1]["show_alert"] is True

    app.page_states.pop((10, 7), None)
    stale_cancel_without_state = FakeCallback(
        data=_callback_payload("cn", _encode_callback_token("run-missing"), _encode_callback_version(1)),
        message=message,
    )
    await app._handle_status_callback(stale_cancel_without_state)
    assert stale_cancel_without_state.answers[-1]["show_alert"] is True

    app._set_page_state((10, 7), status_for(gateway), current_cursor=None, previous_cursors=[], selection=None, screen="main")
    stale_cancel_without_active_run = FakeCallback(
        data=_callback_payload("cn", _encode_callback_token("run-missing"), _encode_callback_version(1)),
        message=message,
    )
    await app._handle_status_callback(stale_cancel_without_active_run)
    assert stale_cancel_without_active_run.answers[-1]["show_alert"] is True

    stale_diagnostics_callback = FakeCallback(
        data=_callback_payload("dg", _encode_callback_token("run-missing"), _encode_callback_version(1)),
        message=message,
    )
    await app._handle_status_callback(stale_diagnostics_callback)
    assert stale_diagnostics_callback.answers[-1]["show_alert"] is True

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


@pytest.mark.asyncio
async def test_edit_callback_status_reraises_real_bad_request() -> None:
    _, gateway, app = make_app()
    message = FakeMessage()

    async def raise_bad_request(text: str, **kwargs: Any) -> None:
        raise telegram_bad_request("editMessageText", "Bad Request: invalid reply markup")

    message.edit_text = raise_bad_request  # type: ignore[method-assign]

    with pytest.raises(TelegramBadRequest):
        await app._edit_callback_status(FakeCallback(data="ib:rf", message=message), status_for(gateway))


@pytest.mark.asyncio
async def test_post_ingest_refresh_failure_mentions_plural_saved_items_and_rejections() -> None:
    _, _, app = make_app()
    message = FakeMessage()

    await app._answer_post_ingest_refresh_failure(
        message,
        [
            IngressRecord(status="accepted", label="one"),
            IngressRecord(status="accepted", label="two"),
            IngressRecord(status="rejected", label="bad", reason="unsupported_message"),
        ],
    )

    assert "Материалы сохранены в inbox на сервере: 2." in message.answers[-1]["text"]
    assert "Отклонено: bad" in message.answers[-1]["text"]


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


def test_bot_display_surface_and_artifact_helpers_cover_edge_branches() -> None:
    status = InboxStatus(
        channel_identity=channel_identity(),
        collection={"collection_id": "inbox-1", "version": 2},
        items=[],
        page={},
        active_runs=[{"analysis_run_id": "run-active", "status": "running"}],
        recent_runs=[
            {"analysis_run_id": "run-active", "status": "running", "version": 1},
            {"analysis_run_id": "run-terminal", "status": "succeeded", "version": 3},
            {"analysis_run_id": "run-empty-version", "status": "succeeded", "version": 0},
        ],
        artifacts_by_run={"run-terminal": [{"artifact_id": "artifact-1"}]},
        diagnostics_by_run={},
        rejected=[],
    )
    state = _PageState(
        current_cursor="cursor-1",
        previous_cursors=["cursor-0"],
        next_cursor="cursor-2",
        screen="materials",
        focused_run_id="run-active",
    )

    assert _latest_active_run(status)["analysis_run_id"] == "run-active"
    assert _active_run_for_focus(status, "run-active")["status"] == "running"
    assert _active_run_for_focus(status, None) is None
    assert _terminal_run_with_payload(status, {"run-terminal": [{"artifact_id": "artifact-1"}]}, "run-terminal")[
        "analysis_run_id"
    ] == "run-terminal"
    assert _terminal_run_with_payload(status, {"run-terminal": []}, "run-terminal") is None
    assert _terminal_run_with_payload(status, {"run-terminal": [{"artifact_id": "artifact-1"}]}, None) is None
    assert _analysis_run_version(status, "run-terminal") == 3
    assert _analysis_run_version(status, "run-empty-version") is None
    assert _analysis_run_version(status, "missing") is None
    assert _run_for_id(status, "run-active")["status"] == "running"
    assert _run_for_id(status, "missing") is None
    assert _status_surface_display_state(status, state)["active_run_ids"] == ["run-active"]
    assert _run_surface_display_state({"analysis_run_id": "run-2", "status": "queued", "version": 1}, _PageState())[
        "focused_run_id"
    ] == "run-2"

    surface = {
        "address": {"chat_id": "10", "message_id": "42"},
        "display_state": {
            "screen": "materials",
            "current_cursor": "cursor-1",
            "previous_cursors": ["cursor-0"],
            "next_cursor": "cursor-2",
        },
        "subjects": [
            "bad-subject",
            {"subject_type": "analysis_run", "subject_role": "primary", "subject_id": "run-active"},
        ],
    }
    assert _telegram_surface_address(chat_id=10, message_id=42) == {"chat_id": 10, "message_id": 42}
    assert _surface_message_id(surface) == 42
    assert _surface_address(surface) == (10, 42)
    assert _surface_address({"address": []}) is None
    assert _surface_address({"address": {"chat_id": "bad", "message_id": "42"}}) is None
    assert _surface_address_matches(surface, chat_id=10, message_id=42) is True
    assert _surface_display_state({"display_state": []}) == {}
    restored_state = _page_state_from_display_state(_surface_display_state(surface), focused_run_id="run-active")
    assert restored_state.previous_cursors == ["cursor-0"]
    assert restored_state.focused_run_id == "run-active"
    assert _surface_subject_id(surface, subject_type="analysis_run", role="primary") == "run-active"
    assert _surface_subject_id({"subjects": "bad"}, subject_type="analysis_run", role="primary") is None
    assert _surface_subject_id({"subjects": [{"subject_type": "artifact"}]}, subject_type="analysis_run", role="primary") is None

    channel_identity_from_metadata = _channel_identity_from_channel_account({"metadata": {"channel_identity": channel_identity()}})
    channel_identity_from_external_ref = _channel_identity_from_channel_account(
        {
            "external_account_ref": "chat:10:user:7",
            "metadata": {"adapter_identity": {"telegram_chat_id": "10", "telegram_user_id": "7"}},
        }
    )
    assert channel_identity_from_metadata == channel_identity()
    assert channel_identity_from_external_ref == channel_identity()
    assert _channel_identity_from_channel_account({"external_account_ref": " "}) is None
    assert _state_key_from_channel_identity({}) is None
    assert _state_key_from_channel_identity({"adapter_identity": {"telegram_chat_id": "bad"}}) is None
    assert _state_key_from_channel_identity(channel_identity()) == (10, 7)

    visible_lines = _visible_item_lines(
        [
            {
                "media_asset_id": "media-1",
                "kind": "text",
                "status": "ready",
                "display_name": "Telegram media",
                "metadata": {"message_id": 100},
            },
            {
                "media_asset_id": "media-2",
                "kind": "photo",
                "status": "validating",
                "display_name": "Telegram photo",
                "metadata": {"media_group_id": "album-1"},
            },
            {
                "media_asset_id": "media-3",
                "kind": "video",
                "status": "deleted",
                "display_name": "Telegram video",
                "metadata": {"media_group_id": "album-1"},
            },
        ]
    )
    assert visible_lines[0].startswith("1. Медиа из Telegram")
    assert visible_lines[1] == "Альбом album-1 (2 шт.)"
    assert _item_label({"media_asset_id": "media-4", "kind": "custom", "status": "custom"}) == "media-4 [custom, custom]"
    assert _kind_text("text") == "текст"
    assert _media_status_text("ready") == "готов"
    assert _display_name_text("Telegram voice") == "Голосовое из Telegram"
    assert "· готовится" in _artifact_label({"artifact_id": "artifact-pending", "kind": "transcript", "status": "pending"})
    assert "Активные задачи: 2" in render_status_text(
        InboxStatus(
            channel_identity=channel_identity(),
            collection={"collection_id": "inbox-1", "version": 2, "items": []},
            items=[],
            page={},
            active_runs=[
                {"analysis_run_id": "run-1", "status": "queued"},
                {"analysis_run_id": "run-2", "status": "running"},
            ],
            recent_runs=[],
            artifacts_by_run={},
            diagnostics_by_run={},
            rejected=[],
        )
    )

    artifacts = [
        {"artifact_id": "skip-kind", "kind": "report", "status": "available", "content_type": "text/plain"},
        {"artifact_id": "skip-status", "kind": "transcript", "status": "pending", "content_type": "text/plain"},
        {"artifact_id": " ", "kind": "transcript", "status": "available", "content_type": "text/plain"},
        {"artifact_id": "markdown", "kind": "transcript", "status": "ready", "content_type": "text/markdown"},
        {"artifact_id": "plain", "kind": "transcript", "status": "available", "content_type": "text/plain; charset=utf-8"},
    ]
    assert _select_transcript_artifact(artifacts)["artifact_id"] == "plain"
    assert _select_transcript_artifact([]) is None
    assert _transcript_artifact_rank("text/markdown") == 1
    assert _transcript_artifact_rank(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ) == 2
    assert _transcript_artifact_rank("text/html") == 3
    assert _transcript_artifact_rank("application/json") == 4
    assert _artifact_download_url({}) is None
    assert _artifact_download_url({"download": {"url": " https://example.test/a.txt "}}) == "https://example.test/a.txt"
    assert _artifact_filename({"filename": "nested/transcript.custom"}) == "transcript.custom"
    assert _artifact_filename({"object_key": "objects/transcript.object"}) == "transcript.object"
    assert _artifact_filename({"download": {"url": "https://example.test/files/transcript%20url.md"}}) == "transcript url.md"
    assert _artifact_filename({"content_type": "text/plain"}) == "transcript.txt"
    assert _artifact_filename({"content_type": "text/markdown"}) == "transcript.md"
    assert _artifact_filename(
        {"content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
    ) == "transcript.docx"
    assert _artifact_filename({"content_type": "application/octet-stream"}) == "transcript.bin"

    assert _classify_telegram_surface_error(telegram_bad_request("editMessageText", "message is not modified")).classification == "telegram_message_not_modified"
    assert _classify_telegram_surface_error(telegram_bad_request("editMessageText", "chat not found")).lifecycle_reason == "telegram_address_unreachable"
    assert _classify_telegram_surface_error(telegram_bad_request("editMessageText", "message to edit not found")).lifecycle_reason == "telegram_message_unavailable"
    assert _classify_telegram_surface_error(telegram_bad_request("editMessageText", "bad request")).fatal is True
    assert _classify_telegram_surface_error(telegram_forbidden("sendMessage", "bot was blocked")).lifecycle_reason == "telegram_address_unreachable"
    assert _classify_telegram_surface_error(
        TelegramNetworkError(method=SimpleNamespace(__api_method__="sendMessage"), message="timeout")
    ).classification == "transient_telegram_delivery_error"
    assert _classify_telegram_surface_error(
        TelegramUnauthorizedError(method=SimpleNamespace(__api_method__="sendMessage"), message="unauthorized")
    ).fatal is True
    assert _classify_telegram_surface_error(
        TelegramAPIError(method=SimpleNamespace(__api_method__="sendMessage"), message="unknown")
    ).fatal is True


def test_run_module_executes_script_exit_path(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_asyncio_run(coro: Any) -> None:
        coro.close()

    monkeypatch.setattr(asyncio, "run", fake_asyncio_run)
    monkeypatch.delitem(sys.modules, "telegram_adapter.__main__", raising=False)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
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


def test_render_status_text_shows_active_run_progress_without_provider_terms() -> None:
    status = InboxStatus(
        channel_identity=channel_identity(),
        collection={"collection_id": "inbox-1", "version": 2},
        items=[
            {
                "media_asset_id": "media-voice",
                "kind": "voice",
                "display_name": "telegram-voice.ogg",
                "metadata": {"duration_seconds": 966},
            }
        ],
        page={},
        active_runs=[
            {
                "analysis_run_id": "run-active",
                "status": "running",
                "version": 2,
                "created_at": "2026-05-20T10:00:00Z",
                "started_at": "2026-05-20T10:00:30Z",
                "latest_event": {
                    "event_type": "analysis_run_step.progress",
                    "created_at": "2026-05-20T10:01:00Z",
                    "payload": {
                        "progress_stage": "transcribing",
                        "progress_message": "Running transcription pipeline",
                        "payload": {"vad_s": 2.92, "asr_inference_s": 53.29},
                    },
                },
            }
        ],
        recent_runs=[],
        artifacts_by_run={},
        diagnostics_by_run={},
        rejected=[],
    )

    text = render_status_text(status)

    assert "telegram-voice.ogg · 16:06" in text
    assert "Активная задача: в работе" in text
    assert "Этап: транскрибируем аудио" in text
    assert "Прошло:" in text
    assert "transcribing" not in text
    assert "Running transcription pipeline" not in text
    assert "vad" not in text.lower()
    assert "asr" not in text.lower()


def test_gateway_enriches_active_runs_with_latest_progress_event() -> None:
    api, gateway, _ = make_app()
    api.runs.append(
        {
            "analysis_run_id": "run-active",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "running",
            "version": 2,
        }
    )
    api.run_events["run-active"] = [
        {
            "analysis_run_event_id": "event-created",
            "analysis_run_id": "run-active",
            "event_type": "analysis_run.created",
            "status": "queued",
            "payload": {},
            "created_at": "2026-05-20T10:00:00Z",
        },
        {
            "analysis_run_event_id": "event-progress",
            "analysis_run_id": "run-active",
            "event_type": "analysis_run_step.progress",
            "status": "running",
            "payload": {"progress_stage": "materializing_sources"},
            "created_at": "2026-05-20T10:00:03Z",
        },
    ]

    status = status_for(gateway)

    assert status.active_runs[0]["latest_event"]["analysis_run_event_id"] == "event-progress"
    assert status.recent_runs[0]["latest_event"]["payload"]["progress_stage"] == "materializing_sources"


def test_download_artifact_bytes_reads_content_and_rejects_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, app = make_app()

    class FakeResponse:
        def __init__(self, content: bytes) -> None:
            self.content = content

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def read(self) -> bytes:
            return self.content

    monkeypatch.setattr("telegram_adapter.bot.urlopen", lambda _url, timeout: FakeResponse(b"artifact bytes"))

    assert app._download_artifact_bytes("http://download.test/transcript.txt") == b"artifact bytes"

    monkeypatch.setattr("telegram_adapter.bot.urlopen", lambda _url, timeout: FakeResponse(b""))

    with pytest.raises(RuntimeError, match="artifact_download_failed"):
        app._download_artifact_bytes("http://download.test/empty.txt")


def test_download_artifact_file_streams_to_anonymous_disk_and_fences_size(monkeypatch: pytest.MonkeyPatch) -> None:
    class StreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.chunks = iter(chunks)

        def __enter__(self):
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return next(self.chunks, b"")

    _, _, app = make_app()
    monkeypatch.setattr("telegram_adapter.bot.urlopen", lambda _url, timeout: StreamingResponse([b"large-", b"export"]))
    handle = app._download_artifact_file("http://minio/export", 12)
    try:
        assert handle.read() == b"large-export"
        assert not hasattr(handle, "name") or isinstance(handle.name, int)
    finally:
        handle.close()

    monkeypatch.setattr("telegram_adapter.bot.urlopen", lambda _url, timeout: StreamingResponse([b"too-large"]))
    with pytest.raises(RuntimeError, match="artifact_download_size_mismatch"):
        app._download_artifact_file("http://minio/export", 3)
