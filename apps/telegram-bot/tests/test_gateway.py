# FILE: apps/telegram-bot/tests/test_gateway.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove Telegram input and controls use inbox, selection, and analysis_run semantics only.
# SCOPE: Text, photo, video, document, link, media group, removal, run start, rejected records, and restore behavior.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from telegram_adapter.bot import TelegramInboxApp, build_status_keyboard, render_status_text
from telegram_adapter.errors import TelegramUserError, TelegramUserErrorCode, safe_callback_answer, user_error_text
from telegram_adapter.gateway import TelegramFileInput, TelegramInboxGateway


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


def owner() -> dict[str, Any]:
    return {
        "owner_type": "telegram",
        "owner_id": "chat:10:user:7",
        "adapter_identity": {"telegram_chat_id": "10", "telegram_user_id": "7"},
    }


def test_text_and_link_messages_become_inbox_media_items() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)

    records = gateway.add_message_inputs(owner=owner(), text="Meeting notes https://example.com/a", message_id=42)

    assert [record.status for record in records] == ["accepted", "accepted"]
    assert [request["kind"] for request in api.add_requests] == ["url", "text"]
    assert api.add_requests[0]["source"] == {"origin_type": "url", "url": "https://example.com/a"}
    assert api.add_requests[1]["source"] == {"origin_type": "text", "text": "Meeting notes"}
    assert api.add_requests[0]["metadata"]["message_id"] == 42


def test_private_chat_scope_is_deterministic_and_groups_are_not_supported() -> None:
    gateway = TelegramInboxGateway(FakeFinalApiClient())

    private_scope = gateway.scope_for(chat_id=10, user_id=7, chat_type="private")

    assert private_scope.visibility == "private"
    assert private_scope.state_key == (10, 7)
    assert private_scope.owner == {
        "owner_type": "telegram",
        "owner_id": "chat:10:user:7",
        "adapter_identity": {
            "telegram_chat_id": "10",
            "telegram_user_id": "7",
            "telegram_chat_type": "private",
        },
    }

    with pytest.raises(TelegramUserError) as group_error:
        gateway.scope_for(chat_id=-100, user_id=7, chat_type="supergroup")
    with pytest.raises(TelegramUserError) as topic_error:
        gateway.scope_for(chat_id=-100, user_id=7, chat_type="supergroup", message_thread_id=42)

    assert group_error.value.code == TelegramUserErrorCode.GROUP_NOT_SUPPORTED
    assert topic_error.value.code == TelegramUserErrorCode.GROUP_NOT_SUPPORTED
    assert "private-chat only" in user_error_text(group_error.value)


def test_mixed_inputs_preserve_supported_and_unsupported_urls_with_files() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    files = [
        TelegramFileInput(kind="photo", file_id="photo-file", file_unique_id="photo-u", size_bytes=10, message_id=43),
        TelegramFileInput(
            kind="document",
            file_id="doc-file",
            file_name="generic.bin",
            content_type="application/octet-stream",
            size_bytes=20,
            message_id=43,
        ),
    ]

    records = gateway.add_message_inputs(
        owner=owner(),
        text="Keep this https://ok.example/a ftp://bad.example/file",
        files=files,
        message_id=43,
    )

    assert [record.status for record in records] == ["accepted", "rejected", "accepted", "accepted", "accepted"]
    assert records[1].label == "ftp://bad.example/file"
    assert records[1].reason == "unsupported_url_scheme"
    assert [request["kind"] for request in api.add_requests] == ["url", "text", "photo", "document"]
    assert api.add_requests[1]["source"] == {"origin_type": "text", "text": "Keep this"}
    assert api.add_requests[3]["source"]["content_type"] == "application/octet-stream"


def test_photo_video_document_and_media_group_inputs_keep_telegram_metadata() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    files = [
        TelegramFileInput(kind="photo", file_id="photo-file", file_unique_id="photo-u", size_bytes=10, media_group_id="grp", message_id=1),
        TelegramFileInput(kind="video", file_id="video-file", file_name="clip.mp4", content_type="video/mp4", size_bytes=20, media_group_id="grp", message_id=2),
        TelegramFileInput(kind="document", file_id="doc-file", file_name="brief.pdf", content_type="application/pdf", size_bytes=30, media_group_id="grp", message_id=3),
    ]

    records = gateway.add_message_inputs(owner=owner(), files=files)

    assert [record.status for record in records] == ["accepted", "accepted", "accepted"]
    assert [request["kind"] for request in api.add_requests] == ["photo", "video", "document"]
    assert api.add_requests[0]["source"]["object_ref"] == "telegram://file/photo-file"
    assert api.add_requests[1]["source"]["original_filename"] == "clip.mp4"
    assert api.add_requests[2]["source"]["content_type"] == "application/pdf"
    assert all(request["metadata"]["media_group_id"] == "grp" for request in api.add_requests)


def test_album_status_preview_groups_visible_media_together() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    files = [
        TelegramFileInput(kind="photo", file_id="photo-file", file_unique_id="photo-u", size_bytes=10, media_group_id="grp", message_id=1),
        TelegramFileInput(kind="video", file_id="video-file", file_name="clip.mp4", content_type="video/mp4", size_bytes=20, media_group_id="grp", message_id=2),
        TelegramFileInput(kind="document", file_id="doc-file", file_name="brief.pdf", content_type="application/pdf", size_bytes=30, media_group_id="grp", message_id=3),
    ]
    gateway.add_message_inputs(owner=owner(), files=files)

    text = render_status_text(gateway.restore_status(owner=owner()))

    assert "Album grp (3 items)" in text
    assert "1. Telegram photo [photo, ready, message 1]" in text
    assert "2. clip.mp4 [video, ready, message 2]" in text
    assert "3. brief.pdf [document, ready, message 3]" in text


def test_invalid_or_empty_messages_return_explicit_rejected_records() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)

    records = gateway.add_message_inputs(owner=owner())
    missing_file = gateway.add_message_inputs(owner=owner(), files=[TelegramFileInput(kind="photo", file_id="")])

    assert records[0].status == "rejected"
    assert records[0].reason == "unsupported_message"
    assert missing_file[0].status == "rejected"
    assert missing_file[0].reason == "missing_file_id"
    assert api.add_requests == []

    text = render_status_text(gateway.restore_status(owner=owner(), rejected=[records[0], missing_file[0]]))
    assert "Rejected: Telegram message (unsupported input:" in text
    assert "Rejected: photo (unsupported input:" in text


def test_status_surface_supports_refresh_paging_slot_removal_clear_and_start_analysis() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=1)
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status, can_go_back=True)
    text = render_status_text(status)
    updated = gateway.remove_visible_slot(owner=owner(), slot=1)
    result = gateway.start_analysis(owner=owner())

    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert "Inbox" in text
    assert "inbox:refresh" in callbacks
    assert "inbox:page:prev" in callbacks
    assert "inbox:page:next" in callbacks
    assert "inbox:remove:1" in callbacks
    assert "inbox:clear" in callbacks
    assert updated.collection is not None
    assert updated.collection["version"] == 2
    assert result.selection["items"] == [{"media_item_id": "media-2", "position": 0}]
    assert result.analysis_run["status"] == "queued"


def test_large_inbox_uses_compact_callbacks_and_clears_only_visible_page() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=5)
    for index in range(12):
        gateway.add_text(owner=owner(), text=f"item {index + 1}")

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status)
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    after_slot_remove = gateway.remove_visible_slot(owner=owner(), slot=2)
    after_clear = gateway.clear_visible_items(owner=owner(), cursor="5")

    assert callbacks[:6] == ["inbox:refresh", "inbox:remove:1", "inbox:remove:2", "inbox:remove:3", "inbox:remove:4", "inbox:remove:5"]
    assert "inbox:page:next" in callbacks
    assert "inbox:clear" in callbacks
    assert max(len(callback) for callback in callbacks) <= 64
    assert all(not callback.startswith("inbox:remove:media-") for callback in callbacks)
    assert [item["media_item_id"] for item in after_slot_remove.collection["items"][:5]] == [
        "media-1",
        "media-3",
        "media-4",
        "media-5",
        "media-6",
    ]
    assert [request["media_item_id"] for request in api.remove_requests[-5:]] == [
        "media-7",
        "media-8",
        "media-9",
        "media-10",
        "media-11",
    ]
    assert [item["media_item_id"] for item in after_clear.collection["items"]] == [
        "media-1",
        "media-3",
        "media-4",
        "media-5",
        "media-6",
        "media-12",
    ]


def test_stale_callback_copy_is_safe_and_actionable() -> None:
    answer = safe_callback_answer(RuntimeError("slot_not_visible"))

    assert answer == {
        "text": "This button is stale. Open /inbox and try again.",
        "show_alert": True,
    }
    assert "slot_not_visible" not in answer["text"]


def test_long_running_run_is_restored_and_later_completion_is_visible_after_restart() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="long transcript")
    result = gateway.start_analysis(owner=owner())

    restarted_gateway = TelegramInboxGateway(api)
    restored = restarted_gateway.restore_status(owner=owner())
    assert restored.active_runs == [result.analysis_run]

    api.runs[0]["status"] = "succeeded"
    completed = restarted_gateway.restore_status(owner=owner())
    run = restarted_gateway.get_run_status(owner=owner(), analysis_run_id=result.analysis_run["analysis_run_id"])

    assert completed.active_runs == []
    assert run["status"] == "succeeded"


def test_long_running_run_does_not_false_fail_and_terminal_result_is_rendered_later() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="long transcript")
    result = gateway.start_analysis(owner=owner())

    queued_text = render_status_text(gateway.restore_status(owner=owner()))

    assert "run-1: queued" in queued_text
    assert "failed" not in queued_text
    assert "available later" in queued_text

    api.runs[0]["status"] = "succeeded"
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": result.analysis_run["analysis_run_id"],
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
        }
    )
    api.diagnostics.append(
        {
            "diagnostic_id": "diagnostic-1",
            "subject_type": "analysis_run",
            "subject_id": result.analysis_run["analysis_run_id"],
            "severity": "info",
            "code": "worker_note",
            "message": "Result stored for later delivery.",
        }
    )

    restarted_gateway = TelegramInboxGateway(api)
    completed = restarted_gateway.restore_status(owner=owner())
    completed_text = render_status_text(completed)

    assert completed.active_runs == []
    assert "Completed runs:" in completed_text
    assert "run-1: succeeded" in completed_text
    assert "artifact-1: transcript [available, text/plain]" in completed_text
    assert "worker_note: Result stored for later delivery." in completed_text


def test_fresh_app_inbox_restore_does_not_need_previous_message_or_page_state() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="restore after reconnect")
    gateway.start_analysis(owner=owner())

    app = TelegramInboxApp(
        SimpleNamespace(allowed_user_ids=set()),
        TelegramInboxGateway(api),
        bot=SimpleNamespace(edit_message_text=None),
    )
    message = _FakeMessage()

    sent = asyncio.run(app._send_or_edit_status(message))

    assert sent is True
    assert app.status_message_ids == {(10, 7): 9001}
    assert app.page_states[(10, 7)].current_cursor is None
    assert "restore after reconnect" in message.answers[0]["text"]
    assert "run-1: queued" in message.answers[0]["text"]
    assert "available later" in message.answers[0]["text"]


class _FakeMessage:
    def __init__(self) -> None:
        self.chat = SimpleNamespace(id=10, type="private")
        self.from_user = SimpleNamespace(id=7)
        self.message_thread_id = None
        self.answers: list[dict[str, Any]] = []

    async def answer(self, text: str, **kwargs) -> SimpleNamespace:
        self.answers.append({"text": text, **kwargs})
        return SimpleNamespace(message_id=9001)
