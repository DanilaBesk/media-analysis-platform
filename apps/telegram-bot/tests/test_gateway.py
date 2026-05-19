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

from telegram_adapter.bot import (
    TelegramInboxApp,
    _decode_callback_token,
    _decode_callback_version,
    _decode_optional_callback_token,
    _parse_callback_payload,
    build_status_keyboard,
    render_status_text,
)
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
        self.upload_requests: list[dict[str, Any]] = []
        self.remove_requests: list[dict[str, Any]] = []
        self.cancel_requests: list[dict[str, Any]] = []
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


def owner() -> dict[str, Any]:
    return {
        "owner_type": "telegram",
        "owner_id": "chat:10:user:7",
        "adapter_identity": {"telegram_chat_id": "10", "telegram_user_id": "7"},
    }


def create_selection_and_run(gateway: TelegramInboxGateway) -> tuple[dict[str, Any], dict[str, Any]]:
    status = gateway.restore_status(owner=owner())
    assert status.collection is not None
    selection = gateway.create_selection_snapshot(
        owner=owner(),
        collection_id=status.collection["collection_id"],
        expected_version=int(status.collection["version"]),
    )
    run = gateway.start_analysis(owner=owner(), selection_snapshot_id=selection["selection_snapshot_id"])
    return selection, run


def test_text_and_link_messages_become_inbox_media_assets() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)

    records = gateway.add_message_inputs(owner=owner(), text="Meeting notes https://example.com/a", message_id=42)

    assert [record.status for record in records] == ["accepted", "accepted"]
    assert [request["kind"] for request in api.add_requests] == ["url", "text"]
    assert api.add_requests[0]["origin"] == {"origin_type": "url", "origin_ref": "https://example.com/a"}
    assert api.add_requests[1]["origin"] == {"origin_type": "text", "origin_ref": "Meeting notes"}
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
    assert "только в личном чате" in user_error_text(group_error.value)


def test_mixed_inputs_preserve_supported_and_unsupported_urls_with_files() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    files = [
        TelegramFileInput(kind="photo", file_id="photo-file", file_unique_id="photo-u", content=b"photo-body", size_bytes=10, message_id=43),
        TelegramFileInput(
            kind="document",
            file_id="doc-file",
            file_name="generic.bin",
            content_type="application/octet-stream",
            content=b"doc-body",
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
    assert [request["kind"] for request in api.add_requests] == ["url", "text"]
    assert [request["kind"] for request in api.upload_requests] == ["photo", "document"]
    assert api.add_requests[1]["origin"] == {"origin_type": "text", "origin_ref": "Keep this"}
    assert api.upload_requests[1]["content_type"] == "application/octet-stream"


def test_photo_video_document_and_media_group_inputs_keep_telegram_metadata() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    files = [
        TelegramFileInput(kind="photo", file_id="photo-file", file_unique_id="photo-u", content=b"photo-body", size_bytes=10, media_group_id="grp", message_id=1),
        TelegramFileInput(kind="video", file_id="video-file", file_name="clip.mp4", content_type="video/mp4", content=b"video-body", size_bytes=20, media_group_id="grp", message_id=2),
        TelegramFileInput(kind="document", file_id="doc-file", file_name="brief.pdf", content_type="application/pdf", content=b"pdf-body", size_bytes=30, media_group_id="grp", message_id=3),
    ]

    records = gateway.add_message_inputs(owner=owner(), files=files)

    assert [record.status for record in records] == ["accepted", "accepted", "accepted"]
    assert api.add_requests == []
    assert [request["kind"] for request in api.upload_requests] == ["photo", "video", "document"]
    assert api.upload_requests[0]["content"] == b"photo-body"
    assert api.upload_requests[1]["file_name"] == "clip.mp4"
    assert api.upload_requests[2]["content_type"] == "application/pdf"
    assert all(request["metadata"]["media_group_id"] == "grp" for request in api.upload_requests)


def test_voice_file_ingress_uses_multipart_upload_and_never_add_media_asset_object_refs() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)

    record = gateway.add_file(
        owner=owner(),
        file_input=TelegramFileInput(
            kind="voice",
            file_id="voice-file",
            file_unique_id="voice-u",
            content=b"voice-body",
            content_type="audio/ogg",
            size_bytes=10,
            message_id=77,
        ),
    )

    assert record.status == "accepted"
    assert api.add_requests == []
    assert len(api.upload_requests) == 1
    assert api.upload_requests[0]["kind"] == "voice"
    assert api.upload_requests[0]["content"] == b"voice-body"
    assert api.upload_requests[0]["metadata"]["file_unique_id"] == "voice-u"
    assert record.media_asset is not None
    assert record.media_asset["origin"]["origin_type"] == "upload"
    assert record.media_asset["origin"]["object_ref"].startswith("sources/")
    assert "telegram://file/" not in record.media_asset["origin"]["object_ref"]


def test_album_status_preview_groups_visible_media_together() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    files = [
        TelegramFileInput(kind="photo", file_id="photo-file", file_unique_id="photo-u", content=b"photo-body", size_bytes=10, media_group_id="grp", message_id=1),
        TelegramFileInput(kind="video", file_id="video-file", file_name="clip.mp4", content_type="video/mp4", content=b"video-body", size_bytes=20, media_group_id="grp", message_id=2),
        TelegramFileInput(kind="document", file_id="doc-file", file_name="brief.pdf", content_type="application/pdf", content=b"pdf-body", size_bytes=30, media_group_id="grp", message_id=3),
    ]
    gateway.add_message_inputs(owner=owner(), files=files)

    text = render_status_text(gateway.restore_status(owner=owner()))

    assert text.startswith("Транскрибация\nМатериалов: 3\n")
    assert "Фото из Telegram · 10 B" in text
    assert "clip.mp4 · 10 B" in text
    assert "brief.pdf · 8 B" in text
    assert "Альбом grp" not in text


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
    assert "Отклонено: Telegram message (неподдерживаемый ввод:" in text
    assert "Отклонено: photo (неподдерживаемый ввод:" in text


def test_status_surface_splits_main_card_and_materials_actions() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=1)
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status, can_go_back=True, current_cursor=None)
    text = render_status_text(status)
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    run_callback = next(callback for callback in callbacks if callback.startswith("ib:rn:"))
    run_action, run_tokens = _parse_callback_payload(run_callback)
    materials_keyboard = build_status_keyboard(status, can_go_back=True, current_cursor=None, screen="materials")
    materials_callbacks = [
        button.callback_data for row in materials_keyboard.inline_keyboard for button in row
    ]
    remove_callback = next(callback for callback in materials_callbacks if callback.startswith("ib:rm:"))
    remove_action, remove_tokens = _parse_callback_payload(remove_callback)
    updated = gateway.remove_collection_item(
        owner=owner(),
        collection_id=_decode_callback_token(remove_tokens[0]),
        media_asset_id=_decode_callback_token(remove_tokens[2]),
        expected_version=_decode_callback_version(remove_tokens[1]),
    )
    updated_keyboard = build_status_keyboard(updated)
    updated_callbacks = [button.callback_data for row in updated_keyboard.inline_keyboard for button in row]
    updated_run_callback = next(callback for callback in updated_callbacks if callback.startswith("ib:rn:"))
    updated_run_action, updated_run_tokens = _parse_callback_payload(updated_run_callback)
    selection = gateway.create_selection_snapshot(
        owner=owner(),
        collection_id=_decode_callback_token(updated_run_tokens[0]),
        expected_version=_decode_callback_version(updated_run_tokens[1]),
    )
    run = gateway.start_analysis(owner=owner(), selection_snapshot_id=selection["selection_snapshot_id"])

    assert text.startswith("Транскрибация\nМатериалов: 2\n")
    assert [button.text for button in keyboard.inline_keyboard[0]] == ["Материалы"]
    assert [button.text for button in keyboard.inline_keyboard[-1]] == ["🎙 Транскрибация (2)"]
    assert run_action == "rn"
    assert _decode_callback_token(run_tokens[0]) == "inbox-1"
    assert _decode_callback_version(run_tokens[1]) == 1
    assert any(callback.startswith("ib:mt") for callback in callbacks)
    assert not any(callback.startswith("ib:rf") for callback in callbacks)
    assert not any(callback.startswith("ib:rm:") for callback in callbacks)
    assert not any(callback.startswith("ib:cl:") for callback in callbacks)
    assert not any(callback.startswith("ib:sl:") for callback in callbacks)
    assert any(callback.startswith("ib:pp") for callback in materials_callbacks)
    assert any(callback.startswith("ib:pn") for callback in materials_callbacks)
    assert any(callback.startswith("ib:rm:") for callback in materials_callbacks)
    assert any(callback.startswith("ib:cl:") for callback in materials_callbacks)
    assert remove_action == "rm"
    assert _decode_callback_token(remove_tokens[0]) == "inbox-1"
    assert _decode_callback_version(remove_tokens[1]) == 1
    assert _decode_callback_token(remove_tokens[2]) == "media-1"
    assert updated_run_action == "rn"
    assert _decode_callback_token(updated_run_tokens[0]) == "inbox-1"
    assert _decode_callback_version(updated_run_tokens[1]) == 2
    assert updated.collection is not None
    assert updated.collection["version"] == 2
    assert selection["items"] == [{"media_asset_id": "media-2", "position": 0}]
    assert run["status"] == "queued"


def test_large_inbox_uses_compact_resource_callbacks_and_clears_only_visible_page() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=5)
    for index in range(12):
        gateway.add_text(owner=owner(), text=f"item {index + 1}")

    status = gateway.restore_status(owner=owner())
    main_keyboard = build_status_keyboard(status, current_cursor=None)
    assert [button.text for button in main_keyboard.inline_keyboard[0]] == ["Материалы"]
    assert [button.text for button in main_keyboard.inline_keyboard[-1]] == ["🎙 Транскрибация (12)"]

    keyboard = build_status_keyboard(status, current_cursor=None, screen="materials")
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert all(callback.startswith("ib:rm:") for callback in callbacks[:5])
    assert "ib:pn" in callbacks
    assert any(callback.startswith("ib:rl:") for callback in callbacks)
    assert any(callback.startswith("ib:cl:") for callback in callbacks)
    assert "ib:rf" not in callbacks
    assert max(len(callback) for callback in callbacks) <= 64
    remove_action, remove_tokens = _parse_callback_payload(callbacks[1])
    after_slot_remove = gateway.remove_collection_item(
        owner=owner(),
        collection_id=_decode_callback_token(remove_tokens[0]),
        media_asset_id=_decode_callback_token(remove_tokens[2]),
        expected_version=_decode_callback_version(remove_tokens[1]),
    )
    page_two_status = gateway.restore_status(owner=owner(), cursor="media-6")
    page_two_keyboard = build_status_keyboard(page_two_status, current_cursor="media-6", screen="materials")
    clear_callback = next(
        button.callback_data
        for row in page_two_keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:cl:")
    )
    clear_action, clear_tokens = _parse_callback_payload(clear_callback)
    after_clear = gateway.clear_visible_items(
        owner=owner(),
        collection_id=_decode_callback_token(clear_tokens[0]),
        expected_version=_decode_callback_version(clear_tokens[1]),
        cursor=_decode_optional_callback_token(clear_tokens[2]),
    )

    assert remove_action == "rm"
    assert clear_action == "cl"
    assert [item["media_asset_id"] for item in after_slot_remove.collection["items"][:5]] == [
        "media-1",
        "media-3",
        "media-4",
        "media-5",
        "media-6",
    ]
    assert [request["media_asset_id"] for request in api.remove_requests[-5:]] == [
        "media-7",
        "media-8",
        "media-9",
        "media-10",
        "media-11",
    ]
    assert [item["media_asset_id"] for item in after_clear.collection["items"]] == [
        "media-1",
        "media-3",
        "media-4",
        "media-5",
        "media-6",
        "media-12",
    ]


def test_clear_collection_removes_all_items_across_pages() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=1)
    gateway.add_text(owner=owner(), text="one")
    gateway.add_text(owner=owner(), text="two")

    cleared = gateway.clear_collection(
        owner=owner(),
        collection_id="inbox-1",
        expected_version=api.collection["version"],
    )

    assert cleared.items == []
    assert cleared.collection is not None
    assert cleared.collection["items"] == []
    assert [request["media_asset_id"] for request in api.remove_requests] == ["media-1", "media-2"]


def test_remove_latest_collection_item_removes_last_item_from_full_collection() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api, page_size=2)
    for index in range(4):
        gateway.add_text(owner=owner(), text=f"item {index + 1}")

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status, screen="materials")
    remove_latest_callback = next(
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rl:")
    )
    action, tokens = _parse_callback_payload(remove_latest_callback)
    updated = gateway.remove_latest_collection_item(
        owner=owner(),
        collection_id=_decode_callback_token(tokens[0]),
        expected_version=_decode_callback_version(tokens[1]),
    )

    assert action == "rl"
    assert api.remove_requests[-1]["media_asset_id"] == "media-4"
    assert [item["media_asset_id"] for item in updated.collection["items"]] == [
        "media-1",
        "media-2",
        "media-3",
    ]


def test_restore_status_uses_collection_membership_instead_of_owner_wide_media_list() -> None:
    class CollectionOnlyRemovalApiClient(FakeFinalApiClient):
        def remove_collection_item(self, **kwargs) -> dict[str, Any]:
            media_asset_id = kwargs["media_asset_id"]
            self.remove_requests.append(kwargs)
            self.collection["items"] = [
                item for item in self.collection["items"] if item["media_asset_id"] != media_asset_id
            ]
            self.collection["version"] += 1
            return self.collection

    api = CollectionOnlyRemovalApiClient()
    gateway = TelegramInboxGateway(api, page_size=5)
    for index in range(5):
        gateway.add_text(owner=owner(), text=f"item {index + 1}")

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status, screen="materials")
    remove_callback = next(
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data.startswith("ib:rm:")
    )
    _, remove_tokens = _parse_callback_payload(remove_callback)
    updated = gateway.remove_collection_item(
        owner=owner(),
        collection_id=_decode_callback_token(remove_tokens[0]),
        media_asset_id=_decode_callback_token(remove_tokens[2]),
        expected_version=_decode_callback_version(remove_tokens[1]),
    )
    updated_text = render_status_text(updated)

    assert [item["media_asset_id"] for item in updated.items] == [
        "media-2",
        "media-3",
        "media-4",
        "media-5",
    ]
    assert "Текст: «item 1»" not in updated_text
    assert "Текст: «item 2»" in updated_text
    assert "Материалов: 4" in updated_text
    assert "версия" not in updated_text


def test_uuid_callbacks_stay_within_telegram_limit() -> None:
    base_status = TelegramInboxGateway(FakeFinalApiClient()).restore_status(owner=owner())
    status = base_status.__class__(
        owner=base_status.owner,
        collection={
            "collection_id": "11111111-1111-1111-1111-111111111111",
            "version": 123456,
            "items": [{"media_asset_id": "22222222-2222-2222-2222-222222222222", "position": 0}],
        },
        items=[
            {
                "media_asset_id": "22222222-2222-2222-2222-222222222222",
                "display_name": "uuid-item",
                "kind": "text",
                "status": "ready",
                "metadata": {},
            }
        ],
        page={"page_size": 5, "has_more": False, "next_cursor": ""},
        active_runs=[],
        recent_runs=[
            {
                "analysis_run_id": "33333333-3333-3333-3333-333333333333",
                "status": "succeeded",
                "version": 123456,
            }
        ],
        artifacts_by_run={
            "33333333-3333-3333-3333-333333333333": [{"artifact_id": "artifact-1", "kind": "report", "status": "available"}]
        },
        diagnostics_by_run={
            "33333333-3333-3333-3333-333333333333": [{"diagnostic_id": "diagnostic-1", "severity": "info"}]
        },
        rejected=[],
    )
    main_keyboard = build_status_keyboard(
        status,
        current_cursor="44444444-4444-4444-4444-444444444444",
        selection={
            "selection_snapshot_id": "55555555-5555-5555-5555-555555555555",
            "items": [{"media_asset_id": "22222222-2222-2222-2222-222222222222", "position": 0}],
        },
        focused_run_id="33333333-3333-3333-3333-333333333333",
    )
    materials_keyboard = build_status_keyboard(
        status,
        current_cursor="44444444-4444-4444-4444-444444444444",
        selection={
            "selection_snapshot_id": "55555555-5555-5555-5555-555555555555",
            "items": [{"media_asset_id": "22222222-2222-2222-2222-222222222222", "position": 0}],
        },
        screen="materials",
    )
    callbacks = [
        *[button.callback_data for row in main_keyboard.inline_keyboard for button in row],
        *[button.callback_data for row in materials_keyboard.inline_keyboard for button in row],
    ]

    assert max(len(callback) for callback in callbacks) <= 64
    assert any(callback.startswith("ib:rm:") for callback in callbacks)
    assert any(callback.startswith("ib:rn:") for callback in callbacks)
    assert any(callback.startswith("ib:mt") for callback in callbacks)
    assert any(callback.startswith("ib:ar:") for callback in callbacks)
    assert any(callback.startswith("ib:dg:") for callback in callbacks)


def test_main_card_hides_historical_result_without_focused_run() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="transcript candidate")

    api.runs.extend(
        [
            {"analysis_run_id": "run-succeeded", "status": "succeeded", "version": 1},
            {"analysis_run_id": "run-failed", "status": "failed", "version": 2},
        ]
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-succeeded",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
        }
    )
    api.diagnostics.extend(
        [
            {
                "diagnostic_id": "diagnostic-older",
                "subject_type": "analysis_run",
                "subject_id": "run-succeeded",
                "severity": "info",
                "message": "Older success note.",
            },
            {
                "diagnostic_id": "diagnostic-newer",
                "subject_type": "analysis_run",
                "subject_id": "run-failed",
                "severity": "error",
                "message": "Newer failure note.",
            },
        ]
    )

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status)
    button_texts = [button.text for row in keyboard.inline_keyboard for button in row]

    assert "Результат" not in button_texts
    assert "Диагностика" not in button_texts


def test_main_card_hides_old_result_while_focused_run_is_active() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="new transcript candidate")
    api.runs.extend(
        [
            {"analysis_run_id": "run-old", "status": "succeeded", "version": 1},
            {"analysis_run_id": "run-current", "status": "running", "version": 2},
        ]
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-old",
            "analysis_run_id": "run-old",
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
        }
    )

    status = gateway.restore_status(owner=owner())
    text = render_status_text(status)
    keyboard = build_status_keyboard(status, focused_run_id="run-current")
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    button_texts = [button.text for row in keyboard.inline_keyboard for button in row]

    assert "Активная задача: в работе" in text
    assert "Результат" not in button_texts
    assert "🎙 Транскрибация (1)" not in button_texts
    assert not any(callback.startswith("ib:rn:") for callback in callbacks)
    assert all("run-old" not in callback for callback in callbacks)


def test_main_card_separates_background_active_run_from_new_transcription_action() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="cancelable transcript candidate")
    api.runs.extend(
        [
            {"analysis_run_id": "run-other", "status": "queued", "version": 1},
            {"analysis_run_id": "run-current", "status": "running", "version": 4},
        ]
    )

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status, focused_run_id="run-current")
    unfocused_keyboard = build_status_keyboard(status)
    callbacks_by_text = {
        button.text: button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
    }
    unfocused_callbacks_by_text = {
        button.text: button.callback_data
        for row in unfocused_keyboard.inline_keyboard
        for button in row
    }
    unfocused_texts = [button.text for row in unfocused_keyboard.inline_keyboard for button in row]
    focused_texts = [button.text for row in keyboard.inline_keyboard for button in row]

    action, tokens = _parse_callback_payload(callbacks_by_text["Отмена"])
    unfocused_action, unfocused_tokens = _parse_callback_payload(unfocused_callbacks_by_text["Отмена"])

    assert "🎙 Транскрибация (1)" not in focused_texts
    assert "🎙 Транскрибация (1)" in unfocused_texts
    assert action == "cn"
    assert _decode_callback_token(tokens[0]) == "run-current"
    assert _decode_callback_version(tokens[1]) == 4
    assert unfocused_action == "cn"
    assert _decode_callback_token(unfocused_tokens[0]) == "run-current"
    assert _decode_callback_version(unfocused_tokens[1]) == 4


def test_gateway_cancel_analysis_run_verifies_version_and_active_status() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    api.runs.append({"analysis_run_id": "run-1", "status": "running", "version": 3})

    status = gateway.cancel_analysis_run(
        owner=owner(),
        analysis_run_id="run-1",
        expected_version=3,
        message="stop",
    )

    assert api.cancel_requests == [
        {"channel_account_id": "channel-account-1", "analysis_run_id": "run-1", "message": "stop"}
    ]
    assert status.active_runs == []
    assert api.runs[0]["status"] == "canceled"

    with pytest.raises(RuntimeError, match="slot_not_visible"):
        gateway.cancel_analysis_run(owner=owner(), analysis_run_id="run-1", expected_version=4)


def test_main_card_result_is_scoped_to_focused_terminal_run() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="transcript candidate")
    api.runs.extend(
        [
            {"analysis_run_id": "run-old", "status": "succeeded", "version": 1},
            {"analysis_run_id": "run-current", "status": "succeeded", "version": 2},
        ]
    )
    api.artifacts.extend(
        [
            {
                "artifact_id": "artifact-old",
                "analysis_run_id": "run-old",
                "kind": "transcript",
                "status": "available",
                "content_type": "text/plain",
            },
            {
                "artifact_id": "artifact-current",
                "analysis_run_id": "run-current",
                "kind": "transcript",
                "status": "available",
                "content_type": "text/plain",
            },
        ]
    )
    api.diagnostics.extend(
        [
            {
                "diagnostic_id": "diagnostic-old",
                "subject_type": "analysis_run",
                "subject_id": "run-old",
                "severity": "info",
                "message": "Old note.",
            },
            {
                "diagnostic_id": "diagnostic-current",
                "subject_type": "analysis_run",
                "subject_id": "run-current",
                "severity": "info",
                "message": "Current note.",
            },
        ]
    )

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(status, focused_run_id="run-current")
    callbacks_by_text = {
        button.text: button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.text in {"Результат", "Диагностика"}
    }

    result_action, result_tokens = _parse_callback_payload(callbacks_by_text["Результат"])
    diagnostics_action, diagnostics_tokens = _parse_callback_payload(callbacks_by_text["Диагностика"])

    assert result_action == "ar"
    assert _decode_callback_token(result_tokens[0]) == "run-current"
    assert _decode_callback_version(result_tokens[1]) == 2
    assert diagnostics_action == "dg"
    assert _decode_callback_token(diagnostics_tokens[0]) == "run-current"
    assert _decode_callback_version(diagnostics_tokens[1]) == 2


def test_selection_and_completed_run_actions_are_explicit_in_keyboard() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="ready for selection")
    api.runs.append(
        {
            "analysis_run_id": "run-1",
            "selection_snapshot_id": "selection-1",
            "run_type": "transcription",
            "status": "succeeded",
            "version": 3,
        }
    )
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
            "message": "Ready for review.",
        }
    )

    status = gateway.restore_status(owner=owner())
    keyboard = build_status_keyboard(
        status,
        selection={"selection_snapshot_id": "selection-1", "items": status.collection["items"]},
        focused_run_id="run-1",
    )
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]

    assert any(callback.startswith("ib:rn:") for callback in callbacks)
    assert any(callback.startswith("ib:ar:") for callback in callbacks)
    assert any(callback.startswith("ib:dg:") for callback in callbacks)

    selection_text = render_status_text(status, selection={"selection_snapshot_id": "selection-1", "items": status.collection["items"]})
    assert "Последние результаты:" not in selection_text
    assert "run-1" not in selection_text
    assert "Выборка готова:" not in selection_text
    assert "Кнопка ниже запустит анализ этой выборки." not in selection_text
    assert "artifact-1: transcript [available, text/plain]" not in selection_text
    assert "worker_note: Ready for review." not in selection_text


def test_gateway_edge_paths_cover_validation_visibility_and_helper_fallbacks() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)

    assert gateway.owner_for(chat_id=10, user_id=7) == {
        **owner(),
        "adapter_identity": {
            "telegram_chat_id": "10",
            "telegram_user_id": "7",
            "telegram_chat_type": "private",
        },
    }

    empty_text = gateway.add_text(owner=owner(), text="   ")
    invalid_link = gateway.add_link(owner=owner(), url="https:///missing-host")
    missing_content = gateway.add_file(owner=owner(), file_input=TelegramFileInput(kind="voice", file_id="voice-1"))
    plain_text_records = gateway.add_message_inputs(owner=owner(), text="plain text only")

    assert empty_text.reason == "empty_text"
    assert invalid_link.reason == "invalid_url"
    assert missing_content.reason == "missing_file_content"
    assert plain_text_records[0].status == "accepted"
    assert api.add_requests[-1]["origin"] == {"origin_type": "text", "origin_ref": "plain text only"}

    caption_record = gateway.add_file(
        owner=owner(),
        file_input=TelegramFileInput(
            kind="document",
            file_id="doc-1",
            file_name="notes.txt",
            content=b"body",
            caption="human caption",
        ),
    )
    assert caption_record.media_asset is not None
    assert api.upload_requests[-1]["metadata"]["caption"] == "human caption"

    api.items.append({"media_asset_id": "", "display_name": "orphan", "kind": "text", "status": "ready", "metadata": {}})
    cleared = gateway.clear_visible_items(owner=owner(), collection_id="inbox-1", expected_version=api.collection["version"])
    assert all(item.get("media_asset_id") != "" for item in cleared.collection["items"])

    api.items.clear()
    api.collection["items"].clear()
    empty_cleared = gateway.clear_visible_items(owner=owner(), collection_id="inbox-1", expected_version=api.collection["version"])
    assert empty_cleared.items == []

    with pytest.raises(RuntimeError, match="slot_missing_media_asset_id"):
        gateway.remove_collection_item(
            owner=owner(),
            collection_id="inbox-1",
            media_asset_id="   ",
            expected_version=api.collection["version"],
        )

    with pytest.raises(RuntimeError, match="inbox_empty"):
        gateway.create_selection_snapshot(owner=owner(), collection_id="inbox-1", expected_version=api.collection["version"])

    with pytest.raises(RuntimeError, match="slot_not_visible"):
        gateway.start_analysis(owner=owner(), selection_snapshot_id="   ")

    with pytest.raises(RuntimeError, match="slot_not_visible"):
        gateway._get_verified_inbox_collection(owner=owner(), collection_id="different", expected_version=api.collection["version"])

    with pytest.raises(RuntimeError, match="slot_not_visible"):
        gateway._get_verified_inbox_collection(owner=owner(), collection_id="inbox-1", expected_version=999)

    api.runs.append({"analysis_run_id": "run-1", "version": 1})
    with pytest.raises(RuntimeError, match="slot_not_visible"):
        gateway._get_verified_run(owner=owner(), analysis_run_id="run-1", expected_version=2)


def test_restore_status_tolerates_missing_collection_and_renders_without_collection_count() -> None:
    api = FakeFinalApiClient()
    api.runs.extend(
        [
            {"analysis_run_id": "run-active", "status": "queued", "version": 1},
            {"analysis_run_id": "run-done", "status": "succeeded", "version": 2},
        ]
    )
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": "run-done",
            "kind": "report",
            "status": "available",
            "content_type": "text/plain",
        }
    )
    api.diagnostics.append(
        {
            "diagnostic_id": "diagnostic-1",
            "subject_type": "analysis_run",
            "subject_id": "run-done",
            "severity": "warning",
            "message": "Watch this.",
        }
    )

    class CollectionErrorApiClient(FakeFinalApiClient):
        def get_inbox_collection(self, **kwargs) -> dict[str, Any]:
            raise RuntimeError("temporary collection failure")

    failing_api = CollectionErrorApiClient()
    failing_api.runs = api.runs
    failing_api.artifacts = api.artifacts
    failing_api.diagnostics = api.diagnostics
    failing_api.items = [{"media_asset_id": "media-1", "display_name": "item", "kind": "text", "status": "ready", "metadata": {}}]
    gateway = TelegramInboxGateway(failing_api)

    status = gateway.restore_status(owner=owner())
    text = render_status_text(status)

    assert status.collection is None
    assert [run["analysis_run_id"] for run in status.active_runs] == ["run-active"]
    assert status.artifacts_by_run["run-done"][0]["artifact_id"] == "artifact-1"
    assert status.diagnostics_by_run["run-done"][0]["diagnostic_id"] == "diagnostic-1"
    assert "Материалов: 1" in text
    assert "Текст: «item»" in text
    assert "run-done" not in text


def test_restore_status_tolerates_flat_page_metadata_from_runtime_api() -> None:
    class FlatPageApiClient(FakeFinalApiClient):
        def get_inbox_collection(self, **kwargs) -> dict[str, Any]:
            raise RuntimeError("collection not found")

        def list_media_assets(self, **kwargs) -> dict[str, Any]:
            return {
                "items": [{"media_asset_id": "media-1", "display_name": "flat page item", "kind": "text", "status": "ready"}],
                "page": 1,
                "page_size": kwargs.get("page_size") or 5,
            }

    status = TelegramInboxGateway(FlatPageApiClient(), page_size=5).restore_status(owner=owner())

    assert status.collection is None
    assert status.page["page"] == 1
    assert status.page["page_size"] == 5
    assert status.page["has_more"] is False
    assert status.items[0]["display_name"] == "flat page item"


def test_stale_callback_copy_is_safe_and_actionable() -> None:
    answer = safe_callback_answer(RuntimeError("slot_not_visible"))

    assert answer == {
        "text": "Эта кнопка устарела. Откройте /inbox ещё раз и повторите действие.",
        "show_alert": True,
    }
    assert "slot_not_visible" not in answer["text"]


def test_long_running_run_is_restored_and_later_completion_is_visible_after_restart() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="long transcript")
    _, run = create_selection_and_run(gateway)

    restarted_gateway = TelegramInboxGateway(api)
    restored = restarted_gateway.restore_status(owner=owner())
    assert restored.active_runs == [run]

    api.runs[0]["status"] = "succeeded"
    completed = restarted_gateway.restore_status(owner=owner())
    run_status = restarted_gateway.get_run_status(owner=owner(), analysis_run_id=run["analysis_run_id"])

    assert completed.active_runs == []
    assert run_status["status"] == "succeeded"


def test_completed_run_actions_fetch_artifacts_and_diagnostics_explicitly() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="long transcript")
    _, run = create_selection_and_run(gateway)

    queued_text = render_status_text(gateway.restore_status(owner=owner()))

    assert "Активная задача: в очереди" in queued_text
    assert "failed" not in queued_text
    assert "Последние результаты:" not in queued_text

    api.runs[0]["status"] = "succeeded"
    api.artifacts.append(
        {
            "artifact_id": "artifact-1",
            "analysis_run_id": run["analysis_run_id"],
            "kind": "transcript",
            "status": "available",
            "content_type": "text/plain",
        }
    )
    api.diagnostics.append(
        {
            "diagnostic_id": "diagnostic-1",
            "subject_type": "analysis_run",
            "subject_id": run["analysis_run_id"],
            "severity": "info",
            "code": "worker_note",
            "message": "Result stored for later delivery.",
        }
    )

    restarted_gateway = TelegramInboxGateway(api)
    completed = restarted_gateway.restore_status(owner=owner())
    completed_text = render_status_text(completed)
    completed_keyboard = build_status_keyboard(completed, focused_run_id=run["analysis_run_id"])
    completed_callbacks = [button.callback_data for row in completed_keyboard.inline_keyboard for button in row]

    assert completed.active_runs == []
    assert "Последние результаты:" not in completed_text
    assert "run-1" not in completed_text
    assert "artifact-1: transcript [available, text/plain]" not in completed_text
    assert "worker_note: Result stored for later delivery." not in completed_text
    assert any(callback.startswith("ib:ar:") for callback in completed_callbacks)
    assert any(callback.startswith("ib:dg:") for callback in completed_callbacks)
    assert restarted_gateway.list_run_artifacts(owner=owner(), analysis_run_id=run["analysis_run_id"], expected_version=1)[0]["artifact_id"] == "artifact-1"
    assert restarted_gateway.list_run_diagnostics(owner=owner(), analysis_run_id=run["analysis_run_id"], expected_version=1)[0]["diagnostic_id"] == "diagnostic-1"


def test_fresh_app_inbox_restore_does_not_need_previous_message_or_page_state() -> None:
    api = FakeFinalApiClient()
    gateway = TelegramInboxGateway(api)
    gateway.add_text(owner=owner(), text="restore after reconnect")
    create_selection_and_run(gateway)

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
    assert "Активная задача: в очереди" in message.answers[0]["text"]
    assert "Последние результаты:" not in message.answers[0]["text"]


class _FakeMessage:
    def __init__(self) -> None:
        self.chat = SimpleNamespace(id=10, type="private")
        self.from_user = SimpleNamespace(id=7)
        self.message_thread_id = None
        self.answers: list[dict[str, Any]] = []

    async def answer(self, text: str, **kwargs) -> SimpleNamespace:
        self.answers.append({"text": text, **kwargs})
        return SimpleNamespace(message_id=9001)
