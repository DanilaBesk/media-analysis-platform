# FILE: apps/telegram-bot/tests/test_api_client.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove the packet-local Telegram adapter client keeps polling-default request shaping and multipart combined-upload semantics.
# SCOPE: Verify default polling delivery for URL submits and multipart field shaping for combined Telegram uploads.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local Telegram adapter client tests for default polling and combined multipart shaping.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   verify-default-polling - Confirm URL submits preserve polling as the default delivery strategy.
#   verify-combined-multipart-shape - Confirm combined Telegram uploads use multipart form data and retain polling delivery fields.
# END_MODULE_MAP

from __future__ import annotations

import json
from types import SimpleNamespace

from telegram_adapter.api_client import TelegramApiClient, UploadFilePart


class FakeHttpResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def test_create_from_url_defaults_delivery_to_polling() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"job": {"job_id": "job-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    client.create_from_url(url="https://youtu.be/demo")

    payload = json.loads(captured["request"].data.decode("utf-8"))
    assert payload["delivery"] == {"strategy": "polling"}


def test_create_combined_upload_uses_multipart_and_polling_fields() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"job": {"job_id": "job-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    client.create_combined_upload(
        files=[
            UploadFilePart("a.ogg", "audio/ogg", b"a"),
            UploadFilePart("b.mp4", "video/mp4", b"b"),
        ]
    )

    content_type = captured["request"].headers["Content-type"]
    body = captured["request"].data.decode("utf-8", errors="replace")
    assert "multipart/form-data" in content_type
    assert 'name="delivery_strategy"' in body
    assert "polling" in body


def test_create_batch_uses_manifest_and_source_label_file_parts() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"job": {"job_id": "batch-root-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    client.create_batch(
        files=[
            UploadFilePart(
                filename="voice.ogg",
                content_type="audio/ogg",
                content_bytes=b"voice",
                field_name="voice_abc123",
            )
        ],
        source_manifest={
            "manifest_version": "batch-transcription.v1",
            "ordered_source_labels": ["voice_abc123", "url_def456"],
            "sources": {
                "voice_abc123": {
                    "source_kind": "telegram_upload",
                    "file_part": "voice_abc123",
                    "display_name": "Voice",
                    "original_filename": "voice.ogg",
                },
                "url_def456": {
                    "source_kind": "youtube_url",
                    "url": "https://youtu.be/demo",
                    "display_name": "YouTube: demo",
                },
            },
            "completion_policy": "succeed_when_all_sources_succeed",
        },
    )

    body = captured["request"].data.decode("utf-8", errors="replace")
    assert captured["request"].full_url == "http://localhost:8080/v1/transcription-jobs/batch"
    assert 'name="source_manifest"' in body
    assert '"ordered_source_labels":["voice_abc123","url_def456"]' in body
    assert 'name="voice_abc123"; filename="voice.ogg"' in body
    assert 'name="files"' not in body


def test_add_batch_draft_upload_item_uses_contract_multipart_fields() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(
            json.dumps(
                {
                    "draft": {
                        "draft_id": "11111111-1111-1111-1111-111111111111",
                        "version": 2,
                        "owner": {
                            "owner_type": "telegram",
                            "telegram_chat_id": "10",
                            "telegram_user_id": "7",
                        },
                        "status": "open",
                        "items": [],
                    }
                }
            ).encode("utf-8")
        )

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    client.add_batch_draft_upload_item(
        draft_id="11111111-1111-1111-1111-111111111111",
        owner={
            "owner_type": "telegram",
            "telegram_chat_id": "10",
            "telegram_user_id": "7",
        },
        expected_version=1,
        item={
            "source_kind": "telegram_upload",
            "display_name": "Audio: voice.ogg",
            "original_filename": "voice.ogg",
            "content_type": "audio/ogg",
            "size_bytes": 5,
        },
        file=UploadFilePart("voice.ogg", "audio/ogg", b"voice", field_name="file"),
    )

    request = captured["request"]
    body = request.data.decode("utf-8", errors="replace")
    assert request.full_url == "http://localhost:8080/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items"
    assert request.get_method() == "POST"
    assert "multipart/form-data" in request.headers["Content-type"]
    assert 'name="owner"' in body
    assert '"owner_type":"telegram"' in body
    assert 'name="expected_version"' in body
    assert "\r\n1\r\n" in body
    assert 'name="item"' in body
    assert '"source_kind":"telegram_upload"' in body
    assert 'name="file"; filename="voice.ogg"' in body
    assert "voice" in body
