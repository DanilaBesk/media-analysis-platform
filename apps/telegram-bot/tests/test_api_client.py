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
