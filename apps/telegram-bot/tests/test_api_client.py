# FILE: apps/telegram-bot/tests/test_api_client.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove the Telegram adapter client speaks the final inbox-first HTTP API.
# SCOPE: Verify media ingestion, inbox removal, selection creation, run creation, and restore/read request shaping.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import json
from urllib.error import URLError

import pytest

from telegram_adapter.api_client import TelegramApiClient, TelegramApiClientError
from telegram_adapter.errors import TelegramUserErrorCode, classify_user_error, user_error_text


class FakeHttpResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


OWNER = {
    "owner_type": "telegram",
    "owner_id": "chat:10:user:7",
    "adapter_identity": {"telegram_chat_id": "10", "telegram_user_id": "7"},
}


def test_add_media_item_posts_final_media_item_payload() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"media_item": {"media_item_id": "media-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    item = client.add_media_item(
        owner=OWNER,
        kind="text",
        source={"origin_type": "text", "text": "hello"},
        display_name="hello",
        metadata={"message_id": 42},
    )

    payload = json.loads(captured["request"].data.decode("utf-8"))
    assert item == {"media_item_id": "media-1"}
    assert captured["request"].full_url == "http://localhost:8080/v1/media-items"
    assert payload["owner"] == OWNER
    assert payload["kind"] == "text"
    assert payload["source"] == {"origin_type": "text", "text": "hello"}
    assert payload["adapter_origin"] == "telegram"
    assert payload["metadata"] == {"message_id": 42}


def test_remove_collection_item_uses_owner_query_and_expected_version() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"collection": {"collection_id": "inbox-1", "version": 3}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    client.remove_collection_item(
        owner=OWNER,
        collection_id="inbox-1",
        media_item_id="media-1",
        expected_version=2,
    )

    request = captured["request"]
    assert request.get_method() == "DELETE"
    assert request.full_url == (
        "http://api:8080/v1/collections/inbox-1/items/media-1"
        "?owner_type=telegram&owner_id=chat%3A10%3Auser%3A7&expected_version=2"
    )


def test_create_selection_and_analysis_run_use_final_identifiers() -> None:
    requests = []

    def fake_urlopen(request):
        requests.append(request)
        if request.full_url.endswith("/v1/selections"):
            return FakeHttpResponse(json.dumps({"selection": {"selection_id": "sel-1"}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    selection = client.create_selection(
        owner=OWNER,
        source_collection_id="inbox-1",
        items=[{"media_item_id": "media-1", "position": 0}],
        option_snapshot={"adapter": "telegram"},
    )
    run = client.create_analysis_run(owner=OWNER, selection_id=selection["selection_id"])

    selection_payload = json.loads(requests[0].data.decode("utf-8"))
    run_payload = json.loads(requests[1].data.decode("utf-8"))
    assert selection == {"selection_id": "sel-1"}
    assert run == {"analysis_run_id": "run-1"}
    assert selection_payload["source_collection_id"] == "inbox-1"
    assert selection_payload["items"] == [{"media_item_id": "media-1", "position": 0}]
    assert run_payload == {
        "owner": OWNER,
        "selection_id": "sel-1",
        "run_type": "transcription",
        "delivery": {"strategy": "polling"},
    }


def test_restore_reads_inbox_media_and_runs_with_owner_query() -> None:
    urls = []

    def fake_urlopen(request):
        urls.append(request.full_url)
        if "/v1/collections/inbox" in request.full_url:
            return FakeHttpResponse(json.dumps({"collection": {"collection_id": "inbox-1"}}).encode("utf-8"))
        if "/v1/media-items" in request.full_url:
            return FakeHttpResponse(json.dumps({"items": [], "page": {"page_size": 5, "has_more": False}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"items": [], "page": {"page_size": 10, "has_more": False}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    client.get_inbox_collection(owner=OWNER)
    client.list_media_items(owner=OWNER, page_size=5)
    client.list_analysis_runs(owner=OWNER, page_size=10)

    assert urls == [
        "http://api:8080/v1/collections/inbox?owner_type=telegram&owner_id=chat%3A10%3Auser%3A7",
        "http://api:8080/v1/media-items?owner_type=telegram&owner_id=chat%3A10%3Auser%3A7&page_size=5",
        "http://api:8080/v1/analysis-runs?owner_type=telegram&owner_id=chat%3A10%3Auser%3A7&page_size=10",
    ]


def test_backend_connection_failure_is_categorized_without_raw_exception_copy() -> None:
    def fake_urlopen(request):
        raise URLError("Connection refused at 127.0.0.1:8080")

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)

    with pytest.raises(TelegramApiClientError) as error:
        client.list_media_items(owner=OWNER, page_size=5)

    user_error = classify_user_error(error.value)
    copy = user_error_text(error.value)
    assert error.value.code == "backend_unavailable"
    assert user_error.code == TelegramUserErrorCode.BACKEND_UNAVAILABLE
    assert "Connection refused" not in copy
    assert "127.0.0.1" not in copy
    assert "Try again" in copy
