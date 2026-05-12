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

import io
import json
from urllib.error import HTTPError, URLError

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


def test_upload_media_item_posts_multipart_payload() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"media_item": {"media_item_id": "media-2"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    item = client.upload_media_item(
        owner=OWNER,
        kind="voice",
        content=b"voice-bytes",
        file_name="voice.ogg",
        content_type="audio/ogg",
        display_name="voice.ogg",
        metadata={"message_id": 42, "file_unique_id": "voice-u"},
    )

    request = captured["request"]
    content_type = request.headers["Content-type"]
    body = request.data
    assert item == {"media_item_id": "media-2"}
    assert request.full_url == "http://localhost:8080/v1/media-items"
    assert content_type.startswith("multipart/form-data; boundary=")
    assert b'"owner_type": "telegram"' in body
    assert b'"kind": "voice"' in body
    assert b'"display_name": "voice.ogg"' in body
    assert b'"message_id": 42' in body
    assert b"voice-bytes" in body


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
    assert selection_payload["owner"] == OWNER
    assert selection_payload["source_collection_id"] == "inbox-1"
    assert selection_payload["items"] == [{"media_item_id": "media-1", "position": 0}]
    assert selection_payload["option_snapshot"] == {"adapter": "telegram"}
    assert selection_payload["duplicate_policy"] == "reject"
    assert selection_payload["created_by"] == "telegram"
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


def test_get_analysis_run_uses_owner_query_and_extracts_wrapped_object() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["url"] = request.full_url
        return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-1", "status": "queued"}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    run = client.get_analysis_run(owner=OWNER, analysis_run_id="run-1")

    assert run == {"analysis_run_id": "run-1", "status": "queued"}
    assert captured["url"] == "http://api:8080/v1/analysis-runs/run-1?owner_type=telegram&owner_id=chat%3A10%3Auser%3A7"


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
    assert "Попробуйте ещё раз" in copy


def test_runtime_download_failures_map_to_specific_unsupported_input_copy() -> None:
    user_error = classify_user_error(RuntimeError("telegram_file_download_failed"))
    copy = user_error_text(RuntimeError("telegram_file_download_failed"))

    assert user_error.code == TelegramUserErrorCode.UNSUPPORTED_INPUT
    assert copy == "неподдерживаемый ввод: не удалось скачать файл из Telegram."


def test_runtime_rejection_reasons_map_to_unsupported_input_copy() -> None:
    user_error = classify_user_error(RuntimeError("missing_file_id"))
    copy = user_error_text(RuntimeError("missing_file_id"))

    assert user_error.code == TelegramUserErrorCode.UNSUPPORTED_INPUT
    assert copy == "неподдерживаемый ввод: Telegram не передал file id."


def test_optional_query_and_payload_fields_are_forwarded_for_full_adapter_surface() -> None:
    captured_urls: list[str] = []
    captured_requests = []
    owner_with_tenant = {**OWNER, "tenant_id": "tenant-1"}

    def fake_urlopen(request):
        captured_requests.append(request)
        captured_urls.append(request.full_url)
        if request.full_url.endswith("/v1/media-items"):
            return FakeHttpResponse(json.dumps({"media_item": {"media_item_id": "media-9"}}).encode("utf-8"))
        if request.full_url.endswith("/v1/analysis-runs"):
            return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-9"}}).encode("utf-8"))
        if "/v1/artifacts/" in request.full_url:
            return FakeHttpResponse(json.dumps({"artifact": {"artifact_id": "artifact-9"}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"items": [], "page": {"page_size": 3, "has_more": False}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    media_item = client.add_media_item(
        owner=owner_with_tenant,
        kind="text",
        source={"origin_type": "text", "text": "hello"},
        collection_id="inbox-9",
    )
    uploaded_item = client.upload_media_item(
        owner=owner_with_tenant,
        kind="audio",
        content=b"audio-body",
        file_name="note.bin",
        collection_id="inbox-9",
    )
    run = client.create_analysis_run(
        owner=owner_with_tenant,
        selection_id="selection-9",
        params={"mode": "deep"},
    )
    list_media = client.list_media_items(
        owner=owner_with_tenant,
        cursor="media-cursor",
        page_size=7,
        status="ready",
        kind="text",
    )
    list_runs = client.list_analysis_runs(
        owner=owner_with_tenant,
        cursor="run-cursor",
        page_size=4,
        status="queued",
        run_type="research",
    )
    list_artifacts = client.list_artifacts(
        owner=owner_with_tenant,
        analysis_run_id="run-9",
        cursor="artifact-cursor",
        page_size=3,
    )
    artifact = client.get_artifact(owner=owner_with_tenant, artifact_id="artifact-9")
    diagnostics = client.list_diagnostics(
        owner=owner_with_tenant,
        subject_type="analysis_run",
        subject_id="run-9",
        cursor="diagnostic-cursor",
        page_size=2,
    )

    add_payload = json.loads(captured_requests[0].data.decode("utf-8"))
    upload_request = captured_requests[1]
    run_payload = json.loads(captured_requests[2].data.decode("utf-8"))

    assert media_item == {"media_item_id": "media-9"}
    assert uploaded_item == {"media_item_id": "media-9"}
    assert run == {"analysis_run_id": "run-9"}
    assert list_media["page"]["page_size"] == 3
    assert list_runs["page"]["page_size"] == 3
    assert list_artifacts["page"]["page_size"] == 3
    assert artifact == {"artifact_id": "artifact-9"}
    assert diagnostics["page"]["page_size"] == 3
    assert add_payload["collection_id"] == "inbox-9"
    assert add_payload["owner"]["tenant_id"] == "tenant-1"
    assert b'"collection_id": "inbox-9"' in upload_request.data
    assert b"Content-Type: application/octet-stream" in upload_request.data
    assert run_payload["params"] == {"mode": "deep"}
    assert "tenant_id=tenant-1" in captured_urls[3]
    assert "cursor=media-cursor" in captured_urls[3]
    assert "status=ready" in captured_urls[3]
    assert "kind=text" in captured_urls[3]
    assert "cursor=run-cursor" in captured_urls[4]
    assert "run_type=research" in captured_urls[4]
    assert "analysis_run_id=run-9" in captured_urls[5]
    assert "cursor=artifact-cursor" in captured_urls[5]
    assert "artifact-9" in captured_urls[6]
    assert "subject_type=analysis_run" in captured_urls[7]
    assert "subject_id=run-9" in captured_urls[7]
    assert "cursor=diagnostic-cursor" in captured_urls[7]


def test_request_handles_empty_payload_http_errors_and_missing_wrapped_objects() -> None:
    empty_client = TelegramApiClient(
        "http://api:8080",
        urlopen_impl=lambda request: FakeHttpResponse(b""),
    )

    assert empty_client._request("/v1/health") == {}

    def fake_http_error(request):
        raise HTTPError(
            request.full_url,
            409,
            "Conflict",
            hdrs=None,
            fp=io.BytesIO(b'{"error":{"message":"stale","code":"conflict"}}'),
        )

    conflict_client = TelegramApiClient("http://api:8080", urlopen_impl=fake_http_error)
    with pytest.raises(TelegramApiClientError) as conflict_error:
        conflict_client.list_media_items(owner=OWNER)

    assert conflict_error.value.status == 409
    assert conflict_error.value.code == "conflict"
    assert str(conflict_error.value) == "stale"

    def fake_non_json_error(request):
        raise HTTPError(
            request.full_url,
            500,
            "Boom",
            hdrs=None,
            fp=io.BytesIO(b"not-json"),
        )

    non_json_client = TelegramApiClient("http://api:8080", urlopen_impl=fake_non_json_error)
    with pytest.raises(TelegramApiClientError) as non_json_error:
        non_json_client.list_analysis_runs(owner=OWNER)

    assert non_json_error.value.status == 500
    assert non_json_error.value.code is None
    assert str(non_json_error.value) == "API request failed with status 500"

    with pytest.raises(RuntimeError, match="media_item"):
        empty_client._extract({}, "media_item")
