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
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

import telegram_adapter.api_client as api_client_module
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


CHANNEL_IDENTITY = {
    "channel": "telegram",
    "external_account_ref": "chat:10:user:7",
    "adapter_identity": {"telegram_chat_id": "10", "telegram_user_id": "7"},
}


def test_default_api_transport_has_bounded_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_urlopen(request, *, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        raise TimeoutError("stalled API request")

    monkeypatch.setattr(api_client_module, "urlopen", fake_urlopen)
    client = TelegramApiClient("http://api:8080")

    with pytest.raises(TelegramApiClientError, match="Backend is unavailable") as error:
        client.heartbeat_export_delivery(
            channel_account_id="channel-account-1",
            export_job_id="job-1",
            export_delivery_id="delivery-1",
            lease_owner="adapter",
            attempt_token="t" * 16,
        )

    assert captured["timeout"] == 15.0
    assert captured["request"].full_url.endswith("/v1/export-jobs/job-1/deliveries/heartbeat")
    assert error.value.status == 0


def test_create_media_asset_posts_target_media_asset_payload() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"media_asset": {"media_asset_id": "media-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    asset = client.create_media_asset(
        channel_account_id="channel-account-1",
        kind="text",
        origin={"origin_type": "text", "origin_ref": "hello"},
        display_name="hello",
        metadata={"message_id": 42},
    )

    payload = json.loads(captured["request"].data.decode("utf-8"))
    assert asset == {"media_asset_id": "media-1"}
    assert captured["request"].full_url == "http://localhost:8080/v1/media-assets"
    assert payload["channel_account_id"] == "channel-account-1"
    assert payload["kind"] == "text"
    assert payload["origin"] == {"origin_type": "text", "origin_ref": "hello"}
    assert payload["metadata"] == {"message_id": 42}


def test_upload_media_asset_posts_multipart_target_payload() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"media_asset": {"media_asset_id": "media-2"}}).encode("utf-8"))

    client = TelegramApiClient("http://localhost:8080", urlopen_impl=fake_urlopen)
    asset = client.upload_media_asset(
        channel_account_id="channel-account-1",
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
    assert asset == {"media_asset_id": "media-2"}
    assert request.full_url == "http://localhost:8080/v1/media-assets/upload"
    assert content_type.startswith("multipart/form-data; boundary=")
    assert b'"channel_account_id": "channel-account-1"' in body
    assert b'"kind": "voice"' in body
    assert b'"display_name": "voice.ogg"' in body
    assert b'"message_id": 42' in body
    assert b"voice-bytes" in body


def test_upload_media_asset_from_path_passes_a_file_stream_to_http_client(tmp_path: Path) -> None:
    path = tmp_path / "large-video.mp4"
    path.write_bytes(b"x" * 1024 * 1024)
    captured: dict[str, object] = {}

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"media_asset": {"media_asset_id": "media-stream"}}

    class Client:
        def __init__(self, **kwargs: object) -> None:
            captured["timeout"] = kwargs["timeout"]

        def __enter__(self) -> "Client":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def post(self, url: str, **kwargs: object) -> Response:
            captured["url"] = url
            captured["file"] = kwargs["files"]["file"][1]  # type: ignore[index]
            return Response()

    client = TelegramApiClient(
        "http://api:8080",
        urlopen_impl=lambda request: (_ for _ in ()).throw(AssertionError("byte multipart path must not be used")),
        http_client_factory=Client,
    )
    asset = client.upload_media_asset(
        channel_account_id="channel-account-1",
        kind="video",
        file_path=path,
        file_name="large-video.mp4",
        content_type="video/mp4",
    )

    stream = captured["file"]
    assert asset == {"media_asset_id": "media-stream"}
    assert captured["url"] == "http://api:8080/v1/media-assets/upload"
    assert getattr(stream, "name") == str(path)
    assert getattr(stream, "closed")


def test_upload_media_asset_from_anonymous_stream_does_not_close_caller_handle() -> None:
    captured: dict[str, object] = {}

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"media_asset": {"media_asset_id": "media-stream"}}

    class Client:
        def __init__(self, **kwargs: object) -> None:
            pass

        def __enter__(self) -> "Client":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def post(self, url: str, **kwargs: object) -> Response:
            stream = kwargs["files"]["file"][1]  # type: ignore[index]
            captured["stream"] = stream
            captured["body"] = stream.read()
            return Response()

    stream = io.BytesIO(b"disk-backed-input")
    stream.seek(5)
    client = TelegramApiClient("http://api:8080", http_client_factory=Client)

    asset = client.upload_media_asset(
        channel_account_id="channel-account-1",
        kind="video",
        file_handle=stream,
        file_name="video.mp4",
        content_type="video/mp4",
    )

    assert asset == {"media_asset_id": "media-stream"}
    assert captured["stream"] is stream
    assert captured["body"] == b"disk-backed-input"
    assert not stream.closed


def test_remove_collection_item_uses_channel_account_query_and_expected_version() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"collection": {"collection_id": "inbox-1", "version": 3}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    client.remove_collection_item(
        channel_account_id="channel-account-1",
        collection_id="inbox-1",
        media_asset_id="media-1",
        expected_version=2,
    )

    request = captured["request"]
    assert request.get_method() == "DELETE"
    assert request.full_url == (
        "http://api:8080/v1/collections/inbox-1/items/media-1"
        "?channel_account_id=channel-account-1&expected_version=2"
    )


def test_create_selection_snapshot_and_analysis_run_use_target_identifiers() -> None:
    requests = []

    def fake_urlopen(request):
        requests.append(request)
        if request.full_url.endswith("/v1/selection-snapshots"):
            return FakeHttpResponse(
                json.dumps({"selection_snapshot": {"selection_snapshot_id": "snapshot-1"}}).encode("utf-8")
            )
        return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-1"}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    snapshot = client.create_selection_snapshot(
        channel_account_id="channel-account-1",
        source_collection_id="inbox-1",
        items=[{"media_asset_id": "media-1", "position": 0}],
        option_snapshot={"channel": "telegram"},
    )
    run = client.create_analysis_run(
        channel_account_id="channel-account-1",
        selection_snapshot_id=snapshot["selection_snapshot_id"],
    )

    snapshot_payload = json.loads(requests[0].data.decode("utf-8"))
    run_payload = json.loads(requests[1].data.decode("utf-8"))
    assert snapshot == {"selection_snapshot_id": "snapshot-1"}
    assert run == {"analysis_run_id": "run-1"}
    assert snapshot_payload["channel_account_id"] == "channel-account-1"
    assert snapshot_payload["source_collection_id"] == "inbox-1"
    assert snapshot_payload["items"] == [{"media_asset_id": "media-1", "position": 0}]
    assert snapshot_payload["option_snapshot"] == {"channel": "telegram"}
    assert snapshot_payload["created_via_channel_account_id"] == "channel-account-1"
    assert run_payload == {
        "channel_account_id": "channel-account-1",
        "selection_snapshot_id": "snapshot-1",
        "run_type": "transcription",
        "delivery": {"strategy": "polling"},
        "created_via_channel_id": "channel-account-1",
    }


def test_start_collection_processing_run_uses_semantic_contract_and_stable_header() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(
            json.dumps(
                {
                    "selection_snapshot": {"selection_snapshot_id": "snapshot-1"},
                    "analysis_run": {"analysis_run_id": "run-1", "steps": []},
                    "detached_media_asset_ids": ["media-2", "media-1"],
                    "collection_version": 8,
                }
            ).encode("utf-8")
        )

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    result = client.start_collection_processing_run(
        channel_account_id="channel-account-1",
        collection_id="inbox-1",
        expected_version=7,
        items=[
            {"media_asset_id": "media-2", "position": 0},
            {"media_asset_id": "media-1", "position": 1},
        ],
        run_type="report",
        option_snapshot={"language": "ru"},
    )

    request = captured["request"]
    assert result["collection_version"] == 8
    assert request.headers["Idempotency-key"] == "processing:inbox-1:7:report"
    assert json.loads(request.data.decode("utf-8")) == {
        "channel_account_id": "channel-account-1",
        "expected_version": 7,
        "selected_item_ids": ["media-2", "media-1"],
        "run_type": "report",
        "options": {"language": "ru"},
        "created_via_channel_account_id": "channel-account-1",
    }


def test_restore_reads_inbox_media_assets_and_runs_with_channel_account_query() -> None:
    urls = []

    def fake_urlopen(request):
        urls.append(request.full_url)
        if "/v1/collections/inbox" in request.full_url:
            return FakeHttpResponse(json.dumps({"collection": {"collection_id": "inbox-1"}}).encode("utf-8"))
        if "/v1/media-assets" in request.full_url:
            return FakeHttpResponse(json.dumps({"items": [], "page": {"page_size": 5, "has_more": False}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"items": [], "page": {"page_size": 10, "has_more": False}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    client.get_inbox_collection(channel_account_id="channel-account-1")
    client.list_media_assets(channel_account_id="channel-account-1", page_size=5)
    client.list_analysis_runs(channel_account_id="channel-account-1", page_size=10)

    assert urls == [
        "http://api:8080/v1/collections/inbox?channel_account_id=channel-account-1",
        "http://api:8080/v1/media-assets?channel_account_id=channel-account-1&page_size=5",
        "http://api:8080/v1/analysis-runs?channel_account_id=channel-account-1&page_size=10",
    ]


def test_get_analysis_run_uses_channel_account_query_and_extracts_wrapped_object() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["url"] = request.full_url
        return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-1", "status": "queued"}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    run = client.get_analysis_run(channel_account_id="channel-account-1", analysis_run_id="run-1")

    assert run == {"analysis_run_id": "run-1", "status": "queued"}
    assert captured["url"] == "http://api:8080/v1/analysis-runs/run-1?channel_account_id=channel-account-1"


def test_export_client_uses_export_and_fenced_delivery_contracts() -> None:
    requests = []

    def fake_urlopen(request):
        requests.append(request)
        if request.full_url.endswith("/exports"):
            return FakeHttpResponse(json.dumps({"export_job": {"export_job_id": "job-1", "status": "queued"}}).encode())
        if request.full_url.endswith("/deliveries/claim"):
            return FakeHttpResponse(json.dumps({"delivery": {"export_delivery_id": "delivery-1"}, "attempt_token": "t" * 16, "lease_owner": "bot"}).encode())
        if "/download-access?" in request.full_url:
            return FakeHttpResponse(json.dumps({"filename": "clip.mp4", "url": "http://minio:9000/clip.mp4"}).encode())
        if "/download?" in request.full_url:
            return FakeHttpResponse(json.dumps({"filename": "clip.mp4", "url": "http://files/clip.mp4"}).encode())
        return FakeHttpResponse(json.dumps({"delivery": {"status": "delivered"}}).encode())

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    job = client.create_export_job(channel_account_id="channel-account-1", media_asset_id="media-1", operation="youtube_video", variant={"video_quality": "720p"}, idempotency_key="export-key")
    claim = client.claim_export_delivery(channel_account_id="channel-account-1", export_job_id="job-1", lease_owner="bot")
    client.acknowledge_export_delivery(channel_account_id="channel-account-1", export_job_id="job-1", export_delivery_id="delivery-1", lease_owner="bot", attempt_token="t" * 16)
    download = client.get_export_download(channel_account_id="channel-account-1", export_job_id="job-1")
    internal_download = client.get_internal_export_download_access(
        channel_account_id="channel-account-1", export_job_id="job-1"
    )

    assert job["export_job_id"] == "job-1"
    assert claim["delivery"]["export_delivery_id"] == "delivery-1"
    assert download["filename"] == "clip.mp4"
    assert internal_download["url"] == "http://minio:9000/clip.mp4"
    assert requests[4].full_url == (
        "http://api:8080/internal/v1/export-jobs/job-1/download-access"
        "?channel_account_id=channel-account-1"
    )
    assert requests[0].headers["Idempotency-key"] == "export-key"
    assert json.loads(requests[0].data.decode()) == {"channel_account_id": "channel-account-1", "operation": "youtube_video", "variant": {"video_quality": "720p"}, "delivery_channel": "telegram"}


def test_export_delivery_heartbeat_renews_the_fenced_claim() -> None:
    captured = {}
    response_claim = {
        "delivery": {"export_delivery_id": "delivery-1"},
        "attempt_token": "t" * 16,
        "lease_owner": "bot",
    }

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps(response_claim).encode())

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    result = client.heartbeat_export_delivery(
        channel_account_id="channel-account-1",
        export_job_id="job-1",
        export_delivery_id="delivery-1",
        lease_owner="bot",
        attempt_token="t" * 16,
        lease_seconds=120,
    )

    request = captured["request"]
    assert result == response_claim
    assert request.full_url == "http://api:8080/v1/export-jobs/job-1/deliveries/heartbeat"
    assert json.loads(request.data.decode()) == {
        "channel_account_id": "channel-account-1",
        "export_delivery_id": "delivery-1",
        "lease_owner": "bot",
        "attempt_token": "t" * 16,
        "lease_seconds": 120,
    }


def test_list_analysis_run_events_uses_channel_account_query_and_page_size() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["url"] = request.full_url
        return FakeHttpResponse(
            json.dumps(
                {
                    "items": [
                        {
                            "analysis_run_event_id": "event-1",
                            "analysis_run_id": "run-1",
                            "event_type": "analysis_run_step.progress",
                            "payload": {"progress_stage": "transcribing"},
                        }
                    ],
                    "page": {"page_size": 5, "has_more": False},
                }
            ).encode("utf-8")
        )

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    events = client.list_analysis_run_events(channel_account_id="channel-account-1", analysis_run_id="run-1", page_size=5)

    assert events["items"][0]["payload"]["progress_stage"] == "transcribing"
    assert captured["url"] == (
        "http://api:8080/v1/analysis-runs/run-1/events"
        "?channel_account_id=channel-account-1&page_size=5"
    )


def test_cancel_analysis_run_posts_channel_account_and_message() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["request"] = request
        return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-1", "status": "canceled"}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    run = client.cancel_analysis_run(
        channel_account_id="channel-account-1",
        analysis_run_id="run-1",
        message="stop from Telegram",
    )

    request = captured["request"]
    payload = json.loads(request.data.decode("utf-8"))
    assert request.get_method() == "POST"
    assert request.full_url == "http://api:8080/v1/analysis-runs/run-1/cancel"
    assert payload == {"channel_account_id": "channel-account-1", "message": "stop from Telegram"}
    assert run == {"analysis_run_id": "run-1", "status": "canceled"}


def test_get_internal_artifact_download_access_uses_internal_endpoint_without_channel_account_query() -> None:
    captured = {}

    def fake_urlopen(request):
        captured["url"] = request.full_url
        return FakeHttpResponse(
            json.dumps(
                {
                    "artifact_id": "artifact-1",
                    "filename": "transcript.txt",
                    "mime_type": "text/plain",
                    "download": {"url": "http://minio:9000/artifacts/run-1/transcript.txt"},
                }
            ).encode("utf-8")
        )

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    access = client.get_internal_artifact_download_access(artifact_id="artifact-1")

    assert access["artifact_id"] == "artifact-1"
    assert access["download"]["url"] == "http://minio:9000/artifacts/run-1/transcript.txt"
    assert captured["url"] == "http://api:8080/internal/v1/artifacts/artifact-1/download-access"


def test_channel_account_and_surface_internal_methods_use_target_contracts() -> None:
    requests = []

    def fake_urlopen(request):
        requests.append(request)
        if request.full_url.endswith("/internal/v1/channel-accounts") and request.get_method() == "PUT":
            return FakeHttpResponse(json.dumps({"channel_account": {"channel_account_id": "channel-account-1"}}).encode("utf-8"))
        if "/internal/v1/channel-accounts" in request.full_url and request.get_method() == "GET":
            return FakeHttpResponse(json.dumps({"items": [{"channel_account_id": "channel-account-1"}]}).encode("utf-8"))
        if request.full_url.endswith("/internal/v1/channel-surfaces") and request.get_method() == "PUT":
            return FakeHttpResponse(json.dumps({"channel_surface": {"channel_surface_id": "surface-1", "version": 1}}).encode("utf-8"))
        if "/display-state" in request.full_url:
            return FakeHttpResponse(json.dumps({"channel_surface": {"channel_surface_id": "surface-1", "version": 2}}).encode("utf-8"))
        if "/supersede" in request.full_url:
            return FakeHttpResponse(json.dumps({"channel_surface_event": {"channel_surface_event_id": "event-1"}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"items": [{"channel_surface_id": "surface-1"}]}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    account = client.resolve_channel_account(channel_identity=CHANNEL_IDENTITY)
    accounts = client.list_channel_accounts(page_size=50)
    surface = client.upsert_channel_surface(
        channel_account_id="channel-account-1",
        surface_type="analysis_task_surface",
        surface_key="analysis_run:run-1",
        address={"chat_id": 10, "message_id": 42},
        address_fingerprint="telegram:10:42",
        display_state={"status": "queued"},
        subjects=[{"subject_type": "analysis_run", "subject_id": "run-1", "subject_role": "primary"}],
    )
    surfaces = client.list_channel_surfaces(
        channel_account_id="channel-account-1",
        subject_type="analysis_run",
        subject_id="run-1",
        active_only=True,
        page_size=10,
    )
    replaced = client.replace_channel_surface_display_state(
        channel_surface_id="surface-1",
        expected_version=1,
        display_state={"status": "running"},
    )
    event = client.supersede_channel_surface(channel_surface_id="surface-1", reason="message_not_editable")

    resolve_payload = json.loads(requests[0].data.decode("utf-8"))
    surface_payload = json.loads(requests[2].data.decode("utf-8"))
    replace_payload = json.loads(requests[4].data.decode("utf-8"))
    supersede_payload = json.loads(requests[5].data.decode("utf-8"))

    assert account == {"channel_account_id": "channel-account-1"}
    assert accounts["items"][0]["channel_account_id"] == "channel-account-1"
    assert surface == {"channel_surface_id": "surface-1", "version": 1}
    assert surfaces["items"][0]["channel_surface_id"] == "surface-1"
    assert replaced["version"] == 2
    assert event["channel_surface_event_id"] == "event-1"
    assert requests[0].full_url == "http://api:8080/internal/v1/channel-accounts"
    assert resolve_payload["channel"] == "telegram"
    assert resolve_payload["external_account_ref"] == CHANNEL_IDENTITY["external_account_ref"]
    assert resolve_payload["metadata"]["channel_identity"] == CHANNEL_IDENTITY
    assert requests[1].full_url == "http://api:8080/internal/v1/channel-accounts?page_size=50"
    assert requests[2].full_url == "http://api:8080/internal/v1/channel-surfaces"
    assert surface_payload["surface_type"] == "analysis_task_surface"
    assert surface_payload["subjects"][0]["subject_type"] == "analysis_run"
    assert requests[3].full_url == (
        "http://api:8080/internal/v1/channel-surfaces/active"
        "?channel_account_id=channel-account-1&subject_type=analysis_run&subject_id=run-1&page_size=10"
    )
    assert requests[4].full_url == "http://api:8080/internal/v1/channel-surfaces/surface-1/display-state"
    assert replace_payload == {"expected_version": 1, "display_state": {"status": "running"}, "actor_type": "telegram_adapter"}
    assert requests[5].full_url == "http://api:8080/internal/v1/channel-surfaces/surface-1/supersede"
    assert supersede_payload == {"reason": "message_not_editable", "actor_type": "telegram_adapter"}


def test_optional_idempotency_actor_and_surface_fields_are_forwarded() -> None:
    requests = []

    def fake_urlopen(request):
        requests.append(request)
        if request.full_url.endswith("/v1/media-assets"):
            return FakeHttpResponse(json.dumps({"media_asset": {"media_asset_id": "media-1"}}).encode("utf-8"))
        if request.full_url.endswith("/v1/media-assets/upload"):
            return FakeHttpResponse(json.dumps({"media_asset": {"media_asset_id": "media-2"}}).encode("utf-8"))
        if request.full_url.endswith("/v1/selection-snapshots"):
            return FakeHttpResponse(json.dumps({"selection_snapshot": {"selection_snapshot_id": "snapshot-1"}}).encode("utf-8"))
        if request.full_url.endswith("/v1/analysis-runs"):
            return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-1"}}).encode("utf-8"))
        if request.full_url.endswith("/internal/v1/channel-surfaces"):
            return FakeHttpResponse(json.dumps({"channel_surface": {"channel_surface_id": "surface-1"}}).encode("utf-8"))
        if "/display-state" in request.full_url:
            return FakeHttpResponse(json.dumps({"channel_surface": {"channel_surface_id": "surface-1", "version": 2}}).encode("utf-8"))
        if "/supersede" in request.full_url:
            return FakeHttpResponse(json.dumps({"channel_surface_event": {"channel_surface_event_id": "event-1"}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"items": []}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    client.create_media_asset(
        channel_account_id="channel-account-1",
        kind="text",
        origin={"origin_type": "text", "origin_ref": "hello"},
        idempotency_key="media-key",
    )
    client.upload_media_asset(
        channel_account_id="channel-account-1",
        kind="voice",
        content=b"voice",
        file_name="voice.ogg",
        idempotency_key="upload-key",
    )
    client.create_selection_snapshot(
        channel_account_id="channel-account-1",
        items=[{"media_asset_id": "media-1", "position": 0}],
        idempotency_key="snapshot-key",
    )
    client.create_analysis_run(
        channel_account_id="channel-account-1",
        selection_snapshot_id="snapshot-1",
        idempotency_key="run-key",
    )
    client.upsert_channel_surface(
        channel_account_id="channel-account-1",
        surface_type="analysis_task_surface",
        surface_key="analysis_run:run-1",
        address={"chat_id": 10, "message_id": 42},
        display_state={"status": "queued"},
        idempotency_key="surface-key",
    )
    client.list_channel_surfaces(
        channel_account_id="channel-account-1",
        lifecycle_status="active",
        page_size=8,
    )
    client.replace_channel_surface_display_state(
        channel_surface_id="surface-1",
        expected_version=1,
        display_state={"status": "running"},
        actor_id="adapter",
        metadata={"reason": "poll"},
    )
    client.supersede_channel_surface(
        channel_surface_id="surface-1",
        reason="message_not_editable",
        actor_id="adapter",
        metadata={"reason": "fallback"},
    )

    media_payload = json.loads(requests[0].data.decode("utf-8"))
    selection_payload = json.loads(requests[2].data.decode("utf-8"))
    run_payload = json.loads(requests[3].data.decode("utf-8"))
    surface_payload = json.loads(requests[4].data.decode("utf-8"))
    replace_payload = json.loads(requests[6].data.decode("utf-8"))
    supersede_payload = json.loads(requests[7].data.decode("utf-8"))

    assert media_payload["idempotency_key"] == "media-key"
    assert b'"idempotency_key": "upload-key"' in requests[1].data
    assert selection_payload["idempotency_key"] == "snapshot-key"
    assert run_payload["idempotency_key"] == "run-key"
    assert surface_payload["idempotency_key"] == "surface-key"
    assert requests[5].full_url == (
        "http://api:8080/internal/v1/channel-surfaces"
        "?channel_account_id=channel-account-1&lifecycle_status=active&page_size=8"
    )
    assert replace_payload["actor_id"] == "adapter"
    assert replace_payload["metadata"] == {"reason": "poll"}
    assert supersede_payload["actor_id"] == "adapter"
    assert supersede_payload["metadata"] == {"reason": "fallback"}


def test_backend_connection_failure_is_categorized_without_raw_exception_copy() -> None:
    def fake_urlopen(request):
        raise URLError("Connection refused at 127.0.0.1:8080")

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)

    with pytest.raises(TelegramApiClientError) as error:
        client.list_media_assets(channel_account_id="channel-account-1", page_size=5)

    user_error = classify_user_error(error.value)
    copy = user_error_text(error.value)
    assert error.value.code == "backend_unavailable"
    assert user_error.code == TelegramUserErrorCode.BACKEND_UNAVAILABLE
    assert "Connection refused" not in copy
    assert "127.0.0.1" not in copy
    assert "inbox уже сохранено" not in copy
    assert "уже сохранено" not in copy
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
    owner_with_tenant = {**CHANNEL_IDENTITY, "partition_ref": "tenant-1"}

    def fake_urlopen(request):
        captured_requests.append(request)
        captured_urls.append(request.full_url)
        if request.full_url.endswith("/v1/media-assets"):
            return FakeHttpResponse(json.dumps({"media_asset": {"media_asset_id": "media-9"}}).encode("utf-8"))
        if request.full_url.endswith("/v1/media-assets/upload"):
            return FakeHttpResponse(json.dumps({"media_asset": {"media_asset_id": "media-upload-9"}}).encode("utf-8"))
        if request.full_url.endswith("/v1/analysis-runs"):
            return FakeHttpResponse(json.dumps({"analysis_run": {"analysis_run_id": "run-9"}}).encode("utf-8"))
        if "/v1/artifacts/" in request.full_url:
            return FakeHttpResponse(json.dumps({"artifact": {"artifact_id": "artifact-9"}}).encode("utf-8"))
        return FakeHttpResponse(json.dumps({"items": [], "page": {"page_size": 3, "has_more": False}}).encode("utf-8"))

    client = TelegramApiClient("http://api:8080", urlopen_impl=fake_urlopen)
    media_asset = client.create_media_asset(
        channel_account_id="channel-account-9",
        kind="text",
        origin={"origin_type": "text", "origin_ref": "hello"},
        collection_id="inbox-9",
    )
    uploaded_asset = client.upload_media_asset(
        channel_account_id="channel-account-9",
        kind="audio",
        content=b"audio-body",
        file_name="note.bin",
        collection_id="inbox-9",
    )
    run = client.create_analysis_run(
        channel_account_id="channel-account-9",
        selection_snapshot_id="selection-snapshot-9",
        params={"mode": "deep"},
    )
    list_media = client.list_media_assets(
        channel_account_id="channel-account-9",
        cursor="media-cursor",
        page_size=7,
        status="ready",
        kind="text",
    )
    list_runs = client.list_analysis_runs(
        channel_account_id="channel-account-9",
        cursor="run-cursor",
        page_size=4,
        status="queued",
        run_type="research",
    )
    list_artifacts = client.list_artifacts(
        channel_account_id="channel-account-9",
        analysis_run_id="run-9",
        cursor="artifact-cursor",
        page_size=3,
    )
    artifact = client.get_artifact(channel_account_id="channel-account-9", artifact_id="artifact-9")
    diagnostics = client.list_diagnostics(
        channel_account_id="channel-account-9",
        subject_type="analysis_run",
        subject_id="run-9",
        cursor="diagnostic-cursor",
        page_size=2,
    )

    add_payload = json.loads(captured_requests[0].data.decode("utf-8"))
    upload_request = captured_requests[1]
    run_payload = json.loads(captured_requests[2].data.decode("utf-8"))

    assert media_asset == {"media_asset_id": "media-9"}
    assert uploaded_asset == {"media_asset_id": "media-upload-9"}
    assert run == {"analysis_run_id": "run-9"}
    assert list_media["page"]["page_size"] == 3
    assert list_runs["page"]["page_size"] == 3
    assert list_artifacts["page"]["page_size"] == 3
    assert artifact == {"artifact_id": "artifact-9"}
    assert diagnostics["page"]["page_size"] == 3
    assert add_payload["collection_id"] == "inbox-9"
    assert add_payload["channel_account_id"] == "channel-account-9"
    assert b'"collection_id": "inbox-9"' in upload_request.data
    assert b'"channel_account_id": "channel-account-9"' in upload_request.data
    assert b"Content-Type: application/octet-stream" in upload_request.data
    assert run_payload["params"] == {"mode": "deep"}
    assert "cursor=media-cursor" in captured_urls[3]
    assert "channel_account_id=channel-account-9" in captured_urls[3]
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
        conflict_client.list_media_assets(channel_account_id="channel-account-1")

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
        non_json_client.list_analysis_runs(channel_account_id="channel-account-1")

    assert non_json_error.value.status == 500
    assert non_json_error.value.code is None
    assert str(non_json_error.value) == "API request failed with status 500"

    with pytest.raises(RuntimeError, match="media_asset"):
        empty_client._extract({}, "media_asset")
