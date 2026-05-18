# FILE: workers/common/tests/test_api_transport.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the packet-local transport and validation branches of the worker-common API client.
# SCOPE: URL building, urllib transport success and failure paths, and input validation for shared worker-control helpers.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local transport and validation coverage for the worker-common API client.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   DummyResponse - Minimal context-manager response used to exercise urllib transport branches.
#   test_build_url_normalizes_paths_and_query - Verifies URL normalization and query shaping.
#   test_urllib_transport_serializes_json_payload_and_parses_response - Verifies JSON request and response behavior.
#   test_urllib_transport_handles_empty_body_and_failures - Verifies empty-body and transport failure branches.
#   test_shared_api_client_validation_rejects_invalid_inputs - Verifies deterministic contract validation failures.
# END_MODULE_MAP

from __future__ import annotations

from types import SimpleNamespace
from urllib import error

import pytest

import transcriber_workers_common.api as api_module
from transcriber_workers_common.api import InternalApiConfig, InternalApiUnavailableError, AnalysisRunControlClient


class DummyResponse:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> "DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self.body


def _claim_payload(*, size_bytes: int = 42, unexpected: bool = False) -> dict[str, object]:
    payload: dict[str, object] = {
        "analysis_run_step_id": "exec-1",
        "analysis_run_id": "11111111-1111-1111-1111-111111111111",
        "run_type": "transcription",
        "selection_snapshot": {
            "selection_snapshot_id": "22222222-2222-2222-2222-222222222222",
            "items": [
                {
                    "selection_snapshot_item_id": "selection-snapshot-item-0",
                    "position": 0,
                    "media_asset_id": "33333333-3333-3333-3333-333333333333",
                    "kind": "audio",
                    "labels": {"display_label": "source.wav"},
                    "origin_snapshot": {
                        "origin_type": "telegram_file",
                        "object_ref": "media/source.wav",
                        "content_type": "audio/wav",
                    },
                    "storage_snapshot": {
                        "stored_object_id": "44444444-4444-4444-4444-444444444444",
                        "object_key": "media/source.wav",
                        "content_type": "audio/wav",
                        "size_bytes": size_bytes,
                    },
                    "display_name": "source.wav",
                    "status_at_selection": "available",
                }
            ],
            "option_snapshot": {},
            "sealed_at": "2026-05-10T12:00:00Z",
        },
        "analysis_run_step_inputs": [
            {
                "analysis_run_step_input_id": "input-1",
                "analysis_run_step_id": "exec-1",
                "input_kind": "selection_snapshot_item",
                "selection_snapshot_item_id": "selection-snapshot-item-0",
                "position": 0,
                "required": True,
            }
        ],
        "params": {},
        "claimed_at": "2026-05-10T12:01:00Z",
    }
    if unexpected:
        payload["unexpected"] = True
    return payload


def test_build_url_normalizes_paths_and_query() -> None:
    config = InternalApiConfig(base_url="http://internal.local/")

    assert (
        config.build_url("internal/v1/analysis-runs/run-1/steps/claim")
        == "http://internal.local/internal/v1/analysis-runs/run-1/steps/claim"
    )
    assert (
        config.build_url("/internal/v1/analysis-runs/run-1/steps/cancel-check", query={"analysis_run_step_id": "exec-1"})
        == "http://internal.local/internal/v1/analysis-runs/run-1/steps/cancel-check?analysis_run_step_id=exec-1"
    )


def test_urllib_transport_serializes_json_payload_and_parses_response(monkeypatch) -> None:
    captured: dict[str, object] = {}
    transport = api_module._UrllibJsonTransport(timeout_seconds=12.5, headers={"X-Test": "demo"})

    def fake_urlopen(http_request, timeout: float):
        captured["url"] = http_request.full_url
        captured["timeout"] = timeout
        captured["content_type"] = http_request.headers.get("Content-type")
        captured["accept"] = http_request.headers.get("Accept")
        captured["x_test"] = http_request.headers.get("X-test")
        captured["payload"] = http_request.data
        return DummyResponse(b'{"status":"ok"}')

    monkeypatch.setattr(api_module.request, "urlopen", fake_urlopen)

    response = transport.request(
        method="POST",
        url="http://internal.local/internal/v1/analysis-runs/run-1/steps/claim",
        payload={"worker_kind": "transcription"},
    )

    assert response == {"status": "ok"}
    assert captured == {
        "url": "http://internal.local/internal/v1/analysis-runs/run-1/steps/claim",
        "timeout": 12.5,
        "content_type": "application/json",
        "accept": "application/json",
        "x_test": "demo",
        "payload": b'{"worker_kind": "transcription"}',
    }


def test_urllib_transport_handles_empty_body_and_failures(monkeypatch) -> None:
    transport = api_module._UrllibJsonTransport(timeout_seconds=5.0, headers={})

    monkeypatch.setattr(api_module.request, "urlopen", lambda http_request, timeout: DummyResponse(b""))
    assert transport.request(method="GET", url="http://internal.local/health") is None

    monkeypatch.setattr(api_module.request, "urlopen", lambda http_request, timeout: DummyResponse(b"{bad json"))
    with pytest.raises(ValueError, match="malformed JSON"):
        transport.request(method="GET", url="http://internal.local/health")

    monkeypatch.setattr(api_module.request, "urlopen", lambda http_request, timeout: (_ for _ in ()).throw(error.URLError("down")))
    with pytest.raises(InternalApiUnavailableError, match="down"):
        transport.request(method="GET", url="http://internal.local/health")

    monkeypatch.setattr(api_module.request, "urlopen", lambda http_request, timeout: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(InternalApiUnavailableError, match="timed out"):
        transport.request(method="GET", url="http://internal.local/health")


def test_shared_api_client_validation_rejects_invalid_inputs() -> None:
    client = AnalysisRunControlClient(InternalApiConfig(base_url="http://internal.local"), transport=SimpleNamespace(request=lambda **kwargs: None))

    with pytest.raises(ValueError, match="invalid worker_kind"):
        client.claim_analysis_run_step("run-1", worker_kind="unknown", step_kind="selection.transcription")
    with pytest.raises(ValueError, match="progress_stage"):
        client.publish_progress("job-1", analysis_run_step_id="exec-1", progress_stage="   ")
    with pytest.raises(ValueError, match="artifacts must not be empty"):
        client.register_artifacts("job-1", analysis_run_step_id="exec-1", artifacts=[])
    with pytest.raises(ValueError, match="invalid worker outcome"):
        client.finalize_analysis_run("job-1", analysis_run_step_id="exec-1", outcome="unknown")


def test_claim_analysis_run_passes_through_internal_api_unavailable() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(request=lambda **kwargs: (_ for _ in ()).throw(InternalApiUnavailableError("offline"))),
    )

    with pytest.raises(InternalApiUnavailableError, match="offline"):
        client.claim_analysis_run_step("run-1", worker_kind="transcription", step_kind="selection.transcription")


def test_claim_analysis_run_rejects_unexpected_and_invalid_fields() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(
            request=lambda **kwargs: _claim_payload(unexpected=True)
        ),
    )

    with pytest.raises(ValueError, match="unexpected field"):
        client.claim_analysis_run_step("run-1", worker_kind="transcription", step_kind="selection.transcription")

    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(
            request=lambda **kwargs: _claim_payload(size_bytes=-1)
        ),
    )

    with pytest.raises(ValueError, match="size_bytes"):
        client.claim_analysis_run_step("run-1", worker_kind="transcription", step_kind="selection.transcription")


def test_check_cancel_rejects_invalid_status() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(
            request=lambda **kwargs: {
                "cancel_requested": False,
                "status": "invalid",
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid cancel-check status"):
        client.check_cancel("job-1", analysis_run_step_id="exec-1")
