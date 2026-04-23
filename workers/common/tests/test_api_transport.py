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
from transcriber_workers_common.api import InternalApiConfig, InternalApiUnavailableError, JobApiClient


class DummyResponse:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> "DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self.body


def test_build_url_normalizes_paths_and_query() -> None:
    config = InternalApiConfig(base_url="http://internal.local/")

    assert config.build_url("internal/v1/jobs/job-1/claim") == "http://internal.local/internal/v1/jobs/job-1/claim"
    assert (
        config.build_url("/internal/v1/jobs/job-1/cancel-check", query={"execution_id": "exec-1"})
        == "http://internal.local/internal/v1/jobs/job-1/cancel-check?execution_id=exec-1"
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
        url="http://internal.local/internal/v1/jobs/job-1/claim",
        payload={"worker_kind": "transcription"},
    )

    assert response == {"status": "ok"}
    assert captured == {
        "url": "http://internal.local/internal/v1/jobs/job-1/claim",
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
    client = JobApiClient(InternalApiConfig(base_url="http://internal.local"), transport=SimpleNamespace(request=lambda **kwargs: None))

    with pytest.raises(ValueError, match="invalid worker_kind"):
        client.claim_job("job-1", worker_kind="unknown", task_type="transcription.run")
    with pytest.raises(ValueError, match="progress_stage"):
        client.publish_progress("job-1", execution_id="exec-1", progress_stage="   ")
    with pytest.raises(ValueError, match="artifacts must not be empty"):
        client.register_artifacts("job-1", execution_id="exec-1", artifacts=[])
    with pytest.raises(ValueError, match="invalid worker outcome"):
        client.finalize_job("job-1", execution_id="exec-1", outcome="unknown")


def test_claim_job_passes_through_internal_api_unavailable() -> None:
    client = JobApiClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(request=lambda **kwargs: (_ for _ in ()).throw(InternalApiUnavailableError("offline"))),
    )

    with pytest.raises(InternalApiUnavailableError, match="offline"):
        client.claim_job("job-1", worker_kind="transcription", task_type="transcription.run")


def test_claim_job_rejects_unexpected_and_invalid_fields() -> None:
    client = JobApiClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(
            request=lambda **kwargs: {
                "execution_id": "exec-1",
                "job_id": "job-1",
                "root_job_id": "root-1",
                "parent_job_id": None,
                "retry_of_job_id": None,
                "job_type": "transcription",
                "version": 1,
                "ordered_inputs": [
                    {
                        "position": 0,
                        "source_id": "source-1",
                        "source_kind": "youtube_url",
                        "size_bytes": -1,
                    }
                ],
                "params": {},
                "unexpected": True,
            }
        ),
    )

    with pytest.raises(ValueError, match="unexpected field"):
        client.claim_job("job-1", worker_kind="transcription", task_type="transcription.run")

    client = JobApiClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(
            request=lambda **kwargs: {
                "execution_id": "exec-1",
                "job_id": "job-1",
                "root_job_id": "root-1",
                "parent_job_id": None,
                "retry_of_job_id": None,
                "job_type": "transcription",
                "version": 1,
                "ordered_inputs": [
                    {
                        "position": 0,
                        "source_id": "source-1",
                        "source_kind": "youtube_url",
                        "size_bytes": -1,
                    }
                ],
                "params": {},
            }
        ),
    )

    with pytest.raises(ValueError, match="size_bytes"):
        client.claim_job("job-1", worker_kind="transcription", task_type="transcription.run")


def test_check_cancel_rejects_invalid_status() -> None:
    client = JobApiClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=SimpleNamespace(
            request=lambda **kwargs: {
                "cancel_requested": False,
                "status": "invalid",
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid cancel-check status"):
        client.check_cancel("job-1", execution_id="exec-1")
