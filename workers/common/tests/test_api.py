# FILE: workers/common/tests/test_api.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the worker-common API client preserves frozen worker-control payload and response shapes.
# SCOPE: Claim, progress, artifact, finalize, cancel-check, and failure-marker assertions.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local worker-common API client verification for payload shape and deterministic failures.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   StubTransport - Captures worker-control requests during tests.
#   test_claim_job_shapes_payload_and_parses_execution - Verifies canonical claim payloads and response parsing.
#   test_list_jobs_shapes_query_and_parses_minimal_snapshots - Verifies shared worker polling reads via the API contract.
#   test_progress_finalize_and_artifact_calls_preserve_contract_shapes - Verifies the remaining mutation payloads.
#   test_check_cancel_uses_query_contract - Verifies cancel-check query and response parsing.
#   test_internal_api_failures_emit_required_marker - Verifies deterministic transport failures and stable log markers.
#   test_claim_job_rejects_malformed_response - Verifies deterministic failure on malformed claim responses.
# END_MODULE_MAP

from __future__ import annotations

import logging
from typing import Mapping

import pytest

from transcriber_workers_common.api import (
    InternalApiConfig,
    InternalApiUnavailableError,
    JobApiClient,
)
from transcriber_workers_common.artifacts import ArtifactDescriptor


class StubTransport:
    def __init__(self, responses: Mapping[tuple[str, str], object] | None = None, error: Exception | None = None) -> None:
        self.responses = dict(responses or {})
        self.error = error
        self.calls: list[dict[str, object]] = []

    def request(self, *, method: str, url: str, payload: Mapping[str, object] | None = None) -> object:
        self.calls.append({"method": method, "url": url, "payload": payload})
        if self.error is not None:
            raise self.error
        return self.responses.get((method, url))


def test_claim_job_shapes_payload_and_parses_execution() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "POST",
                "http://internal.local/internal/v1/jobs/job-1/claim",
            ): {
                "execution_id": "exec-1",
                "job_id": "job-1",
                "root_job_id": "root-1",
                "parent_job_id": None,
                "retry_of_job_id": None,
                "job_type": "transcription",
                "version": 3,
                "ordered_inputs": [
                    {
                        "position": 0,
                        "source_id": "source-1",
                        "source_kind": "youtube_url",
                        "display_name": "YouTube: demo",
                        "original_filename": None,
                        "object_key": None,
                        "source_url": "https://youtu.be/demo123",
                        "sha256": None,
                        "size_bytes": None,
                    }
                ],
                "params": {"language": "ru"},
            }
        }
    )
    client = JobApiClient(config, transport=transport)

    execution = client.claim_job("job-1", worker_kind="transcription", task_type="transcription.run")

    assert execution.execution_id == "exec-1"
    assert execution.ordered_inputs[0].source_kind == "youtube_url"
    assert execution.params == {"language": "ru"}
    assert transport.calls == [
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/jobs/job-1/claim",
            "payload": {"worker_kind": "transcription", "task_type": "transcription.run"},
        }
    ]


def test_list_jobs_shapes_query_and_parses_minimal_snapshots() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/v1/jobs?page=1&page_size=1&status=queued&job_type=transcription",
            ): {
                "items": [
                    {
                        "job_id": "job-1",
                        "root_job_id": "job-1",
                        "job_type": "transcription",
                        "status": "queued",
                        "version": 1,
                    }
                ],
                "page": 1,
                "page_size": 1,
            }
        }
    )
    client = JobApiClient(config, transport=transport)

    jobs = client.list_jobs(status="queued", job_type="transcription", page_size=1)

    assert [job.job_id for job in jobs] == ["job-1"]
    assert transport.calls == [
        {
            "method": "GET",
            "url": "http://internal.local/v1/jobs?page=1&page_size=1&status=queued&job_type=transcription",
            "payload": None,
        }
    ]


def test_list_jobs_allows_unfiltered_polling_query() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/v1/jobs?page=1&page_size=2",
            ): {"items": []}
        }
    )
    client = JobApiClient(config, transport=transport)

    assert client.list_jobs(page_size=2) == ()
    assert transport.calls[0]["url"] == "http://internal.local/v1/jobs?page=1&page_size=2"


def test_progress_finalize_and_artifact_calls_preserve_contract_shapes() -> None:
    client = JobApiClient(InternalApiConfig(base_url="http://internal.local"), transport=StubTransport())
    artifact = ArtifactDescriptor(
        artifact_kind="transcript_plain",
        filename="transcript.txt",
        mime_type="text/plain; charset=utf-8",
        object_key="artifacts/job-1/transcript/plain/transcript.txt",
        size_bytes=42,
        format="plain_text",
    )

    client.publish_progress(
        "job-1",
        execution_id="exec-1",
        progress_stage="transcribing",
        progress_message="running whisper",
    )
    client.register_artifacts("job-1", execution_id="exec-1", artifacts=[artifact])
    client.finalize_job(
        "job-1",
        execution_id="exec-1",
        outcome="succeeded",
        progress_stage="completed",
        progress_message="finished",
        error_code=None,
        error_message=None,
    )

    assert client.transport.calls == [
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/jobs/job-1/progress",
            "payload": {
                "execution_id": "exec-1",
                "progress_stage": "transcribing",
                "progress_message": "running whisper",
            },
        },
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/jobs/job-1/artifacts",
            "payload": {
                "execution_id": "exec-1",
                "artifacts": [
                    {
                        "artifact_kind": "transcript_plain",
                        "format": "plain_text",
                        "filename": "transcript.txt",
                        "mime_type": "text/plain; charset=utf-8",
                        "object_key": "artifacts/job-1/transcript/plain/transcript.txt",
                        "size_bytes": 42,
                    }
                ],
            },
        },
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/jobs/job-1/finalize",
            "payload": {
                "execution_id": "exec-1",
                "outcome": "succeeded",
                "progress_stage": "completed",
                "progress_message": "finished",
                "error_code": None,
                "error_message": None,
            },
        },
    ]


def test_check_cancel_uses_query_contract() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/jobs/job-2/cancel-check?execution_id=exec-2",
            ): {
                "cancel_requested": True,
                "status": "cancel_requested",
                "cancel_requested_at": "2026-04-22T10:00:00Z",
            }
        }
    )
    client = JobApiClient(config, transport=transport)

    result = client.check_cancel("job-2", execution_id="exec-2")

    assert result.cancel_requested is True
    assert result.status == "cancel_requested"
    assert result.cancel_requested_at == "2026-04-22T10:00:00Z"


def test_internal_api_failures_emit_required_marker(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    client = JobApiClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(error=OSError("connection refused")),
    )

    with pytest.raises(InternalApiUnavailableError, match="connection refused"):
        client.claim_job("job-3", worker_kind="transcription", task_type="transcription.run")

    assert "[WorkerCommon][callInternalApi][BLOCK_CALL_INTERNAL_CONTROL_PLANE]" in caplog.text


def test_claim_job_rejects_malformed_response() -> None:
    client = JobApiClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(
            responses={
                (
                    "POST",
                    "http://internal.local/internal/v1/jobs/job-4/claim",
                ): {
                    "execution_id": "exec-4",
                    "job_id": "job-4",
                    "root_job_id": "root-4",
                    "job_type": "transcription",
                    "version": 1,
                    "ordered_inputs": [],
                    "params": {},
                }
            }
        ),
    )

    with pytest.raises(ValueError, match="ordered_inputs"):
        client.claim_job("job-4", worker_kind="transcription", task_type="transcription.run")
