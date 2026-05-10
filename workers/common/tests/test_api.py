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
#   test_claim_analysis_run_shapes_payload_and_parses_execution - Verifies canonical claim payloads and response parsing.
#   test_list_queued_runs_shapes_query_and_parses_minimal_snapshots - Verifies shared worker polling reads via the API contract.
#   test_progress_finalize_and_artifact_calls_preserve_contract_shapes - Verifies the remaining mutation payloads.
#   test_check_cancel_uses_query_contract - Verifies cancel-check query and response parsing.
#   test_internal_api_failures_emit_required_marker - Verifies deterministic transport failures and stable log markers.
#   test_claim_analysis_run_rejects_malformed_response - Verifies deterministic failure on malformed claim responses.
# END_MODULE_MAP

from __future__ import annotations

import logging
from typing import Mapping

import pytest

from transcriber_workers_common.api import (
    ClaimedAnalysisRunExecution,
    InternalApiConfig,
    InternalApiUnavailableError,
    AnalysisRunControlClient,
    SelectionItemMaterialization,
)
from transcriber_workers_common.artifacts import ArtifactDescriptor

RUN_ID = "11111111-1111-1111-1111-111111111111"
EXECUTION_ID = "22222222-2222-2222-2222-222222222222"
SELECTION_ID = "33333333-3333-3333-3333-333333333333"
MEDIA_ID = "44444444-4444-4444-4444-444444444444"
SOURCE_ID = "55555555-5555-5555-5555-555555555555"


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


def _selection_item(*, position: int = 0, origin_type: str = "object", kind: str = "audio") -> dict[str, object]:
    source = {
        "source_id": SOURCE_ID,
        "origin_type": origin_type,
        "external_uri": "https://example.test/source" if origin_type == "url" else None,
        "object_key": "media/run-1/source.wav" if origin_type == "object" else None,
        "text_ref": "text:abc123" if origin_type == "text" else None,
        "checksum": "sha256:demo",
        "size_bytes": 42 if origin_type == "object" else None,
        "mime_type": "audio/wav" if origin_type == "object" else None,
        "expires_at": None,
    }
    return {
        "selection_item_id": f"selection-item-{position}",
        "position": position,
        "media_item_id": MEDIA_ID,
        "kind": kind,
        "media_kind": kind,
        "mime_type": source["mime_type"],
        "role": "primary",
        "labels": {
            "display_label": "Source.wav",
            "source_label": "interview_a",
            "original_filename": "source.wav",
        },
        "source_snapshot": source,
        "display_name": "Source.wav",
        "status_at_selection": "ready",
        "metadata_snapshot": {"original_filename": "source.wav"},
        "retention_snapshot": {"state": "active"},
        "diagnostics": [],
    }


def _claim_response(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "execution_id": EXECUTION_ID,
        "analysis_run_id": RUN_ID,
        "run_type": "transcription",
        "selection": {
            "selection_id": SELECTION_ID,
            "items": [_selection_item()],
            "option_snapshot": {"language": "ru"},
            "sealed_at": "2026-05-10T12:00:00Z",
        },
        "params": {"language": "ru"},
        "claimed_at": "2026-05-10T12:01:00Z",
    }
    payload.update(overrides)
    return payload


def test_claim_analysis_run_shapes_payload_and_parses_execution() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "POST",
                f"http://internal.local/internal/v1/analysis-runs/{RUN_ID}/executions/claim",
            ): _claim_response()
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    execution = client.claim_analysis_run(RUN_ID, worker_kind="transcription", task_type="selection.transcription")

    assert execution.execution_id == EXECUTION_ID
    assert execution.analysis_run_id == RUN_ID
    assert execution.selection.items[0].selection_item_id == "selection-item-0"
    assert execution.selection.items[0].role == "primary"
    assert execution.selection.items[0].labels.display_label == "Source.wav"
    assert execution.selection.items[0].labels.source_label == "interview_a"
    assert execution.selection.items[0].media_kind == "audio"
    assert execution.selection.items[0].mime_type == "audio/wav"
    assert execution.selection.items[0].source_snapshot.origin_type == "object"
    assert execution.ordered_inputs[0].object_key == "media/run-1/source.wav"
    assert execution.params == {"language": "ru"}
    assert transport.calls == [
        {
            "method": "POST",
            "url": f"http://internal.local/internal/v1/analysis-runs/{RUN_ID}/executions/claim",
            "payload": {"worker_kind": "transcription", "task_type": "selection.transcription"},
        }
    ]


def test_selection_item_materialization_classifies_final_multimodal_sources() -> None:
    cases = [
        ("text", "text", None, "text"),
        ("url", "url", None, "url"),
        ("object", "photo", "image/jpeg", "object"),
        ("object", "image", "image/png", "object"),
        ("object", "document", "application/pdf", "object"),
        ("object", "audio", "audio/ogg", "object"),
        ("object", "voice", "audio/ogg", "object"),
        ("object", "video", "video/mp4", "object"),
        ("object", "file", "application/octet-stream", "object"),
    ]

    for position, (origin_type, kind, mime_type, expected_kind) in enumerate(cases):
        payload = _selection_item(position=position, origin_type=origin_type, kind=kind)
        payload["media_item_id"] = f"44444444-4444-4444-4444-4444444444{position:02d}"
        payload["source_snapshot"]["source_id"] = f"55555555-5555-5555-5555-5555555555{position:02d}"
        payload["source_snapshot"]["mime_type"] = mime_type
        payload["mime_type"] = mime_type
        if origin_type == "object":
            payload["source_snapshot"]["object_key"] = f"media/item-{position}"
        item = ClaimedAnalysisRunExecution.from_payload(
            _claim_response(
                selection={
                    "selection_id": SELECTION_ID,
                    "items": [payload],
                    "option_snapshot": {},
                    "sealed_at": "2026-05-10T12:00:00Z",
                }
            )
        ).selection.items[0]

        descriptor = SelectionItemMaterialization.from_selection_item(item)

        assert descriptor.materialization_kind == expected_kind
        assert descriptor.selection_item_id == f"selection-item-{position}"
        assert descriptor.media_item_id == payload["media_item_id"]
        assert descriptor.media_kind == kind
        assert descriptor.mime_type == mime_type
        assert descriptor.role == "primary"
        if origin_type == "object":
            assert descriptor.deterministic_filename.startswith(f"item-{position:04d}-")


def test_selection_item_role_defaults_from_metadata_or_selection_options() -> None:
    item_payload = _selection_item()
    item_payload.pop("role")
    item_payload.pop("labels")
    item_payload["metadata_snapshot"] = {"role": "reference", "source_label": "note_a"}

    execution = ClaimedAnalysisRunExecution.from_payload(
        _claim_response(
            selection={
                "selection_id": SELECTION_ID,
                "items": [item_payload],
                "option_snapshot": {"item_roles": {MEDIA_ID: "primary"}},
                "sealed_at": "2026-05-10T12:00:00Z",
            }
        )
    )

    assert execution.selection.items[0].role == "reference"
    assert execution.selection.items[0].labels.source_label == "note_a"

    item_payload = _selection_item()
    item_payload.pop("role")
    item_payload.pop("labels")
    item_payload["metadata_snapshot"] = {}
    execution = ClaimedAnalysisRunExecution.from_payload(
        _claim_response(
            selection={
                "selection_id": SELECTION_ID,
                "items": [item_payload],
                "option_snapshot": {"item_roles": {MEDIA_ID: "context"}},
                "sealed_at": "2026-05-10T12:00:00Z",
            }
        )
    )

    assert execution.selection.items[0].role == "context"


def test_list_queued_runs_shapes_query_and_parses_minimal_snapshots() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/analysis-runs/queue?page=1&page_size=1&status=queued&run_type=transcription&task_type=selection.transcription",
            ): {
                "items": [
                    {
                        "analysis_run_id": RUN_ID,
                        "run_type": "transcription",
                        "task_type": "selection.transcription",
                        "status": "queued",
                        "version": 1,
                    }
                ],
                "page": 1,
                "page_size": 1,
            }
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    runs = client.list_queued_runs(
        status="queued",
        run_type="transcription",
        task_type="selection.transcription",
        page_size=1,
    )

    assert [run.analysis_run_id for run in runs] == [RUN_ID]
    assert transport.calls == [
        {
            "method": "GET",
            "url": "http://internal.local/internal/v1/analysis-runs/queue?page=1&page_size=1&status=queued&run_type=transcription&task_type=selection.transcription",
            "payload": None,
        }
    ]


def test_list_queued_runs_allows_unfiltered_polling_query() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/analysis-runs/queue?page=1&page_size=2",
            ): {"items": []}
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    assert client.list_queued_runs(page_size=2) == ()
    assert transport.calls[0]["url"] == "http://internal.local/internal/v1/analysis-runs/queue?page=1&page_size=2"


def test_progress_finalize_and_artifact_calls_preserve_contract_shapes() -> None:
    client = AnalysisRunControlClient(InternalApiConfig(base_url="http://internal.local"), transport=StubTransport())
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
    client.finalize_analysis_run(
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
            "url": "http://internal.local/internal/v1/analysis-runs/job-1/executions/progress",
            "payload": {
                "execution_id": "exec-1",
                "progress_stage": "transcribing",
                "progress_message": "running whisper",
            },
        },
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/analysis-runs/job-1/artifacts",
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
            "url": "http://internal.local/internal/v1/analysis-runs/job-1/executions/finalize",
            "payload": {
                "execution_id": "exec-1",
                "outcome": "succeeded",
                "message": "finished",
            },
        },
    ]


def test_check_cancel_uses_query_contract() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/analysis-runs/job-2/executions/cancel-check?execution_id=exec-2",
            ): {
                "cancel_requested": True,
                "status": "cancel_requested",
                "cancel_requested_at": "2026-04-22T10:00:00Z",
            }
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    result = client.check_cancel("job-2", execution_id="exec-2")

    assert result.cancel_requested is True
    assert result.status == "cancel_requested"
    assert result.cancel_requested_at == "2026-04-22T10:00:00Z"


def test_resolve_agent_run_request_access_uses_query_contract() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/analysis-runs/job-agent/request-access?execution_id=exec-agent",
            ): {
                "provider": "minio_presigned_url",
                "url": "https://minio.local/private/request.json",
                "expires_at": "2026-04-25T12:00:00Z",
                "request_ref": "agentreq_digest",
                "request_digest_sha256": "digest",
                "request_bytes": 321,
            }
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    result = client.resolve_agent_run_request_access("job-agent", execution_id="exec-agent")

    assert result.provider == "minio_presigned_url"
    assert result.request_ref == "agentreq_digest"
    assert result.request_bytes == 321


def test_resolve_artifact_uses_internal_download_access_contract() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/artifacts/artifact-1/download-access",
            ): {
                "artifact_id": "artifact-1",
                "analysis_run_id": "job-1",
                "artifact_kind": "transcript_plain",
                "filename": "transcript.txt",
                "mime_type": "text/plain",
                "size_bytes": 17,
                "created_at": "2026-04-25T12:00:00Z",
                "download": {
                    "provider": "minio_presigned_url",
                    "url": "http://minio:9000/artifacts/transcript.txt",
                    "expires_at": "2026-04-25T12:15:00Z",
                },
            }
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    result = client.resolve_artifact("artifact-1")

    assert result.download_url == "http://minio:9000/artifacts/transcript.txt"
    assert result.artifact_kind == "transcript_plain"


def test_internal_api_failures_emit_required_marker(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(error=OSError("connection refused")),
    )

    with pytest.raises(InternalApiUnavailableError, match="connection refused"):
        client.claim_analysis_run("job-3", worker_kind="transcription", task_type="selection.transcription")

    assert "[WorkerCommon][callInternalApi][BLOCK_CALL_INTERNAL_CONTROL_PLANE]" in caplog.text


def test_claim_analysis_run_rejects_malformed_response() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(
            responses={
                (
                    "POST",
                    "http://internal.local/internal/v1/analysis-runs/job-4/executions/claim",
                ): {**_claim_response(), "selection": {"selection_id": SELECTION_ID, "items": [], "option_snapshot": {}, "sealed_at": "2026-05-10T12:00:00Z"}}
            }
        ),
    )

    with pytest.raises(ValueError, match="selection"):
        client.claim_analysis_run("job-4", worker_kind="transcription", task_type="selection.transcription")


def test_claim_analysis_run_allows_agent_run_without_ordered_inputs() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(
            responses={
                (
                    "POST",
                    "http://internal.local/internal/v1/analysis-runs/job-agent/executions/claim",
                ): _claim_response(
                    analysis_run_id="job-agent",
                    run_type="custom",
                    params={"harness_name": "fixture"},
                )
            }
        ),
    )

    execution = client.claim_analysis_run("job-agent", worker_kind="agent_runner", task_type="selection.analysis")

    assert execution.run_type == "custom"
    assert execution.selection.items[0].media_item_id == MEDIA_ID
    assert execution.params == {"harness_name": "fixture"}


def test_claim_analysis_run_rejects_empty_selection_items() -> None:
    with pytest.raises(ValueError, match="selection"):
        ClaimedAnalysisRunExecution(
            execution_id=EXECUTION_ID,
            analysis_run_id=RUN_ID,
            run_type="report",
            selection=type("EmptySelection", (), {"items": ()})(),
            params={},
            claimed_at="2026-05-10T12:01:00Z",
        )
