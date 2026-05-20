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
from pathlib import Path
from typing import Mapping

import pytest

import transcriber_workers_common.api as api_module
from transcriber_workers_common.api import (
    ClaimedAnalysisRunStep,
    InternalApiConfig,
    InternalApiUnavailableError,
    AnalysisRunControlClient,
    SelectionItemMaterialization,
)
from transcriber_workers_common.artifacts import ArtifactDescriptor

RUN_ID = "11111111-1111-1111-1111-111111111111"
STEP_ID = "22222222-2222-2222-2222-222222222222"
SNAPSHOT_ID = "33333333-3333-3333-3333-333333333333"
ASSET_ID = "44444444-4444-4444-4444-444444444444"
OBJECT_ID = "55555555-5555-5555-5555-555555555555"


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
    if origin_type == "text":
        origin_snapshot = {"origin_type": "text", "text": "inline text"}
        storage_snapshot: dict[str, object] = {}
    elif origin_type == "url":
        origin_snapshot = {"origin_type": "url", "url": "https://example.test/source"}
        storage_snapshot = {}
    else:
        origin_snapshot = {
            "origin_type": "telegram_file",
            "object_ref": "media/run-1/source.wav",
            "content_type": "audio/wav",
            "size_bytes": 42,
        }
        storage_snapshot = {
            "stored_object_id": OBJECT_ID,
            "bucket": "sources",
            "object_key": "media/run-1/source.wav",
            "content_type": "audio/wav",
            "size_bytes": 42,
            "checksum": "sha256:demo",
            "storage_status": "available",
            "retention_state": "active",
            "created_at": "2026-05-10T12:00:00Z",
        }
    return {
        "selection_snapshot_item_id": f"selection-snapshot-item-{position}",
        "position": position,
        "media_asset_id": ASSET_ID,
        "kind": kind,
        "role": "primary",
        "labels": {
            "display_label": "Source.wav",
            "source_label": "interview_a",
            "original_filename": "source.wav",
        },
        "origin_snapshot": origin_snapshot,
        "storage_snapshot": storage_snapshot,
        "display_name": "Source.wav",
        "status_at_selection": "available",
        "metadata_snapshot": {"original_filename": "source.wav"},
        "diagnostics": [],
    }


def _claim_response(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "analysis_run_step_id": STEP_ID,
        "analysis_run_id": RUN_ID,
        "run_type": "transcription",
        "selection_snapshot": {
            "selection_snapshot_id": SNAPSHOT_ID,
            "items": [_selection_item()],
            "option_snapshot": {"language": "ru"},
            "sealed_at": "2026-05-10T12:00:00Z",
        },
        "analysis_run_step_inputs": [
            {
                "analysis_run_step_input_id": "input-1",
                "analysis_run_step_id": STEP_ID,
                "input_kind": "selection_snapshot_item",
                "selection_snapshot_item_id": "selection-snapshot-item-0",
                "position": 0,
                "required": True,
                "metadata": {},
            }
        ],
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
                f"http://internal.local/internal/v1/analysis-runs/{RUN_ID}/steps/claim",
            ): _claim_response()
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    execution = client.claim_analysis_run_step(RUN_ID, worker_kind="transcription", step_kind="selection.transcription")

    assert execution.analysis_run_step_id == STEP_ID
    assert execution.analysis_run_id == RUN_ID
    assert execution.selection_snapshot.items[0].selection_snapshot_item_id == "selection-snapshot-item-0"
    assert execution.selection_snapshot.items[0].role == "primary"
    assert execution.selection_snapshot.items[0].labels.display_label == "Source.wav"
    assert execution.selection_snapshot.items[0].labels.source_label == "interview_a"
    assert execution.selection_snapshot.items[0].media_kind == "audio"
    assert execution.selection_snapshot.items[0].mime_type == "audio/wav"
    assert execution.selection_snapshot.items[0].source_snapshot.origin_type == "object"
    assert execution.ordered_inputs[0].object_key == "media/run-1/source.wav"
    assert execution.params == {"language": "ru"}
    assert transport.calls == [
        {
            "method": "POST",
            "url": f"http://internal.local/internal/v1/analysis-runs/{RUN_ID}/steps/claim",
            "payload": {"worker_kind": "transcription", "step_kind": "selection.transcription"},
        }
    ]


def test_claim_analysis_run_accepts_target_selection_snapshot_metadata() -> None:
    payload = _claim_response()
    selection_snapshot = dict(payload["selection_snapshot"])  # type: ignore[index]
    selection_snapshot.update(
        {
            "channel_account_id": "channel-account-1",
            "source_collection_id": "collection-1",
            "status": "sealed",
            "diagnostics": [],
            "created_at": "2026-05-10T11:59:00Z",
        }
    )
    payload["selection_snapshot"] = selection_snapshot

    execution = ClaimedAnalysisRunStep.from_payload(payload)

    assert execution.selection_snapshot.selection_snapshot_id == SNAPSHOT_ID
    assert execution.selection_snapshot.items[0].media_asset_id == ASSET_ID


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
        payload["media_asset_id"] = f"44444444-4444-4444-4444-4444444444{position:02d}"
        if origin_type == "object":
            payload["storage_snapshot"]["stored_object_id"] = f"55555555-5555-5555-5555-5555555555{position:02d}"
            payload["storage_snapshot"]["content_type"] = mime_type
            payload["origin_snapshot"]["content_type"] = mime_type
            payload["origin_snapshot"]["size_bytes"] = 42
        if origin_type == "object":
            payload["storage_snapshot"]["object_key"] = f"media/item-{position}"
        item = ClaimedAnalysisRunStep.from_payload(
            _claim_response(
                selection_snapshot={
                    "selection_snapshot_id": SNAPSHOT_ID,
                    "items": [payload],
                    "option_snapshot": {},
                    "sealed_at": "2026-05-10T12:00:00Z",
                }
            )
        ).selection_snapshot.items[0]

        descriptor = SelectionItemMaterialization.from_selection_item(item)

        assert descriptor.materialization_kind == expected_kind
        assert descriptor.selection_snapshot_item_id == f"selection-snapshot-item-{position}"
        assert descriptor.media_asset_id == payload["media_asset_id"]
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

    execution = ClaimedAnalysisRunStep.from_payload(
        _claim_response(
            selection_snapshot={
                "selection_snapshot_id": SNAPSHOT_ID,
                "items": [item_payload],
                "option_snapshot": {"item_roles": {ASSET_ID: "primary"}},
                "sealed_at": "2026-05-10T12:00:00Z",
            }
        )
    )

    assert execution.selection_snapshot.items[0].role == "reference"
    assert execution.selection_snapshot.items[0].labels.source_label == "note_a"

    item_payload = _selection_item()
    item_payload.pop("role")
    item_payload.pop("labels")
    item_payload["metadata_snapshot"] = {}
    execution = ClaimedAnalysisRunStep.from_payload(
        _claim_response(
            selection_snapshot={
                "selection_snapshot_id": SNAPSHOT_ID,
                "items": [item_payload],
                "option_snapshot": {"item_roles": {ASSET_ID: "context"}},
                "sealed_at": "2026-05-10T12:00:00Z",
            }
        )
    )

    assert execution.selection_snapshot.items[0].role == "context"


def test_list_queued_runs_shapes_query_and_parses_minimal_snapshots() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/analysis-runs/queue?page=1&page_size=1&status=queued&run_type=transcription&worker_kind=transcription&step_kind=selection.transcription",
            ): {
                "items": [
                    {
                        "analysis_run_id": RUN_ID,
                        "run_type": "transcription",
                        "worker_kind": "transcription",
                        "step_kind": "selection.transcription",
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
        worker_kind="transcription",
        step_kind="selection.transcription",
        page_size=1,
    )

    assert [run.analysis_run_id for run in runs] == [RUN_ID]
    assert transport.calls == [
        {
            "method": "GET",
            "url": "http://internal.local/internal/v1/analysis-runs/queue?page=1&page_size=1&status=queued&run_type=transcription&worker_kind=transcription&step_kind=selection.transcription",
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
        object_key="run-1/transcript/plain/transcript.txt",
        size_bytes=42,
        format="plain_text",
    )

    client.publish_progress(
        "run-1",
        analysis_run_step_id="exec-1",
        progress_stage="transcribing",
        progress_message="running asr",
    )
    client.register_artifacts("run-1", analysis_run_step_id="exec-1", artifacts=[artifact])
    client.register_diagnostics(
        "run-1",
        analysis_run_step_id="exec-1",
        diagnostics=[{"code": "warn_partial_source", "severity": "warning"}],
    )
    client.finalize_analysis_run(
        "run-1",
        analysis_run_step_id="exec-1",
        outcome="succeeded",
        progress_stage="completed",
        progress_message="finished",
        error_code=None,
        error_message=None,
    )

    assert client.transport.calls == [
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/analysis-runs/run-1/steps/progress",
            "payload": {
                "analysis_run_step_id": "exec-1",
                "progress_stage": "transcribing",
                "progress_message": "running asr",
            },
        },
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/analysis-runs/run-1/artifacts",
            "payload": {
                "analysis_run_step_id": "exec-1",
                "artifacts": [
                    {
                        "artifact_kind": "transcript_plain",
                        "format": "plain_text",
                        "filename": "transcript.txt",
                        "mime_type": "text/plain; charset=utf-8",
                        "object_key": "run-1/transcript/plain/transcript.txt",
                        "size_bytes": 42,
                    }
                ],
            },
        },
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/analysis-runs/run-1/diagnostics",
            "payload": {
                "analysis_run_step_id": "exec-1",
                "diagnostics": [{"code": "warn_partial_source", "severity": "warning"}],
            },
        },
        {
            "method": "POST",
            "url": "http://internal.local/internal/v1/analysis-runs/run-1/steps/finalize",
            "payload": {
                "analysis_run_step_id": "exec-1",
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
                "http://internal.local/internal/v1/analysis-runs/run-2/steps/cancel-check?analysis_run_step_id=exec-2",
            ): {
                "cancel_requested": True,
                "status": "cancel_requested",
                "cancel_requested_at": "2026-04-22T10:00:00Z",
            }
        }
    )
    client = AnalysisRunControlClient(config, transport=transport)

    result = client.check_cancel("run-2", analysis_run_step_id="exec-2")

    assert result.cancel_requested is True
    assert result.status == "cancel_requested"
    assert result.cancel_requested_at == "2026-04-22T10:00:00Z"


def test_resolve_agent_run_request_access_uses_query_contract() -> None:
    config = InternalApiConfig(base_url="http://internal.local")
    transport = StubTransport(
        responses={
            (
                "GET",
                "http://internal.local/internal/v1/analysis-runs/run-agent/request-access?analysis_run_step_id=exec-agent",
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

    result = client.resolve_agent_run_request_access("run-agent", analysis_run_step_id="exec-agent")

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
                "analysis_run_id": "run-1",
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
        client.claim_analysis_run_step("run-3", worker_kind="transcription", step_kind="selection.transcription")

    assert "[WorkerCommon][callInternalApi][BLOCK_CALL_INTERNAL_CONTROL_PLANE]" in caplog.text


def test_claim_analysis_run_rejects_malformed_response() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(
            responses={
                (
                    "POST",
                    "http://internal.local/internal/v1/analysis-runs/run-4/steps/claim",
                ): {**_claim_response(), "selection_snapshot": {"selection_snapshot_id": SNAPSHOT_ID, "items": [], "option_snapshot": {}, "sealed_at": "2026-05-10T12:00:00Z"}}
            }
        ),
    )

    with pytest.raises(ValueError, match="selection"):
        client.claim_analysis_run_step("run-4", worker_kind="transcription", step_kind="selection.transcription")


def test_claim_analysis_run_allows_agent_run_without_ordered_inputs() -> None:
    client = AnalysisRunControlClient(
        InternalApiConfig(base_url="http://internal.local"),
        transport=StubTransport(
            responses={
                (
                    "POST",
                    "http://internal.local/internal/v1/analysis-runs/run-agent/steps/claim",
                ): _claim_response(
                    analysis_run_id="run-agent",
                    run_type="custom",
                    params={"harness_name": "fixture"},
                )
            }
        ),
    )

    execution = client.claim_analysis_run_step("run-agent", worker_kind="agent_runner", step_kind="report.analysis")

    assert execution.run_type == "custom"
    assert execution.selection_snapshot.items[0].media_asset_id == ASSET_ID
    assert execution.params == {"harness_name": "fixture"}


def test_claim_analysis_run_rejects_empty_selection_snapshot_items() -> None:
    with pytest.raises(ValueError, match="selection"):
        ClaimedAnalysisRunStep(
            analysis_run_step_id=STEP_ID,
            analysis_run_id=RUN_ID,
            run_type="report",
            selection_snapshot=type("EmptySelection", (), {"items": ()})(),
            analysis_run_step_inputs=(),
            params={},
            claimed_at="2026-05-10T12:01:00Z",
        )


def test_selection_item_helpers_cover_defaults_and_metadata_fallbacks() -> None:
    source_snapshot = api_module.MediaSourceSnapshot(
        source_id=OBJECT_ID,
        origin_type="object",
        object_key="media/run-1/source.wav",
        mime_type="audio/wav",
        size_bytes=42,
    )
    item = api_module.SelectionItemSnapshot(
        position=2,
        media_asset_id=ASSET_ID,
        kind="audio",
        origin_snapshot={"origin_type": "telegram_file", "object_ref": "media/run-1/source.wav"},
        storage_snapshot={"stored_object_id": OBJECT_ID, "object_key": "media/run-1/source.wav", "content_type": "audio/wav"},
        source_snapshot=source_snapshot,
        display_name="  Demo source.wav  ",
        status_at_selection="available",
        metadata_snapshot={
            "source_label": "  interview_a  ",
            "original_filename": "   ",
            "filename": " fallback.wav ",
        },
        selection_snapshot_item_id=None,
        media_kind=None,
        mime_type=None,
        labels=None,
    )

    assert item.selection_snapshot_item_id == "selection-snapshot-item-2"
    assert item.media_kind == "audio"
    assert item.mime_type == "audio/wav"
    assert item.labels is not None
    assert item.labels.original_filename == "fallback.wav"
    assert item.labels.source_display_label() == "interview_a"
    assert api_module._metadata_original_filename(item) == "fallback.wav"
    assert api_module._metadata_source_label(item) == "interview_a"

    direct_filename_item = api_module.SelectionItemSnapshot(
        selection_snapshot_item_id="selection-snapshot-item-4",
        position=4,
        media_asset_id=ASSET_ID,
        kind="audio",
        media_kind="audio",
        origin_snapshot={"origin_type": "telegram_file", "object_ref": "media/run-1/source.wav"},
        storage_snapshot={"stored_object_id": OBJECT_ID, "object_key": "media/run-1/source.wav", "content_type": "audio/wav"},
        source_snapshot=source_snapshot,
        display_name="Source.wav",
        status_at_selection="available",
        metadata_snapshot={"original_filename": "  source.wav  "},
        labels=api_module.SelectionItemLabels(display_label="Source.wav"),
    )
    no_filename_item = api_module.SelectionItemSnapshot(
        selection_snapshot_item_id="selection-snapshot-item-5",
        position=5,
        media_asset_id=ASSET_ID,
        kind="audio",
        media_kind="audio",
        origin_snapshot={"origin_type": "telegram_file", "object_ref": "media/run-1/source.wav"},
        storage_snapshot={"stored_object_id": OBJECT_ID, "object_key": "media/run-1/source.wav", "content_type": "audio/wav"},
        source_snapshot=source_snapshot,
        display_name="Source.wav",
        status_at_selection="available",
        metadata_snapshot={"original_filename": "   ", "filename": "   ", "source_label": "   "},
        labels=api_module.SelectionItemLabels(display_label="Source.wav"),
    )

    assert api_module._metadata_original_filename(direct_filename_item) == "source.wav"
    assert api_module._metadata_original_filename(no_filename_item) is None
    assert api_module._metadata_source_label(no_filename_item) is None


def test_media_source_snapshot_payload_and_optional_sizes_cover_target_paths() -> None:
    source_snapshot = api_module.MediaSourceSnapshot.from_payload(
        {
            "source_id": OBJECT_ID,
            "origin_type": "object",
            "external_uri": "https://example.test/source",
            "object_key": "media/run-1/source.wav",
            "text_ref": "inline-ref",
            "checksum": "sha256:demo",
            "mime_type": "audio/wav",
            "expires_at": "2026-05-10T13:00:00Z",
        }
    )
    item = api_module.SelectionItemSnapshot(
        selection_snapshot_item_id="selection-snapshot-item-7",
        position=7,
        media_asset_id=ASSET_ID,
        kind="audio",
        media_kind="audio",
        role="primary",
        origin_snapshot={"origin_type": "telegram_file", "object_ref": "media/run-1/source.wav"},
        storage_snapshot={"stored_object_id": OBJECT_ID, "object_key": "media/run-1/source.wav"},
        source_snapshot=source_snapshot,
        display_name="Source.wav",
        status_at_selection="available",
        metadata_snapshot={},
        labels=api_module.SelectionItemLabels(display_label="Source.wav"),
    )
    selection = api_module.SealedSelectionSnapshotInput(
        selection_snapshot_id=SNAPSHOT_ID,
        items=(item,),
        option_snapshot={},
        sealed_at="2026-05-10T12:00:00Z",
    )
    claim = api_module.ClaimedAnalysisRunStep(
        analysis_run_step_id=STEP_ID,
        analysis_run_id=RUN_ID,
        run_type="transcription",
        selection_snapshot=selection,
        analysis_run_step_inputs=(),
        params={},
        claimed_at="2026-05-10T12:01:00Z",
    )
    descriptor = SelectionItemMaterialization.from_selection_item(item)

    assert source_snapshot.size_bytes is None
    assert item.selection_snapshot_item_id == "selection-snapshot-item-7"
    assert item.media_asset_id == ASSET_ID
    assert descriptor.selection_snapshot_item_id == "selection-snapshot-item-7"
    assert descriptor.media_asset_id == ASSET_ID
    assert descriptor.source_id == OBJECT_ID
    assert selection.selection_snapshot_id == SNAPSHOT_ID
    assert claim.analysis_run_step_id == STEP_ID
    assert claim.selection_snapshot is selection

    target_item_without_sizes = _selection_item()
    target_item_without_sizes["origin_snapshot"].pop("size_bytes")
    target_item_without_sizes["storage_snapshot"].pop("size_bytes")
    parsed = ClaimedAnalysisRunStep.from_payload(
        _claim_response(
            selection_snapshot={
                "selection_snapshot_id": SNAPSHOT_ID,
                "items": [target_item_without_sizes],
                "option_snapshot": {},
                "sealed_at": "2026-05-10T12:00:00Z",
            }
        )
    )
    assert parsed.selection_snapshot.items[0].source_snapshot.size_bytes is None


def test_selection_item_materialization_marks_missing_object_key_as_unsupported() -> None:
    source_snapshot = api_module.MediaSourceSnapshot(
        source_id=OBJECT_ID,
        origin_type="object",
        object_key=None,
        mime_type="audio/wav",
    )
    item = api_module.SelectionItemSnapshot(
        selection_snapshot_item_id="selection-snapshot-item-3",
        position=3,
        media_asset_id=ASSET_ID,
        kind="audio",
        media_kind="audio",
        origin_snapshot={"origin_type": "telegram_file", "object_ref": "media/run-1/source.wav"},
        storage_snapshot={"stored_object_id": OBJECT_ID, "content_type": "audio/wav"},
        source_snapshot=source_snapshot,
        display_name="Source.wav",
        status_at_selection="available",
        metadata_snapshot={},
        labels=api_module.SelectionItemLabels(display_label="Source.wav", source_label="   "),
    )

    descriptor = SelectionItemMaterialization.from_selection_item(item)

    assert descriptor.materialization_kind == "unsupported"
    assert descriptor.unsupported_reason == "object-backed media source is missing object_key"
    assert descriptor.is_object_backed is False


def test_ordered_input_request_access_and_helper_branches_cover_edge_paths() -> None:
    ordered_input = api_module.OrderedWorkerInput.from_payload(
        {
            "position": 1,
            "source_id": OBJECT_ID,
            "source_kind": "object",
            "source_label": " interview_a ",
            "display_name": "Source.wav",
            "original_filename": "source.wav",
            "object_key": "media/run-1/source.wav",
            "source_url": "https://example.test/source",
            "sha256": "sha256:demo",
            "size_bytes": 42,
        }
    )
    request_access = api_module.AgentRunRequestAccessResult(
        provider="minio_presigned_url",
        url="https://minio.local/private/request.json",
        expires_at="2026-04-25T12:00:00Z",
        request_ref="agentreq_digest",
        request_digest_sha256="digest",
        request_bytes=321,
    )

    assert ordered_input.object_key == "media/run-1/source.wav"
    assert request_access.to_payload()["request_bytes"] == 321
    assert api_module._derive_selection_role({}, metadata_snapshot={}, option_snapshot={"item_roles": {"other": "context"}}) == "primary"
    assert api_module._extension_for_mime(None) == ".bin"
    assert api_module._extension_for_mime("   ") == ".bin"


def test_selection_item_labels_and_materialization_helpers_trim_source_labels() -> None:
    labels = api_module.SelectionItemLabels(display_label="Display", source_label="  ", original_filename="file.wav")

    assert labels.source_display_label() == "Display"
    assert api_module.SelectionItemMaterialization(
        selection_snapshot_item_id="selection-snapshot-item-9",
        position=9,
        media_asset_id=ASSET_ID,
        media_kind="audio",
        role="primary",
        labels=labels,
        origin_ref=OBJECT_ID,
        origin_type="object",
        materialization_kind="object",
        mime_type="audio/wav",
        object_key="media/run-1/source.wav",
        deterministic_filename=str(Path("item-0009-source.wav")),
    ).is_object_backed is True
