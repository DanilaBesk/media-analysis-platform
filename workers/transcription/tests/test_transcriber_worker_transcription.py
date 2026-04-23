# FILE: workers/transcription/tests/test_transcriber_worker_transcription.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the dedicated transcription worker claims jobs through the shared control plane, preserves transcript artifacts, and handles ordered combined inputs plus cancellation deterministically.
# SCOPE: Success finalization ordering, combined-media concatenation, cancellation checkpoints, local extraction reuse, and deterministic failure classification.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local verification for the transcription worker shell and extracted local orchestration path.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   RecordingApiClient - Captures claim/progress/artifact/finalize/cancel calls without redefining DTO payloads.
#   FakeSourceStore - Writes claimed input bytes into the worker workspace.
#   InMemoryArtifactStore - Captures uploaded artifact bytes.
#   RecordingTranscriber - Records worker transcription calls while returning deterministic transcript results.
#   test_run_transcription_claims_and_finalizes_after_all_artifacts_exist - Verifies the shared claim client, preserved transcript artifacts, and success finalization ordering.
#   test_run_transcription_combines_sorted_inputs_before_one_final_pass - Verifies ordered combined-media concatenation before the single transcription pass.
#   test_run_transcription_checks_cancellation_inside_worker_loop - Verifies cancellation is observed by the dedicated worker loop instead of legacy Telegram orchestration.
#   test_process_local_transcription_reuses_extracted_local_pipeline - Verifies the extracted local pipeline preserves current transcript artifacts for the service shell.
#   test_run_transcription_classifies_source_materialization_failures - Verifies deterministic `source_fetch_failed` finalization.
# END_MODULE_MAP

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from transcriber_workers_common.api import CancelCheckResult, ClaimedJobExecution, OrderedWorkerInput
from telegram_transcriber_bot.domain import SourceCandidate, TranscriptResult, TranscriptSegment
import transcriber_worker_transcription as worker_module
from transcriber_worker_transcription import (
    WorkerCancellationRequested,
    process_local_transcription,
    runTranscription,
)


class RecordingApiClient:
    def __init__(
        self,
        execution: ClaimedJobExecution,
        *,
        cancel_results: list[CancelCheckResult] | None = None,
    ) -> None:
        self.execution = execution
        self.cancel_results = list(cancel_results or [])
        self.calls: list[tuple[str, dict[str, object]]] = []

    def claim_job(self, job_id: str, *, worker_kind: str, task_type: str) -> ClaimedJobExecution:
        self.calls.append(
            (
                "claim_job",
                {"job_id": job_id, "worker_kind": worker_kind, "task_type": task_type},
            )
        )
        return self.execution

    def publish_progress(
        self,
        job_id: str,
        *,
        execution_id: str,
        progress_stage: str,
        progress_message: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "publish_progress",
                {
                    "job_id": job_id,
                    "execution_id": execution_id,
                    "progress_stage": progress_stage,
                    "progress_message": progress_message,
                },
            )
        )

    def register_artifacts(self, job_id: str, *, execution_id: str, artifacts) -> None:
        self.calls.append(
            (
                "register_artifacts",
                {
                    "job_id": job_id,
                    "execution_id": execution_id,
                    "artifacts": tuple(artifacts),
                },
            )
        )

    def finalize_job(
        self,
        job_id: str,
        *,
        execution_id: str,
        outcome: str,
        progress_stage: str | None = None,
        progress_message: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "finalize_job",
                {
                    "job_id": job_id,
                    "execution_id": execution_id,
                    "outcome": outcome,
                    "progress_stage": progress_stage,
                    "progress_message": progress_message,
                    "error_code": error_code,
                    "error_message": error_message,
                },
            )
        )

    def check_cancel(self, job_id: str, *, execution_id: str) -> CancelCheckResult:
        self.calls.append(
            (
                "check_cancel",
                {"job_id": job_id, "execution_id": execution_id},
            )
        )
        if self.cancel_results:
            return self.cancel_results.pop(0)
        return CancelCheckResult(cancel_requested=False, status="running")


class FakeSourceStore:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = dict(payloads)
        self.calls: list[tuple[str, str]] = []

    def fetch_file(self, *, object_key: str, destination: Path) -> None:
        self.calls.append((object_key, str(destination)))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.payloads[object_key])


class InMemoryArtifactStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_bytes(self, *, object_key: str, content: bytes, mime_type: str) -> None:
        self.calls.append(
            {
                "object_key": object_key,
                "content": content,
                "mime_type": mime_type,
            }
        )


class RecordingTranscriber:
    def __init__(self) -> None:
        self.calls: list[tuple[SourceCandidate, Path]] = []

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        self.calls.append((source, workspace_dir))
        return TranscriptResult(
            title=source.file_name or source.display_name,
            source_label=source.display_name,
            segments=[TranscriptSegment(start_seconds=0.0, end_seconds=1.5, text="Hello world", speaker="Speaker 1")],
            language="ru",
            raw_text="Hello world",
        )


def test_run_transcription_claims_and_finalizes_after_all_artifacts_exist(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-1",
            source_kind="uploaded_file",
            display_name="Audio: call.ogg",
            original_filename="call.ogg",
            object_key="uploads/call.ogg",
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/call.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    result = runTranscription(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    claim_call = api_client.calls[0]
    assert claim_call == (
        "claim_job",
        {"job_id": execution.job_id, "worker_kind": "transcription", "task_type": "transcription.run"},
    )
    assert [call[1]["progress_stage"] for call in api_client.calls if call[0] == "publish_progress"] == [
        "materializing_sources",
        "transcribing",
        "persisting_artifacts",
    ]
    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "transcript_plain",
        "transcript_segmented_markdown",
        "transcript_docx",
    ]
    finalize_call = api_client.calls[-1]
    assert finalize_call[0] == "finalize_job"
    assert finalize_call[1]["outcome"] == "succeeded"
    assert api_client.calls.index(register_call) < api_client.calls.index(finalize_call)
    assert result.artifacts.text_path.exists()
    assert result.artifacts.markdown_path.exists()
    assert result.artifacts.docx_path.exists()
    assert len(artifact_store.calls) == 3
    assert _required_marker() in caplog.text


def test_run_transcription_combines_sorted_inputs_before_one_final_pass(tmp_path: Path, monkeypatch) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=1,
            source_id="source-2",
            source_kind="uploaded_file",
            display_name="Audio: second.ogg",
            original_filename="second.ogg",
            object_key="uploads/second.ogg",
        ),
        OrderedWorkerInput(
            position=0,
            source_id="source-1",
            source_kind="uploaded_file",
            display_name="Audio: first.ogg",
            original_filename="first.ogg",
            object_key="uploads/first.ogg",
        ),
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore(
        {
            "uploads/first.ogg": b"first",
            "uploads/second.ogg": b"second",
        }
    )
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()
    concat_calls: list[list[str]] = []

    def fake_concat(input_paths: list[Path], output_path: Path) -> None:
        concat_calls.append([path.name for path in input_paths])
        output_path.write_bytes(b"combined")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fake_concat)

    result = runTranscription(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert [call[0] for call in source_store.calls] == ["uploads/first.ogg", "uploads/second.ogg"]
    assert concat_calls == [["first.ogg", "second.ogg"]]
    assert len(transcriber.calls) == 1
    assert transcriber.calls[0][0].local_path == result.source.local_path
    assert result.source.local_path is not None
    assert result.source.local_path.name == "combined.wav"


def test_run_transcription_checks_cancellation_inside_worker_loop(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-1",
            source_kind="uploaded_file",
            display_name="Audio: call.ogg",
            original_filename="call.ogg",
            object_key="uploads/call.ogg",
        )
    )
    api_client = RecordingApiClient(
        execution,
        cancel_results=[
            CancelCheckResult(cancel_requested=False, status="running"),
            CancelCheckResult(cancel_requested=False, status="running"),
            CancelCheckResult(cancel_requested=True, status="cancel_requested"),
        ],
    )
    source_store = FakeSourceStore({"uploads/call.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    with pytest.raises(WorkerCancellationRequested, match="was canceled"):
        runTranscription(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=transcriber,
        )

    assert [call[0] for call in api_client.calls if call[0] == "check_cancel"] == [
        "check_cancel",
        "check_cancel",
        "check_cancel",
    ]
    assert not any(call[0] == "register_artifacts" for call in api_client.calls)
    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "canceled",
            "progress_stage": "canceled",
            "progress_message": "Cancellation requested",
            "error_code": None,
            "error_message": None,
        },
    )


def test_process_local_transcription_reuses_extracted_local_pipeline(tmp_path: Path) -> None:
    source_path = tmp_path / "incoming.ogg"
    source_path.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="source-1",
        kind="telegram_audio",
        display_name="Audio: incoming.ogg",
        url=None,
        telegram_file_id=None,
        mime_type="audio/ogg",
        file_name="incoming.ogg",
        file_unique_id="uniq-1",
        local_path=source_path,
    )
    transcriber = RecordingTranscriber()
    workspace_dir = tmp_path / "job-1"

    materialized_source, transcript_result, artifacts = process_local_transcription(
        source,
        workspace_dir=workspace_dir,
        transcriber=transcriber,
    )

    assert materialized_source.local_path == workspace_dir / "source.ogg"
    assert transcript_result.raw_text == "Hello world"
    assert artifacts.text_path.read_text(encoding="utf-8") == "Hello world\n"
    assert artifacts.markdown_path.exists()
    assert artifacts.docx_path.exists()


def test_run_transcription_classifies_source_materialization_failures(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-1",
            source_kind="uploaded_file",
            display_name="Audio: broken.ogg",
            original_filename="broken.ogg",
            object_key=None,
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    with pytest.raises(worker_module.SourceMaterializationError, match="object_key"):
        runTranscription(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=transcriber,
        )

    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Transcription failed",
            "error_code": "source_fetch_failed",
            "error_message": "uploaded_file input must include object_key",
        },
    )


def _build_execution(*ordered_inputs: OrderedWorkerInput) -> ClaimedJobExecution:
    return ClaimedJobExecution(
        execution_id="exec-1",
        job_id="job-1",
        root_job_id="root-1",
        parent_job_id=None,
        retry_of_job_id=None,
        job_type="transcription",
        version=1,
        ordered_inputs=ordered_inputs,
        params={},
    )


def _required_marker() -> str:
    return "[WorkerTranscription][runTranscription][BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE]"
