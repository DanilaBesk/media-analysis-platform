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

import json
import logging
from pathlib import Path

import pytest
from docx import Document

from transcriber_workers_common.api import (
    ArtifactResolutionResult,
    ArtifactSummary,
    CancelCheckResult,
    ChildJobReference,
    ClaimedJobExecution,
    JobSnapshot,
    OrderedWorkerInput,
    SourceSetItem,
)
from transcriber_workers_common.domain import SourceCandidate, TranscriptResult, TranscriptSegment
import transcriber_worker_transcription as worker_module
from transcriber_worker_transcription import (
    WorkerCancellationRequested,
    materialize_local_source,
    process_local_transcription,
    runTranscriptionAggregate,
    runTranscription,
)


class RecordingApiClient:
    def __init__(
        self,
        execution: ClaimedJobExecution,
        *,
        cancel_results: list[CancelCheckResult] | None = None,
        snapshots: dict[str, JobSnapshot] | None = None,
        artifact_downloads: dict[str, Path] | None = None,
    ) -> None:
        self.execution = execution
        self.cancel_results = list(cancel_results or [])
        self.snapshots = dict(snapshots or {})
        self.artifact_downloads = dict(artifact_downloads or {})
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

    def get_job_snapshot(self, job_id: str) -> JobSnapshot:
        self.calls.append(("get_job_snapshot", {"job_id": job_id}))
        return self.snapshots[job_id]

    def resolve_artifact(self, artifact_id: str) -> ArtifactResolutionResult:
        self.calls.append(("resolve_artifact", {"artifact_id": artifact_id}))
        path = self.artifact_downloads[artifact_id]
        return ArtifactResolutionResult(
            artifact_id=artifact_id,
            job_id="child-job",
            artifact_kind="transcript_plain",
            filename=path.name,
            mime_type="text/plain; charset=utf-8",
            size_bytes=path.stat().st_size,
            download_url=path.as_uri(),
        )


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


class FailingTranscriber:
    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        raise RuntimeError("whisper crashed")


def test_ordered_worker_input_parses_source_label_from_claim_payload() -> None:
    ordered_input = OrderedWorkerInput.from_payload(
        {
            "position": 0,
            "source_id": "source-1",
            "source_label": "voice_a",
            "source_kind": "uploaded_file",
            "display_name": "Voice A",
            "original_filename": "voice.ogg",
            "object_key": "uploads/voice.ogg",
            "source_url": None,
            "sha256": None,
            "size_bytes": 5,
        }
    )

    assert ordered_input.source_label == "voice_a"


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


def test_run_transcription_batch_child_preserves_ordered_source_label_without_concat(
    tmp_path: Path,
    monkeypatch,
) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-voice",
            source_label="voice_a",
            source_kind="uploaded_file",
            display_name="Voice A",
            original_filename="voice.ogg",
            object_key="uploads/voice.ogg",
        ),
        params={"batch": {"role": "source", "source_label": "params_label"}},
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/voice.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    def fail_concat(input_paths: list[Path], output_path: Path) -> None:
        raise AssertionError("batch source children must not concatenate inputs first")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fail_concat)

    result = runTranscription(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert result.source.display_name == "voice_a"
    assert result.source.file_name == "voice.ogg"
    assert result.transcript.source_label == "voice_a"
    assert [call[0] for call in source_store.calls] == ["uploads/voice.ogg"]


def test_run_transcription_batch_child_preserves_params_source_label_for_telegram_document_media(
    tmp_path: Path,
) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-video",
            source_kind="telegram_upload",
            display_name="Video attachment",
            original_filename="clip.mp4",
            object_key="telegram/clip.mp4",
        ),
        params={"batch": {"role": "source", "source_label": "video_b"}},
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"telegram/clip.mp4": b"video"})
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

    assert result.source.kind == "telegram_video"
    assert result.source.display_name == "video_b"
    assert result.source.file_name == "clip.mp4"
    assert result.transcript.source_label == "video_b"


def test_run_transcription_batch_child_preserves_source_label_for_youtube_input(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-youtube",
            source_label="youtube_c",
            source_kind="youtube_url",
            display_name="Demo video",
            source_url="https://youtu.be/demo123",
        ),
        params={"batch": {"role": "source", "source_label": "params_label"}},
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({})
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

    assert source_store.calls == []
    assert result.source.kind == "youtube_url"
    assert result.source.url == "https://youtu.be/demo123"
    assert result.source.display_name == "youtube_c"
    assert result.transcript.source_label == "youtube_c"


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


def test_materialize_local_source_keeps_workspace_file_in_place(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "job-1"
    workspace_dir.mkdir(parents=True, exist_ok=True)
    existing = workspace_dir / "source.ogg"
    existing.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="source-2",
        kind="telegram_audio",
        display_name="Audio: source.ogg",
        url=None,
        telegram_file_id=None,
        mime_type="audio/ogg",
        file_name="source.ogg",
        file_unique_id="uniq-2",
        local_path=existing,
    )

    materialized_source = materialize_local_source(source, workspace_dir)

    assert materialized_source.local_path == existing
    assert existing.read_bytes() == b"audio"


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


def test_run_transcription_classifies_transcriber_failures(tmp_path: Path) -> None:
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

    with pytest.raises(RuntimeError, match="whisper crashed"):
        runTranscription(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=FailingTranscriber(),
        )

    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Transcription failed",
            "error_code": "transcription_failed",
            "error_message": "whisper crashed",
        },
    )


def test_run_transcription_aggregate_merges_child_artifacts_in_source_set_order(tmp_path: Path) -> None:
    execution = _build_execution(
        job_id="root-job",
        root_job_id="root-job",
        params={
            "batch": {
                "role": "aggregate",
                "completion_policy": "succeed_when_all_sources_succeed",
                "ordered_source_labels": ["voice_b", "voice_a"],
            }
        },
    )
    downloads = {
        "plain-a": _download_file(tmp_path, "plain-a.txt", "Voice A plain\n"),
        "md-a": _download_file(tmp_path, "md-a.md", "# Child A\n\nA segmented\n"),
        "plain-b": _download_file(tmp_path, "plain-b.txt", "Voice B plain\n"),
        "md-b": _download_file(tmp_path, "md-b.md", "# Child B\n\nB segmented\n"),
    }
    snapshots = {
        "root-job": _snapshot(
            "root-job",
            labels=["voice_b", "voice_a"],
            children=[
                ChildJobReference(job_id="child-a", job_type="transcription", status="succeeded", version=2),
                ChildJobReference(job_id="child-b", job_type="transcription", status="succeeded", version=2),
            ],
        ),
        "child-a": _snapshot(
            "child-a",
            parent_job_id="root-job",
            labels=["voice_a"],
            artifacts=[
                _artifact("plain-a", "transcript_plain", "a.txt"),
                _artifact("md-a", "transcript_segmented_markdown", "a.md"),
            ],
        ),
        "child-b": _snapshot(
            "child-b",
            parent_job_id="root-job",
            labels=["voice_b"],
            artifacts=[
                _artifact("plain-b", "transcript_plain", "b.txt"),
                _artifact("md-b", "transcript_segmented_markdown", "b.md"),
            ],
        ),
    }
    api_client = RecordingApiClient(execution, snapshots=snapshots, artifact_downloads=downloads)
    artifact_store = InMemoryArtifactStore()

    result = runTranscriptionAggregate(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=artifact_store,
    )

    assert api_client.calls[0] == (
        "claim_job",
        {"job_id": "root-job", "worker_kind": "transcription", "task_type": "transcription.aggregate"},
    )
    markdown = result.artifacts.markdown_path.read_text(encoding="utf-8")
    assert markdown.index("## Транскрибация voice_b") < markdown.index("## Транскрибация voice_a")
    assert "B segmented" in markdown
    assert "A segmented" in markdown
    assert result.artifacts.text_path.read_text(encoding="utf-8") == (
        "## Транскрибация voice_b\n\nVoice B plain\n\n"
        "## Транскрибация voice_a\n\nVoice A plain\n"
    )
    assert result.artifacts.docx_path.exists()
    docx_text = "\n".join(paragraph.text for paragraph in Document(result.artifacts.docx_path).paragraphs)
    assert docx_text.index("Транскрибация voice_b") < docx_text.index("Транскрибация voice_a")
    assert "Voice B plain" in docx_text
    assert "Voice A plain" in docx_text
    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "transcript_plain",
        "transcript_segmented_markdown",
        "transcript_docx",
        "source_manifest_json",
        "batch_diagnostics_json",
    ]
    finalize_call = api_client.calls[-1]
    assert finalize_call[0] == "finalize_job"
    assert api_client.calls.index(register_call) < api_client.calls.index(finalize_call)
    diagnostics_upload = next(call for call in artifact_store.calls if call["object_key"].endswith("batch-diagnostics.json"))
    diagnostics = json.loads(diagnostics_upload["content"].decode("utf-8"))
    assert diagnostics["included_count"] == 2
    assert diagnostics["skipped_sources"] == []
    manifest_upload = next(call for call in artifact_store.calls if call["object_key"].endswith("source-manifest.json"))
    manifest = json.loads(manifest_upload["content"].decode("utf-8"))
    assert manifest == {
        "manifest_version": "batch-transcription.aggregate.v1",
        "root_job_id": "root-job",
        "ordered_source_labels": ["voice_b", "voice_a"],
        "included_sources": [
            {"source_label": "voice_b", "child_job_id": "child-b"},
            {"source_label": "voice_a", "child_job_id": "child-a"},
        ],
    }


def test_run_transcription_aggregate_fails_when_succeeded_child_artifact_is_missing(tmp_path: Path) -> None:
    execution = _build_execution(
        job_id="root-job",
        root_job_id="root-job",
        params={
            "batch": {
                "role": "aggregate",
                "completion_policy": "succeed_when_all_sources_succeed",
                "ordered_source_labels": ["voice_a"],
            }
        },
    )
    snapshots = {
        "root-job": _snapshot(
            "root-job",
            labels=["voice_a"],
            children=[ChildJobReference(job_id="child-a", job_type="transcription", status="succeeded", version=2)],
        ),
        "child-a": _snapshot(
            "child-a",
            parent_job_id="root-job",
            labels=["voice_a"],
            artifacts=[_artifact("plain-a", "transcript_plain", "a.txt")],
        ),
    }
    api_client = RecordingApiClient(execution, snapshots=snapshots)

    with pytest.raises(worker_module.MissingChildTranscriptArtifactError, match="transcript_segmented_markdown"):
        runTranscriptionAggregate(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=InMemoryArtifactStore(),
        )

    assert not any(call[0] == "register_artifacts" for call in api_client.calls)
    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": "root-job",
            "execution_id": execution.execution_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Batch transcript aggregation failed",
            "error_code": "missing_child_artifact",
            "error_message": "source voice_a child child-a is missing artifact(s): transcript_segmented_markdown",
        },
    )


def test_run_transcription_aggregate_best_effort_skips_failed_children(tmp_path: Path) -> None:
    execution = _build_execution(
        job_id="root-job",
        root_job_id="root-job",
        params={
            "batch": {
                "role": "aggregate",
                "completion_policy": "succeed_when_any_source_succeeds",
                "ordered_source_labels": ["voice_a", "voice_b"],
            }
        },
    )
    downloads = {
        "plain-a": _download_file(tmp_path, "plain-a.txt", "Voice A plain\n"),
        "md-a": _download_file(tmp_path, "md-a.md", "# Child A\n\nA segmented\n"),
    }
    snapshots = {
        "root-job": _snapshot(
            "root-job",
            labels=["voice_a", "voice_b"],
            children=[
                ChildJobReference(job_id="child-a", job_type="transcription", status="succeeded", version=2),
                ChildJobReference(job_id="child-b", job_type="transcription", status="failed", version=2),
            ],
        ),
        "child-a": _snapshot(
            "child-a",
            parent_job_id="root-job",
            labels=["voice_a"],
            artifacts=[
                _artifact("plain-a", "transcript_plain", "a.txt"),
                _artifact("md-a", "transcript_segmented_markdown", "a.md"),
            ],
        ),
        "child-b": _snapshot("child-b", parent_job_id="root-job", labels=["voice_b"], status="failed"),
    }
    api_client = RecordingApiClient(execution, snapshots=snapshots, artifact_downloads=downloads)

    result = runTranscriptionAggregate(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=InMemoryArtifactStore(),
    )

    assert [section.source_label for section in result.sections] == ["voice_a"]
    assert result.diagnostics["skipped_sources"] == [
        {
            "source_label": "voice_b",
            "reason": "child_not_succeeded",
            "child_statuses": ["failed"],
            "child_job_ids": ["child-b"],
        }
    ]
    assert api_client.calls[-1][0] == "finalize_job"
    assert api_client.calls[-1][1]["outcome"] == "succeeded"


def _build_execution(
    *ordered_inputs: OrderedWorkerInput,
    job_id: str = "job-1",
    root_job_id: str = "root-1",
    params: dict[str, object] | None = None,
) -> ClaimedJobExecution:
    return ClaimedJobExecution(
        execution_id="exec-1",
        job_id=job_id,
        root_job_id=root_job_id,
        parent_job_id=None,
        retry_of_job_id=None,
        job_type="transcription",
        version=1,
        ordered_inputs=ordered_inputs,
        params=params or {},
    )


def _snapshot(
    job_id: str,
    *,
    labels: list[str],
    status: str = "succeeded",
    parent_job_id: str | None = None,
    artifacts: list[ArtifactSummary] | None = None,
    children: list[ChildJobReference] | None = None,
) -> JobSnapshot:
    return JobSnapshot(
        job_id=job_id,
        root_job_id="root-job",
        parent_job_id=parent_job_id,
        job_type="transcription",
        status=status,
        version=2,
        source_set_items=tuple(SourceSetItem(position=index, source_label=label) for index, label in enumerate(labels)),
        artifacts=tuple(artifacts or ()),
        children=tuple(children or ()),
    )


def _artifact(artifact_id: str, artifact_kind: str, filename: str) -> ArtifactSummary:
    return ArtifactSummary(
        artifact_id=artifact_id,
        artifact_kind=artifact_kind,
        filename=filename,
        mime_type="text/plain; charset=utf-8",
        size_bytes=10,
    )


def _download_file(tmp_path: Path, filename: str, content: str) -> Path:
    path = tmp_path / "downloads" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _required_marker() -> str:
    return "[WorkerTranscription][runTranscription][BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE]"
