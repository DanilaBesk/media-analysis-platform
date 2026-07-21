# FILE: workers/transcription/tests/test_transcriber_worker_transcription.py
# VERSION: 1.1.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the dedicated transcription worker claims analysis runs through the shared control plane, preserves transcript artifacts, and handles ordered combined inputs plus cancellation deterministically.
# SCOPE: Success finalization ordering, combined-media and textual-object assembly, cancellation checkpoints, local extraction reuse, and deterministic failure classification.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.1.0 - Added mixed inline and object-backed text document assembly regressions.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   RecordingApiClient - Captures claim/progress/artifact/finalize/cancel calls without redefining DTO payloads.
#   FakeSourceStore - Writes claimed input bytes into the worker workspace.
#   InMemoryArtifactStore - Captures uploaded artifact bytes.
#   RecordingTranscriber - Records worker transcription calls while returning deterministic transcript results.
#   test_run_transcription_claims_and_finalizes_after_all_artifacts_exist - Verifies the shared claim client, preserved transcript artifacts, and success finalization ordering.
#   test_run_transcription_combines_sorted_inputs_before_one_final_pass - Verifies ordered combined-media concatenation before the single transcription pass.
#   test_run_transcription_checks_cancellation_inside_worker_loop - Verifies cancellation is observed by the dedicated worker loop.
#   test_process_local_transcription_reuses_extracted_local_pipeline - Verifies the extracted local pipeline preserves current transcript artifacts for the service shell.
#   test_run_transcription_classifies_source_materialization_failures - Verifies deterministic `source_fetch_failed` finalization.
# END_MODULE_MAP

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path

import pytest

from transcriber_workers_common.api import (
    AnalysisRunStepInput,
    ArtifactResolutionResult,
    CancelCheckResult,
    ClaimedAnalysisRunStep,
    MediaSourceSnapshot,
    OrderedWorkerInput,
    SealedSelectionSnapshotInput,
    SelectionItemLabels,
    SelectionItemSnapshot,
)
from transcriber_workers_common.copper_asr import CopperAsrTranscriptionError
from transcriber_workers_common.domain import SourceCandidate, TranscriptResult, TranscriptSegment
import transcriber_worker_transcription as worker_module
from transcriber_worker_transcription import (
    WorkerCancellationRequested,
    materialize_local_source,
    process_local_transcription,
    runTranscription,
)


class RecordingApiClient:
    def __init__(
        self,
        execution: ClaimedAnalysisRunStep,
        *,
        cancel_results: list[CancelCheckResult] | None = None,
        artifact_downloads: dict[str, Path] | None = None,
    ) -> None:
        self.execution = execution
        self.cancel_results = list(cancel_results or [])
        self.artifact_downloads = dict(artifact_downloads or {})
        self.calls: list[tuple[str, dict[str, object]]] = []

    def claim_analysis_run_step(self, analysis_run_id: str, *, worker_kind: str, step_kind: str) -> ClaimedAnalysisRunStep:
        self.calls.append(
            (
                "claim_analysis_run_step",
                {"analysis_run_id": analysis_run_id, "worker_kind": worker_kind, "step_kind": step_kind},
            )
        )
        return self.execution

    def publish_progress(
        self,
        analysis_run_id: str,
        *,
        analysis_run_step_id: str,
        progress_stage: str,
        progress_message: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "publish_progress",
                {
                    "analysis_run_id": analysis_run_id,
                    "analysis_run_step_id": analysis_run_step_id,
                    "progress_stage": progress_stage,
                    "progress_message": progress_message,
                },
            )
        )

    def register_artifacts(self, analysis_run_id: str, *, analysis_run_step_id: str, artifacts) -> None:
        self.calls.append(
            (
                "register_artifacts",
                {
                    "analysis_run_id": analysis_run_id,
                    "analysis_run_step_id": analysis_run_step_id,
                    "artifacts": tuple(artifacts),
                },
            )
        )

    def register_diagnostics(self, analysis_run_id: str, *, analysis_run_step_id: str, diagnostics) -> None:
        self.calls.append(
            (
                "register_diagnostics",
                {
                    "analysis_run_id": analysis_run_id,
                    "analysis_run_step_id": analysis_run_step_id,
                    "diagnostics": tuple(diagnostics),
                },
            )
        )

    def finalize_analysis_run(
        self,
        analysis_run_id: str,
        *,
        analysis_run_step_id: str,
        outcome: str,
        progress_stage: str | None = None,
        progress_message: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "finalize_analysis_run",
                {
                    "analysis_run_id": analysis_run_id,
                    "analysis_run_step_id": analysis_run_step_id,
                    "outcome": outcome,
                    "progress_stage": progress_stage,
                    "progress_message": progress_message,
                    "error_code": error_code,
                    "error_message": error_message,
                },
            )
        )

    def check_cancel(self, analysis_run_id: str, *, analysis_run_step_id: str) -> CancelCheckResult:
        self.calls.append(
            (
                "check_cancel",
                {"analysis_run_id": analysis_run_id, "analysis_run_step_id": analysis_run_step_id},
            )
        )
        if self.cancel_results:
            return self.cancel_results.pop(0)
        return CancelCheckResult(cancel_requested=False, status="running")

    def resolve_artifact(self, artifact_id: str) -> ArtifactResolutionResult:
        self.calls.append(("resolve_artifact", {"artifact_id": artifact_id}))
        path = self.artifact_downloads[artifact_id]
        return ArtifactResolutionResult(
            artifact_id=artifact_id,
            analysis_run_id="child-run",
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
            provider_metadata={
                "provider": "copperasr",
                "model": "Copperside/CoppersideASR",
                "duration": 1.5,
                "metadata": {"ignored_params": []},
            },
        )


class FailingTranscriber:
    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        raise RuntimeError("asr crashed")


class FailingCopperAsrTranscriber:
    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        raise CopperAsrTranscriptionError(
            "CopperASR invalid_audio: Invalid or unsupported audio",
            diagnostic_code="asr_invalid_audio",
            provider_code="invalid_audio",
            status_code=422,
            retryable=False,
            request_id="req-1",
        )


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
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    claim_call = api_client.calls[0]
    assert claim_call == (
        "claim_analysis_run_step",
        {"analysis_run_id": execution.analysis_run_id, "worker_kind": "transcription", "step_kind": "selection.transcription"},
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
        "run_manifest",
        "run_diagnostics",
    ]
    finalize_call = api_client.calls[-1]
    assert finalize_call[0] == "finalize_analysis_run"
    assert finalize_call[1]["outcome"] == "succeeded"
    assert api_client.calls.index(register_call) < api_client.calls.index(finalize_call)
    assert result.artifacts.text_path.exists()
    assert result.artifacts.markdown_path.exists()
    assert result.artifacts.docx_path.exists()
    assert len(artifact_store.calls) == 5
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert manifest["analysis_run_id"] == execution.analysis_run_id
    assert manifest["summary"]["included_count"] == 1
    assert manifest["transcription_backend"]["provider"] == "copperasr"
    assert manifest["transcription_backend"]["model"] == "Copperside/CoppersideASR"
    assert manifest["transcription_backend"]["metadata"] == {"ignored_params": []}
    assert manifest["items"][0]["lineage"]["media_asset_id"] == "media-source-1"
    assert manifest["items"][0]["outcome"] == "succeeded"
    diagnostics_bundle = _artifact_json(artifact_store, "run/diagnostics/run-diagnostics.json")
    assert diagnostics_bundle["diagnostics"] == []
    assert _required_marker() in caplog.text


def test_run_transcription_sanitizes_workspace_and_artifact_prefix_for_control_plane_ids(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-1",
            source_kind="uploaded_file",
            display_name="Audio: call.ogg",
            original_filename="call.ogg",
            object_key="uploads/call.ogg",
        ),
        analysis_run_id="../escape/run",
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/call.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    workspace_dir = transcriber.calls[0][1].resolve()
    assert workspace_dir.is_relative_to(tmp_path.resolve())
    assert workspace_dir.name.startswith("escape-run-")
    assert not (tmp_path.parent / "escape").exists()
    assert all(not str(call["object_key"]).startswith("../") for call in artifact_store.calls)


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
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert [call[0] for call in source_store.calls] == ["uploads/first.ogg", "uploads/second.ogg"]
    assert concat_calls == [["item-0000-source-1.ogg", "item-0001-source-2.ogg"]]
    assert len(transcriber.calls) == 1
    assert transcriber.calls[0][0].local_path == result.source.local_path
    assert result.source.local_path is not None
    assert result.source.local_path.name == "combined.wav"


def test_run_transcription_uses_selection_metadata_source_label_for_single_object_item(
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
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/voice.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    def fail_concat(input_paths: list[Path], output_path: Path) -> None:
        raise AssertionError("single selection item must not be concatenated")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fail_concat)

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert result.source.display_name == "voice_a"
    assert result.source.file_name == "item-0000-source-voice.ogg"
    assert result.transcript.source_label == "voice_a"
    assert [call[0] for call in source_store.calls] == ["uploads/voice.ogg"]


def test_run_transcription_uses_selection_metadata_source_label_for_video_object_item(
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
            source_label="video_b",
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"telegram/clip.mp4": b"video"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert result.source.kind == "telegram_video"
    assert result.source.display_name == "video_b"
    assert result.source.file_name == "item-0000-source-video.mp4"
    assert result.transcript.source_label == "video_b"


def test_run_transcription_supports_single_youtube_url_only_selection(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-youtube",
            source_label="youtube_c",
            source_kind="youtube_url",
            display_name="Demo video",
            source_url="https://youtu.be/demo123",
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert source_store.calls == []
    assert len(transcriber.calls) == 1
    transcribed_source, workspace_dir = transcriber.calls[0]
    assert workspace_dir == tmp_path / execution.analysis_run_id
    assert transcribed_source.source_id == "source-youtube"
    assert transcribed_source.kind == "youtube_url"
    assert transcribed_source.display_name == "youtube_c"
    assert transcribed_source.url == "https://youtu.be/demo123"
    assert transcribed_source.local_path is None
    assert result.source == transcribed_source
    assert result.diagnostics == ()
    assert not any(call[0] == "register_diagnostics" for call in api_client.calls)
    assert api_client.calls[-1][0] == "finalize_analysis_run"
    assert api_client.calls[-1][1]["outcome"] == "succeeded"
    assert api_client.calls[-1][1]["error_message"] is None
    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "transcript_plain",
        "transcript_segmented_markdown",
        "transcript_docx",
        "run_manifest",
        "run_diagnostics",
    ]
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert manifest["summary"] == {"included_count": 1, "skipped_count": 0, "failed_count": 0}
    assert manifest["items"][0]["outcome"] == "succeeded"
    assert manifest["items"][0]["diagnostic_ids"] == []
    assert manifest["items"][0]["lineage"]["origin_type"] == "url"
    assert manifest["items"][0]["lineage"]["selection_snapshot_item_id"] == "selection-snapshot-item-0"


def test_run_transcription_rejects_single_non_youtube_url_selection(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-external",
            source_kind="external_url",
            display_name="Reference URL",
            source_url="https://example.test/reference",
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    with pytest.raises(worker_module.SourceMaterializationError, match="no object-backed media items"):
        runTranscription(
            execution.analysis_run_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=transcriber,
        )

    assert source_store.calls == []
    assert transcriber.calls == []
    diagnostics_call = next(call for call in api_client.calls if call[0] == "register_diagnostics")
    diagnostics = diagnostics_call[1]["diagnostics"]
    assert [diagnostic["subject_id"] for diagnostic in diagnostics] == ["media-source-external"]
    assert diagnostics[0]["context"]["origin_type"] == "url"
    assert diagnostics[0]["context"]["external_uri"] == "https://example.test/reference"
    assert api_client.calls[-1][1]["outcome"] == "failed"


def test_run_transcription_mixed_selection_records_item_diagnostics_and_partial_outcome(
    tmp_path: Path,
    monkeypatch,
) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-audio",
            source_label="voice_a",
            source_kind="uploaded_file",
            display_name="Voice A",
            original_filename="voice.ogg",
            object_key="uploads/voice.ogg",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-note",
            source_kind="text",
            display_name="Manual note",
        ),
        OrderedWorkerInput(
            position=2,
            source_id="source-url",
            source_kind="youtube_url",
            display_name="Reference URL",
            source_url="https://youtu.be/demo123",
        ),
        analysis_run_id="11111111-1111-1111-1111-111111111111",
        root_analysis_run_id="22222222-2222-2222-2222-222222222222",
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/voice.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    def fail_concat(input_paths: list[Path], output_path: Path) -> None:
        raise AssertionError("single object-backed item must not be concatenated with unsupported text/url items")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fail_concat)

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert [call[0] for call in source_store.calls] == ["uploads/voice.ogg"]
    assert len(transcriber.calls) == 1
    assert result.source.display_name == "voice_a"
    diagnostics_call = next(call for call in api_client.calls if call[0] == "register_diagnostics")
    diagnostics = diagnostics_call[1]["diagnostics"]
    assert [diagnostic["subject_id"] for diagnostic in diagnostics] == ["media-source-url"]
    assert [diagnostic["context"]["item_position"] for diagnostic in diagnostics] == [2]
    assert {diagnostic["context"]["origin_type"] for diagnostic in diagnostics} == {"url"}
    assert all(diagnostic["context"]["analysis_run_id"] == execution.analysis_run_id for diagnostic in diagnostics)
    assert all(diagnostic["context"]["selection_snapshot_id"] == execution.selection_snapshot.selection_snapshot_id for diagnostic in diagnostics)
    assert all(diagnostic["context"]["selection_snapshot_item_id"] for diagnostic in diagnostics)
    assert all(diagnostic["context"]["media_asset_id"] for diagnostic in diagnostics)
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "succeeded", "skipped"]
    assert manifest["summary"] == {"included_count": 2, "skipped_count": 1, "failed_count": 0}
    assert manifest["items"][0]["artifact_kinds"] == [
        "transcript_plain",
        "transcript_segmented_markdown",
        "transcript_docx",
    ]
    assert manifest["items"][1]["lineage"]["selection_snapshot_item_id"] == "selection-snapshot-item-1"
    assert "source-note" in result.artifacts.text_path.read_text(encoding="utf-8")
    diagnostics_bundle = _artifact_json(artifact_store, "run/diagnostics/run-diagnostics.json")
    assert diagnostics_bundle["diagnostics"] == list(diagnostics)
    finalize_call = api_client.calls[-1]
    assert finalize_call[0] == "finalize_analysis_run"
    assert finalize_call[1]["outcome"] == "partially_succeeded"
    assert result.diagnostics == diagnostics


def test_run_transcription_includes_text_materials_in_transcript_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-note",
            source_kind="text",
            display_name="Manual note",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-audio",
            source_label="voice_a",
            source_kind="uploaded_file",
            display_name="Voice A",
            original_filename="voice.ogg",
            object_key="uploads/voice.ogg",
        ),
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/voice.ogg": b"audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    def fail_concat(input_paths: list[Path], output_path: Path) -> None:
        raise AssertionError("single voice plus text must not require media concatenation")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fail_concat)

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert "source-note" in result.artifacts.text_path.read_text(encoding="utf-8")
    assert "Hello world" in result.artifacts.text_path.read_text(encoding="utf-8")
    assert result.diagnostics == ()
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "succeeded"]
    assert manifest["summary"] == {"included_count": 2, "skipped_count": 0, "failed_count": 0}


def test_run_transcription_includes_uploaded_plain_text_document_in_selection_order_without_asr(
    tmp_path: Path,
) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-note-a",
            source_kind="text",
            display_name="Manual note A",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-note-b",
            source_kind="text",
            display_name="Manual note B",
        ),
        OrderedWorkerInput(
            position=2,
            source_id="source-transcript",
            source_kind="uploaded_file",
            display_name="transcript.txt",
            original_filename="transcript.txt",
            object_key="uploads/transcript.txt",
        ),
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"uploads/transcript.txt": "Текст из файла".encode()})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert result.artifacts.text_path.read_text(encoding="utf-8") == (
        "source-note-a\n\nsource-note-b\n\nТекст из файла\n"
    )
    assert [call[0] for call in source_store.calls] == ["uploads/transcript.txt"]
    assert transcriber.calls == []
    assert result.diagnostics == ()
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "succeeded", "succeeded"]
    assert manifest["summary"] == {"included_count": 3, "skipped_count": 0, "failed_count": 0}
    assert api_client.calls[-1][1]["outcome"] == "succeeded"


def test_run_transcription_marks_unreadable_plain_text_document_as_failed_item(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-note",
            source_kind="text",
            display_name="Manual note",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-unreadable",
            source_kind="uploaded_file",
            display_name="broken.txt",
            original_filename="broken.txt",
            object_key="uploads/broken.txt",
        ),
    )
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=FakeSourceStore({"uploads/broken.txt": b"\xff\xff\xff"}),
        artifact_store=artifact_store,
        transcriber=RecordingTranscriber(),
    )

    assert result.artifacts.text_path.read_text(encoding="utf-8") == "source-note\n"
    assert len(result.diagnostics) == 1
    assert "decode text document as UTF-8" in str(result.diagnostics[0]["message"])
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "failed"]
    assert manifest["summary"] == {"included_count": 1, "skipped_count": 0, "failed_count": 1}
    assert api_client.calls[-1][1]["outcome"] == "partially_succeeded"


def test_run_transcription_text_only_selection_writes_transcript_without_asr(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-note-a",
            source_kind="text",
            display_name="Manual note A",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-note-b",
            source_kind="text",
            display_name="Manual note B",
        ),
    )
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=FakeSourceStore({}),
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    transcript_text = result.artifacts.text_path.read_text(encoding="utf-8")
    assert "source-note-a" in transcript_text
    assert "source-note-b" in transcript_text
    assert transcriber.calls == []
    assert result.diagnostics == ()
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "succeeded"]
    assert manifest["summary"] == {"included_count": 2, "skipped_count": 0, "failed_count": 0}


def test_run_transcription_reuses_declared_transcript_artifact_and_transcribes_only_missing_speech(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reused_transcript = tmp_path / "reused-transcript.txt"
    reused_transcript.write_text("Reusable speech transcript", encoding="utf-8")
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-reused",
            source_label="voice_reused",
            source_kind="uploaded_file",
            display_name="Reused voice",
            original_filename="reused.ogg",
            object_key="uploads/reused.ogg",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-new",
            source_label="voice_new",
            source_kind="uploaded_file",
            display_name="New voice",
            original_filename="new.ogg",
            object_key="uploads/new.ogg",
        ),
        OrderedWorkerInput(
            position=2,
            source_id="source-note",
            source_kind="text",
            display_name="Manual note",
        ),
        step_inputs=(
            AnalysisRunStepInput(
                analysis_run_step_input_id="input-reused",
                analysis_run_step_id="exec-1",
                input_kind="transcript_artifact",
                position=0,
                required=True,
                selection_snapshot_item_id="selection-snapshot-item-0",
                artifact_id="artifact-reused",
            ),
            AnalysisRunStepInput(
                analysis_run_step_input_id="input-new",
                analysis_run_step_id="exec-1",
                input_kind="selection_snapshot_item",
                position=1,
                required=True,
                selection_snapshot_item_id="selection-snapshot-item-1",
            ),
            AnalysisRunStepInput(
                analysis_run_step_input_id="input-text",
                analysis_run_step_id="exec-1",
                input_kind="selection_snapshot_item",
                position=2,
                required=True,
                selection_snapshot_item_id="selection-snapshot-item-2",
            ),
        ),
    )
    api_client = RecordingApiClient(execution, artifact_downloads={"artifact-reused": reused_transcript})
    source_store = FakeSourceStore({"uploads/reused.ogg": b"old-audio", "uploads/new.ogg": b"new-audio"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    def fake_concat(input_paths: list[Path], output_path: Path) -> None:
        output_path.write_bytes(b"combined")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fake_concat)

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert [call[0] for call in source_store.calls] == ["uploads/new.ogg"]
    assert [call for call in api_client.calls if call[0] == "resolve_artifact"] == [
        ("resolve_artifact", {"artifact_id": "artifact-reused"})
    ]
    assert len(transcriber.calls) == 1
    transcript_text = result.artifacts.text_path.read_text(encoding="utf-8")
    assert transcript_text.index("Reusable speech transcript") < transcript_text.index("Hello world")
    assert transcript_text.index("Hello world") < transcript_text.index("source-note")
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "succeeded", "succeeded"]


def test_run_transcription_assembles_declared_transcript_artifact_with_uploaded_text_document(
    tmp_path: Path,
) -> None:
    reused_transcript = tmp_path / "reused-transcript.txt"
    reused_transcript.write_text("Reusable speech transcript", encoding="utf-8")
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-reused",
            source_kind="uploaded_file",
            display_name="Reused voice",
            original_filename="reused.ogg",
            object_key="uploads/reused.ogg",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-document",
            source_kind="uploaded_file",
            display_name="notes.txt",
            original_filename="notes.txt",
            object_key="uploads/notes.txt",
        ),
        step_inputs=(
            AnalysisRunStepInput(
                analysis_run_step_input_id="input-reused",
                analysis_run_step_id="exec-1",
                input_kind="transcript_artifact",
                position=0,
                required=True,
                selection_snapshot_item_id="selection-snapshot-item-0",
                artifact_id="artifact-reused",
            ),
            AnalysisRunStepInput(
                analysis_run_step_input_id="input-document",
                analysis_run_step_id="exec-1",
                input_kind="selection_snapshot_item",
                position=1,
                required=True,
                selection_snapshot_item_id="selection-snapshot-item-1",
            ),
        ),
    )
    api_client = RecordingApiClient(execution, artifact_downloads={"artifact-reused": reused_transcript})
    source_store = FakeSourceStore({"uploads/notes.txt": b"Document notes"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert result.artifacts.text_path.read_text(encoding="utf-8") == (
        "Reusable speech transcript\n\nDocument notes\n"
    )
    assert [call[0] for call in source_store.calls] == ["uploads/notes.txt"]
    assert transcriber.calls == []
    assert result.diagnostics == ()
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert [item["outcome"] for item in manifest["items"]] == ["succeeded", "succeeded"]


def test_run_transcription_uses_v2_materialization_without_filename_heuristics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    execution = ClaimedAnalysisRunStep(
        analysis_run_step_id="exec-v2",
        analysis_run_id="run-v2",
        run_type="transcription",
        selection_snapshot=SealedSelectionSnapshotInput(
            selection_snapshot_id="selection-v2",
            option_snapshot={},
            sealed_at="2026-05-10T12:00:00Z",
            items=(
                _v2_selection_item(
                    position=0,
                    selection_snapshot_item_id="sel-item-audio",
                    media_asset_id="media-audio",
                    origin_ref="source-audio",
                    media_kind="audio",
                    mime_type="audio/ogg",
                    object_key="objects/audio-source",
                    display_label="Voice message",
                    source_label="voice_a",
                    original_filename="misleading-video.mp4",
                ),
                _v2_selection_item(
                    position=1,
                    selection_snapshot_item_id="sel-item-text",
                    media_asset_id="media-text",
                    origin_ref="source-text",
                    media_kind="text",
                    origin_type="text",
                    display_label="Manual note",
                    role="context",
                ),
                _v2_selection_item(
                    position=2,
                    selection_snapshot_item_id="sel-item-url",
                    media_asset_id="media-url",
                    origin_ref="source-url",
                    media_kind="url",
                    origin_type="url",
                    external_uri="https://example.test/reference",
                    display_label="Reference URL",
                    role="reference",
                ),
                _v2_selection_item(
                    position=3,
                    selection_snapshot_item_id="sel-item-document",
                    media_asset_id="media-document",
                    origin_ref="source-document",
                    media_kind="document",
                    mime_type="application/pdf",
                    object_key="objects/document-source",
                    display_label="Evidence PDF",
                    original_filename="contract.mp3",
                    role="reference",
                ),
            ),
        ),
        analysis_run_step_inputs=(),
        params={},
        claimed_at="2026-05-10T12:01:00Z",
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({"objects/audio-source": b"audio", "objects/document-source": b"pdf"})
    artifact_store = InMemoryArtifactStore()
    transcriber = RecordingTranscriber()

    def fail_concat(input_paths: list[Path], output_path: Path) -> None:
        raise AssertionError("only one transcribable object-backed item should be sent to the transcriber")

    monkeypatch.setattr(worker_module, "_concatenate_media_inputs", fail_concat)

    result = runTranscription(
        execution.analysis_run_id,
        workspace_root=tmp_path,
        api_client=api_client,
        source_store=source_store,
        artifact_store=artifact_store,
        transcriber=transcriber,
    )

    assert {Path(call[1]).name for call in source_store.calls} == {
        "item-0000-source-audio.ogg",
        "item-0003-source-document.pdf",
    }
    assert result.source.kind == "telegram_audio"
    assert result.source.display_name == "voice_a"
    assert result.source.file_name == "item-0000-source-audio.ogg"
    assert result.source.local_path is not None
    assert result.source.local_path.name == "item-0000-source-audio.ogg"
    diagnostics_call = next(call for call in api_client.calls if call[0] == "register_diagnostics")
    diagnostics = diagnostics_call[1]["diagnostics"]
    assert [diagnostic["subject_id"] for diagnostic in diagnostics] == [
        "media-url",
        "media-document",
    ]
    assert [diagnostic["context"]["role"] for diagnostic in diagnostics] == ["reference", "reference"]
    assert [diagnostic["context"]["selection_snapshot_item_id"] for diagnostic in diagnostics] == [
        "sel-item-url",
        "sel-item-document",
    ]
    assert diagnostics[1]["context"]["materialized_filename"] == "item-0003-source-document.pdf"
    assert "text:source-text" in result.artifacts.text_path.read_text(encoding="utf-8")
    assert api_client.calls[-1][1]["outcome"] == "partially_succeeded"


def test_run_transcription_records_per_item_fetch_failure_without_silent_drop(tmp_path: Path) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-broken",
            source_kind="uploaded_file",
            display_name="Broken voice",
            original_filename="broken.ogg",
            object_key="uploads/missing.ogg",
        )
    )
    api_client = RecordingApiClient(execution)
    source_store = FakeSourceStore({})
    artifact_store = InMemoryArtifactStore()

    with pytest.raises(worker_module.SourceMaterializationError, match="missing.ogg"):
        runTranscription(
            execution.analysis_run_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=RecordingTranscriber(),
        )

    diagnostics_call = next(call for call in api_client.calls if call[0] == "register_diagnostics")
    diagnostics = diagnostics_call[1]["diagnostics"]
    uuid.UUID(str(diagnostics[0]["diagnostic_id"]))
    assert diagnostics[0]["subject_id"] == "media-source-broken"
    assert diagnostics[0]["severity"] == "error"
    assert diagnostics[0]["context"]["selection_snapshot_item_id"] == "selection-snapshot-item-0"
    assert diagnostics[0]["context"]["media_asset_id"] == "media-source-broken"
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert manifest["summary"] == {"included_count": 0, "skipped_count": 0, "failed_count": 1}
    assert manifest["items"][0]["outcome"] == "failed"
    assert manifest["items"][0]["diagnostic_ids"] == [diagnostics[0]["diagnostic_id"]]
    diagnostics_bundle = _artifact_json(artifact_store, "run/diagnostics/run-diagnostics.json")
    assert diagnostics_bundle["diagnostics"] == list(diagnostics)


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

    with pytest.raises(WorkerCancellationRequested, match="was canceled") as exc_info:
        runTranscription(
            execution.analysis_run_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=transcriber,
        )

    assert getattr(exc_info.value, "suppress_worker_traceback") is True
    assert [call[0] for call in api_client.calls if call[0] == "check_cancel"] == [
        "check_cancel",
        "check_cancel",
        "check_cancel",
    ]
    assert not any(call[0] == "register_artifacts" for call in api_client.calls)
    assert api_client.calls[-1] == (
        "finalize_analysis_run",
        {
            "analysis_run_id": execution.analysis_run_id,
            "analysis_run_step_id": execution.analysis_run_step_id,
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
    workspace_dir = tmp_path / "run-1"

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
    workspace_dir = tmp_path / "run-1"
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


def test_materialize_local_source_returns_source_without_local_path(tmp_path: Path) -> None:
    source = SourceCandidate(
        source_id="source-3",
        kind="text",
        display_name="Inline note",
        url=None,
        telegram_file_id=None,
        mime_type="text/plain; charset=utf-8",
        file_name=None,
        file_unique_id=None,
        local_path=None,
    )

    assert materialize_local_source(source, tmp_path / "run-3") is source


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
            execution.analysis_run_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=transcriber,
        )

    assert api_client.calls[-1] == (
        "finalize_analysis_run",
        {
            "analysis_run_id": execution.analysis_run_id,
            "analysis_run_step_id": execution.analysis_run_step_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Transcription failed",
            "error_code": "source_fetch_failed",
            "error_message": "object-backed media source is missing object_key",
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

    with pytest.raises(RuntimeError, match="asr crashed"):
        runTranscription(
            execution.analysis_run_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=FailingTranscriber(),
        )

    assert api_client.calls[-1] == (
        "finalize_analysis_run",
        {
            "analysis_run_id": execution.analysis_run_id,
            "analysis_run_step_id": execution.analysis_run_step_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Transcription failed",
            "error_code": "transcription_failed",
            "error_message": "asr crashed",
        },
    )


def test_run_transcription_uses_copper_asr_diagnostic_code(tmp_path: Path) -> None:
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

    with pytest.raises(CopperAsrTranscriptionError, match="invalid_audio") as exc_info:
        runTranscription(
            execution.analysis_run_id,
            workspace_root=tmp_path,
            api_client=api_client,
            source_store=source_store,
            artifact_store=artifact_store,
            transcriber=FailingCopperAsrTranscriber(),
        )

    assert getattr(exc_info.value, "suppress_worker_traceback") is True
    assert api_client.calls[-1] == (
        "finalize_analysis_run",
        {
            "analysis_run_id": execution.analysis_run_id,
            "analysis_run_step_id": execution.analysis_run_step_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Transcription failed",
            "error_code": "asr_invalid_audio",
            "error_message": "CopperASR invalid_audio: Invalid or unsupported audio",
        },
    )
    diagnostics_call = next(call for call in api_client.calls if call[0] == "register_diagnostics")
    diagnostics = diagnostics_call[1]["diagnostics"]
    assert len(diagnostics) == 1
    assert diagnostics[0]["subject_type"] == "analysis_run"
    assert diagnostics[0]["subject_id"] == execution.analysis_run_id
    assert diagnostics[0]["severity"] == "error"
    assert diagnostics[0]["code"] == "asr_invalid_audio"
    assert diagnostics[0]["message"] == "CopperASR invalid_audio: Invalid or unsupported audio"
    assert diagnostics[0]["context"]["provider_code"] == "invalid_audio"
    assert diagnostics[0]["context"]["status_code"] == 422
    assert diagnostics[0]["context"]["retryable"] is False
    assert diagnostics[0]["context"]["request_id"] == "req-1"
    assert diagnostics[0]["context"]["affected_media_asset_ids"] == ["media-source-1"]

    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "run_manifest",
        "run_diagnostics",
    ]
    assert not any(call["object_key"].endswith("/transcript/plain/transcript.txt") for call in artifact_store.calls)
    manifest = _artifact_json(artifact_store, "run/manifest/run-manifest.json")
    assert manifest["summary"] == {"included_count": 0, "skipped_count": 0, "failed_count": 1}
    assert manifest["items"][0]["outcome"] == "failed"
    assert manifest["items"][0]["diagnostic_ids"] == [diagnostics[0]["diagnostic_id"]]
    diagnostics_bundle = _artifact_json(artifact_store, "run/diagnostics/run-diagnostics.json")
    assert diagnostics_bundle["diagnostics"] == list(diagnostics)


def test_transcription_failure_diagnostics_uses_all_items_when_no_successful_outcomes() -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-failed",
            source_kind="uploaded_file",
            display_name="Failed voice",
            original_filename="failed.ogg",
            object_key="uploads/failed.ogg",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-skipped",
            source_kind="text",
            display_name="Skipped note",
        ),
    )
    item_outcomes = (
        {
            "outcome": "failed",
            "selection_snapshot_item_id": "selection-snapshot-item-0",
            "media_asset_id": "media-source-failed",
        },
        {
            "outcome": "skipped",
            "selection_snapshot_item_id": "selection-snapshot-item-1",
            "media_asset_id": "media-source-skipped",
        },
    )

    diagnostics = worker_module._transcription_failure_diagnostics(
        execution,
        item_outcomes,
        CopperAsrTranscriptionError(
            "CopperASR request_timeout: request timed out",
            diagnostic_code="asr_request_timeout",
            provider_code="request_timeout",
            status_code=504,
            retryable=True,
        ),
    )

    context = diagnostics[0]["context"]
    assert context["affected_selection_snapshot_item_ids"] == [
        "selection-snapshot-item-0",
        "selection-snapshot-item-1",
    ]
    assert context["affected_media_asset_ids"] == ["media-source-failed", "media-source-skipped"]
    assert diagnostics[0]["remediation_hint"] == "retry"


def test_mark_transcription_failed_outcomes_preserves_existing_non_successful_items() -> None:
    skipped = {
        "outcome": "skipped",
        "included": False,
        "diagnostic_ids": ["diag-skip"],
    }
    outcomes = worker_module._mark_transcription_failed_outcomes(
        (
            skipped,
            {
                "outcome": "succeeded",
                "included": True,
                "artifact_kinds": ["transcript_plain"],
                "diagnostic_ids": [],
            },
        ),
        ({"diagnostic_id": "diag-asr"},),
    )

    assert outcomes[0] is skipped
    assert outcomes[1]["outcome"] == "failed"
    assert outcomes[1]["included"] is False
    assert outcomes[1]["artifact_kinds"] == []
    assert outcomes[1]["diagnostic_ids"] == ["diag-asr"]


def test_materialize_execution_source_tolerates_unsupported_object_fetch_failure(tmp_path: Path) -> None:
    execution = ClaimedAnalysisRunStep(
        analysis_run_step_id="exec-unsupported",
        analysis_run_id="run-unsupported",
        run_type="transcription",
        selection_snapshot=SealedSelectionSnapshotInput(
            selection_snapshot_id="selection-unsupported",
            option_snapshot={},
            sealed_at="2026-05-10T12:00:00Z",
            items=(
                _v2_selection_item(
                    position=0,
                    selection_snapshot_item_id="sel-item-document",
                    media_asset_id="media-document",
                    origin_ref="source-document",
                    media_kind="document",
                    mime_type="application/pdf",
                    object_key="objects/missing.pdf",
                    display_label="Evidence PDF",
                    original_filename="evidence.pdf",
                ),
            ),
        ),
        analysis_run_step_inputs=(),
        params={},
        claimed_at="2026-05-10T12:01:00Z",
    )

    with pytest.raises(worker_module.SourceMaterializationError, match="no object-backed media items") as exc_info:
        worker_module._materialize_execution_source(execution, tmp_path, FakeSourceStore({}))

    diagnostic = exc_info.value.diagnostics[0]
    assert diagnostic["context"]["selection_snapshot_item_id"] == "sel-item-document"
    assert "materialized_path" not in diagnostic["context"]


def test_materialize_unsupported_object_descriptor_returns_none_for_non_object(tmp_path: Path) -> None:
    descriptor = worker_module.SelectionItemMaterialization(
        selection_snapshot_item_id="sel-item-text",
        position=0,
        media_asset_id="media-text",
        media_kind="text",
        role="reference",
        labels=SelectionItemLabels(display_label="Manual note"),
        origin_ref="source-text",
        origin_type="text",
        materialization_kind="text",
        text_ref="text:source-text",
    )

    assert worker_module._materialize_unsupported_object_descriptor(descriptor, tmp_path, FakeSourceStore({})) is None


def test_supported_direct_youtube_descriptor_requires_external_uri() -> None:
    descriptor = worker_module.SelectionItemMaterialization(
        selection_snapshot_item_id="sel-item-url",
        position=0,
        media_asset_id="media-url",
        media_kind="url",
        role="primary",
        labels=SelectionItemLabels(display_label="URL without href"),
        origin_ref="source-url",
        origin_type="url",
        materialization_kind="url",
        external_uri=None,
    )

    assert worker_module._is_supported_direct_youtube_descriptor(descriptor) is False


def test_materialize_single_selection_item_downloads_object_backed_source(tmp_path: Path) -> None:
    descriptor = worker_module.SelectionItemMaterialization(
        selection_snapshot_item_id="sel-item-audio",
        position=2,
        media_asset_id="media-audio",
        media_kind="audio",
        role="primary",
        labels=SelectionItemLabels(display_label="Voice message"),
        origin_ref="source-audio",
        origin_type="object",
        materialization_kind="object",
        mime_type="audio/ogg",
        object_key="objects/audio-source",
        deterministic_filename="voice.ogg",
    )

    candidate = worker_module._materialize_single_selection_item(
        descriptor,
        tmp_path,
        FakeSourceStore({"objects/audio-source": b"voice-bytes"}),
    )

    assert candidate.local_path == tmp_path / "inputs" / "02-source-audio" / "voice.ogg"
    assert candidate.local_path.read_bytes() == b"voice-bytes"
    assert candidate.file_name == "voice.ogg"


def test_download_materialization_descriptor_requires_deterministic_filename(tmp_path: Path) -> None:
    descriptor = worker_module.SelectionItemMaterialization(
        selection_snapshot_item_id="sel-item-audio",
        position=0,
        media_asset_id="media-audio",
        media_kind="audio",
        role="primary",
        labels=SelectionItemLabels(display_label="Voice message"),
        origin_ref="source-audio",
        origin_type="object",
        materialization_kind="object",
        mime_type="audio/ogg",
        object_key="objects/audio-source",
        deterministic_filename=None,
    )

    with pytest.raises(worker_module.SourceMaterializationError, match="missing deterministic filename"):
        worker_module._download_materialization_descriptor(descriptor, tmp_path, FakeSourceStore({}))


def test_download_materialization_descriptor_requires_object_key_and_invalid_origin_is_rejected(tmp_path: Path) -> None:
    descriptor = worker_module.SelectionItemMaterialization(
        selection_snapshot_item_id="sel-item-audio",
        position=0,
        media_asset_id="media-audio",
        media_kind="audio",
        role="primary",
        labels=SelectionItemLabels(display_label="Voice message"),
        origin_ref="source-audio",
        origin_type="object",
        materialization_kind="object",
        mime_type="audio/ogg",
        object_key=None,
        deterministic_filename="voice.ogg",
    )

    with pytest.raises(worker_module.SourceMaterializationError, match="object_key"):
        worker_module._download_materialization_descriptor(descriptor, tmp_path, FakeSourceStore({}))

    with pytest.raises(ValueError, match="invalid materialization origin_type"):
        worker_module.SelectionItemMaterialization(
            selection_snapshot_item_id="sel-item-invalid",
            position=0,
            media_asset_id="media-invalid",
            media_kind="audio",
            role="primary",
            labels=SelectionItemLabels(display_label="Invalid"),
            origin_ref="source-invalid",
            origin_type="invalid",
            materialization_kind="unsupported",
        )


def test_concatenate_media_inputs_validates_input_count_and_ffmpeg_result(tmp_path: Path, monkeypatch) -> None:
    output_path = tmp_path / "combined.wav"
    first = tmp_path / "first.ogg"
    second = tmp_path / "second.ogg"
    first.write_bytes(b"first")
    second.write_bytes(b"second")

    with pytest.raises(worker_module.SourceMaterializationError, match="at least two inputs"):
        worker_module._concatenate_media_inputs([first], output_path)

    recorded_commands: list[list[str]] = []

    def fake_success(command, **kwargs):
        recorded_commands.append(list(command))
        return type("Completed", (), {"returncode": 0, "stderr": "", "stdout": ""})()

    monkeypatch.setattr(worker_module.subprocess, "run", fake_success)
    worker_module._concatenate_media_inputs([first, second], output_path)
    assert recorded_commands[0][:2] == ["ffmpeg", "-y"]
    assert any("concat=n=2:v=0:a=1[outa]" in part for part in recorded_commands[0])

    monkeypatch.setattr(
        worker_module.subprocess,
        "run",
        lambda command, **kwargs: type("Completed", (), {"returncode": 1, "stderr": "bad ffmpeg", "stdout": ""})(),
    )
    with pytest.raises(worker_module.SourceMaterializationError, match="ffmpeg concat failed with exit code 1"):
        worker_module._concatenate_media_inputs([first, second], output_path)


def test_outcomes_from_diagnostics_marks_missing_selection_snapshot_items_as_failed() -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="source-a",
            source_kind="uploaded_file",
            display_name="Audio A",
            original_filename="a.ogg",
            object_key="uploads/a.ogg",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="source-b",
            source_kind="uploaded_file",
            display_name="Audio B",
            original_filename="b.ogg",
            object_key="uploads/b.ogg",
        ),
    )
    diagnostics = (
        {"diagnostic_id": "ignored", "context": "not-a-mapping"},
        {
            "diagnostic_id": "exec-1:0:source-skipped",
            "severity": "warning",
            "context": {"selection_snapshot_item_id": "selection-snapshot-item-0"},
        },
    )

    outcomes = worker_module._outcomes_from_diagnostics(execution, diagnostics)

    assert [item["outcome"] for item in outcomes] == ["skipped", "failed"]
    assert outcomes[0]["diagnostic_ids"] == ["exec-1:0:source-skipped"]
    assert outcomes[1]["diagnostic_ids"] == []


def test_assert_required_artifacts_exist_rejects_missing_docx(tmp_path: Path) -> None:
    artifacts = worker_module.TranscriptArtifacts(
        markdown_path=tmp_path / "transcript.md",
        docx_path=tmp_path / "transcript.docx",
        text_path=tmp_path / "transcript.txt",
    )
    artifacts.markdown_path.write_text("# Transcript\n", encoding="utf-8")
    artifacts.text_path.write_text("Transcript\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="required transcript artifact is missing"):
        worker_module._assert_required_artifacts_exist(artifacts)


def test_workspace_dir_for_analysis_run_rejects_defensive_token_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "_safe_workspace_token", lambda value: "../outside")

    with pytest.raises(ValueError, match="outside workspace_root"):
        worker_module._workspace_dir_for_analysis_run(tmp_path, "run-escape")


def _build_execution(
    *ordered_inputs: OrderedWorkerInput,
    analysis_run_id: str = "run-1",
    root_analysis_run_id: str = "root-1",
    params: dict[str, object] | None = None,
    step_inputs: tuple[AnalysisRunStepInput, ...] = (),
) -> ClaimedAnalysisRunStep:
    items = tuple(_selection_item_from_ordered_input(item) for item in ordered_inputs)
    if not items:
        items = (
            SelectionItemSnapshot(
                position=0,
                selection_snapshot_item_id="selection-snapshot-item-0",
                media_asset_id="media-empty",
                kind="audio",
                origin_snapshot={"origin_type": "telegram_file", "object_ref": "empty.wav"},
                storage_snapshot={"stored_object_id": "source-empty", "object_key": "empty.wav", "content_type": "audio/ogg"},
                source_snapshot=MediaSourceSnapshot(source_id="source-empty", origin_type="object", object_key="empty.wav"),
                display_name="empty.wav",
                status_at_selection="available",
                metadata_snapshot={},
            ),
        )
    return ClaimedAnalysisRunStep(
        analysis_run_step_id="exec-1",
        analysis_run_id=analysis_run_id,
        run_type="transcription",
        selection_snapshot=SealedSelectionSnapshotInput(
            selection_snapshot_id=root_analysis_run_id,
            items=items,
            option_snapshot={},
            sealed_at="2026-05-10T12:00:00Z",
        ),
        analysis_run_step_inputs=step_inputs,
        params=params or {},
        claimed_at="2026-05-10T12:01:00Z",
    )


def _selection_item_from_ordered_input(ordered_input: OrderedWorkerInput) -> SelectionItemSnapshot:
    if ordered_input.source_kind in {"uploaded_file", "telegram_upload"}:
        origin_type = "object"
    elif ordered_input.source_kind in {"youtube_url", "external_url"}:
        origin_type = "url"
    else:
        origin_type = ordered_input.source_kind
    metadata_snapshot = {}
    if ordered_input.original_filename:
        metadata_snapshot["original_filename"] = ordered_input.original_filename
    if ordered_input.source_label:
        metadata_snapshot["source_label"] = ordered_input.source_label
    if origin_type == "text":
        kind = "text"
    elif origin_type == "url":
        kind = "url"
    elif (ordered_input.original_filename or "").endswith(".mp4"):
        kind = "video"
    elif (ordered_input.original_filename or "").endswith(".txt"):
        kind = "document"
    else:
        kind = "audio"
    mime_type = (
        "video/mp4" if kind == "video" else "text/plain" if kind == "document" else "audio/ogg"
    )
    target_origin_type = "telegram_file" if origin_type == "object" else origin_type
    origin_snapshot = (
        {"origin_type": "text", "text": f"text:{ordered_input.source_id}"}
        if origin_type == "text"
        else {"origin_type": "url", "url": ordered_input.source_url or ""}
        if origin_type == "url"
        else {
            "origin_type": target_origin_type,
            "object_ref": ordered_input.object_key or ordered_input.source_id,
            "content_type": mime_type,
            "size_bytes": ordered_input.size_bytes or 1,
        }
    )
    storage_snapshot = (
        {}
        if origin_type in {"text", "url"}
        else {
            "stored_object_id": ordered_input.source_id,
            "object_key": ordered_input.object_key or "",
            "content_type": mime_type,
            "size_bytes": ordered_input.size_bytes or 1,
            "storage_status": "available",
            "retention_state": "active",
            "created_at": "2026-05-10T12:00:00Z",
        }
    )
    return SelectionItemSnapshot(
        position=ordered_input.position,
        selection_snapshot_item_id=f"selection-snapshot-item-{ordered_input.position}",
        media_asset_id=f"media-{ordered_input.source_id}",
        kind=kind,
        origin_snapshot=origin_snapshot,
        storage_snapshot=storage_snapshot,
        media_kind=kind,
        mime_type=mime_type,
        role=metadata_snapshot.get("role", "primary"),
        labels=SelectionItemLabels(
            display_label=ordered_input.display_name or ordered_input.source_id,
            source_label=ordered_input.source_label,
            original_filename=ordered_input.original_filename,
        ),
        source_snapshot=MediaSourceSnapshot(
            source_id=ordered_input.source_id,
            origin_type=origin_type,
            external_uri=ordered_input.source_url,
            object_key=ordered_input.object_key,
            checksum=ordered_input.sha256,
            size_bytes=ordered_input.size_bytes,
            mime_type=mime_type,
        ),
        display_name=ordered_input.display_name or ordered_input.source_id,
        status_at_selection="available",
        metadata_snapshot=metadata_snapshot,
    )


def _v2_selection_item(
    *,
    position: int,
    selection_snapshot_item_id: str,
    media_asset_id: str,
    origin_ref: str,
    media_kind: str,
    origin_type: str = "object",
    mime_type: str | None = None,
    object_key: str | None = None,
    external_uri: str | None = None,
    display_label: str,
    source_label: str | None = None,
    original_filename: str | None = None,
    role: str = "primary",
) -> SelectionItemSnapshot:
    target_origin_type = "telegram_file" if origin_type == "object" else origin_type
    origin_snapshot = (
        {"origin_type": "text", "text": f"text:{origin_ref}"}
        if origin_type == "text"
        else {"origin_type": "url", "url": external_uri or ""}
        if origin_type == "url"
        else {"origin_type": target_origin_type, "object_ref": object_key or origin_ref, "content_type": mime_type or "application/octet-stream"}
    )
    storage_snapshot = (
        {}
        if origin_type in {"text", "url"}
        else {"stored_object_id": origin_ref, "object_key": object_key or "", "content_type": mime_type or "application/octet-stream", "storage_status": "available", "retention_state": "active", "created_at": "2026-05-10T12:00:00Z"}
    )
    return SelectionItemSnapshot(
        position=position,
        selection_snapshot_item_id=selection_snapshot_item_id,
        media_asset_id=media_asset_id,
        kind=media_kind,
        origin_snapshot=origin_snapshot,
        storage_snapshot=storage_snapshot,
        media_kind=media_kind,
        mime_type=mime_type,
        role=role,
        labels=SelectionItemLabels(
            display_label=display_label,
            source_label=source_label,
            original_filename=original_filename,
        ),
        source_snapshot=MediaSourceSnapshot(
            source_id=origin_ref,
            origin_type=origin_type,
            external_uri=external_uri,
            object_key=object_key,
            text_ref=f"text:{origin_ref}" if origin_type == "text" else None,
            mime_type=mime_type,
        ),
        display_name=display_label,
        status_at_selection="available",
        metadata_snapshot={
            key: value
            for key, value in {
                "source_label": source_label,
                "original_filename": original_filename,
                "role": role,
            }.items()
            if value is not None
        },
    )


def _required_marker() -> str:
    return "[WorkerTranscription][runTranscription][BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE]"


def _artifact_json(artifact_store: InMemoryArtifactStore, object_key_suffix: str) -> dict[str, object]:
    artifact = next(call for call in artifact_store.calls if str(call["object_key"]).endswith(object_key_suffix))
    content = artifact["content"]
    assert isinstance(content, bytes)
    return json.loads(content.decode("utf-8"))
