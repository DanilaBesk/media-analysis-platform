# FILE: workers/transcription/src/transcriber_worker_transcription.py
# VERSION: 1.1.0
# START_MODULE_CONTRACT
# PURPOSE: Execute claimed transcription analysis runs through the shared control-plane client while preserving the current transcript artifact contract.
# SCOPE: Worker claim/run orchestration, ordered text and media materialization, combined-media concatenation, transcript artifact persistence, cancellation checks, and packet-local helper functions for local transcription materialization.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.1.0 - Added ordered object-backed text document materialization and transcript assembly without ASR.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   SourceObjectStore - Defines the source-download boundary for claimed worker inputs.
#   TranscriptionWorkerResult - Returns the successful worker execution evidence used by packet-local tests.
#   WorkerCancellationRequested - Signals authoritative cancellation observed by the dedicated worker loop.
#   materialize_local_source - Copies a local source into a workspace without changing current bot semantics.
#   process_local_transcription - Executes the preserved local transcription pipeline and writes plain, markdown, and DOCX artifacts.
#   runTranscription - Claims an analysis run, executes the worker pipeline, registers artifacts, and finalizes through the shared control-plane client.
# END_MODULE_MAP

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import subprocess
import urllib.request
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from email.message import Message
from pathlib import Path
from typing import Protocol

from transcriber_workers_common.api import (
    AnalysisRunControlClient,
    AnalysisRunStepInput,
    ClaimedAnalysisRunStep,
    SelectionItemMaterialization,
)
from transcriber_workers_common.artifacts import ArtifactDescriptor, ArtifactObjectStore, ArtifactWriter
from transcriber_workers_common.copper_asr import CopperAsrTranscriptionError
from transcriber_workers_common.documents import build_transcript_markdown, write_transcript_docx
from transcriber_workers_common.domain import SourceCandidate, TranscriptArtifacts, TranscriptResult
from transcriber_workers_common.source_extractor import extract_youtube_video_id


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_EXECUTE_TRANSCRIPTION_PIPELINE = "[WorkerTranscription][runTranscription][BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE]"
_TRANSCRIBABLE_MEDIA_KINDS = frozenset({"audio", "voice", "video"})

__all__ = [
    "SourceObjectStore",
    "TranscriptionWorkerResult",
    "WorkerCancellationRequested",
    "materialize_local_source",
    "process_local_transcription",
    "runTranscription",
]


class SourceObjectStore(Protocol):
    def fetch_file(self, *, object_key: str, destination: Path) -> None: ...


@dataclass(frozen=True, slots=True)
class TranscriptionWorkerResult:
    execution: ClaimedAnalysisRunStep
    source: SourceCandidate
    transcript: TranscriptResult
    artifacts: TranscriptArtifacts
    artifact_descriptors: tuple[ArtifactDescriptor, ...]
    diagnostics: tuple[Mapping[str, object], ...] = ()


class WorkerCancellationRequested(RuntimeError):
    suppress_worker_traceback = True

    pass


class SourceMaterializationError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: tuple[Mapping[str, object], ...] = ()) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


@dataclass(frozen=True, slots=True)
class _TranscriptBlock:
    position: int
    text: str


def materialize_local_source(source: SourceCandidate, workspace_dir: Path) -> SourceCandidate:
    # START_BLOCK_BLOCK_MATERIALIZE_LOCAL_SOURCE
    if not source.local_path:
        return source

    workspace_dir.mkdir(parents=True, exist_ok=True)
    if source.local_path.resolve().is_relative_to(workspace_dir.resolve()):
        return source
    destination = workspace_dir / f"source{source.local_path.suffix or '.bin'}"
    if source.local_path.resolve() != destination.resolve():
        shutil.copy2(source.local_path, destination)
    return replace(source, local_path=destination)
    # END_BLOCK_BLOCK_MATERIALIZE_LOCAL_SOURCE


def process_local_transcription(
    source: SourceCandidate,
    *,
    workspace_dir: Path,
    transcriber,
) -> tuple[SourceCandidate, TranscriptResult, TranscriptArtifacts]:
    # START_BLOCK_BLOCK_PROCESS_LOCAL_TRANSCRIPTION
    materialized_source = materialize_local_source(source, workspace_dir)
    transcript_result = transcriber.transcribe(materialized_source, workspace_dir)
    artifacts = _write_transcript_artifacts(workspace_dir, transcript_result)
    return materialized_source, transcript_result, artifacts
    # END_BLOCK_BLOCK_PROCESS_LOCAL_TRANSCRIPTION


def runTranscription(
    analysis_run_id: str,
    *,
    workspace_root: Path,
    api_client: AnalysisRunControlClient,
    source_store: SourceObjectStore,
    artifact_store: ArtifactObjectStore,
    transcriber,
) -> TranscriptionWorkerResult:
    execution = api_client.claim_analysis_run_step(
        analysis_run_id,
        worker_kind="transcription",
        step_kind="selection.transcription",
    )
    workspace_dir = _workspace_dir_for_analysis_run(workspace_root, execution.analysis_run_id)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    diagnostics: tuple[Mapping[str, object], ...] = ()
    item_outcomes: tuple[Mapping[str, object], ...] = ()
    text_blocks: tuple[_TranscriptBlock, ...] = ()

    try:
        # START_BLOCK_BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE
        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            progress_stage="materializing_sources",
            progress_message="Resolving claimed transcription inputs",
        )
        uses_declared_artifacts = _has_declared_transcript_artifact_inputs(execution)
        if uses_declared_artifacts:
            source = _source_candidate_from_text_materials()
        else:
            source, diagnostics, item_outcomes, text_blocks = _materialize_execution_source(
                execution,
                workspace_dir,
                source_store,
            )
            if diagnostics:
                api_client.register_diagnostics(
                    execution.analysis_run_id,
                    analysis_run_step_id=execution.analysis_run_step_id,
                    diagnostics=diagnostics,
                )

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            progress_stage="transcribing",
            progress_message="Running transcription pipeline",
        )
        _LOGGER.info(
            "%s analysis_run_id=%s analysis_run_step_id=%s ordered_input_count=%s",
            _LOG_MARKER_EXECUTE_TRANSCRIPTION_PIPELINE,
            execution.analysis_run_id,
            execution.analysis_run_step_id,
            len(execution.selection_snapshot.items),
        )
        if uses_declared_artifacts:
            materialized_source, transcript_result, diagnostics, item_outcomes = _transcribe_declared_step_inputs(
                execution,
                workspace_dir,
                api_client,
                source_store,
                transcriber,
            )
            if diagnostics:
                api_client.register_diagnostics(
                    execution.analysis_run_id,
                    analysis_run_step_id=execution.analysis_run_step_id,
                    diagnostics=diagnostics,
                )
            artifacts = _write_transcript_artifacts(workspace_dir, transcript_result)
        elif source.local_path is None and source.url is None:
            materialized_source = source
            transcript_result = _text_materials_transcript(text_blocks)
            artifacts = _write_transcript_artifacts(workspace_dir, transcript_result)
        else:
            materialized_source, transcript_result, artifacts = process_local_transcription(
                source,
                workspace_dir=workspace_dir,
                transcriber=transcriber,
            )
            transcript_result = _include_text_materials_in_transcript(
                execution,
                item_outcomes,
                transcript_result,
                text_blocks,
            )
            artifacts = _write_transcript_artifacts(workspace_dir, transcript_result)

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            progress_stage="persisting_artifacts",
            progress_message="Uploading transcript artifacts",
        )
        artifact_descriptors = _persist_transcript_artifacts(execution.analysis_run_id, artifacts, artifact_store)
        item_outcomes = _attach_artifacts_to_successful_outcomes(
            item_outcomes,
            artifact_kinds=tuple(artifact.artifact_kind for artifact in artifact_descriptors),
        )
        policy_artifacts = _persist_run_policy_artifacts(
            execution,
            artifact_store,
            diagnostics=diagnostics,
            item_outcomes=item_outcomes,
            transcript_result=transcript_result,
        )
        _assert_required_artifacts_exist(artifacts)
        api_client.register_artifacts(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            artifacts=(*artifact_descriptors, *policy_artifacts),
        )

        _check_cancellation(api_client, execution)
        outcome = "partially_succeeded" if diagnostics else "succeeded"
        api_client.finalize_analysis_run(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            outcome=outcome,
            progress_stage="completed",
            progress_message="Transcript ready",
            error_code=None,
            error_message=None,
        )
        return TranscriptionWorkerResult(
            execution=execution,
            source=materialized_source,
            transcript=transcript_result,
            artifacts=artifacts,
            artifact_descriptors=artifact_descriptors,
            diagnostics=diagnostics,
        )
        # END_BLOCK_BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE
    except WorkerCancellationRequested:
        api_client.finalize_analysis_run(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            outcome="canceled",
            progress_stage="canceled",
            progress_message="Cancellation requested",
            error_code=None,
            error_message=None,
        )
        raise
    except CopperAsrTranscriptionError as exc:
        failure_diagnostics = _transcription_failure_diagnostics(execution, item_outcomes, exc)
        combined_diagnostics = (*diagnostics, *failure_diagnostics)
        if failure_diagnostics:
            api_client.register_diagnostics(
                execution.analysis_run_id,
                analysis_run_step_id=execution.analysis_run_step_id,
                diagnostics=failure_diagnostics,
            )
        policy_artifacts = _persist_run_policy_artifacts(
            execution,
            artifact_store,
            diagnostics=combined_diagnostics,
            item_outcomes=_mark_transcription_failed_outcomes(item_outcomes, failure_diagnostics),
        )
        api_client.register_artifacts(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            artifacts=policy_artifacts,
        )
        api_client.finalize_analysis_run(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Transcription failed",
            error_code=_classify_error_code(exc),
            error_message=str(exc),
        )
        raise
    except SourceMaterializationError as exc:
        if exc.diagnostics:
            api_client.register_diagnostics(
                execution.analysis_run_id,
                analysis_run_step_id=execution.analysis_run_step_id,
                diagnostics=exc.diagnostics,
            )
        policy_artifacts = _persist_run_policy_artifacts(
            execution,
            artifact_store,
            diagnostics=exc.diagnostics,
            item_outcomes=_outcomes_from_diagnostics(execution, exc.diagnostics),
        )
        api_client.register_artifacts(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            artifacts=policy_artifacts,
        )
        api_client.finalize_analysis_run(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Transcription failed",
            error_code=_classify_error_code(exc),
            error_message=str(exc),
        )
        raise
    except Exception as exc:
        api_client.finalize_analysis_run(
            execution.analysis_run_id,
            analysis_run_step_id=execution.analysis_run_step_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Transcription failed",
            error_code=_classify_error_code(exc),
            error_message=str(exc),
        )
        raise


def _write_transcript_artifacts(workspace_dir: Path, transcript_result: TranscriptResult) -> TranscriptArtifacts:
    markdown_path = workspace_dir / "transcript.md"
    text_path = workspace_dir / "transcript.txt"
    docx_path = workspace_dir / "transcript.docx"

    markdown_path.write_text(build_transcript_markdown(transcript_result), encoding="utf-8")
    text_path.write_text(transcript_result.raw_text.strip() + "\n", encoding="utf-8")
    write_transcript_docx(docx_path, transcript_result)
    return TranscriptArtifacts(
        markdown_path=markdown_path,
        docx_path=docx_path,
        text_path=text_path,
    )


def _materialize_execution_source(
    execution: ClaimedAnalysisRunStep,
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> tuple[
    SourceCandidate,
    tuple[Mapping[str, object], ...],
    tuple[Mapping[str, object], ...],
    tuple[_TranscriptBlock, ...],
]:
    items = tuple(sorted(execution.selection_snapshot.items, key=lambda item: item.position))
    supports_direct_url = len(items) == 1
    materialized_inputs: list[tuple[SelectionItemMaterialization, Path]] = []
    diagnostics: list[Mapping[str, object]] = []
    item_outcomes: list[Mapping[str, object]] = []
    text_blocks: list[_TranscriptBlock] = []
    for item in items:
        descriptor = SelectionItemMaterialization.from_selection_item(item)
        if supports_direct_url and _is_supported_direct_youtube_descriptor(descriptor):
            item_outcomes.append(_item_outcome(execution, descriptor, "succeeded"))
            return (
                _source_candidate_from_supported_url(descriptor),
                tuple(diagnostics),
                tuple(item_outcomes),
                tuple(text_blocks),
            )
        if _is_text_descriptor(descriptor):
            text = _text_material_content(descriptor)
            if text:
                text_blocks.append(_TranscriptBlock(descriptor.position, text))
            item_outcomes.append(_item_outcome(execution, descriptor, "succeeded"))
            continue
        if _is_textual_object_descriptor(descriptor):
            try:
                local_path, text = _materialize_textual_object(descriptor, workspace_dir, source_store)
            except SourceMaterializationError as exc:
                diagnostic = _failed_selection_item_diagnostic(execution, descriptor, message=str(exc))
                diagnostics.append(diagnostic)
                item_outcomes.append(
                    _item_outcome(
                        execution,
                        descriptor,
                        "failed",
                        diagnostic_ids=(str(diagnostic["diagnostic_id"]),),
                    )
                )
                continue
            text_blocks.append(_TranscriptBlock(descriptor.position, text))
            item_outcomes.append(_item_outcome(execution, descriptor, "succeeded", materialized_path=local_path))
            continue
        if descriptor.is_object_backed and _is_transcribable_descriptor(descriptor):
            input_dir = workspace_dir / "inputs" / f"{descriptor.position:02d}-{descriptor.source_id}"
            input_dir.mkdir(parents=True, exist_ok=True)
            try:
                local_path = _download_materialization_descriptor(descriptor, input_dir, source_store)
            except SourceMaterializationError as exc:
                diagnostic = _failed_selection_item_diagnostic(execution, descriptor, message=str(exc))
                diagnostics.append(diagnostic)
                item_outcomes.append(_item_outcome(execution, descriptor, "failed", diagnostic_ids=(str(diagnostic["diagnostic_id"]),)))
                continue
            materialized_inputs.append((descriptor, local_path))
            item_outcomes.append(_item_outcome(execution, descriptor, "succeeded", materialized_path=local_path))
            continue
        materialized_path = None
        if descriptor.is_object_backed:
            try:
                materialized_path = _materialize_unsupported_object_descriptor(descriptor, workspace_dir, source_store)
            except SourceMaterializationError:
                materialized_path = None
        diagnostic = _unsupported_selection_item_diagnostic(execution, descriptor, materialized_path=materialized_path)
        diagnostics.append(diagnostic)
        item_outcomes.append(_item_outcome(execution, descriptor, "skipped", diagnostic_ids=(str(diagnostic["diagnostic_id"]),)))

    if not materialized_inputs:
        if any(item.get("outcome") == "succeeded" for item in item_outcomes):
            return (
                _source_candidate_from_text_materials(),
                tuple(diagnostics),
                tuple(item_outcomes),
                tuple(text_blocks),
            )
        error_message = "selection contains no object-backed media items that can be transcribed"
        object_key_failures = [
            diagnostic["message"]
            for diagnostic in diagnostics
            if isinstance(diagnostic.get("message"), str) and "object_key" in diagnostic["message"]
        ]
        if object_key_failures:
            error_message = object_key_failures[0]
        else:
            materialization_failures = [
                diagnostic["message"]
                for diagnostic in diagnostics
                if diagnostic.get("severity") == "error" and isinstance(diagnostic.get("message"), str)
            ]
            if materialization_failures:
                error_message = materialization_failures[0]
        raise SourceMaterializationError(
            error_message,
            diagnostics=tuple(diagnostics),
        )

    if len(materialized_inputs) == 1:
        materialization, local_path = materialized_inputs[0]
        return (
            _source_candidate_from_materialized_path(materialization, local_path),
            tuple(diagnostics),
            tuple(item_outcomes),
            tuple(text_blocks),
        )
    return (
        _materialize_combined_source(materialized_inputs, workspace_dir),
        tuple(diagnostics),
        tuple(item_outcomes),
        tuple(text_blocks),
    )


def _has_declared_transcript_artifact_inputs(execution: ClaimedAnalysisRunStep) -> bool:
    return any(step_input.input_kind == "transcript_artifact" for step_input in execution.analysis_run_step_inputs)


def _transcribe_declared_step_inputs(
    execution: ClaimedAnalysisRunStep,
    workspace_dir: Path,
    api_client: AnalysisRunControlClient,
    source_store: SourceObjectStore,
    transcriber,
) -> tuple[SourceCandidate, TranscriptResult, tuple[Mapping[str, object], ...], tuple[Mapping[str, object], ...]]:
    items_by_id = {
        item.selection_snapshot_item_id: item
        for item in execution.selection_snapshot.items
    }
    diagnostics: list[Mapping[str, object]] = []
    item_outcomes: list[Mapping[str, object]] = []
    blocks: list[_TranscriptBlock] = []
    segments = []
    provider_metadata: Mapping[str, object] = {}
    materialized_source = _source_candidate_from_text_materials()

    for step_input in sorted(execution.analysis_run_step_inputs, key=lambda item: (item.position, item.analysis_run_step_input_id)):
        item = _selection_item_for_step_input(step_input, items_by_id)
        materialization = SelectionItemMaterialization.from_selection_item(item) if item is not None else None
        if step_input.input_kind == "transcript_artifact":
            if materialization is None:
                raise SourceMaterializationError("required transcript_artifact input must reference a selection_snapshot_item")
            if not step_input.artifact_id:
                raise SourceMaterializationError("required transcript_artifact input is missing artifact_id")
            text = _read_transcript_artifact_text(api_client, step_input.artifact_id)
            if text:
                blocks.append(_TranscriptBlock(step_input.position, text))
            item_outcomes.append(_item_outcome(execution, materialization, "succeeded"))
            continue

        if step_input.input_kind != "selection_snapshot_item":
            if step_input.required:
                raise SourceMaterializationError(f"unsupported required transcription step input kind: {step_input.input_kind}")
            continue
        if materialization is None:
            if step_input.required:
                raise SourceMaterializationError("required selection_snapshot_item input is missing selection_snapshot_item_id")
            continue

        if _is_text_descriptor(materialization):
            text = _text_material_content(materialization)
            if text:
                blocks.append(_TranscriptBlock(step_input.position, text))
            item_outcomes.append(_item_outcome(execution, materialization, "succeeded"))
            continue
        if _is_textual_object_descriptor(materialization):
            try:
                local_path, text = _materialize_textual_object(materialization, workspace_dir, source_store)
            except SourceMaterializationError as exc:
                diagnostic = _failed_selection_item_diagnostic(execution, materialization, message=str(exc))
                diagnostics.append(diagnostic)
                item_outcomes.append(
                    _item_outcome(
                        execution,
                        materialization,
                        "failed",
                        diagnostic_ids=(str(diagnostic["diagnostic_id"]),),
                    )
                )
                continue
            blocks.append(_TranscriptBlock(step_input.position, text))
            item_outcomes.append(
                _item_outcome(execution, materialization, "succeeded", materialized_path=local_path)
            )
            continue

        source: SourceCandidate | None = None
        if _is_supported_direct_youtube_descriptor(materialization):
            source = _source_candidate_from_supported_url(materialization)
        elif materialization.is_object_backed and _is_transcribable_descriptor(materialization):
            try:
                source = _materialize_single_selection_item(materialization, workspace_dir, source_store)
            except SourceMaterializationError as exc:
                diagnostic = _failed_selection_item_diagnostic(execution, materialization, message=str(exc))
                diagnostics.append(diagnostic)
                item_outcomes.append(_item_outcome(execution, materialization, "failed", diagnostic_ids=(str(diagnostic["diagnostic_id"]),)))
                continue

        if source is None:
            materialized_path = None
            if materialization.is_object_backed:
                try:
                    materialized_path = _materialize_unsupported_object_descriptor(materialization, workspace_dir, source_store)
                except SourceMaterializationError:
                    materialized_path = None
            diagnostic = _unsupported_selection_item_diagnostic(execution, materialization, materialized_path=materialized_path)
            diagnostics.append(diagnostic)
            item_outcomes.append(_item_outcome(execution, materialization, "skipped", diagnostic_ids=(str(diagnostic["diagnostic_id"]),)))
            continue

        materialized_source = source
        transcript_result = transcriber.transcribe(materialized_source, workspace_dir)
        if transcript_result.raw_text.strip():
            blocks.append(_TranscriptBlock(step_input.position, transcript_result.raw_text.strip()))
        if transcript_result.segments:
            segments.extend(transcript_result.segments)
        if transcript_result.provider_metadata and not provider_metadata:
            provider_metadata = transcript_result.provider_metadata
        item_outcomes.append(_item_outcome(execution, materialization, "succeeded", materialized_path=materialized_source.local_path))

    if not blocks:
        if diagnostics:
            raise SourceMaterializationError(
                "selection contains no reusable or transcribable inputs that produced transcript text",
                diagnostics=tuple(diagnostics),
            )
        raise SourceMaterializationError("declared transcription step produced no transcript text")

    transcript_text = "\n\n".join(block.text for block in sorted(blocks, key=lambda block: block.position)).strip()
    return (
        materialized_source,
        TranscriptResult(
            title="Смешанные материалы",
            source_label="Selection transcript",
            segments=segments,
            language="und",
            raw_text=transcript_text,
            provider_metadata=provider_metadata,
        ),
        tuple(diagnostics),
        tuple(item_outcomes),
    )


def _selection_item_for_step_input(
    step_input: AnalysisRunStepInput,
    items_by_id: Mapping[str, object],
):
    if not step_input.selection_snapshot_item_id:
        return None
    return items_by_id.get(step_input.selection_snapshot_item_id)


def _read_transcript_artifact_text(api_client: AnalysisRunControlClient, artifact_id: str) -> str:
    resolution = api_client.resolve_artifact(artifact_id)
    if not resolution.download_url:
        raise SourceMaterializationError(f"transcript artifact {artifact_id} has no download_url")
    with urllib.request.urlopen(resolution.download_url, timeout=60) as response:
        data = response.read()
    return data.decode("utf-8").strip()


def _source_candidate_from_text_materials() -> SourceCandidate:
    return SourceCandidate(
        source_id="text-materials",
        kind="telegram_audio",
        display_name="Text materials",
        url=None,
        telegram_file_id=None,
        mime_type="text/plain",
        file_name=None,
        file_unique_id=None,
        local_path=None,
    )


def _include_text_materials_in_transcript(
    execution: ClaimedAnalysisRunStep,
    item_outcomes: tuple[Mapping[str, object], ...],
    transcript_result: TranscriptResult,
    text_blocks: tuple[_TranscriptBlock, ...],
) -> TranscriptResult:
    succeeded_item_ids = {
        str(item.get("selection_snapshot_item_id") or "")
        for item in item_outcomes
        if item.get("outcome") == "succeeded"
    }
    blocks = [(block.position, block.text) for block in text_blocks]
    speech_position: int | None = None
    for item in sorted(execution.selection_snapshot.items, key=lambda selection_item: selection_item.position):
        if str(item.selection_snapshot_item_id) not in succeeded_item_ids:
            continue
        materialization = SelectionItemMaterialization.from_selection_item(item)
        if speech_position is None and _is_transcribable_descriptor(materialization):
            speech_position = materialization.position
    if not blocks:
        return transcript_result
    if transcript_result.raw_text.strip() and speech_position is not None:
        blocks.append((speech_position, transcript_result.raw_text.strip()))
    combined_text = "\n\n".join(text for _, text in sorted(blocks, key=lambda block: block[0])).strip()
    return replace(transcript_result, raw_text=combined_text)


def _text_materials_transcript(
    text_blocks: tuple[_TranscriptBlock, ...],
) -> TranscriptResult:
    return TranscriptResult(
        title="Текстовые материалы",
        source_label="Text materials",
        segments=[],
        language="und",
        raw_text="\n\n".join(block.text for block in sorted(text_blocks, key=lambda block: block.position)).strip(),
    )


def _unsupported_selection_item_diagnostic(
    execution: ClaimedAnalysisRunStep,
    materialization: SelectionItemMaterialization,
    *,
    materialized_path: Path | None = None,
) -> Mapping[str, object]:
    origin_type = materialization.origin_type
    if origin_type == "text":
        message = "Text media is already textual and is not sent to the transcription engine"
    elif origin_type == "url":
        message = "URL media is not transcribed directly by the worker; provide object-backed media"
    elif materialization.unsupported_reason:
        message = materialization.unsupported_reason
    else:
        message = f"Object-backed {materialization.media_kind} media is not suitable for transcription"
    context: dict[str, object] = {
        "analysis_run_id": execution.analysis_run_id,
        "selection_snapshot_id": execution.selection_snapshot.selection_snapshot_id,
        "selection_snapshot_item_id": materialization.selection_snapshot_item_id,
        "item_position": materialization.position,
        "media_asset_id": materialization.media_asset_id,
        "media_kind": materialization.media_kind,
        "mime_type": materialization.mime_type,
        "role": materialization.role,
        "origin_type": origin_type,
        "source_id": materialization.source_id,
        "display_label": materialization.labels.display_label,
        "source_label": materialization.labels.source_label,
        "original_filename": materialization.labels.original_filename,
        "materialization_kind": materialization.materialization_kind,
    }
    if materialization.external_uri:
        context["external_uri"] = materialization.external_uri
    if materialization.text_ref:
        context["text_ref"] = materialization.text_ref
    if materialization.deterministic_filename:
        context["materialized_filename"] = materialization.deterministic_filename
    if materialized_path is not None:
        context["materialized_path"] = str(materialized_path)
    return {
        "diagnostic_id": _selection_item_diagnostic_id(
            execution,
            materialization,
            "unsupported-transcription-source",
        ),
        "subject_type": "media_asset",
        "subject_id": materialization.media_asset_id,
        "severity": "warning",
        "code": "source_unavailable",
        "message": message,
        "context": context,
        "created_at": execution.claimed_at,
    }


def _failed_selection_item_diagnostic(
    execution: ClaimedAnalysisRunStep,
    materialization: SelectionItemMaterialization,
    *,
    message: str,
) -> Mapping[str, object]:
    context = _lineage_context(execution, materialization)
    context["materialization_kind"] = materialization.materialization_kind
    if materialization.object_key:
        context["object_key"] = materialization.object_key
    if materialization.deterministic_filename:
        context["materialized_filename"] = materialization.deterministic_filename
    return {
        "diagnostic_id": _selection_item_diagnostic_id(
            execution,
            materialization,
            "source-materialization-failed",
        ),
        "subject_type": "media_asset",
        "subject_id": materialization.media_asset_id,
        "severity": "error",
        "code": "source_unavailable",
        "message": message,
        "context": context,
        "created_at": execution.claimed_at,
    }


def _lineage_context(execution: ClaimedAnalysisRunStep, materialization: SelectionItemMaterialization) -> dict[str, object]:
    context: dict[str, object] = {
        "analysis_run_id": execution.analysis_run_id,
        "selection_snapshot_id": execution.selection_snapshot.selection_snapshot_id,
        "selection_snapshot_item_id": materialization.selection_snapshot_item_id,
        "item_position": materialization.position,
        "media_asset_id": materialization.media_asset_id,
        "media_kind": materialization.media_kind,
        "mime_type": materialization.mime_type,
        "role": materialization.role,
        "origin_type": materialization.origin_type,
        "source_id": materialization.source_id,
        "display_label": materialization.labels.display_label,
        "source_label": materialization.labels.source_label,
        "original_filename": materialization.labels.original_filename,
    }
    return context


def _selection_item_diagnostic_id(
    execution: ClaimedAnalysisRunStep,
    materialization: SelectionItemMaterialization,
    reason: str,
) -> str:
    seed = ":".join(
        (
            execution.analysis_run_step_id,
            materialization.selection_snapshot_item_id,
            str(materialization.position),
            materialization.media_asset_id,
            reason,
        )
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"media-analysis-platform:diagnostic:{seed}"))


def _materialize_single_selection_item(
    materialization: SelectionItemMaterialization,
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> SourceCandidate:
    input_dir = workspace_dir / "inputs" / f"{materialization.position:02d}-{materialization.source_id}"
    input_dir.mkdir(parents=True, exist_ok=True)
    local_path = _download_materialization_descriptor(materialization, input_dir, source_store)
    return SourceCandidate(
        source_id=materialization.source_id,
        kind=_source_candidate_kind(materialization),
        display_name=materialization.labels.source_display_label(),
        url=None,
        telegram_file_id=None,
        mime_type=materialization.mime_type,
        file_name=local_path.name,
        file_unique_id=None,
        local_path=local_path,
    )


def _materialize_textual_object(
    materialization: SelectionItemMaterialization,
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> tuple[Path, str]:
    input_dir = workspace_dir / "inputs" / f"{materialization.position:02d}-{materialization.source_id}"
    input_dir.mkdir(parents=True, exist_ok=True)
    local_path = _download_materialization_descriptor(materialization, input_dir, source_store)
    return local_path, _read_textual_object(local_path, materialization.mime_type)


def _read_textual_object(local_path: Path, mime_type: str | None) -> str:
    try:
        data = local_path.read_bytes()
    except OSError as exc:
        raise SourceMaterializationError(f"could not read text document: {exc}") from exc
    encoding = _text_encoding(mime_type, data)
    try:
        text = data.decode(encoding)
    except (LookupError, UnicodeDecodeError) as exc:
        encoding_label = "UTF-8" if encoding == "utf-8-sig" else encoding
        raise SourceMaterializationError(f"could not decode text document as {encoding_label}") from exc
    if "\x00" in text:
        raise SourceMaterializationError("text document contains NUL bytes")
    text = text.strip()
    if not text:
        raise SourceMaterializationError("text document contains no text")
    return text


def _text_encoding(mime_type: str | None, data: bytes) -> str:
    _, charset = _content_type_parts(mime_type)
    if charset:
        return "utf-8-sig" if charset.casefold().replace("_", "-") == "utf-8" else charset
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    return "utf-8-sig"


def _materialize_combined_source(
    materialized_inputs: list[tuple[SelectionItemMaterialization, Path]],
    workspace_dir: Path,
) -> SourceCandidate:
    materializations = tuple(materialization for materialization, _ in materialized_inputs)
    materialized_paths = [local_path for _, local_path in materialized_inputs]
    combined_dir = workspace_dir / "combined"
    combined_dir.mkdir(parents=True, exist_ok=True)

    output_path = combined_dir / "combined.wav"
    _concatenate_media_inputs(materialized_paths, output_path)
    return SourceCandidate(
        source_id=f"{materializations[0].source_id}-combined",
        kind="telegram_audio",
        display_name="Audio: combined-inputs.wav",
        url=None,
        telegram_file_id=None,
        mime_type="audio/wav",
        file_name="combined-inputs.wav",
        file_unique_id=None,
        local_path=output_path,
    )


def _source_candidate_from_materialized_path(
    materialization: SelectionItemMaterialization,
    local_path: Path,
) -> SourceCandidate:
    return SourceCandidate(
        source_id=materialization.source_id,
        kind=_source_candidate_kind(materialization),
        display_name=materialization.labels.source_display_label(),
        url=None,
        telegram_file_id=None,
        mime_type=materialization.mime_type,
        file_name=local_path.name,
        file_unique_id=None,
        local_path=local_path,
    )


def _source_candidate_from_supported_url(materialization: SelectionItemMaterialization) -> SourceCandidate:
    return SourceCandidate(
        source_id=materialization.source_id,
        kind="youtube_url",
        display_name=materialization.labels.source_display_label(),
        url=materialization.external_uri,
        telegram_file_id=None,
        mime_type=materialization.mime_type,
        file_name=None,
        file_unique_id=None,
        local_path=None,
    )


def _materialize_unsupported_object_descriptor(
    materialization: SelectionItemMaterialization,
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> Path | None:
    if materialization.materialization_kind != "object":
        return None
    unsupported_dir = workspace_dir / "unsupported" / f"{materialization.position:02d}-{materialization.source_id}"
    unsupported_dir.mkdir(parents=True, exist_ok=True)
    return _download_materialization_descriptor(materialization, unsupported_dir, source_store)


def _download_materialization_descriptor(
    materialization: SelectionItemMaterialization,
    destination_dir: Path,
    source_store: SourceObjectStore,
) -> Path:
    if not materialization.object_key:
        raise SourceMaterializationError(f"{materialization.media_kind} input must include object_key")
    if not materialization.deterministic_filename:
        raise SourceMaterializationError(f"{materialization.media_kind} input is missing deterministic filename")

    destination = destination_dir / materialization.deterministic_filename
    try:
        source_store.fetch_file(object_key=materialization.object_key, destination=destination)
    except Exception as exc:
        raise SourceMaterializationError(str(exc)) from exc
    return destination


def _is_transcribable_descriptor(materialization: SelectionItemMaterialization) -> bool:
    if materialization.media_kind in _TRANSCRIBABLE_MEDIA_KINDS:
        return True
    mime_type = (materialization.mime_type or "").split(";", 1)[0].strip().casefold()
    return mime_type.startswith("audio/") or mime_type.startswith("video/")


def _is_text_descriptor(materialization: SelectionItemMaterialization) -> bool:
    return materialization.origin_type == "text" or materialization.media_kind == "text"


def _is_textual_object_descriptor(materialization: SelectionItemMaterialization) -> bool:
    media_type, _ = _content_type_parts(materialization.mime_type)
    return (
        materialization.is_object_backed
        and materialization.media_kind == "document"
        and media_type.startswith("text/")
    )


def _content_type_parts(mime_type: str | None) -> tuple[str, str | None]:
    if not mime_type:
        return "", None
    header = Message()
    header["Content-Type"] = mime_type
    return header.get_content_type().casefold(), header.get_content_charset()


def _text_material_content(materialization: SelectionItemMaterialization) -> str:
    return (materialization.text_ref or materialization.origin_ref or materialization.labels.display_label or "").strip()


def _source_candidate_kind(materialization: SelectionItemMaterialization) -> str:
    mime_type = (materialization.mime_type or "").split(";", 1)[0].strip().casefold()
    if materialization.media_kind == "video" or mime_type.startswith("video/"):
        return "telegram_video"
    return "telegram_audio"


def _is_supported_direct_youtube_descriptor(materialization: SelectionItemMaterialization) -> bool:
    if materialization.origin_type != "url":
        return False
    if not materialization.external_uri:
        return False
    return extract_youtube_video_id(materialization.external_uri) is not None


def _concatenate_media_inputs(input_paths: list[Path], output_path: Path) -> None:
    if len(input_paths) < 2:
        raise SourceMaterializationError("combined transcription requires at least two inputs")

    filter_inputs = "".join(f"[{index}:a]" for index in range(len(input_paths)))
    command = ["ffmpeg", "-y"]
    for path in input_paths:
        command.extend(["-i", str(path)])
    command.extend(
        [
            "-filter_complex",
            f"{filter_inputs}concat=n={len(input_paths)}:v=0:a=1[outa]",
            "-map",
            "[outa]",
            "-ac",
            "1",
            "-ar",
            "16000",
            str(output_path),
        ]
    )
    completed = subprocess.run(command, capture_output=True, text=True, check=False, timeout=3600)
    if completed.returncode != 0:
        raise SourceMaterializationError(f"ffmpeg concat failed with exit code {completed.returncode}: {completed.stderr.strip()}")


def _persist_transcript_artifacts(
    analysis_run_id: str,
    artifacts: TranscriptArtifacts,
    artifact_store: ArtifactObjectStore,
) -> tuple[ArtifactDescriptor, ...]:
    writer = ArtifactWriter(analysis_run_id=analysis_run_id, object_store=artifact_store)
    return (
        writer.write_file_artifact(
            "transcript_plain",
            artifacts.text_path,
            mime_type="text/plain; charset=utf-8",
            format="txt",
        ),
        writer.write_file_artifact(
            "transcript_segmented_markdown",
            artifacts.markdown_path,
            mime_type="text/markdown; charset=utf-8",
            format="markdown",
        ),
        writer.write_file_artifact(
            "transcript_docx",
            artifacts.docx_path,
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            format="docx",
        ),
    )


def _persist_run_policy_artifacts(
    execution: ClaimedAnalysisRunStep,
    artifact_store: ArtifactObjectStore,
    *,
    diagnostics: tuple[Mapping[str, object], ...],
    item_outcomes: tuple[Mapping[str, object], ...],
    transcript_result: TranscriptResult | None = None,
) -> tuple[ArtifactDescriptor, ...]:
    writer = ArtifactWriter(analysis_run_id=execution.analysis_run_id, object_store=artifact_store)
    manifest = _run_manifest_payload(execution, item_outcomes, transcript_result=transcript_result)
    diagnostics_payload = {
        "schema_version": "analysis_run_diagnostics/v2",
        "analysis_run_id": execution.analysis_run_id,
        "analysis_run_step_id": execution.analysis_run_step_id,
        "selection_snapshot_id": execution.selection_snapshot.selection_snapshot_id,
        "diagnostics": [dict(diagnostic) for diagnostic in diagnostics],
    }
    return (
        writer.write_text_artifact(
            "run_manifest",
            "run-manifest.json",
            _canonical_json(manifest),
            mime_type="application/json; charset=utf-8",
            format="json",
        ),
        writer.write_text_artifact(
            "run_diagnostics",
            "run-diagnostics.json",
            _canonical_json(diagnostics_payload),
            mime_type="application/json; charset=utf-8",
            format="json",
        ),
    )


def _run_manifest_payload(
    execution: ClaimedAnalysisRunStep,
    item_outcomes: tuple[Mapping[str, object], ...],
    *,
    transcript_result: TranscriptResult | None = None,
) -> Mapping[str, object]:
    summary = {
        "included_count": sum(1 for item in item_outcomes if item.get("outcome") == "succeeded"),
        "skipped_count": sum(1 for item in item_outcomes if item.get("outcome") == "skipped"),
        "failed_count": sum(1 for item in item_outcomes if item.get("outcome") == "failed"),
    }
    manifest: dict[str, object] = {
        "schema_version": "analysis_run_manifest/v2",
        "analysis_run_id": execution.analysis_run_id,
        "analysis_run_step_id": execution.analysis_run_step_id,
        "selection_snapshot_id": execution.selection_snapshot.selection_snapshot_id,
        "run_type": execution.run_type,
        "created_at": execution.claimed_at,
        "artifact_policy": {
            "canonical": ["run_manifest", "run_diagnostics"],
        },
        "summary": summary,
        "items": [dict(item) for item in sorted(item_outcomes, key=lambda item: int(item.get("position", 0)))],
    }
    if transcript_result is not None and transcript_result.provider_metadata:
        manifest["transcription_backend"] = dict(transcript_result.provider_metadata)
    return manifest


def _item_outcome(
    execution: ClaimedAnalysisRunStep,
    materialization: SelectionItemMaterialization,
    outcome: str,
    *,
    diagnostic_ids: tuple[str, ...] = (),
    materialized_path: Path | None = None,
    artifact_kinds: tuple[str, ...] = (),
) -> Mapping[str, object]:
    lineage = _lineage_context(execution, materialization)
    result: dict[str, object] = {
        "selection_snapshot_item_id": materialization.selection_snapshot_item_id,
        "media_asset_id": materialization.media_asset_id,
        "position": materialization.position,
        "outcome": outcome,
        "included": outcome == "succeeded",
        "lineage": lineage,
        "artifact_kinds": list(artifact_kinds),
        "diagnostic_ids": list(diagnostic_ids),
    }
    if materialized_path is not None:
        result["materialized_path"] = str(materialized_path)
    return result


def _attach_artifacts_to_successful_outcomes(
    item_outcomes: tuple[Mapping[str, object], ...],
    *,
    artifact_kinds: tuple[str, ...],
) -> tuple[Mapping[str, object], ...]:
    updated: list[Mapping[str, object]] = []
    for item in item_outcomes:
        if item.get("outcome") != "succeeded":
            updated.append(item)
            continue
        patched = dict(item)
        patched["artifact_kinds"] = list(artifact_kinds)
        updated.append(patched)
    return tuple(updated)


def _outcomes_from_diagnostics(
    execution: ClaimedAnalysisRunStep,
    diagnostics: tuple[Mapping[str, object], ...],
) -> tuple[Mapping[str, object], ...]:
    diagnostics_by_snapshot_item: dict[str, Mapping[str, object]] = {}
    for diagnostic in diagnostics:
        context = diagnostic.get("context")
        if not isinstance(context, Mapping):
            continue
        snapshot_item_id = context.get("selection_snapshot_item_id")
        if isinstance(snapshot_item_id, str):
            diagnostics_by_snapshot_item[snapshot_item_id] = diagnostic

    outcomes: list[Mapping[str, object]] = []
    for item in sorted(execution.selection_snapshot.items, key=lambda snapshot_item: snapshot_item.position):
        materialization = SelectionItemMaterialization.from_selection_item(item)
        diagnostic = diagnostics_by_snapshot_item.get(materialization.selection_snapshot_item_id)
        if diagnostic is None:
            outcomes.append(_item_outcome(execution, materialization, "failed"))
            continue
        severity = diagnostic.get("severity")
        outcome = "failed" if severity == "error" else "skipped"
        outcomes.append(
            _item_outcome(
                execution,
                materialization,
                outcome,
                diagnostic_ids=(str(diagnostic["diagnostic_id"]),),
            )
        )
    return tuple(outcomes)


def _transcription_failure_diagnostics(
    execution: ClaimedAnalysisRunStep,
    item_outcomes: tuple[Mapping[str, object], ...],
    error: CopperAsrTranscriptionError,
) -> tuple[Mapping[str, object], ...]:
    affected_items = [item for item in item_outcomes if item.get("outcome") == "succeeded"]
    if not affected_items:
        affected_items = list(item_outcomes)
    affected_snapshot_item_ids = [
        str(item.get("selection_snapshot_item_id"))
        for item in affected_items
        if str(item.get("selection_snapshot_item_id") or "").strip()
    ]
    affected_media_asset_ids = [
        str(item.get("media_asset_id"))
        for item in affected_items
        if str(item.get("media_asset_id") or "").strip()
    ]
    context: dict[str, object] = {
        "analysis_run_id": execution.analysis_run_id,
        "analysis_run_step_id": execution.analysis_run_step_id,
        "selection_snapshot_id": execution.selection_snapshot.selection_snapshot_id,
        "affected_selection_snapshot_item_ids": affected_snapshot_item_ids,
        "affected_media_asset_ids": affected_media_asset_ids,
        "provider_code": error.provider_code,
        "status_code": error.status_code,
        "retryable": error.retryable,
    }
    if error.request_id:
        context["request_id"] = error.request_id
    code = _classify_error_code(error)
    diagnostic_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "media-analysis-platform:diagnostic:"
            + ":".join(
                (
                    execution.analysis_run_step_id,
                    execution.selection_snapshot.selection_snapshot_id,
                    code,
                    ",".join(affected_snapshot_item_ids),
                    "transcription-failed",
                )
            ),
        )
    )
    return (
        {
            "diagnostic_id": diagnostic_id,
            "subject_type": "analysis_run",
            "subject_id": execution.analysis_run_id,
            "severity": "error",
            "code": code,
            "message": str(error),
            "context": context,
            "remediation_hint": "retry" if error.retryable else "replace_or_remove_source",
            "created_at": execution.claimed_at,
        },
    )


def _mark_transcription_failed_outcomes(
    item_outcomes: tuple[Mapping[str, object], ...],
    diagnostics: tuple[Mapping[str, object], ...],
) -> tuple[Mapping[str, object], ...]:
    diagnostic_ids = [str(diagnostic["diagnostic_id"]) for diagnostic in diagnostics]
    updated: list[Mapping[str, object]] = []
    for item in item_outcomes:
        if item.get("outcome") != "succeeded":
            updated.append(item)
            continue
        patched = dict(item)
        patched["outcome"] = "failed"
        patched["included"] = False
        patched["artifact_kinds"] = []
        patched["diagnostic_ids"] = diagnostic_ids
        updated.append(patched)
    return tuple(updated)


def _canonical_json(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _assert_required_artifacts_exist(artifacts: TranscriptArtifacts) -> None:
    required_paths = (
        artifacts.text_path,
        artifacts.markdown_path,
        artifacts.docx_path,
    )
    for path in required_paths:
        if not path.exists():
            raise RuntimeError(f"required transcript artifact is missing: {path}")


def _check_cancellation(api_client: AnalysisRunControlClient, execution: ClaimedAnalysisRunStep) -> None:
    cancel_state = api_client.check_cancel(execution.analysis_run_id, analysis_run_step_id=execution.analysis_run_step_id)
    if cancel_state.cancel_requested:
        raise WorkerCancellationRequested(f"analysis run {execution.analysis_run_id} was canceled")


def _workspace_dir_for_analysis_run(workspace_root: Path, analysis_run_id: str) -> Path:
    root = Path(workspace_root).resolve()
    destination = (root / _safe_workspace_token(analysis_run_id)).resolve()
    if not destination.is_relative_to(root):
        raise ValueError("analysis_run_id resolved outside workspace_root")
    return destination


def _safe_workspace_token(value: str) -> str:
    stripped = value.strip()
    cleaned = "".join(character if character.isalnum() or character in {"-", "_", "."} else "-" for character in stripped)
    cleaned = cleaned.strip("-_.")
    if (
        stripped
        and cleaned == stripped
        and "/" not in stripped
        and "\\" not in stripped
        and stripped not in {".", ".."}
    ):
        return stripped
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"{cleaned or 'analysis-run'}-{digest}"


def _classify_error_code(error: Exception) -> str:
    if isinstance(error, SourceMaterializationError):
        return "source_fetch_failed"
    diagnostic_code = getattr(error, "diagnostic_code", None)
    if isinstance(diagnostic_code, str) and diagnostic_code.strip():
        return diagnostic_code.strip()
    return "transcription_failed"
