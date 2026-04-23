# FILE: workers/transcription/src/transcriber_worker_transcription.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Execute claimed transcription jobs through the shared control-plane client while preserving the current transcript artifact contract.
# SCOPE: Worker claim/run orchestration, ordered-input materialization, combined-media concatenation, transcript artifact persistence, cancellation checks, and local extraction helpers reused by the legacy service shell.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Introduced the transcription worker shell and extracted the local transcript orchestration path into one packet-scoped module.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   SourceObjectStore - Defines the source-download boundary for claimed worker inputs.
#   TranscriptionWorkerResult - Returns the successful worker execution evidence used by packet-local tests.
#   WorkerCancellationRequested - Signals authoritative cancellation observed by the dedicated worker loop.
#   materialize_local_source - Copies a local source into a workspace without changing current bot semantics.
#   process_local_transcription - Executes the preserved local transcription pipeline and writes plain, markdown, and DOCX artifacts.
#   runTranscription - Claims a job, executes the worker pipeline, registers artifacts, and finalizes through the shared control-plane client.
# END_MODULE_MAP

from __future__ import annotations

import logging
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol


def _ensure_worker_dependency_paths() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    dependency_paths = (
        repo_root / "workers" / "common" / "src",
        repo_root / "src",
    )
    for path in dependency_paths:
        resolved = str(path)
        if path.exists() and resolved not in sys.path:
            sys.path.insert(0, resolved)


_ensure_worker_dependency_paths()

from transcriber_workers_common.api import ClaimedJobExecution, JobApiClient, OrderedWorkerInput
from transcriber_workers_common.artifacts import ArtifactDescriptor, ArtifactObjectStore, ArtifactWriter
from telegram_transcriber_bot.documents import build_transcript_markdown, write_transcript_docx
from telegram_transcriber_bot.domain import SourceCandidate, TranscriptArtifacts, TranscriptResult
from telegram_transcriber_bot.transcribers import _download_youtube_audio


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_EXECUTE_TRANSCRIPTION_PIPELINE = "[WorkerTranscription][runTranscription][BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE]"
_AUDIO_SUFFIXES = frozenset({".aac", ".amr", ".flac", ".m4a", ".mp3", ".ogg", ".oga", ".opus", ".wav", ".wma"})
_VIDEO_SUFFIXES = frozenset({".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".webm"})

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
    execution: ClaimedJobExecution
    source: SourceCandidate
    transcript: TranscriptResult
    artifacts: TranscriptArtifacts
    artifact_descriptors: tuple[ArtifactDescriptor, ...]


class WorkerCancellationRequested(RuntimeError):
    pass


class SourceMaterializationError(RuntimeError):
    pass


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
    job_id: str,
    *,
    workspace_root: Path,
    api_client: JobApiClient,
    source_store: SourceObjectStore,
    artifact_store: ArtifactObjectStore,
    transcriber,
) -> TranscriptionWorkerResult:
    execution = api_client.claim_job(job_id, worker_kind="transcription", task_type="transcription.run")
    workspace_dir = Path(workspace_root) / execution.job_id
    workspace_dir.mkdir(parents=True, exist_ok=True)

    try:
        # START_BLOCK_BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE
        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="materializing_sources",
            progress_message="Resolving claimed transcription inputs",
        )
        source = _materialize_execution_source(execution, workspace_dir, source_store)

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="transcribing",
            progress_message="Running transcription pipeline",
        )
        _LOGGER.info(
            "%s job_id=%s execution_id=%s ordered_input_count=%s",
            _LOG_MARKER_EXECUTE_TRANSCRIPTION_PIPELINE,
            execution.job_id,
            execution.execution_id,
            len(execution.ordered_inputs),
        )
        materialized_source, transcript_result, artifacts = process_local_transcription(
            source,
            workspace_dir=workspace_dir,
            transcriber=transcriber,
        )

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="persisting_artifacts",
            progress_message="Uploading transcript artifacts",
        )
        artifact_descriptors = _persist_transcript_artifacts(execution.job_id, artifacts, artifact_store)
        _assert_required_artifacts_exist(artifacts)
        api_client.register_artifacts(
            execution.job_id,
            execution_id=execution.execution_id,
            artifacts=artifact_descriptors,
        )

        _check_cancellation(api_client, execution)
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="succeeded",
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
        )
        # END_BLOCK_BLOCK_EXECUTE_TRANSCRIPTION_PIPELINE
    except WorkerCancellationRequested:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="canceled",
            progress_stage="canceled",
            progress_message="Cancellation requested",
            error_code=None,
            error_message=None,
        )
        raise
    except Exception as exc:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
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
    execution: ClaimedJobExecution,
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> SourceCandidate:
    ordered_inputs = tuple(sorted(execution.ordered_inputs, key=lambda item: item.position))
    if len(ordered_inputs) == 1:
        return _materialize_single_ordered_input(ordered_inputs[0], workspace_dir, source_store)
    return _materialize_combined_source(ordered_inputs, workspace_dir, source_store)


def _materialize_single_ordered_input(
    ordered_input: OrderedWorkerInput,
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> SourceCandidate:
    input_dir = workspace_dir / "inputs" / f"{ordered_input.position:02d}-{ordered_input.source_id}"
    input_dir.mkdir(parents=True, exist_ok=True)

    if ordered_input.source_kind == "youtube_url":
        if not ordered_input.source_url:
            raise SourceMaterializationError("youtube_url input must include source_url")
        return SourceCandidate(
            source_id=ordered_input.source_id,
            kind="youtube_url",
            display_name=_resolve_display_name(ordered_input),
            url=ordered_input.source_url,
            telegram_file_id=None,
            mime_type=None,
            file_name=ordered_input.original_filename,
            file_unique_id=None,
            local_path=None,
        )

    local_path = _download_ordered_input(ordered_input, input_dir, source_store)
    return SourceCandidate(
        source_id=ordered_input.source_id,
        kind=_guess_media_kind(ordered_input),
        display_name=_resolve_display_name(ordered_input),
        url=None,
        telegram_file_id=None,
        mime_type=None,
        file_name=ordered_input.original_filename or local_path.name,
        file_unique_id=None,
        local_path=local_path,
    )


def _materialize_combined_source(
    ordered_inputs: tuple[OrderedWorkerInput, ...],
    workspace_dir: Path,
    source_store: SourceObjectStore,
) -> SourceCandidate:
    materialized_paths: list[Path] = []
    combined_dir = workspace_dir / "combined"
    combined_dir.mkdir(parents=True, exist_ok=True)

    for ordered_input in ordered_inputs:
        item_dir = combined_dir / f"{ordered_input.position:02d}-{ordered_input.source_id}"
        item_dir.mkdir(parents=True, exist_ok=True)
        if ordered_input.source_kind == "youtube_url":
            if not ordered_input.source_url:
                raise SourceMaterializationError("youtube_url input must include source_url")
            try:
                local_path = _download_youtube_audio(ordered_input.source_url, item_dir)
            except Exception as exc:
                raise SourceMaterializationError(str(exc)) from exc
        else:
            local_path = _download_ordered_input(ordered_input, item_dir, source_store)
        materialized_paths.append(local_path)

    output_path = combined_dir / "combined.wav"
    _concatenate_media_inputs(materialized_paths, output_path)
    return SourceCandidate(
        source_id=f"{ordered_inputs[0].source_id}-combined",
        kind="telegram_audio",
        display_name="Audio: combined-inputs.wav",
        url=None,
        telegram_file_id=None,
        mime_type="audio/wav",
        file_name="combined-inputs.wav",
        file_unique_id=None,
        local_path=output_path,
    )


def _download_ordered_input(
    ordered_input: OrderedWorkerInput,
    destination_dir: Path,
    source_store: SourceObjectStore,
) -> Path:
    if not ordered_input.object_key:
        raise SourceMaterializationError(f"{ordered_input.source_kind} input must include object_key")

    destination = destination_dir / _resolve_materialized_filename(ordered_input)
    try:
        source_store.fetch_file(object_key=ordered_input.object_key, destination=destination)
    except Exception as exc:
        raise SourceMaterializationError(str(exc)) from exc
    return destination


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
    job_id: str,
    artifacts: TranscriptArtifacts,
    artifact_store: ArtifactObjectStore,
) -> tuple[ArtifactDescriptor, ...]:
    writer = ArtifactWriter(job_id=job_id, object_store=artifact_store)
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


def _assert_required_artifacts_exist(artifacts: TranscriptArtifacts) -> None:
    required_paths = (
        artifacts.text_path,
        artifacts.markdown_path,
        artifacts.docx_path,
    )
    for path in required_paths:
        if not path.exists():
            raise RuntimeError(f"required transcript artifact is missing: {path}")


def _check_cancellation(api_client: JobApiClient, execution: ClaimedJobExecution) -> None:
    cancel_state = api_client.check_cancel(execution.job_id, execution_id=execution.execution_id)
    if cancel_state.cancel_requested:
        raise WorkerCancellationRequested(f"job {execution.job_id} was canceled")


def _resolve_display_name(ordered_input: OrderedWorkerInput) -> str:
    if ordered_input.display_name:
        return ordered_input.display_name
    if ordered_input.original_filename:
        media_prefix = "Video" if _guess_media_kind(ordered_input) == "telegram_video" else "Audio"
        return f"{media_prefix}: {ordered_input.original_filename}"
    if ordered_input.source_url:
        return f"YouTube: {ordered_input.source_url}"
    return ordered_input.source_id


def _resolve_materialized_filename(ordered_input: OrderedWorkerInput) -> str:
    if ordered_input.original_filename:
        return ordered_input.original_filename
    if ordered_input.display_name:
        normalized = ordered_input.display_name.split(":", 1)[-1].strip().replace("/", "-")
        suffix = Path(normalized).suffix
        if suffix:
            return normalized
    suffix = ".bin"
    if ordered_input.source_kind == "youtube_url":
        suffix = ".mp3"
    return f"{ordered_input.source_id}{suffix}"


def _guess_media_kind(ordered_input: OrderedWorkerInput) -> str:
    source_name = (ordered_input.original_filename or ordered_input.display_name or "").strip()
    if source_name.startswith("Video:"):
        return "telegram_video"
    if source_name.startswith("Audio:"):
        return "telegram_audio"

    suffix = Path(source_name).suffix.casefold()
    if suffix in _VIDEO_SUFFIXES:
        return "telegram_video"
    if suffix in _AUDIO_SUFFIXES:
        return "telegram_audio"
    return "telegram_audio"


def _classify_error_code(error: Exception) -> str:
    if isinstance(error, SourceMaterializationError):
        return "source_fetch_failed"
    return "transcription_failed"
