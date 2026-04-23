# FILE: workers/report/src/transcriber_worker_report.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Execute claimed report jobs through the shared control-plane client while preserving current cglm-based report generation behavior.
# SCOPE: Worker claim/run orchestration, transcript artifact materialization, cglm subprocess lifecycle handling, report artifact rendering, cancellation checks, and local helpers reused by the legacy service shell.
# DEPENDS: M-WORKER-REPORT, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-REPORT, V-M-WORKER-REPORT
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Introduced the report worker shell and extracted the legacy cglm runner into the dedicated worker package.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   ClaimedInputStore - Defines the transcript-artifact download boundary for claimed worker inputs.
#   ReportWorkerResult - Returns the successful worker execution evidence used by packet-local tests.
#   WorkerCancellationRequested - Signals authoritative cancellation observed by the dedicated worker loop.
#   build_cglm_command - Derive the canonical cglm command from the materialized transcript path.
#   generate_report - Run cglm locally and write normalized markdown for compatibility callers.
#   process_local_report - Execute the preserved local report pipeline and write markdown plus DOCX artifacts.
#   runReport - Claims a job, executes the report pipeline, registers artifacts, and finalizes through the shared control plane.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol


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
from telegram_transcriber_bot.documents import normalize_report_markdown, write_report_docx
from telegram_transcriber_bot.domain import ReportArtifacts


REPORT_PROMPT_TEMPLATE = """Read the transcript markdown file at {transcript_path}.

Produce a formal Markdown analytical report in Russian with the following exact structure:
# Исследовательский отчёт
## Контекст источника
## Исследовательские вопросы
## Краткие ответы на исследовательские вопросы
## Карта уникальных тем и идей
## Линия рассуждения и развитие обсуждения
## Нормализованная позиция обсуждения
## Выводы
## Открытые вопросы

Requirements:
- Base the report strictly on the transcript content and explicitly labeled common-sense inference.
- Capture every distinct topic, idea, hypothesis, argument, objection, comparison, and conclusion present in the transcript.
- Keep the tone formal, strict, neutral, and non-conversational.
- Do not write meta phrases, introductions, or comments about your own work.
- Do not write phrases like "Вот отчёт", "Ниже отчёт", "На основе транскрипта", "я считаю", "мне кажется".
- Do not output horizontal rules.
- Use Markdown headings and plain paragraphs or concise lists only when useful.
- Avoid decorative emphasis and avoid inline markdown like bold unless absolutely necessary.
- In "Краткие ответы на исследовательские вопросы", answer each question separately and tie the answer back to transcript evidence.
- In "Карта уникальных тем и идей", do not collapse different ideas into one vague summary.
- In "Линия рассуждения и развитие обсуждения", reconstruct the vector of thinking: how one premise led to the next, what examples support what claims, and where the discussion reframed or deepened the issue.
- In "Нормализованная позиция обсуждения", synthesize the final coherent position, agreements, disagreements, unresolved tensions, and boundary notes between explicit transcript evidence and reasonable inference.
- In "Открытые вопросы", include only questions that remain open after the discussion.
- Preserve the internal logic of the discussion, not only the final summary.
- If multiple speakers or positions are present, make the structure of agreement, disagreement, and progression explicit.

Return only the final report markdown.
"""

_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_EXECUTE_REPORT_PIPELINE = "[WorkerReport][runReport][BLOCK_EXECUTE_REPORT_PIPELINE]"
_REPORT_FAILURE_CODE = "report_failed"
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 900.0
_DEFAULT_POLL_INTERVAL_SECONDS = 0.25

CommandRunner = Callable[[list[str]], str]

__all__ = [
    "ClaimedInputStore",
    "ReportWorkerResult",
    "WorkerCancellationRequested",
    "build_cglm_command",
    "generate_report",
    "process_local_report",
    "runReport",
]


class ClaimedInputStore(Protocol):
    def fetch_file(self, *, object_key: str, destination: Path) -> None: ...


@dataclass(frozen=True, slots=True)
class ReportWorkerResult:
    execution: ClaimedJobExecution
    transcript_path: Path
    report: ReportArtifacts
    artifact_descriptors: tuple[ArtifactDescriptor, ...]


class WorkerCancellationRequested(RuntimeError):
    pass


class ReportInputError(RuntimeError):
    pass


def build_cglm_command(transcript_path: Path, report_prompt_suffix: str = "") -> list[str]:
    resolved_transcript_path = transcript_path.resolve()
    prompt = REPORT_PROMPT_TEMPLATE.format(transcript_path=resolved_transcript_path).strip()
    if report_prompt_suffix:
        prompt = f"{prompt}\n\n{report_prompt_suffix.strip()}"

    return [
        _resolve_cglm_executable(),
        "-p",
        "--output-format",
        "text",
        "--permission-mode",
        "dontAsk",
        "--add-dir",
        str(resolved_transcript_path.parent),
        "--",
        prompt,
    ]


def generate_report(
    transcript_path: Path,
    report_path: Path,
    report_prompt_suffix: str = "",
    command_runner: CommandRunner | None = None,
) -> Path:
    if not transcript_path.exists():
        raise FileNotFoundError(f"Transcript file does not exist: {transcript_path}")

    runner = command_runner or _run_command
    output = runner(build_cglm_command(transcript_path, report_prompt_suffix=report_prompt_suffix)).strip()
    if not output:
        raise RuntimeError("cglm returned an empty report")

    normalized_output = normalize_report_markdown(output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(normalized_output, encoding="utf-8")
    return report_path


def process_local_report(
    transcript_path: Path,
    *,
    workspace_dir: Path,
    report_prompt_suffix: str = "",
    command_runner: CommandRunner | None = None,
) -> ReportArtifacts:
    # START_BLOCK_BLOCK_PROCESS_LOCAL_REPORT
    workspace_dir.mkdir(parents=True, exist_ok=True)
    report_markdown_path = workspace_dir / "report.md"
    report_docx_path = workspace_dir / "report.docx"
    generate_report(
        transcript_path=transcript_path,
        report_path=report_markdown_path,
        report_prompt_suffix=report_prompt_suffix,
        command_runner=command_runner,
    )
    write_report_docx(report_docx_path, report_markdown_path.read_text(encoding="utf-8"))
    return ReportArtifacts(
        markdown_path=report_markdown_path,
        docx_path=report_docx_path,
    )
    # END_BLOCK_BLOCK_PROCESS_LOCAL_REPORT


def runReport(
    job_id: str,
    *,
    workspace_root: Path,
    api_client: JobApiClient,
    input_store: ClaimedInputStore,
    artifact_store: ArtifactObjectStore,
) -> ReportWorkerResult:
    execution = api_client.claim_job(job_id, worker_kind="report", task_type="report.run")
    workspace_dir = Path(workspace_root) / execution.job_id
    workspace_dir.mkdir(parents=True, exist_ok=True)

    try:
        # START_BLOCK_BLOCK_EXECUTE_REPORT_PIPELINE
        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="materializing_inputs",
            progress_message="Resolving claimed report inputs",
        )
        transcript_path = _materialize_execution_transcript(execution, workspace_dir, input_store)

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="generating_report",
            progress_message="Running cglm report pipeline",
        )
        _LOGGER.info(
            "%s job_id=%s execution_id=%s transcript_path=%s",
            _LOG_MARKER_EXECUTE_REPORT_PIPELINE,
            execution.job_id,
            execution.execution_id,
            transcript_path,
        )
        report = process_local_report(
            transcript_path,
            workspace_dir=workspace_dir,
            report_prompt_suffix=_resolve_report_prompt_suffix(execution),
            command_runner=lambda command: _run_command_with_cancellation(
                command,
                check_cancellation=lambda: _check_cancellation(api_client, execution),
            ),
        )

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="persisting_artifacts",
            progress_message="Uploading report artifacts",
        )
        _assert_required_artifacts_exist(report)
        artifact_descriptors = _persist_report_artifacts(execution.job_id, report, artifact_store)
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
            progress_message="Report ready",
            error_code=None,
            error_message=None,
        )
        return ReportWorkerResult(
            execution=execution,
            transcript_path=transcript_path,
            report=report,
            artifact_descriptors=artifact_descriptors,
        )
        # END_BLOCK_BLOCK_EXECUTE_REPORT_PIPELINE
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
            progress_message="Report generation failed",
            error_code=_REPORT_FAILURE_CODE,
            error_message=str(exc),
        )
        raise


def _materialize_execution_transcript(
    execution: ClaimedJobExecution,
    workspace_dir: Path,
    input_store: ClaimedInputStore,
) -> Path:
    ordered_inputs = tuple(sorted(execution.ordered_inputs, key=lambda item: item.position))
    if len(ordered_inputs) != 1:
        raise ReportInputError("report job must include exactly one ordered input")

    ordered_input = ordered_inputs[0]
    if not ordered_input.object_key:
        raise ReportInputError("report input must include object_key")

    input_dir = workspace_dir / "inputs" / f"{ordered_input.position:02d}-{ordered_input.source_id}"
    input_dir.mkdir(parents=True, exist_ok=True)
    destination = input_dir / _resolve_materialized_filename(ordered_input)
    try:
        input_store.fetch_file(object_key=ordered_input.object_key, destination=destination)
    except Exception as exc:
        raise ReportInputError(str(exc)) from exc
    return destination


def _persist_report_artifacts(
    job_id: str,
    report: ReportArtifacts,
    artifact_store: ArtifactObjectStore,
) -> tuple[ArtifactDescriptor, ...]:
    writer = ArtifactWriter(job_id=job_id, object_store=artifact_store)
    return (
        writer.write_file_artifact(
            "report_markdown",
            report.markdown_path,
            mime_type="text/markdown; charset=utf-8",
            format="markdown",
        ),
        writer.write_file_artifact(
            "report_docx",
            report.docx_path,
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            format="docx",
        ),
    )


def _assert_required_artifacts_exist(report: ReportArtifacts) -> None:
    for path in (report.markdown_path, report.docx_path):
        if not path.exists():
            raise RuntimeError(f"required report artifact is missing: {path}")


def _check_cancellation(api_client: JobApiClient, execution: ClaimedJobExecution) -> None:
    cancel_state = api_client.check_cancel(execution.job_id, execution_id=execution.execution_id)
    if cancel_state.cancel_requested:
        raise WorkerCancellationRequested(f"job {execution.job_id} was canceled")


def _resolve_report_prompt_suffix(execution: ClaimedJobExecution) -> str:
    value = execution.params.get("report_prompt_suffix", "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ReportInputError("report_prompt_suffix must be a string when provided")
    return value.strip()


def _resolve_materialized_filename(ordered_input: OrderedWorkerInput) -> str:
    if ordered_input.original_filename:
        return ordered_input.original_filename
    if ordered_input.display_name:
        normalized = ordered_input.display_name.split(":", 1)[-1].strip().replace("/", "-")
        if normalized:
            return normalized
    return f"{ordered_input.source_id}.md"


def _resolve_cglm_executable() -> str:
    configured = os.getenv("CGLM_BIN", "").strip()
    if configured:
        return configured

    resolved = shutil.which("cglm")
    if resolved:
        return resolved

    fallback = Path.home() / "bin" / "cglm"
    if fallback.exists():
        return str(fallback)

    raise RuntimeError("cglm executable not found. Set CGLM_BIN or add cglm to PATH.")


def _build_command_env() -> dict[str, str]:
    env = dict(os.environ)
    path_entries = [
        str(Path.home() / "bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ]
    existing = env.get("PATH", "")
    if existing:
        path_entries.extend(part for part in existing.split(":") if part)

    normalized: list[str] = []
    seen: set[str] = set()
    for part in path_entries:
        if not part or part in seen:
            continue
        seen.add(part)
        normalized.append(part)
    env["PATH"] = ":".join(normalized)
    return env


def _run_command(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=_DEFAULT_COMMAND_TIMEOUT_SECONDS,
            env=_build_command_env(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"cglm executable not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("cglm timed out while generating a report") from exc
    if completed.returncode != 0:
        raise RuntimeError(f"cglm failed with exit code {completed.returncode}: {completed.stderr.strip()}")
    return completed.stdout


def _run_command_with_cancellation(
    command: list[str],
    *,
    check_cancellation: Callable[[], None],
    timeout_seconds: float = _DEFAULT_COMMAND_TIMEOUT_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
) -> str:
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=_build_command_env(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"cglm executable not found: {command[0]}") from exc

    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            check_cancellation()
            if process.poll() is not None:
                break
            if time.monotonic() >= deadline:
                _terminate_process(process)
                raise RuntimeError("cglm timed out while generating a report")
            time.sleep(poll_interval_seconds)

        stdout, stderr = process.communicate()
    except WorkerCancellationRequested:
        _terminate_process(process)
        raise

    if process.returncode != 0:
        raise RuntimeError(f"cglm failed with exit code {process.returncode}: {stderr.strip()}")
    return stdout


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        try:
            process.communicate()
        except Exception:
            return
        return

    process.terminate()
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=1.0)
