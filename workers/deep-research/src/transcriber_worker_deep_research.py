# FILE: workers/deep-research/src/transcriber_worker_deep_research.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Execute claimed deep-research jobs through the shared control-plane client while preserving the current multi-phase pipeline and required output contract.
# SCOPE: Worker claim/run orchestration, transcript and report artifact materialization, deep-research phase execution, optional execution-log synthesis, cancellation checks, and local helpers reused by the legacy service shell.
# DEPENDS: M-WORKER-DEEP-RESEARCH, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-DEEP-RESEARCH, V-M-WORKER-DEEP-RESEARCH
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Introduced the deep-research worker shell and extracted the legacy multi-phase pipeline into the dedicated worker package.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   ClaimedInputStore - Defines the artifact-download boundary for claimed deep-research inputs.
#   CommandRunner - Defines the injected phase-command runner used by the extracted local pipeline and packet-local tests.
#   DeepResearchPhase - Defines one deterministic deep-research phase and its required artifact gates.
#   MaterializedDeepResearchInputs - Captures the claimed transcript and report inputs required by the extracted pipeline.
#   DeepResearchArtifacts - Returns the final markdown output plus optional execution log synthesized from retained phase logs.
#   DeepResearchWorkerResult - Returns the successful worker execution evidence used by packet-local tests.
#   PHASES - Declares the canonical ordered phase list preserved from the legacy deep-research pipeline.
#   PROMPT_TEMPLATE - Defines the canonical per-phase prompt contract for retained-artifact execution.
#   WorkerCancellationRequested - Signals authoritative cancellation observed by the dedicated worker loop.
#   build_deep_research_command - Derive the canonical phase command for the extracted deep-research pipeline.
#   process_local_deep_research - Execute the preserved local deep-research pipeline and keep deterministic retained artifacts on disk.
#   run_deep_research - Preserve the legacy local service signature while delegating to the extracted pipeline helper.
#   runDeepResearch - Claim a deep-research job, execute the pipeline, register artifacts, and finalize through the shared control plane.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
import re
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


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_EXECUTE_DEEP_RESEARCH_PIPELINE = (
    "[WorkerDeepResearch][runDeepResearch][BLOCK_EXECUTE_DEEP_RESEARCH_PIPELINE]"
)
_DEEP_RESEARCH_FAILURE_CODE = "deep_research_failed"
_DEFAULT_COMMAND_TIMEOUT_SECONDS = 5400.0
_DEFAULT_POLL_INTERVAL_SECONDS = 0.25

CommandRunner = Callable[[list[str]], str]

__all__ = [
    "ClaimedInputStore",
    "CommandRunner",
    "DeepResearchArtifacts",
    "DeepResearchPhase",
    "DeepResearchWorkerResult",
    "MaterializedDeepResearchInputs",
    "PHASES",
    "PROMPT_TEMPLATE",
    "WorkerCancellationRequested",
    "build_deep_research_command",
    "process_local_deep_research",
    "runDeepResearch",
    "run_deep_research",
]


class ClaimedInputStore(Protocol):
    def fetch_file(self, *, object_key: str, destination: Path) -> None: ...


@dataclass(frozen=True, slots=True)
class DeepResearchPhase:
    phase_id: str
    reference_path: str
    required_artifacts: tuple[tuple[str, ...], ...]


@dataclass(frozen=True, slots=True)
class MaterializedDeepResearchInputs:
    transcript_path: Path
    report_path: Path
    source_label: str


@dataclass(frozen=True, slots=True)
class DeepResearchArtifacts:
    final_report_path: Path
    execution_log_path: Path | None


@dataclass(frozen=True, slots=True)
class DeepResearchWorkerResult:
    execution: ClaimedJobExecution
    inputs: MaterializedDeepResearchInputs
    artifacts: DeepResearchArtifacts
    artifact_descriptors: tuple[ArtifactDescriptor, ...]


class WorkerCancellationRequested(RuntimeError):
    pass


class DeepResearchInputError(RuntimeError):
    pass


PHASES: tuple[DeepResearchPhase, ...] = (
    DeepResearchPhase("phase-00-activation", "references/phase-00-activation.md", (("evidence-research-state.md",),)),
    DeepResearchPhase(
        "phase-01-scope-freeze",
        "references/phase-01-scope-freeze.md",
        (("evidence-research-state.md",), ("evidence-research-scope.md",)),
    ),
    DeepResearchPhase(
        "phase-02-decomposition",
        "references/phase-02-decomposition.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-plan.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-graph-schema.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-03-wave-collection",
        "references/phase-03-wave-collection.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-wave-log.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-source-registry.csv",),
            ("evidence-research-evidence-registry.jsonl",),
        ),
    ),
    DeepResearchPhase(
        "phase-04-gap-analysis",
        "references/phase-04-gap-analysis.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-gap-log.md",),
            ("evidence-research-plan.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-05-follow-up-collection",
        "references/phase-05-follow-up-collection.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-wave-log.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-source-registry.csv",),
            ("evidence-research-evidence-registry.jsonl",),
            ("evidence-research-gap-log.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-06-claim-audit",
        "references/phase-06-claim-audit.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-claim-audit.csv",),
        ),
    ),
    DeepResearchPhase(
        "phase-07-contradiction-pass",
        "references/phase-07-contradiction-pass.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-contradiction-register.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-claim-audit.csv",),
            ("evidence-research-gap-log.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-08-normalization",
        "references/phase-08-normalization.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-metrics.csv",),
            ("evidence-research-comparative-matrix.csv",),
            ("evidence-research-graph-schema.md", "evidence-research-graph.json"),
            ("evidence-research-evidence-registry.jsonl",),
        ),
    ),
    DeepResearchPhase(
        "phase-09-synthesis",
        "references/phase-09-synthesis.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-claim-registry.csv",),
            ("evidence-research-risk-control-matrix.md",),
            ("evidence-research-final-report.md",),
        ),
    ),
    DeepResearchPhase(
        "phase-10-verification",
        "references/phase-10-verification.md",
        (
            ("evidence-research-state.md",),
            ("evidence-research-verification-checklist.md",),
            ("evidence-research-final-report.md",),
            ("evidence-research-risk-control-matrix.md",),
        ),
    ),
)


PROMPT_TEMPLATE = """You are executing {phase_id} of a deterministic deep research pipeline for a Telegram bot research job.

Primary source artifacts:
- Transcript markdown: {transcript_path}
- Initial analytical report markdown: {report_path}
- Original source label: {source_label}

Working directory for this run:
- Research workspace: {research_dir}

Normative method references that you must read before acting:
- Skill entrypoint: {skill_root}/SKILL.md
- Control plane: {skill_root}/references/control-plane.md
- Artifact taxonomy: {skill_root}/references/artifact-taxonomy.md
- Final output contract: {skill_root}/references/final-output-contract.md
- Artifact templates: {skill_root}/templates/artifact-templates.md
- Current phase instructions: {skill_root}/{phase_reference}
- Research design basis: {design_doc}

Execution contract:
- This run is `evidence-retained`, not `report-only`.
- Work only through deterministic retained artifacts inside {research_dir}.
- Use the transcript and initial report as seed evidence, but broaden the study with external evidence when the phase permits collection.
- Follow the phase model and gate discipline from the evidence-first orchestration method.
- Use bounded subagents only when the current phase explicitly allows delegation.
- Do not collapse the run into one summary. Preserve the full research memory pack.
- Every material finding must trace to claim IDs, evidence packets, contradiction records, or explicit unresolved status.
- Keep the tone formal, analytical, and machine-usable.

Required artifacts for this phase:
{required_artifacts}

Report emphasis for this project:
- Reconstruct the vector of thought, not only conclusions.
- Preserve question -> answer -> discussion progression.
- Capture every distinct theme, idea, objection, comparison, and unresolved issue present in the transcript and follow-up evidence.
- Make agreements, disagreements, and boundary notes explicit.

Return contract:
- Update the phase artifacts on disk.
- Record the gate result in `evidence-research-state.md`.
- Return only one line in the form: `{phase_id}: <continue|repeat|stop>: <short summary>`.
"""


def build_deep_research_command(
    transcript_path: Path,
    report_path: Path,
    research_dir: Path,
    phase: DeepResearchPhase,
    source_label: str,
) -> list[str]:
    skill_root = _skill_root()
    design_doc = _design_doc()
    required_artifacts = "\n".join(f"- {' or '.join(option)}" for option in phase.required_artifacts)
    prompt = PROMPT_TEMPLATE.format(
        phase_id=phase.phase_id,
        transcript_path=transcript_path.resolve(),
        report_path=report_path.resolve(),
        source_label=source_label or "Unknown source",
        research_dir=research_dir.resolve(),
        skill_root=skill_root.resolve(),
        phase_reference=phase.reference_path,
        design_doc=design_doc.resolve(),
        required_artifacts=required_artifacts,
    ).strip()

    return [
        _resolve_claude_executable(),
        "-p",
        "--output-format",
        "text",
        "--permission-mode",
        "acceptEdits",
        "--model",
        _deep_research_model(),
        "--add-dir",
        str(research_dir.resolve()),
        str(transcript_path.parent.resolve()),
        str(skill_root.resolve()),
        str(design_doc.parent.resolve()),
        "--",
        prompt,
    ]


def process_local_deep_research(
    transcript_path: Path,
    report_path: Path,
    research_dir: Path,
    source_label: str,
    command_runner: CommandRunner | None = None,
) -> DeepResearchArtifacts:
    # START_BLOCK_BLOCK_PROCESS_LOCAL_DEEP_RESEARCH
    if not transcript_path.exists():
        raise FileNotFoundError(f"Transcript file does not exist: {transcript_path}")
    if not report_path.exists():
        raise FileNotFoundError(f"Research report seed does not exist: {report_path}")

    research_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = research_dir / "phase-logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    runner = command_runner or _run_command
    for phase in PHASES:
        output = runner(
            build_deep_research_command(
                transcript_path=transcript_path,
                report_path=report_path,
                research_dir=research_dir,
                phase=phase,
                source_label=source_label,
            )
        ).strip()
        (logs_dir / f"{phase.phase_id}.log").write_text((output + "\n") if output else "", encoding="utf-8")
        gate_status, gate_summary = _parse_gate_result(output, phase.phase_id)
        _validate_phase_artifacts(research_dir, phase)
        if gate_status == "repeat":
            raise RuntimeError(f"{phase.phase_id} requested repeat: {gate_summary}")
        if gate_status == "stop" and phase != PHASES[-1]:
            raise RuntimeError(f"{phase.phase_id} stopped the pipeline early: {gate_summary}")

    final_report = research_dir / "evidence-research-final-report.md"
    if not final_report.exists():
        raise RuntimeError("Deep research pipeline completed without a final report artifact.")
    return DeepResearchArtifacts(
        final_report_path=final_report,
        execution_log_path=_synthesize_execution_log(research_dir),
    )
    # END_BLOCK_BLOCK_PROCESS_LOCAL_DEEP_RESEARCH


def run_deep_research(
    transcript_path: Path,
    report_path: Path,
    research_dir: Path,
    source_label: str,
    command_runner: CommandRunner | None = None,
) -> Path:
    return process_local_deep_research(
        transcript_path=transcript_path,
        report_path=report_path,
        research_dir=research_dir,
        source_label=source_label,
        command_runner=command_runner,
    ).final_report_path


def runDeepResearch(
    job_id: str,
    *,
    workspace_root: Path,
    api_client: JobApiClient,
    input_store: ClaimedInputStore,
    artifact_store: ArtifactObjectStore,
) -> DeepResearchWorkerResult:
    execution = api_client.claim_job(job_id, worker_kind="deep_research", task_type="deep_research.run")
    workspace_dir = Path(workspace_root) / execution.job_id
    workspace_dir.mkdir(parents=True, exist_ok=True)

    try:
        # START_BLOCK_BLOCK_EXECUTE_DEEP_RESEARCH_PIPELINE
        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="loading_report",
            progress_message="Resolving transcript and report inputs",
        )
        materialized_inputs = _materialize_execution_inputs(execution, workspace_dir, input_store)

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="researching",
            progress_message="Running the deep-research phase pipeline",
        )
        _LOGGER.info(
            "%s job_id=%s execution_id=%s transcript_path=%s report_path=%s",
            _LOG_MARKER_EXECUTE_DEEP_RESEARCH_PIPELINE,
            execution.job_id,
            execution.execution_id,
            materialized_inputs.transcript_path,
            materialized_inputs.report_path,
        )
        artifacts = process_local_deep_research(
            transcript_path=materialized_inputs.transcript_path,
            report_path=materialized_inputs.report_path,
            research_dir=workspace_dir / "deep_research",
            source_label=materialized_inputs.source_label,
            command_runner=lambda command: _run_command_with_cancellation(
                command,
                check_cancellation=lambda: _check_cancellation(api_client, execution),
            ),
        )

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="writing_output",
            progress_message="Validating deep-research outputs",
        )
        _assert_required_artifacts_exist(artifacts)

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="uploading_artifacts",
            progress_message="Uploading deep-research artifacts",
        )
        artifact_descriptors = _persist_deep_research_artifacts(execution.job_id, artifacts, artifact_store)
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
            progress_message="Deep research ready",
            error_code=None,
            error_message=None,
        )
        return DeepResearchWorkerResult(
            execution=execution,
            inputs=materialized_inputs,
            artifacts=artifacts,
            artifact_descriptors=artifact_descriptors,
        )
        # END_BLOCK_BLOCK_EXECUTE_DEEP_RESEARCH_PIPELINE
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
            progress_message="Deep research failed",
            error_code=_DEEP_RESEARCH_FAILURE_CODE,
            error_message=str(exc),
        )
        raise


def _materialize_execution_inputs(
    execution: ClaimedJobExecution,
    workspace_dir: Path,
    input_store: ClaimedInputStore,
) -> MaterializedDeepResearchInputs:
    # START_BLOCK_BLOCK_MATERIALIZE_DEEP_RESEARCH_INPUTS
    ordered_inputs = tuple(sorted(execution.ordered_inputs, key=lambda item: item.position))
    transcript_input: OrderedWorkerInput | None = None
    report_input: OrderedWorkerInput | None = None
    for ordered_input in ordered_inputs:
        role = _infer_input_role(ordered_input)
        if role == "transcript":
            if transcript_input is not None:
                raise DeepResearchInputError("deep_research job must not include duplicate transcript inputs")
            transcript_input = ordered_input
            continue
        if role == "report":
            if report_input is not None:
                raise DeepResearchInputError("deep_research job must not include duplicate report inputs")
            report_input = ordered_input
            continue
        raise DeepResearchInputError(
            f"deep_research job input could not be identified as transcript or report: {ordered_input.object_key or ordered_input.display_name or ordered_input.source_id}"
        )

    if transcript_input is None or report_input is None:
        raise DeepResearchInputError("deep_research job must include transcript and report markdown inputs")

    transcript_path = _materialize_execution_input(transcript_input, workspace_dir, input_store)
    report_path = _materialize_execution_input(report_input, workspace_dir, input_store)
    source_label = report_input.display_name or transcript_input.display_name or report_path.stem or "Unknown source"
    return MaterializedDeepResearchInputs(
        transcript_path=transcript_path,
        report_path=report_path,
        source_label=source_label,
    )
    # END_BLOCK_BLOCK_MATERIALIZE_DEEP_RESEARCH_INPUTS


def _infer_input_role(ordered_input: OrderedWorkerInput) -> str | None:
    candidates = [
        ordered_input.object_key or "",
        ordered_input.original_filename or "",
        ordered_input.display_name or "",
    ]
    normalized_candidates = [candidate.lower() for candidate in candidates if candidate]
    if any("/report/" in candidate or candidate.endswith("report.md") or "report:" in candidate for candidate in normalized_candidates):
        return "report"
    if any("/transcript/" in candidate or candidate.endswith("transcript.md") or "transcript:" in candidate for candidate in normalized_candidates):
        return "transcript"
    return None


def _materialize_execution_input(
    ordered_input: OrderedWorkerInput,
    workspace_dir: Path,
    input_store: ClaimedInputStore,
) -> Path:
    if not ordered_input.object_key:
        raise DeepResearchInputError("deep_research input must include object_key")

    input_dir = workspace_dir / "inputs" / f"{ordered_input.position:02d}-{ordered_input.source_id}"
    input_dir.mkdir(parents=True, exist_ok=True)
    destination = input_dir / _resolve_materialized_filename(ordered_input)
    try:
        input_store.fetch_file(object_key=ordered_input.object_key, destination=destination)
    except Exception as exc:
        raise DeepResearchInputError(str(exc)) from exc
    return destination


def _persist_deep_research_artifacts(
    job_id: str,
    artifacts: DeepResearchArtifacts,
    artifact_store: ArtifactObjectStore,
) -> tuple[ArtifactDescriptor, ...]:
    writer = ArtifactWriter(job_id=job_id, object_store=artifact_store)
    descriptors = [
        writer.write_file_artifact(
            "deep_research_markdown",
            artifacts.final_report_path,
            mime_type="text/markdown; charset=utf-8",
            format="markdown",
        )
    ]
    if artifacts.execution_log_path is not None:
        descriptors.append(
            writer.write_file_artifact(
                "execution_log",
                artifacts.execution_log_path,
                mime_type="text/plain; charset=utf-8",
                format="log",
            )
        )
    return tuple(descriptors)


def _assert_required_artifacts_exist(artifacts: DeepResearchArtifacts) -> None:
    if not artifacts.final_report_path.exists():
        raise RuntimeError(f"required deep-research artifact is missing: {artifacts.final_report_path}")
    if artifacts.execution_log_path is not None and not artifacts.execution_log_path.exists():
        raise RuntimeError(f"optional execution log artifact is missing: {artifacts.execution_log_path}")


def _synthesize_execution_log(research_dir: Path) -> Path | None:
    # START_BLOCK_BLOCK_SYNTHESIZE_EXECUTION_LOG
    logs_dir = research_dir / "phase-logs"
    if not logs_dir.exists():
        return None

    rendered_sections: list[str] = []
    for log_path in sorted(logs_dir.glob("*.log")):
        content = log_path.read_text(encoding="utf-8").strip()
        if not content:
            continue
        rendered_sections.append(f"== {log_path.stem} ==\n{content}")

    if not rendered_sections:
        return None

    execution_log_path = research_dir / "execution.log"
    execution_log_path.write_text("\n\n".join(rendered_sections) + "\n", encoding="utf-8")
    return execution_log_path
    # END_BLOCK_BLOCK_SYNTHESIZE_EXECUTION_LOG


def _validate_phase_artifacts(research_dir: Path, phase: DeepResearchPhase) -> None:
    for options in phase.required_artifacts:
        candidates = [research_dir / name for name in options]
        if any(candidate.exists() and candidate.stat().st_size > 0 for candidate in candidates):
            continue
        label = " or ".join(options)
        raise RuntimeError(f"{phase.phase_id} did not produce required artifact: {label}")


def _parse_gate_result(output: str, phase_id: str) -> tuple[str, str]:
    match = re.search(rf"{re.escape(phase_id)}:\s*(continue|repeat|stop)\s*:\s*(.+)", output, re.IGNORECASE)
    if match is None:
        raise RuntimeError(f"{phase_id} did not return a valid gate result line.")
    return match.group(1).lower(), match.group(2).strip()


def _skill_root() -> Path:
    return Path(
        os.getenv(
            "DEEP_RESEARCH_SKILL_ROOT",
            "/Users/danila/.agents/skills/evidence-first-research-orchestrator",
        )
    )


def _design_doc() -> Path:
    return Path(
        os.getenv(
            "DEEP_RESEARCH_DESIGN_DOC",
            "/Users/danila/work/my/ai-workflow/docs/research/2026-04-04-evidence-first-deep-research-system-design.md",
        )
    )


def _deep_research_model() -> str:
    return os.getenv("DEEP_RESEARCH_MODEL", "sonnet").strip() or "sonnet"


def _resolve_claude_executable() -> str:
    configured = os.getenv("DEEP_RESEARCH_BIN", "").strip()
    if configured:
        return configured

    configured = os.getenv("CLAUDE_BIN", "").strip()
    if configured:
        return configured

    resolved = shutil.which("claude")
    if resolved:
        return resolved

    fallback = Path("/opt/homebrew/bin/claude")
    if fallback.exists():
        return str(fallback)

    raise RuntimeError("claude executable not found. Set DEEP_RESEARCH_BIN or add claude to PATH.")


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
            timeout=int(os.getenv("DEEP_RESEARCH_TIMEOUT_SECONDS", str(int(_DEFAULT_COMMAND_TIMEOUT_SECONDS)))),
            env=_build_command_env(),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("deep research pipeline timed out") from exc
    if completed.returncode != 0:
        raise RuntimeError(f"deep research command failed with exit code {completed.returncode}: {completed.stderr.strip()}")
    return completed.stdout


def _run_command_with_cancellation(
    command: list[str],
    *,
    check_cancellation: Callable[[], None],
    timeout_seconds: float | None = None,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
) -> str:
    resolved_timeout_seconds = timeout_seconds
    if resolved_timeout_seconds is None:
        resolved_timeout_seconds = float(
            os.getenv("DEEP_RESEARCH_TIMEOUT_SECONDS", str(int(_DEFAULT_COMMAND_TIMEOUT_SECONDS)))
        )

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=_build_command_env(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"claude executable not found: {command[0]}") from exc

    deadline = time.monotonic() + resolved_timeout_seconds
    try:
        while True:
            check_cancellation()
            if process.poll() is not None:
                break
            if time.monotonic() >= deadline:
                _terminate_process(process)
                raise RuntimeError("deep research pipeline timed out")
            time.sleep(poll_interval_seconds)

        stdout, stderr = process.communicate()
    except WorkerCancellationRequested:
        _terminate_process(process)
        raise

    if process.returncode != 0:
        raise RuntimeError(f"deep research command failed with exit code {process.returncode}: {stderr.strip()}")
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


def _check_cancellation(api_client: JobApiClient, execution: ClaimedJobExecution) -> None:
    cancel_state = api_client.check_cancel(execution.job_id, execution_id=execution.execution_id)
    if cancel_state.cancel_requested:
        raise WorkerCancellationRequested(f"job {execution.job_id} was canceled")


def _resolve_materialized_filename(ordered_input: OrderedWorkerInput) -> str:
    if ordered_input.original_filename:
        return ordered_input.original_filename
    if ordered_input.display_name:
        normalized = ordered_input.display_name.split(":", 1)[-1].strip().replace("/", "-")
        if normalized:
            return normalized
    return f"{ordered_input.source_id}.md"
