# FILE: workers/deep-research/tests/test_transcriber_worker_deep_research.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the dedicated deep-research worker claims jobs through the shared control plane, preserves phase artifacts plus final markdown output, and keeps terminal failures deterministic.
# SCOPE: Success finalization ordering, transcript/report input materialization, local extraction reuse, optional execution-log registration, cancellation checkpoints, and subprocess termination handling.
# DEPENDS: M-WORKER-DEEP-RESEARCH, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-DEEP-RESEARCH, V-M-WORKER-DEEP-RESEARCH
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local verification for the deep-research worker shell and extracted local pipeline.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   RecordingApiClient - Captures claim/progress/artifact/finalize/cancel calls without redefining DTO payloads.
#   FakeInputStore - Writes claimed artifact bytes into the worker workspace.
#   InMemoryArtifactStore - Captures uploaded artifact bytes.
#   test_run_deep_research_claims_and_finalizes_after_required_outputs_exist - Verifies shared claim/finalize reuse, preserved phase artifacts, and optional execution-log registration.
#   test_process_local_deep_research_reuses_extracted_local_pipeline - Verifies the extracted local helper preserves the retained phase artifacts and final report contract.
#   test_run_deep_research_fails_when_required_output_is_missing - Verifies success is impossible before the final markdown output exists.
#   test_run_deep_research_surfaces_deterministic_pipeline_failures - Verifies timeout and blocked-write paths finalize as deterministic worker failures.
#   test_run_deep_research_checks_cancellation_inside_worker_loop - Verifies authoritative cancellation finalizes `canceled` without artifact registration.
#   test_run_command_with_cancellation_terminates_process_before_raising - Verifies subprocess ownership stays inside the worker loop when cancellation is observed.
# END_MODULE_MAP

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from transcriber_workers_common.api import CancelCheckResult, ClaimedJobExecution, OrderedWorkerInput
import transcriber_worker_deep_research as worker_module
from transcriber_worker_deep_research import (
    PHASES,
    WorkerCancellationRequested,
    process_local_deep_research,
    runDeepResearch,
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


class FakeInputStore:
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


def test_run_deep_research_claims_and_finalizes_after_required_outputs_exist(
    tmp_path: Path,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="transcript-1",
            source_kind="uploaded_file",
            display_name="Transcript: transcript.md",
            original_filename="transcript.md",
            object_key="artifacts/job-parent/transcript/segmented/transcript.md",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="report-1",
            source_kind="uploaded_file",
            display_name="Report: report.md",
            original_filename="report.md",
            object_key="artifacts/job-parent/report/markdown/report.md",
        ),
    )
    api_client = RecordingApiClient(execution)
    input_store = FakeInputStore(
        {
            "artifacts/job-parent/transcript/segmented/transcript.md": b"# Transcript\n\n- Theme A\n",
            "artifacts/job-parent/report/markdown/report.md": b"# Report\n\n- Hypothesis A\n",
        }
    )
    artifact_store = InMemoryArtifactStore()
    captured_commands: list[list[str]] = []

    def fake_run_with_cancellation(command: list[str], *, check_cancellation, **_kwargs) -> str:
        captured_commands.append(command)
        check_cancellation()
        research_dir = Path(command[command.index("--add-dir") + 1])
        phase = _phase_from_prompt(command[-1])
        _write_phase_artifacts(research_dir, phase)
        return f"{phase.phase_id}: continue: ok"

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", fake_run_with_cancellation)

    result = runDeepResearch(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        input_store=input_store,
        artifact_store=artifact_store,
    )

    assert api_client.calls[0] == (
        "claim_job",
        {"job_id": execution.job_id, "worker_kind": "deep_research", "task_type": "deep_research.run"},
    )
    assert [call[1]["progress_stage"] for call in api_client.calls if call[0] == "publish_progress"] == [
        "loading_report",
        "researching",
        "writing_output",
        "uploading_artifacts",
    ]
    assert input_store.calls == [
        (
            "artifacts/job-parent/transcript/segmented/transcript.md",
            str(result.inputs.transcript_path),
        ),
        (
            "artifacts/job-parent/report/markdown/report.md",
            str(result.inputs.report_path),
        ),
    ]
    assert result.inputs.transcript_path.read_text(encoding="utf-8") == "# Transcript\n\n- Theme A\n"
    assert result.inputs.report_path.read_text(encoding="utf-8") == "# Report\n\n- Hypothesis A\n"
    assert len(captured_commands) == len(PHASES)

    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "deep_research_markdown",
        "execution_log",
    ]
    assert result.artifacts.final_report_path.exists()
    assert result.artifacts.execution_log_path is not None
    assert result.artifacts.execution_log_path.exists()
    assert len(artifact_store.calls) == 2
    finalize_call = api_client.calls[-1]
    assert finalize_call[0] == "finalize_job"
    assert finalize_call[1]["outcome"] == "succeeded"
    assert api_client.calls.index(register_call) < api_client.calls.index(finalize_call)
    assert _required_marker() in caplog.text


def test_process_local_deep_research_reuses_extracted_local_pipeline(tmp_path: Path) -> None:
    transcript_path = tmp_path / "transcript.md"
    report_path = tmp_path / "report.md"
    transcript_path.write_text("# Transcript\n", encoding="utf-8")
    report_path.write_text("# Report\n", encoding="utf-8")

    def successful_runner(command: list[str]) -> str:
        research_dir = Path(command[command.index("--add-dir") + 1])
        phase = _phase_from_prompt(command[-1])
        _write_phase_artifacts(research_dir, phase)
        return f"{phase.phase_id}: continue: ok"

    artifacts = process_local_deep_research(
        transcript_path=transcript_path,
        report_path=report_path,
        research_dir=tmp_path / "deep_research",
        source_label="Demo source",
        command_runner=successful_runner,
    )

    assert artifacts.final_report_path.read_text(encoding="utf-8").startswith("phase-10-verification")
    assert artifacts.execution_log_path is not None
    assert artifacts.execution_log_path.exists()


def test_run_deep_research_fails_when_required_output_is_missing(tmp_path: Path, monkeypatch) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="transcript-1",
            source_kind="uploaded_file",
            display_name="Transcript: transcript.md",
            original_filename="transcript.md",
            object_key="artifacts/job-parent/transcript/segmented/transcript.md",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="report-1",
            source_kind="uploaded_file",
            display_name="Report: report.md",
            original_filename="report.md",
            object_key="artifacts/job-parent/report/markdown/report.md",
        ),
    )
    api_client = RecordingApiClient(execution)
    input_store = FakeInputStore(
        {
            "artifacts/job-parent/transcript/segmented/transcript.md": b"# Transcript\n",
            "artifacts/job-parent/report/markdown/report.md": b"# Report\n",
        }
    )
    artifact_store = InMemoryArtifactStore()

    def incomplete_runner(command: list[str], *, check_cancellation, **_kwargs) -> str:
        check_cancellation()
        research_dir = Path(command[command.index("--add-dir") + 1])
        phase = _phase_from_prompt(command[-1])
        _write_phase_artifacts(research_dir, phase, skip_artifact="evidence-research-final-report.md")
        return f"{phase.phase_id}: continue: missing final report"

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", incomplete_runner)

    with pytest.raises(RuntimeError, match="did not produce required artifact: evidence-research-final-report.md"):
        runDeepResearch(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            input_store=input_store,
            artifact_store=artifact_store,
        )

    assert not any(call[0] == "register_artifacts" for call in api_client.calls)
    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Deep research failed",
            "error_code": "deep_research_failed",
            "error_message": "phase-09-synthesis did not produce required artifact: evidence-research-final-report.md",
        },
    )


@pytest.mark.parametrize(
    ("failure_message", "match_text"),
    [
        ("deep research pipeline timed out", "timed out"),
        ("Запись файлов заблокирована в текущем режиме разрешений (`don't ask`).", "заблокирована"),
    ],
)
def test_run_deep_research_surfaces_deterministic_pipeline_failures(
    tmp_path: Path,
    monkeypatch,
    failure_message: str,
    match_text: str,
) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="transcript-1",
            source_kind="uploaded_file",
            display_name="Transcript: transcript.md",
            original_filename="transcript.md",
            object_key="artifacts/job-parent/transcript/segmented/transcript.md",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="report-1",
            source_kind="uploaded_file",
            display_name="Report: report.md",
            original_filename="report.md",
            object_key="artifacts/job-parent/report/markdown/report.md",
        ),
    )
    api_client = RecordingApiClient(execution)
    input_store = FakeInputStore(
        {
            "artifacts/job-parent/transcript/segmented/transcript.md": b"# Transcript\n",
            "artifacts/job-parent/report/markdown/report.md": b"# Report\n",
        }
    )
    artifact_store = InMemoryArtifactStore()

    def failing_runner(command: list[str], *, check_cancellation, **_kwargs) -> str:
        check_cancellation()
        raise RuntimeError(failure_message)

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", failing_runner)

    with pytest.raises(RuntimeError, match=match_text):
        runDeepResearch(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            input_store=input_store,
            artifact_store=artifact_store,
        )

    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Deep research failed",
            "error_code": "deep_research_failed",
            "error_message": failure_message,
        },
    )


def test_run_deep_research_checks_cancellation_inside_worker_loop(tmp_path: Path, monkeypatch) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="transcript-1",
            source_kind="uploaded_file",
            display_name="Transcript: transcript.md",
            original_filename="transcript.md",
            object_key="artifacts/job-parent/transcript/segmented/transcript.md",
        ),
        OrderedWorkerInput(
            position=1,
            source_id="report-1",
            source_kind="uploaded_file",
            display_name="Report: report.md",
            original_filename="report.md",
            object_key="artifacts/job-parent/report/markdown/report.md",
        ),
    )
    api_client = RecordingApiClient(
        execution,
        cancel_results=[
            CancelCheckResult(cancel_requested=False, status="running"),
            CancelCheckResult(cancel_requested=False, status="running"),
            CancelCheckResult(cancel_requested=True, status="cancel_requested"),
        ],
    )
    input_store = FakeInputStore(
        {
            "artifacts/job-parent/transcript/segmented/transcript.md": b"# Transcript\n",
            "artifacts/job-parent/report/markdown/report.md": b"# Report\n",
        }
    )
    artifact_store = InMemoryArtifactStore()

    def fake_run_with_cancellation(command: list[str], *, check_cancellation, **_kwargs) -> str:
        check_cancellation()
        pytest.fail("cancellation should interrupt the subprocess runner before it returns")

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", fake_run_with_cancellation)

    with pytest.raises(WorkerCancellationRequested, match="was canceled"):
        runDeepResearch(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            input_store=input_store,
            artifact_store=artifact_store,
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


def test_run_command_with_cancellation_terminates_process_before_raising(monkeypatch) -> None:
    state = {"terminated": 0, "killed": 0, "communicated": 0}

    class FakeProcess:
        def __init__(self) -> None:
            self.returncode = None

        def poll(self):
            return self.returncode

        def communicate(self):
            state["communicated"] += 1
            return ("", "")

        def terminate(self) -> None:
            state["terminated"] += 1
            self.returncode = -15

        def wait(self, timeout: float | None = None) -> int:
            return -15

        def kill(self) -> None:
            state["killed"] += 1
            self.returncode = -9

    monkeypatch.setattr(worker_module.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())

    def raise_cancellation() -> None:
        raise WorkerCancellationRequested("job job-1 was canceled")

    with pytest.raises(WorkerCancellationRequested, match="was canceled"):
        worker_module._run_command_with_cancellation(
            ["claude", "-p"],
            check_cancellation=raise_cancellation,
            poll_interval_seconds=0.0,
        )

    assert state["terminated"] == 1
    assert state["killed"] == 0


def _phase_from_prompt(prompt: str):
    return next(phase for phase in PHASES if phase.phase_id in prompt)


def _write_phase_artifacts(research_dir: Path, phase, *, skip_artifact: str | None = None) -> None:
    research_dir.mkdir(parents=True, exist_ok=True)
    for options in phase.required_artifacts:
        artifact_name = options[0]
        if artifact_name == skip_artifact:
            continue
        artifact_path = research_dir / artifact_name
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")


def _build_execution(*ordered_inputs: OrderedWorkerInput) -> ClaimedJobExecution:
    return ClaimedJobExecution(
        execution_id="exec-1",
        job_id="job-1",
        root_job_id="root-1",
        parent_job_id="job-parent-1",
        retry_of_job_id=None,
        job_type="deep_research",
        version=1,
        ordered_inputs=ordered_inputs,
        params={},
    )


def _required_marker() -> str:
    return "[WorkerDeepResearch][runDeepResearch][BLOCK_EXECUTE_DEEP_RESEARCH_PIPELINE]"
