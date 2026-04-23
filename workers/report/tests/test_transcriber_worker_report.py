# FILE: workers/report/tests/test_transcriber_worker_report.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the dedicated report worker claims jobs through the shared control plane, preserves markdown plus DOCX outputs, and keeps report harness subprocess failures and cancellation deterministic.
# SCOPE: Success finalization ordering, transcript artifact materialization, local report extraction reuse, required-artifact assertions, cancellation checkpoints, and subprocess termination handling.
# DEPENDS: M-WORKER-REPORT, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-REPORT, V-M-WORKER-REPORT
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local verification for the report worker shell and extracted local report pipeline.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   RecordingApiClient - Captures claim/progress/artifact/finalize/cancel calls without redefining DTO payloads.
#   FakeInputStore - Writes claimed transcript artifact bytes into the worker workspace.
#   InMemoryArtifactStore - Captures uploaded artifact bytes.
#   test_run_report_claims_and_finalizes_after_required_artifacts_exist - Verifies shared claim/finalize reuse, transcript artifact materialization, and preserved report artifact outputs.
#   test_process_local_report_reuses_extracted_local_pipeline - Verifies the extracted local helper preserves markdown plus DOCX generation for the legacy shell.
#   test_run_report_fails_when_required_artifacts_are_missing - Verifies success is impossible before both required report artifacts exist.
#   test_run_report_surfaces_deterministic_report_harness_failures - Verifies missing binary, timeout, and non-zero exit surface as deterministic worker failures.
#   test_run_report_checks_cancellation_inside_worker_loop - Verifies authoritative cancellation finalizes `canceled` without artifact registration.
#   test_run_command_with_cancellation_terminates_process_before_raising - Verifies subprocess ownership stays inside the worker loop when cancellation is observed.
# END_MODULE_MAP

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from transcriber_workers_common.api import CancelCheckResult, ClaimedJobExecution, OrderedWorkerInput
import transcriber_worker_report as worker_module
from transcriber_worker_report import WorkerCancellationRequested, process_local_report, runReport


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


def test_run_report_claims_and_finalizes_after_required_artifacts_exist(
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
            object_key="artifacts/job-0/transcript/segmented/transcript.md",
        ),
        report_prompt_suffix="Preserve nuance.",
    )
    api_client = RecordingApiClient(execution)
    input_store = FakeInputStore(
        {
            "artifacts/job-0/transcript/segmented/transcript.md": b"# Transcript\n\n## Notes\n\n- Theme A\n",
        }
    )
    artifact_store = InMemoryArtifactStore()
    captured_commands: list[list[str]] = []

    def fake_run_with_cancellation(command: list[str], *, check_cancellation, **_kwargs) -> str:
        captured_commands.append(command)
        check_cancellation()
        return "## Key Points\n\n- Theme A\n\nConclusion"

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", fake_run_with_cancellation)

    result = runReport(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        input_store=input_store,
        artifact_store=artifact_store,
    )

    assert api_client.calls[0] == (
        "claim_job",
        {"job_id": execution.job_id, "worker_kind": "report", "task_type": "report.run"},
    )
    assert [call[1]["progress_stage"] for call in api_client.calls if call[0] == "publish_progress"] == [
        "materializing_inputs",
        "generating_report",
        "persisting_artifacts",
    ]
    assert input_store.calls == [
        (
            "artifacts/job-0/transcript/segmented/transcript.md",
            str(result.transcript_path),
        )
    ]
    assert result.transcript_path.read_text(encoding="utf-8") == "# Transcript\n\n## Notes\n\n- Theme A\n"
    assert captured_commands
    assert any(str(result.transcript_path) in part for part in captured_commands[0])
    assert any("Preserve nuance." in part for part in captured_commands[0])

    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "report_markdown",
        "report_docx",
    ]
    assert result.report.markdown_path.exists()
    assert result.report.docx_path.exists()
    assert len(artifact_store.calls) == 2
    finalize_call = api_client.calls[-1]
    assert finalize_call[0] == "finalize_job"
    assert finalize_call[1]["outcome"] == "succeeded"
    assert api_client.calls.index(register_call) < api_client.calls.index(finalize_call)
    assert _required_marker() in caplog.text


def test_process_local_report_reuses_extracted_local_pipeline(tmp_path: Path) -> None:
    transcript_path = tmp_path / "transcript.md"
    transcript_path.write_text("# Transcript\n\n- Theme A\n", encoding="utf-8")

    artifacts = process_local_report(
        transcript_path,
        workspace_dir=tmp_path / "job-1",
        report_prompt_suffix="Be concise.",
        command_runner=lambda _: "## Key Points\n\n- Theme A",
    )

    assert artifacts.markdown_path.read_text(encoding="utf-8").startswith("# Исследовательский отчёт")
    assert artifacts.docx_path.exists()


def test_run_report_fails_when_required_artifacts_are_missing(tmp_path: Path, monkeypatch) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="transcript-1",
            source_kind="uploaded_file",
            display_name="Transcript: transcript.md",
            original_filename="transcript.md",
            object_key="artifacts/job-0/transcript/segmented/transcript.md",
        )
    )
    api_client = RecordingApiClient(execution)
    input_store = FakeInputStore(
        {
            "artifacts/job-0/transcript/segmented/transcript.md": b"# Transcript\n",
        }
    )
    artifact_store = InMemoryArtifactStore()

    monkeypatch.setattr(
        worker_module,
        "_run_command_with_cancellation",
        lambda command, *, check_cancellation, **_kwargs: "## Key Points\n\n- Theme A",
    )
    monkeypatch.setattr(worker_module, "write_report_docx", lambda output_path, markdown_content: None)

    with pytest.raises(RuntimeError, match="required report artifact is missing"):
        runReport(
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
            "progress_message": "Report generation failed",
            "error_code": "report_failed",
            "error_message": f"required report artifact is missing: {tmp_path / execution.job_id / 'report.docx'}",
        },
    )


@pytest.mark.parametrize(
    ("failure_message", "match_text"),
    [
        ("report harness executable not found: /custom/bin/report-harness", "executable not found"),
        ("report harness timed out while generating a report", "timed out"),
        ("report harness failed with exit code 7: boom", "exit code 7"),
    ],
)
def test_run_report_surfaces_deterministic_report_harness_failures(
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
            object_key="artifacts/job-0/transcript/segmented/transcript.md",
        )
    )
    api_client = RecordingApiClient(execution)
    input_store = FakeInputStore(
        {
            "artifacts/job-0/transcript/segmented/transcript.md": b"# Transcript\n",
        }
    )
    artifact_store = InMemoryArtifactStore()

    def fake_run_with_cancellation(command: list[str], *, check_cancellation, **_kwargs) -> str:
        check_cancellation()
        raise RuntimeError(failure_message)

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", fake_run_with_cancellation)

    with pytest.raises(RuntimeError, match=match_text):
        runReport(
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
            "progress_message": "Report generation failed",
            "error_code": "report_failed",
            "error_message": failure_message,
        },
    )


def test_run_report_checks_cancellation_inside_worker_loop(tmp_path: Path, monkeypatch) -> None:
    execution = _build_execution(
        OrderedWorkerInput(
            position=0,
            source_id="transcript-1",
            source_kind="uploaded_file",
            display_name="Transcript: transcript.md",
            original_filename="transcript.md",
            object_key="artifacts/job-0/transcript/segmented/transcript.md",
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
    input_store = FakeInputStore(
        {
            "artifacts/job-0/transcript/segmented/transcript.md": b"# Transcript\n",
        }
    )
    artifact_store = InMemoryArtifactStore()

    def fake_run_with_cancellation(command: list[str], *, check_cancellation, **_kwargs) -> str:
        check_cancellation()
        pytest.fail("cancellation should interrupt the subprocess runner before it returns")

    monkeypatch.setattr(worker_module, "_run_command_with_cancellation", fake_run_with_cancellation)

    with pytest.raises(WorkerCancellationRequested, match="was canceled"):
        runReport(
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
            ["cglm", "-p"],
            check_cancellation=raise_cancellation,
            poll_interval_seconds=0.0,
        )

    assert state["terminated"] == 1
    assert state["killed"] == 0


def _build_execution(
    *ordered_inputs: OrderedWorkerInput,
    report_prompt_suffix: str = "",
) -> ClaimedJobExecution:
    return ClaimedJobExecution(
        execution_id="exec-1",
        job_id="job-1",
        root_job_id="root-1",
        parent_job_id="job-parent-1",
        retry_of_job_id=None,
        job_type="report",
        version=1,
        ordered_inputs=ordered_inputs,
        params={"report_prompt_suffix": report_prompt_suffix},
    )


def _required_marker() -> str:
    return "[WorkerReport][runReport][BLOCK_EXECUTE_REPORT_PIPELINE]"
