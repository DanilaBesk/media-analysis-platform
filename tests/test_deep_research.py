# FILE: tests/test_deep_research.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the extracted deep-research worker module preserves the multi-phase WorkerHarness pipeline contract for local execution paths.
# SCOPE: Local pipeline success path, harness failures, cancellation, phase artifact validation, gate-line parsing, input validation, and canonical WorkerHarness request data.
# DEPENDS: M-WORKER-DEEP-RESEARCH, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-DEEP-RESEARCH, V-M-WORKER-DEEP-RESEARCH
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Updated the deep-research tests to target the extracted worker module directly.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   _ensure_worker_deep_research_path - Adds the dedicated deep-research worker source root for direct packet-local imports.
#   test_run_deep_research_succeeds_through_worker_harness - Verifies the default local path emits WorkerHarness requests and materializes fixture artifacts.
#   test_run_deep_research_surfaces_worker_harness_permission_error - Verifies blocked writes surface as deterministic harness failures.
#   test_run_deep_research_surfaces_worker_harness_cancellation - Verifies canceled harness results stop the local pipeline.
#   test_run_deep_research_raises_when_phase_artifact_is_missing - Verifies required phase artifacts remain mandatory.
#   test_run_deep_research_raises_when_phase_requests_repeat - Verifies repeat gates remain terminal for the canonical local pipeline.
#   test_run_deep_research_raises_on_invalid_gate_result_line - Verifies malformed gate responses remain rejected.
#   test_run_deep_research_raises_on_early_stop - Verifies early stop remains terminal before the final phase.
#   test_run_deep_research_raises_when_inputs_are_missing - Verifies transcript/report seed inputs remain required.
# END_MODULE_MAP

from __future__ import annotations

import sys
from pathlib import Path

import pytest


def _ensure_worker_deep_research_path() -> None:
    worker_src = Path(__file__).resolve().parents[1] / "workers" / "deep-research" / "src"
    worker_src_str = str(worker_src)
    if worker_src.exists() and worker_src_str not in sys.path:
        sys.path.insert(0, worker_src_str)


_ensure_worker_deep_research_path()

import transcriber_worker_deep_research as deep_research
from transcriber_worker_deep_research import PHASES, process_local_deep_research
from transcriber_workers_common.harness import HarnessRequest, HarnessResult


def _seed_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    transcript_path = tmp_path / "transcript.md"
    report_path = tmp_path / "report.md"
    research_dir = tmp_path / "research"
    transcript_path.write_text("# Transcript\n", encoding="utf-8")
    report_path.write_text("# Report\n", encoding="utf-8")
    return transcript_path, report_path, research_dir


def test_run_deep_research_succeeds_through_worker_harness(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)
    captured_requests: list[HarnessRequest] = []

    def fake_run_harness(request: HarnessRequest, *, cancellation_hook=None, **_kwargs) -> HarnessResult:
        captured_requests.append(request)
        if cancellation_hook is not None:
            assert cancellation_hook() is False
        return HarnessResult(provider="fixture", output_text=f"{request.metadata['phase_id']}: continue: ok")

    monkeypatch.setattr(deep_research, "run_harness", fake_run_harness)

    artifacts = process_local_deep_research(
        transcript_path=transcript_path,
        report_path=report_path,
        research_dir=research_dir,
        source_label="Demo source",
        job_id="job-local",
        cancellation_hook=lambda: False,
    )

    assert len(captured_requests) == len(PHASES)
    assert [request.metadata["phase_id"] for request in captured_requests] == [phase.phase_id for phase in PHASES]
    assert all(request.job_id == "job-local" for request in captured_requests)
    assert all(request.task_type == "deep_research" for request in captured_requests)
    assert all(request.workspace_dir == research_dir for request in captured_requests)
    assert all(request.input_paths == (transcript_path, report_path) for request in captured_requests)
    assert all(
        request.allowed_input_dirs == (transcript_path.parent, report_path.parent, research_dir)
        for request in captured_requests
    )
    assert all(request.metadata["phase_reference"] for request in captured_requests)
    assert all(request.metadata["required_artifacts"] for request in captured_requests)
    prompt_text = "\n".join(request.prompt for request in captured_requests)
    assert str(transcript_path.resolve()) in prompt_text
    assert str(report_path.resolve()) in prompt_text
    assert str(research_dir.resolve()) in prompt_text
    assert "question -> answer -> discussion progression" in prompt_text
    assert not _contains_forbidden_host_contract(prompt_text)
    assert artifacts.final_report_path == research_dir / "evidence-research-final-report.md"
    assert artifacts.final_report_path.exists()
    assert artifacts.execution_log_path == research_dir / "execution.log"
    assert artifacts.execution_log_path.exists()
    assert not hasattr(deep_research, "build_deep_research_command")
    assert (research_dir / "phase-logs" / "phase-10-verification.log").read_text(encoding="utf-8").startswith(
        "phase-10-verification"
    )


def test_run_deep_research_surfaces_worker_harness_permission_error(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def permission_denied_harness(request: HarnessRequest, *, cancellation_hook=None, **_kwargs) -> HarnessResult:
        assert request.task_type == "deep_research"
        return HarnessResult(
            provider="codex",
            output_text="",
            stderr=(
                "Запись файлов заблокирована в текущем режиме разрешений (`don't ask`). "
                "Для выполнения phase-00-activation необходимо создать файл."
            ),
            failure_code="harness_execution_failed",
            failure_message="harness provider exited with non-zero status",
        )

    monkeypatch.setattr(deep_research, "run_harness", permission_denied_harness)

    with pytest.raises(RuntimeError, match="harness_execution_failed.*Запись файлов заблокирована"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
        )


def test_run_deep_research_surfaces_worker_harness_cancellation(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def canceled_harness(request: HarnessRequest, *, cancellation_hook=None, **_kwargs) -> HarnessResult:
        assert request.metadata["phase_id"] == "phase-00-activation"
        assert cancellation_hook is not None
        assert cancellation_hook() is True
        return HarnessResult(
            provider="fixture",
            output_text="",
            failure_code="harness_canceled",
            failure_message="harness execution canceled",
            canceled=True,
        )

    monkeypatch.setattr(deep_research, "run_harness", canceled_harness)

    with pytest.raises(deep_research.WorkerCancellationRequested, match="job job-local was canceled"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
            job_id="job-local",
            cancellation_hook=lambda: True,
        )

    assert not (research_dir / "evidence-research-final-report.md").exists()


def test_run_deep_research_raises_when_phase_artifact_is_missing(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def incomplete_runner(*, phase, research_dir, **_kwargs) -> str:
        research_dir.mkdir(parents=True, exist_ok=True)
        (research_dir / "evidence-research-state.md").write_text("state\n", encoding="utf-8")
        return f"{phase.phase_id}: continue: missing artifact for validation"

    monkeypatch.setattr(deep_research, "_run_deep_research_phase_harness", incomplete_runner)

    with pytest.raises(RuntimeError, match="phase-01-scope-freeze did not produce required artifact"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
        )


def test_run_deep_research_raises_when_phase_requests_repeat(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def repeat_runner(*, phase, research_dir, **_kwargs) -> str:
        research_dir.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = research_dir / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        if phase.phase_id == "phase-03-wave-collection":
            return f"{phase.phase_id}: repeat: need more evidence"
        return f"{phase.phase_id}: continue: ok"

    monkeypatch.setattr(deep_research, "_run_deep_research_phase_harness", repeat_runner)

    with pytest.raises(RuntimeError, match="phase-03-wave-collection requested repeat"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
        )


def test_run_deep_research_raises_on_invalid_gate_result_line(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def invalid_runner(*, phase, research_dir, **_kwargs) -> str:
        research_dir.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = research_dir / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        return "unexpected output"

    monkeypatch.setattr(deep_research, "_run_deep_research_phase_harness", invalid_runner)

    with pytest.raises(RuntimeError, match="phase-00-activation did not return a valid gate result line"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
        )


def test_run_deep_research_raises_on_early_stop(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def stop_runner(*, phase, research_dir, **_kwargs) -> str:
        research_dir.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = research_dir / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        if phase.phase_id == "phase-02-decomposition":
            return f"{phase.phase_id}: stop: blocked by unresolved scope"
        return f"{phase.phase_id}: continue: ok"

    monkeypatch.setattr(deep_research, "_run_deep_research_phase_harness", stop_runner)

    with pytest.raises(RuntimeError, match="phase-02-decomposition stopped the pipeline early"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
        )


def test_run_deep_research_raises_when_inputs_are_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Transcript file does not exist"):
        process_local_deep_research(
            transcript_path=tmp_path / "missing-transcript.md",
            report_path=tmp_path / "report.md",
            research_dir=tmp_path / "research",
            source_label="Demo source",
        )

    transcript_path = tmp_path / "transcript.md"
    transcript_path.write_text("# Transcript\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Research report seed does not exist"):
        process_local_deep_research(
            transcript_path=transcript_path,
            report_path=tmp_path / "missing-report.md",
            research_dir=tmp_path / "research",
            source_label="Demo source",
        )


def _contains_forbidden_host_contract(value: str) -> bool:
    forbidden_terms = (
        "DEEP_" + "RESEARCH_BIN",
        "CLAUDE_" + "BIN",
        "DEEP_" + "RESEARCH_TIMEOUT_SECONDS",
        "DEEP_" + "RESEARCH_MODEL",
        "DEEP_" + "RESEARCH_SKILL_ROOT",
        "DEEP_" + "RESEARCH_DESIGN_DOC",
        "/opt/" + "homebrew",
        "Path" + ".home()" + ".*" + "bin",
        "HOST_" + "HARNESS",
        "HOST_" + "WORKSPACE_ROOT",
        "host" + ".docker" + ".internal",
        "/" + "Users" + "/",
        "sub" + "process",
        "sh" + "util",
    )
    return any(term in value for term in forbidden_terms)
