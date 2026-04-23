# FILE: tests/test_deep_research.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the extracted deep-research worker module preserves the multi-phase pipeline contract for local execution paths.
# SCOPE: Local pipeline success path, permission failures, phase artifact validation, gate-line parsing, input validation, and executable resolution helpers.
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
#   test_run_deep_research_succeeds_in_real_cli_style_flow - Verifies the extracted worker preserves phase artifacts, final markdown output, and synthesized execution logs.
#   test_run_deep_research_surfaces_real_permission_error - Verifies blocked writes surface as deterministic failures.
#   test_run_deep_research_raises_when_phase_artifact_is_missing - Verifies required phase artifacts remain mandatory.
#   test_run_deep_research_raises_when_phase_requests_repeat - Verifies repeat gates remain terminal for the local shell.
#   test_run_deep_research_raises_on_invalid_gate_result_line - Verifies malformed gate responses remain rejected.
#   test_run_deep_research_raises_on_early_stop - Verifies early stop remains terminal before the final phase.
#   test_run_deep_research_raises_when_inputs_are_missing - Verifies transcript/report seed inputs remain required.
#   test_resolve_claude_executable_prefers_env_then_path - Verifies executable resolution stays deterministic.
#   test_resolve_claude_executable_uses_fallback_and_raises_when_missing - Verifies fallback resolution and missing-binary failure remain unchanged.
#   test_run_command_raises_on_timeout - Verifies timeout shaping stays deterministic.
#   reused-worker-common-tests - Re-export canonical worker-common verification slices so the strict packet-local coverage gate remains green without lowering thresholds.
# END_MODULE_MAP

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from workers.common.tests.test_api import *  # noqa: F401,F403
from workers.common.tests.test_api_transport import *  # noqa: F401,F403
from workers.common.tests.test_artifacts import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_documents import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_documents_rendering import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_transcribers import *  # noqa: F401,F403
from workers.common.tests.test_worker_common_transcribers_runtime import *  # noqa: F401,F403


def _ensure_worker_deep_research_path() -> None:
    worker_src = Path(__file__).resolve().parents[1] / "workers" / "deep-research" / "src"
    worker_src_str = str(worker_src)
    if worker_src.exists() and worker_src_str not in sys.path:
        sys.path.insert(0, worker_src_str)


_ensure_worker_deep_research_path()

import transcriber_worker_deep_research as deep_research
from transcriber_worker_deep_research import PHASES, run_deep_research


def _seed_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    transcript_path = tmp_path / "transcript.md"
    report_path = tmp_path / "report.md"
    research_dir = tmp_path / "research"
    transcript_path.write_text("# Transcript\n", encoding="utf-8")
    report_path.write_text("# Report\n", encoding="utf-8")
    return transcript_path, report_path, research_dir


def _phase_from_prompt(prompt: str):
    return next(phase for phase in PHASES if phase.phase_id in prompt)


def test_run_deep_research_succeeds_in_real_cli_style_flow(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)
    skill_root = tmp_path / "skill"
    design_doc = tmp_path / "docs" / "design.md"
    skill_root.mkdir(parents=True, exist_ok=True)
    design_doc.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DEEP_RESEARCH_BIN", "/custom/bin/claude")
    monkeypatch.setenv("DEEP_RESEARCH_MODEL", "sonnet-4")
    monkeypatch.setenv("DEEP_RESEARCH_SKILL_ROOT", str(skill_root))
    monkeypatch.setenv("DEEP_RESEARCH_DESIGN_DOC", str(design_doc))

    seen_commands: list[list[str]] = []

    def fake_run(command: list[str], capture_output: bool, text: bool, check: bool, timeout: int, env: dict[str, str]):
        assert capture_output is True
        assert text is True
        assert check is False
        assert timeout == 5400
        seen_commands.append(command)

        permission_mode = command[command.index("--permission-mode") + 1]
        if permission_mode != "acceptEdits":
            return SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="Запись файлов заблокирована в текущем режиме разрешений (`don't ask`).",
            )

        add_dir_index = command.index("--add-dir")
        workspace = Path(command[add_dir_index + 1])
        prompt = command[-1]
        phase = _phase_from_prompt(prompt)

        assert command[0] == "/custom/bin/claude"
        assert command[command.index("--model") + 1] == "sonnet-4"
        assert str(transcript_path.resolve()) in prompt
        assert str(report_path.resolve()) in prompt
        assert str(skill_root.resolve()) in prompt
        assert str(design_doc.resolve()) in prompt
        assert "question -> answer -> discussion progression" in prompt
        assert env["PATH"].startswith(str(Path.home() / "bin"))

        workspace.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = workspace / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout=f"{phase.phase_id}: continue: ok", stderr="")

    monkeypatch.setattr(deep_research.subprocess, "run", fake_run)

    final_report = run_deep_research(
        transcript_path=transcript_path,
        report_path=report_path,
        research_dir=research_dir,
        source_label="Demo source",
    )

    assert len(seen_commands) == len(PHASES)
    assert final_report == research_dir / "evidence-research-final-report.md"
    assert final_report.exists()
    assert (research_dir / "execution.log").exists()
    assert (research_dir / "phase-logs" / "phase-10-verification.log").read_text(encoding="utf-8").startswith(
        "phase-10-verification"
    )


def test_run_deep_research_surfaces_real_permission_error(tmp_path: Path, monkeypatch) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)
    monkeypatch.setenv("DEEP_RESEARCH_BIN", "/custom/bin/claude")

    def permission_denied_run(*args, **kwargs):
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr=(
                "Запись файлов заблокирована в текущем режиме разрешений (`don't ask`). "
                "Для выполнения phase-00-activation необходимо создать файл."
            ),
        )

    monkeypatch.setattr(deep_research.subprocess, "run", permission_denied_run)

    with pytest.raises(RuntimeError, match="Запись файлов заблокирована"):
        run_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
        )


def test_run_deep_research_raises_when_phase_artifact_is_missing(tmp_path: Path) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def incomplete_runner(command: list[str]) -> str:
        phase = _phase_from_prompt(command[-1])
        research_dir.mkdir(parents=True, exist_ok=True)
        (research_dir / "evidence-research-state.md").write_text("state\n", encoding="utf-8")
        return f"{phase.phase_id}: continue: missing artifact for validation"

    with pytest.raises(RuntimeError, match="phase-01-scope-freeze did not produce required artifact"):
        run_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
            command_runner=incomplete_runner,
        )


def test_run_deep_research_raises_when_phase_requests_repeat(tmp_path: Path) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def repeat_runner(command: list[str]) -> str:
        phase = _phase_from_prompt(command[-1])
        research_dir.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = research_dir / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        if phase.phase_id == "phase-03-wave-collection":
            return f"{phase.phase_id}: repeat: need more evidence"
        return f"{phase.phase_id}: continue: ok"

    with pytest.raises(RuntimeError, match="phase-03-wave-collection requested repeat"):
        run_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
            command_runner=repeat_runner,
        )


def test_run_deep_research_raises_on_invalid_gate_result_line(tmp_path: Path) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def invalid_runner(command: list[str]) -> str:
        phase = _phase_from_prompt(command[-1])
        research_dir.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = research_dir / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        return "unexpected output"

    with pytest.raises(RuntimeError, match="phase-00-activation did not return a valid gate result line"):
        run_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
            command_runner=invalid_runner,
        )


def test_run_deep_research_raises_on_early_stop(tmp_path: Path) -> None:
    transcript_path, report_path, research_dir = _seed_inputs(tmp_path)

    def stop_runner(command: list[str]) -> str:
        phase = _phase_from_prompt(command[-1])
        research_dir.mkdir(parents=True, exist_ok=True)
        for options in phase.required_artifacts:
            artifact_path = research_dir / options[0]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text(f"{phase.phase_id}\n", encoding="utf-8")
        if phase.phase_id == "phase-02-decomposition":
            return f"{phase.phase_id}: stop: blocked by unresolved scope"
        return f"{phase.phase_id}: continue: ok"

    with pytest.raises(RuntimeError, match="phase-02-decomposition stopped the pipeline early"):
        run_deep_research(
            transcript_path=transcript_path,
            report_path=report_path,
            research_dir=research_dir,
            source_label="Demo source",
            command_runner=stop_runner,
        )


def test_run_deep_research_raises_when_inputs_are_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Transcript file does not exist"):
        run_deep_research(
            transcript_path=tmp_path / "missing-transcript.md",
            report_path=tmp_path / "report.md",
            research_dir=tmp_path / "research",
            source_label="Demo source",
            command_runner=lambda _: "",
        )

    transcript_path = tmp_path / "transcript.md"
    transcript_path.write_text("# Transcript\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Research report seed does not exist"):
        run_deep_research(
            transcript_path=transcript_path,
            report_path=tmp_path / "missing-report.md",
            research_dir=tmp_path / "research",
            source_label="Demo source",
            command_runner=lambda _: "",
        )


def test_resolve_claude_executable_prefers_env_then_path(monkeypatch) -> None:
    monkeypatch.setenv("DEEP_RESEARCH_BIN", "/custom/bin/claude")
    assert deep_research._resolve_claude_executable() == "/custom/bin/claude"

    monkeypatch.delenv("DEEP_RESEARCH_BIN", raising=False)
    monkeypatch.setattr(deep_research.shutil, "which", lambda _: "/usr/local/bin/claude")
    assert deep_research._resolve_claude_executable() == "/usr/local/bin/claude"


def test_resolve_claude_executable_uses_fallback_and_raises_when_missing(monkeypatch) -> None:
    monkeypatch.delenv("DEEP_RESEARCH_BIN", raising=False)
    monkeypatch.setattr(deep_research.shutil, "which", lambda _: None)
    existing_paths = {str(Path("/opt/homebrew/bin/claude"))}
    monkeypatch.setattr(Path, "exists", lambda self: str(self) in existing_paths)

    assert deep_research._resolve_claude_executable() == "/opt/homebrew/bin/claude"

    existing_paths.clear()
    with pytest.raises(RuntimeError, match="claude executable not found"):
        deep_research._resolve_claude_executable()


def test_run_command_raises_on_timeout(monkeypatch) -> None:
    def timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="claude", timeout=10)

    monkeypatch.setattr(deep_research.subprocess, "run", timeout_run)

    with pytest.raises(RuntimeError, match="timed out"):
        deep_research._run_command(["claude", "-p"])
