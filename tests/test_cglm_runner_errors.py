from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_transcriber_bot.cglm_runner as cglm_runner
from telegram_transcriber_bot.cglm_runner import _run_command, generate_report


def test_generate_report_raises_when_transcript_is_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("REPORT_HARNESS_BIN", "/custom/bin/report-harness")
    with pytest.raises(FileNotFoundError):
        generate_report(tmp_path / "missing.md", tmp_path / "report.md")


def test_generate_report_raises_on_empty_output(tmp_path: Path, monkeypatch) -> None:
    transcript_path = tmp_path / "transcript.md"
    transcript_path.write_text("# Transcript", encoding="utf-8")
    monkeypatch.setenv("REPORT_HARNESS_BIN", "/custom/bin/report-harness")

    with pytest.raises(RuntimeError, match="empty report"):
        generate_report(transcript_path, tmp_path / "report.md", command_runner=lambda _: "   ")


def test_resolve_cglm_executable_raises_when_binary_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("REPORT_HARNESS_BIN", raising=False)
    monkeypatch.delenv("CGLM_BIN", raising=False)
    monkeypatch.setattr(cglm_runner.shutil, "which", lambda _: None)
    monkeypatch.setattr(cglm_runner.Path, "home", lambda: Path("/definitely-missing-home"))

    with pytest.raises(RuntimeError, match="report harness executable not found"):
        cglm_runner._resolve_cglm_executable()


def test_run_command_raises_on_non_zero_exit(monkeypatch) -> None:
    monkeypatch.setattr(
        cglm_runner.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=2, stderr="boom", stdout=""),
    )

    with pytest.raises(RuntimeError, match="exit code 2"):
        _run_command(["cglm", "-p"])


def test_run_command_raises_on_timeout(monkeypatch) -> None:
    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="cglm", timeout=10)

    monkeypatch.setattr(cglm_runner.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="timed out"):
        _run_command(["cglm", "-p"])


def test_run_command_raises_when_binary_path_is_missing(monkeypatch) -> None:
    def fake_run(*args, **kwargs):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(cglm_runner.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="executable not found"):
        _run_command(["/custom/bin/cglm", "-p"])


def test_report_harness_timeout_is_configurable(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_HARNESS_TIMEOUT_SECONDS", "123")

    assert cglm_runner._resolve_report_harness_timeout_seconds() == 123
