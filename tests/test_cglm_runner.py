from pathlib import Path

import telegram_transcriber_bot.cglm_runner as cglm_runner
from telegram_transcriber_bot.cglm_runner import build_cglm_command, generate_report


def test_build_cglm_command_uses_print_mode_prompt_separator_and_file_access(tmp_path: Path, monkeypatch) -> None:
    transcript_path = tmp_path / "transcript.md"
    transcript_path.write_text("# Transcript", encoding="utf-8")
    monkeypatch.setenv("CGLM_BIN", "/custom/bin/cglm")

    command = build_cglm_command(
        transcript_path=transcript_path,
        report_prompt_suffix="Be concise.",
    )

    assert command[0] == "/custom/bin/cglm"
    assert "-p" in command
    assert "--add-dir" in command
    assert str(transcript_path.parent.resolve()) in command
    assert "--" in command
    assert any(str(transcript_path.resolve()) in part for part in command)
    assert any("Be concise." in part for part in command)
    assert any("Return only the final report markdown." in part for part in command)
    assert any("Do not write phrases like" in part for part in command)
    assert any("## Исследовательские вопросы" in part for part in command)
    assert any("## Краткие ответы на исследовательские вопросы" in part for part in command)
    assert any("## Линия рассуждения и развитие обсуждения" in part for part in command)


def test_generate_report_uses_runner_and_writes_markdown(tmp_path: Path, monkeypatch) -> None:
    transcript_path = tmp_path / "transcript.md"
    transcript_path.write_text("# Transcript", encoding="utf-8")
    report_path = tmp_path / "report.md"
    monkeypatch.setenv("CGLM_BIN", "/custom/bin/cglm")

    def fake_runner(command: list[str]) -> str:
        assert command[0] == "/custom/bin/cglm"
        assert "--" in command
        return "Вот исследовательский отчёт на основе транскрипта:\n\n---\n\n## Ключевые вопросы\n\n- Theme A"

    result = generate_report(
        transcript_path=transcript_path,
        report_path=report_path,
        command_runner=fake_runner,
    )

    assert result == report_path
    report_text = report_path.read_text(encoding="utf-8")
    assert report_text.startswith("# Исследовательский отчёт")
    assert "Вот исследовательский отчёт" not in report_text
    assert "---" not in report_text


def test_resolve_cglm_executable_falls_back_to_home_bin(tmp_path: Path, monkeypatch) -> None:
    fallback = tmp_path / "bin" / "cglm"
    fallback.parent.mkdir(parents=True, exist_ok=True)
    fallback.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.delenv("CGLM_BIN", raising=False)
    monkeypatch.setattr(cglm_runner.shutil, "which", lambda _: None)
    monkeypatch.setattr(cglm_runner.Path, "home", lambda: tmp_path)

    assert cglm_runner._resolve_cglm_executable() == str(fallback)


def test_build_command_env_adds_homebrew_and_home_bin(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr(cglm_runner.Path, "home", lambda: tmp_path)

    env = cglm_runner._build_command_env()

    assert env["PATH"].startswith(f"{tmp_path}/bin:/opt/homebrew/bin:/usr/local/bin:")
    assert env["PATH"].endswith("/usr/bin:/bin:/usr/sbin:/sbin")
