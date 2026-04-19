from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Callable

from telegram_transcriber_bot.documents import normalize_report_markdown

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

CommandRunner = Callable[[list[str]], str]


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
            timeout=900,
            env=_build_command_env(),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("cglm timed out while generating a report") from exc
    if completed.returncode != 0:
        raise RuntimeError(f"cglm failed with exit code {completed.returncode}: {completed.stderr.strip()}")
    return completed.stdout
