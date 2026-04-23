# FILE: workers/common/tests/test_worker_common_documents.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the worker-common document helpers preserve the current transcript and report rendering behavior.
# SCOPE: Transcript markdown rendering, transcript DOCX rendering, and report markdown normalization.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local document helper regression coverage under worker-common.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   test_build_transcript_markdown_includes_timestamps_and_speakers - Verifies canonical transcript markdown output.
#   test_write_transcript_docx_creates_document - Verifies transcript DOCX output.
#   test_normalize_report_markdown_removes_intro_and_inserts_title - Verifies report markdown normalization.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

from docx import Document

from transcriber_workers_common.documents import (
    build_transcript_markdown,
    normalize_report_markdown,
    write_transcript_docx,
)
from telegram_transcriber_bot.domain import TranscriptResult, TranscriptSegment


def test_build_transcript_markdown_includes_timestamps_and_speakers() -> None:
    transcript = TranscriptResult(
        title="Demo call",
        source_label="YouTube: demo",
        segments=[
            TranscriptSegment(start_seconds=0.0, end_seconds=5.0, text="Intro", speaker="Speaker 1"),
            TranscriptSegment(start_seconds=5.0, end_seconds=12.0, text="Plan", speaker=None),
        ],
        language="ru",
        raw_text="Intro\nPlan",
    )

    markdown = build_transcript_markdown(transcript)

    assert "# Demo call" in markdown
    assert "- Источник: YouTube: demo" in markdown
    assert "[00:00 - 00:05] Speaker 1: Intro" in markdown
    assert "[00:05 - 00:12] Фрагмент: Plan" in markdown


def test_write_transcript_docx_creates_document(tmp_path: Path) -> None:
    transcript = TranscriptResult(
        title="Weekly Sync",
        source_label="Audio: sync.mp3",
        segments=[TranscriptSegment(start_seconds=0.0, end_seconds=3.5, text="Hello team", speaker="Speaker 1")],
        language="ru",
        raw_text="Hello team",
    )

    output_path = tmp_path / "transcript.docx"
    write_transcript_docx(output_path, transcript)

    document = Document(output_path)
    text = "\n".join(paragraph.text for paragraph in document.paragraphs)
    assert "Weekly Sync" in text
    assert "Источник: sync.mp3" in text
    assert "Speaker 1" in text
    assert "Hello team" in text


def test_normalize_report_markdown_removes_intro_and_inserts_title() -> None:
    markdown = normalize_report_markdown(
        "Вот исследовательский отчёт на основе транскрипта:\n\n---\n\n## Ключевые вопросы\n\n- Theme A\n"
    )

    assert markdown.startswith("# Исследовательский отчёт\n\n## Ключевые вопросы")
    assert "Вот исследовательский отчёт" not in markdown
    assert "---" not in markdown
