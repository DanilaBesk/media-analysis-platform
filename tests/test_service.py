from pathlib import Path

import pytest

import telegram_transcriber_bot.service as service_module

from telegram_transcriber_bot.domain import ProcessedJob, SourceCandidate, TranscriptArtifacts, TranscriptResult, TranscriptSegment
from telegram_transcriber_bot.service import ProcessingService


class FakeTranscriber:
    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        assert source.kind == "youtube_url"
        return TranscriptResult(
            title="Demo title",
            source_label=source.display_name,
            segments=[TranscriptSegment(start_seconds=0.0, end_seconds=4.0, text="Segment text", speaker="Speaker 1")],
            language="ru",
            raw_text="Segment text",
        )


def test_processing_service_creates_transcript_artifacts(tmp_path: Path) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
    )
    source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/dQw4w9WgXcQ",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    job = service.process_source(source)

    assert job.source.display_name == "YouTube: demo"
    assert job.transcript.markdown_path.exists()
    assert job.transcript.docx_path.exists()
    assert job.transcript.text_path.exists()
    assert "Segment text" in job.transcript.markdown_path.read_text(encoding="utf-8")
    assert job.transcript.text_path.read_text(encoding="utf-8") == "Segment text\n"


def test_ensure_report_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
    )
    source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/dQw4w9WgXcQ",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    job = service.process_source(source)

    calls = 0

    def fake_generate_report(transcript_path: Path, report_path: Path, report_prompt_suffix: str = "") -> Path:
        nonlocal calls
        calls += 1
        report_path.write_text("# Report\n\n- Theme A\n", encoding="utf-8")
        return report_path

    monkeypatch.setattr(service_module, "generate_report", fake_generate_report)

    first = service.ensure_report(job.job_id)
    second = service.ensure_report(job.job_id)

    assert calls == 1
    assert first.report is not None
    assert second.report is not None
    assert second.report.docx_path.exists()


def test_ensure_deep_research_runs_once_and_reuses_existing_artifact(tmp_path: Path, monkeypatch) -> None:
    calls = 0

    def fake_deep_research_runner(transcript_path: Path, report_path: Path, research_dir: Path, source_label: str) -> Path:
        nonlocal calls
        calls += 1
        assert transcript_path.name == "transcript.md"
        assert report_path.name == "report.md"
        assert source_label == "YouTube: demo"
        research_dir.mkdir(parents=True, exist_ok=True)
        final_report = research_dir / "evidence-research-final-report.md"
        final_report.write_text("# Deep Research\n", encoding="utf-8")
        return final_report

    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
        deep_research_runner=fake_deep_research_runner,
    )
    source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/dQw4w9WgXcQ",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    job = service.process_source(source)

    def fake_generate_report(transcript_path: Path, report_path: Path, report_prompt_suffix: str = "") -> Path:
        report_path.write_text("# Report\n", encoding="utf-8")
        return report_path

    monkeypatch.setattr(service_module, "generate_report", fake_generate_report)
    service.ensure_report(job.job_id)

    first = service.ensure_deep_research(job.job_id)
    second = service.ensure_deep_research(job.job_id)

    assert calls == 1
    assert first == second
    assert first.exists()


def test_ensure_deep_research_requires_existing_report(tmp_path: Path) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
        deep_research_runner=lambda *_: tmp_path / "unused.md",
    )
    source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/dQw4w9WgXcQ",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    job = service.process_source(source)

    with pytest.raises(RuntimeError, match="Research report must exist before deep research starts"):
        service.ensure_deep_research(job.job_id)


def test_processing_service_copies_local_source_and_loads_saved_job(tmp_path: Path) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
    )
    source_file = tmp_path / "incoming.ogg"
    source_file.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="src-2",
        kind="youtube_url",
        display_name="YouTube: copied",
        url="https://youtu.be/dQw4w9WgXcQ",
        telegram_file_id=None,
        mime_type="audio/ogg",
        file_name="incoming.ogg",
        local_path=source_file,
    )

    job = service.process_source(source)
    loaded = service.load_job(job.job_id)

    assert job.source.local_path is not None
    assert job.source.local_path != source_file
    assert job.source.local_path.read_bytes() == b"audio"
    assert loaded.source.local_path == job.source.local_path


def test_processing_service_reuses_completed_job_for_same_telegram_media(tmp_path: Path) -> None:
    class TelegramAudioTranscriber:
        def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
            assert source.kind == "telegram_audio"
            return TranscriptResult(
                title="Voice note",
                source_label=source.display_name,
                segments=[TranscriptSegment(start_seconds=0.0, end_seconds=1.0, text="Voice segment")],
                language="ru",
                raw_text="Voice segment",
            )

    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=TelegramAudioTranscriber(),
    )
    original = SourceCandidate(
        source_id="src-original",
        kind="telegram_audio",
        display_name="Audio: first.ogg",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="first.ogg",
        file_unique_id="uniq-media-1",
    )
    first_job = service.process_source(original)

    class RaisingTranscriber:
        def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
            raise AssertionError("transcribe should not be called for duplicate media")

    service.transcriber = RaisingTranscriber()
    duplicate = SourceCandidate(
        source_id="src-duplicate",
        kind="telegram_audio",
        display_name="Audio: second.ogg",
        url=None,
        telegram_file_id="file-2",
        mime_type="audio/ogg",
        file_name="second.ogg",
        file_unique_id="uniq-media-1",
    )

    reused_job = service.process_source(duplicate)

    assert reused_job.job_id == first_job.job_id
    assert reused_job.transcript.markdown_path == first_job.transcript.markdown_path
    assert reused_job.transcript.text_path == first_job.transcript.text_path


def test_processing_service_loads_job_without_report(tmp_path: Path) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
    )
    source = SourceCandidate(
        source_id="src-3",
        kind="youtube_url",
        display_name="YouTube: no-report",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    job = service.process_source(source)
    payload = job.metadata_path.read_text(encoding="utf-8").replace('"report": null', '"report": false')
    job.metadata_path.write_text(payload, encoding="utf-8")

    loaded = service.load_job(job.job_id)

    assert loaded.report is None


def test_materialize_local_source_skips_copy_when_file_already_in_workspace(tmp_path: Path) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
    )
    workspace_dir = service.jobs_dir / "job-1"
    workspace_dir.mkdir(parents=True, exist_ok=True)
    existing = workspace_dir / "source.ogg"
    existing.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="src-4",
        kind="youtube_url",
        display_name="YouTube: local",
        url="https://youtu.be/demo",
        telegram_file_id=None,
        mime_type="audio/ogg",
        file_name="source.ogg",
        local_path=existing,
    )

    materialized = service._materialize_local_source(source, workspace_dir)

    assert materialized.local_path == existing
    assert existing.read_bytes() == b"audio"


def test_save_job_requires_metadata_path(tmp_path: Path) -> None:
    service = ProcessingService(
        storage_dir=tmp_path,
        transcriber=FakeTranscriber(),
    )
    job = ProcessedJob(
        job_id="job-1",
        source=SourceCandidate(
            source_id="src-5",
            kind="youtube_url",
            display_name="YouTube: demo",
            url="https://youtu.be/demo",
            telegram_file_id=None,
            mime_type=None,
            file_name=None,
        ),
        workspace_dir=tmp_path / "job-1",
        transcript=TranscriptArtifacts(
            markdown_path=tmp_path / "t.md",
            docx_path=tmp_path / "t.docx",
            text_path=tmp_path / "t.txt",
        ),
        metadata_path=None,
    )

    with pytest.raises(ValueError, match="metadata_path must be set before saving a job"):
        service._save_job(job)
