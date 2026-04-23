from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Callable, Protocol
from uuid import uuid4

from telegram_transcriber_bot.cglm_runner import generate_report
from telegram_transcriber_bot.documents import write_report_docx
from telegram_transcriber_bot.domain import (
    ProcessedJob,
    ReportArtifacts,
    SourceCandidate,
    TranscriptArtifacts,
    TranscriptResult,
)


def _ensure_worker_transcription_path() -> None:
    worker_transcription_src = Path(__file__).resolve().parents[2] / "workers" / "transcription" / "src"
    if worker_transcription_src.exists():
        worker_transcription_src_str = str(worker_transcription_src)
        if worker_transcription_src_str not in sys.path:
            sys.path.insert(0, worker_transcription_src_str)


def _ensure_worker_deep_research_path() -> None:
    worker_deep_research_src = Path(__file__).resolve().parents[2] / "workers" / "deep-research" / "src"
    if worker_deep_research_src.exists():
        worker_deep_research_src_str = str(worker_deep_research_src)
        if worker_deep_research_src_str not in sys.path:
            sys.path.insert(0, worker_deep_research_src_str)


try:
    from transcriber_worker_transcription import (
        materialize_local_source as _materialize_local_source_worker,
        process_local_transcription as _process_local_transcription,
    )
except ModuleNotFoundError:
    _ensure_worker_transcription_path()
    from transcriber_worker_transcription import (
        materialize_local_source as _materialize_local_source_worker,
        process_local_transcription as _process_local_transcription,
    )


try:
    from transcriber_worker_deep_research import run_deep_research as _run_deep_research_worker
except ModuleNotFoundError:
    _ensure_worker_deep_research_path()
    from transcriber_worker_deep_research import run_deep_research as _run_deep_research_worker


class Transcriber(Protocol):
    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        """Return normalized transcript data for a source."""


DeepResearchRunner = Callable[[Path, Path, Path, str], Path]


class ProcessingService:
    def __init__(
        self,
        storage_dir: Path,
        transcriber: Transcriber,
        deep_research_runner: DeepResearchRunner = _run_deep_research_worker,
    ) -> None:
        self.storage_dir = Path(storage_dir)
        self.jobs_dir = self.storage_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.transcriber = transcriber
        self.deep_research_runner = deep_research_runner

    def process_source(self, source: SourceCandidate) -> ProcessedJob:
        existing_job = self._find_existing_job_for_source(source)
        if existing_job is not None:
            return existing_job

        job_id = uuid4().hex[:12]
        workspace_dir = self.jobs_dir / job_id
        workspace_dir.mkdir(parents=True, exist_ok=True)
        materialized_source, _transcript_result, transcript_artifacts = _process_local_transcription(
            source,
            workspace_dir=workspace_dir,
            transcriber=self.transcriber,
        )

        job = ProcessedJob(
            job_id=job_id,
            source=materialized_source,
            workspace_dir=workspace_dir,
            transcript=transcript_artifacts,
            metadata_path=workspace_dir / "job.json",
        )
        self._save_job(job)
        return job

    def load_job(self, job_id: str) -> ProcessedJob:
        metadata_path = self.jobs_dir / job_id / "job.json"
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        source_payload = payload["source"]
        report_payload = payload.get("report")
        report = None
        if report_payload:
            report = ReportArtifacts(
                markdown_path=Path(report_payload["markdown_path"]),
                docx_path=Path(report_payload["docx_path"]),
            )

        return ProcessedJob(
            job_id=payload["job_id"],
            source=SourceCandidate(
                source_id=source_payload["source_id"],
                kind=source_payload["kind"],
                display_name=source_payload["display_name"],
                url=source_payload["url"],
                telegram_file_id=source_payload["telegram_file_id"],
                mime_type=source_payload["mime_type"],
                file_name=source_payload["file_name"],
                file_unique_id=source_payload["file_unique_id"],
                local_path=Path(source_payload["local_path"]) if source_payload["local_path"] else None,
            ),
            workspace_dir=Path(payload["workspace_dir"]),
            transcript=TranscriptArtifacts(
                markdown_path=Path(payload["transcript"]["markdown_path"]),
                docx_path=Path(payload["transcript"]["docx_path"]),
                text_path=Path(payload["transcript"]["text_path"]),
            ),
            report=report,
            metadata_path=metadata_path,
            errors=payload.get("errors", []),
        )

    def ensure_report(self, job_id: str, report_prompt_suffix: str = "") -> ProcessedJob:
        job = self.load_job(job_id)
        if job.report and job.report.markdown_path.exists() and job.report.docx_path.exists():
            return job

        report_markdown_path = job.workspace_dir / "report.md"
        report_docx_path = job.workspace_dir / "report.docx"
        generate_report(
            transcript_path=job.transcript.markdown_path,
            report_path=report_markdown_path,
            report_prompt_suffix=report_prompt_suffix,
        )
        write_report_docx(report_docx_path, report_markdown_path.read_text(encoding="utf-8"))
        job.report = ReportArtifacts(
            markdown_path=report_markdown_path,
            docx_path=report_docx_path,
        )
        self._save_job(job)
        return job

    def ensure_deep_research(self, job_id: str) -> Path:
        job = self.load_job(job_id)
        if job.report is None or not job.report.markdown_path.exists():
            raise RuntimeError("Research report must exist before deep research starts")

        research_dir = job.workspace_dir / "deep_research"
        final_report = research_dir / "evidence-research-final-report.md"
        if final_report.exists():
            return final_report

        return self.deep_research_runner(
            job.transcript.markdown_path,
            job.report.markdown_path,
            research_dir,
            job.source.display_name,
        )

    def _materialize_local_source(self, source: SourceCandidate, workspace_dir: Path) -> SourceCandidate:
        return _materialize_local_source_worker(source, workspace_dir)

    def _find_existing_job_for_source(self, source: SourceCandidate) -> ProcessedJob | None:
        if not source.file_unique_id:
            return None

        latest_matches = sorted(
            (path for path in self.jobs_dir.glob("*/job.json") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for metadata_path in latest_matches:
            try:
                payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue

            source_payload = payload.get("source") or {}
            if source_payload.get("kind") != source.kind:
                continue
            if source_payload.get("file_unique_id") != source.file_unique_id:
                continue

            transcript_payload = payload.get("transcript") or {}
            markdown_path = transcript_payload.get("markdown_path")
            docx_path = transcript_payload.get("docx_path")
            text_path = transcript_payload.get("text_path")
            if not markdown_path or not docx_path or not text_path:
                continue
            if not Path(markdown_path).exists() or not Path(docx_path).exists() or not Path(text_path).exists():
                continue

            return self.load_job(payload["job_id"])
        return None

    def _save_job(self, job: ProcessedJob) -> None:
        if job.metadata_path is None:
            raise ValueError("metadata_path must be set before saving a job")

        payload = {
            "job_id": job.job_id,
            "workspace_dir": str(job.workspace_dir),
            "source": {
                "source_id": job.source.source_id,
                "kind": job.source.kind,
                "display_name": job.source.display_name,
                "url": job.source.url,
                "telegram_file_id": job.source.telegram_file_id,
                "mime_type": job.source.mime_type,
                "file_name": job.source.file_name,
                "file_unique_id": job.source.file_unique_id,
                "local_path": str(job.source.local_path) if job.source.local_path else None,
            },
            "transcript": {
                "markdown_path": str(job.transcript.markdown_path),
                "docx_path": str(job.transcript.docx_path),
                "text_path": str(job.transcript.text_path),
            },
            "report": (
                {
                    "markdown_path": str(job.report.markdown_path),
                    "docx_path": str(job.report.docx_path),
                }
                if job.report
                else None
            ),
            "errors": job.errors,
        }
        job.metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
