# FILE: apps/telegram-bot/src/telegram_adapter/gateway.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the API-backed processing gateway that lets the Telegram adapter preserve UX while delegating business logic to the HTTP API.
# SCOPE: Submit single or combined Telegram sources, poll for terminal jobs, resolve artifacts, download files to packet-local storage, and shape callback flows without local orchestration logic.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP, M-CONTRACTS
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the packet-local Telegram gateway that turns API jobs into Telegram-sendable artifacts.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   submit-telegram-sources - Route single and combined Telegram sources to the API with polling-default delivery.
#   wait-for-terminal-job - Poll job snapshots until terminal state without taking over API lineage ownership.
#   materialize-artifacts - Resolve and download transcript, report, and deep-research artifacts into packet-local storage.
# END_MODULE_MAP

from __future__ import annotations

import time
from pathlib import Path

from telegram_adapter.api_client import TelegramApiClient, UploadFilePart
from telegram_transcriber_bot.domain import (
    ProcessedJob,
    ReportArtifacts,
    SourceCandidate,
    TranscriptArtifacts,
)

TERMINAL_JOB_STATUSES = {"succeeded", "failed", "canceled"}


class TelegramApiProcessingGateway:
    def __init__(
        self,
        api_client: TelegramApiClient,
        storage_dir: Path,
        *,
        poll_interval_seconds: float = 1.0,
        poll_timeout_seconds: float = 300.0,
    ) -> None:
        self.api_client = api_client
        self.storage_dir = storage_dir
        self.poll_interval_seconds = poll_interval_seconds
        self.poll_timeout_seconds = poll_timeout_seconds

    def process_source(self, source: SourceCandidate) -> ProcessedJob:
        if source.kind == "youtube_url" and source.url:
            response = self.api_client.create_from_url(
                url=source.url,
                display_name=source.display_name,
                delivery_strategy="polling",
            )
            job_snapshot = self._unwrap_job(response)
            terminal_job = self._wait_for_terminal_job(job_snapshot["job_id"])
            return self._materialize_transcription_job(terminal_job, source)

        upload_part = self._build_upload_part(source)
        response = self.api_client.create_upload(
            files=[upload_part],
            display_name=source.display_name,
            delivery_strategy="polling",
        )
        jobs = response.get("jobs", [])
        if not jobs:
            raise RuntimeError("API did not return transcription jobs")
        terminal_job = self._wait_for_terminal_job(jobs[0]["job_id"])
        return self._materialize_transcription_job(terminal_job, source)

    def process_source_group(self, sources: list[SourceCandidate]) -> ProcessedJob:
        if not sources:
            raise RuntimeError("Combined Telegram submission requires at least one source")
        upload_parts = [self._build_upload_part(source) for source in sources]
        response = self.api_client.create_combined_upload(
            files=upload_parts,
            display_name="Telegram combined media",
            delivery_strategy="polling",
        )
        job_snapshot = self._unwrap_job(response)
        terminal_job = self._wait_for_terminal_job(job_snapshot["job_id"])
        synthetic_source = SourceCandidate(
            source_id=sources[0].source_id,
            kind=sources[0].kind,
            display_name="Telegram combined media",
            url=None,
            telegram_file_id=None,
            mime_type=None,
            file_name=None,
        )
        return self._materialize_transcription_job(terminal_job, synthetic_source)

    def load_job(self, job_id: str) -> ProcessedJob:
        job_snapshot = self._unwrap_job(self.api_client.get_job(job_id))
        source = self._source_from_job(job_snapshot)
        return self._materialize_transcription_job(job_snapshot, source)

    def ensure_report(self, job_id: str, report_prompt_suffix: str = "") -> ProcessedJob:
        del report_prompt_suffix
        report_job = self._unwrap_job(
            self.api_client.create_report(job_id, delivery_strategy="polling")
        )
        terminal_report_job = self._wait_for_terminal_job(report_job["job_id"])
        parent = self.load_job(job_id)
        report = self._download_report_artifacts(
            terminal_report_job["artifacts"],
            parent.workspace_dir,
            report_job_id=terminal_report_job["job_id"],
        )
        return ProcessedJob(
            job_id=parent.job_id,
            source=parent.source,
            workspace_dir=parent.workspace_dir,
            transcript=parent.transcript,
            report=report,
            metadata_path=parent.metadata_path,
            errors=parent.errors,
        )

    def ensure_deep_research(self, job_id: str) -> Path:
        deep_job = self._unwrap_job(
            self.api_client.create_deep_research(job_id, delivery_strategy="polling")
        )
        terminal_job = self._wait_for_terminal_job(deep_job["job_id"])
        workspace_dir = self.storage_dir / "api-jobs" / terminal_job["job_id"]
        workspace_dir.mkdir(parents=True, exist_ok=True)
        return self._download_artifact_by_kind(
            terminal_job["artifacts"],
            "deep_research_markdown",
            workspace_dir / "deep_research",
            fallback_name="evidence-research-final-report.md",
        )

    def _wait_for_terminal_job(self, job_id: str) -> dict:
        started_at = time.monotonic()
        while True:
            job = self._unwrap_job(self.api_client.get_job(job_id))
            status = job.get("status")
            if status in TERMINAL_JOB_STATUSES:
                if status != "succeeded":
                    raise RuntimeError(f"API job {job_id} finished with status {status}")
                return job
            if time.monotonic() - started_at > self.poll_timeout_seconds:
                raise TimeoutError(f"Timed out waiting for job {job_id}")
            time.sleep(self.poll_interval_seconds)

    def _materialize_transcription_job(self, job_snapshot: dict, source: SourceCandidate) -> ProcessedJob:
        workspace_dir = self.storage_dir / "api-jobs" / job_snapshot["job_id"]
        workspace_dir.mkdir(parents=True, exist_ok=True)
        transcript_dir = workspace_dir / "transcript"
        transcript_dir.mkdir(parents=True, exist_ok=True)

        transcript = TranscriptArtifacts(
            markdown_path=self._download_artifact_by_kind(
                job_snapshot["artifacts"],
                "transcript_segmented_markdown",
                transcript_dir,
                fallback_name="transcript.md",
            ),
            docx_path=self._download_artifact_by_kind(
                job_snapshot["artifacts"],
                "transcript_docx",
                transcript_dir,
                fallback_name="transcript.docx",
            ),
            text_path=self._download_artifact_by_kind(
                job_snapshot["artifacts"],
                "transcript_plain",
                transcript_dir,
                fallback_name="transcript.txt",
            ),
        )

        return ProcessedJob(
            job_id=job_snapshot["job_id"],
            source=source,
            workspace_dir=workspace_dir,
            transcript=transcript,
            report=None,
            metadata_path=None,
        )

    def _download_report_artifacts(
        self,
        artifacts: list[dict],
        workspace_dir: Path,
        *,
        report_job_id: str | None = None,
    ) -> ReportArtifacts:
        report_dir = workspace_dir / "report"
        report_dir.mkdir(parents=True, exist_ok=True)
        return ReportArtifacts(
            job_id=report_job_id,
            markdown_path=self._download_artifact_by_kind(
                artifacts,
                "report_markdown",
                report_dir,
                fallback_name="report.md",
            ),
            docx_path=self._download_artifact_by_kind(
                artifacts,
                "report_docx",
                report_dir,
                fallback_name="report.docx",
            ),
        )

    def _download_artifact_by_kind(
        self,
        artifacts: list[dict],
        artifact_kind: str,
        target_dir: Path,
        *,
        fallback_name: str,
    ) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        artifact = next((item for item in artifacts if item.get("artifact_kind") == artifact_kind), None)
        if artifact is None:
            raise FileNotFoundError(f"Artifact {artifact_kind} is missing")

        artifact_resolution = self.api_client.resolve_artifact(artifact["artifact_id"])
        filename = artifact_resolution.get("filename") or fallback_name
        destination = target_dir / filename
        destination.write_bytes(self.api_client.download_bytes(artifact_resolution["download"]["url"]))
        return destination

    def _build_upload_part(self, source: SourceCandidate) -> UploadFilePart:
        if source.local_path is None:
            raise RuntimeError("Telegram source must be downloaded locally before API submission")
        content_type = source.mime_type or "application/octet-stream"
        return UploadFilePart(
            filename=source.file_name or source.local_path.name,
            content_type=content_type,
            content_bytes=source.local_path.read_bytes(),
        )

    def _source_from_job(self, job_snapshot: dict) -> SourceCandidate:
        items = (job_snapshot.get("source_set") or {}).get("items") or []
        source = items[0]["source"] if items else {}
        return SourceCandidate(
            source_id=source.get("source_id", job_snapshot["job_id"]),
            kind=source.get("source_kind", "telegram_audio"),
            display_name=source.get("display_name") or job_snapshot.get("display_name") or "Telegram source",
            url=source.get("source_url"),
            telegram_file_id=None,
            mime_type=None,
            file_name=source.get("original_filename"),
        )

    def _unwrap_job(self, payload: dict) -> dict:
        job = payload.get("job")
        if isinstance(job, dict):
            return job
        return payload
