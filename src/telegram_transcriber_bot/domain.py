from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


SourceKind = Literal["youtube_url", "telegram_video", "telegram_audio"]
MediaKind = Literal["telegram_video", "telegram_audio"]


@dataclass(slots=True)
class MediaAttachment:
    telegram_file_id: str
    kind: MediaKind
    file_name: str
    mime_type: str | None
    file_unique_id: str


@dataclass(slots=True)
class SourceCandidate:
    source_id: str
    kind: SourceKind
    display_name: str
    url: str | None
    telegram_file_id: str | None
    mime_type: str | None
    file_name: str | None
    file_unique_id: str | None = None
    local_path: Path | None = None


@dataclass(slots=True)
class ExtractionResult:
    candidates: list[SourceCandidate]
    rejected_urls: list[str]


@dataclass(slots=True)
class TranscriptSegment:
    start_seconds: float
    end_seconds: float
    text: str
    speaker: str | None = None


@dataclass(slots=True)
class TranscriptResult:
    title: str
    source_label: str
    segments: list[TranscriptSegment]
    language: str
    raw_text: str


@dataclass(slots=True)
class TranscriptArtifacts:
    markdown_path: Path
    docx_path: Path
    text_path: Path


@dataclass(slots=True)
class ReportArtifacts:
    markdown_path: Path
    docx_path: Path


@dataclass(slots=True)
class ProcessedJob:
    job_id: str
    source: SourceCandidate
    workspace_dir: Path
    transcript: TranscriptArtifacts
    report: ReportArtifacts | None = None
    metadata_path: Path | None = None
    errors: list[str] = field(default_factory=list)

