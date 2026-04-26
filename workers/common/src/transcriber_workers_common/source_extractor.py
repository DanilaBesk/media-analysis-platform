# FILE: workers/common/src/transcriber_workers_common/source_extractor.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Extract canonical source candidates from user text and Telegram media attachments.
# SCOPE: YouTube URL recognition, rejected URL reporting, and Telegram media attachment conversion.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Moved source extraction helpers into worker-common shared contracts.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   URL_PATTERN - Shared URL detection expression.
#   YOUTUBE_HOSTS - Supported YouTube host allowlist.
#   extract_sources - Convert text and attachments into ordered SourceCandidate values.
#   build_url_candidate - Build a SourceCandidate for supported YouTube URLs.
#   extract_youtube_video_id - Parse supported YouTube URL variants.
# END_MODULE_MAP

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from transcriber_workers_common.domain import ExtractionResult, MediaAttachment, SourceCandidate

URL_PATTERN = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
YOUTUBE_HOSTS = {
    "youtu.be",
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
}


def extract_sources(text: str, attachments: list[MediaAttachment]) -> ExtractionResult:
    candidates: list[SourceCandidate] = []
    rejected_urls: list[str] = []

    for raw_url in URL_PATTERN.findall(text or ""):
        candidate = build_url_candidate(raw_url.rstrip(".,);]"))
        if candidate is None:
            rejected_urls.append(raw_url.rstrip(".,);]"))
            continue
        candidates.append(candidate)

    for attachment in attachments:
        candidates.append(
            SourceCandidate(
                source_id=f"src-{uuid4().hex[:12]}",
                kind=attachment.kind,
                display_name=_attachment_display_name(attachment),
                url=None,
                telegram_file_id=attachment.telegram_file_id,
                mime_type=attachment.mime_type,
                file_name=attachment.file_name,
                file_unique_id=attachment.file_unique_id,
            )
        )

    return ExtractionResult(candidates=candidates, rejected_urls=rejected_urls)


def build_url_candidate(url: str) -> SourceCandidate | None:
    video_id = extract_youtube_video_id(url)
    if video_id is None:
        return None
    return SourceCandidate(
        source_id=f"src-{uuid4().hex[:12]}",
        kind="youtube_url",
        display_name=f"YouTube: {video_id}",
        url=url,
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )


def extract_youtube_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host not in YOUTUBE_HOSTS:
        return None

    if host == "youtu.be":
        candidate = parsed.path.lstrip("/")
        return candidate or None

    if parsed.path == "/watch":
        candidate = parse_qs(parsed.query).get("v", [None])[0]
        return candidate

    if parsed.path.startswith("/shorts/"):
        candidate = parsed.path.split("/", 2)[2]
        return candidate or None

    if parsed.path.startswith("/embed/"):
        candidate = parsed.path.split("/", 2)[2]
        return candidate or None

    return None


def _attachment_display_name(attachment: MediaAttachment) -> str:
    if attachment.kind == "telegram_video":
        return f"Video: {attachment.file_name}"
    return f"Audio: {attachment.file_name}"


__all__ = [
    "URL_PATTERN",
    "YOUTUBE_HOSTS",
    "build_url_candidate",
    "extract_sources",
    "extract_youtube_video_id",
]
