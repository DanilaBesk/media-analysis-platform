# FILE: workers/common/src/transcriber_workers_common/source_labels.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Humanize canonical source labels for Telegram status text and rendered artifacts.
# SCOPE: YouTube, Telegram audio/video, and machine-generated filename detection.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Moved source-label helpers into worker-common shared contracts.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   humanize_source_label - Render user-facing source labels.
#   is_human_readable_title - Guard transcript title selection.
#   looks_like_machine_file_name - Detect Telegram-style opaque media names.
# END_MODULE_MAP

from __future__ import annotations

import re


def humanize_source_label(source_label: str) -> str:
    normalized = source_label.strip()
    if normalized.startswith("YouTube:"):
        value = normalized.split(":", 1)[1].strip()
        return f"YouTube: {value}" if value else "YouTube"
    if normalized.startswith("Audio:"):
        value = normalized.split(":", 1)[1].strip()
        return _humanize_media_source(value, kind="audio")
    if normalized.startswith("Video:"):
        value = normalized.split(":", 1)[1].strip()
        return _humanize_media_source(value, kind="video")
    return normalized or "Неизвестный источник"


def is_human_readable_title(value: str) -> bool:
    if not value:
        return False
    return not looks_like_machine_file_name(value)


def looks_like_machine_file_name(value: str) -> bool:
    candidate = value.strip()
    if "." in candidate:
        candidate = candidate.rsplit(".", 1)[0]
    if len(candidate) < 12:
        return False
    if re.fullmatch(r"[A-Za-z0-9_-]+", candidate) is None:
        return False
    if re.fullmatch(r"Ag[A-Za-z0-9_-]{8,}", candidate):
        return True
    letters = sum(char.isalpha() for char in candidate)
    digits = sum(char.isdigit() for char in candidate)
    uppercase = sum(char.isupper() for char in candidate)
    lowercase = sum(char.islower() for char in candidate)
    separators = candidate.count("-") + candidate.count("_")
    return (
        letters + digits + separators == len(candidate)
        and digits >= 3
        and uppercase >= 2
        and lowercase >= 2
    )


def _humanize_media_source(file_name: str, kind: str) -> str:
    if not file_name:
        return "Аудиофайл из Telegram" if kind == "audio" else "Видеофайл из Telegram"
    if looks_like_machine_file_name(file_name):
        return "Аудиофайл из Telegram" if kind == "audio" else "Видеофайл из Telegram"
    return file_name


__all__ = [
    "humanize_source_label",
    "is_human_readable_title",
    "looks_like_machine_file_name",
]
