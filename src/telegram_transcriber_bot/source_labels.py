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
