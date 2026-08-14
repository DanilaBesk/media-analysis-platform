# FILE: apps/telegram-bot/src/telegram_adapter/presentation.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Render compact Telegram-facing material summaries for the processing card.
# SCOPE: Format file, text, and link media items into short user-facing lines and apply top-N overflow compaction.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_MODULE_MAP
#   render_material_summary - Render one compact Telegram material summary.
#   render_material_summary_lines - Render bounded material summary lines.
# END_MODULE_MAP

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

JsonMapping = Mapping[str, Any]
DEFAULT_TEXT_PREVIEW_LIMIT = 48
DEFAULT_URL_PREVIEW_LIMIT = 48
DEFAULT_SUMMARY_LIMIT = 5
_YOUTUBE_TITLE_PENDING = "загружаем название..."
_YOUTUBE_PENDING_STATUSES = {"pending", "queued", "running", "resolving"}
_YOUTUBE_SUCCEEDED_STATUSES = {"succeeded", "success", "completed"}


def render_material_summary(
    item: JsonMapping,
    *,
    text_preview_limit: int = DEFAULT_TEXT_PREVIEW_LIMIT,
    url_preview_limit: int = DEFAULT_URL_PREVIEW_LIMIT,
) -> str:
    if _is_text_item(item):
        return f'Текст: «{_truncate(_extract_text_preview(item), text_preview_limit)}»'
    if _is_url_item(item):
        url = _extract_url(item)
        if _is_youtube_url(url):
            return _render_youtube_summary(item, title_limit=url_preview_limit)
        return _compact_url(url, limit=url_preview_limit)
    return _render_file_summary(item)


def render_material_summary_lines(
    items: Sequence[JsonMapping],
    *,
    limit: int = DEFAULT_SUMMARY_LIMIT,
    text_preview_limit: int = DEFAULT_TEXT_PREVIEW_LIMIT,
    url_preview_limit: int = DEFAULT_URL_PREVIEW_LIMIT,
) -> list[str]:
    visible_limit = max(limit, 0)
    visible_items = list(items[:visible_limit])
    lines = [
        render_material_summary(
            item,
            text_preview_limit=text_preview_limit,
            url_preview_limit=url_preview_limit,
        )
        for item in visible_items
    ]
    hidden_count = max(len(items) - len(visible_items), 0)
    if hidden_count:
        lines.append(f"+ ещё {hidden_count} материалов")
    return lines


def _render_file_summary(item: JsonMapping) -> str:
    parts = [_fallback_label(item)]
    size_bytes = _extract_size_bytes(item)
    if size_bytes is not None:
        parts.append(_format_size(size_bytes))
    duration_seconds = _extract_duration_seconds(item)
    if duration_seconds is not None:
        parts.append(_format_duration(duration_seconds))
    return " · ".join(part for part in parts if part)


def _is_text_item(item: JsonMapping) -> bool:
    return str(item.get("kind") or "") == "text" or str(_origin(item).get("origin_type") or "") == "text"


def _is_url_item(item: JsonMapping) -> bool:
    return str(item.get("kind") or "") == "url" or str(_origin(item).get("origin_type") or "") == "url"


def _extract_text_preview(item: JsonMapping) -> str:
    for value in (_origin(item).get("origin_ref"), item.get("text"), item.get("display_name")):
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return _fallback_label(item)


def _extract_url(item: JsonMapping) -> str:
    for value in (_origin(item).get("origin_ref"), item.get("url"), item.get("display_name")):
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return _fallback_label(item)


def _extract_size_bytes(item: JsonMapping) -> int | None:
    for mapping in (item, _origin(item), _metadata(item)):
        parsed = _coerce_non_negative_int(mapping.get("size_bytes"))
        if parsed is not None:
            return parsed
    return None


def _extract_duration_seconds(item: JsonMapping) -> int | None:
    for mapping in (item, _origin(item), _metadata(item)):
        for key in ("duration_seconds", "duration_secs", "duration"):
            parsed = _coerce_duration_seconds(mapping.get(key))
            if parsed is not None:
                return parsed
        parsed_ms = _coerce_duration_milliseconds(mapping.get("duration_ms"))
        if parsed_ms is not None:
            return parsed_ms
    return None


def _compact_url(value: str, *, limit: int) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/")
    if host:
        compact = f"{host}{path}" if path else host
        return _truncate(compact, limit)
    return _truncate(_normalize_text(value) or "ссылка", limit)


def _is_youtube_url(value: str) -> bool:
    host = (urlparse(value).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


def _render_youtube_summary(item: JsonMapping, *, title_limit: int) -> str:
    enrichment = _youtube_enrichment(item)
    status = _normalize_text(enrichment.get("status")).lower()
    if status in _YOUTUBE_PENDING_STATUSES:
        return f"YouTube: {_YOUTUBE_TITLE_PENDING}"
    if status and status not in _YOUTUBE_SUCCEEDED_STATUSES:
        return f"YouTube: {_YOUTUBE_TITLE_PENDING}"
    title = _youtube_title(item, enrichment)
    if title:
        return f"YouTube: {_truncate(title, title_limit)}"
    return f"YouTube: {_YOUTUBE_TITLE_PENDING}"


def _youtube_enrichment(item: JsonMapping) -> JsonMapping:
    metadata = _metadata(item)
    for container in (item, metadata):
        for key in ("provider_metadata", "enrichment"):
            candidate = container.get(key)
            if not isinstance(candidate, Mapping):
                continue
            provider = _normalize_text(candidate.get("provider")).lower()
            if not provider or provider == "youtube":
                return candidate
    return {}


def _youtube_title(item: JsonMapping, enrichment: JsonMapping) -> str:
    metadata = _metadata(item)
    for mapping, keys in (
        (enrichment, ("title", "video_title")),
        (item, ("youtube_title", "video_title")),
        (metadata, ("youtube_title", "video_title", "title")),
    ):
        for key in keys:
            title = _normalize_text(mapping.get(key))
            if title:
                return title
    return ""


def _format_size(size_bytes: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size_bytes)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    rounded = round(value, 1)
    if rounded.is_integer():
        return f"{int(rounded)} {units[unit_index]}"
    return f"{rounded:.1f} {units[unit_index]}"


def _format_duration(duration_seconds: int) -> str:
    total_seconds = max(duration_seconds, 0)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _truncate(value: str, limit: int) -> str:
    normalized_limit = max(limit, 1)
    if len(value) <= normalized_limit:
        return value
    if normalized_limit == 1:
        return "…"
    return value[: normalized_limit - 1].rstrip() + "…"


def _fallback_label(item: JsonMapping) -> str:
    for value in (item.get("display_name"), item.get("media_asset_id")):
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return "Материал"


def _origin(item: JsonMapping) -> JsonMapping:
    origin = item.get("origin")
    if isinstance(origin, Mapping):
        return origin
    return {}


def _metadata(item: JsonMapping) -> JsonMapping:
    metadata = item.get("metadata")
    if isinstance(metadata, Mapping):
        return metadata
    return {}


def _normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())


def _coerce_non_negative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if value >= 0 else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = int(float(stripped))
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def _coerce_duration_seconds(value: object) -> int | None:
    parsed = _coerce_non_negative_int(value)
    return parsed if parsed is not None else None


def _coerce_duration_milliseconds(value: object) -> int | None:
    parsed = _coerce_non_negative_int(value)
    if parsed is None:
        return None
    return parsed // 1000


__all__ = [
    "render_material_summary",
    "render_material_summary_lines",
]
