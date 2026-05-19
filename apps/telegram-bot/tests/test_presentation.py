from __future__ import annotations

from telegram_adapter.presentation import render_material_summary, render_material_summary_lines


def test_file_summary_uses_name_size_and_optional_duration_without_media_kind_labels() -> None:
    item = {
        "media_asset_id": "media-1",
        "kind": "audio",
        "display_name": "team-sync.m4a",
        "origin": {"origin_type": "upload", "size_bytes": 1_536},
        "duration_seconds": 125,
    }

    assert render_material_summary(item) == "team-sync.m4a · 1.5 KB · 02:05"


def test_file_summary_omits_missing_optional_parts() -> None:
    item = {
        "media_asset_id": "media-2",
        "kind": "document",
        "display_name": "brief.pdf",
        "origin": {"origin_type": "upload"},
    }

    assert render_material_summary(item) == "brief.pdf"


def test_text_summary_uses_bounded_preview_from_source_text() -> None:
    item = {
        "media_asset_id": "media-3",
        "kind": "text",
        "display_name": "stale fallback",
        "origin": {
            "origin_type": "text",
            "origin_ref": "  Очень длинный   текст для карточки Telegram, который должен аккуратно сокращаться.  ",
        },
    }

    assert render_material_summary(item, text_preview_limit=32) == 'Текст: «Очень длинный текст для карточк…»'


def test_link_summary_compacts_url_for_telegram_display() -> None:
    item = {
        "media_asset_id": "media-4",
        "kind": "url",
        "display_name": "https://www.example.com/reports/2026/q2/summary?utm_source=telegram",
        "origin": {
            "origin_type": "url",
            "origin_ref": "https://www.example.com/reports/2026/q2/summary?utm_source=telegram",
        },
    }

    assert render_material_summary(item, url_preview_limit=28) == "example.com/reports/2026/q2…"


def test_render_material_summary_lines_limits_items_and_adds_overflow_line() -> None:
    items = [
        {
            "media_asset_id": "media-1",
            "kind": "document",
            "display_name": "brief.pdf",
            "origin": {"origin_type": "upload", "size_bytes": 2_048},
        },
        {
            "media_asset_id": "media-2",
            "kind": "text",
            "origin": {"origin_type": "text", "origin_ref": "Нужен короткий статус"},
        },
        {
            "media_asset_id": "media-3",
            "kind": "url",
            "origin": {"origin_type": "url", "origin_ref": "https://example.com/very/long/path"},
        },
    ]

    assert render_material_summary_lines(items, limit=2) == [
        "brief.pdf · 2 KB",
        "Текст: «Нужен короткий статус»",
        "+ ещё 1 материалов",
    ]


def test_summary_fallbacks_cover_minimal_text_url_and_unnamed_materials() -> None:
    assert render_material_summary(
        {
            "media_asset_id": "media-text-fallback",
            "kind": "text",
            "origin": {"origin_type": "text"},
        }
    ) == "Текст: «media-text-fallback»"
    assert render_material_summary(
        {
            "media_asset_id": "media-url-fallback",
            "kind": "url",
            "origin": {"origin_type": "url"},
        }
    ) == "media-url-fallback"
    assert render_material_summary({"kind": "binary"}) == "Материал"


def test_file_summary_normalizes_size_duration_and_truncation_edges() -> None:
    assert render_material_summary(
        {
            "media_asset_id": "media-duration-ms",
            "kind": "voice",
            "display_name": "long-call.ogg",
            "origin": {"origin_type": "upload", "size_bytes": 1_099_511_627_776},
            "metadata": {"duration_ms": "3661000"},
        }
    ) == "long-call.ogg · 1 TB · 01:01:01"
    assert render_material_summary(
        {
            "media_asset_id": "media-byte-size",
            "kind": "document",
            "display_name": "byte-size.bin",
            "origin": {"origin_type": "upload", "size_bytes": 512},
        }
    ) == "byte-size.bin · 512 B"
    assert render_material_summary(
        {
            "media_asset_id": "media-float-size",
            "kind": "document",
            "display_name": "float-size.bin",
            "size_bytes": 1536.5,
        }
    ) == "float-size.bin · 1.5 KB"
    assert render_material_summary(
        {
            "media_asset_id": "media-bad-size",
            "kind": "document",
            "display_name": "bad-size.bin",
            "size_bytes": True,
            "origin": {"size_bytes": "not-a-number"},
            "metadata": {"size_bytes": "-1", "duration_ms": ""},
        }
    ) == "bad-size.bin"
    assert render_material_summary(
        {
            "media_asset_id": "media-short-text",
            "kind": "text",
            "origin": {"origin_type": "text", "origin_ref": "short"},
        },
        text_preview_limit=1,
    ) == "Текст: «…»"
    assert render_material_summary(
        {
            "media_asset_id": "media-plain-url",
            "kind": "url",
            "origin": {"origin_type": "url", "origin_ref": "  plain local reference  "},
        },
        url_preview_limit=8,
    ) == "plain l…"
