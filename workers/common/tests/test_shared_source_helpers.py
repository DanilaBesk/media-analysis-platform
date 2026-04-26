from __future__ import annotations

from transcriber_workers_common.domain import MediaAttachment
from transcriber_workers_common.media_groups import MediaGroupAccumulator
from transcriber_workers_common.source_extractor import extract_sources, extract_youtube_video_id
from transcriber_workers_common.source_labels import humanize_source_label, looks_like_machine_file_name


def test_extract_sources_returns_shared_domain_candidates() -> None:
    result = extract_sources(
        "watch https://youtu.be/abc123 and https://example.com/nope",
        [
            MediaAttachment(
                telegram_file_id="file-1",
                kind="telegram_audio",
                file_name="voice.ogg",
                mime_type="audio/ogg",
                file_unique_id="unique-1",
            )
        ],
    )

    assert [candidate.kind for candidate in result.candidates] == ["youtube_url", "telegram_audio"]
    assert result.candidates[0].display_name == "YouTube: abc123"
    assert result.candidates[1].telegram_file_id == "file-1"
    assert result.rejected_urls == ["https://example.com/nope"]


def test_shared_youtube_id_parser_supports_expected_url_shapes() -> None:
    assert extract_youtube_video_id("https://www.youtube.com/watch?v=abc123") == "abc123"
    assert extract_youtube_video_id("https://www.youtube.com/shorts/short123") == "short123"
    assert extract_youtube_video_id("https://www.youtube.com/embed/embed123") == "embed123"
    assert extract_youtube_video_id("https://example.com/watch?v=abc123") is None


def test_media_group_accumulator_preserves_chat_group_isolation() -> None:
    accumulator = MediaGroupAccumulator()
    attachment = MediaAttachment(
        telegram_file_id="file-1",
        kind="telegram_video",
        file_name="video.mp4",
        mime_type="video/mp4",
        file_unique_id="unique-1",
    )

    accumulator.add(1, "group-a", attachment)

    assert accumulator.pop(2, "group-a") == []
    assert accumulator.pop(1, "group-a") == [attachment]
    assert accumulator.pop(1, "group-a") == []


def test_source_label_helpers_stay_shared_for_artifacts_and_adapter() -> None:
    assert humanize_source_label("Audio: AgAB12345-Qwert.ogg") == "Аудиофайл из Telegram"
    assert humanize_source_label("Video: readable-name.mp4") == "readable-name.mp4"
    assert looks_like_machine_file_name("AgAB12345-Qwert.ogg") is True
