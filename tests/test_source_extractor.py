from telegram_transcriber_bot.domain import MediaAttachment
from telegram_transcriber_bot.source_extractor import extract_sources, extract_youtube_video_id


def test_extract_sources_returns_supported_and_rejected_urls() -> None:
    result = extract_sources(
        text=(
            "Разбери https://youtu.be/dQw4w9WgXcQ "
            "и https://example.com/article "
            "и https://www.youtube.com/watch?v=oHg5SJYRHA0"
        ),
        attachments=[],
    )

    assert [candidate.kind for candidate in result.candidates] == [
        "youtube_url",
        "youtube_url",
    ]
    assert result.rejected_urls == ["https://example.com/article"]


def test_extract_sources_combines_links_and_media_attachments() -> None:
    result = extract_sources(
        text="Вот ссылка https://youtu.be/dQw4w9WgXcQ",
        attachments=[
            MediaAttachment(
                telegram_file_id="file-1",
                kind="telegram_video",
                file_name="demo.mp4",
                mime_type="video/mp4",
                file_unique_id="uniq-1",
            ),
            MediaAttachment(
                telegram_file_id="file-2",
                kind="telegram_audio",
                file_name="call.ogg",
                mime_type="audio/ogg",
                file_unique_id="uniq-2",
            ),
        ],
    )

    assert len(result.candidates) == 3
    assert result.candidates[0].display_name.startswith("YouTube:")
    assert result.candidates[1].display_name == "Video: demo.mp4"
    assert result.candidates[2].display_name == "Audio: call.ogg"


def test_extract_youtube_video_id_supports_multiple_shapes() -> None:
    assert extract_youtube_video_id("https://youtu.be/demo123") == "demo123"
    assert extract_youtube_video_id("https://www.youtube.com/watch?v=demo123") == "demo123"
    assert extract_youtube_video_id("https://www.youtube.com/shorts/demo123") == "demo123"
    assert extract_youtube_video_id("https://www.youtube.com/embed/demo123") == "demo123"
