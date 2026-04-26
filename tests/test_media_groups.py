from transcriber_workers_common.domain import MediaAttachment
from transcriber_workers_common.media_groups import MediaGroupAccumulator


def test_media_group_accumulator_collects_items_by_group_key() -> None:
    accumulator = MediaGroupAccumulator()

    accumulator.add(
        chat_id=42,
        media_group_id="group-1",
        attachment=MediaAttachment(
            telegram_file_id="file-1",
            kind="telegram_video",
            file_name="part-1.mp4",
            mime_type="video/mp4",
            file_unique_id="uniq-1",
        ),
    )
    accumulator.add(
        chat_id=42,
        media_group_id="group-1",
        attachment=MediaAttachment(
            telegram_file_id="file-2",
            kind="telegram_audio",
            file_name="part-2.ogg",
            mime_type="audio/ogg",
            file_unique_id="uniq-2",
        ),
    )

    items = accumulator.pop(chat_id=42, media_group_id="group-1")

    assert [item.file_name for item in items] == ["part-1.mp4", "part-2.ogg"]
