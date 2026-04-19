from __future__ import annotations

from collections import defaultdict

from telegram_transcriber_bot.domain import MediaAttachment


class MediaGroupAccumulator:
    def __init__(self) -> None:
        self._groups: dict[tuple[int, str], list[MediaAttachment]] = defaultdict(list)

    def add(self, chat_id: int, media_group_id: str, attachment: MediaAttachment) -> None:
        self._groups[(chat_id, media_group_id)].append(attachment)

    def pop(self, chat_id: int, media_group_id: str) -> list[MediaAttachment]:
        return self._groups.pop((chat_id, media_group_id), [])

