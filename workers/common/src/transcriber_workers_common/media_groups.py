# FILE: workers/common/src/transcriber_workers_common/media_groups.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Accumulate Telegram media-group attachments before creating combined source candidates.
# SCOPE: In-memory grouping keyed by chat and Telegram media group id.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Moved media-group accumulation into worker-common shared contracts.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   MediaGroupAccumulator - Store and pop ordered media attachments per chat/media-group key.
# END_MODULE_MAP

from __future__ import annotations

from collections import defaultdict

from transcriber_workers_common.domain import MediaAttachment


class MediaGroupAccumulator:
    def __init__(self) -> None:
        self._groups: dict[tuple[int, str], list[MediaAttachment]] = defaultdict(list)

    def add(self, chat_id: int, media_group_id: str, attachment: MediaAttachment) -> None:
        self._groups[(chat_id, media_group_id)].append(attachment)

    def pop(self, chat_id: int, media_group_id: str) -> list[MediaAttachment]:
        return self._groups.pop((chat_id, media_group_id), [])


__all__ = ["MediaGroupAccumulator"]
