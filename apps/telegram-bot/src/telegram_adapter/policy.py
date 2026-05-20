from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from telegram_adapter.errors import TelegramUserError, TelegramUserErrorCode

JsonObject = dict[str, Any]
TelegramVisibility = Literal["private"]


@dataclass(frozen=True, slots=True)
class TelegramChatScope:
    channel_identity: JsonObject
    state_key: tuple[int, int | None]
    visibility: TelegramVisibility


class TelegramChatPolicy:
    def resolve(
        self,
        *,
        chat_id: int,
        user_id: int | None,
        chat_type: str | None = "private",
        message_thread_id: int | None = None,
    ) -> TelegramChatScope:
        normalized_chat_type = (chat_type or "private").lower()
        if normalized_chat_type != "private" or message_thread_id is not None:
            raise TelegramUserError(TelegramUserErrorCode.GROUP_NOT_SUPPORTED, detail=normalized_chat_type)
        user_part = user_id if user_id is not None else 0
        channel_identity = {
            "channel": "telegram",
            "external_account_ref": f"chat:{chat_id}:user:{user_part}",
            "adapter_identity": {
                "telegram_chat_id": str(chat_id),
                "telegram_user_id": str(user_part),
                "telegram_chat_type": "private",
            },
        }
        return TelegramChatScope(
            channel_identity=channel_identity,
            state_key=(chat_id, user_id),
            visibility="private",
        )
