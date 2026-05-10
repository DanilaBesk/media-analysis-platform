from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from telegram_adapter.api_client import TelegramApiClientError


class TelegramUserErrorCode(StrEnum):
    BACKEND_UNAVAILABLE = "backend_unavailable"
    STALE_ACTION = "stale_action"
    UNSUPPORTED_INPUT = "unsupported_input"
    GROUP_NOT_SUPPORTED = "group_not_supported"
    ACCESS_DENIED = "access_denied"
    UNKNOWN_ACTION = "unknown_action"


@dataclass(frozen=True, slots=True)
class TelegramUserError(RuntimeError):
    code: TelegramUserErrorCode
    detail: str | None = None

    def __str__(self) -> str:
        return self.code.value


USER_ERROR_COPY: dict[TelegramUserErrorCode, str] = {
    TelegramUserErrorCode.BACKEND_UNAVAILABLE: (
        "Service is temporarily unavailable. Try again in a minute; your inbox is stored on the server."
    ),
    TelegramUserErrorCode.STALE_ACTION: "This button is stale. Open /inbox and try again.",
    TelegramUserErrorCode.UNSUPPORTED_INPUT: (
        "Unsupported input. Send text, links, photos, videos, documents, audio, or voice."
    ),
    TelegramUserErrorCode.GROUP_NOT_SUPPORTED: (
        "This bot is private-chat only. Open a direct chat with the bot and send the media there."
    ),
    TelegramUserErrorCode.ACCESS_DENIED: "Access to this bot is restricted.",
    TelegramUserErrorCode.UNKNOWN_ACTION: "This action is not available. Open /inbox and use the latest buttons.",
}

REJECTION_COPY: dict[str, str] = {
    "empty_text": "unsupported input: send non-empty text.",
    "unsupported_url_scheme": "unsupported input: send http or https links.",
    "invalid_url": "unsupported input: send a complete link.",
    "missing_file_id": "unsupported input: Telegram did not provide a file id.",
    "unsupported_message": "unsupported input: send text, links, photos, videos, documents, audio, or voice.",
}

STALE_RUNTIME_REASONS = {
    "slot_not_visible",
    "slot_missing_media_item_id",
    "inbox_empty",
}


def user_error_text(error: BaseException | TelegramUserErrorCode) -> str:
    code = error if isinstance(error, TelegramUserErrorCode) else classify_user_error(error).code
    return USER_ERROR_COPY[code]


def classify_user_error(error: BaseException) -> TelegramUserError:
    if isinstance(error, TelegramUserError):
        return error
    if isinstance(error, TelegramApiClientError):
        return TelegramUserError(TelegramUserErrorCode.BACKEND_UNAVAILABLE, detail=error.code)
    if isinstance(error, RuntimeError) and str(error) in STALE_RUNTIME_REASONS:
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION, detail=str(error))
    return TelegramUserError(TelegramUserErrorCode.BACKEND_UNAVAILABLE, detail=type(error).__name__)


def rejected_reason_text(reason: str | None) -> str:
    if not reason:
        return REJECTION_COPY["unsupported_message"]
    return REJECTION_COPY.get(reason, USER_ERROR_COPY[TelegramUserErrorCode.UNSUPPORTED_INPUT])


def safe_callback_answer(error: BaseException | TelegramUserErrorCode) -> dict[str, Any]:
    return {"text": user_error_text(error), "show_alert": True}
