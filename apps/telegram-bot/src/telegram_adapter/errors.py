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
        "Сервис временно недоступен. Попробуйте ещё раз через минуту; содержимое inbox уже сохранено на сервере."
    ),
    TelegramUserErrorCode.STALE_ACTION: "Эта кнопка устарела. Откройте /inbox ещё раз и повторите действие.",
    TelegramUserErrorCode.UNSUPPORTED_INPUT: (
        "Неподдерживаемый ввод. Отправьте текст, ссылку, фото, видео, документ, аудио или голосовое."
    ),
    TelegramUserErrorCode.GROUP_NOT_SUPPORTED: (
        "Этот бот работает только в личном чате. Откройте диалог с ботом и отправьте медиа туда."
    ),
    TelegramUserErrorCode.ACCESS_DENIED: "Доступ к этому боту ограничен.",
    TelegramUserErrorCode.UNKNOWN_ACTION: "Это действие сейчас недоступно. Откройте /inbox и используйте актуальные кнопки.",
}

REJECTION_COPY: dict[str, str] = {
    "empty_text": "неподдерживаемый ввод: отправьте непустой текст.",
    "unsupported_url_scheme": "неподдерживаемый ввод: отправьте ссылку http или https.",
    "invalid_url": "неподдерживаемый ввод: отправьте полную ссылку.",
    "missing_file_id": "неподдерживаемый ввод: Telegram не передал file id.",
    "missing_file_content": "неподдерживаемый ввод: не удалось скачать файл из Telegram.",
    "unsupported_message": "неподдерживаемый ввод: отправьте текст, ссылку, фото, видео, документ, аудио или голосовое.",
}

STALE_RUNTIME_REASONS = {
    "slot_not_visible",
    "slot_missing_media_asset_id",
    "inbox_empty",
}


def user_error_text(error: BaseException | TelegramUserErrorCode) -> str:
    if isinstance(error, TelegramUserErrorCode):
        return USER_ERROR_COPY[error]
    classified = classify_user_error(error)
    if classified.code == TelegramUserErrorCode.UNSUPPORTED_INPUT and classified.detail in REJECTION_COPY:
        return REJECTION_COPY[classified.detail]
    code = classified.code
    return USER_ERROR_COPY[code]


def classify_user_error(error: BaseException) -> TelegramUserError:
    if isinstance(error, TelegramUserError):
        return error
    if isinstance(error, TelegramApiClientError):
        return TelegramUserError(TelegramUserErrorCode.BACKEND_UNAVAILABLE, detail=error.code)
    if isinstance(error, RuntimeError) and str(error) in STALE_RUNTIME_REASONS:
        return TelegramUserError(TelegramUserErrorCode.STALE_ACTION, detail=str(error))
    if isinstance(error, RuntimeError) and str(error) == "telegram_file_download_failed":
        return TelegramUserError(TelegramUserErrorCode.UNSUPPORTED_INPUT, detail="missing_file_content")
    if isinstance(error, RuntimeError) and str(error) in REJECTION_COPY:
        return TelegramUserError(TelegramUserErrorCode.UNSUPPORTED_INPUT, detail=str(error))
    return TelegramUserError(TelegramUserErrorCode.BACKEND_UNAVAILABLE, detail=type(error).__name__)


def rejected_reason_text(reason: str | None) -> str:
    if not reason:
        return REJECTION_COPY["unsupported_message"]
    return REJECTION_COPY.get(reason, USER_ERROR_COPY[TelegramUserErrorCode.UNSUPPORTED_INPUT])


def safe_callback_answer(error: BaseException | TelegramUserErrorCode) -> dict[str, Any]:
    return {"text": user_error_text(error), "show_alert": True}
