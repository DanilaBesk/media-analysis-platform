# FILE: apps/telegram-bot/src/telegram_adapter/i18n/service.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide locale normalization, resolution, and formatting helpers for the packet-local Telegram i18n foundation.
# SCOPE: Supported-locale coercion, fallback resolution, typed text lookup, and defensive format validation.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

from typing import Any, Mapping, cast

from telegram_adapter.i18n.catalogs import TRANSLATION_CATALOGS, TranslationCatalogs
from telegram_adapter.i18n.keys import (
    DEFAULT_LOCALE,
    SUPPORTED_LOCALE_SET,
    SupportedLocale,
    TelegramTextKey,
)
from telegram_adapter.i18n.validation import validate_translation_catalogs


class TelegramLocaleService:
    def __init__(
        self,
        *,
        catalogs: TranslationCatalogs | None = None,
        default_locale: SupportedLocale = DEFAULT_LOCALE,
    ) -> None:
        self.catalogs = catalogs or TRANSLATION_CATALOGS
        self.default_locale = default_locale
        validate_translation_catalogs(self.catalogs, default_locale=self.default_locale)

    def normalize_locale(self, raw_locale: str | None) -> SupportedLocale:
        return self._coerce_locale(raw_locale) or self.default_locale

    def resolve_locale(
        self,
        *,
        user_locale: str | None,
        chat_locale: str | None = None,
        fallback_locale: str | None = None,
    ) -> SupportedLocale:
        for candidate in (user_locale, chat_locale, fallback_locale):
            locale = self._coerce_locale(candidate)
            if locale is not None:
                return locale
        return self.default_locale

    def text(
        self,
        key: TelegramTextKey,
        *,
        locale: str | None = None,
        **params: Any,
    ) -> str:
        resolved_locale = self.normalize_locale(locale)
        template = self.catalogs[resolved_locale][key]
        try:
            return template.format_map(params)
        except KeyError as exc:
            missing_name = exc.args[0]
            raise ValueError(
                f"Missing format parameter '{missing_name}' for key {key.name} in locale '{resolved_locale}'."
            ) from exc

    def catalog(self, locale: str | None = None) -> Mapping[TelegramTextKey, str]:
        resolved_locale = self.normalize_locale(locale)
        return self.catalogs[resolved_locale]

    def _coerce_locale(self, raw_locale: str | None) -> SupportedLocale | None:
        if raw_locale is None:
            return None
        normalized = raw_locale.strip().lower()
        if not normalized:
            return None
        primary = normalized.replace("_", "-").split("-", 1)[0]
        if primary not in SUPPORTED_LOCALE_SET:
            return None
        return cast(SupportedLocale, primary)
