# FILE: apps/telegram-bot/src/telegram_adapter/i18n/validation.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Validate packet-local Telegram translation catalogs before the locale service serves runtime text.
# SCOPE: Locale presence, keyset parity, value presence, and placeholder-shape validation across catalogs.
# DEPENDS: M-TELEGRAM-ADAPTER
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

from string import Formatter
from typing import Mapping

from telegram_adapter.i18n.keys import TelegramTextKey


class CatalogValidationError(ValueError):
    pass


def validate_translation_catalogs(
    catalogs: Mapping[str, Mapping[TelegramTextKey, str]],
    *,
    default_locale: str,
) -> None:
    if default_locale not in catalogs:
        raise CatalogValidationError(f"Default locale '{default_locale}' is missing from translation catalogs.")

    reference_catalog = catalogs[default_locale]
    reference_keys = set(reference_catalog)
    if not reference_keys:
        raise CatalogValidationError("Translation catalogs must not be empty.")

    reference_placeholders = {
        key: _extract_placeholders(template)
        for key, template in reference_catalog.items()
    }

    for locale, catalog in catalogs.items():
        keyset = set(catalog)
        if keyset != reference_keys:
            missing = sorted(key.value for key in reference_keys - keyset)
            unexpected = sorted(key.value for key in keyset - reference_keys)
            raise CatalogValidationError(
                f"Locale '{locale}' keyset mismatch: missing={missing or '[]'} unexpected={unexpected or '[]'}."
            )
        for key, value in catalog.items():
            if not isinstance(value, str) or not value.strip():
                raise CatalogValidationError(f"Locale '{locale}' has an empty translation for key {key.name}.")
            placeholders = _extract_placeholders(value)
            if placeholders != reference_placeholders[key]:
                raise CatalogValidationError(
                    f"Locale '{locale}' placeholder mismatch for key {key.name}: "
                    f"expected={reference_placeholders[key]} actual={placeholders}."
                )


def _extract_placeholders(template: str) -> tuple[str, ...]:
    names: list[str] = []
    for _, field_name, _, _ in Formatter().parse(template):
        if field_name is None:
            continue
        base_name = field_name.split(".", 1)[0].split("[", 1)[0]
        if base_name:
            names.append(base_name)
    return tuple(names)
