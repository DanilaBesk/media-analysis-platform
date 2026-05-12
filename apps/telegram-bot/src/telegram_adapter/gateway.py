# FILE: apps/telegram-bot/src/telegram_adapter/gateway.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Adapt Telegram inputs and controls to API-owned inbox, selection, and analysis_run state.
# SCOPE: Ingest text, links, Telegram file metadata, refresh inbox status, remove items, create selections, and start runs.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP, M-CONTRACTS
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlparse

from telegram_adapter.api_client import TelegramApiClient
from telegram_adapter.policy import TelegramChatPolicy, TelegramChatScope

JsonObject = dict[str, Any]
IngressStatus = Literal["accepted", "rejected"]
ACTIVE_RUN_STATUSES = {"queued", "running", "cancel_requested"}
TERMINAL_RUN_STATUSES = {"partially_succeeded", "succeeded", "failed", "canceled", "expired"}
VISIBLE_RUN_STATUSES = ACTIVE_RUN_STATUSES | TERMINAL_RUN_STATUSES
URL_RE = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s<>]+", re.IGNORECASE)
SUPPORTED_URL_SCHEMES = {"http", "https"}


@dataclass(frozen=True, slots=True)
class TelegramFileInput:
    kind: str
    file_id: str
    file_unique_id: str | None = None
    file_name: str | None = None
    content_type: str | None = None
    content: bytes | None = None
    size_bytes: int | None = None
    caption: str | None = None
    media_group_id: str | None = None
    message_id: int | None = None


@dataclass(frozen=True, slots=True)
class IngressRecord:
    status: IngressStatus
    label: str
    reason: str | None = None
    media_item: JsonObject | None = None


@dataclass(frozen=True, slots=True)
class InboxStatus:
    owner: JsonObject
    collection: JsonObject | None
    items: list[JsonObject]
    page: JsonObject
    active_runs: list[JsonObject]
    recent_runs: list[JsonObject]
    artifacts_by_run: dict[str, list[JsonObject]]
    diagnostics_by_run: dict[str, list[JsonObject]]
    rejected: list[IngressRecord]


class TelegramInboxGateway:
    def __init__(
        self,
        api_client: TelegramApiClient,
        *,
        page_size: int = 5,
        chat_policy: TelegramChatPolicy | None = None,
    ) -> None:
        self.api_client = api_client
        self.page_size = page_size
        self.chat_policy = chat_policy or TelegramChatPolicy()

    def scope_for(
        self,
        *,
        chat_id: int,
        user_id: int | None,
        chat_type: str | None = "private",
        message_thread_id: int | None = None,
    ) -> TelegramChatScope:
        return self.chat_policy.resolve(
            chat_id=chat_id,
            user_id=user_id,
            chat_type=chat_type,
            message_thread_id=message_thread_id,
        )

    def owner_for(
        self,
        *,
        chat_id: int,
        user_id: int | None,
        chat_type: str | None = "private",
        message_thread_id: int | None = None,
    ) -> JsonObject:
        return self.scope_for(
            chat_id=chat_id,
            user_id=user_id,
            chat_type=chat_type,
            message_thread_id=message_thread_id,
        ).owner

    def add_text(self, *, owner: JsonObject, text: str, message_id: int | None = None) -> IngressRecord:
        clean = text.strip()
        if not clean:
            return IngressRecord(status="rejected", label="Text message", reason="empty_text")
        item = self.api_client.add_media_item(
            owner=owner,
            kind="text",
            source={"origin_type": "text", "text": clean},
            display_name=_display_name(clean, fallback="Text"),
            metadata=_telegram_metadata(message_id=message_id),
        )
        return IngressRecord(status="accepted", label=item.get("display_name", "Text"), media_item=item)

    def add_link(self, *, owner: JsonObject, url: str, message_id: int | None = None) -> IngressRecord:
        clean = url.strip().rstrip(".,)")
        parsed = urlparse(clean)
        if parsed.scheme not in SUPPORTED_URL_SCHEMES:
            return IngressRecord(status="rejected", label=clean or "Link", reason="unsupported_url_scheme")
        if not parsed.netloc:
            return IngressRecord(status="rejected", label=clean or "Link", reason="invalid_url")
        item = self.api_client.add_media_item(
            owner=owner,
            kind="url",
            source={"origin_type": "url", "url": clean},
            display_name=clean,
            metadata=_telegram_metadata(message_id=message_id),
        )
        return IngressRecord(status="accepted", label=item.get("display_name", clean), media_item=item)

    def add_file(self, *, owner: JsonObject, file_input: TelegramFileInput) -> IngressRecord:
        if not file_input.file_id.strip():
            return IngressRecord(status="rejected", label=file_input.file_name or file_input.kind, reason="missing_file_id")
        if file_input.content is None:
            return IngressRecord(
                status="rejected",
                label=file_input.file_name or file_input.kind,
                reason="missing_file_content",
            )
        display_name = file_input.file_name or _kind_label(file_input.kind)
        item = self.api_client.upload_media_item(
            owner=owner,
            kind=file_input.kind,
            content=file_input.content,
            file_name=file_input.file_name or _default_upload_filename(file_input.kind, file_input.content_type),
            content_type=file_input.content_type,
            display_name=display_name,
            metadata=_telegram_metadata(
                message_id=file_input.message_id,
                media_group_id=file_input.media_group_id,
                file_unique_id=file_input.file_unique_id,
                caption=file_input.caption,
            ),
        )
        return IngressRecord(status="accepted", label=item.get("display_name", display_name), media_item=item)

    def add_message_inputs(
        self,
        *,
        owner: JsonObject,
        text: str | None = None,
        files: list[TelegramFileInput] | None = None,
        message_id: int | None = None,
    ) -> list[IngressRecord]:
        records: list[IngressRecord] = []
        if text:
            links = list(extract_links(text))
            if links:
                records.extend(self.add_link(owner=owner, url=link, message_id=message_id) for link in links)
                remaining_text = URL_RE.sub("", text).strip()
                if remaining_text:
                    records.append(self.add_text(owner=owner, text=remaining_text, message_id=message_id))
            else:
                records.append(self.add_text(owner=owner, text=text, message_id=message_id))
        for file_input in files or []:
            records.append(self.add_file(owner=owner, file_input=file_input))
        if not records:
            records.append(IngressRecord(status="rejected", label="Telegram message", reason="unsupported_message"))
        return records

    def restore_status(
        self,
        *,
        owner: JsonObject,
        cursor: str | None = None,
        rejected: list[IngressRecord] | None = None,
    ) -> InboxStatus:
        collection: JsonObject | None
        try:
            collection = self.api_client.get_inbox_collection(owner=owner)
        except Exception:
            collection = None
        items: list[JsonObject]
        page_meta: JsonObject
        if collection is not None:
            items, page_meta = self._restore_collection_items(owner=owner, collection=collection, cursor=cursor)
        else:
            page = self.api_client.list_media_items(owner=owner, cursor=cursor, page_size=self.page_size)
            items = list(page.get("items", []))
            page_meta = dict(page.get("page", {}))
        runs_page = self.api_client.list_analysis_runs(owner=owner, page_size=10)
        recent_runs = [
            run
            for run in runs_page.get("items", [])
            if run.get("status") in VISIBLE_RUN_STATUSES
        ]
        active_runs = [
            run
            for run in recent_runs
            if run.get("status") in ACTIVE_RUN_STATUSES
        ]
        terminal_runs = [
            run
            for run in recent_runs
            if run.get("status") in TERMINAL_RUN_STATUSES and run.get("analysis_run_id")
        ]
        artifacts_by_run: dict[str, list[JsonObject]] = {}
        diagnostics_by_run: dict[str, list[JsonObject]] = {}
        for run in terminal_runs[:5]:
            run_id = str(run["analysis_run_id"])
            artifacts_by_run[run_id] = list(
                self.api_client.list_artifacts(owner=owner, analysis_run_id=run_id, page_size=3).get("items", [])
            )
            diagnostics_by_run[run_id] = list(
                self.api_client.list_diagnostics(
                    owner=owner,
                    subject_type="analysis_run",
                    subject_id=run_id,
                    page_size=3,
                ).get("items", [])
            )
        return InboxStatus(
            owner=owner,
            collection=collection,
            items=items,
            page=page_meta,
            active_runs=active_runs,
            recent_runs=recent_runs,
            artifacts_by_run=artifacts_by_run,
            diagnostics_by_run=diagnostics_by_run,
            rejected=list(rejected or []),
        )

    def remove_collection_item(
        self,
        *,
        owner: JsonObject,
        collection_id: str,
        media_item_id: str,
        expected_version: int,
        cursor: str | None = None,
    ) -> InboxStatus:
        collection = self._get_verified_inbox_collection(
            owner=owner,
            collection_id=collection_id,
            expected_version=expected_version,
        )
        if not media_item_id.strip():
            raise RuntimeError("slot_missing_media_item_id")
        self.api_client.remove_collection_item(
            owner=owner,
            collection_id=collection["collection_id"],
            media_item_id=media_item_id,
            expected_version=int(collection["version"]),
        )
        return self.restore_status(owner=owner, cursor=cursor)

    def clear_visible_items(
        self,
        *,
        owner: JsonObject,
        collection_id: str,
        expected_version: int,
        cursor: str | None = None,
    ) -> InboxStatus:
        collection = self._get_verified_inbox_collection(
            owner=owner,
            collection_id=collection_id,
            expected_version=expected_version,
        )
        status = self.restore_status(owner=owner, cursor=cursor)
        if not status.items:
            return status
        version = int(collection["version"])
        for item in status.items:
            media_item_id = item.get("media_item_id")
            if not media_item_id:
                continue
            collection = self.api_client.remove_collection_item(
                owner=owner,
                collection_id=collection["collection_id"],
                media_item_id=media_item_id,
                expected_version=version,
            )
            version = int(collection["version"])
        return self.restore_status(owner=owner, cursor=cursor)

    def create_selection(
        self,
        *,
        owner: JsonObject,
        collection_id: str,
        expected_version: int,
    ) -> JsonObject:
        collection = self._get_verified_inbox_collection(
            owner=owner,
            collection_id=collection_id,
            expected_version=expected_version,
        )
        item_ids = [
            item["media_item_id"]
            for item in collection.get("items", [])
            if item.get("media_item_id")
        ]
        if not item_ids:
            raise RuntimeError("inbox_empty")
        return self.api_client.create_selection(
            owner=owner,
            source_collection_id=collection["collection_id"],
            items=[
                {"media_item_id": media_item_id, "position": index}
                for index, media_item_id in enumerate(item_ids)
            ],
            option_snapshot={"adapter": "telegram", "source": "inbox"},
            created_by="telegram",
        )

    def start_analysis(
        self,
        *,
        owner: JsonObject,
        selection_id: str,
        run_type: str = "transcription",
    ) -> JsonObject:
        if not selection_id.strip():
            raise RuntimeError("slot_not_visible")
        return self.api_client.create_analysis_run(
            owner=owner,
            selection_id=selection_id,
            run_type=run_type,
            delivery={"strategy": "polling"},
        )

    def get_run_status(self, *, owner: JsonObject, analysis_run_id: str) -> JsonObject:
        return self.api_client.get_analysis_run(owner=owner, analysis_run_id=analysis_run_id)

    def list_run_artifacts(
        self,
        *,
        owner: JsonObject,
        analysis_run_id: str,
        expected_version: int,
        page_size: int = 10,
    ) -> list[JsonObject]:
        self._get_verified_run(owner=owner, analysis_run_id=analysis_run_id, expected_version=expected_version)
        return list(
            self.api_client.list_artifacts(owner=owner, analysis_run_id=analysis_run_id, page_size=page_size).get(
                "items",
                [],
            )
        )

    def list_run_diagnostics(
        self,
        *,
        owner: JsonObject,
        analysis_run_id: str,
        expected_version: int,
        page_size: int = 10,
    ) -> list[JsonObject]:
        self._get_verified_run(owner=owner, analysis_run_id=analysis_run_id, expected_version=expected_version)
        return list(
            self.api_client.list_diagnostics(
                owner=owner,
                subject_type="analysis_run",
                subject_id=analysis_run_id,
                page_size=page_size,
            ).get("items", [])
        )

    def _get_verified_inbox_collection(
        self,
        *,
        owner: JsonObject,
        collection_id: str,
        expected_version: int,
    ) -> JsonObject:
        collection = self.api_client.get_inbox_collection(owner=owner)
        if str(collection.get("collection_id") or "") != collection_id:
            raise RuntimeError("slot_not_visible")
        if int(collection.get("version") or 0) != expected_version:
            raise RuntimeError("slot_not_visible")
        return collection

    def _get_verified_run(
        self,
        *,
        owner: JsonObject,
        analysis_run_id: str,
        expected_version: int,
    ) -> JsonObject:
        run = self.api_client.get_analysis_run(owner=owner, analysis_run_id=analysis_run_id)
        if int(run.get("version") or 0) != expected_version:
            raise RuntimeError("slot_not_visible")
        return run

    def _restore_collection_items(
        self,
        *,
        owner: JsonObject,
        collection: JsonObject,
        cursor: str | None,
    ) -> tuple[list[JsonObject], JsonObject]:
        collection_items = list(collection.get("items", []) or [])
        visible_collection_items, page_meta = self._page_collection_items(collection_items, cursor=cursor)
        visible_ids = [
            str(item.get("media_item_id") or "")
            for item in visible_collection_items
            if item.get("media_item_id")
        ]
        if not visible_ids:
            return [], page_meta
        return self._load_media_items_by_id(owner=owner, media_item_ids=visible_ids), page_meta

    def _page_collection_items(
        self,
        collection_items: list[JsonObject],
        *,
        cursor: str | None,
    ) -> tuple[list[JsonObject], JsonObject]:
        start = 0
        if cursor:
            for idx, item in enumerate(collection_items):
                if str(item.get("media_item_id") or "") == cursor:
                    start = idx + 1
                    break
        end = start + self.page_size
        has_more = end < len(collection_items)
        visible = collection_items[start:end]
        page: JsonObject = {"page_size": self.page_size, "has_more": has_more}
        if has_more and visible:
            page["next_cursor"] = str(visible[-1].get("media_item_id") or "")
        return visible, page

    def _load_media_items_by_id(
        self,
        *,
        owner: JsonObject,
        media_item_ids: list[str],
    ) -> list[JsonObject]:
        wanted = {media_item_id for media_item_id in media_item_ids if media_item_id}
        if not wanted:
            return []
        found: dict[str, JsonObject] = {}
        cursor: str | None = None
        page_size = max(self.page_size, len(media_item_ids), 50)
        while wanted - found.keys():
            page = self.api_client.list_media_items(owner=owner, cursor=cursor, page_size=page_size)
            page_items = list(page.get("items", []))
            for item in page_items:
                media_item_id = str(item.get("media_item_id") or "")
                if media_item_id in wanted:
                    found[media_item_id] = item
            next_cursor = page.get("page", {}).get("next_cursor") or None
            if not page.get("page", {}).get("has_more") or next_cursor is None:
                break
            cursor = str(next_cursor)
        return [found[media_item_id] for media_item_id in media_item_ids if media_item_id in found]


def extract_links(text: str) -> tuple[str, ...]:
    return tuple(match.group(0) for match in URL_RE.finditer(text or ""))


def _display_name(text: str, *, fallback: str) -> str:
    clean = " ".join(text.split())
    return clean[:64] if clean else fallback


def _kind_label(kind: str) -> str:
    labels = {
        "photo": "Telegram photo",
        "image": "Telegram image",
        "video": "Telegram video",
        "document": "Telegram document",
        "audio": "Telegram audio",
        "voice": "Telegram voice",
        "file": "Telegram file",
    }
    return labels.get(kind, "Telegram media")


def _default_upload_filename(kind: str, content_type: str | None) -> str:
    by_content_type = {
        "audio/ogg": ".ogg",
        "audio/opus": ".opus",
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "video/mp4": ".mp4",
        "application/pdf": ".pdf",
    }
    by_kind = {
        "photo": ".jpg",
        "image": ".png",
        "video": ".mp4",
        "document": ".bin",
        "audio": ".bin",
        "voice": ".ogg",
        "file": ".bin",
    }
    extension = by_content_type.get((content_type or "").lower()) or by_kind.get(kind, ".bin")
    return f"telegram-{kind}{extension}"


def _telegram_metadata(
    *,
    message_id: int | None = None,
    media_group_id: str | None = None,
    file_unique_id: str | None = None,
    caption: str | None = None,
) -> JsonObject:
    metadata: JsonObject = {"adapter": "telegram"}
    if message_id is not None:
        metadata["message_id"] = message_id
    if media_group_id:
        metadata["media_group_id"] = media_group_id
    if file_unique_id:
        metadata["file_unique_id"] = file_unique_id
    if caption:
        metadata["caption"] = caption
    return metadata
