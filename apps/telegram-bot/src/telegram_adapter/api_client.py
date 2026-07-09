# FILE: apps/telegram-bot/src/telegram_adapter/api_client.py
# VERSION: 2.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide a thin HTTP client for Telegram inbox, selection, analysis_run, artifact, and diagnostic API calls.
# SCOPE: Shape final inbox-first JSON requests for the Telegram adapter without local product state or old execution routes.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP, M-CONTRACTS
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT

from __future__ import annotations

import json
import uuid
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

JsonObject = dict[str, Any]
UrlopenLike = Callable[[Request], Any]


class TelegramApiClientError(RuntimeError):
    def __init__(self, path: str, status: int, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.path = path
        self.status = status
        self.code = code


class TelegramApiClient:
    def __init__(self, base_url: str, urlopen_impl: UrlopenLike | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.urlopen_impl = urlopen_impl or urlopen

    def create_media_asset(
        self,
        *,
        channel_account_id: str,
        kind: str,
        origin: JsonObject,
        display_name: str | None = None,
        collection_id: str | None = None,
        metadata: JsonObject | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "channel_account_id": channel_account_id,
            "origin": origin,
            "kind": kind,
        }
        if display_name:
            payload["display_name"] = display_name
        if collection_id:
            payload["collection_id"] = collection_id
        if metadata:
            payload["metadata"] = metadata
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        return self._extract(self._request_json("/v1/media-assets", method="POST", json_body=payload), "media_asset")

    def upload_media_asset(
        self,
        *,
        channel_account_id: str,
        kind: str,
        content: bytes,
        file_name: str,
        content_type: str | None = None,
        display_name: str | None = None,
        collection_id: str | None = None,
        metadata: JsonObject | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "channel_account_id": channel_account_id,
            "kind": kind,
        }
        if display_name:
            payload["display_name"] = display_name
        if collection_id:
            payload["collection_id"] = collection_id
        if metadata:
            payload["metadata"] = metadata
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        boundary = f"codex-{uuid.uuid4().hex}"
        body = _encode_multipart_form(
            boundary=boundary,
            metadata_json=json.dumps(payload),
            file_name=file_name,
            file_content=content,
            file_content_type=content_type or "application/octet-stream",
        )
        return self._extract(
            self._request(
                "/v1/media-assets/upload",
                method="POST",
                body=body,
                headers={
                    "Accept": "application/json",
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                },
            ),
            "media_asset",
        )

    def list_media_assets(
        self,
        *,
        channel_account_id: str,
        cursor: str | None = None,
        page_size: int | None = None,
        status: str | None = None,
        kind: str | None = None,
    ) -> JsonObject:
        params = _channel_account_query(channel_account_id)
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        if status:
            params["status"] = status
        if kind:
            params["kind"] = kind
        return self._request_json(f"/v1/media-assets?{urlencode(params)}")

    def get_inbox_collection(self, *, channel_account_id: str) -> JsonObject:
        payload = self._request_json(f"/v1/collections/inbox?{urlencode(_channel_account_query(channel_account_id))}")
        return self._extract(payload, "collection")

    def remove_collection_item(
        self,
        *,
        channel_account_id: str,
        collection_id: str,
        media_asset_id: str,
        expected_version: int,
    ) -> JsonObject:
        params = _channel_account_query(channel_account_id)
        params["expected_version"] = str(expected_version)
        payload = self._request_json(
            f"/v1/collections/{collection_id}/items/{media_asset_id}?{urlencode(params)}",
            method="DELETE",
        )
        return self._extract(payload, "collection")

    def create_selection_snapshot(
        self,
        *,
        channel_account_id: str,
        items: list[JsonObject],
        source_collection_id: str | None = None,
        option_snapshot: JsonObject | None = None,
        created_via_channel_account_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "channel_account_id": channel_account_id,
            "items": items,
            "created_via_channel_account_id": created_via_channel_account_id or channel_account_id,
        }
        if source_collection_id:
            payload["source_collection_id"] = source_collection_id
        if option_snapshot:
            payload["option_snapshot"] = option_snapshot
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        return self._extract(
            self._request_json("/v1/selection-snapshots", method="POST", json_body=payload),
            "selection_snapshot",
        )

    def create_analysis_run(
        self,
        *,
        channel_account_id: str,
        selection_snapshot_id: str,
        run_type: str = "transcription",
        params: JsonObject | None = None,
        delivery: JsonObject | None = None,
        created_via_channel_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "channel_account_id": channel_account_id,
            "selection_snapshot_id": selection_snapshot_id,
            "run_type": run_type,
            "delivery": delivery or {"strategy": "polling"},
            "created_via_channel_id": created_via_channel_id or channel_account_id,
        }
        if params:
            payload["params"] = params
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        return self._extract(self._request_json("/v1/analysis-runs", method="POST", json_body=payload), "analysis_run")

    def list_analysis_runs(
        self,
        *,
        channel_account_id: str,
        cursor: str | None = None,
        page_size: int | None = None,
        status: str | None = None,
        run_type: str | None = None,
    ) -> JsonObject:
        params = _channel_account_query(channel_account_id)
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        if status:
            params["status"] = status
        if run_type:
            params["run_type"] = run_type
        return self._request_json(f"/v1/analysis-runs?{urlencode(params)}")

    def get_analysis_run(self, *, channel_account_id: str, analysis_run_id: str) -> JsonObject:
        payload = self._request_json(
            f"/v1/analysis-runs/{analysis_run_id}?{urlencode(_channel_account_query(channel_account_id))}"
        )
        return self._extract(payload, "analysis_run")

    def list_analysis_run_events(
        self,
        *,
        channel_account_id: str,
        analysis_run_id: str,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> JsonObject:
        params = _channel_account_query(channel_account_id)
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        return self._request_json(f"/v1/analysis-runs/{analysis_run_id}/events?{urlencode(params)}")

    def cancel_analysis_run(
        self,
        *,
        channel_account_id: str,
        analysis_run_id: str,
        message: str = "Canceled from Telegram",
    ) -> JsonObject:
        payload = self._request_json(
            f"/v1/analysis-runs/{analysis_run_id}/cancel",
            method="POST",
            json_body={"channel_account_id": channel_account_id, "message": message},
        )
        return self._extract(payload, "analysis_run")

    def list_artifacts(
        self,
        *,
        channel_account_id: str,
        analysis_run_id: str | None = None,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> JsonObject:
        params = _channel_account_query(channel_account_id)
        if analysis_run_id:
            params["analysis_run_id"] = analysis_run_id
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        return self._request_json(f"/v1/artifacts?{urlencode(params)}")

    def get_artifact(self, *, channel_account_id: str, artifact_id: str) -> JsonObject:
        payload = self._request_json(f"/v1/artifacts/{artifact_id}?{urlencode(_channel_account_query(channel_account_id))}")
        return self._extract(payload, "artifact")

    def get_internal_artifact_download_access(self, *, artifact_id: str) -> JsonObject:
        return self._request_json(f"/internal/v1/artifacts/{artifact_id}/download-access")

    def get_reusable_transcript(
        self,
        *,
        channel_account_id: str,
        stored_object_id: str,
        checksum: str | None = None,
    ) -> JsonObject | None:
        params = _channel_account_query(channel_account_id)
        params["stored_object_id"] = stored_object_id
        if checksum:
            params["checksum"] = checksum
        payload = self._request_json(f"/internal/v1/reusable-transcripts?{urlencode(params)}")
        value = payload.get("reusable_transcript")
        return value if isinstance(value, dict) else None

    def resolve_channel_account(self, *, channel_identity: JsonObject) -> JsonObject:
        metadata: JsonObject = {"channel_identity": channel_identity}
        if channel_identity.get("adapter_identity"):
            metadata["adapter_identity"] = channel_identity["adapter_identity"]
        payload: JsonObject = {
            "channel": "telegram",
            "external_account_ref": str(channel_identity["external_account_ref"]),
            "display_name": str(channel_identity["external_account_ref"]),
            "status": "active",
            "metadata": metadata,
        }
        return self._extract(
            self._request_json("/internal/v1/channel-accounts", method="PUT", json_body=payload),
            "channel_account",
        )

    def list_channel_accounts(self, *, page_size: int | None = None) -> JsonObject:
        params: dict[str, str] = {}
        if page_size:
            params["page_size"] = str(page_size)
        query = f"?{urlencode(params)}" if params else ""
        return self._request_json(f"/internal/v1/channel-accounts{query}")

    def upsert_channel_surface(
        self,
        *,
        channel_account_id: str,
        surface_type: str,
        surface_key: str,
        address: JsonObject,
        display_state: JsonObject,
        address_fingerprint: str | None = None,
        subjects: list[JsonObject] | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "channel_account_id": channel_account_id,
            "channel": "telegram",
            "surface_type": surface_type,
            "surface_key": surface_key,
            "address": address,
            "display_state": display_state,
        }
        if address_fingerprint:
            payload["address_fingerprint"] = address_fingerprint
        if subjects:
            payload["subjects"] = subjects
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        return self._extract(
            self._request_json("/internal/v1/channel-surfaces", method="PUT", json_body=payload),
            "channel_surface",
        )

    def list_channel_surfaces(
        self,
        *,
        channel_account_id: str,
        subject_type: str | None = None,
        subject_id: str | None = None,
        lifecycle_status: str | None = None,
        active_only: bool = False,
        page_size: int | None = None,
    ) -> JsonObject:
        params = {"channel_account_id": channel_account_id}
        if subject_type:
            params["subject_type"] = subject_type
        if subject_id:
            params["subject_id"] = subject_id
        if lifecycle_status:
            params["lifecycle_status"] = lifecycle_status
        if page_size:
            params["page_size"] = str(page_size)
        path = "/internal/v1/channel-surfaces/active" if active_only else "/internal/v1/channel-surfaces"
        return self._request_json(f"{path}?{urlencode(params)}")

    def replace_channel_surface_display_state(
        self,
        *,
        channel_surface_id: str,
        expected_version: int,
        display_state: JsonObject,
        actor_type: str = "telegram_adapter",
        actor_id: str | None = None,
        metadata: JsonObject | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "expected_version": expected_version,
            "display_state": display_state,
            "actor_type": actor_type,
        }
        if actor_id:
            payload["actor_id"] = actor_id
        if metadata:
            payload["metadata"] = metadata
        return self._extract(
            self._request_json(
                f"/internal/v1/channel-surfaces/{channel_surface_id}/display-state",
                method="PATCH",
                json_body=payload,
            ),
            "channel_surface",
        )

    def supersede_channel_surface(
        self,
        *,
        channel_surface_id: str,
        reason: str,
        actor_type: str = "telegram_adapter",
        actor_id: str | None = None,
        metadata: JsonObject | None = None,
    ) -> JsonObject:
        payload: JsonObject = {"reason": reason, "actor_type": actor_type}
        if actor_id:
            payload["actor_id"] = actor_id
        if metadata:
            payload["metadata"] = metadata
        return self._extract(
            self._request_json(
                f"/internal/v1/channel-surfaces/{channel_surface_id}/supersede",
                method="POST",
                json_body=payload,
            ),
            "channel_surface_event",
        )

    def list_diagnostics(
        self,
        *,
        channel_account_id: str,
        subject_type: str | None = None,
        subject_id: str | None = None,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> JsonObject:
        params = _channel_account_query(channel_account_id)
        if subject_type:
            params["subject_type"] = subject_type
        if subject_id:
            params["subject_id"] = subject_id
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        return self._request_json(f"/v1/diagnostics?{urlencode(params)}")

    def _request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        json_body: JsonObject | None = None,
    ) -> JsonObject:
        body = json.dumps(json_body).encode("utf-8") if json_body is not None else None
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        return self._request(path, method=method, body=body, headers=headers)

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> JsonObject:
        request = Request(
            urljoin(f"{self.base_url}/", path.lstrip("/")),
            data=body,
            headers=headers or {},
            method=method,
        )
        try:
            with self.urlopen_impl(request) as response:
                payload = response.read()
                if not payload:
                    return {}
                return json.loads(payload.decode("utf-8"))
        except HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="replace")
            try:
                error_body = json.loads(payload)
            except json.JSONDecodeError:
                error_body = {}
            error = error_body.get("error", {}) if isinstance(error_body, dict) else {}
            raise TelegramApiClientError(
                path=path,
                status=exc.code,
                message=error.get("message", f"API request failed with status {exc.code}"),
                code=error.get("code"),
            ) from exc
        except (TimeoutError, URLError, OSError) as exc:
            raise TelegramApiClientError(
                path=path,
                status=0,
                message="Backend is unavailable",
                code="backend_unavailable",
            ) from exc

    def _extract(self, payload: JsonObject, key: str) -> JsonObject:
        value = payload.get(key)
        if not isinstance(value, dict):
            raise RuntimeError(f"API response does not include {key}")
        return value


def _channel_account_query(channel_account_id: str) -> dict[str, str]:
    return {"channel_account_id": str(channel_account_id)}


def _encode_multipart_form(
    *,
    boundary: str,
    metadata_json: str,
    file_name: str,
    file_content: bytes,
    file_content_type: str,
) -> bytes:
    return b"".join(
        [
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="metadata"\r\n'
                "Content-Type: application/json\r\n\r\n"
                f"{metadata_json}\r\n"
            ).encode("utf-8"),
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; filename="{file_name}"\r\n'
                f"Content-Type: {file_content_type}\r\n\r\n"
            ).encode("utf-8"),
            file_content,
            f"\r\n--{boundary}--\r\n".encode("utf-8"),
        ]
    )
