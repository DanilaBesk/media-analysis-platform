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

    def add_media_item(
        self,
        *,
        owner: JsonObject,
        kind: str,
        source: JsonObject,
        display_name: str | None = None,
        collection_id: str | None = None,
        adapter_origin: str = "telegram",
        metadata: JsonObject | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "owner": _owner_body(owner),
            "kind": kind,
            "source": source,
            "adapter_origin": adapter_origin,
        }
        if display_name:
            payload["display_name"] = display_name
        if collection_id:
            payload["collection_id"] = collection_id
        if metadata:
            payload["metadata"] = metadata
        return self._extract(self._request_json("/v1/media-items", method="POST", json_body=payload), "media_item")

    def list_media_items(
        self,
        *,
        owner: JsonObject,
        cursor: str | None = None,
        page_size: int | None = None,
        status: str | None = None,
        kind: str | None = None,
    ) -> JsonObject:
        params = _owner_query(owner)
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        if status:
            params["status"] = status
        if kind:
            params["kind"] = kind
        return self._request_json(f"/v1/media-items?{urlencode(params)}")

    def get_inbox_collection(self, *, owner: JsonObject) -> JsonObject:
        payload = self._request_json(f"/v1/collections/inbox?{urlencode(_owner_query(owner))}")
        return self._extract(payload, "collection")

    def remove_collection_item(
        self,
        *,
        owner: JsonObject,
        collection_id: str,
        media_item_id: str,
        expected_version: int,
    ) -> JsonObject:
        params = _owner_query(owner)
        params["expected_version"] = str(expected_version)
        payload = self._request_json(
            f"/v1/collections/{collection_id}/items/{media_item_id}?{urlencode(params)}",
            method="DELETE",
        )
        return self._extract(payload, "collection")

    def create_selection(
        self,
        *,
        owner: JsonObject,
        items: list[JsonObject],
        source_collection_id: str | None = None,
        option_snapshot: JsonObject | None = None,
        duplicate_policy: str = "preserve",
        created_by: str = "telegram",
    ) -> JsonObject:
        payload: JsonObject = {
            "owner": _owner_body(owner),
            "items": items,
            "duplicate_policy": duplicate_policy,
            "created_by": created_by,
        }
        if source_collection_id:
            payload["source_collection_id"] = source_collection_id
        if option_snapshot:
            payload["option_snapshot"] = option_snapshot
        return self._extract(self._request_json("/v1/selections", method="POST", json_body=payload), "selection")

    def create_analysis_run(
        self,
        *,
        owner: JsonObject,
        selection_id: str,
        run_type: str = "transcription",
        params: JsonObject | None = None,
        delivery: JsonObject | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "owner": _owner_body(owner),
            "selection_id": selection_id,
            "run_type": run_type,
            "delivery": delivery or {"strategy": "polling"},
        }
        if params:
            payload["params"] = params
        return self._extract(self._request_json("/v1/analysis-runs", method="POST", json_body=payload), "analysis_run")

    def list_analysis_runs(
        self,
        *,
        owner: JsonObject,
        cursor: str | None = None,
        page_size: int | None = None,
        status: str | None = None,
        run_type: str | None = None,
    ) -> JsonObject:
        params = _owner_query(owner)
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        if status:
            params["status"] = status
        if run_type:
            params["run_type"] = run_type
        return self._request_json(f"/v1/analysis-runs?{urlencode(params)}")

    def get_analysis_run(self, *, owner: JsonObject, analysis_run_id: str) -> JsonObject:
        payload = self._request_json(f"/v1/analysis-runs/{analysis_run_id}?{urlencode(_owner_query(owner))}")
        return self._extract(payload, "analysis_run")

    def list_artifacts(
        self,
        *,
        owner: JsonObject,
        analysis_run_id: str | None = None,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> JsonObject:
        params = _owner_query(owner)
        if analysis_run_id:
            params["analysis_run_id"] = analysis_run_id
        if cursor:
            params["cursor"] = cursor
        if page_size:
            params["page_size"] = str(page_size)
        return self._request_json(f"/v1/artifacts?{urlencode(params)}")

    def get_artifact(self, *, owner: JsonObject, artifact_id: str) -> JsonObject:
        payload = self._request_json(f"/v1/artifacts/{artifact_id}?{urlencode(_owner_query(owner))}")
        return self._extract(payload, "artifact")

    def list_diagnostics(
        self,
        *,
        owner: JsonObject,
        subject_type: str | None = None,
        subject_id: str | None = None,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> JsonObject:
        params = _owner_query(owner)
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
        request = Request(
            urljoin(f"{self.base_url}/", path.lstrip("/")),
            data=body,
            headers=headers,
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


def _owner_body(owner: JsonObject) -> JsonObject:
    body: JsonObject = {
        "owner_type": owner["owner_type"],
        "owner_id": owner["owner_id"],
    }
    if owner.get("tenant_id"):
        body["tenant_id"] = owner["tenant_id"]
    if owner.get("adapter_identity"):
        body["adapter_identity"] = owner["adapter_identity"]
    return body


def _owner_query(owner: JsonObject) -> dict[str, str]:
    params = {
        "owner_type": str(owner["owner_type"]),
        "owner_id": str(owner["owner_id"]),
    }
    if owner.get("tenant_id"):
        params["tenant_id"] = str(owner["tenant_id"])
    return params
