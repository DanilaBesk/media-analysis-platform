# FILE: apps/telegram-bot/src/telegram_adapter/api_client.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide a thin, packet-local HTTP client for Telegram adapter flows without reintroducing business logic or shared-SDK extraction.
# SCOPE: Shape JSON and multipart requests for Telegram submission, child-job actions, cancel or retry, job reads, and artifact resolution through the HTTP API only.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP, M-CONTRACTS
# LINKS: M-TELEGRAM-ADAPTER, V-M-TELEGRAM-ADAPTER
# ROLE: RUNTIME
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the packet-local Telegram adapter HTTP client with JSON and multipart transport helpers.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   build-request-payloads - Normalize polling-default JSON and multipart request payloads for the Telegram adapter.
#   send-http-requests - Execute API calls and preserve upstream error envelopes.
# END_MODULE_MAP

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen
from uuid import uuid4

JsonObject = dict[str, Any]
UrlopenLike = Callable[[Request], Any]


class TelegramApiClientError(RuntimeError):
    def __init__(self, path: str, status: int, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.path = path
        self.status = status
        self.code = code


@dataclass(frozen=True, slots=True)
class UploadFilePart:
    filename: str
    content_type: str
    content_bytes: bytes


def build_delivery_payload(
    delivery_strategy: str = "polling",
    delivery_webhook_url: str | None = None,
) -> JsonObject:
    if delivery_strategy not in {"polling", "webhook"}:
        raise ValueError("delivery_strategy must be polling or webhook")

    payload: JsonObject = {"strategy": delivery_strategy}
    if delivery_strategy == "webhook" and delivery_webhook_url:
        payload["webhook"] = {"url": delivery_webhook_url}
    return payload


def build_multipart_body(
    *,
    files: list[UploadFilePart],
    display_name: str | None = None,
    client_ref: str | None = None,
    delivery_strategy: str = "polling",
    delivery_webhook_url: str | None = None,
) -> tuple[bytes, str]:
    boundary = f"----telegram-adapter-{uuid4().hex}"
    chunks: list[bytes] = []

    def append_header(name: str, value: str) -> None:
        chunks.append(f"{name}: {value}\r\n".encode("utf-8"))

    def attach_file(file_part: UploadFilePart) -> None:
        chunks.append(f"--{boundary}\r\n".encode("utf-8"))
        append_header(
            "Content-Disposition",
            f'form-data; name="files"; filename="{file_part.filename}"',
        )
        append_header("Content-Type", file_part.content_type)
        chunks.append(b"\r\n")
        chunks.append(file_part.content_bytes)
        chunks.append(b"\r\n")

    def attach_text(name: str, value: str) -> None:
        chunks.append(f"--{boundary}\r\n".encode("utf-8"))
        append_header("Content-Disposition", f'form-data; name="{name}"')
        chunks.append(b"\r\n")
        chunks.append(value.encode("utf-8"))
        chunks.append(b"\r\n")

    for file_part in files:
        attach_file(file_part)

    if display_name:
        attach_text("display_name", display_name)
    if client_ref:
        attach_text("client_ref", client_ref)
    attach_text("delivery_strategy", delivery_strategy)
    if delivery_strategy == "webhook" and delivery_webhook_url:
        attach_text("delivery_webhook_url", delivery_webhook_url)

    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


class TelegramApiClient:
    def __init__(self, base_url: str, urlopen_impl: UrlopenLike | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.urlopen_impl = urlopen_impl or urlopen

    def get_job(self, job_id: str) -> JsonObject:
        return self._request_json(f"/v1/jobs/{job_id}")

    def list_jobs(
        self,
        *,
        status: str | None = None,
        job_type: str | None = None,
        root_job_id: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> JsonObject:
        params = {
            "page": page,
            "page_size": page_size,
        }
        if status:
            params["status"] = status
        if job_type:
            params["job_type"] = job_type
        if root_job_id:
            params["root_job_id"] = root_job_id
        return self._request_json(f"/v1/jobs?{urlencode(params)}")

    def create_upload(
        self,
        *,
        files: list[UploadFilePart],
        display_name: str | None = None,
        client_ref: str | None = None,
        delivery_strategy: str = "polling",
        delivery_webhook_url: str | None = None,
    ) -> JsonObject:
        body, content_type = build_multipart_body(
            files=files,
            display_name=display_name,
            client_ref=client_ref,
            delivery_strategy=delivery_strategy,
            delivery_webhook_url=delivery_webhook_url,
        )
        return self._request_json(
            "/v1/transcription-jobs",
            method="POST",
            body=body,
            headers={"Content-Type": content_type},
        )

    def create_combined_upload(
        self,
        *,
        files: list[UploadFilePart],
        display_name: str | None = None,
        client_ref: str | None = None,
        delivery_strategy: str = "polling",
        delivery_webhook_url: str | None = None,
    ) -> JsonObject:
        body, content_type = build_multipart_body(
            files=files,
            display_name=display_name,
            client_ref=client_ref,
            delivery_strategy=delivery_strategy,
            delivery_webhook_url=delivery_webhook_url,
        )
        return self._request_json(
            "/v1/transcription-jobs/combined",
            method="POST",
            body=body,
            headers={"Content-Type": content_type},
        )

    def create_from_url(
        self,
        *,
        url: str,
        display_name: str | None = None,
        client_ref: str | None = None,
        delivery_strategy: str = "polling",
        delivery_webhook_url: str | None = None,
    ) -> JsonObject:
        payload: JsonObject = {
            "source_kind": "youtube_url",
            "url": url,
            "delivery": build_delivery_payload(delivery_strategy, delivery_webhook_url),
        }
        if display_name:
            payload["display_name"] = display_name
        if client_ref:
            payload["client_ref"] = client_ref
        return self._request_json("/v1/transcription-jobs/from-url", method="POST", json_body=payload)

    def create_report(
        self,
        job_id: str,
        *,
        client_ref: str | None = None,
        delivery_strategy: str = "polling",
        delivery_webhook_url: str | None = None,
    ) -> JsonObject:
        payload = self._build_child_payload(client_ref, delivery_strategy, delivery_webhook_url)
        return self._request_json(
            f"/v1/transcription-jobs/{job_id}/report-jobs",
            method="POST",
            json_body=payload,
        )

    def create_deep_research(
        self,
        job_id: str,
        *,
        client_ref: str | None = None,
        delivery_strategy: str = "polling",
        delivery_webhook_url: str | None = None,
    ) -> JsonObject:
        payload = self._build_child_payload(client_ref, delivery_strategy, delivery_webhook_url)
        return self._request_json(
            f"/v1/report-jobs/{job_id}/deep-research-jobs",
            method="POST",
            json_body=payload,
        )

    def cancel_job(self, job_id: str) -> JsonObject:
        return self._request_json(f"/v1/jobs/{job_id}/cancel", method="POST")

    def retry_job(self, job_id: str) -> JsonObject:
        return self._request_json(f"/v1/jobs/{job_id}/retry", method="POST")

    def resolve_artifact(self, artifact_id: str) -> JsonObject:
        return self._request_json(f"/v1/artifacts/{artifact_id}")

    def download_bytes(self, url: str) -> bytes:
        request = Request(url)
        with self.urlopen_impl(request) as response:
            return response.read()

    def _request_json(
        self,
        path: str,
        *,
        method: str = "GET",
        json_body: JsonObject | None = None,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> JsonObject:
        request_headers = {
            "Accept": "application/json",
            **(headers or {}),
        }
        request_body = body
        if json_body is not None:
            request_body = json.dumps(json_body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"

        request = Request(
            urljoin(f"{self.base_url}/", path.lstrip("/")),
            data=request_body,
            headers=request_headers,
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

    def _build_child_payload(
        self,
        client_ref: str | None,
        delivery_strategy: str,
        delivery_webhook_url: str | None,
    ) -> JsonObject:
        payload: JsonObject = {
            "delivery": build_delivery_payload(delivery_strategy, delivery_webhook_url),
        }
        if client_ref:
            payload["client_ref"] = client_ref
        return payload
