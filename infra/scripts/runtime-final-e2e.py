#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.client import RemoteDisconnected
from typing import Any


API_BASE_URL = os.environ.get("RUNTIME_E2E_API_BASE_URL", "http://localhost:8080")
POLL_TIMEOUT_SECONDS = int(os.environ.get("RUNTIME_E2E_POLL_TIMEOUT_SECONDS", "180"))
POLL_INTERVAL_SECONDS = float(os.environ.get("RUNTIME_E2E_POLL_INTERVAL_SECONDS", "2"))
MINIO_HOST_ENDPOINT = (
    os.environ.get("RUNTIME_E2E_MINIO_HOST_ENDPOINT")
    or os.environ.get("MINIO_PUBLIC_ENDPOINT")
    or f"http://localhost:{os.environ.get('MINIO_HOST_PORT', '19100')}"
)


class RuntimeProofError(RuntimeError):
    pass


@dataclass(frozen=True)
class ChannelAccount:
    channel_account_id: str

    def query(self) -> str:
        return urllib.parse.urlencode({"channel_account_id": self.channel_account_id})


def _request(
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    expected: tuple[int, ...] = (200,),
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request_headers = {
        "Accept": "application/json",
        **({"Content-Type": "application/json"} if body is not None else {}),
        **(headers or {}),
    }
    request = urllib.request.Request(
        urllib.parse.urljoin(f"{API_BASE_URL}/", path.lstrip("/")),
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read().decode("utf-8")
            if response.status not in expected:
                raise RuntimeProofError(f"{method} {path} returned {response.status}, expected {expected}: {payload}")
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        if exc.code not in expected:
            raise RuntimeProofError(f"{method} {path} returned {exc.code}, expected {expected}: {payload}") from exc
        return json.loads(payload) if payload else {}
    except (urllib.error.URLError, RemoteDisconnected, TimeoutError, OSError) as exc:
        raise RuntimeProofError(f"{method} {path} transport failed: {exc}") from exc


def _download_bytes(url: str) -> bytes:
    parsed = urllib.parse.urlparse(url)
    headers: dict[str, str] = {}
    request_url = url
    if parsed.hostname == "minio":
        host_endpoint = urllib.parse.urlparse(MINIO_HOST_ENDPOINT)
        if host_endpoint.scheme not in {"http", "https"} or not host_endpoint.netloc:
            raise RuntimeProofError(f"invalid MINIO host endpoint for local download rewrite: {MINIO_HOST_ENDPOINT}")
        public = parsed._replace(
            scheme=host_endpoint.scheme,
            netloc=host_endpoint.netloc,
        )
        request_url = urllib.parse.urlunparse(public)
        headers["Host"] = parsed.netloc
    request = urllib.request.Request(request_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, RemoteDisconnected, TimeoutError, OSError) as exc:
        raise RuntimeProofError(f"artifact download failed for {url}: {exc}") from exc


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeProofError(message)


def _resolve_channel_account(*, suffix: str, label: str) -> ChannelAccount:
    account = _request(
        "/internal/v1/channel-accounts",
        method="PUT",
        body={
            "channel": "mcp",
            "external_account_ref": f"target-runtime-proof-{label}-{suffix}",
            "display_name": f"Target runtime proof {label} {suffix}",
            "metadata": {"runtime_final_e2e": True, "label": label},
        },
    )["channel_account"]
    channel_account_id = str(account["channel_account_id"])
    _assert(channel_account_id != "", "channel account id must not be empty")
    return ChannelAccount(channel_account_id=channel_account_id)


def _poll_run(account: ChannelAccount, analysis_run_id: str) -> dict[str, Any]:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_run: dict[str, Any] | None = None
    while time.time() < deadline:
        payload = _request(f"/v1/analysis-runs/{analysis_run_id}?{account.query()}")
        run = payload["analysis_run"]
        last_run = run
        if run["status"] in {"succeeded", "partially_succeeded", "failed", "canceled", "expired"}:
            return run
        time.sleep(POLL_INTERVAL_SECONDS)
    raise RuntimeProofError(f"analysis run {analysis_run_id} did not reach terminal state: last={last_run}")


def _wait_for_api() -> None:
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _request("/internal/v1/channel-accounts?page_size=1")
            return
        except RuntimeProofError as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeProofError(f"API did not become ready: {last_error}")


def _list_run_artifacts(account: ChannelAccount, analysis_run_id: str) -> dict[str, Any]:
    nested_path = f"/v1/analysis-runs/{analysis_run_id}/artifacts?{account.query()}&page_size=20"
    return _request(nested_path)


def _resolve_artifact(account: ChannelAccount, artifact_ids: list[str]) -> tuple[str, dict[str, Any]]:
    last_error: Exception | None = None
    for artifact_id in artifact_ids:
        try:
            artifact = _request(f"/v1/artifacts/{artifact_id}?{account.query()}")["artifact"]
            return artifact_id, artifact
        except RuntimeProofError as exc:
            last_error = exc
        try:
            artifact = _request(f"/v1/artifacts/{artifact_id}/refresh?{account.query()}", method="POST")["artifact"]
            return artifact_id, artifact
        except RuntimeProofError as exc:
            last_error = exc
    raise RuntimeProofError(f"no artifact could be resolved from {artifact_ids}: {last_error}")


def _assert_sealed_snapshot_preserved(account: ChannelAccount, selection_snapshot_id: str, media_asset_id: str) -> None:
    snapshot = _request(f"/v1/selection-snapshots/{selection_snapshot_id}?{account.query()}")["selection_snapshot"]
    _assert(snapshot["status"] == "sealed", "selection snapshot must remain sealed")
    _assert(
        any(item["media_asset_id"] == media_asset_id for item in snapshot["items"]),
        "selection snapshot changed after media asset soft delete",
    )


def main() -> int:
    suffix = str(int(time.time()))
    _wait_for_api()

    account = _resolve_channel_account(suffix=suffix, label="primary")
    other_account = _resolve_channel_account(suffix=suffix, label="other")

    media_asset = _request(
        "/v1/media-assets",
        method="POST",
        headers={"Idempotency-Key": f"runtime-final-e2e-media-{suffix}"},
        body={
            "channel_account_id": account.channel_account_id,
            "kind": "text",
            "display_name": "Runtime target proof note",
            "origin": {
                "origin_type": "text",
                "origin_ref": f"target-runtime-proof://{suffix}/note",
            },
            "metadata": {
                "runtime_final_e2e": True,
                "text_preview": "Runtime target proof note for channel-aware E2E.",
            },
        },
        expected=(201,),
    )["media_asset"]
    media_asset_id = str(media_asset["media_asset_id"])

    listed_media = _request(f"/v1/media-assets?{account.query()}&page_size=20")
    _assert(
        any(item["media_asset_id"] == media_asset_id for item in listed_media["items"]),
        "created media asset missing from list_media_assets",
    )

    inbox = _request(f"/v1/collections/inbox?{account.query()}&page_size=20")["collection"]
    inbox_id = str(inbox["collection_id"])
    _assert(any(item["media_asset_id"] == media_asset_id for item in inbox["items"]), "created media asset missing from inbox")

    selection_snapshot = _request(
        "/v1/selection-snapshots",
        method="POST",
        headers={"Idempotency-Key": f"runtime-final-e2e-snapshot-{suffix}"},
        body={
            "channel_account_id": account.channel_account_id,
            "source_collection_id": inbox_id,
            "items": [{"media_asset_id": media_asset_id, "position": 0}],
            "option_snapshot": {"source": "target-runtime-proof", "fixture": "agent-runner"},
            "created_via_channel_account_id": account.channel_account_id,
        },
        expected=(201,),
    )["selection_snapshot"]
    selection_snapshot_id = str(selection_snapshot["selection_snapshot_id"])
    _assert(selection_snapshot["status"] == "sealed", "selection snapshot must be sealed")

    analysis_run = _request(
        "/v1/analysis-runs",
        method="POST",
        headers={"Idempotency-Key": f"runtime-final-e2e-run-{suffix}"},
        body={
            "channel_account_id": account.channel_account_id,
            "selection_snapshot_id": selection_snapshot_id,
            "run_type": "report",
            "params": {
                "harness_name": "fixture",
                "request": {"operation": "report", "subject": "runtime-target-proof"},
                "request_access_policy": {"allow_inline_request": True},
            },
            "delivery": {"strategy": "polling"},
            "created_via_channel_id": account.channel_account_id,
        },
        expected=(201,),
    )["analysis_run"]
    analysis_run_id = str(analysis_run["analysis_run_id"])
    _assert(
        any(step["worker_kind"] == "agent_runner" for step in analysis_run.get("analysis_run_steps", [])),
        "target report run did not create an agent_runner step",
    )

    terminal_run = _poll_run(account, analysis_run_id)
    _assert(terminal_run["status"] in {"succeeded", "partially_succeeded"}, f"run finished in unexpected state {terminal_run['status']}")

    events = _request(f"/v1/analysis-runs/{analysis_run_id}/events?{account.query()}&page_size=50")
    _assert(len(events["items"]) >= 2, "run events did not include worker progress/finalization")

    artifacts_page = _list_run_artifacts(account, analysis_run_id)
    _assert(len(artifacts_page["items"]) >= 1, "run artifacts are empty")
    artifact_ids = [str(item["artifact_id"]) for item in artifacts_page["items"]]
    artifact_id, artifact = _resolve_artifact(account, artifact_ids)
    _assert(str(artifact["analysis_run_id"]) == analysis_run_id, "artifact does not belong to the expected run")

    download_access = _request(f"/internal/v1/artifacts/{artifact_id}/download-access")
    _assert(str(download_access["analysis_run_id"]) == analysis_run_id, "download access belongs to the wrong run")
    download_url = str(download_access["download"]["url"])
    downloaded = _download_bytes(download_url)
    _assert(len(downloaded) > 0, "downloaded artifact payload is empty")

    diagnostics_page = _request(
        f"/v1/diagnostics?{account.query()}&subject_type=analysis_run&subject_id={analysis_run_id}&page_size=20"
    )
    diagnostics_items = diagnostics_page.get("items")
    if not isinstance(diagnostics_items, list):
        diagnostics_items = []

    refreshed_artifact = _request(f"/v1/artifacts/{artifact_id}/refresh?{account.query()}", method="POST")["artifact"]
    _assert(str(refreshed_artifact["artifact_id"]) == artifact_id, "artifact refresh returned the wrong artifact")

    denied_run = _request(
        f"/v1/analysis-runs/{analysis_run_id}?{other_account.query()}",
        expected=(404,),
    )
    _assert(denied_run.get("error", {}).get("code") == "not_found", "cross-channel denial must be not_found")

    removed_media = _request(
        f"/v1/media-assets/{media_asset_id}?{account.query()}",
        method="DELETE",
    )["media_asset"]
    _assert(removed_media["status"] == "deleted", "soft delete did not mark the media asset deleted")

    preserved_run = _request(f"/v1/analysis-runs/{analysis_run_id}?{account.query()}")["analysis_run"]
    _assert(
        preserved_run["selection_snapshot_id"] == selection_snapshot_id,
        "analysis run lost its sealed selection snapshot reference after media asset soft delete",
    )
    _assert_sealed_snapshot_preserved(account, selection_snapshot_id, media_asset_id)

    print(
        json.dumps(
            {
                "channel_account_id": account.channel_account_id,
                "media_asset_id": media_asset_id,
                "selection_snapshot_id": selection_snapshot_id,
                "analysis_run_id": analysis_run_id,
                "artifact_id": artifact_id,
                "download_bytes": len(downloaded),
                "terminal_status": terminal_run["status"],
                "diagnostic_count": len(diagnostics_items),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeProofError as exc:
        print(f"[RuntimeFinalE2E] {exc}", file=sys.stderr)
        raise SystemExit(1)
