#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.client import RemoteDisconnected
from dataclasses import dataclass
from typing import Any


API_BASE_URL = "http://localhost:8080"
POLL_TIMEOUT_SECONDS = 90
POLL_INTERVAL_SECONDS = 2


class RuntimeProofError(RuntimeError):
    pass


@dataclass(frozen=True)
class Owner:
    owner_type: str
    owner_id: str

    def query(self) -> str:
        return urllib.parse.urlencode({"owner_type": self.owner_type, "owner_id": self.owner_id})

    def payload(self) -> dict[str, str]:
        return {"owner_type": self.owner_type, "owner_id": self.owner_id}


def _request(path: str, *, method: str = "GET", body: dict[str, Any] | None = None, expected: tuple[int, ...] = (200,)) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        urllib.parse.urljoin(f"{API_BASE_URL}/", path.lstrip("/")),
        data=data,
        headers={"Accept": "application/json", **({"Content-Type": "application/json"} if body is not None else {})},
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
    except (urllib.error.URLError, RemoteDisconnected) as exc:
        raise RuntimeProofError(f"{method} {path} transport failed: {exc}") from exc


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeProofError(message)


def _poll_run(owner: Owner, analysis_run_id: str) -> dict[str, Any]:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_run: dict[str, Any] | None = None
    while time.time() < deadline:
        payload = _request(f"/v1/analysis-runs/{analysis_run_id}?{owner.query()}")
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
            _request("/v1/admin/observability")
            return
        except RuntimeProofError as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeProofError(f"API did not become ready: {last_error}")


def _list_run_artifacts(owner: Owner, analysis_run_id: str) -> dict[str, Any]:
    nested_path = f"/v1/analysis-runs/{analysis_run_id}/artifacts?{owner.query()}&page_size=20"
    try:
        return _request(nested_path)
    except RuntimeProofError as exc:
        if "returned 404" not in str(exc):
            raise
    fallback_path = f"/v1/artifacts?{owner.query()}&analysis_run_id={analysis_run_id}&page_size=20"
    return _request(fallback_path)


def _resolve_artifact(owner: Owner, artifact_ids: list[str]) -> tuple[str, dict[str, Any]]:
    last_error: Exception | None = None
    for artifact_id in artifact_ids:
        try:
            artifact = _request(f"/v1/artifacts/{artifact_id}?{owner.query()}")["artifact"]
            return artifact_id, artifact
        except RuntimeProofError as exc:
            last_error = exc
        try:
            artifact = _request(f"/v1/artifacts/{artifact_id}/refresh?{owner.query()}", method="POST")["artifact"]
            return artifact_id, artifact
        except RuntimeProofError as exc:
            last_error = exc
    raise RuntimeProofError(f"no artifact could be resolved from {artifact_ids}: {last_error}")


def main() -> int:
    suffix = str(int(time.time()))
    owner = Owner(owner_type="mcp", owner_id=f"runtime-proof-{suffix}")
    other_owner = Owner(owner_type="mcp", owner_id=f"runtime-proof-other-{suffix}")

    _wait_for_api()

    media_item = _request(
        "/v1/media-items",
        method="POST",
        body={
            "owner": owner.payload(),
            "kind": "text",
            "display_name": "Runtime proof note",
            "adapter_origin": "runtime-proof",
            "source": {
                "origin_type": "text",
                "text": "Runtime proof note for inbox-first e2e.",
            },
        },
        expected=(201,),
    )["media_item"]
    media_item_id = str(media_item["media_item_id"])

    listed_media = _request(f"/v1/media-items?{owner.query()}&page_size=20")
    _assert(any(item["media_item_id"] == media_item_id for item in listed_media["items"]), "created media item missing from list_media")

    inbox = _request(f"/v1/collections/inbox?{owner.query()}")["collection"]
    inbox_id = str(inbox["collection_id"])
    inbox_version = int(inbox["version"])
    _assert(any(item["media_item_id"] == media_item_id for item in inbox["items"]), "created media item missing from inbox")

    selection = _request(
        "/v1/selections",
        method="POST",
        body={
            "owner": owner.payload(),
            "source_collection_id": inbox_id,
            "items": [{"media_item_id": media_item_id, "position": 0}],
            "option_snapshot": {"source": "runtime-proof"},
            "duplicate_policy": "reject",
            "created_by": "runtime-proof",
        },
        expected=(201,),
    )["selection"]
    selection_id = str(selection["selection_id"])
    _assert(selection["status"] == "sealed", "selection must be sealed")

    analysis_run = _request(
        "/v1/analysis-runs",
        method="POST",
        body={
            "owner": owner.payload(),
            "selection_id": selection_id,
            "run_type": "custom",
            "params": {"harness_name": "fixture"},
            "delivery": {"strategy": "polling"},
        },
        expected=(202,),
    )["analysis_run"]
    analysis_run_id = str(analysis_run["analysis_run_id"])

    reconcile = _request(
        "/v1/admin/reconcile-queue",
        method="POST",
        body={"limit": 20},
        expected=(202,),
    )

    terminal_run = _poll_run(owner, analysis_run_id)
    _assert(terminal_run["status"] in {"succeeded", "partially_succeeded"}, f"run finished in unexpected state {terminal_run['status']}")

    events = _request(f"/v1/analysis-runs/{analysis_run_id}/events?{owner.query()}&page_size=50")
    _assert(len(events["items"]) >= 1, "run events are empty")

    artifacts_page = _list_run_artifacts(owner, analysis_run_id)
    _assert(len(artifacts_page["items"]) >= 1, "run artifacts are empty")
    artifact_ids = [str(item["artifact_id"]) for item in artifacts_page["items"]]
    artifact_id, artifact = _resolve_artifact(owner, artifact_ids)

    diagnostics_page = _request(
        f"/v1/diagnostics?{owner.query()}&subject_type=analysis_run&subject_id={analysis_run_id}&page_size=20"
    )
    diagnostics_items = diagnostics_page.get("items")
    if not isinstance(diagnostics_items, list):
        diagnostics_items = []

    _assert(str(artifact["analysis_run_id"]) == analysis_run_id, "artifact does not belong to the expected run")

    refreshed_artifact = _request(f"/v1/artifacts/{artifact_id}/refresh?{owner.query()}", method="POST")["artifact"]
    _assert(str(refreshed_artifact["artifact_id"]) == artifact_id, "artifact refresh returned the wrong artifact")

    denied_run = _request(
        f"/v1/analysis-runs/{analysis_run_id}?{other_owner.query()}",
        expected=(404,),
    )
    _assert(denied_run.get("error", {}).get("code") == "not_found", "cross-owner denial must be not_found")

    removed_media = _request(
        f"/v1/media-items/{media_item_id}?{owner.query()}",
        method="DELETE",
    )["media_item"]
    _assert(removed_media["status"] == "deleted", "soft delete did not mark the media item deleted")
    _assert(removed_media["retention"]["state"] == "soft_deleted", "soft delete did not set retention.soft_deleted")

    preserved_run = _request(f"/v1/analysis-runs/{analysis_run_id}?{owner.query()}")["analysis_run"]
    _assert(
        preserved_run["selection"]["items"][0]["media_item_id"] == media_item_id,
        "run selection snapshot changed after media soft delete",
    )

    print(
        json.dumps(
            {
                "owner": owner.payload(),
                "media_item_id": media_item_id,
                "selection_id": selection_id,
                "analysis_run_id": analysis_run_id,
                "artifact_id": artifact_id,
                "terminal_status": terminal_run["status"],
                "reconciled": reconcile["reconciled"],
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
