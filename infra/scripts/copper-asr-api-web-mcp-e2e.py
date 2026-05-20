#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "infra" / "fixtures" / "target" / "manifest.json"
API_BASE_URL = os.environ.get("COPPER_ASR_API_WEB_MCP_E2E_API_BASE_URL", "http://localhost:8080")
POLL_TIMEOUT_SECONDS = int(os.environ.get("COPPER_ASR_API_WEB_MCP_E2E_POLL_TIMEOUT_SECONDS", "300"))
POLL_INTERVAL_SECONDS = float(os.environ.get("COPPER_ASR_API_WEB_MCP_E2E_POLL_INTERVAL_SECONDS", "2"))
TERMINAL_STATUSES = {"succeeded", "partially_succeeded", "failed", "canceled", "expired"}
TRANSCRIPT_WORKER_KINDS = {"transcript_plain", "transcript_segmented_markdown", "transcript_docx"}
POLICY_ARTIFACT_KINDS = {"run_manifest", "run_diagnostics"}


class ApiWebMcpE2EError(RuntimeError):
    pass


@dataclass(frozen=True)
class ChannelAccount:
    channel_account_id: str
    channel: str

    def query(self) -> str:
        return urllib.parse.urlencode({"channel_account_id": self.channel_account_id})


def _request(
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    expected: tuple[int, ...] = (200,),
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> dict[str, Any]:
    payload = json.dumps(body).encode("utf-8") if body is not None else data
    request_headers = {
        "Accept": "application/json",
        **({"Content-Type": "application/json"} if body is not None else {}),
        **(headers or {}),
    }
    request = urllib.request.Request(
        urllib.parse.urljoin(f"{API_BASE_URL}/", path.lstrip("/")),
        data=payload,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read().decode("utf-8")
            if response.status not in expected:
                raise ApiWebMcpE2EError(f"{method} {path} returned {response.status}, expected {expected}: {text}")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        if exc.code in expected:
            return json.loads(text) if text else {}
        raise ApiWebMcpE2EError(f"{method} {path} returned {exc.code}, expected {expected}: {text}") from exc
    except (urllib.error.URLError, RemoteDisconnected, TimeoutError, OSError) as exc:
        raise ApiWebMcpE2EError(f"{method} {path} transport failed: {exc}") from exc


def _download_bytes(url: str) -> bytes:
    parsed = urllib.parse.urlparse(url)
    request_url = url
    headers: dict[str, str] = {}
    if parsed.hostname == "minio":
        public = parsed._replace(netloc=f"localhost:{parsed.port or 9000}")
        request_url = urllib.parse.urlunparse(public)
        headers["Host"] = parsed.netloc
    request = urllib.request.Request(request_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, RemoteDisconnected, TimeoutError, OSError) as exc:
        raise ApiWebMcpE2EError(f"artifact download failed for {url}: {exc}") from exc


def _load_manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _case(case_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    fixtures = _load_manifest()["fixtures"]
    stored_by_id = {str(item["stored_object_id"]): item for item in fixtures["stored_objects"]}
    cases = {str(item["case_id"]): item for item in fixtures["copper_asr_e2e"]["cases"]}
    case = cases[case_id]
    return case, stored_by_id[str(case["stored_object_id"])]


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise ApiWebMcpE2EError(message)


def _wait_for_api() -> None:
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _request("/internal/v1/channel-accounts?page_size=1")
            return
        except ApiWebMcpE2EError as exc:
            last_error = exc
            time.sleep(2)
    raise ApiWebMcpE2EError(f"API did not become ready: {last_error}")


def _resolve_channel_account(*, channel: str, label: str, suffix: str) -> ChannelAccount:
    account = _request(
        "/internal/v1/channel-accounts",
        method="PUT",
        body={
            "channel": channel,
            "external_account_ref": f"copper-asr-api-web-mcp-e2e-{label}-{suffix}",
            "display_name": f"CopperASR API/Web/MCP E2E {label} {suffix}",
            "metadata": {"copper_asr_api_web_mcp_e2e": True, "label": label},
        },
    )["channel_account"]
    channel_account_id = str(account["channel_account_id"])
    _assert(channel_account_id != "", "channel account id must not be empty")
    return ChannelAccount(channel_account_id=channel_account_id, channel=channel)


def _multipart_upload(account: ChannelAccount, stored_object: dict[str, Any], *, case_id: str) -> dict[str, Any]:
    fixture_path = ROOT / str(stored_object["fixture_path"])
    boundary = f"----map-copper-asr-api-web-mcp-{uuid.uuid4().hex}"
    metadata = {
        "channel_account_id": account.channel_account_id,
        "kind": "voice",
        "display_name": Path(str(stored_object["object_key"])).name,
        "metadata": {"fixture_case_id": case_id, "source": "copper-asr-api-web-mcp-e2e"},
    }
    body = fixture_path.read_bytes()
    payload = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="metadata"\r\n\r\n'
        f"{json.dumps(metadata, ensure_ascii=False)}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{fixture_path.name}"\r\n'
        f"Content-Type: {stored_object['content_type']}\r\n\r\n"
    ).encode("utf-8") + body + b"\r\n" + f"--{boundary}--\r\n".encode("utf-8")
    return _request(
        "/v1/media-assets/upload",
        method="POST",
        data=payload,
        expected=(201,),
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Idempotency-Key": f"copper-asr-api-web-mcp-{case_id}-upload-{uuid.uuid4().hex}",
        },
    )["media_asset"]


def _create_transcription_run(account: ChannelAccount, media_asset_id: str, *, suffix: str) -> dict[str, Any]:
    inbox = _request(f"/v1/collections/inbox?{account.query()}&page_size=20")["collection"]
    _assert(
        any(item["media_asset_id"] == media_asset_id for item in inbox.get("items", [])),
        "uploaded media asset missing from inbox collection",
    )
    snapshot = _request(
        "/v1/selection-snapshots",
        method="POST",
        expected=(201,),
        headers={"Idempotency-Key": f"copper-asr-api-web-mcp-{suffix}-snapshot"},
        body={
            "channel_account_id": account.channel_account_id,
            "source_collection_id": inbox["collection_id"],
            "items": [{"media_asset_id": media_asset_id, "position": 0}],
            "option_snapshot": {"source": "copper-asr-api-web-mcp-e2e", "language": "ru"},
            "created_via_channel_account_id": account.channel_account_id,
        },
    )["selection_snapshot"]
    run = _request(
        "/v1/analysis-runs",
        method="POST",
        expected=(201,),
        headers={"Idempotency-Key": f"copper-asr-api-web-mcp-{suffix}-run"},
        body={
            "channel_account_id": account.channel_account_id,
            "selection_snapshot_id": snapshot["selection_snapshot_id"],
            "run_type": "transcription",
            "params": {"language": "ru"},
            "delivery": {"strategy": "polling"},
            "created_via_channel_id": account.channel_account_id,
        },
    )["analysis_run"]
    return {"selection_snapshot": snapshot, "analysis_run": run}


def _poll_run(account: ChannelAccount, analysis_run_id: str) -> dict[str, Any]:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_run: dict[str, Any] | None = None
    while time.time() < deadline:
        run = _request(f"/v1/analysis-runs/{analysis_run_id}?{account.query()}")["analysis_run"]
        last_run = run
        if run["status"] in TERMINAL_STATUSES:
            return run
        time.sleep(POLL_INTERVAL_SECONDS)
    raise ApiWebMcpE2EError(f"analysis run {analysis_run_id} did not reach terminal state: last={last_run}")


def _list_run_artifacts(account: ChannelAccount, analysis_run_id: str) -> list[dict[str, Any]]:
    page = _request(f"/v1/analysis-runs/{analysis_run_id}/artifacts?{account.query()}&page_size=20")
    return list(page.get("items") or [])


def _get_artifact(account: ChannelAccount, artifact_id: str) -> dict[str, Any]:
    return _request(f"/v1/artifacts/{artifact_id}?{account.query()}")["artifact"]


def _refresh_artifact(account: ChannelAccount, artifact_id: str) -> dict[str, Any]:
    return _request(f"/v1/artifacts/{artifact_id}/refresh?{account.query()}", method="POST")["artifact"]


def _artifact_worker_kind(artifact: dict[str, Any]) -> str:
    preview = artifact.get("preview")
    if isinstance(preview, dict):
        value = preview.get("worker_artifact_kind")
        if isinstance(value, str):
            return value
    value = artifact.get("worker_artifact_kind")
    return value if isinstance(value, str) else ""


def _download_artifact_payload(artifact_id: str) -> bytes:
    access = _request(f"/internal/v1/artifacts/{artifact_id}/download-access")
    url = str(access["download"]["url"])
    payload = _download_bytes(url)
    _assert(len(payload) > 0, f"artifact {artifact_id} download payload is empty")
    return payload


def _artifact_by_kind(artifacts: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    for artifact in artifacts:
        if artifact.get("kind") == kind:
            return artifact
    raise ApiWebMcpE2EError(f"artifact kind {kind} not found in {[artifact.get('kind') for artifact in artifacts]}")


def _plain_transcript_artifact(artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    transcript_artifacts = [artifact for artifact in artifacts if artifact.get("kind") == "transcript"]
    for artifact in transcript_artifacts:
        worker_kind = _artifact_worker_kind(artifact)
        content_type = str(artifact.get("content_type") or "")
        if worker_kind == "transcript_plain" or content_type.startswith("text/plain"):
            return artifact
    if transcript_artifacts:
        return transcript_artifacts[0]
    raise ApiWebMcpE2EError("no transcript artifact was published")


def _assert_artifacts(
    account: ChannelAccount,
    analysis_run_id: str,
) -> dict[str, Any]:
    summaries = _list_run_artifacts(account, analysis_run_id)
    _assert(len(summaries) >= 5, f"expected transcript and policy artifacts, got {len(summaries)}")
    details = [_get_artifact(account, str(artifact["artifact_id"])) for artifact in summaries]
    public_kinds = {str(artifact.get("kind") or "") for artifact in details}
    _assert("transcript" in public_kinds, f"transcript public artifact missing: {public_kinds}")
    _assert(POLICY_ARTIFACT_KINDS.issubset(public_kinds), f"policy artifacts missing: {public_kinds}")

    worker_kinds = {_artifact_worker_kind(artifact) for artifact in details}
    worker_kinds.discard("")
    if worker_kinds:
        _assert(
            TRANSCRIPT_WORKER_KINDS.issubset(worker_kinds),
            f"transcript worker artifact kinds missing: {worker_kinds}",
        )

    plain_artifact = _plain_transcript_artifact(details)
    plain_payload = _download_artifact_payload(str(plain_artifact["artifact_id"]))

    manifest_artifact = _artifact_by_kind(details, "run_manifest")
    manifest_payload = json.loads(_download_artifact_payload(str(manifest_artifact["artifact_id"])).decode("utf-8"))
    backend = manifest_payload.get("transcription_backend")
    _assert(isinstance(backend, dict), f"run_manifest missing transcription_backend: {manifest_payload}")
    _assert(backend.get("provider") == "copperasr", f"run_manifest provider is not copperasr: {backend}")
    _assert(manifest_payload.get("summary", {}).get("included_count") == 1, f"unexpected manifest summary: {manifest_payload}")
    _assert("whisper" not in json.dumps(manifest_payload, ensure_ascii=False).lower(), "run_manifest leaked removed ASR wording")

    refreshed = _refresh_artifact(account, str(plain_artifact["artifact_id"]))
    _assert(refreshed.get("artifact_id") == plain_artifact["artifact_id"], "artifact refresh returned the wrong artifact")

    return {
        "artifact_ids": [str(artifact["artifact_id"]) for artifact in details],
        "public_kinds": sorted(public_kinds),
        "worker_kinds": sorted(worker_kinds),
        "plain_transcript_bytes": len(plain_payload),
        "manifest_backend": backend,
    }


def _assert_diagnostics(account: ChannelAccount, analysis_run_id: str) -> dict[str, Any]:
    page = _request(
        f"/v1/diagnostics?{account.query()}&subject_type=analysis_run&subject_id={analysis_run_id}&page_size=20"
    )
    diagnostics = list(page.get("items") or [])
    codes = {str(item.get("code") or "") for item in diagnostics}
    _assert("backend_unavailable" not in codes, f"unexpected generic backend_unavailable diagnostic: {codes}")
    return {"diagnostic_count": len(diagnostics), "diagnostic_codes": sorted(codes)}


def _assert_cross_channel_denial(other: ChannelAccount, analysis_run_id: str, artifact_id: str) -> dict[str, Any]:
    denied_run = _request(f"/v1/analysis-runs/{analysis_run_id}?{other.query()}", expected=(404,))
    denied_artifact = _request(f"/v1/artifacts/{artifact_id}?{other.query()}", expected=(404,))
    _assert(denied_run.get("error", {}).get("code") == "not_found", "cross-channel run denial must be not_found")
    _assert(denied_artifact.get("error", {}).get("code") == "not_found", "cross-channel artifact denial must be not_found")
    return {
        "run_denial": denied_run.get("error", {}).get("code"),
        "artifact_denial": denied_artifact.get("error", {}).get("code"),
    }


def _assert_history_preserved(
    account: ChannelAccount,
    media_asset_id: str,
    selection_snapshot_id: str,
    analysis_run_id: str,
) -> dict[str, Any]:
    removed = _request(f"/v1/media-assets/{media_asset_id}?{account.query()}", method="DELETE")["media_asset"]
    _assert(removed["status"] == "deleted", "media asset delete did not return deleted status")
    run = _request(f"/v1/analysis-runs/{analysis_run_id}?{account.query()}")["analysis_run"]
    snapshot = _request(f"/v1/selection-snapshots/{selection_snapshot_id}?{account.query()}")["selection_snapshot"]
    artifacts = _list_run_artifacts(account, analysis_run_id)
    _assert(run["selection_snapshot_id"] == selection_snapshot_id, "run lost its selection snapshot after media delete")
    _assert(snapshot["status"] == "sealed", "selection snapshot did not stay sealed after media delete")
    _assert(len(artifacts) >= 1, "artifacts disappeared after source media delete")
    return {
        "media_asset_status": removed["status"],
        "selection_snapshot_status": snapshot["status"],
        "artifact_count_after_delete": len(artifacts),
    }


def run_api_web_mcp_e2e() -> dict[str, Any]:
    suffix = str(int(time.time()))
    _wait_for_api()
    primary = _resolve_channel_account(channel="web", label="primary", suffix=suffix)
    other = _resolve_channel_account(channel="mcp", label="other", suffix=suffix)
    case, stored_object = _case("short_voice")
    media = _multipart_upload(primary, stored_object, case_id=str(case["case_id"]))
    media_asset_id = str(media["media_asset_id"])
    run_context = _create_transcription_run(primary, media_asset_id, suffix=suffix)
    selection_snapshot_id = str(run_context["selection_snapshot"]["selection_snapshot_id"])
    analysis_run_id = str(run_context["analysis_run"]["analysis_run_id"])
    terminal = _poll_run(primary, analysis_run_id)
    _assert(
        terminal["status"] in {"succeeded", "partially_succeeded"},
        f"transcription run finished in unexpected state {terminal['status']}",
    )
    artifact_result = _assert_artifacts(primary, analysis_run_id)
    diagnostics_result = _assert_diagnostics(primary, analysis_run_id)
    cross_channel = _assert_cross_channel_denial(other, analysis_run_id, artifact_result["artifact_ids"][0])
    history = _assert_history_preserved(primary, media_asset_id, selection_snapshot_id, analysis_run_id)
    return {
        "api_worker": {
            "channel_account_id": primary.channel_account_id,
            "media_asset_id": media_asset_id,
            "selection_snapshot_id": selection_snapshot_id,
            "analysis_run_id": analysis_run_id,
            "terminal_status": terminal["status"],
            **artifact_result,
            **diagnostics_result,
            **history,
        },
        "web_surface": {
            "channel": primary.channel,
            "contract": "Web reads run detail, list_artifacts, get_artifact, refresh_artifact, and diagnostics through the same channel-scoped API shapes.",
            "normal_artifact_public_kinds": [
                kind for kind in artifact_result["public_kinds"] if kind not in POLICY_ARTIFACT_KINDS
            ],
        },
        "mcp_surface": {
            "channel": other.channel,
            "contract": "MCP tool tests exercise run, artifact, preview, refresh, and diagnostics tools over these same API routes.",
            "cross_channel": cross_channel,
            "run_manifest_provider": artifact_result["manifest_backend"].get("provider"),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run live CopperASR API/worker artifact proof for Web and MCP surfaces.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable proof.")
    args = parser.parse_args(argv)
    try:
        result = run_api_web_mcp_e2e()
    except ApiWebMcpE2EError as exc:
        print(f"[CopperAsrApiWebMcpE2E] {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print("[CopperAsrApiWebMcpE2E] completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
