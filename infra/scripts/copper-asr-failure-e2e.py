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
API_BASE_URL = os.environ.get("COPPER_ASR_FAILURE_E2E_API_BASE_URL", "http://localhost:8080")
POLL_TIMEOUT_SECONDS = int(os.environ.get("COPPER_ASR_FAILURE_E2E_POLL_TIMEOUT_SECONDS", "180"))
POLL_INTERVAL_SECONDS = float(os.environ.get("COPPER_ASR_FAILURE_E2E_POLL_INTERVAL_SECONDS", "2"))
TERMINAL_STATUSES = {"succeeded", "partially_succeeded", "failed", "canceled", "expired"}


class FailureE2EError(RuntimeError):
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
                raise FailureE2EError(f"{method} {path} returned {response.status}, expected {expected}: {text}")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        if exc.code in expected:
            return json.loads(text) if text else {}
        raise FailureE2EError(f"{method} {path} returned {exc.code}, expected {expected}: {text}") from exc
    except (urllib.error.URLError, RemoteDisconnected, TimeoutError, OSError) as exc:
        raise FailureE2EError(f"{method} {path} transport failed: {exc}") from exc


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
        raise FailureE2EError(message)


def _wait_for_api() -> None:
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _request("/internal/v1/channel-accounts?page_size=1")
            return
        except FailureE2EError as exc:
            last_error = exc
            time.sleep(2)
    raise FailureE2EError(f"API did not become ready: {last_error}")


def _resolve_channel_account(*, label: str, suffix: str) -> ChannelAccount:
    account = _request(
        "/internal/v1/channel-accounts",
        method="PUT",
        body={
            "channel": "mcp",
            "external_account_ref": f"copper-asr-failure-e2e-{label}-{suffix}",
            "display_name": f"CopperASR failure E2E {label} {suffix}",
            "metadata": {"copper_asr_failure_e2e": True, "label": label},
        },
    )["channel_account"]
    channel_account_id = str(account["channel_account_id"])
    _assert(channel_account_id != "", "channel account id must not be empty")
    return ChannelAccount(channel_account_id=channel_account_id)


def _multipart_upload(account: ChannelAccount, stored_object: dict[str, Any], *, case_id: str) -> dict[str, Any]:
    fixture_path = ROOT / str(stored_object["fixture_path"])
    boundary = f"----map-copper-asr-{uuid.uuid4().hex}"
    metadata = {
        "channel_account_id": account.channel_account_id,
        "kind": "voice",
        "display_name": Path(str(stored_object["object_key"])).name,
        "metadata": {"fixture_case_id": case_id, "source": "copper-asr-failure-e2e"},
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
            "Idempotency-Key": f"copper-asr-{case_id}-upload-{uuid.uuid4().hex}",
        },
    )["media_asset"]


def _create_transcription_run(account: ChannelAccount, media_asset_id: str, *, label: str) -> dict[str, Any]:
    suffix = f"{label}-{uuid.uuid4().hex}"
    inbox = _request(f"/v1/collections/inbox?{account.query()}&page_size=20")["collection"]
    snapshot = _request(
        "/v1/selection-snapshots",
        method="POST",
        expected=(201,),
        headers={"Idempotency-Key": f"copper-asr-{suffix}-snapshot"},
        body={
            "channel_account_id": account.channel_account_id,
            "source_collection_id": inbox["collection_id"],
            "items": [{"media_asset_id": media_asset_id, "position": 0}],
            "option_snapshot": {"source": "copper-asr-failure-e2e", "label": label},
            "created_via_channel_account_id": account.channel_account_id,
        },
    )["selection_snapshot"]
    return _request(
        "/v1/analysis-runs",
        method="POST",
        expected=(201,),
        headers={"Idempotency-Key": f"copper-asr-{suffix}-run"},
        body={
            "channel_account_id": account.channel_account_id,
            "selection_snapshot_id": snapshot["selection_snapshot_id"],
            "run_type": "transcription",
            "params": {"language": "ru"},
            "delivery": {"strategy": "polling"},
            "created_via_channel_id": account.channel_account_id,
        },
    )["analysis_run"]


def _poll_run(account: ChannelAccount, analysis_run_id: str) -> dict[str, Any]:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_run: dict[str, Any] | None = None
    while time.time() < deadline:
        run = _request(f"/v1/analysis-runs/{analysis_run_id}?{account.query()}")["analysis_run"]
        last_run = run
        if run["status"] in TERMINAL_STATUSES:
            return run
        time.sleep(POLL_INTERVAL_SECONDS)
    raise FailureE2EError(f"analysis run {analysis_run_id} did not reach terminal state: last={last_run}")


def _run_diagnostics(account: ChannelAccount, analysis_run_id: str) -> list[dict[str, Any]]:
    page = _request(
        f"/v1/diagnostics?{account.query()}&subject_type=analysis_run&subject_id={analysis_run_id}&page_size=20"
    )
    return list(page.get("items") or [])


def _run_artifacts(account: ChannelAccount, analysis_run_id: str) -> list[dict[str, Any]]:
    page = _request(f"/v1/analysis-runs/{analysis_run_id}/artifacts?{account.query()}&page_size=20")
    return list(page.get("items") or [])


def _assert_failed_run_has_diagnostics_and_policy_artifacts(
    account: ChannelAccount,
    run: dict[str, Any],
    *,
    allowed_codes: set[str],
) -> dict[str, Any]:
    analysis_run_id = str(run["analysis_run_id"])
    _assert(run["status"] == "failed", f"run {analysis_run_id} status={run['status']}, want failed")
    diagnostics = _run_diagnostics(account, analysis_run_id)
    _assert(len(diagnostics) >= 1, f"run {analysis_run_id} has no diagnostics")
    codes = {str(item.get("code") or "") for item in diagnostics}
    _assert("backend_unavailable" not in codes, f"run {analysis_run_id} exposed generic backend_unavailable: {codes}")
    _assert(any(code in allowed_codes for code in codes), f"run {analysis_run_id} diagnostic codes {codes} not in {allowed_codes}")
    artifacts = _run_artifacts(account, analysis_run_id)
    artifact_kinds = {str(item.get("kind") or "") for item in artifacts}
    _assert({"run_manifest", "run_diagnostics"}.issubset(artifact_kinds), f"run {analysis_run_id} policy artifacts missing: {artifact_kinds}")
    _assert(
        not {"transcript_plain", "transcript_segmented_markdown", "transcript_docx"} & artifact_kinds,
        f"run {analysis_run_id} published transcript artifacts after failed ASR: {artifact_kinds}",
    )
    return {
        "analysis_run_id": analysis_run_id,
        "status": run["status"],
        "diagnostic_codes": sorted(codes),
        "artifact_kinds": sorted(artifact_kinds),
    }


def _run_corrupt_and_retry_case(account: ChannelAccount) -> dict[str, Any]:
    case, stored_object = _case("corrupt_audio")
    allowed_codes = set(case.get("accepted_live_diagnostic_codes") or [case.get("expected_diagnostic_code")])
    media = _multipart_upload(account, stored_object, case_id=str(case["case_id"]))
    run = _create_transcription_run(account, str(media["media_asset_id"]), label="corrupt")
    terminal = _poll_run(account, str(run["analysis_run_id"]))
    corrupt_result = _assert_failed_run_has_diagnostics_and_policy_artifacts(
        account,
        terminal,
        allowed_codes={str(code) for code in allowed_codes if str(code or "").strip()},
    )
    retry = _request(
        f"/v1/analysis-runs/{terminal['analysis_run_id']}/retry",
        method="POST",
        expected=(202,),
        body={
            "channel_account_id": account.channel_account_id,
            "idempotency_key": f"copper-asr-retry-{uuid.uuid4().hex}",
        },
    )["analysis_run"]
    _assert(
        str(retry["analysis_run_id"]) != str(terminal["analysis_run_id"]),
        "retry must create a new analysis run id",
    )
    retry_terminal = _poll_run(account, str(retry["analysis_run_id"]))
    retry_result = _assert_failed_run_has_diagnostics_and_policy_artifacts(
        account,
        retry_terminal,
        allowed_codes={str(code) for code in allowed_codes if str(code or "").strip()},
    )
    return {"corrupt": corrupt_result, "retry": retry_result}


def _run_cancel_case(account: ChannelAccount) -> dict[str, Any]:
    case, stored_object = _case("cancellation_voice")
    media = _multipart_upload(account, stored_object, case_id=str(case["case_id"]))
    run = _create_transcription_run(account, str(media["media_asset_id"]), label="cancel")
    canceled = _request(
        f"/v1/analysis-runs/{run['analysis_run_id']}/cancel",
        method="POST",
        body={"channel_account_id": account.channel_account_id, "message": "copper-asr failure e2e cancel"},
    )["analysis_run"]
    terminal = _poll_run(account, str(canceled["analysis_run_id"]))
    _assert(terminal["status"] == "canceled", f"canceled run terminal status={terminal['status']}, want canceled")
    artifacts = _run_artifacts(account, str(terminal["analysis_run_id"]))
    _assert(artifacts == [], f"canceled run should not publish artifacts: {artifacts}")
    return {
        "analysis_run_id": str(terminal["analysis_run_id"]),
        "status": terminal["status"],
        "artifact_count": len(artifacts),
    }


def _resource_limit_config() -> dict[str, str]:
    copper_env = (ROOT / "infra/env/copper-asr.env.example").read_text(encoding="utf-8")
    worker_env = (ROOT / "infra/env/worker-transcription.env.example").read_text(encoding="utf-8")
    required = {
        "COPPER_ASR_MAX_CONCURRENT_REQUESTS": "1",
        "COPPER_ASR_ACQUIRE_TIMEOUT_S": "30",
        "COPPER_ASR_CLIENT_TIMEOUT_S": "28800",
    }
    for key, value in required.items():
        source = copper_env if key != "COPPER_ASR_CLIENT_TIMEOUT_S" else worker_env
        _assert(f"{key}={value}" in source, f"{key}={value} missing from runtime env examples")
    return required


def run_failure_e2e() -> dict[str, Any]:
    suffix = str(int(time.time()))
    _wait_for_api()
    account = _resolve_channel_account(label="primary", suffix=suffix)
    return {
        "channel_account_id": account.channel_account_id,
        "corrupt_and_retry": _run_corrupt_and_retry_case(account),
        "cancellation": _run_cancel_case(account),
        "resource_limits": _resource_limit_config(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run live CopperASR failure, retry, cancellation, and limit E2E checks.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable proof.")
    args = parser.parse_args(argv)
    try:
        result = run_failure_e2e()
    except FailureE2EError as exc:
        print(f"[CopperAsrFailureE2E] {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print("[CopperAsrFailureE2E] completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
