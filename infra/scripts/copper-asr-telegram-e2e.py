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
API_BASE_URL = os.environ.get("COPPER_ASR_TELEGRAM_E2E_API_BASE_URL", "http://localhost:8080")
POLL_TIMEOUT_SECONDS = int(os.environ.get("COPPER_ASR_TELEGRAM_E2E_POLL_TIMEOUT_SECONDS", "300"))
POLL_INTERVAL_SECONDS = float(os.environ.get("COPPER_ASR_TELEGRAM_E2E_POLL_INTERVAL_SECONDS", "2"))
TERMINAL_STATUSES = {"succeeded", "partially_succeeded", "failed", "canceled", "expired"}
POLICY_ARTIFACT_KINDS = {"run_manifest", "run_diagnostics"}
TRANSCRIPT_WORKER_KINDS = {"transcript_plain", "transcript_segmented_markdown", "transcript_docx"}


class TelegramE2EError(RuntimeError):
    pass


@dataclass(frozen=True)
class TelegramAccount:
    channel_account_id: str
    external_account_ref: str
    chat_id: int
    user_id: int

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
                raise TelegramE2EError(f"{method} {path} returned {response.status}, expected {expected}: {text}")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        if exc.code in expected:
            return json.loads(text) if text else {}
        raise TelegramE2EError(f"{method} {path} returned {exc.code}, expected {expected}: {text}") from exc
    except (urllib.error.URLError, RemoteDisconnected, TimeoutError, OSError) as exc:
        raise TelegramE2EError(f"{method} {path} transport failed: {exc}") from exc


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
        raise TelegramE2EError(f"artifact download failed for {url}: {exc}") from exc


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
        raise TelegramE2EError(message)


def _wait_for_api() -> None:
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _request("/internal/v1/channel-accounts?page_size=1")
            return
        except TelegramE2EError as exc:
            last_error = exc
            time.sleep(2)
    raise TelegramE2EError(f"API did not become ready: {last_error}")


def _resolve_telegram_account(*, suffix: str) -> TelegramAccount:
    numeric_suffix = int(suffix[-6:], 16) % 900000 + 100000
    chat_id = 190000000 + numeric_suffix
    user_id = 290000000 + numeric_suffix
    external_ref = f"chat:{chat_id}:user:{user_id}"
    account = _request(
        "/internal/v1/channel-accounts",
        method="PUT",
        body={
            "channel": "telegram",
            "external_account_ref": external_ref,
            "display_name": f"CopperASR Telegram E2E {suffix}",
            "metadata": {
                "copper_asr_telegram_e2e": True,
                "channel_identity": {
                    "channel": "telegram",
                    "external_account_ref": external_ref,
                    "telegram_chat_id": str(chat_id),
                    "telegram_user_id": str(user_id),
                },
            },
        },
    )["channel_account"]
    channel_account_id = str(account["channel_account_id"])
    _assert(channel_account_id != "", "channel account id must not be empty")
    return TelegramAccount(
        channel_account_id=channel_account_id,
        external_account_ref=external_ref,
        chat_id=chat_id,
        user_id=user_id,
    )


def _multipart_upload(account: TelegramAccount, stored_object: dict[str, Any], *, case_id: str, suffix: str) -> dict[str, Any]:
    fixture_path = ROOT / str(stored_object["fixture_path"])
    body = fixture_path.read_bytes()
    boundary = f"----map-copper-asr-telegram-{uuid.uuid4().hex}"
    metadata = {
        "channel_account_id": account.channel_account_id,
        "kind": "voice",
        "display_name": Path(str(stored_object["object_key"])).name,
        "metadata": {
            "source": "copper-asr-telegram-e2e",
            "fixture_case_id": case_id,
            "telegram": {
                "chat_id": account.chat_id,
                "user_id": account.user_id,
                "message_id": 5001,
                "file_id": f"file-{suffix}",
                "file_unique_id": f"unique-{suffix}",
            },
            "message_id": 5001,
            "file_unique_id": f"unique-{suffix}",
        },
    }
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
            "Idempotency-Key": f"copper-asr-telegram-{case_id}-upload-{suffix}",
        },
    )["media_asset"]


def _inbox(account: TelegramAccount) -> dict[str, Any]:
    return _request(f"/v1/collections/inbox?{account.query()}&page_size=20")["collection"]


def _surface_subject(subject_type: str, subject_id: str, subject_role: str) -> dict[str, str]:
    return {"subject_type": subject_type, "subject_id": subject_id, "subject_role": subject_role}


def _telegram_address(account: TelegramAccount, message_id: int) -> dict[str, int]:
    return {"chat_id": account.chat_id, "message_id": message_id}


def _telegram_address_fingerprint(account: TelegramAccount, message_id: int) -> str:
    return f"telegram:{account.chat_id}:{message_id}"


def _upsert_surface(
    account: TelegramAccount,
    *,
    surface_type: str,
    surface_key: str,
    message_id: int,
    display_state: dict[str, Any],
    subjects: list[dict[str, str]],
    idempotency_key: str,
) -> dict[str, Any]:
    return _request(
        "/internal/v1/channel-surfaces",
        method="PUT",
        body={
            "channel_account_id": account.channel_account_id,
            "channel": "telegram",
            "surface_type": surface_type,
            "surface_key": surface_key,
            "address": _telegram_address(account, message_id),
            "address_fingerprint": _telegram_address_fingerprint(account, message_id),
            "display_state": display_state,
            "subjects": subjects,
            "idempotency_key": idempotency_key,
        },
    )["channel_surface"]


def _list_active_surfaces(account: TelegramAccount, *, subject_type: str, subject_id: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(
        {
            "channel_account_id": account.channel_account_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "page_size": "20",
        }
    )
    return list(_request(f"/internal/v1/channel-surfaces/active?{query}").get("items") or [])


def _create_selection_snapshot(account: TelegramAccount, collection: dict[str, Any], media_asset_id: str, *, suffix: str) -> dict[str, Any]:
    return _request(
        "/v1/selection-snapshots",
        method="POST",
        expected=(201,),
        headers={"Idempotency-Key": f"copper-asr-telegram-{suffix}-snapshot"},
        body={
            "channel_account_id": account.channel_account_id,
            "source_collection_id": collection["collection_id"],
            "items": [{"media_asset_id": media_asset_id, "position": 0}],
            "option_snapshot": {"channel": "telegram", "surface": "current_materials"},
            "created_via_channel_account_id": account.channel_account_id,
        },
    )["selection_snapshot"]


def _create_analysis_run(account: TelegramAccount, selection_snapshot_id: str, *, suffix: str) -> dict[str, Any]:
    return _request(
        "/v1/analysis-runs",
        method="POST",
        expected=(201,),
        headers={"Idempotency-Key": f"copper-asr-telegram-{suffix}-run"},
        body={
            "channel_account_id": account.channel_account_id,
            "selection_snapshot_id": selection_snapshot_id,
            "run_type": "transcription",
            "params": {"language": "ru"},
            "delivery": {"strategy": "polling"},
            "created_via_channel_id": account.channel_account_id,
        },
    )["analysis_run"]


def _poll_run(account: TelegramAccount, analysis_run_id: str) -> dict[str, Any]:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_run: dict[str, Any] | None = None
    while time.time() < deadline:
        run = _request(f"/v1/analysis-runs/{analysis_run_id}?{account.query()}")["analysis_run"]
        last_run = run
        if run["status"] in TERMINAL_STATUSES:
            return run
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TelegramE2EError(f"analysis run {analysis_run_id} did not reach terminal state: last={last_run}")


def _list_run_events(account: TelegramAccount, analysis_run_id: str) -> list[dict[str, Any]]:
    page = _request(f"/v1/analysis-runs/{analysis_run_id}/events?{account.query()}&page_size=50")
    return list(page.get("items") or [])


def _list_run_artifacts(account: TelegramAccount, analysis_run_id: str) -> list[dict[str, Any]]:
    page = _request(f"/v1/analysis-runs/{analysis_run_id}/artifacts?{account.query()}&page_size=20")
    return list(page.get("items") or [])


def _get_artifact(account: TelegramAccount, artifact_id: str) -> dict[str, Any]:
    return _request(f"/v1/artifacts/{artifact_id}?{account.query()}")["artifact"]


def _artifact_worker_kind(artifact: dict[str, Any]) -> str:
    preview = artifact.get("preview")
    if isinstance(preview, dict):
        value = preview.get("worker_artifact_kind")
        if isinstance(value, str):
            return value
    value = artifact.get("worker_artifact_kind")
    return value if isinstance(value, str) else ""


def _artifact_by_kind(artifacts: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    for artifact in artifacts:
        if artifact.get("kind") == kind:
            return artifact
    raise TelegramE2EError(f"artifact kind {kind} not found in {[artifact.get('kind') for artifact in artifacts]}")


def _plain_transcript_artifact(artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    transcript_artifacts = [artifact for artifact in artifacts if artifact.get("kind") == "transcript"]
    for artifact in transcript_artifacts:
        content_type = str(artifact.get("content_type") or "")
        worker_kind = _artifact_worker_kind(artifact)
        if worker_kind == "transcript_plain" or content_type.startswith("text/plain"):
            return artifact
    if transcript_artifacts:
        return transcript_artifacts[0]
    raise TelegramE2EError("no transcript artifact was published")


def _download_artifact_payload(artifact_id: str) -> bytes:
    access = _request(f"/internal/v1/artifacts/{artifact_id}/download-access")
    url = str(access["download"]["url"])
    payload = _download_bytes(url)
    _assert(len(payload) > 0, f"artifact {artifact_id} download payload is empty")
    return payload


def _assert_artifacts(account: TelegramAccount, analysis_run_id: str) -> dict[str, Any]:
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

    transcript_artifact = _plain_transcript_artifact(details)
    transcript_payload = _download_artifact_payload(str(transcript_artifact["artifact_id"]))
    transcript_text = transcript_payload.decode("utf-8", errors="replace")
    _assert(transcript_text.strip() != "", "transcript payload decoded to empty text")

    manifest_artifact = _artifact_by_kind(details, "run_manifest")
    manifest_payload = json.loads(_download_artifact_payload(str(manifest_artifact["artifact_id"])).decode("utf-8"))
    backend = manifest_payload.get("transcription_backend")
    _assert(isinstance(backend, dict), f"run_manifest missing transcription_backend: {manifest_payload}")
    _assert(backend.get("provider") == "copperasr", f"run_manifest provider is not copperasr: {backend}")
    _assert("whisper" not in json.dumps(manifest_payload, ensure_ascii=False).lower(), "run_manifest leaked removed ASR wording")

    return {
        "artifact_ids": [str(artifact["artifact_id"]) for artifact in details],
        "public_kinds": sorted(public_kinds),
        "worker_kinds": sorted(worker_kinds),
        "transcript_artifact_id": str(transcript_artifact["artifact_id"]),
        "transcript_bytes": len(transcript_payload),
        "transcript_delivery_mode": "text" if len(transcript_payload) <= 3500 else "document",
        "transcript_preview": transcript_text[:120],
        "manifest_backend": backend,
    }


def _assert_run_events(account: TelegramAccount, analysis_run_id: str) -> dict[str, Any]:
    events = _list_run_events(account, analysis_run_id)
    event_types = [str(event.get("event_type") or "") for event in events]
    _assert("analysis_run.created" in event_types, f"analysis_run.created event missing: {event_types}")
    _assert("analysis_run_step.progress" in event_types, f"analysis_run_step.progress event missing: {event_types}")
    _assert("analysis_run_step.finalized" in event_types, f"analysis_run_step.finalized event missing: {event_types}")
    return {"event_types": event_types}


def _assert_diagnostics(account: TelegramAccount, analysis_run_id: str) -> dict[str, Any]:
    page = _request(
        f"/v1/diagnostics?{account.query()}&subject_type=analysis_run&subject_id={analysis_run_id}&page_size=20"
    )
    diagnostics = list(page.get("items") or [])
    codes = {str(item.get("code") or "") for item in diagnostics}
    _assert("backend_unavailable" not in codes, f"unexpected generic backend_unavailable diagnostic: {codes}")
    return {"diagnostic_count": len(diagnostics), "diagnostic_codes": sorted(codes)}


def _deliver_result_surface(
    account: TelegramAccount,
    *,
    artifact_id: str,
    analysis_run_id: str,
    delivery_mode: str,
    suffix: str,
) -> dict[str, Any]:
    before = _list_active_surfaces(account, subject_type="artifact", subject_id=artifact_id)
    _assert(before == [], f"result surface already exists before delivery: {before}")
    surface = _upsert_surface(
        account,
        surface_type="result_artifact_surface",
        surface_key=f"artifact:{artifact_id}",
        message_id=9001,
        display_state={
            "analysis_run_id": analysis_run_id,
            "artifact_id": artifact_id,
            "delivery_mode": delivery_mode,
            "kind": "transcript",
        },
        subjects=[
            _surface_subject("artifact", artifact_id, "primary"),
            _surface_subject("analysis_run", analysis_run_id, "context"),
        ],
        idempotency_key=f"telegram:surface:artifact:{artifact_id}:{suffix}",
    )
    after = _list_active_surfaces(account, subject_type="artifact", subject_id=artifact_id)
    _assert(len(after) == 1, f"result surface was not recorded exactly once: {after}")
    duplicate_prevented = bool(after[0].get("address"))
    if not duplicate_prevented:
        raise TelegramE2EError("duplicate guard cannot work without a persisted Telegram address")
    return {
        "surface_id": str(surface["channel_surface_id"]),
        "surface_type": surface["surface_type"],
        "surface_key": surface["surface_key"],
        "delivery_mode": delivery_mode,
        "duplicate_prevented": duplicate_prevented,
        "active_surface_count": len(after),
    }


def _clear_inbox(account: TelegramAccount, collection: dict[str, Any], media_asset_id: str) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {
            "channel_account_id": account.channel_account_id,
            "expected_version": str(collection["version"]),
        }
    )
    cleared = _request(
        f"/v1/collections/{collection['collection_id']}/items/{media_asset_id}?{query}",
        method="DELETE",
    )["collection"]
    refreshed = _inbox(account)
    remaining_ids = [str(item.get("media_asset_id") or "") for item in refreshed.get("items", [])]
    _assert(media_asset_id not in remaining_ids, f"media asset still visible in inbox after clear: {remaining_ids}")
    return {
        "collection_id": str(cleared["collection_id"]),
        "version_after_clear": int(cleared["version"]),
        "remaining_item_count": len(remaining_ids),
    }


def run_telegram_e2e() -> dict[str, Any]:
    suffix = uuid.uuid4().hex[:12]
    _wait_for_api()
    account = _resolve_telegram_account(suffix=suffix)
    case, stored_object = _case("short_voice")
    media = _multipart_upload(account, stored_object, case_id=str(case["case_id"]), suffix=suffix)
    media_asset_id = str(media["media_asset_id"])
    collection = _inbox(account)
    collection_items = list(collection.get("items") or [])
    _assert(
        any(str(item.get("media_asset_id") or "") == media_asset_id for item in collection_items),
        "uploaded Telegram voice is missing from inbox collection",
    )
    current_surface = _upsert_surface(
        account,
        surface_type="current_materials_panel",
        surface_key=f"current:{account.external_account_ref}",
        message_id=8001,
        display_state={
            "screen": "main",
            "collection_id": collection["collection_id"],
            "item_count": len(collection_items),
            "focused_run_id": "",
        },
        subjects=[_surface_subject("collection", str(collection["collection_id"]), "primary")],
        idempotency_key=f"telegram:surface:current:{account.external_account_ref}:{suffix}",
    )
    snapshot = _create_selection_snapshot(account, collection, media_asset_id, suffix=suffix)
    run = _create_analysis_run(account, str(snapshot["selection_snapshot_id"]), suffix=suffix)
    analysis_run_id = str(run["analysis_run_id"])
    analysis_step_ids = [
        str(step.get("analysis_run_step_id") or "")
        for step in run.get("analysis_run_steps", [])
        if step.get("analysis_run_step_id")
    ]
    _assert(analysis_step_ids, f"analysis run did not expose planned steps: {run}")
    task_surface = _upsert_surface(
        account,
        surface_type="analysis_task_surface",
        surface_key=f"analysis_run:{analysis_run_id}",
        message_id=8002,
        display_state={
            "analysis_run_id": analysis_run_id,
            "status": run["status"],
            "screen": "main",
            "focused_run_id": analysis_run_id,
        },
        subjects=[_surface_subject("analysis_run", analysis_run_id, "primary")],
        idempotency_key=f"telegram:surface:analysis-run:{analysis_run_id}:{suffix}",
    )
    terminal = _poll_run(account, analysis_run_id)
    _assert(
        terminal["status"] in {"succeeded", "partially_succeeded"},
        f"transcription run finished in unexpected state {terminal['status']}",
    )
    event_result = _assert_run_events(account, analysis_run_id)
    artifact_result = _assert_artifacts(account, analysis_run_id)
    diagnostics_result = _assert_diagnostics(account, analysis_run_id)
    result_surface = _deliver_result_surface(
        account,
        artifact_id=artifact_result["transcript_artifact_id"],
        analysis_run_id=analysis_run_id,
        delivery_mode=artifact_result["transcript_delivery_mode"],
        suffix=suffix,
    )
    inbox_clear = _clear_inbox(account, collection, media_asset_id)
    return {
        "telegram_flow": {
            "channel_account_id": account.channel_account_id,
            "external_account_ref": account.external_account_ref,
            "media_asset_id": media_asset_id,
            "media_kind": media.get("kind"),
            "collection_id": str(collection["collection_id"]),
            "inbox_item_count_before_clear": len(collection_items),
            "current_materials_surface_id": str(current_surface["channel_surface_id"]),
            "selection_snapshot_id": str(snapshot["selection_snapshot_id"]),
            "analysis_run_id": analysis_run_id,
            "analysis_run_step_ids": analysis_step_ids,
            "analysis_task_surface_id": str(task_surface["channel_surface_id"]),
            "terminal_status": terminal["status"],
            **event_result,
            **artifact_result,
            **diagnostics_result,
            "result_surface": result_surface,
            "inbox_clear": inbox_clear,
        }
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run live Telegram-shaped CopperASR voice-to-transcript E2E proof.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable proof.")
    args = parser.parse_args(argv)
    try:
        result = run_telegram_e2e()
    except TelegramE2EError as exc:
        print(f"[CopperAsrTelegramE2E] {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print("[CopperAsrTelegramE2E] completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
