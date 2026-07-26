#!/usr/bin/env python3
"""Isolated Compose proof for deduplication, export recovery, and physical retention."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


class E2EError(RuntimeError):
    pass


def _local_url(value: str, *, name: str) -> str:
    parsed = urllib.parse.urlparse(value.rstrip("/"))
    if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise E2EError(f"{name} must be an http localhost endpoint")
    if parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise E2EError(f"{name} must be a bare localhost endpoint without credentials, path, query, or fragment")
    return urllib.parse.urlunparse(parsed)


API_BASE_URL = _local_url(os.environ.get("MEDIA_EXPORT_E2E_API_BASE_URL", "http://127.0.0.1:8080"), name="MEDIA_EXPORT_E2E_API_BASE_URL")
MINIO_HOST_ENDPOINT = _local_url(
    os.environ.get("MEDIA_EXPORT_E2E_MINIO_HOST_ENDPOINT", f"http://127.0.0.1:{os.environ.get('MINIO_HOST_PORT', '19100')}"),
    name="MEDIA_EXPORT_E2E_MINIO_HOST_ENDPOINT",
)
POLL_TIMEOUT_SECONDS = int(os.environ.get("MEDIA_EXPORT_E2E_POLL_TIMEOUT_SECONDS", "180"))
POLL_INTERVAL_SECONDS = float(os.environ.get("MEDIA_EXPORT_E2E_POLL_INTERVAL_SECONDS", "2"))
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")
REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
POSTGRES_USER = "telegram_transcriber"
POSTGRES_DB = "telegram_transcriber"


@dataclass(frozen=True)
class LocalConfig:
    internal_token: str
    minio_access_key: str
    minio_secret_key: str


def _config() -> LocalConfig:
    # Defaults are the documented local Compose example values. Do not load dotenv files.
    return LocalConfig(
        internal_token=os.environ.get("PLATFORM_INTERNAL_TOKEN", "local-media-platform-internal"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise E2EError(message)


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _compose(project: str, env: Mapping[str, str], *args: str, timeout: int = 300) -> str:
    command = ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE), *args]
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env={**os.environ, **dict(env)},
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        stderr = getattr(exc, "stderr", "") or ""
        summary = stderr.strip().splitlines()[-1] if stderr.strip() else "compose command failed"
        raise E2EError(summary) from exc
    return completed.stdout.strip()


def _psql(project: str, env: Mapping[str, str], sql: str) -> str:
    return _compose(
        project,
        env,
        "exec",
        "-T",
        "postgres",
        "psql",
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        POSTGRES_USER,
        "-d",
        POSTGRES_DB,
        "-Atq",
        "-c",
        sql,
        timeout=60,
    )


def _uuid(value: str, name: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise E2EError(f"{name} is not a UUID") from exc
    return str(parsed)


def _api_url(path: str) -> str:
    return urllib.parse.urljoin(f"{API_BASE_URL}/", path.lstrip("/"))


def _request(
    path: str,
    *,
    method: str = "GET",
    body: Mapping[str, Any] | None = None,
    data: bytes | None = None,
    headers: Mapping[str, str] | None = None,
    expected: tuple[int, ...] = (200,),
) -> dict[str, Any]:
    if body is not None and data is not None:
        raise ValueError("request accepts either JSON body or raw data")
    request_data = json.dumps(body, separators=(",", ":")).encode("utf-8") if body is not None else data
    request_headers = {"Accept": "application/json", **(dict(headers) if headers else {})}
    if body is not None:
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(_api_url(path), data=request_data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            if response.status not in expected:
                raise E2EError(f"{method} {path} returned unexpected status {response.status}")
    except urllib.error.HTTPError as exc:
        if exc.code not in expected:
            raise E2EError(f"{method} {path} returned unexpected status {exc.code}") from exc
        raw = exc.read()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise E2EError(f"{method} {path} could not reach localhost service") from exc
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise E2EError(f"{method} {path} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise E2EError(f"{method} {path} returned a non-object JSON payload")
    return payload


def _internal_headers(config: LocalConfig) -> dict[str, str]:
    return {"X-Platform-Internal-Token": config.internal_token} if config.internal_token else {}


def _multipart_upload(*, metadata: Mapping[str, Any], filename: str, content_type: str, payload: bytes, idempotency_key: str) -> dict[str, Any]:
    boundary = f"----media-export-e2e-{uuid.uuid4().hex}"
    encoded_metadata = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    body = b"".join(
        (
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"metadata\"\r\nContent-Type: application/json\r\n\r\n".encode(),
            encoded_metadata,
            b"\r\n",
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n".encode(),
            payload,
            f"\r\n--{boundary}--\r\n".encode(),
        )
    )
    return _request(
        "/v1/media-assets/upload",
        method="POST",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Idempotency-Key": idempotency_key},
        expected=(201,),
    )


def _canonical_uri(bucket: str, object_key: str) -> str:
    return "/" + "/".join(urllib.parse.quote(part, safe="") for part in (bucket, *object_key.split("/")))


def _signature_key(secret: str, date_stamp: str, region: str) -> bytes:
    key = ("AWS4" + secret).encode("utf-8")
    for value in (date_stamp, region, "s3", "aws4_request"):
        key = hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()
    return key


def _s3_get_optional(config: LocalConfig, *, bucket: str, object_key: str) -> bytes | None:
    endpoint = urllib.parse.urlparse(MINIO_HOST_ENDPOINT)
    host = endpoint.netloc
    now = datetime.now(UTC)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_uri = _canonical_uri(bucket, object_key)
    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
    canonical_request = "\n".join(("GET", canonical_uri, "", canonical_headers, signed_headers, payload_hash))
    scope = f"{date_stamp}/{MINIO_REGION}/s3/aws4_request"
    string_to_sign = "\n".join(("AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()))
    signature = hmac.new(_signature_key(config.minio_secret_key, date_stamp, MINIO_REGION), string_to_sign.encode(), hashlib.sha256).hexdigest()
    authorization = f"AWS4-HMAC-SHA256 Credential={config.minio_access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    url = urllib.parse.urlunparse((endpoint.scheme, endpoint.netloc, canonical_uri, "", "", ""))
    request = urllib.request.Request(url, headers={"X-Amz-Content-Sha256": payload_hash, "X-Amz-Date": amz_date, "Authorization": authorization}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise E2EError("S3-backed source object could not be accessed through local MinIO") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise E2EError("S3-backed source object could not be accessed through local MinIO") from exc


def _s3_get(config: LocalConfig, *, bucket: str, object_key: str) -> bytes:
    payload = _s3_get_optional(config, bucket=bucket, object_key=object_key)
    if payload is None:
        raise E2EError("S3-backed source object is missing")
    return payload


def _download_local(url: str) -> bytes:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1", "minio"}:
        raise E2EError("export download URL is not local MinIO")
    if parsed.hostname == "minio":
        host = urllib.parse.urlparse(MINIO_HOST_ENDPOINT)
        url = urllib.parse.urlunparse(parsed._replace(scheme=host.scheme, netloc=host.netloc))
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=30) as response:
            return response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
        raise E2EError("export output could not be downloaded from local MinIO") from exc


def _resolve_channel_account(config: LocalConfig, suffix: str) -> str:
    payload = _request(
        "/internal/v1/channel-accounts",
        method="PUT",
        body={
            "channel": "mcp",
            "external_account_ref": f"media-export-retention-e2e-{suffix}",
            "display_name": f"Media export retention E2E {suffix}",
            "metadata": {"media_export_retention_e2e": True},
        },
        headers=_internal_headers(config),
    )
    account = payload.get("channel_account")
    _assert(isinstance(account, dict), "channel account response is missing channel_account")
    account_id = str(account.get("channel_account_id", ""))
    _assert(account_id != "", "channel account id is empty")
    return account_id


def _wait_for_api(config: LocalConfig) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            _request("/internal/v1/channel-accounts?page_size=1", headers=_internal_headers(config))
            return
        except E2EError:
            time.sleep(2)
    raise E2EError("local API did not become ready")


def _wait_for_export(channel_account_id: str, export_job_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    last_status = "unknown"
    while time.monotonic() < deadline:
        payload = _request(f"/v1/export-jobs/{export_job_id}?{urllib.parse.urlencode({'channel_account_id': channel_account_id})}")
        job = payload.get("export_job")
        _assert(isinstance(job, dict), "export job response is missing export_job")
        last_status = str(job.get("status", "unknown"))
        if last_status in {"succeeded", "failed", "canceled", "expired"}:
            return job
        time.sleep(POLL_INTERVAL_SECONDS)
    raise E2EError(f"export job did not become terminal (last status: {last_status})")


def _generated_video() -> bytes:
    with tempfile.TemporaryDirectory(prefix="media-export-e2e-") as directory:
        video = Path(directory) / "source.mp4"
        command = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", "color=c=black:s=16x16:d=1",
            "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
            "-shortest", "-c:v", "mpeg4", "-c:a", "aac", str(video),
        ]
        try:
            subprocess.run(command, check=True, capture_output=True, timeout=30)
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise E2EError("--with-export requires a working host ffmpeg") from exc
        return video.read_bytes()


def _ack_export_delivery(config: LocalConfig, channel_account_id: str, job: Mapping[str, Any], suffix: str) -> str:
    export_job_id = _uuid(str(job.get("export_job_id", "")), "export job id")
    deliveries = job.get("deliveries")
    _assert(isinstance(deliveries, list) and len(deliveries) == 1, "export did not create one delivery")
    delivery = deliveries[0]
    _assert(isinstance(delivery, dict) and delivery.get("status") == "pending", "export delivery is not pending")
    lease_owner = f"media-export-e2e-delivery-{suffix[:12]}"
    claim = _request(
        f"/v1/export-jobs/{export_job_id}/deliveries/claim",
        method="POST",
        body={
            "channel_account_id": channel_account_id,
            "channel": "telegram",
            "lease_owner": lease_owner,
            "lease_seconds": 60,
        },
    )
    claimed_delivery = claim.get("delivery")
    _assert(isinstance(claimed_delivery, dict) and claimed_delivery.get("status") == "claimed", "delivery was not claimed")
    delivery_id = _uuid(str(claimed_delivery.get("export_delivery_id", "")), "export delivery id")
    acknowledged = _request(
        f"/v1/export-jobs/{export_job_id}/deliveries/ack",
        method="POST",
        body={
            "channel_account_id": channel_account_id,
            "export_delivery_id": delivery_id,
            "lease_owner": lease_owner,
            "attempt_token": str(claim.get("attempt_token", "")),
        },
    ).get("delivery")
    _assert(isinstance(acknowledged, dict) and acknowledged.get("status") == "delivered", "delivery was not acknowledged")
    return delivery_id


def _assert_workspace_clean(project: str, env: Mapping[str, str], export_job_id: str) -> None:
    job_id = _uuid(export_job_id, "export job id")
    matches = _compose(
        project,
        env,
        "exec",
        "-T",
        "worker-media-export",
        "find",
        "/tmp/runtime/media-export",
        "-mindepth",
        "1",
        "-maxdepth",
        "1",
        "-name",
        f"{job_id}--*",
        "-print",
        timeout=30,
    )
    _assert(matches == "", "export attempt workspace was not removed")


def _run_export(
    config: LocalConfig,
    channel_account_id: str,
    suffix: str,
    *,
    project: str,
    compose_env: Mapping[str, str],
) -> dict[str, Any]:
    video = _generated_video()
    media_asset = _multipart_upload(
        metadata={"channel_account_id": channel_account_id, "kind": "video", "display_name": "media-export-e2e.mp4", "metadata": {"media_export_retention_e2e": True}},
        filename="media-export-e2e.mp4",
        content_type="video/mp4",
        payload=video,
        idempotency_key=f"media-export-retention-e2e-video-{suffix}",
    ).get("media_asset")
    _assert(isinstance(media_asset, dict), "video upload response is missing media_asset")
    asset_id = str(media_asset.get("media_asset_id", ""))
    _assert(asset_id != "", "video media asset id is empty")
    job_payload = _request(
        f"/v1/media-assets/{asset_id}/exports",
        method="POST",
        body={"channel_account_id": channel_account_id, "operation": "video_to_audio", "variant": {"audio_bitrate_kbps": 64}},
        headers={"Idempotency-Key": f"media-export-retention-e2e-job-{suffix}"},
        expected=(201,),
    )
    job = job_payload.get("export_job")
    _assert(isinstance(job, dict), "create export response is missing export_job")
    export_job_id = str(job.get("export_job_id", ""))
    _assert(export_job_id != "", "export job id is empty")
    terminal = _wait_for_export(channel_account_id, export_job_id)
    _assert(terminal.get("status") == "succeeded", f"generated video export ended as {terminal.get('status')}")
    download = _request(f"/v1/export-jobs/{export_job_id}/download?{urllib.parse.urlencode({'channel_account_id': channel_account_id})}")
    output = _download_local(str(download.get("url", "")))
    _assert(len(output) == int(download.get("size_bytes", -1)), "export download size differs from response")
    _assert(len(output) > 0 and str(download.get("content_type", "")).startswith("audio/"), "export download is not an audio object")
    delivery_id = _ack_export_delivery(config, channel_account_id, terminal, suffix)
    _assert_workspace_clean(project, compose_env, export_job_id)
    return {
        "media_asset_id": asset_id,
        "export_job_id": export_job_id,
        "delivery_id": delivery_id,
        "output_size_bytes": len(output),
        "workspace_clean": True,
    }


def _run_restart_recovery(
    config: LocalConfig,
    channel_account_id: str,
    suffix: str,
    *,
    project: str,
    compose_env: Mapping[str, str],
) -> dict[str, Any]:
    _compose(project, compose_env, "stop", "worker-media-export", timeout=60)
    worker_started = False
    asset_id = ""
    export_job_id = ""
    try:
        video = _generated_video()
        media_asset = _multipart_upload(
            metadata={
                "channel_account_id": channel_account_id,
                "kind": "video",
                "display_name": "media-export-recovery.mp4",
                "metadata": {"media_export_retention_e2e": True, "recovery": True},
            },
            filename="media-export-recovery.mp4",
            content_type="video/mp4",
            payload=video,
            idempotency_key=f"media-export-retention-e2e-recovery-video-{suffix}",
        ).get("media_asset")
        _assert(isinstance(media_asset, dict), "recovery video upload response is missing media_asset")
        asset_id = _uuid(str(media_asset.get("media_asset_id", "")), "recovery media asset id")
        job = _request(
            f"/v1/media-assets/{asset_id}/exports",
            method="POST",
            body={
                "channel_account_id": channel_account_id,
                "operation": "video_to_audio",
                "variant": {"audio_bitrate_kbps": 64},
            },
            headers={"Idempotency-Key": f"media-export-retention-e2e-recovery-job-{suffix}"},
            expected=(201,),
        ).get("export_job")
        _assert(isinstance(job, dict), "recovery export response is missing export_job")
        export_job_id = _uuid(str(job.get("export_job_id", "")), "recovery export job id")
        claim = _request(
            f"/internal/v1/export-jobs/{export_job_id}/claim",
            method="POST",
            body={"lease_owner": f"media-export-e2e-crashed-{suffix[:12]}", "lease_seconds": 2},
            headers=_internal_headers(config),
        )
        _assert(str((claim.get("export_job") or {}).get("status", "")) == "claimed", "recovery export was not claimed")

        _compose(project, compose_env, "restart", "api", timeout=90)
        _wait_for_api(config)
        time.sleep(3)
        reclaimed = _request(
            "/internal/v1/export-jobs/reclaim",
            method="POST",
            body={"batch_size": 100},
            headers=_internal_headers(config),
        )
        _assert(
            int(reclaimed.get("examined", 0)) == 1
            and int(reclaimed.get("requeued", 0)) == 1
            and int(reclaimed.get("failed", 0)) == 0,
            "expired export lease was not uniquely requeued after API restart",
        )
        recovered = _request(
            f"/v1/export-jobs/{export_job_id}?{urllib.parse.urlencode({'channel_account_id': channel_account_id})}"
        ).get("export_job")
        _assert(isinstance(recovered, dict) and recovered.get("status") == "queued", "reclaimed export is not queued for retry")

        _compose(project, compose_env, "start", "worker-media-export", timeout=90)
        worker_started = True
        terminal = _wait_for_export(channel_account_id, export_job_id)
        _assert(terminal.get("status") == "succeeded", f"recovered export ended as {terminal.get('status')}")
        delivery_id = _ack_export_delivery(config, channel_account_id, terminal, suffix + "-recovery")
        _assert_workspace_clean(project, compose_env, export_job_id)
        return {
            "media_asset_id": asset_id,
            "export_job_id": export_job_id,
            "delivery_id": delivery_id,
            "lease_requeued": True,
            "workspace_clean": True,
        }
    finally:
        if not worker_started:
            try:
                _compose(project, compose_env, "start", "worker-media-export", timeout=90)
            except E2EError:
                pass


def _soft_delete(channel_account_id: str, asset_id: str) -> None:
    deleted = _request(
        f"/v1/media-assets/{_uuid(asset_id, 'media asset id')}?{urllib.parse.urlencode({'channel_account_id': channel_account_id})}",
        method="DELETE",
    ).get("media_asset")
    _assert(isinstance(deleted, dict) and deleted.get("status") == "deleted", "E2E media asset was not soft-deleted")


def _expire_duplicate_source(
    config: LocalConfig,
    *,
    project: str,
    compose_env: Mapping[str, str],
    channel_account_id: str,
    stored_object_id: str,
    object_key: str,
    asset_ids: list[str],
) -> dict[str, Any]:
    account_id = _uuid(channel_account_id, "channel account id")
    object_id = _uuid(stored_object_id, "stored object id")
    updated = _psql(
        project,
        compose_env,
        f"""
WITH updated AS (
    UPDATE stored_objects
    SET expires_at=to_timestamp(0)
    WHERE id='{object_id}'::uuid
      AND channel_account_id='{account_id}'::uuid
      AND storage_status='available'
      AND hold_state='none'
      AND NOT EXISTS (
          SELECT 1 FROM stored_object_pins
          WHERE stored_object_id='{object_id}'::uuid AND released_at IS NULL
      )
    RETURNING id
)
SELECT id FROM updated;
""",
    )
    _assert(updated == object_id, "isolated source object could not be made retention-eligible")
    first_candidate = _psql(
        project,
        compose_env,
        """
SELECT id
FROM stored_objects so
WHERE so.storage_status='available'
  AND so.expires_at IS NOT NULL AND so.expires_at <= now()
  AND so.hold_state='none'
  AND NOT EXISTS (
      SELECT 1 FROM stored_object_pins p
      WHERE p.stored_object_id=so.id AND p.released_at IS NULL
  )
ORDER BY so.expires_at ASC
LIMIT 1;
""",
    )
    _assert(first_candidate == object_id, "isolated retention target is not the first eligible object")
    sweep = _request(
        "/internal/v1/retention/sweep",
        method="POST",
        body={"batch_size": 1, "deletion_owner": f"media-export-e2e-{project[-12:]}", "claim_seconds": 60},
        headers=_internal_headers(config),
    )
    claims = sweep.get("claims")
    _assert(
        int(sweep.get("claimed", 0)) == 1
        and int(sweep.get("deleted", 0)) == 1
        and int(sweep.get("failed", 0)) == 0
        and isinstance(claims, list)
        and len(claims) == 1
        and claims[0].get("stored_object_id") == object_id,
        "retention sweep did not delete exactly the isolated source object",
    )
    _assert(
        _s3_get_optional(config, bucket="sources", object_key=object_key) is None,
        "retention sweep left source bytes in MinIO",
    )
    object_state = _psql(
        project,
        compose_env,
        f"""
SELECT storage_status || '|' || retention_state || '|' || (deleted_at IS NOT NULL)::text
FROM stored_objects
WHERE id='{object_id}'::uuid AND channel_account_id='{account_id}'::uuid;
""",
    )
    _assert(object_state == "deleted|expired|true", f"stored object history has unexpected state: {object_state}")
    for asset_id in asset_ids:
        preserved = _request(
            f"/v1/media-assets/{_uuid(asset_id, 'media asset id')}?{urllib.parse.urlencode({'channel_account_id': account_id})}"
        ).get("media_asset")
        _assert(
            isinstance(preserved, dict)
            and preserved.get("status") == "deleted"
            and str((preserved.get("origin") or {}).get("stored_object_id", "")) == object_id,
            "soft-deleted media history was not preserved after physical expiry",
        )
    return {"stored_object_state": object_state, "s3_body_deleted": True, "media_history_preserved": True}


def _verify_reconcile_dry_run(
    config: LocalConfig,
    *,
    project: str,
    compose_env: Mapping[str, str],
    source_bytes: bytes,
    object_key: str,
) -> dict[str, Any]:
    state_before = _psql(
        project,
        compose_env,
        """
SELECT COALESCE(string_agg(
    id::text || '|' || storage_status || '|' || retention_state || '|' || generation::text,
    ',' ORDER BY id
), '')
FROM stored_objects;
""",
    )
    cursors_before = _psql(
        project,
        compose_env,
        "SELECT COALESCE(string_agg(name || '=' || cursor, ',' ORDER BY name), '') FROM storage_reconcile_cursors;",
    )
    reconciled = _request(
        "/internal/v1/retention/reconcile",
        method="POST",
        body={"batch_size": 1000, "dry_run": True},
        headers=_internal_headers(config),
    )
    _assert(int(reconciled.get("examined", 0)) > 0, "retention dry-run did not examine isolated stored objects")
    _assert(
        int(reconciled.get("orphans_deleted", -1)) == 0
        and int(reconciled.get("publications_reconciled", -1)) == 0
        and int(reconciled.get("objects_marked_missing", -1)) == 0,
        "retention dry-run found unexpected candidates in the coherent isolated store",
    )
    state_after = _psql(
        project,
        compose_env,
        """
SELECT COALESCE(string_agg(
    id::text || '|' || storage_status || '|' || retention_state || '|' || generation::text,
    ',' ORDER BY id
), '')
FROM stored_objects;
""",
    )
    cursors_after = _psql(
        project,
        compose_env,
        "SELECT COALESCE(string_agg(name || '=' || cursor, ',' ORDER BY name), '') FROM storage_reconcile_cursors;",
    )
    _assert(state_after == state_before, "retention dry-run mutated stored-object state")
    _assert(cursors_after == cursors_before, "retention dry-run advanced reconciliation cursors")
    _assert(
        _s3_get(config, bucket="sources", object_key=object_key) == source_bytes,
        "retention dry-run mutated canonical source bytes",
    )
    return {
        "examined": int(reconciled["examined"]),
        "candidates": 0,
        "database_unchanged": True,
        "cursors_unchanged": True,
        "s3_body_unchanged": True,
    }


def _run_proof(*, project: str, compose_env: Mapping[str, str]) -> dict[str, Any]:
    config = _config()
    _wait_for_api(config)
    suffix = uuid.uuid4().hex
    account_id = _resolve_channel_account(config, suffix)
    source_bytes = ("media-export-retention-e2e:" + suffix + ":identical-source").encode("utf-8")
    uploaded: list[dict[str, Any]] = []
    for index in (1, 2):
        response = _multipart_upload(
            metadata={"channel_account_id": account_id, "kind": "document", "display_name": f"identical-{index}.txt", "metadata": {"media_export_retention_e2e": True}},
            filename=f"identical-{index}.txt",
            content_type="text/plain",
            payload=source_bytes,
            idempotency_key=f"media-export-retention-e2e-upload-{suffix}-{index}",
        )
        asset = response.get("media_asset")
        _assert(isinstance(asset, dict), "upload response is missing media_asset")
        uploaded.append(asset)

    asset_ids = [str(asset.get("media_asset_id", "")) for asset in uploaded]
    stored_object_ids = [str((asset.get("origin") or {}).get("stored_object_id", "")) for asset in uploaded]
    object_keys = [str((asset.get("origin") or {}).get("object_ref", "")) for asset in uploaded]
    _assert(all(asset_ids), "uploaded media asset id is empty")
    _assert(len(set(stored_object_ids)) == 1 and stored_object_ids[0] != "", "identical uploads did not share one stored object identity")
    _assert(len(set(object_keys)) == 1 and object_keys[0] != "", "identical uploads did not share one published object key")

    listed = _request(f"/v1/media-assets?{urllib.parse.urlencode({'channel_account_id': account_id, 'page_size': 20})}")
    occurrences = [item for item in listed.get("items", []) if isinstance(item, dict) and item.get("media_asset_id") in asset_ids]
    _assert(len(occurrences) == 2, "isolated account does not contain exactly two duplicate media occurrences")
    _assert(_s3_get(config, bucket="sources", object_key=object_keys[0]) == source_bytes, "published S3 object bytes differ from uploaded source")

    export_summary = _run_export(
        config,
        account_id,
        suffix,
        project=project,
        compose_env=compose_env,
    )
    recovery_summary = _run_restart_recovery(
        config,
        account_id,
        suffix,
        project=project,
        compose_env=compose_env,
    )
    reconcile_summary = _verify_reconcile_dry_run(
        config,
        project=project,
        compose_env=compose_env,
        source_bytes=source_bytes,
        object_key=object_keys[0],
    )
    cleanup_ids = [*asset_ids, export_summary["media_asset_id"], recovery_summary["media_asset_id"]]
    for asset_id in cleanup_ids:
        _soft_delete(account_id, asset_id)
    retention_summary = _expire_duplicate_source(
        config,
        project=project,
        compose_env=compose_env,
        channel_account_id=account_id,
        stored_object_id=stored_object_ids[0],
        object_key=object_keys[0],
        asset_ids=asset_ids,
    )
    recovered_history = _request(
        f"/v1/export-jobs/{recovery_summary['export_job_id']}?{urllib.parse.urlencode({'channel_account_id': account_id})}"
    ).get("export_job")
    _assert(isinstance(recovered_history, dict) and recovered_history.get("status") == "succeeded", "export history was not preserved")

    return {
        "ok": True,
        "occurrences": len(occurrences),
        "source_size_bytes": len(source_bytes),
        "deduplicated_stored_objects": len(set(stored_object_ids)),
        "s3_publication_verified": True,
        "export": export_summary,
        "restart_recovery": recovery_summary,
        "reconciliation_dry_run": reconcile_summary,
        "retention": retention_summary,
    }


def run() -> dict[str, Any]:
    global API_BASE_URL, MINIO_HOST_ENDPOINT

    suffix = uuid.uuid4().hex
    project = f"media-retention-e2e-{suffix[:12]}"
    api_port, minio_port, minio_console_port = (_free_loopback_port() for _ in range(3))
    while len({api_port, minio_port, minio_console_port}) != 3:
        api_port, minio_port, minio_console_port = (_free_loopback_port() for _ in range(3))
    API_BASE_URL = f"http://127.0.0.1:{api_port}"
    MINIO_HOST_ENDPOINT = f"http://127.0.0.1:{minio_port}"
    compose_env = {
        "API_HOST_PORT": str(api_port),
        "MINIO_HOST_PORT": str(minio_port),
        "MINIO_CONSOLE_HOST_PORT": str(minio_console_port),
        "MINIO_PUBLIC_ENDPOINT": MINIO_HOST_ENDPOINT,
    }
    try:
        _compose(
            project,
            compose_env,
            "up",
            "-d",
            "--build",
            "--wait",
            "api",
            "worker-media-export",
            timeout=900,
        )
        result = _run_proof(project=project, compose_env=compose_env)
        result["isolated_compose"] = True
        return result
    finally:
        try:
            _compose(project, compose_env, "down", "-v", "--remove-orphans", timeout=180)
        except E2EError:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit a JSON-only result")
    args = parser.parse_args(argv)
    try:
        result = run()
    except (E2EError, ValueError) as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")))
        else:
            print(f"media-export retention E2E failed: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, separators=(",", ":")))
    else:
        print("media-export retention E2E passed")
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
