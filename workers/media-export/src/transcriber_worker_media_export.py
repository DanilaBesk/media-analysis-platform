# FILE: workers/media-export/src/transcriber_worker_media_export.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Execute API-owned, fenced media export jobs in bounded attempt workspaces.
# SCOPE: Queue claim/poll, source materialization, yt-dlp/ffmpeg invocation, transient output staging, progress, cancellation, and terminal finalization.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-MEDIA-EXPORT, V-MEDIA-EXPORT
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import logging
import math
import os
import signal
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from urllib import error, parse, request

from transcriber_workers_common.workspace import attempt_workspace, reap_abandoned_workspaces


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER = "[WorkerMediaExport][run_export_job]"
_AUDIO_OPERATIONS = frozenset({"youtube_audio", "video_to_audio"})
_OPERATIONS = frozenset({*_AUDIO_OPERATIONS, "youtube_video"})
_AAC_AUDIO_PROFILES = frozenset({"audio_m4a_aac_legacy", "audio_m4a_aac_v1"})
_OUTPUT_PROFILES = frozenset({*_AAC_AUDIO_PROFILES, "audio_ogg_opus_v1", "video_mp4_v1"})

__all__ = [
    "ExportClaim",
    "ExportControlClient",
    "ExportJob",
    "ExportSource",
    "HttpExportControlClient",
    "MediaExportWorker",
    "MediaExportWorkerConfig",
    "MinioExportObjectStore",
]


class ExportWorkerError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ExportCancellationRequested(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ExportJob:
    export_job_id: str
    operation: str
    variant: Mapping[str, object]
    output_profile: str | None = None

    def __post_init__(self) -> None:
        if not self.export_job_id.strip():
            raise ValueError("export_job_id must not be empty")
        if self.operation not in _OPERATIONS:
            raise ValueError("unsupported export operation")
        if self.output_profile is not None and self.output_profile not in _OUTPUT_PROFILES:
            raise ValueError("unsupported export output profile")

    @classmethod
    def from_payload(cls, payload: object) -> "ExportJob":
        data = _mapping(payload, "export job")
        return cls(
            export_job_id=_string(data.get("export_job_id"), "export_job_id"),
            operation=_string(data.get("operation"), "operation"),
            variant=_mapping(data.get("variant"), "variant"),
            output_profile=_optional_string(data.get("output_profile"), "output_profile"),
        )


@dataclass(frozen=True, slots=True)
class ExportSource:
    media_asset_id: str
    source_type: str
    url: str
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.source_type not in {"uploaded_object", "remote_reference"}:
            raise ValueError("unsupported export source type")
        parsed = parse.urlparse(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("export source URL must be an http(s) URI")
        if self.source_type == "remote_reference" and parsed.scheme != "https":
            raise ValueError("remote export source URL must be HTTPS")
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("source size must be non-negative")

    @classmethod
    def from_payload(cls, payload: object) -> "ExportSource":
        data = _mapping(payload, "export source")
        value = data.get("size_bytes")
        if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
            raise ValueError("source size_bytes must be a non-negative integer")
        return cls(
            media_asset_id=_string(data.get("media_asset_id"), "media_asset_id"),
            source_type=_string(data.get("source_type"), "source_type"),
            url=_string(data.get("url"), "source URL"),
            size_bytes=value,
        )


@dataclass(frozen=True, slots=True)
class ExportClaim:
    job: ExportJob
    attempt_token: str
    lease_owner: str
    source: ExportSource

    @classmethod
    def from_payload(cls, payload: object) -> "ExportClaim":
        data = _mapping(payload, "export claim")
        return cls(
            job=ExportJob.from_payload(data.get("export_job")),
            attempt_token=_string(data.get("attempt_token"), "attempt_token"),
            lease_owner=_string(data.get("lease_owner"), "lease_owner"),
            source=ExportSource.from_payload(data.get("source")),
        )


class ExportControlClient(Protocol):
    def list_queue(self, *, page_size: int) -> tuple[ExportJob, ...]: ...

    def claim_job(self, export_job_id: str, *, lease_owner: str, lease_seconds: int) -> ExportClaim: ...

    def publish_progress(self, claim: ExportClaim, progress: Mapping[str, object]) -> None: ...

    def cancel_requested_for(self, claim: ExportClaim) -> bool: ...

    def finalize(
        self,
        claim: ExportClaim,
        *,
        outcome: str,
        output: Mapping[str, object] | None = None,
        diagnostic_code: str | None = None,
        diagnostic_message: str | None = None,
    ) -> None: ...


class ExportObjectStore(Protocol):
    def put_file(
        self,
        *,
        object_key: str,
        source: Path,
        content_type: str,
        metadata: Mapping[str, str],
        cancelled: Callable[[], bool],
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class MediaExportWorkerConfig:
    workspace_root: Path
    lease_owner: str
    api_base_url: str = "http://api:8080"
    internal_token: str = ""
    poll_interval_seconds: float = 5.0
    lease_seconds: int = 120
    max_duration_seconds: int = 14_400
    max_input_bytes: int = 4_294_967_296
    max_output_bytes: int = 2_147_483_648
    workspace_max_bytes: int = 6_442_450_944
    timeout_seconds: float = 1_800.0
    concurrency: int = 1
    workspace_orphan_grace_seconds: float = 1_800.0
    workspace_absolute_ttl_seconds: float = 86_400.0

    def __post_init__(self) -> None:
        if not self.lease_owner.strip():
            raise ValueError("lease_owner must not be empty")
        if not self.internal_token.strip():
            raise ValueError("internal_token must not be empty")
        if not 1 <= self.lease_seconds <= 900:
            raise ValueError("lease_seconds must be within 1..900")
        if self.poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be non-negative")
        for name in (
            "max_duration_seconds",
            "max_input_bytes",
            "max_output_bytes",
            "workspace_max_bytes",
            "timeout_seconds",
            "concurrency",
            "workspace_orphan_grace_seconds",
            "workspace_absolute_ttl_seconds",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_input_bytes + self.max_output_bytes > self.workspace_max_bytes:
            raise ValueError("workspace_max_bytes must cover configured input and output limits")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "MediaExportWorkerConfig":
        values = os.environ if env is None else env
        return cls(
            workspace_root=Path(values.get("WORKER_WORKSPACE_ROOT", "/tmp/runtime/media-export")),
            lease_owner=values.get("MEDIA_EXPORT_LEASE_OWNER", f"media-export-{socket.gethostname()}"),
            api_base_url=values.get("API_BASE_URL", "http://api:8080"),
            internal_token=values.get("PLATFORM_INTERNAL_TOKEN", ""),
            poll_interval_seconds=_number(values.get("WORKER_POLL_INTERVAL_SECONDS"), 5.0),
            lease_seconds=_integer(values.get("MEDIA_EXPORT_LEASE_SECONDS"), 120),
            max_duration_seconds=_integer(values.get("MEDIA_EXPORT_MAX_DURATION_SECONDS"), 14_400),
            max_input_bytes=_integer(values.get("MEDIA_EXPORT_MAX_INPUT_BYTES"), 4_294_967_296),
            max_output_bytes=_integer(values.get("MEDIA_EXPORT_MAX_OUTPUT_BYTES"), 2_147_483_648),
            workspace_max_bytes=_integer(values.get("MEDIA_EXPORT_WORKSPACE_MAX_BYTES"), 6_442_450_944),
            timeout_seconds=_number(values.get("MEDIA_EXPORT_TIMEOUT_SECONDS"), 1_800.0),
            concurrency=_integer(values.get("MEDIA_EXPORT_CONCURRENCY"), 1),
            workspace_orphan_grace_seconds=60.0
            * _number(values.get("WORKSPACE_ORPHAN_GRACE_MINUTES"), 30.0),
            workspace_absolute_ttl_seconds=3_600.0
            * _number(values.get("WORKSPACE_ABSOLUTE_TTL_HOURS"), 24.0),
        )


class HttpExportControlClient:
    """Small typed client for the internal API; every mutation is owner/token fenced."""

    def __init__(self, config: MediaExportWorkerConfig) -> None:
        self.base_url = config.api_base_url.rstrip("/")
        self.internal_token = config.internal_token

    def list_queue(self, *, page_size: int) -> tuple[ExportJob, ...]:
        payload = self._request("GET", "/internal/v1/export-jobs/queue", query={"page_size": str(page_size)})
        data = _mapping(payload, "export queue")
        items = data.get("items")
        if not isinstance(items, list):
            raise ValueError("export queue items must be a list")
        return tuple(ExportJob.from_payload(item) for item in items)

    def claim_job(self, export_job_id: str, *, lease_owner: str, lease_seconds: int) -> ExportClaim:
        return ExportClaim.from_payload(self._request("POST", f"/internal/v1/export-jobs/{parse.quote(export_job_id, safe='')}/claim", payload={"lease_owner": lease_owner, "lease_seconds": lease_seconds}))

    def publish_progress(self, claim: ExportClaim, progress: Mapping[str, object]) -> None:
        self._request("POST", self._attempt_path(claim, "progress"), payload={"lease_owner": claim.lease_owner, "attempt_token": claim.attempt_token, "progress": dict(progress)})

    def cancel_requested_for(self, claim: ExportClaim) -> bool:
        payload = self._request("GET", self._attempt_path(claim, "cancel-check"), query={"lease_owner": claim.lease_owner, "attempt_token": claim.attempt_token})
        return bool(_mapping(payload, "cancel check").get("cancel_requested"))

    def finalize(self, claim: ExportClaim, *, outcome: str, output: Mapping[str, object] | None = None, diagnostic_code: str | None = None, diagnostic_message: str | None = None) -> None:
        payload: dict[str, object] = {"lease_owner": claim.lease_owner, "attempt_token": claim.attempt_token, "outcome": outcome}
        if output is not None:
            payload["output"] = dict(output)
        if diagnostic_code is not None:
            payload["diagnostic_code"] = diagnostic_code
        if diagnostic_message is not None:
            payload["diagnostic_message"] = diagnostic_message[:1000]
        self._request("POST", self._attempt_path(claim, "finalize"), payload=payload)

    def _attempt_path(self, claim: ExportClaim, suffix: str) -> str:
        return f"/internal/v1/export-jobs/{parse.quote(claim.job.export_job_id, safe='')}/{suffix}"

    def _request(self, method: str, path: str, *, payload: Mapping[str, object] | None = None, query: Mapping[str, str] | None = None) -> object:
        url = self.base_url + path
        if query:
            url += "?" + parse.urlencode(query)
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if self.internal_token:
            headers["X-Platform-Internal-Token"] = self.internal_token
        try:
            with request.urlopen(request.Request(url, data=body, headers=headers, method=method), timeout=30) as response:
                raw = response.read()
        except error.HTTPError as exc:
            raise ExportWorkerError("export_control_request_failed", f"control plane returned HTTP {exc.code}") from exc
        except error.URLError as exc:
            raise ExportWorkerError("export_control_request_failed", "control plane request failed") from exc
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExportWorkerError("export_control_invalid_response", "control plane returned invalid JSON") from exc


class MinioExportObjectStore:
    """Stream an export body to the artifact bucket with S3 SHA-256 user metadata."""

    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        artifact_bucket: str,
        region: str = "us-east-1",
        now: Callable[[], datetime] | None = None,
        connection_factory: Callable[..., http.client.HTTPConnection] | None = None,
    ) -> None:
        parsed = parse.urlparse(endpoint.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("MINIO_ENDPOINT must be an http(s) URL")
        if not all(value.strip() for value in (access_key, secret_key, artifact_bucket)):
            raise ValueError("MinIO credentials and artifact bucket are required")
        self.endpoint = parsed
        self.access_key = access_key
        self.secret_key = secret_key
        self.artifact_bucket = artifact_bucket
        self.region = region
        self.now = now or (lambda: datetime.now(UTC))
        self.connection_factory = connection_factory

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "MinioExportObjectStore":
        values = os.environ if env is None else env
        return cls(
            endpoint=values.get("MINIO_ENDPOINT", ""),
            access_key=values.get("MINIO_ACCESS_KEY", ""),
            secret_key=values.get("MINIO_SECRET_KEY", ""),
            artifact_bucket=values.get("MINIO_BUCKET_ARTIFACTS", "artifacts"),
            region=values.get("MINIO_REGION", "us-east-1"),
        )

    def put_file(
        self,
        *,
        object_key: str,
        source: Path,
        content_type: str,
        metadata: Mapping[str, str],
        cancelled: Callable[[], bool],
    ) -> None:
        if not source.is_file():
            raise ExportWorkerError("export_output_missing", "cannot upload missing output")
        digest = metadata.get("sha256", "")
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("sha256 metadata must be a lowercase digest")
        canonical_uri = "/" + "/".join(
            parse.quote(part, safe="") for part in (self.artifact_bucket, *object_key.split("/"))
        )
        if ".." in object_key.split("/") or object_key.startswith("/"):
            raise ValueError("object_key must be a relative POSIX key")
        now = self.now().astimezone(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        host = self.endpoint.netloc
        headers = {
            "content-type": content_type,
            "host": host,
            "x-amz-content-sha256": digest,
            "x-amz-date": amz_date,
            "x-amz-meta-sha256": digest,
        }
        signed_headers = ";".join(sorted(headers))
        canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
        canonical_request = "\n".join(["PUT", canonical_uri, "", canonical_headers, signed_headers, digest])
        scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()]
        )
        signing_key = _signature_key(self.secret_key, date_stamp, self.region, "s3")
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        )
        connection = self._connection()
        try:
            connection.putrequest("PUT", canonical_uri, skip_host=True, skip_accept_encoding=True)
            connection.putheader("Host", host)
            connection.putheader("Content-Type", content_type)
            connection.putheader("Content-Length", str(source.stat().st_size))
            connection.putheader("X-Amz-Content-Sha256", digest)
            connection.putheader("X-Amz-Date", amz_date)
            connection.putheader("X-Amz-Meta-Sha256", digest)
            connection.putheader("Authorization", authorization)
            connection.endheaders()
            with source.open("rb") as body:
                while chunk := body.read(1024 * 1024):
                    if cancelled():
                        raise ExportCancellationRequested()
                    connection.send(chunk)
            response = connection.getresponse()
            response.read()
            if not 200 <= response.status < 300:
                raise ExportWorkerError("export_output_upload_failed", f"artifact upload returned HTTP {response.status}")
        except OSError as exc:
            raise ExportWorkerError("export_output_upload_failed", "artifact upload failed") from exc
        finally:
            connection.close()

    def _connection(self) -> http.client.HTTPConnection:
        factory = self.connection_factory
        if factory is not None:
            return factory(self.endpoint.hostname, self.endpoint.port)
        if self.endpoint.scheme == "https":
            return http.client.HTTPSConnection(self.endpoint.hostname, self.endpoint.port, timeout=60)
        return http.client.HTTPConnection(self.endpoint.hostname, self.endpoint.port, timeout=60)


class MediaExportWorker:
    def __init__(
        self,
        config: MediaExportWorkerConfig,
        *,
        control: ExportControlClient,
        object_store: ExportObjectStore,
        download_source: Callable[..., None] | None = None,
        run_tool: Callable[..., None] | None = None,
        probe_duration: Callable[[Path], float] | None = None,
    ) -> None:
        self.config = config
        self.control = control
        self.object_store = object_store
        self.download_source = download_source or _download_source
        self.run_tool = run_tool or _run_tool
        self.probe_duration = probe_duration or _probe_duration
        self._slots = threading.BoundedSemaphore(config.concurrency)

    def run_once(self) -> int:
        jobs = self.control.list_queue(page_size=self.config.concurrency)
        if not jobs:
            return 0
        with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            return sum(executor.map(self._claim_and_run, jobs[: self.config.concurrency]))

    def run_forever(self, *, max_idle_polls: int | None = None, sleeper: Callable[[float], None] = time.sleep) -> int:
        idle_polls = 0
        processed = 0
        while max_idle_polls is None or idle_polls < max_idle_polls:
            reap_abandoned_workspaces(
                self.config.workspace_root,
                orphan_grace_seconds=self.config.workspace_orphan_grace_seconds,
                absolute_ttl_seconds=self.config.workspace_absolute_ttl_seconds,
            )
            try:
                count = self.run_once()
            except Exception:
                _LOGGER.exception("%s queue poll failed", _LOG_MARKER)
                count = 0
            processed += count
            idle_polls = 0 if count else idle_polls + 1
            if max_idle_polls is not None and idle_polls >= max_idle_polls:
                break
            sleeper(self.config.poll_interval_seconds)
        return processed

    def _claim_and_run(self, job: ExportJob) -> int:
        with self._slots:
            try:
                claim = self.control.claim_job(job.export_job_id, lease_owner=self.config.lease_owner, lease_seconds=self.config.lease_seconds)
            except Exception:
                _LOGGER.info("%s claim skipped export_job_id=%s", _LOG_MARKER, job.export_job_id)
                return 0
            self._run_claim(claim)
            return 1

    def _run_claim(self, claim: ExportClaim) -> None:
        try:
            with attempt_workspace(
                self.config.workspace_root,
                claim.job.export_job_id,
                attempt_token=claim.attempt_token,
            ) as workspace:
                self._check_cancel(claim)
                self._progress(claim, "materializing_source", 5, "Resolving export source")
                source_path = self._materialize_source(claim, workspace)
                self._check_duration(source_path)
                self._check_workspace_size(workspace)
                self._check_cancel(claim)
                self._progress(claim, "converting", 40, "Creating export")
                output_path, content_type = self._convert(claim, source_path, workspace)
                self._check_file_limit(output_path, self.config.max_output_bytes, "export_output_limit_exceeded")
                self._check_workspace_size(workspace)
                self._check_cancel(claim)
                self._progress(claim, "uploading", 85, "Uploading export")
                digest = _sha256_file(output_path)
                filename = output_path.name
                staging_key = f"transient/staging/{claim.job.export_job_id}/{claim.attempt_token}/{filename}"
                output: dict[str, object] = {
                    "content_type": content_type,
                    "filename": filename,
                    "size_bytes": output_path.stat().st_size,
                    "sha256": digest,
                    "staging_key": staging_key,
                }
                if claim.job.output_profile in _AAC_AUDIO_PROFILES:
                    output["duration_seconds"] = _rounded_positive_duration(self.probe_duration(output_path))
                self.object_store.put_file(
                    object_key=staging_key,
                    source=output_path,
                    content_type=content_type,
                    metadata={"sha256": digest},
                    cancelled=self._lease_guard(claim, stage="uploading", percent=85),
                )
                self._check_cancel(claim)
                self.control.finalize(
                    claim,
                    outcome="succeeded",
                    output=output,
                )
        except ExportCancellationRequested:
            self.control.finalize(claim, outcome="canceled")
        except ExportWorkerError as exc:
            _LOGGER.warning("%s failed export_job_id=%s code=%s", _LOG_MARKER, claim.job.export_job_id, exc.code)
            self.control.finalize(claim, outcome="failed", diagnostic_code=exc.code, diagnostic_message=str(exc))
        except Exception:
            _LOGGER.exception("%s failed export_job_id=%s", _LOG_MARKER, claim.job.export_job_id)
            self.control.finalize(claim, outcome="failed", diagnostic_code="export_worker_failed", diagnostic_message="export processing failed")

    def _materialize_source(self, claim: ExportClaim, workspace: Path) -> Path:
        if claim.source.size_bytes is not None and claim.source.size_bytes > self.config.max_input_bytes:
            raise ExportWorkerError("export_input_limit_exceeded", "source exceeds configured input limit")
        if claim.source.source_type == "uploaded_object":
            destination = workspace / "source.bin"
            self.download_source(
                claim.source.url,
                destination,
                max_bytes=self.config.max_input_bytes,
                cancelled=self._lease_guard(claim, stage="materializing_source", percent=5),
            )
            self._check_file_limit(destination, self.config.max_input_bytes, "export_input_limit_exceeded")
            return destination
        template = workspace / "remote-source.%(ext)s"
        command = [
            "yt-dlp",
            "--ignore-config",
            "--no-playlist",
            "--no-progress",
            "--max-filesize",
            str(self.config.max_input_bytes),
            "--match-filter",
            f"!is_live & duration <= {self.config.max_duration_seconds}",
            "-f",
            _youtube_format_selector(claim.job),
            "-o",
            str(template),
            claim.source.url,
        ]
        self._tool(command, workspace, claim, stage="materializing_source", percent=5)
        files = tuple(path for path in workspace.glob("remote-source.*") if path.is_file())
        if len(files) != 1:
            raise ExportWorkerError("export_provider_resolution_failed", "provider did not produce one source file")
        self._check_file_limit(files[0], self.config.max_input_bytes, "export_input_limit_exceeded")
        return files[0]

    def _convert(self, claim: ExportClaim, source: Path, workspace: Path) -> tuple[Path, str]:
        variant = claim.job.variant
        if claim.job.operation in _AUDIO_OPERATIONS:
            bitrate = variant.get("audio_bitrate_kbps")
            output_profile = claim.job.output_profile or "audio_ogg_opus_v1"
            supported_bitrates = {64, 96, 128, 192, 256, 320} if output_profile in _AAC_AUDIO_PROFILES else {64, 96, 128, 192, 256}
            if not isinstance(bitrate, int) or isinstance(bitrate, bool) or bitrate not in supported_bitrates:
                raise ExportWorkerError("export_invalid_variant", "invalid audio bitrate")
            if output_profile in _AAC_AUDIO_PROFILES:
                output = workspace / f"export-{claim.job.export_job_id}.m4a"
                command = [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(source),
                    "-vn",
                    "-c:a",
                    "aac",
                    "-b:a",
                    f"{bitrate}k",
                    "-movflags",
                    "+faststart",
                    str(output),
                ]
                self._tool(command, workspace, claim, stage="converting", percent=40, output_path=output)
                return output, "audio/mp4"
            if output_profile != "audio_ogg_opus_v1":
                raise ExportWorkerError("export_invalid_variant", "invalid audio output profile")
            output = workspace / f"export-{claim.job.export_job_id}.ogg"
            command = [
                "ffmpeg",
                "-y",
                "-i",
                str(source),
                "-vn",
                "-c:a",
                "libopus",
                "-b:a",
                f"{bitrate}k",
                "-vbr",
                "on",
                "-application",
                "audio",
                str(output),
            ]
            self._tool(command, workspace, claim, stage="converting", percent=40, output_path=output)
            return output, "audio/ogg"
        if claim.job.output_profile not in {None, "video_mp4_v1"}:
            raise ExportWorkerError("export_invalid_variant", "invalid video output profile")
        quality = variant.get("video_quality")
        if quality not in {"360p", "480p", "720p", "1080p"}:
            raise ExportWorkerError("export_invalid_variant", "invalid video quality")
        output = workspace / f"export-{claim.job.export_job_id}.mp4"
        self._tool(
            ["ffmpeg", "-y", "-i", str(source), "-c:v", "copy", "-c:a", "aac", "-movflags", "+faststart", str(output)],
            workspace,
            claim,
            stage="converting",
            percent=40,
            output_path=output,
        )
        return output, "video/mp4"

    def _tool(
        self,
        command: list[str],
        workspace: Path,
        claim: ExportClaim,
        *,
        stage: str,
        percent: int,
        output_path: Path | None = None,
    ) -> None:
        def enforce_resource_limits() -> None:
            if output_path is not None:
                try:
                    output_size = output_path.stat().st_size
                except FileNotFoundError:
                    pass
                else:
                    if output_size > self.config.max_output_bytes:
                        raise ExportWorkerError("export_output_limit_exceeded", "media bytes exceed configured limit")
            self._check_workspace_size(workspace)

        self.run_tool(
            command,
            cwd=workspace,
            timeout_seconds=self.config.timeout_seconds,
            cancelled=self._lease_guard(claim, stage=stage, percent=percent),
            resource_guard=enforce_resource_limits,
        )

    def _lease_guard(self, claim: ExportClaim, *, stage: str, percent: int) -> Callable[[], bool]:
        next_cancel_check = 0.0
        heartbeat_interval = min(30.0, max(1.0, self.config.lease_seconds / 3))
        next_heartbeat = time.monotonic() + heartbeat_interval
        cancel_requested = False

        def check() -> bool:
            nonlocal cancel_requested, next_cancel_check, next_heartbeat
            current = time.monotonic()
            if current >= next_heartbeat:
                self.control.publish_progress(
                    claim,
                    {"stage": stage, "percent": percent, "message": "Export is still running"},
                )
                next_heartbeat = current + heartbeat_interval
            if current >= next_cancel_check:
                cancel_requested = self.control.cancel_requested_for(claim)
                next_cancel_check = current + 1.0
            return cancel_requested

        return check

    def _check_cancel(self, claim: ExportClaim) -> None:
        if self.control.cancel_requested_for(claim):
            raise ExportCancellationRequested()

    def _progress(self, claim: ExportClaim, stage: str, percent: int, message: str) -> None:
        self.control.publish_progress(claim, {"stage": stage, "percent": percent, "message": message})

    def _check_file_limit(self, path: Path, limit: int, code: str) -> None:
        if not path.is_file():
            raise ExportWorkerError("export_output_missing", "expected export output is missing")
        if path.stat().st_size > limit:
            raise ExportWorkerError(code, "media bytes exceed configured limit")

    def _check_workspace_size(self, workspace: Path) -> None:
        total = 0
        for path in workspace.rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                continue
        if total > self.config.workspace_max_bytes:
            raise ExportWorkerError("export_workspace_limit_exceeded", "workspace bytes exceed configured limit")

    def _check_duration(self, source: Path) -> None:
        if self.probe_duration(source) > self.config.max_duration_seconds:
            raise ExportWorkerError("export_duration_limit_exceeded", "source duration exceeds configured limit")


def _download_source(
    url: str,
    destination: Path,
    *,
    max_bytes: int,
    cancelled: Callable[[], bool],
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with request.urlopen(request.Request(url, headers={"Accept": "application/octet-stream"}), timeout=60) as response, destination.open("wb") as target:
            raw_length = response.headers.get("Content-Length")
            if raw_length is not None and int(raw_length) > max_bytes:
                raise ExportWorkerError("export_input_limit_exceeded", "source exceeds configured input limit")
            written = 0
            while chunk := response.read(1024 * 1024):
                if cancelled():
                    raise ExportCancellationRequested()
                written += len(chunk)
                if written > max_bytes:
                    raise ExportWorkerError("export_input_limit_exceeded", "source exceeds configured input limit")
                target.write(chunk)
    except ExportWorkerError:
        destination.unlink(missing_ok=True)
        raise
    except (error.URLError, TimeoutError, ValueError) as exc:
        destination.unlink(missing_ok=True)
        raise ExportWorkerError("export_source_download_failed", "could not materialize uploaded source") from exc


def _run_tool(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: float,
    cancelled: Callable[[], bool],
    resource_guard: Callable[[], None],
) -> None:
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except FileNotFoundError as exc:
        raise ExportWorkerError("export_tool_unavailable", f"required tool {command[0]} is unavailable") from exc
    deadline = time.monotonic() + timeout_seconds
    try:
        while process.poll() is None:
            if cancelled():
                raise ExportCancellationRequested()
            if time.monotonic() >= deadline:
                raise ExportWorkerError("export_timeout", "media conversion timed out")
            resource_guard()
            time.sleep(0.2)
    except BaseException:
        if process.poll() is None:
            _terminate_process_group(process)
        raise
    if process.returncode != 0:
        raise ExportWorkerError("export_tool_failed", f"{command[0]} exited unsuccessfully")


def _probe_duration(source: Path) -> float:
    try:
        completed = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nokey=1:noprint_wrappers=1", str(source)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise ExportWorkerError("export_duration_probe_failed", "could not determine source duration") from exc
    if completed.returncode != 0:
        raise ExportWorkerError("export_duration_probe_failed", "could not determine source duration")
    try:
        duration = float(completed.stdout.strip())
    except ValueError as exc:
        raise ExportWorkerError("export_duration_probe_failed", "source duration was invalid") from exc
    if duration < 0:
        raise ExportWorkerError("export_duration_probe_failed", "source duration was invalid")
    return duration


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _youtube_format_selector(job: ExportJob) -> str:
    if job.operation == "youtube_audio":
        return "bestaudio/best"
    if job.operation == "youtube_video":
        quality = job.variant.get("video_quality")
        if not isinstance(quality, str) or not quality.endswith("p") or not quality[:-1].isdigit():
            raise ExportWorkerError("export_invalid_variant", "invalid video quality")
        return f"bestvideo[height<={quality[:-1]}]+bestaudio/best[height<={quality[:-1]}]"
    raise ExportWorkerError("export_invalid_operation", "remote source does not support this export operation")


def _signature_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    date_key = hmac.new(("AWS4" + secret_key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    region_key = hmac.new(date_key, region.encode("utf-8"), hashlib.sha256).digest()
    service_key = hmac.new(region_key, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


def _mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{context} must be an object")
    return value


def _string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context} must be a non-empty string")
    return value


def _optional_string(value: object, context: str) -> str | None:
    if value is None:
        return None
    return _string(value, context)


def _rounded_positive_duration(value: float) -> int:
    if not math.isfinite(value) or value < 0:
        raise ExportWorkerError("export_duration_probe_failed", "output duration was invalid")
    return max(1, int(value + 0.5))


def _number(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError("environment value must be numeric") from exc


def _integer(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError("environment value must be an integer") from exc
