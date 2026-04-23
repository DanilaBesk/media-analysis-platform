# FILE: workers/common/src/transcriber_workers_common/object_store.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the shared MinIO object-store adapter used by compose-ready worker launchers.
# SCOPE: Env-backed MinIO config, path-style S3 object GET/PUT, AWS SigV4 signing, and deterministic transport injection for tests.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added a dependency-free MinIO object store for worker source downloads and artifact uploads.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   ObjectStoreRequestFailed - Reports MinIO/S3 transport failures through one worker-common error type.
#   RawObjectTransport - Defines the injectable byte-level object-store transport used by tests.
#   WorkerObjectStoreConfig - Carries MinIO endpoint, credentials, buckets, and region.
#   WorkerObjectStore - Implements fetch_file and put_bytes for worker source/artifact contracts.
# END_MODULE_MAP

from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Callable, Mapping, Protocol
from urllib import error, parse, request


__all__ = [
    "ObjectStoreRequestFailed",
    "RawObjectTransport",
    "WorkerObjectStore",
    "WorkerObjectStoreConfig",
]


class ObjectStoreRequestFailed(RuntimeError):
    pass


class RawObjectTransport(Protocol):
    def request(self, *, method: str, url: str, headers: Mapping[str, str], body: bytes | None = None) -> bytes: ...


class _UrllibObjectTransport:
    def request(self, *, method: str, url: str, headers: Mapping[str, str], body: bytes | None = None) -> bytes:
        http_request = request.Request(url=url, data=body, headers=dict(headers), method=method)
        try:
            with request.urlopen(http_request, timeout=60) as response:
                return response.read()
        except error.HTTPError as exc:  # pragma: no cover - integration failure path
            raise ObjectStoreRequestFailed(f"object-store request failed with HTTP {exc.code}: {exc.reason}") from exc
        except error.URLError as exc:  # pragma: no cover - integration failure path
            raise ObjectStoreRequestFailed(f"object-store request failed: {exc.reason}") from exc


# START_CONTRACT: WorkerObjectStoreConfig
# PURPOSE: Carry MinIO/S3 connection settings for worker source and artifact IO.
# INPUTS: { endpoint/access_key/secret_key/source_bucket/artifact_bucket/region - MinIO runtime settings }
# OUTPUTS: { WorkerObjectStoreConfig - Immutable object-store config }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-INFRA-COMPOSE
# END_CONTRACT: WorkerObjectStoreConfig
@dataclass(frozen=True, slots=True)
class WorkerObjectStoreConfig:
    endpoint: str
    access_key: str
    secret_key: str
    source_bucket: str
    artifact_bucket: str
    region: str = "us-east-1"

    def __post_init__(self) -> None:
        _require(bool(self.endpoint.strip()), "MINIO_ENDPOINT is required")
        _require(bool(self.access_key.strip()), "MINIO_ACCESS_KEY is required")
        _require(bool(self.secret_key.strip()), "MINIO_SECRET_KEY is required")
        _require(bool(self.source_bucket.strip()), "MINIO_BUCKET_SOURCES is required")
        _require(bool(self.artifact_bucket.strip()), "MINIO_BUCKET_ARTIFACTS is required")
        parsed = parse.urlparse(self.endpoint)
        _require(parsed.scheme in {"http", "https"} and bool(parsed.netloc), "MINIO_ENDPOINT must be an http(s) URL")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "WorkerObjectStoreConfig":
        values = os.environ if env is None else env
        return cls(
            endpoint=values.get("MINIO_ENDPOINT", "").strip(),
            access_key=values.get("MINIO_ACCESS_KEY", "").strip(),
            secret_key=values.get("MINIO_SECRET_KEY", "").strip(),
            source_bucket=values.get("MINIO_BUCKET_SOURCES", "sources").strip(),
            artifact_bucket=values.get("MINIO_BUCKET_ARTIFACTS", "artifacts").strip(),
            region=values.get("MINIO_REGION", "us-east-1").strip() or "us-east-1",
        )


# START_CONTRACT: WorkerObjectStore
# PURPOSE: Fetch claimed source objects and upload worker artifacts through the shared MinIO adapter.
# INPUTS: { config: WorkerObjectStoreConfig - MinIO settings, transport: RawObjectTransport | None - Test/integration transport override }
# OUTPUTS: { WorkerObjectStore - SourceObjectStore and ArtifactObjectStore compatible adapter }
# SIDE_EFFECTS: MinIO/S3 HTTP IO and local file writes for fetched sources
# LINKS: M-WORKER-COMMON, DF-001, DF-006
# END_CONTRACT: WorkerObjectStore
class WorkerObjectStore:
    def __init__(
        self,
        config: WorkerObjectStoreConfig,
        *,
        transport: RawObjectTransport | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.transport = transport or _UrllibObjectTransport()
        self.now = now or (lambda: datetime.now(timezone.utc))

    # START_CONTRACT: fetch_file
    # PURPOSE: Download one claimed source/artifact object into a local worker workspace path.
    # INPUTS: { object_key: str - Source object key, destination: Path - Local destination }
    # OUTPUTS: { None - Destination file contains object bytes }
    # SIDE_EFFECTS: object-store GET and local filesystem write
    # LINKS: M-WORKER-COMMON, M-WORKER-TRANSCRIPTION, M-WORKER-REPORT, M-WORKER-DEEP-RESEARCH
    # END_CONTRACT: fetch_file
    def fetch_file(self, *, object_key: str, destination: Path) -> None:
        body = self._request_object(method="GET", bucket=self.config.source_bucket, object_key=object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(body)

    # START_CONTRACT: put_bytes
    # PURPOSE: Upload one worker artifact byte payload to the artifact bucket.
    # INPUTS: { object_key: str - Artifact object key, content: bytes - Artifact bytes, mime_type: str - MIME type }
    # OUTPUTS: { None - Object-store side effect is authoritative }
    # SIDE_EFFECTS: object-store PUT
    # LINKS: M-WORKER-COMMON, M-WORKER-TRANSCRIPTION, M-WORKER-REPORT, M-WORKER-DEEP-RESEARCH
    # END_CONTRACT: put_bytes
    def put_bytes(self, *, object_key: str, content: bytes, mime_type: str) -> None:
        self._request_object(
            method="PUT",
            bucket=self.config.artifact_bucket,
            object_key=object_key,
            body=content,
            content_type=mime_type,
        )

    def _request_object(
        self,
        *,
        method: str,
        bucket: str,
        object_key: str,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> bytes:
        _require(bool(object_key.strip()), "object_key must not be empty")
        request_body = body or b""
        url, canonical_uri, host = self._object_url(bucket=bucket, object_key=object_key)
        headers = self._signed_headers(
            method=method,
            canonical_uri=canonical_uri,
            host=host,
            body=request_body,
            content_type=content_type,
        )
        return self.transport.request(method=method, url=url, headers=headers, body=body)

    def _object_url(self, *, bucket: str, object_key: str) -> tuple[str, str, str]:
        parsed = parse.urlparse(self.config.endpoint.rstrip("/"))
        key_path = str(PurePosixPath(bucket, object_key))
        canonical_uri = "/" + "/".join(parse.quote(part, safe="") for part in key_path.split("/"))
        url = parse.urlunparse((parsed.scheme, parsed.netloc, canonical_uri, "", "", ""))
        return url, canonical_uri, parsed.netloc

    def _signed_headers(
        self,
        *,
        method: str,
        canonical_uri: str,
        host: str,
        body: bytes,
        content_type: str | None,
    ) -> dict[str, str]:
        now = self.now().astimezone(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest()
        headers = {
            "Host": host,
            "X-Amz-Content-Sha256": payload_hash,
            "X-Amz-Date": amz_date,
        }
        if content_type:
            headers["Content-Type"] = content_type

        canonical_headers = (
            f"host:{host}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{self.config.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signing_key = _signature_key(self.config.secret_key, date_stamp, self.config.region, "s3")
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        headers["Authorization"] = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.config.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        return headers


def _signature_key(key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
    date_key = hmac.new(("AWS4" + key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    date_region_key = hmac.new(date_key, region_name.encode("utf-8"), hashlib.sha256).digest()
    date_region_service_key = hmac.new(date_region_key, service_name.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(date_region_service_key, b"aws4_request", hashlib.sha256).digest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)
