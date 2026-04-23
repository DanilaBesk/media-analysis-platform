# FILE: workers/common/tests/test_object_store.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the worker-common MinIO object-store adapter preserves path-style object IO and SigV4 request shape without requiring live MinIO.
# SCOPE: Env config, source fetch writes, artifact upload request shape, and deterministic signing headers.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for the worker-common MinIO object-store adapter.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   RecordingObjectTransport - Captures object-store requests without requiring live MinIO.
#   test_object_store_config_from_env_preserves_minio_contract - Verifies env-backed MinIO config parsing.
#   test_fetch_file_uses_source_bucket_and_writes_destination - Verifies source downloads and signed GET request shape.
#   test_fetch_file_uses_artifact_bucket_for_claimed_artifact_inputs - Verifies child-worker artifact inputs are read from the artifact bucket.
#   test_put_bytes_uses_artifact_bucket_and_content_headers - Verifies artifact uploads and signed PUT request shape.
# END_MODULE_MAP

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from transcriber_workers_common.object_store import WorkerObjectStore, WorkerObjectStoreConfig


class RecordingObjectTransport:
    def __init__(self, response: bytes = b"payload") -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def request(self, *, method: str, url: str, headers: Mapping[str, str], body: bytes | None = None) -> bytes:
        self.calls.append({"method": method, "url": url, "headers": dict(headers), "body": body})
        return self.response


def test_object_store_config_from_env_preserves_minio_contract() -> None:
    config = WorkerObjectStoreConfig.from_env(
        {
            "MINIO_ENDPOINT": " http://minio:9000 ",
            "MINIO_ACCESS_KEY": "access",
            "MINIO_SECRET_KEY": "secret",
            "MINIO_BUCKET_SOURCES": "source-bucket",
            "MINIO_BUCKET_ARTIFACTS": "artifact-bucket",
            "MINIO_REGION": "test-region",
        }
    )

    assert config.endpoint == "http://minio:9000"
    assert config.access_key == "access"
    assert config.source_bucket == "source-bucket"
    assert config.artifact_bucket == "artifact-bucket"
    assert config.region == "test-region"


def test_fetch_file_uses_source_bucket_and_writes_destination(tmp_path: Path) -> None:
    transport = RecordingObjectTransport(response=b"source-bytes")
    store = WorkerObjectStore(_config(), transport=transport, now=_fixed_now)

    destination = tmp_path / "inputs" / "source.mp3"
    store.fetch_file(object_key="sources/job/source.mp3", destination=destination)

    assert destination.read_bytes() == b"source-bytes"
    assert transport.calls[0]["method"] == "GET"
    assert transport.calls[0]["url"] == "http://minio:9000/sources/sources/job/source.mp3"
    headers = transport.calls[0]["headers"]
    assert headers["X-Amz-Date"] == "20260423T120000Z"
    assert headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=access/20260423/us-east-1/s3/aws4_request")


def test_fetch_file_uses_artifact_bucket_for_claimed_artifact_inputs(tmp_path: Path) -> None:
    transport = RecordingObjectTransport(response=b"artifact-bytes")
    store = WorkerObjectStore(_config(), transport=transport, now=_fixed_now)

    destination = tmp_path / "inputs" / "transcript.md"
    store.fetch_file(object_key="artifacts/job/transcript/segmented/transcript.md", destination=destination)

    assert destination.read_bytes() == b"artifact-bytes"
    assert transport.calls[0]["method"] == "GET"
    assert transport.calls[0]["url"] == "http://minio:9000/artifacts/artifacts/job/transcript/segmented/transcript.md"


def test_put_bytes_uses_artifact_bucket_and_content_headers() -> None:
    transport = RecordingObjectTransport()
    store = WorkerObjectStore(_config(), transport=transport, now=_fixed_now)

    store.put_bytes(
        object_key="artifacts/job/transcript/plain/transcript.txt",
        content=b"transcript",
        mime_type="text/plain",
    )

    call = transport.calls[0]
    assert call["method"] == "PUT"
    assert call["url"] == "http://minio:9000/artifacts/artifacts/job/transcript/plain/transcript.txt"
    assert call["body"] == b"transcript"
    assert call["headers"]["Content-Type"] == "text/plain"
    assert call["headers"]["X-Amz-Content-Sha256"] == "54e6289e14c7b0e7ad9acc2dfc4c1e3d027d0eef7f5c4c3fe7c292761d0e06a6"


def _config() -> WorkerObjectStoreConfig:
    return WorkerObjectStoreConfig(
        endpoint="http://minio:9000",
        access_key="access",
        secret_key="secret",
        source_bucket="sources",
        artifact_bucket="artifacts",
    )


def _fixed_now() -> datetime:
    return datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
