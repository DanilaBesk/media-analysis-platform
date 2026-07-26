from __future__ import annotations

from pathlib import Path

import pytest

from transcriber_worker_media_export import MediaExportWorkerConfig
from transcriber_worker_media_export_main import build_worker


def test_build_worker_uses_export_specific_config_and_dependencies(tmp_path: Path) -> None:
    worker = build_worker(
        {
            "WORKER_WORKSPACE_ROOT": str(tmp_path / "scratch"),
            "MEDIA_EXPORT_LEASE_OWNER": "worker-1",
            "PLATFORM_INTERNAL_TOKEN": "internal-token",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "access",
            "MINIO_SECRET_KEY": "secret",
            "MINIO_BUCKET_ARTIFACTS": "artifacts",
        }
    )

    assert worker.config.lease_owner == "worker-1"
    assert worker.config.workspace_root == tmp_path / "scratch"


def test_configuration_rejects_missing_internal_token(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="internal_token"):
        MediaExportWorkerConfig(workspace_root=tmp_path, lease_owner="worker")
