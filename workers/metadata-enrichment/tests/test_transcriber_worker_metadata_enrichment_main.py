from __future__ import annotations

from transcriber_worker_metadata_enrichment import (
    HttpMetadataEnrichmentControlClient,
    YtDlpMetadataResolver,
)
from transcriber_worker_metadata_enrichment_main import build_worker


def test_build_worker_wires_production_control_client_and_resolver() -> None:
    worker = build_worker(
        {
            "METADATA_ENRICHMENT_LEASE_OWNER": "metadata-worker-1",
            "PLATFORM_INTERNAL_TOKEN": "internal-token",
            "METADATA_ENRICHMENT_HEARTBEAT_INTERVAL_SECONDS": "20",
        }
    )

    assert worker.config.lease_owner == "metadata-worker-1"
    assert isinstance(worker.control, HttpMetadataEnrichmentControlClient)
    assert isinstance(worker.resolver, YtDlpMetadataResolver)
