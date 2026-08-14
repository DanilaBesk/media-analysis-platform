# FILE: workers/metadata-enrichment/src/transcriber_worker_metadata_enrichment_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Compose and launch the production metadata enrichment worker.
# SCOPE: Environment-backed runtime wiring and cooperative process shutdown.
# DEPENDS: M-METADATA-ENRICHMENT
# LINKS: V-M-METADATA-ENRICHMENT
# ROLE: SCRIPT
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_MODULE_MAP
#   build_worker - Compose the metadata-enrichment worker from environment-backed dependencies.
#   main - Launch the metadata-enrichment worker process.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
import signal
import threading
from collections.abc import Mapping

from transcriber_worker_metadata_enrichment import (
    HttpMetadataEnrichmentControlClient,
    MetadataEnrichmentWorker,
    MetadataEnrichmentWorkerConfig,
    YtDlpMetadataResolver,
)


def build_worker(
    env: Mapping[str, str] | None = None, *, stop_event: threading.Event | None = None
) -> MetadataEnrichmentWorker:
    config = MetadataEnrichmentWorkerConfig.from_env(env)
    return MetadataEnrichmentWorker(
        config,
        control=HttpMetadataEnrichmentControlClient(config),
        resolver=YtDlpMetadataResolver(config),
        stop_event=stop_event,
    )


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop_event = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    build_worker(stop_event=stop_event).run_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    raise SystemExit(main())
