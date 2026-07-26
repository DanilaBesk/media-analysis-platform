# FILE: workers/media-export/src/transcriber_worker_media_export_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Compose the production media-export worker from environment-backed dependencies.
# SCOPE: Runtime configuration and process entrypoint only.
# DEPENDS: M-WORKER-MEDIA-EXPORT
# LINKS: M-MEDIA-EXPORT, V-MEDIA-EXPORT
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT

from __future__ import annotations

import logging
import os
from collections.abc import Mapping

from transcriber_worker_media_export import (
    HttpExportControlClient,
    MediaExportWorker,
    MediaExportWorkerConfig,
    MinioExportObjectStore,
)


def build_worker(env: Mapping[str, str] | None = None) -> MediaExportWorker:
    config = MediaExportWorkerConfig.from_env(env)
    return MediaExportWorker(
        config,
        control=HttpExportControlClient(config),
        object_store=MinioExportObjectStore.from_env(env),
    )


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    build_worker().run_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    raise SystemExit(main())
