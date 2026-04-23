# FILE: workers/deep-research/src/transcriber_worker_deep_research_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the compose-ready executable launcher for the deep-research worker.
# SCOPE: Runtime env loading, shared worker loop wiring, MinIO object-store adapter wiring, and deep-research runner construction.
# DEPENDS: M-WORKER-DEEP-RESEARCH, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-DEEP-RESEARCH, V-M-WORKER-DEEP-RESEARCH
# ROLE: SCRIPT
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the compose-ready deep-research worker launcher wired through worker-common runtime.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   launch-deep-research-worker - Build the API client, object-store adapter, and shared worker loop.
#   build-deep-research-runner - Adapt claimed job IDs into runDeepResearch calls without duplicating queue logic.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Mapping


def _ensure_worker_dependency_paths() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    for path in (repo_root / "workers" / "common" / "src", repo_root / "src"):
        resolved = str(path)
        if path.exists() and resolved not in sys.path:
            sys.path.insert(0, resolved)


_ensure_worker_dependency_paths()

from transcriber_worker_deep_research import runDeepResearch
from transcriber_workers_common.api import JobApiClient
from transcriber_workers_common.object_store import WorkerObjectStore, WorkerObjectStoreConfig
from transcriber_workers_common.runtime import WorkerRuntimeConfig, run_worker_loop


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_LAUNCH_DEEP_RESEARCH = "[WorkerDeepResearch][main][BLOCK_LAUNCH_DEEP_RESEARCH_WORKER]"

__all__ = ["build_runner", "main"]


def build_runner(
    config: WorkerRuntimeConfig,
    *,
    api_client: JobApiClient,
    object_store: WorkerObjectStore,
):
    def _runner(job_id: str) -> object:
        return runDeepResearch(
            job_id,
            workspace_root=config.workspace_root,
            api_client=api_client,
            input_store=object_store,
            artifact_store=object_store,
        )

    return _runner


def main(env: Mapping[str, str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    values = os.environ if env is None else env
    config = WorkerRuntimeConfig.from_env(
        worker_kind="deep_research",
        task_type="deep_research.run",
        job_type="deep_research",
        env=values,
    )
    api_client = JobApiClient(config.api_config)
    object_store = WorkerObjectStore(WorkerObjectStoreConfig.from_env(values))
    _LOGGER.info("%s workspace_root=%s", _LOG_MARKER_LAUNCH_DEEP_RESEARCH, config.workspace_root)
    run_worker_loop(config, build_runner(config, api_client=api_client, object_store=object_store), api_client=api_client)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
