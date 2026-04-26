# FILE: workers/agent-runner/src/transcriber_worker_agent_runner_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the compose-ready executable launcher for the generic agent-runner worker.
# SCOPE: Runtime env loading, shared worker loop wiring, object-store adapter wiring, and agent-runner construction.
# DEPENDS: M-WORKER-AGENT-RUNNER, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
# ROLE: SCRIPT
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the compose-ready agent-runner launcher wired through worker-common runtime.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   launch-agent-runner-worker - Build the API client, object-store adapter, and shared worker loop.
#   build-agent-runner - Adapt claimed job IDs into runAgentHarness calls without duplicating queue logic.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
from typing import Mapping

from transcriber_worker_agent_runner import DefaultAgentHarnessRegistry, LocalAgentHarnessLeaseClient, runAgentHarness
from transcriber_workers_common.api import JobApiClient
from transcriber_workers_common.object_store import WorkerObjectStore, WorkerObjectStoreConfig
from transcriber_workers_common.runtime import WorkerRuntimeConfig, run_worker_loop


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_LAUNCH_AGENT_RUNNER = "[WorkerAgentRunner][main][BLOCK_LAUNCH_AGENT_RUNNER_WORKER]"

__all__ = ["build_runner", "main"]


def build_runner(
    config: WorkerRuntimeConfig,
    *,
    api_client: JobApiClient,
    object_store: WorkerObjectStore,
    env: Mapping[str, str] | None = None,
):
    harness_registry = DefaultAgentHarnessRegistry.from_env(env)
    lease_client = LocalAgentHarnessLeaseClient.from_env(env)

    def _runner(job_id: str) -> object:
        return runAgentHarness(
            job_id,
            workspace_root=config.workspace_root,
            api_client=api_client,
            artifact_store=object_store,
            harness_registry=harness_registry,
            lease_client=lease_client,
        )

    return _runner


def main(env: Mapping[str, str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    values = os.environ if env is None else env
    config = WorkerRuntimeConfig.from_env(
        worker_kind="agent_runner",
        task_type="agent_run.run",
        job_type="agent_run",
        env=values,
    )
    api_client = JobApiClient(config.api_config)
    object_store = WorkerObjectStore(WorkerObjectStoreConfig.from_env(values))
    _LOGGER.info("%s workspace_root=%s", _LOG_MARKER_LAUNCH_AGENT_RUNNER, config.workspace_root)
    run_worker_loop(
        config,
        build_runner(config, api_client=api_client, object_store=object_store, env=values),
        api_client=api_client,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
