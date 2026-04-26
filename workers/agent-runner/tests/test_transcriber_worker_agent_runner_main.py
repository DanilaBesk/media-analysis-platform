# FILE: workers/agent-runner/tests/test_transcriber_worker_agent_runner_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the agent-runner launcher delegates execution through shared worker-common runtime boundaries.
# SCOPE: build_runner wiring and main WorkerRuntimeConfig identity only.
# DEPENDS: M-WORKER-AGENT-RUNNER, M-WORKER-COMMON
# LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for agent-runner launcher wiring.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   test_build_runner_delegates_to_run_agent_harness - Verifies dependency injection into runAgentHarness.
#   test_main_wires_agent_runner_runtime_identity - Verifies the compose launcher uses agent_run identity.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

from transcriber_workers_common.api import InternalApiConfig
from transcriber_workers_common.runtime import WorkerRuntimeConfig

import transcriber_worker_agent_runner_main as launcher


def test_build_runner_delegates_to_run_agent_harness(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="agent_runner",
        task_type="agent_run.run",
        job_type="agent_run",
        workspace_root=tmp_path / "runtime",
    )
    api_client = object()
    object_store = object()
    calls: list[dict[str, object]] = []

    def fake_run_agent_harness(job_id: str, **kwargs):
        calls.append({"job_id": job_id, **kwargs})
        return "ok"

    monkeypatch.setattr(launcher, "runAgentHarness", fake_run_agent_harness)

    result = launcher.build_runner(config, api_client=api_client, object_store=object_store)("job-1")

    assert result == "ok"
    assert calls[0]["job_id"] == "job-1"
    assert calls[0]["workspace_root"] == tmp_path / "runtime"
    assert calls[0]["api_client"] is api_client
    assert calls[0]["artifact_store"] is object_store
    assert calls[0]["harness_registry"].__class__.__name__ == "DefaultAgentHarnessRegistry"
    assert calls[0]["lease_client"].__class__.__name__ == "LocalAgentHarnessLeaseClient"


def test_main_wires_agent_runner_runtime_identity(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakeObjectStore:
        def __init__(self, config):
            captured["object_store_config"] = config

    monkeypatch.setattr(launcher, "WorkerObjectStore", FakeObjectStore)
    monkeypatch.setattr(launcher.WorkerObjectStoreConfig, "from_env", staticmethod(lambda env: {"env": dict(env)}))

    def fake_loop(config, run_job, *, api_client=None, sleeper=None):
        captured["config"] = config
        captured["api_client"] = api_client
        captured["runner_result"] = run_job("job-main")

    monkeypatch.setattr(launcher, "run_worker_loop", fake_loop)
    monkeypatch.setattr(launcher, "JobApiClient", lambda api_config: {"api_config": api_config})
    monkeypatch.setattr(launcher, "runAgentHarness", lambda job_id, **kwargs: {"job_id": job_id, **kwargs})

    exit_code = launcher.main(
        {
            "API_BASE_URL": "http://api",
            "WORKER_WORKSPACE_ROOT": str(tmp_path / "workspace"),
            "WORKER_MAX_PROCESSED_JOBS": "1",
        }
    )

    assert exit_code == 0
    config = captured["config"]
    assert isinstance(config, WorkerRuntimeConfig)
    assert config.worker_kind == "agent_runner"
    assert config.task_type == "agent_run.run"
    assert config.job_type == "agent_run"
    assert captured["runner_result"]["job_id"] == "job-main"
    assert captured["runner_result"]["harness_registry"].__class__.__name__ == "DefaultAgentHarnessRegistry"
    assert captured["runner_result"]["lease_client"].__class__.__name__ == "LocalAgentHarnessLeaseClient"


def test_launcher_has_no_hidden_worker_dependency_path_bootstrap() -> None:
    assert not hasattr(launcher, "_ensure_worker_dependency_paths")
