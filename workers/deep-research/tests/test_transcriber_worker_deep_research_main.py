# FILE: workers/deep-research/tests/test_transcriber_worker_deep_research_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the deep-research worker launcher delegates execution through the shared runtime boundaries.
# SCOPE: build_runner wiring only; worker loop behavior remains covered by worker-common tests.
# DEPENDS: M-WORKER-DEEP-RESEARCH, M-WORKER-COMMON
# LINKS: M-WORKER-DEEP-RESEARCH, V-M-WORKER-DEEP-RESEARCH
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for deep-research launcher runner wiring.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   test_build_runner_delegates_to_run_deep_research - Verifies the launcher preserves deep-research worker dependency wiring.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

from transcriber_workers_common.api import InternalApiConfig
from transcriber_workers_common.runtime import WorkerRuntimeConfig

import transcriber_worker_deep_research_main as launcher


def test_build_runner_delegates_to_run_deep_research(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="deep_research",
        task_type="deep_research.run",
        job_type="deep_research",
        workspace_root=tmp_path / "runtime",
    )
    api_client = object()
    object_store = object()
    calls: list[dict[str, object]] = []

    def fake_run_deep_research(job_id: str, **kwargs):
        calls.append({"job_id": job_id, **kwargs})
        return "ok"

    monkeypatch.setattr(launcher, "runDeepResearch", fake_run_deep_research)

    result = launcher.build_runner(config, api_client=api_client, object_store=object_store)("job-1")

    assert result == "ok"
    assert calls == [
        {
            "job_id": "job-1",
            "workspace_root": tmp_path / "runtime",
            "api_client": api_client,
            "input_store": object_store,
            "artifact_store": object_store,
        }
    ]
