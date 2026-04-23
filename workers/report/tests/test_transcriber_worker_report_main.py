# FILE: workers/report/tests/test_transcriber_worker_report_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the report worker launcher delegates execution through the shared runtime boundaries.
# SCOPE: build_runner wiring only; worker loop behavior remains covered by worker-common tests.
# DEPENDS: M-WORKER-REPORT, M-WORKER-COMMON
# LINKS: M-WORKER-REPORT, V-M-WORKER-REPORT
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for report launcher runner wiring.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   test_build_runner_delegates_to_run_report - Verifies the launcher preserves report worker dependency wiring.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

from transcriber_workers_common.api import InternalApiConfig
from transcriber_workers_common.runtime import WorkerRuntimeConfig

import transcriber_worker_report_main as launcher


def test_build_runner_delegates_to_run_report(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="report",
        task_type="report.run",
        job_type="report",
        workspace_root=tmp_path / "runtime",
    )
    api_client = object()
    object_store = object()
    calls: list[dict[str, object]] = []

    def fake_run_report(job_id: str, **kwargs):
        calls.append({"job_id": job_id, **kwargs})
        return "ok"

    monkeypatch.setattr(launcher, "runReport", fake_run_report)

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
