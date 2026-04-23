# FILE: workers/common/tests/test_runtime.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the shared worker runtime scaffold polls API-owned queued jobs and runs worker-specific job functions without forking queue behavior.
# SCOPE: Env config parsing, explicit one-shot jobs, queued polling, idle exit, and failure accounting.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for the shared worker loop scaffold.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   FakeApiClient - Supplies deterministic queued-job snapshots to the worker loop.
#   test_runtime_config_from_env_normalizes_worker_settings - Verifies launcher env parsing.
#   test_run_worker_loop_executes_explicit_job_without_polling - Verifies WORKER_JOB_ID one-shot mode.
#   test_run_worker_loop_polls_queued_job_through_api_client - Verifies queued polling and runner dispatch.
#   test_run_worker_loop_returns_after_idle_limit - Verifies deterministic idle exit for tests/smoke.
#   test_run_worker_loop_accounts_runner_failures - Verifies failures are logged and counted.
# END_MODULE_MAP

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from transcriber_workers_common.api import InternalApiConfig, JobListItem
from transcriber_workers_common.runtime import WorkerRuntimeConfig, run_worker_loop


class FakeApiClient:
    def __init__(self, batches: list[tuple[JobListItem, ...]]) -> None:
        self.batches = list(batches)
        self.calls: list[dict[str, object]] = []

    def list_jobs(
        self,
        *,
        status: str | None = None,
        job_type: str | None = None,
        page_size: int = 20,
    ) -> tuple[JobListItem, ...]:
        self.calls.append({"status": status, "job_type": job_type, "page_size": page_size})
        if not self.batches:
            return ()
        return self.batches.pop(0)


def test_runtime_config_from_env_normalizes_worker_settings(tmp_path: Path) -> None:
    config = WorkerRuntimeConfig.from_env(
        worker_kind="transcription",
        task_type="transcription.run",
        job_type="transcription",
        env={
            "API_BASE_URL": " http://api.internal ",
            "INTERNAL_API_TIMEOUT_SECONDS": "7.5",
            "WORKER_WORKSPACE_ROOT": str(tmp_path / "workspace"),
            "WORKER_POLL_INTERVAL_SECONDS": "0.25",
            "WORKER_JOB_ID": "job-1",
            "WORKER_MAX_IDLE_POLLS": "2",
            "WORKER_MAX_PROCESSED_JOBS": "3",
        },
    )

    assert config.api_config.base_url == "http://api.internal"
    assert config.api_config.timeout_seconds == 7.5
    assert config.workspace_root == tmp_path / "workspace"
    assert config.poll_interval_seconds == 0.25
    assert config.job_id == "job-1"
    assert config.max_idle_polls == 2
    assert config.max_processed_jobs == 3


def test_run_worker_loop_executes_explicit_job_without_polling(tmp_path: Path) -> None:
    config = _config(tmp_path, job_id="job-explicit")
    api_client = FakeApiClient([])
    processed: list[str] = []

    result = run_worker_loop(config, processed.append, api_client=api_client, sleeper=lambda _: None)

    assert processed == ["job-explicit"]
    assert api_client.calls == []
    assert result.processed_jobs == 1
    assert result.failed_jobs == 0
    assert result.idle_polls == 0


def test_run_worker_loop_accounts_explicit_job_failure(tmp_path: Path) -> None:
    config = _config(tmp_path, job_id="job-explicit")

    def fail_job(job_id: str) -> None:
        raise RuntimeError(job_id)

    result = run_worker_loop(config, fail_job, api_client=FakeApiClient([]), sleeper=lambda _: None)

    assert result.processed_jobs == 0
    assert result.failed_jobs == 1
    assert result.idle_polls == 0


def test_run_worker_loop_polls_queued_job_through_api_client(tmp_path: Path) -> None:
    config = _config(tmp_path, max_processed_jobs=1)
    api_client = FakeApiClient(
        [
            (
                JobListItem(
                    job_id="job-queued",
                    job_type="transcription",
                    status="queued",
                    version=1,
                ),
            )
        ]
    )
    processed: list[str] = []

    result = run_worker_loop(config, processed.append, api_client=api_client, sleeper=lambda _: None)

    assert processed == ["job-queued"]
    assert api_client.calls == [{"status": "queued", "job_type": "transcription", "page_size": 1}]
    assert result.processed_jobs == 1
    assert result.failed_jobs == 0


def test_run_worker_loop_counts_queue_poll_failures(tmp_path: Path) -> None:
    class FailingApiClient:
        def list_jobs(self, *, status: str | None = None, job_type: str | None = None, page_size: int = 20):
            raise RuntimeError(f"{status}:{job_type}:{page_size}")

    config = _config(tmp_path, max_idle_polls=1)

    result = run_worker_loop(config, lambda job_id: job_id, api_client=FailingApiClient(), sleeper=lambda _: None)

    assert result.processed_jobs == 0
    assert result.failed_jobs == 1
    assert result.idle_polls == 1


def test_run_worker_loop_returns_after_idle_limit(tmp_path: Path) -> None:
    config = _config(tmp_path, max_idle_polls=2)
    api_client = FakeApiClient([(), ()])
    sleeps: list[float] = []

    result = run_worker_loop(config, lambda job_id: job_id, api_client=api_client, sleeper=sleeps.append)

    assert result.processed_jobs == 0
    assert result.failed_jobs == 0
    assert result.idle_polls == 2
    assert sleeps == [5.0]


def test_run_worker_loop_accounts_runner_failures(
    tmp_path: Path,
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    config = _config(tmp_path, max_processed_jobs=1, max_idle_polls=1)
    api_client = FakeApiClient(
        [
            (
                JobListItem(
                    job_id="job-fails",
                    job_type="transcription",
                    status="queued",
                    version=1,
                ),
            ),
            (),
        ]
    )

    def fail_job(job_id: str) -> None:
        raise RuntimeError(f"{job_id} failed")

    result = run_worker_loop(config, fail_job, api_client=api_client, sleeper=lambda _: None)

    assert result.processed_jobs == 0
    assert result.failed_jobs == 1
    assert result.idle_polls == 1
    assert "[WorkerCommon][runWorkerLoop][BLOCK_RUN_WORKER_LOOP]" in caplog.text


def test_runtime_config_rejects_invalid_numeric_env() -> None:
    with pytest.raises(ValueError, match="number"):
        WorkerRuntimeConfig.from_env(
            worker_kind="transcription",
            task_type="transcription.run",
            job_type="transcription",
            env={
                "API_BASE_URL": "http://api",
                "INTERNAL_API_TIMEOUT_SECONDS": "bad",
            },
        )


def _config(
    tmp_path: Path,
    *,
    job_id: str | None = None,
    max_idle_polls: int | None = None,
    max_processed_jobs: int | None = None,
) -> WorkerRuntimeConfig:
    return WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://internal.local"),
        worker_kind="transcription",
        task_type="transcription.run",
        job_type="transcription",
        workspace_root=tmp_path / "runtime",
        job_id=job_id,
        max_idle_polls=max_idle_polls,
        max_processed_jobs=max_processed_jobs,
    )
