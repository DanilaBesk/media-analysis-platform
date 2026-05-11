# FILE: workers/common/tests/test_runtime.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the shared worker runtime scaffold polls API-owned queued analysis runs and runs worker-specific functions without forking queue behavior.
# SCOPE: Env config parsing, explicit one-shot runs, queued polling, idle exit, and failure accounting.
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
#   FakeApiClient - Supplies deterministic queued analysis-run snapshots to the worker loop.
#   test_runtime_config_from_env_normalizes_worker_settings - Verifies launcher env parsing.
#   test_run_worker_loop_executes_explicit_run_without_polling - Verifies WORKER_ANALYSIS_RUN_ID one-shot mode.
#   test_run_worker_loop_polls_queued_run_through_api_client - Verifies queued polling and runner dispatch.
#   test_run_worker_loop_returns_after_idle_limit - Verifies deterministic idle exit for tests/smoke.
#   test_run_worker_loop_accounts_runner_failures - Verifies failures are logged and counted.
# END_MODULE_MAP

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from transcriber_workers_common.api import InternalApiConfig, AnalysisRunQueueItem
from transcriber_workers_common.runtime import WorkerRuntimeConfig, run_worker_loop


class FakeApiClient:
    def __init__(self, pages: list[tuple[AnalysisRunQueueItem, ...]]) -> None:
        self.pages = list(pages)
        self.calls: list[dict[str, object]] = []

    def list_queued_runs(
        self,
        *,
        status: str | None = None,
        run_type: str | None = None,
        task_type: str | None = None,
        page_size: int = 20,
    ) -> tuple[AnalysisRunQueueItem, ...]:
        self.calls.append({"status": status, "run_type": run_type, "task_type": task_type, "page_size": page_size})
        if not self.pages:
            return ()
        return self.pages.pop(0)


def test_runtime_config_from_env_normalizes_worker_settings(tmp_path: Path) -> None:
    config = WorkerRuntimeConfig.from_env(
        worker_kind="transcription",
        task_type="selection.transcription",
        run_type="transcription",
        env={
            "API_BASE_URL": " http://api.internal ",
            "INTERNAL_API_TIMEOUT_SECONDS": "7.5",
            "WORKER_WORKSPACE_ROOT": str(tmp_path / "workspace"),
            "WORKER_POLL_INTERVAL_SECONDS": "0.25",
            "WORKER_ANALYSIS_RUN_ID": "run-1",
            "WORKER_MAX_IDLE_POLLS": "2",
            "WORKER_MAX_PROCESSED_RUNS": "3",
        },
    )

    assert config.api_config.base_url == "http://api.internal"
    assert config.api_config.timeout_seconds == 7.5
    assert config.workspace_root == tmp_path / "workspace"
    assert config.poll_interval_seconds == 0.25
    assert config.analysis_run_id == "run-1"
    assert config.max_idle_polls == 2
    assert config.max_processed_runs == 3


def test_runtime_config_accepts_agent_runner_identity(tmp_path: Path) -> None:
    config = WorkerRuntimeConfig.from_env(
        worker_kind="agent_runner",
        task_type="selection.analysis",
        run_type="custom",
        env={
            "API_BASE_URL": "http://api.internal",
            "WORKER_WORKSPACE_ROOT": str(tmp_path / "workspace"),
        },
    )

    assert config.worker_kind == "agent_runner"
    assert config.task_type == "selection.analysis"
    assert config.run_type == "custom"


def test_run_worker_loop_executes_explicit_run_without_polling(tmp_path: Path) -> None:
    config = _config(tmp_path, analysis_run_id="run-explicit")
    api_client = FakeApiClient([])
    processed: list[str] = []

    result = run_worker_loop(config, processed.append, api_client=api_client, sleeper=lambda _: None)

    assert processed == ["run-explicit"]
    assert api_client.calls == []
    assert result.processed_runs == 1
    assert result.failed_runs == 0
    assert result.idle_polls == 0


def test_run_worker_loop_accounts_explicit_run_failure(tmp_path: Path) -> None:
    config = _config(tmp_path, analysis_run_id="run-explicit")

    def fail_run(analysis_run_id: str) -> None:
        raise RuntimeError(analysis_run_id)

    result = run_worker_loop(config, fail_run, api_client=FakeApiClient([]), sleeper=lambda _: None)

    assert result.processed_runs == 0
    assert result.failed_runs == 1
    assert result.idle_polls == 0


def test_run_worker_loop_polls_queued_run_through_api_client(tmp_path: Path) -> None:
    config = _config(tmp_path, max_processed_runs=1)
    api_client = FakeApiClient(
        [
            (
                AnalysisRunQueueItem(
                    analysis_run_id="run-queued",
                    run_type="transcription",
                    task_type="selection.transcription",
                    status="queued",
                    version=1,
                ),
            )
        ]
    )
    processed: list[str] = []

    result = run_worker_loop(config, processed.append, api_client=api_client, sleeper=lambda _: None)

    assert processed == ["run-queued"]
    assert api_client.calls == [
        {"status": "queued", "run_type": "transcription", "task_type": "selection.transcription", "page_size": 1}
    ]
    assert result.processed_runs == 1
    assert result.failed_runs == 0


def test_run_worker_loop_counts_queue_poll_failures(tmp_path: Path) -> None:
    class FailingApiClient:
        def list_queued_runs(
            self,
            *,
            status: str | None = None,
            run_type: str | None = None,
            task_type: str | None = None,
            page_size: int = 20,
        ):
            raise RuntimeError(f"{status}:{run_type}:{task_type}:{page_size}")

    config = _config(tmp_path, max_idle_polls=1)

    result = run_worker_loop(config, lambda analysis_run_id: analysis_run_id, api_client=FailingApiClient(), sleeper=lambda _: None)

    assert result.processed_runs == 0
    assert result.failed_runs == 1
    assert result.idle_polls == 1


def test_run_worker_loop_sleeps_and_recovers_after_queue_poll_failure(tmp_path: Path) -> None:
    class FlakyApiClient:
        def __init__(self) -> None:
            self.calls = 0

        def list_queued_runs(
            self,
            *,
            status: str | None = None,
            run_type: str | None = None,
            task_type: str | None = None,
            page_size: int = 20,
        ):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary queue failure")
            return (
                AnalysisRunQueueItem(
                    analysis_run_id="run-recovered",
                    run_type="transcription",
                    task_type="selection.transcription",
                    status="queued",
                    version=1,
                ),
            )

    config = _config(tmp_path, max_processed_runs=1, max_idle_polls=2)
    processed: list[str] = []
    sleeps: list[float] = []

    result = run_worker_loop(config, processed.append, api_client=FlakyApiClient(), sleeper=sleeps.append)

    assert processed == ["run-recovered"]
    assert result.processed_runs == 1
    assert result.failed_runs == 1
    assert result.idle_polls == 0
    assert sleeps == [5.0]


def test_run_worker_loop_returns_after_idle_limit(tmp_path: Path) -> None:
    config = _config(tmp_path, max_idle_polls=2)
    api_client = FakeApiClient([(), ()])
    sleeps: list[float] = []

    result = run_worker_loop(config, lambda analysis_run_id: analysis_run_id, api_client=api_client, sleeper=sleeps.append)

    assert result.processed_runs == 0
    assert result.failed_runs == 0
    assert result.idle_polls == 2
    assert sleeps == [5.0]


def test_run_worker_loop_accounts_runner_failures(
    tmp_path: Path,
    caplog,
) -> None:
    caplog.set_level(logging.INFO)
    config = _config(tmp_path, max_processed_runs=1, max_idle_polls=1)
    api_client = FakeApiClient(
        [
            (
                AnalysisRunQueueItem(
                    analysis_run_id="run-fails",
                    run_type="transcription",
                    task_type="selection.transcription",
                    status="queued",
                    version=1,
                ),
            ),
            (),
        ]
    )

    def fail_run(analysis_run_id: str) -> None:
        raise RuntimeError(f"{analysis_run_id} failed")

    result = run_worker_loop(config, fail_run, api_client=api_client, sleeper=lambda _: None)

    assert result.processed_runs == 0
    assert result.failed_runs == 1
    assert result.idle_polls == 1
    assert "[WorkerCommon][runWorkerLoop][BLOCK_RUN_WORKER_LOOP]" in caplog.text


def test_runtime_config_rejects_invalid_numeric_env() -> None:
    with pytest.raises(ValueError, match="number"):
        WorkerRuntimeConfig.from_env(
            worker_kind="transcription",
            task_type="selection.transcription",
            run_type="transcription",
            env={
                "API_BASE_URL": "http://api",
                "INTERNAL_API_TIMEOUT_SECONDS": "bad",
            },
        )


def test_runtime_config_rejects_invalid_optional_integer_env() -> None:
    with pytest.raises(ValueError, match="integer"):
        WorkerRuntimeConfig.from_env(
            worker_kind="transcription",
            task_type="selection.transcription",
            run_type="transcription",
            env={
                "API_BASE_URL": "http://api",
                "WORKER_MAX_IDLE_POLLS": "bad",
            },
        )


def test_runtime_config_rejects_nonpositive_processed_limit() -> None:
    with pytest.raises(ValueError, match="positive"):
        WorkerRuntimeConfig.from_env(
            worker_kind="transcription",
            task_type="selection.transcription",
            run_type="transcription",
            env={
                "API_BASE_URL": "http://api",
                "WORKER_MAX_PROCESSED_RUNS": "0",
            },
        )


def _config(
    tmp_path: Path,
    *,
    analysis_run_id: str | None = None,
    max_idle_polls: int | None = None,
    max_processed_runs: int | None = None,
) -> WorkerRuntimeConfig:
    return WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://internal.local"),
        worker_kind="transcription",
        task_type="selection.transcription",
        run_type="transcription",
        workspace_root=tmp_path / "runtime",
        analysis_run_id=analysis_run_id,
        max_idle_polls=max_idle_polls,
        max_processed_runs=max_processed_runs,
    )
