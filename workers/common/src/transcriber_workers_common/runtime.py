# FILE: workers/common/src/transcriber_workers_common/runtime.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the shared worker execution scaffold used by compose-ready worker launchers.
# SCOPE: Env-backed runtime config, queued-job polling through the API client, one-shot job execution, retry-safe loop control, and deterministic loop results for tests.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the canonical worker loop scaffold so worker-specific launchers do not fork queue polling behavior.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   JobRunner - Defines the callable protocol injected by worker-specific launchers.
#   WorkerRuntimeConfig - Carries API, worker identity, polling, workspace, and test-limit settings.
#   WorkerLoopResult - Reports processed, failed, and idle loop counts for verification.
#   run_worker_loop - Polls queued jobs through the API contract and invokes one injected job runner.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol

from transcriber_workers_common.api import InternalApiConfig, JobApiClient


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_RUN_WORKER_LOOP = "[WorkerCommon][runWorkerLoop][BLOCK_RUN_WORKER_LOOP]"
_WORKER_KINDS = frozenset({"transcription", "agent_runner"})
_TASK_TYPES = frozenset({"transcription.run", "agent_run.run"})
_JOB_TYPES = frozenset({"transcription", "report", "deep_research", "agent_run"})

__all__ = [
    "JobRunner",
    "WorkerLoopResult",
    "WorkerRuntimeConfig",
    "run_worker_loop",
]


class JobRunner(Protocol):
    def __call__(self, job_id: str) -> object: ...


# START_CONTRACT: WorkerRuntimeConfig
# PURPOSE: Carry the shared worker runtime settings that every executable launcher consumes.
# INPUTS: { api_config: InternalApiConfig - API boundary, worker_kind/task_type/job_type - frozen worker identity, workspace_root: Path - local workspace, polling and test-limit settings }
# OUTPUTS: { WorkerRuntimeConfig - Immutable runtime config for run_worker_loop }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: WorkerRuntimeConfig
@dataclass(frozen=True, slots=True)
class WorkerRuntimeConfig:
    api_config: InternalApiConfig
    worker_kind: str
    task_type: str
    job_type: str
    workspace_root: Path
    poll_interval_seconds: float = 5.0
    job_id: str | None = None
    max_idle_polls: int | None = None
    max_processed_jobs: int | None = None

    def __post_init__(self) -> None:
        _require(self.worker_kind in _WORKER_KINDS, "invalid worker_kind")
        _require(self.task_type in _TASK_TYPES, "invalid task_type")
        _require(self.job_type in _JOB_TYPES, "invalid job_type")
        _require(self.poll_interval_seconds >= 0, "poll interval must be non-negative")
        if self.max_idle_polls is not None:
            _require(self.max_idle_polls >= 0, "max_idle_polls must be non-negative")
        if self.max_processed_jobs is not None:
            _require(self.max_processed_jobs > 0, "max_processed_jobs must be positive")

    @classmethod
    def from_env(
        cls,
        *,
        worker_kind: str,
        task_type: str,
        job_type: str,
        env: Mapping[str, str] | None = None,
    ) -> "WorkerRuntimeConfig":
        values = os.environ if env is None else env
        api_base_url = values.get("API_BASE_URL", "http://api:8080").strip() or "http://api:8080"
        timeout_seconds = _parse_positive_float(values.get("INTERNAL_API_TIMEOUT_SECONDS"), 30.0)
        return cls(
            api_config=InternalApiConfig(base_url=api_base_url, timeout_seconds=timeout_seconds),
            worker_kind=worker_kind,
            task_type=task_type,
            job_type=job_type,
            workspace_root=Path(values.get("WORKER_WORKSPACE_ROOT", "/tmp/runtime")),
            poll_interval_seconds=_parse_non_negative_float(values.get("WORKER_POLL_INTERVAL_SECONDS"), 5.0),
            job_id=_optional_env(values.get("WORKER_JOB_ID")),
            max_idle_polls=_parse_optional_non_negative_int(values.get("WORKER_MAX_IDLE_POLLS")),
            max_processed_jobs=_parse_optional_positive_int(values.get("WORKER_MAX_PROCESSED_JOBS")),
        )


# START_CONTRACT: WorkerLoopResult
# PURPOSE: Report worker-loop progress for packet-local tests and launcher diagnostics.
# INPUTS: { processed_jobs: int, failed_jobs: int, idle_polls: int - Loop counters }
# OUTPUTS: { WorkerLoopResult - Deterministic execution summary }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# END_CONTRACT: WorkerLoopResult
@dataclass(frozen=True, slots=True)
class WorkerLoopResult:
    processed_jobs: int
    failed_jobs: int
    idle_polls: int


# START_CONTRACT: run_worker_loop
# PURPOSE: Poll queued jobs through the canonical API client and invoke one worker-specific job runner.
# INPUTS: { config: WorkerRuntimeConfig - Runtime settings, run_job: JobRunner - Worker-specific one-job function, api_client: JobApiClient | None - Optional test/client override, sleeper: Callable - Sleep hook }
# OUTPUTS: { WorkerLoopResult - Summary when a finite one-shot/test-limited run exits }
# SIDE_EFFECTS: API reads, worker-specific runner side effects, sleeps between idle polls
# LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001, DF-006
# END_CONTRACT: run_worker_loop
def run_worker_loop(
    config: WorkerRuntimeConfig,
    run_job: JobRunner,
    *,
    api_client: JobApiClient | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> WorkerLoopResult:
    # START_BLOCK_BLOCK_RUN_WORKER_LOOP
    client = api_client or JobApiClient(config.api_config)
    processed_jobs = 0
    failed_jobs = 0
    idle_polls = 0

    config.workspace_root.mkdir(parents=True, exist_ok=True)
    _LOGGER.info(
        "%s worker_kind=%s task_type=%s job_type=%s workspace_root=%s",
        _LOG_MARKER_RUN_WORKER_LOOP,
        config.worker_kind,
        config.task_type,
        config.job_type,
        config.workspace_root,
    )

    if config.job_id:
        if _run_one_job(config.job_id, run_job):
            processed_jobs += 1
        else:
            failed_jobs += 1
        return WorkerLoopResult(processed_jobs=processed_jobs, failed_jobs=failed_jobs, idle_polls=idle_polls)

    while True:
        try:
            queued_jobs = client.list_jobs(status="queued", job_type=config.job_type, page_size=1)
        except Exception:
            failed_jobs += 1
            idle_polls += 1
            _LOGGER.exception("%s queue_poll_failed job_type=%s", _LOG_MARKER_RUN_WORKER_LOOP, config.job_type)
            if _should_stop(config, processed_jobs=processed_jobs, idle_polls=idle_polls):
                return WorkerLoopResult(processed_jobs=processed_jobs, failed_jobs=failed_jobs, idle_polls=idle_polls)
            sleeper(config.poll_interval_seconds)
            continue

        if not queued_jobs:
            idle_polls += 1
            _LOGGER.info("%s idle job_type=%s idle_polls=%d", _LOG_MARKER_RUN_WORKER_LOOP, config.job_type, idle_polls)
            if _should_stop(config, processed_jobs=processed_jobs, idle_polls=idle_polls):
                return WorkerLoopResult(processed_jobs=processed_jobs, failed_jobs=failed_jobs, idle_polls=idle_polls)
            sleeper(config.poll_interval_seconds)
            continue

        idle_polls = 0
        for job in queued_jobs:
            if _run_one_job(job.job_id, run_job):
                processed_jobs += 1
            else:
                failed_jobs += 1
            if _should_stop(config, processed_jobs=processed_jobs, idle_polls=idle_polls):
                return WorkerLoopResult(processed_jobs=processed_jobs, failed_jobs=failed_jobs, idle_polls=idle_polls)
    # END_BLOCK_BLOCK_RUN_WORKER_LOOP


def _run_one_job(job_id: str, run_job: JobRunner) -> bool:
    _LOGGER.info("%s run_job job_id=%s", _LOG_MARKER_RUN_WORKER_LOOP, job_id)
    try:
        run_job(job_id)
    except Exception:
        _LOGGER.exception("%s run_job_failed job_id=%s", _LOG_MARKER_RUN_WORKER_LOOP, job_id)
        return False
    return True


def _should_stop(config: WorkerRuntimeConfig, *, processed_jobs: int, idle_polls: int) -> bool:
    if config.max_processed_jobs is not None and processed_jobs >= config.max_processed_jobs:
        return True
    if config.max_idle_polls is not None and idle_polls >= config.max_idle_polls:
        return True
    return False


def _optional_env(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_positive_float(value: str | None, default: float) -> float:
    parsed = _parse_non_negative_float(value, default)
    _require(parsed > 0, "value must be positive")
    return parsed


def _parse_non_negative_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError("value must be a number") from exc
    _require(parsed >= 0, "value must be non-negative")
    return parsed


def _parse_optional_non_negative_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError("value must be an integer") from exc
    _require(parsed >= 0, "value must be non-negative")
    return parsed


def _parse_optional_positive_int(value: str | None) -> int | None:
    parsed = _parse_optional_non_negative_int(value)
    if parsed is not None:
        _require(parsed > 0, "value must be positive")
    return parsed


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)
