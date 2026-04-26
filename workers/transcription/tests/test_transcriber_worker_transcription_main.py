# FILE: workers/transcription/tests/test_transcriber_worker_transcription_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the transcription worker launcher delegates execution through the shared runtime boundaries.
# SCOPE: build_runner wiring only; worker loop behavior remains covered by worker-common tests.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for transcription launcher runner wiring.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   test_build_runner_delegates_to_run_transcription - Verifies the launcher preserves transcription worker dependency wiring.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

from transcriber_workers_common.api import ChildJobReference, InternalApiConfig, JobSnapshot
from transcriber_workers_common.runtime import WorkerRuntimeConfig

import transcriber_worker_transcription_main as launcher


def test_build_runner_delegates_to_run_transcription(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="transcription",
        task_type="transcription.run",
        job_type="transcription",
        workspace_root=tmp_path / "runtime",
    )
    api_client = _FakeApiClient(
        JobSnapshot(
            job_id="job-1",
            root_job_id="job-1",
            parent_job_id=None,
            job_type="transcription",
            status="queued",
            version=1,
            source_set_items=(),
            artifacts=(),
            children=(),
        )
    )
    object_store = object()
    transcriber = object()
    calls: list[dict[str, object]] = []

    def fake_run_transcription(job_id: str, **kwargs):
        calls.append({"job_id": job_id, **kwargs})
        return "ok"

    monkeypatch.setattr(launcher, "runTranscription", fake_run_transcription)

    result = launcher.build_runner(
        config,
        api_client=api_client,
        object_store=object_store,
        transcriber=transcriber,
    )("job-1")

    assert result == "ok"
    assert calls == [
        {
            "job_id": "job-1",
            "workspace_root": tmp_path / "runtime",
            "api_client": api_client,
            "source_store": object_store,
            "artifact_store": object_store,
            "transcriber": transcriber,
        }
    ]
    assert api_client.calls == ["job-1"]


def test_build_runner_delegates_batch_root_to_aggregate(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="transcription",
        task_type="transcription.run",
        job_type="transcription",
        workspace_root=tmp_path / "runtime",
    )
    api_client = _FakeApiClient(
        JobSnapshot(
            job_id="root-job",
            root_job_id="root-job",
            parent_job_id=None,
            job_type="transcription",
            status="queued",
            version=1,
            source_set_items=(),
            artifacts=(),
            children=(ChildJobReference(job_id="child-job", job_type="transcription", status="succeeded", version=2),),
        )
    )
    object_store = object()
    transcriber = object()
    calls: list[dict[str, object]] = []

    def fake_run_aggregate(job_id: str, **kwargs):
        calls.append({"job_id": job_id, **kwargs})
        return "aggregate-ok"

    def fail_run_transcription(job_id: str, **kwargs):
        raise AssertionError("batch root should use aggregate runner")

    monkeypatch.setattr(launcher, "runTranscriptionAggregate", fake_run_aggregate)
    monkeypatch.setattr(launcher, "runTranscription", fail_run_transcription)

    result = launcher.build_runner(
        config,
        api_client=api_client,
        object_store=object_store,
        transcriber=transcriber,
    )("root-job")

    assert result == "aggregate-ok"
    assert calls == [
        {
            "job_id": "root-job",
            "workspace_root": tmp_path / "runtime",
            "api_client": api_client,
            "artifact_store": object_store,
        }
    ]


def test_launcher_has_no_hidden_worker_dependency_path_bootstrap() -> None:
    assert not hasattr(launcher, "_ensure_worker_dependency_paths")


class _FakeApiClient:
    def __init__(self, snapshot: JobSnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[str] = []

    def get_job_snapshot(self, job_id: str) -> JobSnapshot:
        self.calls.append(job_id)
        return self.snapshot
