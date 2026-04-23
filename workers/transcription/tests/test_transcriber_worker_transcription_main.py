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

from transcriber_workers_common.api import InternalApiConfig
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
    api_client = object()
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
