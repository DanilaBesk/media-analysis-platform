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
#   test_build_runner_keeps_selection_runs_on_transcription_worker - Verifies selection runs remain on the transcription worker.
#   test_build_transcriber_uses_copper_asr_env - Verifies the transcriber uses CopperASR environment settings.
#   test_launcher_has_no_hidden_worker_dependency_path_bootstrap - Verifies the launcher has no implicit dependency-path bootstrap.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

from transcriber_workers_common.api import InternalApiConfig
from transcriber_workers_common.copper_asr import CopperAsrHttpTranscriber
from transcriber_workers_common.runtime import WorkerRuntimeConfig

import transcriber_worker_transcription_main as launcher


def test_build_runner_delegates_to_run_transcription(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="transcription",
        step_kind="selection.transcription",
        run_type="transcription",
        workspace_root=tmp_path / "runtime",
    )
    api_client = object()
    object_store = object()
    transcriber = object()
    calls: list[dict[str, object]] = []

    def fake_run_transcription(analysis_run_id: str, **kwargs):
        calls.append({"analysis_run_id": analysis_run_id, **kwargs})
        return "ok"

    monkeypatch.setattr(launcher, "runTranscription", fake_run_transcription)

    result = launcher.build_runner(
        config,
        api_client=api_client,
        object_store=object_store,
        transcriber=transcriber,
    )("run-1")

    assert result == "ok"
    assert calls[0]["analysis_run_id"] == "run-1"
    attempt_root = calls[0]["workspace_root"]
    assert isinstance(attempt_root, Path)
    assert attempt_root.parent == (tmp_path / "runtime").resolve()
    assert attempt_root.name.startswith("run-1--")
    assert not attempt_root.exists()
    assert calls[0]["api_client"] is api_client
    assert calls[0]["source_store"] is object_store
    assert calls[0]["artifact_store"] is object_store
    assert calls[0]["transcriber"] is transcriber


def test_build_runner_keeps_selection_runs_on_transcription_worker(monkeypatch, tmp_path: Path) -> None:
    config = WorkerRuntimeConfig(
        api_config=InternalApiConfig(base_url="http://api"),
        worker_kind="transcription",
        step_kind="selection.transcription",
        run_type="transcription",
        workspace_root=tmp_path / "runtime",
    )
    api_client = object()
    object_store = object()
    transcriber = object()
    calls: list[dict[str, object]] = []

    def fake_run_transcription(analysis_run_id: str, **kwargs):
        calls.append({"analysis_run_id": analysis_run_id, **kwargs})
        return "ok"

    monkeypatch.setattr(launcher, "runTranscription", fake_run_transcription)

    result = launcher.build_runner(
        config,
        api_client=api_client,
        object_store=object_store,
        transcriber=transcriber,
    )("run-1")

    assert result == "ok"
    assert calls[0]["analysis_run_id"] == "run-1"
    attempt_root = calls[0]["workspace_root"]
    assert isinstance(attempt_root, Path)
    assert attempt_root.parent == (tmp_path / "runtime").resolve()
    assert not attempt_root.exists()


def test_launcher_has_no_hidden_worker_dependency_path_bootstrap() -> None:
    assert not hasattr(launcher, "_ensure_worker_dependency_paths")


def test_build_transcriber_uses_copper_asr_env() -> None:
    transcriber = launcher._build_transcriber(
        {
            "COPPER_ASR_BASE_URL": "http://copper-asr-test:8000",
            "COPPER_ASR_CLIENT_TIMEOUT_S": "42",
            "COPPER_ASR_LANGUAGE": "ru",
            "COPPER_ASR_PAUSE_THRESHOLD_S": "1.75",
            "COPPER_ASR_DIARIZATION": "false",
            "LEGACY_ASR_MODEL": "must-not-be-read",
        }
    )

    assert isinstance(transcriber, CopperAsrHttpTranscriber)
    assert transcriber.config.base_url == "http://copper-asr-test:8000"
    assert transcriber.config.timeout_seconds == 42
    assert transcriber.config.pause_threshold_seconds == 1.75
    assert transcriber.config.diarization is False
