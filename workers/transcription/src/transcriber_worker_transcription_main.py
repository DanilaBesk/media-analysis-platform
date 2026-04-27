# FILE: workers/transcription/src/transcriber_worker_transcription_main.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the compose-ready executable launcher for the transcription worker.
# SCOPE: Runtime env loading, shared worker loop wiring, MinIO object-store adapter wiring, and transcription runner construction.
# DEPENDS: M-WORKER-TRANSCRIPTION, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-TRANSCRIPTION, V-M-WORKER-TRANSCRIPTION
# ROLE: SCRIPT
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the compose-ready transcription worker launcher wired through worker-common runtime.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   launch-transcription-worker - Build the API client, object-store adapter, default transcriber, and shared worker loop.
#   build-transcription-runner - Adapt claimed job IDs into runTranscription calls without duplicating queue logic.
# END_MODULE_MAP

from __future__ import annotations

import logging
import os
from typing import Mapping

from transcriber_worker_transcription import runTranscription, runTranscriptionAggregate
from transcriber_workers_common.api import JobApiClient
from transcriber_workers_common.object_store import WorkerObjectStore, WorkerObjectStoreConfig
from transcriber_workers_common.runtime import WorkerRuntimeConfig, run_worker_loop
from transcriber_workers_common.transcribers import DefaultTranscriber, PODLODKA_WHISPER_MODEL


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_LAUNCH_TRANSCRIPTION = "[WorkerTranscription][main][BLOCK_LAUNCH_TRANSCRIPTION_WORKER]"

__all__ = ["build_runner", "main"]


def build_runner(
    config: WorkerRuntimeConfig,
    *,
    api_client: JobApiClient,
    object_store: WorkerObjectStore,
    transcriber: object,
):
    def _runner(job_id: str) -> object:
        snapshot = api_client.get_job_snapshot(job_id)
        if (
            snapshot.job_type == "transcription"
            and snapshot.parent_job_id is None
            and any(child.job_type == "transcription" for child in snapshot.children)
        ):
            return runTranscriptionAggregate(
                job_id,
                workspace_root=config.workspace_root,
                api_client=api_client,
                artifact_store=object_store,
            )
        return runTranscription(
            job_id,
            workspace_root=config.workspace_root,
            api_client=api_client,
            source_store=object_store,
            artifact_store=object_store,
            transcriber=transcriber,
        )

    return _runner


def main(env: Mapping[str, str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    values = os.environ if env is None else env
    config = WorkerRuntimeConfig.from_env(
        worker_kind="transcription",
        task_type="transcription.run",
        job_type="transcription",
        env=values,
    )
    api_client = JobApiClient(config.api_config)
    object_store = WorkerObjectStore(WorkerObjectStoreConfig.from_env(values))
    runner = build_runner(
        config,
        api_client=api_client,
        object_store=object_store,
        transcriber=_build_transcriber(values),
    )
    _LOGGER.info("%s workspace_root=%s", _LOG_MARKER_LAUNCH_TRANSCRIPTION, config.workspace_root)
    run_worker_loop(config, runner, api_client=api_client)
    return 0


def _build_transcriber(env: Mapping[str, str]) -> DefaultTranscriber:
    languages = tuple(
        part.strip()
        for part in env.get("YOUTUBE_TRANSCRIPT_LANGUAGES", "ru,en").split(",")
        if part.strip()
    )
    return DefaultTranscriber(
        youtube_languages=languages or ("ru", "en"),
        whisper_model=env.get("WHISPER_MODEL", PODLODKA_WHISPER_MODEL).strip() or PODLODKA_WHISPER_MODEL,
        whisper_device=env.get("WHISPER_DEVICE", "cpu").strip() or "cpu",
        whisper_compute_type=env.get("WHISPER_COMPUTE_TYPE", "default").strip() or "default",
    )


if __name__ == "__main__":
    raise SystemExit(main())
