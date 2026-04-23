# FILE: workers/common/src/transcriber_workers_common/__init__.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Expose the shared worker-common runtime surfaces extracted during Phase-3 packet execution.
# SCOPE: Re-export the internal API client, artifact writers, and reusable transcription and document helpers.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: BARREL
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Introduced the worker-common package boundary for shared worker runtime and compatibility re-exports.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   api-surface - Re-export the shared internal control-plane client and typed execution DTOs.
#   artifact-surface - Re-export canonical artifact writers and descriptor helpers.
#   content-surface - Re-export reusable transcription and document-rendering helpers used by later worker packets.
# END_MODULE_MAP

from transcriber_workers_common.api import (
    CancelCheckResult,
    ClaimedJobExecution,
    InternalApiConfig,
    InternalApiUnavailableError,
    JobApiClient,
    JobListItem,
    JsonTransport,
    OrderedWorkerInput,
)
from transcriber_workers_common.artifacts import (
    ArtifactDescriptor,
    ArtifactObjectStore,
    ArtifactWriter,
    build_artifact_object_key,
)
from transcriber_workers_common.documents import (
    build_source_label,
    build_transcript_markdown,
    build_transcript_title,
    format_timestamp,
    normalize_report_markdown,
    write_report_docx,
    write_transcript_docx,
)
from transcriber_workers_common.object_store import (
    ObjectStoreRequestFailed,
    RawObjectTransport,
    WorkerObjectStore,
    WorkerObjectStoreConfig,
)
from transcriber_workers_common.transcribers import (
    DefaultTranscriber,
    WhisperTranscriber,
    YouTubeTranscriptTranscriber,
)
from transcriber_workers_common.runtime import (
    JobRunner,
    WorkerLoopResult,
    WorkerRuntimeConfig,
    run_worker_loop,
)

__all__ = [
    "ArtifactDescriptor",
    "ArtifactObjectStore",
    "ArtifactWriter",
    "CancelCheckResult",
    "ClaimedJobExecution",
    "DefaultTranscriber",
    "InternalApiConfig",
    "InternalApiUnavailableError",
    "JobApiClient",
    "JobListItem",
    "JobRunner",
    "JsonTransport",
    "ObjectStoreRequestFailed",
    "OrderedWorkerInput",
    "RawObjectTransport",
    "WorkerObjectStore",
    "WorkerObjectStoreConfig",
    "WorkerLoopResult",
    "WorkerRuntimeConfig",
    "WhisperTranscriber",
    "YouTubeTranscriptTranscriber",
    "build_artifact_object_key",
    "build_source_label",
    "build_transcript_markdown",
    "build_transcript_title",
    "format_timestamp",
    "normalize_report_markdown",
    "run_worker_loop",
    "write_report_docx",
    "write_transcript_docx",
]
