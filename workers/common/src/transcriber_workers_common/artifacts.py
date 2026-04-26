# FILE: workers/common/src/transcriber_workers_common/artifacts.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide canonical artifact-writer helpers that preserve the frozen worker artifact-registration DTO shape.
# SCOPE: Object-key derivation, descriptor validation, and reusable text, bytes, or file-backed artifact writes through an injected store.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added canonical artifact writers and descriptor validation for worker-common.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   ArtifactObjectStore - Defines the write boundary used by worker-common artifact helpers.
#   ArtifactDescriptor - Represents one canonical artifact-registration payload entry.
#   build_artifact_object_key - Derives the canonical MinIO object-key path for one worker artifact.
#   ArtifactWriter - Writes artifacts through an injected store and returns canonical descriptors.
# END_MODULE_MAP

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol


_ARTIFACT_KINDS = frozenset(
    {
        "transcript_plain",
        "transcript_segmented_markdown",
        "transcript_docx",
        "report_markdown",
        "report_docx",
        "deep_research_markdown",
        "agent_result_json",
        "execution_log",
        "source_manifest_json",
        "batch_diagnostics_json",
    }
)
_ARTIFACT_KEY_SEGMENTS = {
    "transcript_plain": ("transcript", "plain"),
    "transcript_segmented_markdown": ("transcript", "segmented"),
    "transcript_docx": ("transcript", "docx"),
    "report_markdown": ("report", "markdown"),
    "report_docx": ("report", "docx"),
    "deep_research_markdown": ("deep-research", "markdown"),
    "agent_result_json": ("agent", "result"),
    "execution_log": ("logs",),
    "source_manifest_json": (),
    "batch_diagnostics_json": (),
}

__all__ = [
    "ArtifactDescriptor",
    "ArtifactObjectStore",
    "ArtifactWriter",
    "build_artifact_object_key",
]


# START_CONTRACT: ArtifactObjectStore
# PURPOSE: Define the object-store boundary used by worker-common artifact writers.
# INPUTS: { object_key: str - Canonical object key, content: bytes - Artifact bytes, mime_type: str - Artifact mime type }
# OUTPUTS: { None - The side effect is the durable object write }
# SIDE_EFFECTS: object storage IO through the concrete implementation
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: ArtifactObjectStore
class ArtifactObjectStore(Protocol):
    def put_bytes(self, *, object_key: str, content: bytes, mime_type: str) -> None: ...


# START_CONTRACT: ArtifactDescriptor
# PURPOSE: Represent one canonical artifact-registration payload entry.
# INPUTS: { artifact_kind: str - Frozen artifact kind, filename: str - Artifact filename, mime_type: str - Artifact mime type, object_key: str - Canonical object-store key, size_bytes: int - Artifact size, format: str | None - Optional artifact format }
# OUTPUTS: { ArtifactDescriptor - Reusable artifact metadata surface }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: ArtifactDescriptor
@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    artifact_kind: str
    filename: str
    mime_type: str
    object_key: str
    size_bytes: int
    format: str | None = None

    def __post_init__(self) -> None:
        _require(self.artifact_kind in _ARTIFACT_KINDS, "invalid artifact_kind")
        _require(bool(self.filename), "filename must not be empty")
        _require(bool(self.mime_type), "mime_type must not be empty")
        _require(bool(self.object_key), "object_key must not be empty")
        _require(self.size_bytes >= 0, "size_bytes must be non-negative")

    def to_payload(self) -> dict[str, object]:
        return {
            "artifact_kind": self.artifact_kind,
            "format": self.format,
            "filename": self.filename,
            "mime_type": self.mime_type,
            "object_key": self.object_key,
            "size_bytes": self.size_bytes,
        }


# START_CONTRACT: build_artifact_object_key
# PURPOSE: Derive the canonical object-store key for one worker artifact.
# INPUTS: { job_id: str - Owning job identifier, artifact_kind: str - Frozen artifact kind, filename: str - Stored filename }
# OUTPUTS: { str - Canonical `artifacts/...` object key }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: build_artifact_object_key
def build_artifact_object_key(job_id: str, artifact_kind: str, filename: str) -> str:
    # START_BLOCK_BLOCK_BUILD_CANONICAL_ARTIFACT_KEY
    _require(bool(job_id), "job_id must not be empty")
    _require(artifact_kind in _ARTIFACT_KEY_SEGMENTS, "invalid artifact_kind")
    _require(bool(filename), "filename must not be empty")
    return str(PurePosixPath("artifacts", job_id, *_ARTIFACT_KEY_SEGMENTS[artifact_kind], filename))
    # END_BLOCK_BLOCK_BUILD_CANONICAL_ARTIFACT_KEY


# START_CONTRACT: ArtifactWriter
# PURPOSE: Persist artifacts through an injected store and return canonical descriptors for API registration.
# INPUTS: { job_id: str - Owning job identifier, object_store: ArtifactObjectStore - Durable object-store boundary }
# OUTPUTS: { ArtifactWriter - Reusable artifact-writing helper }
# SIDE_EFFECTS: object storage IO through the injected store
# LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
# END_CONTRACT: ArtifactWriter
class ArtifactWriter:
    def __init__(self, *, job_id: str, object_store: ArtifactObjectStore) -> None:
        _require(bool(job_id), "job_id must not be empty")
        self.job_id = job_id
        self.object_store = object_store

    # START_CONTRACT: write_bytes_artifact
    # PURPOSE: Persist arbitrary artifact bytes and return the canonical descriptor.
    # INPUTS: { artifact_kind: str - Frozen artifact kind, filename: str - Stored filename, content: bytes - Artifact bytes, mime_type: str - Artifact mime type, format: str | None - Optional artifact format }
    # OUTPUTS: { ArtifactDescriptor - Canonical artifact metadata for API registration }
    # SIDE_EFFECTS: object store write
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: write_bytes_artifact
    def write_bytes_artifact(
        self,
        artifact_kind: str,
        filename: str,
        content: bytes,
        *,
        mime_type: str,
        format: str | None = None,
    ) -> ArtifactDescriptor:
        # START_BLOCK_BLOCK_WRITE_ARTIFACT_BYTES
        object_key = build_artifact_object_key(self.job_id, artifact_kind, filename)
        self.object_store.put_bytes(object_key=object_key, content=content, mime_type=mime_type)
        return ArtifactDescriptor(
            artifact_kind=artifact_kind,
            filename=filename,
            mime_type=mime_type,
            object_key=object_key,
            size_bytes=len(content),
            format=format,
        )
        # END_BLOCK_BLOCK_WRITE_ARTIFACT_BYTES

    # START_CONTRACT: write_text_artifact
    # PURPOSE: Persist UTF-8 text content and return the canonical descriptor.
    # INPUTS: { artifact_kind: str - Frozen artifact kind, filename: str - Stored filename, content: str - Text content, mime_type: str - Artifact mime type, format: str | None - Optional artifact format, encoding: str - Text encoding }
    # OUTPUTS: { ArtifactDescriptor - Canonical artifact metadata for API registration }
    # SIDE_EFFECTS: object store write
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: write_text_artifact
    def write_text_artifact(
        self,
        artifact_kind: str,
        filename: str,
        content: str,
        *,
        mime_type: str = "text/plain; charset=utf-8",
        format: str | None = None,
        encoding: str = "utf-8",
    ) -> ArtifactDescriptor:
        return self.write_bytes_artifact(
            artifact_kind,
            filename,
            content.encode(encoding),
            mime_type=mime_type,
            format=format,
        )

    # START_CONTRACT: write_file_artifact
    # PURPOSE: Persist an already-rendered file and return the canonical descriptor.
    # INPUTS: { artifact_kind: str - Frozen artifact kind, file_path: Path - Local file to persist, mime_type: str - Artifact mime type, filename: str | None - Optional override filename, format: str | None - Optional artifact format }
    # OUTPUTS: { ArtifactDescriptor - Canonical artifact metadata for API registration }
    # SIDE_EFFECTS: file read and object store write
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: write_file_artifact
    def write_file_artifact(
        self,
        artifact_kind: str,
        file_path: Path,
        *,
        mime_type: str,
        filename: str | None = None,
        format: str | None = None,
    ) -> ArtifactDescriptor:
        resolved_filename = filename or file_path.name
        return self.write_bytes_artifact(
            artifact_kind,
            resolved_filename,
            file_path.read_bytes(),
            mime_type=mime_type,
            format=format,
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)
