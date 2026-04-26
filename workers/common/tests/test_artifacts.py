# FILE: workers/common/tests/test_artifacts.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the worker-common artifact writers preserve canonical object-key and descriptor behavior.
# SCOPE: Object-key derivation, text writes, file writes, and descriptor validation.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local artifact writer verification for worker-common.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   InMemoryObjectStore - Captures artifact bytes during tests.
#   test_build_artifact_object_key_uses_canonical_layout - Verifies canonical object-key prefixes.
#   test_write_text_artifact_returns_contract_shaped_descriptor - Verifies text writes and descriptor payloads.
#   test_write_file_artifact_uses_existing_file_bytes - Verifies file-backed artifact registration.
#   test_descriptor_rejects_invalid_artifact_kind - Verifies deterministic validation failures.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path

import pytest

from transcriber_workers_common.artifacts import (
    ArtifactDescriptor,
    ArtifactWriter,
    build_artifact_object_key,
)


class InMemoryObjectStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_bytes(self, *, object_key: str, content: bytes, mime_type: str) -> None:
        self.calls.append(
            {
                "object_key": object_key,
                "content": content,
                "mime_type": mime_type,
            }
        )


def test_build_artifact_object_key_uses_canonical_layout() -> None:
    assert (
        build_artifact_object_key("job-1", "transcript_segmented_markdown", "transcript.md")
        == "artifacts/job-1/transcript/segmented/transcript.md"
    )
    assert build_artifact_object_key("job-1", "execution_log", "execution.log") == "artifacts/job-1/logs/execution.log"
    assert (
        build_artifact_object_key("job-1", "agent_result_json", "result.json")
        == "artifacts/job-1/agent/result/result.json"
    )


def test_write_text_artifact_returns_contract_shaped_descriptor() -> None:
    object_store = InMemoryObjectStore()
    writer = ArtifactWriter(job_id="job-2", object_store=object_store)

    descriptor = writer.write_text_artifact(
        "report_markdown",
        "report.md",
        "# Report\n",
        mime_type="text/markdown; charset=utf-8",
        format="markdown",
    )

    assert descriptor.to_payload() == {
        "artifact_kind": "report_markdown",
        "format": "markdown",
        "filename": "report.md",
        "mime_type": "text/markdown; charset=utf-8",
        "object_key": "artifacts/job-2/report/markdown/report.md",
        "size_bytes": len("# Report\n".encode("utf-8")),
    }
    assert object_store.calls == [
        {
            "object_key": "artifacts/job-2/report/markdown/report.md",
            "content": b"# Report\n",
            "mime_type": "text/markdown; charset=utf-8",
        }
    ]


def test_write_agent_result_json_uses_agent_result_path() -> None:
    object_store = InMemoryObjectStore()
    writer = ArtifactWriter(job_id="job-agent", object_store=object_store)

    descriptor = writer.write_text_artifact(
        "agent_result_json",
        "result.json",
        '{"status":"ok"}',
        mime_type="application/json; charset=utf-8",
        format="json",
    )

    assert descriptor.to_payload() == {
        "artifact_kind": "agent_result_json",
        "format": "json",
        "filename": "result.json",
        "mime_type": "application/json; charset=utf-8",
        "object_key": "artifacts/job-agent/agent/result/result.json",
        "size_bytes": len(b'{"status":"ok"}'),
    }
    assert object_store.calls == [
        {
            "object_key": "artifacts/job-agent/agent/result/result.json",
            "content": b'{"status":"ok"}',
            "mime_type": "application/json; charset=utf-8",
        }
    ]


def test_write_file_artifact_uses_existing_file_bytes(tmp_path: Path) -> None:
    object_store = InMemoryObjectStore()
    writer = ArtifactWriter(job_id="job-3", object_store=object_store)
    docx_path = tmp_path / "transcript.docx"
    docx_path.write_bytes(b"docx-bytes")

    descriptor = writer.write_file_artifact(
        "transcript_docx",
        docx_path,
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )

    assert descriptor.object_key == "artifacts/job-3/transcript/docx/transcript.docx"
    assert descriptor.size_bytes == len(b"docx-bytes")
    assert object_store.calls[0]["content"] == b"docx-bytes"


def test_descriptor_rejects_invalid_artifact_kind() -> None:
    with pytest.raises(ValueError, match="invalid artifact_kind"):
        ArtifactDescriptor(
            artifact_kind="unknown",
            filename="artifact.txt",
            mime_type="text/plain",
            object_key="artifacts/job-1/unknown/artifact.txt",
            size_bytes=1,
        )
