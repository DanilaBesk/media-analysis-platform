# FILE: apps/telegram-bot/tests/test_gateway.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Prove the Telegram adapter gateway import surface works only through the explicit worker-common path contract, without hidden runtime path mutation.
# SCOPE: Verify direct module import and package re-export resolve the same gateway class in the supported explicit PYTHONPATH mode.
# DEPENDS: M-TELEGRAM-ADAPTER, M-API-HTTP
# LINKS: V-M-TELEGRAM-ADAPTER
# ROLE: TEST
# MAP_MODE: SUMMARY
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added adapter-level import-contract coverage for the gateway package and module surfaces after removing hidden path bootstrap logic.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   verify-explicit-import-contract - Confirm explicit worker-common import mode resolves both telegram_adapter.gateway and the package re-export without hidden shims.
# END_MODULE_MAP

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from telegram_adapter.gateway import TelegramApiProcessingGateway
from transcriber_workers_common.domain import SourceCandidate


class FakeGatewayApiClient:
    def __init__(self) -> None:
        self.batch_requests: list[dict[str, Any]] = []
        self.report_requests: list[str] = []
        self.deep_research_requests: list[str] = []
        self.downloads = {
            "memory://transcript.md": b"# Transcript\n",
            "memory://transcript.docx": b"transcript-docx",
            "memory://transcript.txt": b"Transcript text\n",
            "memory://report.md": b"# Report\n",
            "memory://report.docx": b"report-docx",
            "memory://deep-research.md": b"# Deep Research\n",
        }
        self.resolutions = {
            "transcript-md": self._resolution("transcript.md", "memory://transcript.md"),
            "transcript-docx": self._resolution("transcript.docx", "memory://transcript.docx"),
            "transcript-txt": self._resolution("transcript.txt", "memory://transcript.txt"),
            "report-md": self._resolution("report.md", "memory://report.md"),
            "report-docx": self._resolution("report.docx", "memory://report.docx"),
            "deep-research-md": self._resolution(
                "evidence-research-final-report.md",
                "memory://deep-research.md",
            ),
        }

    def create_batch(
        self,
        *,
        files,
        source_manifest: dict[str, Any],
        display_name: str | None = None,
        delivery_strategy: str,
    ) -> dict[str, Any]:
        self.batch_requests.append(
            {
                "files": files,
                "source_manifest": source_manifest,
                "display_name": display_name,
                "delivery_strategy": delivery_strategy,
            }
        )
        assert delivery_strategy == "polling"
        return {
            "job": {
                "job_id": "batch-root-1",
                "job_type": "transcription",
                "status": "queued",
            }
        }

    def create_report(self, job_id: str, *, delivery_strategy: str) -> dict[str, Any]:
        self.report_requests.append(job_id)
        assert delivery_strategy == "polling"
        return {
            "job": {
                "job_id": "agent-report-1",
                "job_type": "agent_run",
                "status": "queued",
            }
        }

    def create_deep_research(self, job_id: str, *, delivery_strategy: str) -> dict[str, Any]:
        self.deep_research_requests.append(job_id)
        assert delivery_strategy == "polling"
        return {
            "job": {
                "job_id": "agent-deep-research-1",
                "job_type": "agent_run",
                "status": "queued",
            }
        }

    def get_job(self, job_id: str) -> dict[str, Any]:
        return {"job": self._jobs()[job_id]}

    def resolve_artifact(self, artifact_id: str) -> dict[str, Any]:
        return self.resolutions[artifact_id]

    def download_bytes(self, url: str) -> bytes:
        return self.downloads[url]

    def _jobs(self) -> dict[str, dict[str, Any]]:
        return {
            "transcription-1": {
                "job_id": "transcription-1",
                "job_type": "transcription",
                "status": "succeeded",
                "display_name": "Telegram audio",
                "source_set": {
                    "items": [
                        {
                            "source": {
                                "source_id": "source-1",
                                "source_kind": "telegram_audio",
                                "display_name": "Telegram audio",
                                "original_filename": "clip.ogg",
                            }
                        }
                    ]
                },
                "artifacts": [
                    {"artifact_id": "transcript-md", "artifact_kind": "transcript_segmented_markdown"},
                    {"artifact_id": "transcript-docx", "artifact_kind": "transcript_docx"},
                    {"artifact_id": "transcript-txt", "artifact_kind": "transcript_plain"},
                ],
            },
            "batch-root-1": {
                "job_id": "batch-root-1",
                "root_job_id": "batch-root-1",
                "job_type": "transcription",
                "status": "succeeded",
                "display_name": "Telegram basket (2 sources)",
                "source_set": {
                    "items": [
                        {
                            "source": {
                                "source_id": "source-file",
                                "source_kind": "telegram_upload",
                                "display_name": "Audio: call.ogg",
                                "original_filename": "call.ogg",
                            },
                        },
                        {
                            "source": {
                                "source_id": "source-url",
                                "source_kind": "youtube_url",
                                "display_name": "YouTube: demo",
                            },
                        },
                    ]
                },
                "artifacts": [
                    {"artifact_id": "transcript-md", "artifact_kind": "transcript_segmented_markdown"},
                    {"artifact_id": "transcript-docx", "artifact_kind": "transcript_docx"},
                    {"artifact_id": "transcript-txt", "artifact_kind": "transcript_plain"},
                ],
            },
            "agent-report-1": {
                "job_id": "agent-report-1",
                "job_type": "agent_run",
                "status": "succeeded",
                "parent_job_id": "transcription-1",
                "artifacts": [
                    {"artifact_id": "report-md", "artifact_kind": "report_markdown"},
                    {"artifact_id": "report-docx", "artifact_kind": "report_docx"},
                ],
            },
            "agent-deep-research-1": {
                "job_id": "agent-deep-research-1",
                "job_type": "agent_run",
                "status": "succeeded",
                "parent_job_id": "agent-report-1",
                "artifacts": [
                    {"artifact_id": "deep-research-md", "artifact_kind": "deep_research_markdown"},
                ],
            },
        }

    def _resolution(self, filename: str, url: str) -> dict[str, Any]:
        return {
            "filename": filename,
            "download": {
                "url": url,
            },
        }


def test_gateway_package_reexport_matches_module_class() -> None:
    gateway_module = importlib.import_module("telegram_adapter.gateway")
    package_module = importlib.import_module("telegram_adapter")

    assert not hasattr(gateway_module, "_ensure_worker_common_path")
    assert (
        package_module.TelegramApiProcessingGateway
        is gateway_module.TelegramApiProcessingGateway
    )


def test_gateway_materializes_agent_run_report_and_preserves_returned_job_id(tmp_path) -> None:
    api_client = FakeGatewayApiClient()
    gateway = TelegramApiProcessingGateway(api_client, tmp_path, poll_interval_seconds=0)

    processed = gateway.ensure_report("transcription-1")

    assert api_client.report_requests == ["transcription-1"]
    assert processed.report is not None
    assert processed.report.job_id == "agent-report-1"
    assert processed.report.markdown_path.read_text(encoding="utf-8") == "# Report\n"
    assert processed.report.docx_path.read_bytes() == b"report-docx"


def test_gateway_submits_mixed_sources_to_batch_endpoint_and_polls_root(tmp_path: Path) -> None:
    media_path = tmp_path / "call.ogg"
    media_path.write_bytes(b"voice")
    api_client = FakeGatewayApiClient()
    gateway = TelegramApiProcessingGateway(api_client, tmp_path, poll_interval_seconds=0)

    processed = gateway.process_source_group(
        [
            SourceCandidate(
                source_id="telegram-message:42:call.ogg",
                kind="telegram_audio",
                display_name="Audio: call.ogg",
                url=None,
                telegram_file_id="file-1",
                mime_type="audio/ogg",
                file_name="call.ogg",
                local_path=media_path,
            ),
            SourceCandidate(
                source_id="url:https://youtu.be/demo",
                kind="youtube_url",
                display_name="YouTube: demo",
                url="https://youtu.be/demo",
                telegram_file_id=None,
                mime_type=None,
                file_name=None,
            ),
        ]
    )

    assert processed.job_id == "batch-root-1"
    assert len(api_client.batch_requests) == 1
    request = api_client.batch_requests[0]
    assert request["display_name"] == "Telegram basket (2 sources)"
    manifest = request["source_manifest"]
    assert manifest["manifest_version"] == "batch-transcription.v1"
    assert manifest["completion_policy"] == "succeed_when_all_sources_succeed"
    labels = manifest["ordered_source_labels"]
    assert labels[0].startswith("telegram-message_42_call_ogg_")
    assert labels[1].startswith("https_youtu_be_demo_")
    assert manifest["sources"][labels[0]]["source_kind"] == "telegram_upload"
    assert manifest["sources"][labels[0]]["file_part"] == labels[0]
    assert manifest["sources"][labels[1]]["url"] == "https://youtu.be/demo"
    assert request["files"][0].field_name == labels[0]
    assert request["files"][0].content_bytes == b"voice"


def test_gateway_creates_deep_research_from_agent_run_report_job(tmp_path) -> None:
    api_client = FakeGatewayApiClient()
    gateway = TelegramApiProcessingGateway(api_client, tmp_path, poll_interval_seconds=0)

    deep_research_path = gateway.ensure_deep_research("agent-report-1")

    assert api_client.deep_research_requests == ["agent-report-1"]
    assert deep_research_path.name == "evidence-research-final-report.md"
    assert deep_research_path.read_text(encoding="utf-8") == "# Deep Research\n"
