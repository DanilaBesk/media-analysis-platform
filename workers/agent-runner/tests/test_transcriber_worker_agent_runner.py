# FILE: workers/agent-runner/tests/test_transcriber_worker_agent_runner.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the generic agent-runner worker claims agent_run jobs and persists deterministic agent artifacts.
# SCOPE: Success finalization, unsupported harness failure, cancellation checkpoints, and provider-neutral registry dispatch.
# DEPENDS: M-WORKER-AGENT-RUNNER, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local coverage for the generic agent-runner worker runtime slice.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   RecordingApiClient - Captures worker-control calls without redefining DTO parsing.
#   InMemoryArtifactStore - Captures uploaded agent-runner artifacts.
#   FakeHarnessRegistry - Supplies deterministic injected harness results for success tests.
#   test_run_agent_harness_claims_dispatches_writes_artifacts_and_finalizes - Verifies the happy path.
#   test_run_agent_harness_unsupported_harness_fails_with_stable_error_code - Verifies deterministic unsupported harness failure.
#   test_run_agent_harness_cancels_before_dispatch - Verifies cancellation before expensive harness execution.
# END_MODULE_MAP

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

import transcriber_worker_agent_runner as agent_runner
from transcriber_workers_common.api import AgentRunRequestAccessResult, CancelCheckResult, ClaimedJobExecution
from transcriber_worker_agent_runner import (
    AgentClaudeCodeConfig,
    AgentClaudeCodeHarness,
    AgentHarnessConcurrencyUnavailable,
    AgentHarnessExecutionFailed,
    AgentHarnessLease,
    AgentHarnessRequest,
    AgentHarnessResult,
    AgentHarnessNotSupported,
    DefaultAgentHarnessRegistry,
    LocalAgentHarnessLeaseClient,
    LocalAgentHarnessRegistry,
    WorkerCancellationRequested,
    runAgentHarness,
)


class RecordingApiClient:
    def __init__(
        self,
        execution: ClaimedJobExecution,
        *,
        cancel_requested: bool = False,
        request_digest_sha256: str = "digest",
    ) -> None:
        self.execution = execution
        self.cancel_requested = cancel_requested
        self.request_digest_sha256 = request_digest_sha256
        self.calls: list[tuple[str, dict[str, object]]] = []

    def claim_job(self, job_id: str, *, worker_kind: str, task_type: str) -> ClaimedJobExecution:
        self.calls.append(("claim_job", {"job_id": job_id, "worker_kind": worker_kind, "task_type": task_type}))
        return self.execution

    def publish_progress(
        self,
        job_id: str,
        *,
        execution_id: str,
        progress_stage: str,
        progress_message: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "publish_progress",
                {
                    "job_id": job_id,
                    "execution_id": execution_id,
                    "progress_stage": progress_stage,
                    "progress_message": progress_message,
                },
            )
        )

    def register_artifacts(self, job_id: str, *, execution_id: str, artifacts) -> None:
        self.calls.append(
            (
                "register_artifacts",
                {
                    "job_id": job_id,
                    "execution_id": execution_id,
                    "artifacts": tuple(artifacts),
                },
            )
        )

    def finalize_job(
        self,
        job_id: str,
        *,
        execution_id: str,
        outcome: str,
        progress_stage: str | None = None,
        progress_message: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.calls.append(
            (
                "finalize_job",
                {
                    "job_id": job_id,
                    "execution_id": execution_id,
                    "outcome": outcome,
                    "progress_stage": progress_stage,
                    "progress_message": progress_message,
                    "error_code": error_code,
                    "error_message": error_message,
                },
            )
        )

    def check_cancel(self, job_id: str, *, execution_id: str) -> CancelCheckResult:
        self.calls.append(("check_cancel", {"job_id": job_id, "execution_id": execution_id}))
        if self.cancel_requested:
            return CancelCheckResult(cancel_requested=True, status="cancel_requested")
        return CancelCheckResult(cancel_requested=False, status="running")

    def resolve_agent_run_request_access(self, job_id: str, *, execution_id: str) -> AgentRunRequestAccessResult:
        self.calls.append(("resolve_agent_run_request_access", {"job_id": job_id, "execution_id": execution_id}))
        return AgentRunRequestAccessResult(
            provider="minio_presigned_url",
            url="https://minio.local/private/request.json",
            expires_at="2026-04-25T12:00:00Z",
            request_ref="agentreq_digest",
            request_digest_sha256=self.request_digest_sha256,
            request_bytes=123,
        )


class InMemoryArtifactStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_bytes(self, *, object_key: str, content: bytes, mime_type: str) -> None:
        self.calls.append({"object_key": object_key, "content": content, "mime_type": mime_type})


class FakeHarnessRegistry:
    def __init__(self) -> None:
        self.requests: list[tuple[str, AgentHarnessRequest]] = []
        self.leases: list[AgentHarnessLease] = []

    def run(
        self,
        harness_name: str,
        request: AgentHarnessRequest,
        *,
        lease: AgentHarnessLease | None = None,
        cancellation_hook=None,
    ) -> AgentHarnessResult:
        self.requests.append((harness_name, request))
        assert lease is not None
        self.leases.append(lease)
        assert cancellation_hook is not None
        assert cancellation_hook() is False
        return AgentHarnessResult(
            harness_name=harness_name,
            result_payload={"status": "ok", "echo": request.params["payload_hash"]},
            execution_log="fixture agent completed\n",
        )


class PassiveHarnessRegistry:
    def run(
        self,
        harness_name: str,
        request: AgentHarnessRequest,
        *,
        lease: AgentHarnessLease | None = None,
        cancellation_hook=None,
    ) -> AgentHarnessResult:
        return AgentHarnessResult(
            harness_name=harness_name,
            result_payload={"status": "ok"},
            execution_log="completed before upload\n",
        )


class FailingHarnessRegistry:
    def run(
        self,
        harness_name: str,
        request: AgentHarnessRequest,
        *,
        lease: AgentHarnessLease | None = None,
        cancellation_hook=None,
    ) -> AgentHarnessResult:
        raise RuntimeError("provider leaked token secret-token and prompt raw prompt")


class FakeClaudeCodeRunner:
    def __init__(self, *, stdout: str = "claude-code result\n") -> None:
        self.stdout = stdout
        self.calls: list[dict[str, object]] = []

    def __call__(self, command, *, cwd, env, input, text, capture_output, timeout):
        self.calls.append(
            {
                "command": tuple(command),
                "cwd": Path(cwd),
                "env": dict(env),
                "input": input,
                "text": text,
                "capture_output": capture_output,
                "timeout": timeout,
            }
        )
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=self.stdout,
            stderr="",
        )


class FakeHTTPResponse:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def read(self) -> bytes:
        return self.body


def _fake_request_access_envelope(envelope: dict[str, object]) -> tuple[bytes, str]:
    body = json.dumps(envelope, sort_keys=True).encode("utf-8")
    return body, hashlib.sha256(body).hexdigest()


def _request_url(value) -> str:
    return value.full_url if hasattr(value, "full_url") else str(value)


def test_run_agent_harness_claims_dispatches_writes_artifacts_and_finalizes(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "fixture", "payload_hash": "abc123"})
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()
    registry = FakeHarnessRegistry()

    result = runAgentHarness(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=artifact_store,
        harness_registry=registry,
    )

    assert api_client.calls[0] == (
        "claim_job",
        {"job_id": execution.job_id, "worker_kind": "agent_runner", "task_type": "agent_run.run"},
    )
    assert registry.requests == [
        (
            "fixture",
            AgentHarnessRequest(job_id=execution.job_id, workspace_dir=tmp_path / execution.job_id, params=execution.params),
        )
    ]
    assert registry.leases[0].harness_name == "fixture"
    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == ["agent_result_json", "execution_log"]
    assert artifact_store.calls[0]["object_key"] == "artifacts/job-agent/agent/result/result.json"
    assert json.loads(artifact_store.calls[0]["content"]) == {
        "echo": "abc123",
        "harness_name": "fixture",
        "status": "ok",
    }
    assert artifact_store.calls[1]["object_key"] == "artifacts/job-agent/logs/execution.log"
    assert api_client.calls[-1][0] == "finalize_job"
    assert api_client.calls[-1][1]["outcome"] == "succeeded"
    assert result.artifact_descriptors == register_call[1]["artifacts"]


def test_local_fixture_registry_supports_test_fixture_and_redacts_prompt_metadata(tmp_path: Path) -> None:
    registry = LocalAgentHarnessRegistry()
    request = AgentHarnessRequest(
        job_id="job-agent",
        workspace_dir=tmp_path / "job-agent",
        params={"harness_name": "test_fixture", "prompt": "raw secret prompt", "temperature": 0},
    )

    result = registry.run("test_fixture", request, cancellation_hook=lambda: False)

    assert result.harness_name == "test_fixture"
    assert result.result_payload["params_metadata"] == {
        "keys": ["harness_name", "prompt", "temperature"],
        "redacted_fields": ["prompt"],
        "value_types": {"harness_name": "str", "prompt": "str", "temperature": "int"},
    }
    serialized = json.dumps(result.result_payload, sort_keys=True)
    assert "raw secret prompt" not in serialized
    assert "raw secret prompt" not in result.execution_log


def test_run_agent_harness_unsupported_harness_fails_with_stable_error_code(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "unsupported"})
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()

    with pytest.raises(AgentHarnessNotSupported, match="unsupported"):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=artifact_store,
        )

    assert artifact_store.calls == []
    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "failed",
            "progress_stage": "failed",
            "progress_message": "Agent harness failed",
            "error_code": "agent_harness_not_supported",
            "error_message": "agent harness is not supported: unsupported",
        },
    )


def test_run_agent_harness_codex_is_not_supported_in_first_claude_code_iteration(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "codex"})
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()

    with pytest.raises(AgentHarnessNotSupported, match="codex"):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=artifact_store,
        )

    assert artifact_store.calls == []
    assert api_client.calls[-1][1]["error_code"] == "agent_harness_not_supported"


def test_run_agent_harness_claude_code_runs_container_local_with_private_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = json.dumps(
        {
            "schema_version": "agent_run_request_envelope/v1",
            "harness_name": "claude-code",
            "request": {"prompt": "raw secret prompt"},
        },
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(envelope).hexdigest()
    execution = _execution(
        {
            "harness_name": "claude-code",
            "request": {"prompt_sha256": "abc", "prompt_bytes": 19},
        }
    )
    api_client = RecordingApiClient(execution, request_digest_sha256=digest)
    artifact_store = InMemoryArtifactStore()
    runner = FakeClaudeCodeRunner()

    def fake_urlopen(url: str, timeout: float):
        assert url == "https://minio.local/private/request.json"
        assert timeout == 30.0
        return FakeHTTPResponse(envelope)

    monkeypatch.setattr(agent_runner.urlrequest, "urlopen", fake_urlopen)

    result = runAgentHarness(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=artifact_store,
        harness_registry=DefaultAgentHarnessRegistry(
            claude_code_harness=AgentClaudeCodeHarness(
                AgentClaudeCodeConfig(
                    provider_api_key="secret-token",
                    base_url="https://api.z.ai/api/anthropic",
                    model="glm-5",
                    config_dir=tmp_path / "claude-config",
                ),
                runner=runner,
            )
        ),
        lease_client=LocalAgentHarnessLeaseClient({"claude-code": 1}),
    )

    assert result.harness_result.harness_name == "claude-code"
    assert runner.calls[0]["input"] == "raw secret prompt"
    assert runner.calls[0]["command"][:2] == ("claude", "--print")
    assert runner.calls[0]["env"]["CLAUDE_CONFIG_DIR"] == str(tmp_path / "claude-config")
    settings = json.loads((tmp_path / "claude-config" / "settings.json").read_text(encoding="utf-8"))
    assert settings["env"] == {
        "ANTHROPIC_API_KEY": "secret-token",
        "ANTHROPIC_AUTH_TOKEN": "secret-token",
        "ANTHROPIC_BASE_URL": "https://api.z.ai/api/anthropic",
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": "glm-5",
        "ANTHROPIC_DEFAULT_OPUS_MODEL": "glm-5",
        "ANTHROPIC_DEFAULT_SONNET_MODEL": "glm-5",
        "ANTHROPIC_SMALL_FAST_MODEL": "glm-5",
        "API_TIMEOUT_MS": "3000000",
        "CLAUDE_API_BASE_URL": "https://api.z.ai/api/anthropic",
        "CLAUDE_API_KEY": "secret-token",
        "CLAUDE_CODE_SUBAGENT_MODEL": "glm-5",
    }
    result_payload = json.loads(artifact_store.calls[0]["content"])
    assert result_payload["output_text"] == "claude-code result\n"
    assert result_payload["request_ref"] == "agentreq_digest"
    assert api_client.calls[1] == ("resolve_agent_run_request_access", {"job_id": "job-agent", "execution_id": "exec-agent"})
    assert api_client.calls[-1][1]["outcome"] == "succeeded"


def test_run_agent_harness_claude_code_report_materializes_inputs_and_persists_operation_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id = "44444444-4444-4444-4444-444444444444"
    envelope_body, digest = _fake_request_access_envelope(
        {
            "schema_version": "agent_run_request_envelope/v1",
            "harness_name": "claude-code",
            "request": {
                "operation": "report",
                "expected_output_artifacts": ["report_markdown", "report_docx"],
                "prompt": "Build a report from the transcript.",
                "input_artifacts": [
                    {"artifact_id": artifact_id, "artifact_kind": "transcript_segmented_markdown", "filename": "transcript.md"}
                ],
            },
        }
    )
    execution = _execution({"harness_name": "claude-code", "request": {"operation": "report"}})
    api_client = RecordingApiClient(execution, request_digest_sha256=digest)
    artifact_store = InMemoryArtifactStore()
    runner = FakeClaudeCodeRunner(stdout="Вот исследовательский отчёт\n\n## Findings\n\nImportant result\n")
    monkeypatch.setenv("API_BASE_URL", "http://api.local")

    def fake_urlopen(value, timeout: float):
        url = _request_url(value)
        if url == "https://minio.local/private/request.json":
            return FakeHTTPResponse(envelope_body)
        if url == f"http://api.local/internal/v1/artifacts/{artifact_id}/download-access":
            return FakeHTTPResponse(
                json.dumps(
                    {
                        "artifact_id": artifact_id,
                        "job_id": "parent-job",
                        "artifact_kind": "transcript_segmented_markdown",
                        "filename": "transcript.md",
                        "mime_type": "text/markdown; charset=utf-8",
                        "size_bytes": 18,
                        "created_at": "2026-04-26T00:00:00Z",
                        "download": {
                            "provider": "minio_presigned_url",
                            "url": "https://minio.local/presigned/transcript.md",
                            "expires_at": "2026-04-26T01:00:00Z",
                        },
                    },
                    sort_keys=True,
                ).encode("utf-8")
            )
        if url == "https://minio.local/presigned/transcript.md":
            return FakeHTTPResponse(b"# Transcript\n\nSource text\n")
        raise AssertionError(f"unexpected urlopen URL: {url}")

    monkeypatch.setattr(agent_runner.urlrequest, "urlopen", fake_urlopen)

    result = runAgentHarness(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=artifact_store,
        harness_registry=DefaultAgentHarnessRegistry(
            claude_code_harness=AgentClaudeCodeHarness(
                AgentClaudeCodeConfig(provider_api_key="secret-token", config_dir=tmp_path / "claude-config"),
                runner=runner,
            )
        ),
        lease_client=LocalAgentHarnessLeaseClient({"claude-code": 1}),
    )

    prompt = runner.calls[0]["input"]
    local_artifact_path = tmp_path / execution.job_id / "input-artifacts" / "transcript.md"
    assert local_artifact_path.read_text(encoding="utf-8") == "# Transcript\n\nSource text\n"
    assert str(local_artifact_path) in prompt
    assert "Build a report from the transcript." in prompt
    assert "artifact_id" not in prompt
    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "agent_result_json",
        "execution_log",
        "report_markdown",
        "report_docx",
    ]
    report_markdown = next(call for call in artifact_store.calls if call["object_key"].endswith("/report/markdown/report.md"))
    assert report_markdown["content"].decode("utf-8") == "# Исследовательский отчёт\n\n## Findings\n\nImportant result\n"
    report_docx = next(call for call in artifact_store.calls if call["object_key"].endswith("/report/docx/report.docx"))
    assert report_docx["content"].startswith(b"PK")
    assert [artifact.artifact_kind for artifact in result.artifact_descriptors] == [
        "agent_result_json",
        "execution_log",
        "report_markdown",
        "report_docx",
    ]


def test_run_agent_harness_claude_code_deep_research_persists_requested_markdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope_body, digest = _fake_request_access_envelope(
        {
            "schema_version": "agent_run_request_envelope/v1",
            "harness_name": "claude-code",
            "request": {
                "prompt": "Research this topic.",
                "payload": {
                    "operation": "deep_research",
                    "expected_output_artifacts": ["deep_research_markdown"],
                },
            },
        }
    )
    execution = _execution(
        {
            "harness_name": "claude-code",
            "request": {
                "operation": "deep_research",
                "expected_output_artifacts": ["deep_research_markdown"],
            },
        }
    )
    api_client = RecordingApiClient(execution, request_digest_sha256=digest)
    artifact_store = InMemoryArtifactStore()
    runner = FakeClaudeCodeRunner(stdout="# Deep Research\n\nEvidence\n")

    monkeypatch.setattr(
        agent_runner.urlrequest,
        "urlopen",
        lambda url, timeout: FakeHTTPResponse(envelope_body),
    )

    runAgentHarness(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=artifact_store,
        harness_registry=DefaultAgentHarnessRegistry(
            claude_code_harness=AgentClaudeCodeHarness(
                AgentClaudeCodeConfig(provider_api_key="secret-token", config_dir=tmp_path / "claude-config"),
                runner=runner,
            )
        ),
        lease_client=LocalAgentHarnessLeaseClient({"claude-code": 1}),
    )

    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "agent_result_json",
        "execution_log",
        "deep_research_markdown",
    ]
    deep_research = next(
        call for call in artifact_store.calls if call["object_key"].endswith("/deep-research/markdown/deep-research.md")
    )
    assert deep_research["content"].decode("utf-8") == "# Deep Research\n\nEvidence\n"


def test_run_agent_harness_claude_code_deep_research_materializes_transcript_and_report_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transcript_artifact_id = "55555555-5555-5555-5555-555555555555"
    report_artifact_id = "66666666-6666-6666-6666-666666666666"
    envelope_body, digest = _fake_request_access_envelope(
        {
            "schema_version": "agent_run_request_envelope/v1",
            "harness_name": "claude-code",
            "request": {
                "prompt": "Build deep research from these private artifacts.",
                "payload": {
                    "operation": "deep_research",
                    "expected_output_artifacts": ["deep_research_markdown"],
                },
                "input_artifacts": [
                    {
                        "artifact_id": transcript_artifact_id,
                        "artifact_kind": "transcript_segmented_markdown",
                        "filename": "transcript.md",
                    },
                    {
                        "artifact_id": report_artifact_id,
                        "artifact_kind": "report_markdown",
                        "filename": "report.md",
                    },
                ],
            },
        }
    )
    execution = _execution(
        {
            "harness_name": "claude-code",
            "request": {
                "payload": {
                    "operation": "deep_research",
                    "expected_output_artifacts": ["deep_research_markdown"],
                }
            },
        }
    )
    api_client = RecordingApiClient(execution, request_digest_sha256=digest)
    artifact_store = InMemoryArtifactStore()
    runner = FakeClaudeCodeRunner(stdout="# Deep Research\n\nCross-artifact evidence\n")
    monkeypatch.setenv("API_BASE_URL", "http://api.local")

    def artifact_resolution(artifact_id: str, artifact_kind: str, filename: str, download_url: str) -> bytes:
        return json.dumps(
            {
                "artifact_id": artifact_id,
                "job_id": "source-job",
                "artifact_kind": artifact_kind,
                "filename": filename,
                "mime_type": "text/markdown; charset=utf-8",
                "size_bytes": 20,
                "created_at": "2026-04-26T00:00:00Z",
                "download": {
                    "provider": "minio_presigned_url",
                    "url": download_url,
                    "expires_at": "2026-04-26T01:00:00Z",
                },
            },
            sort_keys=True,
        ).encode("utf-8")

    def fake_urlopen(value, timeout: float):
        url = _request_url(value)
        if url == "https://minio.local/private/request.json":
            return FakeHTTPResponse(envelope_body)
        if url == f"http://api.local/internal/v1/artifacts/{transcript_artifact_id}/download-access":
            return FakeHTTPResponse(
                artifact_resolution(
                    transcript_artifact_id,
                    "transcript_segmented_markdown",
                    "transcript.md",
                    "https://minio.local/presigned/transcript.md",
                )
            )
        if url == f"http://api.local/internal/v1/artifacts/{report_artifact_id}/download-access":
            return FakeHTTPResponse(
                artifact_resolution(
                    report_artifact_id,
                    "report_markdown",
                    "report.md",
                    "https://minio.local/presigned/report.md",
                )
            )
        if url == "https://minio.local/presigned/transcript.md":
            return FakeHTTPResponse(b"# Transcript\n\nSource text\n")
        if url == "https://minio.local/presigned/report.md":
            return FakeHTTPResponse(b"# Report\n\nPrior findings\n")
        raise AssertionError(f"unexpected urlopen URL: {url}")

    monkeypatch.setattr(agent_runner.urlrequest, "urlopen", fake_urlopen)

    runAgentHarness(
        execution.job_id,
        workspace_root=tmp_path,
        api_client=api_client,
        artifact_store=artifact_store,
        harness_registry=DefaultAgentHarnessRegistry(
            claude_code_harness=AgentClaudeCodeHarness(
                AgentClaudeCodeConfig(provider_api_key="secret-token", config_dir=tmp_path / "claude-config"),
                runner=runner,
            )
        ),
        lease_client=LocalAgentHarnessLeaseClient({"claude-code": 1}),
    )

    workspace_dir = tmp_path / execution.job_id
    assert (workspace_dir / "input-artifacts" / "transcript.md").read_text(encoding="utf-8") == "# Transcript\n\nSource text\n"
    assert (workspace_dir / "input-artifacts" / "report.md").read_text(encoding="utf-8") == "# Report\n\nPrior findings\n"
    prompt = runner.calls[0]["input"]
    assert str(workspace_dir / "input-artifacts" / "transcript.md") in prompt
    assert str(workspace_dir / "input-artifacts" / "report.md") in prompt
    assert "artifact_id" not in prompt
    register_call = next(call for call in api_client.calls if call[0] == "register_artifacts")
    assert [artifact.artifact_kind for artifact in register_call[1]["artifacts"]] == [
        "agent_result_json",
        "execution_log",
        "deep_research_markdown",
    ]
    result_payload = json.loads(artifact_store.calls[0]["content"])
    assert result_payload["materialized_input_artifacts"] == [
        {
            "artifact_kind": "transcript_segmented_markdown",
            "filename": "transcript.md",
            "local_path": str(workspace_dir / "input-artifacts" / "transcript.md"),
            "mime_type": "text/markdown; charset=utf-8",
            "position": 0,
            "size_bytes": len(b"# Transcript\n\nSource text\n"),
        },
        {
            "artifact_kind": "report_markdown",
            "filename": "report.md",
            "local_path": str(workspace_dir / "input-artifacts" / "report.md"),
            "mime_type": "text/markdown; charset=utf-8",
            "position": 1,
            "size_bytes": len(b"# Report\n\nPrior findings\n"),
        },
    ]
    deep_research = next(
        call for call in artifact_store.calls if call["object_key"].endswith("/deep-research/markdown/deep-research.md")
    )
    assert deep_research["content"].decode("utf-8") == "# Deep Research\n\nCross-artifact evidence\n"


def test_run_agent_harness_claude_code_materialization_failure_is_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id = "44444444-4444-4444-4444-444444444444"
    envelope_body, digest = _fake_request_access_envelope(
        {
            "schema_version": "agent_run_request_envelope/v1",
            "harness_name": "claude-code",
            "operation": "report",
            "request": {
                "prompt": "raw secret prompt",
                "input_artifacts": [{"artifact_id": artifact_id, "filename": "transcript.md"}],
            },
        }
    )
    execution = _execution({"harness_name": "claude-code"})
    api_client = RecordingApiClient(execution, request_digest_sha256=digest)
    artifact_store = InMemoryArtifactStore()
    monkeypatch.setenv("API_BASE_URL", "http://api.local")

    def fake_urlopen(value, timeout: float):
        url = _request_url(value)
        if url == "https://minio.local/private/request.json":
            return FakeHTTPResponse(envelope_body)
        raise agent_runner.error.URLError("token secret-token prompt raw secret prompt")

    monkeypatch.setattr(agent_runner.urlrequest, "urlopen", fake_urlopen)

    with pytest.raises(AgentHarnessExecutionFailed):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=artifact_store,
            harness_registry=DefaultAgentHarnessRegistry(
                claude_code_harness=AgentClaudeCodeHarness(
                    AgentClaudeCodeConfig(provider_api_key="secret-token", config_dir=tmp_path / "claude-config"),
                    runner=FakeClaudeCodeRunner(),
                )
            ),
            lease_client=LocalAgentHarnessLeaseClient({"claude-code": 1}),
        )

    assert artifact_store.calls == []
    final_call = api_client.calls[-1][1]
    assert final_call["error_code"] == "agent_harness_execution_failed"
    assert final_call["error_message"] == "agent harness failure details were redacted"


def test_claude_code_config_from_env_writes_zai_settings(tmp_path: Path) -> None:
    config = AgentClaudeCodeConfig.from_env(
        {
            "AGENT_RUNNER_CLAUDE_CODE_AUTH_TOKEN": "secret-token",
            "AGENT_RUNNER_CLAUDE_CODE_BASE_URL": "https://api.z.ai/api/anthropic",
            "AGENT_RUNNER_CLAUDE_CODE_API_TIMEOUT_MS": "3000000",
            "AGENT_RUNNER_CLAUDE_CODE_MODEL": "glm-5",
            "AGENT_RUNNER_CLAUDE_CODE_CONFIG_DIR": str(tmp_path / "config"),
        }
    )

    assert config is not None
    settings_path = config.write_settings()

    settings = json.loads(settings_path.read_text(encoding="utf-8"))
    assert settings["env"]["ANTHROPIC_API_KEY"] == "secret-token"
    assert settings["env"]["ANTHROPIC_AUTH_TOKEN"] == "secret-token"
    assert settings["env"]["ANTHROPIC_BASE_URL"] == "https://api.z.ai/api/anthropic"
    assert settings["env"]["ANTHROPIC_DEFAULT_OPUS_MODEL"] == "glm-5"
    assert settings["env"]["ANTHROPIC_DEFAULT_SONNET_MODEL"] == "glm-5"
    assert settings["env"]["ANTHROPIC_DEFAULT_HAIKU_MODEL"] == "glm-5"
    assert settings["env"]["ANTHROPIC_SMALL_FAST_MODEL"] == "glm-5"
    assert settings["env"]["CLAUDE_API_KEY"] == "secret-token"
    assert settings["env"]["CLAUDE_API_BASE_URL"] == "https://api.z.ai/api/anthropic"
    assert settings["env"]["CLAUDE_CODE_SUBAGENT_MODEL"] == "glm-5"
    assert settings["env"]["API_TIMEOUT_MS"] == "3000000"
    assert settings["model"] == "glm-5"
    assert (tmp_path / "config" / ".credentials.json").read_text(encoding="utf-8") == "{}\n"
    assert (tmp_path / "config").stat().st_mode & 0o777 == 0o700
    assert settings_path.stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "config" / ".credentials.json").stat().st_mode & 0o777 == 0o600


def test_run_agent_harness_enforces_per_harness_concurrency(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "fixture", "payload_hash": "abc123"})
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()
    lease_client = LocalAgentHarnessLeaseClient({"fixture": 1})
    held_lease = lease_client.acquire(
        "fixture",
        AgentHarnessRequest(job_id="already-running", workspace_dir=tmp_path / "held", params={"harness_name": "fixture"}),
    )

    try:
        with pytest.raises(AgentHarnessConcurrencyUnavailable, match="fixture"):
            runAgentHarness(
                execution.job_id,
                workspace_root=tmp_path,
                api_client=api_client,
                artifact_store=artifact_store,
                harness_registry=FakeHarnessRegistry(),
                lease_client=lease_client,
            )
    finally:
        lease_client.release(held_lease)

    assert artifact_store.calls == []
    assert api_client.calls[-1][1]["error_code"] == "agent_harness_concurrency_unavailable"


def test_run_agent_harness_releases_lease_after_success(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "fixture", "payload_hash": "abc123"})
    lease_client = LocalAgentHarnessLeaseClient({"fixture": 1})

    for _ in range(2):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=RecordingApiClient(execution),
            artifact_store=InMemoryArtifactStore(),
            harness_registry=FakeHarnessRegistry(),
            lease_client=lease_client,
        )


def test_run_agent_harness_redacts_sensitive_generic_error_message(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "fixture"})
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()

    with pytest.raises(RuntimeError, match="secret-token"):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=artifact_store,
            harness_registry=FailingHarnessRegistry(),
        )

    final_call = api_client.calls[-1][1]
    assert final_call["error_code"] == "agent_harness_failed"
    assert final_call["error_message"] == "agent harness failure details were redacted"


def test_run_agent_harness_redacts_sensitive_claude_code_failure_message(tmp_path: Path) -> None:
    class FailingClaudeCodeHarness:
        def run(self, request: AgentHarnessRequest, *, cancellation_hook=None) -> AgentHarnessResult:
            raise AgentHarnessExecutionFailed("prompt raw secret prompt token secret-token")

    execution = _execution({"harness_name": "claude-code"})
    api_client = RecordingApiClient(execution)

    with pytest.raises(AgentHarnessExecutionFailed):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=InMemoryArtifactStore(),
            harness_registry=DefaultAgentHarnessRegistry(claude_code_harness=FailingClaudeCodeHarness()),
            lease_client=LocalAgentHarnessLeaseClient({"claude-code": 1}),
        )

    final_call = api_client.calls[-1][1]
    assert final_call["error_code"] == "agent_harness_execution_failed"
    assert final_call["error_message"] == "agent harness failure details were redacted"


def test_run_agent_harness_cancels_before_dispatch(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "fixture"})
    api_client = RecordingApiClient(execution, cancel_requested=True)
    artifact_store = InMemoryArtifactStore()
    registry = FakeHarnessRegistry()

    with pytest.raises(WorkerCancellationRequested, match="was canceled"):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=artifact_store,
            harness_registry=registry,
        )

    assert registry.requests == []
    assert artifact_store.calls == []
    assert not any(call[0] == "register_artifacts" for call in api_client.calls)
    assert api_client.calls[-1] == (
        "finalize_job",
        {
            "job_id": execution.job_id,
            "execution_id": execution.execution_id,
            "outcome": "canceled",
            "progress_stage": "canceled",
            "progress_message": "Cancellation requested",
            "error_code": None,
            "error_message": None,
        },
    )


def test_run_agent_harness_cancels_before_artifact_upload(tmp_path: Path) -> None:
    execution = _execution({"harness_name": "fixture"})
    api_client = RecordingApiClient(execution)
    artifact_store = InMemoryArtifactStore()
    check_count = 0

    def delayed_cancel(job_id: str, *, execution_id: str) -> CancelCheckResult:
        nonlocal check_count
        check_count += 1
        api_client.calls.append(("check_cancel", {"job_id": job_id, "execution_id": execution_id}))
        return CancelCheckResult(cancel_requested=check_count >= 2, status="cancel_requested")

    api_client.check_cancel = delayed_cancel

    with pytest.raises(WorkerCancellationRequested, match="was canceled"):
        runAgentHarness(
            execution.job_id,
            workspace_root=tmp_path,
            api_client=api_client,
            artifact_store=artifact_store,
            harness_registry=PassiveHarnessRegistry(),
        )

    assert artifact_store.calls == []
    assert not any(call[0] == "register_artifacts" for call in api_client.calls)
    assert api_client.calls[-1][0] == "finalize_job"
    assert api_client.calls[-1][1]["outcome"] == "canceled"


def _execution(params: dict[str, object]) -> ClaimedJobExecution:
    return ClaimedJobExecution(
        execution_id="exec-agent",
        job_id="job-agent",
        root_job_id="job-agent",
        parent_job_id=None,
        retry_of_job_id=None,
        job_type="agent_run",
        version=1,
        ordered_inputs=(),
        params=params,
    )
