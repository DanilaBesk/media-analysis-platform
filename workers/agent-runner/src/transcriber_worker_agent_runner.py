# FILE: workers/agent-runner/src/transcriber_worker_agent_runner.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Execute generic agent_run jobs through a provider-neutral deterministic harness registry.
# SCOPE: Worker claim/run orchestration, harness_name dispatch, cancellation checkpoints, artifact writes, and finalize calls.
# DEPENDS: M-WORKER-AGENT-RUNNER, M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the generic agent-runner worker runtime slice with deterministic local harness dispatch.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   AgentHarnessRequest - Carries one provider-neutral agent harness invocation.
#   AgentHarnessResult - Carries bounded result payload and execution log text.
#   AgentHarnessLease - Carries one worker-local per-harness dispatch lease.
#   AgentClaudeCodeConfig - Carries container-local Claude Code settings for Anthropic-compatible providers.
#   AgentClaudeCodeHarness - Runs Claude Code inside the worker container with generated settings JSON.
#   DefaultAgentHarnessRegistry - Routes local fixtures and container-local Claude Code through one generic registry.
#   LocalAgentHarnessLeaseClient - Enforces worker-local per-harness concurrency limits.
#   LocalAgentHarnessRegistry - Provides deterministic fixture/local harness dispatch only.
#   AgentRunnerWorkerResult - Returns successful worker execution evidence for tests.
#   WorkerCancellationRequested - Signals authoritative cancellation from worker-control.
#   AgentHarnessNotSupported - Signals stable unsupported harness failures.
#   AgentHarnessConcurrencyUnavailable - Signals per-harness lease saturation.
#   AgentHarnessExecutionFailed - Signals provider-reported harness execution failure.
#   runAgentHarness - Claims an agent_run job, dispatches harness_name, persists artifacts, and finalizes.
# END_MODULE_MAP

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol
from urllib import error, request as urlrequest

from transcriber_workers_common.api import AgentRunRequestAccessResult, ClaimedJobExecution, JobApiClient
from transcriber_workers_common.artifacts import ArtifactDescriptor, ArtifactObjectStore, ArtifactWriter
from transcriber_workers_common.documents import normalize_report_markdown, write_report_docx


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_RUN_AGENT_HARNESS = "[WorkerAgentRunner][runAgentHarness][BLOCK_EXECUTE_AGENT_HARNESS]"
_LOG_MARKER_DISPATCH_AGENT_HARNESS = "[AgentRunner][dispatchHarness][BLOCK_DISPATCH_AGENT_HARNESS]"
_UNSUPPORTED_HARNESS_CODE = "agent_harness_not_supported"
_CONCURRENCY_UNAVAILABLE_CODE = "agent_harness_concurrency_unavailable"
_HARNESS_EXECUTION_FAILED_CODE = "agent_harness_execution_failed"
_GENERIC_FAILURE_CODE = "agent_harness_failed"
_REQUEST_ACCESS_HARNESSES = frozenset({"claude-code"})
_LOCAL_FIXTURE_HARNESSES = frozenset({"fixture", "test_fixture"})
_REDACTED_ERROR_MESSAGE = "agent harness failure details were redacted"
_DEFAULT_CLAUDE_CODE_BASE_URL = "https://api.z.ai/api/anthropic"
_DEFAULT_CLAUDE_CODE_API_TIMEOUT_MS = 3_000_000
_DEFAULT_CLAUDE_CODE_MODEL = "glm-5"
_DEFAULT_CLAUDE_CODE_CONFIG_DIR = "/tmp/runtime/agent-runner/claude-code"
_ARTIFACT_RESOLVE_TIMEOUT_SECONDS = 30.0
_ARTIFACT_DOWNLOAD_TIMEOUT_SECONDS = 60.0
_EXPECTED_OPERATION_ARTIFACTS = frozenset({"report_markdown", "report_docx", "deep_research_markdown"})

__all__ = [
    "AgentHarnessNotSupported",
    "AgentHarnessRequest",
    "AgentHarnessResult",
    "AgentHarnessConcurrencyUnavailable",
    "AgentHarnessExecutionFailed",
    "AgentHarnessLease",
    "AgentClaudeCodeConfig",
    "AgentClaudeCodeHarness",
    "AgentRunnerWorkerResult",
    "DefaultAgentHarnessRegistry",
    "LocalAgentHarnessLeaseClient",
    "LocalAgentHarnessRegistry",
    "WorkerCancellationRequested",
    "runAgentHarness",
]


class AgentHarnessRegistry(Protocol):
    def run(
        self,
        harness_name: str,
        request: "AgentHarnessRequest",
        *,
        lease: "AgentHarnessLease | None" = None,
        cancellation_hook=None,
    ) -> "AgentHarnessResult": ...


class AgentHarnessLeaseClient(Protocol):
    def acquire(self, harness_name: str, request: "AgentHarnessRequest") -> "AgentHarnessLease": ...

    def release(self, lease: "AgentHarnessLease") -> None: ...


# START_CONTRACT: AgentHarnessRequest
# PURPOSE: Carry one provider-neutral agent-runner invocation.
# INPUTS: { job_id: str, workspace_dir: Path, params: Mapping[str, object] }
# OUTPUTS: { AgentHarnessRequest - Immutable request consumed by the harness registry }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-AGENT-RUNNER, M-CONTRACTS
# END_CONTRACT: AgentHarnessRequest
@dataclass(frozen=True, slots=True)
class AgentHarnessRequest:
    job_id: str
    workspace_dir: Path
    params: Mapping[str, object]
    request_access: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if not self.job_id.strip():
            raise ValueError("job_id must not be empty")


# START_CONTRACT: AgentHarnessResult
# PURPOSE: Carry a bounded provider-neutral agent result for artifact persistence.
# INPUTS: { harness_name: str, result_payload: Mapping[str, object], execution_log: str }
# OUTPUTS: { AgentHarnessResult - Immutable normalized harness result }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-AGENT-RUNNER, M-CONTRACTS
# END_CONTRACT: AgentHarnessResult
@dataclass(frozen=True, slots=True)
class AgentHarnessResult:
    harness_name: str
    result_payload: Mapping[str, object]
    execution_log: str

    def __post_init__(self) -> None:
        if not self.harness_name.strip():
            raise ValueError("harness_name must not be empty")


@dataclass(frozen=True, slots=True)
class AgentHarnessLease:
    harness_name: str
    lease_id: str
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.harness_name.strip():
            raise ValueError("harness_name must not be empty")
        if not self.lease_id.strip():
            raise ValueError("lease_id must not be empty")


@dataclass(frozen=True, slots=True)
class AgentRunnerWorkerResult:
    execution: ClaimedJobExecution
    harness_result: AgentHarnessResult
    artifact_descriptors: tuple[ArtifactDescriptor, ...]


class WorkerCancellationRequested(RuntimeError):
    pass


class AgentHarnessNotSupported(RuntimeError):
    error_code = _UNSUPPORTED_HARNESS_CODE


class AgentHarnessConcurrencyUnavailable(RuntimeError):
    error_code = _CONCURRENCY_UNAVAILABLE_CODE


class AgentHarnessExecutionFailed(RuntimeError):
    error_code = _HARNESS_EXECUTION_FAILED_CODE


@dataclass(frozen=True, slots=True)
class _AgentRunPromptContext:
    prompt: str
    operation: str | None
    expected_output_artifacts: tuple[str, ...]
    materialized_input_artifacts: tuple[Mapping[str, object], ...]


class LocalAgentHarnessLeaseClient:
    def __init__(self, policy: Mapping[str, int] | None = None) -> None:
        self._policy = {_canonical_harness_name(key): int(value) for key, value in (policy or {}).items()}
        for harness_name, limit in self._policy.items():
            if limit < 1:
                raise ValueError(f"concurrency limit for {harness_name} must be positive")
        self._active: dict[str, int] = {}
        self._sequence = 0
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "LocalAgentHarnessLeaseClient":
        values = os.environ if env is None else env
        return cls(_parse_concurrency_policy(values.get("AGENT_RUNNER_HARNESS_CONCURRENCY")))

    # START_CONTRACT: reserveHarnessLease
    # PURPOSE: Enforce per-harness worker-local admission before dispatching an agent harness.
    # INPUTS: { harness_name: str, request: AgentHarnessRequest }
    # OUTPUTS: { AgentHarnessLease - Lease released after dispatch completes }
    # SIDE_EFFECTS: in-process active lease counters
    # LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
    # END_CONTRACT: reserveHarnessLease
    def acquire(self, harness_name: str, request: AgentHarnessRequest) -> AgentHarnessLease:
        canonical = _canonical_harness_name(harness_name)
        limit = self._policy.get(canonical, self._policy.get("*", 1))
        with self._lock:
            active = self._active.get(canonical, 0)
            if active >= limit:
                raise AgentHarnessConcurrencyUnavailable(f"agent harness concurrency unavailable: {canonical}")
            self._sequence += 1
            self._active[canonical] = active + 1
            return AgentHarnessLease(
                harness_name=canonical,
                lease_id=f"local-{canonical}-{self._sequence}",
                metadata={"policy_limit": limit, "job_id": request.job_id},
            )

    def release(self, lease: AgentHarnessLease) -> None:
        canonical = _canonical_harness_name(lease.harness_name)
        with self._lock:
            active = self._active.get(canonical, 0)
            if active <= 1:
                self._active.pop(canonical, None)
                return
            self._active[canonical] = active - 1


@dataclass(frozen=True, slots=True)
class AgentClaudeCodeConfig:
    provider_api_key: str
    base_url: str = _DEFAULT_CLAUDE_CODE_BASE_URL
    api_timeout_ms: int = _DEFAULT_CLAUDE_CODE_API_TIMEOUT_MS
    model: str = _DEFAULT_CLAUDE_CODE_MODEL
    config_dir: Path = Path(_DEFAULT_CLAUDE_CODE_CONFIG_DIR)
    run_timeout_seconds: float = 3600.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "AgentClaudeCodeConfig | None":
        values = os.environ if env is None else env
        provider_api_key = _read_secret(
            values,
            direct_key="AGENT_RUNNER_CLAUDE_CODE_PROVIDER_API_KEY",
            file_key="AGENT_RUNNER_CLAUDE_CODE_PROVIDER_API_KEY_FILE",
        ) or _read_secret(
            values,
            direct_key="AGENT_RUNNER_CLAUDE_CODE_AUTH_TOKEN",
            file_key="AGENT_RUNNER_CLAUDE_CODE_AUTH_TOKEN_FILE",
        )
        if not provider_api_key:
            return None
        return cls(
            provider_api_key=provider_api_key,
            base_url=_optional_str(values.get("AGENT_RUNNER_CLAUDE_CODE_BASE_URL")) or _DEFAULT_CLAUDE_CODE_BASE_URL,
            api_timeout_ms=_parse_positive_int(
                values.get("AGENT_RUNNER_CLAUDE_CODE_API_TIMEOUT_MS"),
                _DEFAULT_CLAUDE_CODE_API_TIMEOUT_MS,
            ),
            model=_optional_str(values.get("AGENT_RUNNER_CLAUDE_CODE_MODEL")) or _DEFAULT_CLAUDE_CODE_MODEL,
            config_dir=Path(
                _optional_str(values.get("AGENT_RUNNER_CLAUDE_CODE_CONFIG_DIR")) or _DEFAULT_CLAUDE_CODE_CONFIG_DIR
            ),
            run_timeout_seconds=_parse_positive_float(values.get("AGENT_RUNNER_CLAUDE_CODE_RUN_TIMEOUT_SECONDS"), 3600.0),
        )

    def redaction_values(self) -> tuple[str, ...]:
        return (self.provider_api_key,)

    def provider_env(self) -> dict[str, str]:
        return {
            "ANTHROPIC_API_KEY": self.provider_api_key,
            "ANTHROPIC_AUTH_TOKEN": self.provider_api_key,
            "ANTHROPIC_BASE_URL": self.base_url,
            "ANTHROPIC_DEFAULT_OPUS_MODEL": self.model,
            "ANTHROPIC_DEFAULT_SONNET_MODEL": self.model,
            "ANTHROPIC_DEFAULT_HAIKU_MODEL": self.model,
            "ANTHROPIC_SMALL_FAST_MODEL": self.model,
            "CLAUDE_API_KEY": self.provider_api_key,
            "CLAUDE_API_BASE_URL": self.base_url,
            "CLAUDE_CODE_SUBAGENT_MODEL": self.model,
            "API_TIMEOUT_MS": str(self.api_timeout_ms),
        }

    def write_settings(self) -> Path:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.chmod(0o700)
        settings_path = self.config_dir / "settings.json"
        credentials_path = self.config_dir / ".credentials.json"
        settings = {
            "env": self.provider_env(),
            "model": self.model,
            "permissions": {"allow": ["Bash(*)", "Read(*)", "Write(*)", "Edit(*)"]},
            "skipDangerousModePermissionPrompt": True,
        }
        settings_path.write_text(json.dumps(settings, indent=2, sort_keys=True), encoding="utf-8")
        settings_path.chmod(0o600)
        if not credentials_path.exists():
            credentials_path.write_text("{}\n", encoding="utf-8")
        credentials_path.chmod(0o600)
        return settings_path


class AgentClaudeCodeHarness:
    def __init__(self, config: AgentClaudeCodeConfig, *, runner=None) -> None:
        self.config = config
        self._runner = runner or subprocess.run

    # START_CONTRACT: runClaudeCodeHarness
    # PURPOSE: Execute Claude Code inside the worker container using generated Anthropic-compatible provider settings.
    # INPUTS: { request: AgentHarnessRequest with request_access, config: AgentClaudeCodeConfig }
    # OUTPUTS: { AgentHarnessResult - Claude Code stdout and execution metadata }
    # SIDE_EFFECTS: writes CLAUDE_CONFIG_DIR/settings.json and runs the container-local `claude` executable
    # LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
    # END_CONTRACT: runClaudeCodeHarness
    def run(self, request: AgentHarnessRequest, *, cancellation_hook=None) -> AgentHarnessResult:
        if cancellation_hook is not None and cancellation_hook():
            raise WorkerCancellationRequested(f"job {request.job_id} was canceled")
        if not request.request_access:
            raise AgentHarnessExecutionFailed("claude-code request access is missing")

        request.workspace_dir.mkdir(parents=True, exist_ok=True)
        prompt_context = _load_agent_run_prompt_context(
            request.request_access,
            workspace_dir=request.workspace_dir,
            request_params=request.params,
            redaction_values=self.config.redaction_values(),
        )
        settings_path = self.config.write_settings()
        env = {
            **os.environ,
            "CLAUDE_CONFIG_DIR": str(self.config.config_dir),
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
            "CLAUDE_DISABLE_UPDATE_CHECK": "1",
            **self.config.provider_env(),
        }
        command = (
            "claude",
            "--print",
            "--input-format",
            "text",
            "--settings",
            str(settings_path),
            "--model",
            self.config.model,
        )
        try:
            completed = self._runner(
                command,
                cwd=request.workspace_dir,
                env=env,
                input=prompt_context.prompt,
                text=True,
                capture_output=True,
                timeout=self.config.run_timeout_seconds,
            )
        except FileNotFoundError as exc:
            raise AgentHarnessExecutionFailed("claude-code executable is unavailable in the worker container") from exc
        except subprocess.TimeoutExpired as exc:
            stdout = _redact_text(exc.stdout or "", self.config.redaction_values())
            stderr = _redact_text(exc.stderr or "", self.config.redaction_values())
            raise AgentHarnessExecutionFailed(
                f"claude-code timed out after {self.config.run_timeout_seconds:g}s\nstdout={stdout}\nstderr={stderr}"
            ) from exc

        stdout = _redact_text(completed.stdout or "", self.config.redaction_values())
        stderr = _redact_text(completed.stderr or "", self.config.redaction_values())
        if completed.returncode != 0:
            raise AgentHarnessExecutionFailed(
                f"claude-code exited with status {completed.returncode}\nstdout={stdout}\nstderr={stderr}"
            )
        request_ref = str(request.request_access.get("request_ref") or "")
        return AgentHarnessResult(
            harness_name="claude-code",
            result_payload={
                "status": "ok",
                "harness_name": "claude-code",
                "provider_mode": "anthropic-compatible-settings",
                "model": self.config.model,
                "request_ref": request_ref,
                "output_text": stdout,
                "operation": prompt_context.operation,
                "expected_output_artifacts": list(prompt_context.expected_output_artifacts),
                "materialized_input_artifacts": list(prompt_context.materialized_input_artifacts),
            },
            execution_log="\n".join(
                [
                    "claude-code container-local execution",
                    f"model={self.config.model}",
                    f"base_url={self.config.base_url}",
                    f"request_ref={request_ref}",
                    "stdout:",
                    stdout,
                    "stderr:",
                    stderr,
                    "",
                ]
            ),
        )


class DefaultAgentHarnessRegistry:
    def __init__(self, *, claude_code_harness: AgentClaudeCodeHarness | None = None) -> None:
        self._local = LocalAgentHarnessRegistry()
        self._claude_code_harness = claude_code_harness

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "DefaultAgentHarnessRegistry":
        config = AgentClaudeCodeConfig.from_env(env)
        return cls(claude_code_harness=AgentClaudeCodeHarness(config) if config is not None else None)

    def run(
        self,
        harness_name: str,
        request: AgentHarnessRequest,
        *,
        lease: AgentHarnessLease | None = None,
        cancellation_hook=None,
    ) -> AgentHarnessResult:
        canonical = _canonical_harness_name(harness_name)
        if canonical in _LOCAL_FIXTURE_HARNESSES:
            return self._local.run(canonical, request, lease=lease, cancellation_hook=cancellation_hook)
        if canonical == "claude-code":
            if self._claude_code_harness is None:
                raise AgentHarnessExecutionFailed("claude-code provider settings are not configured")
            return self._claude_code_harness.run(request, cancellation_hook=cancellation_hook)
        raise AgentHarnessNotSupported(f"agent harness is not supported: {harness_name}")


# START_CONTRACT: LocalAgentHarnessRegistry
# PURPOSE: Dispatch only deterministic local fixture agent harnesses for the first runtime slice.
# INPUTS: { harness_name: str, request: AgentHarnessRequest, cancellation_hook: callable | None }
# OUTPUTS: { AgentHarnessResult - Deterministic local result }
# SIDE_EFFECTS: workspace directory creation
# LINKS: M-WORKER-AGENT-RUNNER, V-M-WORKER-AGENT-RUNNER
# END_CONTRACT: LocalAgentHarnessRegistry
class LocalAgentHarnessRegistry:
    _SUPPORTED = frozenset({"fixture", "test_fixture"})

    def run(
        self,
        harness_name: str,
        request: AgentHarnessRequest,
        *,
        lease: AgentHarnessLease | None = None,
        cancellation_hook=None,
    ) -> AgentHarnessResult:
        # START_BLOCK_BLOCK_RUN_LOCAL_AGENT_HARNESS
        normalized = _canonical_harness_name(harness_name)
        if normalized not in self._SUPPORTED:
            raise AgentHarnessNotSupported(f"agent harness is not supported: {harness_name}")
        if cancellation_hook is not None and cancellation_hook():
            raise WorkerCancellationRequested(f"job {request.job_id} was canceled")

        request.workspace_dir.mkdir(parents=True, exist_ok=True)
        params_metadata = _build_redacted_params_metadata(request.params)
        metadata_json = json.dumps(params_metadata, sort_keys=True, separators=(",", ":"), default=str)
        metadata_digest = hashlib.sha256(metadata_json.encode("utf-8")).hexdigest()[:16]
        return AgentHarnessResult(
            harness_name=normalized,
            result_payload={
                "status": "ok",
                "harness_name": normalized,
                "job_id": request.job_id,
                "params_metadata": params_metadata,
                "params_metadata_digest": metadata_digest,
            },
            execution_log="\n".join(
                [
                    "agent-runner fixture execution",
                    f"job_id={request.job_id}",
                    f"harness_name={normalized}",
                    f"params_metadata_digest={metadata_digest}",
                    "",
                ]
            ),
        )
        # END_BLOCK_BLOCK_RUN_LOCAL_AGENT_HARNESS


# START_CONTRACT: runAgentHarness
# PURPOSE: Claim and execute one generic agent_run job through the shared control plane.
# INPUTS: { job_id: str, workspace_root: Path, api_client: JobApiClient, artifact_store: ArtifactObjectStore, harness_registry: AgentHarnessRegistry | None }
# OUTPUTS: { AgentRunnerWorkerResult - Successful execution evidence }
# SIDE_EFFECTS: API claim/progress/artifact/finalize calls and object-store writes
# LINKS: M-WORKER-AGENT-RUNNER, M-WORKER-COMMON, M-CONTRACTS, DF-001, DF-007
# END_CONTRACT: runAgentHarness
def runAgentHarness(
    job_id: str,
    *,
    workspace_root: Path,
    api_client: JobApiClient,
    artifact_store: ArtifactObjectStore,
    harness_registry: AgentHarnessRegistry | None = None,
    lease_client: AgentHarnessLeaseClient | None = None,
) -> AgentRunnerWorkerResult:
    execution = api_client.claim_job(job_id, worker_kind="agent_runner", task_type="agent_run.run")
    workspace_dir = Path(workspace_root) / execution.job_id
    workspace_dir.mkdir(parents=True, exist_ok=True)

    try:
        # START_BLOCK_BLOCK_EXECUTE_AGENT_HARNESS
        harness_name = _resolve_harness_name(execution.params)
        registry = harness_registry or DefaultAgentHarnessRegistry.from_env()
        leases = lease_client or LocalAgentHarnessLeaseClient.from_env()
        request_access = _resolve_request_access(api_client, execution, harness_name)
        request = AgentHarnessRequest(
            job_id=execution.job_id,
            workspace_dir=workspace_dir,
            params=execution.params,
            request_access=request_access.to_payload() if request_access is not None else None,
        )
        lease: AgentHarnessLease | None = None

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="running_agent_harness",
            progress_message="Running agent harness",
        )
        _LOGGER.info(
            "%s %s job_id=%s execution_id=%s harness_name=%s workspace_dir=%s",
            _LOG_MARKER_RUN_AGENT_HARNESS,
            _LOG_MARKER_DISPATCH_AGENT_HARNESS,
            execution.job_id,
            execution.execution_id,
            harness_name,
            workspace_dir,
        )
        try:
            lease = leases.acquire(harness_name, request)
            harness_result = registry.run(
                harness_name,
                request,
                lease=lease,
                cancellation_hook=_build_harness_cancellation_hook(api_client=api_client, execution=execution),
            )
        finally:
            if lease is not None:
                leases.release(lease)

        _check_cancellation(api_client, execution)
        api_client.publish_progress(
            execution.job_id,
            execution_id=execution.execution_id,
            progress_stage="persisting_artifacts",
            progress_message="Uploading agent-runner artifacts",
        )
        artifact_descriptors = _persist_agent_artifacts(
            execution.job_id,
            harness_result,
            artifact_store,
            workspace_dir=workspace_dir,
        )
        api_client.register_artifacts(
            execution.job_id,
            execution_id=execution.execution_id,
            artifacts=artifact_descriptors,
        )

        _check_cancellation(api_client, execution)
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="succeeded",
            progress_stage="completed",
            progress_message="Agent harness completed",
            error_code=None,
            error_message=None,
        )
        return AgentRunnerWorkerResult(
            execution=execution,
            harness_result=harness_result,
            artifact_descriptors=artifact_descriptors,
        )
        # END_BLOCK_BLOCK_EXECUTE_AGENT_HARNESS
    except WorkerCancellationRequested:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="canceled",
            progress_stage="canceled",
            progress_message="Cancellation requested",
            error_code=None,
            error_message=None,
        )
        raise
    except AgentHarnessNotSupported as exc:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Agent harness failed",
            error_code=_UNSUPPORTED_HARNESS_CODE,
            error_message=_redact_control_plane_error_message(str(exc)),
        )
        raise
    except AgentHarnessConcurrencyUnavailable as exc:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Agent harness failed",
            error_code=_CONCURRENCY_UNAVAILABLE_CODE,
            error_message=_redact_control_plane_error_message(str(exc)),
        )
        raise
    except AgentHarnessExecutionFailed as exc:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Agent harness failed",
            error_code=_HARNESS_EXECUTION_FAILED_CODE,
            error_message=_redact_control_plane_error_message(str(exc)),
        )
        raise
    except Exception as exc:
        api_client.finalize_job(
            execution.job_id,
            execution_id=execution.execution_id,
            outcome="failed",
            progress_stage="failed",
            progress_message="Agent harness failed",
            error_code=_GENERIC_FAILURE_CODE,
            error_message=_redact_control_plane_error_message(str(exc)),
        )
        raise


def _resolve_harness_name(params: Mapping[str, object]) -> str:
    harness_name = params.get("harness_name")
    if not isinstance(harness_name, str) or not harness_name.strip():
        raise AgentHarnessNotSupported("agent harness is not supported: <missing>")
    return harness_name.strip()


def _resolve_request_access(
    api_client: JobApiClient,
    execution: ClaimedJobExecution,
    harness_name: str,
) -> AgentRunRequestAccessResult | None:
    if _canonical_harness_name(harness_name) not in _REQUEST_ACCESS_HARNESSES:
        return None
    return api_client.resolve_agent_run_request_access(execution.job_id, execution_id=execution.execution_id)


def _canonical_harness_name(harness_name: str) -> str:
    normalized = harness_name.strip().lower()
    if normalized in {"claude", "claude_code"}:
        return "claude-code"
    return normalized


def _build_redacted_params_metadata(params: Mapping[str, object]) -> dict[str, object]:
    return {
        "keys": sorted(str(key) for key in params),
        "value_types": {str(key): type(value).__name__ for key, value in sorted(params.items(), key=lambda item: str(item[0]))},
        "redacted_fields": [
            str(key)
            for key in sorted(params, key=str)
            if _is_redacted_param_name(str(key))
        ],
    }


def _is_redacted_param_name(name: str) -> bool:
    normalized = name.lower()
    return any(fragment in normalized for fragment in ("prompt", "token", "secret", "password", "credential", "api_key"))


def _redact_control_plane_error_message(message: str) -> str:
    if _message_has_sensitive_fragment(message):
        return _REDACTED_ERROR_MESSAGE
    return message[:500]


def _message_has_sensitive_fragment(message: str) -> bool:
    normalized = message.lower()
    return any(fragment in normalized for fragment in ("prompt", "token", "secret", "password", "credential", "api_key"))


def _parse_concurrency_policy(value: str | None) -> dict[str, int]:
    if not value or not value.strip():
        return {}
    policy: dict[str, int] = {}
    for item in value.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        if "=" not in stripped:
            raise ValueError("AGENT_RUNNER_HARNESS_CONCURRENCY entries must use harness=limit")
        harness_name, limit_text = stripped.split("=", 1)
        canonical = _canonical_harness_name(harness_name)
        if not canonical:
            raise ValueError("AGENT_RUNNER_HARNESS_CONCURRENCY harness name must not be empty")
        try:
            limit = int(limit_text.strip())
        except ValueError as exc:
            raise ValueError("AGENT_RUNNER_HARNESS_CONCURRENCY limits must be integers") from exc
        if limit < 1:
            raise ValueError("AGENT_RUNNER_HARNESS_CONCURRENCY limits must be positive")
        policy[canonical] = limit
    return policy


def _parse_positive_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    parsed = float(value)
    if parsed <= 0:
        raise ValueError("value must be positive")
    return parsed


def _parse_positive_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError("value must be positive")
    return parsed


def _read_secret(values: Mapping[str, str], *, direct_key: str, file_key: str) -> str | None:
    direct = _optional_str(values.get(direct_key))
    if direct:
        return direct
    secret_file = _optional_str(values.get(file_key))
    if not secret_file:
        return None
    return Path(secret_file).read_text(encoding="utf-8").strip() or None


def _optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _expect_mapping(payload: object, *, context: str) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise AgentHarnessExecutionFailed(f"{context} must be an object")
    return payload


def _load_agent_run_prompt_context(
    request_access: Mapping[str, object],
    *,
    workspace_dir: Path,
    request_params: Mapping[str, object],
    redaction_values: tuple[str, ...],
) -> _AgentRunPromptContext:
    url = request_access.get("url")
    if not isinstance(url, str) or not url.strip():
        raise AgentHarnessExecutionFailed("claude-code request access url is missing")
    expected_digest = request_access.get("request_digest_sha256")
    if not isinstance(expected_digest, str) or not expected_digest.strip():
        raise AgentHarnessExecutionFailed("claude-code request access digest is missing")
    try:
        with urlrequest.urlopen(url, timeout=30.0) as response:
            body = response.read()
    except error.URLError as exc:
        raise AgentHarnessExecutionFailed(f"claude-code request envelope download failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise AgentHarnessExecutionFailed("claude-code request envelope download timed out") from exc
    digest = hashlib.sha256(body).hexdigest()
    if digest != expected_digest:
        raise AgentHarnessExecutionFailed("claude-code request envelope digest mismatch")
    try:
        envelope = json.loads(body)
    except json.JSONDecodeError as exc:
        raise AgentHarnessExecutionFailed("claude-code request envelope is malformed JSON") from exc
    mapping = _expect_mapping(envelope, context="agent request envelope")
    request = _expect_mapping(mapping.get("request"), context="agent request envelope request")
    payload = request.get("payload")
    payload_mapping = payload if isinstance(payload, Mapping) else None
    params_request = request_params.get("request")
    params_request_mapping = params_request if isinstance(params_request, Mapping) else None
    params_payload = params_request_mapping.get("payload") if params_request_mapping is not None else None
    params_payload_mapping = params_payload if isinstance(params_payload, Mapping) else None
    operation = _resolve_agent_operation(
        request=request,
        payload=payload_mapping,
        envelope=mapping,
        params_request=params_request_mapping,
        params_payload=params_payload_mapping,
    )
    expected_output_artifacts = _resolve_expected_output_artifacts(
        request=request,
        payload=payload_mapping,
        envelope=mapping,
        params_request=params_request_mapping,
        params_payload=params_payload_mapping,
    )
    prompt = request.get("prompt")
    input_artifacts = request.get("input_artifacts")
    materialized_artifacts = _materialize_input_artifacts(input_artifacts, workspace_dir=workspace_dir)
    chunks: list[str] = []
    if isinstance(prompt, str) and prompt.strip():
        chunks.append(prompt.strip())
    if payload is not None:
        chunks.append("Payload:\n" + json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    if materialized_artifacts:
        chunks.append("Input artifacts:\n" + _format_materialized_artifacts_for_prompt(materialized_artifacts))
    if not chunks:
        raise AgentHarnessExecutionFailed("claude-code request envelope contains no prompt, payload, or input artifacts")
    return _AgentRunPromptContext(
        prompt=_redact_text("\n\n".join(chunks), redaction_values),
        operation=operation,
        expected_output_artifacts=expected_output_artifacts,
        materialized_input_artifacts=tuple(materialized_artifacts),
    )


def _resolve_agent_operation(
    *,
    request: Mapping[str, object],
    payload: Mapping[str, object] | None,
    envelope: Mapping[str, object],
    params_request: Mapping[str, object] | None,
    params_payload: Mapping[str, object] | None,
) -> str | None:
    for candidate in (
        request.get("operation"),
        payload.get("operation") if payload is not None else None,
        envelope.get("operation"),
        params_request.get("operation") if params_request is not None else None,
        params_payload.get("operation") if params_payload is not None else None,
    ):
        if isinstance(candidate, str) and candidate.strip() in {"report", "deep_research"}:
            return candidate.strip()
    return None


def _resolve_expected_output_artifacts(
    *,
    request: Mapping[str, object],
    payload: Mapping[str, object] | None,
    envelope: Mapping[str, object],
    params_request: Mapping[str, object] | None,
    params_payload: Mapping[str, object] | None,
) -> tuple[str, ...]:
    for candidate in (
        request.get("expected_output_artifacts"),
        payload.get("expected_output_artifacts") if payload is not None else None,
        envelope.get("expected_output_artifacts"),
        params_request.get("expected_output_artifacts") if params_request is not None else None,
        params_payload.get("expected_output_artifacts") if params_payload is not None else None,
    ):
        artifacts = _normalize_expected_output_artifacts(candidate)
        if artifacts:
            return artifacts
    return ()


def _normalize_expected_output_artifacts(value: object) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        return ()
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        artifact_kind = item.strip()
        if artifact_kind in _EXPECTED_OPERATION_ARTIFACTS and artifact_kind not in normalized:
            normalized.append(artifact_kind)
    return tuple(normalized)


def _materialize_input_artifacts(input_artifacts: object, *, workspace_dir: Path) -> list[Mapping[str, object]]:
    if input_artifacts is None:
        return []
    if not isinstance(input_artifacts, list):
        raise AgentHarnessExecutionFailed("claude-code input_artifacts must be a list")
    materialized: list[Mapping[str, object]] = []
    for position, item in enumerate(input_artifacts):
        artifact = _expect_mapping(item, context="claude-code input_artifacts entry")
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str) or not artifact_id.strip():
            continue
        resolution = _resolve_artifact(artifact_id.strip())
        destination = _write_materialized_artifact(resolution, workspace_dir=workspace_dir, position=position)
        materialized.append(
            {
                "position": position,
                "artifact_kind": str(resolution.get("artifact_kind") or artifact.get("artifact_kind") or ""),
                "filename": destination.name,
                "mime_type": str(resolution.get("mime_type") or ""),
                "size_bytes": destination.stat().st_size,
                "local_path": str(destination),
            }
        )
    return materialized


def _resolve_artifact(artifact_id: str) -> Mapping[str, object]:
    api_base_url = _optional_str(os.environ.get("API_BASE_URL"))
    if not api_base_url:
        raise AgentHarnessExecutionFailed("API_BASE_URL is required to materialize claude-code input artifacts")
    url = f"{api_base_url.rstrip('/')}/internal/v1/artifacts/{artifact_id}/download-access"
    http_request = urlrequest.Request(url=url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urlrequest.urlopen(http_request, timeout=_ARTIFACT_RESOLVE_TIMEOUT_SECONDS) as response:
            body = response.read()
    except error.URLError as exc:
        raise AgentHarnessExecutionFailed(f"claude-code input artifact resolution failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise AgentHarnessExecutionFailed("claude-code input artifact resolution timed out") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise AgentHarnessExecutionFailed("claude-code input artifact resolution returned malformed JSON") from exc
    return _expect_mapping(payload, context="claude-code input artifact resolution")


def _write_materialized_artifact(resolution: Mapping[str, object], *, workspace_dir: Path, position: int) -> Path:
    download = _expect_mapping(resolution.get("download"), context="claude-code input artifact download")
    download_url = download.get("url")
    if not isinstance(download_url, str) or not download_url.strip():
        raise AgentHarnessExecutionFailed("claude-code input artifact download url is missing")
    try:
        with urlrequest.urlopen(download_url, timeout=_ARTIFACT_DOWNLOAD_TIMEOUT_SECONDS) as response:
            body = response.read()
    except error.URLError as exc:
        raise AgentHarnessExecutionFailed(f"claude-code input artifact download failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise AgentHarnessExecutionFailed("claude-code input artifact download timed out") from exc

    input_dir = workspace_dir / "input-artifacts"
    input_dir.mkdir(parents=True, exist_ok=True)
    filename = _safe_materialized_artifact_filename(resolution.get("filename"), position=position)
    destination = input_dir / filename
    destination.write_bytes(body)
    return destination


def _safe_materialized_artifact_filename(value: object, *, position: int) -> str:
    if isinstance(value, str):
        basename = Path(value).name.strip()
        if basename and basename not in {".", ".."}:
            return basename
    return f"artifact-{position + 1:02d}.bin"


def _format_materialized_artifacts_for_prompt(artifacts: list[Mapping[str, object]]) -> str:
    lines: list[str] = []
    for artifact in artifacts:
        lines.append(
            "- "
            f"local_path={artifact.get('local_path')} "
            f"filename={artifact.get('filename')} "
            f"artifact_kind={artifact.get('artifact_kind')} "
            f"mime_type={artifact.get('mime_type')} "
            f"size_bytes={artifact.get('size_bytes')}"
        )
    return "\n".join(lines)


def _redact_text(value: str, redaction_values: tuple[str, ...]) -> str:
    redacted = value
    for secret in redaction_values:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def _persist_agent_artifacts(
    job_id: str,
    harness_result: AgentHarnessResult,
    artifact_store: ArtifactObjectStore,
    *,
    workspace_dir: Path,
) -> tuple[ArtifactDescriptor, ...]:
    writer = ArtifactWriter(job_id=job_id, object_store=artifact_store)
    result_payload = dict(harness_result.result_payload)
    result_payload.setdefault("harness_name", harness_result.harness_name)
    result_json = json.dumps(result_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    descriptors = [
        writer.write_bytes_artifact(
            "agent_result_json",
            "result.json",
            result_json,
            mime_type="application/json; charset=utf-8",
            format="json",
        ),
        writer.write_text_artifact(
            "execution_log",
            "execution.log",
            harness_result.execution_log,
            mime_type="text/plain; charset=utf-8",
            format="log",
        ),
    ]
    descriptors.extend(_persist_requested_operation_artifacts(writer, result_payload, workspace_dir=workspace_dir))
    return tuple(descriptors)


def _persist_requested_operation_artifacts(
    writer: ArtifactWriter,
    result_payload: Mapping[str, object],
    *,
    workspace_dir: Path,
) -> tuple[ArtifactDescriptor, ...]:
    operation = result_payload.get("operation")
    expected = _normalize_expected_output_artifacts(result_payload.get("expected_output_artifacts"))
    if operation == "report":
        return _persist_report_operation_artifacts(writer, result_payload, expected, workspace_dir=workspace_dir)
    if operation == "deep_research":
        return _persist_deep_research_operation_artifacts(writer, result_payload, expected)
    return ()


def _persist_report_operation_artifacts(
    writer: ArtifactWriter,
    result_payload: Mapping[str, object],
    expected: tuple[str, ...],
    *,
    workspace_dir: Path,
) -> tuple[ArtifactDescriptor, ...]:
    requested = [kind for kind in expected if kind in {"report_markdown", "report_docx"}]
    if not requested:
        return ()
    output_text = _operation_output_text(result_payload, operation="report")
    normalized_markdown = normalize_report_markdown(output_text)
    descriptors: list[ArtifactDescriptor] = []
    if "report_markdown" in requested:
        descriptors.append(
            writer.write_text_artifact(
                "report_markdown",
                "report.md",
                normalized_markdown,
                mime_type="text/markdown; charset=utf-8",
                format="markdown",
            )
        )
    if "report_docx" in requested:
        docx_path = workspace_dir / "operation-artifacts" / "report.docx"
        write_report_docx(docx_path, normalized_markdown)
        descriptors.append(
            writer.write_file_artifact(
                "report_docx",
                docx_path,
                mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                format="docx",
            )
        )
    return tuple(descriptors)


def _persist_deep_research_operation_artifacts(
    writer: ArtifactWriter,
    result_payload: Mapping[str, object],
    expected: tuple[str, ...],
) -> tuple[ArtifactDescriptor, ...]:
    if "deep_research_markdown" not in expected:
        return ()
    output_text = _operation_output_text(result_payload, operation="deep_research")
    return (
        writer.write_text_artifact(
            "deep_research_markdown",
            "deep-research.md",
            _ensure_trailing_newline(output_text.strip()),
            mime_type="text/markdown; charset=utf-8",
            format="markdown",
        ),
    )


def _operation_output_text(result_payload: Mapping[str, object], *, operation: str) -> str:
    output_text = result_payload.get("output_text")
    if not isinstance(output_text, str) or not output_text.strip():
        raise AgentHarnessExecutionFailed(f"claude-code {operation} output was empty")
    return output_text


def _ensure_trailing_newline(value: str) -> str:
    return value if value.endswith("\n") else value + "\n"


def _check_cancellation(api_client: JobApiClient, execution: ClaimedJobExecution) -> None:
    cancel_state = api_client.check_cancel(execution.job_id, execution_id=execution.execution_id)
    if cancel_state.cancel_requested:
        raise WorkerCancellationRequested(f"job {execution.job_id} was canceled")


def _build_harness_cancellation_hook(*, api_client: JobApiClient, execution: ClaimedJobExecution):
    def cancellation_hook() -> bool:
        try:
            _check_cancellation(api_client, execution)
        except WorkerCancellationRequested:
            return True
        return False

    return cancellation_hook
