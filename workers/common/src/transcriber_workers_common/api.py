# FILE: workers/common/src/transcriber_workers_common/api.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the shared internal worker-control API client and typed execution DTO parsing.
# SCOPE: Claim, progress, artifact, finalize, and cancel-check calls plus deterministic response validation.
# DEPENDS: M-WORKER-COMMON, M-CONTRACTS
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added the canonical worker-control client with stable log markers and DTO-shape guards.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   InternalApiConfig - Stores worker-control base URL, timeout, and headers.
#   JsonTransport - Defines the transport contract used by the shared API client.
#   JobListItem - Represents the minimal queued-job list item consumed by the shared worker loop.
#   OrderedWorkerInput - Represents one claimed ordered input from the frozen worker-control contract.
#   ClaimedJobExecution - Represents the claim response consumed by later worker packets.
#   CancelCheckResult - Represents the cancel-check response consumed by later worker packets.
#   JobSnapshot - Represents public job state used by aggregate workers to resolve child artifacts.
#   ArtifactResolutionResult - Represents a resolved downloadable artifact locator.
#   AgentRunRequestAccessResult - Represents short-lived private request access for claimed agent_run executions.
#   InternalApiUnavailableError - Signals deterministic internal control-plane transport failures.
#   JobApiClient - Calls the canonical internal worker endpoints without DTO drift.
# END_MODULE_MAP

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence
from urllib import error, parse, request

from transcriber_workers_common.artifacts import ArtifactDescriptor


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_CALL_INTERNAL_CONTROL_PLANE = "[WorkerCommon][callInternalApi][BLOCK_CALL_INTERNAL_CONTROL_PLANE]"
_WORKER_KINDS = frozenset({"transcription", "agent_runner"})
_TASK_TYPES = frozenset({"transcription.run", "transcription.aggregate", "agent_run.run"})
_JOB_TYPES = frozenset({"transcription", "report", "deep_research", "agent_run"})
_JOB_STATUSES = frozenset({"queued", "running", "cancel_requested", "succeeded", "failed", "canceled"})
_WORKER_OUTCOMES = frozenset({"succeeded", "failed", "canceled"})

__all__ = [
    "AgentRunRequestAccessResult",
    "ArtifactResolutionResult",
    "CancelCheckResult",
    "ChildJobReference",
    "ClaimedJobExecution",
    "InternalApiConfig",
    "InternalApiUnavailableError",
    "JobApiClient",
    "JobListItem",
    "JobSnapshot",
    "JsonTransport",
    "OrderedWorkerInput",
    "SourceSetItem",
]


class InternalApiUnavailableError(RuntimeError):
    pass


# START_CONTRACT: JsonTransport
# PURPOSE: Define the transport boundary used by the worker-common API client.
# INPUTS: { method: str - HTTP method, url: str - Fully-qualified endpoint URL, payload: Mapping[str, object] | None - JSON payload when present }
# OUTPUTS: { object - Parsed JSON response payload or None for empty responses }
# SIDE_EFFECTS: network IO or test doubles only
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: JsonTransport
class JsonTransport(Protocol):
    def request(self, *, method: str, url: str, payload: Mapping[str, object] | None = None) -> object: ...


class _UrllibJsonTransport:
    def __init__(self, *, timeout_seconds: float, headers: Mapping[str, str]) -> None:
        self.timeout_seconds = timeout_seconds
        self.headers = dict(headers)

    def request(self, *, method: str, url: str, payload: Mapping[str, object] | None = None) -> object:
        # START_BLOCK_BLOCK_SEND_JSON_REQUEST
        request_headers = {"Accept": "application/json", **self.headers}
        request_data: bytes | None = None
        if payload is not None:
            request_headers["Content-Type"] = "application/json"
            request_data = json.dumps(payload).encode("utf-8")

        http_request = request.Request(url=url, data=request_data, headers=request_headers, method=method)
        try:
            with request.urlopen(http_request, timeout=self.timeout_seconds) as response:
                body = response.read()
        except error.HTTPError as exc:  # pragma: no cover - exercised through the shared wrapper behavior
            raise InternalApiUnavailableError(
                f"internal control-plane request failed with HTTP {exc.code}: {exc.reason}"
            ) from exc
        except error.URLError as exc:
            raise InternalApiUnavailableError(f"internal control-plane request failed: {exc.reason}") from exc
        except TimeoutError as exc:
            raise InternalApiUnavailableError("internal control-plane request timed out") from exc

        if not body:
            return None

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError("internal control-plane returned malformed JSON") from exc
        # END_BLOCK_BLOCK_SEND_JSON_REQUEST


# START_CONTRACT: InternalApiConfig
# PURPOSE: Carry the shared internal API connection configuration.
# INPUTS: { base_url: str - Internal API root URL, timeout_seconds: float - Per-request timeout, headers: Mapping[str, str] - Default HTTP headers }
# OUTPUTS: { InternalApiConfig - Immutable worker-control transport configuration }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: InternalApiConfig
@dataclass(frozen=True, slots=True)
class InternalApiConfig:
    base_url: str
    timeout_seconds: float = 30.0
    headers: Mapping[str, str] = field(default_factory=dict)

    def build_url(self, path: str, query: Mapping[str, str] | None = None) -> str:
        normalized_base = self.base_url.rstrip("/")
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{normalized_base}{normalized_path}"
        if not query:
            return url
        return f"{url}?{parse.urlencode(query)}"


# START_CONTRACT: JobListItem
# PURPOSE: Represent the minimal queued-job list item consumed by worker-common polling.
# INPUTS: { job_id: str - Job identifier, job_type: str - Frozen job type, status: str - Canonical job status, version: int - Snapshot version }
# OUTPUTS: { JobListItem - Minimal job snapshot for queue polling }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: JobListItem
@dataclass(frozen=True, slots=True)
class JobListItem:
    job_id: str
    job_type: str
    status: str
    version: int

    def __post_init__(self) -> None:
        _require(self.job_type in _JOB_TYPES, "invalid listed job_type")
        _require(self.status in _JOB_STATUSES, "invalid listed job status")
        _require(self.version >= 1, "listed job version must be >= 1")

    @classmethod
    def from_payload(cls, payload: object) -> "JobListItem":
        # START_BLOCK_BLOCK_VALIDATE_JOB_LIST_ITEM
        mapping = _expect_mapping(payload, context="job list item")
        return cls(
            job_id=_expect_str(mapping.get("job_id"), context="job list item job_id"),
            job_type=_expect_str(mapping.get("job_type"), context="job list item job_type"),
            status=_expect_str(mapping.get("status"), context="job list item status"),
            version=_expect_int(mapping.get("version"), context="job list item version", minimum=1),
        )
        # END_BLOCK_BLOCK_VALIDATE_JOB_LIST_ITEM


# START_CONTRACT: OrderedWorkerInput
# PURPOSE: Represent one ordered worker input from the frozen claim response.
# INPUTS: { position: int - Stable source ordering, source_id: str - Canonical source identifier, source_kind: str - Frozen source kind, display_name/original_filename/object_key/source_url/sha256/size_bytes - Optional source metadata }
# OUTPUTS: { OrderedWorkerInput - Typed input metadata for later worker packets }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: OrderedWorkerInput
@dataclass(frozen=True, slots=True)
class OrderedWorkerInput:
    position: int
    source_id: str
    source_kind: str
    source_label: str | None = None
    display_name: str | None = None
    original_filename: str | None = None
    object_key: str | None = None
    source_url: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        _require(self.position >= 0, "ordered input position must be non-negative")
        _require(
            self.source_kind in {"uploaded_file", "telegram_upload", "youtube_url", "external_url"},
            "invalid ordered input source_kind",
        )
        if self.size_bytes is not None:
            _require(self.size_bytes >= 0, "ordered input size_bytes must be non-negative")

    @classmethod
    def from_payload(cls, payload: object) -> "OrderedWorkerInput":
        # START_BLOCK_BLOCK_VALIDATE_ORDERED_INPUT_PAYLOAD
        mapping = _expect_mapping(payload, context="claim ordered input")
        _ensure_allowed_keys(
            mapping,
            required={"position", "source_id", "source_kind"},
            optional={
                "source_label",
                "display_name",
                "original_filename",
                "object_key",
                "source_url",
                "sha256",
                "size_bytes",
            },
            context="claim ordered input",
        )
        return cls(
            position=_expect_int(mapping.get("position"), context="claim ordered input position", minimum=0),
            source_id=_expect_str(mapping.get("source_id"), context="claim ordered input source_id"),
            source_label=_expect_optional_str(mapping.get("source_label"), context="claim ordered input source_label"),
            source_kind=_expect_str(mapping.get("source_kind"), context="claim ordered input source_kind"),
            display_name=_expect_optional_str(mapping.get("display_name"), context="claim ordered input display_name"),
            original_filename=_expect_optional_str(
                mapping.get("original_filename"), context="claim ordered input original_filename"
            ),
            object_key=_expect_optional_str(mapping.get("object_key"), context="claim ordered input object_key"),
            source_url=_expect_optional_str(mapping.get("source_url"), context="claim ordered input source_url"),
            sha256=_expect_optional_str(mapping.get("sha256"), context="claim ordered input sha256"),
            size_bytes=_expect_optional_int(mapping.get("size_bytes"), context="claim ordered input size_bytes", minimum=0),
        )
        # END_BLOCK_BLOCK_VALIDATE_ORDERED_INPUT_PAYLOAD


# START_CONTRACT: ClaimedJobExecution
# PURPOSE: Carry the canonical claim response that later worker packets execute against.
# INPUTS: { execution_id/job_id/root_job_id: str - Execution lineage, parent_job_id/retry_of_job_id: str | None - Optional lineage links, job_type: str - Frozen job type, version: int - Optimistic version, ordered_inputs: tuple[OrderedWorkerInput, ...] - Claimed ordered inputs, params: Mapping[str, object] - Worker parameters }
# OUTPUTS: { ClaimedJobExecution - Typed job execution context }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: ClaimedJobExecution
@dataclass(frozen=True, slots=True)
class ClaimedJobExecution:
    execution_id: str
    job_id: str
    root_job_id: str
    parent_job_id: str | None
    retry_of_job_id: str | None
    job_type: str
    version: int
    ordered_inputs: tuple[OrderedWorkerInput, ...]
    params: Mapping[str, object]

    def __post_init__(self) -> None:
        _require(self.job_type in _JOB_TYPES, "invalid claimed job_type")
        _require(self.version >= 1, "claimed job version must be >= 1")
        batch_params = self.params.get("batch")
        batch_role = batch_params.get("role") if isinstance(batch_params, Mapping) else None
        if self.job_type != "agent_run" and batch_role != "aggregate":
            _require(bool(self.ordered_inputs), "claimed job ordered_inputs must include at least one item")

    @classmethod
    def from_payload(cls, payload: object) -> "ClaimedJobExecution":
        # START_BLOCK_BLOCK_VALIDATE_CLAIM_RESPONSE
        mapping = _expect_mapping(payload, context="claim response")
        _ensure_allowed_keys(
            mapping,
            required={"execution_id", "job_id", "root_job_id", "job_type", "version", "ordered_inputs", "params"},
            optional={"parent_job_id", "retry_of_job_id"},
            context="claim response",
        )
        ordered_inputs_payload = mapping.get("ordered_inputs")
        _require(isinstance(ordered_inputs_payload, list), "claim response ordered_inputs must be a list")
        params = _expect_mapping(mapping.get("params"), context="claim response params")
        return cls(
            execution_id=_expect_str(mapping.get("execution_id"), context="claim response execution_id"),
            job_id=_expect_str(mapping.get("job_id"), context="claim response job_id"),
            root_job_id=_expect_str(mapping.get("root_job_id"), context="claim response root_job_id"),
            parent_job_id=_expect_optional_str(mapping.get("parent_job_id"), context="claim response parent_job_id"),
            retry_of_job_id=_expect_optional_str(mapping.get("retry_of_job_id"), context="claim response retry_of_job_id"),
            job_type=_expect_str(mapping.get("job_type"), context="claim response job_type"),
            version=_expect_int(mapping.get("version"), context="claim response version", minimum=1),
            ordered_inputs=tuple(OrderedWorkerInput.from_payload(item) for item in ordered_inputs_payload),
            params=dict(params),
        )
        # END_BLOCK_BLOCK_VALIDATE_CLAIM_RESPONSE


# START_CONTRACT: CancelCheckResult
# PURPOSE: Represent the authoritative cancel-check response used by worker control flow.
# INPUTS: { cancel_requested: bool - Whether cancellation was requested, status: str - Canonical job status, cancel_requested_at: str | None - Optional timestamp }
# OUTPUTS: { CancelCheckResult - Typed cancel-check result }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: CancelCheckResult
@dataclass(frozen=True, slots=True)
class CancelCheckResult:
    cancel_requested: bool
    status: str
    cancel_requested_at: str | None = None

    def __post_init__(self) -> None:
        _require(self.status in _JOB_STATUSES, "invalid cancel-check status")

    @classmethod
    def from_payload(cls, payload: object) -> "CancelCheckResult":
        mapping = _expect_mapping(payload, context="cancel-check response")
        _ensure_allowed_keys(
            mapping,
            required={"cancel_requested", "status"},
            optional={"cancel_requested_at"},
            context="cancel-check response",
        )
        return cls(
            cancel_requested=_expect_bool(mapping.get("cancel_requested"), context="cancel-check response cancel_requested"),
            status=_expect_str(mapping.get("status"), context="cancel-check response status"),
            cancel_requested_at=_expect_optional_str(
                mapping.get("cancel_requested_at"), context="cancel-check response cancel_requested_at"
            ),
        )


@dataclass(frozen=True, slots=True)
class SourceSetItem:
    position: int
    source_label: str | None

    @classmethod
    def from_payload(cls, payload: object) -> "SourceSetItem":
        mapping = _expect_mapping(payload, context="job snapshot source_set item")
        return cls(
            position=_expect_int(mapping.get("position"), context="job snapshot source_set item position", minimum=0),
            source_label=_expect_optional_str(
                mapping.get("source_label"), context="job snapshot source_set item source_label"
            ),
        )


@dataclass(frozen=True, slots=True)
class ArtifactSummary:
    artifact_id: str
    artifact_kind: str
    filename: str
    mime_type: str
    size_bytes: int

    @classmethod
    def from_payload(cls, payload: object) -> "ArtifactSummary":
        mapping = _expect_mapping(payload, context="job snapshot artifact")
        return cls(
            artifact_id=_expect_str(mapping.get("artifact_id"), context="job snapshot artifact artifact_id"),
            artifact_kind=_expect_str(mapping.get("artifact_kind"), context="job snapshot artifact artifact_kind"),
            filename=_expect_str(mapping.get("filename"), context="job snapshot artifact filename"),
            mime_type=_expect_str(mapping.get("mime_type"), context="job snapshot artifact mime_type"),
            size_bytes=_expect_int(mapping.get("size_bytes"), context="job snapshot artifact size_bytes", minimum=0),
        )


@dataclass(frozen=True, slots=True)
class ChildJobReference:
    job_id: str
    job_type: str
    status: str
    version: int

    @classmethod
    def from_payload(cls, payload: object) -> "ChildJobReference":
        mapping = _expect_mapping(payload, context="job snapshot child")
        return cls(
            job_id=_expect_str(mapping.get("job_id"), context="job snapshot child job_id"),
            job_type=_expect_str(mapping.get("job_type"), context="job snapshot child job_type"),
            status=_expect_str(mapping.get("status"), context="job snapshot child status"),
            version=_expect_int(mapping.get("version"), context="job snapshot child version", minimum=1),
        )


@dataclass(frozen=True, slots=True)
class JobSnapshot:
    job_id: str
    root_job_id: str
    parent_job_id: str | None
    job_type: str
    status: str
    version: int
    source_set_items: tuple[SourceSetItem, ...]
    artifacts: tuple[ArtifactSummary, ...]
    children: tuple[ChildJobReference, ...]

    @classmethod
    def from_payload(cls, payload: object) -> "JobSnapshot":
        mapping = _expect_mapping(payload, context="job snapshot envelope")
        raw_job = mapping.get("job", payload)
        job = _expect_mapping(raw_job, context="job snapshot")
        source_set = _expect_mapping(job.get("source_set"), context="job snapshot source_set")
        items = source_set.get("items")
        artifacts = job.get("artifacts")
        children = job.get("children")
        _require(isinstance(items, list), "job snapshot source_set items must be a list")
        _require(isinstance(artifacts, list), "job snapshot artifacts must be a list")
        _require(isinstance(children, list), "job snapshot children must be a list")
        return cls(
            job_id=_expect_str(job.get("job_id"), context="job snapshot job_id"),
            root_job_id=_expect_str(job.get("root_job_id"), context="job snapshot root_job_id"),
            parent_job_id=_expect_optional_str(job.get("parent_job_id"), context="job snapshot parent_job_id"),
            job_type=_expect_str(job.get("job_type"), context="job snapshot job_type"),
            status=_expect_str(job.get("status"), context="job snapshot status"),
            version=_expect_int(job.get("version"), context="job snapshot version", minimum=1),
            source_set_items=tuple(SourceSetItem.from_payload(item) for item in items),
            artifacts=tuple(ArtifactSummary.from_payload(artifact) for artifact in artifacts),
            children=tuple(ChildJobReference.from_payload(child) for child in children),
        )


@dataclass(frozen=True, slots=True)
class ArtifactResolutionResult:
    artifact_id: str
    job_id: str
    artifact_kind: str
    filename: str
    mime_type: str
    size_bytes: int
    download_url: str

    @classmethod
    def from_payload(cls, payload: object) -> "ArtifactResolutionResult":
        mapping = _expect_mapping(payload, context="artifact resolution")
        download = _expect_mapping(mapping.get("download"), context="artifact resolution download")
        return cls(
            artifact_id=_expect_str(mapping.get("artifact_id"), context="artifact resolution artifact_id"),
            job_id=_expect_str(mapping.get("job_id"), context="artifact resolution job_id"),
            artifact_kind=_expect_str(mapping.get("artifact_kind"), context="artifact resolution artifact_kind"),
            filename=_expect_str(mapping.get("filename"), context="artifact resolution filename"),
            mime_type=_expect_str(mapping.get("mime_type"), context="artifact resolution mime_type"),
            size_bytes=_expect_int(mapping.get("size_bytes"), context="artifact resolution size_bytes", minimum=0),
            download_url=_expect_str(download.get("url"), context="artifact resolution download url"),
        )


@dataclass(frozen=True, slots=True)
class AgentRunRequestAccessResult:
    provider: str
    url: str
    expires_at: str
    request_ref: str
    request_digest_sha256: str
    request_bytes: int

    @classmethod
    def from_payload(cls, payload: object) -> "AgentRunRequestAccessResult":
        mapping = _expect_mapping(payload, context="agent-run request-access response")
        _ensure_allowed_keys(
            mapping,
            required={"provider", "url", "expires_at", "request_ref", "request_digest_sha256", "request_bytes"},
            optional=set(),
            context="agent-run request-access response",
        )
        return cls(
            provider=_expect_str(mapping.get("provider"), context="agent-run request-access response provider"),
            url=_expect_str(mapping.get("url"), context="agent-run request-access response url"),
            expires_at=_expect_str(mapping.get("expires_at"), context="agent-run request-access response expires_at"),
            request_ref=_expect_str(mapping.get("request_ref"), context="agent-run request-access response request_ref"),
            request_digest_sha256=_expect_str(
                mapping.get("request_digest_sha256"), context="agent-run request-access response request_digest_sha256"
            ),
            request_bytes=_expect_int(
                mapping.get("request_bytes"),
                context="agent-run request-access response request_bytes",
                minimum=1,
            ),
        )

    def to_payload(self) -> Mapping[str, object]:
        return {
            "provider": self.provider,
            "url": self.url,
            "expires_at": self.expires_at,
            "request_ref": self.request_ref,
            "request_digest_sha256": self.request_digest_sha256,
            "request_bytes": self.request_bytes,
        }


# START_CONTRACT: JobApiClient
# PURPOSE: Call the frozen internal worker-control endpoints through one canonical shared client.
# INPUTS: { config: InternalApiConfig - Internal API connection parameters, transport: JsonTransport | None - Optional transport override for tests or future adapters }
# OUTPUTS: { JobApiClient - Reusable worker-control client }
# SIDE_EFFECTS: network IO through the configured transport
# LINKS: M-WORKER-COMMON, M-CONTRACTS, V-M-WORKER-COMMON
# END_CONTRACT: JobApiClient
class JobApiClient:
    def __init__(self, config: InternalApiConfig, transport: JsonTransport | None = None) -> None:
        self.config = config
        self.transport = transport or _UrllibJsonTransport(
            timeout_seconds=config.timeout_seconds,
            headers=config.headers,
        )

    # START_CONTRACT: list_jobs
    # PURPOSE: Read authoritative job snapshots for shared worker polling without bypassing the API contract.
    # INPUTS: { status: str | None - Optional canonical status filter, job_type: str | None - Optional frozen job type filter, page_size: int - Max items to read }
    # OUTPUTS: { tuple[JobListItem, ...] - Minimal snapshots consumed by the worker runtime scaffold }
    # SIDE_EFFECTS: API GET request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: list_jobs
    def list_jobs(
        self,
        *,
        status: str | None = None,
        job_type: str | None = None,
        page_size: int = 20,
    ) -> tuple[JobListItem, ...]:
        if status is not None:
            _require(status in _JOB_STATUSES, "invalid job status filter")
        if job_type is not None:
            _require(job_type in _JOB_TYPES, "invalid job_type filter")
        _require(page_size > 0, "page_size must be positive")

        query = {"page": "1", "page_size": str(page_size)}
        if status:
            query["status"] = status
        if job_type:
            query["job_type"] = job_type
        response = self._call_internal_api("GET", "/v1/jobs", query=query)
        mapping = _expect_mapping(response, context="job list response")
        items = mapping.get("items")
        _require(isinstance(items, list), "job list response items must be a list")
        return tuple(JobListItem.from_payload(item) for item in items)

    # START_CONTRACT: claim_job
    # PURPOSE: Claim one job through the canonical worker-control contract and parse the execution context.
    # INPUTS: { job_id: str - Job identifier, worker_kind: str - Frozen worker kind, task_type: str - Frozen queue task type }
    # OUTPUTS: { ClaimedJobExecution - Typed claim response }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: claim_job
    def claim_job(self, job_id: str, *, worker_kind: str, task_type: str) -> ClaimedJobExecution:
        _require(worker_kind in _WORKER_KINDS, "invalid worker_kind")
        _require(task_type in _TASK_TYPES, "invalid task_type")
        payload = {"worker_kind": worker_kind, "task_type": task_type}
        response = self._call_internal_api("POST", f"/internal/v1/jobs/{job_id}/claim", payload=payload)
        return ClaimedJobExecution.from_payload(response)

    # START_CONTRACT: publish_progress
    # PURPOSE: Emit one canonical progress update for a running worker execution.
    # INPUTS: { job_id: str - Job identifier, execution_id: str - Claimed execution identifier, progress_stage: str - Stable progress stage, progress_message: str | None - Optional human-readable progress message }
    # OUTPUTS: { None - The API side effect is authoritative }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-003
    # END_CONTRACT: publish_progress
    def publish_progress(
        self,
        job_id: str,
        *,
        execution_id: str,
        progress_stage: str,
        progress_message: str | None = None,
    ) -> None:
        _require(bool(progress_stage.strip()), "progress_stage must not be empty")
        payload = {
            "execution_id": execution_id,
            "progress_stage": progress_stage,
            "progress_message": progress_message,
        }
        self._call_internal_api("POST", f"/internal/v1/jobs/{job_id}/progress", payload=payload)

    # START_CONTRACT: register_artifacts
    # PURPOSE: Report canonical artifact metadata after object persistence succeeds.
    # INPUTS: { job_id: str - Job identifier, execution_id: str - Claimed execution identifier, artifacts: Sequence[ArtifactDescriptor] - Canonical artifact descriptors }
    # OUTPUTS: { None - The API side effect is authoritative }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: register_artifacts
    def register_artifacts(self, job_id: str, *, execution_id: str, artifacts: Sequence[ArtifactDescriptor]) -> None:
        _require(bool(artifacts), "artifacts must not be empty")
        payload = {
            "execution_id": execution_id,
            "artifacts": [artifact.to_payload() for artifact in artifacts],
        }
        self._call_internal_api("POST", f"/internal/v1/jobs/{job_id}/artifacts", payload=payload)

    # START_CONTRACT: finalize_job
    # PURPOSE: Finalize one worker execution through the canonical internal contract.
    # INPUTS: { job_id: str - Job identifier, execution_id: str - Claimed execution identifier, outcome: str - Frozen worker outcome, progress_stage/progress_message/error_code/error_message: str | None - Optional terminal metadata }
    # OUTPUTS: { None - The API side effect is authoritative }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001, DF-007
    # END_CONTRACT: finalize_job
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
        _require(outcome in _WORKER_OUTCOMES, "invalid worker outcome")
        payload = {
            "execution_id": execution_id,
            "outcome": outcome,
            "progress_stage": progress_stage,
            "progress_message": progress_message,
            "error_code": error_code,
            "error_message": error_message,
        }
        self._call_internal_api("POST", f"/internal/v1/jobs/{job_id}/finalize", payload=payload)

    # START_CONTRACT: check_cancel
    # PURPOSE: Read the authoritative cancellation state for a running worker execution.
    # INPUTS: { job_id: str - Job identifier, execution_id: str - Claimed execution identifier }
    # OUTPUTS: { CancelCheckResult - Typed cancel-check response }
    # SIDE_EFFECTS: internal API GET request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-007
    # END_CONTRACT: check_cancel
    def check_cancel(self, job_id: str, *, execution_id: str) -> CancelCheckResult:
        response = self._call_internal_api(
            "GET",
            f"/internal/v1/jobs/{job_id}/cancel-check",
            query={"execution_id": execution_id},
        )
        return CancelCheckResult.from_payload(response)

    def get_job_snapshot(self, job_id: str) -> JobSnapshot:
        response = self._call_internal_api("GET", f"/v1/jobs/{job_id}")
        return JobSnapshot.from_payload(response)

    def resolve_artifact(self, artifact_id: str) -> ArtifactResolutionResult:
        response = self._call_internal_api("GET", f"/v1/artifacts/{artifact_id}")
        return ArtifactResolutionResult.from_payload(response)

    def resolve_agent_run_request_access(self, job_id: str, *, execution_id: str) -> AgentRunRequestAccessResult:
        response = self._call_internal_api(
            "GET",
            f"/internal/v1/jobs/{job_id}/request-access",
            query={"execution_id": execution_id},
        )
        return AgentRunRequestAccessResult.from_payload(response)

    def _call_internal_api(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        query: Mapping[str, str] | None = None,
    ) -> object:
        # START_BLOCK_BLOCK_CALL_INTERNAL_CONTROL_PLANE
        url = self.config.build_url(path, query=query)
        _LOGGER.info("%s %s %s", _LOG_MARKER_CALL_INTERNAL_CONTROL_PLANE, method, url)
        try:
            return self.transport.request(method=method, url=url, payload=payload)
        except InternalApiUnavailableError:
            raise
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise InternalApiUnavailableError(f"internal control-plane request failed: {exc}") from exc
        # END_BLOCK_BLOCK_CALL_INTERNAL_CONTROL_PLANE


def _expect_mapping(payload: object, *, context: str) -> Mapping[str, object]:
    _require(isinstance(payload, Mapping), f"{context} must be an object")
    return payload


def _expect_str(value: object, *, context: str) -> str:
    _require(isinstance(value, str) and bool(value), f"{context} must be a non-empty string")
    return value


def _expect_optional_str(value: object, *, context: str) -> str | None:
    if value is None:
        return None
    return _expect_str(value, context=context)


def _expect_bool(value: object, *, context: str) -> bool:
    _require(isinstance(value, bool), f"{context} must be a boolean")
    return value


def _expect_int(value: object, *, context: str, minimum: int | None = None) -> int:
    _require(isinstance(value, int) and not isinstance(value, bool), f"{context} must be an integer")
    if minimum is not None:
        _require(value >= minimum, f"{context} must be >= {minimum}")
    return value


def _expect_optional_int(value: object, *, context: str, minimum: int | None = None) -> int | None:
    if value is None:
        return None
    return _expect_int(value, context=context, minimum=minimum)


def _ensure_allowed_keys(
    payload: Mapping[str, object],
    *,
    required: set[str],
    optional: set[str],
    context: str,
) -> None:
    expected = required | optional
    missing = sorted(required - payload.keys())
    unexpected = sorted(set(payload.keys()) - expected)
    _require(not missing, f"{context} is missing required field(s): {', '.join(missing)}")
    _require(not unexpected, f"{context} contains unexpected field(s): {', '.join(unexpected)}")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)
