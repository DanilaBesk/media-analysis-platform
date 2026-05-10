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
#   AnalysisRunQueueItem - Represents the minimal queued analysis-run task consumed by the shared worker loop.
#   SelectionItemSnapshot - Represents one immutable selected media item from the worker-control contract.
#   ClaimedAnalysisRunExecution - Represents the final analysis_run claim response consumed by workers.
#   CancelCheckResult - Represents the cancel-check response consumed by later worker packets.
#   ArtifactResolutionResult - Represents a resolved downloadable artifact locator.
#   AgentRunRequestAccessResult - Represents short-lived private request access for claimed agent_run executions.
#   InternalApiUnavailableError - Signals deterministic internal control-plane transport failures.
#   AnalysisRunControlClient - Calls the canonical internal worker endpoints without DTO drift.
# END_MODULE_MAP

from __future__ import annotations

import json
import logging
import mimetypes
from dataclasses import dataclass, field
from typing import Mapping, Protocol, Sequence
from urllib import error, parse, request

from transcriber_workers_common.artifacts import ArtifactDescriptor


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_CALL_INTERNAL_CONTROL_PLANE = "[WorkerCommon][callInternalApi][BLOCK_CALL_INTERNAL_CONTROL_PLANE]"
_WORKER_KINDS = frozenset({"transcription", "agent_runner"})
_TASK_TYPES = frozenset({"selection.transcription", "selection.analysis"})
_RUN_TYPES = frozenset({"transcription", "summary", "report", "deep_research", "custom"})
_MEDIA_KINDS = frozenset({"text", "url", "file", "photo", "image", "audio", "voice", "video", "document"})
_SOURCE_ORIGINS = frozenset({"text", "url", "object"})
_MATERIALIZATION_KINDS = frozenset({"text", "url", "object", "unsupported"})
_JOB_STATUSES = frozenset({"queued", "running", "cancel_requested", "succeeded", "failed", "canceled"})
_WORKER_OUTCOMES = frozenset({"succeeded", "partially_succeeded", "failed", "canceled"})
_MIME_EXTENSION_OVERRIDES = {
    "audio/aac": ".aac",
    "audio/flac": ".flac",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/wav": ".wav",
    "audio/webm": ".webm",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "application/pdf": ".pdf",
    "text/plain": ".txt",
    "video/mp4": ".mp4",
    "video/mpeg": ".mpeg",
    "video/quicktime": ".mov",
    "video/webm": ".webm",
}

__all__ = [
    "AgentRunRequestAccessResult",
    "ArtifactResolutionResult",
    "CancelCheckResult",
    "ClaimedAnalysisRunExecution",
    "AnalysisRunControlClient",
    "AnalysisRunQueueItem",
    "InternalApiConfig",
    "InternalApiUnavailableError",
    "JsonTransport",
    "MediaSourceSnapshot",
    "OrderedWorkerInput",
    "SealedSelectionInput",
    "SelectionItemLabels",
    "SelectionItemMaterialization",
    "SelectionItemSnapshot",
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


# START_CONTRACT: AnalysisRunQueueItem
# PURPOSE: Represent the minimal queued analysis-run task item consumed by worker-common polling.
# INPUTS: { analysis_run_id: str - Analysis run identifier, run_type/task_type - Frozen run/task routing, status: str - Canonical task status, version: int - Run snapshot version }
# OUTPUTS: { AnalysisRunQueueItem - Minimal analysis-run task snapshot for queue polling }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: AnalysisRunQueueItem
@dataclass(frozen=True, slots=True)
class AnalysisRunQueueItem:
    analysis_run_id: str
    run_type: str
    task_type: str
    status: str
    version: int

    def __post_init__(self) -> None:
        _require(self.run_type in _RUN_TYPES, "invalid listed run_type")
        _require(self.task_type in _TASK_TYPES, "invalid listed task_type")
        _require(self.status in _JOB_STATUSES, "invalid listed analysis run task status")
        _require(self.version >= 1, "listed analysis run version must be >= 1")

    @classmethod
    def from_payload(cls, payload: object) -> "AnalysisRunQueueItem":
        # START_BLOCK_BLOCK_VALIDATE_ANALYSIS_RUN_QUEUE_ITEM
        mapping = _expect_mapping(payload, context="analysis run queue item")
        return cls(
            analysis_run_id=_expect_str(
                mapping.get("analysis_run_id"), context="analysis run queue item analysis_run_id"
            ),
            run_type=_expect_str(mapping.get("run_type"), context="analysis run queue item run_type"),
            task_type=_expect_str(mapping.get("task_type"), context="analysis run queue item task_type"),
            status=_expect_str(mapping.get("status"), context="analysis run queue item status"),
            version=_expect_int(mapping.get("version"), context="analysis run queue item version", minimum=1),
        )
        # END_BLOCK_BLOCK_VALIDATE_ANALYSIS_RUN_QUEUE_ITEM


# START_CONTRACT: MediaSourceSnapshot
# PURPOSE: Represent final source metadata captured in a sealed selection snapshot.
# INPUTS: { source_id/origin_type plus optional external_uri/object_key/text_ref/checksum/size_bytes/mime_type/expires_at }
# OUTPUTS: { MediaSourceSnapshot - Typed source metadata for execution-plane workers }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: MediaSourceSnapshot
@dataclass(frozen=True, slots=True)
class MediaSourceSnapshot:
    source_id: str
    origin_type: str
    external_uri: str | None = None
    object_key: str | None = None
    text_ref: str | None = None
    checksum: str | None = None
    size_bytes: int | None = None
    mime_type: str | None = None
    expires_at: str | None = None

    def __post_init__(self) -> None:
        _require(self.origin_type in _SOURCE_ORIGINS, "invalid selection source origin_type")
        if self.size_bytes is not None:
            _require(self.size_bytes >= 0, "selection source size_bytes must be non-negative")

    @classmethod
    def from_payload(cls, payload: object) -> "MediaSourceSnapshot":
        mapping = _expect_mapping(payload, context="selection source_snapshot")
        _ensure_allowed_keys(
            mapping,
            required={"source_id", "origin_type"},
            optional={
                "external_uri",
                "object_key",
                "text_ref",
                "checksum",
                "size_bytes",
                "mime_type",
                "expires_at",
            },
            context="selection source_snapshot",
        )
        return cls(
            source_id=_expect_str(mapping.get("source_id"), context="selection source_snapshot source_id"),
            origin_type=_expect_str(mapping.get("origin_type"), context="selection source_snapshot origin_type"),
            external_uri=_expect_optional_str(mapping.get("external_uri"), context="selection source_snapshot external_uri"),
            object_key=_expect_optional_str(mapping.get("object_key"), context="selection source_snapshot object_key"),
            text_ref=_expect_optional_str(mapping.get("text_ref"), context="selection source_snapshot text_ref"),
            checksum=_expect_optional_str(mapping.get("checksum"), context="selection source_snapshot checksum"),
            size_bytes=_expect_optional_int(
                mapping.get("size_bytes"), context="selection source_snapshot size_bytes", minimum=0
            ),
            mime_type=_expect_optional_str(mapping.get("mime_type"), context="selection source_snapshot mime_type"),
            expires_at=_expect_optional_str(mapping.get("expires_at"), context="selection source_snapshot expires_at"),
        )


@dataclass(frozen=True, slots=True)
class SelectionItemLabels:
    display_label: str
    source_label: str | None = None
    original_filename: str | None = None

    def __post_init__(self) -> None:
        _require(bool(self.display_label.strip()), "selection item labels display_label must not be empty")

    @classmethod
    def from_payload(cls, payload: object) -> "SelectionItemLabels":
        mapping = _expect_mapping(payload, context="selection item labels")
        _ensure_allowed_keys(
            mapping,
            required={"display_label"},
            optional={"source_label", "original_filename"},
            context="selection item labels",
        )
        return cls(
            display_label=_expect_str(mapping.get("display_label"), context="selection item labels display_label"),
            source_label=_expect_optional_str(mapping.get("source_label"), context="selection item labels source_label"),
            original_filename=_expect_optional_str(
                mapping.get("original_filename"), context="selection item labels original_filename"
            ),
        )

    @classmethod
    def from_selection_metadata(cls, *, display_name: str, metadata: Mapping[str, object]) -> "SelectionItemLabels":
        return cls(
            display_label=display_name,
            source_label=_metadata_value(metadata, "source_label"),
            original_filename=_metadata_value(metadata, "original_filename") or _metadata_value(metadata, "filename"),
        )

    def source_display_label(self) -> str:
        if self.source_label and self.source_label.strip():
            return self.source_label.strip()
        return self.display_label


@dataclass(frozen=True, slots=True)
class SelectionItemSnapshot:
    position: int
    media_item_id: str
    kind: str
    source_snapshot: MediaSourceSnapshot
    display_name: str
    status_at_selection: str
    metadata_snapshot: Mapping[str, object]
    retention_snapshot: Mapping[str, object]
    diagnostics: tuple[Mapping[str, object], ...] = ()
    selection_item_id: str | None = None
    media_kind: str | None = None
    mime_type: str | None = None
    role: str = "primary"
    labels: SelectionItemLabels | None = None

    def __post_init__(self) -> None:
        _require(self.position >= 0, "selection item position must be non-negative")
        _require(self.kind in _MEDIA_KINDS, "invalid selection item kind")
        if self.selection_item_id is None:
            object.__setattr__(self, "selection_item_id", f"selection-item-{self.position}")
        _require(bool(str(self.selection_item_id).strip()), "selection item selection_item_id must not be empty")
        if self.media_kind is None:
            object.__setattr__(self, "media_kind", self.kind)
        _require(self.media_kind in _MEDIA_KINDS, "invalid selection item media_kind")
        if self.mime_type is None:
            object.__setattr__(self, "mime_type", self.source_snapshot.mime_type)
        _require(bool(self.role.strip()), "selection item role must not be empty")
        if self.labels is None:
            object.__setattr__(
                self,
                "labels",
                SelectionItemLabels.from_selection_metadata(
                    display_name=self.display_name,
                    metadata=self.metadata_snapshot,
                ),
            )
        _require(bool(self.display_name.strip()), "selection item display_name must not be empty")

    @classmethod
    def from_payload(
        cls,
        payload: object,
        *,
        option_snapshot: Mapping[str, object] | None = None,
    ) -> "SelectionItemSnapshot":
        mapping = _expect_mapping(payload, context="selection item")
        metadata_snapshot = dict(_expect_mapping(mapping.get("metadata_snapshot", {}), context="selection item metadata_snapshot"))
        _ensure_allowed_keys(
            mapping,
            required={
                "selection_item_id",
                "position",
                "media_item_id",
                "kind",
                "media_kind",
                "mime_type",
                "source_snapshot",
                "display_name",
                "status_at_selection",
                "retention_snapshot",
            },
            optional={"metadata_snapshot", "diagnostics", "role", "labels"},
            context="selection item",
        )
        diagnostics = mapping.get("diagnostics", [])
        _require(isinstance(diagnostics, list), "selection item diagnostics must be a list")
        return cls(
            selection_item_id=_expect_str(mapping.get("selection_item_id"), context="selection item selection_item_id"),
            position=_expect_int(mapping.get("position"), context="selection item position", minimum=0),
            media_item_id=_expect_str(mapping.get("media_item_id"), context="selection item media_item_id"),
            kind=_expect_str(mapping.get("kind"), context="selection item kind"),
            media_kind=_expect_str(mapping.get("media_kind"), context="selection item media_kind"),
            mime_type=_expect_optional_str(mapping.get("mime_type"), context="selection item mime_type"),
            role=_derive_selection_role(mapping, metadata_snapshot=metadata_snapshot, option_snapshot=option_snapshot),
            labels=(
                SelectionItemLabels.from_payload(mapping.get("labels"))
                if "labels" in mapping
                else SelectionItemLabels.from_selection_metadata(
                    display_name=_expect_str(mapping.get("display_name"), context="selection item display_name"),
                    metadata=metadata_snapshot,
                )
            ),
            source_snapshot=MediaSourceSnapshot.from_payload(mapping.get("source_snapshot")),
            display_name=_expect_str(mapping.get("display_name"), context="selection item display_name"),
            status_at_selection=_expect_str(
                mapping.get("status_at_selection"), context="selection item status_at_selection"
            ),
            metadata_snapshot=metadata_snapshot,
            retention_snapshot=dict(_expect_mapping(mapping.get("retention_snapshot"), context="selection item retention_snapshot")),
            diagnostics=tuple(_expect_mapping(item, context="selection item diagnostic") for item in diagnostics),
        )


@dataclass(frozen=True, slots=True)
class SelectionItemMaterialization:
    selection_item_id: str
    position: int
    media_item_id: str
    media_kind: str
    role: str
    labels: SelectionItemLabels
    source_id: str
    origin_type: str
    materialization_kind: str
    mime_type: str | None = None
    object_key: str | None = None
    external_uri: str | None = None
    text_ref: str | None = None
    checksum: str | None = None
    size_bytes: int | None = None
    deterministic_filename: str | None = None
    unsupported_reason: str | None = None

    def __post_init__(self) -> None:
        _require(self.media_kind in _MEDIA_KINDS, "invalid materialization media_kind")
        _require(self.origin_type in _SOURCE_ORIGINS, "invalid materialization origin_type")
        _require(self.materialization_kind in _MATERIALIZATION_KINDS, "invalid materialization_kind")

    @classmethod
    def from_selection_item(cls, item: SelectionItemSnapshot) -> "SelectionItemMaterialization":
        source = item.source_snapshot
        materialization_kind = source.origin_type
        unsupported_reason = None
        deterministic_filename = None
        if source.origin_type == "object":
            if source.object_key:
                deterministic_filename = _deterministic_materialized_filename(
                    position=item.position,
                    source_id=source.source_id,
                    mime_type=item.mime_type or source.mime_type,
                )
            else:
                materialization_kind = "unsupported"
                unsupported_reason = "object-backed media source is missing object_key"
        labels = item.labels or SelectionItemLabels.from_selection_metadata(
            display_name=item.display_name,
            metadata=item.metadata_snapshot,
        )
        return cls(
            selection_item_id=str(item.selection_item_id),
            position=item.position,
            media_item_id=item.media_item_id,
            media_kind=str(item.media_kind),
            role=item.role,
            labels=labels,
            source_id=source.source_id,
            origin_type=source.origin_type,
            materialization_kind=materialization_kind,
            mime_type=item.mime_type or source.mime_type,
            object_key=source.object_key,
            external_uri=source.external_uri,
            text_ref=source.text_ref,
            checksum=source.checksum,
            size_bytes=source.size_bytes,
            deterministic_filename=deterministic_filename,
            unsupported_reason=unsupported_reason,
        )

    @property
    def is_object_backed(self) -> bool:
        return self.materialization_kind == "object"


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

    @classmethod
    def from_payload(cls, payload: object) -> "OrderedWorkerInput":
        mapping = _expect_mapping(payload, context="claim ordered input compatibility helper")
        return cls(
            position=_expect_int(mapping.get("position"), context="claim ordered input position", minimum=0),
            source_id=_expect_str(mapping.get("source_id"), context="claim ordered input source_id"),
            source_kind=_expect_str(mapping.get("source_kind"), context="claim ordered input source_kind"),
            source_label=_expect_optional_str(mapping.get("source_label"), context="claim ordered input source_label"),
            display_name=_expect_optional_str(mapping.get("display_name"), context="claim ordered input display_name"),
            original_filename=_expect_optional_str(
                mapping.get("original_filename"), context="claim ordered input original_filename"
            ),
            object_key=_expect_optional_str(mapping.get("object_key"), context="claim ordered input object_key"),
            source_url=_expect_optional_str(mapping.get("source_url"), context="claim ordered input source_url"),
            sha256=_expect_optional_str(mapping.get("sha256"), context="claim ordered input sha256"),
            size_bytes=_expect_optional_int(mapping.get("size_bytes"), context="claim ordered input size_bytes", minimum=0),
        )

    @classmethod
    def from_selection_item(cls, item: SelectionItemSnapshot) -> "OrderedWorkerInput":
        return cls(
            position=item.position,
            source_id=item.source_snapshot.source_id,
            source_kind=item.source_snapshot.origin_type,
            source_label=item.labels.source_label if item.labels else _metadata_source_label(item),
            display_name=item.display_name,
            original_filename=item.labels.original_filename if item.labels else _metadata_original_filename(item),
            object_key=item.source_snapshot.object_key,
            source_url=item.source_snapshot.external_uri,
            sha256=item.source_snapshot.checksum,
            size_bytes=item.source_snapshot.size_bytes,
        )


@dataclass(frozen=True, slots=True)
class SealedSelectionInput:
    selection_id: str
    items: tuple[SelectionItemSnapshot, ...]
    option_snapshot: Mapping[str, object]
    sealed_at: str

    def __post_init__(self) -> None:
        _require(bool(self.items), "claimed selection must include at least one item")

    @classmethod
    def from_payload(cls, payload: object) -> "SealedSelectionInput":
        mapping = _expect_mapping(payload, context="claim response selection")
        _ensure_allowed_keys(
            mapping,
            required={"selection_id", "items", "option_snapshot", "sealed_at"},
            optional=set(),
            context="claim response selection",
        )
        items = mapping.get("items")
        _require(isinstance(items, list), "claim response selection items must be a list")
        option_snapshot = dict(_expect_mapping(mapping.get("option_snapshot"), context="claim response option_snapshot"))
        return cls(
            selection_id=_expect_str(mapping.get("selection_id"), context="claim response selection_id"),
            items=tuple(SelectionItemSnapshot.from_payload(item, option_snapshot=option_snapshot) for item in items),
            option_snapshot=option_snapshot,
            sealed_at=_expect_str(mapping.get("sealed_at"), context="claim response sealed_at"),
        )


# START_CONTRACT: ClaimedAnalysisRunExecution
# PURPOSE: Carry the canonical final claim response that workers execute against.
# INPUTS: { execution_id/analysis_run_id/run_type/selection/params/claimed_at from internal worker-control }
# OUTPUTS: { ClaimedAnalysisRunExecution - Typed analysis run execution context }
# SIDE_EFFECTS: none
# LINKS: M-WORKER-COMMON, M-CONTRACTS
# END_CONTRACT: ClaimedAnalysisRunExecution
@dataclass(frozen=True, slots=True)
class ClaimedAnalysisRunExecution:
    execution_id: str
    analysis_run_id: str
    run_type: str
    selection: SealedSelectionInput
    params: Mapping[str, object]
    claimed_at: str

    def __post_init__(self) -> None:
        _require(self.run_type in _RUN_TYPES, "invalid claimed run_type")
        _require(bool(self.selection.items), "claimed selection must include at least one item")

    @property
    def ordered_inputs(self) -> tuple[OrderedWorkerInput, ...]:
        return tuple(OrderedWorkerInput.from_selection_item(item) for item in self.selection.items)

    @classmethod
    def from_payload(cls, payload: object) -> "ClaimedAnalysisRunExecution":
        # START_BLOCK_BLOCK_VALIDATE_CLAIM_RESPONSE
        mapping = _expect_mapping(payload, context="claim response")
        _ensure_allowed_keys(
            mapping,
            required={"execution_id", "analysis_run_id", "run_type", "selection", "params", "claimed_at"},
            optional=set(),
            context="claim response",
        )
        params = _expect_mapping(mapping.get("params"), context="claim response params")
        return cls(
            execution_id=_expect_str(mapping.get("execution_id"), context="claim response execution_id"),
            analysis_run_id=_expect_str(mapping.get("analysis_run_id"), context="claim response analysis_run_id"),
            run_type=_expect_str(mapping.get("run_type"), context="claim response run_type"),
            selection=SealedSelectionInput.from_payload(mapping.get("selection")),
            params=dict(params),
            claimed_at=_expect_str(mapping.get("claimed_at"), context="claim response claimed_at"),
        )
        # END_BLOCK_BLOCK_VALIDATE_CLAIM_RESPONSE


# START_CONTRACT: CancelCheckResult
# PURPOSE: Represent the authoritative cancel-check response used by worker control flow.
# INPUTS: { cancel_requested: bool - Whether cancellation was requested, status: str - Canonical analysis run status, cancel_requested_at: str | None - Optional timestamp }
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
class ArtifactResolutionResult:
    artifact_id: str
    analysis_run_id: str
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
            analysis_run_id=_expect_str(mapping.get("analysis_run_id"), context="artifact resolution analysis_run_id"),
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


# START_CONTRACT: AnalysisRunControlClient
# PURPOSE: Call the frozen internal worker-control endpoints through one canonical shared client.
# INPUTS: { config: InternalApiConfig - Internal API connection parameters, transport: JsonTransport | None - Optional transport override for tests or future adapters }
# OUTPUTS: { AnalysisRunControlClient - Reusable worker-control client }
# SIDE_EFFECTS: network IO through the configured transport
# LINKS: M-WORKER-COMMON, M-CONTRACTS, V-M-WORKER-COMMON
# END_CONTRACT: AnalysisRunControlClient
class AnalysisRunControlClient:
    def __init__(self, config: InternalApiConfig, transport: JsonTransport | None = None) -> None:
        self.config = config
        self.transport = transport or _UrllibJsonTransport(
            timeout_seconds=config.timeout_seconds,
            headers=config.headers,
        )

    # START_CONTRACT: list_queued_runs
    # PURPOSE: Read authoritative analysis-run task snapshots for shared worker polling without bypassing the API contract.
    # INPUTS: { status: str | None - Optional task status filter, run_type: str | None - Optional run type filter, task_type: str | None - Optional task type filter, page_size: int - Max items to read }
    # OUTPUTS: { tuple[AnalysisRunQueueItem, ...] - Minimal snapshots consumed by the worker runtime scaffold }
    # SIDE_EFFECTS: API GET request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: list_queued_runs
    def list_queued_runs(
        self,
        *,
        status: str | None = None,
        run_type: str | None = None,
        task_type: str | None = None,
        page_size: int = 20,
    ) -> tuple[AnalysisRunQueueItem, ...]:
        if status is not None:
            _require(status in _JOB_STATUSES, "invalid analysis run task status filter")
        if run_type is not None:
            _require(run_type in _RUN_TYPES, "invalid run_type filter")
        if task_type is not None:
            _require(task_type in _TASK_TYPES, "invalid task_type filter")
        _require(page_size > 0, "page_size must be positive")

        query = {"page": "1", "page_size": str(page_size)}
        if status:
            query["status"] = status
        if run_type:
            query["run_type"] = run_type
        if task_type:
            query["task_type"] = task_type
        response = self._call_internal_api("GET", "/internal/v1/analysis-runs/queue", query=query)
        mapping = _expect_mapping(response, context="analysis run queue response")
        items = mapping.get("items")
        _require(isinstance(items, list), "analysis run queue response items must be a list")
        return tuple(AnalysisRunQueueItem.from_payload(item) for item in items)

    # START_CONTRACT: claim_analysis_run
    # PURPOSE: Claim one analysis run through the canonical worker-control contract and parse the execution context.
    # INPUTS: { analysis_run_id: str - Analysis run identifier, worker_kind: str - Frozen worker kind, task_type: str - Frozen queue task type }
    # OUTPUTS: { ClaimedAnalysisRunExecution - Typed claim response }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: claim_analysis_run
    def claim_analysis_run(self, analysis_run_id: str, *, worker_kind: str, task_type: str) -> ClaimedAnalysisRunExecution:
        _require(worker_kind in _WORKER_KINDS, "invalid worker_kind")
        _require(task_type in _TASK_TYPES, "invalid task_type")
        payload = {"worker_kind": worker_kind, "task_type": task_type}
        response = self._call_internal_api(
            "POST",
            f"/internal/v1/analysis-runs/{analysis_run_id}/executions/claim",
            payload=payload,
        )
        return ClaimedAnalysisRunExecution.from_payload(response)

    # START_CONTRACT: publish_progress
    # PURPOSE: Emit one canonical progress update for a running worker execution.
    # INPUTS: { analysis_run_id: str - Analysis run identifier, execution_id: str - Claimed execution identifier, progress_stage: str - Stable progress stage, progress_message: str | None - Optional human-readable progress message }
    # OUTPUTS: { None - The API side effect is authoritative }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-003
    # END_CONTRACT: publish_progress
    def publish_progress(
        self,
        analysis_run_id: str,
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
        self._call_internal_api("POST", f"/internal/v1/analysis-runs/{analysis_run_id}/executions/progress", payload=payload)

    # START_CONTRACT: register_artifacts
    # PURPOSE: Report canonical artifact metadata after object persistence succeeds.
    # INPUTS: { analysis_run_id: str - Analysis run identifier, execution_id: str - Claimed execution identifier, artifacts: Sequence[ArtifactDescriptor] - Canonical artifact descriptors }
    # OUTPUTS: { None - The API side effect is authoritative }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001
    # END_CONTRACT: register_artifacts
    def register_artifacts(self, analysis_run_id: str, *, execution_id: str, artifacts: Sequence[ArtifactDescriptor]) -> None:
        _require(bool(artifacts), "artifacts must not be empty")
        payload = {
            "execution_id": execution_id,
            "artifacts": [artifact.to_payload() for artifact in artifacts],
        }
        self._call_internal_api("POST", f"/internal/v1/analysis-runs/{analysis_run_id}/artifacts", payload=payload)

    def register_diagnostics(
        self,
        analysis_run_id: str,
        *,
        execution_id: str,
        diagnostics: Sequence[Mapping[str, object]],
    ) -> None:
        _require(bool(diagnostics), "diagnostics must not be empty")
        payload = {"execution_id": execution_id, "diagnostics": [dict(item) for item in diagnostics]}
        self._call_internal_api("POST", f"/internal/v1/analysis-runs/{analysis_run_id}/diagnostics", payload=payload)

    # START_CONTRACT: finalize_analysis_run
    # PURPOSE: Finalize one worker execution through the canonical internal contract.
    # INPUTS: { analysis_run_id: str - Analysis run identifier, execution_id: str - Claimed execution identifier, outcome: str - Frozen worker outcome, progress_stage/progress_message/error_code/error_message: str | None - Optional terminal metadata }
    # OUTPUTS: { None - The API side effect is authoritative }
    # SIDE_EFFECTS: internal API POST request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-001, DF-007
    # END_CONTRACT: finalize_analysis_run
    def finalize_analysis_run(
        self,
        analysis_run_id: str,
        *,
        execution_id: str,
        outcome: str,
        progress_stage: str | None = None,
        progress_message: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        _require(outcome in _WORKER_OUTCOMES, "invalid worker outcome")
        message = progress_message or error_message
        payload = {
            "execution_id": execution_id,
            "outcome": outcome,
            "message": message,
        }
        self._call_internal_api("POST", f"/internal/v1/analysis-runs/{analysis_run_id}/executions/finalize", payload=payload)

    # START_CONTRACT: check_cancel
    # PURPOSE: Read the authoritative cancellation state for a running worker execution.
    # INPUTS: { analysis_run_id: str - Analysis run identifier, execution_id: str - Claimed execution identifier }
    # OUTPUTS: { CancelCheckResult - Typed cancel-check response }
    # SIDE_EFFECTS: internal API GET request
    # LINKS: M-WORKER-COMMON, M-CONTRACTS, DF-007
    # END_CONTRACT: check_cancel
    def check_cancel(self, analysis_run_id: str, *, execution_id: str) -> CancelCheckResult:
        response = self._call_internal_api(
            "GET",
            f"/internal/v1/analysis-runs/{analysis_run_id}/executions/cancel-check",
            query={"execution_id": execution_id},
        )
        return CancelCheckResult.from_payload(response)

    def resolve_artifact(self, artifact_id: str) -> ArtifactResolutionResult:
        response = self._call_internal_api("GET", f"/internal/v1/artifacts/{artifact_id}/download-access")
        return ArtifactResolutionResult.from_payload(response)

    def resolve_agent_run_request_access(self, analysis_run_id: str, *, execution_id: str) -> AgentRunRequestAccessResult:
        response = self._call_internal_api(
            "GET",
            f"/internal/v1/analysis-runs/{analysis_run_id}/request-access",
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


def _metadata_original_filename(item: SelectionItemSnapshot) -> str | None:
    value = item.metadata_snapshot.get("original_filename")
    if isinstance(value, str) and value.strip():
        return value.strip()
    value = item.metadata_snapshot.get("filename")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _metadata_source_label(item: SelectionItemSnapshot) -> str | None:
    value = item.metadata_snapshot.get("source_label")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _metadata_value(metadata: Mapping[str, object], key: str) -> str | None:
    value = metadata.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _derive_selection_role(
    mapping: Mapping[str, object],
    *,
    metadata_snapshot: Mapping[str, object],
    option_snapshot: Mapping[str, object] | None,
) -> str:
    role = _metadata_value(mapping, "role")
    if role:
        return role
    role = _metadata_value(metadata_snapshot, "role")
    if role:
        return role
    item_roles = (option_snapshot or {}).get("item_roles")
    if isinstance(item_roles, Mapping):
        media_item_id = mapping.get("media_item_id")
        position = mapping.get("position")
        for key in (media_item_id, str(position) if isinstance(position, int) else None):
            if key is None:
                continue
            value = item_roles.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return "primary"


def _deterministic_materialized_filename(*, position: int, source_id: str, mime_type: str | None) -> str:
    safe_source_id = _safe_filename_token(source_id)
    return f"item-{position:04d}-{safe_source_id}{_extension_for_mime(mime_type)}"


def _extension_for_mime(mime_type: str | None) -> str:
    if not mime_type:
        return ".bin"
    normalized = mime_type.split(";", 1)[0].strip().casefold()
    if not normalized:
        return ".bin"
    if normalized in _MIME_EXTENSION_OVERRIDES:
        return _MIME_EXTENSION_OVERRIDES[normalized]
    guessed = mimetypes.guess_extension(normalized, strict=False)
    return guessed or ".bin"


def _safe_filename_token(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in value)
    cleaned = cleaned.strip("-_")
    return cleaned or "source"
