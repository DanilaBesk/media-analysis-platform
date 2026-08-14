# FILE: workers/metadata-enrichment/src/transcriber_worker_metadata_enrichment.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Resolve API-owned YouTube metadata enrichment jobs with fenced leases and bounded metadata-only subprocesses.
# SCOPE: Queue polling, claim/progress/finalize HTTP calls, canonical URL validation, yt-dlp execution, metadata sanitization, and retry classification.
# DEPENDS: M-CONTRACTS
# LINKS: M-METADATA-ENRICHMENT, V-M-METADATA-ENRICHMENT
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_MODULE_MAP
#   EnrichmentClaim - Represents a fenced metadata-enrichment claim.
#   EnrichmentJob - Represents one metadata-enrichment job.
#   EnrichmentMetadata - Represents normalized metadata resolved for one source.
#   HttpMetadataEnrichmentControlClient - Implements metadata-enrichment control-plane calls over HTTP.
#   MetadataEnrichmentControlClient - Defines the metadata-enrichment control-plane boundary.
#   MetadataEnrichmentWorker - Executes claimed metadata-enrichment jobs.
#   MetadataEnrichmentWorkerConfig - Stores validated metadata-enrichment runtime settings.
#   MetadataResolverError - Reports stable metadata resolver failures.
#   YtDlpMetadataResolver - Resolves YouTube metadata through a bounded yt-dlp subprocess.
# END_MODULE_MAP

from __future__ import annotations

import json
import logging
import math
import os
import re
import signal
import socket
import subprocess
import threading
import time
import unicodedata
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from urllib import error, parse, request


_LOGGER = logging.getLogger(__name__)
_LOG_MARKER = "[WorkerMetadataEnrichment][resolve_metadata]"
_YOUTUBE_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
_YOUTUBE_THUMBNAIL_HOST = re.compile(r"^(?:img\.youtube\.com|i(?:\d+)?\.ytimg\.com)$")
_TERMINAL_PROVIDER_ERRORS = (
    "private video",
    "video unavailable",
    "this video is unavailable",
    "has been removed",
    "unsupported url",
    "sign in to confirm your age",
    "members-only content",
    "join this channel",
)

__all__ = [
    "EnrichmentClaim",
    "EnrichmentJob",
    "EnrichmentMetadata",
    "HttpMetadataEnrichmentControlClient",
    "MetadataEnrichmentControlClient",
    "MetadataEnrichmentWorker",
    "MetadataEnrichmentWorkerConfig",
    "MetadataResolverError",
    "YtDlpMetadataResolver",
]


class MetadataEnrichmentError(RuntimeError):
    pass


class ControlPlaneError(MetadataEnrichmentError):
    pass


class MetadataEnrichmentShutdown(MetadataEnrichmentError):
    pass


class MetadataResolverError(MetadataEnrichmentError):
    def __init__(self, code: str, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class EnrichmentJob:
    enrichment_id: str
    media_asset_id: str
    channel_account_id: str
    provider: str
    canonical_url: str
    status: str
    attempt_no: int
    max_attempts: int

    def __post_init__(self) -> None:
        for name in ("enrichment_id", "media_asset_id", "channel_account_id"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.provider != "youtube":
            raise ValueError("only the youtube provider is supported")
        _youtube_video_id(self.canonical_url)
        if self.status not in {"queued", "retry_wait", "claimed", "running"}:
            raise ValueError("unsupported metadata enrichment status")
        if self.attempt_no < 0 or self.max_attempts <= 0 or self.attempt_no > self.max_attempts:
            raise ValueError("invalid metadata enrichment attempt counters")

    @classmethod
    def from_payload(cls, payload: object) -> "EnrichmentJob":
        data = _mapping(payload, "metadata enrichment")
        return cls(
            enrichment_id=_string(data.get("enrichment_id"), "enrichment_id"),
            media_asset_id=_string(data.get("media_asset_id"), "media_asset_id"),
            channel_account_id=_string(data.get("channel_account_id"), "channel_account_id"),
            provider=_string(data.get("provider"), "provider"),
            canonical_url=_string(data.get("canonical_url"), "canonical_url"),
            status=_string(data.get("status"), "status"),
            attempt_no=_integer_field(data.get("attempt_no"), "attempt_no"),
            max_attempts=_integer_field(data.get("max_attempts"), "max_attempts"),
        )


@dataclass(frozen=True, slots=True)
class EnrichmentClaim:
    enrichment: EnrichmentJob
    attempt_token: str
    lease_owner: str
    lease_expires_at: datetime

    @classmethod
    def from_payload(cls, payload: object) -> "EnrichmentClaim":
        data = _mapping(payload, "metadata enrichment claim")
        expires_at = _timestamp(data.get("lease_expires_at"), "lease_expires_at")
        return cls(
            enrichment=EnrichmentJob.from_payload(data.get("enrichment")),
            attempt_token=_string(data.get("attempt_token"), "attempt_token"),
            lease_owner=_string(data.get("lease_owner"), "lease_owner"),
            lease_expires_at=expires_at,
        )


@dataclass(frozen=True, slots=True)
class EnrichmentMetadata:
    title: str
    thumbnail_url: str
    duration_seconds: int
    performer: str = ""


class MetadataEnrichmentControlClient(Protocol):
    def list_queue(self, *, page_size: int) -> tuple[EnrichmentJob, ...]: ...

    def claim(self, enrichment_id: str, *, lease_owner: str, lease_seconds: int) -> EnrichmentClaim: ...

    def publish_progress(self, claim: EnrichmentClaim, progress: Mapping[str, object]) -> None: ...

    def finalize_success(self, claim: EnrichmentClaim, metadata: EnrichmentMetadata) -> None: ...

    def finalize_failure(self, claim: EnrichmentClaim, error: MetadataResolverError) -> None: ...


@dataclass(frozen=True, slots=True)
class MetadataEnrichmentWorkerConfig:
    lease_owner: str
    internal_token: str
    api_base_url: str = "http://api:8080"
    poll_interval_seconds: float = 5.0
    lease_seconds: int = 120
    heartbeat_interval_seconds: float = 30.0
    resolver_timeout_seconds: float = 30.0
    resolver_max_stdout_bytes: int = 4 * 1024 * 1024
    resolver_max_stderr_bytes: int = 64 * 1024
    api_response_max_bytes: int = 1024 * 1024
    concurrency: int = 2

    def __post_init__(self) -> None:
        if not self.lease_owner.strip():
            raise ValueError("lease_owner must not be empty")
        if not self.internal_token.strip():
            raise ValueError("internal_token must not be empty")
        parsed = parse.urlparse(self.api_base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("api_base_url must be an absolute HTTP(S) URL without credentials")
        if not 1 <= self.lease_seconds <= 900:
            raise ValueError("lease_seconds must be within 1..900")
        if self.poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be non-negative")
        if self.heartbeat_interval_seconds <= 0 or self.heartbeat_interval_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_interval_seconds must be positive and shorter than the lease")
        for name in (
            "resolver_timeout_seconds",
            "resolver_max_stdout_bytes",
            "resolver_max_stderr_bytes",
            "api_response_max_bytes",
            "concurrency",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.concurrency > 32:
            raise ValueError("concurrency must not exceed 32")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "MetadataEnrichmentWorkerConfig":
        values = os.environ if env is None else env
        return cls(
            lease_owner=values.get(
                "METADATA_ENRICHMENT_LEASE_OWNER", f"metadata-enrichment-{socket.gethostname()}"
            ),
            internal_token=values.get("PLATFORM_INTERNAL_TOKEN", ""),
            api_base_url=values.get("API_BASE_URL", "http://api:8080"),
            poll_interval_seconds=_number(values.get("WORKER_POLL_INTERVAL_SECONDS"), 5.0),
            lease_seconds=_integer(values.get("METADATA_ENRICHMENT_LEASE_SECONDS"), 120),
            heartbeat_interval_seconds=_number(
                values.get("METADATA_ENRICHMENT_HEARTBEAT_INTERVAL_SECONDS"), 30.0
            ),
            resolver_timeout_seconds=_number(values.get("METADATA_ENRICHMENT_TIMEOUT_SECONDS"), 30.0),
            resolver_max_stdout_bytes=_integer(
                values.get("METADATA_ENRICHMENT_MAX_STDOUT_BYTES"), 4 * 1024 * 1024
            ),
            resolver_max_stderr_bytes=_integer(
                values.get("METADATA_ENRICHMENT_MAX_STDERR_BYTES"), 64 * 1024
            ),
            api_response_max_bytes=_integer(
                values.get("METADATA_ENRICHMENT_API_RESPONSE_MAX_BYTES"), 1024 * 1024
            ),
            concurrency=_integer(values.get("METADATA_ENRICHMENT_CONCURRENCY"), 2),
        )


class HttpMetadataEnrichmentControlClient:
    def __init__(
        self,
        config: MetadataEnrichmentWorkerConfig,
        *,
        opener: Callable[..., object] | None = None,
    ) -> None:
        self.base_url = config.api_base_url.rstrip("/")
        self.internal_token = config.internal_token
        self.max_response_bytes = config.api_response_max_bytes
        self.opener = opener or _open_without_redirects

    def list_queue(self, *, page_size: int) -> tuple[EnrichmentJob, ...]:
        payload = self._request(
            "GET",
            "/internal/v1/metadata-enrichment-jobs/queue",
            query={"page_size": str(page_size)},
        )
        data = _mapping(payload, "metadata enrichment queue")
        items = data.get("items")
        if not isinstance(items, list):
            raise ValueError("metadata enrichment queue items must be a list")
        return tuple(EnrichmentJob.from_payload(item) for item in items)

    def claim(self, enrichment_id: str, *, lease_owner: str, lease_seconds: int) -> EnrichmentClaim:
        claim = EnrichmentClaim.from_payload(
            self._request(
                "POST",
                self._path(enrichment_id, "claim"),
                payload={"lease_owner": lease_owner, "lease_seconds": lease_seconds},
            )
        )
        if claim.enrichment.enrichment_id != enrichment_id or claim.lease_owner != lease_owner:
            raise ControlPlaneError("control plane returned a mismatched metadata enrichment claim")
        return claim

    def publish_progress(self, claim: EnrichmentClaim, progress: Mapping[str, object]) -> None:
        self._request(
            "POST",
            self._path(claim.enrichment.enrichment_id, "progress"),
            payload={
                "lease_owner": claim.lease_owner,
                "attempt_token": claim.attempt_token,
                "progress": dict(progress),
            },
        )

    def finalize_success(self, claim: EnrichmentClaim, metadata: EnrichmentMetadata) -> None:
        payload: dict[str, object] = {
            "lease_owner": claim.lease_owner,
            "attempt_token": claim.attempt_token,
            "outcome": "succeeded",
            "title": metadata.title,
            "duration_seconds": metadata.duration_seconds,
        }
        if metadata.thumbnail_url:
            payload["thumbnail_url"] = metadata.thumbnail_url
        if metadata.performer:
            payload["performer"] = metadata.performer
        self._request(
            "POST",
            self._path(claim.enrichment.enrichment_id, "finalize"),
            payload=payload,
        )

    def finalize_failure(self, claim: EnrichmentClaim, failure: MetadataResolverError) -> None:
        self._request(
            "POST",
            self._path(claim.enrichment.enrichment_id, "finalize"),
            payload={
                "lease_owner": claim.lease_owner,
                "attempt_token": claim.attempt_token,
                "outcome": "failed",
                "error_code": failure.code,
                "error_message": _sanitize_text(str(failure), 1000),
                "retryable": failure.retryable,
            },
        )

    def _path(self, enrichment_id: str, suffix: str) -> str:
        return f"/internal/v1/metadata-enrichment-jobs/{parse.quote(enrichment_id, safe='')}/{suffix}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        query: Mapping[str, str] | None = None,
    ) -> object:
        url = self.base_url + path
        if query:
            url += "?" + parse.urlencode(query)
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = {
            "Accept": "application/json",
            "X-Platform-Internal-Token": self.internal_token,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            with self.opener(
                request.Request(url, data=body, headers=headers, method=method), timeout=30
            ) as response:
                raw = response.read(self.max_response_bytes + 1)
        except error.HTTPError as exc:
            raise ControlPlaneError(f"control plane returned HTTP {exc.code}") from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise ControlPlaneError("control plane request failed") from exc
        if len(raw) > self.max_response_bytes:
            raise ControlPlaneError("control plane response exceeded the configured limit")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ControlPlaneError("control plane returned invalid JSON") from exc


class YtDlpMetadataResolver:
    def __init__(
        self,
        config: MetadataEnrichmentWorkerConfig,
        *,
        popen: Callable[..., subprocess.Popen[bytes]] = subprocess.Popen,
        monotonic: Callable[[], float] = time.monotonic,
        terminate: Callable[[subprocess.Popen[bytes]], None] | None = None,
    ) -> None:
        self.timeout_seconds = config.resolver_timeout_seconds
        self.max_stdout_bytes = config.resolver_max_stdout_bytes
        self.max_stderr_bytes = config.resolver_max_stderr_bytes
        self.popen = popen
        self.monotonic = monotonic
        self.terminate = terminate or _terminate_process_group

    def resolve(
        self,
        canonical_url: str,
        *,
        heartbeat: Callable[[], None],
        cancelled: Callable[[], bool],
    ) -> EnrichmentMetadata:
        expected_video_id = _youtube_video_id(canonical_url)
        command = [
            "yt-dlp",
            "--ignore-config",
            "--dump-single-json",
            "--skip-download",
            "--no-playlist",
            "--no-warnings",
            "--no-cache-dir",
            "--",
            canonical_url,
        ]
        try:
            process = self.popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                env=_resolver_environment(),
            )
        except FileNotFoundError as exc:
            raise MetadataResolverError(
                "metadata_resolver_unavailable", "metadata resolver is unavailable", retryable=True
            ) from exc
        deadline = self.monotonic() + self.timeout_seconds
        stdout_buffer = bytearray()
        stderr_buffer = bytearray()
        output_limit_exceeded = threading.Event()
        readers = (
            threading.Thread(
                target=_read_bounded_pipe,
                args=(process.stdout, stdout_buffer, self.max_stdout_bytes, output_limit_exceeded),
                daemon=True,
            ),
            threading.Thread(
                target=_read_bounded_pipe,
                args=(process.stderr, stderr_buffer, self.max_stderr_bytes, output_limit_exceeded),
                daemon=True,
            ),
        )
        for reader in readers:
            reader.start()
        try:
            while process.poll() is None:
                if cancelled():
                    self.terminate(process)
                    raise MetadataEnrichmentShutdown("metadata enrichment worker is stopping")
                if output_limit_exceeded.is_set():
                    self.terminate(process)
                    raise MetadataResolverError(
                        "metadata_provider_output_limit_exceeded",
                        "metadata provider output exceeded the configured limit",
                        retryable=False,
                    )
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    self.terminate(process)
                    raise MetadataResolverError(
                        "metadata_provider_timeout", "metadata provider resolution timed out", retryable=True
                    )
                heartbeat()
                time.sleep(min(0.05, remaining))
        except BaseException:
            if process.poll() is None:
                self.terminate(process)
            raise
        finally:
            for reader in readers:
                reader.join(timeout=1)
        if output_limit_exceeded.is_set():
            raise MetadataResolverError(
                "metadata_provider_output_limit_exceeded",
                "metadata provider output exceeded the configured limit",
                retryable=False,
            )
        if any(reader.is_alive() for reader in readers):
            raise MetadataResolverError(
                "metadata_provider_invalid_response",
                "metadata provider output did not terminate cleanly",
                retryable=True,
            )
        stdout = bytes(stdout_buffer)
        stderr = bytes(stderr_buffer)
        if process.returncode != 0:
            raise _classify_yt_dlp_failure(stderr)
        return _metadata_from_yt_dlp(stdout, expected_video_id=expected_video_id)


class MetadataResolver(Protocol):
    def resolve(
        self,
        canonical_url: str,
        *,
        heartbeat: Callable[[], None],
        cancelled: Callable[[], bool],
    ) -> EnrichmentMetadata: ...


class MetadataEnrichmentWorker:
    def __init__(
        self,
        config: MetadataEnrichmentWorkerConfig,
        *,
        control: MetadataEnrichmentControlClient,
        resolver: MetadataResolver,
        stop_event: threading.Event | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.control = control
        self.resolver = resolver
        self.stop_event = stop_event or threading.Event()
        self.monotonic = monotonic

    def run_once(self) -> int:
        jobs = self.control.list_queue(page_size=self.config.concurrency)
        if not jobs or self.stop_event.is_set():
            return 0
        with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
            return sum(executor.map(self._claim_and_run, jobs[: self.config.concurrency]))

    def run_forever(self, *, max_idle_polls: int | None = None) -> int:
        idle_polls = 0
        processed = 0
        while not self.stop_event.is_set() and (max_idle_polls is None or idle_polls < max_idle_polls):
            try:
                count = self.run_once()
            except Exception:
                _LOGGER.exception("%s queue poll failed", _LOG_MARKER)
                count = 0
            processed += count
            idle_polls = 0 if count else idle_polls + 1
            if max_idle_polls is not None and idle_polls >= max_idle_polls:
                break
            self.stop_event.wait(self.config.poll_interval_seconds)
        return processed

    def _claim_and_run(self, queued: EnrichmentJob) -> int:
        if self.stop_event.is_set():
            return 0
        try:
            claim = self.control.claim(
                queued.enrichment_id,
                lease_owner=self.config.lease_owner,
                lease_seconds=self.config.lease_seconds,
            )
        except Exception:
            _LOGGER.info("%s claim skipped enrichment_id=%s", _LOG_MARKER, queued.enrichment_id)
            return 0
        self._run_claim(claim)
        return 1

    def _run_claim(self, claim: EnrichmentClaim) -> None:
        try:
            self.control.publish_progress(
                claim, {"stage": "resolving", "percent": 10, "message": "Resolving provider metadata"}
            )
            next_heartbeat = self.monotonic() + self.config.heartbeat_interval_seconds

            def heartbeat() -> None:
                nonlocal next_heartbeat
                current = self.monotonic()
                if current >= next_heartbeat:
                    self.control.publish_progress(
                        claim,
                        {"stage": "resolving", "percent": 50, "message": "Metadata resolver is running"},
                    )
                    next_heartbeat = current + self.config.heartbeat_interval_seconds

            metadata = self.resolver.resolve(
                claim.enrichment.canonical_url,
                heartbeat=heartbeat,
                cancelled=self.stop_event.is_set,
            )
            if self.stop_event.is_set():
                raise MetadataEnrichmentShutdown("metadata enrichment worker is stopping")
            self.control.publish_progress(
                claim, {"stage": "validating", "percent": 90, "message": "Validating provider metadata"}
            )
            self.control.finalize_success(claim, metadata)
        except MetadataEnrichmentShutdown:
            _LOGGER.info(
                "%s interrupted enrichment_id=%s", _LOG_MARKER, claim.enrichment.enrichment_id
            )
        except ControlPlaneError:
            _LOGGER.exception(
                "%s lease/control failure enrichment_id=%s",
                _LOG_MARKER,
                claim.enrichment.enrichment_id,
            )
        except MetadataResolverError as exc:
            if self.stop_event.is_set():
                _LOGGER.info(
                    "%s interrupted enrichment_id=%s", _LOG_MARKER, claim.enrichment.enrichment_id
                )
                return
            _LOGGER.warning(
                "%s failed enrichment_id=%s code=%s retryable=%s",
                _LOG_MARKER,
                claim.enrichment.enrichment_id,
                exc.code,
                exc.retryable,
            )
            self.control.finalize_failure(claim, exc)
        except Exception:
            _LOGGER.exception(
                "%s unexpected failure enrichment_id=%s",
                _LOG_MARKER,
                claim.enrichment.enrichment_id,
            )
            self.control.finalize_failure(
                claim,
                MetadataResolverError(
                    "metadata_enrichment_worker_failed",
                    "metadata enrichment worker failed",
                    retryable=True,
                ),
            )


def _youtube_video_id(raw_url: str) -> str:
    parsed = parse.urlparse(raw_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.youtube.com"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/watch"
        or parsed.params
        or parsed.fragment
    ):
        raise ValueError("canonical_url must be a canonical YouTube HTTPS watch URL")
    query = parse.parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    if set(query) != {"v"} or len(query["v"]) != 1 or not _YOUTUBE_VIDEO_ID.fullmatch(query["v"][0]):
        raise ValueError("canonical_url must contain exactly one valid YouTube video id")
    canonical = f"https://www.youtube.com/watch?v={query['v'][0]}"
    if raw_url != canonical:
        raise ValueError("canonical_url is not in canonical form")
    return query["v"][0]


def _metadata_from_yt_dlp(raw: bytes, *, expected_video_id: str) -> EnrichmentMetadata:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise MetadataResolverError(
            "metadata_provider_invalid_response", "metadata provider returned invalid JSON", retryable=True
        ) from exc
    data = _mapping(payload, "metadata provider response")
    provider_video_id = data.get("id")
    if provider_video_id != expected_video_id:
        raise MetadataResolverError(
            "metadata_provider_identity_mismatch",
            "metadata provider returned a different media identity",
            retryable=False,
        )
    title_value = data.get("title")
    if not isinstance(title_value, str):
        raise MetadataResolverError(
            "metadata_provider_invalid_response", "metadata provider returned an invalid title", retryable=False
        )
    title = _sanitize_text(title_value, 200)
    if not title:
        raise MetadataResolverError(
            "metadata_provider_invalid_response", "metadata provider returned an empty title", retryable=False
        )
    duration_value = data.get("duration")
    invalid_duration = not isinstance(duration_value, (int, float)) or isinstance(duration_value, bool)
    if isinstance(duration_value, float) and not math.isfinite(duration_value):
        invalid_duration = True
    if not invalid_duration and (duration_value < 0 or duration_value > 31 * 24 * 60 * 60):
        invalid_duration = True
    if invalid_duration:
        raise MetadataResolverError(
            "metadata_provider_invalid_response",
            "metadata provider returned an invalid duration",
            retryable=False,
        )
    thumbnail_value = data.get("thumbnail", "")
    if thumbnail_value is None:
        thumbnail_value = ""
    if not isinstance(thumbnail_value, str):
        raise MetadataResolverError(
            "metadata_provider_invalid_response",
            "metadata provider returned an invalid thumbnail URL",
            retryable=False,
        )
    thumbnail_url = _sanitize_thumbnail_url(thumbnail_value)
    performer = ""
    for field in ("artist", "creator", "uploader", "channel"):
        raw_performer = data.get(field)
        if isinstance(raw_performer, str):
            performer = _sanitize_text(raw_performer, 200)
            if performer:
                break
    if not performer:
        performer = "YouTube"
    return EnrichmentMetadata(
        title=title,
        thumbnail_url=thumbnail_url,
        duration_seconds=int(duration_value + 0.5),
        performer=performer,
    )


def _sanitize_thumbnail_url(raw: str) -> str:
    trimmed = raw.strip()
    if not trimmed:
        return ""
    if len(trimmed) > 2048:
        raise MetadataResolverError(
            "metadata_provider_invalid_response", "metadata thumbnail URL is too long", retryable=False
        )
    parsed = parse.urlparse(trimmed)
    hostname = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or not _YOUTUBE_THUMBNAIL_HOST.fullmatch(hostname)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.fragment
    ):
        raise MetadataResolverError(
            "metadata_provider_invalid_response",
            "metadata provider returned a disallowed thumbnail URL",
            retryable=False,
        )
    return parsed.geturl()


def _sanitize_text(raw: str, max_chars: int) -> str:
    cleaned = " ".join(
        "".join(" " if unicodedata.category(char).startswith("C") else char for char in raw).split()
    )
    return cleaned[:max_chars].strip()


def _resolver_environment() -> dict[str, str]:
    env = {"PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"), "LANG": "C.UTF-8"}
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        value = os.environ.get(name)
        if value:
            env[name] = value
    return env


class _NoRedirectHandler(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _open_without_redirects(http_request: request.Request, *, timeout: float):
    return request.build_opener(_NoRedirectHandler()).open(http_request, timeout=timeout)


def _read_bounded_pipe(
    pipe: object,
    destination: bytearray,
    limit: int,
    output_limit_exceeded: threading.Event,
) -> None:
    if pipe is None or not hasattr(pipe, "read"):
        output_limit_exceeded.set()
        return
    try:
        while not output_limit_exceeded.is_set():
            remaining = limit + 1 - len(destination)
            if remaining <= 0:
                output_limit_exceeded.set()
                return
            chunk = pipe.read(min(64 * 1024, remaining))
            if not chunk:
                return
            destination.extend(chunk)
            if len(destination) > limit:
                output_limit_exceeded.set()
                return
    except OSError:
        output_limit_exceeded.set()


def _classify_yt_dlp_failure(stderr: bytes) -> MetadataResolverError:
    diagnostic = stderr[:4096].decode("utf-8", errors="replace").lower()
    retryable = not any(marker in diagnostic for marker in _TERMINAL_PROVIDER_ERRORS)
    code = "metadata_provider_unavailable" if retryable else "metadata_provider_media_unavailable"
    return MetadataResolverError(code, "metadata provider resolution failed", retryable=retryable)


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            return


def _mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise MetadataResolverError(
            "metadata_provider_invalid_response", f"{context} must be an object", retryable=True
        )
    return value


def _string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context} must be a non-empty string")
    return value


def _integer_field(value: object, context: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{context} must be an integer")
    return value


def _timestamp(value: object, context: str) -> datetime:
    raw = _string(value, context)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{context} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{context} must include a timezone")
    return parsed


def _number(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError("environment value must be numeric") from exc
    if not math.isfinite(parsed):
        raise ValueError("environment value must be finite")
    return parsed


def _integer(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError("environment value must be an integer") from exc
