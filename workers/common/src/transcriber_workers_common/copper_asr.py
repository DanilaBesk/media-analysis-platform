# FILE: workers/common/src/transcriber_workers_common/copper_asr.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide the CopperASR HTTP runtime boundary used by the transcription worker.
# SCOPE: CopperASR client config, multipart transport, response normalization, provider error mapping, and URL audio materialization.
# DEPENDS: M-WORKER-COMMON, M-COPPER-ASR
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION, media-b8s.1.5
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added CopperASR HTTP transcriber boundary as the only active ASR path.
# END_CHANGE_SUMMARY
# START_MODULE_MAP
#   CopperAsrClientConfig - Immutable CopperASR client/env configuration.
#   CopperAsrTranscriptionError - Stable worker-visible provider failure.
#   CopperAsrHttpTransport - HTTP multipart transport for the CopperASR runtime.
#   CopperAsrHttpTranscriber - Maps SourceCandidate inputs into CopperASR TranscriptResult outputs.
# END_MODULE_MAP

from __future__ import annotations

import json
import logging
import mimetypes
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol
from uuid import uuid4

import httpx

from transcriber_workers_common.domain import SourceCandidate, TranscriptResult, TranscriptSegment

_LOGGER = logging.getLogger(__name__)
_LOG_MARKER_COPPER_ASR_TRANSCRIBE = "[WorkerCommon][CopperASR][BLOCK_TRANSCRIBE_WITH_COPPER_ASR]"

_DEFAULT_BASE_URL = "http://copper-asr:8000"
_DEFAULT_TIMEOUT_SECONDS = 28800.0
_DEFAULT_LANGUAGE = "ru"
_DEFAULT_PAUSE_THRESHOLD = 2.0

_PROVIDER_ERROR_MAP = {
    "invalid_params": ("asr_invalid_params", False),
    "upload_too_large": ("asr_upload_too_large", False),
    "invalid_audio": ("asr_invalid_audio", False),
    "empty_transcript": ("asr_empty_transcript", False),
    "runtime_unavailable": ("asr_runtime_unavailable", True),
    "busy_runtime_unavailable": ("asr_runtime_busy", True),
    "request_timeout": ("asr_request_timeout", True),
    "unexpected_runtime_error": ("asr_unexpected_runtime_error", True),
}

__all__ = [
    "CopperAsrClientConfig",
    "CopperAsrHttpTranscriber",
    "CopperAsrHttpTransport",
    "CopperAsrTransport",
    "CopperAsrTranscriptionError",
]


class CopperAsrTransport(Protocol):
    def transcribe(
        self,
        audio_path: Path,
        *,
        params: Mapping[str, object],
        timeout_seconds: float,
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class CopperAsrClientConfig:
    base_url: str = _DEFAULT_BASE_URL
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    language: str = _DEFAULT_LANGUAGE
    pause_threshold_seconds: float = _DEFAULT_PAUSE_THRESHOLD
    diarization: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "CopperAsrClientConfig":
        return cls(
            base_url=_env_str(env, "COPPER_ASR_BASE_URL", _DEFAULT_BASE_URL),
            timeout_seconds=_env_float(env, "COPPER_ASR_CLIENT_TIMEOUT_S", _DEFAULT_TIMEOUT_SECONDS),
            language=_env_str(env, "COPPER_ASR_LANGUAGE", _DEFAULT_LANGUAGE),
            pause_threshold_seconds=_env_float(env, "COPPER_ASR_PAUSE_THRESHOLD_S", _DEFAULT_PAUSE_THRESHOLD),
            diarization=_env_bool(env, "COPPER_ASR_DIARIZATION", False),
        )


class CopperAsrTranscriptionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        diagnostic_code: str,
        provider_code: str | None = None,
        status_code: int | None = None,
        retryable: bool = False,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.diagnostic_code = diagnostic_code
        self.provider_code = provider_code
        self.status_code = status_code
        self.retryable = retryable
        self.request_id = request_id

    @property
    def suppress_worker_traceback(self) -> bool:
        return not self.retryable


class CopperAsrHttpTransport:
    def __init__(self, base_url: str, *, client: httpx.Client | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = client

    def transcribe(
        self,
        audio_path: Path,
        *,
        params: Mapping[str, object],
        timeout_seconds: float,
    ) -> Mapping[str, object]:
        url = f"{self.base_url}/transcribe"
        request_id = uuid4().hex
        try:
            with audio_path.open("rb") as audio_file:
                files = {"file": (audio_path.name, audio_file, _guess_mime_type(audio_path))}
                data = {"params": json.dumps(dict(params), ensure_ascii=False, sort_keys=True)}
                if self._client is None:
                    with httpx.Client(timeout=timeout_seconds) as client:
                        response = client.post(url, data=data, files=files, headers={"x-request-id": request_id})
                else:
                    response = self._client.post(url, data=data, files=files, headers={"x-request-id": request_id})
        except httpx.TimeoutException as exc:
            raise CopperAsrTranscriptionError(
                "CopperASR transport timed out",
                diagnostic_code="asr_transport_unavailable",
                provider_code="transport_timeout",
                retryable=True,
                request_id=request_id,
            ) from exc
        except httpx.HTTPError as exc:
            raise CopperAsrTranscriptionError(
                f"CopperASR transport failed: {exc.__class__.__name__}",
                diagnostic_code="asr_transport_unavailable",
                provider_code="transport_error",
                retryable=True,
                request_id=request_id,
            ) from exc

        if response.status_code >= 400:
            _raise_provider_error(response, request_id=request_id)
        return _response_json(response, request_id=request_id)


class CopperAsrHttpTranscriber:
    def __init__(
        self,
        config: CopperAsrClientConfig,
        *,
        transport: CopperAsrTransport | None = None,
    ) -> None:
        self.config = config
        self.transport = transport or CopperAsrHttpTransport(config.base_url)

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        audio_path = source.local_path
        if audio_path is None and source.url:
            audio_path = _download_youtube_audio(source.url, workspace_dir)
        if audio_path is None:
            raise ValueError("CopperASR transcriber requires either a local file or a URL source")

        started_at = time.perf_counter()
        params = {
            "language": self.config.language,
            "pause_threshold": self.config.pause_threshold_seconds,
            "diarization": self.config.diarization,
        }
        try:
            payload = self.transport.transcribe(
                audio_path,
                params=params,
                timeout_seconds=self.config.timeout_seconds,
            )
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            result = _transcript_from_payload(payload, source=source, audio_path=audio_path)
            _LOGGER.info(
                "%s provider=%s model=%s source_id=%s elapsed_ms=%s segment_count=%s",
                _LOG_MARKER_COPPER_ASR_TRANSCRIBE,
                payload.get("provider", "copperasr"),
                payload.get("model", "unknown"),
                source.source_id,
                elapsed_ms,
                len(result.segments),
            )
            return result
        except CopperAsrTranscriptionError as exc:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            _LOGGER.warning(
                "%s failed source_id=%s diagnostic_code=%s provider_code=%s status_code=%s retryable=%s elapsed_ms=%s",
                _LOG_MARKER_COPPER_ASR_TRANSCRIBE,
                source.source_id,
                exc.diagnostic_code,
                exc.provider_code,
                exc.status_code,
                exc.retryable,
                elapsed_ms,
            )
            raise


def _transcript_from_payload(
    payload: Mapping[str, object],
    *,
    source: SourceCandidate,
    audio_path: Path,
) -> TranscriptResult:
    raw_text = str(payload.get("text") or "").strip()
    if not raw_text:
        raise CopperAsrTranscriptionError(
            "CopperASR returned an empty transcript",
            diagnostic_code="asr_empty_transcript",
            provider_code="empty_transcript",
            retryable=False,
        )

    segments = _segments_from_payload(payload, fallback_text=raw_text)
    title = source.file_name or source.display_name or audio_path.name
    return TranscriptResult(
        title=title,
        source_label=source.display_name,
        segments=segments,
        language=str(payload.get("language") or "unknown"),
        raw_text=raw_text,
        provider_metadata=_provider_metadata(payload),
    )


def _segments_from_payload(payload: Mapping[str, object], *, fallback_text: str) -> list[TranscriptSegment]:
    raw_segments = payload.get("segments")
    if isinstance(raw_segments, list):
        segments = [_segment_from_mapping(item) for item in raw_segments if isinstance(item, Mapping)]
        segments = [segment for segment in segments if segment.text.strip()]
        if segments:
            return segments

    raw_words = payload.get("words")
    if isinstance(raw_words, list):
        word_bounds = [_word_bounds(item) for item in raw_words if isinstance(item, Mapping)]
        word_bounds = [bounds for bounds in word_bounds if bounds is not None]
        if word_bounds:
            return [
                TranscriptSegment(
                    start_seconds=word_bounds[0][0],
                    end_seconds=word_bounds[-1][1],
                    text=fallback_text,
                    speaker=_single_speaker(raw_words),
                )
            ]

    duration = _optional_float(payload.get("duration"), default=0.0)
    return [TranscriptSegment(start_seconds=0.0, end_seconds=max(0.0, duration), text=fallback_text)]


def _segment_from_mapping(item: Mapping[str, object]) -> TranscriptSegment:
    start_seconds, end_seconds = _segment_bounds(item)
    return TranscriptSegment(
        start_seconds=start_seconds,
        end_seconds=end_seconds,
        text=str(item.get("text") or "").strip(),
        speaker=_optional_str(item.get("speaker")),
    )


def _segment_bounds(item: Mapping[str, object]) -> tuple[float, float]:
    timestamp = item.get("timestamp")
    if isinstance(timestamp, (list, tuple)) and len(timestamp) >= 2:
        return (
            _optional_float(timestamp[0], default=0.0),
            _optional_float(timestamp[1], default=0.0),
        )
    return (
        _optional_float(item.get("start"), default=0.0),
        _optional_float(item.get("end"), default=0.0),
    )


def _word_bounds(item: Mapping[str, object]) -> tuple[float, float] | None:
    text = str(item.get("text") or "").strip()
    if not text:
        return None
    return (
        _optional_float(item.get("start"), default=0.0),
        _optional_float(item.get("end"), default=0.0),
    )


def _single_speaker(raw_words: list[object]) -> str | None:
    speakers = {
        str(item.get("speaker")).strip()
        for item in raw_words
        if isinstance(item, Mapping) and item.get("speaker") is not None and str(item.get("speaker")).strip()
    }
    if len(speakers) == 1:
        return next(iter(speakers))
    return None


def _raise_provider_error(response: httpx.Response, *, request_id: str) -> None:
    payload = _safe_response_json(response)
    error_payload = payload.get("error") if isinstance(payload, Mapping) else None
    if isinstance(error_payload, Mapping):
        provider_code = str(error_payload.get("code") or f"http_{response.status_code}")
        message = str(error_payload.get("message") or "CopperASR runtime failed")
        response_request_id = _optional_str(error_payload.get("request_id")) or request_id
    else:
        provider_code = f"http_{response.status_code}"
        message = f"CopperASR runtime returned HTTP {response.status_code}"
        response_request_id = request_id

    diagnostic_code, retryable = _PROVIDER_ERROR_MAP.get(
        provider_code,
        ("asr_runtime_error", response.status_code >= 500),
    )
    raise CopperAsrTranscriptionError(
        f"CopperASR {provider_code}: {message}",
        diagnostic_code=diagnostic_code,
        provider_code=provider_code,
        status_code=response.status_code,
        retryable=retryable,
        request_id=response_request_id,
    )


def _response_json(response: httpx.Response, *, request_id: str) -> Mapping[str, object]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise CopperAsrTranscriptionError(
            "CopperASR returned malformed JSON",
            diagnostic_code="asr_bad_response",
            provider_code="malformed_json",
            status_code=response.status_code,
            retryable=True,
            request_id=request_id,
        ) from exc
    if not isinstance(payload, Mapping):
        raise CopperAsrTranscriptionError(
            "CopperASR returned a non-object JSON payload",
            diagnostic_code="asr_bad_response",
            provider_code="bad_json_shape",
            status_code=response.status_code,
            retryable=True,
            request_id=request_id,
        )
    return payload


def _safe_response_json(response: httpx.Response) -> Mapping[str, object]:
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _provider_metadata(payload: Mapping[str, object]) -> Mapping[str, object]:
    metadata = payload.get("metadata")
    return {
        "provider": str(payload.get("provider") or "copperasr"),
        "model": str(payload.get("model") or "unknown"),
        "revision": _optional_str(payload.get("revision")),
        "duration": _json_safe_value(payload.get("duration")),
        "metadata": _json_safe_value(metadata if isinstance(metadata, Mapping) else {}),
    }


def _download_youtube_audio(url: str, workspace_dir: Path) -> Path:
    output_template = workspace_dir / "source.%(ext)s"
    command = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--no-playlist",
        "-x",
        "--audio-format",
        "mp3",
        "-o",
        str(output_template),
        url,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False, timeout=900)
    if completed.returncode != 0:
        raise RuntimeError(f"yt-dlp failed with exit code {completed.returncode}: {completed.stderr.strip()}")

    for candidate in workspace_dir.glob("source.*"):
        if candidate.suffix != ".part":
            return candidate
    raise RuntimeError("yt-dlp finished without producing an audio file")


def _guess_mime_type(path: Path) -> str:
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def _env_str(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key, "").strip()
    return value or default


def _env_float(env: Mapping[str, str], key: str, default: float) -> float:
    value = env.get(key, "").strip()
    if not value:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return parsed


def _env_bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    value = env.get(key, "").strip().casefold()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{key} must be a boolean")


def _optional_float(value: object, *, default: float) -> float:
    if isinstance(value, bool) or value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except ValueError:
        return default


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_safe_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    return str(value)
