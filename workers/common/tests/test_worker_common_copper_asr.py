# FILE: workers/common/tests/test_worker_common_copper_asr.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the CopperASR worker-common runtime boundary.
# SCOPE: env config, HTTP transport error mapping, response normalization, and URL materialization.
# DEPENDS: M-WORKER-COMMON, M-COPPER-ASR
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION, media-b8s.1.5
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added CopperASR provider-boundary regression coverage.
# END_CHANGE_SUMMARY

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping

import httpx
import pytest

import transcriber_workers_common.copper_asr as copper_module
from transcriber_workers_common.copper_asr import (
    CopperAsrClientConfig,
    CopperAsrHttpTranscriber,
    CopperAsrHttpTransport,
    CopperAsrTranscriptionError,
)
from transcriber_workers_common.domain import SourceCandidate


class RecordingTransport:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def transcribe(
        self,
        audio_path: Path,
        *,
        params: Mapping[str, object],
        timeout_seconds: float,
    ) -> Mapping[str, object]:
        self.calls.append({"audio_path": audio_path, "params": dict(params), "timeout_seconds": timeout_seconds})
        return self.payload


def _source(path: Path | None = None) -> SourceCandidate:
    return SourceCandidate(
        source_id="source-1",
        kind="telegram_audio",
        display_name="Audio: voice.ogg",
        url=None,
        telegram_file_id="telegram-file-1",
        mime_type="audio/ogg",
        file_name="voice.ogg",
        local_path=path,
    )


def test_config_from_env_uses_copper_asr_knobs() -> None:
    config = CopperAsrClientConfig.from_env(
        {
            "COPPER_ASR_BASE_URL": "http://asr:9000/",
            "COPPER_ASR_CLIENT_TIMEOUT_S": "123.5",
            "COPPER_ASR_LANGUAGE": "ru",
            "COPPER_ASR_PAUSE_THRESHOLD_S": "1.25",
            "COPPER_ASR_DIARIZATION": "true",
        }
    )

    assert config.base_url == "http://asr:9000/"
    assert config.timeout_seconds == 123.5
    assert config.language == "ru"
    assert config.pause_threshold_seconds == 1.25
    assert config.diarization is True


def test_transcriber_maps_copper_asr_segments(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    transport = RecordingTransport(
        {
            "text": "Привет мир",
            "language": "ru",
            "provider": "copperasr",
            "model": "Copperside/CoppersideASR",
            "segments": [
                {"timestamp": [0.0, 1.2], "text": "Привет", "speaker": "S1"},
                {"timestamp": [1.2, 2.5], "text": "мир"},
            ],
            "words": [],
            "duration": 2.5,
            "metadata": {"ignored_params": ["beam_size"], "diarization": {"enabled": False}},
        }
    )
    transcriber = CopperAsrHttpTranscriber(
        CopperAsrClientConfig(base_url="http://asr", timeout_seconds=5.0, pause_threshold_seconds=1.5),
        transport=transport,
    )

    result = transcriber.transcribe(_source(audio), tmp_path)

    assert result.title == "voice.ogg"
    assert result.source_label == "Audio: voice.ogg"
    assert result.language == "ru"
    assert result.raw_text == "Привет мир"
    assert [(segment.start_seconds, segment.end_seconds, segment.text, segment.speaker) for segment in result.segments] == [
        (0.0, 1.2, "Привет", "S1"),
        (1.2, 2.5, "мир", None),
    ]
    assert result.provider_metadata == {
        "provider": "copperasr",
        "model": "Copperside/CoppersideASR",
        "revision": None,
        "duration": 2.5,
        "metadata": {"ignored_params": ["beam_size"], "diarization": {"enabled": False}},
    }
    assert transport.calls == [
        {
            "audio_path": audio,
            "params": {"language": "ru", "pause_threshold": 1.5, "diarization": False},
            "timeout_seconds": 5.0,
        }
    ]


def test_transcriber_logs_copper_asr_processing_telemetry(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    transport = RecordingTransport(
        {
            "text": "Привет",
            "provider": "copperasr",
            "model": "Copperside/CoppersideASR",
            "segments": [{"timestamp": [0.0, 1.0], "text": "Привет"}],
            "metadata": {
                "processing": {
                    "audio_duration_s": 1501.9595625,
                    "audio_preparation_s": 1.4432668829686008,
                    "vad_s": 5.327543514024001,
                    "asr_inference_s": 87.42638859117869,
                    "total_s": 94.5157656900119,
                    "chunk_count": 86,
                    "vad_segment_count": 86,
                    "word_count": 2834,
                }
            },
        }
    )
    transcriber = CopperAsrHttpTranscriber(CopperAsrClientConfig(), transport=transport)

    caplog.set_level(logging.INFO, logger="transcriber_workers_common.copper_asr")
    transcriber.transcribe(_source(audio), tmp_path)

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "processing_total_s=94.5157656900119" in message
        and "audio_duration_s=1501.9595625" in message
        and "audio_preparation_s=1.4432668829686008" in message
        and "vad_s=5.327543514024001" in message
        and "asr_inference_s=87.42638859117869" in message
        and "chunk_count=86" in message
        and "vad_segment_count=86" in message
        and "word_count=2834" in message
        for message in messages
    )


def test_transcriber_builds_stable_segment_from_words_when_sentences_missing(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    transport = RecordingTransport(
        {
            "text": "hello world",
            "language": "en",
            "segments": [],
            "words": [
                {"start": 0.4, "end": 0.9, "text": "hello", "speaker": "A"},
                {"start": 1.0, "end": 1.4, "text": "world", "speaker": "A"},
            ],
        }
    )
    transcriber = CopperAsrHttpTranscriber(CopperAsrClientConfig(), transport=transport)

    result = transcriber.transcribe(_source(audio), tmp_path)

    assert len(result.segments) == 1
    assert result.segments[0].start_seconds == 0.4
    assert result.segments[0].end_seconds == 1.4
    assert result.segments[0].text == "hello world"
    assert result.segments[0].speaker == "A"


def test_transcriber_rejects_empty_copper_asr_text(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    transcriber = CopperAsrHttpTranscriber(CopperAsrClientConfig(), transport=RecordingTransport({"text": "  "}))

    with pytest.raises(CopperAsrTranscriptionError) as exc_info:
        transcriber.transcribe(_source(audio), tmp_path)

    assert exc_info.value.diagnostic_code == "asr_empty_transcript"
    assert exc_info.value.provider_code == "empty_transcript"


def test_transcriber_materializes_youtube_url_before_copper_asr_call(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    audio = tmp_path / "downloaded.mp3"
    audio.write_bytes(b"audio")
    transport = RecordingTransport({"text": "downloaded", "language": "ru", "segments": []})
    monkeypatch.setattr(copper_module, "_download_youtube_audio", lambda url, workspace_dir: audio)
    transcriber = CopperAsrHttpTranscriber(CopperAsrClientConfig(), transport=transport)
    source = SourceCandidate(
        source_id="yt-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo123",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    result = transcriber.transcribe(source, tmp_path)

    assert result.raw_text == "downloaded"
    assert transport.calls[0]["audio_path"] == audio


def test_http_transport_maps_provider_error_codes(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/transcribe"
        assert request.headers["content-type"].startswith("multipart/form-data")
        return httpx.Response(
            503,
            json={"error": {"code": "busy_runtime_unavailable", "message": "busy", "request_id": "req-1"}},
        )

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://asr")
    transport = CopperAsrHttpTransport("http://asr", client=client)

    with pytest.raises(CopperAsrTranscriptionError) as exc_info:
        transport.transcribe(audio, params={"language": "ru"}, timeout_seconds=5.0)

    assert exc_info.value.diagnostic_code == "asr_runtime_busy"
    assert exc_info.value.provider_code == "busy_runtime_unavailable"
    assert exc_info.value.status_code == 503
    assert exc_info.value.retryable is True
    assert exc_info.value.request_id == "req-1"


def test_http_transport_rejects_malformed_success_payload(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text="not-json")))
    transport = CopperAsrHttpTransport("http://asr", client=client)

    with pytest.raises(CopperAsrTranscriptionError) as exc_info:
        transport.transcribe(audio, params={}, timeout_seconds=5.0)

    assert exc_info.value.diagnostic_code == "asr_bad_response"
    assert exc_info.value.provider_code == "malformed_json"


def test_http_transport_uses_default_client_for_success_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    calls: list[dict[str, object]] = []

    class FakeClient:
        def __init__(self, *, timeout: float) -> None:
            self.timeout = timeout

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            return None

        def post(
            self,
            url: str,
            *,
            data: Mapping[str, object],
            files: Mapping[str, object],
            headers: Mapping[str, str],
        ) -> httpx.Response:
            calls.append(
                {"url": url, "data": data, "files": files, "headers": headers, "timeout": self.timeout}
            )
            return httpx.Response(200, json={"text": "ok", "segments": []})

    monkeypatch.setattr(copper_module.httpx, "Client", FakeClient)

    payload = CopperAsrHttpTransport("http://asr/").transcribe(
        audio,
        params={"language": "ru"},
        timeout_seconds=5.0,
    )

    assert payload == {"text": "ok", "segments": []}
    assert calls[0]["url"] == "http://asr/transcribe"
    assert calls[0]["data"] == {"params": '{"language": "ru"}'}
    assert calls[0]["timeout"] == 5.0
    assert calls[0]["headers"]["x-request-id"]


def test_transport_error_properties_and_retry_suppression() -> None:
    retryable = CopperAsrTranscriptionError(
        "retry later",
        diagnostic_code="asr_transport_unavailable",
        retryable=True,
    )
    terminal = CopperAsrTranscriptionError(
        "bad audio",
        diagnostic_code="asr_invalid_audio",
        retryable=False,
    )

    assert retryable.suppress_worker_traceback is False
    assert terminal.suppress_worker_traceback is True


def test_http_transport_maps_transport_exceptions(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")

    class TimeoutClient:
        def post(self, *args: object, **kwargs: object) -> httpx.Response:
            raise httpx.TimeoutException("slow")

    class BrokenClient:
        def post(self, *args: object, **kwargs: object) -> httpx.Response:
            raise httpx.ConnectError("offline")

    with pytest.raises(CopperAsrTranscriptionError) as timeout_exc:
        CopperAsrHttpTransport("http://asr", client=TimeoutClient()).transcribe(
            audio,
            params={"language": "ru"},
            timeout_seconds=5.0,
        )
    assert timeout_exc.value.provider_code == "transport_timeout"
    assert timeout_exc.value.retryable is True

    with pytest.raises(CopperAsrTranscriptionError) as http_exc:
        CopperAsrHttpTransport("http://asr", client=BrokenClient()).transcribe(
            audio,
            params={"language": "ru"},
            timeout_seconds=5.0,
        )
    assert http_exc.value.provider_code == "transport_error"
    assert http_exc.value.retryable is True


def test_http_transport_handles_provider_error_without_structured_payload(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(502, text="gateway down")))
    transport = CopperAsrHttpTransport("http://asr", client=client)

    with pytest.raises(CopperAsrTranscriptionError) as exc_info:
        transport.transcribe(audio, params={}, timeout_seconds=5.0)

    assert exc_info.value.diagnostic_code == "asr_runtime_error"
    assert exc_info.value.provider_code == "http_502"
    assert exc_info.value.status_code == 502
    assert exc_info.value.retryable is True


def test_http_transport_rejects_non_object_success_payload(tmp_path: Path) -> None:
    audio = tmp_path / "voice.unknown"
    audio.write_bytes(b"audio")
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=["not", "object"])))
    transport = CopperAsrHttpTransport("http://asr", client=client)

    with pytest.raises(CopperAsrTranscriptionError) as exc_info:
        transport.transcribe(audio, params={}, timeout_seconds=5.0)

    assert exc_info.value.diagnostic_code == "asr_bad_response"
    assert exc_info.value.provider_code == "bad_json_shape"


def test_transcriber_requires_local_file_or_url(tmp_path: Path) -> None:
    transcriber = CopperAsrHttpTranscriber(CopperAsrClientConfig(), transport=RecordingTransport({"text": "unused"}))
    source = _source(None)

    with pytest.raises(ValueError, match="local file or a URL"):
        transcriber.transcribe(source, tmp_path)


def test_transcriber_falls_back_to_start_end_duration_and_unset_speaker(tmp_path: Path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    transport = RecordingTransport(
        {
            "text": "hello from fallback",
            "segments": [
                {"start": "not-a-number", "end": True, "text": "   "},
            ],
            "words": [
                {"text": "   ", "start": 0.0, "end": 0.1, "speaker": "A"},
                {"text": "hello", "start": "0.5", "end": "1.0", "speaker": "A"},
                {"text": "fallback", "start": "bad", "end": None, "speaker": "B"},
            ],
            "duration": "not-a-duration",
            "provider": "",
            "model": "",
            "metadata": {"not_json_native": object(), "items": [object()]},
        }
    )
    transcriber = CopperAsrHttpTranscriber(CopperAsrClientConfig(), transport=transport)

    result = transcriber.transcribe(_source(audio), tmp_path)

    assert [
        (segment.start_seconds, segment.end_seconds, segment.text, segment.speaker)
        for segment in result.segments
    ] == [(0.5, 0.0, "hello from fallback", None)]
    assert result.provider_metadata["provider"] == "copperasr"
    assert result.provider_metadata["model"] == "unknown"
    assert result.provider_metadata["duration"] == "not-a-duration"
    metadata = result.provider_metadata["metadata"]
    assert isinstance(metadata, dict)
    assert "object object" in metadata["not_json_native"]
    assert "object object" in metadata["items"][0]


def test_download_youtube_audio_success_and_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def successful_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        assert command[-1] == "https://youtu.be/demo123"
        (tmp_path / "source.part").write_bytes(b"partial")
        (tmp_path / "source.mp3").write_bytes(b"audio")
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(copper_module.subprocess, "run", successful_run)

    assert copper_module._download_youtube_audio("https://youtu.be/demo123", tmp_path) == tmp_path / "source.mp3"

    def failing_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=1, stderr="network down")

    monkeypatch.setattr(copper_module.subprocess, "run", failing_run)

    with pytest.raises(RuntimeError, match="network down"):
        copper_module._download_youtube_audio("https://youtu.be/demo123", tmp_path)

    for candidate in tmp_path.glob("source.*"):
        candidate.unlink()

    monkeypatch.setattr(
        copper_module.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(returncode=0, stderr=""),
    )

    with pytest.raises(RuntimeError, match="without producing an audio file"):
        copper_module._download_youtube_audio("https://youtu.be/demo123", tmp_path)


def test_config_from_env_defaults_and_validation() -> None:
    default_config = CopperAsrClientConfig.from_env({})
    assert default_config.base_url == "http://copper-asr:8000"
    assert default_config.timeout_seconds == 28800.0
    assert default_config.pause_threshold_seconds == 2.0
    assert default_config.diarization is False

    false_config = CopperAsrClientConfig.from_env({"COPPER_ASR_DIARIZATION": "off"})
    assert false_config.diarization is False

    with pytest.raises(ValueError, match="COPPER_ASR_CLIENT_TIMEOUT_S must be a number"):
        CopperAsrClientConfig.from_env({"COPPER_ASR_CLIENT_TIMEOUT_S": "slow"})
    with pytest.raises(ValueError, match="COPPER_ASR_PAUSE_THRESHOLD_S must be greater than zero"):
        CopperAsrClientConfig.from_env({"COPPER_ASR_PAUSE_THRESHOLD_S": "0"})
    with pytest.raises(ValueError, match="COPPER_ASR_DIARIZATION must be a boolean"):
        CopperAsrClientConfig.from_env({"COPPER_ASR_DIARIZATION": "maybe"})
