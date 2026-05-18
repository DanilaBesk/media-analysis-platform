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

from pathlib import Path
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
