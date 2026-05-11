# FILE: workers/common/tests/test_worker_common_transcribers.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the worker-common transcription helpers preserve the current subtitle and Whisper behavior.
# SCOPE: Speaker extraction, subtitle mapping, and Whisper fallback behavior.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local transcription helper regression coverage under worker-common.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   FakeFetchedTranscript - Provides deterministic YouTube transcript fixtures.
#   test_extract_speaker_returns_cleaned_segments - Verifies speaker extraction normalization.
#   test_youtube_transcriber_maps_segments_and_speakers - Verifies subtitle mapping output.
#   test_default_transcriber_falls_back_to_whisper - Verifies subtitle fallback behavior.
# END_MODULE_MAP

from __future__ import annotations

import builtins
from pathlib import Path
import sys
from types import ModuleType

import pytest

import transcriber_workers_common.transcribers as transcribers_module
from transcriber_workers_common.transcribers import (
    DefaultTranscriber,
    YouTubeTranscriptTranscriber,
    WhisperTranscriber,
    _build_podlodka_converter,
    _download_podlodka_snapshot,
    _extract_speaker,
)
from transcriber_workers_common.domain import SourceCandidate, TranscriptResult, TranscriptSegment


class FakeFetchedTranscript(list):
    language_code = "en"


def test_extract_speaker_returns_cleaned_segments() -> None:
    assert _extract_speaker("Alice: Hello world") == ("Alice", "Hello world")
    assert _extract_speaker("  No prefix  ") == (None, "No prefix")
    assert _extract_speaker("\n\n") == (None, "")


def test_youtube_transcriber_maps_segments_and_speakers(tmp_path: Path, monkeypatch) -> None:
    transcriber = YouTubeTranscriptTranscriber(("en",))
    fake_transcript = FakeFetchedTranscript(
        [
            {"text": "Alice: Hello", "start": 1.0, "duration": 2.0},
            {"text": "General update", "start": 3.0, "duration": 4.0},
        ]
    )
    monkeypatch.setattr(transcriber._api, "fetch", lambda *args, **kwargs: fake_transcript)
    source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo123",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    result = transcriber.transcribe(source, tmp_path)

    assert result.language == "en"
    assert result.segments[0].speaker == "Alice"
    assert result.segments[0].text == "Hello"
    assert result.segments[1].speaker is None


def test_default_transcriber_falls_back_to_whisper(tmp_path: Path, monkeypatch) -> None:
    default = DefaultTranscriber(
        youtube_languages=("en",),
        whisper_model="bond005/whisper-podlodka-turbo",
        whisper_device="auto",
        whisper_compute_type="default",
    )
    expected = TranscriptResult(
        title="Fallback",
        source_label="YouTube: demo",
        segments=[TranscriptSegment(start_seconds=0, end_seconds=1, text="Recovered")],
        language="en",
        raw_text="Recovered",
    )
    monkeypatch.setattr(default.youtube_transcriber, "transcribe", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no subtitles")))
    monkeypatch.setattr(default.whisper_transcriber, "transcribe", lambda *args, **kwargs: expected)
    source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo123",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )

    result = default.transcribe(source, tmp_path)

    assert result == expected


def test_whisper_transcriber_downloads_youtube_audio_before_transcribing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    transcriber = WhisperTranscriber(model_name="tiny", device="cpu", compute_type="int8")
    audio_path = tmp_path / "source.mp3"
    audio_path.write_bytes(b"demo")
    source = SourceCandidate(
        source_id="src-yt",
        kind="youtube_url",
        display_name="YouTube: demo",
        url="https://youtu.be/demo123",
        telegram_file_id=None,
        mime_type=None,
        file_name="clip.mp3",
    )
    expected = TranscriptResult(
        title="clip.mp3",
        source_label="YouTube: demo",
        segments=[TranscriptSegment(start_seconds=0, end_seconds=1, text="Recovered")],
        language="en",
        raw_text="Recovered",
    )

    monkeypatch.setattr(transcribers_module, "_download_youtube_audio", lambda url, workspace_dir: audio_path)
    monkeypatch.setattr(transcriber, "_get_model", lambda workspace_dir: type("Model", (), {"transcribe": lambda self, path, **kwargs: ([type("Segment", (), {"text": "Recovered", "start": 0, "end": 1})()], type("Info", (), {"language": "en"})())})())

    result = transcriber.transcribe(source, tmp_path)

    assert result == expected


def test_podlodka_snapshot_requires_huggingface_hub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__
    monkeypatch.delitem(sys.modules, "huggingface_hub", raising=False)

    def raising_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "huggingface_hub":
            raise ImportError("missing huggingface_hub")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", raising_import)

    with pytest.raises(RuntimeError, match="requires huggingface_hub"):
        _download_podlodka_snapshot(tmp_path)


def test_podlodka_snapshot_uses_snapshot_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_module = ModuleType("huggingface_hub")
    calls: list[tuple[str, Path, int]] = []

    def fake_snapshot_download(model_name: str, *, cache_dir: Path, max_workers: int) -> str:
        calls.append((model_name, cache_dir, max_workers))
        return str(tmp_path / "snapshot")

    fake_module.snapshot_download = fake_snapshot_download  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)

    result = _download_podlodka_snapshot(tmp_path)

    assert result == tmp_path / "snapshot"
    assert calls == [("bond005/whisper-podlodka-turbo", tmp_path, 1)]


def test_podlodka_converter_requires_transformers_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__
    monkeypatch.delitem(sys.modules, "ctranslate2.converters.transformers", raising=False)

    def raising_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "ctranslate2.converters.transformers":
            raise ImportError("missing converters")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", raising_import)

    with pytest.raises(RuntimeError, match="requires transformers and torch"):
        _build_podlodka_converter("bond005/whisper-podlodka-turbo")


def test_podlodka_converter_uses_transformers_converter(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_transformers_module = ModuleType("ctranslate2.converters.transformers")

    class FakeConverter:
        def __init__(self, model_name: str, **kwargs) -> None:
            self.model_name = model_name
            self.kwargs = kwargs

    fake_transformers_module.TransformersConverter = FakeConverter  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "ctranslate2", ModuleType("ctranslate2"))
    monkeypatch.setitem(sys.modules, "ctranslate2.converters", ModuleType("ctranslate2.converters"))
    monkeypatch.setitem(sys.modules, "ctranslate2.converters.transformers", fake_transformers_module)

    converter = _build_podlodka_converter("bond005/whisper-podlodka-turbo", low_cpu_mem_usage=True)

    assert isinstance(converter, FakeConverter)
    assert converter.model_name == "bond005/whisper-podlodka-turbo"
    assert converter.kwargs == {"low_cpu_mem_usage": True}
