from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from threading import Lock
import time

import pytest

import telegram_transcriber_bot.transcribers as transcribers_module
from telegram_transcriber_bot.domain import SourceCandidate, TranscriptResult, TranscriptSegment
from telegram_transcriber_bot.transcribers import (
    DefaultTranscriber,
    WhisperTranscriber,
    YouTubeTranscriptTranscriber,
    _download_youtube_audio,
    _extract_speaker,
    _is_broken_model_cache_error,
    _snippet_value,
)


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
        whisper_model="turbo",
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


def test_default_transcriber_uses_whisper_for_direct_audio(tmp_path: Path, monkeypatch) -> None:
    default = DefaultTranscriber(
        youtube_languages=("en",),
        whisper_model="turbo",
        whisper_device="auto",
        whisper_compute_type="default",
    )
    expected = TranscriptResult(
        title="Audio",
        source_label="Audio: call",
        segments=[TranscriptSegment(start_seconds=0, end_seconds=1, text="Audio result")],
        language="ru",
        raw_text="Audio result",
    )
    monkeypatch.setattr(default.whisper_transcriber, "transcribe", lambda *args, **kwargs: expected)
    source = SourceCandidate(
        source_id="src-1",
        kind="telegram_audio",
        display_name="Audio: call",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="call.ogg",
    )

    result = default.transcribe(source, tmp_path)

    assert result == expected


def test_download_youtube_audio_returns_generated_file(tmp_path: Path, monkeypatch) -> None:
    output_file = tmp_path / "source.mp3"

    def fake_run(*args, **kwargs):
        output_file.write_bytes(b"audio")
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(transcribers_module.subprocess, "run", fake_run)

    result = _download_youtube_audio("https://youtu.be/demo123", tmp_path)

    assert result == output_file


def test_download_youtube_audio_raises_on_non_zero_exit(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        transcribers_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="bad", stdout=""),
    )

    with pytest.raises(RuntimeError, match="exit code 1"):
        _download_youtube_audio("https://youtu.be/demo123", tmp_path)


def test_download_youtube_audio_raises_when_output_is_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        transcribers_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr="", stdout=""),
    )

    with pytest.raises(RuntimeError, match="without producing an audio file"):
        _download_youtube_audio("https://youtu.be/demo123", tmp_path)


def test_whisper_transcriber_raises_for_missing_audio(tmp_path: Path) -> None:
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    source = SourceCandidate(
        source_id="src-1",
        kind="telegram_audio",
        display_name="Audio: call",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="call.ogg",
    )

    with pytest.raises(ValueError, match="requires either a local file or a YouTube URL"):
        transcriber.transcribe(source, tmp_path)


def test_whisper_transcriber_normalizes_segments(tmp_path: Path, monkeypatch) -> None:
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    audio_path = tmp_path / "audio.ogg"
    audio_path.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="src-1",
        kind="telegram_audio",
        display_name="Audio: call",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="call.ogg",
        local_path=audio_path,
    )
    fake_segments = [
        SimpleNamespace(start=0.0, end=1.0, text="  "),
        SimpleNamespace(start=1.0, end=3.0, text=" Hello "),
    ]
    fake_info = SimpleNamespace(language="ru")
    fake_model = SimpleNamespace(transcribe=lambda *args, **kwargs: (fake_segments, fake_info))
    monkeypatch.setattr(transcriber, "_get_model", lambda workspace_dir: fake_model)

    result = transcriber.transcribe(source, tmp_path)

    assert result.title == "call.ogg"
    assert result.language == "ru"
    assert result.raw_text == "Hello"
    assert result.segments[0].text == "Hello"


def test_whisper_transcriber_raises_on_empty_segments(tmp_path: Path, monkeypatch) -> None:
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    audio_path = tmp_path / "audio.ogg"
    audio_path.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="src-1",
        kind="telegram_audio",
        display_name="Audio: call",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="call.ogg",
        local_path=audio_path,
    )
    fake_model = SimpleNamespace(
        transcribe=lambda *args, **kwargs: ([SimpleNamespace(start=0.0, end=1.0, text="  ")], SimpleNamespace(language="ru"))
    )
    monkeypatch.setattr(transcriber, "_get_model", lambda workspace_dir: fake_model)

    with pytest.raises(RuntimeError, match="empty transcript"):
        transcriber.transcribe(source, tmp_path)


def test_whisper_transcriber_caches_model_instance(tmp_path: Path, monkeypatch) -> None:
    created = []

    class FakeModel:
        def __init__(self, *args, **kwargs) -> None:
            created.append(kwargs["download_root"])

    monkeypatch.setattr(transcribers_module, "WhisperModel", FakeModel)
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")

    first = transcriber._get_model(tmp_path / "job-1")
    second = transcriber._get_model(tmp_path / "job-2")

    assert first is second
    assert len(created) == 1


def test_whisper_transcriber_uses_stable_data_level_model_cache(tmp_path: Path) -> None:
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    workspace_dir = tmp_path / "jobs" / "job-1"
    workspace_dir.mkdir(parents=True)

    cache_root = transcriber._model_cache_root(workspace_dir)

    assert cache_root == tmp_path / "models"


def test_whisper_transcriber_recovers_from_broken_model_cache(tmp_path: Path, monkeypatch) -> None:
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    broken_root = tmp_path / "models"
    broken_root.mkdir(parents=True)
    stale_file = broken_root / "stale.txt"
    stale_file.write_text("stale", encoding="utf-8")
    calls = []

    class FakeModel:
        pass

    def fake_whisper_model(*args, **kwargs):
        calls.append(kwargs["download_root"])
        if len(calls) == 1:
            raise RuntimeError("Unable to open file 'model.bin' in model cache")
        assert not stale_file.exists()
        return FakeModel()

    monkeypatch.setattr(transcribers_module, "WhisperModel", fake_whisper_model)

    model = transcriber._load_model(broken_root)

    assert isinstance(model, FakeModel)
    assert calls == [str(broken_root), str(broken_root)]


def test_whisper_transcriber_serializes_shared_model_usage(tmp_path: Path, monkeypatch) -> None:
    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    audio_a = tmp_path / "audio-a.ogg"
    audio_b = tmp_path / "audio-b.ogg"
    audio_a.write_bytes(b"a")
    audio_b.write_bytes(b"b")
    source_a = SourceCandidate(
        source_id="src-a",
        kind="telegram_audio",
        display_name="Audio: A",
        url=None,
        telegram_file_id="file-a",
        mime_type="audio/ogg",
        file_name="a.ogg",
        local_path=audio_a,
    )
    source_b = SourceCandidate(
        source_id="src-b",
        kind="telegram_audio",
        display_name="Audio: B",
        url=None,
        telegram_file_id="file-b",
        mime_type="audio/ogg",
        file_name="b.ogg",
        local_path=audio_b,
    )
    active_calls = 0
    max_parallel = 0
    state_lock = Lock()

    def fake_transcribe(*args, **kwargs):
        nonlocal active_calls, max_parallel
        with state_lock:
            active_calls += 1
            max_parallel = max(max_parallel, active_calls)
        time.sleep(0.05)
        with state_lock:
            active_calls -= 1
        return ([SimpleNamespace(start=0.0, end=1.0, text="Hello")], SimpleNamespace(language="ru"))

    fake_model = SimpleNamespace(transcribe=fake_transcribe)
    monkeypatch.setattr(transcriber, "_get_model", lambda workspace_dir: fake_model)

    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(transcriber.transcribe, source_a, tmp_path / "job-a")
        right = pool.submit(transcriber.transcribe, source_b, tmp_path / "job-b")
        left.result()
        right.result()

    assert max_parallel == 1


def test_youtube_transcriber_rejects_invalid_or_empty_transcripts(tmp_path: Path, monkeypatch) -> None:
    transcriber = YouTubeTranscriptTranscriber(("en",))
    bad_source = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: bad",
        url="https://example.com/video",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    empty_source = SourceCandidate(
        source_id="src-2",
        kind="youtube_url",
        display_name="YouTube: empty",
        url="https://youtu.be/demo123",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    monkeypatch.setattr(transcriber._api, "fetch", lambda *args, **kwargs: FakeFetchedTranscript([{"text": "   "}]))

    with pytest.raises(ValueError, match="Unsupported YouTube URL"):
        transcriber.transcribe(bad_source, tmp_path)
    with pytest.raises(RuntimeError, match="empty"):
        transcriber.transcribe(empty_source, tmp_path)


def test_snippet_value_supports_attributes_and_dicts() -> None:
    assert _snippet_value({"text": "hello"}, "text") == "hello"
    assert _snippet_value(SimpleNamespace(text="hello"), "text") == "hello"
    assert _snippet_value(object(), "missing", default="fallback") == "fallback"


def test_broken_model_cache_error_detection_is_specific() -> None:
    assert _is_broken_model_cache_error(RuntimeError("Unable to open file 'model.bin' in model cache"))
    assert _is_broken_model_cache_error(RuntimeError("No such file or directory"))
    assert not _is_broken_model_cache_error(RuntimeError("Some other error"))


from workers.common.tests.test_api import *  # noqa: F401,F403,E402
from workers.common.tests.test_api_transport import *  # noqa: F401,F403,E402
