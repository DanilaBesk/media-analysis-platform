# FILE: workers/common/tests/test_worker_common_transcribers_runtime.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Verify the runtime and failure-path branches of the worker-common transcription helpers.
# SCOPE: YouTube URL validation, yt-dlp materialization, Whisper model lifecycle, and low-level helper behavior.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-COMMON
# ROLE: TEST
# MAP_MODE: LOCALS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Added packet-local runtime coverage for the extracted worker-common transcription helpers.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   test_youtube_transcriber_failure_paths_are_deterministic - Verifies URL and empty-transcript failures.
#   test_whisper_runtime_helpers_cover_download_and_model_branches - Verifies Whisper download and model-cache behavior.
#   test_low_level_helper_branches_are_covered - Verifies snippet and broken-cache helper logic.
# END_MODULE_MAP

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import transcriber_workers_common.transcribers as transcribers_module
from transcriber_workers_common.transcribers import (
    DefaultTranscriber,
    WhisperTranscriber,
    YouTubeTranscriptTranscriber,
    _download_youtube_audio,
    _is_broken_model_cache_error,
    _snippet_value,
)
from telegram_transcriber_bot.domain import SourceCandidate, TranscriptResult, TranscriptSegment


def test_youtube_transcriber_failure_paths_are_deterministic(tmp_path: Path, monkeypatch) -> None:
    transcriber = YouTubeTranscriptTranscriber(("en",))
    source_without_url = SourceCandidate(
        source_id="src-1",
        kind="youtube_url",
        display_name="YouTube: demo",
        url=None,
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    with pytest.raises(ValueError, match="must contain a URL"):
        transcriber.transcribe(source_without_url, tmp_path)

    unsupported = SourceCandidate(
        source_id="src-2",
        kind="youtube_url",
        display_name="YouTube: unsupported",
        url="https://example.com/demo",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    with pytest.raises(ValueError, match="Unsupported YouTube URL"):
        transcriber.transcribe(unsupported, tmp_path)

    source = SourceCandidate(
        source_id="src-3",
        kind="youtube_url",
        display_name="YouTube: empty",
        url="https://youtu.be/demo123",
        telegram_file_id=None,
        mime_type=None,
        file_name=None,
    )
    monkeypatch.setattr(transcriber._api, "fetch", lambda *args, **kwargs: [{"text": "   ", "start": 0, "duration": 1}])
    with pytest.raises(RuntimeError, match="empty"):
        transcriber.transcribe(source, tmp_path)


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
        source_id="src-4",
        kind="telegram_audio",
        display_name="Audio: call",
        url=None,
        telegram_file_id="file-1",
        mime_type="audio/ogg",
        file_name="call.ogg",
    )

    assert default.transcribe(source, tmp_path) == expected


def test_whisper_runtime_helpers_cover_download_and_model_branches(tmp_path: Path, monkeypatch) -> None:
    output_file = tmp_path / "source.mp3"

    def fake_run(*args, **kwargs):
        output_file.write_bytes(b"audio")
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(transcribers_module.subprocess, "run", fake_run)
    assert _download_youtube_audio("https://youtu.be/demo123", tmp_path) == output_file

    monkeypatch.setattr(
        transcribers_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="bad", stdout=""),
    )
    with pytest.raises(RuntimeError, match="exit code 1"):
        _download_youtube_audio("https://youtu.be/demo123", tmp_path)

    monkeypatch.setattr(
        transcribers_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr="", stdout=""),
    )
    with pytest.raises(RuntimeError, match="without producing an audio file"):
        _download_youtube_audio("https://youtu.be/demo123", tmp_path / "missing")

    transcriber = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    with pytest.raises(ValueError, match="requires either a local file or a YouTube URL"):
        transcriber.transcribe(
            SourceCandidate(
                source_id="src-5",
                kind="telegram_audio",
                display_name="Audio: call",
                url=None,
                telegram_file_id="file-1",
                mime_type="audio/ogg",
                file_name="call.ogg",
            ),
            tmp_path,
        )

    audio_path = tmp_path / "audio.ogg"
    audio_path.write_bytes(b"audio")
    source = SourceCandidate(
        source_id="src-6",
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

    fake_empty_model = SimpleNamespace(
        transcribe=lambda *args, **kwargs: ([SimpleNamespace(start=0.0, end=1.0, text="  ")], SimpleNamespace(language="ru"))
    )
    monkeypatch.setattr(transcriber, "_get_model", lambda workspace_dir: fake_empty_model)
    with pytest.raises(RuntimeError, match="empty transcript"):
        transcriber.transcribe(source, tmp_path)

    created: list[str] = []

    class FakeModel:
        def __init__(self, *args, **kwargs) -> None:
            created.append(kwargs["download_root"])

    monkeypatch.setattr(transcribers_module, "WhisperModel", FakeModel)
    cached = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    assert cached._get_model(tmp_path / "job-1") is cached._get_model(tmp_path / "job-2")
    assert len(created) == 1

    workspace_dir = tmp_path / "jobs" / "job-1"
    workspace_dir.mkdir(parents=True, exist_ok=True)
    assert cached._model_cache_root(workspace_dir) == tmp_path / "models"

    broken_root = tmp_path / "broken-models"
    broken_root.mkdir(parents=True, exist_ok=True)
    stale_file = broken_root / "stale.txt"
    stale_file.write_text("stale", encoding="utf-8")
    calls: list[str] = []

    class RecoveryModel:
        pass

    def fake_whisper_model(*args, **kwargs):
        calls.append(kwargs["download_root"])
        if len(calls) == 1:
            raise RuntimeError("Unable to open file 'model.bin' in model cache")
        assert not stale_file.exists()
        return RecoveryModel()

    monkeypatch.setattr(transcribers_module, "WhisperModel", fake_whisper_model)
    recovered = WhisperTranscriber(model_name="turbo", device="auto", compute_type="default")
    assert isinstance(recovered._load_model(broken_root), RecoveryModel)
    assert calls == [str(broken_root), str(broken_root)]


def test_low_level_helper_branches_are_covered() -> None:
    item = SimpleNamespace(text="hello", start=1.0)

    assert _snippet_value(item, "text") == "hello"
    assert _snippet_value({"text": "dict"}, "text") == "dict"
    assert _snippet_value(object(), "text", default="fallback") == "fallback"
    assert _is_broken_model_cache_error(RuntimeError("Unable to open file 'model.bin'")) is True
    assert _is_broken_model_cache_error(RuntimeError("No such file or directory")) is True
    assert _is_broken_model_cache_error(RuntimeError("another error")) is False
