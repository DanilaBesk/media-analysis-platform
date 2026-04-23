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

from pathlib import Path

from transcriber_workers_common.transcribers import (
    DefaultTranscriber,
    YouTubeTranscriptTranscriber,
    _extract_speaker,
)
from telegram_transcriber_bot.domain import SourceCandidate, TranscriptResult, TranscriptSegment


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
