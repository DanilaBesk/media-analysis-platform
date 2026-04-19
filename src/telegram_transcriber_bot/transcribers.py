from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path
from threading import Lock

from faster_whisper import WhisperModel
from youtube_transcript_api import YouTubeTranscriptApi

from telegram_transcriber_bot.domain import SourceCandidate, TranscriptResult, TranscriptSegment
from telegram_transcriber_bot.source_extractor import extract_youtube_video_id


class YouTubeTranscriptTranscriber:
    def __init__(self, languages: tuple[str, ...]) -> None:
        self.languages = languages
        self._api = YouTubeTranscriptApi()

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        if not source.url:
            raise ValueError("YouTube source must contain a URL")

        video_id = extract_youtube_video_id(source.url)
        if not video_id:
            raise ValueError(f"Unsupported YouTube URL: {source.url}")

        transcript = self._api.fetch(video_id, languages=self.languages, preserve_formatting=True)
        segments: list[TranscriptSegment] = []
        raw_lines: list[str] = []
        for item in transcript:
            raw_text = _snippet_value(item, "text")
            speaker, cleaned_text = _extract_speaker(raw_text)
            if not cleaned_text:
                continue
            start_seconds = float(_snippet_value(item, "start", default=0.0))
            duration = float(_snippet_value(item, "duration", default=0.0))
            end_seconds = start_seconds + duration
            segments.append(
                TranscriptSegment(
                    start_seconds=start_seconds,
                    end_seconds=end_seconds,
                    text=cleaned_text,
                    speaker=speaker,
                )
            )
            raw_lines.append(cleaned_text)

        if not segments:
            raise RuntimeError("YouTube transcript is empty")

        return TranscriptResult(
            title=source.display_name,
            source_label=source.display_name,
            segments=segments,
            language=str(getattr(transcript, "language_code", None) or getattr(transcript, "language", None) or "unknown"),
            raw_text="\n".join(raw_lines),
        )


class WhisperTranscriber:
    def __init__(self, model_name: str, device: str, compute_type: str) -> None:
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self._model: WhisperModel | None = None
        self._transcribe_lock = Lock()

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        audio_path = source.local_path
        if audio_path is None and source.url:
            audio_path = _download_youtube_audio(source.url, workspace_dir)
        if audio_path is None:
            raise ValueError("Whisper transcriber requires either a local file or a YouTube URL")

        with self._transcribe_lock:
            model = self._get_model(workspace_dir)
            segments, info = model.transcribe(
                str(audio_path),
                vad_filter=True,
                beam_size=5,
            )

        normalized_segments: list[TranscriptSegment] = []
        raw_lines: list[str] = []
        for segment in segments:
            text = str(segment.text).strip()
            if not text:
                continue
            normalized_segments.append(
                TranscriptSegment(
                    start_seconds=float(segment.start),
                    end_seconds=float(segment.end),
                    text=text,
                    speaker=None,
                )
            )
            raw_lines.append(text)

        if not normalized_segments:
            raise RuntimeError("Whisper returned an empty transcript")

        title = source.file_name or source.display_name
        return TranscriptResult(
            title=title,
            source_label=source.display_name,
            segments=normalized_segments,
            language=str(getattr(info, "language", None) or "unknown"),
            raw_text="\n".join(raw_lines),
        )

    def _get_model(self, workspace_dir: Path) -> WhisperModel:
        if self._model is None:
            download_root = self._model_cache_root(workspace_dir)
            self._model = self._load_model(download_root)
        return self._model

    def _model_cache_root(self, workspace_dir: Path) -> Path:
        data_dir = workspace_dir.parent.parent
        download_root = data_dir / "models"
        download_root.mkdir(parents=True, exist_ok=True)
        return download_root

    def _load_model(self, download_root: Path) -> WhisperModel:
        try:
            return WhisperModel(
                self.model_name,
                device=self.device,
                compute_type=self.compute_type,
                download_root=str(download_root),
            )
        except RuntimeError as exc:
            if not _is_broken_model_cache_error(exc):
                raise
            shutil.rmtree(download_root, ignore_errors=True)
            download_root.mkdir(parents=True, exist_ok=True)
            return WhisperModel(
                self.model_name,
                device=self.device,
                compute_type=self.compute_type,
                download_root=str(download_root),
            )


class DefaultTranscriber:
    def __init__(
        self,
        youtube_languages: tuple[str, ...],
        whisper_model: str,
        whisper_device: str,
        whisper_compute_type: str,
    ) -> None:
        self.youtube_transcriber = YouTubeTranscriptTranscriber(youtube_languages)
        self.whisper_transcriber = WhisperTranscriber(
            model_name=whisper_model,
            device=whisper_device,
            compute_type=whisper_compute_type,
        )

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        if source.kind == "youtube_url":
            try:
                return self.youtube_transcriber.transcribe(source, workspace_dir)
            except Exception:
                return self.whisper_transcriber.transcribe(source, workspace_dir)
        return self.whisper_transcriber.transcribe(source, workspace_dir)


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


def _snippet_value(item: object, field: str, default: object | None = None) -> object:
    if hasattr(item, field):
        return getattr(item, field)
    if isinstance(item, dict):
        return item.get(field, default)
    return default


def _extract_speaker(text: str) -> tuple[str | None, str]:
    normalized = " ".join(part.strip() for part in text.splitlines() if part.strip())
    if not normalized:
        return None, ""

    colon_match = re.match(r"^(?P<speaker>[^:]{1,32}):\s+(?P<content>.+)$", normalized)
    if colon_match:
        speaker = colon_match.group("speaker").strip()
        content = colon_match.group("content").strip()
        return speaker or None, content

    return None, normalized


def _is_broken_model_cache_error(error: RuntimeError) -> bool:
    message = str(error)
    return "Unable to open file 'model.bin'" in message or "No such file or directory" in message
