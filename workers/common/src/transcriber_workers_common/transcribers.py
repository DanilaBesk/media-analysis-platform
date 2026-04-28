# FILE: workers/common/src/transcriber_workers_common/transcribers.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
# PURPOSE: Provide reusable transcription helpers that later worker packets can call without changing transcript behavior.
# SCOPE: YouTube transcript fetching, Whisper fallback transcription, speaker extraction, and YouTube audio materialization.
# DEPENDS: M-WORKER-COMMON
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION
# ROLE: RUNTIME
# MAP_MODE: EXPORTS
# END_MODULE_CONTRACT
#
# START_CHANGE_SUMMARY
#   LAST_CHANGE: v1.0.0 - Extracted reusable transcription helpers into worker-common without changing current runtime behavior.
# END_CHANGE_SUMMARY
#
# START_MODULE_MAP
#   YouTubeTranscriptTranscriber - Fetches transcript segments directly from YouTube subtitles.
#   WhisperTranscriber - Runs faster-whisper transcription with a shared model cache.
#   DefaultTranscriber - Uses subtitle fast path first and falls back to Whisper.
# END_MODULE_MAP

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from threading import Lock

from faster_whisper import WhisperModel
from youtube_transcript_api import YouTubeTranscriptApi

from transcriber_workers_common.domain import SourceCandidate, TranscriptResult, TranscriptSegment
from transcriber_workers_common.source_extractor import extract_youtube_video_id

PODLODKA_WHISPER_MODEL = "bond005/whisper-podlodka-turbo"
_PODLODKA_CTRANSLATE2_DIR = "bond005-whisper-podlodka-turbo-ct2"
_PODLODKA_CTRANSLATE2_QUANTIZATION = "int8"

__all__ = [
    "DefaultTranscriber",
    "PODLODKA_WHISPER_MODEL",
    "WhisperTranscriber",
    "YouTubeTranscriptTranscriber",
]


# START_CONTRACT: YouTubeTranscriptTranscriber
# PURPOSE: Fetch transcript segments from the YouTube transcript API and normalize them into the canonical transcript result.
# INPUTS: { languages: tuple[str, ...] - Preferred transcript languages }
# OUTPUTS: { YouTubeTranscriptTranscriber - Subtitle-first transcriber }
# SIDE_EFFECTS: external API requests through youtube-transcript-api
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION
# END_CONTRACT: YouTubeTranscriptTranscriber
class YouTubeTranscriptTranscriber:
    def __init__(self, languages: tuple[str, ...]) -> None:
        self.languages = languages
        self._api = YouTubeTranscriptApi()

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        # START_BLOCK_BLOCK_FETCH_YOUTUBE_TRANSCRIPT
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
        # END_BLOCK_BLOCK_FETCH_YOUTUBE_TRANSCRIPT


# START_CONTRACT: WhisperTranscriber
# PURPOSE: Run faster-whisper transcription for local or downloaded audio inputs.
# INPUTS: { model_name/device/compute_type: str - Whisper runtime configuration }
# OUTPUTS: { WhisperTranscriber - Whisper-based transcript generator }
# SIDE_EFFECTS: local model downloads, subprocess execution through yt-dlp, and faster-whisper inference
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION
# END_CONTRACT: WhisperTranscriber
class WhisperTranscriber:
    def __init__(self, model_name: str, device: str, compute_type: str) -> None:
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self._model: WhisperModel | None = None
        self._transcribe_lock = Lock()

    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult:
        # START_BLOCK_BLOCK_TRANSCRIBE_WITH_WHISPER
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
        # END_BLOCK_BLOCK_TRANSCRIBE_WITH_WHISPER

    def _get_model(self, workspace_dir: Path) -> WhisperModel:
        if self._model is None:
            download_root = self._model_cache_root(workspace_dir)
            self._model = self._load_model(download_root)
        return self._model

    def _model_cache_root(self, workspace_dir: Path) -> Path:
        configured_root = os.getenv("WHISPER_MODEL_CACHE_DIR", "").strip()
        if configured_root:
            download_root = Path(configured_root)
            download_root.mkdir(parents=True, exist_ok=True)
            return download_root
        data_dir = workspace_dir.parent.parent
        download_root = data_dir / "models"
        download_root.mkdir(parents=True, exist_ok=True)
        return download_root

    def _load_model(self, download_root: Path) -> WhisperModel:
        model_name = self._model_source(download_root)
        try:
            return WhisperModel(
                model_name,
                device=self.device,
                compute_type=self.compute_type,
                download_root=str(download_root),
            )
        except RuntimeError as exc:
            if not _is_broken_model_cache_error(exc):
                raise
            if self.model_name == PODLODKA_WHISPER_MODEL:
                shutil.rmtree(download_root / _PODLODKA_CTRANSLATE2_DIR, ignore_errors=True)
                model_name = self._model_source(download_root)
                return WhisperModel(
                    model_name,
                    device=self.device,
                    compute_type=self.compute_type,
                    download_root=str(download_root),
                )
            shutil.rmtree(download_root, ignore_errors=True)
            download_root.mkdir(parents=True, exist_ok=True)
            return WhisperModel(
                model_name,
                device=self.device,
                compute_type=self.compute_type,
                download_root=str(download_root),
            )

    def _model_source(self, download_root: Path) -> str:
        if self.model_name != PODLODKA_WHISPER_MODEL:
            return self.model_name
        return str(_ensure_podlodka_ctranslate2_model(download_root))


# START_CONTRACT: DefaultTranscriber
# PURPOSE: Prefer subtitle extraction first and fall back to Whisper when subtitles are unavailable or unsuitable.
# INPUTS: { youtube_languages/whisper_model/whisper_device/whisper_compute_type - Transcriber runtime configuration }
# OUTPUTS: { DefaultTranscriber - Compatibility transcriber used by current bot runtime }
# SIDE_EFFECTS: depends on the underlying YouTube transcript and Whisper transcribers
# LINKS: M-WORKER-COMMON, V-M-WORKER-TRANSCRIPTION
# END_CONTRACT: DefaultTranscriber
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


def _ensure_podlodka_ctranslate2_model(download_root: Path) -> Path:
    converted_dir = download_root / _PODLODKA_CTRANSLATE2_DIR
    model_file = converted_dir / "model.bin"
    if model_file.exists():
        return converted_dir

    shutil.rmtree(converted_dir, ignore_errors=True)
    converted_dir.mkdir(parents=True, exist_ok=True)

    snapshot_dir = _download_podlodka_snapshot(download_root)
    converter = _build_podlodka_converter(
        str(snapshot_dir),
        copy_files=["tokenizer.json", "preprocessor_config.json"],
        low_cpu_mem_usage=True,
    )
    converter.convert(
        str(converted_dir),
        quantization=_PODLODKA_CTRANSLATE2_QUANTIZATION,
        force=True,
    )

    if not model_file.exists():
        raise RuntimeError("Converted Podlodka Whisper model cache is missing model.bin")
    return converted_dir


def _download_podlodka_snapshot(download_root: Path) -> Path:
    os.environ.setdefault("HF_HUB_DISABLE_XET", "1")
    try:
        from huggingface_hub import snapshot_download
    except ImportError as exc:
        raise RuntimeError("bond005/whisper-podlodka-turbo requires huggingface_hub to download the model") from exc
    return Path(snapshot_download(PODLODKA_WHISPER_MODEL, cache_dir=download_root, max_workers=1))


def _build_podlodka_converter(model_name: str, **kwargs):
    try:
        from ctranslate2.converters.transformers import TransformersConverter
    except ImportError as exc:
        raise RuntimeError(
            "bond005/whisper-podlodka-turbo requires transformers and torch to build the local CTranslate2 cache"
        ) from exc
    return TransformersConverter(model_name, **kwargs)
