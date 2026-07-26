from __future__ import annotations

import os
import stat
import subprocess
import sys
import time
import wave
from datetime import UTC, datetime
from pathlib import Path

import pytest

from transcriber_worker_media_export import (
    ExportClaim,
    ExportJob,
    ExportSource,
    MediaExportWorker,
    MediaExportWorkerConfig,
    MinioExportObjectStore,
)


class RecordingControl:
    def __init__(self, claim: ExportClaim, *, cancel_requested: bool = False) -> None:
        self.claim = claim
        self.cancel_requested = cancel_requested
        self.progress: list[dict[str, object]] = []
        self.finalized: list[dict[str, object]] = []

    def list_queue(self, *, page_size: int) -> tuple[ExportJob, ...]:
        return (self.claim.job,)

    def claim_job(self, export_job_id: str, *, lease_owner: str, lease_seconds: int) -> ExportClaim:
        assert export_job_id == self.claim.job.export_job_id
        return self.claim

    def publish_progress(self, claim: ExportClaim, progress: dict[str, object]) -> None:
        self.progress.append(progress)

    def cancel_requested_for(self, claim: ExportClaim) -> bool:
        return self.cancel_requested

    def finalize(self, claim: ExportClaim, *, outcome: str, output=None, diagnostic_code=None, diagnostic_message=None) -> None:
        self.finalized.append(
            {
                "outcome": outcome,
                "output": output,
                "diagnostic_code": diagnostic_code,
                "diagnostic_message": diagnostic_message,
            }
        )


class RecordingStore:
    def __init__(self) -> None:
        self.uploads: list[dict[str, object]] = []

    def put_file(self, *, object_key: str, source: Path, content_type: str, metadata: dict[str, str], cancelled) -> None:
        assert cancelled() is False
        self.uploads.append(
            {
                "object_key": object_key,
                "content": source.read_bytes(),
                "content_type": content_type,
                "metadata": metadata,
            }
        )


def _claim(
    *,
    operation: str = "video_to_audio",
    source_type: str = "uploaded_object",
    audio_bitrate_kbps: int = 128,
) -> ExportClaim:
    job = ExportJob(
        export_job_id="job-123",
        operation=operation,
        variant={"audio_bitrate_kbps": audio_bitrate_kbps} if operation != "youtube_video" else {"video_quality": "720p"},
    )
    return ExportClaim(
        job=job,
        attempt_token="attempt-token-123456",
        lease_owner="test-worker",
        source=ExportSource(
            media_asset_id="asset-123",
            source_type=source_type,
            url="https://files.example/source.mp4" if source_type == "uploaded_object" else "https://www.youtube.com/watch?v=abc123",
            size_bytes=3,
        ),
    )


def _config(tmp_path: Path, **changes: object) -> MediaExportWorkerConfig:
    values: dict[str, object] = {
        "workspace_root": tmp_path / "scratch",
        "lease_owner": "test-worker",
        "internal_token": "test-token",
        "max_input_bytes": 10,
        "max_output_bytes": 10,
        "workspace_max_bytes": 1_000,
        "timeout_seconds": 5,
    }
    values.update(changes)
    return MediaExportWorkerConfig(**values)


def _install_tool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, body: str) -> None:
    executable = tmp_path / "bin" / name
    executable.parent.mkdir()
    executable.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{executable.parent}:{os.environ.get('PATH', '')}")


@pytest.mark.parametrize(
    ("operation", "source_type"),
    (("video_to_audio", "uploaded_object"), ("youtube_audio", "remote_reference")),
)
def test_audio_exports_publish_ogg_opus_to_attempt_staging_and_finalize(
    tmp_path: Path, operation: str, source_type: str
) -> None:
    claim = _claim(operation=operation, source_type=source_type)
    control = RecordingControl(claim)
    store = RecordingStore()
    commands: list[list[str]] = []

    def download(_url: str, destination: Path, *, max_bytes: int, cancelled) -> None:
        assert cancelled() is False
        destination.write_bytes(b"raw")

    def tool(command: list[str], *, cwd: Path, timeout_seconds: float, cancelled, resource_guard) -> None:
        commands.append(command)
        if command[0] == "yt-dlp":
            (cwd / "remote-source.webm").write_bytes(b"source")
        else:
            Path(command[-1]).write_bytes(b"audio")

    worker = MediaExportWorker(_config(tmp_path), control=control, object_store=store, download_source=download, run_tool=tool, probe_duration=lambda _path: 1)

    assert worker.run_once() == 1

    ffmpeg = next(command for command in commands if command[0] == "ffmpeg")
    assert ffmpeg[-1].endswith("export-job-123.ogg")
    assert ffmpeg[ffmpeg.index("-c:a") + 1] == "libopus"
    assert ffmpeg[ffmpeg.index("-b:a") + 1] == "128k"
    assert ffmpeg[ffmpeg.index("-vbr") + 1] == "on"
    assert ffmpeg[ffmpeg.index("-application") + 1] == "audio"
    assert store.uploads == [
        {
            "object_key": "transient/staging/job-123/attempt-token-123456/export-job-123.ogg",
            "content": b"audio",
            "content_type": "audio/ogg",
            "metadata": {"sha256": "6ed8919ce20490a5e3ad8630a4fab69475297abd07db73918dd5f36fcfaeb11b"},
        }
    ]
    assert control.finalized[0]["outcome"] == "succeeded"
    assert control.finalized[0]["output"] == {
        "content_type": "audio/ogg",
        "filename": "export-job-123.ogg",
        "size_bytes": 5,
        "sha256": "6ed8919ce20490a5e3ad8630a4fab69475297abd07db73918dd5f36fcfaeb11b",
        "staging_key": "transient/staging/job-123/attempt-token-123456/export-job-123.ogg",
    }
    assert not any((_config(tmp_path).workspace_root).glob("*"))


@pytest.mark.parametrize("bitrate", [64, 96, 128, 192, 256])
def test_every_supported_audio_bitrate_produces_real_ogg_opus(tmp_path: Path, bitrate: int) -> None:
    source = tmp_path / "source.wav"
    with wave.open(str(source), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(48_000)
        wav.writeframes(b"\x00\x00" * 4_800)

    claim = _claim(audio_bitrate_kbps=bitrate)
    worker = MediaExportWorker(
        _config(
            tmp_path,
            max_input_bytes=1_000_000,
            max_output_bytes=1_000_000,
            workspace_max_bytes=2_000_000,
            timeout_seconds=10,
        ),
        control=RecordingControl(claim),
        object_store=RecordingStore(),
    )

    output, content_type = worker._convert(claim, source, tmp_path)
    probe = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            "stream=codec_name:format=format_name",
            "-of",
            "default=noprint_wrappers=1",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert content_type == "audio/ogg"
    assert "codec_name=opus" in probe.stdout
    assert "format_name=ogg" in probe.stdout


def test_cancel_requested_before_materialization_finalizes_canceled_without_tool_or_upload(tmp_path: Path) -> None:
    control = RecordingControl(_claim(), cancel_requested=True)
    worker = MediaExportWorker(
        _config(tmp_path),
        control=control,
        object_store=RecordingStore(),
        download_source=lambda *_args, **_kwargs: pytest.fail("download must not run"),
        run_tool=lambda *_args, **_kwargs: pytest.fail("tool must not run"),
        probe_duration=lambda _path: pytest.fail("probe must not run"),
    )

    assert worker.run_once() == 1
    assert control.finalized == [{"outcome": "canceled", "output": None, "diagnostic_code": None, "diagnostic_message": None}]


def test_output_larger_than_limit_fails_without_publication(tmp_path: Path) -> None:
    control = RecordingControl(_claim())
    store = RecordingStore()

    def download(_url: str, destination: Path, *, max_bytes: int, cancelled) -> None:
        assert cancelled() is False
        destination.write_bytes(b"raw")

    def tool(command: list[str], *, cwd: Path, timeout_seconds: float, cancelled, resource_guard) -> None:
        Path(command[-1]).write_bytes(b"too-large")

    worker = MediaExportWorker(
        _config(tmp_path, max_output_bytes=3), control=control, object_store=store, download_source=download, run_tool=tool, probe_duration=lambda _path: 1
    )

    worker.run_once()
    assert store.uploads == []
    assert control.finalized[0]["outcome"] == "failed"
    assert control.finalized[0]["diagnostic_code"] == "export_output_limit_exceeded"


def test_growing_ffmpeg_output_is_stopped_before_tool_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    invocation = tmp_path / "ffmpeg-invocation.txt"
    child_marker = tmp_path / "ffmpeg-child-survived.txt"
    child_program = (
        "import pathlib, time; time.sleep(1); "
        f"pathlib.Path({str(child_marker)!r}).write_text('survived')"
    )
    _install_tool(
        tmp_path,
        monkeypatch,
        "ffmpeg",
        f"import pathlib, subprocess, sys, time\nsubprocess.Popen([sys.executable, '-c', {child_program!r}])\n"
        f"pathlib.Path({str(invocation)!r}).write_text(repr(sys.argv))\n"
        "pathlib.Path(sys.argv[-1]).write_bytes(b'too-large')\ntime.sleep(10)\n",
    )
    control = RecordingControl(_claim())
    worker = MediaExportWorker(
        _config(tmp_path, max_output_bytes=3, timeout_seconds=3),
        control=control,
        object_store=RecordingStore(),
        download_source=lambda _url, destination, **_kwargs: destination.write_bytes(b"raw"),
        probe_duration=lambda _path: 1,
    )

    worker.run_once()

    assert invocation.read_text()
    assert control.finalized[0]["diagnostic_code"] == "export_output_limit_exceeded"
    time.sleep(1.2)
    assert not child_marker.exists()


def test_growing_ytdlp_workspace_is_stopped_before_tool_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_tool(
        tmp_path,
        monkeypatch,
        "yt-dlp",
        "import pathlib, sys, time\n"
        "template = pathlib.Path(sys.argv[sys.argv.index('-o') + 1])\n"
        "template.with_name(template.name.replace('%(ext)s', 'webm')).write_bytes(b'12345678')\n"
        "(template.parent / 'fragment-1.part').write_bytes(b'12345678')\n"
        "(template.parent / 'fragment-2.part').write_bytes(b'12345678')\n"
        "time.sleep(10)\n",
    )
    claim = _claim(operation="youtube_video", source_type="remote_reference")
    control = RecordingControl(claim)
    worker = MediaExportWorker(
        _config(tmp_path, workspace_max_bytes=20, timeout_seconds=3),
        control=control,
        object_store=RecordingStore(),
        probe_duration=lambda _path: 1,
    )

    worker.run_once()

    assert control.finalized[0]["diagnostic_code"] == "export_workspace_limit_exceeded"


def test_workspace_reservation_requires_room_for_input_and_output(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="input and output"):
        _config(tmp_path, max_input_bytes=8, max_output_bytes=8, workspace_max_bytes=10)


def test_duration_over_limit_fails_before_conversion(tmp_path: Path) -> None:
    control = RecordingControl(_claim())

    worker = MediaExportWorker(
        _config(tmp_path, max_duration_seconds=10),
        control=control,
        object_store=RecordingStore(),
        download_source=lambda _url, destination, **_kwargs: destination.write_bytes(b"raw"),
        run_tool=lambda *_args, **_kwargs: pytest.fail("conversion must not run"),
        probe_duration=lambda _path: 11,
    )

    worker.run_once()
    assert control.finalized[0]["diagnostic_code"] == "export_duration_limit_exceeded"


def test_youtube_video_selects_the_requested_semantic_quality_before_ffmpeg(tmp_path: Path) -> None:
    control = RecordingControl(_claim(operation="youtube_video", source_type="remote_reference"))
    commands: list[list[str]] = []

    def tool(command: list[str], *, cwd: Path, timeout_seconds: float, cancelled, resource_guard) -> None:
        commands.append(command)
        if command[0] == "yt-dlp":
            (cwd / "remote-source.webm").write_bytes(b"video")
        else:
            Path(command[-1]).write_bytes(b"output")

    worker = MediaExportWorker(
        _config(tmp_path),
        control=control,
        object_store=RecordingStore(),
        run_tool=tool,
        probe_duration=lambda _path: 1,
    )

    worker.run_once()
    assert commands[0][0] == "yt-dlp"
    assert "--ignore-config" in commands[0]
    assert commands[0][commands[0].index("--match-filter") + 1] == "!is_live & duration <= 14400"
    assert commands[0][commands[0].index("-f") + 1] == "bestvideo[height<=720]+bestaudio/best[height<=720]"
    ffmpeg = next(command for command in commands if command[0] == "ffmpeg")
    assert ffmpeg[-1].endswith("export-job-123.mp4")
    assert ffmpeg[ffmpeg.index("-c:v") + 1] == "copy"
    assert ffmpeg[ffmpeg.index("-c:a") + 1] == "aac"
    assert control.finalized[0]["outcome"] == "succeeded"


def test_queue_poll_failure_is_retried_as_an_idle_poll(tmp_path: Path) -> None:
    class UnavailableControl:
        def list_queue(self, *, page_size: int):
            raise RuntimeError("API unavailable")

    worker = MediaExportWorker(
        _config(tmp_path),
        control=UnavailableControl(),
        object_store=RecordingStore(),
    )

    assert worker.run_forever(max_idle_polls=1, sleeper=lambda _seconds: None) == 0


def test_minio_upload_streams_output_with_sha256_metadata(tmp_path: Path) -> None:
    class Response:
        status = 200

        def read(self) -> bytes:
            return b""

    class Connection:
        def __init__(self) -> None:
            self.headers: list[tuple[str, str]] = []
            self.sent = b""

        def putrequest(self, *_args, **_kwargs) -> None:
            pass

        def putheader(self, name: str, value: str) -> None:
            self.headers.append((name, value))

        def endheaders(self) -> None:
            pass

        def send(self, body: bytes) -> None:
            self.sent += body

        def getresponse(self) -> Response:
            return Response()

        def close(self) -> None:
            pass

    connection = Connection()
    source = tmp_path / "result.m4a"
    source.write_bytes(b"audio")
    store = MinioExportObjectStore(
        endpoint="http://minio:9000",
        access_key="access",
        secret_key="secret",
        artifact_bucket="artifacts",
        now=lambda: datetime(2026, 7, 26, tzinfo=UTC),
        connection_factory=lambda *_args: connection,
    )

    store.put_file(
        object_key="transient/staging/job/token/export.m4a",
        source=source,
        content_type="audio/mp4",
        metadata={"sha256": "6ed8919ce20490a5e3ad8630a4fab69475297abd07db73918dd5f36fcfaeb11b"},
        cancelled=lambda: False,
    )

    assert connection.sent == b"audio"
    assert ("X-Amz-Meta-Sha256", "6ed8919ce20490a5e3ad8630a4fab69475297abd07db73918dd5f36fcfaeb11b") in connection.headers
    assert any(name == "Authorization" and value.startswith("AWS4-HMAC-SHA256") for name, value in connection.headers)
