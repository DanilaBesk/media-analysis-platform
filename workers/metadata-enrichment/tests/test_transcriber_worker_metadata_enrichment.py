from __future__ import annotations

import json
import io
import subprocess
import sys
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from transcriber_worker_metadata_enrichment import (
    EnrichmentClaim,
    EnrichmentJob,
    EnrichmentMetadata,
    ControlPlaneError,
    HttpMetadataEnrichmentControlClient,
    MetadataEnrichmentShutdown,
    MetadataEnrichmentWorker,
    MetadataEnrichmentWorkerConfig,
    MetadataResolverError,
    YtDlpMetadataResolver,
)


CANONICAL_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def _job(**changes: object) -> EnrichmentJob:
    values: dict[str, object] = {
        "enrichment_id": "enrichment-123",
        "media_asset_id": "asset-123",
        "channel_account_id": "channel-123",
        "provider": "youtube",
        "canonical_url": CANONICAL_URL,
        "status": "queued",
        "attempt_no": 0,
        "max_attempts": 3,
    }
    values.update(changes)
    return EnrichmentJob(**values)


def _claim() -> EnrichmentClaim:
    return EnrichmentClaim(
        enrichment=_job(status="claimed", attempt_no=1),
        attempt_token="attempt-token-123",
        lease_owner="worker-1",
        lease_expires_at=datetime(2026, 7, 26, 12, tzinfo=UTC),
    )


def _config(**changes: object) -> MetadataEnrichmentWorkerConfig:
    values: dict[str, object] = {
        "lease_owner": "worker-1",
        "internal_token": "internal-token",
        "heartbeat_interval_seconds": 1,
        "lease_seconds": 10,
        "resolver_timeout_seconds": 5,
        "concurrency": 1,
    }
    values.update(changes)
    return MetadataEnrichmentWorkerConfig(**values)


class RecordingControl:
    def __init__(self, *, claim: EnrichmentClaim | None = None) -> None:
        self.claim_result = claim or _claim()
        self.progress: list[dict[str, object]] = []
        self.successes: list[EnrichmentMetadata] = []
        self.failures: list[MetadataResolverError] = []

    def list_queue(self, *, page_size: int) -> tuple[EnrichmentJob, ...]:
        assert page_size == 1
        return (_job(),)

    def claim(self, enrichment_id: str, *, lease_owner: str, lease_seconds: int) -> EnrichmentClaim:
        assert enrichment_id == "enrichment-123"
        assert lease_owner == "worker-1"
        assert lease_seconds == 10
        return self.claim_result

    def publish_progress(self, claim: EnrichmentClaim, progress: dict[str, object]) -> None:
        self.progress.append(progress)

    def finalize_success(self, claim: EnrichmentClaim, metadata: EnrichmentMetadata) -> None:
        self.successes.append(metadata)

    def finalize_failure(self, claim: EnrichmentClaim, failure: MetadataResolverError) -> None:
        self.failures.append(failure)


class FixedResolver:
    def __init__(self, result: EnrichmentMetadata | Exception) -> None:
        self.result = result
        self.urls: list[str] = []

    def resolve(self, canonical_url: str, *, heartbeat, cancelled) -> EnrichmentMetadata:
        self.urls.append(canonical_url)
        assert cancelled() is False
        heartbeat()
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_worker_claims_resolves_reports_progress_and_finalizes_success() -> None:
    metadata = EnrichmentMetadata(
        title="A useful title",
        thumbnail_url="https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
        duration_seconds=213,
    )
    control = RecordingControl()
    resolver = FixedResolver(metadata)
    worker = MetadataEnrichmentWorker(_config(), control=control, resolver=resolver)

    assert worker.run_once() == 1

    assert resolver.urls == [CANONICAL_URL]
    assert [progress["stage"] for progress in control.progress] == ["resolving", "validating"]
    assert control.successes == [metadata]
    assert control.failures == []


def test_retryable_resolver_failure_is_finalized_without_provider_details() -> None:
    control = RecordingControl()
    worker = MetadataEnrichmentWorker(
        _config(),
        control=control,
        resolver=FixedResolver(
            MetadataResolverError(
                "metadata_provider_timeout", "metadata provider resolution timed out", retryable=True
            )
        ),
    )

    assert worker.run_once() == 1

    assert control.successes == []
    assert len(control.failures) == 1
    assert control.failures[0].code == "metadata_provider_timeout"
    assert control.failures[0].retryable is True


def test_shutdown_during_resolution_leaves_attempt_for_lease_reclaim() -> None:
    class StoppingResolver:
        def resolve(self, _url: str, *, heartbeat, cancelled) -> EnrichmentMetadata:
            stop_event.set()
            raise MetadataResolverError("should_not_finalize", "interrupted", retryable=True)

    stop_event = threading.Event()
    control = RecordingControl()
    worker = MetadataEnrichmentWorker(
        _config(), control=control, resolver=StoppingResolver(), stop_event=stop_event
    )

    worker.run_once()

    assert control.successes == []
    assert control.failures == []


@pytest.mark.parametrize(
    "url",
    [
        "http://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://youtube.com/watch?v=dQw4w9WgXcQ",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL123",
        "https://www.youtube.com/watch?v=too-short",
        "https://user@www.youtube.com/watch?v=dQw4w9WgXcQ",
    ],
)
def test_job_rejects_noncanonical_or_nonallowlisted_urls(url: str) -> None:
    with pytest.raises(ValueError, match="canonical"):
        _job(canonical_url=url)


def test_http_client_uses_internal_token_and_top_level_finalize_contract() -> None:
    responses = [
        {
            "items": [
                {
                    "enrichment_id": "enrichment-123",
                    "media_asset_id": "asset-123",
                    "channel_account_id": "channel-123",
                    "provider": "youtube",
                    "canonical_url": CANONICAL_URL,
                    "status": "queued",
                    "attempt_no": 0,
                    "max_attempts": 3,
                }
            ]
        },
        {
            "enrichment": {
                "enrichment_id": "enrichment-123",
                "media_asset_id": "asset-123",
                "channel_account_id": "channel-123",
                "provider": "youtube",
                "canonical_url": CANONICAL_URL,
                "status": "claimed",
                "attempt_no": 1,
                "max_attempts": 3,
            },
            "attempt_token": "attempt-token-123",
            "lease_owner": "worker-1",
            "lease_expires_at": "2026-07-26T12:00:00Z",
        },
        None,
        {"enrichment": {}},
        {"enrichment": {}},
    ]
    requests: list[object] = []

    class Response:
        def __init__(self, payload: object) -> None:
            self.raw = b"" if payload is None else json.dumps(payload).encode()

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            pass

        def read(self, limit: int) -> bytes:
            return self.raw[:limit]

    def opener(http_request, *, timeout: int):
        assert timeout == 30
        requests.append(http_request)
        return Response(responses.pop(0))

    client = HttpMetadataEnrichmentControlClient(_config(), opener=opener)
    jobs = client.list_queue(page_size=1)
    claim = client.claim("enrichment-123", lease_owner="worker-1", lease_seconds=10)
    client.publish_progress(claim, {"stage": "resolving", "percent": 50})
    client.finalize_success(
        claim,
        EnrichmentMetadata(
            title="Title",
            thumbnail_url="https://i.ytimg.com/vi/dQw4w9WgXcQ/default.jpg",
            duration_seconds=10,
        ),
    )

    assert jobs[0].canonical_url == CANONICAL_URL
    assert requests[0].full_url.endswith("/internal/v1/metadata-enrichment-jobs/queue?page_size=1")
    assert all(req.get_header("X-platform-internal-token") == "internal-token" for req in requests)
    final_payload = json.loads(requests[-1].data)
    assert final_payload == {
        "lease_owner": "worker-1",
        "attempt_token": "attempt-token-123",
        "outcome": "succeeded",
        "title": "Title",
        "thumbnail_url": "https://i.ytimg.com/vi/dQw4w9WgXcQ/default.jpg",
        "duration_seconds": 10,
    }
    assert "metadata" not in final_payload

    client.finalize_success(
        claim,
        EnrichmentMetadata(title="No image", thumbnail_url="", duration_seconds=11),
    )
    assert "thumbnail_url" not in json.loads(requests[-1].data)


def test_yt_dlp_is_metadata_only_and_uses_a_proxy_free_environment(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid")
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setenv("HOME", "/secret-home")

    class Process:
        returncode = 0
        pid = 123

        def __init__(self) -> None:
            self.stdout = io.BytesIO(
                json.dumps(
                    {
                        "id": "dQw4w9WgXcQ",
                        "title": "  Title\u0000 with   spaces  ",
                        "thumbnail": "https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg",
                        "duration": 212.6,
                    }
                ).encode()
            )
            self.stderr = io.BytesIO()

        def poll(self) -> int:
            return 0

    def popen(command: list[str], **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return Process()

    resolver = YtDlpMetadataResolver(_config(), popen=popen)
    result = resolver.resolve(CANONICAL_URL, heartbeat=lambda: None, cancelled=lambda: False)

    assert captured["command"] == [
        "yt-dlp",
        "--ignore-config",
        "--dump-single-json",
        "--skip-download",
        "--no-playlist",
        "--no-warnings",
        "--no-cache-dir",
        "--",
        CANONICAL_URL,
    ]
    child_env = captured["kwargs"]["env"]
    assert "HTTPS_PROXY" not in child_env
    assert "NO_PROXY" not in child_env
    assert "HOME" not in child_env
    assert result == EnrichmentMetadata(
        title="Title with spaces",
        thumbnail_url="https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg",
        duration_seconds=213,
    )


def test_yt_dlp_timeout_terminates_process_group_and_is_retryable() -> None:
    terminated: list[object] = []
    clock = iter([0.0, 2.0])

    class Process:
        returncode = None
        pid = 123

        def __init__(self) -> None:
            self.stdout = io.BytesIO()
            self.stderr = io.BytesIO()

        def poll(self):
            return self.returncode

    process = Process()
    resolver = YtDlpMetadataResolver(
        _config(resolver_timeout_seconds=1),
        popen=lambda *_args, **_kwargs: process,
        monotonic=lambda: next(clock),
        terminate=lambda child: (terminated.append(child), setattr(child, "returncode", -15)),
    )

    with pytest.raises(MetadataResolverError) as caught:
        resolver.resolve(CANONICAL_URL, heartbeat=lambda: None, cancelled=lambda: False)

    assert caught.value.code == "metadata_provider_timeout"
    assert caught.value.retryable is True
    assert terminated == [process]


def test_yt_dlp_bounds_partial_output_before_waiting_for_completion() -> None:
    terminated: list[object] = []
    heartbeats: list[bool] = []

    class Process:
        returncode = None
        pid = 123

        def __init__(self) -> None:
            self.stdout = io.BytesIO(b"x" * 9)
            self.stderr = io.BytesIO()

        def poll(self):
            return self.returncode

    process = Process()

    def terminate(child) -> None:
        terminated.append(child)
        child.returncode = -15

    resolver = YtDlpMetadataResolver(
        _config(resolver_max_stdout_bytes=8),
        popen=lambda *_args, **_kwargs: process,
        terminate=terminate,
    )

    with pytest.raises(MetadataResolverError) as caught:
        resolver.resolve(
            CANONICAL_URL,
            heartbeat=lambda: heartbeats.append(True),
            cancelled=lambda: False,
        )

    assert caught.value.code == "metadata_provider_output_limit_exceeded"
    assert caught.value.retryable is False
    assert terminated == [process]
    assert heartbeats == []


def test_yt_dlp_invokes_heartbeat_while_subprocess_is_running() -> None:
    heartbeats: list[bool] = []

    class Process:
        returncode = None
        pid = 123
        calls = 0

        def __init__(self) -> None:
            self.stdout = io.BytesIO(
                json.dumps(
                    {
                        "id": "dQw4w9WgXcQ",
                        "title": "Title",
                        "thumbnail": "",
                        "duration": 1,
                    }
                ).encode()
            )
            self.stderr = io.BytesIO()

        def poll(self):
            self.calls += 1
            if self.calls >= 2:
                self.returncode = 0
            return self.returncode

    resolver = YtDlpMetadataResolver(_config(), popen=lambda *_args, **_kwargs: Process())

    resolver.resolve(
        CANONICAL_URL,
        heartbeat=lambda: heartbeats.append(True),
        cancelled=lambda: False,
    )

    assert heartbeats == [True]


def test_yt_dlp_cancellation_terminates_process_group_without_finalization() -> None:
    terminated: list[object] = []

    class Process:
        returncode = None
        pid = 123

        def __init__(self) -> None:
            self.stdout = io.BytesIO()
            self.stderr = io.BytesIO()

        def poll(self):
            return self.returncode

    process = Process()

    def terminate(child) -> None:
        terminated.append(child)
        child.returncode = -15

    resolver = YtDlpMetadataResolver(
        _config(), popen=lambda *_args, **_kwargs: process, terminate=terminate
    )

    with pytest.raises(MetadataEnrichmentShutdown):
        resolver.resolve(CANONICAL_URL, heartbeat=lambda: None, cancelled=lambda: True)

    assert terminated == [process]


@pytest.mark.parametrize(
    ("stderr", "retryable"),
    [
        (b"ERROR: Private video", False),
        (b"ERROR: HTTP Error 503: Service Unavailable", True),
    ],
)
def test_yt_dlp_exit_failures_are_classified_without_exposing_stderr(
    stderr: bytes, retryable: bool
) -> None:
    class Process:
        returncode = 1
        pid = 123

        def __init__(self) -> None:
            self.stdout = io.BytesIO()
            self.stderr = io.BytesIO(stderr)

        def poll(self) -> int:
            return 1

    resolver = YtDlpMetadataResolver(_config(), popen=lambda *_args, **_kwargs: Process())

    with pytest.raises(MetadataResolverError) as caught:
        resolver.resolve(CANONICAL_URL, heartbeat=lambda: None, cancelled=lambda: False)

    assert caught.value.retryable is retryable
    assert "private" not in str(caught.value).lower()
    assert "503" not in str(caught.value)


def test_identity_mismatch_and_disallowed_thumbnail_are_nonretryable() -> None:
    payloads = [
        {
            "id": "aaaaaaaaaaa",
            "title": "Wrong media",
            "thumbnail": "https://i.ytimg.com/example.jpg",
            "duration": 1,
        },
        {
            "id": "dQw4w9WgXcQ",
            "title": "Right media",
            "thumbnail": "https://metadata.invalid/example.jpg",
            "duration": 1,
        },
    ]

    for payload in payloads:
        class Process:
            returncode = 0
            pid = 123

            def __init__(self) -> None:
                self.stdout = io.BytesIO(json.dumps(payload).encode())
                self.stderr = io.BytesIO()

            def poll(self) -> int:
                return 0

        resolver = YtDlpMetadataResolver(_config(), popen=lambda *_args, **_kwargs: Process())
        with pytest.raises(MetadataResolverError) as caught:
            resolver.resolve(CANONICAL_URL, heartbeat=lambda: None, cancelled=lambda: False)
        assert caught.value.retryable is False


def test_configuration_requires_internal_token_and_heartbeat_inside_lease() -> None:
    with pytest.raises(ValueError, match="internal_token"):
        MetadataEnrichmentWorkerConfig(lease_owner="worker", internal_token="")
    with pytest.raises(ValueError, match="heartbeat"):
        _config(lease_seconds=10, heartbeat_interval_seconds=10)


def test_control_client_rejects_cross_origin_redirect_without_forwarding_internal_token() -> None:
    received_tokens: list[str | None] = []

    class TargetHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            received_tokens.append(self.headers.get("X-Platform-Internal-Token"))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"items":[]}')

        def log_message(self, *_args) -> None:
            pass

    target = ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)

    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(302)
            self.send_header(
                "Location", f"http://127.0.0.1:{target.server_address[1]}/stolen"
            )
            self.end_headers()

        def log_message(self, *_args) -> None:
            pass

    origin = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    threads = [threading.Thread(target=server.serve_forever) for server in (origin, target)]
    for thread in threads:
        thread.start()
    try:
        client = HttpMetadataEnrichmentControlClient(
            _config(api_base_url=f"http://127.0.0.1:{origin.server_address[1]}")
        )
        with pytest.raises(ControlPlaneError, match="HTTP 302"):
            client.list_queue(page_size=1)
    finally:
        origin.shutdown()
        target.shutdown()
        origin.server_close()
        target.server_close()
        for thread in threads:
            thread.join()

    assert received_tokens == []


def test_fast_large_subprocess_output_is_streamed_into_a_capped_buffer() -> None:
    terminated: list[object] = []

    def popen(_command: list[str], **kwargs):
        return subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'x' * (32 * 1024 * 1024))"],
            **kwargs,
        )

    resolver = YtDlpMetadataResolver(
        _config(resolver_max_stdout_bytes=1024),
        popen=popen,
        terminate=lambda child: (terminated.append(child), child.kill(), child.wait()),
    )

    with pytest.raises(MetadataResolverError) as caught:
        resolver.resolve(CANONICAL_URL, heartbeat=lambda: None, cancelled=lambda: False)

    assert caught.value.code == "metadata_provider_output_limit_exceeded"
    assert len(terminated) == 1
