#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "infra" / "fixtures" / "target" / "manifest.json"
TELEGRAM_E2E_SCRIPT = ROOT / "infra" / "scripts" / "copper-asr-telegram-e2e.py"
DEFAULT_ARTIFACT_PATH = ROOT / "docs" / "benchmarks" / "copper-asr-long-voice-benchmark-latest.json"

API_BASE_URL = os.environ.get("COPPER_ASR_BENCHMARK_API_BASE_URL", "http://localhost:8080")
POLL_TIMEOUT_SECONDS = int(os.environ.get("COPPER_ASR_BENCHMARK_POLL_TIMEOUT_SECONDS", "1200"))
POLL_INTERVAL_SECONDS = float(os.environ.get("COPPER_ASR_BENCHMARK_POLL_INTERVAL_SECONDS", "5"))
SAMPLE_INTERVAL_SECONDS = float(os.environ.get("COPPER_ASR_BENCHMARK_SAMPLE_INTERVAL_SECONDS", "2"))
MAX_RUN_WALL_SECONDS = float(os.environ.get("COPPER_ASR_BENCHMARK_MAX_RUN_WALL_SECONDS", "300"))
MAX_COPPER_ASR_CPU_PERCENT = float(os.environ.get("COPPER_ASR_BENCHMARK_MAX_CPU_PERCENT", "450"))
MAX_COPPER_ASR_MEMORY_MIB = float(os.environ.get("COPPER_ASR_BENCHMARK_MAX_MEMORY_MIB", "4096"))

BENCHMARK_SERVICES = ("copper-asr", "worker-transcription", "api")


class BenchmarkE2EError(RuntimeError):
    pass


def _load_telegram_e2e_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("copper_asr_telegram_e2e", TELEGRAM_E2E_SCRIPT)
    if spec is None or spec.loader is None:
        raise BenchmarkE2EError(f"cannot load Telegram E2E helper from {TELEGRAM_E2E_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.API_BASE_URL = API_BASE_URL
    module.POLL_TIMEOUT_SECONDS = POLL_TIMEOUT_SECONDS
    module.POLL_INTERVAL_SECONDS = POLL_INTERVAL_SECONDS
    return module


def _load_manifest() -> dict[str, Any]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _case(case_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    fixtures = _load_manifest()["fixtures"]
    stored_by_id = {str(item["stored_object_id"]): item for item in fixtures["stored_objects"]}
    cases = {str(item["case_id"]): item for item in fixtures["copper_asr_e2e"]["cases"]}
    case = cases[case_id]
    return case, stored_by_id[str(case["stored_object_id"])]


def _run_command(command: list[str], *, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False, timeout=timeout)


def _compose_ps() -> dict[str, dict[str, Any]]:
    completed = _run_command(["docker", "compose", "-f", "infra/docker-compose.yml", "ps", "--format", "json"])
    if completed.returncode != 0:
        raise BenchmarkE2EError(f"docker compose ps failed: {completed.stderr.strip()}")

    services: dict[str, dict[str, Any]] = {}
    for line in completed.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        service = str(item.get("Service") or "")
        if service in BENCHMARK_SERVICES:
            services[service] = item
    missing = [service for service in BENCHMARK_SERVICES if service not in services]
    if missing:
        raise BenchmarkE2EError(f"compose services are not running or visible: {', '.join(missing)}")
    return services


def _container_env(container_name: str) -> dict[str, str]:
    completed = _run_command(
        ["docker", "inspect", "--format", "{{json .Config.Env}}", container_name],
        timeout=30,
    )
    if completed.returncode != 0:
        return {}
    try:
        values = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {}
    env: dict[str, str] = {}
    if isinstance(values, list):
        for item in values:
            key, _, value = str(item).partition("=")
            if key:
                env[key] = value
    return env


def _container_resource_limits(container_name: str) -> dict[str, Any]:
    completed = _run_command(
        ["docker", "inspect", "--format", "{{json .HostConfig}}", container_name],
        timeout=30,
    )
    if completed.returncode != 0:
        return {"nano_cpus": 0, "cpu_quota": 0, "cpu_period": 0, "cpus": 0}
    try:
        host_config = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {"nano_cpus": 0, "cpu_quota": 0, "cpu_period": 0, "cpus": 0}
    if not isinstance(host_config, dict):
        return {"nano_cpus": 0, "cpu_quota": 0, "cpu_period": 0, "cpus": 0}

    nano_cpus = int(host_config.get("NanoCpus") or 0)
    cpu_quota = int(host_config.get("CpuQuota") or 0)
    cpu_period = int(host_config.get("CpuPeriod") or 0)
    cpus = nano_cpus / 1_000_000_000 if nano_cpus > 0 else 0
    if cpus == 0 and cpu_quota > 0 and cpu_period > 0:
        cpus = cpu_quota / cpu_period
    return {
        "nano_cpus": nano_cpus,
        "cpu_quota": cpu_quota,
        "cpu_period": cpu_period,
        "cpus": _round(cpus),
    }


def _wait_copper_asr_healthy(timeout_seconds: float = 600) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        services = _compose_ps()
        service = services["copper-asr"]
        if str(service.get("State") or "") == "running" and str(service.get("Health") or "") == "healthy":
            return
        time.sleep(5)
    raise BenchmarkE2EError("copper-asr did not become healthy after restart")


def _restart_runtime() -> dict[str, Any]:
    started = time.perf_counter()
    restart_asr = _run_command(
        ["docker", "compose", "-f", "infra/docker-compose.yml", "restart", "copper-asr"],
        timeout=900,
    )
    if restart_asr.returncode != 0:
        raise BenchmarkE2EError(f"failed to restart copper-asr: {restart_asr.stderr.strip()}")
    _wait_copper_asr_healthy()
    restart_worker = _run_command(
        ["docker", "compose", "-f", "infra/docker-compose.yml", "restart", "worker-transcription"],
        timeout=300,
    )
    if restart_worker.returncode != 0:
        raise BenchmarkE2EError(f"failed to restart worker-transcription: {restart_worker.stderr.strip()}")
    return {
        "cold_start_measured": True,
        "runtime_restart_seconds": _round(time.perf_counter() - started),
    }


def _parse_percent(value: object) -> float:
    text = str(value or "").strip().replace("%", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


_MEMORY_UNIT_TO_MIB = {
    "B": 1 / (1024 * 1024),
    "KB": 1000 / (1024 * 1024),
    "KIB": 1 / 1024,
    "MB": 1000 * 1000 / (1024 * 1024),
    "MIB": 1,
    "GB": 1000 * 1000 * 1000 / (1024 * 1024),
    "GIB": 1024,
    "TB": 1000 * 1000 * 1000 * 1000 / (1024 * 1024),
    "TIB": 1024 * 1024,
}


def _parse_memory_mib(value: object) -> float:
    text = str(value or "").split("/", 1)[0].strip()
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$", text)
    if not match:
        return 0.0
    amount = float(match.group(1))
    unit = match.group(2).upper()
    return amount * _MEMORY_UNIT_TO_MIB.get(unit, 0.0)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _round(value: float) -> float:
    return round(float(value), 3)


class ResourceSampler:
    def __init__(self, services: dict[str, dict[str, Any]], *, interval_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self.containers_by_service = {
            service: str(item.get("Name") or item.get("Names") or "")
            for service, item in services.items()
            if str(item.get("Name") or item.get("Names") or "")
        }
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.capture_once()
        self._thread = threading.Thread(target=self._run, name="copper-asr-resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(5.0, self.interval_seconds + 3.0))
        self.capture_once()

    def capture_once(self) -> None:
        container_names = list(self.containers_by_service.values())
        if not container_names:
            self.errors.append("no benchmark containers resolved")
            return
        completed = _run_command(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_names],
            timeout=30,
        )
        if completed.returncode != 0:
            self.errors.append(completed.stderr.strip() or "docker stats failed")
            return
        service_by_container = {container: service for service, container in self.containers_by_service.items()}
        captured_at = _utc_now()
        for line in completed.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                self.errors.append(f"malformed docker stats line: {line[:120]}")
                continue
            container = str(payload.get("Name") or payload.get("Container") or "")
            service = service_by_container.get(container, container)
            self.samples.append(
                {
                    "captured_at": captured_at,
                    "service": service,
                    "container": container,
                    "cpu_percent": _round(_parse_percent(payload.get("CPUPerc"))),
                    "memory_mib": _round(_parse_memory_mib(payload.get("MemUsage"))),
                    "memory_percent": _round(_parse_percent(payload.get("MemPerc"))),
                    "pids": int(str(payload.get("PIDs") or "0") or "0"),
                }
            )

    def summarize(self) -> dict[str, Any]:
        services: dict[str, dict[str, Any]] = {}
        for service in BENCHMARK_SERVICES:
            rows = [sample for sample in self.samples if sample["service"] == service]
            if not rows:
                services[service] = {
                    "sample_count": 0,
                    "container": self.containers_by_service.get(service, ""),
                    "max_cpu_percent": 0,
                    "avg_cpu_percent": 0,
                    "max_memory_mib": 0,
                    "avg_memory_mib": 0,
                }
                continue
            cpu_values = [float(row["cpu_percent"]) for row in rows]
            memory_values = [float(row["memory_mib"]) for row in rows]
            services[service] = {
                "sample_count": len(rows),
                "container": rows[0]["container"],
                "first_sample_at": rows[0]["captured_at"],
                "last_sample_at": rows[-1]["captured_at"],
                "max_cpu_percent": _round(max(cpu_values)),
                "avg_cpu_percent": _round(sum(cpu_values) / len(cpu_values)),
                "max_memory_mib": _round(max(memory_values)),
                "avg_memory_mib": _round(sum(memory_values) / len(memory_values)),
            }
        return {
            "sample_interval_seconds": self.interval_seconds,
            "sample_count": len(self.samples),
            "services": services,
            "sampler_errors": self.errors,
        }

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self.capture_once()


def _download_run_manifest(telegram: ModuleType, account: Any, artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    manifest_artifact = telegram._artifact_by_kind(artifacts, "run_manifest")
    payload = telegram._download_artifact_payload(str(manifest_artifact["artifact_id"]))
    return json.loads(payload.decode("utf-8"))


def _artifact_details(telegram: ModuleType, account: Any, analysis_run_id: str) -> list[dict[str, Any]]:
    summaries = telegram._list_run_artifacts(account, analysis_run_id)
    return [telegram._get_artifact(account, str(artifact["artifact_id"])) for artifact in summaries]


def _parse_event_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _event_timeline(events: list[dict[str, Any]]) -> dict[str, Any]:
    timeline: list[dict[str, Any]] = []
    stage_times: dict[str, datetime] = {}
    for event in events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        stage = str(payload.get("progress_stage") or "") if isinstance(payload, dict) else ""
        created_at = str(event.get("created_at") or "")
        parsed = _parse_event_time(created_at)
        if stage and parsed is not None and stage not in stage_times:
            stage_times[stage] = parsed
        timeline.append(
            {
                "event_type": str(event.get("event_type") or ""),
                "status": str(event.get("status") or ""),
                "created_at": created_at,
                "progress_stage": stage,
                "progress_message": str(payload.get("progress_message") or "") if isinstance(payload, dict) else "",
            }
        )

    stage_durations: dict[str, float] = {}
    ordered_stage_names = ["materializing_sources", "transcribing", "persisting_artifacts", "completed"]
    for current, next_stage in zip(ordered_stage_names, ordered_stage_names[1:]):
        start = stage_times.get(current)
        end = stage_times.get(next_stage)
        if start is not None and end is not None:
            stage_durations[f"{current}_to_{next_stage}_seconds"] = _round((end - start).total_seconds())

    return {
        "timeline": timeline,
        "stage_durations_seconds": stage_durations,
    }


def _threshold_result(
    *,
    run_wall_seconds: float,
    resources: dict[str, Any],
    max_run_wall_seconds: float,
    max_copper_asr_cpu_percent: float,
    max_copper_asr_memory_mib: float,
    blocker_issue_id: str,
) -> dict[str, Any]:
    copper = resources["services"]["copper-asr"]
    failures: list[str] = []
    if run_wall_seconds > max_run_wall_seconds:
        failures.append(f"run_wall_seconds {_round(run_wall_seconds)} > {max_run_wall_seconds}")
    if float(copper["max_cpu_percent"]) > max_copper_asr_cpu_percent:
        failures.append(f"copper-asr max_cpu_percent {copper['max_cpu_percent']} > {max_copper_asr_cpu_percent}")
    if float(copper["max_memory_mib"]) > max_copper_asr_memory_mib:
        failures.append(f"copper-asr max_memory_mib {copper['max_memory_mib']} > {max_copper_asr_memory_mib}")
    passed = not failures
    return {
        "max_run_wall_seconds": max_run_wall_seconds,
        "max_copper_asr_cpu_percent": max_copper_asr_cpu_percent,
        "max_copper_asr_memory_mib": max_copper_asr_memory_mib,
        "passed": passed,
        "failures": failures,
        "blocker_issue_id": "" if passed else blocker_issue_id,
    }


def run_benchmark(
    *,
    restart_runtime: bool,
    max_run_wall_seconds: float,
    max_copper_asr_cpu_percent: float,
    max_copper_asr_memory_mib: float,
    blocker_issue_id: str,
) -> dict[str, Any]:
    telegram = _load_telegram_e2e_module()
    restart_result = _restart_runtime() if restart_runtime else {"cold_start_measured": False}
    services = _compose_ps()
    env_by_service = {
        service: _container_env(str(item.get("Name") or item.get("Names") or ""))
        for service, item in services.items()
    }
    resource_limits_by_service = {
        service: _container_resource_limits(str(item.get("Name") or item.get("Names") or ""))
        for service, item in services.items()
    }

    suffix = uuid.uuid4().hex[:12]
    case, stored_object = _case("representative_long_voice")
    fixture_duration_seconds = float(case["duration_seconds"])
    fixture_path = ROOT / str(stored_object["fixture_path"])

    started_at = _utc_now()
    total_started = time.perf_counter()
    telegram._wait_for_api()
    account = telegram._resolve_telegram_account(suffix=suffix)

    upload_started = time.perf_counter()
    media = telegram._multipart_upload(account, stored_object, case_id=str(case["case_id"]), suffix=suffix)
    upload_seconds = time.perf_counter() - upload_started
    media_asset_id = str(media["media_asset_id"])
    collection = telegram._inbox(account)
    collection_items = list(collection.get("items") or [])
    telegram._assert(
        any(str(item.get("media_asset_id") or "") == media_asset_id for item in collection_items),
        "uploaded representative long voice is missing from inbox collection",
    )
    current_surface = telegram._upsert_surface(
        account,
        surface_type="current_materials_panel",
        surface_key=f"benchmark-current:{account.external_account_ref}",
        message_id=8101,
        display_state={
            "screen": "benchmark",
            "collection_id": collection["collection_id"],
            "item_count": len(collection_items),
            "focused_run_id": "",
        },
        subjects=[telegram._surface_subject("collection", str(collection["collection_id"]), "primary")],
        idempotency_key=f"telegram:surface:benchmark-current:{account.external_account_ref}:{suffix}",
    )
    snapshot = telegram._create_selection_snapshot(account, collection, media_asset_id, suffix=f"benchmark-{suffix}")

    sampler = ResourceSampler(services, interval_seconds=SAMPLE_INTERVAL_SECONDS)
    sampler.start()
    run_started = time.perf_counter()
    run = telegram._create_analysis_run(account, str(snapshot["selection_snapshot_id"]), suffix=f"benchmark-{suffix}")
    analysis_run_id = str(run["analysis_run_id"])
    terminal = telegram._poll_run(account, analysis_run_id)
    run_finished = time.perf_counter()
    sampler.stop()

    telegram._assert(
        terminal["status"] in {"succeeded", "partially_succeeded"},
        f"benchmark transcription run finished in unexpected state {terminal['status']}",
    )
    events = telegram._list_run_events(account, analysis_run_id)
    event_result = _event_timeline(events)
    artifact_result = telegram._assert_artifacts(account, analysis_run_id)
    diagnostics_result = telegram._assert_diagnostics(account, analysis_run_id)
    artifact_details = _artifact_details(telegram, account, analysis_run_id)
    run_manifest = _download_run_manifest(telegram, account, artifact_details)
    result_delivery_started = time.perf_counter()
    result_surface = telegram._deliver_result_surface(
        account,
        artifact_id=artifact_result["transcript_artifact_id"],
        analysis_run_id=analysis_run_id,
        delivery_mode=artifact_result["transcript_delivery_mode"],
        suffix=f"benchmark-{suffix}",
    )
    delivery_latency_seconds = time.perf_counter() - result_delivery_started
    inbox_clear = telegram._clear_inbox(account, collection, media_asset_id)
    total_seconds = time.perf_counter() - total_started

    run_wall_seconds = run_finished - run_started
    resources = sampler.summarize()
    copper_env = env_by_service.get("copper-asr", {})
    worker_env = env_by_service.get("worker-transcription", {})
    backend = run_manifest.get("transcription_backend") if isinstance(run_manifest, dict) else {}
    backend = backend if isinstance(backend, dict) else {}
    backend_metadata = backend.get("metadata") if isinstance(backend.get("metadata"), dict) else {}
    processing = backend_metadata.get("processing") if isinstance(backend_metadata.get("processing"), dict) else {}
    vad_timing_seconds = processing.get("vad_s")
    vad_segment_count = processing.get("vad_segment_count")
    chunk_count = processing.get("chunk_count")
    runtime_exposes_vad_timing = isinstance(vad_timing_seconds, int | float)
    runtime_exposes_segment_count = isinstance(vad_segment_count, int) and isinstance(chunk_count, int)
    thresholds = _threshold_result(
        run_wall_seconds=run_wall_seconds,
        resources=resources,
        max_run_wall_seconds=max_run_wall_seconds,
        max_copper_asr_cpu_percent=max_copper_asr_cpu_percent,
        max_copper_asr_memory_mib=max_copper_asr_memory_mib,
        blocker_issue_id=blocker_issue_id,
    )

    return {
        "schema_version": "copper-asr-long-voice-benchmark-v1",
        "bead_id": "media-b8s.2.7",
        "generated_at": _utc_now(),
        "started_at": started_at,
        "fixture": {
            "case_id": case["case_id"],
            "media_kind": case["media_kind"],
            "duration_seconds": fixture_duration_seconds,
            "fixture_path": str(fixture_path.relative_to(ROOT)),
            "size_bytes": int(stored_object["size_bytes"]),
            "sha256": stored_object["sha256"],
            "content_type": stored_object["content_type"],
        },
        "backend": {
            "provider": str(backend.get("provider") or "copperasr"),
            "model": str(backend.get("model") or "unknown"),
            "revision": backend.get("revision"),
            "duration_seconds": backend.get("duration"),
            "metadata": backend.get("metadata") if isinstance(backend.get("metadata"), dict) else {},
            "removed_asr_allowed": False,
        },
        "runtime": {
            "api_base_url": API_BASE_URL,
            "compose_services": {
                service: {
                    "container": str(item.get("Name") or item.get("Names") or ""),
                    "state": str(item.get("State") or ""),
                    "health": str(item.get("Health") or ""),
                    "status": str(item.get("Status") or ""),
                    "created_at": str(item.get("CreatedAt") or ""),
                }
                for service, item in services.items()
            },
            "concurrency": {
                "COPPER_ASR_MAX_CONCURRENT_REQUESTS": copper_env.get("COPPER_ASR_MAX_CONCURRENT_REQUESTS", ""),
                "COPPER_ASR_ONNX_NUM_THREADS": copper_env.get("COPPER_ASR_ONNX_NUM_THREADS", ""),
                "COPPER_ASR_TORCH_NUM_THREADS": copper_env.get("COPPER_ASR_TORCH_NUM_THREADS", ""),
                "COPPER_ASR_TORCH_INTEROP_THREADS": copper_env.get("COPPER_ASR_TORCH_INTEROP_THREADS", ""),
                "COPPER_ASR_FFMPEG_THREADS": copper_env.get("COPPER_ASR_FFMPEG_THREADS", ""),
                "COPPER_ASR_ACQUIRE_TIMEOUT_S": copper_env.get("COPPER_ASR_ACQUIRE_TIMEOUT_S", ""),
                "COPPER_ASR_CLIENT_TIMEOUT_S": worker_env.get("COPPER_ASR_CLIENT_TIMEOUT_S", ""),
            },
            "model_warm_cold": {
                **restart_result,
                "mode": "restarted_preloaded_container" if restart_runtime else "warm_existing_container",
                "COPPER_ASR_PRELOAD_MODEL": copper_env.get("COPPER_ASR_PRELOAD_MODEL", ""),
                "COPPER_ASR_DEVICE": copper_env.get("COPPER_ASR_DEVICE", ""),
                "COPPER_ASR_MODEL_PATH": copper_env.get("COPPER_ASR_MODEL_PATH", ""),
                "COPPER_ASR_CACHE_DIR": copper_env.get("COPPER_ASR_CACHE_DIR", ""),
                "TORCH_HOME": copper_env.get("TORCH_HOME", ""),
            },
            "compose_resource_limits": resource_limits_by_service,
        },
        "timings": {
            "input_duration_seconds": fixture_duration_seconds,
            "upload_seconds": _round(upload_seconds),
            "run_wall_seconds": _round(run_wall_seconds),
            "total_wall_seconds": _round(total_seconds),
            "delivery_latency_seconds": _round(delivery_latency_seconds),
            "real_time_factor": _round(run_wall_seconds / fixture_duration_seconds),
            "speedup_vs_realtime": _round(fixture_duration_seconds / run_wall_seconds) if run_wall_seconds else 0,
        },
        "vad_segmentation": {
            "runtime_exposes_vad_timing": runtime_exposes_vad_timing,
            "runtime_exposes_segment_count": runtime_exposes_segment_count,
            "timing_source": "CopperASR HTTP response metadata.processing"
            if runtime_exposes_vad_timing or runtime_exposes_segment_count
            else "coarse analysis_run_step progress events only; CopperASR HTTP response/run_manifest do not expose VAD timing yet",
            "runtime_processing_seconds": {
                "audio_preparation_s": processing.get("audio_preparation_s"),
                "vad_s": vad_timing_seconds,
                "asr_inference_s": processing.get("asr_inference_s"),
                "total_s": processing.get("total_s"),
                "audio_duration_s": processing.get("audio_duration_s"),
            },
            "runtime_counts": {
                "vad_segment_count": vad_segment_count,
                "chunk_count": chunk_count,
                "word_count": processing.get("word_count"),
                "sentence_count": processing.get("sentence_count"),
            },
            "coarse_progress_stage_durations_seconds": event_result["stage_durations_seconds"],
        },
        "progress_events": event_result["timeline"],
        "resources": resources,
        "artifacts": {
            "artifact_ids": artifact_result["artifact_ids"],
            "public_kinds": artifact_result["public_kinds"],
            "worker_kinds": artifact_result["worker_kinds"],
            "transcript_artifact_id": artifact_result["transcript_artifact_id"],
            "transcript_bytes": artifact_result["transcript_bytes"],
            "transcript_delivery_mode": artifact_result["transcript_delivery_mode"],
            "manifest_backend": artifact_result["manifest_backend"],
        },
        "telegram_flow": {
            "channel_account_id": account.channel_account_id,
            "external_account_ref": account.external_account_ref,
            "media_asset_id": media_asset_id,
            "collection_id": str(collection["collection_id"]),
            "current_materials_surface_id": str(current_surface["channel_surface_id"]),
            "selection_snapshot_id": str(snapshot["selection_snapshot_id"]),
            "analysis_run_id": analysis_run_id,
            "terminal_status": terminal["status"],
            "diagnostic_count": diagnostics_result["diagnostic_count"],
            "diagnostic_codes": diagnostics_result["diagnostic_codes"],
            "result_surface": result_surface,
            "inbox_clear": inbox_clear,
        },
        "thresholds": thresholds,
        "previous_runtime_comparison": {
            "previous_backend": "removed faster-whisper CPU",
            "removed_runtime_preserved": False,
            "comparison_mode": "current CopperASR-only runtime measured under compose; removed runtime is not retained as a fallback",
            "evidence_basis": "media-11o runtime diagnosis and user-observed long voice latency complaint",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run live CopperASR long Telegram voice benchmark under compose.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable benchmark result.")
    parser.add_argument(
        "--write-artifact",
        type=Path,
        default=None,
        help="Write benchmark JSON to this path. Defaults to stdout only.",
    )
    parser.add_argument("--restart-runtime", action="store_true", help="Restart copper-asr and worker before measuring.")
    parser.add_argument("--max-run-wall-seconds", type=float, default=MAX_RUN_WALL_SECONDS)
    parser.add_argument("--max-copper-asr-cpu-percent", type=float, default=MAX_COPPER_ASR_CPU_PERCENT)
    parser.add_argument("--max-copper-asr-memory-mib", type=float, default=MAX_COPPER_ASR_MEMORY_MIB)
    parser.add_argument(
        "--blocker-issue-id",
        default="",
        help="Required in committed artifacts when thresholds fail and the spike is filed as a blocker.",
    )
    args = parser.parse_args(argv)

    try:
        result = run_benchmark(
            restart_runtime=args.restart_runtime,
            max_run_wall_seconds=args.max_run_wall_seconds,
            max_copper_asr_cpu_percent=args.max_copper_asr_cpu_percent,
            max_copper_asr_memory_mib=args.max_copper_asr_memory_mib,
            blocker_issue_id=args.blocker_issue_id,
        )
    except BenchmarkE2EError as exc:
        print(f"[CopperAsrBenchmarkE2E] {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"[CopperAsrBenchmarkE2E] unexpected failure: {exc}", file=sys.stderr)
        return 1

    if args.write_artifact is not None:
        output_path = args.write_artifact if args.write_artifact.is_absolute() else ROOT / args.write_artifact
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        status = "passed" if result["thresholds"]["passed"] else "failed"
        print(f"[CopperAsrBenchmarkE2E] completed threshold_status={status}")

    if not result["thresholds"]["passed"] and not result["thresholds"]["blocker_issue_id"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
