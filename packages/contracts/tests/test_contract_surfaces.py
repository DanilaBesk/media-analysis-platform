from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OPENAPI_PATH = ROOT / "openapi" / "openapi.yaml"
SCHEMA_ROOT = ROOT / "schemas"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_contract_surface() -> dict[str, dict]:
    print("[Contracts][validateContractSurface][BLOCK_VALIDATE_CONTRACT_SURFACE] loading frozen contract surface")
    return {
        "openapi": _load_json(OPENAPI_PATH),
        "enums": _load_json(SCHEMA_ROOT / "common" / "enums.schema.json"),
        "job_snapshot": _load_json(SCHEMA_ROOT / "common" / "job-snapshot.schema.json"),
        "upload": _load_json(SCHEMA_ROOT / "http" / "create-transcription-upload.schema.json"),
        "from_url": _load_json(SCHEMA_ROOT / "http" / "create-transcription-from-url.schema.json"),
        "report": _load_json(SCHEMA_ROOT / "http" / "create-report.schema.json"),
        "deep_research": _load_json(SCHEMA_ROOT / "http" / "create-deep-research.schema.json"),
        "job_control": _load_json(SCHEMA_ROOT / "http" / "job-control.schema.json"),
        "artifact_resolution": _load_json(SCHEMA_ROOT / "http" / "artifact-resolution.schema.json"),
        "worker_control": _load_json(SCHEMA_ROOT / "internal" / "worker-control.schema.json"),
        "ws_event": _load_json(SCHEMA_ROOT / "ws" / "job-event.schema.json"),
        "webhook_event": _load_json(SCHEMA_ROOT / "webhook" / "job-lifecycle.schema.json"),
    }


def _path_item(spec: dict, path: str, method: str) -> dict:
    return spec["paths"][path][method]


def _parameter_names(operation: dict) -> list[str]:
    names: list[str] = []
    for parameter in operation.get("parameters", []):
        if "name" in parameter:
            names.append(parameter["name"])
        elif "$ref" in parameter:
            names.append(parameter["$ref"])
    return names


def test_openapi_contains_frozen_phase_1_paths() -> None:
    surface = validate_contract_surface()
    spec = surface["openapi"]

    expected_paths = {
        "/v1/transcription-jobs",
        "/v1/transcription-jobs/combined",
        "/v1/transcription-jobs/from-url",
        "/v1/jobs/{job_id}",
        "/v1/jobs",
        "/v1/transcription-jobs/{job_id}/report-jobs",
        "/v1/report-jobs/{job_id}/deep-research-jobs",
        "/v1/jobs/{job_id}/cancel",
        "/v1/jobs/{job_id}/retry",
        "/v1/artifacts/{artifact_id}",
        "/v1/jobs/{job_id}/events",
        "/v1/ws",
        "/internal/v1/jobs/{job_id}/claim",
        "/internal/v1/jobs/{job_id}/progress",
        "/internal/v1/jobs/{job_id}/artifacts",
        "/internal/v1/jobs/{job_id}/finalize",
        "/internal/v1/jobs/{job_id}/cancel-check",
    }

    assert expected_paths.issubset(spec["paths"])


def test_idempotency_boundary_and_combined_route_shape() -> None:
    surface = validate_contract_surface()
    spec = surface["openapi"]

    create_upload = _path_item(spec, "/v1/transcription-jobs", "post")
    create_combined = _path_item(spec, "/v1/transcription-jobs/combined", "post")
    create_from_url = _path_item(spec, "/v1/transcription-jobs/from-url", "post")
    create_report = _path_item(spec, "/v1/transcription-jobs/{job_id}/report-jobs", "post")
    create_deep_research = _path_item(spec, "/v1/report-jobs/{job_id}/deep-research-jobs", "post")
    retry_job = _path_item(spec, "/v1/jobs/{job_id}/retry", "post")

    assert "Idempotency-Key" in _parameter_names(create_upload)
    assert "Idempotency-Key" in _parameter_names(create_combined)
    assert "Idempotency-Key" in _parameter_names(create_from_url)
    assert "Idempotency-Key" in _parameter_names(create_report)
    assert "Idempotency-Key" in _parameter_names(create_deep_research)
    assert "Idempotency-Key" not in _parameter_names(retry_job)

    combined_schema_ref = create_combined["requestBody"]["content"]["multipart/form-data"]["schema"]["$ref"]
    assert combined_schema_ref.endswith("create-transcription-upload.schema.json#/$defs/combinedTranscriptionRequest")

    upload_response = create_upload["responses"]["202"]["content"]["application/json"]["schema"]
    combined_response = create_combined["responses"]["202"]["content"]["application/json"]["schema"]
    assert upload_response["required"] == ["jobs"]
    assert combined_response["$ref"].endswith("job-control.schema.json")


def test_delivery_rules_and_v1_artifact_enum_are_frozen() -> None:
    surface = validate_contract_surface()
    enums = surface["enums"]["$defs"]
    upload_defs = surface["upload"]["$defs"]

    assert enums["deliveryStrategy"]["enum"] == ["polling", "webhook"]
    assert "source_original" not in enums["artifactKind"]["enum"]

    delivery_request = upload_defs["deliveryRequest"]
    webhook_requirement = delivery_request["allOf"][0]
    webhook_consistency = delivery_request["allOf"][1]

    assert webhook_requirement["if"]["properties"]["strategy"]["const"] == "webhook"
    assert webhook_requirement["then"]["required"] == ["webhook"]
    assert webhook_consistency["then"]["properties"]["strategy"]["const"] == "webhook"


def test_canonical_job_snapshot_is_reused_across_http_ws_webhook_and_worker_surfaces() -> None:
    surface = validate_contract_surface()
    job_control = surface["job_control"]
    artifact_resolution = surface["artifact_resolution"]
    worker_control = surface["worker_control"]["$defs"]
    ws_event = surface["ws_event"]
    webhook_event = surface["webhook_event"]
    job_snapshot = surface["job_snapshot"]

    assert job_control["properties"]["job"]["$ref"] == "../common/job-snapshot.schema.json"
    assert artifact_resolution["properties"]["artifact_kind"]["$ref"] == "../common/enums.schema.json#/$defs/artifactKind"
    assert worker_control["finalizeRequest"]["properties"]["outcome"]["$ref"] == "../common/enums.schema.json#/$defs/workerOutcome"
    assert worker_control["claimResponse"]["properties"]["ordered_inputs"]["items"]["$ref"] == "#/$defs/orderedWorkerInput"
    assert ws_event["properties"]["payload"]["properties"]["status"]["$ref"] == "../common/enums.schema.json#/$defs/jobStatus"
    assert webhook_event["properties"]["payload"]["properties"]["status"]["$ref"] == "../common/enums.schema.json#/$defs/jobStatus"

    required_snapshot_fields = {"job_id", "root_job_id", "job_type", "status", "version", "delivery", "source_set"}
    assert required_snapshot_fields.issubset(job_snapshot["required"])
