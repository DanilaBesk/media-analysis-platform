from __future__ import annotations

import json
import subprocess
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
        "batch": _load_json(SCHEMA_ROOT / "http" / "create-transcription-batch.schema.json"),
        "batch_draft": _load_json(SCHEMA_ROOT / "http" / "batch-draft.schema.json"),
        "agent_run": _load_json(SCHEMA_ROOT / "http" / "create-agent-run.schema.json"),
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
        "/v1/transcription-jobs/batch",
        "/v1/agent-runs",
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
        "/internal/v1/jobs/{job_id}/request-access",
        "/internal/v1/artifacts/{artifact_id}/download-access",
    }

    assert expected_paths.issubset(spec["paths"])


def test_idempotency_boundary_and_combined_route_shape() -> None:
    surface = validate_contract_surface()
    spec = surface["openapi"]

    create_upload = _path_item(spec, "/v1/transcription-jobs", "post")
    create_combined = _path_item(spec, "/v1/transcription-jobs/combined", "post")
    create_from_url = _path_item(spec, "/v1/transcription-jobs/from-url", "post")
    create_agent_run = _path_item(spec, "/v1/agent-runs", "post")
    create_report = _path_item(spec, "/v1/transcription-jobs/{job_id}/report-jobs", "post")
    create_deep_research = _path_item(spec, "/v1/report-jobs/{job_id}/deep-research-jobs", "post")
    retry_job = _path_item(spec, "/v1/jobs/{job_id}/retry", "post")

    assert "Idempotency-Key" in _parameter_names(create_upload)
    assert "Idempotency-Key" in _parameter_names(create_combined)
    assert "Idempotency-Key" in _parameter_names(create_from_url)
    assert "Idempotency-Key" in _parameter_names(create_agent_run)
    assert "Idempotency-Key" in _parameter_names(create_report)
    assert "Idempotency-Key" in _parameter_names(create_deep_research)
    assert "Idempotency-Key" not in _parameter_names(retry_job)

    combined_schema_ref = create_combined["requestBody"]["content"]["multipart/form-data"]["schema"]["$ref"]
    agent_schema_ref = create_agent_run["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    assert combined_schema_ref.endswith("create-transcription-upload.schema.json#/$defs/combinedTranscriptionRequest")
    assert agent_schema_ref.endswith("create-agent-run.schema.json")
    assert "agent_run job" in create_report["summary"]
    assert "harness_name=claude-code" in create_report["description"]
    assert "operation=report" in create_report["description"]
    assert "agent_run job" in create_deep_research["summary"]
    assert "report-backed agent_run parent" in create_deep_research["description"]
    assert "operation=deep_research" in create_deep_research["description"]

    upload_response = create_upload["responses"]["202"]["content"]["application/json"]["schema"]
    combined_response = create_combined["responses"]["202"]["content"]["application/json"]["schema"]
    assert upload_response["required"] == ["jobs"]
    assert combined_response["$ref"].endswith("job-control.schema.json")


def test_batch_transcription_contract_freezes_mixed_source_basket() -> None:
    surface = validate_contract_surface()
    spec = surface["openapi"]
    batch = _load_json(SCHEMA_ROOT / "http" / "create-transcription-batch.schema.json")

    create_batch = _path_item(spec, "/v1/transcription-jobs/batch", "post")

    assert "Idempotency-Key" in _parameter_names(create_batch)
    assert "worker-transcription aggregate" in create_batch["description"]
    assert "report/deep_research handoff" in create_batch["description"]
    batch_schema_ref = create_batch["requestBody"]["content"]["multipart/form-data"]["schema"]["$ref"]
    assert batch_schema_ref.endswith("create-transcription-batch.schema.json")
    assert create_batch["responses"]["202"]["content"]["application/json"]["schema"]["$ref"].endswith("job-control.schema.json")

    request = batch["$defs"]["batchTranscriptionRequest"]
    manifest = batch["$defs"]["sourceManifest"]
    source = batch["$defs"]["batchSource"]
    uploaded = batch["$defs"]["uploadedFileSource"]
    url_source = batch["$defs"]["urlSource"]

    assert request["required"] == ["source_manifest"]
    assert request["properties"]["files"]["items"]["$ref"] == "#/$defs/binaryFileField"
    assert request["properties"]["source_manifest"]["$ref"] == "#/$defs/sourceManifest"
    assert request["properties"]["delivery"]["$ref"].endswith("deliveryRequest")

    assert manifest["required"] == [
        "manifest_version",
        "ordered_source_labels",
        "sources",
        "completion_policy",
    ]
    assert manifest["properties"]["ordered_source_labels"]["uniqueItems"] is True
    assert manifest["properties"]["ordered_source_labels"]["items"]["$ref"] == "#/$defs/sourceLabel"
    assert manifest["properties"]["sources"]["propertyNames"]["$ref"] == "#/$defs/sourceLabel"
    assert manifest["properties"]["sources"]["additionalProperties"]["$ref"] == "#/$defs/batchSource"
    assert manifest["properties"]["completion_policy"]["enum"] == ["succeed_when_all_sources_succeed", "succeed_when_any_source_succeeds"]

    assert source["oneOf"] == [
        {"$ref": "#/$defs/uploadedFileSource"},
        {"$ref": "#/$defs/urlSource"},
    ]
    assert uploaded["required"] == ["source_kind", "file_part"]
    assert uploaded["properties"]["source_kind"]["enum"] == ["uploaded_file", "telegram_upload"]
    assert uploaded["properties"]["file_part"]["minLength"] == 1
    assert url_source["required"] == ["source_kind", "url"]
    assert url_source["properties"]["source_kind"]["enum"] == ["youtube_url", "external_url"]


def test_batch_draft_add_item_splits_json_url_and_multipart_upload_body() -> None:
    surface = validate_contract_surface()
    spec = surface["openapi"]
    batch_draft = surface["batch_draft"]

    add_item = _path_item(spec, "/v1/batch-drafts/{draft_id}/items", "post")
    content = add_item["requestBody"]["content"]
    assert content["application/json"]["schema"]["$ref"].endswith("batch-draft.schema.json#/$defs/addBatchDraftItemRequest")
    assert content["multipart/form-data"]["schema"]["$ref"].endswith("batch-draft.schema.json#/$defs/addBatchDraftItemMultipartRequest")

    uploaded = batch_draft["$defs"]["uploadedDraftItemSource"]
    assert uploaded["required"] == ["source_kind", "uploaded_source_ref"]
    assert uploaded["properties"]["source_kind"]["enum"] == ["uploaded_file", "telegram_upload"]
    assert "content_type" in uploaded["properties"]
    assert uploaded["properties"]["size_bytes"]["minimum"] == 1

    json_request = batch_draft["$defs"]["addBatchDraftItemRequest"]
    assert json_request["properties"]["item"]["$ref"] == "#/$defs/urlDraftItemSource"
    assert batch_draft["$defs"]["urlDraftItemSource"]["properties"]["source_kind"]["enum"] == ["youtube_url", "external_url"]

    multipart = batch_draft["$defs"]["addBatchDraftItemMultipartRequest"]
    assert multipart["required"] == ["owner", "expected_version", "item"]
    assert multipart["properties"]["owner"]["contentMediaType"] == "application/json"
    assert multipart["properties"]["item"]["contentMediaType"] == "application/json"
    assert multipart["properties"]["item"]["contentSchema"]["$ref"] == "#/$defs/uploadedDraftItemMultipartMetadata"
    assert multipart["properties"]["file"]["$ref"] == "#/$defs/binaryDraftUpload"
    assert multipart["properties"]["files"]["$ref"] == "#/$defs/binaryDraftUpload"
    assert batch_draft["$defs"]["binaryDraftUpload"]["format"] == "binary"

    metadata = batch_draft["$defs"]["uploadedDraftItemMultipartMetadata"]
    assert metadata["required"] == ["source_kind"]
    assert metadata["properties"]["source_kind"]["enum"] == ["uploaded_file", "telegram_upload"]
    assert "uploaded_source_ref" not in metadata["properties"]


def test_delivery_rules_and_v1_artifact_enum_are_frozen() -> None:
    surface = validate_contract_surface()
    enums = surface["enums"]["$defs"]
    upload_defs = surface["upload"]["$defs"]

    assert enums["deliveryStrategy"]["enum"] == ["polling", "webhook"]
    assert "source_original" not in enums["artifactKind"]["enum"]
    assert "external_url" in enums["sourceKind"]["enum"]
    assert "batch_transcription" in enums["sourceSetInputKind"]["enum"]
    assert "agent_run" in enums["jobType"]["enum"]
    assert "agent_run_create" in enums["submissionKind"]["enum"]
    assert "agent_run" in enums["sourceSetInputKind"]["enum"]
    assert enums["workerKind"]["enum"] == ["transcription", "agent_runner"]
    assert enums["taskType"]["enum"] == ["transcription.run", "transcription.aggregate", "agent_run.run"]
    assert "agent_result_json" in enums["artifactKind"]["enum"]
    assert "source_manifest_json" in enums["artifactKind"]["enum"]
    assert "batch_diagnostics_json" in enums["artifactKind"]["enum"]

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
    assert worker_control["agentRunRequestAccessResponse"]["properties"]["request_ref"]["minLength"] == 1
    assert worker_control["claimResponse"]["properties"]["ordered_inputs"]["items"]["$ref"] == "#/$defs/orderedWorkerInput"
    assert worker_control["orderedWorkerInput"]["properties"]["source_label"]["$ref"] == "../common/job-snapshot.schema.json#/$defs/sourceLabel"
    worker_claim_rule = worker_control["claimResponse"]["allOf"][0]
    assert worker_claim_rule["if"]["properties"]["job_type"]["const"] == "agent_run"
    assert worker_claim_rule["then"]["properties"]["ordered_inputs"]["maxItems"] == 0
    assert worker_claim_rule["else"]["properties"]["ordered_inputs"]["minItems"] == 1
    assert ws_event["properties"]["payload"]["properties"]["status"]["$ref"] == "../common/enums.schema.json#/$defs/jobStatus"
    assert webhook_event["properties"]["payload"]["properties"]["status"]["$ref"] == "../common/enums.schema.json#/$defs/jobStatus"

    required_snapshot_fields = {"job_id", "root_job_id", "job_type", "status", "version", "delivery", "source_set"}
    assert required_snapshot_fields.issubset(job_snapshot["required"])
    source_set_rule = job_snapshot["$defs"]["sourceSet"]["allOf"][0]
    assert source_set_rule["if"]["properties"]["input_kind"]["const"] == "agent_run"
    assert source_set_rule["then"]["properties"]["items"]["maxItems"] == 0
    assert source_set_rule["else"]["properties"]["items"]["minItems"] == 1
    assert job_snapshot["$defs"]["sourceSetItem"]["properties"]["source_label"]["$ref"] == "#/$defs/sourceLabel"
    assert {"total_sources", "completed_sources", "failed_sources", "current_source_label"}.issubset(
        job_snapshot["$defs"]["progressState"]["properties"]
    )


def test_agent_run_create_schema_is_request_only_and_redaction_oriented() -> None:
    surface = validate_contract_surface()
    agent_run = surface["agent_run"]
    request = agent_run["properties"]["request"]
    request_def = agent_run["$defs"]["agentRunRequest"]

    assert request["$ref"] == "#/$defs/agentRunRequest"
    assert "harness_name" in agent_run["required"]
    assert "request" in agent_run["required"]
    assert "job_type" not in agent_run["properties"]
    assert "source_set_id" not in agent_run["properties"]
    assert "root_job_id" not in agent_run["properties"]
    assert request_def["additionalProperties"] is False
    assert {"prompt", "payload", "input_artifacts"}.issubset(request_def["properties"])
    assert request_def["properties"]["prompt"]["pattern"] == "\\S"
    assert request_def["properties"]["input_artifacts"]["minItems"] == 1


def test_report_and_deep_research_public_schemas_are_agent_run_orchestration_surfaces() -> None:
    surface = validate_contract_surface()
    report = surface["report"]
    deep_research = surface["deep_research"]

    assert "agent_run job" in report["description"]
    assert "harness_name=claude-code" in report["description"]
    assert "operation=report" in report["description"]
    assert "job_type" not in report["properties"]
    assert "harness_name" not in report["properties"]
    assert "request" not in report["properties"]

    assert "report-backed agent_run parent" in deep_research["description"]
    assert "harness_name=claude-code" in deep_research["description"]
    assert "operation=deep_research" in deep_research["description"]
    assert "job_type" not in deep_research["properties"]
    assert "harness_name" not in deep_research["properties"]
    assert "request" not in deep_research["properties"]


def test_compose_topology_uses_agent_runner_without_dedicated_ai_workers() -> None:
    compose_path = ROOT.parents[1] / "infra" / "docker-compose.yml"

    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_path), "config", "--services"],
        check=True,
        capture_output=True,
        text=True,
    )

    services = set(result.stdout.splitlines())
    assert "worker-agent-runner" in services
    assert "worker-transcription" in services
    assert "worker-report" not in services
    assert "worker-deep-research" not in services
