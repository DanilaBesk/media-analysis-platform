from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OPENAPI_PATH = ROOT / "openapi" / "openapi.yaml"
SCHEMA_ROOT = ROOT / "schemas"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _contract_surface() -> dict[str, dict]:
    return {
        "openapi": _load_json(OPENAPI_PATH),
        "enums": _load_json(SCHEMA_ROOT / "common" / "enums.schema.json"),
        "channel": _load_json(SCHEMA_ROOT / "common" / "channel-account.schema.json"),
        "operation_request": _load_json(SCHEMA_ROOT / "common" / "operation-request.schema.json"),
        "media_asset": _load_json(SCHEMA_ROOT / "http" / "media-asset.schema.json"),
        "metadata_enrichment": _load_json(SCHEMA_ROOT / "http" / "metadata-enrichment.schema.json"),
        "collection": _load_json(SCHEMA_ROOT / "http" / "collection.schema.json"),
        "export_job": _load_json(SCHEMA_ROOT / "http" / "export-job.schema.json"),
        "selection_snapshot": _load_json(SCHEMA_ROOT / "http" / "selection-snapshot.schema.json"),
        "analysis_run": _load_json(SCHEMA_ROOT / "http" / "analysis-run.schema.json"),
        "artifact": _load_json(SCHEMA_ROOT / "http" / "artifact.schema.json"),
        "diagnostic": _load_json(SCHEMA_ROOT / "http" / "diagnostic.schema.json"),
        "worker_control": _load_json(SCHEMA_ROOT / "internal" / "worker-control.schema.json"),
        "export_control": _load_json(SCHEMA_ROOT / "internal" / "export-control.schema.json"),
        "metadata_enrichment_control": _load_json(SCHEMA_ROOT / "internal" / "metadata-enrichment-control.schema.json"),
        "retention_control": _load_json(SCHEMA_ROOT / "internal" / "retention-control.schema.json"),
        "ws_event": _load_json(SCHEMA_ROOT / "ws" / "run-event.schema.json"),
        "webhook_event": _load_json(SCHEMA_ROOT / "webhook" / "run-lifecycle.schema.json"),
    }


def test_openapi_exposes_only_target_routes() -> None:
    spec = _contract_surface()["openapi"]

    expected_paths = {
        "/v1/media-assets",
        "/v1/media-assets/upload",
        "/v1/media-assets/{media_asset_id}",
        "/v1/media-assets/{media_asset_id}/exports",
        "/v1/media-assets/{media_asset_id}/refresh-metadata",
        "/v1/collections/inbox",
        "/v1/collections",
        "/v1/collections/{collection_id}",
        "/v1/collections/{collection_id}/items",
        "/v1/collections/{collection_id}/items/{media_asset_id}",
        "/v1/collections/{collection_id}/processing-runs",
        "/v1/selection-snapshots",
        "/v1/selection-snapshots/{selection_snapshot_id}",
        "/v1/analysis-runs",
        "/v1/analysis-runs/{analysis_run_id}",
        "/v1/analysis-runs/{analysis_run_id}/cancel",
        "/v1/analysis-runs/{analysis_run_id}/retry",
        "/v1/analysis-runs/{analysis_run_id}/events",
        "/v1/analysis-runs/{analysis_run_id}/artifacts",
        "/v1/export-jobs",
        "/v1/export-jobs/{export_job_id}",
        "/v1/export-jobs/{export_job_id}/cancel",
        "/v1/export-jobs/{export_job_id}/retry",
        "/v1/export-jobs/{export_job_id}/deliveries/claim",
        "/v1/export-jobs/{export_job_id}/deliveries/ack",
        "/v1/export-jobs/{export_job_id}/deliveries/fail",
        "/v1/export-jobs/{export_job_id}/download",
        "/v1/artifacts",
        "/v1/artifacts/{artifact_id}",
        "/v1/artifacts/{artifact_id}/refresh",
        "/v1/diagnostics",
        "/v1/admin/observability",
        "/v1/ws",
        "/internal/v1/channel-accounts",
        "/internal/v1/channel-accounts/{channel_account_id}",
        "/internal/v1/channel-surfaces",
        "/internal/v1/channel-surfaces/active",
        "/internal/v1/channel-surfaces/{channel_surface_id}/display-state",
        "/internal/v1/channel-surfaces/{channel_surface_id}/supersede",
        "/internal/v1/channel-surfaces/{channel_surface_id}/events",
        "/internal/v1/analysis-runs/queue",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/claim",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/progress",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/finalize",
        "/internal/v1/analysis-runs/{analysis_run_id}/request-access",
        "/internal/v1/artifacts/{artifact_id}/download-access",
        "/internal/v1/analysis-runs/{analysis_run_id}/artifacts",
        "/internal/v1/analysis-runs/{analysis_run_id}/diagnostics",
        "/internal/v1/export-jobs/queue",
        "/internal/v1/export-jobs/{export_job_id}/claim",
        "/internal/v1/export-jobs/{export_job_id}/progress",
        "/internal/v1/export-jobs/{export_job_id}/cancel-check",
        "/internal/v1/export-jobs/{export_job_id}/finalize",
        "/internal/v1/export-jobs/{export_job_id}/download-access",
        "/internal/v1/export-jobs/reclaim",
        "/internal/v1/metadata-enrichment-jobs/queue",
        "/internal/v1/metadata-enrichment-jobs/{enrichment_id}/claim",
        "/internal/v1/metadata-enrichment-jobs/{enrichment_id}/progress",
        "/internal/v1/metadata-enrichment-jobs/{enrichment_id}/finalize",
        "/internal/v1/metadata-enrichment-jobs/reclaim",
        "/internal/v1/retention/sweep",
        "/internal/v1/retention/reconcile",
    }

    assert set(spec["paths"]) == expected_paths
    assert _path_item(spec, "/v1/media-assets", "post")["operationId"] == "createMediaAsset"
    assert _path_item(spec, "/v1/selection-snapshots", "post")["operationId"] == "createSelectionSnapshot"
    assert _path_item(spec, "/v1/collections/{collection_id}/processing-runs", "post")["operationId"] == (
        "createCollectionProcessingRun"
    )
    assert _path_item(spec, "/v1/media-assets/{media_asset_id}/exports", "post")["operationId"] == (
        "createExportJob"
    )
    internal_export_access = _path_item(
        spec, "/internal/v1/export-jobs/{export_job_id}/download-access", "get"
    )
    assert internal_export_access["operationId"] == "resolveInternalExportDownloadAccess"
    assert internal_export_access["responses"]["200"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "export-job.schema.json#/$defs/exportDownloadResponse"
    )
    assert _path_item(spec, "/internal/v1/analysis-runs/{analysis_run_id}/steps/claim", "post")[
        "operationId"
    ] == "claimAnalysisRunStep"
    assert _path_item(spec, "/internal/v1/analysis-runs/{analysis_run_id}/request-access", "get")[
        "parameters"
    ][-1]["name"] == "analysis_run_step_id"


def test_public_target_routes_use_channel_scope_and_idempotency() -> None:
    spec = _contract_surface()["openapi"]

    channel_scoped = (
        ("/v1/media-assets", "get"),
        ("/v1/media-assets/{media_asset_id}", "get"),
        ("/v1/collections/inbox", "get"),
        ("/v1/collections", "get"),
        ("/v1/collections/{collection_id}", "get"),
        ("/v1/collections/{collection_id}/items/{media_asset_id}", "delete"),
        ("/v1/selection-snapshots/{selection_snapshot_id}", "get"),
        ("/v1/analysis-runs", "get"),
        ("/v1/analysis-runs/{analysis_run_id}", "get"),
        ("/v1/analysis-runs/{analysis_run_id}/events", "get"),
        ("/v1/analysis-runs/{analysis_run_id}/artifacts", "get"),
        ("/v1/export-jobs", "get"),
        ("/v1/export-jobs/{export_job_id}", "get"),
        ("/v1/export-jobs/{export_job_id}/download", "get"),
        ("/v1/artifacts", "get"),
        ("/v1/artifacts/{artifact_id}", "get"),
        ("/v1/artifacts/{artifact_id}/refresh", "post"),
        ("/v1/diagnostics", "get"),
    )
    for path, method in channel_scoped:
        assert "#/components/parameters/ChannelAccountId" in _parameter_names(_path_item(spec, path, method))

    idempotent = (
        ("/v1/media-assets", "post"),
        ("/v1/collections", "post"),
        ("/v1/selection-snapshots", "post"),
        ("/v1/analysis-runs", "post"),
        ("/v1/analysis-runs/{analysis_run_id}/retry", "post"),
        ("/v1/collections/{collection_id}/processing-runs", "post"),
        ("/v1/media-assets/{media_asset_id}/exports", "post"),
        ("/v1/media-assets/{media_asset_id}/refresh-metadata", "post"),
        ("/v1/export-jobs/{export_job_id}/retry", "post"),
    )
    for path, method in idempotent:
        assert "#/components/parameters/IdempotencyKey" in _parameter_names(_path_item(spec, path, method))


def test_target_schemas_are_channel_aware() -> None:
    surface = _contract_surface()
    media_defs = surface["media_asset"]["$defs"]
    selection_defs = surface["selection_snapshot"]["$defs"]
    run_defs = surface["analysis_run"]["$defs"]
    artifact_defs = surface["artifact"]["$defs"]
    diagnostic_subjects = set(surface["enums"]["$defs"]["diagnosticSubjectType"]["enum"])

    assert media_defs["createMediaAssetRequest"]["required"] == ["channel_account_id", "kind", "origin"]
    assert "media_asset_id" in media_defs["mediaAsset"]["required"]
    assert selection_defs["selectionSnapshot"]["properties"]["items"]["items"]["$ref"] == (
        "#/$defs/selectionSnapshotItem"
    )
    assert run_defs["createAnalysisRunRequest"]["required"] == ["channel_account_id", "selection_snapshot_id", "run_type"]
    assert artifact_defs["artifact"]["properties"]["subjects"]["items"]["$ref"] == "#/$defs/artifactSubject"
    assert {
        "media_asset",
        "stored_object",
        "selection_snapshot",
        "selection_snapshot_item",
        "analysis_run_step",
        "artifact_subject",
        "channel_surface",
        "export_job",
        "export_delivery",
        "stored_object_pin",
    }.issubset(diagnostic_subjects)


def test_worker_control_schema_uses_step_contract() -> None:
    defs = _contract_surface()["worker_control"]["$defs"]

    assert "claimStepRequest" in defs
    assert "claimStepResponse" in defs
    assert "claimRequest" not in defs
    assert "claimResponse" not in defs
    assert "sealedSelectionInput" not in defs
    assert "legacyExecutionProgressRequest" not in defs
    assert "legacyExecutionFinalizeRequest" not in defs
    assert defs["diagnosticDescriptor"]["properties"]["safe_channel_context"]["type"] == "object"


def test_processing_and_export_schemas_are_fenced_and_semantic() -> None:
    surface = _contract_surface()
    collection_defs = surface["collection"]["$defs"]
    export_defs = surface["export_job"]["$defs"]
    export_control_defs = surface["export_control"]["$defs"]

    assert collection_defs["createProcessingRunRequest"]["required"] == [
        "channel_account_id",
        "expected_version",
        "run_type",
        "selected_item_ids",
    ]
    assert export_defs["createExportJobRequest"]["properties"]["operation"]["$ref"].endswith(
        "#/$defs/exportOperation"
    )
    assert export_defs["exportJob"]["properties"]["status"]["$ref"].endswith(
        "#/$defs/exportJobStatus"
    )
    assert export_control_defs["exportClaimResponse"]["required"] == [
        "export_job",
        "attempt_token",
        "lease_owner",
        "lease_expires_at",
        "source",
    ]
    for name in ("exportProgressRequest", "exportCancelCheckRequest", "exportFinalizeRequest"):
        assert {"lease_owner", "attempt_token"}.issubset(export_control_defs[name]["required"])
    assert {"failure_code", "retryable"}.issubset(
        export_defs["failExportDeliveryRequest"]["properties"]
    )
    assert "staging_key" in export_control_defs["exportOutputPublication"]["properties"]


def test_metadata_enrichment_is_separate_fenced_and_bounded() -> None:
    surface = _contract_surface()
    enrichment_defs = surface["metadata_enrichment"]["$defs"]
    control_defs = surface["metadata_enrichment_control"]["$defs"]

    item = enrichment_defs["metadataEnrichment"]
    assert {
        "enrichment_id", "media_asset_id", "channel_account_id", "provider",
        "canonical_url", "status", "attempt_no", "max_attempts",
    }.issubset(item["required"])
    assert enrichment_defs["providerMetadata"]["properties"]["title"]["maxLength"] == 200
    assert control_defs["claimResponse"]["required"] == [
        "enrichment", "attempt_token", "lease_owner", "lease_expires_at",
    ]
    for name in ("progressRequest", "finalizeRequest"):
        assert {"lease_owner", "attempt_token"}.issubset(control_defs[name]["required"])
    assert set(control_defs["finalizeRequest"]["properties"]["outcome"]["enum"]) == {"succeeded", "failed"}
    assert "thumbnail_url" not in control_defs["finalizeRequest"]["required"]
    representative_without_thumbnail = {
        "lease_owner": "metadata-worker",
        "attempt_token": "attempt-token-123",
        "outcome": "succeeded",
        "title": "Resolved title",
        "duration_seconds": 42,
    }
    assert set(representative_without_thumbnail).issubset(control_defs["finalizeRequest"]["properties"])


def test_export_and_retention_enums_match_approved_lifecycles() -> None:
    defs = _contract_surface()["enums"]["$defs"]

    assert defs["storedObjectStatus"]["enum"] == [
        "publishing",
        "available",
        "delete_scheduled",
        "deleted",
        "missing",
    ]
    assert defs["exportOperation"]["enum"] == ["youtube_audio", "youtube_video", "video_to_audio"]
    assert defs["exportJobStatus"]["enum"] == [
        "queued",
        "claimed",
        "running",
        "cancel_requested",
        "succeeded",
        "failed",
        "canceled",
        "expired",
    ]
    assert defs["exportDeliveryStatus"]["enum"] == [
        "pending",
        "claimed",
        "delivered",
        "failed",
        "expired",
    ]
    assert {
        "export_provider_resolution_failed",
        "export_retry_exhausted",
        "export_delivery_failed",
        "retention_delete_failed",
        "retention_reconciled_orphan",
        "stored_object_missing",
        "output_missing",
    }.issubset(defs["diagnosticCode"]["enum"])
    assert "export_job" in defs["channelSurfaceSubjectType"]["enum"]


def test_retention_control_uses_bounded_batches_and_delete_fences() -> None:
    defs = _contract_surface()["retention_control"]["$defs"]

    for name in ("retentionSweepRequest", "retentionReconcileRequest"):
        assert "batch_size" in defs[name]["required"]
        assert defs[name]["properties"]["batch_size"]["maximum"] == 1000
    assert {"deletion_owner", "deletion_token", "lease_expires_at"}.issubset(
        defs["retentionClaim"]["required"]
    )


def test_contract_surface_has_no_legacy_vocabulary() -> None:
    surface_text = json.dumps(_contract_surface(), sort_keys=True)
    forbidden = (
        "media_item",
        "media-item",
        "owner_type",
        "owner_id",
        "tenant_id",
        "selection_id",
        "execution_id",
        "analysis_run_task",
        "adapter_projection",
        "safe_adapter_context",
        "/v1/media-items",
        "/v1/selections",
        "/v1/admin/reconcile-queue",
        "/executions/",
    )
    for token in forbidden:
        assert token not in surface_text


def test_no_compatibility_schema_surface_is_present() -> None:
    compatibility_root = SCHEMA_ROOT / "compatibility"
    assert not compatibility_root.exists()
