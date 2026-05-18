from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OPENAPI_PATH = ROOT / "openapi" / "openapi.yaml"
SCHEMA_ROOT = ROOT / "schemas"

def _hyphen(*parts: str) -> str:
    return "-".join(parts)


def _snake(*parts: str) -> str:
    return "_".join(parts)


STALE_PUBLIC_TOKENS = (
    _hyphen("transcription", "jobs"),
    _hyphen("batch", "drafts"),
    _hyphen("create", "transcription"),
    _hyphen("batch", "draft"),
    _hyphen("execution", "request"),
    _hyphen("grouped", "submission"),
    "Job" + "Snapshot",
    _snake("source", "set"),
    _hyphen("job", "create"),
)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_contract_surface() -> dict[str, dict]:
    print("[Contracts][validateContractSurface][BLOCK_VALIDATE_CONTRACT_SURFACE] loading inbox-first contract surface")
    return {
        "openapi": _load_json(OPENAPI_PATH),
        "enums": _load_json(SCHEMA_ROOT / "common" / "enums.schema.json"),
        "error": _load_json(SCHEMA_ROOT / "common" / "error-envelope.schema.json"),
        "pagination": _load_json(SCHEMA_ROOT / "common" / "pagination.schema.json"),
        "owner": _load_json(SCHEMA_ROOT / "common" / "owner-scope.schema.json"),
        "channel": _load_json(SCHEMA_ROOT / "common" / "channel-account.schema.json"),
        "operation_request": _load_json(SCHEMA_ROOT / "common" / "operation-request.schema.json"),
        "media_asset": _load_json(SCHEMA_ROOT / "http" / "media-asset.schema.json"),
        "media": _load_json(SCHEMA_ROOT / "http" / "media-item.schema.json"),
        "collection": _load_json(SCHEMA_ROOT / "http" / "collection.schema.json"),
        "selection_snapshot": _load_json(SCHEMA_ROOT / "http" / "selection-snapshot.schema.json"),
        "selection": _load_json(SCHEMA_ROOT / "http" / "selection.schema.json"),
        "analysis_run": _load_json(SCHEMA_ROOT / "http" / "analysis-run.schema.json"),
        "artifact": _load_json(SCHEMA_ROOT / "http" / "artifact.schema.json"),
        "diagnostic": _load_json(SCHEMA_ROOT / "http" / "diagnostic.schema.json"),
        "compatibility": _load_json(SCHEMA_ROOT / "compatibility" / "deprecated-v1.schema.json"),
        "worker_control": _load_json(SCHEMA_ROOT / "internal" / "worker-control.schema.json"),
        "ws_event": _load_json(SCHEMA_ROOT / "ws" / "run-event.schema.json"),
        "webhook_event": _load_json(SCHEMA_ROOT / "webhook" / "run-lifecycle.schema.json"),
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


def _assert_owner_scope_query_parameters(operation: dict) -> None:
    names = _parameter_names(operation)
    assert "#/components/parameters/OwnerType" in names
    assert "#/components/parameters/OwnerId" in names
    assert "#/components/parameters/TenantId" in names


def _target_operation_json(spec: dict, path: str, method: str) -> str:
    operation = _path_item(spec, path, method)
    assert operation.get("deprecated") is not True
    return json.dumps(operation, sort_keys=True)


def _assert_channel_scope_query_parameters(operation: dict) -> None:
    names = _parameter_names(operation)
    assert "#/components/parameters/ChannelAccountId" in names


def test_target_openapi_contains_media_asset_snapshot_and_channel_surface_paths() -> None:
    spec = validate_contract_surface()["openapi"]

    expected_target_paths = {
        "/v1/media-assets",
        "/v1/media-assets/upload",
        "/v1/media-assets/{media_asset_id}",
        "/v1/collections/inbox",
        "/v1/collections",
        "/v1/collections/{collection_id}",
        "/v1/collections/{collection_id}/items",
        "/v1/collections/{collection_id}/items/{media_asset_id}",
        "/v1/selection-snapshots",
        "/v1/selection-snapshots/{selection_snapshot_id}",
        "/v1/analysis-runs",
        "/v1/analysis-runs/{analysis_run_id}",
        "/v1/analysis-runs/{analysis_run_id}/cancel",
        "/v1/analysis-runs/{analysis_run_id}/retry",
        "/v1/analysis-runs/{analysis_run_id}/events",
        "/v1/analysis-runs/{analysis_run_id}/artifacts",
        "/v1/artifacts",
        "/v1/artifacts/{artifact_id}",
        "/v1/artifacts/{artifact_id}/refresh",
        "/v1/diagnostics",
        "/internal/v1/channel-accounts",
        "/internal/v1/channel-accounts/{channel_account_id}",
        "/internal/v1/channel-surfaces",
        "/internal/v1/channel-surfaces/active",
        "/internal/v1/channel-surfaces/{channel_surface_id}/display-state",
        "/internal/v1/channel-surfaces/{channel_surface_id}/supersede",
        "/internal/v1/channel-surfaces/{channel_surface_id}/events",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/claim",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/progress",
        "/internal/v1/analysis-runs/{analysis_run_id}/steps/finalize",
    }

    assert expected_target_paths.issubset(spec["paths"])
    assert _path_item(spec, "/v1/media-assets", "post")["operationId"] == "createMediaAsset"
    assert _path_item(spec, "/v1/media-assets/upload", "post")["operationId"] == "uploadMediaAsset"
    assert _path_item(spec, "/v1/media-assets/{media_asset_id}", "get")["operationId"] == "getMediaAsset"
    assert _path_item(spec, "/v1/collections/{collection_id}/items/{media_asset_id}", "delete")["operationId"] == (
        "removeCollectionItem"
    )
    assert _path_item(spec, "/v1/selection-snapshots", "post")["operationId"] == "createSelectionSnapshot"
    assert _path_item(spec, "/v1/selection-snapshots/{selection_snapshot_id}", "get")["operationId"] == (
        "getSelectionSnapshot"
    )
    assert _path_item(spec, "/internal/v1/channel-surfaces", "put")["operationId"] == "upsertChannelSurface"
    assert _path_item(spec, "/internal/v1/channel-surfaces/active", "get")["operationId"] == (
        "listActiveChannelSurfaces"
    )
    assert _path_item(spec, "/internal/v1/analysis-runs/{analysis_run_id}/steps/claim", "post")["operationId"] == (
        "claimAnalysisRunStep"
    )
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/media-assets", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/media-assets/{media_asset_id}", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/collections/inbox", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/collections", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/collections/{collection_id}", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/collections/{collection_id}/items/{media_asset_id}", "delete"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/selection-snapshots/{selection_snapshot_id}", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/analysis-runs", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/analysis-runs/{analysis_run_id}", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/analysis-runs/{analysis_run_id}/events", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/analysis-runs/{analysis_run_id}/artifacts", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/artifacts", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/artifacts/{artifact_id}", "get"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/artifacts/{artifact_id}/refresh", "post"))
    _assert_channel_scope_query_parameters(_path_item(spec, "/v1/diagnostics", "get"))


def test_target_dto_schemas_use_channel_aware_vocabulary() -> None:
    surface = validate_contract_surface()
    media_defs = surface["media_asset"]["$defs"]
    channel_defs = surface["channel"]["$defs"]
    operation_defs = surface["operation_request"]["$defs"]
    selection_defs = surface["selection_snapshot"]["$defs"]
    run_defs = surface["analysis_run"]["$defs"]
    artifact_defs = surface["artifact"]["$defs"]
    diagnostic_subjects = surface["enums"]["$defs"]["diagnosticSubjectType"]["enum"]

    assert media_defs["createMediaAssetRequest"]["required"] == ["channel_account_id", "kind", "origin"]
    assert media_defs["mediaAsset"]["required"] == [
        "media_asset_id",
        "channel_account_id",
        "origin",
        "kind",
        "status",
        "display_name",
        "retention",
        "created_at",
        "updated_at",
    ]
    assert media_defs["mediaAsset"]["properties"]["stored_object"]["$ref"] == "#/$defs/storedObject"
    assert channel_defs["channelAccount"]["required"] == [
        "channel_account_id",
        "channel",
        "external_account_ref",
        "status",
        "created_at",
        "updated_at",
    ]
    assert operation_defs["operationRequest"]["required"] == [
        "operation_request_id",
        "channel_account_id",
        "operation_type",
        "idempotency_key",
        "status",
        "created_at",
    ]
    assert selection_defs["selectionSnapshot"]["properties"]["items"]["items"]["$ref"] == (
        "#/$defs/selectionSnapshotItem"
    )
    assert run_defs["createAnalysisRunRequest"]["required"] == ["channel_account_id", "selection_snapshot_id", "run_type"]
    assert run_defs["analysisRun"]["properties"]["selection_snapshot"]["$ref"].endswith(
        "selection-snapshot.schema.json#/$defs/selectionSnapshot"
    )
    assert artifact_defs["artifact"]["properties"]["subjects"]["items"]["$ref"] == "#/$defs/artifactSubject"
    assert {
        "media_asset",
        "stored_object",
        "selection_snapshot",
        "selection_snapshot_item",
        "analysis_run_step",
        "artifact_subject",
        "channel_surface",
    }.issubset(diagnostic_subjects)


def test_compatibility_routes_are_deprecated_and_map_one_to_one_to_target_dtos() -> None:
    surface = validate_contract_surface()
    spec = surface["openapi"]
    compatibility_defs = surface["compatibility"]["$defs"]

    compatibility_operations = (
        ("/v1/media-items", "post", "mediaAsset"),
        ("/v1/media-items", "get", "mediaAsset"),
        ("/v1/media-items/{media_item_id}", "get", "mediaAsset"),
        ("/v1/media-items/{media_item_id}", "delete", "mediaAsset"),
        ("/v1/selections", "post", "selectionSnapshot"),
        ("/v1/selections/{selection_id}", "get", "selectionSnapshot"),
    )
    for path, method, target in compatibility_operations:
        operation = _path_item(spec, path, method)
        assert operation["deprecated"] is True
        assert operation["x-compatibility"]["target"] == target
        assert operation["x-compatibility"]["policy"] == "deprecated_transition_only"

    assert compatibility_defs["mediaItemCompatibility"]["x-target-mapping"]["target_schema"] == "mediaAsset"
    assert compatibility_defs["mediaItemCompatibility"]["x-target-mapping"]["id_field"] == (
        "media_item_id -> media_asset_id"
    )
    assert compatibility_defs["selectionCompatibility"]["x-target-mapping"]["target_schema"] == "selectionSnapshot"
    assert compatibility_defs["selectionCompatibility"]["x-target-mapping"]["id_field"] == (
        "selection_id -> selection_snapshot_id"
    )


def test_target_operations_do_not_reintroduce_compatibility_names() -> None:
    spec = validate_contract_surface()["openapi"]
    target_operations = (
        ("/v1/media-assets", "post"),
        ("/v1/media-assets", "get"),
        ("/v1/media-assets/upload", "post"),
        ("/v1/media-assets/{media_asset_id}", "get"),
        ("/v1/media-assets/{media_asset_id}", "delete"),
        ("/v1/collections/inbox", "get"),
        ("/v1/collections", "post"),
        ("/v1/collections", "get"),
        ("/v1/collections/{collection_id}", "get"),
        ("/v1/collections/{collection_id}", "patch"),
        ("/v1/collections/{collection_id}/items", "post"),
        ("/v1/collections/{collection_id}/items/{media_asset_id}", "delete"),
        ("/v1/selection-snapshots", "post"),
        ("/v1/selection-snapshots/{selection_snapshot_id}", "get"),
        ("/v1/analysis-runs", "post"),
        ("/v1/analysis-runs", "get"),
        ("/v1/analysis-runs/{analysis_run_id}", "get"),
        ("/v1/analysis-runs/{analysis_run_id}/cancel", "post"),
        ("/v1/analysis-runs/{analysis_run_id}/retry", "post"),
        ("/v1/analysis-runs/{analysis_run_id}/events", "get"),
        ("/v1/analysis-runs/{analysis_run_id}/artifacts", "get"),
        ("/v1/artifacts", "get"),
        ("/v1/artifacts/{artifact_id}", "get"),
        ("/v1/artifacts/{artifact_id}/refresh", "post"),
        ("/v1/diagnostics", "get"),
        ("/internal/v1/analysis-runs/{analysis_run_id}/steps/claim", "post"),
        ("/internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check", "get"),
        ("/internal/v1/analysis-runs/{analysis_run_id}/steps/progress", "post"),
        ("/internal/v1/analysis-runs/{analysis_run_id}/steps/finalize", "post"),
        ("/internal/v1/channel-surfaces", "put"),
        ("/internal/v1/channel-surfaces/active", "get"),
    )
    disallowed_tokens = (
        "media_item",
        "media-item",
        "owner",
        "selection_id",
        "analysis_run_task",
        "adapter_projection",
        "telegram_message_id",
    )

    for path, method in target_operations:
        operation_json = _target_operation_json(spec, path, method)
        for token in disallowed_tokens:
            assert token not in operation_json, f"{token!r} leaked into target {method.upper()} {path}"


def test_openapi_contains_final_inbox_first_public_paths() -> None:
    spec = validate_contract_surface()["openapi"]

    expected_paths = {
        "/v1/media-items",
        "/v1/media-items/{media_item_id}",
        "/v1/collections/inbox",
        "/v1/collections",
        "/v1/collections/{collection_id}",
        "/v1/collections/{collection_id}/items",
        "/v1/collections/{collection_id}/items/{media_asset_id}",
        "/v1/selections",
        "/v1/selections/{selection_id}",
        "/v1/analysis-runs",
        "/v1/analysis-runs/{analysis_run_id}",
        "/v1/analysis-runs/{analysis_run_id}/cancel",
        "/v1/analysis-runs/{analysis_run_id}/retry",
        "/v1/analysis-runs/{analysis_run_id}/events",
        "/v1/analysis-runs/{analysis_run_id}/artifacts",
        "/v1/artifacts",
        "/v1/artifacts/{artifact_id}",
        "/v1/artifacts/{artifact_id}/refresh",
        "/v1/diagnostics",
        "/v1/admin/reconcile-queue",
        "/v1/admin/observability",
        "/v1/ws",
        "/internal/v1/analysis-runs/{analysis_run_id}/request-access",
        "/internal/v1/analysis-runs/{analysis_run_id}/executions/cancel-check",
        "/internal/v1/artifacts/{artifact_id}/download-access",
    }

    assert expected_paths.issubset(spec["paths"])
    assert not {
        f"/v1/{_hyphen('transcription', 'jobs')}",
        f"/v1/{_hyphen('transcription', 'jobs')}/combined",
        f"/v1/{_hyphen('transcription', 'jobs')}/from-url",
        f"/v1/{_hyphen('transcription', 'jobs')}/batch",
        f"/v1/{_hyphen('batch', 'drafts')}",
        "/v1/agent-runs",
        "/v1/jobs",
        f"/v1/jobs/{{{'_'.join(('job', 'id'))}}}",
    }.intersection(spec["paths"])


def test_public_operations_use_inbox_first_vocabulary_and_idempotency_boundaries() -> None:
    spec = validate_contract_surface()["openapi"]

    assert _path_item(spec, "/v1/media-items", "post")["operationId"] == "addMediaItem"
    assert _path_item(spec, "/v1/collections", "post")["operationId"] == "createCollection"
    assert _path_item(spec, "/v1/selections", "post")["operationId"] == "createSelection"
    assert _path_item(spec, "/v1/analysis-runs", "post")["operationId"] == "run_analysis"
    assert _path_item(spec, "/v1/analysis-runs/{analysis_run_id}", "get")["operationId"] == "getAnalysisRun"
    assert _path_item(spec, "/v1/analysis-runs/{analysis_run_id}/retry", "post")["operationId"] == "retryAnalysisRun"
    assert _path_item(spec, "/v1/artifacts/{artifact_id}", "get")["operationId"] == "getArtifact"
    assert _path_item(spec, "/v1/artifacts/{artifact_id}/refresh", "post")["operationId"] == "refreshArtifactLink"
    assert _path_item(spec, "/v1/diagnostics", "get")["operationId"] == "getDiagnostics"
    assert _path_item(spec, "/v1/admin/reconcile-queue", "post")["operationId"] == "reconcileAnalysisRunQueue"
    assert _path_item(spec, "/v1/admin/observability", "get")["operationId"] == "getObservabilitySnapshot"

    for path in ("/v1/media-items", "/v1/collections", "/v1/selections", "/v1/analysis-runs"):
        assert "#/components/parameters/IdempotencyKey" in _parameter_names(_path_item(spec, path, "post"))
    assert "#/components/parameters/IdempotencyKey" in _parameter_names(
        _path_item(spec, "/v1/analysis-runs/{analysis_run_id}/retry", "post")
    )
    assert "query" in _parameter_names(_path_item(spec, "/v1/media-items", "get"))

    assert "#/components/parameters/ExpectedVersion" in _parameter_names(
        _path_item(spec, "/v1/collections/{collection_id}/items/{media_asset_id}", "delete")
    )
    diagnostics_parameters = _parameter_names(_path_item(spec, "/v1/diagnostics", "get"))
    assert "severity" in diagnostics_parameters
    assert "code" in diagnostics_parameters
    assert "correlation_id" in diagnostics_parameters


def test_owner_scoped_public_routes_document_owner_scope_query_contract() -> None:
    spec = validate_contract_surface()["openapi"]
    parameters = spec["components"]["parameters"]

    assert parameters["OwnerType"] == {
        "name": "owner_type",
        "in": "query",
        "required": True,
        "schema": {"$ref": "../schemas/common/enums.schema.json#/$defs/ownerType"},
    }
    assert parameters["OwnerId"] == {
        "name": "owner_id",
        "in": "query",
        "required": True,
        "schema": {"type": "string", "minLength": 1},
    }
    assert parameters["TenantId"] == {
        "name": "tenant_id",
        "in": "query",
        "required": False,
        "schema": {"type": ["string", "null"], "minLength": 1},
    }

    owner_scoped_operations = (
        ("/v1/media-items", "get"),
        ("/v1/media-items/{media_item_id}", "get"),
        ("/v1/media-items/{media_item_id}", "delete"),
        ("/v1/selections/{selection_id}", "get"),
    )

    for path, method in owner_scoped_operations:
        _assert_owner_scope_query_parameters(_path_item(spec, path, method))


def test_diagnostics_route_documents_all_supported_filters() -> None:
    spec = validate_contract_surface()["openapi"]
    parameter_names = _parameter_names(_path_item(spec, "/v1/diagnostics", "get"))

    assert {"subject_type", "subject_id", "severity", "code", "correlation_id"}.issubset(parameter_names)


def test_media_inputs_are_first_class_and_not_hidden_in_execution_requests() -> None:
    surface = validate_contract_surface()
    media_defs = surface["media"]["$defs"]
    media_kind_enum = surface["enums"]["$defs"]["mediaKind"]["enum"]

    assert media_kind_enum == ["text", "url", "file", "photo", "image", "audio", "voice", "video", "document"]
    assert media_defs["source"]["oneOf"] == [
        {"$ref": "#/$defs/textSource"},
        {"$ref": "#/$defs/urlSource"},
        {"$ref": "#/$defs/objectSource"},
    ]
    assert media_defs["textSource"]["properties"]["origin_type"]["const"] == "text"
    assert media_defs["urlSource"]["properties"]["origin_type"]["const"] == "url"
    assert media_defs["objectSource"]["properties"]["origin_type"]["const"] == "object"
    assert media_defs["addMediaItemRequest"]["required"] == ["owner", "kind", "source"]
    assert media_defs["addMediaItemMultipartRequest"]["properties"]["metadata"]["contentSchema"]["$ref"] == (
        "#/$defs/addMediaItemMultipartMetadata"
    )
    assert media_defs["mediaItem"]["properties"]["diagnostics"]["items"]["$ref"].endswith(
        "diagnostic.schema.json#/$defs/diagnosticSummary"
    )


def test_collections_inbox_and_optimistic_versions_are_contractual() -> None:
    surface = validate_contract_surface()
    collection_defs = surface["collection"]["$defs"]
    pagination_defs = surface["pagination"]["$defs"]

    collection = collection_defs["collection"]
    collection_item = collection_defs["collectionItem"]
    update_request = collection_defs["updateCollectionItemsRequest"]

    assert collection["required"] == [
        "collection_id",
        "channel_account_id",
        "kind",
        "name",
        "status",
        "version",
        "items",
        "created_at",
        "updated_at",
    ]
    assert collection["properties"]["kind"]["$ref"].endswith("collectionKind")
    assert collection["properties"]["version"]["$ref"].endswith("optimisticVersion")
    assert collection_item["required"] == ["media_asset_id", "position", "added_at"]
    assert update_request["required"] == ["channel_account_id", "expected_version", "items"]
    assert update_request["properties"]["expected_version"]["$ref"].endswith("optimisticVersion")
    assert pagination_defs["conflictEnvelope"]["required"] == ["code", "message", "expected_version", "actual_version"]


def test_selection_snapshot_is_immutable_and_run_creation_requires_selection() -> None:
    surface = validate_contract_surface()
    selection_defs = surface["selection_snapshot"]["$defs"]
    run_defs = surface["analysis_run"]["$defs"]

    selection = selection_defs["selectionSnapshot"]
    item_snapshot = selection_defs["selectionSnapshotItem"]
    run_request = run_defs["createAnalysisRunRequest"]
    retry_request = run_defs["retryAnalysisRunRequest"]
    run = run_defs["analysisRun"]

    assert selection["required"] == [
        "selection_snapshot_id",
        "channel_account_id",
        "status",
        "items",
        "option_snapshot",
        "created_at",
        "sealed_at",
    ]
    assert selection["properties"]["items"]["minItems"] == 1
    assert selection["properties"]["status"]["$ref"].endswith("selectionSnapshotStatus")
    assert item_snapshot["required"] == [
        "selection_snapshot_item_id",
        "position",
        "media_asset_id",
        "kind",
        "display_name",
        "origin_snapshot",
        "storage_snapshot",
        "metadata_snapshot",
        "status_at_selection",
    ]
    assert item_snapshot["properties"]["origin_snapshot"]["$ref"].endswith(
        "media-asset.schema.json#/$defs/mediaAssetOrigin"
    )
    assert run_request["required"] == ["channel_account_id", "selection_snapshot_id", "run_type"]
    assert retry_request["required"] == ["channel_account_id"]
    assert run["properties"]["selection_snapshot"]["$ref"].endswith(
        "selection-snapshot.schema.json#/$defs/selectionSnapshot"
    )
    assert run["properties"]["status"]["$ref"].endswith("analysisRunStatus")
    assert run["properties"]["idempotency"]["$ref"].endswith("idempotencyRecord")


def test_run_events_artifacts_diagnostics_and_retention_are_first_class() -> None:
    surface = validate_contract_surface()
    run_defs = surface["analysis_run"]["$defs"]
    artifact_defs = surface["artifact"]["$defs"]
    diagnostic_defs = surface["diagnostic"]["$defs"]
    enums = surface["enums"]["$defs"]

    assert "partially_succeeded" in enums["analysisRunStatus"]["enum"]
    assert "artifact.created" in enums["runEventType"]["enum"]
    assert "diagnostic.recorded" in enums["runEventType"]["enum"]
    assert "retention_denied" in enums["diagnosticCode"]["enum"]
    assert "artifact_resolution_failed" in enums["diagnosticCode"]["enum"]
    assert "orphan_object_cleanup_failed" in enums["diagnosticCode"]["enum"]
    assert "run_manifest" in enums["artifactKind"]["enum"]
    assert "run_diagnostics" in enums["artifactKind"]["enum"]

    event = run_defs["runEvent"]
    assert event["required"] == ["event_id", "analysis_run_id", "event_type", "version", "emitted_at", "payload"]
    assert event["properties"]["artifact"]["$ref"].endswith("artifact.schema.json#/$defs/artifactSummary")
    assert event["properties"]["diagnostic"]["$ref"].endswith("diagnostic.schema.json#/$defs/diagnosticSummary")

    artifact = artifact_defs["artifact"]
    assert artifact["properties"]["preview"]["$ref"] == "#/$defs/artifactPreview"
    assert artifact["properties"]["download"]["$ref"] == "#/$defs/artifactDownload"
    assert artifact["properties"]["retention"]["$ref"].endswith("retentionMetadata")

    diagnostic = diagnostic_defs["diagnostic"]
    assert diagnostic["required"] == [
        "diagnostic_id",
        "channel_account_id",
        "subject",
        "severity",
        "code",
        "message",
        "created_at",
    ]
    observability = diagnostic_defs["observabilitySnapshot"]
    assert observability["required"] == [
        "queue_tasks",
        "queue_lag_seconds",
        "cleanup_failures",
        "artifact_resolution_failures",
        "generated_at",
    ]


def test_owner_scope_error_pagination_and_adapter_identity_are_shared_envelopes() -> None:
    surface = validate_contract_surface()
    owner_defs = surface["owner"]["$defs"]
    error = surface["error"]
    pagination_defs = surface["pagination"]["$defs"]

    assert owner_defs["ownerScope"]["required"] == ["owner_type", "owner_id"]
    assert owner_defs["ownerScope"]["properties"]["owner_type"]["$ref"].endswith("ownerType")
    assert {"telegram_chat_id", "telegram_user_id", "web_session_id", "mcp_caller_id"}.issubset(
        owner_defs["adapterIdentity"]["properties"]
    )
    assert error["properties"]["error"]["properties"]["diagnostics"]["items"]["$ref"].endswith(
        "diagnostic.schema.json#/$defs/diagnosticSummary"
    )
    assert error["properties"]["error"]["properties"]["conflict"]["$ref"].endswith("conflictEnvelope")
    assert pagination_defs["paginatedResponse"]["required"] == ["items", "page"]
    assert pagination_defs["page"]["required"] == ["page_size", "has_more"]


def test_internal_worker_contract_consumes_sealed_selection_and_publishes_run_outputs() -> None:
    surface = validate_contract_surface()
    worker_defs = surface["worker_control"]["$defs"]
    enums = surface["enums"]["$defs"]

    claim_response = worker_defs["claimStepResponse"]
    selection_input = worker_defs["selectionSnapshotInput"]
    progress_request = worker_defs["progressRequest"]
    artifact_request = worker_defs["artifactUpsertRequest"]
    diagnostic_request = worker_defs["diagnosticUpsertRequest"]
    finalize_request = worker_defs["finalizeRequest"]
    queue_response = worker_defs["analysisRunQueueResponse"]
    request_access_response = worker_defs["requestAccessResponse"]
    cancel_check_response = worker_defs["cancelCheckResponse"]
    artifact_download_response = worker_defs["artifactDownloadAccessResponse"]

    assert claim_response["required"] == [
        "analysis_run_step_id",
        "analysis_run_id",
        "run_type",
        "selection_snapshot",
        "analysis_run_step_inputs",
        "params",
        "claimed_at",
    ]
    assert claim_response["properties"]["selection_snapshot"]["$ref"] == "#/$defs/selectionSnapshotInput"
    assert selection_input["properties"]["items"]["items"]["$ref"] == "../http/selection-snapshot.schema.json#/$defs/selectionSnapshotItem"
    assert request_access_response["required"] == [
        "provider",
        "url",
        "expires_at",
        "request_ref",
        "request_digest_sha256",
        "request_bytes",
    ]
    assert request_access_response["properties"]["request_digest_sha256"]["pattern"] == "^[a-f0-9]{64}$"
    assert cancel_check_response["required"] == ["cancel_requested", "status"]
    assert cancel_check_response["properties"]["status"]["$ref"].endswith("analysisRunStatus")
    assert artifact_download_response["properties"]["artifact_kind"]["$ref"] == "#/$defs/workerArtifactDescriptorKind"
    assert artifact_download_response["properties"]["download"]["required"] == ["provider", "url", "expires_at"]
    item_props = surface["selection_snapshot"]["$defs"]["selectionSnapshotItem"]["properties"]
    assert {"selection_snapshot_item_id", "media_asset_id", "kind", "origin_snapshot", "storage_snapshot"}.issubset(item_props)
    assert queue_response["properties"]["items"]["items"]["$ref"] == "#/$defs/analysisRunQueueItem"
    queue_item = worker_defs["analysisRunQueueItem"]
    assert "analysis_run_step_id" in queue_item["required"]
    assert "step_kind" in queue_item["required"]
    assert "task_type" not in queue_item["properties"]
    assert progress_request["required"] == ["analysis_run_step_id", "progress_stage"]
    assert artifact_request["properties"]["artifacts"]["items"]["$ref"] == "#/$defs/artifactDescriptor"
    assert artifact_request["required"] == ["analysis_run_step_id", "artifacts"]
    assert diagnostic_request["properties"]["diagnostics"]["items"]["$ref"] == "#/$defs/diagnosticDescriptor"
    assert diagnostic_request["required"] == ["analysis_run_step_id", "diagnostics"]
    assert finalize_request["required"] == ["analysis_run_step_id", "outcome"]
    artifact_descriptor = worker_defs["artifactDescriptor"]
    assert {"artifact_kind", "mime_type", "object_key", "size_bytes", "filename", "format"}.issubset(
        artifact_descriptor["properties"]
    )
    assert artifact_descriptor["properties"]["artifact_kind"]["$ref"] == "#/$defs/workerArtifactDescriptorKind"
    worker_artifact_kinds = worker_defs["workerArtifactDescriptorKind"]["enum"]
    assert {"transcript_plain", "transcript_segmented_markdown", "summary_markdown", "agent_result_json"}.issubset(
        worker_artifact_kinds
    )
    assert not {"transcript_plain", "transcript_segmented_markdown", "summary_markdown", "agent_result_json"}.intersection(
        enums["artifactKind"]["enum"]
    )
    diagnostic_descriptor = worker_defs["diagnosticDescriptor"]
    assert {"diagnostic_id", "subject_type", "subject_id", "context", "created_at"}.issubset(
        diagnostic_descriptor["properties"]
    )


def test_stale_public_route_and_schema_vocabulary_is_absent_from_contract_package() -> None:
    for path in [OPENAPI_PATH, *SCHEMA_ROOT.rglob("*.json")]:
        text = path.read_text(encoding="utf-8")
        for token in STALE_PUBLIC_TOKENS:
            assert token not in text, f"{token!r} leaked into {path.relative_to(ROOT)}"
