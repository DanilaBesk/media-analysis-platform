-- +goose Up
-- Local target reset: this database is disposable during the single-user,
-- channel-aware rebuild. The reset removes both legacy and target tables before
-- recreating the target contract.
DROP TABLE IF EXISTS channel_surface_events;
DROP TABLE IF EXISTS channel_surface_subjects;
DROP TABLE IF EXISTS channel_surfaces;
DROP TABLE IF EXISTS diagnostics;
DROP TABLE IF EXISTS artifact_subjects;
DROP TABLE IF EXISTS analysis_run_step_inputs;
DROP TABLE IF EXISTS artifacts;
DROP TABLE IF EXISTS analysis_run_events;
DROP TABLE IF EXISTS analysis_run_steps;
DROP TABLE IF EXISTS analysis_runs;
DROP TABLE IF EXISTS selection_snapshot_items;
DROP TABLE IF EXISTS selection_snapshots;
DROP TABLE IF EXISTS collection_items;
DROP TABLE IF EXISTS collections;
DROP TABLE IF EXISTS media_assets;
DROP TABLE IF EXISTS stored_objects;
DROP TABLE IF EXISTS operation_requests;
DROP TABLE IF EXISTS channel_accounts;
DROP TABLE IF EXISTS analysis_run_tasks;
DROP TABLE IF EXISTS selection_items;
DROP TABLE IF EXISTS selections;
DROP TABLE IF EXISTS media_items;
DROP TABLE IF EXISTS sources;

DROP FUNCTION IF EXISTS prevent_selection_snapshots_mutation();

CREATE TABLE channel_accounts (
    id uuid PRIMARY KEY,
    channel text NOT NULL,
    external_account_ref text NOT NULL,
    display_name text NULL,
    status text NOT NULL DEFAULT 'active',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NULL,
    disabled_at timestamptz NULL,
    CONSTRAINT channel_accounts_status_chk CHECK (status IN ('active', 'disabled'))
);

CREATE UNIQUE INDEX channel_accounts_channel_ref_unique_idx
    ON channel_accounts (channel, external_account_ref);

CREATE TABLE operation_requests (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    operation_type text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NULL,
    status text NOT NULL DEFAULT 'accepted',
    target_type text NULL,
    target_id uuid NULL,
    error_code text NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz NULL,
    CONSTRAINT operation_requests_status_chk CHECK (status IN ('accepted', 'completed', 'failed', 'conflict'))
);

CREATE UNIQUE INDEX operation_requests_channel_idempotency_unique_idx
    ON operation_requests (channel_account_id, operation_type, idempotency_key);
CREATE INDEX operation_requests_created_at_idx
    ON operation_requests (created_at DESC);

CREATE TABLE stored_objects (
    id uuid PRIMARY KEY,
    bucket text NOT NULL,
    object_key text NOT NULL,
    content_type text NULL,
    size_bytes bigint NOT NULL DEFAULT 0,
    checksum text NULL,
    storage_status text NOT NULL DEFAULT 'available',
    retention_state text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NULL,
    deleted_at timestamptz NULL,
    CONSTRAINT stored_objects_storage_status_chk CHECK (storage_status IN ('pending', 'available', 'missing', 'deleted')),
    CONSTRAINT stored_objects_retention_state_chk CHECK (retention_state IN ('active', 'soft_deleted', 'expires_scheduled', 'expired', 'held'))
);

CREATE UNIQUE INDEX stored_objects_bucket_key_unique_idx
    ON stored_objects (bucket, object_key);
CREATE INDEX stored_objects_retention_idx
    ON stored_objects (retention_state, expires_at)
    WHERE deleted_at IS NULL;

CREATE TABLE media_assets (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    stored_object_id uuid REFERENCES stored_objects(id),
    origin_type text NOT NULL,
    origin_ref text NULL,
    kind text NOT NULL,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'available',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz NULL,
    CONSTRAINT media_assets_origin_type_chk CHECK (origin_type IN ('text', 'url', 'upload', 'telegram_file', 'generated')),
    CONSTRAINT media_assets_status_chk CHECK (status IN ('ingesting', 'available', 'invalid', 'quarantined', 'deleted'))
);

CREATE INDEX media_assets_channel_status_created_at_desc_idx
    ON media_assets (channel_account_id, status, created_at DESC);
CREATE INDEX media_assets_stored_object_id_idx
    ON media_assets (stored_object_id)
    WHERE stored_object_id IS NOT NULL;

CREATE TABLE collections (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    kind text NOT NULL,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    archived_at timestamptz NULL,
    deleted_at timestamptz NULL,
    CONSTRAINT collections_kind_chk CHECK (kind IN ('inbox', 'user')),
    CONSTRAINT collections_status_chk CHECK (status IN ('active', 'archived', 'deleted')),
    CONSTRAINT collections_version_positive_chk CHECK (version >= 1)
);

CREATE UNIQUE INDEX collections_active_inbox_channel_unique_idx
    ON collections (channel_account_id, kind)
    WHERE kind = 'inbox' AND status = 'active' AND deleted_at IS NULL;
CREATE INDEX collections_channel_updated_at_desc_idx
    ON collections (channel_account_id, updated_at DESC);

CREATE TABLE collection_items (
    id uuid PRIMARY KEY,
    collection_id uuid NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    media_asset_id uuid NOT NULL REFERENCES media_assets(id),
    position int NOT NULL,
    added_via_channel_account_id uuid REFERENCES channel_accounts(id),
    added_at timestamptz NOT NULL DEFAULT now(),
    removed_at timestamptz NULL,
    CONSTRAINT collection_items_position_nonnegative_chk CHECK (position >= 0)
);

CREATE UNIQUE INDEX collection_items_active_asset_unique_idx
    ON collection_items (collection_id, media_asset_id)
    WHERE removed_at IS NULL;
CREATE UNIQUE INDEX collection_items_active_position_unique_idx
    ON collection_items (collection_id, position)
    WHERE removed_at IS NULL;
CREATE INDEX collection_items_collection_position_idx
    ON collection_items (collection_id, position ASC);

CREATE TABLE selection_snapshots (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    source_collection_id uuid REFERENCES collections(id),
    status text NOT NULL DEFAULT 'sealed',
    option_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_via_channel_account_id uuid REFERENCES channel_accounts(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    sealed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT selection_snapshots_status_chk CHECK (status IN ('sealed', 'invalidated'))
);

CREATE INDEX selection_snapshots_channel_created_at_desc_idx
    ON selection_snapshots (channel_account_id, created_at DESC);

CREATE TABLE selection_snapshot_items (
    id uuid PRIMARY KEY,
    selection_snapshot_id uuid NOT NULL REFERENCES selection_snapshots(id) ON DELETE CASCADE,
    position int NOT NULL,
    media_asset_id uuid NOT NULL REFERENCES media_assets(id),
    kind text NOT NULL,
    display_name text NOT NULL,
    origin_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    storage_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    status_at_selection text NOT NULL,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb
);

CREATE UNIQUE INDEX selection_snapshot_items_position_unique_idx
    ON selection_snapshot_items (selection_snapshot_id, position);
CREATE INDEX selection_snapshot_items_asset_idx
    ON selection_snapshot_items (media_asset_id);

-- +goose StatementBegin
CREATE FUNCTION prevent_selection_snapshots_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'selection_snapshots are immutable';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER selection_snapshots_immutable_update_trg
    BEFORE UPDATE ON selection_snapshots
    FOR EACH ROW EXECUTE FUNCTION prevent_selection_snapshots_mutation();

CREATE TRIGGER selection_snapshots_immutable_delete_trg
    BEFORE DELETE ON selection_snapshots
    FOR EACH ROW EXECUTE FUNCTION prevent_selection_snapshots_mutation();

CREATE TABLE analysis_runs (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    selection_snapshot_id uuid NOT NULL REFERENCES selection_snapshots(id),
    run_type text NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    version bigint NOT NULL DEFAULT 1,
    idempotency_key text NULL,
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    delivery jsonb NOT NULL DEFAULT '{"strategy":"polling"}'::jsonb,
    evidence_gate_state text NOT NULL DEFAULT 'not_required',
    created_via_channel_account_id uuid REFERENCES channel_accounts(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    cancel_requested_at timestamptz NULL,
    canceled_at timestamptz NULL,
    expires_at timestamptz NULL,
    CONSTRAINT analysis_runs_status_chk CHECK (status IN ('queued', 'claiming', 'running', 'cancel_requested', 'partially_succeeded', 'succeeded', 'failed', 'canceled', 'expired')),
    CONSTRAINT analysis_runs_version_positive_chk CHECK (version >= 1)
);

CREATE INDEX analysis_runs_channel_created_at_desc_idx
    ON analysis_runs (channel_account_id, created_at DESC);
CREATE UNIQUE INDEX analysis_runs_channel_idempotency_unique_idx
    ON analysis_runs (channel_account_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE analysis_run_steps (
    id uuid PRIMARY KEY,
    analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
    step_kind text NOT NULL,
    worker_kind text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempt_no int NOT NULL DEFAULT 1,
    lease_owner text NULL,
    claimed_at timestamptz NULL,
    heartbeat_at timestamptz NULL,
    finalized_at timestamptz NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analysis_run_steps_status_chk CHECK (status IN ('pending', 'queued', 'claimed', 'succeeded', 'partially_succeeded', 'failed', 'canceled')),
    CONSTRAINT analysis_run_steps_attempt_positive_chk CHECK (attempt_no >= 1)
);

CREATE INDEX analysis_run_steps_run_status_idx
    ON analysis_run_steps (analysis_run_id, status);
CREATE INDEX analysis_run_steps_worker_status_created_at_idx
    ON analysis_run_steps (worker_kind, status, created_at ASC);

CREATE TABLE analysis_run_events (
    id uuid PRIMARY KEY,
    analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    version bigint NOT NULL,
    status text NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analysis_run_events_run_version_unique UNIQUE (analysis_run_id, version)
);

CREATE INDEX analysis_run_events_run_created_at_idx
    ON analysis_run_events (analysis_run_id, created_at ASC);

CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
    stored_object_id uuid REFERENCES stored_objects(id),
    kind text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    content_type text NOT NULL,
    checksum text NULL,
    size_bytes bigint NOT NULL DEFAULT 0,
    visibility text NOT NULL DEFAULT 'private',
    preview jsonb NOT NULL DEFAULT '{"available":false}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NULL,
    deleted_at timestamptz NULL,
    CONSTRAINT artifacts_status_chk CHECK (status IN ('pending', 'available', 'failed', 'expired', 'deleted')),
    CONSTRAINT artifacts_visibility_chk CHECK (visibility IN ('private', 'channel_deliverable'))
);

CREATE INDEX artifacts_run_created_at_idx
    ON artifacts (analysis_run_id, created_at ASC);
CREATE INDEX artifacts_channel_created_at_desc_idx
    ON artifacts (channel_account_id, created_at DESC);

CREATE TABLE analysis_run_step_inputs (
    id uuid PRIMARY KEY,
    analysis_run_step_id uuid NOT NULL REFERENCES analysis_run_steps(id) ON DELETE CASCADE,
    input_kind text NOT NULL,
    selection_snapshot_item_id uuid REFERENCES selection_snapshot_items(id),
    artifact_id uuid REFERENCES artifacts(id),
    position int NOT NULL DEFAULT 0,
    required boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analysis_run_step_inputs_kind_chk CHECK (input_kind IN ('selection_snapshot_item', 'transcript_artifact', 'text_corpus_artifact', 'metadata_artifact')),
    CONSTRAINT analysis_run_step_inputs_subject_chk CHECK (
        (input_kind = 'selection_snapshot_item' AND selection_snapshot_item_id IS NOT NULL AND artifact_id IS NULL) OR
        (input_kind IN ('transcript_artifact', 'text_corpus_artifact', 'metadata_artifact') AND artifact_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX analysis_run_step_inputs_position_unique_idx
    ON analysis_run_step_inputs (analysis_run_step_id, input_kind, position);

CREATE TABLE artifact_subjects (
    id uuid PRIMARY KEY,
    artifact_id uuid NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    subject_role text NOT NULL DEFAULT 'primary',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT artifact_subjects_type_chk CHECK (subject_type IN ('analysis_run', 'analysis_run_step', 'selection_snapshot', 'selection_snapshot_item', 'media_asset', 'diagnostic')),
    CONSTRAINT artifact_subjects_role_chk CHECK (subject_role IN ('primary', 'source', 'result', 'diagnostic', 'manifest_entry'))
);

CREATE INDEX artifact_subjects_subject_idx
    ON artifact_subjects (subject_type, subject_id, subject_role);

CREATE TABLE diagnostics (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    severity text NOT NULL,
    code text NOT NULL,
    message text NOT NULL,
    context jsonb NOT NULL DEFAULT '{}'::jsonb,
    safe_channel_context jsonb NOT NULL DEFAULT '{}'::jsonb,
    correlation_id text NULL,
    remediation_hint text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT diagnostics_severity_chk CHECK (severity IN ('info', 'warning', 'error'))
);

CREATE INDEX diagnostics_channel_subject_created_at_desc_idx
    ON diagnostics (channel_account_id, subject_type, subject_id, created_at DESC);
CREATE INDEX diagnostics_channel_code_created_at_desc_idx
    ON diagnostics (channel_account_id, code, created_at DESC);

CREATE TABLE channel_surfaces (
    id uuid PRIMARY KEY,
    channel_account_id uuid REFERENCES channel_accounts(id),
    channel text NOT NULL,
    surface_type text NOT NULL,
    surface_key text NOT NULL,
    address jsonb NOT NULL DEFAULT '{}'::jsonb,
    address_fingerprint text NULL,
    display_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    lifecycle_status text NOT NULL DEFAULT 'active',
    version bigint NOT NULL DEFAULT 1,
    idempotency_key text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_rendered_at timestamptz NULL,
    superseded_at timestamptz NULL,
    deleted_at timestamptz NULL,
    CONSTRAINT channel_surfaces_status_chk CHECK (lifecycle_status IN ('active', 'superseded', 'deleted', 'failed')),
    CONSTRAINT channel_surfaces_version_positive_chk CHECK (version >= 1)
);

CREATE UNIQUE INDEX channel_surfaces_active_key_idx
    ON channel_surfaces (channel_account_id, channel, surface_type, surface_key)
    WHERE lifecycle_status = 'active' AND deleted_at IS NULL;
CREATE UNIQUE INDEX channel_surfaces_active_address_idx
    ON channel_surfaces (channel_account_id, channel, address_fingerprint)
    WHERE address_fingerprint IS NOT NULL AND lifecycle_status = 'active' AND deleted_at IS NULL;
CREATE INDEX channel_surfaces_active_subject_lookup_idx
    ON channel_surfaces (channel_account_id, channel, lifecycle_status)
    WHERE deleted_at IS NULL;

CREATE TABLE channel_surface_subjects (
    surface_id uuid NOT NULL REFERENCES channel_surfaces(id) ON DELETE CASCADE,
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    subject_role text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (surface_id, subject_role, subject_type, subject_id),
    CONSTRAINT channel_surface_subjects_role_chk CHECK (subject_role IN ('primary', 'context', 'result', 'diagnostic', 'navigation_target'))
);

CREATE UNIQUE INDEX channel_surface_one_primary_subject_idx
    ON channel_surface_subjects (surface_id)
    WHERE subject_role = 'primary';
CREATE INDEX channel_surface_subjects_subject_idx
    ON channel_surface_subjects (subject_type, subject_id, subject_role);

CREATE TABLE channel_surface_events (
    id uuid PRIMARY KEY,
    surface_id uuid REFERENCES channel_surfaces(id) ON DELETE SET NULL,
    event_type text NOT NULL,
    reason text NULL,
    previous_version bigint NULL,
    next_version bigint NULL,
    actor_type text NOT NULL,
    actor_id text NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX channel_surface_events_surface_created_at_idx
    ON channel_surface_events (surface_id, created_at ASC);

-- +goose Down
DROP TABLE IF EXISTS channel_surface_events;
DROP TABLE IF EXISTS channel_surface_subjects;
DROP TABLE IF EXISTS channel_surfaces;
DROP TABLE IF EXISTS diagnostics;
DROP TABLE IF EXISTS artifact_subjects;
DROP TABLE IF EXISTS analysis_run_step_inputs;
DROP TABLE IF EXISTS artifacts;
DROP TABLE IF EXISTS analysis_run_events;
DROP TABLE IF EXISTS analysis_run_steps;
DROP TABLE IF EXISTS analysis_runs;
DROP TABLE IF EXISTS selection_snapshot_items;
DROP TABLE IF EXISTS selection_snapshots;
DROP TABLE IF EXISTS collection_items;
DROP TABLE IF EXISTS collections;
DROP TABLE IF EXISTS media_assets;
DROP TABLE IF EXISTS stored_objects;
DROP TABLE IF EXISTS operation_requests;
DROP TABLE IF EXISTS channel_accounts;
DROP FUNCTION IF EXISTS prevent_selection_snapshots_mutation();
