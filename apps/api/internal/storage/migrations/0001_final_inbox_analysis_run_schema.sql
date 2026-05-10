-- +goose Up
CREATE TABLE sources (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
    origin_type text NOT NULL,
    external_uri text NULL,
    object_key text NULL,
    text_ref text NULL,
    checksum text NULL,
    size_bytes bigint NULL,
    mime_type text NULL,
    expires_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sources_origin_shape_chk CHECK (
        (origin_type = 'text' AND text_ref IS NOT NULL AND external_uri IS NULL AND object_key IS NULL) OR
        (origin_type = 'url' AND external_uri IS NOT NULL AND text_ref IS NULL AND object_key IS NULL) OR
        (origin_type = 'object' AND object_key IS NOT NULL AND text_ref IS NULL AND external_uri IS NULL)
    )
);

CREATE INDEX sources_owner_created_at_desc_idx ON sources (owner_type, owner_id, tenant_id, created_at DESC);
CREATE UNIQUE INDEX sources_object_key_unique_idx ON sources (object_key) WHERE object_key IS NOT NULL;

CREATE TABLE media_items (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
    source_id uuid NOT NULL REFERENCES sources(id),
    adapter_origin text NULL,
    kind text NOT NULL,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'ready',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    retention_state text NOT NULL DEFAULT 'active',
    retention_policy_id text NULL,
    expires_at timestamptz NULL,
    deleted_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT media_items_status_chk CHECK (status IN ('validating', 'ready', 'quarantined', 'deleted')),
    CONSTRAINT media_items_retention_state_chk CHECK (retention_state IN ('active', 'soft_deleted', 'expires_scheduled', 'expired', 'hard_delete_eligible', 'held'))
);

CREATE INDEX media_items_owner_status_created_at_desc_idx ON media_items (owner_type, owner_id, tenant_id, status, created_at DESC);
CREATE INDEX media_items_source_id_idx ON media_items (source_id);

CREATE TABLE collections (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
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

CREATE UNIQUE INDEX collections_active_inbox_owner_unique_idx
    ON collections (owner_type, owner_id, COALESCE(tenant_id, ''))
    WHERE kind = 'inbox' AND status = 'active';
CREATE INDEX collections_owner_updated_at_desc_idx ON collections (owner_type, owner_id, tenant_id, updated_at DESC);

CREATE TABLE collection_items (
    id uuid PRIMARY KEY,
    collection_id uuid NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    media_item_id uuid NOT NULL REFERENCES media_items(id),
    position int NOT NULL,
    added_by text NULL,
    added_at timestamptz NOT NULL DEFAULT now(),
    removed_at timestamptz NULL,
    CONSTRAINT collection_items_position_nonnegative_chk CHECK (position >= 0)
);

CREATE UNIQUE INDEX collection_items_active_collection_media_unique_idx
    ON collection_items (collection_id, media_item_id)
    WHERE removed_at IS NULL;
CREATE UNIQUE INDEX collection_items_active_collection_position_unique_idx
    ON collection_items (collection_id, position)
    WHERE removed_at IS NULL;
CREATE INDEX collection_items_collection_position_idx ON collection_items (collection_id, position ASC);

CREATE TABLE selections (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
    status text NOT NULL DEFAULT 'sealed',
    source_collection_id uuid NULL REFERENCES collections(id),
    option_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_by text NOT NULL,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    sealed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT selections_status_chk CHECK (status IN ('sealed', 'invalidated'))
);

CREATE INDEX selections_owner_created_at_desc_idx ON selections (owner_type, owner_id, tenant_id, created_at DESC);

CREATE TABLE selection_items (
    id uuid PRIMARY KEY,
    selection_id uuid NOT NULL REFERENCES selections(id) ON DELETE CASCADE,
    position int NOT NULL,
    media_item_id uuid NOT NULL REFERENCES media_items(id),
    kind text NOT NULL,
    source_snapshot jsonb NOT NULL,
    display_name text NOT NULL,
    status_at_selection text NOT NULL,
    metadata_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    retention_snapshot jsonb NOT NULL,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
    CONSTRAINT selection_items_selection_position_unique UNIQUE (selection_id, position)
);

CREATE TABLE analysis_runs (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
    selection_id uuid NOT NULL REFERENCES selections(id),
    run_type text NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    version bigint NOT NULL DEFAULT 1,
    idempotency_key text NULL,
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    delivery jsonb NOT NULL DEFAULT '{"strategy":"polling"}'::jsonb,
    evidence_gate_state text NOT NULL DEFAULT 'not_required',
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    canceled_at timestamptz NULL,
    expires_at timestamptz NULL,
    CONSTRAINT analysis_runs_status_chk CHECK (status IN ('queued', 'running', 'cancel_requested', 'partially_succeeded', 'succeeded', 'failed', 'canceled', 'expired')),
    CONSTRAINT analysis_runs_version_positive_chk CHECK (version >= 1)
);

CREATE INDEX analysis_runs_owner_created_at_desc_idx ON analysis_runs (owner_type, owner_id, tenant_id, created_at DESC);
CREATE UNIQUE INDEX analysis_runs_owner_idempotency_unique_idx
    ON analysis_runs (owner_type, owner_id, COALESCE(tenant_id, ''), idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE analysis_run_tasks (
    id uuid PRIMARY KEY,
    analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
    worker_kind text NOT NULL,
    task_type text NOT NULL,
    status text NOT NULL DEFAULT 'pending_enqueue',
    attempt_no int NOT NULL DEFAULT 1,
    lease_owner text NULL,
    claimed_at timestamptz NULL,
    heartbeat_at timestamptz NULL,
    finalized_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analysis_run_tasks_status_chk CHECK (status IN ('pending_enqueue', 'queued', 'claimed', 'succeeded', 'partially_succeeded', 'failed', 'canceled')),
    CONSTRAINT analysis_run_tasks_attempt_positive_chk CHECK (attempt_no >= 1)
);

CREATE INDEX analysis_run_tasks_run_status_idx ON analysis_run_tasks (analysis_run_id, status);

CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
    analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
    kind text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    object_key text NULL,
    content_type text NOT NULL,
    checksum text NULL,
    size_bytes bigint NOT NULL DEFAULT 0,
    visibility text NOT NULL DEFAULT 'owner',
    preview jsonb NOT NULL DEFAULT '{"available":false}'::jsonb,
    retention_state text NOT NULL DEFAULT 'active',
    retention_policy_id text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NULL,
    deleted_at timestamptz NULL,
    CONSTRAINT artifacts_status_chk CHECK (status IN ('pending', 'available', 'failed', 'expired', 'deleted')),
    CONSTRAINT artifacts_visibility_chk CHECK (visibility IN ('owner', 'private_execution'))
);

CREATE INDEX artifacts_run_created_at_idx ON artifacts (analysis_run_id, created_at ASC);
CREATE INDEX artifacts_owner_created_at_desc_idx ON artifacts (owner_type, owner_id, tenant_id, created_at DESC);

CREATE TABLE diagnostics (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    owner_id text NOT NULL,
    tenant_id text NULL,
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    severity text NOT NULL,
    code text NOT NULL,
    message text NOT NULL,
    context jsonb NOT NULL DEFAULT '{}'::jsonb,
    safe_adapter_context jsonb NOT NULL DEFAULT '{}'::jsonb,
    correlation_id text NULL,
    remediation_hint text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT diagnostics_severity_chk CHECK (severity IN ('info', 'warning', 'error'))
);

CREATE INDEX diagnostics_owner_subject_created_at_desc_idx ON diagnostics (owner_type, owner_id, tenant_id, subject_type, subject_id, created_at DESC);
CREATE INDEX diagnostics_owner_code_created_at_desc_idx ON diagnostics (owner_type, owner_id, tenant_id, code, created_at DESC);

CREATE TABLE analysis_run_events (
    id uuid PRIMARY KEY,
    analysis_run_id uuid NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    version bigint NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analysis_run_events_run_version_unique UNIQUE (analysis_run_id, version)
);

CREATE INDEX analysis_run_events_run_created_at_idx ON analysis_run_events (analysis_run_id, created_at ASC);

-- +goose Down
DROP TABLE IF EXISTS analysis_run_events;
DROP TABLE IF EXISTS diagnostics;
DROP TABLE IF EXISTS artifacts;
DROP TABLE IF EXISTS analysis_run_tasks;
DROP TABLE IF EXISTS analysis_runs;
DROP TABLE IF EXISTS selection_items;
DROP TABLE IF EXISTS selections;
DROP TABLE IF EXISTS collection_items;
DROP TABLE IF EXISTS collections;
DROP TABLE IF EXISTS media_items;
DROP TABLE IF EXISTS sources;
