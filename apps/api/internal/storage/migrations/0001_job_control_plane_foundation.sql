-- +goose Up
CREATE TABLE job_submissions (
    id uuid PRIMARY KEY,
    submission_kind text NOT NULL,
    idempotency_key text NOT NULL,
    request_fingerprint text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT job_submissions_submission_key_unique UNIQUE (submission_kind, idempotency_key)
);

CREATE INDEX job_submissions_created_at_desc_idx ON job_submissions (created_at DESC);

CREATE TABLE sources (
    id uuid PRIMARY KEY,
    source_kind text NOT NULL,
    display_name text NOT NULL,
    original_filename text NULL,
    mime_type text NULL,
    source_url text NULL,
    object_key text NULL,
    size_bytes bigint NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sources_storage_shape_chk CHECK (
        (source_kind = 'youtube_url' AND source_url IS NOT NULL AND object_key IS NULL) OR
        (source_kind IN ('uploaded_file', 'telegram_upload') AND object_key IS NOT NULL)
    )
);

CREATE UNIQUE INDEX sources_object_key_unique_idx ON sources (object_key) WHERE object_key IS NOT NULL;
CREATE INDEX sources_kind_created_at_desc_idx ON sources (source_kind, created_at DESC);

CREATE TABLE source_sets (
    id uuid PRIMARY KEY,
    input_kind text NOT NULL,
    display_name text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_sets_input_kind_chk CHECK (input_kind IN ('single_source', 'combined_upload', 'combined_telegram'))
);

CREATE TABLE source_set_items (
    id uuid PRIMARY KEY,
    source_set_id uuid NOT NULL REFERENCES source_sets(id) ON DELETE CASCADE,
    source_id uuid NOT NULL REFERENCES sources(id),
    position int NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_set_items_source_set_position_unique UNIQUE (source_set_id, position),
    CONSTRAINT source_set_items_source_set_source_unique UNIQUE (source_set_id, source_id)
);

CREATE INDEX source_set_items_source_set_idx ON source_set_items (source_set_id ASC);

CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    submission_id uuid NULL REFERENCES job_submissions(id),
    root_job_id uuid NOT NULL REFERENCES jobs(id),
    parent_job_id uuid NULL REFERENCES jobs(id),
    retry_of_job_id uuid NULL REFERENCES jobs(id),
    source_set_id uuid NOT NULL REFERENCES source_sets(id),
    job_type text NOT NULL,
    status text NOT NULL,
    delivery_strategy text NOT NULL DEFAULT 'polling',
    webhook_url text NULL,
    version bigint NOT NULL DEFAULT 1,
    progress_stage text NOT NULL DEFAULT 'queued',
    progress_message text NOT NULL DEFAULT '',
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    client_ref text NULL,
    error_code text NULL,
    error_message text NULL,
    cancel_requested_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz NULL,
    finished_at timestamptz NULL,
    CONSTRAINT jobs_delivery_strategy_chk CHECK (
        (delivery_strategy = 'webhook' AND webhook_url IS NOT NULL) OR
        (delivery_strategy = 'polling' AND webhook_url IS NULL)
    ),
    CONSTRAINT jobs_cancel_requested_chk CHECK (status <> 'cancel_requested' OR cancel_requested_at IS NOT NULL)
);

CREATE INDEX jobs_root_job_id_created_at_desc_idx ON jobs (root_job_id, created_at DESC);
CREATE INDEX jobs_parent_job_id_job_type_created_at_desc_idx ON jobs (parent_job_id, job_type, created_at DESC);
CREATE INDEX jobs_retry_of_job_id_idx ON jobs (retry_of_job_id);
CREATE INDEX jobs_source_set_id_created_at_desc_idx ON jobs (source_set_id, created_at DESC);
CREATE INDEX jobs_job_type_status_created_at_desc_idx ON jobs (job_type, status, created_at DESC);
CREATE UNIQUE INDEX jobs_canonical_child_unique_idx
    ON jobs (parent_job_id, job_type)
    WHERE parent_job_id IS NOT NULL
      AND retry_of_job_id IS NULL
      AND status IN ('queued', 'running', 'cancel_requested', 'succeeded');

CREATE TABLE job_executions (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_kind text NOT NULL,
    task_type text NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz NULL,
    outcome text NULL,
    CONSTRAINT job_executions_outcome_requires_finish_chk CHECK (
        (finished_at IS NULL AND outcome IS NULL) OR
        (finished_at IS NOT NULL AND outcome IN ('succeeded', 'failed', 'canceled'))
    )
);

CREATE UNIQUE INDEX job_executions_active_job_unique_idx
    ON job_executions (job_id)
    WHERE finished_at IS NULL;
CREATE INDEX job_executions_job_id_claimed_at_desc_idx ON job_executions (job_id, claimed_at DESC);

CREATE TABLE job_artifacts (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    artifact_kind text NOT NULL,
    filename text NOT NULL,
    format text NOT NULL,
    mime_type text NOT NULL,
    object_key text NOT NULL,
    size_bytes bigint NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT job_artifacts_job_kind_unique UNIQUE (job_id, artifact_kind),
    CONSTRAINT job_artifacts_object_key_unique UNIQUE (object_key)
);

CREATE INDEX job_artifacts_job_id_created_at_asc_idx ON job_artifacts (job_id, created_at ASC);

CREATE TABLE job_events (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    root_job_id uuid NOT NULL REFERENCES jobs(id),
    event_type text NOT NULL,
    version bigint NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT job_events_job_version_unique UNIQUE (job_id, version)
);

CREATE INDEX job_events_job_id_created_at_asc_idx ON job_events (job_id, created_at ASC);
CREATE INDEX job_events_root_job_id_created_at_asc_idx ON job_events (root_job_id, created_at ASC);

CREATE TABLE webhook_deliveries (
    id uuid PRIMARY KEY,
    job_event_id uuid NOT NULL UNIQUE REFERENCES job_events(id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    target_url text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    attempt_count int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_attempt_at timestamptz NULL,
    delivered_at timestamptz NULL,
    last_http_status int NULL,
    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT webhook_deliveries_state_chk CHECK (state IN ('pending', 'delivered', 'dead'))
);

CREATE INDEX webhook_deliveries_state_next_attempt_idx ON webhook_deliveries (state, next_attempt_at);
CREATE INDEX webhook_deliveries_job_id_created_at_idx ON webhook_deliveries (job_id, created_at);

-- +goose Down
DROP TABLE IF EXISTS webhook_deliveries;
DROP TABLE IF EXISTS job_events;
DROP TABLE IF EXISTS job_artifacts;
DROP TABLE IF EXISTS job_executions;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS source_set_items;
DROP TABLE IF EXISTS source_sets;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS job_submissions;
