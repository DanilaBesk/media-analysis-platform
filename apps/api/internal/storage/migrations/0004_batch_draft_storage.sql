-- +goose Up
CREATE TABLE batch_drafts (
    id uuid PRIMARY KEY,
    owner_type text NOT NULL,
    telegram_chat_id text NOT NULL,
    telegram_user_id text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'open',
    display_name text NULL,
    client_ref text NULL,
    expires_at timestamptz NULL,
    submitted_root_job_id uuid NULL REFERENCES jobs(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT batch_drafts_owner_scope_chk CHECK (
        owner_type = 'telegram'
        AND telegram_chat_id <> ''
        AND telegram_user_id <> ''
    ),
    CONSTRAINT batch_drafts_status_chk CHECK (status IN ('open', 'submitted', 'expired', 'canceled')),
    CONSTRAINT batch_drafts_version_positive_chk CHECK (version >= 1),
    CONSTRAINT batch_drafts_submitted_root_chk CHECK (
        (status = 'submitted' AND submitted_root_job_id IS NOT NULL) OR
        (status <> 'submitted' AND submitted_root_job_id IS NULL)
    )
);

CREATE INDEX batch_drafts_owner_status_updated_at_idx
    ON batch_drafts (owner_type, telegram_chat_id, telegram_user_id, status, updated_at DESC);
CREATE INDEX batch_drafts_expires_at_idx
    ON batch_drafts (expires_at)
    WHERE status = 'open' AND expires_at IS NOT NULL;

CREATE TABLE batch_draft_items (
    id uuid PRIMARY KEY,
    draft_id uuid NOT NULL REFERENCES batch_drafts(id) ON DELETE CASCADE,
    source_id uuid NOT NULL,
    position int NOT NULL,
    source_label text NOT NULL,
    source_kind text NOT NULL,
    uploaded_source_ref text NULL,
    url text NULL,
    display_name text NULL,
    original_filename text NULL,
    content_type text NULL,
    size_bytes bigint NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT batch_draft_items_position_unique UNIQUE (draft_id, position),
    CONSTRAINT batch_draft_items_source_label_unique UNIQUE (draft_id, source_label),
    CONSTRAINT batch_draft_items_source_id_unique UNIQUE (draft_id, source_id),
    CONSTRAINT batch_draft_items_source_label_shape_chk CHECK (source_label ~ '^[a-z][a-z0-9_-]{0,63}$'),
    CONSTRAINT batch_draft_items_source_shape_chk CHECK (
        (source_kind IN ('uploaded_file', 'telegram_upload') AND uploaded_source_ref IS NOT NULL AND url IS NULL) OR
        (source_kind IN ('youtube_url', 'external_url') AND url IS NOT NULL AND uploaded_source_ref IS NULL)
    )
);

CREATE INDEX batch_draft_items_draft_position_idx
    ON batch_draft_items (draft_id, position ASC);

-- +goose Down
DROP TABLE IF EXISTS batch_draft_items;
DROP TABLE IF EXISTS batch_drafts;
