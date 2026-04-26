-- +goose Up
ALTER TABLE source_sets DROP CONSTRAINT IF EXISTS source_sets_input_kind_chk;
ALTER TABLE source_sets
    ADD CONSTRAINT source_sets_input_kind_chk
    CHECK (input_kind IN ('single_source', 'combined_upload', 'combined_telegram', 'batch_transcription', 'agent_run'));

ALTER TABLE source_set_items
    ADD COLUMN source_label text NULL,
    ADD COLUMN source_label_version text NULL,
    ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN lineage jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE source_set_items
    ADD CONSTRAINT source_set_items_source_label_shape_chk
    CHECK (source_label IS NULL OR source_label ~ '^[a-z][a-z0-9_-]{0,63}$');

ALTER TABLE source_set_items
    ADD CONSTRAINT source_set_items_source_label_version_pair_chk
    CHECK (
        (source_label IS NULL AND source_label_version IS NULL) OR
        (source_label IS NOT NULL AND source_label_version IS NOT NULL)
    );

CREATE UNIQUE INDEX source_set_items_source_set_source_label_unique_idx
    ON source_set_items (source_set_id, source_label)
    WHERE source_label IS NOT NULL;

DROP INDEX IF EXISTS jobs_canonical_child_unique_idx;
CREATE UNIQUE INDEX jobs_canonical_child_unique_idx
    ON jobs (parent_job_id, job_type)
    WHERE parent_job_id IS NOT NULL
      AND retry_of_job_id IS NULL
      AND job_type IN ('agent_run', 'report', 'deep_research')
      AND status IN ('queued', 'running', 'cancel_requested', 'succeeded');

-- +goose Down
DROP INDEX IF EXISTS jobs_canonical_child_unique_idx;
CREATE UNIQUE INDEX jobs_canonical_child_unique_idx
    ON jobs (parent_job_id, job_type)
    WHERE parent_job_id IS NOT NULL
      AND retry_of_job_id IS NULL
      AND status IN ('queued', 'running', 'cancel_requested', 'succeeded');

DROP INDEX IF EXISTS source_set_items_source_set_source_label_unique_idx;

ALTER TABLE source_set_items
    DROP CONSTRAINT IF EXISTS source_set_items_source_label_version_pair_chk,
    DROP CONSTRAINT IF EXISTS source_set_items_source_label_shape_chk;

ALTER TABLE source_set_items
    DROP COLUMN IF EXISTS lineage,
    DROP COLUMN IF EXISTS metadata,
    DROP COLUMN IF EXISTS source_label_version,
    DROP COLUMN IF EXISTS source_label;

ALTER TABLE source_sets DROP CONSTRAINT IF EXISTS source_sets_input_kind_chk;
ALTER TABLE source_sets
    ADD CONSTRAINT source_sets_input_kind_chk
    CHECK (input_kind IN ('single_source', 'combined_upload', 'combined_telegram', 'agent_run'));
