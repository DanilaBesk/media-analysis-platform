-- +goose Up
ALTER TABLE source_sets DROP CONSTRAINT IF EXISTS source_sets_input_kind_chk;
ALTER TABLE source_sets
    ADD CONSTRAINT source_sets_input_kind_chk
    CHECK (input_kind IN ('single_source', 'combined_upload', 'combined_telegram', 'agent_run'));

-- +goose Down
ALTER TABLE source_sets DROP CONSTRAINT IF EXISTS source_sets_input_kind_chk;
ALTER TABLE source_sets
    ADD CONSTRAINT source_sets_input_kind_chk
    CHECK (input_kind IN ('single_source', 'combined_upload', 'combined_telegram'));
