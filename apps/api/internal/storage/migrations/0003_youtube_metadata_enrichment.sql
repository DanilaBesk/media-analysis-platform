-- +goose Up

CREATE TABLE metadata_enrichment_jobs (
    id uuid PRIMARY KEY,
    media_asset_id uuid NOT NULL REFERENCES media_assets(id),
    channel_account_id uuid NOT NULL REFERENCES channel_accounts(id),
    provider text NOT NULL,
    canonical_url text NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    version bigint NOT NULL DEFAULT 1,
    idempotency_key text NOT NULL,
    attempt_no integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3,
    attempt_token text NULL,
    lease_owner text NULL,
    lease_expires_at timestamptz NULL,
    heartbeat_at timestamptz NULL,
    next_attempt_at timestamptz NULL,
    progress jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_code text NULL,
    error_message text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    CONSTRAINT metadata_enrichment_provider_chk CHECK (provider IN ('youtube')),
    CONSTRAINT metadata_enrichment_status_chk CHECK (status IN ('queued','claimed','running','retry_wait','succeeded','failed')),
    CONSTRAINT metadata_enrichment_version_chk CHECK (version > 0),
    CONSTRAINT metadata_enrichment_attempt_chk CHECK (attempt_no >= 0 AND max_attempts > 0 AND attempt_no <= max_attempts),
    CONSTRAINT metadata_enrichment_active_fence_chk CHECK (
        status NOT IN ('claimed','running')
        OR (attempt_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX metadata_enrichment_channel_idempotency_idx
    ON metadata_enrichment_jobs (channel_account_id, idempotency_key);
CREATE UNIQUE INDEX metadata_enrichment_asset_active_idx
    ON metadata_enrichment_jobs (media_asset_id)
    WHERE status IN ('queued','claimed','running','retry_wait');
CREATE INDEX metadata_enrichment_queue_idx
    ON metadata_enrichment_jobs (COALESCE(next_attempt_at, created_at), created_at)
    WHERE status IN ('queued','retry_wait');
CREATE INDEX metadata_enrichment_lease_idx
    ON metadata_enrichment_jobs (lease_expires_at)
    WHERE status IN ('claimed','running');

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0003 is forward-only; roll back the application while preserving metadata jobs';
END $$;
-- +goose StatementEnd
