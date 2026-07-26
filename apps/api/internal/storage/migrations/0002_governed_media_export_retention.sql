-- +goose Up

ALTER TABLE stored_objects
    ADD COLUMN channel_account_id uuid REFERENCES channel_accounts(id),
    ADD COLUMN staging_key text NULL,
    ADD COLUMN generation integer NOT NULL DEFAULT 1,
    ADD COLUMN generation_published_at timestamptz NULL,
    ADD COLUMN checksum_algorithm text NOT NULL DEFAULT 'sha256',
    ADD COLUMN hold_state text NOT NULL DEFAULT 'none',
    ADD COLUMN last_successful_use_at timestamptz NULL,
    ADD COLUMN delete_owner text NULL,
    ADD COLUMN delete_token text NULL,
    ADD COLUMN delete_lease_expires_at timestamptz NULL,
    ADD COLUMN delete_attempts integer NOT NULL DEFAULT 0;

ALTER TABLE stored_objects
    DROP CONSTRAINT stored_objects_storage_status_chk,
    DROP CONSTRAINT stored_objects_retention_state_chk;

DO $$
BEGIN
    IF EXISTS (
        SELECT stored_object_id
        FROM (
            SELECT stored_object_id, channel_account_id FROM media_assets WHERE stored_object_id IS NOT NULL
            UNION ALL
            SELECT stored_object_id, channel_account_id FROM artifacts WHERE stored_object_id IS NOT NULL
        ) refs
        WHERE channel_account_id IS NOT NULL
        GROUP BY stored_object_id
        HAVING count(DISTINCT channel_account_id) > 1
    ) THEN
        RAISE EXCEPTION 'stored object is referenced by more than one channel account';
    END IF;
END $$;

UPDATE stored_objects so
SET channel_account_id=scope.channel_account_id
FROM (
    SELECT stored_object_id, min(channel_account_id::text)::uuid AS channel_account_id
    FROM (
        SELECT stored_object_id, channel_account_id FROM media_assets WHERE stored_object_id IS NOT NULL
        UNION ALL
        SELECT stored_object_id, channel_account_id FROM artifacts WHERE stored_object_id IS NOT NULL
    ) refs
    WHERE channel_account_id IS NOT NULL
    GROUP BY stored_object_id
) scope
WHERE so.id=scope.stored_object_id;

UPDATE stored_objects
SET generation_published_at=created_at,
    storage_status=CASE WHEN storage_status='pending' THEN 'publishing' ELSE storage_status END,
    hold_state=CASE WHEN retention_state='held' THEN 'held' ELSE hold_state END;

CREATE TABLE stored_object_aliases (
    alias_id uuid PRIMARY KEY,
    canonical_stored_object_id uuid NOT NULL REFERENCES stored_objects(id),
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT stored_object_aliases_reason_chk CHECK (reason IN ('legacy_channel_digest_duplicate'))
);

WITH ranked AS (
    SELECT id,
           first_value(id) OVER (
               PARTITION BY channel_account_id, checksum, size_bytes
               ORDER BY CASE storage_status WHEN 'available' THEN 0 WHEN 'publishing' THEN 1 ELSE 2 END,
                        created_at ASC,
                        id ASC
           ) AS canonical_id
    FROM stored_objects
    WHERE channel_account_id IS NOT NULL AND checksum IS NOT NULL AND checksum <> ''
), duplicates AS (
    SELECT id AS alias_id, canonical_id
    FROM ranked
    WHERE id <> canonical_id
)
INSERT INTO stored_object_aliases (alias_id, canonical_stored_object_id, reason)
SELECT alias_id, canonical_id, 'legacy_channel_digest_duplicate'
FROM duplicates;

UPDATE media_assets ma
SET stored_object_id=aliases.canonical_stored_object_id
FROM stored_object_aliases aliases
WHERE ma.stored_object_id=aliases.alias_id;

UPDATE artifacts artifact
SET stored_object_id=aliases.canonical_stored_object_id
FROM stored_object_aliases aliases
WHERE artifact.stored_object_id=aliases.alias_id;

UPDATE selection_snapshot_items item
SET storage_snapshot=item.storage_snapshot || jsonb_build_object(
    'stored_object_id', aliases.canonical_stored_object_id::text,
    'bucket', canonical.bucket,
    'object_key', canonical.object_key
)
FROM stored_object_aliases aliases
JOIN stored_objects canonical ON canonical.id=aliases.canonical_stored_object_id
WHERE item.storage_snapshot->>'stored_object_id'=aliases.alias_id::text;

WITH grouped AS (
    SELECT aliases.canonical_stored_object_id,
           bool_or(member.hold_state='held') AS any_held,
           max(member.generation) AS max_generation,
           max(member.generation_published_at) AS latest_generation_published_at,
           max(member.last_successful_use_at) AS latest_successful_use_at,
           bool_or(member.expires_at IS NULL) AS any_expiry_unscheduled,
           max(member.expires_at) AS latest_expires_at
    FROM stored_object_aliases aliases
    JOIN stored_objects member
      ON member.id=aliases.alias_id OR member.id=aliases.canonical_stored_object_id
    GROUP BY aliases.canonical_stored_object_id
)
UPDATE stored_objects canonical
SET hold_state=CASE WHEN grouped.any_held THEN 'held' ELSE canonical.hold_state END,
    retention_state=CASE WHEN grouped.any_held THEN 'held' ELSE canonical.retention_state END,
    generation=GREATEST(canonical.generation, grouped.max_generation),
    generation_published_at=GREATEST(canonical.generation_published_at, grouped.latest_generation_published_at),
    last_successful_use_at=CASE
        WHEN grouped.latest_successful_use_at IS NULL THEN canonical.last_successful_use_at
        ELSE GREATEST(COALESCE(canonical.last_successful_use_at, grouped.latest_successful_use_at), grouped.latest_successful_use_at)
    END,
    expires_at=CASE
        WHEN grouped.any_expiry_unscheduled THEN NULL
        WHEN grouped.latest_expires_at IS NULL THEN canonical.expires_at
        ELSE GREATEST(COALESCE(canonical.expires_at, grouped.latest_expires_at), grouped.latest_expires_at)
    END
FROM grouped
WHERE canonical.id=grouped.canonical_stored_object_id;

DELETE FROM stored_objects so
USING stored_object_aliases aliases
WHERE so.id=aliases.alias_id;

ALTER TABLE stored_objects
    ALTER COLUMN generation_published_at SET NOT NULL,
    ADD CONSTRAINT stored_objects_storage_status_chk CHECK (storage_status IN ('publishing', 'available', 'delete_scheduled', 'missing', 'deleted')),
    ADD CONSTRAINT stored_objects_retention_state_chk CHECK (retention_state IN ('active', 'soft_deleted', 'expires_scheduled', 'expired', 'hard_delete_eligible', 'held')),
    ADD CONSTRAINT stored_objects_generation_chk CHECK (generation > 0),
    ADD CONSTRAINT stored_objects_delete_attempts_chk CHECK (delete_attempts >= 0),
    ADD CONSTRAINT stored_objects_checksum_algorithm_chk CHECK (checksum_algorithm IN ('sha256')),
    ADD CONSTRAINT stored_objects_hold_state_chk CHECK (hold_state IN ('none', 'held'));

UPDATE stored_objects
SET storage_status='delete_scheduled',
    retention_state='expires_scheduled',
    expires_at=COALESCE(expires_at, now())
WHERE channel_account_id IS NULL AND storage_status IN ('publishing','available');

ALTER TABLE stored_objects
    ADD CONSTRAINT stored_objects_channel_scope_chk CHECK (
        channel_account_id IS NOT NULL OR storage_status IN ('delete_scheduled', 'missing', 'deleted')
    ),
    ADD CONSTRAINT stored_objects_delete_fence_chk CHECK (
        (storage_status='delete_scheduled' AND delete_owner IS NOT NULL AND delete_token IS NOT NULL AND delete_lease_expires_at IS NOT NULL)
        OR storage_status <> 'delete_scheduled'
        OR (delete_owner IS NULL AND delete_token IS NULL AND delete_lease_expires_at IS NULL)
    );

CREATE UNIQUE INDEX stored_objects_channel_digest_unique_idx
    ON stored_objects (channel_account_id, checksum, size_bytes)
    WHERE channel_account_id IS NOT NULL AND checksum IS NOT NULL AND checksum <> '';
CREATE INDEX stored_objects_delete_claim_idx
    ON stored_objects (storage_status, expires_at, delete_lease_expires_at)
    WHERE deleted_at IS NULL;

CREATE TABLE object_delete_fences (
    bucket text NOT NULL,
    object_key text NOT NULL,
    token text NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, object_key)
);

CREATE TABLE storage_reconcile_cursors (
    name text PRIMARY KEY,
    cursor text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stored_object_pins (
    id uuid PRIMARY KEY,
    stored_object_id uuid NOT NULL REFERENCES stored_objects(id) ON DELETE CASCADE,
    owner_type text NOT NULL,
    owner_id uuid NOT NULL,
    purpose text NOT NULL,
    expires_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    released_at timestamptz NULL,
    CONSTRAINT stored_object_pins_owner_type_chk CHECK (owner_type IN ('analysis_run', 'export_job', 'export_delivery')),
    CONSTRAINT stored_object_pins_purpose_chk CHECK (purpose IN ('source', 'delivery')),
    CONSTRAINT stored_object_pins_owner_purpose_chk CHECK (
        (owner_type IN ('analysis_run','export_job') AND purpose='source' AND expires_at IS NULL)
        OR (owner_type='export_delivery' AND purpose='delivery' AND expires_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX stored_object_pins_active_owner_idx
    ON stored_object_pins (stored_object_id, owner_type, owner_id, purpose)
    WHERE released_at IS NULL;
CREATE INDEX stored_object_pins_active_object_idx
    ON stored_object_pins (stored_object_id, expires_at)
    WHERE released_at IS NULL;

CREATE TABLE export_jobs (
    id uuid PRIMARY KEY,
    channel_account_id uuid NOT NULL REFERENCES channel_accounts(id),
    media_asset_id uuid NOT NULL REFERENCES media_assets(id),
    operation text NOT NULL,
    delivery_channel text NOT NULL DEFAULT 'telegram',
    variant jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'queued',
    version bigint NOT NULL DEFAULT 1,
    idempotency_key text NULL,
    retry_generation integer NOT NULL DEFAULT 0,
    attempt_no integer NOT NULL DEFAULT 0,
    attempt_token text NULL,
    lease_owner text NULL,
    lease_expires_at timestamptz NULL,
    heartbeat_at timestamptz NULL,
    max_attempts integer NOT NULL DEFAULT 3,
    progress jsonb NOT NULL DEFAULT '{}'::jsonb,
    output_stored_object_id uuid REFERENCES stored_objects(id),
    diagnostic_id uuid REFERENCES diagnostics(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    cancel_requested_at timestamptz NULL,
    canceled_at timestamptz NULL,
    expires_at timestamptz NULL,
    CONSTRAINT export_jobs_operation_chk CHECK (operation IN ('youtube_audio', 'youtube_video', 'video_to_audio')),
    CONSTRAINT export_jobs_delivery_channel_chk CHECK (delivery_channel IN ('telegram', 'web')),
    CONSTRAINT export_jobs_status_chk CHECK (status IN ('queued', 'claimed', 'running', 'cancel_requested', 'succeeded', 'failed', 'canceled', 'expired')),
    CONSTRAINT export_jobs_version_chk CHECK (version > 0),
    CONSTRAINT export_jobs_attempt_chk CHECK (attempt_no >= 0 AND max_attempts > 0 AND attempt_no <= max_attempts),
    CONSTRAINT export_jobs_active_fence_chk CHECK (
        status NOT IN ('claimed','running','cancel_requested')
        OR (attempt_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX export_jobs_channel_idempotency_idx
    ON export_jobs (channel_account_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX export_jobs_queue_idx
    ON export_jobs (status, created_at)
    WHERE status IN ('queued', 'claimed', 'running', 'cancel_requested');

CREATE TABLE export_deliveries (
    id uuid PRIMARY KEY,
    export_job_id uuid NOT NULL REFERENCES export_jobs(id) ON DELETE CASCADE,
    channel_account_id uuid NOT NULL REFERENCES channel_accounts(id),
    channel text NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    version bigint NOT NULL DEFAULT 1,
    attempt_no integer NOT NULL DEFAULT 0,
    attempt_token text NULL,
    lease_owner text NULL,
    lease_expires_at timestamptz NULL,
	    next_attempt_at timestamptz NULL,
    max_attempts integer NOT NULL DEFAULT 5,
    expires_at timestamptz NOT NULL,
    delivered_at timestamptz NULL,
    failure_code text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT export_deliveries_channel_chk CHECK (channel IN ('telegram', 'web')),
    CONSTRAINT export_deliveries_status_chk CHECK (status IN ('pending', 'claimed', 'delivered', 'failed', 'expired')),
    CONSTRAINT export_deliveries_attempt_chk CHECK (attempt_no >= 0 AND max_attempts > 0 AND attempt_no <= max_attempts),
    CONSTRAINT export_deliveries_claim_fence_chk CHECK (
        status <> 'claimed'
        OR (attempt_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX export_deliveries_job_channel_idx
    ON export_deliveries (export_job_id, channel);
CREATE INDEX export_deliveries_claim_idx
    ON export_deliveries (status, next_attempt_at, lease_expires_at, expires_at);

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0002 is forward-only; roll back the application while preserving governed media data';
END $$;
-- +goose StatementEnd
