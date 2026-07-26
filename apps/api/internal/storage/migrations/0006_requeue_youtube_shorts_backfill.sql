-- +goose Up

WITH repairable AS (
    SELECT
        job.id,
        substring(asset.origin_ref FROM '(?i)^https://(?:www\.|m\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})(?:[?&#/]|$)') AS video_id
    FROM metadata_enrichment_jobs AS job
    JOIN media_assets AS asset ON asset.id = job.media_asset_id
    WHERE job.idempotency_key LIKE 'youtube-metadata-enrichment:backfill:%'
      AND job.status = 'failed'
      AND job.error_code = 'provider_url_invalid'
)
UPDATE metadata_enrichment_jobs AS job
SET
    canonical_url = 'https://www.youtube.com/watch?v=' || repairable.video_id,
    status = 'queued',
    version = version + 1,
    error_code = NULL,
    error_message = NULL,
    completed_at = NULL,
    next_attempt_at = NULL
FROM repairable
WHERE job.id = repairable.id
  AND repairable.video_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM metadata_enrichment_jobs AS active_job
      WHERE active_job.media_asset_id = job.media_asset_id
        AND active_job.id <> job.id
        AND active_job.status IN ('queued', 'claimed', 'running', 'retry_wait')
  );

WITH missing_shorts AS (
    SELECT
        asset.id,
        asset.channel_account_id,
        substring(asset.origin_ref FROM '(?i)^https://(?:www\.|m\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})(?:[?&#/]|$)') AS video_id,
        md5('youtube-metadata-enrichment:' || asset.id::text) AS identity_hash
    FROM media_assets AS asset
    WHERE asset.channel_account_id IS NOT NULL
      AND asset.origin_type = 'url'
      AND asset.status <> 'deleted'
      AND NOT (COALESCE(asset.metadata, '{}'::jsonb) ? 'provider_metadata')
      AND asset.origin_ref ~* '^https://(?:www\.|m\.)?youtube\.com/shorts/'
      AND NOT EXISTS (
          SELECT 1
          FROM metadata_enrichment_jobs AS historical_job
          WHERE historical_job.media_asset_id = asset.id
            AND historical_job.idempotency_key = 'youtube-metadata-enrichment:backfill:' || asset.id::text
      )
      AND NOT EXISTS (
          SELECT 1
          FROM metadata_enrichment_jobs AS active_job
          WHERE active_job.media_asset_id = asset.id
            AND active_job.status IN ('queued', 'claimed', 'running', 'retry_wait')
      )
), eligible_shorts AS (
    SELECT *
    FROM missing_shorts
    WHERE video_id IS NOT NULL
)
INSERT INTO metadata_enrichment_jobs (
    id,
    media_asset_id,
    channel_account_id,
    provider,
    canonical_url,
    status,
    version,
    idempotency_key,
    attempt_no,
    max_attempts,
    created_at
)
SELECT
    (
        substr(identity_hash, 1, 8) || '-' ||
        substr(identity_hash, 9, 4) || '-4' ||
        substr(identity_hash, 14, 3) || '-8' ||
        substr(identity_hash, 18, 3) || '-' ||
        substr(identity_hash, 21, 12)
    )::uuid,
    id,
    channel_account_id,
    'youtube',
    'https://www.youtube.com/watch?v=' || video_id,
    'queued',
    1,
    'youtube-metadata-enrichment:backfill:' || id::text,
    0,
    3,
    now()
FROM eligible_shorts
ON CONFLICT DO NOTHING;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0006 is forward-only; preserve repaired YouTube Shorts backfill history';
END $$;
-- +goose StatementEnd
