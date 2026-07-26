-- +goose Up

WITH repairable AS (
    SELECT
        job.id,
        COALESCE(
            substring(asset.origin_ref FROM '(?i)[?&]v=([A-Za-z0-9_-]{11})(?:[&#]|$)'),
            substring(asset.origin_ref FROM '(?i)^https://youtu\.be/([A-Za-z0-9_-]{11})(?:[?&#/]|$)'),
            substring(asset.origin_ref FROM '(?i)^https://(?:www\.|m\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})(?:[?&#/]|$)')
        ) AS video_id
    FROM metadata_enrichment_jobs AS job
    JOIN media_assets AS asset ON asset.id = job.media_asset_id
    WHERE job.status IN ('queued', 'retry_wait')
      AND job.idempotency_key LIKE 'youtube-metadata-enrichment:backfill:%'
)
UPDATE metadata_enrichment_jobs AS job
SET canonical_url = 'https://www.youtube.com/watch?v=' || repairable.video_id
FROM repairable
WHERE job.id = repairable.id
  AND repairable.video_id IS NOT NULL;

UPDATE metadata_enrichment_jobs
SET
    status = 'failed',
    version = version + 1,
    error_code = 'provider_url_invalid',
    error_message = 'Stored YouTube reference does not identify one video',
    completed_at = now()
WHERE status IN ('queued', 'retry_wait')
  AND idempotency_key LIKE 'youtube-metadata-enrichment:backfill:%'
  AND canonical_url !~ '^https://www\.youtube\.com/watch\?v=[A-Za-z0-9_-]{11}$';

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0005 is forward-only; do not restore noncanonical provider URLs';
END $$;
-- +goose StatementEnd
