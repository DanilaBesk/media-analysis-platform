-- +goose Up

WITH candidate_assets AS (
    SELECT
        id,
        channel_account_id,
        origin_ref,
        COALESCE(
            substring(origin_ref FROM '(?i)[?&]v=([A-Za-z0-9_-]{11})(?:[&#]|$)'),
            substring(origin_ref FROM '(?i)^https://youtu\.be/([A-Za-z0-9_-]{11})(?:[?&#/]|$)'),
            substring(origin_ref FROM '(?i)^https://(?:www\.|m\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})(?:[?&#/]|$)')
        ) AS video_id,
        md5('youtube-metadata-enrichment:' || id::text) AS identity_hash
    FROM media_assets
    WHERE channel_account_id IS NOT NULL
      AND origin_type = 'url'
      AND status <> 'deleted'
      AND NOT (COALESCE(metadata, '{}'::jsonb) ? 'provider_metadata')
      AND (
          origin_ref ~* '^https://((www|m)\.)?youtube\.com/'
          OR origin_ref ~* '^https://youtu\.be/'
      )
), youtube_assets AS (
    SELECT *
    FROM candidate_assets
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
FROM youtube_assets
ON CONFLICT DO NOTHING;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0004 is forward-only; preserve completed and pending backfill history';
END $$;
-- +goose StatementEnd
