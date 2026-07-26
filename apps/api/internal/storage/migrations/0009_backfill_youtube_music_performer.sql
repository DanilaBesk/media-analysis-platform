-- +goose Up

WITH candidate_assets AS (
    SELECT
        asset.id,
        asset.channel_account_id,
        COALESCE(
            substring(asset.origin_ref FROM '(?i)[?&]v=([A-Za-z0-9_-]{11})(?:[&#]|$)'),
            substring(asset.origin_ref FROM '(?i)^https://youtu\.be/([A-Za-z0-9_-]{11})(?:[?&#/]|$)'),
            substring(asset.origin_ref FROM '(?i)^https://(?:www\.|m\.)?youtube\.com/shorts/([A-Za-z0-9_-]{11})(?:[?&#/]|$)')
        ) AS video_id,
        md5('youtube-metadata-enrichment:native-music:' || asset.id::text) AS identity_hash
    FROM media_assets AS asset
    WHERE asset.channel_account_id IS NOT NULL
      AND asset.origin_type = 'url'
      AND asset.status <> 'deleted'
      AND NULLIF(btrim(COALESCE(asset.metadata #>> '{provider_metadata,performer}', '')), '') IS NULL
      AND (
          asset.origin_ref ~* '^https://((www|m)\.)?youtube\.com/'
          OR asset.origin_ref ~* '^https://youtu\.be/'
      )
      AND NOT EXISTS (
          SELECT 1
          FROM metadata_enrichment_jobs AS existing
          WHERE existing.channel_account_id = asset.channel_account_id
            AND existing.idempotency_key = 'youtube-metadata-enrichment:native-music:' || asset.id::text
      )
      AND NOT EXISTS (
          SELECT 1
          FROM metadata_enrichment_jobs AS active
          WHERE active.media_asset_id = asset.id
            AND active.status IN ('queued', 'claimed', 'running', 'retry_wait')
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
    'youtube-metadata-enrichment:native-music:' || id::text,
    0,
    3,
    now()
FROM youtube_assets
ON CONFLICT DO NOTHING;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0009 is forward-only; preserve YouTube performer enrichment history';
END $$;
-- +goose StatementEnd
