-- +goose Up

ALTER TABLE export_jobs
    ADD COLUMN output_profile text NULL,
    ADD COLUMN presentation_title text NULL,
    ADD COLUMN presentation_performer text NULL,
    ADD COLUMN presentation_duration_seconds integer NULL,
    ADD COLUMN presentation_frozen_at timestamptz NULL;

WITH classified_jobs AS (
    SELECT job.id, job.operation, job.status, job.variant,
           COALESCE(output.content_type, '') AS output_content_type,
           COALESCE(output.object_key, '') AS output_object_key
    FROM export_jobs job
    LEFT JOIN stored_objects output ON output.id = job.output_stored_object_id
)
UPDATE export_jobs job
SET output_profile = CASE
    WHEN classified.operation = 'youtube_video' THEN 'video_mp4_v1'
    WHEN classified.operation IN ('youtube_audio', 'video_to_audio')
         AND classified.status IN ('succeeded', 'expired')
         AND (
             lower(btrim(split_part(classified.output_content_type, ';', 1))) IN ('audio/mp4', 'audio/x-m4a', 'video/mp4')
             OR lower(classified.output_object_key) ~ '\.(m4a|mp4)$'
         ) THEN 'audio_m4a_aac_legacy'
    WHEN classified.operation IN ('youtube_audio', 'video_to_audio')
         AND classified.status IN ('succeeded', 'expired')
         AND (
             lower(btrim(split_part(classified.output_content_type, ';', 1))) IN ('audio/ogg', 'audio/opus', 'application/ogg')
             OR lower(classified.output_object_key) ~ '\.(ogg|opus)$'
         ) THEN 'audio_ogg_opus_v1'
    WHEN classified.operation IN ('youtube_audio', 'video_to_audio')
         AND COALESCE(classified.variant->>'audio_bitrate_kbps', '') = '320'
        THEN 'audio_m4a_aac_legacy'
    WHEN classified.operation IN ('youtube_audio', 'video_to_audio') THEN 'audio_ogg_opus_v1'
    ELSE 'video_mp4_v1'
END
FROM classified_jobs classified
WHERE classified.id = job.id;

ALTER TABLE export_jobs
    ALTER COLUMN output_profile SET NOT NULL,
    ADD CONSTRAINT export_jobs_output_profile_chk CHECK (
        output_profile IN (
            'audio_ogg_opus_v1',
            'audio_m4a_aac_legacy',
            'audio_m4a_aac_v1',
            'video_mp4_v1'
        )
    ),
    ADD CONSTRAINT export_jobs_presentation_title_chk CHECK (
        presentation_title IS NULL OR char_length(presentation_title) BETWEEN 1 AND 64
    ),
    ADD CONSTRAINT export_jobs_presentation_performer_chk CHECK (
        presentation_performer IS NULL OR char_length(presentation_performer) BETWEEN 1 AND 64
    ),
    ADD CONSTRAINT export_jobs_presentation_duration_chk CHECK (
        presentation_duration_seconds IS NULL
        OR presentation_duration_seconds BETWEEN 1 AND 2678400
    ),
    ADD CONSTRAINT export_jobs_current_music_snapshot_chk CHECK (
        output_profile <> 'audio_m4a_aac_v1'
        OR (presentation_title IS NOT NULL AND presentation_performer IS NOT NULL)
    ),
    ADD CONSTRAINT export_jobs_current_music_finalization_chk CHECK (
        output_profile <> 'audio_m4a_aac_v1'
        OR status NOT IN ('succeeded', 'expired')
        OR (presentation_duration_seconds IS NOT NULL AND presentation_frozen_at IS NOT NULL)
    );

-- +goose Down

-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0008 is forward-only; do not discard immutable export profile or presentation snapshots';
END $$;
-- +goose StatementEnd
