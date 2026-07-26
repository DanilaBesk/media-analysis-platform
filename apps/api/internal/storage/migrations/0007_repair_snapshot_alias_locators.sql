-- +goose Up

WITH locator_targets AS (
    SELECT aliases.alias_id AS referenced_id, aliases.canonical_stored_object_id
    FROM stored_object_aliases AS aliases
    UNION
    SELECT aliases.canonical_stored_object_id AS referenced_id, aliases.canonical_stored_object_id
    FROM stored_object_aliases AS aliases
)
UPDATE selection_snapshot_items AS item
SET storage_snapshot=item.storage_snapshot || jsonb_build_object(
    'stored_object_id', targets.canonical_stored_object_id::text,
    'bucket', canonical.bucket,
    'object_key', canonical.object_key
)
FROM locator_targets AS targets
JOIN stored_objects AS canonical ON canonical.id=targets.canonical_stored_object_id
WHERE item.storage_snapshot->>'stored_object_id'=targets.referenced_id::text
  AND (
      item.storage_snapshot->>'stored_object_id' IS DISTINCT FROM targets.canonical_stored_object_id::text
      OR item.storage_snapshot->>'bucket' IS DISTINCT FROM canonical.bucket
      OR item.storage_snapshot->>'object_key' IS DISTINCT FROM canonical.object_key
  );

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    RAISE EXCEPTION '0007 is forward-only; do not restore deleted stored-object alias locators';
END $$;
-- +goose StatementEnd
