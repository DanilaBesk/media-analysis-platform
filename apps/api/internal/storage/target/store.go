package target

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("target storage: db is required")
	}
	return &Store{db: db}, nil
}

func (s *Store) UpsertChannelAccount(ctx context.Context, record ChannelAccountRecord) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO channel_accounts (
    id, channel, external_account_ref, display_name, status, metadata,
    created_at, updated_at, last_seen_at, disabled_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (channel, external_account_ref) DO UPDATE
SET display_name=EXCLUDED.display_name,
    status=EXCLUDED.status,
    metadata=EXCLUDED.metadata,
    updated_at=EXCLUDED.updated_at,
    last_seen_at=EXCLUDED.last_seen_at,
    disabled_at=EXCLUDED.disabled_at`,
		record.ID, record.Channel, record.ExternalAccountRef, nullString(record.DisplayName),
		withDefault(record.Status, "active"), jsonOrDefault(record.MetadataJSON, "{}"),
		record.CreatedAt, record.UpdatedAt, record.LastSeenAt, record.DisabledAt)
	return err
}

func (s *Store) RecordOperationRequest(ctx context.Context, record OperationRequestRecord) (OperationRequestRecord, error) {
	result, err := s.db.ExecContext(ctx, `
INSERT INTO operation_requests (
    id, channel_account_id, operation_type, idempotency_key, request_hash, status,
    target_type, target_id, error_code, metadata, created_at, completed_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (channel_account_id, operation_type, idempotency_key) DO NOTHING`,
		record.ID, record.ChannelAccountID, record.OperationType, record.IdempotencyKey,
		nullString(record.RequestHash), withDefault(record.Status, "accepted"),
		nullString(record.TargetType), nullString(record.TargetID), nullString(record.ErrorCode),
		jsonOrDefault(record.MetadataJSON, "{}"), record.CreatedAt, record.CompletedAt)
	if err != nil {
		return OperationRequestRecord{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return OperationRequestRecord{}, err
	}
	if affected == 0 {
		return s.getOperationRequestByIdempotency(ctx, record.ChannelAccountID, record.OperationType, record.IdempotencyKey)
	}
	return record, nil
}

func (s *Store) getOperationRequestByIdempotency(ctx context.Context, channelAccountID, operationType, idempotencyKey string) (OperationRequestRecord, error) {
	var record OperationRequestRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), operation_type, idempotency_key,
       COALESCE(request_hash,''), status, COALESCE(target_type,''),
       COALESCE(target_id::text,''), COALESCE(error_code,''), metadata,
       created_at, completed_at
FROM operation_requests
WHERE channel_account_id=$1 AND operation_type=$2 AND idempotency_key=$3`,
		channelAccountID, operationType, idempotencyKey).Scan(
		&record.ID,
		&record.ChannelAccountID,
		&record.OperationType,
		&record.IdempotencyKey,
		&record.RequestHash,
		&record.Status,
		&record.TargetType,
		&record.TargetID,
		&record.ErrorCode,
		&record.MetadataJSON,
		&record.CreatedAt,
		&record.CompletedAt,
	)
	return record, err
}

func (s *Store) CreateMediaAssetWithInbox(ctx context.Context, params CreateMediaAssetWithInboxParams) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if params.StoredObject.ID != "" {
			if err := insertStoredObject(ctx, tx, params.StoredObject); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO media_assets (
    id, channel_account_id, stored_object_id, origin_type, origin_ref, kind,
    display_name, status, metadata, created_at, updated_at, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			params.MediaAsset.ID, params.MediaAsset.ChannelAccountID,
			nullString(params.MediaAsset.StoredObjectID), params.MediaAsset.OriginType,
			nullString(params.MediaAsset.OriginRef), params.MediaAsset.Kind, params.MediaAsset.DisplayName,
			withDefault(params.MediaAsset.Status, "available"), jsonOrDefault(params.MediaAsset.MetadataJSON, "{}"),
			params.MediaAsset.CreatedAt, params.MediaAsset.UpdatedAt, params.MediaAsset.DeletedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO collections (
    id, channel_account_id, kind, name, status, version, created_at, updated_at,
    archived_at, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT ON CONSTRAINT collections_pkey DO UPDATE
SET updated_at=EXCLUDED.updated_at`,
			params.InboxCollection.ID, params.InboxCollection.ChannelAccountID, params.InboxCollection.Kind,
			params.InboxCollection.Name, withDefault(params.InboxCollection.Status, "active"),
			positiveVersion(params.InboxCollection.Version), params.InboxCollection.CreatedAt,
			params.InboxCollection.UpdatedAt, params.InboxCollection.ArchivedAt,
			params.InboxCollection.DeletedAt); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO collection_items (
    id, collection_id, media_asset_id, position, added_via_channel_account_id,
    added_at, removed_at
)
VALUES (
    $1,$2,$3,
    COALESCE((SELECT MAX(position) + 1 FROM collection_items WHERE collection_id=$2 AND removed_at IS NULL), $4),
    $5,$6,$7
)`,
			params.CollectionItem.ID, params.CollectionItem.CollectionID, params.CollectionItem.MediaAssetID,
			params.CollectionItem.Position, nullString(params.CollectionItem.AddedViaChannel),
			params.CollectionItem.AddedAt, params.CollectionItem.RemovedAt)
		return err
	})
}

func (s *Store) ListSelectionSnapshotItems(ctx context.Context, selectionSnapshotID string) ([]SelectionSnapshotItemRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, selection_snapshot_id, position, media_asset_id, kind, display_name,
       origin_snapshot, storage_snapshot, metadata_snapshot, status_at_selection,
       diagnostics
FROM selection_snapshot_items
WHERE selection_snapshot_id=$1
ORDER BY position ASC`, selectionSnapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SelectionSnapshotItemRecord
	for rows.Next() {
		var item SelectionSnapshotItemRecord
		if err := rows.Scan(
			&item.ID,
			&item.SelectionSnapshotID,
			&item.Position,
			&item.MediaAssetID,
			&item.Kind,
			&item.DisplayName,
			&item.OriginSnapshotJSON,
			&item.StorageSnapshotJSON,
			&item.MetadataJSON,
			&item.StatusAtSelection,
			&item.DiagnosticsJSON,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) GetSelectionSnapshot(ctx context.Context, channelAccountID, selectionSnapshotID string) (SelectionSnapshotRecord, []SelectionSnapshotItemRecord, error) {
	var snapshot SelectionSnapshotRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), COALESCE(source_collection_id::text,''),
       status, option_snapshot, diagnostics, COALESCE(created_via_channel_account_id::text,''),
       created_at, sealed_at
FROM selection_snapshots
WHERE id=$1 AND channel_account_id=$2`, selectionSnapshotID, channelAccountID).Scan(
		&snapshot.ID,
		&snapshot.ChannelAccountID,
		&snapshot.SourceCollectionID,
		&snapshot.Status,
		&snapshot.OptionSnapshotJSON,
		&snapshot.DiagnosticsJSON,
		&snapshot.CreatedViaChannel,
		&snapshot.CreatedAt,
		&snapshot.SealedAt,
	)
	if err != nil {
		return SelectionSnapshotRecord{}, nil, err
	}
	items, err := s.ListSelectionSnapshotItems(ctx, selectionSnapshotID)
	if err != nil {
		return SelectionSnapshotRecord{}, nil, err
	}
	return snapshot, items, nil
}

func (s *Store) ListMediaAssets(ctx context.Context, channelAccountID string, limit int) ([]MediaAssetRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), COALESCE(stored_object_id::text,''),
       origin_type, COALESCE(origin_ref,''), kind, display_name, status,
       metadata, created_at, updated_at, deleted_at
FROM media_assets
WHERE channel_account_id=$1 AND status <> 'deleted'
ORDER BY created_at DESC
LIMIT $2`, channelAccountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var assets []MediaAssetRecord
	for rows.Next() {
		var asset MediaAssetRecord
		if err := scanMediaAsset(rows, &asset); err != nil {
			return nil, err
		}
		assets = append(assets, asset)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return assets, nil
}

func (s *Store) GetMediaAsset(ctx context.Context, channelAccountID, mediaAssetID string) (MediaAssetRecord, error) {
	var asset MediaAssetRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), COALESCE(stored_object_id::text,''),
       origin_type, COALESCE(origin_ref,''), kind, display_name, status,
       metadata, created_at, updated_at, deleted_at
FROM media_assets
WHERE id=$1 AND channel_account_id=$2`, mediaAssetID, channelAccountID).Scan(
		&asset.ID,
		&asset.ChannelAccountID,
		&asset.StoredObjectID,
		&asset.OriginType,
		&asset.OriginRef,
		&asset.Kind,
		&asset.DisplayName,
		&asset.Status,
		&asset.MetadataJSON,
		&asset.CreatedAt,
		&asset.UpdatedAt,
		&asset.DeletedAt,
	)
	return asset, err
}

func (s *Store) GetStoredObject(ctx context.Context, storedObjectID string) (StoredObjectRecord, error) {
	var object StoredObjectRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, bucket, object_key, COALESCE(content_type,''), size_bytes,
       COALESCE(checksum,''), storage_status, retention_state, created_at,
       expires_at, deleted_at
FROM stored_objects
WHERE id=$1`, storedObjectID).Scan(
		&object.ID,
		&object.Bucket,
		&object.ObjectKey,
		&object.ContentType,
		&object.SizeBytes,
		&object.Checksum,
		&object.StorageStatus,
		&object.RetentionState,
		&object.CreatedAt,
		&object.ExpiresAt,
		&object.DeletedAt,
	)
	return object, err
}

func (s *Store) DeleteMediaAsset(ctx context.Context, channelAccountID, mediaAssetID string, deletedAt time.Time) (MediaAssetRecord, error) {
	var asset MediaAssetRecord
	err := s.db.QueryRowContext(ctx, `
UPDATE media_assets
SET status='deleted', deleted_at=$3, updated_at=$3
WHERE id=$1 AND channel_account_id=$2
RETURNING id, COALESCE(channel_account_id::text,''), COALESCE(stored_object_id::text,''),
          origin_type, COALESCE(origin_ref,''), kind, display_name, status,
          metadata, created_at, updated_at, deleted_at`, mediaAssetID, channelAccountID, deletedAt).Scan(
		&asset.ID,
		&asset.ChannelAccountID,
		&asset.StoredObjectID,
		&asset.OriginType,
		&asset.OriginRef,
		&asset.Kind,
		&asset.DisplayName,
		&asset.Status,
		&asset.MetadataJSON,
		&asset.CreatedAt,
		&asset.UpdatedAt,
		&asset.DeletedAt,
	)
	return asset, err
}

func (s *Store) ListChannelAccounts(ctx context.Context, limit int) ([]ChannelAccountRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, channel, external_account_ref, COALESCE(display_name,''), status,
       metadata, created_at, updated_at, last_seen_at, disabled_at
FROM channel_accounts
ORDER BY COALESCE(last_seen_at, updated_at) DESC
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []ChannelAccountRecord
	for rows.Next() {
		var account ChannelAccountRecord
		if err := scanChannelAccount(rows, &account); err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return accounts, nil
}

func (s *Store) UpdateChannelAccount(ctx context.Context, params UpdateChannelAccountParams) (ChannelAccountRecord, error) {
	var account ChannelAccountRecord
	err := s.db.QueryRowContext(ctx, `
UPDATE channel_accounts
SET display_name=COALESCE(NULLIF($2,''), display_name),
    status=COALESCE(NULLIF($3,''), status),
    metadata=COALESCE(NULLIF($4::jsonb, '{}'::jsonb), metadata),
    last_seen_at=COALESCE($5, last_seen_at),
    disabled_at=$6,
    updated_at=$7
WHERE id=$1
RETURNING id, channel, external_account_ref, COALESCE(display_name,''), status,
          metadata, created_at, updated_at, last_seen_at, disabled_at`,
		params.ID, params.DisplayName, params.Status, jsonOrDefault(params.MetadataJSON, "{}"),
		params.LastSeenAt, params.DisabledAt, params.UpdatedAt).Scan(
		&account.ID,
		&account.Channel,
		&account.ExternalAccountRef,
		&account.DisplayName,
		&account.Status,
		&account.MetadataJSON,
		&account.CreatedAt,
		&account.UpdatedAt,
		&account.LastSeenAt,
		&account.DisabledAt,
	)
	return account, err
}

type mediaAssetScanner interface {
	Scan(dest ...any) error
}

func scanMediaAsset(scanner mediaAssetScanner, asset *MediaAssetRecord) error {
	return scanner.Scan(
		&asset.ID,
		&asset.ChannelAccountID,
		&asset.StoredObjectID,
		&asset.OriginType,
		&asset.OriginRef,
		&asset.Kind,
		&asset.DisplayName,
		&asset.Status,
		&asset.MetadataJSON,
		&asset.CreatedAt,
		&asset.UpdatedAt,
		&asset.DeletedAt,
	)
}

type channelAccountScanner interface {
	Scan(dest ...any) error
}

func scanChannelAccount(scanner channelAccountScanner, account *ChannelAccountRecord) error {
	return scanner.Scan(
		&account.ID,
		&account.Channel,
		&account.ExternalAccountRef,
		&account.DisplayName,
		&account.Status,
		&account.MetadataJSON,
		&account.CreatedAt,
		&account.UpdatedAt,
		&account.LastSeenAt,
		&account.DisabledAt,
	)
}

func (s *Store) GetInboxCollection(ctx context.Context, channelAccountID string) (CollectionRecord, []CollectionItemRecord, error) {
	var collection CollectionRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), kind, name, status, version,
       created_at, updated_at, archived_at, deleted_at
FROM collections
WHERE channel_account_id=$1 AND kind='inbox' AND status <> 'deleted'
ORDER BY created_at ASC
LIMIT 1`, channelAccountID).Scan(
		&collection.ID,
		&collection.ChannelAccountID,
		&collection.Kind,
		&collection.Name,
		&collection.Status,
		&collection.Version,
		&collection.CreatedAt,
		&collection.UpdatedAt,
		&collection.ArchivedAt,
		&collection.DeletedAt,
	)
	if err != nil {
		return CollectionRecord{}, nil, err
	}
	items, err := s.listCollectionItems(ctx, collection.ID)
	if err != nil {
		return CollectionRecord{}, nil, err
	}
	return collection, items, nil
}

func (s *Store) CreateCollection(ctx context.Context, collection CollectionRecord, items []CollectionItemRecord) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO collections (
    id, channel_account_id, kind, name, status, version, created_at, updated_at,
    archived_at, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			collection.ID, collection.ChannelAccountID, withDefault(collection.Kind, "user"),
			collection.Name, withDefault(collection.Status, "active"),
			positiveVersion(collection.Version), collection.CreatedAt, collection.UpdatedAt,
			collection.ArchivedAt, collection.DeletedAt); err != nil {
			return err
		}
		for _, item := range items {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO collection_items (
    id, collection_id, media_asset_id, position, added_via_channel_account_id,
    added_at, removed_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
				item.ID, item.CollectionID, item.MediaAssetID, item.Position,
				nullString(item.AddedViaChannel), item.AddedAt, item.RemovedAt); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ListCollections(ctx context.Context, channelAccountID string, limit int) ([]CollectionRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), kind, name, status, version,
       created_at, updated_at, archived_at, deleted_at
FROM collections
WHERE channel_account_id=$1 AND status <> 'deleted'
ORDER BY updated_at DESC
LIMIT $2`, channelAccountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var collections []CollectionRecord
	for rows.Next() {
		var collection CollectionRecord
		if err := scanCollection(rows, &collection); err != nil {
			return nil, err
		}
		collections = append(collections, collection)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return collections, nil
}

func (s *Store) GetCollection(ctx context.Context, channelAccountID, collectionID string) (CollectionRecord, []CollectionItemRecord, error) {
	var collection CollectionRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), kind, name, status, version,
       created_at, updated_at, archived_at, deleted_at
FROM collections
WHERE id=$1 AND channel_account_id=$2 AND status <> 'deleted'`, collectionID, channelAccountID).Scan(
		&collection.ID,
		&collection.ChannelAccountID,
		&collection.Kind,
		&collection.Name,
		&collection.Status,
		&collection.Version,
		&collection.CreatedAt,
		&collection.UpdatedAt,
		&collection.ArchivedAt,
		&collection.DeletedAt,
	)
	if err != nil {
		return CollectionRecord{}, nil, err
	}
	items, err := s.listCollectionItems(ctx, collection.ID)
	if err != nil {
		return CollectionRecord{}, nil, err
	}
	return collection, items, nil
}

func (s *Store) UpdateCollection(ctx context.Context, params UpdateCollectionParams) (CollectionRecord, []CollectionItemRecord, error) {
	var collection CollectionRecord
	err := s.db.QueryRowContext(ctx, `
UPDATE collections
SET name=COALESCE(NULLIF($4,''), name),
    status=COALESCE(NULLIF($5,''), status),
    version=version+1,
    updated_at=$6,
    archived_at=CASE WHEN $5='archived' THEN $6 ELSE archived_at END,
    deleted_at=CASE WHEN $5='deleted' THEN $6 ELSE deleted_at END
WHERE id=$1 AND channel_account_id=$2 AND version=$3
RETURNING id, COALESCE(channel_account_id::text,''), kind, name, status, version,
          created_at, updated_at, archived_at, deleted_at`,
		params.CollectionID, params.ChannelAccountID, params.ExpectedVersion,
		params.Name, params.Status, params.UpdatedAt).Scan(
		&collection.ID,
		&collection.ChannelAccountID,
		&collection.Kind,
		&collection.Name,
		&collection.Status,
		&collection.Version,
		&collection.CreatedAt,
		&collection.UpdatedAt,
		&collection.ArchivedAt,
		&collection.DeletedAt,
	)
	if err != nil {
		return CollectionRecord{}, nil, err
	}
	items, err := s.listCollectionItems(ctx, collection.ID)
	if err != nil {
		return CollectionRecord{}, nil, err
	}
	return collection, items, nil
}

func (s *Store) UpdateCollectionItems(ctx context.Context, params UpdateCollectionItemsParams) (CollectionRecord, []CollectionItemRecord, error) {
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
UPDATE collections
SET version=version+1, updated_at=$4
WHERE id=$1 AND channel_account_id=$2 AND version=$3`,
			params.CollectionID, params.ChannelAccountID, params.ExpectedVersion, params.UpdatedAt)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return sql.ErrNoRows
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE collection_items
SET removed_at=$2
WHERE collection_id=$1 AND removed_at IS NULL`, params.CollectionID, params.UpdatedAt); err != nil {
			return err
		}
		for _, item := range params.Items {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO collection_items (
    id, collection_id, media_asset_id, position, added_via_channel_account_id,
    added_at, removed_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
				item.ID, params.CollectionID, item.MediaAssetID, item.Position,
				nullString(item.AddedViaChannel), params.UpdatedAt, item.RemovedAt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return CollectionRecord{}, nil, err
	}
	return s.GetCollection(ctx, params.ChannelAccountID, params.CollectionID)
}

func (s *Store) RemoveCollectionItem(ctx context.Context, params RemoveCollectionItemParams) (CollectionRecord, []CollectionItemRecord, error) {
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
UPDATE collections
SET version=version+1, updated_at=$4
WHERE id=$1 AND channel_account_id=$2 AND version=$3`,
			params.CollectionID, params.ChannelAccountID, params.ExpectedVersion, params.RemovedAt)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return sql.ErrNoRows
		}
		_, err = tx.ExecContext(ctx, `
UPDATE collection_items
SET removed_at=$4
WHERE collection_id=$1 AND media_asset_id=$2 AND removed_at IS NULL
  AND EXISTS (
      SELECT 1 FROM collections
      WHERE collections.id=$1 AND collections.channel_account_id=$3
  )`, params.CollectionID, params.MediaAssetID, params.ChannelAccountID, params.RemovedAt)
		return err
	}); err != nil {
		return CollectionRecord{}, nil, err
	}
	return s.GetCollection(ctx, params.ChannelAccountID, params.CollectionID)
}

type collectionScanner interface {
	Scan(dest ...any) error
}

func scanCollection(scanner collectionScanner, collection *CollectionRecord) error {
	return scanner.Scan(
		&collection.ID,
		&collection.ChannelAccountID,
		&collection.Kind,
		&collection.Name,
		&collection.Status,
		&collection.Version,
		&collection.CreatedAt,
		&collection.UpdatedAt,
		&collection.ArchivedAt,
		&collection.DeletedAt,
	)
}

func (s *Store) listCollectionItems(ctx context.Context, collectionID string) ([]CollectionItemRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT ci.id, ci.collection_id, ci.media_asset_id, ci.position,
       COALESCE(ci.added_via_channel_account_id::text,''), ci.added_at, ci.removed_at,
       ma.id, COALESCE(ma.channel_account_id::text,''), COALESCE(ma.stored_object_id::text,''),
       ma.origin_type, COALESCE(ma.origin_ref,''), ma.kind, ma.display_name, ma.status,
       ma.metadata, ma.created_at, ma.updated_at, ma.deleted_at
FROM collection_items ci
JOIN media_assets ma ON ma.id=ci.media_asset_id
WHERE ci.collection_id=$1 AND ci.removed_at IS NULL
ORDER BY ci.position ASC`, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []CollectionItemRecord
	for rows.Next() {
		var item CollectionItemRecord
		var asset MediaAssetRecord
		if err := rows.Scan(
			&item.ID,
			&item.CollectionID,
			&item.MediaAssetID,
			&item.Position,
			&item.AddedViaChannel,
			&item.AddedAt,
			&item.RemovedAt,
			&asset.ID,
			&asset.ChannelAccountID,
			&asset.StoredObjectID,
			&asset.OriginType,
			&asset.OriginRef,
			&asset.Kind,
			&asset.DisplayName,
			&asset.Status,
			&asset.MetadataJSON,
			&asset.CreatedAt,
			&asset.UpdatedAt,
			&asset.DeletedAt,
		); err != nil {
			return nil, err
		}
		item.MediaAsset = &asset
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) CreateSelectionSnapshot(ctx context.Context, snapshot SelectionSnapshotRecord, items []SelectionSnapshotItemRecord) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO selection_snapshots (
    id, channel_account_id, source_collection_id, status, option_snapshot,
    diagnostics, created_via_channel_account_id, created_at, sealed_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			snapshot.ID, snapshot.ChannelAccountID, nullString(snapshot.SourceCollectionID),
			withDefault(snapshot.Status, "sealed"), jsonOrDefault(snapshot.OptionSnapshotJSON, "{}"),
			jsonOrDefault(snapshot.DiagnosticsJSON, "[]"), nullString(snapshot.CreatedViaChannel),
			snapshot.CreatedAt, snapshot.SealedAt); err != nil {
			return err
		}
		for _, item := range items {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO selection_snapshot_items (
    id, selection_snapshot_id, position, media_asset_id, kind, display_name,
    origin_snapshot, storage_snapshot, metadata_snapshot, status_at_selection,
    diagnostics
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
				item.ID, item.SelectionSnapshotID, item.Position, item.MediaAssetID, item.Kind,
				item.DisplayName, jsonOrDefault(item.OriginSnapshotJSON, "{}"),
				jsonOrDefault(item.StorageSnapshotJSON, "{}"), jsonOrDefault(item.MetadataJSON, "{}"),
				item.StatusAtSelection, jsonOrDefault(item.DiagnosticsJSON, "[]")); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) CreateAnalysisRunGraph(ctx context.Context, graph AnalysisRunGraph) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		run := graph.Run
		if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_runs (
    id, channel_account_id, selection_snapshot_id, run_type, status, version,
    idempotency_key, params, delivery, evidence_gate_state,
    created_via_channel_account_id, created_at, started_at, completed_at,
    cancel_requested_at, canceled_at, expires_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
			run.ID, run.ChannelAccountID, run.SelectionSnapshot, run.RunType,
			withDefault(run.Status, "queued"), positiveVersion(run.Version), nullString(run.IdempotencyKey),
			jsonOrDefault(run.ParamsJSON, "{}"), jsonOrDefault(run.DeliveryJSON, `{"strategy":"polling"}`),
			withDefault(run.EvidenceGateState, "not_required"), nullString(run.CreatedViaChannel),
			run.CreatedAt, run.StartedAt, run.CompletedAt, run.CancelRequestedAt, run.CanceledAt,
			run.ExpiresAt); err != nil {
			return err
		}
		for _, step := range graph.Steps {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_steps (
    id, analysis_run_id, step_kind, worker_kind, status, attempt_no,
    lease_owner, claimed_at, heartbeat_at, finalized_at, metadata, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
				step.ID, step.AnalysisRunID, step.StepKind, step.WorkerKind,
				withDefault(step.Status, "pending"), positiveInt(step.AttemptNo),
				nullString(step.LeaseOwner), step.ClaimedAt, step.HeartbeatAt, step.FinalizedAt,
				jsonOrDefault(step.MetadataJSON, "{}"), step.CreatedAt); err != nil {
				return err
			}
		}
		for _, input := range graph.StepInputs {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_step_inputs (
    id, analysis_run_step_id, input_kind, selection_snapshot_item_id, artifact_id,
    position, required, metadata, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
				input.ID, input.AnalysisRunStepID, input.InputKind,
				nullString(input.SelectionSnapshotItemID), nullString(input.ArtifactID),
				input.Position, input.Required, jsonOrDefault(input.MetadataJSON, "{}"),
				input.CreatedAt); err != nil {
				return err
			}
		}
		event := graph.Event
		_, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (
    id, analysis_run_id, event_type, version, status, payload, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			event.ID, event.AnalysisRunID, event.EventType, event.Version,
			nullString(event.Status), jsonOrDefault(event.PayloadJSON, "{}"), event.CreatedAt)
		return err
	})
}

func (s *Store) ClaimAnalysisRunStep(ctx context.Context, analysisRunID, workerKind, stepKind, leaseOwner string, claimedAt time.Time) (AnalysisRunStepRecord, []AnalysisRunStepInputRecord, bool, error) {
	var step AnalysisRunStepRecord
	err := s.db.QueryRowContext(ctx, `
UPDATE analysis_run_steps
SET status='claimed',
    lease_owner=$4,
    claimed_at=$5,
    heartbeat_at=$5
WHERE analysis_run_id=$1
  AND worker_kind=$2
  AND step_kind=$3
  AND status='queued'
RETURNING id, analysis_run_id, step_kind, worker_kind, status, attempt_no,
          COALESCE(lease_owner,''), claimed_at, heartbeat_at, finalized_at,
          metadata, created_at`,
		analysisRunID, workerKind, stepKind, leaseOwner, claimedAt).Scan(
		&step.ID,
		&step.AnalysisRunID,
		&step.StepKind,
		&step.WorkerKind,
		&step.Status,
		&step.AttemptNo,
		&step.LeaseOwner,
		&step.ClaimedAt,
		&step.HeartbeatAt,
		&step.FinalizedAt,
		&step.MetadataJSON,
		&step.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return AnalysisRunStepRecord{}, nil, false, nil
	}
	if err != nil {
		return AnalysisRunStepRecord{}, nil, false, err
	}
	inputs, err := s.listAnalysisRunStepInputs(ctx, step.ID)
	if err != nil {
		return AnalysisRunStepRecord{}, nil, false, err
	}
	return step, inputs, true, nil
}

func (s *Store) ListAnalysisRuns(ctx context.Context, channelAccountID string, limit int) ([]AnalysisRunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
       status, version, COALESCE(idempotency_key,''), params, delivery,
       evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
       created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at
FROM analysis_runs
WHERE channel_account_id=$1
ORDER BY created_at DESC
LIMIT $2`, channelAccountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []AnalysisRunRecord
	for rows.Next() {
		var run AnalysisRunRecord
		if err := scanAnalysisRun(rows, &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func (s *Store) GetAnalysisRun(ctx context.Context, channelAccountID, analysisRunID string) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
       status, version, COALESCE(idempotency_key,''), params, delivery,
       evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
       created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at
FROM analysis_runs
WHERE id=$1 AND channel_account_id=$2`, analysisRunID, channelAccountID).Scan(
		&run.ID,
		&run.ChannelAccountID,
		&run.SelectionSnapshot,
		&run.RunType,
		&run.Status,
		&run.Version,
		&run.IdempotencyKey,
		&run.ParamsJSON,
		&run.DeliveryJSON,
		&run.EvidenceGateState,
		&run.CreatedViaChannel,
		&run.CreatedAt,
		&run.StartedAt,
		&run.CompletedAt,
		&run.CancelRequestedAt,
		&run.CanceledAt,
		&run.ExpiresAt,
	)
	return run, err
}

func (s *Store) GetAnalysisRunByID(ctx context.Context, analysisRunID string) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
       status, version, COALESCE(idempotency_key,''), params, delivery,
       evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
       created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at
FROM analysis_runs
WHERE id=$1`, analysisRunID).Scan(
		&run.ID,
		&run.ChannelAccountID,
		&run.SelectionSnapshot,
		&run.RunType,
		&run.Status,
		&run.Version,
		&run.IdempotencyKey,
		&run.ParamsJSON,
		&run.DeliveryJSON,
		&run.EvidenceGateState,
		&run.CreatedViaChannel,
		&run.CreatedAt,
		&run.StartedAt,
		&run.CompletedAt,
		&run.CancelRequestedAt,
		&run.CanceledAt,
		&run.ExpiresAt,
	)
	return run, err
}

func (s *Store) ListAnalysisRunStepQueue(ctx context.Context, status, runType, workerKind, stepKind string, limit int) ([]AnalysisRunStepQueueRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT r.id, r.run_type, s.worker_kind, s.step_kind, s.status, r.version,
       s.attempt_no, s.id, s.created_at
FROM analysis_run_steps s
JOIN analysis_runs r ON r.id=s.analysis_run_id
WHERE ($1='' OR s.status=$1)
  AND ($2='' OR r.run_type=$2)
  AND ($3='' OR s.worker_kind=$3)
  AND ($4='' OR s.step_kind=$4)
ORDER BY s.created_at ASC
LIMIT $5`, status, runType, workerKind, stepKind, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AnalysisRunStepQueueRecord
	for rows.Next() {
		var item AnalysisRunStepQueueRecord
		if err := rows.Scan(
			&item.AnalysisRunID,
			&item.RunType,
			&item.WorkerKind,
			&item.StepKind,
			&item.Status,
			&item.Version,
			&item.AttemptNo,
			&item.AnalysisRunStepID,
			&item.CreatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) ListArtifacts(ctx context.Context, channelAccountID, analysisRunID string, limit int) ([]ArtifactRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), analysis_run_id,
       COALESCE(stored_object_id::text,''), kind, status, content_type,
       COALESCE(checksum,''), size_bytes, visibility, preview,
       created_at, expires_at, deleted_at
FROM artifacts
WHERE channel_account_id=$1 AND ($2='' OR analysis_run_id::text=$2)
ORDER BY created_at DESC
LIMIT $3`, channelAccountID, analysisRunID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var artifacts []ArtifactRecord
	for rows.Next() {
		var artifact ArtifactRecord
		if err := scanArtifact(rows, &artifact); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return artifacts, nil
}

func (s *Store) GetArtifact(ctx context.Context, channelAccountID, artifactID string) (ArtifactRecord, error) {
	var artifact ArtifactRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), analysis_run_id,
       COALESCE(stored_object_id::text,''), kind, status, content_type,
       COALESCE(checksum,''), size_bytes, visibility, preview,
       created_at, expires_at, deleted_at
FROM artifacts
WHERE id=$1 AND channel_account_id=$2`, artifactID, channelAccountID).Scan(
		&artifact.ID,
		&artifact.ChannelAccountID,
		&artifact.AnalysisRunID,
		&artifact.StoredObjectID,
		&artifact.Kind,
		&artifact.Status,
		&artifact.ContentType,
		&artifact.Checksum,
		&artifact.SizeBytes,
		&artifact.Visibility,
		&artifact.PreviewJSON,
		&artifact.CreatedAt,
		&artifact.ExpiresAt,
		&artifact.DeletedAt,
	)
	return artifact, err
}

func (s *Store) ListDiagnostics(ctx context.Context, query DiagnosticQuery, limit int) ([]DiagnosticRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), subject_type, subject_id,
       severity, code, message, context, safe_channel_context,
       COALESCE(correlation_id,''), COALESCE(remediation_hint,''), created_at
FROM diagnostics
WHERE channel_account_id=$1
  AND ($2='' OR subject_type=$2)
  AND ($3='' OR subject_id::text=$3)
  AND ($4='' OR severity=$4)
  AND ($5='' OR code=$5)
  AND ($6='' OR correlation_id=$6)
ORDER BY created_at DESC
LIMIT $7`, query.ChannelAccountID, query.SubjectType, query.SubjectID, query.Severity, query.Code, query.CorrelationID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var diagnostics []DiagnosticRecord
	for rows.Next() {
		var diagnostic DiagnosticRecord
		if err := rows.Scan(
			&diagnostic.ID,
			&diagnostic.ChannelAccountID,
			&diagnostic.SubjectType,
			&diagnostic.SubjectID,
			&diagnostic.Severity,
			&diagnostic.Code,
			&diagnostic.Message,
			&diagnostic.ContextJSON,
			&diagnostic.SafeChannelContext,
			&diagnostic.CorrelationID,
			&diagnostic.RemediationHint,
			&diagnostic.CreatedAt,
		); err != nil {
			return nil, err
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return diagnostics, nil
}

type artifactScanner interface {
	Scan(dest ...any) error
}

func scanArtifact(scanner artifactScanner, artifact *ArtifactRecord) error {
	return scanner.Scan(
		&artifact.ID,
		&artifact.ChannelAccountID,
		&artifact.AnalysisRunID,
		&artifact.StoredObjectID,
		&artifact.Kind,
		&artifact.Status,
		&artifact.ContentType,
		&artifact.Checksum,
		&artifact.SizeBytes,
		&artifact.Visibility,
		&artifact.PreviewJSON,
		&artifact.CreatedAt,
		&artifact.ExpiresAt,
		&artifact.DeletedAt,
	)
}

type analysisRunScanner interface {
	Scan(dest ...any) error
}

func scanAnalysisRun(scanner analysisRunScanner, run *AnalysisRunRecord) error {
	return scanner.Scan(
		&run.ID,
		&run.ChannelAccountID,
		&run.SelectionSnapshot,
		&run.RunType,
		&run.Status,
		&run.Version,
		&run.IdempotencyKey,
		&run.ParamsJSON,
		&run.DeliveryJSON,
		&run.EvidenceGateState,
		&run.CreatedViaChannel,
		&run.CreatedAt,
		&run.StartedAt,
		&run.CompletedAt,
		&run.CancelRequestedAt,
		&run.CanceledAt,
		&run.ExpiresAt,
	)
}

func (s *Store) listAnalysisRunStepInputs(ctx context.Context, stepID string) ([]AnalysisRunStepInputRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, analysis_run_step_id, input_kind,
       COALESCE(selection_snapshot_item_id::text, ''),
       COALESCE(artifact_id::text, ''),
       position, required, metadata, created_at
FROM analysis_run_step_inputs
WHERE analysis_run_step_id=$1
ORDER BY position ASC`, stepID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var inputs []AnalysisRunStepInputRecord
	for rows.Next() {
		var input AnalysisRunStepInputRecord
		if err := rows.Scan(
			&input.ID,
			&input.AnalysisRunStepID,
			&input.InputKind,
			&input.SelectionSnapshotItemID,
			&input.ArtifactID,
			&input.Position,
			&input.Required,
			&input.MetadataJSON,
			&input.CreatedAt,
		); err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return inputs, nil
}

func (s *Store) RequestAnalysisRunCancel(ctx context.Context, channelAccountID, analysisRunID string, event AnalysisRunEventRecord, requestedAt time.Time) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=CASE
        WHEN status IN ('succeeded', 'failed', 'partially_succeeded', 'canceled', 'expired') THEN status
        ELSE 'cancel_requested'
    END,
    cancel_requested_at=COALESCE(cancel_requested_at, $3),
    version=version+1
WHERE id=$1 AND channel_account_id=$2
RETURNING id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
          status, version, COALESCE(idempotency_key,''), params, delivery,
          evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
          created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at`,
			analysisRunID, channelAccountID, requestedAt).Scan(
			&run.ID,
			&run.ChannelAccountID,
			&run.SelectionSnapshot,
			&run.RunType,
			&run.Status,
			&run.Version,
			&run.IdempotencyKey,
			&run.ParamsJSON,
			&run.DeliveryJSON,
			&run.EvidenceGateState,
			&run.CreatedViaChannel,
			&run.CreatedAt,
			&run.StartedAt,
			&run.CompletedAt,
			&run.CancelRequestedAt,
			&run.CanceledAt,
			&run.ExpiresAt,
		)
		if err != nil {
			return err
		}
		event.Version = run.Version
		_, err = tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (
    id, analysis_run_id, event_type, version, status, payload, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			event.ID, event.AnalysisRunID, event.EventType, event.Version,
			nullString(event.Status), jsonOrDefault(event.PayloadJSON, "{}"), event.CreatedAt)
		return err
	}); err != nil {
		return AnalysisRunRecord{}, err
	}
	return run, nil
}

func (s *Store) ListAnalysisRunEvents(ctx context.Context, channelAccountID, analysisRunID string, limit int) ([]AnalysisRunEventRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT e.id, e.analysis_run_id, e.event_type, e.version, COALESCE(e.status,''),
       e.payload, e.created_at
FROM analysis_run_events e
JOIN analysis_runs r ON r.id=e.analysis_run_id
WHERE e.analysis_run_id=$1 AND r.channel_account_id=$2
ORDER BY e.created_at ASC
LIMIT $3`, analysisRunID, channelAccountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []AnalysisRunEventRecord
	for rows.Next() {
		var event AnalysisRunEventRecord
		if err := rows.Scan(
			&event.ID,
			&event.AnalysisRunID,
			&event.EventType,
			&event.Version,
			&event.Status,
			&event.PayloadJSON,
			&event.CreatedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Store) CheckAnalysisRunStepCancel(ctx context.Context, analysisRunID, analysisRunStepID string) (AnalysisRunRecord, AnalysisRunStepRecord, error) {
	var run AnalysisRunRecord
	var step AnalysisRunStepRecord
	err := s.db.QueryRowContext(ctx, `
SELECT r.id, COALESCE(r.channel_account_id::text,''), r.selection_snapshot_id, r.run_type,
       r.status, r.version, COALESCE(r.idempotency_key,''), r.params, r.delivery,
       r.evidence_gate_state, COALESCE(r.created_via_channel_account_id::text,''),
       r.created_at, r.started_at, r.completed_at, r.cancel_requested_at, r.canceled_at, r.expires_at,
       s.id, s.analysis_run_id, s.step_kind, s.worker_kind, s.status, s.attempt_no,
       COALESCE(s.lease_owner,''), s.claimed_at, s.heartbeat_at, s.finalized_at,
       s.metadata, s.created_at
FROM analysis_runs r
JOIN analysis_run_steps s ON s.analysis_run_id=r.id
WHERE r.id=$1 AND s.id=$2`, analysisRunID, analysisRunStepID).Scan(
		&run.ID,
		&run.ChannelAccountID,
		&run.SelectionSnapshot,
		&run.RunType,
		&run.Status,
		&run.Version,
		&run.IdempotencyKey,
		&run.ParamsJSON,
		&run.DeliveryJSON,
		&run.EvidenceGateState,
		&run.CreatedViaChannel,
		&run.CreatedAt,
		&run.StartedAt,
		&run.CompletedAt,
		&run.CancelRequestedAt,
		&run.CanceledAt,
		&run.ExpiresAt,
		&step.ID,
		&step.AnalysisRunID,
		&step.StepKind,
		&step.WorkerKind,
		&step.Status,
		&step.AttemptNo,
		&step.LeaseOwner,
		&step.ClaimedAt,
		&step.HeartbeatAt,
		&step.FinalizedAt,
		&step.MetadataJSON,
		&step.CreatedAt,
	)
	return run, step, err
}

func (s *Store) RecordAnalysisRunStepProgress(ctx context.Context, params RecordAnalysisRunProgressParams) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
UPDATE analysis_run_steps
SET heartbeat_at=$3
WHERE analysis_run_id=$1 AND id=$2`,
			params.AnalysisRunID, params.AnalysisRunStepID, params.HeartbeatAt)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return sql.ErrNoRows
		}
		var version int64
		if err := tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=CASE WHEN status='queued' THEN 'running' ELSE status END,
    started_at=COALESCE(started_at, $2),
    version=version+1
WHERE id=$1
RETURNING version`, params.AnalysisRunID, params.HeartbeatAt).Scan(&version); err != nil {
			return err
		}
		event := params.Event
		event.Version = version
		_, err = tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (
    id, analysis_run_id, event_type, version, status, payload, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			event.ID, event.AnalysisRunID, event.EventType, event.Version,
			nullString(event.Status), jsonOrDefault(event.PayloadJSON, "{}"), event.CreatedAt)
		return err
	})
}

func (s *Store) FinalizeAnalysisRunStep(ctx context.Context, params FinalizeAnalysisRunStepParams) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		var currentStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM analysis_runs WHERE id=$1`, params.AnalysisRunID).Scan(&currentStatus); err != nil {
			return err
		}
		stepStatus := params.StepStatus
		runStatus := params.RunStatus
		canceled := currentStatus == "cancel_requested" || currentStatus == "canceled"
		if canceled {
			stepStatus = "canceled"
			runStatus = "canceled"
		}
		result, err := tx.ExecContext(ctx, `
UPDATE analysis_run_steps
SET status=$3, finalized_at=$4, heartbeat_at=$4
WHERE analysis_run_id=$1 AND id=$2`,
			params.AnalysisRunID, params.AnalysisRunStepID, stepStatus, params.FinalizedAt)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return sql.ErrNoRows
		}
		err = tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=$2,
    completed_at=$3,
    canceled_at=CASE WHEN $2='canceled' THEN $3 ELSE canceled_at END,
    version=version+1
WHERE id=$1
RETURNING id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
          status, version, COALESCE(idempotency_key,''), params, delivery,
          evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
          created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at`,
			params.AnalysisRunID, runStatus, params.FinalizedAt).Scan(
			&run.ID,
			&run.ChannelAccountID,
			&run.SelectionSnapshot,
			&run.RunType,
			&run.Status,
			&run.Version,
			&run.IdempotencyKey,
			&run.ParamsJSON,
			&run.DeliveryJSON,
			&run.EvidenceGateState,
			&run.CreatedViaChannel,
			&run.CreatedAt,
			&run.StartedAt,
			&run.CompletedAt,
			&run.CancelRequestedAt,
			&run.CanceledAt,
			&run.ExpiresAt,
		)
		if err != nil {
			return err
		}
		event := params.Event
		event.Version = run.Version
		event.Status = run.Status
		_, err = tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (
    id, analysis_run_id, event_type, version, status, payload, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			event.ID, event.AnalysisRunID, event.EventType, event.Version,
			nullString(event.Status), jsonOrDefault(event.PayloadJSON, "{}"), event.CreatedAt)
		return err
	}); err != nil {
		return AnalysisRunRecord{}, err
	}
	return run, nil
}

func (s *Store) RecordArtifacts(ctx context.Context, storedObjects []StoredObjectRecord, artifacts []ArtifactRecord, subjects []ArtifactSubjectRecord) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, object := range storedObjects {
			if err := insertStoredObject(ctx, tx, object); err != nil {
				return err
			}
		}
		for _, artifact := range artifacts {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO artifacts (
    id, channel_account_id, analysis_run_id, stored_object_id, kind, status,
    content_type, checksum, size_bytes, visibility, preview, created_at,
    expires_at, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
				artifact.ID, artifact.ChannelAccountID, artifact.AnalysisRunID,
				nullString(artifact.StoredObjectID), artifact.Kind, withDefault(artifact.Status, "pending"),
				artifact.ContentType, nullString(artifact.Checksum), artifact.SizeBytes,
				withDefault(artifact.Visibility, "private"), jsonOrDefault(artifact.PreviewJSON, `{"available":false}`),
				artifact.CreatedAt, artifact.ExpiresAt, artifact.DeletedAt); err != nil {
				return err
			}
		}
		for _, subject := range subjects {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO artifact_subjects (
    id, artifact_id, subject_type, subject_id, subject_role, created_at
)
VALUES ($1,$2,$3,$4,$5,$6)`,
				subject.ID, subject.ArtifactID, subject.SubjectType, subject.SubjectID,
				withDefault(subject.SubjectRole, "primary"), subject.CreatedAt); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) RecordDiagnostics(ctx context.Context, diagnostics []DiagnosticRecord) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, diagnostic := range diagnostics {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO diagnostics (
    id, channel_account_id, subject_type, subject_id, severity, code, message,
    context, safe_channel_context, correlation_id, remediation_hint, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
				diagnostic.ID, diagnostic.ChannelAccountID, diagnostic.SubjectType,
				diagnostic.SubjectID, diagnostic.Severity, diagnostic.Code, diagnostic.Message,
				jsonOrDefault(diagnostic.ContextJSON, "{}"), jsonOrDefault(diagnostic.SafeChannelContext, "{}"),
				nullString(diagnostic.CorrelationID), nullString(diagnostic.RemediationHint),
				diagnostic.CreatedAt); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) UpsertChannelSurface(ctx context.Context, surface ChannelSurfaceRecord, subjects []ChannelSurfaceSubjectRecord) (ChannelSurfaceRecord, error) {
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `
	INSERT INTO channel_surfaces (
	    id, channel_account_id, channel, surface_type, surface_key, address,
	    address_fingerprint, display_state, lifecycle_status, version,
	    idempotency_key, created_at, updated_at, last_rendered_at,
	    superseded_at, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (channel_account_id, channel, surface_type, surface_key)
WHERE lifecycle_status = 'active' AND deleted_at IS NULL
DO UPDATE SET
    address=EXCLUDED.address,
    address_fingerprint=EXCLUDED.address_fingerprint,
    display_state=EXCLUDED.display_state,
	    version=channel_surfaces.version + 1,
	    idempotency_key=EXCLUDED.idempotency_key,
	    updated_at=EXCLUDED.updated_at,
	    last_rendered_at=EXCLUDED.last_rendered_at
	RETURNING id, COALESCE(channel_account_id::text,''), channel, surface_type, surface_key,
	          address, COALESCE(address_fingerprint,''), display_state, lifecycle_status,
	          version, COALESCE(idempotency_key,''), created_at, updated_at,
	          last_rendered_at, superseded_at, deleted_at`,
			surface.ID, surface.ChannelAccountID, surface.Channel, surface.SurfaceType,
			surface.SurfaceKey, jsonOrDefault(surface.AddressJSON, "{}"),
			nullString(surface.AddressFingerprint), jsonOrDefault(surface.DisplayStateJSON, "{}"),
			withDefault(surface.LifecycleStatus, "active"), positiveVersion(surface.Version),
			nullString(surface.IdempotencyKey), surface.CreatedAt, surface.UpdatedAt,
			surface.LastRenderedAt, surface.SupersededAt, surface.DeletedAt).Scan(
			&surface.ID,
			&surface.ChannelAccountID,
			&surface.Channel,
			&surface.SurfaceType,
			&surface.SurfaceKey,
			&surface.AddressJSON,
			&surface.AddressFingerprint,
			&surface.DisplayStateJSON,
			&surface.LifecycleStatus,
			&surface.Version,
			&surface.IdempotencyKey,
			&surface.CreatedAt,
			&surface.UpdatedAt,
			&surface.LastRenderedAt,
			&surface.SupersededAt,
			&surface.DeletedAt,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM channel_surface_subjects WHERE surface_id=$1`, surface.ID); err != nil {
			return err
		}
		for _, subject := range subjects {
			if _, err := tx.ExecContext(ctx, `
	INSERT INTO channel_surface_subjects (
	    surface_id, subject_type, subject_id, subject_role, created_at
	)
	VALUES ($1,$2,$3,$4,$5)
	ON CONFLICT (surface_id, subject_role, subject_type, subject_id) DO NOTHING`,
				surface.ID, subject.SubjectType, subject.SubjectID,
				subject.SubjectRole, subject.CreatedAt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return ChannelSurfaceRecord{}, err
	}
	return surface, nil
}

func (s *Store) ListChannelSurfaces(ctx context.Context, query ChannelSurfaceQuery, limit int) ([]ChannelSurfaceRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT DISTINCT cs.id, COALESCE(cs.channel_account_id::text,''), cs.channel,
       cs.surface_type, cs.surface_key, cs.address, COALESCE(cs.address_fingerprint,''),
       cs.display_state, cs.lifecycle_status, cs.version, COALESCE(cs.idempotency_key,''),
       cs.created_at, cs.updated_at, cs.last_rendered_at, cs.superseded_at, cs.deleted_at
FROM channel_surfaces cs
LEFT JOIN channel_surface_subjects css ON css.surface_id=cs.id
WHERE ($1='' OR cs.channel_account_id::text=$1)
  AND ($2='' OR css.subject_type=$2)
  AND ($3='' OR css.subject_id::text=$3)
  AND ($4='' OR cs.lifecycle_status=$4)
  AND ($5=false OR (cs.lifecycle_status='active' AND cs.deleted_at IS NULL))
ORDER BY cs.updated_at DESC
LIMIT $6`,
		query.ChannelAccountID, query.SubjectType, query.SubjectID,
		query.LifecycleStatus, query.ActiveOnly, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var surfaces []ChannelSurfaceRecord
	for rows.Next() {
		var surface ChannelSurfaceRecord
		if err := scanChannelSurface(rows, &surface); err != nil {
			return nil, err
		}
		surfaces = append(surfaces, surface)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return surfaces, nil
}

func (s *Store) ListChannelSurfaceSubjects(ctx context.Context, surfaceID string) ([]ChannelSurfaceSubjectRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT surface_id, subject_type, subject_id, subject_role, created_at
FROM channel_surface_subjects
WHERE surface_id=$1
ORDER BY created_at ASC`, surfaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subjects []ChannelSurfaceSubjectRecord
	for rows.Next() {
		var subject ChannelSurfaceSubjectRecord
		if err := rows.Scan(
			&subject.SurfaceID,
			&subject.SubjectType,
			&subject.SubjectID,
			&subject.SubjectRole,
			&subject.CreatedAt,
		); err != nil {
			return nil, err
		}
		subjects = append(subjects, subject)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return subjects, nil
}

func (s *Store) ReplaceChannelSurfaceDisplayState(ctx context.Context, params ReplaceChannelSurfaceDisplayStateParams) (ChannelSurfaceRecord, error) {
	var surface ChannelSurfaceRecord
	if err := s.withTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `
UPDATE channel_surfaces
SET display_state=$3,
    version=version+1,
    updated_at=$4,
    last_rendered_at=$4
WHERE id=$1 AND version=$2 AND lifecycle_status='active' AND deleted_at IS NULL
RETURNING id, COALESCE(channel_account_id::text,''), channel, surface_type, surface_key,
          address, COALESCE(address_fingerprint,''), display_state, lifecycle_status,
          version, COALESCE(idempotency_key,''), created_at, updated_at,
          last_rendered_at, superseded_at, deleted_at`,
			params.SurfaceID, params.ExpectedVersion, jsonOrDefault(params.DisplayStateJSON, "{}"),
			params.UpdatedAt).Scan(
			&surface.ID,
			&surface.ChannelAccountID,
			&surface.Channel,
			&surface.SurfaceType,
			&surface.SurfaceKey,
			&surface.AddressJSON,
			&surface.AddressFingerprint,
			&surface.DisplayStateJSON,
			&surface.LifecycleStatus,
			&surface.Version,
			&surface.IdempotencyKey,
			&surface.CreatedAt,
			&surface.UpdatedAt,
			&surface.LastRenderedAt,
			&surface.SupersededAt,
			&surface.DeletedAt,
		)
		if err != nil {
			return err
		}
		event := params.Event
		event.NextVersion = surface.Version
		_, err = tx.ExecContext(ctx, `
INSERT INTO channel_surface_events (
    id, surface_id, event_type, reason, previous_version, next_version,
    actor_type, actor_id, metadata, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			event.ID, nullString(event.SurfaceID), event.EventType, nullString(event.Reason),
			event.PreviousVersion, event.NextVersion, event.ActorType, nullString(event.ActorID),
			jsonOrDefault(event.MetadataJSON, "{}"), event.CreatedAt)
		return err
	}); err != nil {
		return ChannelSurfaceRecord{}, err
	}
	return surface, nil
}

func (s *Store) SupersedeChannelSurface(ctx context.Context, params SupersedeChannelSurfaceParams) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
UPDATE channel_surfaces
SET lifecycle_status='superseded',
    superseded_at=$2,
    updated_at=$2,
    version=version+1
WHERE id=$1 AND lifecycle_status='active' AND deleted_at IS NULL`,
			params.SurfaceID, params.SupersededAt); err != nil {
			return err
		}
		event := params.Event
		_, err := tx.ExecContext(ctx, `
INSERT INTO channel_surface_events (
    id, surface_id, event_type, reason, previous_version, next_version,
    actor_type, actor_id, metadata, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			event.ID, nullString(event.SurfaceID), event.EventType, nullString(event.Reason),
			event.PreviousVersion, event.NextVersion, event.ActorType, nullString(event.ActorID),
			jsonOrDefault(event.MetadataJSON, "{}"), event.CreatedAt)
		return err
	})
}

func (s *Store) ListChannelSurfaceEvents(ctx context.Context, surfaceID string, limit int) ([]ChannelSurfaceEventRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, COALESCE(surface_id::text,''), event_type, COALESCE(reason,''),
       previous_version, next_version, actor_type, COALESCE(actor_id,''),
       metadata, created_at
FROM channel_surface_events
WHERE surface_id=$1
ORDER BY created_at ASC
LIMIT $2`, surfaceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []ChannelSurfaceEventRecord
	for rows.Next() {
		var event ChannelSurfaceEventRecord
		if err := rows.Scan(
			&event.ID,
			&event.SurfaceID,
			&event.EventType,
			&event.Reason,
			&event.PreviousVersion,
			&event.NextVersion,
			&event.ActorType,
			&event.ActorID,
			&event.MetadataJSON,
			&event.CreatedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

type channelSurfaceScanner interface {
	Scan(dest ...any) error
}

func scanChannelSurface(scanner channelSurfaceScanner, surface *ChannelSurfaceRecord) error {
	return scanner.Scan(
		&surface.ID,
		&surface.ChannelAccountID,
		&surface.Channel,
		&surface.SurfaceType,
		&surface.SurfaceKey,
		&surface.AddressJSON,
		&surface.AddressFingerprint,
		&surface.DisplayStateJSON,
		&surface.LifecycleStatus,
		&surface.Version,
		&surface.IdempotencyKey,
		&surface.CreatedAt,
		&surface.UpdatedAt,
		&surface.LastRenderedAt,
		&surface.SupersededAt,
		&surface.DeletedAt,
	)
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func insertStoredObject(ctx context.Context, tx *sql.Tx, record StoredObjectRecord) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO stored_objects (
    id, bucket, object_key, content_type, size_bytes, checksum,
    storage_status, retention_state, created_at, expires_at, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (bucket, object_key) DO NOTHING`,
		record.ID, record.Bucket, record.ObjectKey, nullString(record.ContentType),
		record.SizeBytes, nullString(record.Checksum), withDefault(record.StorageStatus, "available"),
		withDefault(record.RetentionState, "active"), record.CreatedAt, record.ExpiresAt,
		record.DeletedAt)
	return err
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func jsonOrDefault(value []byte, fallback string) []byte {
	if len(value) == 0 {
		return []byte(fallback)
	}
	return value
}

func withDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func positiveVersion(value int64) int64 {
	if value < 1 {
		return 1
	}
	return value
}

func positiveInt(value int) int {
	if value < 1 {
		return 1
	}
	return value
}
