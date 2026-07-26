package target

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

var ErrProcessingRunIdempotencyConflict = errors.New("processing_run_idempotency_conflict")
var ErrExportJobRetryIdempotencyConflict = errors.New("export_job_retry_idempotency_conflict")

type Store struct {
	db *sql.DB
}

const storedObjectSelect = `
SELECT id, COALESCE(channel_account_id::text,''), bucket, object_key,
       COALESCE(staging_key,''), generation, generation_published_at,
       COALESCE(content_type,''), size_bytes, checksum_algorithm,
       COALESCE(checksum,''), storage_status, retention_state, hold_state,
       last_successful_use_at, created_at, expires_at, COALESCE(delete_owner,''),
       COALESCE(delete_token,''), delete_lease_expires_at, delete_attempts, deleted_at
FROM stored_objects`

func NewStore(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("target storage: db is required")
	}
	return &Store{db: db}, nil
}

func (s *Store) UpsertChannelAccount(ctx context.Context, record ChannelAccountRecord) (ChannelAccountRecord, error) {
	var account ChannelAccountRecord
	err := scanChannelAccount(s.db.QueryRowContext(ctx, `
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
    disabled_at=EXCLUDED.disabled_at
RETURNING id, channel, external_account_ref, COALESCE(display_name,''), status,
          metadata, created_at, updated_at, last_seen_at, disabled_at`,
		record.ID, record.Channel, record.ExternalAccountRef, nullString(record.DisplayName),
		withDefault(record.Status, "active"), jsonOrDefault(record.MetadataJSON, "{}"),
		record.CreatedAt, record.UpdatedAt, record.LastSeenAt, record.DisabledAt), &account)
	return account, err
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
			if params.StoredObject.ChannelAccountID == "" {
				params.StoredObject.ChannelAccountID = params.MediaAsset.ChannelAccountID
			}
			if err := insertStoredObject(ctx, tx, params.StoredObject); err != nil {
				return err
			}
			if params.StoredObject.StorageStatus == "available" {
				result, err := tx.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='available', staging_key=NULL,
    generation_published_at=$3, deleted_at=NULL,
    delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL
WHERE id=$1 AND (
    storage_status='available'
    OR (storage_status='publishing' AND staging_key=$2)
)`, params.StoredObject.ID, params.StoredObject.StagingKey, params.StoredObject.GenerationPublishedAt)
				if err != nil {
					return err
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					if err != nil {
						return err
					}
					return sql.ErrNoRows
				}
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
		if err != nil {
			return err
		}
		if params.Enrichment.ID != "" {
			_, err = insertMetadataEnrichment(ctx, tx, params.Enrichment)
		}
		return err
	})
}

const metadataEnrichmentSelect = `
SELECT id, media_asset_id, channel_account_id, provider, canonical_url, status, version,
       idempotency_key, attempt_no, max_attempts, COALESCE(attempt_token,''),
       COALESCE(lease_owner,''), lease_expires_at, heartbeat_at, next_attempt_at,
       progress, COALESCE(error_code,''), COALESCE(error_message,''), created_at,
       started_at, completed_at
FROM metadata_enrichment_jobs`

func insertMetadataEnrichment(ctx context.Context, tx *sql.Tx, enrichment MetadataEnrichmentRecord) (MetadataEnrichmentRecord, error) {
	result, err := tx.ExecContext(ctx, `
INSERT INTO metadata_enrichment_jobs (
    id, media_asset_id, channel_account_id, provider, canonical_url, status, version,
    idempotency_key, attempt_no, max_attempts, progress, created_at
)
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
FROM media_assets
WHERE id=$2 AND channel_account_id=$3 AND origin_type='url'
  AND status <> 'deleted' AND deleted_at IS NULL
ON CONFLICT DO NOTHING`, enrichment.ID, enrichment.MediaAssetID, enrichment.ChannelAccountID,
		withDefault(enrichment.Provider, "youtube"), enrichment.CanonicalURL,
		withDefault(enrichment.Status, "queued"), positiveVersion(enrichment.Version),
		enrichment.IdempotencyKey, enrichment.AttemptNo, positiveInt(enrichment.MaxAttempts),
		jsonOrDefault(enrichment.ProgressJSON, "{}"), enrichment.CreatedAt)
	if err != nil {
		return MetadataEnrichmentRecord{}, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return MetadataEnrichmentRecord{}, err
	} else if affected == 0 {
		row := tx.QueryRowContext(ctx, metadataEnrichmentSelect+`
WHERE channel_account_id=$1 AND (
    idempotency_key=$2 OR (media_asset_id=$3 AND status IN ('queued','claimed','running','retry_wait'))
)
ORDER BY (idempotency_key=$2) DESC, created_at DESC
LIMIT 1`, enrichment.ChannelAccountID, enrichment.IdempotencyKey, enrichment.MediaAssetID)
		return scanMetadataEnrichment(row)
	}
	return enrichment, nil
}

func (s *Store) CreateMetadataEnrichment(ctx context.Context, enrichment MetadataEnrichmentRecord) (MetadataEnrichmentRecord, error) {
	var record MetadataEnrichmentRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		record, err = insertMetadataEnrichment(ctx, tx, enrichment)
		return err
	})
	return record, err
}

func (s *Store) ListMetadataEnrichmentQueue(ctx context.Context, now time.Time, limit int) ([]MetadataEnrichmentRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, metadataEnrichmentSelect+`
WHERE status IN ('queued','retry_wait') AND attempt_no < max_attempts
  AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
ORDER BY COALESCE(next_attempt_at, created_at), created_at
LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]MetadataEnrichmentRecord, 0)
	for rows.Next() {
		record, err := scanMetadataEnrichment(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) GetMetadataEnrichmentByID(ctx context.Context, enrichmentID string) (MetadataEnrichmentRecord, error) {
	return scanMetadataEnrichment(s.db.QueryRowContext(ctx, metadataEnrichmentSelect+`
WHERE id=$1`, enrichmentID))
}

func (s *Store) ClaimMetadataEnrichment(ctx context.Context, params ClaimMetadataEnrichmentParams) (MetadataEnrichmentRecord, bool, error) {
	record, err := scanMetadataEnrichment(s.db.QueryRowContext(ctx, `
UPDATE metadata_enrichment_jobs
SET status='claimed', version=version+1, attempt_no=attempt_no+1,
    attempt_token=$2, lease_owner=$3, lease_expires_at=$4, heartbeat_at=$5,
    next_attempt_at=NULL, started_at=COALESCE(started_at,$5),
    progress='{"stage":"claimed"}'::jsonb, error_code=NULL, error_message=NULL
WHERE id=$1 AND status IN ('queued','retry_wait') AND attempt_no < max_attempts
  AND (next_attempt_at IS NULL OR next_attempt_at <= $5)
RETURNING id, media_asset_id, channel_account_id, provider, canonical_url, status, version,
          idempotency_key, attempt_no, max_attempts, COALESCE(attempt_token,''),
          COALESCE(lease_owner,''), lease_expires_at, heartbeat_at, next_attempt_at,
          progress, COALESCE(error_code,''), COALESCE(error_message,''), created_at,
          started_at, completed_at`, params.EnrichmentID, params.AttemptToken, params.LeaseOwner,
		params.LeaseExpiresAt, params.ClaimedAt))
	if errors.Is(err, sql.ErrNoRows) {
		return MetadataEnrichmentRecord{}, false, nil
	}
	return record, err == nil, err
}

func (s *Store) RecordMetadataEnrichmentProgress(ctx context.Context, params RecordMetadataEnrichmentProgressParams) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE metadata_enrichment_jobs
SET status='running', version=version+1, progress=$4,
    lease_expires_at=$5 + (lease_expires_at - heartbeat_at), heartbeat_at=$5
WHERE id=$1 AND lease_owner=$2 AND attempt_token=$3
  AND status IN ('claimed','running') AND lease_expires_at > $5`, params.EnrichmentID,
		params.LeaseOwner, params.AttemptToken, jsonOrDefault(params.ProgressJSON, "{}"), params.HeartbeatAt)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		if err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) FinalizeMetadataEnrichment(ctx context.Context, params FinalizeMetadataEnrichmentParams) (MetadataEnrichmentRecord, error) {
	var record MetadataEnrichmentRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
UPDATE metadata_enrichment_jobs
SET status=$4, version=version+1, progress=CASE WHEN $4='succeeded'
        THEN '{"stage":"succeeded","percent":100}'::jsonb ELSE progress END,
    attempt_token=NULL, lease_owner=NULL, lease_expires_at=NULL, heartbeat_at=NULL,
    next_attempt_at=$5, error_code=NULLIF($6,''), error_message=NULLIF($7,''),
    completed_at=CASE WHEN $4 IN ('succeeded','failed') THEN $8 ELSE NULL END
WHERE id=$1 AND lease_owner=$2 AND attempt_token=$3
  AND status IN ('claimed','running') AND lease_expires_at > $8
RETURNING id, media_asset_id, channel_account_id, provider, canonical_url, status, version,
          idempotency_key, attempt_no, max_attempts, COALESCE(attempt_token,''),
          COALESCE(lease_owner,''), lease_expires_at, heartbeat_at, next_attempt_at,
          progress, COALESCE(error_code,''), COALESCE(error_message,''), created_at,
          started_at, completed_at`, params.EnrichmentID, params.LeaseOwner, params.AttemptToken,
			params.Status, params.RetryAt, params.ErrorCode, params.ErrorMessage, params.CompletedAt)
		var err error
		record, err = scanMetadataEnrichment(row)
		if err != nil {
			return err
		}
		if params.Status == "succeeded" {
			result, err := tx.ExecContext(ctx, `
UPDATE media_assets
SET display_name=$2,
    metadata=COALESCE(metadata,'{}'::jsonb) || jsonb_build_object('provider_metadata',$3::jsonb),
    updated_at=$4
WHERE id=$1 AND channel_account_id=$5 AND status <> 'deleted' AND deleted_at IS NULL`,
				record.MediaAssetID, params.DisplayName,
				jsonOrDefault(params.ProviderMetadataJSON, "{}"), params.CompletedAt, record.ChannelAccountID)
			if err != nil {
				return err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				if err != nil {
					return err
				}
				return sql.ErrNoRows
			}
		}
		return nil
	})
	return record, err
}

func (s *Store) ReclaimMetadataEnrichments(ctx context.Context, now time.Time, limit int) (MetadataEnrichmentReclaimResult, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
WITH stale AS (
    SELECT id FROM metadata_enrichment_jobs
    WHERE status IN ('claimed','running') AND lease_expires_at <= $1
    ORDER BY lease_expires_at
    FOR UPDATE SKIP LOCKED
    LIMIT $2
), updated AS (
    UPDATE metadata_enrichment_jobs job
    SET status=CASE WHEN job.attempt_no >= job.max_attempts THEN 'failed' ELSE 'retry_wait' END,
        version=job.version+1, attempt_token=NULL, lease_owner=NULL,
        lease_expires_at=NULL, heartbeat_at=NULL,
        next_attempt_at=CASE WHEN job.attempt_no >= job.max_attempts THEN NULL
            ELSE $1 + make_interval(secs => LEAST(300, 5 * (1 << LEAST(job.attempt_no - 1, 6)))) END,
        error_code='metadata_enrichment_lease_expired',
        error_message='worker lease expired',
        completed_at=CASE WHEN job.attempt_no >= job.max_attempts THEN $1 ELSE NULL END
    FROM stale WHERE job.id=stale.id
    RETURNING job.status
)
SELECT status, count(*) FROM updated GROUP BY status`, now, limit)
	if err != nil {
		return MetadataEnrichmentReclaimResult{}, err
	}
	defer rows.Close()
	var result MetadataEnrichmentReclaimResult
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return MetadataEnrichmentReclaimResult{}, err
		}
		result.Examined += count
		if status == "failed" {
			result.Failed += count
		} else {
			result.Requeued += count
		}
	}
	return result, rows.Err()
}

func (s *Store) PrepareStoredObjectPublication(ctx context.Context, candidate StoredObjectRecord) (PrepareStoredObjectPublicationResult, error) {
	var result PrepareStoredObjectPublicationResult
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := ensureObjectLocationsWritable(ctx, tx, candidate.Bucket, candidate.ObjectKey, candidate.StagingKey); err != nil {
			return err
		}
		inserted := StoredObjectRecord{}
		row := tx.QueryRowContext(ctx, `
INSERT INTO stored_objects (
    id, channel_account_id, bucket, object_key, staging_key, generation,
    generation_published_at, content_type, size_bytes, checksum_algorithm,
    checksum, storage_status, retention_state, hold_state, created_at, expires_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'publishing',$12,$13,$14,$15)
ON CONFLICT (channel_account_id, checksum, size_bytes)
WHERE channel_account_id IS NOT NULL AND checksum IS NOT NULL AND checksum <> ''
DO NOTHING
RETURNING id, COALESCE(channel_account_id::text,''), bucket, object_key,
          COALESCE(staging_key,''), generation, generation_published_at,
          COALESCE(content_type,''), size_bytes, checksum_algorithm,
          COALESCE(checksum,''), storage_status, retention_state, hold_state,
          last_successful_use_at, created_at, expires_at,
          COALESCE(delete_owner,''), COALESCE(delete_token,''),
          delete_lease_expires_at, delete_attempts, deleted_at`,
			candidate.ID, candidate.ChannelAccountID, candidate.Bucket, candidate.ObjectKey,
			candidate.StagingKey, positiveInt(candidate.Generation), candidate.GenerationPublishedAt,
			nullString(candidate.ContentType), candidate.SizeBytes,
			withDefault(candidate.ChecksumAlgorithm, "sha256"), nullString(candidate.Checksum),
			withDefault(candidate.RetentionState, "active"), withDefault(candidate.HoldState, "none"),
			candidate.CreatedAt, candidate.ExpiresAt)
		if err := scanStoredObject(row, &inserted); err == nil {
			result = PrepareStoredObjectPublicationResult{StoredObject: inserted, Publisher: true}
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		var existing StoredObjectRecord
		if err := scanStoredObject(tx.QueryRowContext(ctx, storedObjectSelect+`
WHERE channel_account_id=$1 AND checksum=$2 AND size_bytes=$3
FOR UPDATE`, candidate.ChannelAccountID, candidate.Checksum, candidate.SizeBytes), &existing); err != nil {
			return err
		}
		if existing.StorageStatus == "deleted" || existing.StorageStatus == "missing" {
			nextObjectKey := fmt.Sprintf("sources/uploads/%s/%d/source", existing.ID, existing.Generation+1)
			if err := ensureObjectLocationsWritable(ctx, tx, candidate.Bucket, nextObjectKey, candidate.StagingKey); err != nil {
				return err
			}
			if err := scanStoredObject(tx.QueryRowContext(ctx, `
UPDATE stored_objects
SET bucket=$2, object_key=$3, staging_key=$4, generation=generation+1,
    generation_published_at=$5, content_type=$6, size_bytes=$7,
    checksum_algorithm=$8, checksum=$9, storage_status='publishing',
    retention_state=$10, hold_state='none', expires_at=$11,
    delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL, deleted_at=NULL
WHERE id=$1 AND storage_status IN ('deleted','missing')
RETURNING id, COALESCE(channel_account_id::text,''), bucket, object_key,
          COALESCE(staging_key,''), generation, generation_published_at,
          COALESCE(content_type,''), size_bytes, checksum_algorithm,
          COALESCE(checksum,''), storage_status, retention_state, hold_state,
          last_successful_use_at, created_at, expires_at,
          COALESCE(delete_owner,''), COALESCE(delete_token,''),
          delete_lease_expires_at, delete_attempts, deleted_at`,
				existing.ID, candidate.Bucket, nextObjectKey, candidate.StagingKey,
				candidate.GenerationPublishedAt, nullString(candidate.ContentType), candidate.SizeBytes,
				withDefault(candidate.ChecksumAlgorithm, "sha256"), nullString(candidate.Checksum),
				withDefault(candidate.RetentionState, "active"), candidate.ExpiresAt), &existing); err != nil {
				return err
			}
			result = PrepareStoredObjectPublicationResult{StoredObject: existing, Publisher: true}
			return nil
		}
		if existing.StorageStatus == "available" {
			if err := scanStoredObject(tx.QueryRowContext(ctx, `
UPDATE stored_objects
SET expires_at=CASE
        WHEN expires_at IS NULL THEN NULL
        WHEN $2::timestamptz IS NULL THEN expires_at
        ELSE GREATEST(expires_at,$2)
    END
WHERE id=$1 AND storage_status='available' AND deleted_at IS NULL
RETURNING id, COALESCE(channel_account_id::text,''), bucket, object_key,
          COALESCE(staging_key,''), generation, generation_published_at,
          COALESCE(content_type,''), size_bytes, checksum_algorithm,
          COALESCE(checksum,''), storage_status, retention_state, hold_state,
          last_successful_use_at, created_at, expires_at,
          COALESCE(delete_owner,''), COALESCE(delete_token,''),
          delete_lease_expires_at, delete_attempts, deleted_at`, existing.ID, candidate.ExpiresAt), &existing); err != nil {
				return err
			}
		}
		result = PrepareStoredObjectPublicationResult{StoredObject: existing, Publisher: false}
		return nil
	})
	return result, err
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
	row := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), bucket, object_key,
       COALESCE(staging_key,''), generation, generation_published_at,
       COALESCE(content_type,''), size_bytes, checksum_algorithm,
       COALESCE(checksum,''), storage_status, retention_state, hold_state,
       last_successful_use_at, created_at, expires_at, COALESCE(delete_owner,''),
       COALESCE(delete_token,''), delete_lease_expires_at, delete_attempts, deleted_at
FROM stored_objects
WHERE id=COALESCE(
    (SELECT canonical_stored_object_id FROM stored_object_aliases WHERE alias_id=$1),
    $1::uuid
)`, storedObjectID)
	err := scanStoredObject(row, &object)
	return object, err
}

func (s *Store) FindStoredObjectByDigest(ctx context.Context, channelAccountID, checksum string) (StoredObjectRecord, error) {
	var object StoredObjectRecord
	row := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), bucket, object_key,
       COALESCE(staging_key,''), generation, generation_published_at,
       COALESCE(content_type,''), size_bytes, checksum_algorithm,
       COALESCE(checksum,''), storage_status, retention_state, hold_state,
       last_successful_use_at, created_at, expires_at, COALESCE(delete_owner,''),
       COALESCE(delete_token,''), delete_lease_expires_at, delete_attempts, deleted_at
FROM stored_objects
WHERE channel_account_id=$1 AND checksum=$2
ORDER BY generation DESC
LIMIT 1`, channelAccountID, checksum)
	err := scanStoredObject(row, &object)
	return object, err
}

func (s *Store) FindStoredObjectByLocation(ctx context.Context, bucket, objectKey string) (StoredObjectRecord, error) {
	var object StoredObjectRecord
	err := scanStoredObject(s.db.QueryRowContext(ctx, storedObjectSelect+`
WHERE bucket=$1 AND (object_key=$2 OR staging_key=$2)
ORDER BY CASE WHEN object_key=$2 THEN 0 ELSE 1 END
LIMIT 1`, bucket, objectKey), &object)
	return object, err
}

func (s *Store) ListStoredObjectsForReconcile(ctx context.Context, afterID string, limit int) ([]StoredObjectRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, storedObjectSelect+`
WHERE storage_status IN ('publishing','available','delete_scheduled')
  AND id > COALESCE(NULLIF($1,'')::uuid, '00000000-0000-0000-0000-000000000000'::uuid)
ORDER BY id ASC
LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := make([]StoredObjectRecord, 0, limit)
	for rows.Next() {
		var object StoredObjectRecord
		if err := scanStoredObject(rows, &object); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, rows.Err()
}

func (s *Store) CompleteStoredObjectPublication(ctx context.Context, storedObjectID string, generation int, stagingKey string, publishedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='available', staging_key=NULL, generation_published_at=$4,
    deleted_at=NULL, delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL
WHERE id=$1 AND generation=$2 AND storage_status='publishing' AND staging_key=$3`,
		storedObjectID, generation, stagingKey, publishedAt)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) MarkStoredObjectMissing(ctx context.Context, storedObjectID string, generation int, markedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='missing', retention_state='expired', staging_key=NULL,
    delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL, deleted_at=$3
WHERE id=$1 AND generation=$2 AND storage_status IN ('publishing','available')`,
		storedObjectID, generation, markedAt)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) ClaimObjectDeleteFence(ctx context.Context, bucket, objectKey, token string, now, leaseExpiresAt time.Time) (bool, error) {
	claimed := false
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := lockObjectLocation(ctx, tx, bucket, objectKey); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM object_delete_fences WHERE bucket=$1 AND object_key=$2 AND lease_expires_at <= $3`, bucket, objectKey, now); err != nil {
			return err
		}
		var referenced bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM stored_objects
    WHERE bucket=$1 AND (object_key=$2 OR staging_key=$2)
      AND storage_status NOT IN ('missing','deleted')
)`, bucket, objectKey).Scan(&referenced); err != nil {
			return err
		}
		if referenced {
			return nil
		}
		result, err := tx.ExecContext(ctx, `
INSERT INTO object_delete_fences (bucket, object_key, token, lease_expires_at, created_at)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (bucket, object_key) DO NOTHING`, bucket, objectKey, token, leaseExpiresAt, now)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		claimed = err == nil && rows == 1
		return err
	})
	return claimed, err
}

func (s *Store) ReleaseObjectDeleteFence(ctx context.Context, bucket, objectKey, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM object_delete_fences WHERE bucket=$1 AND object_key=$2 AND token=$3`, bucket, objectKey, token)
	return err
}

func (s *Store) GetReconcileCursor(ctx context.Context, name string) (string, error) {
	var cursor string
	err := s.db.QueryRowContext(ctx, `SELECT cursor FROM storage_reconcile_cursors WHERE name=$1`, name).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return cursor, err
}

func (s *Store) SetReconcileCursor(ctx context.Context, name, cursor string, updatedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO storage_reconcile_cursors (name, cursor, updated_at)
VALUES ($1,$2,$3)
ON CONFLICT (name) DO UPDATE SET cursor=EXCLUDED.cursor, updated_at=EXCLUDED.updated_at`, name, cursor, updatedAt)
	return err
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
		return insertSelectionSnapshotTx(ctx, tx, snapshot, items)
	})
}

func insertSelectionSnapshotTx(ctx context.Context, tx *sql.Tx, snapshot SelectionSnapshotRecord, items []SelectionSnapshotItemRecord) error {
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
}

func (s *Store) CreateAnalysisRunGraph(ctx context.Context, graph AnalysisRunGraph) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, pin := range graph.SourcePins {
			if err := lockAvailableStoredObject(ctx, tx, pin.StoredObjectID); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `
INSERT INTO stored_object_pins (
    id, stored_object_id, owner_type, owner_id, purpose, expires_at, created_at, released_at
)
SELECT $1,$2,'analysis_run',$3,'source',NULL,$4,NULL
FROM stored_objects so
WHERE so.id=$2 AND so.storage_status='available' AND so.deleted_at IS NULL
  AND EXISTS (
      SELECT 1
      FROM selection_snapshot_items item
      WHERE item.selection_snapshot_id=$5
        AND item.storage_snapshot->>'stored_object_id'=so.id::text
  )`, pin.ID, pin.StoredObjectID, graph.Run.ID, pin.CreatedAt, graph.Run.SelectionSnapshot)
			if err != nil {
				return err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				if err != nil {
					return err
				}
				return sql.ErrNoRows
			}
		}
		return insertAnalysisRunGraphTx(ctx, tx, graph)
	})
}

func insertAnalysisRunGraphTx(ctx context.Context, tx *sql.Tx, graph AnalysisRunGraph) error {
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
}

func (s *Store) CreateProcessingRun(ctx context.Context, params CreateProcessingRunParams) (CreateProcessingRunResult, error) {
	var createdResult CreateProcessingRunResult
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if params.Graph.Run.IdempotencyKey != "" {
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, params.ChannelAccountID+":processing_run:"+params.Graph.Run.IdempotencyKey); err != nil {
				return err
			}
			replay, found, err := findProcessingRunReplay(ctx, tx, params)
			if err != nil {
				return err
			}
			if found {
				createdResult = replay
				return nil
			}
		}
		var version int64
		if err := tx.QueryRowContext(ctx, `
SELECT version
FROM collections
WHERE id=$1 AND channel_account_id=$2 AND status='active' AND deleted_at IS NULL
FOR UPDATE`, params.CollectionID, params.ChannelAccountID).Scan(&version); err != nil {
			return err
		}
		if version != params.ExpectedVersion || len(params.CapturedAssetIDs) == 0 {
			return sql.ErrNoRows
		}
		seen := make(map[string]struct{}, len(params.CapturedAssetIDs))
		for _, mediaAssetID := range params.CapturedAssetIDs {
			if _, duplicate := seen[mediaAssetID]; duplicate {
				return sql.ErrNoRows
			}
			seen[mediaAssetID] = struct{}{}
			var present bool
			if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM collection_items ci
    JOIN media_assets ma ON ma.id=ci.media_asset_id
    WHERE ci.collection_id=$1 AND ci.media_asset_id=$2 AND ci.removed_at IS NULL
      AND ma.channel_account_id=$3 AND ma.status='available' AND ma.deleted_at IS NULL
)`, params.CollectionID, mediaAssetID, params.ChannelAccountID).Scan(&present); err != nil {
				return err
			}
			if !present {
				return sql.ErrNoRows
			}
		}
		for _, pin := range params.SourcePins {
			if err := lockAvailableStoredObject(ctx, tx, pin.StoredObjectID); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `
INSERT INTO stored_object_pins (
    id, stored_object_id, owner_type, owner_id, purpose, expires_at, created_at, released_at
)
SELECT $1,$2,'analysis_run',$3,'source',NULL,$4,NULL
FROM stored_objects so
WHERE so.id=$2 AND so.storage_status='available' AND so.deleted_at IS NULL
  AND EXISTS (
      SELECT 1 FROM media_assets ma
      WHERE ma.stored_object_id=so.id AND ma.channel_account_id=$5
        AND ma.id::text=ANY($6)
  )`, pin.ID, pin.StoredObjectID, params.Graph.Run.ID, pin.CreatedAt,
				params.ChannelAccountID, params.CapturedAssetIDs)
			if err != nil {
				return err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				if err != nil {
					return err
				}
				return sql.ErrNoRows
			}
		}
		if err := insertSelectionSnapshotTx(ctx, tx, params.Snapshot, params.SnapshotItems); err != nil {
			return err
		}
		if err := insertAnalysisRunGraphTx(ctx, tx, params.Graph); err != nil {
			return err
		}
		for _, mediaAssetID := range params.CapturedAssetIDs {
			result, err := tx.ExecContext(ctx, `
UPDATE collection_items
SET removed_at=$3
WHERE collection_id=$1 AND media_asset_id=$2 AND removed_at IS NULL`, params.CollectionID, mediaAssetID, params.DetachedAt)
			if err != nil {
				return err
			}
			if affected, err := result.RowsAffected(); err != nil || affected != 1 {
				if err != nil {
					return err
				}
				return sql.ErrNoRows
			}
		}
		collectionUpdate, err := tx.ExecContext(ctx, `
UPDATE collections
SET version=version+1, updated_at=$4
WHERE id=$1 AND channel_account_id=$2 AND version=$3`, params.CollectionID, params.ChannelAccountID, params.ExpectedVersion, params.DetachedAt)
		if err != nil {
			return err
		}
		affected, err := collectionUpdate.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return sql.ErrNoRows
		}
		createdResult = CreateProcessingRunResult{
			Snapshot: params.Snapshot, SnapshotItems: append([]SelectionSnapshotItemRecord(nil), params.SnapshotItems...),
			Run: params.Graph.Run, DetachedAssetIDs: append([]string(nil), params.CapturedAssetIDs...),
			CollectionVersion: params.ExpectedVersion + 1,
		}
		return nil
	})
	return createdResult, err
}

func findProcessingRunReplay(ctx context.Context, tx *sql.Tx, params CreateProcessingRunParams) (CreateProcessingRunResult, bool, error) {
	var run AnalysisRunRecord
	err := scanAnalysisRun(tx.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
       status, version, COALESCE(idempotency_key,''), params, delivery,
       evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
       created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at
FROM analysis_runs
WHERE channel_account_id=$1 AND idempotency_key=$2`, params.ChannelAccountID, params.Graph.Run.IdempotencyKey), &run)
	if errors.Is(err, sql.ErrNoRows) {
		return CreateProcessingRunResult{}, false, nil
	}
	if err != nil {
		return CreateProcessingRunResult{}, false, err
	}
	var snapshot SelectionSnapshotRecord
	if err := tx.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), COALESCE(source_collection_id::text,''),
       status, option_snapshot, diagnostics, COALESCE(created_via_channel_account_id::text,''),
       created_at, sealed_at
FROM selection_snapshots
WHERE id=$1 AND channel_account_id=$2`, run.SelectionSnapshot, params.ChannelAccountID).Scan(
		&snapshot.ID, &snapshot.ChannelAccountID, &snapshot.SourceCollectionID, &snapshot.Status,
		&snapshot.OptionSnapshotJSON, &snapshot.DiagnosticsJSON, &snapshot.CreatedViaChannel,
		&snapshot.CreatedAt, &snapshot.SealedAt,
	); err != nil {
		return CreateProcessingRunResult{}, false, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT id, selection_snapshot_id, position, COALESCE(media_asset_id::text,''), kind,
       display_name, origin_snapshot, storage_snapshot, metadata_snapshot,
       status_at_selection, diagnostics
FROM selection_snapshot_items
WHERE selection_snapshot_id=$1
ORDER BY position ASC`, snapshot.ID)
	if err != nil {
		return CreateProcessingRunResult{}, false, err
	}
	defer rows.Close()
	items := make([]SelectionSnapshotItemRecord, 0)
	for rows.Next() {
		var item SelectionSnapshotItemRecord
		if err := rows.Scan(
			&item.ID, &item.SelectionSnapshotID, &item.Position, &item.MediaAssetID,
			&item.Kind, &item.DisplayName, &item.OriginSnapshotJSON, &item.StorageSnapshotJSON,
			&item.MetadataJSON, &item.StatusAtSelection, &item.DiagnosticsJSON,
		); err != nil {
			return CreateProcessingRunResult{}, false, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return CreateProcessingRunResult{}, false, err
	}
	if !processingRunRequestMatches(params, run, snapshot, items) {
		return CreateProcessingRunResult{}, false, ErrProcessingRunIdempotencyConflict
	}
	var collectionVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT version FROM collections WHERE id=$1 AND channel_account_id=$2`, params.CollectionID, params.ChannelAccountID).Scan(&collectionVersion); err != nil {
		return CreateProcessingRunResult{}, false, err
	}
	detached := make([]string, 0, len(items))
	for _, item := range items {
		detached = append(detached, item.MediaAssetID)
	}
	return CreateProcessingRunResult{
		Snapshot: snapshot, SnapshotItems: items, Run: run, DetachedAssetIDs: detached,
		CollectionVersion: collectionVersion, Replayed: true,
	}, true, nil
}

func processingRunRequestMatches(params CreateProcessingRunParams, run AnalysisRunRecord, snapshot SelectionSnapshotRecord, items []SelectionSnapshotItemRecord) bool {
	if run.RunType != params.Graph.Run.RunType || run.CreatedViaChannel != params.Graph.Run.CreatedViaChannel ||
		snapshot.SourceCollectionID != params.CollectionID || snapshot.CreatedViaChannel != params.Snapshot.CreatedViaChannel ||
		!jsonValuesEqual(run.ParamsJSON, params.Graph.Run.ParamsJSON) ||
		!jsonValuesEqual(run.DeliveryJSON, params.Graph.Run.DeliveryJSON) ||
		!jsonValuesEqual(snapshot.OptionSnapshotJSON, params.Snapshot.OptionSnapshotJSON) ||
		len(items) != len(params.SnapshotItems) {
		return false
	}
	expected := append([]SelectionSnapshotItemRecord(nil), params.SnapshotItems...)
	sort.Slice(expected, func(i, j int) bool { return expected[i].Position < expected[j].Position })
	for index := range items {
		if items[index].Position != expected[index].Position || items[index].MediaAssetID != expected[index].MediaAssetID {
			return false
		}
	}
	return true
}

func jsonValuesEqual(left, right []byte) bool {
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func (s *Store) ClaimAnalysisRunStep(ctx context.Context, analysisRunID, workerKind, stepKind, leaseOwner string, claimedAt time.Time) (AnalysisRunStepRecord, []AnalysisRunStepInputRecord, bool, error) {
	var step AnalysisRunStepRecord
	claimed := false
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var runStatus string
		if err := tx.QueryRowContext(ctx, `
SELECT status
FROM analysis_runs
WHERE id=$1
FOR UPDATE`, analysisRunID).Scan(&runStatus); err != nil {
			return err
		}
		if runStatus != "queued" && runStatus != "running" {
			return nil
		}
		err := tx.QueryRowContext(ctx, `
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
			return nil
		}
		if err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		return AnalysisRunStepRecord{}, nil, false, err
	}
	if !claimed {
		return AnalysisRunStepRecord{}, nil, false, nil
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

func (s *Store) FindReusableTranscriptBySource(ctx context.Context, channelAccountID, storedObjectID, checksum string) (AnalysisRunRecord, ArtifactRecord, error) {
	var run AnalysisRunRecord
	var artifact ArtifactRecord
	err := s.db.QueryRowContext(ctx, `
SELECT r.id, COALESCE(r.channel_account_id::text,''), r.selection_snapshot_id, r.run_type,
       r.status, r.version, COALESCE(r.idempotency_key,''), r.params, r.delivery,
       r.evidence_gate_state, COALESCE(r.created_via_channel_account_id::text,''),
       r.created_at, r.started_at, r.completed_at, r.cancel_requested_at, r.canceled_at, r.expires_at,
       a.id, COALESCE(a.channel_account_id::text,''), a.analysis_run_id,
       COALESCE(a.stored_object_id::text,''), a.kind, a.status, a.content_type,
       COALESCE(a.checksum,''), a.size_bytes, a.visibility, a.preview,
       a.created_at, a.expires_at, a.deleted_at
FROM analysis_runs r
JOIN selection_snapshot_items ssi ON ssi.selection_snapshot_id=r.selection_snapshot_id
JOIN artifacts a ON a.analysis_run_id=r.id
WHERE r.channel_account_id=$1
  AND r.run_type='transcription'
  AND r.status IN ('succeeded', 'partially_succeeded')
  AND a.kind='transcript'
  AND a.status='available'
  AND a.visibility='channel_deliverable'
  AND a.deleted_at IS NULL
  AND ($2<>'' OR $3<>'')
  AND ($2='' OR ssi.storage_snapshot->>'stored_object_id'=$2)
  AND ($3='' OR ssi.storage_snapshot->>'checksum'=$3)
  AND NOT EXISTS (
      SELECT 1
      FROM selection_snapshot_items other
      WHERE other.selection_snapshot_id=r.selection_snapshot_id
        AND other.id<>ssi.id
  )
ORDER BY COALESCE(r.completed_at, r.created_at) DESC,
         CASE
             WHEN lower(a.content_type) LIKE 'text/plain%' THEN 0
             WHEN lower(a.content_type) LIKE 'text/markdown%' THEN 1
             WHEN lower(a.content_type)='application/vnd.openxmlformats-officedocument.wordprocessingml.document' THEN 2
             ELSE 3
         END ASC,
         a.created_at DESC
LIMIT 1`, channelAccountID, storedObjectID, checksum).Scan(
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
	return run, artifact, err
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

func (s *Store) GetArtifactByID(ctx context.Context, artifactID string) (ArtifactRecord, error) {
	var artifact ArtifactRecord
	err := s.db.QueryRowContext(ctx, `
SELECT id, COALESCE(channel_account_id::text,''), analysis_run_id,
       COALESCE(stored_object_id::text,''), kind, status, content_type,
       COALESCE(checksum,''), size_bytes, visibility, preview,
       created_at, expires_at, deleted_at
FROM artifacts
WHERE id=$1`, artifactID).Scan(
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
		var currentStatus string
		var hasActiveStep bool
		if err := tx.QueryRowContext(ctx, `
SELECT r.status,
       EXISTS (
           SELECT 1
           FROM analysis_run_steps step
           WHERE step.analysis_run_id=r.id
             AND step.status IN ('claimed','running')
       )
FROM analysis_runs r
WHERE r.id=$1 AND r.channel_account_id=$2
FOR UPDATE`, analysisRunID, channelAccountID).Scan(&currentStatus, &hasActiveStep); err != nil {
			return err
		}
		terminal := currentStatus == "succeeded" || currentStatus == "failed" || currentStatus == "partially_succeeded" || currentStatus == "canceled" || currentStatus == "expired"
		nextStatus := "cancel_requested"
		if terminal {
			nextStatus = currentStatus
		} else if !hasActiveStep {
			nextStatus = "canceled"
		}
		err := tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=$3,
    cancel_requested_at=COALESCE(cancel_requested_at, $4),
    canceled_at=CASE WHEN $3='canceled' THEN COALESCE(canceled_at,$4) ELSE canceled_at END,
    completed_at=CASE WHEN $3='canceled' THEN COALESCE(completed_at,$4) ELSE completed_at END,
    version=version+1
WHERE id=$1 AND channel_account_id=$2
RETURNING id, COALESCE(channel_account_id::text,''), selection_snapshot_id, run_type,
          status, version, COALESCE(idempotency_key,''), params, delivery,
          evidence_gate_state, COALESCE(created_via_channel_account_id::text,''),
          created_at, started_at, completed_at, cancel_requested_at, canceled_at, expires_at`,
			analysisRunID, channelAccountID, nextStatus, requestedAt).Scan(
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
		if !terminal {
			if _, err := tx.ExecContext(ctx, `
UPDATE analysis_run_steps
SET status='canceled', finalized_at=COALESCE(finalized_at,$2), heartbeat_at=COALESCE(heartbeat_at,$2)
WHERE analysis_run_id=$1 AND status IN ('pending','queued')`, analysisRunID, requestedAt); err != nil {
				return err
			}
		}
		if nextStatus == "canceled" {
			if _, err := tx.ExecContext(ctx, `
UPDATE stored_object_pins
SET released_at=COALESCE(released_at,$2)
WHERE owner_type='analysis_run' AND owner_id=$1 AND purpose='source'`, analysisRunID, requestedAt); err != nil {
				return err
			}
		}
		event.Version = run.Version
		event.Status = run.Status
		if nextStatus == "canceled" {
			event.EventType = "analysis_run.canceled"
		}
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
		if runStatus == "succeeded" && params.RetentionDays > 0 {
			if _, err := tx.ExecContext(ctx, `
UPDATE stored_objects so
SET last_successful_use_at=GREATEST(COALESCE(so.last_successful_use_at,$2),$2),
    expires_at=GREATEST(so.generation_published_at,$2) + make_interval(days => $3)
FROM stored_object_pins pin
WHERE pin.owner_type='analysis_run' AND pin.owner_id=$1
  AND pin.purpose='source' AND pin.stored_object_id=so.id`,
				params.AnalysisRunID, params.FinalizedAt, params.RetentionDays); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE stored_object_pins
SET released_at=COALESCE(released_at,$2)
WHERE owner_type='analysis_run' AND owner_id=$1 AND purpose='source'`,
			params.AnalysisRunID, params.FinalizedAt); err != nil {
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
		objectChannels := make(map[string]string, len(artifacts))
		for _, artifact := range artifacts {
			if artifact.StoredObjectID != "" && artifact.ChannelAccountID != "" {
				objectChannels[artifact.StoredObjectID] = artifact.ChannelAccountID
			}
		}
		for _, object := range storedObjects {
			if object.ChannelAccountID == "" {
				object.ChannelAccountID = objectChannels[object.ID]
			}
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
		if surface.AddressFingerprint != "" {
			if _, err := tx.ExecContext(ctx, `
UPDATE channel_surfaces
SET lifecycle_status='superseded',
    superseded_at=$6,
    updated_at=$6,
    version=version+1
WHERE channel_account_id=$1
  AND channel=$2
  AND address_fingerprint=$3
  AND lifecycle_status='active'
  AND deleted_at IS NULL
  AND NOT (surface_type=$4 AND surface_key=$5)`,
				surface.ChannelAccountID, surface.Channel, surface.AddressFingerprint,
				surface.SurfaceType, surface.SurfaceKey, surface.UpdatedAt); err != nil {
				return err
			}
		}
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

func (s *Store) CreateExportJob(ctx context.Context, params CreateExportJobParams) (ExportJobRecord, error) {
	job := params.Job
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if params.SourcePin.ID != "" {
			if err := lockAvailableStoredObject(ctx, tx, params.SourcePin.StoredObjectID); err != nil {
				return err
			}
		}
		result, err := tx.ExecContext(ctx, `
INSERT INTO export_jobs (
    id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version,
    idempotency_key, retry_generation, attempt_no, attempt_token, lease_owner,
    lease_expires_at, heartbeat_at, max_attempts, progress, output_stored_object_id,
    diagnostic_id, created_at, started_at, completed_at, cancel_requested_at,
    canceled_at, expires_at
)
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25
FROM media_assets
WHERE id=$3 AND channel_account_id=$2 AND status='available' AND deleted_at IS NULL
ON CONFLICT (channel_account_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
			job.ID, job.ChannelAccountID, job.MediaAssetID, job.Operation,
			withDefault(job.DeliveryChannel, "telegram"), jsonOrDefault(job.VariantJSON, "{}"), withDefault(job.Status, "queued"),
			positiveVersion(job.Version), nullString(job.IdempotencyKey), job.RetryGeneration,
			job.AttemptNo, nullString(job.AttemptToken), nullString(job.LeaseOwner),
			job.LeaseExpiresAt, job.HeartbeatAt, positiveInt(job.MaxAttempts),
			jsonOrDefault(job.ProgressJSON, "{}"), nullString(job.OutputStoredObjectID),
			nullString(job.DiagnosticID), job.CreatedAt, job.StartedAt, job.CompletedAt,
			job.CancelRequestedAt, job.CanceledAt, job.ExpiresAt)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			if job.IdempotencyKey == "" {
				return sql.ErrNoRows
			}
			row := tx.QueryRowContext(ctx, exportJobSelect+`
WHERE channel_account_id=$1 AND idempotency_key=$2`, job.ChannelAccountID, job.IdempotencyKey)
			return scanExportJob(row, &job)
		}
		if params.SourcePin.ID != "" {
			pinResult, err := tx.ExecContext(ctx, `
INSERT INTO stored_object_pins (
    id, stored_object_id, owner_type, owner_id, purpose, expires_at, created_at, released_at
)
SELECT $1,$2,$3,$4,$5,$6,$7,$8
FROM stored_objects
WHERE id=$2 AND storage_status='available' AND deleted_at IS NULL`,
				params.SourcePin.ID, params.SourcePin.StoredObjectID, params.SourcePin.OwnerType,
				params.SourcePin.OwnerID, params.SourcePin.Purpose, params.SourcePin.ExpiresAt,
				params.SourcePin.CreatedAt, params.SourcePin.ReleasedAt)
			if err != nil {
				return err
			}
			if rows, err := pinResult.RowsAffected(); err != nil || rows != 1 {
				if err != nil {
					return err
				}
				return sql.ErrNoRows
			}
		}
		return nil
	})
	return job, err
}

const exportJobSelect = `
SELECT id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version,
       COALESCE(idempotency_key,''), retry_generation, attempt_no,
       COALESCE(attempt_token,''), COALESCE(lease_owner,''), lease_expires_at,
       heartbeat_at, max_attempts, progress, COALESCE(output_stored_object_id::text,''),
       COALESCE(diagnostic_id::text,''), created_at, started_at, completed_at,
       cancel_requested_at, canceled_at, expires_at
FROM export_jobs`

func (s *Store) GetExportJob(ctx context.Context, channelAccountID, exportJobID string) (ExportJobRecord, error) {
	var job ExportJobRecord
	err := scanExportJob(s.db.QueryRowContext(ctx, exportJobSelect+`
WHERE id=$1 AND channel_account_id=$2`, exportJobID, channelAccountID), &job)
	return job, err
}

func (s *Store) GetExportJobByID(ctx context.Context, exportJobID string) (ExportJobRecord, error) {
	var job ExportJobRecord
	err := scanExportJob(s.db.QueryRowContext(ctx, exportJobSelect+`
WHERE id=$1`, exportJobID), &job)
	return job, err
}

func (s *Store) ListExportJobs(ctx context.Context, channelAccountID, status string, limit int) ([]ExportJobRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, exportJobSelect+`
WHERE channel_account_id=$1 AND ($2='' OR status=$2)
ORDER BY created_at DESC
LIMIT $3`, channelAccountID, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]ExportJobRecord, 0)
	for rows.Next() {
		var job ExportJobRecord
		if err := scanExportJob(rows, &job); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ListExportJobQueue(ctx context.Context, limit int) ([]ExportJobRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, exportJobSelect+`
WHERE status='queued' AND attempt_no < max_attempts
ORDER BY created_at ASC
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]ExportJobRecord, 0)
	for rows.Next() {
		var job ExportJobRecord
		if err := scanExportJob(rows, &job); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ClaimExportJob(ctx context.Context, params ClaimExportJobParams) (ExportJobRecord, bool, error) {
	var job ExportJobRecord
	err := scanExportJob(s.db.QueryRowContext(ctx, `
UPDATE export_jobs
SET status='claimed', version=version+1, attempt_no=attempt_no+1,
    attempt_token=$2, lease_owner=$3, lease_expires_at=$4, heartbeat_at=$5,
    started_at=COALESCE(started_at,$5)
WHERE id=$1 AND status='queued' AND attempt_no < max_attempts
  AND (
      NOT EXISTS (
          SELECT 1 FROM media_assets ma
          WHERE ma.id=export_jobs.media_asset_id AND ma.stored_object_id IS NOT NULL
      )
      OR EXISTS (
          SELECT 1 FROM media_assets ma
          JOIN stored_object_pins p ON p.stored_object_id=ma.stored_object_id
          WHERE ma.id=export_jobs.media_asset_id
            AND p.owner_type='export_job' AND p.owner_id=export_jobs.id
            AND p.purpose='source' AND p.released_at IS NULL
      )
  )
RETURNING id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version,
          COALESCE(idempotency_key,''), retry_generation, attempt_no,
          COALESCE(attempt_token,''), COALESCE(lease_owner,''), lease_expires_at,
          heartbeat_at, max_attempts, progress, COALESCE(output_stored_object_id::text,''),
          COALESCE(diagnostic_id::text,''), created_at, started_at, completed_at,
          cancel_requested_at, canceled_at, expires_at`, params.ExportJobID,
		params.AttemptToken, params.LeaseOwner, params.LeaseExpiresAt, params.ClaimedAt), &job)
	if err == sql.ErrNoRows {
		return ExportJobRecord{}, false, nil
	}
	return job, err == nil, err
}

func (s *Store) RecordExportJobProgress(ctx context.Context, params RecordExportJobProgressParams) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE export_jobs
SET status='running', version=version+1, progress=$4, heartbeat_at=$5,
    lease_expires_at=$5 + (lease_expires_at - heartbeat_at)
WHERE id=$1 AND lease_owner=$2 AND attempt_token=$3
  AND status IN ('claimed','running') AND lease_expires_at > $5`, params.ExportJobID, params.LeaseOwner,
		params.AttemptToken, jsonOrDefault(params.ProgressJSON, "{}"), params.HeartbeatAt)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) RequestExportJobCancel(ctx context.Context, channelAccountID, exportJobID string, requestedAt time.Time) (ExportJobRecord, error) {
	var job ExportJobRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx, `
SELECT status FROM export_jobs WHERE id=$1 AND channel_account_id=$2 FOR UPDATE`, exportJobID, channelAccountID).Scan(&status); err != nil {
			return err
		}
		next := status
		switch status {
		case "queued":
			next = "canceled"
		case "claimed", "running":
			next = "cancel_requested"
		case "cancel_requested", "canceled", "failed", "succeeded", "expired":
		default:
			return sql.ErrNoRows
		}
		if next == "canceled" {
			if _, err := tx.ExecContext(ctx, `
UPDATE stored_object_pins SET released_at=$2
WHERE owner_type='export_job' AND owner_id=$1 AND released_at IS NULL`, exportJobID, requestedAt); err != nil {
				return err
			}
		}
		row := tx.QueryRowContext(ctx, `
UPDATE export_jobs
SET status=$3, version=version+1, cancel_requested_at=COALESCE(cancel_requested_at,$4),
    canceled_at=CASE WHEN $3='canceled' THEN $4 ELSE canceled_at END,
    completed_at=CASE WHEN $3='canceled' THEN $4 ELSE completed_at END
WHERE id=$1 AND channel_account_id=$2
RETURNING id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version,
          COALESCE(idempotency_key,''), retry_generation, attempt_no,
          COALESCE(attempt_token,''), COALESCE(lease_owner,''), lease_expires_at,
          heartbeat_at, max_attempts, progress, COALESCE(output_stored_object_id::text,''),
          COALESCE(diagnostic_id::text,''), created_at, started_at, completed_at,
          cancel_requested_at, canceled_at, expires_at`, exportJobID, channelAccountID, next, requestedAt)
		return scanExportJob(row, &job)
	})
	return job, err
}

func (s *Store) RetryExportJob(ctx context.Context, channelAccountID, exportJobID, idempotencyKey string, pin StoredObjectPinRecord, retriedAt time.Time) (ExportJobRecord, error) {
	var job ExportJobRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if idempotencyKey != "" {
			inserted, err := tx.ExecContext(ctx, `
INSERT INTO operation_requests (
    id, channel_account_id, operation_type, idempotency_key, request_hash,
    status, target_type, target_id, metadata, created_at, completed_at
)
VALUES (md5($1 || ':export_job.retry:' || $3)::uuid,$1,'export_job.retry',$3,$2,'completed','export_job',$2,'{}',$4,$4)
ON CONFLICT (channel_account_id, operation_type, idempotency_key) DO NOTHING`, channelAccountID, exportJobID, idempotencyKey, retriedAt)
			if err != nil {
				return err
			}
			rows, err := inserted.RowsAffected()
			if err != nil {
				return err
			}
			if rows == 0 {
				var targetID, requestHash string
				if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(target_id::text,''), COALESCE(request_hash,'')
FROM operation_requests
WHERE channel_account_id=$1 AND operation_type='export_job.retry' AND idempotency_key=$2
FOR UPDATE`, channelAccountID, idempotencyKey).Scan(&targetID, &requestHash); err != nil {
					return err
				}
				if targetID != exportJobID || requestHash != exportJobID {
					return ErrExportJobRetryIdempotencyConflict
				}
				return scanExportJob(tx.QueryRowContext(ctx, exportJobSelect+` WHERE id=$1 AND channel_account_id=$2`, exportJobID, channelAccountID), &job)
			}
		}
		var status string
		if err := tx.QueryRowContext(ctx, `
SELECT status FROM export_jobs WHERE id=$1 AND channel_account_id=$2 FOR UPDATE`, exportJobID, channelAccountID).Scan(&status); err != nil {
			return err
		}
		if status != "failed" && status != "canceled" {
			return sql.ErrNoRows
		}
		if pin.ID != "" {
			if err := lockAvailableStoredObject(ctx, tx, pin.StoredObjectID); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `
INSERT INTO stored_object_pins (id, stored_object_id, owner_type, owner_id, purpose, expires_at, created_at)
SELECT $1,$2,'export_job',$3,'source',NULL,$4
FROM stored_objects
WHERE id=$2 AND storage_status='available' AND deleted_at IS NULL`, pin.ID, pin.StoredObjectID, exportJobID, retriedAt)
			if err != nil {
				return err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				if err != nil {
					return err
				}
				return sql.ErrNoRows
			}
		}
		row := tx.QueryRowContext(ctx, `
UPDATE export_jobs
SET status='queued', version=version+1, retry_generation=retry_generation+1,
    attempt_no=0, attempt_token=NULL, lease_owner=NULL, lease_expires_at=NULL,
    heartbeat_at=NULL, progress='{}'::jsonb, diagnostic_id=NULL, completed_at=NULL,
    cancel_requested_at=NULL, canceled_at=NULL, expires_at=NULL
WHERE id=$1 AND channel_account_id=$2
RETURNING id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version,
          COALESCE(idempotency_key,''), retry_generation, attempt_no,
          COALESCE(attempt_token,''), COALESCE(lease_owner,''), lease_expires_at,
          heartbeat_at, max_attempts, progress, COALESCE(output_stored_object_id::text,''),
          COALESCE(diagnostic_id::text,''), created_at, started_at, completed_at,
          cancel_requested_at, canceled_at, expires_at`, exportJobID, channelAccountID)
		return scanExportJob(row, &job)
	})
	return job, err
}

func (s *Store) FinalizeExportJob(ctx context.Context, params FinalizeExportJobParams) (ExportJobRecord, error) {
	var job ExportJobRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var currentStatus string
		if err := tx.QueryRowContext(ctx, `
SELECT status FROM export_jobs
WHERE id=$1 AND lease_owner=$2 AND attempt_token=$3
  AND lease_expires_at > $4
FOR UPDATE`, params.ExportJobID, params.LeaseOwner, params.AttemptToken, params.CompletedAt).Scan(&currentStatus); err != nil {
			return err
		}
		status := params.Status
		if currentStatus == "cancel_requested" {
			status = "canceled"
		} else if currentStatus != "claimed" && currentStatus != "running" {
			return sql.ErrNoRows
		}
		if status != "succeeded" && status != "failed" && status != "canceled" {
			return sql.ErrNoRows
		}
		if status == "succeeded" {
			if params.Output.ID == "" || params.Delivery.ID == "" || params.DeliveryPin.ID == "" {
				return sql.ErrNoRows
			}
			if err := ensureObjectLocationsWritable(ctx, tx, params.Output.Bucket, params.Output.ObjectKey); err != nil {
				return err
			}
			registeredOutput, err := registerExportOutput(ctx, tx, params.Output)
			if err != nil {
				return err
			}
			params.Output = registeredOutput
			params.DeliveryPin.StoredObjectID = registeredOutput.ID
			if _, err := tx.ExecContext(ctx, `
INSERT INTO export_deliveries (
    id, export_job_id, channel_account_id, channel, status, version, attempt_no,
    attempt_token, lease_owner, lease_expires_at, next_attempt_at, max_attempts, expires_at,
    delivered_at, failure_code, created_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
				params.Delivery.ID, params.Delivery.ExportJobID, params.Delivery.ChannelAccountID,
				params.Delivery.Channel, withDefault(params.Delivery.Status, "pending"),
				positiveVersion(params.Delivery.Version), params.Delivery.AttemptNo,
				nullString(params.Delivery.AttemptToken), nullString(params.Delivery.LeaseOwner),
				params.Delivery.LeaseExpiresAt, params.Delivery.NextAttemptAt,
				positiveInt(params.Delivery.MaxAttempts), params.Delivery.ExpiresAt,
				params.Delivery.DeliveredAt, nullString(params.Delivery.FailureCode),
				params.Delivery.CreatedAt); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO stored_object_pins (
    id, stored_object_id, owner_type, owner_id, purpose, expires_at, created_at
)
VALUES ($1,$2,'export_delivery',$3,'delivery',$4,$5)`, params.DeliveryPin.ID,
				params.DeliveryPin.StoredObjectID, params.DeliveryPin.OwnerID,
				params.DeliveryPin.ExpiresAt, params.DeliveryPin.CreatedAt); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE stored_object_pins SET released_at=$2
WHERE owner_type='export_job' AND owner_id=$1 AND purpose='source' AND released_at IS NULL`, params.ExportJobID, params.CompletedAt); err != nil {
			return err
		}
		if status == "succeeded" && params.RetentionDays > 0 {
			if _, err := tx.ExecContext(ctx, `
UPDATE stored_objects so
SET last_successful_use_at=GREATEST(COALESCE(last_successful_use_at,$2),$2),
    expires_at=GREATEST(generation_published_at,$2) + make_interval(days => $3)
FROM media_assets ma
WHERE ma.id=(SELECT media_asset_id FROM export_jobs WHERE id=$1)
  AND ma.stored_object_id=so.id`, params.ExportJobID, params.CompletedAt, params.RetentionDays); err != nil {
				return err
			}
		}
		row := tx.QueryRowContext(ctx, `
UPDATE export_jobs
SET status=$4, version=version+1, output_stored_object_id=$5,
    diagnostic_id=$6, completed_at=$7,
    canceled_at=CASE WHEN $4='canceled' THEN $7 ELSE canceled_at END,
    lease_expires_at=NULL, heartbeat_at=$7
WHERE id=$1 AND lease_owner=$2 AND attempt_token=$3
  AND lease_expires_at > $7
RETURNING id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version,
          COALESCE(idempotency_key,''), retry_generation, attempt_no,
          COALESCE(attempt_token,''), COALESCE(lease_owner,''), lease_expires_at,
          heartbeat_at, max_attempts, progress, COALESCE(output_stored_object_id::text,''),
          COALESCE(diagnostic_id::text,''), created_at, started_at, completed_at,
          cancel_requested_at, canceled_at, expires_at`, params.ExportJobID,
			params.LeaseOwner, params.AttemptToken, status, nullString(params.Output.ID),
			nullString(params.DiagnosticID), params.CompletedAt)
		return scanExportJob(row, &job)
	})
	return job, err
}

func registerExportOutput(ctx context.Context, tx *sql.Tx, candidate StoredObjectRecord) (StoredObjectRecord, error) {
	inserted, err := insertCanonicalExportOutput(ctx, tx, candidate)
	if err != nil {
		return StoredObjectRecord{}, err
	}
	if inserted {
		return candidate, nil
	}

	var existing StoredObjectRecord
	err = scanStoredObject(tx.QueryRowContext(ctx, storedObjectSelect+`
WHERE channel_account_id=$1 AND checksum=$2 AND size_bytes=$3
FOR UPDATE`, candidate.ChannelAccountID, candidate.Checksum, candidate.SizeBytes), &existing)
	if err != nil {
		return StoredObjectRecord{}, err
	}
	switch existing.StorageStatus {
	case "available":
		var registered StoredObjectRecord
		err := scanStoredObject(tx.QueryRowContext(ctx, `
UPDATE stored_objects
		SET expires_at=CASE
		        WHEN expires_at IS NULL THEN NULL
		        WHEN $2::timestamptz IS NULL THEN expires_at
		        ELSE GREATEST(expires_at,$2)
		    END
WHERE id=$1 AND storage_status='available' AND deleted_at IS NULL
RETURNING id, COALESCE(channel_account_id::text,''), bucket, object_key,
          COALESCE(staging_key,''), generation, generation_published_at,
          COALESCE(content_type,''), size_bytes, checksum_algorithm,
          COALESCE(checksum,''), storage_status, retention_state, hold_state,
          last_successful_use_at, created_at, expires_at,
          COALESCE(delete_owner,''), COALESCE(delete_token,''),
	          delete_lease_expires_at, delete_attempts, deleted_at`, existing.ID, candidate.ExpiresAt), &registered)
		return registered, err
	case "deleted", "missing":
		var registered StoredObjectRecord
		err := scanStoredObject(tx.QueryRowContext(ctx, `
UPDATE stored_objects
SET bucket=$2, object_key=$3, staging_key=NULL, generation=generation+1,
    generation_published_at=$4, content_type=$5, size_bytes=$6,
    checksum_algorithm=$7, checksum=$8, storage_status='available',
    retention_state=$9, hold_state='none', expires_at=$10,
    delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL, deleted_at=NULL
WHERE id=$1 AND storage_status IN ('deleted','missing')
RETURNING id, COALESCE(channel_account_id::text,''), bucket, object_key,
          COALESCE(staging_key,''), generation, generation_published_at,
          COALESCE(content_type,''), size_bytes, checksum_algorithm,
          COALESCE(checksum,''), storage_status, retention_state, hold_state,
          last_successful_use_at, created_at, expires_at,
          COALESCE(delete_owner,''), COALESCE(delete_token,''),
          delete_lease_expires_at, delete_attempts, deleted_at`, existing.ID,
			candidate.Bucket, candidate.ObjectKey, candidate.GenerationPublishedAt,
			nullString(candidate.ContentType), candidate.SizeBytes,
			withDefault(candidate.ChecksumAlgorithm, "sha256"), nullString(candidate.Checksum),
			withDefault(candidate.RetentionState, "expires_scheduled"), candidate.ExpiresAt), &registered)
		return registered, err
	default:
		return StoredObjectRecord{}, sql.ErrNoRows
	}
}

func insertCanonicalExportOutput(ctx context.Context, tx *sql.Tx, record StoredObjectRecord) (bool, error) {
	publishedAt := record.GenerationPublishedAt
	if publishedAt.IsZero() {
		publishedAt = record.CreatedAt
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO stored_objects (
    id, channel_account_id, bucket, object_key, staging_key, generation,
    generation_published_at, content_type, size_bytes, checksum_algorithm,
    checksum, storage_status, retention_state, hold_state, last_successful_use_at,
    created_at, expires_at, delete_owner, delete_token, delete_lease_expires_at,
    delete_attempts, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
ON CONFLICT (channel_account_id, checksum, size_bytes)
WHERE channel_account_id IS NOT NULL AND checksum IS NOT NULL AND checksum <> ''
DO NOTHING`,
		record.ID, nullString(record.ChannelAccountID), record.Bucket, record.ObjectKey,
		nullString(record.StagingKey), positiveInt(record.Generation), publishedAt,
		nullString(record.ContentType), record.SizeBytes,
		withDefault(record.ChecksumAlgorithm, "sha256"), nullString(record.Checksum),
		withDefault(record.StorageStatus, "available"),
		withDefault(record.RetentionState, "expires_scheduled"), withDefault(record.HoldState, "none"),
		record.LastSuccessfulUseAt, record.CreatedAt, record.ExpiresAt,
		nullString(record.DeleteOwner), nullString(record.DeleteToken),
		record.DeleteLeaseExpiresAt, record.DeleteAttempts, record.DeletedAt)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

func (s *Store) ListExportDeliveries(ctx context.Context, channelAccountID, exportJobID string) ([]ExportDeliveryRecord, error) {
	rows, err := s.db.QueryContext(ctx, exportDeliverySelect+`
WHERE channel_account_id=$1 AND export_job_id=$2
ORDER BY created_at ASC`, channelAccountID, exportJobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := make([]ExportDeliveryRecord, 0)
	for rows.Next() {
		var delivery ExportDeliveryRecord
		if err := scanExportDelivery(rows, &delivery); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

const exportDeliverySelect = `
SELECT id, export_job_id, channel_account_id, channel, status, version,
       attempt_no, COALESCE(attempt_token,''), COALESCE(lease_owner,''),
       lease_expires_at, next_attempt_at, max_attempts, expires_at, delivered_at,
       COALESCE(failure_code,''), created_at
FROM export_deliveries`

func (s *Store) ClaimExportDelivery(ctx context.Context, params ClaimExportDeliveryParams) (ExportDeliveryRecord, bool, error) {
	var delivery ExportDeliveryRecord
	err := scanExportDelivery(s.db.QueryRowContext(ctx, `
UPDATE export_deliveries
SET status='claimed', version=version+1, attempt_no=attempt_no+1,
    attempt_token=$4, lease_owner=$5, lease_expires_at=$6, next_attempt_at=NULL, failure_code=NULL
WHERE export_job_id=$1 AND channel_account_id=$2 AND channel=$3
  AND status IN ('pending','failed') AND attempt_no < max_attempts
  AND expires_at > $7 AND (next_attempt_at IS NULL OR next_attempt_at <= $7)
RETURNING id, export_job_id, channel_account_id, channel, status, version,
          attempt_no, COALESCE(attempt_token,''), COALESCE(lease_owner,''),
          lease_expires_at, next_attempt_at, max_attempts, expires_at, delivered_at,
          COALESCE(failure_code,''), created_at`, params.ExportJobID,
		params.ChannelAccountID, params.Channel, params.AttemptToken, params.LeaseOwner,
		params.LeaseExpiresAt, params.ClaimedAt), &delivery)
	if err == sql.ErrNoRows {
		return ExportDeliveryRecord{}, false, nil
	}
	return delivery, err == nil, err
}

func (s *Store) FinalizeExportDelivery(ctx context.Context, params FinalizeExportDeliveryParams) (ExportDeliveryRecord, error) {
	var delivery ExportDeliveryRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if params.Status != "delivered" && params.Status != "failed" {
			return sql.ErrNoRows
		}
		row := tx.QueryRowContext(ctx, `
UPDATE export_deliveries
SET status=CASE
        WHEN $6='delivered' THEN 'delivered'
        WHEN NOT $9 OR attempt_no >= max_attempts OR expires_at <= $8 THEN 'expired'
        ELSE 'pending'
    END,
    version=version+1,
    attempt_token=NULL,
    lease_owner=NULL,
    lease_expires_at=NULL,
    next_attempt_at=CASE
        WHEN $6='failed' AND $9 AND attempt_no < max_attempts AND expires_at > $8
        THEN LEAST(expires_at, $8 + make_interval(secs => LEAST(30 * power(2, GREATEST(attempt_no-1,0)), 900)::double precision))
        ELSE NULL
    END,
	    delivered_at=CASE WHEN $6='delivered' THEN $8 ELSE delivered_at END,
	    failure_code=CASE WHEN $6='failed' THEN NULLIF($7,'') ELSE NULL END
WHERE export_job_id=$1 AND channel_account_id=$2 AND id=$3
  AND lease_owner=$4 AND attempt_token=$5 AND status='claimed'
  AND lease_expires_at > $8
RETURNING id, export_job_id, channel_account_id, channel, status, version,
          attempt_no, COALESCE(attempt_token,''), COALESCE(lease_owner,''),
          lease_expires_at, next_attempt_at, max_attempts, expires_at, delivered_at,
          COALESCE(failure_code,''), created_at`, params.ExportJobID,
			params.ChannelAccountID, params.ExportDeliveryID, params.LeaseOwner, params.AttemptToken,
			params.Status, params.FailureCode, params.FinalizedAt, params.Retryable)
		if err := scanExportDelivery(row, &delivery); err != nil {
			return err
		}
		if delivery.Status == "delivered" || delivery.Status == "expired" {
			_, err := tx.ExecContext(ctx, `
UPDATE stored_object_pins SET released_at=$2
WHERE owner_type='export_delivery' AND owner_id=$1 AND purpose='delivery'
  AND released_at IS NULL`, delivery.ID, params.FinalizedAt)
			return err
		}
		return nil
	})
	return delivery, err
}

func (s *Store) ReclaimExportJobs(ctx context.Context, now time.Time, limit int) (ExportJobReclaimResult, error) {
	if limit <= 0 {
		limit = 100
	}
	var result ExportJobReclaimResult
	err := s.db.QueryRowContext(ctx, `
WITH expired AS (
    SELECT id, attempt_no, max_attempts
    FROM export_jobs
    WHERE status IN ('claimed','running','cancel_requested')
      AND lease_expires_at IS NOT NULL AND lease_expires_at <= $1
    FOR UPDATE SKIP LOCKED
	LIMIT $2
), updated AS (
UPDATE export_jobs ej
SET status=CASE
        WHEN expired.attempt_no < expired.max_attempts AND ej.status <> 'cancel_requested' THEN 'queued'
        WHEN ej.status='cancel_requested' THEN 'canceled'
        ELSE 'failed'
    END,
    version=version+1,
    attempt_token=NULL,
    lease_owner=NULL,
    lease_expires_at=NULL,
    heartbeat_at=NULL,
    completed_at=CASE
        WHEN expired.attempt_no >= expired.max_attempts OR ej.status='cancel_requested' THEN $1
        ELSE completed_at
    END,
    canceled_at=CASE WHEN ej.status='cancel_requested' THEN $1 ELSE canceled_at END
FROM expired
WHERE ej.id=expired.id
RETURNING ej.id, ej.status
), released AS (
UPDATE stored_object_pins p
SET released_at=$1
FROM updated
WHERE updated.status IN ('failed','canceled')
  AND p.owner_type='export_job' AND p.owner_id=updated.id
  AND p.purpose='source' AND p.released_at IS NULL
RETURNING p.id
)
SELECT count(*),
       count(*) FILTER (WHERE status='queued'),
       count(*) FILTER (WHERE status='failed')
FROM updated`, now, limit).Scan(&result.Examined, &result.Requeued, &result.Failed)
	return result, err
}

func (s *Store) ReclaimExportDeliveries(ctx context.Context, now time.Time, limit int) (int64, error) {
	return s.reclaimExportDeliveries(ctx, now, limit)
}

func (s *Store) reclaimExportDeliveries(ctx context.Context, now time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	var reclaimed int64
	err := s.db.QueryRowContext(ctx, `
WITH candidates AS (
    SELECT id, expires_at, attempt_no, max_attempts
    FROM export_deliveries
    WHERE (status='claimed' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $1)
       OR (status IN ('pending','failed') AND expires_at <= $1)
    FOR UPDATE SKIP LOCKED
	LIMIT $2
), updated AS (
    UPDATE export_deliveries d
    SET status=CASE
            WHEN candidates.expires_at <= $1 OR candidates.attempt_no >= candidates.max_attempts THEN 'expired'
            ELSE 'pending'
        END,
        version=version+1,
        attempt_token=NULL,
        lease_owner=NULL,
        lease_expires_at=NULL,
        next_attempt_at=NULL,
        failure_code=CASE
            WHEN candidates.expires_at <= $1 THEN 'delivery_expired'
            WHEN candidates.attempt_no >= candidates.max_attempts THEN 'delivery_retry_exhausted'
            ELSE failure_code
        END
    FROM candidates
    WHERE d.id=candidates.id
    RETURNING d.id, d.status
), released AS (
UPDATE stored_object_pins p
SET released_at=$1
FROM updated
WHERE updated.status='expired' AND p.owner_type='export_delivery'
  AND p.owner_id=updated.id AND p.purpose='delivery' AND p.released_at IS NULL
RETURNING p.id
)
SELECT count(*) FROM updated`, now, limit).Scan(&reclaimed)
	return reclaimed, err
}

func (s *Store) ClaimRetentionDeletes(ctx context.Context, owner, token string, now, leaseExpiresAt time.Time, limit int) ([]RetentionDeleteClaimRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
WITH expired_pins AS (
    UPDATE stored_object_pins
    SET released_at=$3
    WHERE released_at IS NULL AND expires_at IS NOT NULL AND expires_at <= $3
), candidates AS (
    SELECT so.id
    FROM stored_objects so
    WHERE (
          (so.storage_status='available' AND so.expires_at IS NOT NULL AND so.expires_at <= $3)
          OR (
              so.storage_status='delete_scheduled'
              AND (so.delete_lease_expires_at IS NULL OR so.delete_lease_expires_at <= $3)
	          AND (so.expires_at IS NULL OR so.expires_at <= $3)
          )
      )
      AND so.hold_state='none'
      AND NOT EXISTS (
          SELECT 1 FROM stored_object_pins p
          WHERE p.stored_object_id=so.id AND p.released_at IS NULL
      )
    ORDER BY so.expires_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $5
)
UPDATE stored_objects so
SET storage_status='delete_scheduled', retention_state='expires_scheduled',
    delete_owner=$1, delete_token=$2, delete_lease_expires_at=$4,
    delete_attempts=delete_attempts+1
FROM candidates
WHERE so.id=candidates.id
RETURNING so.id, COALESCE(so.channel_account_id::text,''), so.bucket, so.object_key,
          COALESCE(so.staging_key,''), so.generation, so.generation_published_at,
          COALESCE(so.content_type,''), so.size_bytes, so.checksum_algorithm,
          COALESCE(so.checksum,''), so.storage_status, so.retention_state,
          so.hold_state, so.last_successful_use_at, so.created_at, so.expires_at,
          COALESCE(so.delete_owner,''), COALESCE(so.delete_token,''),
          so.delete_lease_expires_at, so.delete_attempts, so.deleted_at`,
		owner, token, now, leaseExpiresAt, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	claims := make([]RetentionDeleteClaimRecord, 0)
	for rows.Next() {
		var object StoredObjectRecord
		if err := scanStoredObject(rows, &object); err != nil {
			return nil, err
		}
		claims = append(claims, RetentionDeleteClaimRecord{
			StoredObject: object,
			DeleteOwner:  owner,
			DeleteToken:  token,
		})
	}
	return claims, rows.Err()
}

func (s *Store) CompleteRetentionDelete(ctx context.Context, storedObjectID string, generation int, owner, token string, deletedAt time.Time) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='deleted', retention_state='expired', deleted_at=$6,
    delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL
WHERE id=$1 AND storage_status='delete_scheduled' AND delete_owner=$2
  AND delete_token=$3 AND generation=$4 AND delete_lease_expires_at >= $5`, storedObjectID, owner, token, generation, deletedAt, deletedAt)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return sql.ErrNoRows
		}
		_, err = tx.ExecContext(ctx, `
UPDATE export_jobs
SET status='expired', version=version+1, expires_at=$2
WHERE output_stored_object_id=$1 AND status='succeeded'`, storedObjectID, deletedAt)
		return err
	})
}

func (s *Store) FailRetentionDelete(ctx context.Context, storedObjectID string, generation int, owner, token string, failedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='delete_scheduled', retention_state='expires_scheduled',
    delete_owner=NULL, delete_token=NULL, delete_lease_expires_at=NULL,
    expires_at=$5 + make_interval(secs => LEAST(30 * power(2, GREATEST(delete_attempts-1,0)), 3600)::double precision)
WHERE id=$1 AND storage_status='delete_scheduled' AND delete_owner=$2
  AND delete_token=$3 AND generation=$4 AND delete_lease_expires_at >= $5`, storedObjectID, owner, token, generation, failedAt)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

type exportDeliveryScanner interface {
	Scan(dest ...any) error
}

func scanExportDelivery(scanner exportDeliveryScanner, delivery *ExportDeliveryRecord) error {
	return scanner.Scan(
		&delivery.ID, &delivery.ExportJobID, &delivery.ChannelAccountID,
		&delivery.Channel, &delivery.Status, &delivery.Version, &delivery.AttemptNo,
		&delivery.AttemptToken, &delivery.LeaseOwner, &delivery.LeaseExpiresAt, &delivery.NextAttemptAt,
		&delivery.MaxAttempts, &delivery.ExpiresAt, &delivery.DeliveredAt,
		&delivery.FailureCode, &delivery.CreatedAt,
	)
}

type exportJobScanner interface {
	Scan(dest ...any) error
}

func scanExportJob(scanner exportJobScanner, job *ExportJobRecord) error {
	return scanner.Scan(
		&job.ID, &job.ChannelAccountID, &job.MediaAssetID, &job.Operation,
		&job.DeliveryChannel, &job.VariantJSON, &job.Status, &job.Version, &job.IdempotencyKey,
		&job.RetryGeneration, &job.AttemptNo, &job.AttemptToken, &job.LeaseOwner,
		&job.LeaseExpiresAt, &job.HeartbeatAt, &job.MaxAttempts, &job.ProgressJSON,
		&job.OutputStoredObjectID, &job.DiagnosticID, &job.CreatedAt, &job.StartedAt,
		&job.CompletedAt, &job.CancelRequestedAt, &job.CanceledAt, &job.ExpiresAt,
	)
}

type metadataEnrichmentScanner interface {
	Scan(dest ...any) error
}

func scanMetadataEnrichment(scanner metadataEnrichmentScanner) (MetadataEnrichmentRecord, error) {
	var record MetadataEnrichmentRecord
	err := scanner.Scan(
		&record.ID, &record.MediaAssetID, &record.ChannelAccountID, &record.Provider,
		&record.CanonicalURL, &record.Status, &record.Version, &record.IdempotencyKey,
		&record.AttemptNo, &record.MaxAttempts, &record.AttemptToken, &record.LeaseOwner,
		&record.LeaseExpiresAt, &record.HeartbeatAt, &record.NextAttemptAt,
		&record.ProgressJSON, &record.ErrorCode, &record.ErrorMessage, &record.CreatedAt,
		&record.StartedAt, &record.CompletedAt,
	)
	return record, err
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

func lockAvailableStoredObject(ctx context.Context, tx *sql.Tx, storedObjectID string) error {
	var available bool
	err := tx.QueryRowContext(ctx, `
SELECT storage_status='available' AND deleted_at IS NULL
FROM stored_objects
WHERE id=$1
FOR UPDATE`, storedObjectID).Scan(&available)
	if err != nil {
		return err
	}
	if !available {
		return sql.ErrNoRows
	}
	return nil
}

func insertStoredObject(ctx context.Context, tx *sql.Tx, record StoredObjectRecord) error {
	publishedAt := record.GenerationPublishedAt
	if publishedAt.IsZero() {
		publishedAt = record.CreatedAt
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO stored_objects (
    id, channel_account_id, bucket, object_key, staging_key, generation,
    generation_published_at, content_type, size_bytes, checksum_algorithm,
    checksum, storage_status, retention_state, hold_state, last_successful_use_at,
    created_at, expires_at, delete_owner, delete_token, delete_lease_expires_at,
    delete_attempts, deleted_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
ON CONFLICT (id) DO UPDATE
SET object_key=CASE
        WHEN stored_objects.storage_status IN ('publishing','deleted','missing') THEN EXCLUDED.object_key
        ELSE stored_objects.object_key
    END,
    staging_key=CASE
        WHEN stored_objects.storage_status IN ('publishing','deleted','missing') THEN EXCLUDED.staging_key
        ELSE stored_objects.staging_key
    END,
    generation=CASE
        WHEN stored_objects.storage_status IN ('deleted','missing') THEN stored_objects.generation + 1
        ELSE stored_objects.generation
    END,
    generation_published_at=CASE
        WHEN stored_objects.storage_status IN ('deleted','missing') THEN EXCLUDED.generation_published_at
        ELSE stored_objects.generation_published_at
    END,
    content_type=CASE
        WHEN stored_objects.storage_status IN ('publishing','deleted','missing') THEN EXCLUDED.content_type
        ELSE stored_objects.content_type
    END,
    size_bytes=CASE
        WHEN stored_objects.storage_status IN ('publishing','deleted','missing') THEN EXCLUDED.size_bytes
        ELSE stored_objects.size_bytes
    END,
    checksum_algorithm=CASE
        WHEN stored_objects.storage_status IN ('publishing','deleted','missing') THEN EXCLUDED.checksum_algorithm
        ELSE stored_objects.checksum_algorithm
    END,
    checksum=CASE
        WHEN stored_objects.storage_status IN ('publishing','deleted','missing') THEN EXCLUDED.checksum
        ELSE stored_objects.checksum
    END,
    storage_status=CASE
        WHEN stored_objects.storage_status IN ('deleted','missing') THEN EXCLUDED.storage_status
        ELSE stored_objects.storage_status
    END,
    retention_state=CASE
        WHEN stored_objects.storage_status IN ('deleted','missing') THEN EXCLUDED.retention_state
        ELSE stored_objects.retention_state
    END,
    expires_at=CASE
        WHEN stored_objects.storage_status IN ('deleted','missing') THEN EXCLUDED.expires_at
        ELSE stored_objects.expires_at
    END,
    deleted_at=CASE
        WHEN stored_objects.storage_status IN ('deleted','missing') THEN NULL
        ELSE stored_objects.deleted_at
    END
WHERE stored_objects.channel_account_id=EXCLUDED.channel_account_id`,
		record.ID, nullString(record.ChannelAccountID), record.Bucket, record.ObjectKey,
		nullString(record.StagingKey), positiveInt(record.Generation), publishedAt,
		nullString(record.ContentType), record.SizeBytes,
		withDefault(record.ChecksumAlgorithm, "sha256"), nullString(record.Checksum),
		withDefault(record.StorageStatus, "available"),
		withDefault(record.RetentionState, "active"), withDefault(record.HoldState, "none"),
		record.LastSuccessfulUseAt, record.CreatedAt, record.ExpiresAt,
		nullString(record.DeleteOwner), nullString(record.DeleteToken),
		record.DeleteLeaseExpiresAt, record.DeleteAttempts, record.DeletedAt)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

type storedObjectScanner interface {
	Scan(dest ...any) error
}

func scanStoredObject(scanner storedObjectScanner, object *StoredObjectRecord) error {
	return scanner.Scan(
		&object.ID,
		&object.ChannelAccountID,
		&object.Bucket,
		&object.ObjectKey,
		&object.StagingKey,
		&object.Generation,
		&object.GenerationPublishedAt,
		&object.ContentType,
		&object.SizeBytes,
		&object.ChecksumAlgorithm,
		&object.Checksum,
		&object.StorageStatus,
		&object.RetentionState,
		&object.HoldState,
		&object.LastSuccessfulUseAt,
		&object.CreatedAt,
		&object.ExpiresAt,
		&object.DeleteOwner,
		&object.DeleteToken,
		&object.DeleteLeaseExpiresAt,
		&object.DeleteAttempts,
		&object.DeletedAt,
	)
}

func ensureObjectLocationsWritable(ctx context.Context, tx *sql.Tx, bucket string, objectKeys ...string) error {
	keys := make([]string, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		if objectKey != "" {
			keys = append(keys, objectKey)
		}
	}
	sort.Strings(keys)
	for _, objectKey := range keys {
		if err := lockObjectLocation(ctx, tx, bucket, objectKey); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM object_delete_fences WHERE bucket=$1 AND object_key=$2 AND lease_expires_at <= now()`, bucket, objectKey); err != nil {
			return err
		}
		var fenced bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM object_delete_fences WHERE bucket=$1 AND object_key=$2)`, bucket, objectKey).Scan(&fenced); err != nil {
			return err
		}
		if fenced {
			return sql.ErrNoRows
		}
	}
	return nil
}

func lockObjectLocation(ctx context.Context, tx *sql.Tx, bucket, objectKey string) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, bucket+"/"+objectKey)
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
