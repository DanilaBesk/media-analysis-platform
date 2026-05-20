package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (s *SQLStateStore) AddMediaItem(ctx context.Context, item MediaItemRecord, inbox CollectionRecord, targetCollectionID string) (MediaItemRecord, CollectionRecord, error) {
	var collection CollectionRecord
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO sources (id, owner_type, owner_id, tenant_id, origin_type, external_uri, object_key, text_ref, checksum, size_bytes, mime_type, expires_at, created_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),NULLIF($7,''),NULLIF($8,''),NULLIF($9,''),$10,NULLIF($11,''),$12,$13)
`, item.Source.SourceID, item.Owner.OwnerType, item.Owner.OwnerID, item.Owner.TenantID, item.Source.OriginType, item.Source.ExternalURI, item.Source.ObjectKey, item.Source.TextRef, item.Source.Checksum, item.Source.SizeBytes, item.Source.MIMEType, item.Source.ExpiresAt, item.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO media_items (id, owner_type, owner_id, tenant_id, source_id, adapter_origin, kind, display_name, status, metadata, retention_state, retention_policy_id, expires_at, deleted_at, created_at, updated_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),$7,$8,$9,$10,$11,NULLIF($12,''),$13,$14,$15,$16)
`, item.ID, item.Owner.OwnerType, item.Owner.OwnerID, item.Owner.TenantID, item.Source.SourceID, item.AdapterOrigin, item.Kind, item.DisplayName, item.Status, item.MetadataJSON, item.Retention.State, item.Retention.PolicyID, item.Retention.ExpiresAt, item.DeletedAt, item.CreatedAt, item.UpdatedAt); err != nil {
			return err
		}
		found, err := selectInboxCollection(ctx, tx, item.Owner)
		if err != nil && !errors.Is(err, ErrCollectionNotFound) {
			return err
		}
		if errors.Is(err, ErrCollectionNotFound) {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO collections (id, owner_type, owner_id, tenant_id, kind, name, status, version, created_at, updated_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,$9,$10)
`, inbox.ID, inbox.Owner.OwnerType, inbox.Owner.OwnerID, inbox.Owner.TenantID, inbox.Kind, inbox.Name, inbox.Status, inbox.Version, inbox.CreatedAt, inbox.UpdatedAt); err != nil {
				return err
			}
			found = inbox
		}
		if err := appendCollectionMembership(ctx, tx, found.ID, item.ID, "", item.CreatedAt); err != nil {
			return err
		}
		if targetCollectionID != "" {
			if _, err := selectCollectionHeader(ctx, tx, item.Owner, targetCollectionID); err != nil {
				return err
			}
			if err := appendCollectionMembership(ctx, tx, targetCollectionID, item.ID, "", item.CreatedAt); err != nil {
				return err
			}
		}
		collection = found
		return nil
	})
	if err != nil {
		return MediaItemRecord{}, CollectionRecord{}, err
	}
	collection, _ = s.GetCollection(ctx, item.Owner, collection.ID)
	return item, collection, nil
}

func (s *SQLStateStore) ListMediaItems(ctx context.Context, owner OwnerScope) ([]MediaItemRecord, error) {
	rows, err := s.db.QueryContext(ctx, mediaItemSelectSQL()+`
WHERE mi.owner_type=$1 AND mi.owner_id=$2 AND COALESCE(mi.tenant_id,'')=COALESCE(NULLIF($3,''),'') AND mi.status <> 'deleted'
ORDER BY mi.created_at DESC`, owner.OwnerType, owner.OwnerID, owner.TenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []MediaItemRecord
	for rows.Next() {
		item, err := scanMediaItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *SQLStateStore) GetMediaItem(ctx context.Context, owner OwnerScope, mediaItemID string) (MediaItemRecord, error) {
	row := s.db.QueryRowContext(ctx, mediaItemSelectSQL()+`
WHERE mi.id=$1 AND mi.owner_type=$2 AND mi.owner_id=$3 AND COALESCE(mi.tenant_id,'')=COALESCE(NULLIF($4,''),'')`, mediaItemID, owner.OwnerType, owner.OwnerID, owner.TenantID)
	item, err := scanMediaItem(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return MediaItemRecord{}, ErrMediaItemNotFound
		}
		return MediaItemRecord{}, err
	}
	return item, nil
}

func (s *SQLStateStore) SoftDeleteMediaItem(ctx context.Context, owner OwnerScope, mediaItemID string, deletedAt time.Time) (MediaItemRecord, error) {
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
UPDATE media_items
SET status='deleted', retention_state='soft_deleted', deleted_at=$1, updated_at=$1
WHERE id=$2 AND owner_type=$3 AND owner_id=$4 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($5,''),'')`, deletedAt, mediaItemID, owner.OwnerType, owner.OwnerID, owner.TenantID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return ErrMediaItemNotFound
		}
		_, err = tx.ExecContext(ctx, `
UPDATE collection_items
SET removed_at=$1
WHERE media_item_id=$2::uuid AND removed_at IS NULL`, deletedAt, mediaItemID)
		return err
	})
	if err != nil {
		return MediaItemRecord{}, err
	}
	return s.GetMediaItem(ctx, owner, mediaItemID)
}

func (s *SQLStateStore) CreateCollection(ctx context.Context, collection CollectionRecord, itemIDs []string) (CollectionRecord, error) {
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO collections (id, owner_type, owner_id, tenant_id, kind, name, status, version, created_at, updated_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,$9,$10)
`, collection.ID, collection.Owner.OwnerType, collection.Owner.OwnerID, collection.Owner.TenantID, collection.Kind, collection.Name, collection.Status, collection.Version, collection.CreatedAt, collection.UpdatedAt); err != nil {
			return err
		}
		for position, itemID := range itemIDs {
			if _, err := selectMediaItemHeader(ctx, tx, collection.Owner, itemID); err != nil {
				return err
			}
			if err := insertCollectionMembership(ctx, tx, collection.ID, itemID, position, "", collection.CreatedAt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return CollectionRecord{}, err
	}
	return s.GetCollection(ctx, collection.Owner, collection.ID)
}

func (s *SQLStateStore) ListCollections(ctx context.Context, owner OwnerScope) ([]CollectionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), kind, name, status, version, created_at, updated_at, archived_at, deleted_at
FROM collections
WHERE owner_type=$1 AND owner_id=$2 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($3,''),'') AND status <> 'deleted'
ORDER BY updated_at DESC`, owner.OwnerType, owner.OwnerID, owner.TenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var collections []CollectionRecord
	for rows.Next() {
		collection, err := scanCollectionHeader(rows)
		if err != nil {
			return nil, err
		}
		collection.Items, _ = s.listCollectionItems(ctx, collection.ID)
		collections = append(collections, collection)
	}
	return collections, rows.Err()
}

func (s *SQLStateStore) GetCollection(ctx context.Context, owner OwnerScope, collectionID string) (CollectionRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), kind, name, status, version, created_at, updated_at, archived_at, deleted_at
FROM collections
WHERE id=$1 AND owner_type=$2 AND owner_id=$3 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'')`, collectionID, owner.OwnerType, owner.OwnerID, owner.TenantID)
	collection, err := scanCollectionHeader(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return CollectionRecord{}, ErrCollectionNotFound
		}
		return CollectionRecord{}, err
	}
	collection.Items, err = s.listCollectionItems(ctx, collection.ID)
	return collection, err
}

func (s *SQLStateStore) UpdateCollection(ctx context.Context, req UpdateCollectionRequest, updatedAt time.Time) (CollectionRecord, error) {
	result, err := s.db.ExecContext(ctx, `
UPDATE collections
SET name=COALESCE(NULLIF($1,''), name), status=COALESCE(NULLIF($2,''), status), version=version+1, updated_at=$3
WHERE id=$4 AND owner_type=$5 AND owner_id=$6 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($7,''),'') AND version=$8
`, req.Name, req.Status, updatedAt, req.CollectionID, req.Owner.OwnerType, req.Owner.OwnerID, req.Owner.TenantID, req.ExpectedVersion)
	if err != nil {
		return CollectionRecord{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return CollectionRecord{}, ErrCollectionVersionConflict
	}
	return s.GetCollection(ctx, req.Owner, req.CollectionID)
}

func (s *SQLStateStore) UpdateCollectionItems(ctx context.Context, req UpdateCollectionItemsRequest, updatedAt time.Time) (CollectionRecord, error) {
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE collections SET version=version+1, updated_at=$1 WHERE id=$2 AND owner_type=$3 AND owner_id=$4 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($5,''),'') AND version=$6`, updatedAt, req.CollectionID, req.Owner.OwnerType, req.Owner.OwnerID, req.Owner.TenantID, req.ExpectedVersion)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return ErrCollectionVersionConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE collection_items SET removed_at=$1 WHERE collection_id=$2 AND removed_at IS NULL`, updatedAt, req.CollectionID); err != nil {
			return err
		}
		for _, item := range req.Items {
			if _, err := selectMediaItemHeader(ctx, tx, req.Owner, item.MediaItemID); err != nil {
				return err
			}
			if err := insertCollectionMembership(ctx, tx, req.CollectionID, item.MediaItemID, item.Position, req.AddedBy, updatedAt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return CollectionRecord{}, err
	}
	return s.GetCollection(ctx, req.Owner, req.CollectionID)
}

func (s *SQLStateStore) CreateSelection(ctx context.Context, selection SelectionRecord, requestedItems []CollectionItemRecord) (SelectionRecord, error) {
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO selections (id, owner_type, owner_id, tenant_id, status, source_collection_id, option_snapshot, created_by, diagnostics, created_at, sealed_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,'')::uuid,$7,$8,'[]'::jsonb,$9,$10)
`, selection.ID, selection.Owner.OwnerType, selection.Owner.OwnerID, selection.Owner.TenantID, selection.Status, selection.SourceCollectionID, selection.OptionSnapshotJSON, selection.CreatedBy, selection.CreatedAt, selection.SealedAt); err != nil {
			return err
		}
		for _, requested := range requestedItems {
			item, err := s.GetMediaItem(ctx, selection.Owner, requested.MediaItemID)
			if err != nil {
				return err
			}
			sourceJSON, _ := json.Marshal(item.Source)
			retentionJSON, _ := json.Marshal(item.Retention)
			selectionItemID := uuidString()
			if _, err := tx.ExecContext(ctx, `
INSERT INTO selection_items (id, selection_id, position, media_item_id, kind, source_snapshot, display_name, status_at_selection, metadata_snapshot, retention_snapshot, diagnostics)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'[]'::jsonb)
`, selectionItemID, selection.ID, requested.Position, item.ID, item.Kind, sourceJSON, item.DisplayName, item.Status, item.MetadataJSON, retentionJSON); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return SelectionRecord{}, err
	}
	return s.GetSelection(ctx, selection.Owner, selection.ID)
}

func (s *SQLStateStore) GetSelection(ctx context.Context, owner OwnerScope, selectionID string) (SelectionRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), status, COALESCE(source_collection_id::text,''), option_snapshot, created_by, diagnostics, created_at, sealed_at
FROM selections
WHERE id=$1 AND owner_type=$2 AND owner_id=$3 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'')`, selectionID, owner.OwnerType, owner.OwnerID, owner.TenantID)
	selection, err := scanSelection(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return SelectionRecord{}, ErrSelectionNotFound
		}
		return SelectionRecord{}, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, position, media_item_id, kind, source_snapshot, display_name, status_at_selection, metadata_snapshot, retention_snapshot, diagnostics
FROM selection_items WHERE selection_id=$1 ORDER BY position ASC`, selectionID)
	if err != nil {
		return SelectionRecord{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var item SelectionItemSnapshot
		var sourceJSON, retentionJSON, diagnosticsJSON []byte
		if err := rows.Scan(&item.ID, &item.Position, &item.MediaItemID, &item.Kind, &sourceJSON, &item.DisplayName, &item.StatusAtSelection, &item.MetadataJSON, &retentionJSON, &diagnosticsJSON); err != nil {
			return SelectionRecord{}, err
		}
		_ = json.Unmarshal(sourceJSON, &item.SourceSnapshot)
		_ = json.Unmarshal(retentionJSON, &item.RetentionSnapshot)
		selection.Items = append(selection.Items, item)
	}
	return selection, rows.Err()
}

func (s *SQLStateStore) CreateAnalysisRun(ctx context.Context, run AnalysisRunRecord, task AnalysisRunTaskRecord, event RunEventRecord) (AnalysisRunRecord, error) {
	selection, err := s.GetSelection(ctx, run.Owner, run.SelectionID)
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_runs (id, owner_type, owner_id, tenant_id, selection_id, run_type, status, version, params, delivery, evidence_gate_state, created_at, expires_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,$9,$10,$11,$12,$13)
`, run.ID, run.Owner.OwnerType, run.Owner.OwnerID, run.Owner.TenantID, run.SelectionID, run.RunType, run.Status, run.Version, run.ParamsJSON, run.DeliveryJSON, run.EvidenceGateState, run.CreatedAt, run.ExpiresAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_tasks (id, analysis_run_id, worker_kind, task_type, status, attempt_no, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
`, task.ID, task.AnalysisRunID, task.WorkerKind, task.TaskType, task.Status, task.AttemptNo, task.CreatedAt); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (id, analysis_run_id, event_type, version, payload, status, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
`, event.ID, event.AnalysisRunID, event.EventType, event.Version, event.PayloadJSON, event.Status, event.CreatedAt)
		return err
	})
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	run.Selection = selection
	return run, nil
}

func (s *SQLStateStore) GetAnalysisRunByID(ctx context.Context, analysisRunID string) (AnalysisRunRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), selection_id, run_type, status, version, params, delivery, evidence_gate_state, created_at, started_at, completed_at, canceled_at, expires_at
FROM analysis_runs
WHERE id=$1::uuid`, analysisRunID)
	run, err := scanAnalysisRun(row)
	if err == sql.ErrNoRows {
		return AnalysisRunRecord{}, ErrAnalysisRunNotFound
	}
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	run.Selection, _ = s.GetSelection(ctx, run.Owner, run.SelectionID)
	return run, nil
}

func (s *SQLStateStore) GetAnalysisRun(ctx context.Context, owner OwnerScope, analysisRunID string) (AnalysisRunRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), selection_id, run_type, status, version, params, delivery, evidence_gate_state, created_at, started_at, completed_at, canceled_at, expires_at
FROM analysis_runs
WHERE id=$1 AND owner_type=$2 AND owner_id=$3 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'')`, analysisRunID, owner.OwnerType, owner.OwnerID, owner.TenantID)
	run, err := scanAnalysisRun(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return AnalysisRunRecord{}, ErrAnalysisRunNotFound
		}
		return AnalysisRunRecord{}, err
	}
	run.Selection, _ = s.GetSelection(ctx, owner, run.SelectionID)
	return run, nil
}

func (s *SQLStateStore) ListAnalysisRuns(ctx context.Context, owner OwnerScope) ([]AnalysisRunRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), selection_id, run_type, status, version, params, delivery, evidence_gate_state, created_at, started_at, completed_at, canceled_at, expires_at
FROM analysis_runs
WHERE owner_type=$1 AND owner_id=$2 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($3,''),'')
ORDER BY created_at DESC`, owner.OwnerType, owner.OwnerID, owner.TenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []AnalysisRunRecord
	for rows.Next() {
		run, err := scanAnalysisRun(rows)
		if err != nil {
			return nil, err
		}
		run.Selection, _ = s.GetSelection(ctx, owner, run.SelectionID)
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *SQLStateStore) ListRunEvents(ctx context.Context, owner OwnerScope, analysisRunID string) ([]RunEventRecord, error) {
	if _, err := s.GetAnalysisRun(ctx, owner, analysisRunID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, analysis_run_id, event_type, version, payload, COALESCE(status,''), created_at FROM analysis_run_events WHERE analysis_run_id=$1 ORDER BY created_at ASC`, analysisRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []RunEventRecord
	for rows.Next() {
		var event RunEventRecord
		if err := rows.Scan(&event.ID, &event.AnalysisRunID, &event.EventType, &event.Version, &event.PayloadJSON, &event.Status, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *SQLStateStore) RecordArtifacts(ctx context.Context, owner OwnerScope, analysisRunID string, artifacts []ArtifactRecord, createdAt time.Time) ([]ArtifactRecord, error) {
	if _, err := s.GetAnalysisRun(ctx, owner, analysisRunID); err != nil {
		return nil, err
	}
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, artifact := range artifacts {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO artifacts (id, owner_type, owner_id, tenant_id, analysis_run_id, kind, status, object_key, content_type, checksum, size_bytes, visibility, preview, retention_state, retention_policy_id, created_at, expires_at, deleted_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7,NULLIF($8,''),$9,NULLIF($10,''),$11,$12,$13,$14,NULLIF($15,''),$16,$17,$18)
`, artifact.ID, owner.OwnerType, owner.OwnerID, owner.TenantID, analysisRunID, artifact.Kind, artifact.Status, artifact.ObjectKey, artifact.ContentType, artifact.Checksum, artifact.SizeBytes, artifact.Visibility, artifact.PreviewJSON, artifact.Retention.State, artifact.Retention.PolicyID, firstNonZeroTime(artifact.CreatedAt, createdAt), artifact.ExpiresAt, artifact.DeletedAt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.ListArtifacts(ctx, owner, analysisRunID)
}

func (s *SQLStateStore) ListArtifacts(ctx context.Context, owner OwnerScope, analysisRunID string) ([]ArtifactRecord, error) {
	if analysisRunID != "" {
		if _, err := s.GetAnalysisRun(ctx, owner, analysisRunID); err != nil {
			return nil, err
		}
	}
	rows, err := s.db.QueryContext(ctx, artifactSelectSQL()+`
WHERE a.owner_type=$1 AND a.owner_id=$2 AND COALESCE(a.tenant_id,'')=COALESCE(NULLIF($3,''),'')
  AND a.visibility='owner'
  AND a.status <> 'deleted'
  AND ($4='' OR a.analysis_run_id::text=$4)
ORDER BY a.created_at DESC`, owner.OwnerType, owner.OwnerID, owner.TenantID, analysisRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var artifacts []ArtifactRecord
	for rows.Next() {
		artifact, err := scanArtifact(rows)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *SQLStateStore) GetArtifact(ctx context.Context, owner OwnerScope, artifactID string) (ArtifactRecord, error) {
	row := s.db.QueryRowContext(ctx, artifactSelectSQL()+`
WHERE a.id=$1 AND a.owner_type=$2 AND a.owner_id=$3 AND COALESCE(a.tenant_id,'')=COALESCE(NULLIF($4,''),'')
  AND a.visibility='owner'
  AND a.status <> 'deleted'`, artifactID, owner.OwnerType, owner.OwnerID, owner.TenantID)
	artifact, err := scanArtifact(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return ArtifactRecord{}, ErrArtifactNotFound
		}
		return ArtifactRecord{}, err
	}
	return artifact, nil
}

func (s *SQLStateStore) GetArtifactByID(ctx context.Context, artifactID string) (ArtifactRecord, error) {
	if s.columnExists(ctx, "artifacts", "channel_account_id", false) {
		return s.getTargetArtifactByID(ctx, artifactID)
	}
	row := s.db.QueryRowContext(ctx, artifactSelectSQL()+`
WHERE a.id=$1
  AND a.status <> 'deleted'`, artifactID)
	artifact, err := scanArtifact(row)
	if err != nil {
		if isLegacyArtifactSchemaMismatch(err) {
			return s.getTargetArtifactByID(ctx, artifactID)
		}
		if err == sql.ErrNoRows {
			return ArtifactRecord{}, ErrArtifactNotFound
		}
		return ArtifactRecord{}, err
	}
	return artifact, nil
}

func (s *SQLStateStore) getTargetArtifactByID(ctx context.Context, artifactID string) (ArtifactRecord, error) {
	var artifact ArtifactRecord
	var channelAccountID string
	err := s.db.QueryRowContext(ctx, `
SELECT a.id, COALESCE(a.channel_account_id::text,''), a.analysis_run_id::text,
       a.kind, a.status, COALESCE(so.object_key,''), a.content_type,
       COALESCE(a.checksum,''), a.size_bytes, a.visibility, a.preview,
       COALESCE(so.retention_state,'active'), a.created_at, a.expires_at, a.deleted_at
FROM artifacts a
LEFT JOIN stored_objects so ON so.id = a.stored_object_id
WHERE a.id=$1
  AND a.status <> 'deleted'`, artifactID).Scan(
		&artifact.ID,
		&channelAccountID,
		&artifact.AnalysisRunID,
		&artifact.Kind,
		&artifact.Status,
		&artifact.ObjectKey,
		&artifact.ContentType,
		&artifact.Checksum,
		&artifact.SizeBytes,
		&artifact.Visibility,
		&artifact.PreviewJSON,
		&artifact.Retention.State,
		&artifact.CreatedAt,
		&artifact.ExpiresAt,
		&artifact.DeletedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ArtifactRecord{}, ErrArtifactNotFound
		}
		return ArtifactRecord{}, err
	}
	artifact.Owner = OwnerScope{OwnerType: "channel_account", OwnerID: channelAccountID}
	return artifact, nil
}

func isLegacyArtifactSchemaMismatch(err error) bool {
	return isLegacyRuntimeSchemaMismatch(err)
}

func isLegacyRuntimeSchemaMismatch(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "SQLSTATE 42703") || strings.Contains(message, "SQLSTATE 42P01")
}

func (s *SQLStateStore) relationExists(ctx context.Context, relationName string) bool {
	var relation sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass($1)`, "public."+strings.TrimSpace(relationName)).Scan(&relation); err != nil {
		return true
	}
	return relation.Valid && strings.TrimSpace(relation.String) != ""
}

func (s *SQLStateStore) columnExists(ctx context.Context, tableName, columnName string, defaultOnError bool) bool {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema='public'
    AND table_name=$1
    AND column_name=$2
)`, strings.TrimSpace(tableName), strings.TrimSpace(columnName)).Scan(&exists); err != nil {
		return defaultOnError
	}
	return exists
}

func (s *SQLStateStore) ListDiagnostics(ctx context.Context, owner OwnerScope, query DiagnosticQuery) ([]DiagnosticRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), subject_type, subject_id, severity, code, message, context, safe_adapter_context, COALESCE(correlation_id,''), COALESCE(remediation_hint,''), created_at
FROM diagnostics
WHERE owner_type=$1 AND owner_id=$2 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($3,''),'')
  AND ($4='' OR subject_type=$4)
  AND ($5='' OR subject_id::text=$5)
  AND ($6='' OR severity=$6)
  AND ($7='' OR code=$7)
  AND ($8='' OR COALESCE(correlation_id,'')=$8)
ORDER BY created_at DESC`, owner.OwnerType, owner.OwnerID, owner.TenantID, query.SubjectType, query.SubjectID, query.Severity, query.Code, query.CorrelationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var diagnostics []DiagnosticRecord
	for rows.Next() {
		var d DiagnosticRecord
		if err := rows.Scan(&d.ID, &d.Owner.OwnerType, &d.Owner.OwnerID, &d.Owner.TenantID, &d.SubjectType, &d.SubjectID, &d.Severity, &d.Code, &d.Message, &d.ContextJSON, &d.SafeAdapterJSON, &d.CorrelationID, &d.RemediationHint, &d.CreatedAt); err != nil {
			return nil, err
		}
		diagnostics = append(diagnostics, d)
	}
	return diagnostics, rows.Err()
}

func (s *SQLStateStore) RecordDiagnostics(ctx context.Context, owner OwnerScope, analysisRunID string, diagnostics []DiagnosticRecord, createdAt time.Time) ([]DiagnosticRecord, error) {
	if _, err := s.GetAnalysisRun(ctx, owner, analysisRunID); err != nil {
		return nil, err
	}
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, diagnostic := range diagnostics {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO diagnostics (id, owner_type, owner_id, tenant_id, subject_type, subject_id, severity, code, message, context, safe_adapter_context, correlation_id, remediation_hint, created_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,$6::uuid,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),$14)
`, diagnostic.ID, owner.OwnerType, owner.OwnerID, owner.TenantID, diagnostic.SubjectType, diagnostic.SubjectID, diagnostic.Severity, diagnostic.Code, diagnostic.Message, diagnostic.ContextJSON, diagnostic.SafeAdapterJSON, diagnostic.CorrelationID, diagnostic.RemediationHint, firstNonZeroTime(diagnostic.CreatedAt, createdAt)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.ListDiagnostics(ctx, owner, DiagnosticQuery{})
}

func (s *SQLStateStore) RecordAnalysisRunProgress(ctx context.Context, owner OwnerScope, analysisRunID string, event RunEventRecord, recordedAt time.Time) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=CASE WHEN status='queued' THEN 'running' ELSE status END,
    started_at=COALESCE(started_at, $5),
    version=version+1
WHERE id=$1::uuid
  AND owner_type=$2
  AND owner_id=$3
  AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'')
  AND status NOT IN ('succeeded','partially_succeeded','failed','canceled','expired')
RETURNING version`, analysisRunID, owner.OwnerType, owner.OwnerID, owner.TenantID, recordedAt)
		if err := row.Scan(&event.Version); err != nil {
			if err == sql.ErrNoRows {
				return ErrAnalysisRunNotFound
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (id, analysis_run_id, event_type, version, payload, status, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
`, event.ID, event.AnalysisRunID, event.EventType, event.Version, event.PayloadJSON, event.Status, firstNonZeroTime(event.CreatedAt, recordedAt)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE analysis_run_tasks
SET heartbeat_at=$2
WHERE analysis_run_id=$1::uuid AND status='claimed'`, analysisRunID, recordedAt); err != nil {
			return err
		}
		var err error
		run, err = selectAnalysisRunByID(ctx, tx, analysisRunID)
		return err
	})
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	run.Selection, _ = s.GetSelection(ctx, run.Owner, run.SelectionID)
	return run, nil
}

func (s *SQLStateStore) RequestAnalysisRunCancellation(ctx context.Context, owner OwnerScope, analysisRunID string, event RunEventRecord, requestedAt time.Time) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		existing, err := selectAnalysisRunByIDForUpdate(ctx, tx, analysisRunID)
		if err != nil {
			return err
		}
		if !SameOwner(existing.Owner, owner) {
			return ErrOwnerMismatch
		}
		if terminalRunStatus(existing.Status) || existing.Status == AnalysisRunStatusCancelRequested {
			run = existing
			return nil
		}

		var claimed bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1 FROM analysis_run_tasks
  WHERE analysis_run_id=$1::uuid AND status='claimed'
)`, analysisRunID).Scan(&claimed); err != nil {
			return err
		}
		targetStatus := AnalysisRunStatusCanceled
		if claimed {
			targetStatus = AnalysisRunStatusCancelRequested
		}
		row := tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=$5,
    completed_at=CASE WHEN $5='canceled' THEN $6 ELSE completed_at END,
    canceled_at=COALESCE(canceled_at, $6),
    version=version+1
WHERE id=$1::uuid
  AND owner_type=$2
  AND owner_id=$3
  AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'')
  AND status NOT IN ('succeeded','partially_succeeded','failed','canceled','expired')
RETURNING version`, analysisRunID, owner.OwnerType, owner.OwnerID, owner.TenantID, targetStatus, requestedAt)
		if err := row.Scan(&event.Version); err != nil {
			return err
		}
		event.Status = targetStatus
		event.EventType = "analysis_run." + targetStatus
		if _, err := tx.ExecContext(ctx, `
UPDATE analysis_run_tasks
SET status='canceled', finalized_at=$2, heartbeat_at=$2
WHERE analysis_run_id=$1::uuid
  AND (status IN ('queued','pending_enqueue') OR ($3='canceled' AND status='claimed'))`, analysisRunID, requestedAt, targetStatus); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (id, analysis_run_id, event_type, version, payload, status, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
`, event.ID, event.AnalysisRunID, event.EventType, event.Version, event.PayloadJSON, event.Status, firstNonZeroTime(event.CreatedAt, requestedAt)); err != nil {
			return err
		}
		run, err = selectAnalysisRunByID(ctx, tx, analysisRunID)
		return err
	})
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	run.Selection, _ = s.GetSelection(ctx, run.Owner, run.SelectionID)
	return run, nil
}

func (s *SQLStateStore) FinalizeAnalysisRunTask(ctx context.Context, owner OwnerScope, analysisRunID, status string, event RunEventRecord, finalizedAt time.Time) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		var finalStatus string
		row := tx.QueryRowContext(ctx, `
UPDATE analysis_runs
SET status=CASE WHEN status='cancel_requested' THEN 'canceled' ELSE $5 END,
    completed_at=$6,
    canceled_at=CASE WHEN status='cancel_requested' OR $5='canceled' THEN COALESCE(canceled_at, $6) ELSE canceled_at END,
    version=version+1
WHERE id=$1::uuid
  AND owner_type=$2
  AND owner_id=$3
  AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'')
  AND status NOT IN ('succeeded','partially_succeeded','failed','canceled','expired')
RETURNING version, status`, analysisRunID, owner.OwnerType, owner.OwnerID, owner.TenantID, status, finalizedAt)
		if err := row.Scan(&event.Version, &finalStatus); err != nil {
			if err == sql.ErrNoRows {
				existing, lookupErr := selectAnalysisRunByID(ctx, tx, analysisRunID)
				if lookupErr != nil {
					return lookupErr
				}
				if !SameOwner(existing.Owner, owner) {
					return ErrOwnerMismatch
				}
				run = existing
				return nil
			}
			return err
		}
		event.Status = finalStatus
		event.EventType = "analysis_run." + finalStatus
		if _, err := tx.ExecContext(ctx, `
UPDATE analysis_run_tasks
SET status=$2, finalized_at=$3, heartbeat_at=$3
WHERE analysis_run_id=$1::uuid AND status IN ('claimed','queued','pending_enqueue')`, analysisRunID, finalStatus, finalizedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO analysis_run_events (id, analysis_run_id, event_type, version, payload, status, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
`, event.ID, event.AnalysisRunID, event.EventType, event.Version, event.PayloadJSON, event.Status, firstNonZeroTime(event.CreatedAt, finalizedAt)); err != nil {
			return err
		}
		var err error
		run, err = selectAnalysisRunByID(ctx, tx, analysisRunID)
		return err
	})
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	run.Selection, _ = s.GetSelection(ctx, run.Owner, run.SelectionID)
	return run, nil
}

func (s *SQLStateStore) ApplyRetentionPolicies(ctx context.Context, now time.Time) (RetentionSweepResult, error) {
	var result RetentionSweepResult
	if !s.hasLegacyRetentionSchema(ctx) {
		return result, nil
	}
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
UPDATE media_items mi
SET status='deleted', retention_state='expired', deleted_at=$1, updated_at=$1
FROM sources s
WHERE mi.source_id=s.id
  AND mi.status <> 'deleted'
  AND mi.retention_state <> 'held'
  AND ((mi.expires_at IS NOT NULL AND mi.expires_at <= $1) OR (s.expires_at IS NOT NULL AND s.expires_at <= $1))`, now)
		if err != nil {
			return err
		}
		result.ExpiredMediaItems = rowsAffectedInt(res)

		res, err = tx.ExecContext(ctx, `
UPDATE collection_items ci
SET removed_at=$1
FROM media_items mi
WHERE ci.media_item_id=mi.id
  AND ci.removed_at IS NULL
  AND mi.retention_state='expired'`, now)
		if err != nil {
			return err
		}
		result.RemovedCollectionItems = rowsAffectedInt(res)

		res, err = tx.ExecContext(ctx, `
UPDATE collections c
SET status='archived', archived_at=$1, updated_at=$1
WHERE c.kind='user'
  AND c.status='active'
  AND NOT EXISTS (
      SELECT 1 FROM collection_items ci
      WHERE ci.collection_id=c.id AND ci.removed_at IS NULL
  )`, now)
		if err != nil {
			return err
		}
		result.ArchivedCollections = rowsAffectedInt(res)

		res, err = tx.ExecContext(ctx, `
UPDATE selections s
SET status='invalidated'
WHERE s.status='sealed'
  AND EXISTS (
      SELECT 1
      FROM selection_items si
      JOIN media_items mi ON mi.id=si.media_item_id
      WHERE si.selection_id=s.id AND mi.retention_state='expired'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM analysis_runs ar
      WHERE ar.selection_id=s.id AND ar.status IN ('queued','running','cancel_requested')
  )`)
		if err != nil {
			return err
		}
		result.InvalidatedSelections = rowsAffectedInt(res)

		res, err = tx.ExecContext(ctx, `
UPDATE analysis_runs
SET status='expired', completed_at=$1, version=version+1
WHERE expires_at IS NOT NULL
  AND expires_at <= $1
  AND status NOT IN ('succeeded','partially_succeeded','failed','canceled','expired')`, now)
		if err != nil {
			return err
		}
		result.ExpiredAnalysisRuns = rowsAffectedInt(res)

		res, err = tx.ExecContext(ctx, `
UPDATE artifacts
SET status='expired', retention_state='expired', deleted_at=$1
WHERE expires_at IS NOT NULL
  AND expires_at <= $1
  AND status NOT IN ('expired','deleted')`, now)
		if err != nil {
			return err
		}
		result.ExpiredArtifacts = rowsAffectedInt(res)
		return nil
	})
	if isLegacyRuntimeSchemaMismatch(err) {
		return RetentionSweepResult{}, nil
	}
	return result, err
}

func (s *SQLStateStore) DetectOrphanObjects(ctx context.Context) ([]OrphanObjectRecord, error) {
	if !s.hasLegacyRetentionSchema(ctx) {
		return []OrphanObjectRecord{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT 'source', s.id::text, s.owner_type, s.owner_id, COALESCE(s.tenant_id,''), 'sources', s.object_key,
       CASE WHEN mi.retention_state='expired' THEN 'expired_media_source' ELSE 'deleted_media_source' END
FROM sources s
JOIN media_items mi ON mi.source_id=s.id
WHERE s.object_key IS NOT NULL
  AND (mi.status='deleted' OR mi.retention_state IN ('expired','hard_delete_eligible'))
UNION ALL
SELECT 'artifact', a.id::text, a.owner_type, a.owner_id, COALESCE(a.tenant_id,''), 'artifacts', a.object_key,
       CASE WHEN a.status='expired' THEN 'expired_artifact' ELSE 'deleted_artifact' END
FROM artifacts a
WHERE a.object_key IS NOT NULL
  AND (a.status IN ('expired','deleted') OR a.retention_state IN ('expired','hard_delete_eligible'))
ORDER BY 1, 2`)
	if err != nil {
		if isLegacyRuntimeSchemaMismatch(err) {
			return []OrphanObjectRecord{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	var orphans []OrphanObjectRecord
	for rows.Next() {
		var orphan OrphanObjectRecord
		if err := rows.Scan(&orphan.SubjectType, &orphan.SubjectID, &orphan.Owner.OwnerType, &orphan.Owner.OwnerID, &orphan.Owner.TenantID, &orphan.Bucket, &orphan.ObjectKey, &orphan.Reason); err != nil {
			return nil, err
		}
		orphans = append(orphans, orphan)
	}
	return orphans, rows.Err()
}

func (s *SQLStateStore) hasLegacyRetentionSchema(ctx context.Context) bool {
	if !s.relationExists(ctx, "sources") ||
		!s.relationExists(ctx, "media_items") ||
		!s.relationExists(ctx, "selection_items") ||
		!s.relationExists(ctx, "selections") {
		return false
	}
	return s.columnExists(ctx, "artifacts", "owner_type", true) &&
		s.columnExists(ctx, "artifacts", "retention_state", true)
}

func (s *SQLStateStore) RecordOrphanObjectCleanup(ctx context.Context, orphan OrphanObjectRecord, deleted bool, message string, now time.Time) error {
	status := "hard_delete_eligible"
	if deleted {
		status = "deleted"
	}
	severity := "warning"
	code := "orphan_object_cleanup"
	if strings.Contains(strings.ToLower(message), "delete failed") {
		severity = "error"
		code = "orphan_object_cleanup_failed"
	}
	contextJSON, _ := json.Marshal(map[string]any{
		"bucket":     orphan.Bucket,
		"object_key": orphan.ObjectKey,
		"reason":     orphan.Reason,
		"deleted":    deleted,
	})
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		switch orphan.SubjectType {
		case "source":
			if _, err := tx.ExecContext(ctx, `
UPDATE media_items
SET retention_state=$1, updated_at=$2
WHERE source_id=$3::uuid`, status, now, orphan.SubjectID); err != nil {
				return err
			}
			if deleted {
				if _, err := tx.ExecContext(ctx, `UPDATE sources SET expires_at=$1 WHERE id=$2::uuid`, now, orphan.SubjectID); err != nil {
					return err
				}
			}
		case "artifact":
			if _, err := tx.ExecContext(ctx, `
UPDATE artifacts
SET status=CASE WHEN $1 THEN 'deleted' ELSE status END,
    retention_state=$2,
    deleted_at=COALESCE(deleted_at, $3)
WHERE id=$4::uuid`, deleted, status, now, orphan.SubjectID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unsupported orphan subject %q", ErrContractViolation, orphan.SubjectType)
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO diagnostics (id, owner_type, owner_id, tenant_id, subject_type, subject_id, severity, code, message, context, safe_adapter_context, created_at)
VALUES ($1,$2,$3,NULLIF($4,''),$5,$6::uuid,$7,$8,$9,$10,'{}'::jsonb,$11)
`, uuidString(), orphan.Owner.OwnerType, orphan.Owner.OwnerID, orphan.Owner.TenantID, orphan.SubjectType, orphan.SubjectID, severity, code, message, contextJSON, now)
		return err
	})
}

func (s *SQLStateStore) ListPendingEnqueueTasks(ctx context.Context, limit int) ([]AnalysisRunTaskRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if !s.relationExists(ctx, "analysis_run_tasks") {
		return []AnalysisRunTaskRecord{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT t.id, t.analysis_run_id, t.worker_kind, t.task_type, t.status, t.attempt_no, COALESCE(t.lease_owner,''), t.claimed_at, t.heartbeat_at, t.finalized_at, t.created_at
FROM analysis_run_tasks t
JOIN analysis_runs ar ON ar.id=t.analysis_run_id
WHERE t.status='pending_enqueue'
  AND ar.status NOT IN ('succeeded','partially_succeeded','failed','canceled','expired')
ORDER BY t.created_at ASC
LIMIT $1`, limit)
	if err != nil {
		if isLegacyRuntimeSchemaMismatch(err) {
			return []AnalysisRunTaskRecord{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	return scanAnalysisRunTasks(rows)
}

func (s *SQLStateStore) ListAnalysisRunQueue(ctx context.Context, status, runType, taskType string, limit int) ([]AnalysisRunQueueRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if !s.relationExists(ctx, "analysis_run_tasks") {
		return []AnalysisRunQueueRecord{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT t.analysis_run_id, ar.run_type, t.worker_kind, t.task_type, t.status, ar.version, t.attempt_no, t.created_at
FROM analysis_run_tasks t
JOIN analysis_runs ar ON ar.id=t.analysis_run_id
WHERE ($1='' OR t.status=$1)
  AND ($2='' OR ar.run_type=$2)
  AND ($3='' OR t.task_type=$3)
  AND ar.status NOT IN ('succeeded','partially_succeeded','failed','canceled','expired')
ORDER BY t.created_at ASC
LIMIT $4`, status, runType, taskType, limit)
	if err != nil {
		if isLegacyRuntimeSchemaMismatch(err) {
			return []AnalysisRunQueueRecord{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	records := []AnalysisRunQueueRecord{}
	for rows.Next() {
		var record AnalysisRunQueueRecord
		if err := rows.Scan(
			&record.AnalysisRunID,
			&record.RunType,
			&record.WorkerKind,
			&record.TaskType,
			&record.Status,
			&record.Version,
			&record.AttemptNo,
			&record.CreatedAt,
		); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *SQLStateStore) ListOperationalDiagnostics(ctx context.Context, codes []string) ([]DiagnosticRecord, error) {
	normalized := make([]string, 0, len(codes))
	for _, code := range codes {
		if code = strings.TrimSpace(code); code != "" {
			normalized = append(normalized, code)
		}
	}
	if len(normalized) == 0 {
		return []DiagnosticRecord{}, nil
	}
	if !s.columnExists(ctx, "diagnostics", "owner_type", true) {
		return []DiagnosticRecord{}, nil
	}
	placeholders := make([]string, 0, len(normalized))
	args := make([]any, 0, len(normalized))
	for idx, code := range normalized {
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx+1))
		args = append(args, code)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), subject_type, subject_id, severity, code, message, context, safe_adapter_context, COALESCE(correlation_id,''), COALESCE(remediation_hint,''), created_at
FROM diagnostics
WHERE code IN (`+strings.Join(placeholders, ",")+`)
ORDER BY created_at DESC`, args...)
	if err != nil {
		if isLegacyRuntimeSchemaMismatch(err) {
			return []DiagnosticRecord{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	diagnostics := []DiagnosticRecord{}
	for rows.Next() {
		var d DiagnosticRecord
		if err := rows.Scan(&d.ID, &d.Owner.OwnerType, &d.Owner.OwnerID, &d.Owner.TenantID, &d.SubjectType, &d.SubjectID, &d.Severity, &d.Code, &d.Message, &d.ContextJSON, &d.SafeAdapterJSON, &d.CorrelationID, &d.RemediationHint, &d.CreatedAt); err != nil {
			return nil, err
		}
		diagnostics = append(diagnostics, d)
	}
	return diagnostics, rows.Err()
}

func (s *SQLStateStore) MarkAnalysisRunTaskQueued(ctx context.Context, analysisRunID, taskType string, queuedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE analysis_run_tasks t
SET status='queued', heartbeat_at=$1
FROM analysis_runs ar
WHERE t.analysis_run_id=ar.id
  AND t.analysis_run_id=$2::uuid
  AND t.task_type=$3
  AND t.status='pending_enqueue'
  AND ar.status IN ('queued','running')`, queuedAt, analysisRunID, taskType)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return ErrExecutionNotFound
	}
	return nil
}

func (s *SQLStateStore) ClaimAnalysisRunTask(ctx context.Context, analysisRunID, workerKind, taskType, leaseOwner string, claimedAt time.Time) (AnalysisRunRecord, bool, error) {
	claimed := false
	var run AnalysisRunRecord
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
UPDATE analysis_run_tasks t
SET status='claimed', lease_owner=NULLIF($4,''), claimed_at=$5, heartbeat_at=$5
FROM analysis_runs ar
WHERE t.analysis_run_id=ar.id
  AND t.analysis_run_id=$1::uuid
  AND t.worker_kind=$2
  AND t.task_type=$3
  AND t.status IN ('queued','pending_enqueue')
  AND ar.status IN ('queued','running')
RETURNING ar.id, ar.owner_type, ar.owner_id, COALESCE(ar.tenant_id,''), ar.selection_id, ar.run_type, ar.status, ar.version, ar.params, ar.delivery, ar.evidence_gate_state, ar.created_at, ar.started_at, ar.completed_at, ar.canceled_at, ar.expires_at
`, analysisRunID, workerKind, taskType, leaseOwner, claimedAt)
		updated, err := scanAnalysisRun(row)
		if err == sql.ErrNoRows {
			existing, lookupErr := selectAnalysisRunByID(ctx, tx, analysisRunID)
			if lookupErr != nil {
				return lookupErr
			}
			run = existing
			return nil
		}
		if err != nil {
			return err
		}
		claimed = true
		if _, err := tx.ExecContext(ctx, `
UPDATE analysis_runs
SET status='running', started_at=COALESCE(started_at, $2), version=version+1
WHERE id=$1::uuid AND status='queued'`, analysisRunID, claimedAt); err != nil {
			return err
		}
		run, err = selectAnalysisRunByID(ctx, tx, updated.ID)
		return err
	})
	if err != nil {
		return AnalysisRunRecord{}, false, err
	}
	run.Selection, _ = s.GetSelection(ctx, run.Owner, run.SelectionID)
	return run, claimed, nil
}

func selectMediaItemHeader(ctx context.Context, tx *sql.Tx, owner OwnerScope, mediaItemID string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `
SELECT id FROM media_items
WHERE id=$1 AND owner_type=$2 AND owner_id=$3 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'') AND status <> 'deleted'
`, mediaItemID, owner.OwnerType, owner.OwnerID, owner.TenantID).Scan(&id)
	if err == sql.ErrNoRows {
		return "", ErrMediaItemNotFound
	}
	return id, err
}

func selectCollectionHeader(ctx context.Context, tx *sql.Tx, owner OwnerScope, collectionID string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `
SELECT id FROM collections
WHERE id=$1 AND owner_type=$2 AND owner_id=$3 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($4,''),'') AND status <> 'deleted'
FOR UPDATE
`, collectionID, owner.OwnerType, owner.OwnerID, owner.TenantID).Scan(&id)
	if err == sql.ErrNoRows {
		return "", ErrCollectionNotFound
	}
	return id, err
}

func insertCollectionMembership(ctx context.Context, tx *sql.Tx, collectionID, mediaItemID string, position int, addedBy string, addedAt time.Time) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO collection_items (id, collection_id, media_item_id, position, added_by, added_at)
VALUES ($1,$2,$3,$4,NULLIF($5,''),$6)`, uuidString(), collectionID, mediaItemID, position, addedBy, addedAt)
	return err
}

func appendCollectionMembership(ctx context.Context, tx *sql.Tx, collectionID, mediaItemID string, addedBy string, addedAt time.Time) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO collection_items (id, collection_id, media_item_id, position, added_by, added_at)
SELECT $1, $2, $3, COALESCE(MAX(position) + 1, 0), NULLIF($4,''), $5
FROM collection_items
WHERE collection_id=$2 AND removed_at IS NULL`, uuidString(), collectionID, mediaItemID, addedBy, addedAt)
	return err
}

func selectInboxCollection(ctx context.Context, tx *sql.Tx, owner OwnerScope) (CollectionRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), kind, name, status, version, created_at, updated_at, archived_at, deleted_at
FROM collections
WHERE owner_type=$1 AND owner_id=$2 AND COALESCE(tenant_id,'')=COALESCE(NULLIF($3,''),'') AND kind='inbox' AND status='active'
FOR UPDATE`, owner.OwnerType, owner.OwnerID, owner.TenantID)
	collection, err := scanCollectionHeader(row)
	if err == sql.ErrNoRows {
		return CollectionRecord{}, ErrCollectionNotFound
	}
	return collection, err
}

func selectAnalysisRunByID(ctx context.Context, tx *sql.Tx, analysisRunID string) (AnalysisRunRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), selection_id, run_type, status, version, params, delivery, evidence_gate_state, created_at, started_at, completed_at, canceled_at, expires_at
FROM analysis_runs
WHERE id=$1::uuid`, analysisRunID)
	run, err := scanAnalysisRun(row)
	if err == sql.ErrNoRows {
		return AnalysisRunRecord{}, ErrAnalysisRunNotFound
	}
	return run, err
}

func selectAnalysisRunByIDForUpdate(ctx context.Context, tx *sql.Tx, analysisRunID string) (AnalysisRunRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT id, owner_type, owner_id, COALESCE(tenant_id,''), selection_id, run_type, status, version, params, delivery, evidence_gate_state, created_at, started_at, completed_at, canceled_at, expires_at
FROM analysis_runs
WHERE id=$1::uuid
FOR UPDATE`, analysisRunID)
	run, err := scanAnalysisRun(row)
	if err == sql.ErrNoRows {
		return AnalysisRunRecord{}, ErrAnalysisRunNotFound
	}
	return run, err
}

func (s *SQLStateStore) listCollectionItems(ctx context.Context, collectionID string) ([]CollectionItemRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items WHERE collection_id=$1 AND removed_at IS NULL ORDER BY position ASC`, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []CollectionItemRecord{}
	for rows.Next() {
		var item CollectionItemRecord
		if err := rows.Scan(&item.MediaItemID, &item.Position, &item.AddedBy, &item.AddedAt, &item.RemovedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func mediaItemSelectSQL() string {
	return `
SELECT mi.id, mi.owner_type, mi.owner_id, COALESCE(mi.tenant_id,''), s.id, s.origin_type, COALESCE(s.external_uri,''), COALESCE(s.object_key,''), COALESCE(s.text_ref,''), COALESCE(s.checksum,''), s.size_bytes, COALESCE(s.mime_type,''), s.expires_at, mi.kind, mi.status, mi.display_name, COALESCE(mi.adapter_origin,''), mi.metadata, mi.retention_state, COALESCE(mi.retention_policy_id,''), mi.expires_at, mi.deleted_at, mi.created_at, mi.updated_at
FROM media_items mi
JOIN sources s ON s.id=mi.source_id
`
}

func scanMediaItem(row scanner) (MediaItemRecord, error) {
	var item MediaItemRecord
	var size sql.NullInt64
	if err := row.Scan(&item.ID, &item.Owner.OwnerType, &item.Owner.OwnerID, &item.Owner.TenantID, &item.Source.SourceID, &item.Source.OriginType, &item.Source.ExternalURI, &item.Source.ObjectKey, &item.Source.TextRef, &item.Source.Checksum, &size, &item.Source.MIMEType, &item.Source.ExpiresAt, &item.Kind, &item.Status, &item.DisplayName, &item.AdapterOrigin, &item.MetadataJSON, &item.Retention.State, &item.Retention.PolicyID, &item.Retention.ExpiresAt, &item.DeletedAt, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return MediaItemRecord{}, err
	}
	if size.Valid {
		item.Source.SizeBytes = &size.Int64
	}
	return item, nil
}

func scanCollectionHeader(row scanner) (CollectionRecord, error) {
	var c CollectionRecord
	err := row.Scan(&c.ID, &c.Owner.OwnerType, &c.Owner.OwnerID, &c.Owner.TenantID, &c.Kind, &c.Name, &c.Status, &c.Version, &c.CreatedAt, &c.UpdatedAt, &c.ArchivedAt, &c.DeletedAt)
	return c, err
}

func scanSelection(row scanner) (SelectionRecord, error) {
	var s SelectionRecord
	var diagnosticsJSON []byte
	err := row.Scan(&s.ID, &s.Owner.OwnerType, &s.Owner.OwnerID, &s.Owner.TenantID, &s.Status, &s.SourceCollectionID, &s.OptionSnapshotJSON, &s.CreatedBy, &diagnosticsJSON, &s.CreatedAt, &s.SealedAt)
	return s, err
}

func scanAnalysisRun(row scanner) (AnalysisRunRecord, error) {
	var run AnalysisRunRecord
	err := row.Scan(&run.ID, &run.Owner.OwnerType, &run.Owner.OwnerID, &run.Owner.TenantID, &run.SelectionID, &run.RunType, &run.Status, &run.Version, &run.ParamsJSON, &run.DeliveryJSON, &run.EvidenceGateState, &run.CreatedAt, &run.StartedAt, &run.CompletedAt, &run.CanceledAt, &run.ExpiresAt)
	return run, err
}

func artifactSelectSQL() string {
	return `
SELECT a.id, a.owner_type, a.owner_id, COALESCE(a.tenant_id,''), a.analysis_run_id, a.kind, a.status, COALESCE(a.object_key,''), a.content_type, COALESCE(a.checksum,''), a.size_bytes, a.visibility, a.preview, a.retention_state, COALESCE(a.retention_policy_id,''), a.created_at, a.expires_at, a.deleted_at
FROM artifacts a
`
}

func scanArtifact(row scanner) (ArtifactRecord, error) {
	var artifact ArtifactRecord
	err := row.Scan(&artifact.ID, &artifact.Owner.OwnerType, &artifact.Owner.OwnerID, &artifact.Owner.TenantID, &artifact.AnalysisRunID, &artifact.Kind, &artifact.Status, &artifact.ObjectKey, &artifact.ContentType, &artifact.Checksum, &artifact.SizeBytes, &artifact.Visibility, &artifact.PreviewJSON, &artifact.Retention.State, &artifact.Retention.PolicyID, &artifact.CreatedAt, &artifact.ExpiresAt, &artifact.DeletedAt)
	return artifact, err
}

func scanAnalysisRunTasks(rows *sql.Rows) ([]AnalysisRunTaskRecord, error) {
	tasks := []AnalysisRunTaskRecord{}
	for rows.Next() {
		var task AnalysisRunTaskRecord
		if err := rows.Scan(&task.ID, &task.AnalysisRunID, &task.WorkerKind, &task.TaskType, &task.Status, &task.AttemptNo, &task.LeaseOwner, &task.ClaimedAt, &task.HeartbeatAt, &task.FinalizedAt, &task.CreatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func rowsAffectedInt(result sql.Result) int {
	affected, _ := result.RowsAffected()
	return int(affected)
}

func firstNonZeroTime(candidate, fallback time.Time) time.Time {
	if candidate.IsZero() {
		return fallback
	}
	return candidate
}

func uuidString() string {
	return uuid.NewString()
}
