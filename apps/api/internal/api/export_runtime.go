package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

var exportFilenameSanitizer = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func (s *TargetRuntimeService) CreateExportJob(ctx context.Context, req TargetCreateExportJobRequest) (TargetExportJob, error) {
	if s.store == nil {
		return TargetExportJob{}, fmt.Errorf("target storage is required")
	}
	variant, err := normalizeExportVariant(req.Operation, req.Variant)
	if err != nil {
		return TargetExportJob{}, err
	}
	deliveryChannel := withDefaultString(strings.TrimSpace(req.DeliveryChannel), "telegram")
	if deliveryChannel != "telegram" && deliveryChannel != "web" {
		return TargetExportJob{}, storage.ContractViolationf("delivery_channel must be telegram or web")
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey != "" {
		existing, err := s.store.GetExportJobByIdempotency(ctx, req.ChannelAccountID, idempotencyKey)
		if err == nil {
			if !exportJobRequestMatches(existing, req.MediaAssetID, req.Operation, deliveryChannel, variant) {
				return TargetExportJob{}, storage.ErrExportJobConflict
			}
			return s.exportJobFromRecord(ctx, existing)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return TargetExportJob{}, err
		}
	}
	asset, err := s.store.GetMediaAsset(ctx, req.ChannelAccountID, req.MediaAssetID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrMediaAssetNotFound
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	if err := validateExportOperation(asset, req.Operation); err != nil {
		return TargetExportJob{}, err
	}
	if idempotencyKey == "" {
		idempotencyKey = "implicit-action:" + uuid.NewString()
	}
	now := s.now()
	jobID := s.nextID()
	params := targetstore.CreateExportJobParams{Job: targetstore.ExportJobRecord{
		ID: jobID, ChannelAccountID: req.ChannelAccountID, MediaAssetID: req.MediaAssetID,
		Operation: req.Operation, DeliveryChannel: deliveryChannel, VariantJSON: variant,
		Status: "queued", Version: 1, IdempotencyKey: idempotencyKey,
		MaxAttempts: 3, ProgressJSON: []byte(`{"stage":"queued","percent":0}`), CreatedAt: now,
	}}
	if asset.StoredObjectID != "" {
		params.SourcePin = targetstore.StoredObjectPinRecord{
			ID: s.nextID(), StoredObjectID: asset.StoredObjectID, OwnerType: "export_job",
			OwnerID: jobID, Purpose: "source", CreatedAt: now,
		}
	}
	record, err := s.store.CreateExportJob(ctx, params)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrStoredObjectUnavailable
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	if !exportJobRequestMatches(record, req.MediaAssetID, req.Operation, deliveryChannel, variant) {
		return TargetExportJob{}, storage.ErrExportJobConflict
	}
	return s.exportJobFromRecord(ctx, record)
}

func exportJobRequestMatches(record targetstore.ExportJobRecord, mediaAssetID, operation, deliveryChannel string, variant []byte) bool {
	return record.MediaAssetID == mediaAssetID && record.Operation == operation &&
		record.DeliveryChannel == deliveryChannel && jsonBytesEqual(record.VariantJSON, variant)
}

func (s *TargetRuntimeService) ListExportJobs(ctx context.Context, req TargetListExportJobsRequest) (TargetExportJobPage, error) {
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListExportJobs(ctx, req.ChannelAccountID, req.Status, limit)
	if err != nil {
		return TargetExportJobPage{}, err
	}
	items := make([]TargetExportJob, 0, len(records))
	for _, record := range records {
		item, err := s.exportJobFromRecord(ctx, record)
		if err != nil {
			return TargetExportJobPage{}, err
		}
		items = append(items, item)
	}
	return TargetExportJobPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) GetExportJob(ctx context.Context, req TargetGetExportJobRequest) (TargetExportJob, error) {
	record, err := s.store.GetExportJob(ctx, req.ChannelAccountID, req.ExportJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrExportJobNotFound
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	return s.exportJobFromRecord(ctx, record)
}

func (s *TargetRuntimeService) CancelExportJob(ctx context.Context, req TargetExportJobMutationRequest) (TargetExportJob, error) {
	record, err := s.store.RequestExportJobCancel(ctx, req.ChannelAccountID, req.ExportJobID, s.now())
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrExportJobNotFound
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	return s.exportJobFromRecord(ctx, record)
}

func (s *TargetRuntimeService) RetryExportJob(ctx context.Context, req TargetExportJobMutationRequest) (TargetExportJob, error) {
	current, err := s.store.GetExportJob(ctx, req.ChannelAccountID, req.ExportJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrExportJobNotFound
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	asset, err := s.store.GetMediaAsset(ctx, req.ChannelAccountID, current.MediaAssetID)
	if err != nil {
		return TargetExportJob{}, storage.ErrMediaAssetNotFound
	}
	pin := targetstore.StoredObjectPinRecord{}
	if asset.StoredObjectID != "" {
		pin = targetstore.StoredObjectPinRecord{ID: s.nextID(), StoredObjectID: asset.StoredObjectID}
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = "implicit-retry-action:" + uuid.NewString()
	}
	record, err := s.store.RetryExportJob(ctx, req.ChannelAccountID, req.ExportJobID, idempotencyKey, pin, s.now())
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrRetryRequiresTerminalStatus
	}
	if errors.Is(err, targetstore.ErrExportJobRetryIdempotencyConflict) {
		return TargetExportJob{}, storage.ErrExportJobConflict
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	return s.exportJobFromRecord(ctx, record)
}

func (s *TargetRuntimeService) ListExportJobQueue(ctx context.Context, req TargetExportQueueRequest) (TargetExportJobPage, error) {
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListExportJobQueue(ctx, limit)
	if err != nil {
		return TargetExportJobPage{}, err
	}
	items := make([]TargetExportJob, 0, len(records))
	for _, record := range records {
		item, err := s.exportJobFromRecord(ctx, record)
		if err != nil {
			return TargetExportJobPage{}, err
		}
		items = append(items, item)
	}
	return TargetExportJobPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) ClaimExportJob(ctx context.Context, req TargetClaimExportJobRequest) (TargetExportJobClaim, error) {
	leaseSeconds := req.LeaseSeconds
	if leaseSeconds <= 0 || leaseSeconds > 900 {
		leaseSeconds = 120
	}
	now := s.now()
	token := strings.ReplaceAll(uuid.NewString(), "-", "")
	record, claimed, err := s.store.ClaimExportJob(ctx, targetstore.ClaimExportJobParams{
		ExportJobID: req.ExportJobID, LeaseOwner: req.LeaseOwner, AttemptToken: token,
		ClaimedAt: now, LeaseExpiresAt: now.Add(time.Duration(leaseSeconds) * time.Second),
	})
	if err != nil {
		return TargetExportJobClaim{}, err
	}
	if !claimed {
		return TargetExportJobClaim{}, storage.ErrExportJobConflict
	}
	asset, err := s.store.GetMediaAsset(ctx, record.ChannelAccountID, record.MediaAssetID)
	if err != nil {
		return TargetExportJobClaim{}, storage.ErrMediaAssetNotFound
	}
	source, err := s.resolveExportSource(ctx, asset)
	if err != nil {
		return TargetExportJobClaim{}, err
	}
	job, err := s.exportJobFromRecord(ctx, record)
	if err != nil {
		return TargetExportJobClaim{}, err
	}
	return TargetExportJobClaim{
		ExportJob: job, AttemptToken: token, LeaseOwner: req.LeaseOwner,
		LeaseExpiresAt: *record.LeaseExpiresAt, Source: source,
	}, nil
}

func (s *TargetRuntimeService) CheckExportJobCancel(ctx context.Context, req TargetExportAttemptRequest) (TargetExportCancelState, error) {
	record, err := s.store.GetExportJobByID(ctx, req.ExportJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportCancelState{}, storage.ErrExportJobNotFound
	}
	if err != nil {
		return TargetExportCancelState{}, err
	}
	if record.LeaseOwner != req.LeaseOwner || record.AttemptToken != req.AttemptToken {
		return TargetExportCancelState{}, storage.ErrExportJobConflict
	}
	return TargetExportCancelState{
		CancelRequested: record.Status == "cancel_requested" || record.Status == "canceled",
		Status:          record.Status, CancelRequestedAt: record.CancelRequestedAt,
	}, nil
}

func (s *TargetRuntimeService) RecordExportJobProgress(ctx context.Context, req TargetRecordExportProgressRequest) error {
	if len(req.Progress) == 0 || !json.Valid(req.Progress) {
		return storage.ContractViolationf("progress must be a JSON object")
	}
	now := s.now()
	err := s.store.RecordExportJobProgress(ctx, targetstore.RecordExportJobProgressParams{
		ExportJobID: req.ExportJobID, LeaseOwner: req.LeaseOwner, AttemptToken: req.AttemptToken,
		ProgressJSON: req.Progress, HeartbeatAt: now,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrExportJobConflict
	}
	return err
}

func (s *TargetRuntimeService) FinalizeExportJob(ctx context.Context, req TargetFinalizeExportJobRequest) (TargetExportJob, error) {
	current, err := s.store.GetExportJobByID(ctx, req.ExportJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrExportJobNotFound
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	if current.LeaseOwner != req.LeaseOwner || current.AttemptToken != req.AttemptToken {
		return TargetExportJob{}, storage.ErrExportJobConflict
	}
	if current.Status == "cancel_requested" {
		req.Outcome = "canceled"
	} else if current.Status != "claimed" && current.Status != "running" {
		return TargetExportJob{}, storage.ErrExportJobConflict
	}
	now := s.now()
	params := targetstore.FinalizeExportJobParams{
		ExportJobID: req.ExportJobID, LeaseOwner: req.LeaseOwner, AttemptToken: req.AttemptToken,
		Status: req.Outcome, CompletedAt: now, RetentionDays: s.mediaObjectRetentionDays,
	}
	publishedOutput := targetstore.StoredObjectRecord{}
	if req.Outcome == "succeeded" {
		if req.Output == nil {
			return TargetExportJob{}, storage.ContractViolationf("successful export requires output")
		}
		output, err := s.publishExportOutput(ctx, current, req.AttemptToken, *req.Output, now)
		if err != nil {
			return TargetExportJob{}, err
		}
		publishedOutput = output
		deliveryExpiresAt := now.Add(s.exportDeliveryTTL)
		params.Output = output
		params.Delivery = targetstore.ExportDeliveryRecord{
			ID: s.nextID(), ExportJobID: current.ID, ChannelAccountID: current.ChannelAccountID,
			Channel: current.DeliveryChannel, Status: "pending", Version: 1,
			MaxAttempts: s.exportDeliveryMaxAttempts, ExpiresAt: deliveryExpiresAt, CreatedAt: now,
		}
		params.DeliveryPin = targetstore.StoredObjectPinRecord{
			ID: s.nextID(), StoredObjectID: output.ID, OwnerType: "export_delivery",
			OwnerID: params.Delivery.ID, Purpose: "delivery", ExpiresAt: &deliveryExpiresAt, CreatedAt: now,
		}
	} else if req.Outcome == "failed" && req.DiagnosticCode != "" {
		diagnosticID := s.nextID()
		if err := s.store.RecordDiagnostics(ctx, []targetstore.DiagnosticRecord{{
			ID: diagnosticID, ChannelAccountID: current.ChannelAccountID, SubjectType: "export_job",
			SubjectID: current.ID, Severity: "error", Code: req.DiagnosticCode,
			Message: req.DiagnosticMessage, CreatedAt: now,
		}}); err != nil {
			return TargetExportJob{}, err
		}
		params.DiagnosticID = diagnosticID
	}
	record, err := s.store.FinalizeExportJob(ctx, params)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportJob{}, storage.ErrExportJobConflict
	}
	if err != nil {
		return TargetExportJob{}, err
	}
	if publishedOutput.ID != "" && record.OutputStoredObjectID != "" {
		if registered, getErr := s.store.GetStoredObject(ctx, record.OutputStoredObjectID); getErr == nil && registered.ObjectKey != publishedOutput.ObjectKey {
			if objects, ok := s.objects.(storage.ManagedObjectStore); ok {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_ = objects.DeleteObject(cleanupCtx, publishedOutput.Bucket, publishedOutput.ObjectKey)
				cancel()
			}
		}
	}
	return s.exportJobFromRecord(ctx, record)
}

func (s *TargetRuntimeService) ReclaimExportJobs(ctx context.Context, req TargetExportReclaimRequest) (TargetExportReclaimResult, error) {
	batch := req.BatchSize
	if batch <= 0 || batch > 1000 {
		batch = 100
	}
	now := s.now()
	jobs, err := s.store.ReclaimExportJobs(ctx, now, batch)
	if err != nil {
		return TargetExportReclaimResult{}, err
	}
	if _, err := s.store.ReclaimExportDeliveries(ctx, now, batch); err != nil {
		return TargetExportReclaimResult{}, err
	}
	return TargetExportReclaimResult{
		Examined: jobs.Examined,
		Requeued: jobs.Requeued,
		Failed:   jobs.Failed,
	}, nil
}

func (s *TargetRuntimeService) ClaimExportDelivery(ctx context.Context, req TargetClaimExportDeliveryRequest) (TargetExportDeliveryClaim, error) {
	leaseSeconds := req.LeaseSeconds
	if leaseSeconds <= 0 || leaseSeconds > 900 {
		leaseSeconds = 120
	}
	now := s.now()
	token := strings.ReplaceAll(uuid.NewString(), "-", "")
	delivery, claimed, err := s.store.ClaimExportDelivery(ctx, targetstore.ClaimExportDeliveryParams{
		ExportJobID: req.ExportJobID, ChannelAccountID: req.ChannelAccountID,
		Channel: req.Channel, LeaseOwner: req.LeaseOwner, AttemptToken: token,
		ClaimedAt: now, LeaseExpiresAt: now.Add(time.Duration(leaseSeconds) * time.Second),
	})
	if err != nil {
		return TargetExportDeliveryClaim{}, err
	}
	if !claimed {
		return TargetExportDeliveryClaim{}, storage.ErrExportJobConflict
	}
	return TargetExportDeliveryClaim{
		Delivery: exportDeliveryFromRecord(delivery), AttemptToken: token,
		LeaseOwner: req.LeaseOwner, LeaseExpiresAt: *delivery.LeaseExpiresAt,
	}, nil
}

func (s *TargetRuntimeService) HeartbeatExportDelivery(ctx context.Context, req TargetHeartbeatExportDeliveryRequest) (TargetExportDeliveryClaim, error) {
	if strings.TrimSpace(req.ChannelAccountID) == "" || strings.TrimSpace(req.ExportJobID) == "" ||
		strings.TrimSpace(req.ExportDeliveryID) == "" || strings.TrimSpace(req.LeaseOwner) == "" ||
		strings.TrimSpace(req.AttemptToken) == "" {
		return TargetExportDeliveryClaim{}, storage.ContractViolationf("export delivery heartbeat fence is required")
	}
	if len(req.LeaseOwner) > 160 || len(req.AttemptToken) < 16 || len(req.AttemptToken) > 160 {
		return TargetExportDeliveryClaim{}, storage.ContractViolationf("export delivery heartbeat fence is invalid")
	}
	leaseSeconds := 120
	if req.LeaseSeconds != nil {
		leaseSeconds = *req.LeaseSeconds
		if leaseSeconds < 1 || leaseSeconds > 900 {
			return TargetExportDeliveryClaim{}, storage.ContractViolationf("lease_seconds must be between 1 and 900")
		}
	}
	now := s.now()
	delivery, err := s.store.HeartbeatExportDelivery(ctx, targetstore.HeartbeatExportDeliveryParams{
		ExportJobID: req.ExportJobID, ChannelAccountID: req.ChannelAccountID,
		ExportDeliveryID: req.ExportDeliveryID, LeaseOwner: req.LeaseOwner, AttemptToken: req.AttemptToken,
		HeartbeatAt: now, LeaseExpiresAt: now.Add(time.Duration(leaseSeconds) * time.Second),
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportDeliveryClaim{}, storage.ErrExportJobConflict
	}
	if err != nil {
		return TargetExportDeliveryClaim{}, err
	}
	if delivery.ID != req.ExportDeliveryID || delivery.LeaseExpiresAt == nil {
		return TargetExportDeliveryClaim{}, storage.ErrExportJobConflict
	}
	return TargetExportDeliveryClaim{
		Delivery: exportDeliveryFromRecord(delivery), AttemptToken: req.AttemptToken,
		LeaseOwner: req.LeaseOwner, LeaseExpiresAt: *delivery.LeaseExpiresAt,
	}, nil
}

func (s *TargetRuntimeService) FinalizeExportDelivery(ctx context.Context, req TargetFinalizeExportDeliveryRequest) (TargetExportDelivery, error) {
	delivery, err := s.store.FinalizeExportDelivery(ctx, targetstore.FinalizeExportDeliveryParams{
		ExportJobID: req.ExportJobID, ChannelAccountID: req.ChannelAccountID,
		ExportDeliveryID: req.ExportDeliveryID, LeaseOwner: req.LeaseOwner, AttemptToken: req.AttemptToken,
		Status: req.Status, FailureCode: req.FailureCode, Retryable: req.Retryable, FinalizedAt: s.now(),
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportDelivery{}, storage.ErrExportJobConflict
	}
	if err != nil {
		return TargetExportDelivery{}, err
	}
	if delivery.ID != req.ExportDeliveryID {
		return TargetExportDelivery{}, storage.ErrExportJobConflict
	}
	return exportDeliveryFromRecord(delivery), nil
}

func (s *TargetRuntimeService) ResolveExportDownload(ctx context.Context, req TargetGetExportJobRequest) (TargetExportDownload, error) {
	return s.resolveExportDownload(ctx, req, false)
}

func (s *TargetRuntimeService) ResolveInternalExportDownloadAccess(ctx context.Context, req TargetGetExportJobRequest) (TargetExportDownload, error) {
	return s.resolveExportDownload(ctx, req, true)
}

func (s *TargetRuntimeService) resolveExportDownload(ctx context.Context, req TargetGetExportJobRequest, internal bool) (TargetExportDownload, error) {
	job, err := s.store.GetExportJob(ctx, req.ChannelAccountID, req.ExportJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportDownload{}, storage.ErrExportJobNotFound
	}
	if err != nil {
		return TargetExportDownload{}, err
	}
	if job.Status != "succeeded" || job.OutputStoredObjectID == "" {
		return TargetExportDownload{}, storage.ErrExportJobNotFound
	}
	object, err := s.store.GetStoredObject(ctx, job.OutputStoredObjectID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportDownload{}, storage.ErrStoredObjectUnavailable
	}
	if err != nil {
		return TargetExportDownload{}, err
	}
	if object.StorageStatus != "available" {
		return TargetExportDownload{}, storage.ErrStoredObjectUnavailable
	}
	ttl := s.exportWebAccessTTL
	if object.ExpiresAt != nil && time.Until(*object.ExpiresAt) < ttl {
		ttl = time.Until(*object.ExpiresAt)
	}
	if ttl <= 0 {
		return TargetExportDownload{}, storage.ErrStoredObjectUnavailable
	}
	download, err := s.presignArtifactDownload(ctx, object, ttl, internal)
	if err != nil {
		return TargetExportDownload{}, err
	}
	return TargetExportDownload{
		ExportJobID: job.ID, Filename: exportFilename(job, object),
		ContentType: object.ContentType, SizeBytes: object.SizeBytes,
		URL: download.URL, ExpiresAt: download.ExpiresAt,
	}, nil
}

func (s *TargetRuntimeService) SweepRetention(ctx context.Context, req TargetRetentionSweepRequest) (TargetRetentionSweepResult, error) {
	objects, ok := s.objects.(storage.ManagedObjectStore)
	if !ok {
		return TargetRetentionSweepResult{}, storage.ContractViolationf("managed object store is required")
	}
	batch := req.BatchSize
	if batch <= 0 || batch > 1000 {
		batch = 100
	}
	claimSeconds := req.ClaimSeconds
	if claimSeconds <= 0 || claimSeconds > 900 {
		claimSeconds = 120
	}
	owner := strings.TrimSpace(req.DeletionOwner)
	if owner == "" {
		owner = "api-retention"
	}
	now := s.now()
	token := strings.ReplaceAll(uuid.NewString(), "-", "")
	leaseExpiresAt := now.Add(time.Duration(claimSeconds) * time.Second)
	claims, err := s.store.ClaimRetentionDeletes(ctx, owner, token, now, leaseExpiresAt, batch)
	if err != nil {
		return TargetRetentionSweepResult{}, err
	}
	result := TargetRetentionSweepResult{Claimed: len(claims), Claims: make([]TargetRetentionClaim, 0, len(claims))}
	for _, claim := range claims {
		result.Claims = append(result.Claims, TargetRetentionClaim{
			StoredObjectID: claim.StoredObject.ID, Generation: claim.StoredObject.Generation,
			DeletionOwner: owner, DeletionToken: token, LeaseExpiresAt: leaseExpiresAt,
		})
		if err := objects.DeleteObject(ctx, claim.StoredObject.Bucket, claim.StoredObject.ObjectKey); err != nil {
			result.Failed++
			failedAt := s.now()
			if claim.StoredObject.ChannelAccountID != "" {
				_ = s.store.RecordDiagnostics(ctx, []targetstore.DiagnosticRecord{{
					ID: s.nextID(), ChannelAccountID: claim.StoredObject.ChannelAccountID,
					SubjectType: "stored_object", SubjectID: claim.StoredObject.ID,
					Severity: "error", Code: "retention_delete_failed",
					Message: "Object deletion failed and was scheduled for retry", CreatedAt: failedAt,
				}})
			}
			_ = s.store.FailRetentionDelete(ctx, claim.StoredObject.ID, claim.StoredObject.Generation, owner, token, failedAt)
			continue
		}
		if err := s.store.CompleteRetentionDelete(ctx, claim.StoredObject.ID, claim.StoredObject.Generation, owner, token, s.now()); err != nil {
			result.Failed++
			continue
		}
		result.Deleted++
	}
	return result, nil
}

func (s *TargetRuntimeService) ReconcileRetention(ctx context.Context, req TargetRetentionReconcileRequest) (TargetRetentionReconcileResult, error) {
	objects, ok := s.objects.(storage.ManagedObjectStore)
	if !ok {
		return TargetRetentionReconcileResult{}, storage.ContractViolationf("managed object store is required")
	}
	batch := req.BatchSize
	if batch <= 0 || batch > 1000 {
		batch = 100
	}
	now := s.now()
	result := TargetRetentionReconcileResult{}
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	dbCursor, err := s.store.GetReconcileCursor(ctx, "stored_objects")
	if err != nil {
		return result, err
	}
	records, err := s.store.ListStoredObjectsForReconcile(ctx, dbCursor, batch)
	if err != nil {
		return result, err
	}
	for _, record := range records {
		result.Examined++
		switch record.StorageStatus {
		case "publishing":
			reconciled, missing, err := s.reconcilePublishingObject(ctx, objects, record, now, req.DryRun)
			if err != nil {
				return result, err
			}
			if reconciled {
				result.PublicationsReconciled++
			}
			if missing {
				result.ObjectsMarkedMissing++
			}
		case "available":
			published, err := objects.StatObject(ctx, record.Bucket, record.ObjectKey)
			if errors.Is(err, storage.ErrObjectNotFound) {
				if !req.DryRun {
					if err := s.store.MarkStoredObjectMissing(ctx, record.ID, record.Generation, now); err != nil && !errors.Is(err, sql.ErrNoRows) {
						return result, err
					}
				}
				result.ObjectsMarkedMissing++
			} else if err != nil {
				return result, err
			} else if published.SizeBytes != record.SizeBytes || storedObjectChecksumMismatch(record, published.Metadata) {
				if !req.DryRun {
					if err := s.store.MarkStoredObjectMissing(ctx, record.ID, record.Generation, now); err != nil && !errors.Is(err, sql.ErrNoRows) {
						return result, err
					}
				}
				result.ObjectsMarkedMissing++
			}
		}
	}
	nextDBCursor := ""
	if len(records) == batch {
		nextDBCursor = records[len(records)-1].ID
	}
	if !req.DryRun {
		if err := s.store.SetReconcileCursor(ctx, "stored_objects", nextDBCursor, now); err != nil {
			return result, err
		}
	}

	prefixes := []struct {
		bucket string
		prefix string
	}{
		{storage.SourcesBucket, "staging/uploads/"},
		{storage.SourcesBucket, "sources/uploads/"},
		{storage.ArtifactsBucket, "transient/staging/"},
		{storage.ArtifactsBucket, "transient/exports/"},
	}
	for _, managed := range prefixes {
		cursorKey := managed.bucket + "/" + managed.prefix
		objectCursor, err := s.store.GetReconcileCursor(ctx, cursorKey)
		if err != nil {
			return result, err
		}
		entries, err := objects.ListObjects(ctx, managed.bucket, managed.prefix, objectCursor, batch)
		if err != nil {
			return result, err
		}
		for _, entry := range entries {
			result.Examined++
			if entry.LastModified.IsZero() || now.Sub(entry.LastModified) < s.objectOrphanGrace {
				continue
			}
			record, err := s.store.FindStoredObjectByLocation(ctx, entry.Bucket, entry.ObjectKey)
			if err == nil && record.StorageStatus != "missing" && record.StorageStatus != "deleted" {
				continue
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return result, err
			}
			if !req.DryRun {
				deleteToken := strings.ReplaceAll(uuid.NewString(), "-", "")
				claimed, err := s.store.ClaimObjectDeleteFence(ctx, entry.Bucket, entry.ObjectKey, deleteToken, now, now.Add(2*time.Minute))
				if err != nil {
					return result, err
				}
				if !claimed {
					continue
				}
				if err := objects.DeleteObject(ctx, entry.Bucket, entry.ObjectKey); err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
					_ = s.store.ReleaseObjectDeleteFence(ctx, entry.Bucket, entry.ObjectKey, deleteToken)
					return result, err
				}
				if err := s.store.ReleaseObjectDeleteFence(ctx, entry.Bucket, entry.ObjectKey, deleteToken); err != nil {
					return result, err
				}
			}
			result.OrphansDeleted++
		}
		nextObjectCursor := ""
		if len(entries) == batch {
			nextObjectCursor = entries[len(entries)-1].ObjectKey
		}
		if !req.DryRun {
			if err := s.store.SetReconcileCursor(ctx, cursorKey, nextObjectCursor, now); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func (s *TargetRuntimeService) reconcilePublishingObject(
	ctx context.Context,
	objects storage.ManagedObjectStore,
	record targetstore.StoredObjectRecord,
	now time.Time,
	dryRun bool,
) (bool, bool, error) {
	if published, err := objects.StatObject(ctx, record.Bucket, record.ObjectKey); err == nil {
		if published.SizeBytes != record.SizeBytes || !metadataSHA256Matches(published.Metadata, strings.TrimPrefix(record.Checksum, "sha256:")) {
			return false, false, storage.ContractViolationf("published object size or sha256 does not match reservation")
		}
		if !dryRun {
			if err := s.store.CompleteStoredObjectPublication(ctx, record.ID, record.Generation, record.StagingKey, now); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return false, false, err
			}
		}
		return true, false, nil
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return false, false, err
	}
	if record.StagingKey != "" {
		staged, err := objects.StatObject(ctx, record.Bucket, record.StagingKey)
		if err == nil {
			if staged.SizeBytes != record.SizeBytes {
				return false, false, storage.ContractViolationf("staged object size does not match reservation")
			}
			if dryRun {
				return true, false, nil
			}
			sha := strings.TrimPrefix(record.Checksum, "sha256:")
			promoteErr := objects.PromoteObject(
				ctx,
				record.Bucket,
				record.StagingKey,
				record.ObjectKey,
				map[string]string{"sha256": sha},
			)
			published, statErr := objects.StatObject(ctx, record.Bucket, record.ObjectKey)
			if statErr != nil {
				if promoteErr != nil {
					return false, false, promoteErr
				}
				return false, false, statErr
			}
			if published.SizeBytes != record.SizeBytes || !metadataSHA256Matches(published.Metadata, sha) {
				return false, false, storage.ContractViolationf("promoted object size or sha256 does not match reservation")
			}
			if err := s.store.CompleteStoredObjectPublication(ctx, record.ID, record.Generation, record.StagingKey, now); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return false, false, err
			}
			return true, false, nil
		}
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return false, false, err
		}
	}
	if now.Sub(record.CreatedAt) < s.objectOrphanGrace {
		return false, false, nil
	}
	if !dryRun {
		if err := s.store.MarkStoredObjectMissing(ctx, record.ID, record.Generation, now); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, false, err
		}
	}
	return false, true, nil
}

func (s *TargetRuntimeService) resolveExportSource(ctx context.Context, asset targetstore.MediaAssetRecord) (TargetExportSource, error) {
	if asset.StoredObjectID == "" {
		canonical, err := canonicalYouTubeURL(asset.OriginRef)
		if err != nil {
			return TargetExportSource{}, err
		}
		return TargetExportSource{
			MediaAssetID: asset.ID, SourceType: "remote_reference", URL: canonical,
			ExpiresAt: s.now().Add(15 * time.Minute),
		}, nil
	}
	object, err := s.store.GetStoredObject(ctx, asset.StoredObjectID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetExportSource{}, storage.ErrStoredObjectUnavailable
	}
	if err != nil {
		return TargetExportSource{}, err
	}
	if object.StorageStatus != "available" {
		return TargetExportSource{}, storage.ErrStoredObjectUnavailable
	}
	download, err := s.presignArtifactDownload(ctx, object, 15*time.Minute, true)
	if err != nil {
		return TargetExportSource{}, err
	}
	return TargetExportSource{
		MediaAssetID: asset.ID, SourceType: "uploaded_object", URL: download.URL,
		ExpiresAt: download.ExpiresAt, ContentType: object.ContentType, SizeBytes: object.SizeBytes,
	}, nil
}

func (s *TargetRuntimeService) publishExportOutput(ctx context.Context, job targetstore.ExportJobRecord, attemptToken string, publication TargetExportPublication, now time.Time) (targetstore.StoredObjectRecord, error) {
	objects, ok := s.objects.(storage.ManagedObjectStore)
	if !ok {
		return targetstore.StoredObjectRecord{}, storage.ContractViolationf("managed object store is required")
	}
	sha := strings.ToLower(strings.TrimSpace(publication.SHA256))
	if decoded, err := hex.DecodeString(sha); err != nil || len(decoded) != 32 {
		return targetstore.StoredObjectRecord{}, storage.ContractViolationf("output sha256 is invalid")
	}
	expectedPrefix := path.Join("transient/staging", job.ID, attemptToken) + "/"
	if !strings.HasPrefix(publication.StagingKey, expectedPrefix) {
		return targetstore.StoredObjectRecord{}, storage.ContractViolationf("output staging key is outside the attempt prefix")
	}
	staged, err := objects.StatObject(ctx, storage.ArtifactsBucket, publication.StagingKey)
	if err != nil {
		return targetstore.StoredObjectRecord{}, fmt.Errorf("%w: stat staged output: %v", storage.ErrStorageUnavailable, err)
	}
	if staged.SizeBytes != publication.SizeBytes || !metadataSHA256Matches(staged.Metadata, sha) {
		return targetstore.StoredObjectRecord{}, storage.ContractViolationf("staged output size or sha256 metadata does not match")
	}
	filename := sanitizeExportFilename(publication.Filename, job.Operation)
	objectKey := path.Join(
		"transient/exports",
		job.ID,
		fmt.Sprintf("%d", job.RetryGeneration),
		fmt.Sprintf("%d", job.AttemptNo),
		attemptTokenPathSegment(attemptToken),
		filename,
	)
	if err := objects.PromoteObject(ctx, storage.ArtifactsBucket, publication.StagingKey, objectKey, nil); err != nil {
		return targetstore.StoredObjectRecord{}, fmt.Errorf("%w: promote export output: %v", storage.ErrStorageUnavailable, err)
	}
	published, err := objects.StatObject(ctx, storage.ArtifactsBucket, objectKey)
	if err != nil || published.SizeBytes != publication.SizeBytes || !metadataSHA256Matches(published.Metadata, sha) {
		return targetstore.StoredObjectRecord{}, fmt.Errorf("%w: verify promoted export output", storage.ErrStorageUnavailable)
	}
	expiresAt := now.Add(time.Duration(s.mediaObjectRetentionDays) * 24 * time.Hour)
	return targetstore.StoredObjectRecord{
		ID:               stableTargetID(strings.Join([]string{"export-output", job.ID, attemptToken}, ":")),
		ChannelAccountID: job.ChannelAccountID, Bucket: storage.ArtifactsBucket,
		ObjectKey: objectKey, Generation: 1, GenerationPublishedAt: now,
		ContentType: publication.ContentType, SizeBytes: publication.SizeBytes,
		ChecksumAlgorithm: "sha256", Checksum: "sha256:" + sha,
		StorageStatus: "available", RetentionState: "expires_scheduled", HoldState: "none",
		CreatedAt: now, ExpiresAt: &expiresAt,
	}, nil
}

func attemptTokenPathSegment(attemptToken string) string {
	digest := sha256.Sum256([]byte(attemptToken))
	return hex.EncodeToString(digest[:8])
}

func (s *TargetRuntimeService) exportJobFromRecord(ctx context.Context, record targetstore.ExportJobRecord) (TargetExportJob, error) {
	deliveries, err := s.store.ListExportDeliveries(ctx, record.ChannelAccountID, record.ID)
	if err != nil {
		return TargetExportJob{}, err
	}
	dto := TargetExportJob{
		ExportJobID: record.ID, ChannelAccountID: record.ChannelAccountID,
		MediaAssetID: record.MediaAssetID, Operation: record.Operation,
		Variant: record.VariantJSON, Status: record.Status, Version: record.Version,
		RetryGeneration: record.RetryGeneration, AttemptNo: record.AttemptNo,
		MaxAttempts: record.MaxAttempts, Progress: record.ProgressJSON,
		Deliveries: make([]TargetExportDelivery, 0, len(deliveries)), CreatedAt: record.CreatedAt,
		StartedAt: record.StartedAt, CompletedAt: record.CompletedAt,
		CancelRequestedAt: record.CancelRequestedAt, CanceledAt: record.CanceledAt, ExpiresAt: record.ExpiresAt,
	}
	for _, delivery := range deliveries {
		dto.Deliveries = append(dto.Deliveries, exportDeliveryFromRecord(delivery))
	}
	if record.OutputStoredObjectID != "" {
		object, err := s.store.GetStoredObject(ctx, record.OutputStoredObjectID)
		if err != nil {
			return TargetExportJob{}, err
		}
		dto.Output = &TargetExportOutput{
			ContentType: object.ContentType, Filename: exportFilename(record, object),
			SizeBytes: object.SizeBytes, SHA256: strings.TrimPrefix(object.Checksum, "sha256:"),
		}
	}
	return dto, nil
}

func exportDeliveryFromRecord(record targetstore.ExportDeliveryRecord) TargetExportDelivery {
	return TargetExportDelivery{
		ExportDeliveryID: record.ID, ExportJobID: record.ExportJobID,
		ChannelAccountID: record.ChannelAccountID, Channel: record.Channel,
		Status: record.Status, Version: record.Version, AttemptNo: record.AttemptNo,
		MaxAttempts: record.MaxAttempts, LeaseExpiresAt: record.LeaseExpiresAt,
		NextAttemptAt: record.NextAttemptAt,
		ExpiresAt:     record.ExpiresAt, DeliveredAt: record.DeliveredAt,
		FailureCode: record.FailureCode, CreatedAt: record.CreatedAt,
	}
}

func normalizeExportVariant(operation string, raw json.RawMessage) ([]byte, error) {
	var variant map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &variant) != nil || len(variant) != 1 {
		return nil, storage.ContractViolationf("variant must contain exactly one semantic quality")
	}
	switch operation {
	case "youtube_audio", "video_to_audio":
		value, ok := variant["audio_bitrate_kbps"].(float64)
		if !ok || !containsInt([]int{64, 96, 128, 192, 256}, int(value)) || value != float64(int(value)) {
			return nil, storage.ContractViolationf("audio_bitrate_kbps is unsupported")
		}
	case "youtube_video":
		value, ok := variant["video_quality"].(string)
		if !ok || !containsString([]string{"360p", "480p", "720p", "1080p"}, value) {
			return nil, storage.ContractViolationf("video_quality is unsupported")
		}
	default:
		return nil, storage.ContractViolationf("export operation is unsupported")
	}
	return json.Marshal(variant)
}

func validateExportOperation(asset targetstore.MediaAssetRecord, operation string) error {
	switch operation {
	case "youtube_audio", "youtube_video":
		if asset.OriginType != "url" {
			return storage.ContractViolationf("YouTube export requires a YouTube URL material")
		}
		_, err := canonicalYouTubeURL(asset.OriginRef)
		return err
	case "video_to_audio":
		if asset.Kind != "video" || asset.StoredObjectID == "" {
			return storage.ContractViolationf("video_to_audio requires an uploaded video")
		}
		return nil
	default:
		return storage.ContractViolationf("export operation is unsupported")
	}
}

func canonicalYouTubeURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" {
		return "", storage.ContractViolationf("YouTube URL must use HTTPS")
	}
	host := strings.ToLower(parsed.Hostname())
	videoID := ""
	switch {
	case host == "youtu.be":
		videoID = strings.Trim(strings.TrimPrefix(parsed.Path, "/"), " ")
		videoID = strings.Split(videoID, "/")[0]
	case host == "youtube.com" || host == "www.youtube.com" || host == "m.youtube.com":
		videoID = parsed.Query().Get("v")
		if videoID == "" && strings.HasPrefix(parsed.Path, "/shorts/") {
			videoID = strings.TrimPrefix(parsed.Path, "/shorts/")
			videoID = strings.Split(videoID, "/")[0]
		}
	default:
		return "", storage.ContractViolationf("unsupported YouTube host")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`).MatchString(videoID) {
		return "", storage.ContractViolationf("invalid YouTube video id")
	}
	return "https://www.youtube.com/watch?v=" + videoID, nil
}

func exportFilename(job targetstore.ExportJobRecord, object targetstore.StoredObjectRecord) string {
	ext := path.Ext(object.ObjectKey)
	if ext == "" {
		if strings.Contains(object.ContentType, "audio") {
			ext = ".mp3"
		} else {
			ext = ".mp4"
		}
	}
	jobPrefix := job.ID
	if len(jobPrefix) > 8 {
		jobPrefix = jobPrefix[:8]
	}
	return "export-" + jobPrefix + ext
}

func sanitizeExportFilename(filename, operation string) string {
	filename = exportFilenameSanitizer.ReplaceAllString(path.Base(filename), "_")
	filename = strings.Trim(filename, "._-")
	if filename == "" {
		if operation == "youtube_video" {
			return "video.mp4"
		}
		return "audio.mp3"
	}
	return filename
}

func metadataSHA256Matches(metadata map[string]string, expected string) bool {
	for key, value := range metadata {
		if strings.EqualFold(key, "sha256") || strings.EqualFold(key, "x-amz-meta-sha256") {
			return strings.EqualFold(strings.TrimSpace(value), expected)
		}
	}
	return false
}

func storedObjectChecksumMismatch(record targetstore.StoredObjectRecord, metadata map[string]string) bool {
	checksum := strings.TrimSpace(record.Checksum)
	if checksum == "" {
		return false
	}
	return !metadataSHA256Matches(metadata, strings.TrimPrefix(checksum, "sha256:"))
}

func jsonBytesEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

func containsInt(values []int, candidate int) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
