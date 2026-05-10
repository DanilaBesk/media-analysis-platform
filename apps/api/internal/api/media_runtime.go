package api

import (
	"context"
	"encoding/json"

	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

type finalRuntimeStorageService interface {
	AddMediaItem(ctx context.Context, req storage.AddMediaItemRequest) (storage.MediaItemRecord, error)
	ListMediaItems(ctx context.Context, owner storage.OwnerScope) ([]storage.MediaItemRecord, error)
	GetMediaItem(ctx context.Context, owner storage.OwnerScope, mediaItemID string) (storage.MediaItemRecord, error)
	RemoveMediaItem(ctx context.Context, owner storage.OwnerScope, mediaItemID string) (storage.MediaItemRecord, error)
	CreateCollection(ctx context.Context, req storage.CreateCollectionRequest) (storage.CollectionRecord, error)
	ListCollections(ctx context.Context, owner storage.OwnerScope) ([]storage.CollectionRecord, error)
	GetCollection(ctx context.Context, owner storage.OwnerScope, collectionID string) (storage.CollectionRecord, error)
	UpdateCollection(ctx context.Context, req storage.UpdateCollectionRequest) (storage.CollectionRecord, error)
	UpdateCollectionItems(ctx context.Context, req storage.UpdateCollectionItemsRequest) (storage.CollectionRecord, error)
	CreateSelection(ctx context.Context, req storage.CreateSelectionRequest) (storage.SelectionRecord, error)
	GetSelection(ctx context.Context, owner storage.OwnerScope, selectionID string) (storage.SelectionRecord, error)
	CreateAnalysisRun(ctx context.Context, req storage.CreateAnalysisRunRequest) (storage.AnalysisRunRecord, error)
	CancelAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID, message string) (storage.AnalysisRunRecord, error)
	RetryAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID, idempotencyKey string) (storage.AnalysisRunRecord, error)
	GetAnalysisRunByID(ctx context.Context, analysisRunID string) (storage.AnalysisRunRecord, error)
	ListAnalysisRuns(ctx context.Context, owner storage.OwnerScope) ([]storage.AnalysisRunRecord, error)
	GetAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID string) (storage.AnalysisRunRecord, error)
	ListAnalysisRunEvents(ctx context.Context, owner storage.OwnerScope, analysisRunID string) ([]storage.RunEventRecord, error)
	ListArtifacts(ctx context.Context, owner storage.OwnerScope, analysisRunID string) ([]storage.ArtifactRecord, error)
	GetArtifact(ctx context.Context, owner storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error)
	GetInternalArtifactDownloadAccess(ctx context.Context, artifactID string) (storage.ArtifactRecord, error)
	RefreshArtifactLink(ctx context.Context, owner storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error)
	ListDiagnostics(ctx context.Context, owner storage.OwnerScope, subjectType, subjectID string) ([]storage.DiagnosticRecord, error)
	GetObservabilitySnapshot(ctx context.Context) (storage.ObservabilitySnapshot, error)
	RecordArtifacts(ctx context.Context, owner storage.OwnerScope, analysisRunID string, artifacts []storage.ArtifactRecord) ([]storage.ArtifactRecord, error)
	RecordDiagnostics(ctx context.Context, owner storage.OwnerScope, analysisRunID string, diagnostics []storage.DiagnosticRecord) ([]storage.DiagnosticRecord, error)
	RecordAnalysisRunProgress(ctx context.Context, owner storage.OwnerScope, analysisRunID, stage, message string, payload json.RawMessage) (storage.AnalysisRunRecord, error)
	FinalizeAnalysisRunTask(ctx context.Context, owner storage.OwnerScope, analysisRunID, status, message string) (storage.AnalysisRunRecord, error)
	ListPendingEnqueueTasks(ctx context.Context, limit int) ([]storage.AnalysisRunTaskRecord, error)
	ListAnalysisRunQueue(ctx context.Context, status, runType, taskType string, limit int) ([]storage.AnalysisRunQueueRecord, error)
	MarkAnalysisRunTaskQueued(ctx context.Context, analysisRunID, taskType string) error
	ClaimAnalysisRunTask(ctx context.Context, analysisRunID, workerKind, taskType, leaseOwner string) (storage.AnalysisRunRecord, bool, error)
}

func (s *publicRuntimeService) finalStore() (finalRuntimeStorageService, error) {
	return s.store, nil
}

func (s *publicRuntimeService) AddMediaItem(ctx context.Context, req storage.AddMediaItemRequest) (storage.MediaItemRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.MediaItemRecord{}, err
	}
	return store.AddMediaItem(ctx, req)
}

func (s *publicRuntimeService) ListMediaItems(ctx context.Context, owner storage.OwnerScope) ([]storage.MediaItemRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return nil, err
	}
	return store.ListMediaItems(ctx, owner)
}

func (s *publicRuntimeService) GetMediaItem(ctx context.Context, owner storage.OwnerScope, mediaItemID string) (storage.MediaItemRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.MediaItemRecord{}, err
	}
	return store.GetMediaItem(ctx, owner, mediaItemID)
}

func (s *publicRuntimeService) RemoveMediaItem(ctx context.Context, owner storage.OwnerScope, mediaItemID string) (storage.MediaItemRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.MediaItemRecord{}, err
	}
	return store.RemoveMediaItem(ctx, owner, mediaItemID)
}

func (s *publicRuntimeService) GetInboxCollection(ctx context.Context, owner storage.OwnerScope) (storage.CollectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.CollectionRecord{}, err
	}
	collections, err := store.ListCollections(ctx, owner)
	if err != nil {
		return storage.CollectionRecord{}, err
	}
	for _, collection := range collections {
		if collection.Kind == storage.CollectionKindInbox {
			return collection, nil
		}
	}
	return storage.CollectionRecord{}, storage.ErrCollectionNotFound
}

func (s *publicRuntimeService) CreateCollection(ctx context.Context, req storage.CreateCollectionRequest) (storage.CollectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.CollectionRecord{}, err
	}
	return store.CreateCollection(ctx, req)
}

func (s *publicRuntimeService) ListCollections(ctx context.Context, owner storage.OwnerScope) ([]storage.CollectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return nil, err
	}
	return store.ListCollections(ctx, owner)
}

func (s *publicRuntimeService) GetCollection(ctx context.Context, owner storage.OwnerScope, collectionID string) (storage.CollectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.CollectionRecord{}, err
	}
	return store.GetCollection(ctx, owner, collectionID)
}

func (s *publicRuntimeService) UpdateCollection(ctx context.Context, req storage.UpdateCollectionRequest) (storage.CollectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.CollectionRecord{}, err
	}
	return store.UpdateCollection(ctx, req)
}

func (s *publicRuntimeService) UpdateCollectionItems(ctx context.Context, req storage.UpdateCollectionItemsRequest) (storage.CollectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.CollectionRecord{}, err
	}
	return store.UpdateCollectionItems(ctx, req)
}

func (s *publicRuntimeService) CreateSelection(ctx context.Context, req storage.CreateSelectionRequest) (storage.SelectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.SelectionRecord{}, err
	}
	return store.CreateSelection(ctx, req)
}

func (s *publicRuntimeService) GetSelection(ctx context.Context, owner storage.OwnerScope, selectionID string) (storage.SelectionRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.SelectionRecord{}, err
	}
	return store.GetSelection(ctx, owner, selectionID)
}

func (s *publicRuntimeService) CreateAnalysisRun(ctx context.Context, req storage.CreateAnalysisRunRequest) (storage.AnalysisRunRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	run, err := store.CreateAnalysisRun(ctx, req)
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	if err := s.enqueueCreatedAnalysisRun(ctx, store, run); err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	return run, nil
}

func (s *publicRuntimeService) CancelAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID, message string) (storage.AnalysisRunRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	return store.CancelAnalysisRun(ctx, owner, analysisRunID, message)
}

func (s *publicRuntimeService) RetryAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID, idempotencyKey string) (storage.AnalysisRunRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	run, err := store.RetryAnalysisRun(ctx, owner, analysisRunID, idempotencyKey)
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	if err := s.enqueueCreatedAnalysisRun(ctx, store, run); err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	return run, nil
}

func (s *publicRuntimeService) enqueueCreatedAnalysisRun(ctx context.Context, store finalRuntimeStorageService, run storage.AnalysisRunRecord) error {
	if s.queue == nil {
		return nil
	}
	taskType := queue.TaskTypeSelectionAnalysis
	if run.RunType == "transcription" {
		taskType = queue.TaskTypeSelectionTranscription
	}
	if _, err := s.queue.Enqueue(ctx, queue.EnqueueRequest{
		AnalysisRunID: run.ID,
		RunType:       queue.RunTypeAnalysis,
		TaskType:      taskType,
		Attempt:       1,
	}); err != nil {
		return err
	}
	return store.MarkAnalysisRunTaskQueued(ctx, run.ID, taskType)
}

func (s *publicRuntimeService) ReconcileAnalysisRunQueue(ctx context.Context, limit int) (int, error) {
	store, err := s.finalStore()
	if err != nil {
		return 0, err
	}
	if s.queue == nil {
		return 0, nil
	}
	tasks, err := store.ListPendingEnqueueTasks(ctx, limit)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, task := range tasks {
		if _, err := s.queue.Enqueue(ctx, queue.EnqueueRequest{
			AnalysisRunID: task.AnalysisRunID,
			RunType:       queue.RunTypeAnalysis,
			TaskType:      task.TaskType,
			Attempt:       task.AttemptNo,
		}); err != nil {
			return recovered, err
		}
		if err := store.MarkAnalysisRunTaskQueued(ctx, task.AnalysisRunID, task.TaskType); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}

func (s *publicRuntimeService) ListAnalysisRuns(ctx context.Context, owner storage.OwnerScope) ([]storage.AnalysisRunRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return nil, err
	}
	return store.ListAnalysisRuns(ctx, owner)
}

func (s *publicRuntimeService) GetAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID string) (storage.AnalysisRunRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	return store.GetAnalysisRun(ctx, owner, analysisRunID)
}

func (s *publicRuntimeService) ListAnalysisRunEvents(ctx context.Context, owner storage.OwnerScope, analysisRunID string) ([]storage.RunEventRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return nil, err
	}
	return store.ListAnalysisRunEvents(ctx, owner, analysisRunID)
}

func (s *publicRuntimeService) ListArtifacts(ctx context.Context, owner storage.OwnerScope, analysisRunID string) ([]storage.ArtifactRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return nil, err
	}
	return store.ListArtifacts(ctx, owner, analysisRunID)
}

func (s *publicRuntimeService) GetArtifact(ctx context.Context, owner storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.ArtifactRecord{}, err
	}
	return store.GetArtifact(ctx, owner, artifactID)
}

func (s *publicRuntimeService) RefreshArtifactLink(ctx context.Context, owner storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.ArtifactRecord{}, err
	}
	return store.RefreshArtifactLink(ctx, owner, artifactID)
}

func (s *publicRuntimeService) ListDiagnostics(ctx context.Context, owner storage.OwnerScope, subjectType, subjectID string) ([]storage.DiagnosticRecord, error) {
	store, err := s.finalStore()
	if err != nil {
		return nil, err
	}
	return store.ListDiagnostics(ctx, owner, subjectType, subjectID)
}

func (s *publicRuntimeService) GetObservabilitySnapshot(ctx context.Context) (storage.ObservabilitySnapshot, error) {
	store, err := s.finalStore()
	if err != nil {
		return storage.ObservabilitySnapshot{}, err
	}
	return store.GetObservabilitySnapshot(ctx)
}
