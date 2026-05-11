package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

type deletableObjectStore struct {
	*fakeObjectStore
	deleteErr error
	deletions []objectPresignRecord
}

func newDeletableObjectStore() *deletableObjectStore {
	return &deletableObjectStore{fakeObjectStore: newFakeObjectStore()}
}

func (d *deletableObjectStore) DeleteObject(_ context.Context, bucket, objectKey string) error {
	d.deletions = append(d.deletions, objectPresignRecord{bucket: bucket, objectKey: objectKey})
	return d.deleteErr
}

type observabilityStateStore struct {
	*memoryStateStore
	queueRecords []AnalysisRunQueueRecord
	ops          []DiagnosticRecord
}

func newObservabilityStateStore() *observabilityStateStore {
	return &observabilityStateStore{memoryStateStore: newMemoryStateStore()}
}

func (s *observabilityStateStore) ListAnalysisRunQueue(context.Context, string, string, string, int) ([]AnalysisRunQueueRecord, error) {
	return append([]AnalysisRunQueueRecord(nil), s.queueRecords...), nil
}

func (s *observabilityStateStore) ListOperationalDiagnostics(context.Context, []string) ([]DiagnosticRecord, error) {
	return append([]DiagnosticRecord(nil), s.ops...), nil
}

func TestApiStorageInternalArtifactResolutionFailureRecordsDiagnostic(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	runID := "11111111-1111-1111-1111-111111111111"
	state.analysisRuns[runID] = AnalysisRunRecord{
		ID:          runID,
		Owner:       owner,
		SelectionID: "selection-1",
		RunType:     "report",
		Status:      AnalysisRunStatusRunning,
		Version:     1,
		CreatedAt:   time.Date(2026, 5, 12, 9, 59, 0, 0, time.UTC),
	}
	state.artifacts["artifact-1"] = ArtifactRecord{
		ID:            "artifact-1",
		Owner:         owner,
		AnalysisRunID: runID,
		Kind:          "report",
		Status:        ArtifactStatusAvailable,
		ObjectKey:     "artifacts/run-1/report.md",
		ContentType:   "text/markdown",
		Visibility:    "owner",
		Retention:     RetentionMetadata{State: RetentionStateActive},
		CreatedAt:     time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC),
	}
	repo, err := NewRepository(state, &failingObjectStore{})
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	if _, err := repo.GetInternalArtifactDownloadAccess(context.Background(), "artifact-1"); !errors.Is(err, ErrArtifactResolutionFailed) {
		t.Fatalf("GetInternalArtifactDownloadAccess() error = %v, want ErrArtifactResolutionFailed", err)
	}
	diagnostics, err := repo.ListDiagnostics(context.Background(), owner, DiagnosticQuery{SubjectType: "analysis_run", SubjectID: runID})
	if err != nil {
		t.Fatalf("ListDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 1 || diagnostics[0].Code != "artifact_resolution_failed" {
		t.Fatalf("diagnostics = %#v, want artifact_resolution_failed", diagnostics)
	}
}

func TestApiStorageCleanOrphanObjectsDeletesWhenObjectStoreSupportsIt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 11, 0, 0, 0, time.UTC)
	expiredAt := now.Add(-time.Hour)
	state := newMemoryStateStore()
	objectStore := newDeletableObjectStore()
	repo, err := NewRepository(state, objectStore,
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"source-1", "media-1", "inbox-1",
			"selection-1",
			"run-1", "task-1", "event-1",
			"artifact-1",
		)),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: owner,
		Kind:  "file",
		Source: AddMediaSource{
			OriginType:  "object",
			ObjectRef:   "sources/orphan.bin",
			ContentType: "application/octet-stream",
			SizeBytes:   10,
		},
		Retention: RetentionMetadata{State: RetentionStateActive, ExpiresAt: &expiredAt},
	}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{Owner: owner, Items: []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}}})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	if _, err := repo.RecordArtifacts(context.Background(), owner, run.ID, []ArtifactRecord{{
		ID:          "artifact-1",
		Kind:        "transcript",
		Status:      ArtifactStatusAvailable,
		ObjectKey:   "artifacts/orphan.txt",
		ContentType: "text/plain",
		Visibility:  "owner",
		ExpiresAt:   &expiredAt,
	}}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	if _, err := repo.ApplyRetentionPolicies(context.Background()); err != nil {
		t.Fatalf("ApplyRetentionPolicies() error = %v", err)
	}

	result, err := repo.CleanOrphanObjects(context.Background())
	if err != nil {
		t.Fatalf("CleanOrphanObjects() error = %v", err)
	}
	if result.Detected != 2 || result.Deleted != 2 || result.MetadataOnly != 0 {
		t.Fatalf("cleanup result = %#v, want hard deletes", result)
	}
	if len(objectStore.deletions) != 2 {
		t.Fatalf("deletions = %#v, want two delete calls", objectStore.deletions)
	}
}

func TestApiStorageCleanOrphanObjectsRecordsDeleteFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 11, 30, 0, 0, time.UTC)
	expiredAt := now.Add(-time.Hour)
	state := newMemoryStateStore()
	objectStore := newDeletableObjectStore()
	objectStore.deleteErr = errors.New("delete failed")
	repo, err := NewRepository(state, objectStore,
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"source-1", "media-1", "inbox-1",
			"selection-1",
			"run-1", "task-1", "event-1",
			"artifact-1",
		)),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: owner,
		Kind:  "file",
		Source: AddMediaSource{
			OriginType:  "object",
			ObjectRef:   "sources/orphan.bin",
			ContentType: "application/octet-stream",
			SizeBytes:   10,
		},
		Retention: RetentionMetadata{State: RetentionStateActive, ExpiresAt: &expiredAt},
	}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{Owner: owner, Items: []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}}})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	if _, err := repo.RecordArtifacts(context.Background(), owner, run.ID, []ArtifactRecord{{
		ID:          "artifact-1",
		Kind:        "transcript",
		Status:      ArtifactStatusAvailable,
		ObjectKey:   "artifacts/orphan.txt",
		ContentType: "text/plain",
		Visibility:  "owner",
		ExpiresAt:   &expiredAt,
	}}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	if _, err := repo.ApplyRetentionPolicies(context.Background()); err != nil {
		t.Fatalf("ApplyRetentionPolicies() error = %v", err)
	}

	result, err := repo.CleanOrphanObjects(context.Background())
	if err != nil {
		t.Fatalf("CleanOrphanObjects() error = %v", err)
	}
	if result.Detected != 2 || result.Deleted != 0 || result.MetadataOnly != 2 || result.DeleteFailures != 2 {
		t.Fatalf("cleanup result = %#v, want delete failures recorded", result)
	}
}

func TestApiStorageObservabilityIgnoresFutureQueueRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	state := newObservabilityStateStore()
	state.queueRecords = []AnalysisRunQueueRecord{
		{AnalysisRunID: "run-1", CreatedAt: now.Add(-30 * time.Second)},
		{AnalysisRunID: "run-2", CreatedAt: time.Time{}},
		{AnalysisRunID: "run-3", CreatedAt: now.Add(time.Minute)},
	}
	state.ops = []DiagnosticRecord{
		{Code: "orphan_object_cleanup_failed"},
		{Code: "artifact_resolution_failed"},
	}
	repo, err := NewRepository(state, newFakeObjectStore(), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	snapshot, err := repo.GetObservabilitySnapshot(context.Background())
	if err != nil {
		t.Fatalf("GetObservabilitySnapshot() error = %v", err)
	}
	if snapshot.QueueTasks != 3 || snapshot.QueueLagSeconds != 30 || snapshot.CleanupFailures != 1 || snapshot.ArtifactResolutionFailures != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
