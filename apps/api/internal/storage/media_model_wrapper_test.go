package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

type erroringWrapperStateStore struct {
	*memoryStateStore
	err error

	lastOwner        OwnerScope
	lastMediaItemID  string
	lastCollectionID string
	lastSelectionID  string
	lastAnalysisRun  string
	lastStatus       string
	lastRunType      string
	lastTaskType     string
	lastLimit        int
	lastLeaseOwner   string
	lastQueuedAt     time.Time
	lastDeletedAt    time.Time
	lastQuery        DiagnosticQuery
	recordCleanup    bool
}

func newErroringWrapperStateStore(err error) *erroringWrapperStateStore {
	return &erroringWrapperStateStore{
		memoryStateStore: newMemoryStateStore(),
		err:              err,
	}
}

func (s *erroringWrapperStateStore) ListMediaItems(_ context.Context, owner OwnerScope) ([]MediaItemRecord, error) {
	s.lastOwner = owner
	return nil, s.err
}

func (s *erroringWrapperStateStore) GetMediaItem(_ context.Context, owner OwnerScope, mediaItemID string) (MediaItemRecord, error) {
	s.lastOwner = owner
	s.lastMediaItemID = mediaItemID
	return MediaItemRecord{}, s.err
}

func (s *erroringWrapperStateStore) SoftDeleteMediaItem(_ context.Context, owner OwnerScope, mediaItemID string, deletedAt time.Time) (MediaItemRecord, error) {
	s.lastOwner = owner
	s.lastMediaItemID = mediaItemID
	s.lastDeletedAt = deletedAt
	return MediaItemRecord{}, s.err
}

func (s *erroringWrapperStateStore) ListCollections(_ context.Context, owner OwnerScope) ([]CollectionRecord, error) {
	s.lastOwner = owner
	return nil, s.err
}

func (s *erroringWrapperStateStore) GetCollection(_ context.Context, owner OwnerScope, collectionID string) (CollectionRecord, error) {
	s.lastOwner = owner
	s.lastCollectionID = collectionID
	return CollectionRecord{}, s.err
}

func (s *erroringWrapperStateStore) GetSelection(_ context.Context, owner OwnerScope, selectionID string) (SelectionRecord, error) {
	s.lastOwner = owner
	s.lastSelectionID = selectionID
	return SelectionRecord{}, s.err
}

func (s *erroringWrapperStateStore) GetAnalysisRunByID(_ context.Context, analysisRunID string) (AnalysisRunRecord, error) {
	s.lastAnalysisRun = analysisRunID
	return AnalysisRunRecord{}, s.err
}

func (s *erroringWrapperStateStore) GetAnalysisRun(_ context.Context, owner OwnerScope, analysisRunID string) (AnalysisRunRecord, error) {
	s.lastOwner = owner
	s.lastAnalysisRun = analysisRunID
	return AnalysisRunRecord{}, s.err
}

func (s *erroringWrapperStateStore) ListAnalysisRuns(_ context.Context, owner OwnerScope) ([]AnalysisRunRecord, error) {
	s.lastOwner = owner
	return nil, s.err
}

func (s *erroringWrapperStateStore) ListRunEvents(_ context.Context, owner OwnerScope, analysisRunID string) ([]RunEventRecord, error) {
	s.lastOwner = owner
	s.lastAnalysisRun = analysisRunID
	return nil, s.err
}

func (s *erroringWrapperStateStore) ListArtifacts(_ context.Context, owner OwnerScope, analysisRunID string) ([]ArtifactRecord, error) {
	s.lastOwner = owner
	s.lastAnalysisRun = analysisRunID
	return nil, s.err
}

func (s *erroringWrapperStateStore) ListDiagnostics(_ context.Context, owner OwnerScope, query DiagnosticQuery) ([]DiagnosticRecord, error) {
	s.lastOwner = owner
	s.lastQuery = query
	return nil, s.err
}

func (s *erroringWrapperStateStore) GetArtifactByID(_ context.Context, artifactID string) (ArtifactRecord, error) {
	s.lastAnalysisRun = artifactID
	return ArtifactRecord{}, s.err
}

func (s *erroringWrapperStateStore) DetectOrphanObjects(context.Context) ([]OrphanObjectRecord, error) {
	return nil, s.err
}

func (s *erroringWrapperStateStore) RecordOrphanObjectCleanup(context.Context, OrphanObjectRecord, bool, string, time.Time) error {
	s.recordCleanup = true
	return s.err
}

func (s *erroringWrapperStateStore) ListOperationalDiagnostics(context.Context, []string) ([]DiagnosticRecord, error) {
	return nil, s.err
}

func (s *erroringWrapperStateStore) ListPendingEnqueueTasks(_ context.Context, limit int) ([]AnalysisRunTaskRecord, error) {
	s.lastLimit = limit
	return nil, s.err
}

func (s *erroringWrapperStateStore) ListAnalysisRunQueue(_ context.Context, status, runType, taskType string, limit int) ([]AnalysisRunQueueRecord, error) {
	s.lastStatus = status
	s.lastRunType = runType
	s.lastTaskType = taskType
	s.lastLimit = limit
	return nil, s.err
}

func (s *erroringWrapperStateStore) MarkAnalysisRunTaskQueued(_ context.Context, analysisRunID, taskType string, queuedAt time.Time) error {
	s.lastAnalysisRun = analysisRunID
	s.lastTaskType = taskType
	s.lastQueuedAt = queuedAt
	return s.err
}

func (s *erroringWrapperStateStore) ClaimAnalysisRunTask(_ context.Context, analysisRunID, workerKind, taskType, leaseOwner string, claimedAt time.Time) (AnalysisRunRecord, bool, error) {
	s.lastAnalysisRun = analysisRunID
	s.lastRunType = workerKind
	s.lastTaskType = taskType
	s.lastLeaseOwner = leaseOwner
	s.lastQueuedAt = claimedAt
	return AnalysisRunRecord{}, false, s.err
}

type observabilityDiagnosticsErrorStateStore struct {
	*memoryStateStore
	err error
}

func (s *observabilityDiagnosticsErrorStateStore) ListOperationalDiagnostics(context.Context, []string) ([]DiagnosticRecord, error) {
	return nil, s.err
}

type orphanCleanupRecordErrorStateStore struct {
	*memoryStateStore
	err          error
	cleanupCalls int
}

func (s *orphanCleanupRecordErrorStateStore) DetectOrphanObjects(context.Context) ([]OrphanObjectRecord, error) {
	return []OrphanObjectRecord{{
		SubjectType: "source",
		SubjectID:   "source-1",
		Owner:       OwnerScope{OwnerType: "web", OwnerID: "owner-1"},
		Bucket:      SourcesBucket,
		ObjectKey:   "sources/source-1/source.bin",
		Reason:      "expired_media_source",
	}}, nil
}

func (s *orphanCleanupRecordErrorStateStore) RecordOrphanObjectCleanup(context.Context, OrphanObjectRecord, bool, string, time.Time) error {
	s.cleanupCalls++
	return s.err
}

func TestRepositoryWrapperErrorPropagationAndNormalization(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	expectedErr := errors.New("wrapper failure")
	state := newErroringWrapperStateStore(expectedErr)
	repo, err := NewRepository(state, newFakeObjectStore(), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	owner := OwnerScope{OwnerType: " web ", OwnerID: " owner-1 ", TenantID: " tenant-1 "}
	ctx := context.Background()

	if _, err := repo.ListMediaItems(ctx, owner); !errors.Is(err, expectedErr) {
		t.Fatalf("ListMediaItems() error = %v, want expectedErr", err)
	}
	if state.lastOwner.OwnerType != "web" || state.lastOwner.OwnerID != "owner-1" || state.lastOwner.TenantID != "tenant-1" {
		t.Fatalf("normalized owner = %#v", state.lastOwner)
	}

	if _, err := repo.GetMediaItem(ctx, owner, " media-1 "); !errors.Is(err, expectedErr) {
		t.Fatalf("GetMediaItem() error = %v, want expectedErr", err)
	}
	if state.lastMediaItemID != "media-1" {
		t.Fatalf("media item id = %q, want trimmed value", state.lastMediaItemID)
	}

	if _, err := repo.RemoveMediaItem(ctx, owner, " media-2 "); !errors.Is(err, expectedErr) {
		t.Fatalf("RemoveMediaItem() error = %v, want expectedErr", err)
	}
	if state.lastDeletedAt != now {
		t.Fatalf("deletedAt = %v, want %v", state.lastDeletedAt, now)
	}

	if _, err := repo.ListCollections(ctx, owner); !errors.Is(err, expectedErr) {
		t.Fatalf("ListCollections() error = %v, want expectedErr", err)
	}
	if _, err := repo.GetCollection(ctx, owner, " collection-1 "); !errors.Is(err, expectedErr) {
		t.Fatalf("GetCollection() error = %v, want expectedErr", err)
	}
	if state.lastCollectionID != "collection-1" {
		t.Fatalf("collection id = %q, want trimmed value", state.lastCollectionID)
	}

	if _, err := repo.GetSelection(ctx, owner, " selection-1 "); !errors.Is(err, expectedErr) {
		t.Fatalf("GetSelection() error = %v, want expectedErr", err)
	}
	if state.lastSelectionID != "selection-1" {
		t.Fatalf("selection id = %q, want trimmed value", state.lastSelectionID)
	}

	if _, err := repo.GetAnalysisRunByID(ctx, " run-1 "); !errors.Is(err, expectedErr) {
		t.Fatalf("GetAnalysisRunByID() error = %v, want expectedErr", err)
	}
	if state.lastAnalysisRun != "run-1" {
		t.Fatalf("analysis run id = %q, want trimmed value", state.lastAnalysisRun)
	}

	if _, err := repo.GetAnalysisRun(ctx, owner, " run-2 "); !errors.Is(err, expectedErr) {
		t.Fatalf("GetAnalysisRun() error = %v, want expectedErr", err)
	}
	if _, err := repo.RetryAnalysisRun(ctx, owner, " run-2 ", "retry"); !errors.Is(err, expectedErr) {
		t.Fatalf("RetryAnalysisRun() error = %v, want expectedErr", err)
	}
	if _, err := repo.ListAnalysisRuns(ctx, owner); !errors.Is(err, expectedErr) {
		t.Fatalf("ListAnalysisRuns() error = %v, want expectedErr", err)
	}
	if _, err := repo.ListAnalysisRunEvents(ctx, owner, " run-3 "); !errors.Is(err, expectedErr) {
		t.Fatalf("ListAnalysisRunEvents() error = %v, want expectedErr", err)
	}

	if _, err := repo.ListArtifacts(ctx, owner, " run-4 "); !errors.Is(err, expectedErr) {
		t.Fatalf("ListArtifacts() error = %v, want expectedErr", err)
	}
	if _, err := repo.GetInternalArtifactDownloadAccess(ctx, " artifact-1 "); !errors.Is(err, expectedErr) {
		t.Fatalf("GetInternalArtifactDownloadAccess() error = %v, want expectedErr", err)
	}
	if _, err := repo.ListDiagnostics(ctx, owner, DiagnosticQuery{Severity: "warning"}); !errors.Is(err, expectedErr) {
		t.Fatalf("ListDiagnostics() error = %v, want expectedErr", err)
	}
	if state.lastQuery.Severity != "warning" {
		t.Fatalf("diagnostic query = %#v, want severity warning", state.lastQuery)
	}

	if _, err := repo.CleanOrphanObjects(ctx); !errors.Is(err, expectedErr) {
		t.Fatalf("CleanOrphanObjects() error = %v, want expectedErr", err)
	}
	if _, err := repo.GetObservabilitySnapshot(ctx); !errors.Is(err, expectedErr) {
		t.Fatalf("GetObservabilitySnapshot() error = %v, want expectedErr", err)
	}
	if _, err := repo.ListPendingEnqueueTasks(ctx, 17); !errors.Is(err, expectedErr) {
		t.Fatalf("ListPendingEnqueueTasks() error = %v, want expectedErr", err)
	}
	if state.lastLimit != 17 {
		t.Fatalf("last limit = %d, want 17", state.lastLimit)
	}

	if _, err := repo.ListAnalysisRunQueue(ctx, " queued ", " report ", " selection.analysis ", 9); !errors.Is(err, expectedErr) {
		t.Fatalf("ListAnalysisRunQueue() error = %v, want expectedErr", err)
	}
	if state.lastStatus != "queued" || state.lastRunType != "report" || state.lastTaskType != "selection.analysis" || state.lastLimit != 9 {
		t.Fatalf("list analysis run queue inputs = status=%q runType=%q taskType=%q limit=%d", state.lastStatus, state.lastRunType, state.lastTaskType, state.lastLimit)
	}

	if err := repo.MarkAnalysisRunTaskQueued(ctx, " run-5 ", " selection.analysis "); !errors.Is(err, expectedErr) {
		t.Fatalf("MarkAnalysisRunTaskQueued() error = %v, want expectedErr", err)
	}
	if state.lastAnalysisRun != "run-5" || state.lastTaskType != "selection.analysis" || state.lastQueuedAt != now {
		t.Fatalf("mark queued inputs = run=%q task=%q queuedAt=%v", state.lastAnalysisRun, state.lastTaskType, state.lastQueuedAt)
	}

	if _, _, err := repo.ClaimAnalysisRunTask(ctx, " run-6 ", " analysis_runner ", " selection.analysis ", " worker-1 "); !errors.Is(err, expectedErr) {
		t.Fatalf("ClaimAnalysisRunTask() error = %v, want expectedErr", err)
	}
	if state.lastAnalysisRun != "run-6" || state.lastRunType != "analysis_runner" || state.lastTaskType != "selection.analysis" || state.lastLeaseOwner != "worker-1" || state.lastQueuedAt != now {
		t.Fatalf("claim inputs = run=%q worker=%q task=%q lease=%q claimedAt=%v", state.lastAnalysisRun, state.lastRunType, state.lastTaskType, state.lastLeaseOwner, state.lastQueuedAt)
	}
}

func TestRepositoryWrapperValidationAndLateErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 11, 0, 0, 0, time.UTC)
	expectedErr := errors.New("wrapper failure")
	owner := OwnerScope{OwnerType: " web ", OwnerID: " owner-1 "}
	ctx := context.Background()

	state := newErroringWrapperStateStore(expectedErr)
	repo, err := NewRepository(state, newFakeObjectStore(), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	if _, err := repo.CreateAnalysisRun(ctx, CreateAnalysisRunRequest{
		Owner:       owner,
		SelectionID: " selection-1 ",
		RunType:     " transcription ",
	}); !errors.Is(err, expectedErr) {
		t.Fatalf("CreateAnalysisRun() error = %v, want expectedErr", err)
	}
	if state.lastSelectionID != "selection-1" || state.lastOwner.OwnerType != "web" || state.lastOwner.OwnerID != "owner-1" {
		t.Fatalf("selection lookup inputs = selection=%q owner=%#v", state.lastSelectionID, state.lastOwner)
	}

	if _, err := repo.RecordArtifacts(ctx, OwnerScope{}, "run-1", nil); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("RecordArtifacts(contract violation) error = %v, want ErrContractViolation", err)
	}
	if _, err := repo.RecordDiagnostics(ctx, owner, "not-a-uuid", nil); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("RecordDiagnostics(contract violation) error = %v, want ErrContractViolation", err)
	}
	if _, err := repo.RecordAnalysisRunProgress(ctx, owner, " run-1 ", " ", "", nil); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("RecordAnalysisRunProgress(contract violation) error = %v, want ErrContractViolation", err)
	}
	if _, err := repo.FinalizeAnalysisRunTask(ctx, owner, " run-1 ", AnalysisRunStatusExpired, ""); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("FinalizeAnalysisRunTask(contract violation) error = %v, want ErrContractViolation", err)
	}
}

func TestRepositoryCleanupAndObservabilityLateErrors(t *testing.T) {
	t.Parallel()

	cleanupErr := errors.New("cleanup failed")
	cleanupState := &orphanCleanupRecordErrorStateStore{
		memoryStateStore: newMemoryStateStore(),
		err:              cleanupErr,
	}
	cleanupRepo, err := NewRepository(cleanupState, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository(cleanupRepo) error = %v", err)
	}

	if _, err := cleanupRepo.CleanOrphanObjects(context.Background()); !errors.Is(err, cleanupErr) {
		t.Fatalf("CleanOrphanObjects(cleanup error) error = %v, want cleanupErr", err)
	}
	if cleanupState.cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleanupState.cleanupCalls)
	}

	observabilityErr := errors.New("observability diagnostics failed")
	observabilityState := &observabilityDiagnosticsErrorStateStore{
		memoryStateStore: newMemoryStateStore(),
		err:              observabilityErr,
	}
	observabilityRepo, err := NewRepository(
		observabilityState,
		newFakeObjectStore(),
		WithClock(func() time.Time { return time.Date(2026, 5, 12, 11, 30, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatalf("NewRepository(observabilityRepo) error = %v", err)
	}

	if _, err := observabilityRepo.GetObservabilitySnapshot(context.Background()); !errors.Is(err, observabilityErr) {
		t.Fatalf("GetObservabilitySnapshot(diagnostics error) error = %v, want observabilityErr", err)
	}
}
