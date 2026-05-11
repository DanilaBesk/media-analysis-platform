package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

type markQueuedErrorStore struct {
	*fakePublicService
	markErr error
}

func (s *markQueuedErrorStore) MarkAnalysisRunTaskQueued(_ context.Context, analysisRunID, taskType string) error {
	for i, task := range s.pendingTasks {
		if task.AnalysisRunID == analysisRunID && task.TaskType == taskType {
			s.pendingTasks = append(s.pendingTasks[:i], s.pendingTasks[i+1:]...)
			return s.markErr
		}
	}
	return storage.ErrExecutionNotFound
}

func TestApiServerOptionAndErrorBranches(t *testing.T) {
	t.Parallel()

	logger := &capturingLogger{}
	server := NewServer(Dependencies{}, WithLogger(logger), WithMaxRequestBytes(128), WithMaxRequestBytes(0))
	if server.maxRequestBytes != 128 {
		t.Fatalf("maxRequestBytes = %d, want 128", server.maxRequestBytes)
	}

	server.logf("hello %s", "world")
	if len(logger.lines) != 1 || logger.lines[0] != "hello world" {
		t.Fatalf("logger lines = %#v", logger.lines)
	}

	serverWithoutLogger := NewServer(Dependencies{})
	serverWithoutLogger.logf("ignored")

	if !isAllowedLocalHTTPOrigin("https://localhost:3000") {
		t.Fatalf("https localhost origin must be allowed")
	}
	if isAllowedLocalHTTPOrigin("mailto:test@example.com") {
		t.Fatalf("non-http origin must not be allowed")
	}
	if isAllowedLocalHTTPOrigin("://bad") {
		t.Fatalf("malformed origin must not be allowed")
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/media-items", strings.NewReader(`{"owner":`))
	var body map[string]any
	if err := decodeJSONBody(req, &body); err == nil {
		t.Fatalf("decodeJSONBody() error = nil, want malformed JSON error")
	}

	var apiErr apiError
	if asAPIError(nil, &apiErr) {
		t.Fatalf("asAPIError(nil) = true, want false")
	}
	if !asAPIError(apiError{status: http.StatusTeapot, code: "brew"}, &apiErr) || apiErr.code != "brew" {
		t.Fatalf("asAPIError(apiError) = %#v", apiErr)
	}

	rec := httptest.NewRecorder()
	server.writeAPIError(rec, apiError{status: http.StatusBadRequest, message: "missing_code"})
	assertErrorCode(t, rec, http.StatusBadRequest, "internal_error")

	notImplemented := httptest.NewRecorder()
	server.writeAPIError(notImplemented, routeNotImplemented("queue"))
	assertErrorCode(t, notImplemented, http.StatusInternalServerError, "internal_error")
}

func TestApiPublicRuntimeServiceForwarders(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	store := &fakePublicService{
		mediaItem: storage.MediaItemRecord{
			ID:          "media-1",
			Owner:       owner,
			Kind:        "text",
			Status:      storage.MediaStatusReady,
			DisplayName: "note",
			Source:      storage.MediaSourceMetadata{SourceID: "source-1", OriginType: "text", TextRef: "inline:source-1"},
			Retention:   storage.RetentionMetadata{State: storage.RetentionStateActive},
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		mediaItems: []storage.MediaItemRecord{{
			ID:          "media-1",
			Owner:       owner,
			Kind:        "text",
			Status:      storage.MediaStatusReady,
			DisplayName: "note",
			Source:      storage.MediaSourceMetadata{SourceID: "source-1", OriginType: "text", TextRef: "inline:source-1"},
			Retention:   storage.RetentionMetadata{State: storage.RetentionStateActive},
			CreatedAt:   now,
			UpdatedAt:   now,
		}},
		collection: storage.CollectionRecord{
			ID:        "collection-1",
			Owner:     owner,
			Kind:      storage.CollectionKindUser,
			Name:      "Review",
			Status:    storage.CollectionStatusActive,
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		},
		collections: []storage.CollectionRecord{
			{
				ID:        "inbox-1",
				Owner:     owner,
				Kind:      storage.CollectionKindInbox,
				Name:      "Inbox",
				Status:    storage.CollectionStatusActive,
				Version:   1,
				CreatedAt: now,
				UpdatedAt: now,
			},
			{
				ID:        "collection-1",
				Owner:     owner,
				Kind:      storage.CollectionKindUser,
				Name:      "Review",
				Status:    storage.CollectionStatusActive,
				Version:   1,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		selection: storage.SelectionRecord{
			ID:        "selection-1",
			Owner:     owner,
			Status:    storage.SelectionStatusSealed,
			CreatedBy: "u-1",
			CreatedAt: now,
			SealedAt:  now,
		},
		run: storage.AnalysisRunRecord{
			ID:                "run-1",
			Owner:             owner,
			SelectionID:       "selection-1",
			RunType:           "report",
			Status:            storage.AnalysisRunStatusQueued,
			Version:           1,
			EvidenceGateState: "not_required",
			CreatedAt:         now,
		},
		runs: []storage.AnalysisRunRecord{{
			ID:                "run-1",
			Owner:             owner,
			SelectionID:       "selection-1",
			RunType:           "report",
			Status:            storage.AnalysisRunStatusQueued,
			Version:           1,
			EvidenceGateState: "not_required",
			CreatedAt:         now,
		}},
		events: []storage.RunEventRecord{{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Status:        storage.AnalysisRunStatusQueued,
			CreatedAt:     now,
		}},
		artifact: storage.ArtifactRecord{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "report",
			Status:        storage.ArtifactStatusAvailable,
			ContentType:   "text/markdown",
			SizeBytes:     12,
			Visibility:    "owner",
			CreatedAt:     now,
		},
		artifacts: []storage.ArtifactRecord{{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "report",
			Status:        storage.ArtifactStatusAvailable,
			ContentType:   "text/markdown",
			SizeBytes:     12,
			Visibility:    "owner",
			CreatedAt:     now,
		}},
		diagnostics: []storage.DiagnosticRecord{{
			ID:          "diag-1",
			Owner:       owner,
			SubjectType: "analysis_run",
			SubjectID:   "run-1",
			Severity:    "warning",
			Code:        "source_unavailable",
			Message:     "warn",
			CreatedAt:   now,
		}},
		observability: storage.ObservabilitySnapshot{QueueTasks: 3, GeneratedAt: now},
	}

	client := &flakyQueueClient{}
	publisher, err := queue.NewPublisher(client)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	service := &publicRuntimeService{store: store, queue: publisher}

	if _, err := service.AddMediaItem(context.Background(), storage.AddMediaItemRequest{Owner: owner}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	if items, err := service.ListMediaItems(context.Background(), owner); err != nil || len(items) != 1 {
		t.Fatalf("ListMediaItems() items=%#v err=%v", items, err)
	}
	if item, err := service.GetMediaItem(context.Background(), owner, "media-1"); err != nil || item.ID != "media-1" {
		t.Fatalf("GetMediaItem() item=%#v err=%v", item, err)
	}
	if item, err := service.RemoveMediaItem(context.Background(), owner, "media-1"); err != nil || item.ID != "media-1" {
		t.Fatalf("RemoveMediaItem() item=%#v err=%v", item, err)
	}
	if inbox, err := service.GetInboxCollection(context.Background(), owner); err != nil || inbox.Kind != storage.CollectionKindInbox {
		t.Fatalf("GetInboxCollection() inbox=%#v err=%v", inbox, err)
	}
	if _, err := service.CreateCollection(context.Background(), storage.CreateCollectionRequest{Owner: owner, Name: "Review"}); err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	if collections, err := service.ListCollections(context.Background(), owner); err != nil || len(collections) != 2 {
		t.Fatalf("ListCollections() collections=%#v err=%v", collections, err)
	}
	if collection, err := service.GetCollection(context.Background(), owner, "collection-1"); err != nil || collection.ID != "collection-1" {
		t.Fatalf("GetCollection() collection=%#v err=%v", collection, err)
	}
	if _, err := service.UpdateCollection(context.Background(), storage.UpdateCollectionRequest{CollectionID: "collection-1", Owner: owner, ExpectedVersion: 1}); err != nil {
		t.Fatalf("UpdateCollection() error = %v", err)
	}
	if _, err := service.UpdateCollectionItems(context.Background(), storage.UpdateCollectionItemsRequest{CollectionID: "collection-1", Owner: owner, ExpectedVersion: 1}); err != nil {
		t.Fatalf("UpdateCollectionItems() error = %v", err)
	}
	if _, err := service.CreateSelection(context.Background(), storage.CreateSelectionRequest{Owner: owner}); err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	if selection, err := service.GetSelection(context.Background(), owner, "selection-1"); err != nil || selection.ID != "selection-1" {
		t.Fatalf("GetSelection() selection=%#v err=%v", selection, err)
	}
	if run, err := service.CreateAnalysisRun(context.Background(), storage.CreateAnalysisRunRequest{Owner: owner, SelectionID: "selection-1", RunType: "report"}); err != nil || run.ID != "run-1" {
		t.Fatalf("CreateAnalysisRun() run=%#v err=%v", run, err)
	}
	if client.calls != 1 {
		t.Fatalf("enqueue calls = %d, want 1", client.calls)
	}
	if run, err := service.CancelAnalysisRun(context.Background(), owner, "run-1", "stop"); err != nil || run.Status != storage.AnalysisRunStatusCanceled {
		t.Fatalf("CancelAnalysisRun() run=%#v err=%v", run, err)
	}
	store.run.Status = storage.AnalysisRunStatusQueued
	store.pendingTasks = []storage.AnalysisRunTaskRecord{{
		ID:            "task-2",
		AnalysisRunID: "run-1",
		WorkerKind:    "analysis",
		TaskType:      "selection.analysis",
		Status:        storage.AnalysisRunTaskStatusPendingEnqueue,
		AttemptNo:     1,
		CreatedAt:     now,
	}}
	if run, err := service.RetryAnalysisRun(context.Background(), owner, "run-1", "retry-key"); err != nil || run.ID != "run-1" {
		t.Fatalf("RetryAnalysisRun() run=%#v err=%v", run, err)
	}
	if client.calls != 2 {
		t.Fatalf("enqueue calls after retry = %d, want 2", client.calls)
	}
	if runs, err := service.ListAnalysisRuns(context.Background(), owner); err != nil || len(runs) != 1 {
		t.Fatalf("ListAnalysisRuns() runs=%#v err=%v", runs, err)
	}
	if run, err := service.GetAnalysisRun(context.Background(), owner, "run-1"); err != nil || run.ID != "run-1" {
		t.Fatalf("GetAnalysisRun() run=%#v err=%v", run, err)
	}
	if events, err := service.ListAnalysisRunEvents(context.Background(), owner, "run-1"); err != nil || len(events) != 1 {
		t.Fatalf("ListAnalysisRunEvents() events=%#v err=%v", events, err)
	}
	if artifacts, err := service.ListArtifacts(context.Background(), owner, "run-1"); err != nil || len(artifacts) != 1 {
		t.Fatalf("ListArtifacts() artifacts=%#v err=%v", artifacts, err)
	}
	if artifact, err := service.GetArtifact(context.Background(), owner, "artifact-1"); err != nil || artifact.ID != "artifact-1" {
		t.Fatalf("GetArtifact() artifact=%#v err=%v", artifact, err)
	}
	if artifact, err := service.RefreshArtifactLink(context.Background(), owner, "artifact-1"); err != nil || artifact.ID != "artifact-1" {
		t.Fatalf("RefreshArtifactLink() artifact=%#v err=%v", artifact, err)
	}
	if diagnostics, err := service.ListDiagnostics(context.Background(), owner, storage.DiagnosticQuery{}); err != nil || len(diagnostics) != 1 {
		t.Fatalf("ListDiagnostics() diagnostics=%#v err=%v", diagnostics, err)
	}
	if snapshot, err := service.GetObservabilitySnapshot(context.Background()); err != nil || snapshot.QueueTasks != 3 {
		t.Fatalf("GetObservabilitySnapshot() snapshot=%#v err=%v", snapshot, err)
	}

	missingInbox := &publicRuntimeService{store: &fakePublicService{collections: []storage.CollectionRecord{{ID: "collection-1", Owner: owner, Kind: storage.CollectionKindUser}}}}
	if _, err := missingInbox.GetInboxCollection(context.Background(), owner); !errors.Is(err, storage.ErrCollectionNotFound) {
		t.Fatalf("GetInboxCollection(no inbox) error = %v, want ErrCollectionNotFound", err)
	}

	noQueue := &publicRuntimeService{store: store}
	if recovered, err := noQueue.ReconcileAnalysisRunQueue(context.Background(), 5); err != nil || recovered != 0 {
		t.Fatalf("ReconcileAnalysisRunQueue(no queue) recovered=%d err=%v", recovered, err)
	}

	queueErrClient := &flakyQueueClient{err: errors.New("queue down")}
	queueErrPublisher, err := queue.NewPublisher(queueErrClient)
	if err != nil {
		t.Fatalf("NewPublisher(queue error) error = %v", err)
	}
	queueErrService := &publicRuntimeService{store: store, queue: queueErrPublisher}
	if _, err := queueErrService.CreateAnalysisRun(context.Background(), storage.CreateAnalysisRunRequest{Owner: owner, SelectionID: "selection-1", RunType: "report"}); !errors.Is(err, queue.ErrQueueUnavailable) {
		t.Fatalf("CreateAnalysisRun(queue error) = %v, want ErrQueueUnavailable", err)
	}
	if _, err := queueErrService.RetryAnalysisRun(context.Background(), owner, "run-1", "retry-key-2"); !errors.Is(err, queue.ErrQueueUnavailable) {
		t.Fatalf("RetryAnalysisRun(queue error) = %v, want ErrQueueUnavailable", err)
	}
	store.err = storage.ErrAnalysisRunNotFound
	if _, err := (&publicRuntimeService{store: store, queue: publisher}).GetInboxCollection(context.Background(), owner); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
		t.Fatalf("GetInboxCollection(store error) = %v, want propagated store error", err)
	}
	store.err = nil

	noQueueCreate := &publicRuntimeService{store: store}
	if run, err := noQueueCreate.CreateAnalysisRun(context.Background(), storage.CreateAnalysisRunRequest{Owner: owner, SelectionID: "selection-1", RunType: "report"}); err != nil || run.ID != "run-1" {
		t.Fatalf("CreateAnalysisRun(no queue) run=%#v err=%v", run, err)
	}
	if run, err := noQueueCreate.RetryAnalysisRun(context.Background(), owner, "run-1", "retry-key-3"); err != nil || run.ID != "run-1" {
		t.Fatalf("RetryAnalysisRun(no queue) run=%#v err=%v", run, err)
	}
}

func TestApiWorkerRuntimeServiceDirectBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	store := &fakePublicService{
		run: storage.AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "transcription",
			Status:      storage.AnalysisRunStatusCancelRequested,
			ParamsJSON: []byte(`{
				"request_access":{
					"provider":"presigned",
					"url":"https://example.test/request.json",
					"expires_at":"2099-04-25T12:00:00Z",
					"request_ref":"request-1",
					"request_digest_sha256":"abc",
					"request_bytes":123
				}
			}`),
			Selection: storage.SelectionRecord{
				ID: "selection-1",
				Items: []storage.SelectionItemSnapshot{{
					ID:                "selection-item-1",
					Position:          0,
					MediaItemID:       "media-1",
					Kind:              "audio",
					DisplayName:       "source.wav",
					StatusAtSelection: storage.MediaStatusReady,
					SourceSnapshot: storage.MediaSourceMetadata{
						SourceID:   "source-1",
						OriginType: "object",
						ObjectKey:  "nested/source.wav",
						MIMEType:   "audio/wav",
					},
					MetadataJSON:      []byte(`{"filename":"source.wav"}`),
					RetentionSnapshot: storage.RetentionMetadata{State: storage.RetentionStateActive},
				}},
			},
			CreatedAt: now,
		},
		artifact: storage.ArtifactRecord{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "transcript",
			Status:        storage.ArtifactStatusAvailable,
			ContentType:   "text/plain",
			SizeBytes:     42,
			ObjectKey:     "artifacts/run-1/nested/transcript.txt",
			CreatedAt:     now,
			Download:      &storage.DownloadDescriptor{Provider: "object_store", URL: "https://example.test/transcript.txt", ExpiresAt: now.Add(time.Hour)},
		},
		pendingTasks: []storage.AnalysisRunTaskRecord{{
			ID:            "task-1",
			AnalysisRunID: "run-1",
			WorkerKind:    "transcription",
			TaskType:      "selection.transcription",
			Status:        storage.AnalysisRunTaskStatusQueued,
			AttemptNo:     1,
			CreatedAt:     now,
		}},
	}
	service := &workerRuntimeService{store: store}

	if _, err := NewRuntimeDependencies(nil, nil, nil, nil); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("NewRuntimeDependencies(nil) error = %v, want ErrContractViolation", err)
	}
	deps, err := NewRuntimeDependencies(&storage.Repository{}, nil, nil, &fakeWebsocketAcceptor{})
	if err != nil || deps.Public == nil || deps.Worker == nil || deps.Websocket == nil {
		t.Fatalf("NewRuntimeDependencies(success) deps=%#v err=%v", deps, err)
	}

	queueResp, err := service.ListAnalysisRunQueue(context.Background(), AnalysisRunQueueRequest{Status: storage.AnalysisRunTaskStatusQueued, RunType: "transcription", TaskType: "selection.transcription", PageSize: 1})
	if err != nil || len(queueResp.Items) != 1 || queueResp.PageSize != 1 {
		t.Fatalf("ListAnalysisRunQueue() resp=%#v err=%v", queueResp, err)
	}

	claimResp, err := service.ClaimExecution(context.Background(), "run-1", ExecutionClaimRequest{WorkerKind: "transcription", TaskType: "selection.transcription", LeaseOwner: "worker-1"})
	if err != nil || claimResp.ExecutionID != "run-1" || claimResp.Selection.SelectionID != "selection-1" {
		t.Fatalf("ClaimExecution() resp=%#v err=%v", claimResp, err)
	}

	if _, err := service.ResolveRequestAccess(context.Background(), "run-1", ""); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("ResolveRequestAccess(missing execution) error = %v, want ErrContractViolation", err)
	}
	if access, err := service.ResolveRequestAccess(context.Background(), "run-1", "exec-1"); err != nil || access.RequestBytes != 123 {
		t.Fatalf("ResolveRequestAccess() access=%#v err=%v", access, err)
	}
	store.run.ParamsJSON = []byte(`{"request_access":{"provider":"presigned"}}`)
	if _, err := service.ResolveRequestAccess(context.Background(), "run-1", "exec-1"); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("ResolveRequestAccess(incomplete) error = %v, want ErrContractViolation", err)
	}
	store.run.ParamsJSON = []byte(`{"something_else":true}`)
	if _, err := service.ResolveRequestAccess(context.Background(), "run-1", "exec-1"); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("ResolveRequestAccess(missing request_access) error = %v, want ErrContractViolation", err)
	}
	store.run.Status = storage.AnalysisRunStatusCancelRequested

	if _, err := service.CheckCancel(context.Background(), "run-1", ""); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("CheckCancel(missing execution) error = %v, want ErrContractViolation", err)
	}
	cancelResp, err := service.CheckCancel(context.Background(), "run-1", "exec-1")
	if err != nil || !cancelResp.CancelRequested {
		t.Fatalf("CheckCancel() resp=%#v err=%v", cancelResp, err)
	}

	if artifactResp, err := service.ResolveArtifactDownloadAccess(context.Background(), "artifact-1"); err != nil || artifactResp.Filename != "transcript.txt" {
		t.Fatalf("ResolveArtifactDownloadAccess() resp=%#v err=%v", artifactResp, err)
	}
	store.artifact.Download = nil
	if _, err := service.ResolveArtifactDownloadAccess(context.Background(), "artifact-1"); !errors.Is(err, storage.ErrArtifactResolutionFailed) {
		t.Fatalf("ResolveArtifactDownloadAccess(no download) error = %v, want ErrArtifactResolutionFailed", err)
	}

	store.run.Status = storage.AnalysisRunStatusRunning
	if err := service.RecordExecutionProgress(context.Background(), "run-1", ExecutionProgressRequest{Stage: "uploading", Message: "working"}); err != nil {
		t.Fatalf("RecordExecutionProgress() error = %v", err)
	}
	if store.recordedProgressStage != "uploading" || store.recordedProgressMsg != "working" {
		t.Fatalf("RecordExecutionProgress() stored stage=%q message=%q", store.recordedProgressStage, store.recordedProgressMsg)
	}

	if err := service.RecordExecutionArtifacts(context.Background(), "run-1", ExecutionArtifactsRequest{
		Artifacts: []workerArtifactDescriptor{{ArtifactKind: "summary_markdown", MIMEType: "text/markdown", ObjectKey: "artifacts/run-1/summary.md", SizeBytes: 7, Filename: "summary.md"}},
	}); err != nil {
		t.Fatalf("RecordExecutionArtifacts() error = %v", err)
	}
	if len(store.recordedArtifacts) != 1 || store.recordedArtifacts[0].Kind != "summary" {
		t.Fatalf("RecordExecutionArtifacts() artifacts=%#v", store.recordedArtifacts)
	}
	if err := service.RecordExecutionArtifacts(context.Background(), "run-1", ExecutionArtifactsRequest{
		Artifacts: []workerArtifactDescriptor{{ArtifactKind: "unknown_kind"}},
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("RecordExecutionArtifacts(unknown) error = %v, want ErrContractViolation", err)
	}

	if err := service.RecordExecutionDiagnostics(context.Background(), "run-1", ExecutionDiagnosticsRequest{
		ExecutionID: "exec-1",
		Diagnostics: []workerDiagnosticDescriptor{{
			DiagnosticID: "diag-1",
			SubjectType:  "media_item",
			SubjectID:    "media-1",
			Severity:     "warning",
			Code:         "source_unavailable",
			Message:      "warn",
			Context:      map[string]any{"existing": "value"},
		}},
	}); err != nil {
		t.Fatalf("RecordExecutionDiagnostics() error = %v", err)
	}
	var contextPayload map[string]any
	if err := json.Unmarshal(store.recordedDiagnostics[0].ContextJSON, &contextPayload); err != nil {
		t.Fatalf("Unmarshal(context JSON) error = %v", err)
	}
	if contextPayload["existing"] != "value" || contextPayload["execution_id"] != "exec-1" {
		t.Fatalf("diagnostic context = %#v", contextPayload)
	}

	if _, err := service.FinalizeExecution(context.Background(), "run-1", ExecutionFinalizeRequest{Outcome: "bogus"}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("FinalizeExecution(invalid) error = %v, want ErrContractViolation", err)
	}
	if finalized, err := service.FinalizeExecution(context.Background(), "run-1", ExecutionFinalizeRequest{Status: "canceled", Message: "stop"}); err != nil || finalized.Status != storage.AnalysisRunStatusCanceled {
		t.Fatalf("FinalizeExecution() run=%#v err=%v", finalized, err)
	}

	if got := accessInt64(map[string]any{"n": 5}, "n"); got != 5 {
		t.Fatalf("accessInt64(int) = %d, want 5", got)
	}
	if got := accessInt64(map[string]any{"n": float64(7)}, "n"); got != 7 {
		t.Fatalf("accessInt64(float64) = %d, want 7", got)
	}
	if got := accessInt64(map[string]any{"n": "bad"}, "n"); got != 0 {
		t.Fatalf("accessInt64(default) = %d, want 0", got)
	}
	if kind := internalWorkerArtifactKind(storage.ArtifactRecord{Kind: "report"}); kind != "report" {
		t.Fatalf("internalWorkerArtifactKind(fallback) = %q, want report", kind)
	}
	if filename := internalArtifactFilename(storage.ArtifactRecord{ID: "artifact-9", ObjectKey: " / "}); filename != "artifact-9" {
		t.Fatalf("internalArtifactFilename(fallback) = %q, want artifact id", filename)
	}
	if status := workerOutcomeStatus(" succeeded "); status != storage.AnalysisRunStatusSucceeded {
		t.Fatalf("workerOutcomeStatus(succeeded) = %q", status)
	}
	if status := workerOutcomeStatus("unknown"); status != "" {
		t.Fatalf("workerOutcomeStatus(unknown) = %q, want empty", status)
	}
	if decoded := jsonObject([]byte(`{"ok":true}`)); decoded["ok"] != true {
		t.Fatalf("jsonObject(valid) = %#v", decoded)
	}
	if decoded := jsonObject([]byte(`{`)); len(decoded) != 0 {
		t.Fatalf("jsonObject(invalid) = %#v, want empty", decoded)
	}
	if merged := string(mergeRuntimeContext([]byte(`{"execution_id":"kept"}`), map[string]any{"execution_id": "new", "other": "x"})); !strings.Contains(merged, `"kept"`) || !strings.Contains(merged, `"other":"x"`) {
		t.Fatalf("mergeRuntimeContext() = %s", merged)
	}
}

func TestApiWorkerRuntimeHelperBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}

	t.Run("claim execution prefers started_at and queue errors propagate", func(t *testing.T) {
		t.Parallel()

		startedAt := now.Add(5 * time.Minute)
		store := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:        "run-1",
				Owner:     owner,
				RunType:   "report",
				Selection: storage.SelectionRecord{ID: "selection-1"},
				StartedAt: &startedAt,
				CreatedAt: now,
			},
		}
		service := &workerRuntimeService{store: store}

		resp, err := service.ClaimExecution(context.Background(), "run-1", ExecutionClaimRequest{WorkerKind: "analysis_runner", TaskType: "selection.analysis"})
		if err != nil {
			t.Fatalf("ClaimExecution() error = %v", err)
		}
		if !resp.ClaimedAt.Equal(startedAt.UTC()) {
			t.Fatalf("ClaimedAt = %v, want %v", resp.ClaimedAt, startedAt.UTC())
		}

		errService := &workerRuntimeService{store: &fakePublicService{err: storage.ErrAnalysisRunNotFound}}
		if _, err := errService.ListAnalysisRunQueue(context.Background(), AnalysisRunQueueRequest{PageSize: 10}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("ListAnalysisRunQueue(error) = %v, want ErrAnalysisRunNotFound", err)
		}

		zeroTimeStore := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:        "run-2",
				Owner:     owner,
				RunType:   "report",
				Selection: storage.SelectionRecord{ID: "selection-2"},
			},
		}
		zeroTimeService := &workerRuntimeService{store: zeroTimeStore}
		before := time.Now().UTC()
		respZeroTime, err := zeroTimeService.ClaimExecution(context.Background(), "run-2", ExecutionClaimRequest{WorkerKind: "analysis_runner", TaskType: "selection.analysis"})
		after := time.Now().UTC()
		if err != nil {
			t.Fatalf("ClaimExecution(zero time) error = %v", err)
		}
		if respZeroTime.ClaimedAt.Before(before) || respZeroTime.ClaimedAt.After(after) {
			t.Fatalf("ClaimedAt = %v, want between %v and %v", respZeroTime.ClaimedAt, before, after)
		}

		createdOnlyStore := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:        "run-3",
				Owner:     owner,
				RunType:   "report",
				Selection: storage.SelectionRecord{ID: "selection-3"},
				CreatedAt: now,
			},
		}
		createdOnlyService := &workerRuntimeService{store: createdOnlyStore}
		respCreatedOnly, err := createdOnlyService.ClaimExecution(context.Background(), "run-3", ExecutionClaimRequest{WorkerKind: "analysis_runner", TaskType: "selection.analysis"})
		if err != nil {
			t.Fatalf("ClaimExecution(created only) error = %v", err)
		}
		if !respCreatedOnly.ClaimedAt.Equal(now.UTC()) {
			t.Fatalf("ClaimedAt(created only) = %v, want %v", respCreatedOnly.ClaimedAt, now.UTC())
		}
	})

	t.Run("selection label and source helpers cover fallback branches", func(t *testing.T) {
		t.Parallel()

		item := storage.SelectionItemSnapshot{
			MediaItemID: "media-1",
			SourceSnapshot: storage.MediaSourceMetadata{
				ExternalURI: "https://example.test/source",
			},
		}
		labels := selectionLabels(item, map[string]any{})
		if labels.DisplayLabel != "media-1" {
			t.Fatalf("DisplayLabel = %q, want media-1", labels.DisplayLabel)
		}
		if labels.SourceLabel == nil || *labels.SourceLabel != "https://example.test/source" {
			t.Fatalf("SourceLabel = %#v, want external URI", labels.SourceLabel)
		}
		if labels.OriginalFilename != nil {
			t.Fatalf("OriginalFilename = %#v, want nil", labels.OriginalFilename)
		}

		if label := sourceLabelFromSnapshot(storage.MediaSourceMetadata{ObjectKey: "sources/a.wav"}); label == nil || *label != "sources/a.wav" {
			t.Fatalf("sourceLabelFromSnapshot(object) = %#v", label)
		}
		if label := sourceLabelFromSnapshot(storage.MediaSourceMetadata{TextRef: "inline:1"}); label == nil || *label != "inline:1" {
			t.Fatalf("sourceLabelFromSnapshot(text) = %#v", label)
		}
		if label := sourceLabelFromSnapshot(storage.MediaSourceMetadata{SourceID: "source-1"}); label == nil || *label != "source-1" {
			t.Fatalf("sourceLabelFromSnapshot(source id) = %#v", label)
		}
		if label := sourceLabelFromSnapshot(storage.MediaSourceMetadata{}); label != nil {
			t.Fatalf("sourceLabelFromSnapshot(empty) = %#v, want nil", label)
		}
		if role := selectionItemRole(storage.SelectionItemSnapshot{ID: "selection-item-1"}, map[string]any{"role": "secondary"}, map[string]any{}); role != "secondary" {
			t.Fatalf("selectionItemRole(metadata) = %q, want secondary", role)
		}
		if role := selectionItemRole(
			storage.SelectionItemSnapshot{ID: "selection-item-2", MediaItemID: "media-2", Position: 3},
			map[string]any{},
			map[string]any{"item_roles": map[string]any{"3": "supporting"}},
		); role != "supporting" {
			t.Fatalf("selectionItemRole(position option snapshot) = %q, want supporting", role)
		}
		if got := firstString(nil, nil); got != nil {
			t.Fatalf("firstString(nil, nil) = %#v, want nil", got)
		}
	})

	t.Run("runtime helper status and serialization fallbacks", func(t *testing.T) {
		t.Parallel()

		if got := accessInt64(map[string]any{"n": int64(9)}, "n"); got != 9 {
			t.Fatalf("accessInt64(int64) = %d, want 9", got)
		}
		if got := firstNonEmpty("", "  "); got != "" {
			t.Fatalf("firstNonEmpty(empty) = %q, want empty", got)
		}
		if status := workerOutcomeStatus("partially_succeeded"); status != storage.AnalysisRunStatusPartiallySucceeded {
			t.Fatalf("workerOutcomeStatus(partially_succeeded) = %q", status)
		}
		if status := workerOutcomeStatus("failed"); status != storage.AnalysisRunStatusFailed {
			t.Fatalf("workerOutcomeStatus(failed) = %q", status)
		}
		if status := workerOutcomeStatus("canceled"); status != storage.AnalysisRunStatusCanceled {
			t.Fatalf("workerOutcomeStatus(canceled) = %q", status)
		}
		if got := string(jsonObjectBytes(map[string]any{"bad": make(chan int)})); got != "{}" {
			t.Fatalf("jsonObjectBytes(marshal error) = %s, want {}", got)
		}
		if got := string(mergeRuntimeContext([]byte(`{`), map[string]any{"bad": make(chan int)})); got != "{}" {
			t.Fatalf("mergeRuntimeContext(invalid+marshal error) = %s, want {}", got)
		}
	})

	t.Run("runtime services propagate remaining direct errors", func(t *testing.T) {
		t.Parallel()

		store := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:      "run-1",
				Owner:   owner,
				RunType: "report",
			},
			artifact: storage.ArtifactRecord{
				ID:          "artifact-1",
				ContentType: "text/plain",
				SizeBytes:   5,
				CreatedAt:   now,
				Download:    &storage.DownloadDescriptor{Provider: "object_store", URL: "https://example.test/file.txt", ExpiresAt: now.Add(time.Hour)},
			},
			err: storage.ErrAnalysisRunNotFound,
		}
		service := &workerRuntimeService{store: store}

		if _, err := service.ResolveArtifactDownloadAccess(context.Background(), "artifact-1"); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("ResolveArtifactDownloadAccess(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if err := service.RecordExecutionProgress(context.Background(), "run-1", ExecutionProgressRequest{ProgressStage: "uploading"}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("RecordExecutionProgress(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if _, err := service.FinalizeExecution(context.Background(), "run-1", ExecutionFinalizeRequest{Outcome: "failed"}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("FinalizeExecution(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if _, err := service.ClaimExecution(context.Background(), "run-1", ExecutionClaimRequest{WorkerKind: "analysis_runner", TaskType: "selection.analysis"}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("ClaimExecution(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if _, err := service.ResolveRequestAccess(context.Background(), "run-1", "exec-1"); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("ResolveRequestAccess(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if _, err := service.CheckCancel(context.Background(), "run-1", "exec-1"); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("CheckCancel(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if err := service.RecordExecutionArtifacts(context.Background(), "run-1", ExecutionArtifactsRequest{
			Artifacts: []workerArtifactDescriptor{{ArtifactKind: "summary_markdown"}},
		}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("RecordExecutionArtifacts(store error) = %v, want ErrAnalysisRunNotFound", err)
		}
		if err := service.RecordExecutionDiagnostics(context.Background(), "run-1", ExecutionDiagnosticsRequest{
			Diagnostics: []workerDiagnosticDescriptor{{DiagnosticID: "diag-x"}},
		}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("RecordExecutionDiagnostics(store error) = %v, want ErrAnalysisRunNotFound", err)
		}

		canceledAt := now.Add(15 * time.Minute)
		cancelStore := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:         "run-2",
				Owner:      owner,
				Status:     storage.AnalysisRunStatusCanceled,
				CanceledAt: &canceledAt,
			},
		}
		cancelService := &workerRuntimeService{store: cancelStore}
		cancelResp, err := cancelService.CheckCancel(context.Background(), "run-2", "exec-2")
		if err != nil {
			t.Fatalf("CheckCancel(canceled) error = %v", err)
		}
		if !cancelResp.CancelRequested || cancelResp.CancelRequestedAt == nil || !cancelResp.CancelRequestedAt.Equal(canceledAt) {
			t.Fatalf("CheckCancel(canceled) = %#v", cancelResp)
		}

		runningStore := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:     "run-4",
				Owner:  owner,
				Status: storage.AnalysisRunStatusRunning,
			},
		}
		runningService := &workerRuntimeService{store: runningStore}
		runningResp, err := runningService.CheckCancel(context.Background(), "run-4", "exec-4")
		if err != nil {
			t.Fatalf("CheckCancel(running) error = %v", err)
		}
		if runningResp.CancelRequested || runningResp.CancelRequestedAt != nil {
			t.Fatalf("CheckCancel(running) = %#v, want non-cancelled response", runningResp)
		}

		diagStore := &fakePublicService{
			run: storage.AnalysisRunRecord{
				ID:      "run-3",
				Owner:   owner,
				RunType: "report",
			},
		}
		diagService := &workerRuntimeService{store: diagStore}
		if err := diagService.RecordExecutionDiagnostics(context.Background(), "run-3", ExecutionDiagnosticsRequest{
			Diagnostics: []workerDiagnosticDescriptor{{
				DiagnosticID: "diag-2",
				SubjectType:  "analysis_run",
				SubjectID:    "run-3",
				Severity:     "info",
				Code:         "worker_note",
				Message:      "note",
			}},
		}); err != nil {
			t.Fatalf("RecordExecutionDiagnostics(no execution id) error = %v", err)
		}
		var contextPayload map[string]any
		if err := json.Unmarshal(diagStore.recordedDiagnostics[0].ContextJSON, &contextPayload); err != nil {
			t.Fatalf("Unmarshal(context JSON without execution id) error = %v", err)
		}
		if _, exists := contextPayload["execution_id"]; exists {
			t.Fatalf("diagnostic context unexpectedly contains execution_id: %#v", contextPayload)
		}
	})
}

func TestApiRuntimeRemainingDirectBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)

	t.Run("reconcile queue propagates list and mark errors", func(t *testing.T) {
		t.Parallel()

		errStore := &fakePublicService{err: storage.ErrAnalysisRunNotFound}
		errClient := &flakyQueueClient{}
		errPublisher, err := queue.NewPublisher(errClient)
		if err != nil {
			t.Fatalf("NewPublisher(err path) error = %v", err)
		}
		service := &publicRuntimeService{store: errStore, queue: errPublisher}
		if _, err := service.ReconcileAnalysisRunQueue(context.Background(), 10); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
			t.Fatalf("ReconcileAnalysisRunQueue(list error) = %v, want ErrAnalysisRunNotFound", err)
		}

		client := &flakyQueueClient{}
		publisher, err := queue.NewPublisher(client)
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
			markErrStore := &markQueuedErrorStore{fakePublicService: &fakePublicService{
				run: storage.AnalysisRunRecord{ID: "run-1", RunType: "report", Version: 1, CreatedAt: now},
				pendingTasks: []storage.AnalysisRunTaskRecord{{
					ID:            "task-1",
				AnalysisRunID: "run-1",
				WorkerKind:    "analysis",
				TaskType:      "selection.analysis",
					Status:        storage.AnalysisRunTaskStatusPendingEnqueue,
					AttemptNo:     1,
					CreatedAt:     now,
				}},
			}, markErr: storage.ErrAnalysisRunNotFound}
			if _, err := (&publicRuntimeService{store: markErrStore, queue: publisher}).ReconcileAnalysisRunQueue(context.Background(), 10); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
				t.Fatalf("ReconcileAnalysisRunQueue(mark error) = %v, want ErrAnalysisRunNotFound", err)
			}

		createErrStore := &fakePublicService{err: storage.ErrContractViolation}
		if _, err := (&publicRuntimeService{store: createErrStore, queue: publisher}).CreateAnalysisRun(context.Background(), storage.CreateAnalysisRunRequest{Owner: storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}}); !errors.Is(err, storage.ErrContractViolation) {
			t.Fatalf("CreateAnalysisRun(store error) = %v, want ErrContractViolation", err)
		}
		if _, err := (&publicRuntimeService{store: createErrStore, queue: publisher}).RetryAnalysisRun(context.Background(), storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}, "run-1", "retry"); !errors.Is(err, storage.ErrContractViolation) {
			t.Fatalf("RetryAnalysisRun(store error) = %v, want ErrContractViolation", err)
		}
	})

	t.Run("write api error fills zero-value defaults", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		NewServer(Dependencies{}).writeAPIError(rec, apiError{})
		assertErrorCode(t, rec, http.StatusInternalServerError, "internal_error")
	})
}

func TestApiHttpFinalRouteRemainingBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)
	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	public := &fakePublicService{
		mediaItems: []storage.MediaItemRecord{
			{
				ID:          "media-1",
				Owner:       owner,
				Kind:        "text",
				Status:      storage.MediaStatusReady,
				DisplayName: "first",
				Source:      storage.MediaSourceMetadata{SourceID: "source-1", OriginType: "text", TextRef: "inline:1"},
				Retention:   storage.RetentionMetadata{State: storage.RetentionStateActive},
				CreatedAt:   now,
				UpdatedAt:   now,
			},
			{
				ID:          "media-2",
				Owner:       owner,
				Kind:        "text",
				Status:      storage.MediaStatusReady,
				DisplayName: "second",
				Source:      storage.MediaSourceMetadata{SourceID: "source-2", OriginType: "text", TextRef: "inline:2"},
				Retention:   storage.RetentionMetadata{State: storage.RetentionStateActive},
				CreatedAt:   now.Add(time.Minute),
				UpdatedAt:   now.Add(time.Minute),
			},
		},
		run: storage.AnalysisRunRecord{
			ID:                "run-1",
			Owner:             owner,
			SelectionID:       "selection-1",
			RunType:           "report",
			Status:            storage.AnalysisRunStatusQueued,
			Version:           2,
			EvidenceGateState: "not_required",
			CreatedAt:         now,
		},
		runs: []storage.AnalysisRunRecord{{
			ID:                "run-1",
			Owner:             owner,
			SelectionID:       "selection-1",
			RunType:           "report",
			Status:            storage.AnalysisRunStatusQueued,
			Version:           2,
			EvidenceGateState: "not_required",
			CreatedAt:         now,
		}},
		events: []storage.RunEventRecord{
			{ID: "event-1", AnalysisRunID: "run-1", EventType: "analysis_run.created", Version: 1, CreatedAt: now},
			{ID: "event-2", AnalysisRunID: "run-1", EventType: "analysis_run.progress", Version: 2, CreatedAt: now.Add(time.Minute)},
		},
		diagnostics: []storage.DiagnosticRecord{
			{ID: "diag-1", Owner: owner, SubjectType: "analysis_run", SubjectID: "run-1", Severity: "warning", Code: "worker_partial", Message: "warn", CorrelationID: "corr-1", CreatedAt: now},
			{ID: "diag-2", Owner: owner, SubjectType: "analysis_run", SubjectID: "run-2", Severity: "info", Code: "other", Message: "skip", CorrelationID: "corr-2", CreatedAt: now},
		},
		reconciled: 7,
	}
	mux := newFinalMux(Dependencies{Public: public})

	t.Run("list media items paginates with cursor", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/media-items?owner_type=web&owner_id=u-1&cursor=media-1&page_size=1", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d want 200 body=%s", rec.Code, rec.Body.String())
		}
		var body struct {
			Items []struct {
				ID string `json:"media_item_id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("Unmarshal(list media items) error = %v", err)
		}
		if len(body.Items) != 1 || body.Items[0].ID != "media-2" {
			t.Fatalf("items = %#v, want media-2 page", body.Items)
		}
	})

	t.Run("create analysis run uses idempotency header", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := jsonRequest(http.MethodPost, "/v1/analysis-runs", map[string]any{
			"owner":        map[string]any{"owner_type": "web", "owner_id": "u-1"},
			"selection_id": "selection-1",
			"run_type":     "report",
		})
		req.Header.Set("Idempotency-Key", "hdr-key")
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d want 202 body=%s", rec.Code, rec.Body.String())
		}
		if public.lastRun.IdempotencyKey != "hdr-key" || public.lastRun.RunType != "report" {
			t.Fatalf("run request = %#v", public.lastRun)
		}
	})

	t.Run("retry falls back to body owner and body idempotency key", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry", map[string]any{
			"owner":           map[string]any{"owner_type": "web", "owner_id": "u-1"},
			"idempotency_key": "body-key",
		}))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d want 202 body=%s", rec.Code, rec.Body.String())
		}
		if public.retriedRunID != "run-1" {
			t.Fatalf("retried run id = %q, want run-1", public.retriedRunID)
		}
	})

	t.Run("list events and diagnostics paginate and filter", func(t *testing.T) {
		eventsRec := httptest.NewRecorder()
		mux.ServeHTTP(eventsRec, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/events?owner_type=web&owner_id=u-1&cursor=event-1&page_size=1", nil))
		if eventsRec.Code != http.StatusOK {
			t.Fatalf("events status = %d want 200 body=%s", eventsRec.Code, eventsRec.Body.String())
		}
		var eventsBody struct {
			Items []struct {
				ID string `json:"event_id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(eventsRec.Body.Bytes(), &eventsBody); err != nil {
			t.Fatalf("Unmarshal(events) error = %v", err)
		}
		if len(eventsBody.Items) != 1 || eventsBody.Items[0].ID != "event-2" {
			t.Fatalf("events page = %#v, want event-2", eventsBody.Items)
		}

		diagRec := httptest.NewRecorder()
		mux.ServeHTTP(diagRec, httptest.NewRequest(http.MethodGet, "/v1/diagnostics?owner_type=web&owner_id=u-1&subject_type=analysis_run&subject_id=run-1&severity=warning&code=worker_partial&correlation_id=corr-1&page_size=1", nil))
		if diagRec.Code != http.StatusOK {
			t.Fatalf("diagnostics status = %d want 200 body=%s", diagRec.Code, diagRec.Body.String())
		}
		if public.lastDiagnosticQuery != (storage.DiagnosticQuery{
			SubjectType:   "analysis_run",
			SubjectID:     "run-1",
			Severity:      "warning",
			Code:          "worker_partial",
			CorrelationID: "corr-1",
		}) {
			t.Fatalf("diagnostic query = %#v", public.lastDiagnosticQuery)
		}

		var diagBody struct {
			Items []struct {
				ID string `json:"diagnostic_id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(diagRec.Body.Bytes(), &diagBody); err != nil {
			t.Fatalf("Unmarshal(diagnostics) error = %v", err)
		}
		if len(diagBody.Items) != 1 || diagBody.Items[0].ID != "diag-1" {
			t.Fatalf("diagnostic page = %#v, want diag-1", diagBody.Items)
		}
	})

	t.Run("update collection items success encodes collection body", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{
			"owner":            map[string]any{"owner_type": "web", "owner_id": "u-1"},
			"expected_version": 1,
			"items": []map[string]any{
				{"media_item_id": "media-1", "position": 0},
			},
		}))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d want 200 body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("reconcile queue defaults non-positive limit", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/admin/reconcile-queue", map[string]any{"limit": 0}))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d want 202 body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("remaining handler error branches map service failures", func(t *testing.T) {
		errMux := newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrContractViolation}})

		cases := []struct {
			method string
			path   string
			body   string
		}{
			{method: http.MethodGet, path: "/v1/media-items?owner_type=web&owner_id=u-1"},
			{method: http.MethodPost, path: "/v1/collections/collection-1/items", body: `{"owner":{"owner_type":"web","owner_id":"u-1"},"expected_version":1,"items":[{"media_item_id":"media-1","position":0}]}`},
			{method: http.MethodPost, path: "/v1/analysis-runs", body: `{"owner":{"owner_type":"web","owner_id":"u-1"},"selection_id":"selection-1","run_type":"report"}`},
			{method: http.MethodGet, path: "/v1/diagnostics?owner_type=web&owner_id=u-1"},
		}
		for _, tc := range cases {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			errMux.ServeHTTP(rec, req)
			assertErrorCode(t, rec, http.StatusBadRequest, "invalid_request")
		}
	})

	t.Run("remove collection item keeps items when target is absent", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/missing?owner_type=web&owner_id=u-1&expected_version=3", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d want 200 body=%s", rec.Code, rec.Body.String())
		}
		if len(public.lastUpdateCollectionItems.Items) != 0 {
			t.Fatalf("remove absent item request = %#v", public.lastUpdateCollectionItems)
		}
	})

	t.Run("remove collection item maps update failures", func(t *testing.T) {
		errMux := newFinalMux(Dependencies{Public: &fakePublicService{
			collection: storage.CollectionRecord{
				ID:      "collection-1",
				Owner:   owner,
				Kind:    storage.CollectionKindUser,
				Name:    "Review",
				Status:  storage.CollectionStatusActive,
				Version: 3,
				Items: []storage.CollectionItemRecord{{
					MediaItemID: "media-1",
					Position:    0,
				}},
			},
			err: storage.ErrContractViolation,
		}})
		rec := httptest.NewRecorder()
		errMux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-1?owner_type=web&owner_id=u-1&expected_version=3", nil))
		assertErrorCode(t, rec, http.StatusBadRequest, "invalid_request")
	})
}

func TestApiPublicRuntimeQueueEdgeBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 15, 0, 0, 0, time.UTC)
	store := &fakePublicService{
		run: storage.AnalysisRunRecord{ID: "run-1", RunType: "report", Version: 1, CreatedAt: now},
		pendingTasks: []storage.AnalysisRunTaskRecord{{
			ID:            "task-1",
			AnalysisRunID: "run-1",
			WorkerKind:    "analysis",
			TaskType:      "selection.analysis",
			Status:        storage.AnalysisRunTaskStatusPendingEnqueue,
			AttemptNo:     1,
			CreatedAt:     now,
		}},
	}

	t.Run("enqueue failure returns recovered count", func(t *testing.T) {
		client := &flakyQueueClient{err: errors.New("queue down")}
		publisher, err := queue.NewPublisher(client)
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
		recovered, err := (&publicRuntimeService{store: store, queue: publisher}).ReconcileAnalysisRunQueue(context.Background(), 10)
		if err == nil || recovered != 0 {
			t.Fatalf("recovered=%d err=%v, want queue error with zero recovered", recovered, err)
		}
	})

	t.Run("mark queued failure returns after successful enqueue", func(t *testing.T) {
		client := &flakyQueueClient{}
		publisher, err := queue.NewPublisher(client)
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
		markErrStore := &fakePublicService{
			run: store.run,
			pendingTasks: append([]storage.AnalysisRunTaskRecord(nil), store.pendingTasks...),
			err: storage.ErrAnalysisRunNotFound,
		}
		recovered, err := (&publicRuntimeService{store: markErrStore, queue: publisher}).ReconcileAnalysisRunQueue(context.Background(), 10)
		if !errors.Is(err, storage.ErrAnalysisRunNotFound) || recovered != 0 {
			t.Fatalf("recovered=%d err=%v, want mark error with zero recovered", recovered, err)
		}
	})
}

func TestApiHandlerErrorBranches(t *testing.T) {
	t.Parallel()

	server := NewServer(Dependencies{})
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	dependencyCases := []struct {
		method string
		path   string
		body   string
		code   string
	}{
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/queue", code: "dependency_unavailable"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/claim", body: `{}`, code: "dependency_unavailable"},
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/run-1/request-access?execution_id=exec-1", code: "dependency_unavailable"},
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/run-1/executions/cancel-check?execution_id=exec-1", code: "dependency_unavailable"},
		{method: http.MethodGet, path: "/internal/v1/artifacts/artifact-1/download-access", code: "dependency_unavailable"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/progress", body: `{}`, code: "dependency_unavailable"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/artifacts", body: `{}`, code: "dependency_unavailable"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/diagnostics", body: `{}`, code: "dependency_unavailable"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/finalize", body: `{}`, code: "dependency_unavailable"},
	}

	for _, tc := range dependencyCases {
		tc := tc
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assertErrorCode(t, rec, http.StatusServiceUnavailable, tc.code)
		})
	}

	public := &fakePublicService{err: storage.ErrContractViolation}
	workerErr := &errorWorkerService{err: storage.ErrArtifactResolutionFailed}
	errorMux := newFinalMux(Dependencies{Public: public, Worker: workerErr})

	invalidCases := []struct {
		method string
		path   string
		body   string
		code   string
	}{
		{method: http.MethodPost, path: "/v1/media-items", body: `{"owner":`, code: "invalid_media_item"},
		{method: http.MethodPost, path: "/v1/collections", body: `{"owner":`, code: "invalid_collection"},
		{method: http.MethodPatch, path: "/v1/collections/collection-1", body: `{"owner":`, code: "invalid_collection"},
		{method: http.MethodPost, path: "/v1/collections/collection-1/items", body: `{"owner":`, code: "invalid_collection_items"},
		{method: http.MethodPost, path: "/v1/selections", body: `{"owner":`, code: "invalid_selection"},
		{method: http.MethodPost, path: "/v1/analysis-runs", body: `{"owner":`, code: "invalid_analysis_run"},
		{method: http.MethodPost, path: "/v1/analysis-runs/run-1/cancel", body: `{"message":`, code: "invalid_cancel_request"},
		{method: http.MethodPost, path: "/v1/analysis-runs/run-1/retry", body: `{"owner":`, code: "invalid_retry_request"},
		{method: http.MethodPost, path: "/v1/admin/reconcile-queue", body: `{"limit":`, code: "invalid_reconcile_request"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/claim", body: `{"worker_kind":`, code: "invalid_execution_claim"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/progress", body: `{"stage":`, code: "invalid_execution_progress"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/artifacts", body: `{"artifacts":`, code: "invalid_execution_artifacts"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/diagnostics", body: `{"diagnostics":`, code: "invalid_execution_diagnostics"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/finalize", body: `{"outcome":`, code: "invalid_execution_finalize"},
	}
	for _, tc := range invalidCases {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		errorMux.ServeHTTP(rec, req)
		assertErrorCode(t, rec, http.StatusBadRequest, tc.code)
	}

	notFoundCases := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/v1/media-items/media-1?owner_type=web&owner_id=u-1"},
		{method: http.MethodDelete, path: "/v1/media-items/media-1?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/collections/inbox?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/collections?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/collections/collection-1?owner_type=web&owner_id=u-1"},
		{method: http.MethodDelete, path: "/v1/collections/collection-1/items/media-1?owner_type=web&owner_id=u-1&expected_version=1"},
		{method: http.MethodGet, path: "/v1/selections/selection-1?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/analysis-runs?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/analysis-runs/run-1?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/analysis-runs/run-1/events?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/artifacts?owner_type=web&owner_id=u-1"},
		{method: http.MethodGet, path: "/v1/artifacts/artifact-1?owner_type=web&owner_id=u-1"},
		{method: http.MethodPost, path: "/v1/artifacts/artifact-1/refresh?owner_type=web&owner_id=u-1", body: "{}"},
		{method: http.MethodGet, path: "/v1/diagnostics?owner_type=web&owner_id=u-1"},
		{method: http.MethodPost, path: "/v1/admin/reconcile-queue", body: `{"limit":10}`},
		{method: http.MethodGet, path: "/v1/admin/observability"},
	}
	for _, tc := range notFoundCases {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		errorMux.ServeHTTP(rec, req)
		assertErrorCode(t, rec, http.StatusBadRequest, "invalid_request")
	}

	missingExecutionID := newFinalMux(Dependencies{Worker: workerErr})
	for _, tc := range []struct {
		path string
		code string
	}{
		{path: "/internal/v1/analysis-runs/run-1/request-access", code: "invalid_request_access"},
		{path: "/internal/v1/analysis-runs/run-1/executions/cancel-check", code: "invalid_cancel_check"},
	} {
		rec := httptest.NewRecorder()
		missingExecutionID.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		assertErrorCode(t, rec, http.StatusBadRequest, tc.code)
	}

	artifactResolution := httptest.NewRecorder()
	missingExecutionID.ServeHTTP(artifactResolution, httptest.NewRequest(http.MethodGet, "/internal/v1/artifacts/artifact-1/download-access", nil))
	assertErrorCode(t, artifactResolution, http.StatusBadGateway, "artifact_resolution_failed")
}

func TestApiInternalWorkerRouteErrorMappings(t *testing.T) {
	t.Parallel()

	runNotFoundMux := newFinalMux(Dependencies{Worker: &errorWorkerService{err: storage.ErrAnalysisRunNotFound}})
	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/queue?status=queued&page_size=5"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/claim", body: `{"worker_kind":"transcription","task_type":"selection.transcription"}`},
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/run-1/request-access?execution_id=exec-1"},
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/run-1/executions/cancel-check?execution_id=exec-1"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/progress", body: `{"stage":"uploading","message":"working"}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/artifacts", body: `{"artifacts":[]}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/diagnostics", body: `{"diagnostics":[]}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/finalize", body: `{"outcome":"failed"}`},
	} {
		tc := tc
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			runNotFoundMux.ServeHTTP(rec, req)
			assertErrorCode(t, rec, http.StatusNotFound, "not_found")
		})
	}

	invalidMux := newFinalMux(Dependencies{Worker: &errorWorkerService{err: storage.ErrContractViolation}})
	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/internal/v1/analysis-runs/queue?status=queued&page_size=bad"},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/claim", body: `{"worker_kind":"transcription","task_type":"selection.transcription"}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/progress", body: `{"progress_stage":"uploading"}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/artifacts", body: `{"artifacts":[]}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/diagnostics", body: `{"diagnostics":[]}`},
		{method: http.MethodPost, path: "/internal/v1/analysis-runs/run-1/executions/finalize", body: `{"status":"failed"}`},
	} {
		tc := tc
		t.Run("contract violation "+tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			invalidMux.ServeHTTP(rec, req)
			assertErrorCode(t, rec, http.StatusBadRequest, "invalid_request")
		})
	}
}

type capturingLogger struct {
	lines []string
}

func (l *capturingLogger) Printf(format string, args ...any) {
	l.lines = append(l.lines, strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(format), "%!s(MISSING)", "")))
	if len(args) > 0 {
		l.lines[len(l.lines)-1] = strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(format), "%s", args[0].(string)))
	}
}

type errorWorkerService struct {
	err error
}

func (s *errorWorkerService) ListAnalysisRunQueue(context.Context, AnalysisRunQueueRequest) (AnalysisRunQueueResponse, error) {
	return AnalysisRunQueueResponse{}, s.err
}
func (s *errorWorkerService) ClaimExecution(context.Context, string, ExecutionClaimRequest) (ExecutionClaimResponse, error) {
	return ExecutionClaimResponse{}, s.err
}
func (s *errorWorkerService) ResolveRequestAccess(context.Context, string, string) (RequestAccessResponse, error) {
	return RequestAccessResponse{}, s.err
}
func (s *errorWorkerService) CheckCancel(context.Context, string, string) (CancelCheckResponse, error) {
	return CancelCheckResponse{}, s.err
}
func (s *errorWorkerService) ResolveArtifactDownloadAccess(context.Context, string) (ArtifactDownloadAccessResponse, error) {
	return ArtifactDownloadAccessResponse{}, s.err
}
func (s *errorWorkerService) RecordExecutionProgress(context.Context, string, ExecutionProgressRequest) error {
	return s.err
}
func (s *errorWorkerService) RecordExecutionArtifacts(context.Context, string, ExecutionArtifactsRequest) error {
	return s.err
}
func (s *errorWorkerService) RecordExecutionDiagnostics(context.Context, string, ExecutionDiagnosticsRequest) error {
	return s.err
}
func (s *errorWorkerService) FinalizeExecution(context.Context, string, ExecutionFinalizeRequest) (storage.AnalysisRunRecord, error) {
	return storage.AnalysisRunRecord{}, s.err
}
