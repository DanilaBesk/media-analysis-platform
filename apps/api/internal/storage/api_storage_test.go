package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestApiStorageAddMediaPersistsInboxMetadataWithoutExecution(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(),
		WithClock(func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) }),
		WithIDGenerator(sequenceIDs("source-1", "media-1", "inbox-1")),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	item, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner:       OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
		Kind:        "text",
		Source:      AddMediaSource{OriginType: "text", Text: "hello"},
		DisplayName: "note",
	})
	if err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}

	if item.Source.OriginType != "text" || item.Source.TextRef == "" {
		t.Fatalf("source metadata = %#v, want text metadata", item.Source)
	}
	inbox := state.collections["inbox-1"]
	if inbox.Kind != CollectionKindInbox || len(inbox.Items) != 1 || inbox.Items[0].MediaItemID != item.ID {
		t.Fatalf("inbox = %#v, want one item membership", inbox)
	}
	if got := len(state.analysisRuns); got != 0 {
		t.Fatalf("add media must not create analysis runs, got %d", got)
	}
	if item.Retention.State != RetentionStateActive {
		t.Fatalf("retention state = %q, want active", item.Retention.State)
	}
}

func TestApiStorageFinalModelSupportsMediaKindsAndCollectionVersionConflicts(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	ids := []string{
		"source-text", "media-text", "inbox-text",
		"source-url", "media-url", "inbox-url",
		"source-file", "media-file", "inbox-file",
		"source-photo", "media-photo", "inbox-photo",
		"source-image", "media-image", "inbox-image",
		"source-document", "media-document", "inbox-document",
		"source-generic", "media-generic", "inbox-generic",
		"collection-1",
	}
	repo, err := NewRepository(state, newFakeObjectStore(), WithIDGenerator(sequenceIDs(ids...)))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	cases := []struct {
		kind   string
		source AddMediaSource
	}{
		{kind: "text", source: AddMediaSource{OriginType: "text", Text: "hello"}},
		{kind: "url", source: AddMediaSource{OriginType: "url", URL: "https://example.com/a"}},
		{kind: "file", source: AddMediaSource{OriginType: "object", ObjectRef: "sources/file.bin", ContentType: "application/octet-stream", SizeBytes: 10}},
		{kind: "photo", source: AddMediaSource{OriginType: "object", ObjectRef: "sources/photo.jpg", ContentType: "image/jpeg", SizeBytes: 11}},
		{kind: "image", source: AddMediaSource{OriginType: "object", ObjectRef: "sources/image.png", ContentType: "image/png", SizeBytes: 12}},
		{kind: "document", source: AddMediaSource{OriginType: "object", ObjectRef: "sources/doc.pdf", ContentType: "application/pdf", SizeBytes: 13}},
		{kind: "generic", source: AddMediaSource{OriginType: "object", ObjectRef: "sources/raw.dat", ContentType: "application/octet-stream", SizeBytes: 14}},
	}
	for _, tc := range cases {
		if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: owner, Kind: tc.kind, Source: tc.source}); err != nil {
			t.Fatalf("AddMediaItem(%s) error = %v", tc.kind, err)
		}
	}

	collection, err := repo.CreateCollection(context.Background(), CreateCollectionRequest{
		Owner: owner,
		Name:  "Review set",
		Items: []string{"media-text", "media-url"},
	})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	if collection.Version != 1 || len(collection.Items) != 2 {
		t.Fatalf("collection = %#v, want version 1 with two items", collection)
	}

	_, err = repo.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
		CollectionID:    collection.ID,
		Owner:           owner,
		ExpectedVersion: 99,
		Items:           []CollectionItemRecord{{MediaItemID: "media-text", Position: 0}},
	})
	if !errors.Is(err, ErrCollectionVersionConflict) {
		t.Fatalf("UpdateCollectionItems() error = %v, want ErrCollectionVersionConflict", err)
	}
}

func TestApiStorageSelectionSnapshotIsImmutableAndRunCreatesExecutionGraph(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(), WithIDGenerator(sequenceIDs(
		"source-1", "media-1", "inbox-1",
		"collection-1",
		"selection-1",
		"run-1", "task-1", "event-1",
	)))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "mcp", OwnerID: "caller-1"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: owner,
		Kind:  "file",
		Source: AddMediaSource{
			OriginType:  "object",
			ObjectRef:   "sources/clip.mp3",
			ContentType: "audio/mpeg",
			SizeBytes:   123,
		},
		DisplayName: "clip.mp3",
	}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	collection, err := repo.CreateCollection(context.Background(), CreateCollectionRequest{
		Owner: owner,
		Name:  "Run input",
		Items: []string{"media-1"},
	})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner:              owner,
		SourceCollectionID: collection.ID,
		Items:              []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}},
		OptionSnapshotJSON: []byte(`{"language":"ru"}`),
		CreatedBy:          "caller-1",
	})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	if selection.Status != SelectionStatusSealed || selection.Items[0].DisplayName != "clip.mp3" {
		t.Fatalf("selection snapshot = %#v", selection)
	}
	if selection.Items[0].ID == "" {
		t.Fatalf("selection item id must be persisted in snapshot: %#v", selection.Items[0])
	}

	state.mediaItems["media-1"] = withDisplayName(state.mediaItems["media-1"], "changed-after-seal")
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{
		Owner:       owner,
		SelectionID: selection.ID,
		RunType:     "transcription",
	})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	if run.Selection.Items[0].DisplayName != "clip.mp3" {
		t.Fatalf("run selection display_name = %q, want sealed snapshot value", run.Selection.Items[0].DisplayName)
	}
	if len(state.runTasks) != 1 || state.runTasks[0].AnalysisRunID != run.ID || state.runTasks[0].TaskType != "selection.transcription" {
		t.Fatalf("run tasks = %#v, want one analysis_run task", state.runTasks)
	}
	if got := len(state.runEvents[run.ID]); got != 1 {
		t.Fatalf("run events = %d, want created event", got)
	}
}

func TestApiStorageUploadedBodyPersistsSourceObjectBeforeSelectionSnapshot(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	objectStore := newFakeObjectStore()
	repo, err := NewRepository(state, objectStore, WithIDGenerator(sequenceIDs(
		"source-upload",
		"media-upload",
		"inbox-upload",
		"collection-upload",
		"selection-upload",
	)))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"}
	item, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: owner,
		Kind:  "voice",
		Source: AddMediaSource{
			OriginType:       "object",
			OriginalFilename: "voice.ogg",
			ContentType:      "audio/ogg",
			SizeBytes:        int64(len([]byte("voice-body"))),
			UploadBody:       []byte("voice-body"),
		},
		DisplayName: "voice.ogg",
	})
	if err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}

	if len(objectStore.puts) != 1 {
		t.Fatalf("object store puts = %d, want 1", len(objectStore.puts))
	}
	if objectStore.puts[0].bucket != SourcesBucket {
		t.Fatalf("bucket = %q, want %q", objectStore.puts[0].bucket, SourcesBucket)
	}
	if item.Source.ObjectKey == "" || item.Source.ObjectKey == "telegram://file/voice-file" {
		t.Fatalf("source object_key = %q, want canonical stored key", item.Source.ObjectKey)
	}
	if objectStore.puts[0].objectKey != item.Source.ObjectKey {
		t.Fatalf("stored key = %q, want %q", objectStore.puts[0].objectKey, item.Source.ObjectKey)
	}

	collection, err := repo.CreateCollection(context.Background(), CreateCollectionRequest{
		Owner: owner,
		Name:  "Run input",
		Items: []string{item.ID},
	})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner:              owner,
		SourceCollectionID: collection.ID,
		Items:              []CollectionItemRecord{{MediaItemID: item.ID, Position: 0}},
	})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	if selection.Items[0].SourceSnapshot.ObjectKey != item.Source.ObjectKey {
		t.Fatalf("selection source object_key = %q, want %q", selection.Items[0].SourceSnapshot.ObjectKey, item.Source.ObjectKey)
	}
}

func TestApiStorageSoftDeleteRemovesMutableMembershipsButPreservesRunLineage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(),
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"source-1", "media-1", "inbox-1",
			"collection-1",
			"selection-1",
			"run-1", "task-1", "event-1",
			"cancel-event-1",
			"retry-run-1", "retry-task-1", "retry-event-1",
		)),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: owner, Kind: "text", Source: AddMediaSource{OriginType: "text", Text: "hello"}}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	collection, err := repo.CreateCollection(context.Background(), CreateCollectionRequest{Owner: owner, Name: "Review", Items: []string{"media-1"}})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{Owner: owner, SourceCollectionID: collection.ID, Items: []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}}})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}

	deleted, err := repo.RemoveMediaItem(context.Background(), owner, "media-1")
	if err != nil {
		t.Fatalf("RemoveMediaItem() error = %v", err)
	}
	if deleted.Status != MediaStatusDeleted || deleted.Retention.State != RetentionStateSoftDeleted || deleted.DeletedAt == nil {
		t.Fatalf("deleted media = %#v, want soft-deleted retention metadata", deleted)
	}
	if got := state.collections["inbox-1"].Items; len(got) != 0 {
		t.Fatalf("inbox memberships = %#v, want deleted media hidden from mutable inbox", got)
	}
	if got := state.collections[collection.ID].Items; len(got) != 0 {
		t.Fatalf("collection memberships = %#v, want deleted media hidden from mutable collection", got)
	}
	preservedRun, err := repo.GetAnalysisRun(context.Background(), owner, run.ID)
	if err != nil {
		t.Fatalf("GetAnalysisRun(after delete) error = %v", err)
	}
	if len(preservedRun.Selection.Items) != 1 || preservedRun.Selection.Items[0].MediaItemID != "media-1" {
		t.Fatalf("run selection lineage = %#v, want immutable deleted-media reference preserved", preservedRun.Selection.Items)
	}

	canceled, err := repo.CancelAnalysisRun(context.Background(), owner, run.ID, "operator canceled")
	if err != nil {
		t.Fatalf("CancelAnalysisRun() error = %v", err)
	}
	if canceled.Status != AnalysisRunStatusCanceled || canceled.CanceledAt == nil {
		t.Fatalf("canceled run = %#v, want canceled state", canceled)
	}
	retry, err := repo.RetryAnalysisRun(context.Background(), owner, run.ID, "retry-key")
	if err != nil {
		t.Fatalf("RetryAnalysisRun() error = %v", err)
	}
	if retry.ID == run.ID || retry.SelectionID != selection.ID || retry.RunType != run.RunType {
		t.Fatalf("retry = %#v, want new run from same sealed selection", retry)
	}
}

func TestApiStorageOwnerScopeAndDiagnosticsArePersisted(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	owner := OwnerScope{OwnerType: "web", OwnerID: "owner-1"}
	other := OwnerScope{OwnerType: "web", OwnerID: "owner-2"}
	state.diagnostics = append(state.diagnostics, DiagnosticRecord{
		ID:          "diag-1",
		Owner:       owner,
		SubjectType: "media_item",
		SubjectID:   "media-1",
		Severity:    "warning",
		Code:        "media_item_invalid",
		Message:     "unsupported body",
		CreatedAt:   time.Now().UTC(),
	})
	repo, err := NewRepository(state, newFakeObjectStore(), WithIDGenerator(sequenceIDs("source-1", "media-1", "inbox-1")))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: owner, Kind: "text", Source: AddMediaSource{OriginType: "text", Text: "hello"}}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	if _, err := repo.GetMediaItem(context.Background(), other, "media-1"); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("GetMediaItem(other owner) error = %v, want ErrOwnerMismatch", err)
	}
	diagnostics, err := repo.ListDiagnostics(context.Background(), owner, DiagnosticQuery{SubjectType: "media_item", SubjectID: "media-1"})
	if err != nil {
		t.Fatalf("ListDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 1 || diagnostics[0].Code != "media_item_invalid" {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
}

func TestApiStorageListDiagnosticsAppliesEachFilter(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	owner := OwnerScope{OwnerType: "web", OwnerID: "owner-1"}
	state.diagnostics = append(state.diagnostics,
		DiagnosticRecord{
			ID:            "diag-subject",
			Owner:         owner,
			SubjectType:   "media_item",
			SubjectID:     "media-1",
			Severity:      "warning",
			Code:          "source_unavailable",
			CorrelationID: "corr-1",
			Message:       "kept by subject",
			CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		},
		DiagnosticRecord{
			ID:            "diag-severity",
			Owner:         owner,
			SubjectType:   "analysis_run",
			SubjectID:     "run-1",
			Severity:      "error",
			Code:          "source_unavailable",
			CorrelationID: "corr-1",
			Message:       "kept by severity",
			CreatedAt:     time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC),
		},
		DiagnosticRecord{
			ID:            "diag-code",
			Owner:         owner,
			SubjectType:   "analysis_run",
			SubjectID:     "run-2",
			Severity:      "warning",
			Code:          "retention_denied",
			CorrelationID: "corr-1",
			Message:       "kept by code",
			CreatedAt:     time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC),
		},
		DiagnosticRecord{
			ID:            "diag-correlation",
			Owner:         owner,
			SubjectType:   "analysis_run",
			SubjectID:     "run-3",
			Severity:      "warning",
			Code:          "source_unavailable",
			CorrelationID: "corr-2",
			Message:       "kept by correlation",
			CreatedAt:     time.Date(2026, 5, 10, 12, 3, 0, 0, time.UTC),
		},
		DiagnosticRecord{
			ID:            "diag-other-owner",
			Owner:         OwnerScope{OwnerType: "web", OwnerID: "owner-2"},
			SubjectType:   "media_item",
			SubjectID:     "media-1",
			Severity:      "warning",
			Code:          "source_unavailable",
			CorrelationID: "corr-1",
			Message:       "must stay hidden",
			CreatedAt:     time.Date(2026, 5, 10, 12, 4, 0, 0, time.UTC),
		},
	)
	repo, err := NewRepository(state, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	testCases := []struct {
		name   string
		query  DiagnosticQuery
		wantID string
	}{
		{
			name:   "subject",
			query:  DiagnosticQuery{SubjectType: "media_item", SubjectID: "media-1"},
			wantID: "diag-subject",
		},
		{
			name:   "severity",
			query:  DiagnosticQuery{Severity: "error"},
			wantID: "diag-severity",
		},
		{
			name:   "code",
			query:  DiagnosticQuery{Code: "retention_denied"},
			wantID: "diag-code",
		},
		{
			name:   "correlation_id",
			query:  DiagnosticQuery{CorrelationID: "corr-2"},
			wantID: "diag-correlation",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := repo.ListDiagnostics(context.Background(), owner, tc.query)
			if err != nil {
				t.Fatalf("ListDiagnostics() error = %v", err)
			}
			if len(diagnostics) != 1 || diagnostics[0].ID != tc.wantID {
				t.Fatalf("diagnostics = %#v, want only %q", diagnostics, tc.wantID)
			}
		})
	}
}

func TestApiStorageRejectsCrossOwnerCollectionSelectionAndArtifactAccess(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(), WithIDGenerator(sequenceIDs(
		"source-owner", "media-owner", "inbox-owner",
		"source-other", "media-other", "inbox-other",
		"selection-owner",
		"run-owner", "task-owner", "event-owner",
		"artifact-owner", "artifact-private",
	)))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "owner-1"}
	other := OwnerScope{OwnerType: "web", OwnerID: "owner-2"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: owner, Kind: "text", Source: AddMediaSource{OriginType: "text", Text: "owner text"}}); err != nil {
		t.Fatalf("AddMediaItem(owner) error = %v", err)
	}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: other, Kind: "text", Source: AddMediaSource{OriginType: "text", Text: "other text"}}); err != nil {
		t.Fatalf("AddMediaItem(other) error = %v", err)
	}
	if _, err := repo.CreateCollection(context.Background(), CreateCollectionRequest{Owner: owner, Name: "mixed", Items: []string{"media-other"}}); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("CreateCollection(cross-owner item) error = %v, want ErrOwnerMismatch", err)
	}
	if _, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner: owner,
		Items: []CollectionItemRecord{{MediaItemID: "media-other", Position: 0}},
	}); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("CreateSelection(cross-owner item) error = %v, want ErrOwnerMismatch", err)
	}

	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner: owner,
		Items: []CollectionItemRecord{{MediaItemID: "media-owner", Position: 0}},
	})
	if err != nil {
		t.Fatalf("CreateSelection(owner) error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	_, err = repo.RecordArtifacts(context.Background(), owner, run.ID, []ArtifactRecord{
		{
			ID:          "artifact-owner",
			Kind:        "transcript",
			Status:      "available",
			ObjectKey:   "runs/run-owner/transcript.txt",
			ContentType: "text/plain",
			SizeBytes:   42,
			Visibility:  "owner",
			PreviewJSON: []byte(`{"available":true}`),
		},
		{
			ID:          "artifact-private",
			Kind:        "execution_log",
			Status:      "available",
			ObjectKey:   "runs/run-owner/execution.log",
			ContentType: "text/plain",
			SizeBytes:   13,
			Visibility:  "private_execution",
			PreviewJSON: []byte(`{"available":false}`),
		},
	})
	if err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	artifacts, err := repo.ListArtifacts(context.Background(), owner, run.ID)
	if err != nil {
		t.Fatalf("ListArtifacts(owner) error = %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].ID != "artifact-owner" {
		t.Fatalf("public artifacts = %#v, want only owner-visible artifact", artifacts)
	}
	resolved, err := repo.GetArtifact(context.Background(), owner, "artifact-owner")
	if err != nil {
		t.Fatalf("GetArtifact(owner) error = %v", err)
	}
	if resolved.Download == nil || resolved.Download.URL == "" {
		t.Fatalf("artifact download = %#v, want owner-scoped presigned descriptor", resolved.Download)
	}
	if _, err := repo.GetArtifact(context.Background(), other, "artifact-owner"); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("GetArtifact(other owner) error = %v, want ErrOwnerMismatch", err)
	}
	if _, err := repo.GetArtifact(context.Background(), owner, "artifact-private"); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("GetArtifact(private execution artifact) error = %v, want ErrArtifactNotFound", err)
	}
}

func TestApiStorageRecordsArtifactResolutionFailuresForObservability(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	state := newMemoryStateStore()
	repo, err := NewRepository(state, &failingObjectStore{},
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"11111111-1111-1111-1111-111111111111",
			"22222222-2222-2222-2222-222222222222",
			"33333333-3333-3333-3333-333333333333",
			"44444444-4444-4444-4444-444444444444",
			"55555555-5555-5555-5555-555555555555",
			"66666666-6666-6666-6666-666666666666",
			"77777777-7777-7777-7777-777777777777",
		)),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: owner, Kind: "text", Source: AddMediaSource{OriginType: "text", Text: "hello"}}); err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{Owner: owner, Items: []CollectionItemRecord{{MediaItemID: "22222222-2222-2222-2222-222222222222", Position: 0}}})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	if _, err := repo.RecordArtifacts(context.Background(), owner, run.ID, []ArtifactRecord{{
		ID:          "66666666-6666-6666-6666-666666666666",
		Kind:        "transcript",
		Status:      ArtifactStatusAvailable,
		ObjectKey:   "artifacts/run/transcript.txt",
		ContentType: "text/plain",
		Visibility:  "owner",
	}}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}

	if _, err := repo.GetArtifact(context.Background(), owner, "66666666-6666-6666-6666-666666666666"); !errors.Is(err, ErrArtifactResolutionFailed) {
		t.Fatalf("GetArtifact() error = %v, want ErrArtifactResolutionFailed", err)
	}
	snapshot, err := repo.GetObservabilitySnapshot(context.Background())
	if err != nil {
		t.Fatalf("GetObservabilitySnapshot() error = %v", err)
	}
	if snapshot.ArtifactResolutionFailures != 1 {
		t.Fatalf("observability = %#v, want one artifact resolution failure", snapshot)
	}
	diagnostics, err := repo.ListDiagnostics(context.Background(), owner, DiagnosticQuery{SubjectType: "artifact", SubjectID: "66666666-6666-6666-6666-666666666666"})
	if err != nil {
		t.Fatalf("ListDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 1 || diagnostics[0].Code != "artifact_resolution_failed" {
		t.Fatalf("diagnostics = %#v, want artifact_resolution_failed", diagnostics)
	}
}

func TestApiStorageArtifactResolutionStripsArtifactsPrefixBeforePresign(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	state.artifacts["artifact-1"] = ArtifactRecord{
		ID:            "artifact-1",
		Owner:         owner,
		AnalysisRunID: "run-1",
		Kind:          "run_manifest",
		Status:        ArtifactStatusAvailable,
		ObjectKey:     "artifacts/run-1/run/manifest/run-manifest.json",
		ContentType:   "application/json; charset=utf-8",
		Visibility:    "owner",
		Retention:     RetentionMetadata{State: RetentionStateActive},
		CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}
	objectStore := newFakeObjectStore()
	repo, err := NewRepository(state, objectStore)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	artifact, err := repo.GetArtifact(context.Background(), owner, "artifact-1")
	if err != nil {
		t.Fatalf("GetArtifact() error = %v", err)
	}
	if artifact.Download == nil {
		t.Fatalf("artifact = %#v, want download descriptor", artifact)
	}
	if len(objectStore.presigns) != 1 {
		t.Fatalf("presigns = %#v, want one presign call", objectStore.presigns)
	}
	if objectStore.presigns[0].bucket != ArtifactsBucket {
		t.Fatalf("presign bucket = %q, want %q", objectStore.presigns[0].bucket, ArtifactsBucket)
	}
	if objectStore.presigns[0].objectKey != "run-1/run/manifest/run-manifest.json" {
		t.Fatalf("presign object key = %q, want stripped artifact key", objectStore.presigns[0].objectKey)
	}
}

func TestApiStorageRetentionSweepTransitionsExpiredState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	expiredAt := now.Add(-time.Hour)
	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(),
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"source-expired", "media-expired", "inbox-1",
			"collection-expired",
			"selection-expired",
			"source-active", "media-active", "inbox-2",
			"selection-active",
			"run-expired", "task-expired", "event-expired",
			"artifact-expired",
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
			ObjectRef:   "sources/expired.bin",
			ContentType: "application/octet-stream",
			SizeBytes:   10,
		},
		Retention: RetentionMetadata{State: RetentionStateActive, ExpiresAt: &expiredAt},
	}); err != nil {
		t.Fatalf("AddMediaItem(expired) error = %v", err)
	}
	collection, err := repo.CreateCollection(context.Background(), CreateCollectionRequest{Owner: owner, Name: "Aging", Items: []string{"media-expired"}})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner: owner,
		Items: []CollectionItemRecord{{MediaItemID: "media-expired", Position: 0}},
	})
	if err != nil {
		t.Fatalf("CreateSelection(expired input before sweep) error = %v", err)
	}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner:  owner,
		Kind:   "text",
		Source: AddMediaSource{OriginType: "text", Text: "still active"},
	}); err != nil {
		t.Fatalf("AddMediaItem(active) error = %v", err)
	}
	activeSelection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner: owner,
		Items: []CollectionItemRecord{{MediaItemID: "media-active", Position: 0}},
	})
	if err != nil {
		t.Fatalf("CreateSelection(active) error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: activeSelection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	run.ExpiresAt = &expiredAt
	state.analysisRuns[run.ID] = run
	if _, err := repo.RecordArtifacts(context.Background(), owner, run.ID, []ArtifactRecord{{
		ID:          "artifact-expired",
		Kind:        "transcript",
		Status:      ArtifactStatusAvailable,
		ObjectKey:   "artifacts/transcript.txt",
		ContentType: "text/plain",
		Visibility:  "owner",
		ExpiresAt:   &expiredAt,
	}}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}

	result, err := repo.ApplyRetentionPolicies(context.Background())
	if err != nil {
		t.Fatalf("ApplyRetentionPolicies() error = %v", err)
	}
	if result.ExpiredMediaItems != 1 || result.RemovedCollectionItems != 2 || result.ArchivedCollections != 1 || result.InvalidatedSelections != 1 || result.ExpiredAnalysisRuns != 1 || result.ExpiredArtifacts != 1 {
		t.Fatalf("retention result = %#v", result)
	}
	if got := state.mediaItems["media-expired"]; got.Status != MediaStatusDeleted || got.Retention.State != RetentionStateExpired {
		t.Fatalf("expired media = %#v", got)
	}
	if got := state.collections[collection.ID]; got.Status != CollectionStatusArchived {
		t.Fatalf("collection status = %q, want archived", got.Status)
	}
	if got := state.selections[selection.ID]; got.Status != SelectionStatusInvalidated {
		t.Fatalf("selection status = %q, want invalidated", got.Status)
	}
	if _, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"}); !errors.Is(err, ErrSelectionInvalid) {
		t.Fatalf("CreateAnalysisRun(invalidated selection) error = %v, want ErrSelectionInvalid", err)
	}
	if got := state.analysisRuns[run.ID]; got.Status != AnalysisRunStatusExpired {
		t.Fatalf("run status = %q, want expired", got.Status)
	}
	if got := state.artifacts["artifact-expired"]; got.Status != ArtifactStatusExpired || got.Retention.State != RetentionStateExpired {
		t.Fatalf("artifact = %#v, want expired retention", got)
	}
}

func TestApiStorageDetectsAndRecordsOrphanObjectCleanupWithoutDeleteSupport(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	expiredAt := now.Add(-time.Hour)
	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(),
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

	orphans, err := repo.DetectOrphanObjects(context.Background())
	if err != nil {
		t.Fatalf("DetectOrphanObjects() error = %v", err)
	}
	if len(orphans) != 2 {
		t.Fatalf("orphans = %#v, want source and artifact", orphans)
	}
	result, err := repo.CleanOrphanObjects(context.Background())
	if err != nil {
		t.Fatalf("CleanOrphanObjects() error = %v", err)
	}
	if result.Detected != 2 || result.MetadataOnly != 2 || result.Deleted != 0 || result.DiagnosticsRecorded != 2 {
		t.Fatalf("cleanup result = %#v, want metadata-only diagnostics", result)
	}
	diagnostics, err := repo.ListDiagnostics(context.Background(), owner, DiagnosticQuery{})
	if err != nil {
		t.Fatalf("ListDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 2 {
		t.Fatalf("diagnostics = %#v, want cleanup diagnostics", diagnostics)
	}
}

func TestApiStorageClaimAnalysisRunTaskIsAtomicForDuplicateQueueDeliveries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(),
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"source-1", "media-1", "inbox-1",
			"selection-1",
			"run-1", "task-1", "event-1",
		)),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	if _, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{Owner: owner, Kind: "text", Source: AddMediaSource{OriginType: "text", Text: "hello"}}); err != nil {
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
	if err := repo.MarkAnalysisRunTaskQueued(context.Background(), run.ID, "selection.transcription"); err != nil {
		t.Fatalf("MarkAnalysisRunTaskQueued() error = %v", err)
	}

	claimedRun, claimed, err := repo.ClaimAnalysisRunTask(context.Background(), run.ID, "transcription", "selection.transcription", "worker-1")
	if err != nil {
		t.Fatalf("ClaimAnalysisRunTask(first) error = %v", err)
	}
	if !claimed || claimedRun.Status != AnalysisRunStatusRunning {
		t.Fatalf("first claim = claimed:%v run:%#v, want running claim", claimed, claimedRun)
	}
	_, claimed, err = repo.ClaimAnalysisRunTask(context.Background(), run.ID, "transcription", "selection.transcription", "worker-2")
	if err != nil {
		t.Fatalf("ClaimAnalysisRunTask(second) error = %v", err)
	}
	if claimed {
		t.Fatalf("second duplicate queue delivery claimed execution again")
	}
}

func TestApiStorageRecordsWorkerRunArtifactsDiagnosticsAndFinalizeState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore(),
		WithClock(func() time.Time { return now }),
		WithIDGenerator(sequenceIDs(
			"11111111-1111-1111-1111-111111111111",
			"22222222-2222-2222-2222-222222222222",
			"33333333-3333-3333-3333-333333333333",
			"44444444-4444-4444-4444-444444444444",
			"55555555-5555-5555-5555-555555555555",
			"66666666-6666-6666-6666-666666666666",
			"77777777-7777-7777-7777-777777777777",
			"88888888-8888-8888-8888-888888888888",
			"99999999-9999-9999-9999-999999999999",
			"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		)),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	item, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: owner,
		Kind:  "audio",
		Source: AddMediaSource{
			OriginType:  "object",
			ObjectRef:   "sources/source.wav",
			ContentType: "audio/wav",
			SizeBytes:   42,
		},
		DisplayName: "source.wav",
	})
	if err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	selection, err := repo.CreateSelection(context.Background(), CreateSelectionRequest{
		Owner: owner,
		Items: []CollectionItemRecord{{MediaItemID: item.ID, Position: 0}},
	})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	run, err := repo.CreateAnalysisRun(context.Background(), CreateAnalysisRunRequest{Owner: owner, SelectionID: selection.ID, RunType: "transcription"})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}

	artifacts, err := repo.RecordArtifacts(context.Background(), owner, run.ID, []ArtifactRecord{
		{Kind: "run_manifest", ObjectKey: "artifacts/" + run.ID + "/run/manifest/run-manifest.json", ContentType: "application/json; charset=utf-8", SizeBytes: 100},
		{Kind: "run_diagnostics", ObjectKey: "artifacts/" + run.ID + "/run/diagnostics/run-diagnostics.json", ContentType: "application/json; charset=utf-8", SizeBytes: 80},
	})
	if err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	kinds := map[string]bool{}
	for _, artifact := range artifacts {
		if artifact.AnalysisRunID != run.ID {
			t.Fatalf("artifact = %#v, want analysis_run_id %q", artifact, run.ID)
		}
		kinds[artifact.Kind] = true
	}
	if len(artifacts) != 2 || !kinds["run_manifest"] || !kinds["run_diagnostics"] {
		t.Fatalf("artifacts = %#v, want canonical run artifacts owned by analysis_run", artifacts)
	}

	diagnostics, err := repo.RecordDiagnostics(context.Background(), owner, run.ID, []DiagnosticRecord{
		{
			ID:          "worker-diagnostic:0",
			SubjectType: "media_item",
			SubjectID:   item.ID,
			Severity:    "warning",
			Code:        "source_unavailable",
			Message:     "URL source skipped",
			ContextJSON: []byte(`{"selection_item_id":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb","media_kind":"audio","role":"primary"}`),
		},
		{
			ID:          "worker-diagnostic:1",
			SubjectType: "media_item",
			SubjectID:   "external-source-label",
			Severity:    "warning",
			Code:        "source_unavailable",
			Message:     "Non-object source skipped",
		},
	})
	if err != nil {
		t.Fatalf("RecordDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 2 {
		t.Fatalf("diagnostics = %#v, want 2 records", diagnostics)
	}
	if diagnostics[0].Owner.OwnerID != owner.OwnerID {
		t.Fatalf("diagnostic owner = %#v, want run owner", diagnostics[0].Owner)
	}
	first := state.diagnostics[0]
	if first.ID == "worker-diagnostic:0" || first.SubjectType != "media_item" || first.SubjectID != item.ID {
		t.Fatalf("first diagnostic = %#v, want generated storage id and media_item subject", first)
	}
	var firstContext map[string]any
	if err := json.Unmarshal(first.ContextJSON, &firstContext); err != nil {
		t.Fatalf("first diagnostic context JSON error = %v", err)
	}
	if firstContext["analysis_run_id"] != run.ID || firstContext["worker_diagnostic_id"] != "worker-diagnostic:0" || firstContext["selection_item_id"] != "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" {
		t.Fatalf("first diagnostic context = %#v", firstContext)
	}
	second := state.diagnostics[1]
	if second.SubjectType != "analysis_run" || second.SubjectID != run.ID {
		t.Fatalf("second diagnostic subject = %#v, want analysis_run fallback", second)
	}
	var secondContext map[string]any
	if err := json.Unmarshal(second.ContextJSON, &secondContext); err != nil {
		t.Fatalf("second diagnostic context JSON error = %v", err)
	}
	if secondContext["original_subject_id"] != "external-source-label" || secondContext["analysis_run_id"] != run.ID {
		t.Fatalf("second diagnostic context = %#v", secondContext)
	}

	if _, err := repo.RecordAnalysisRunProgress(context.Background(), owner, run.ID, "persisting_artifacts", "Uploading artifacts", nil); err != nil {
		t.Fatalf("RecordAnalysisRunProgress() error = %v", err)
	}
	finalized, err := repo.FinalizeAnalysisRunTask(context.Background(), owner, run.ID, AnalysisRunStatusPartiallySucceeded, "Completed with skipped inputs")
	if err != nil {
		t.Fatalf("FinalizeAnalysisRunTask() error = %v", err)
	}
	if finalized.Status != AnalysisRunStatusPartiallySucceeded || finalized.CompletedAt == nil {
		t.Fatalf("finalized run = %#v, want partially_succeeded terminal state", finalized)
	}
	events, err := repo.ListAnalysisRunEvents(context.Background(), owner, run.ID)
	if err != nil {
		t.Fatalf("ListAnalysisRunEvents() error = %v", err)
	}
	if len(events) != 3 || events[1].EventType != "analysis_run.progress" || events[2].Status != AnalysisRunStatusPartiallySucceeded {
		t.Fatalf("events = %#v, want created/progress/final terminal events", events)
	}
}

type memoryStateStore struct {
	mediaItems   map[string]MediaItemRecord
	collections  map[string]CollectionRecord
	selections   map[string]SelectionRecord
	analysisRuns map[string]AnalysisRunRecord
	artifacts    map[string]ArtifactRecord
	runTasks     []AnalysisRunTaskRecord
	runEvents    map[string][]RunEventRecord
	diagnostics  []DiagnosticRecord
}

func newMemoryStateStore() *memoryStateStore {
	return &memoryStateStore{
		mediaItems:   map[string]MediaItemRecord{},
		collections:  map[string]CollectionRecord{},
		selections:   map[string]SelectionRecord{},
		analysisRuns: map[string]AnalysisRunRecord{},
		artifacts:    map[string]ArtifactRecord{},
		runEvents:    map[string][]RunEventRecord{},
	}
}

func withDisplayName(item MediaItemRecord, displayName string) MediaItemRecord {
	item.DisplayName = displayName
	return item
}

func (m *memoryStateStore) AddMediaItem(_ context.Context, item MediaItemRecord, inbox CollectionRecord, targetCollectionID string) (MediaItemRecord, CollectionRecord, error) {
	m.mediaItems[item.ID] = item
	existingInbox, ok := m.findInbox(item.Owner)
	if !ok {
		existingInbox = inbox
	}
	itemCopy := item
	existingInbox.Items = append(existingInbox.Items, CollectionItemRecord{MediaItemID: item.ID, Position: len(existingInbox.Items), MediaItem: &itemCopy, AddedAt: item.CreatedAt})
	m.collections[existingInbox.ID] = existingInbox
	if targetCollectionID != "" {
		target, ok := m.collections[targetCollectionID]
		if !ok {
			return MediaItemRecord{}, CollectionRecord{}, ErrCollectionNotFound
		}
		if !SameOwner(target.Owner, item.Owner) {
			return MediaItemRecord{}, CollectionRecord{}, ErrOwnerMismatch
		}
		target.Items = append(target.Items, CollectionItemRecord{MediaItemID: item.ID, Position: len(target.Items), MediaItem: &itemCopy, AddedAt: item.CreatedAt})
		m.collections[target.ID] = target
	}
	return item, existingInbox, nil
}

func (m *memoryStateStore) ListMediaItems(_ context.Context, owner OwnerScope) ([]MediaItemRecord, error) {
	items := make([]MediaItemRecord, 0, len(m.mediaItems))
	for _, item := range m.mediaItems {
		if SameOwner(item.Owner, owner) && item.Status != MediaStatusDeleted {
			items = append(items, item)
		}
	}
	return items, nil
}

func (m *memoryStateStore) GetMediaItem(_ context.Context, owner OwnerScope, mediaItemID string) (MediaItemRecord, error) {
	item, ok := m.mediaItems[mediaItemID]
	if !ok || item.Status == MediaStatusDeleted {
		return MediaItemRecord{}, ErrMediaItemNotFound
	}
	if !SameOwner(item.Owner, owner) {
		return MediaItemRecord{}, ErrOwnerMismatch
	}
	return item, nil
}

func (m *memoryStateStore) SoftDeleteMediaItem(_ context.Context, owner OwnerScope, mediaItemID string, deletedAt time.Time) (MediaItemRecord, error) {
	item, err := m.GetMediaItem(context.Background(), owner, mediaItemID)
	if err != nil {
		return MediaItemRecord{}, err
	}
	item.Status = MediaStatusDeleted
	item.Retention.State = RetentionStateSoftDeleted
	item.DeletedAt = &deletedAt
	m.mediaItems[item.ID] = item
	for id, collection := range m.collections {
		items := collection.Items[:0]
		for _, member := range collection.Items {
			if member.MediaItemID != item.ID {
				items = append(items, member)
			}
		}
		collection.Items = items
		m.collections[id] = collection
	}
	return item, nil
}

func (m *memoryStateStore) CreateCollection(_ context.Context, collection CollectionRecord, itemIDs []string) (CollectionRecord, error) {
	for position, itemID := range itemIDs {
		item, ok := m.mediaItems[itemID]
		if !ok {
			return CollectionRecord{}, ErrMediaItemNotFound
		}
		if !SameOwner(item.Owner, collection.Owner) {
			return CollectionRecord{}, ErrOwnerMismatch
		}
		itemCopy := item
		collection.Items = append(collection.Items, CollectionItemRecord{MediaItemID: itemID, Position: position, MediaItem: &itemCopy, AddedAt: collection.CreatedAt})
	}
	m.collections[collection.ID] = collection
	return collection, nil
}

func (m *memoryStateStore) ListCollections(_ context.Context, owner OwnerScope) ([]CollectionRecord, error) {
	collections := make([]CollectionRecord, 0, len(m.collections))
	for _, collection := range m.collections {
		if SameOwner(collection.Owner, owner) && collection.Status != CollectionStatusDeleted {
			collections = append(collections, collection)
		}
	}
	return collections, nil
}

func (m *memoryStateStore) GetCollection(_ context.Context, owner OwnerScope, collectionID string) (CollectionRecord, error) {
	collection, ok := m.collections[collectionID]
	if !ok || collection.Status == CollectionStatusDeleted {
		return CollectionRecord{}, ErrCollectionNotFound
	}
	if !SameOwner(collection.Owner, owner) {
		return CollectionRecord{}, ErrOwnerMismatch
	}
	return collection, nil
}

func (m *memoryStateStore) UpdateCollection(_ context.Context, req UpdateCollectionRequest, updatedAt time.Time) (CollectionRecord, error) {
	collection, err := m.GetCollection(context.Background(), req.Owner, req.CollectionID)
	if err != nil {
		return CollectionRecord{}, err
	}
	if collection.Version != req.ExpectedVersion {
		return CollectionRecord{}, ErrCollectionVersionConflict
	}
	if req.Name != "" {
		collection.Name = req.Name
	}
	if req.Status != "" {
		collection.Status = req.Status
	}
	collection.Version++
	collection.UpdatedAt = updatedAt
	m.collections[collection.ID] = collection
	return collection, nil
}

func (m *memoryStateStore) UpdateCollectionItems(_ context.Context, req UpdateCollectionItemsRequest, updatedAt time.Time) (CollectionRecord, error) {
	collection, err := m.GetCollection(context.Background(), req.Owner, req.CollectionID)
	if err != nil {
		return CollectionRecord{}, err
	}
	if collection.Version != req.ExpectedVersion {
		return CollectionRecord{}, ErrCollectionVersionConflict
	}
	items := make([]CollectionItemRecord, 0, len(req.Items))
	for _, requested := range req.Items {
		item, ok := m.mediaItems[requested.MediaItemID]
		if !ok {
			return CollectionRecord{}, ErrMediaItemNotFound
		}
		if !SameOwner(item.Owner, req.Owner) {
			return CollectionRecord{}, ErrOwnerMismatch
		}
		itemCopy := item
		items = append(items, CollectionItemRecord{MediaItemID: requested.MediaItemID, Position: requested.Position, MediaItem: &itemCopy, AddedBy: req.AddedBy, AddedAt: updatedAt})
	}
	collection.Items = items
	collection.Version++
	collection.UpdatedAt = updatedAt
	m.collections[collection.ID] = collection
	return collection, nil
}

func (m *memoryStateStore) CreateSelection(_ context.Context, selection SelectionRecord, requestedItems []CollectionItemRecord) (SelectionRecord, error) {
	for _, requested := range requestedItems {
		item, ok := m.mediaItems[requested.MediaItemID]
		if !ok {
			return SelectionRecord{}, ErrSelectionInvalid
		}
		if !SameOwner(item.Owner, selection.Owner) {
			return SelectionRecord{}, ErrOwnerMismatch
		}
		selection.Items = append(selection.Items, SelectionItemSnapshot{
			ID:                uuidString(),
			Position:          requested.Position,
			MediaItemID:       item.ID,
			Kind:              item.Kind,
			SourceSnapshot:    item.Source,
			DisplayName:       item.DisplayName,
			StatusAtSelection: item.Status,
			MetadataJSON:      append([]byte(nil), item.MetadataJSON...),
			RetentionSnapshot: item.Retention,
		})
	}
	m.selections[selection.ID] = selection
	return selection, nil
}

func (m *memoryStateStore) GetSelection(_ context.Context, owner OwnerScope, selectionID string) (SelectionRecord, error) {
	selection, ok := m.selections[selectionID]
	if !ok {
		return SelectionRecord{}, ErrSelectionNotFound
	}
	if !SameOwner(selection.Owner, owner) {
		return SelectionRecord{}, ErrOwnerMismatch
	}
	return selection, nil
}

func (m *memoryStateStore) CreateAnalysisRun(_ context.Context, run AnalysisRunRecord, task AnalysisRunTaskRecord, event RunEventRecord) (AnalysisRunRecord, error) {
	selection, err := m.GetSelection(context.Background(), run.Owner, run.SelectionID)
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	if selection.Status != SelectionStatusSealed {
		return AnalysisRunRecord{}, ErrSelectionInvalid
	}
	run.Selection = selection
	m.analysisRuns[run.ID] = run
	m.runTasks = append(m.runTasks, task)
	m.runEvents[run.ID] = append(m.runEvents[run.ID], event)
	return run, nil
}

func (m *memoryStateStore) GetAnalysisRunByID(_ context.Context, analysisRunID string) (AnalysisRunRecord, error) {
	run, ok := m.analysisRuns[analysisRunID]
	if !ok {
		return AnalysisRunRecord{}, ErrAnalysisRunNotFound
	}
	return run, nil
}

func (m *memoryStateStore) GetAnalysisRun(_ context.Context, owner OwnerScope, analysisRunID string) (AnalysisRunRecord, error) {
	run, ok := m.analysisRuns[analysisRunID]
	if !ok {
		return AnalysisRunRecord{}, ErrAnalysisRunNotFound
	}
	if !SameOwner(run.Owner, owner) {
		return AnalysisRunRecord{}, ErrOwnerMismatch
	}
	return run, nil
}

func (m *memoryStateStore) ListAnalysisRuns(_ context.Context, owner OwnerScope) ([]AnalysisRunRecord, error) {
	runs := make([]AnalysisRunRecord, 0, len(m.analysisRuns))
	for _, run := range m.analysisRuns {
		if SameOwner(run.Owner, owner) {
			runs = append(runs, run)
		}
	}
	return runs, nil
}

func (m *memoryStateStore) ListRunEvents(_ context.Context, owner OwnerScope, analysisRunID string) ([]RunEventRecord, error) {
	if _, err := m.GetAnalysisRun(context.Background(), owner, analysisRunID); err != nil {
		return nil, err
	}
	return append([]RunEventRecord(nil), m.runEvents[analysisRunID]...), nil
}

func (m *memoryStateStore) RecordArtifacts(_ context.Context, owner OwnerScope, analysisRunID string, artifacts []ArtifactRecord, _ time.Time) ([]ArtifactRecord, error) {
	if _, err := m.GetAnalysisRun(context.Background(), owner, analysisRunID); err != nil {
		return nil, err
	}
	for _, artifact := range artifacts {
		artifact.Owner = owner
		artifact.AnalysisRunID = analysisRunID
		m.artifacts[artifact.ID] = artifact
		run := m.analysisRuns[analysisRunID]
		run.Artifacts = append(run.Artifacts, artifact)
		m.analysisRuns[analysisRunID] = run
	}
	return m.ListArtifacts(context.Background(), owner, analysisRunID)
}

func (m *memoryStateStore) ListArtifacts(_ context.Context, owner OwnerScope, analysisRunID string) ([]ArtifactRecord, error) {
	if analysisRunID != "" {
		if _, err := m.GetAnalysisRun(context.Background(), owner, analysisRunID); err != nil {
			return nil, err
		}
	}
	artifacts := make([]ArtifactRecord, 0, len(m.artifacts))
	for _, artifact := range m.artifacts {
		if SameOwner(artifact.Owner, owner) && artifact.Visibility == "owner" && artifact.Status != "deleted" && (analysisRunID == "" || artifact.AnalysisRunID == analysisRunID) {
			artifacts = append(artifacts, artifact)
		}
	}
	return artifacts, nil
}

func (m *memoryStateStore) GetArtifact(_ context.Context, owner OwnerScope, artifactID string) (ArtifactRecord, error) {
	artifact, ok := m.artifacts[artifactID]
	if !ok || artifact.Status == "deleted" || artifact.Visibility != "owner" {
		return ArtifactRecord{}, ErrArtifactNotFound
	}
	if !SameOwner(artifact.Owner, owner) {
		return ArtifactRecord{}, ErrOwnerMismatch
	}
	return artifact, nil
}

func (m *memoryStateStore) GetArtifactByID(_ context.Context, artifactID string) (ArtifactRecord, error) {
	artifact, ok := m.artifacts[artifactID]
	if !ok || artifact.Status == "deleted" {
		return ArtifactRecord{}, ErrArtifactNotFound
	}
	return artifact, nil
}

func (m *memoryStateStore) ListDiagnostics(_ context.Context, owner OwnerScope, query DiagnosticQuery) ([]DiagnosticRecord, error) {
	diagnostics := make([]DiagnosticRecord, 0, len(m.diagnostics))
	for _, diagnostic := range m.diagnostics {
		if !SameOwner(diagnostic.Owner, owner) ||
			(query.SubjectType != "" && diagnostic.SubjectType != query.SubjectType) ||
			(query.SubjectID != "" && diagnostic.SubjectID != query.SubjectID) ||
			(query.Severity != "" && diagnostic.Severity != query.Severity) ||
			(query.Code != "" && diagnostic.Code != query.Code) ||
			(query.CorrelationID != "" && diagnostic.CorrelationID != query.CorrelationID) {
			continue
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics, nil
}

func (m *memoryStateStore) RecordDiagnostics(_ context.Context, owner OwnerScope, analysisRunID string, diagnostics []DiagnosticRecord, _ time.Time) ([]DiagnosticRecord, error) {
	if _, err := m.GetAnalysisRun(context.Background(), owner, analysisRunID); err != nil {
		return nil, err
	}
	for _, diagnostic := range diagnostics {
		diagnostic.Owner = owner
		m.diagnostics = append(m.diagnostics, diagnostic)
		run := m.analysisRuns[analysisRunID]
		run.Diagnostics = append(run.Diagnostics, diagnostic)
		m.analysisRuns[analysisRunID] = run
	}
	return m.ListDiagnostics(context.Background(), owner, DiagnosticQuery{})
}

func (m *memoryStateStore) RecordAnalysisRunProgress(_ context.Context, owner OwnerScope, analysisRunID string, event RunEventRecord, recordedAt time.Time) (AnalysisRunRecord, error) {
	run, err := m.GetAnalysisRun(context.Background(), owner, analysisRunID)
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	if terminalRunStatus(run.Status) {
		return AnalysisRunRecord{}, ErrAnalysisRunNotFound
	}
	if run.Status == AnalysisRunStatusQueued {
		run.Status = AnalysisRunStatusRunning
		run.StartedAt = &recordedAt
	}
	run.Version++
	event.Version = run.Version
	m.runEvents[analysisRunID] = append(m.runEvents[analysisRunID], event)
	m.analysisRuns[analysisRunID] = run
	for i, task := range m.runTasks {
		if task.AnalysisRunID == analysisRunID && task.Status == AnalysisRunTaskStatusClaimed {
			task.HeartbeatAt = &recordedAt
			m.runTasks[i] = task
		}
	}
	return run, nil
}

func (m *memoryStateStore) FinalizeAnalysisRunTask(_ context.Context, owner OwnerScope, analysisRunID, status string, event RunEventRecord, finalizedAt time.Time) (AnalysisRunRecord, error) {
	run, err := m.GetAnalysisRun(context.Background(), owner, analysisRunID)
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	if terminalRunStatus(run.Status) {
		return run, nil
	}
	run.Status = status
	run.CompletedAt = &finalizedAt
	if status == AnalysisRunStatusCanceled {
		run.CanceledAt = &finalizedAt
	}
	run.Version++
	event.Version = run.Version
	m.runEvents[analysisRunID] = append(m.runEvents[analysisRunID], event)
	m.analysisRuns[analysisRunID] = run
	for i, task := range m.runTasks {
		if task.AnalysisRunID == analysisRunID && (task.Status == AnalysisRunTaskStatusClaimed || task.Status == AnalysisRunTaskStatusQueued || task.Status == AnalysisRunTaskStatusPendingEnqueue) {
			task.Status = status
			task.FinalizedAt = &finalizedAt
			task.HeartbeatAt = &finalizedAt
			m.runTasks[i] = task
		}
	}
	return run, nil
}

func (m *memoryStateStore) ApplyRetentionPolicies(_ context.Context, now time.Time) (RetentionSweepResult, error) {
	var result RetentionSweepResult
	expiredMedia := map[string]struct{}{}
	for id, item := range m.mediaItems {
		if item.Retention.State == RetentionStateHeld {
			continue
		}
		if due(item.Retention.ExpiresAt, now) || due(item.Source.ExpiresAt, now) {
			item.Status = MediaStatusDeleted
			item.Retention.State = RetentionStateExpired
			item.DeletedAt = &now
			item.UpdatedAt = now
			m.mediaItems[id] = item
			expiredMedia[id] = struct{}{}
			result.ExpiredMediaItems++
		}
	}
	for id, collection := range m.collections {
		items := collection.Items[:0]
		for _, item := range collection.Items {
			if _, expired := expiredMedia[item.MediaItemID]; expired {
				item.RemovedAt = &now
				result.RemovedCollectionItems++
				continue
			}
			items = append(items, item)
		}
		collection.Items = items
		if collection.Kind == CollectionKindUser && collection.Status == CollectionStatusActive && len(collection.Items) == 0 {
			collection.Status = CollectionStatusArchived
			collection.ArchivedAt = &now
			result.ArchivedCollections++
		}
		m.collections[id] = collection
	}
	for id, selection := range m.selections {
		if selection.Status != SelectionStatusSealed {
			continue
		}
		invalid := false
		for _, item := range selection.Items {
			if _, expired := expiredMedia[item.MediaItemID]; expired {
				invalid = true
				break
			}
		}
		if invalid && !m.selectionHasActiveRun(selection.ID) {
			selection.Status = SelectionStatusInvalidated
			m.selections[id] = selection
			result.InvalidatedSelections++
		}
	}
	for id, run := range m.analysisRuns {
		if due(run.ExpiresAt, now) && !terminalRunStatus(run.Status) {
			run.Status = AnalysisRunStatusExpired
			run.CompletedAt = &now
			run.Version++
			m.analysisRuns[id] = run
			result.ExpiredAnalysisRuns++
		}
	}
	for id, artifact := range m.artifacts {
		if due(artifact.ExpiresAt, now) && artifact.Status != ArtifactStatusExpired && artifact.Status != ArtifactStatusDeleted {
			artifact.Status = ArtifactStatusExpired
			artifact.Retention.State = RetentionStateExpired
			artifact.DeletedAt = &now
			m.artifacts[id] = artifact
			result.ExpiredArtifacts++
		}
	}
	return result, nil
}

func (m *memoryStateStore) DetectOrphanObjects(_ context.Context) ([]OrphanObjectRecord, error) {
	var orphans []OrphanObjectRecord
	for _, item := range m.mediaItems {
		if item.Source.ObjectKey == "" {
			continue
		}
		if item.Status == MediaStatusDeleted || item.Retention.State == RetentionStateExpired || item.Retention.State == RetentionStateHardDeleteEligible {
			orphans = append(orphans, OrphanObjectRecord{
				SubjectType: "source",
				SubjectID:   item.Source.SourceID,
				Owner:       item.Owner,
				Bucket:      SourcesBucket,
				ObjectKey:   item.Source.ObjectKey,
				Reason:      "expired_media_source",
			})
		}
	}
	for _, artifact := range m.artifacts {
		if artifact.ObjectKey == "" {
			continue
		}
		if artifact.Status == ArtifactStatusExpired || artifact.Status == ArtifactStatusDeleted || artifact.Retention.State == RetentionStateExpired || artifact.Retention.State == RetentionStateHardDeleteEligible {
			orphans = append(orphans, OrphanObjectRecord{
				SubjectType: "artifact",
				SubjectID:   artifact.ID,
				Owner:       artifact.Owner,
				Bucket:      ArtifactsBucket,
				ObjectKey:   artifact.ObjectKey,
				Reason:      "expired_artifact",
			})
		}
	}
	return orphans, nil
}

func (m *memoryStateStore) RecordOrphanObjectCleanup(_ context.Context, orphan OrphanObjectRecord, deleted bool, message string, now time.Time) error {
	switch orphan.SubjectType {
	case "source":
		for id, item := range m.mediaItems {
			if item.Source.SourceID == orphan.SubjectID {
				if deleted {
					item.Retention.State = RetentionStateHardDeleteEligible
				}
				m.mediaItems[id] = item
			}
		}
	case "artifact":
		artifact := m.artifacts[orphan.SubjectID]
		if deleted {
			artifact.Status = ArtifactStatusDeleted
			artifact.Retention.State = RetentionStateHardDeleteEligible
		}
		m.artifacts[orphan.SubjectID] = artifact
	}
	severity := "warning"
	code := "orphan_object_cleanup"
	if strings.Contains(strings.ToLower(message), "delete failed") {
		severity = "error"
		code = "orphan_object_cleanup_failed"
	}
	m.diagnostics = append(m.diagnostics, DiagnosticRecord{
		ID:          "diag-" + orphan.SubjectID,
		Owner:       orphan.Owner,
		SubjectType: orphan.SubjectType,
		SubjectID:   orphan.SubjectID,
		Severity:    severity,
		Code:        code,
		Message:     message,
		CreatedAt:   now,
	})
	return nil
}

func (m *memoryStateStore) ListOperationalDiagnostics(_ context.Context, codes []string) ([]DiagnosticRecord, error) {
	allowed := map[string]bool{}
	for _, code := range codes {
		if code = strings.TrimSpace(code); code != "" {
			allowed[code] = true
		}
	}
	diagnostics := make([]DiagnosticRecord, 0, len(m.diagnostics))
	for _, diagnostic := range m.diagnostics {
		if len(allowed) == 0 || allowed[diagnostic.Code] {
			diagnostics = append(diagnostics, diagnostic)
		}
	}
	return diagnostics, nil
}

func (m *memoryStateStore) ListPendingEnqueueTasks(_ context.Context, limit int) ([]AnalysisRunTaskRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	tasks := make([]AnalysisRunTaskRecord, 0, limit)
	for _, task := range m.runTasks {
		run := m.analysisRuns[task.AnalysisRunID]
		if task.Status == AnalysisRunTaskStatusPendingEnqueue && !terminalRunStatus(run.Status) {
			tasks = append(tasks, task)
			if len(tasks) == limit {
				break
			}
		}
	}
	return tasks, nil
}

func (m *memoryStateStore) ListAnalysisRunQueue(_ context.Context, status, runType, taskType string, limit int) ([]AnalysisRunQueueRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	records := make([]AnalysisRunQueueRecord, 0, limit)
	for _, task := range m.runTasks {
		run := m.analysisRuns[task.AnalysisRunID]
		if terminalRunStatus(run.Status) {
			continue
		}
		if status != "" && task.Status != status {
			continue
		}
		if runType != "" && run.RunType != runType {
			continue
		}
		if taskType != "" && task.TaskType != taskType {
			continue
		}
		records = append(records, AnalysisRunQueueRecord{
			AnalysisRunID: task.AnalysisRunID,
			RunType:       run.RunType,
			WorkerKind:    task.WorkerKind,
			TaskType:      task.TaskType,
			Status:        task.Status,
			Version:       run.Version,
			AttemptNo:     task.AttemptNo,
			CreatedAt:     task.CreatedAt,
		})
		if len(records) == limit {
			break
		}
	}
	return records, nil
}

func (m *memoryStateStore) MarkAnalysisRunTaskQueued(_ context.Context, analysisRunID, taskType string, queuedAt time.Time) error {
	for i, task := range m.runTasks {
		if task.AnalysisRunID == analysisRunID && task.TaskType == taskType && task.Status == AnalysisRunTaskStatusPendingEnqueue {
			task.Status = AnalysisRunTaskStatusQueued
			task.HeartbeatAt = &queuedAt
			m.runTasks[i] = task
			return nil
		}
	}
	return ErrExecutionNotFound
}

func (m *memoryStateStore) ClaimAnalysisRunTask(_ context.Context, analysisRunID, workerKind, taskType, leaseOwner string, claimedAt time.Time) (AnalysisRunRecord, bool, error) {
	run, ok := m.analysisRuns[analysisRunID]
	if !ok {
		return AnalysisRunRecord{}, false, ErrAnalysisRunNotFound
	}
	for i, task := range m.runTasks {
		if task.AnalysisRunID != analysisRunID || task.WorkerKind != workerKind || task.TaskType != taskType {
			continue
		}
		if task.Status != AnalysisRunTaskStatusQueued && task.Status != AnalysisRunTaskStatusPendingEnqueue {
			return run, false, nil
		}
		task.Status = AnalysisRunTaskStatusClaimed
		task.LeaseOwner = leaseOwner
		task.ClaimedAt = &claimedAt
		task.HeartbeatAt = &claimedAt
		m.runTasks[i] = task
		if run.Status == AnalysisRunStatusQueued {
			run.Status = AnalysisRunStatusRunning
			run.StartedAt = &claimedAt
			run.Version++
			m.analysisRuns[analysisRunID] = run
		}
		return run, true, nil
	}
	return run, false, nil
}

func (m *memoryStateStore) findInbox(owner OwnerScope) (CollectionRecord, bool) {
	for _, collection := range m.collections {
		if collection.Kind == CollectionKindInbox && SameOwner(collection.Owner, owner) && collection.Status == CollectionStatusActive {
			return collection, true
		}
	}
	return CollectionRecord{}, false
}

func (m *memoryStateStore) selectionHasActiveRun(selectionID string) bool {
	for _, run := range m.analysisRuns {
		if run.SelectionID == selectionID && !terminalRunStatus(run.Status) {
			return true
		}
	}
	return false
}

func due(expiresAt *time.Time, now time.Time) bool {
	return expiresAt != nil && !expiresAt.After(now)
}

type fakeObjectStore struct {
	puts     []objectPutRecord
	presigns []objectPresignRecord
}

type objectPutRecord struct {
	bucket      string
	objectKey   string
	contentType string
	body        []byte
}

type objectPresignRecord struct {
	bucket    string
	objectKey string
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{}
}

func (f *fakeObjectStore) PutObject(_ context.Context, bucket, objectKey, contentType string, body []byte) error {
	f.puts = append(f.puts, objectPutRecord{
		bucket:      bucket,
		objectKey:   objectKey,
		contentType: contentType,
		body:        append([]byte(nil), body...),
	})
	return nil
}

func (f *fakeObjectStore) PresignGetObject(_ context.Context, bucket, objectKey string, _ time.Duration) (string, time.Time, error) {
	f.presigns = append(f.presigns, objectPresignRecord{
		bucket: bucket,
		objectKey: objectKey,
	})
	return "https://minio.local/presigned", time.Now().UTC().Add(time.Minute), nil
}

type failingObjectStore struct{}

func (f *failingObjectStore) PutObject(context.Context, string, string, string, []byte) error {
	return nil
}

func (f *failingObjectStore) PresignGetObject(context.Context, string, string, time.Duration) (string, time.Time, error) {
	return "", time.Time{}, errors.New("presign failed")
}

func sequenceIDs(ids ...string) func() string {
	idx := 0
	return func() string {
		if idx >= len(ids) {
			return "generated-id"
		}
		id := ids[idx]
		idx++
		return id
	}
}
