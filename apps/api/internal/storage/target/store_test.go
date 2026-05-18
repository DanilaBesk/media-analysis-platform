package target

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreWritesTargetStateWithoutLegacyTables(t *testing.T) {
	t.Parallel()

	db, recorder := openRecordingDB(t)
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	now := time.Date(2026, 5, 18, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	if err := store.UpsertChannelAccount(ctx, ChannelAccountRecord{
		ID:                 "channel-account-1",
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Danila",
		Status:             "active",
		MetadataJSON:       []byte(`{"locale":"ru"}`),
		CreatedAt:          now,
		UpdatedAt:          now,
		LastSeenAt:         &now,
	}); err != nil {
		t.Fatalf("UpsertChannelAccount() error = %v", err)
	}
	if _, err := store.RecordOperationRequest(ctx, OperationRequestRecord{
		ID:               "operation-1",
		ChannelAccountID: "channel-account-1",
		OperationType:    "media_asset.create",
		IdempotencyKey:   "telegram:update:1",
		RequestHash:      "sha256:request",
		Status:           "accepted",
		TargetType:       "media_asset",
		TargetID:         "asset-1",
		MetadataJSON:     []byte(`{}`),
		CreatedAt:        now,
	}); err != nil {
		t.Fatalf("RecordOperationRequest() error = %v", err)
	}
	if err := store.CreateMediaAssetWithInbox(ctx, CreateMediaAssetWithInboxParams{
		StoredObject: StoredObjectRecord{
			ID:             "stored-object-1",
			Bucket:         "sources",
			ObjectKey:      "sources/asset-1/source.ogg",
			ContentType:    "audio/ogg",
			SizeBytes:      100,
			Checksum:       "sha256:object",
			StorageStatus:  "available",
			RetentionState: "active",
			CreatedAt:      now,
		},
		MediaAsset: MediaAssetRecord{
			ID:               "asset-1",
			ChannelAccountID: "channel-account-1",
			StoredObjectID:   "stored-object-1",
			OriginType:       "telegram_file",
			OriginRef:        "voice-file-id",
			Kind:             "voice",
			DisplayName:      "voice.ogg",
			Status:           "available",
			MetadataJSON:     []byte(`{}`),
			CreatedAt:        now,
			UpdatedAt:        now,
		},
		InboxCollection: CollectionRecord{
			ID:               "inbox-1",
			ChannelAccountID: "channel-account-1",
			Kind:             "inbox",
			Name:             "Inbox",
			Status:           "active",
			Version:          1,
			CreatedAt:        now,
			UpdatedAt:        now,
		},
		CollectionItem: CollectionItemRecord{
			ID:              "collection-item-1",
			CollectionID:    "inbox-1",
			MediaAssetID:    "asset-1",
			Position:        0,
			AddedViaChannel: "channel-account-1",
			AddedAt:         now,
		},
	}); err != nil {
		t.Fatalf("CreateMediaAssetWithInbox() error = %v", err)
	}
	if err := store.CreateSelectionSnapshot(ctx, SelectionSnapshotRecord{
		ID:                 "snapshot-1",
		ChannelAccountID:   "channel-account-1",
		SourceCollectionID: "inbox-1",
		Status:             "sealed",
		OptionSnapshotJSON: []byte(`{"language":"ru"}`),
		DiagnosticsJSON:    []byte(`[]`),
		CreatedViaChannel:  "channel-account-1",
		CreatedAt:          now,
		SealedAt:           now,
	}, []SelectionSnapshotItemRecord{{
		ID:                  "snapshot-item-1",
		SelectionSnapshotID: "snapshot-1",
		Position:            0,
		MediaAssetID:        "asset-1",
		Kind:                "voice",
		DisplayName:         "voice.ogg",
		OriginSnapshotJSON:  []byte(`{"origin_type":"telegram_file"}`),
		StorageSnapshotJSON: []byte(`{"object_key":"sources/asset-1/source.ogg"}`),
		MetadataJSON:        []byte(`{}`),
		StatusAtSelection:   "available",
		DiagnosticsJSON:     []byte(`[]`),
	}}); err != nil {
		t.Fatalf("CreateSelectionSnapshot() error = %v", err)
	}
	if err := store.CreateAnalysisRunGraph(ctx, AnalysisRunGraph{
		Run: AnalysisRunRecord{
			ID:                "run-1",
			ChannelAccountID:  "channel-account-1",
			SelectionSnapshot: "snapshot-1",
			RunType:           "transcription",
			Status:            "queued",
			Version:           1,
			ParamsJSON:        []byte(`{}`),
			DeliveryJSON:      []byte(`{"strategy":"polling"}`),
			EvidenceGateState: "not_required",
			CreatedViaChannel: "channel-account-1",
			CreatedAt:         now,
		},
		Steps: []AnalysisRunStepRecord{{
			ID:            "step-1",
			AnalysisRunID: "run-1",
			StepKind:      "selection.transcription",
			WorkerKind:    "transcription",
			Status:        "queued",
			AttemptNo:     1,
			MetadataJSON:  []byte(`{}`),
			CreatedAt:     now,
		}},
		StepInputs: []AnalysisRunStepInputRecord{{
			ID:                      "step-input-1",
			AnalysisRunStepID:       "step-1",
			InputKind:               "selection_snapshot_item",
			SelectionSnapshotItemID: "snapshot-item-1",
			Position:                0,
			Required:                true,
			MetadataJSON:            []byte(`{}`),
			CreatedAt:               now,
		}},
		Event: AnalysisRunEventRecord{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Version:       1,
			Status:        "queued",
			PayloadJSON:   []byte(`{}`),
			CreatedAt:     now,
		},
	}); err != nil {
		t.Fatalf("CreateAnalysisRunGraph() error = %v", err)
	}
	if err := store.RecordArtifacts(ctx, []StoredObjectRecord{{
		ID:             "stored-object-2",
		Bucket:         "artifacts",
		ObjectKey:      "run-1/transcript/plain/transcript.txt",
		ContentType:    "text/plain",
		SizeBytes:      10,
		Checksum:       "sha256:artifact",
		StorageStatus:  "available",
		RetentionState: "active",
		CreatedAt:      now,
	}}, []ArtifactRecord{{
		ID:               "artifact-1",
		ChannelAccountID: "channel-account-1",
		AnalysisRunID:    "run-1",
		StoredObjectID:   "stored-object-2",
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Checksum:         "sha256:artifact",
		SizeBytes:        10,
		Visibility:       "channel_deliverable",
		PreviewJSON:      []byte(`{"available":true}`),
		CreatedAt:        now,
	}}, []ArtifactSubjectRecord{{
		ID:          "artifact-subject-1",
		ArtifactID:  "artifact-1",
		SubjectType: "selection_snapshot_item",
		SubjectID:   "snapshot-item-1",
		SubjectRole: "result",
		CreatedAt:   now,
	}}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	if err := store.RecordDiagnostics(ctx, []DiagnosticRecord{{
		ID:                 "diagnostic-1",
		ChannelAccountID:   "channel-account-1",
		SubjectType:        "analysis_run",
		SubjectID:          "run-1",
		Severity:           "warning",
		Code:               "analysis_prerequisite_missing",
		Message:            "Transcript is missing",
		ContextJSON:        []byte(`{}`),
		SafeChannelContext: []byte(`{"channel":"telegram"}`),
		CorrelationID:      "corr-1",
		RemediationHint:    "retry after transcription",
		CreatedAt:          now,
	}}); err != nil {
		t.Fatalf("RecordDiagnostics() error = %v", err)
	}
	if _, err := store.UpsertChannelSurface(ctx, ChannelSurfaceRecord{
		ID:                 "surface-1",
		ChannelAccountID:   "channel-account-1",
		Channel:            "telegram",
		SurfaceType:        "message",
		SurfaceKey:         "run:run-1",
		AddressJSON:        []byte(`{"chat_id":"chat-1","message_id":42}`),
		AddressFingerprint: "telegram:chat-1:42",
		DisplayStateJSON:   []byte(`{"status":"queued"}`),
		LifecycleStatus:    "active",
		Version:            1,
		IdempotencyKey:     "surface:key",
		CreatedAt:          now,
		UpdatedAt:          now,
		LastRenderedAt:     &now,
	}, []ChannelSurfaceSubjectRecord{{
		SurfaceID:   "surface-1",
		SubjectType: "analysis_run",
		SubjectID:   "run-1",
		SubjectRole: "primary",
		CreatedAt:   now,
	}}); err != nil {
		t.Fatalf("UpsertChannelSurface() error = %v", err)
	}
	if err := store.SupersedeChannelSurface(ctx, SupersedeChannelSurfaceParams{
		SurfaceID:    "surface-1",
		SupersededAt: now,
		Event: ChannelSurfaceEventRecord{
			ID:              "surface-event-1",
			SurfaceID:       "surface-1",
			EventType:       "channel_surface.superseded",
			Reason:          "message_not_editable",
			PreviousVersion: 1,
			NextVersion:     2,
			ActorType:       "telegram_adapter",
			ActorID:         "bot",
			MetadataJSON:    []byte(`{}`),
			CreatedAt:       now,
		},
	}); err != nil {
		t.Fatalf("SupersedeChannelSurface() error = %v", err)
	}

	queries := recorder.joinedQueries()
	required := []string{
		"INSERT INTO channel_accounts",
		"INSERT INTO operation_requests",
		"ON CONFLICT (channel_account_id, operation_type, idempotency_key)",
		"DO NOTHING",
		"INSERT INTO stored_objects",
		"INSERT INTO media_assets",
		"INSERT INTO collections",
		"INSERT INTO collection_items",
		"INSERT INTO selection_snapshots",
		"INSERT INTO selection_snapshot_items",
		"INSERT INTO analysis_runs",
		"INSERT INTO analysis_run_steps",
		"INSERT INTO analysis_run_step_inputs",
		"INSERT INTO analysis_run_events",
		"INSERT INTO artifacts",
		"INSERT INTO artifact_subjects",
		"INSERT INTO diagnostics",
		"safe_channel_context",
		"INSERT INTO channel_surfaces",
		"ON CONFLICT (channel_account_id, channel, surface_type, surface_key)",
		"INSERT INTO channel_surface_subjects",
		"UPDATE channel_surfaces",
		"lifecycle_status='superseded'",
		"INSERT INTO channel_surface_events",
	}
	for _, fragment := range required {
		if !strings.Contains(queries, fragment) {
			t.Fatalf("store SQL missing %q\nSQL:\n%s", fragment, queries)
		}
	}

	forbidden := []string{
		"INSERT INTO sources",
		"INSERT INTO media_items",
		"INSERT INTO selections",
		"INSERT INTO selection_items",
		"INSERT INTO analysis_run_tasks",
		"owner_type",
		"owner_id",
		"tenant_id",
		"safe_adapter_context",
		"media_item_id",
	}
	for _, fragment := range forbidden {
		if strings.Contains(queries, fragment) {
			t.Fatalf("store SQL contains legacy fragment %q\nSQL:\n%s", fragment, queries)
		}
	}
}

func TestDeterministicSeedFixturesAreStable(t *testing.T) {
	t.Parallel()

	first := DeterministicSeedFixtures()
	second := DeterministicSeedFixtures()
	if fmt.Sprintf("%#v", first) != fmt.Sprintf("%#v", second) {
		t.Fatalf("DeterministicSeedFixtures() changed between calls:\n%#v\n%#v", first, second)
	}
	if first.ChannelAccount.ID != "00000000-0000-4000-8000-000000000001" {
		t.Fatalf("seed channel account id = %q", first.ChannelAccount.ID)
	}
	if first.InboxCollection.Kind != "inbox" || first.InboxCollection.ChannelAccountID != first.ChannelAccount.ID {
		t.Fatalf("seed inbox collection = %#v", first.InboxCollection)
	}
}

type recordingDriverState struct {
	mu      sync.Mutex
	queries []string
}

func (s *recordingDriverState) joinedQueries() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.queries, "\n\n")
}

func (s *recordingDriverState) record(query string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, query)
}

func openRecordingDB(t *testing.T) (*sql.DB, *recordingDriverState) {
	t.Helper()

	state := &recordingDriverState{}
	name := fmt.Sprintf("target_storage_recording_%d", recordingDriverSeq.Add(1))
	sql.Register(name, recordingDriver{state: state})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open(%s) error = %v", name, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})
	return db, state
}

var recordingDriverSeq atomic.Uint64

type recordingDriver struct {
	state *recordingDriverState
}

func (d recordingDriver) Open(string) (driver.Conn, error) {
	return &recordingConn{state: d.state}, nil
}

type recordingConn struct {
	state *recordingDriverState
}

func (c *recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are not used by target storage tests")
}

func (c *recordingConn) Close() error {
	return nil
}

func (c *recordingConn) Begin() (driver.Tx, error) {
	return recordingTx{}, nil
}

func (c *recordingConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return recordingTx{}, nil
}

func (c *recordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.record(query)
	return driver.RowsAffected(1), nil
}

func (c *recordingConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.record(query)
	now := time.Date(2026, 5, 18, 9, 0, 0, 0, time.UTC)
	return &recordingRows{
		columns: []string{
			"id",
			"channel_account_id",
			"channel",
			"surface_type",
			"surface_key",
			"address",
			"address_fingerprint",
			"display_state",
			"lifecycle_status",
			"version",
			"idempotency_key",
			"created_at",
			"updated_at",
			"last_rendered_at",
			"superseded_at",
			"deleted_at",
		},
		values: []driver.Value{
			"surface-1",
			"channel-account-1",
			"telegram",
			"message",
			"run:run-1",
			[]byte(`{"chat_id":"chat-1","message_id":42}`),
			"telegram:chat-1:42",
			[]byte(`{"status":"queued"}`),
			"active",
			int64(1),
			"surface:key",
			now,
			now,
			now,
			nil,
			nil,
		},
	}, nil
}

type recordingRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (r *recordingRows) Columns() []string {
	return r.columns
}

func (r *recordingRows) Close() error {
	return nil
}

func (r *recordingRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}

type recordingTx struct{}

func (recordingTx) Commit() error {
	return nil
}

func (recordingTx) Rollback() error {
	return nil
}
