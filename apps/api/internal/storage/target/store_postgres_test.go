package target

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestTargetStorePostgresContracts(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyTargetMigration(t, ctx, db)
	assertTargetSchemaState(t, ctx, db)

	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	now := time.Date(2026, 5, 18, 15, 0, 0, 0, time.UTC)
	seed := DeterministicSeedFixtures()
	seedAccount, err := store.UpsertChannelAccount(ctx, seed.ChannelAccount)
	if err != nil {
		t.Fatalf("upsert deterministic channel account: %v", err)
	}
	if seedAccount.ID != seed.ChannelAccount.ID {
		t.Fatalf("seed channel account id = %q, want %q", seedAccount.ID, seed.ChannelAccount.ID)
	}
	telegramAccount, err := store.UpsertChannelAccount(ctx, ChannelAccountRecord{
		ID:                 targetTestTelegramChannelID,
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Telegram",
		Status:             "active",
		MetadataJSON:       []byte(`{"fixture":"telegram"}`),
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil {
		t.Fatalf("upsert telegram channel account: %v", err)
	}
	if telegramAccount.ID != targetTestTelegramChannelID {
		t.Fatalf("telegram channel account id = %q, want %q", telegramAccount.ID, targetTestTelegramChannelID)
	}
	replayedTelegramAccount, err := store.UpsertChannelAccount(ctx, ChannelAccountRecord{
		ID:                 "00000000-0000-4000-8000-000000009999",
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Telegram replay",
		Status:             "active",
		MetadataJSON:       []byte(`{"fixture":"telegram-replay"}`),
		CreatedAt:          now.Add(time.Second),
		UpdatedAt:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("upsert replayed telegram channel account: %v", err)
	}
	if replayedTelegramAccount.ID != targetTestTelegramChannelID {
		t.Fatalf("replayed telegram channel account id = %q, want persisted conflict id %q", replayedTelegramAccount.ID, targetTestTelegramChannelID)
	}
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM channel_accounts WHERE channel=$1 AND external_account_ref=$2`, 1, "telegram", "chat-1")

	t.Run("operation requests replay the original target", func(t *testing.T) {
		first, err := store.RecordOperationRequest(ctx, OperationRequestRecord{
			ID:               targetTestOperationID,
			ChannelAccountID: seed.ChannelAccount.ID,
			OperationType:    "media_asset.create",
			IdempotencyKey:   "target-fixture:create-media",
			RequestHash:      "sha256:first",
			Status:           "accepted",
			TargetType:       "media_asset",
			TargetID:         targetTestLocalAssetID,
			MetadataJSON:     []byte(`{"attempt":1}`),
			CreatedAt:        now,
		})
		if err != nil {
			t.Fatalf("RecordOperationRequest(first) error = %v", err)
		}
		replayed, err := store.RecordOperationRequest(ctx, OperationRequestRecord{
			ID:               targetTestReplayOperationID,
			ChannelAccountID: seed.ChannelAccount.ID,
			OperationType:    "media_asset.create",
			IdempotencyKey:   "target-fixture:create-media",
			RequestHash:      "sha256:second",
			Status:           "accepted",
			TargetType:       "media_asset",
			TargetID:         targetTestTelegramAssetID,
			MetadataJSON:     []byte(`{"attempt":2}`),
			CreatedAt:        now.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("RecordOperationRequest(replay) error = %v", err)
		}
		if replayed.ID != first.ID || replayed.TargetID != first.TargetID || replayed.RequestHash != first.RequestHash {
			t.Fatalf("replayed operation = %#v, want original %#v", replayed, first)
		}
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM operation_requests WHERE channel_account_id=$1 AND operation_type=$2 AND idempotency_key=$3`, 1, seed.ChannelAccount.ID, "media_asset.create", "target-fixture:create-media")
	})

	t.Run("media assets are channel scoped and deleted assets leave lists", func(t *testing.T) {
		createTargetTestMediaAsset(t, ctx, store, targetMediaFixture{
			channelID:    seed.ChannelAccount.ID,
			inboxID:      seed.InboxCollection.ID,
			storedID:     targetTestLocalStoredObjectID,
			assetID:      targetTestLocalAssetID,
			itemID:       targetTestLocalCollectionItemID,
			bucket:       "media-inputs",
			objectKey:    "document-note.txt",
			kind:         "document",
			displayName:  "document-note.txt",
			originType:   "upload",
			originRef:    "document-note.txt",
			checksum:     "sha256:local",
			createdAt:    now,
			metadataJSON: []byte(`{"fixture":"local"}`),
		})
		createTargetTestMediaAsset(t, ctx, store, targetMediaFixture{
			channelID:    targetTestTelegramChannelID,
			inboxID:      targetTestTelegramInboxID,
			storedID:     targetTestTelegramStoredObjectID,
			assetID:      targetTestTelegramAssetID,
			itemID:       targetTestTelegramCollectionItemID,
			bucket:       "media-inputs",
			objectKey:    "telegram-voice.ogg",
			kind:         "voice",
			displayName:  "voice.ogg",
			originType:   "telegram_file",
			originRef:    "telegram-file-id",
			checksum:     "sha256:telegram",
			createdAt:    now.Add(time.Second),
			metadataJSON: []byte(`{"fixture":"telegram"}`),
		})

		localAssets, err := store.ListMediaAssets(ctx, seed.ChannelAccount.ID, 1)
		if err != nil {
			t.Fatalf("ListMediaAssets(local) error = %v", err)
		}
		if len(localAssets) != 1 || localAssets[0].ID != targetTestLocalAssetID {
			t.Fatalf("local media assets = %#v", localAssets)
		}
		if _, err := store.GetMediaAsset(ctx, targetTestTelegramChannelID, targetTestLocalAssetID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("GetMediaAsset(cross-channel) error = %v, want sql.ErrNoRows", err)
		}
		deleted, err := store.DeleteMediaAsset(ctx, seed.ChannelAccount.ID, targetTestLocalAssetID, now.Add(2*time.Second))
		if err != nil {
			t.Fatalf("DeleteMediaAsset() error = %v", err)
		}
		if deleted.Status != "deleted" || deleted.DeletedAt == nil {
			t.Fatalf("deleted media asset = %#v", deleted)
		}
		localAssets, err = store.ListMediaAssets(ctx, seed.ChannelAccount.ID, 20)
		if err != nil {
			t.Fatalf("ListMediaAssets(after delete) error = %v", err)
		}
		if len(localAssets) != 0 {
			t.Fatalf("deleted local asset must not appear in lists: %#v", localAssets)
		}
	})

	t.Run("collections use optimistic versions and reject duplicate active positions", func(t *testing.T) {
		collection := CollectionRecord{
			ID:               targetTestCollectionID,
			ChannelAccountID: targetTestTelegramChannelID,
			Kind:             "user",
			Name:             "Research",
			Status:           "active",
			Version:          1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		must(t, store.CreateCollection(ctx, collection, []CollectionItemRecord{{
			ID:              targetTestCollectionItemID,
			CollectionID:    targetTestCollectionID,
			MediaAssetID:    targetTestTelegramAssetID,
			Position:        0,
			AddedViaChannel: targetTestTelegramChannelID,
			AddedAt:         now,
		}}), "create collection")
		updated, _, err := store.UpdateCollection(ctx, UpdateCollectionParams{
			ChannelAccountID: targetTestTelegramChannelID,
			CollectionID:     targetTestCollectionID,
			ExpectedVersion:  1,
			Name:             "Research v2",
			UpdatedAt:        now.Add(3 * time.Second),
		})
		if err != nil {
			t.Fatalf("UpdateCollection() error = %v", err)
		}
		if updated.Version != 2 || updated.Name != "Research v2" {
			t.Fatalf("updated collection = %#v", updated)
		}
		if _, _, err := store.UpdateCollection(ctx, UpdateCollectionParams{
			ChannelAccountID: targetTestTelegramChannelID,
			CollectionID:     targetTestCollectionID,
			ExpectedVersion:  1,
			Name:             "stale",
			UpdatedAt:        now.Add(4 * time.Second),
		}); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("UpdateCollection(stale) error = %v, want sql.ErrNoRows", err)
		}
		if _, _, err := store.UpdateCollectionItems(ctx, UpdateCollectionItemsParams{
			ChannelAccountID: targetTestTelegramChannelID,
			CollectionID:     targetTestCollectionID,
			ExpectedVersion:  2,
			Items: []CollectionItemRecord{
				{ID: targetTestCollectionItemTwoID, MediaAssetID: targetTestTelegramAssetID, Position: 0, AddedViaChannel: targetTestTelegramChannelID},
				{ID: targetTestCollectionItemThreeID, MediaAssetID: targetTestTelegramAssetID, Position: 0, AddedViaChannel: targetTestTelegramChannelID},
			},
			UpdatedAt: now.Add(5 * time.Second),
		}); err == nil {
			t.Fatalf("UpdateCollectionItems(duplicate positions) error = nil, want constraint failure")
		}
	})

	t.Run("selection snapshots are immutable and preserve copied item facts", func(t *testing.T) {
		snapshot := SelectionSnapshotRecord{
			ID:                 targetTestSnapshotID,
			ChannelAccountID:   targetTestTelegramChannelID,
			SourceCollectionID: targetTestTelegramInboxID,
			Status:             "sealed",
			OptionSnapshotJSON: []byte(`{"language":"ru"}`),
			DiagnosticsJSON:    []byte(`[]`),
			CreatedViaChannel:  targetTestTelegramChannelID,
			CreatedAt:          now,
			SealedAt:           now,
		}
		items := []SelectionSnapshotItemRecord{{
			ID:                  targetTestSnapshotItemID,
			SelectionSnapshotID: targetTestSnapshotID,
			Position:            0,
			MediaAssetID:        targetTestTelegramAssetID,
			Kind:                "voice",
			DisplayName:         "voice.ogg",
			OriginSnapshotJSON:  []byte(`{"origin_type":"telegram_file"}`),
			StorageSnapshotJSON: []byte(`{"bucket":"media-inputs","object_key":"telegram-voice.ogg"}`),
			MetadataJSON:        []byte(`{"fixture":"snapshot"}`),
			StatusAtSelection:   "available",
			DiagnosticsJSON:     []byte(`[]`),
		}}
		must(t, store.CreateSelectionSnapshot(ctx, snapshot, items), "create selection snapshot")
		if _, err := db.ExecContext(ctx, `UPDATE selection_snapshots SET status='invalidated' WHERE id=$1`, targetTestSnapshotID); err == nil {
			t.Fatalf("selection snapshot update succeeded, want immutable trigger failure")
		}
		if _, err := db.ExecContext(ctx, `UPDATE media_assets SET display_name='renamed.ogg' WHERE id=$1`, targetTestTelegramAssetID); err != nil {
			t.Fatalf("rename media asset after snapshot: %v", err)
		}
		snapshotItems, err := store.ListSelectionSnapshotItems(ctx, targetTestSnapshotID)
		if err != nil {
			t.Fatalf("ListSelectionSnapshotItems() error = %v", err)
		}
		if len(snapshotItems) != 1 || snapshotItems[0].DisplayName != "voice.ogg" {
			t.Fatalf("snapshot items changed after media edit: %#v", snapshotItems)
		}
	})

	t.Run("analysis run lifecycle uses declared inputs, events, cancellation, artifacts, diagnostics, and retention states", func(t *testing.T) {
		graph := AnalysisRunGraph{
			Run: AnalysisRunRecord{
				ID:                targetTestRunID,
				ChannelAccountID:  targetTestTelegramChannelID,
				SelectionSnapshot: targetTestSnapshotID,
				RunType:           "transcription",
				Status:            "queued",
				Version:           1,
				IdempotencyKey:    "target-fixture:run",
				ParamsJSON:        []byte(`{"language":"ru"}`),
				DeliveryJSON:      []byte(`{"strategy":"polling"}`),
				EvidenceGateState: "not_required",
				CreatedViaChannel: targetTestTelegramChannelID,
				CreatedAt:         now,
			},
			Steps: []AnalysisRunStepRecord{{
				ID:            targetTestStepID,
				AnalysisRunID: targetTestRunID,
				StepKind:      "selection.transcription",
				WorkerKind:    "transcription",
				Status:        "queued",
				AttemptNo:     1,
				MetadataJSON:  []byte(`{}`),
				CreatedAt:     now,
			}},
			StepInputs: []AnalysisRunStepInputRecord{{
				ID:                      targetTestStepInputID,
				AnalysisRunStepID:       targetTestStepID,
				InputKind:               "selection_snapshot_item",
				SelectionSnapshotItemID: targetTestSnapshotItemID,
				Position:                0,
				Required:                true,
				MetadataJSON:            []byte(`{}`),
				CreatedAt:               now,
			}},
			Event: AnalysisRunEventRecord{
				ID:            targetTestRunEventID,
				AnalysisRunID: targetTestRunID,
				EventType:     "analysis_run.created",
				Version:       1,
				Status:        "queued",
				PayloadJSON:   []byte(`{}`),
				CreatedAt:     now,
			},
		}
		must(t, store.CreateAnalysisRunGraph(ctx, graph), "create analysis run graph")
		queue, err := store.ListAnalysisRunStepQueue(ctx, "queued", "transcription", "transcription", "selection.transcription", 1)
		if err != nil {
			t.Fatalf("ListAnalysisRunStepQueue() error = %v", err)
		}
		if len(queue) != 1 || queue[0].AnalysisRunStepID != targetTestStepID {
			t.Fatalf("queue = %#v", queue)
		}
		step, inputs, claimed, err := store.ClaimAnalysisRunStep(ctx, targetTestRunID, "transcription", "selection.transcription", "worker-1", now.Add(time.Second))
		if err != nil || !claimed {
			t.Fatalf("ClaimAnalysisRunStep() step=%#v claimed=%v err=%v", step, claimed, err)
		}
		if len(inputs) != 1 || inputs[0].SelectionSnapshotItemID != targetTestSnapshotItemID {
			t.Fatalf("claimed inputs = %#v", inputs)
		}
		must(t, store.RecordAnalysisRunStepProgress(ctx, RecordAnalysisRunProgressParams{
			AnalysisRunID:     targetTestRunID,
			AnalysisRunStepID: targetTestStepID,
			HeartbeatAt:       now.Add(2 * time.Second),
			Event: AnalysisRunEventRecord{
				ID:            targetTestProgressEventID,
				AnalysisRunID: targetTestRunID,
				EventType:     "analysis_run_step.progress",
				Status:        "running",
				PayloadJSON:   []byte(`{"progress_stage":"transcribing"}`),
				CreatedAt:     now.Add(2 * time.Second),
			},
		}), "record progress")
		canceled, err := store.RequestAnalysisRunCancel(ctx, targetTestTelegramChannelID, targetTestRunID, AnalysisRunEventRecord{
			ID:            targetTestCancelEventID,
			AnalysisRunID: targetTestRunID,
			EventType:     "analysis_run.cancel_requested",
			Status:        "cancel_requested",
			PayloadJSON:   []byte(`{"message":"stop"}`),
			CreatedAt:     now.Add(3 * time.Second),
		}, now.Add(3*time.Second))
		if err != nil {
			t.Fatalf("RequestAnalysisRunCancel() error = %v", err)
		}
		if canceled.Status != "cancel_requested" || canceled.CancelRequestedAt == nil {
			t.Fatalf("canceled run = %#v", canceled)
		}
		finalRun, err := store.FinalizeAnalysisRunStep(ctx, FinalizeAnalysisRunStepParams{
			AnalysisRunID:     targetTestRunID,
			AnalysisRunStepID: targetTestStepID,
			StepStatus:        "succeeded",
			RunStatus:         "succeeded",
			FinalizedAt:       now.Add(4 * time.Second),
			Event: AnalysisRunEventRecord{
				ID:            targetTestFinalizeEventID,
				AnalysisRunID: targetTestRunID,
				EventType:     "analysis_run_step.finalized",
				PayloadJSON:   []byte(`{"outcome":"succeeded"}`),
				CreatedAt:     now.Add(4 * time.Second),
			},
		})
		if err != nil {
			t.Fatalf("FinalizeAnalysisRunStep() error = %v", err)
		}
		if finalRun.Status != "canceled" || finalRun.CanceledAt == nil {
			t.Fatalf("final run after cancel race = %#v", finalRun)
		}
		events, err := store.ListAnalysisRunEvents(ctx, targetTestTelegramChannelID, targetTestRunID, 10)
		if err != nil {
			t.Fatalf("ListAnalysisRunEvents() error = %v", err)
		}
		if len(events) != 4 || events[0].Version != 1 || events[3].Status != "canceled" {
			t.Fatalf("run events = %#v", events)
		}

		must(t, store.RecordArtifacts(ctx, []StoredObjectRecord{{
			ID:             targetTestArtifactStoredObjectID,
			Bucket:         "artifacts",
			ObjectKey:      "run-summary/report.md",
			ContentType:    "text/markdown",
			SizeBytes:      122,
			Checksum:       "sha256:report",
			StorageStatus:  "available",
			RetentionState: "held",
			CreatedAt:      now,
		}}, []ArtifactRecord{{
			ID:               targetTestArtifactID,
			ChannelAccountID: targetTestTelegramChannelID,
			AnalysisRunID:    targetTestRunID,
			StoredObjectID:   targetTestArtifactStoredObjectID,
			Kind:             "report",
			Status:           "available",
			ContentType:      "text/markdown",
			Checksum:         "sha256:report",
			SizeBytes:        122,
			Visibility:       "channel_deliverable",
			PreviewJSON:      []byte(`{"available":true}`),
			CreatedAt:        now,
		}}, []ArtifactSubjectRecord{{
			ID:          targetTestArtifactSubjectID,
			ArtifactID:  targetTestArtifactID,
			SubjectType: "analysis_run_step",
			SubjectID:   targetTestStepID,
			SubjectRole: "result",
			CreatedAt:   now,
		}}), "record artifact")
		if _, err := store.GetArtifact(ctx, seed.ChannelAccount.ID, targetTestArtifactID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("GetArtifact(cross-channel) error = %v, want sql.ErrNoRows", err)
		}
		object, err := store.GetStoredObject(ctx, targetTestArtifactStoredObjectID)
		if err != nil {
			t.Fatalf("GetStoredObject(artifact) error = %v", err)
		}
		if object.RetentionState != "held" || object.StorageStatus != "available" {
			t.Fatalf("artifact stored object = %#v", object)
		}

		must(t, store.RecordDiagnostics(ctx, []DiagnosticRecord{{
			ID:                 targetTestDiagnosticID,
			ChannelAccountID:   targetTestTelegramChannelID,
			SubjectType:        "analysis_run",
			SubjectID:          targetTestRunID,
			Severity:           "warning",
			Code:               "analysis_prerequisite_missing",
			Message:            "Transcript is missing",
			ContextJSON:        []byte(`{"internal":"operator-evidence"}`),
			SafeChannelContext: []byte(`{"channel":"telegram","hint":"retry"}`),
			CorrelationID:      "corr-target-1",
			RemediationHint:    "Run transcription first",
			CreatedAt:          now,
		}}), "record diagnostic")
		diagnostics, err := store.ListDiagnostics(ctx, DiagnosticQuery{
			ChannelAccountID: targetTestTelegramChannelID,
			SubjectType:      "analysis_run",
			SubjectID:        targetTestRunID,
			Severity:         "warning",
			Code:             "analysis_prerequisite_missing",
			CorrelationID:    "corr-target-1",
		}, 10)
		if err != nil {
			t.Fatalf("ListDiagnostics() error = %v", err)
		}
		if len(diagnostics) != 1 || strings.Contains(string(diagnostics[0].SafeChannelContext), "token") {
			t.Fatalf("diagnostics = %#v", diagnostics)
		}
	})

	t.Run("channel surface upsert hands off an active address to a new surface key", func(t *testing.T) {
		_, err := store.UpsertChannelSurface(ctx, ChannelSurfaceRecord{
			ID:                 targetTestSurfaceAddressOwnerID,
			ChannelAccountID:   seed.ChannelAccount.ID,
			Channel:            "telegram",
			SurfaceType:        "current_materials_panel",
			SurfaceKey:         "current:chat-1:user-1",
			AddressJSON:        []byte(`{"chat_id":"chat-1","message_id":404}`),
			AddressFingerprint: "telegram:chat-1:404",
			DisplayStateJSON:   []byte(`{"screen":"main"}`),
			LifecycleStatus:    "active",
			Version:            1,
			IdempotencyKey:     "surface:address-owner",
			CreatedAt:          now,
			UpdatedAt:          now,
			LastRenderedAt:     &now,
		}, []ChannelSurfaceSubjectRecord{{
			SurfaceID:   targetTestSurfaceAddressOwnerID,
			SubjectType: "collection",
			SubjectID:   targetTestTelegramInboxID,
			SubjectRole: "primary",
			CreatedAt:   now,
		}})
		if err != nil {
			t.Fatalf("UpsertChannelSurface(address owner) error = %v", err)
		}

		handoff, err := store.UpsertChannelSurface(ctx, ChannelSurfaceRecord{
			ID:                 targetTestSurfaceAddressHandoffID,
			ChannelAccountID:   seed.ChannelAccount.ID,
			Channel:            "telegram",
			SurfaceType:        "analysis_task_surface",
			SurfaceKey:         "run:" + targetTestRunID + ":handoff",
			AddressJSON:        []byte(`{"chat_id":"chat-1","message_id":404}`),
			AddressFingerprint: "telegram:chat-1:404",
			DisplayStateJSON:   []byte(`{"screen":"main","focused_run_id":"handoff"}`),
			LifecycleStatus:    "active",
			Version:            1,
			IdempotencyKey:     "surface:address-handoff",
			CreatedAt:          now.Add(time.Second),
			UpdatedAt:          now.Add(time.Second),
			LastRenderedAt:     &now,
		}, []ChannelSurfaceSubjectRecord{{
			SurfaceID:   targetTestSurfaceAddressHandoffID,
			SubjectType: "analysis_run",
			SubjectID:   targetTestRunID,
			SubjectRole: "primary",
			CreatedAt:   now.Add(time.Second),
		}})
		if err != nil {
			t.Fatalf("UpsertChannelSurface(address handoff) error = %v", err)
		}
		if handoff.ID != targetTestSurfaceAddressHandoffID || handoff.SurfaceType != "analysis_task_surface" {
			t.Fatalf("handoff surface = %#v", handoff)
		}
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM channel_surfaces WHERE channel_account_id=$1 AND channel=$2 AND address_fingerprint=$3 AND lifecycle_status='active'`, 1, seed.ChannelAccount.ID, "telegram", "telegram:chat-1:404")
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM channel_surfaces WHERE id=$1 AND lifecycle_status='superseded'`, 1, targetTestSurfaceAddressOwnerID)
	})

	t.Run("channel surfaces enforce active uniqueness, primary subject uniqueness, version conflicts, and event history", func(t *testing.T) {
		surface, err := store.UpsertChannelSurface(ctx, ChannelSurfaceRecord{
			ID:                 targetTestSurfaceID,
			ChannelAccountID:   targetTestTelegramChannelID,
			Channel:            "telegram",
			SurfaceType:        "analysis_task_surface",
			SurfaceKey:         "run:" + targetTestRunID,
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
			SurfaceID:   targetTestSurfaceID,
			SubjectType: "analysis_run",
			SubjectID:   targetTestRunID,
			SubjectRole: "primary",
			CreatedAt:   now,
		}})
		if err != nil {
			t.Fatalf("UpsertChannelSurface(first) error = %v", err)
		}
		rebound, err := store.UpsertChannelSurface(ctx, ChannelSurfaceRecord{
			ID:                 targetTestSurfaceReplayID,
			ChannelAccountID:   targetTestTelegramChannelID,
			Channel:            "telegram",
			SurfaceType:        "analysis_task_surface",
			SurfaceKey:         "run:" + targetTestRunID,
			AddressJSON:        []byte(`{"chat_id":"chat-1","message_id":42}`),
			AddressFingerprint: "telegram:chat-1:42",
			DisplayStateJSON:   []byte(`{"status":"running"}`),
			LifecycleStatus:    "active",
			Version:            1,
			IdempotencyKey:     "surface:key:replay",
			CreatedAt:          now.Add(time.Second),
			UpdatedAt:          now.Add(time.Second),
			LastRenderedAt:     &now,
		}, []ChannelSurfaceSubjectRecord{{
			SurfaceID:   targetTestSurfaceReplayID,
			SubjectType: "artifact",
			SubjectID:   targetTestArtifactID,
			SubjectRole: "primary",
			CreatedAt:   now.Add(time.Second),
		}})
		if err != nil {
			t.Fatalf("UpsertChannelSurface(rebind) error = %v", err)
		}
		if rebound.ID != surface.ID || rebound.Version != 2 {
			t.Fatalf("rebound surface = %#v, original = %#v", rebound, surface)
		}
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM channel_surfaces WHERE channel_account_id=$1 AND channel=$2 AND surface_type=$3 AND surface_key=$4`, 1, targetTestTelegramChannelID, "telegram", "analysis_task_surface", "run:"+targetTestRunID)
		subjects, err := store.ListChannelSurfaceSubjects(ctx, surface.ID)
		if err != nil {
			t.Fatalf("ListChannelSurfaceSubjects() error = %v", err)
		}
		if len(subjects) != 1 || subjects[0].SubjectType != "artifact" {
			t.Fatalf("rebound subjects = %#v", subjects)
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO channel_surface_subjects (surface_id, subject_type, subject_id, subject_role, created_at)
VALUES ($1, 'diagnostic', $2, 'primary', $3)`, surface.ID, targetTestDiagnosticID, now); err == nil {
			t.Fatalf("second primary channel surface subject insert succeeded, want uniqueness failure")
		}
		replaced, err := store.ReplaceChannelSurfaceDisplayState(ctx, ReplaceChannelSurfaceDisplayStateParams{
			SurfaceID:        surface.ID,
			ExpectedVersion:  2,
			DisplayStateJSON: []byte(`{"status":"delivered"}`),
			UpdatedAt:        now.Add(2 * time.Second),
			Event: ChannelSurfaceEventRecord{
				ID:              targetTestSurfaceDisplayEventID,
				SurfaceID:       surface.ID,
				EventType:       "channel_surface.display_state_replaced",
				Reason:          "display_state_replaced",
				PreviousVersion: 2,
				ActorType:       "telegram_adapter",
				ActorID:         "bot",
				MetadataJSON:    []byte(`{}`),
				CreatedAt:       now.Add(2 * time.Second),
			},
		})
		if err != nil {
			t.Fatalf("ReplaceChannelSurfaceDisplayState() error = %v", err)
		}
		if replaced.Version != 3 {
			t.Fatalf("replaced surface version = %d, want 3", replaced.Version)
		}
		if _, err := store.ReplaceChannelSurfaceDisplayState(ctx, ReplaceChannelSurfaceDisplayStateParams{
			SurfaceID:        surface.ID,
			ExpectedVersion:  2,
			DisplayStateJSON: []byte(`{"status":"stale"}`),
			UpdatedAt:        now.Add(3 * time.Second),
			Event: ChannelSurfaceEventRecord{
				ID:        targetTestSurfaceStaleEventID,
				SurfaceID: surface.ID,
				EventType: "channel_surface.display_state_replaced",
				ActorType: "telegram_adapter",
				CreatedAt: now.Add(3 * time.Second),
			},
		}); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("ReplaceChannelSurfaceDisplayState(stale) error = %v, want sql.ErrNoRows", err)
		}
		must(t, store.SupersedeChannelSurface(ctx, SupersedeChannelSurfaceParams{
			SurfaceID:    surface.ID,
			SupersededAt: now.Add(4 * time.Second),
			Event: ChannelSurfaceEventRecord{
				ID:              targetTestSurfaceSupersedeEventID,
				SurfaceID:       surface.ID,
				EventType:       "channel_surface.superseded",
				Reason:          "message_not_editable",
				PreviousVersion: 3,
				NextVersion:     4,
				ActorType:       "telegram_adapter",
				ActorID:         "bot",
				MetadataJSON:    []byte(`{}`),
				CreatedAt:       now.Add(4 * time.Second),
			},
		}), "supersede channel surface")
		active, err := store.ListChannelSurfaces(ctx, ChannelSurfaceQuery{ChannelAccountID: targetTestTelegramChannelID, ActiveOnly: true}, 10)
		if err != nil {
			t.Fatalf("ListChannelSurfaces(active) error = %v", err)
		}
		if len(active) != 0 {
			t.Fatalf("superseded surface returned as active: %#v", active)
		}
		events, err := store.ListChannelSurfaceEvents(ctx, surface.ID, 10)
		if err != nil {
			t.Fatalf("ListChannelSurfaceEvents() error = %v", err)
		}
		if len(events) != 2 || events[0].EventType != "channel_surface.display_state_replaced" || events[1].EventType != "channel_surface.superseded" {
			t.Fatalf("surface events = %#v", events)
		}
	})
}

type targetMediaFixture struct {
	channelID    string
	inboxID      string
	storedID     string
	assetID      string
	itemID       string
	bucket       string
	objectKey    string
	kind         string
	displayName  string
	originType   string
	originRef    string
	checksum     string
	createdAt    time.Time
	metadataJSON []byte
}

func createTargetTestMediaAsset(t *testing.T, ctx context.Context, store *Store, fixture targetMediaFixture) {
	t.Helper()
	err := store.CreateMediaAssetWithInbox(ctx, CreateMediaAssetWithInboxParams{
		StoredObject: StoredObjectRecord{
			ID:             fixture.storedID,
			Bucket:         fixture.bucket,
			ObjectKey:      fixture.objectKey,
			ContentType:    "text/plain",
			SizeBytes:      42,
			Checksum:       fixture.checksum,
			StorageStatus:  "available",
			RetentionState: "active",
			CreatedAt:      fixture.createdAt,
		},
		MediaAsset: MediaAssetRecord{
			ID:               fixture.assetID,
			ChannelAccountID: fixture.channelID,
			StoredObjectID:   fixture.storedID,
			OriginType:       fixture.originType,
			OriginRef:        fixture.originRef,
			Kind:             fixture.kind,
			DisplayName:      fixture.displayName,
			Status:           "available",
			MetadataJSON:     fixture.metadataJSON,
			CreatedAt:        fixture.createdAt,
			UpdatedAt:        fixture.createdAt,
		},
		InboxCollection: CollectionRecord{
			ID:               fixture.inboxID,
			ChannelAccountID: fixture.channelID,
			Kind:             "inbox",
			Name:             "Inbox",
			Status:           "active",
			Version:          1,
			CreatedAt:        fixture.createdAt,
			UpdatedAt:        fixture.createdAt,
		},
		CollectionItem: CollectionItemRecord{
			ID:              fixture.itemID,
			CollectionID:    fixture.inboxID,
			MediaAssetID:    fixture.assetID,
			Position:        0,
			AddedViaChannel: fixture.channelID,
			AddedAt:         fixture.createdAt,
		},
	})
	if err != nil {
		t.Fatalf("CreateMediaAssetWithInbox(%s) error = %v", fixture.assetID, err)
	}
}

func openTargetPostgresTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv("TARGET_TEST_DATABASE_URL")); dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("sql.Open(TARGET_TEST_DATABASE_URL) error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	if err := exec.CommandContext(ctx, "docker", "version").Run(); err != nil {
		t.Skipf("docker is required for target Postgres contract tests: %v", err)
	}
	container := fmt.Sprintf("map-target-store-test-%d", time.Now().UnixNano())
	run := exec.CommandContext(ctx, "docker", "run", "--rm", "-d", "--name", container, "-e", "POSTGRES_PASSWORD=postgres", "-p", "127.0.0.1::5432", "postgres:16-alpine")
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("docker run postgres: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", container).Run()
	})
	inspect := exec.CommandContext(ctx, "docker", "inspect", "-f", `{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}`, container)
	output, err := inspect.Output()
	if err != nil {
		t.Fatalf("docker inspect postgres port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%s/postgres?sslmode=disable", strings.TrimSpace(string(output)))
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open(postgres container) error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	deadline := time.Now().Add(30 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db
		}
		if time.Now().After(deadline) {
			t.Fatalf("postgres container did not become ready: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func applyTargetMigration(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "migrations", "0001_final_inbox_analysis_run_schema.sql"))
	if err != nil {
		t.Fatalf("read target migration: %v", err)
	}
	up := strings.Split(string(body), "-- +goose Down")[0]
	if _, err := db.ExecContext(ctx, "SET client_min_messages TO warning;\n"+up); err != nil {
		t.Fatalf("apply target migration: %v", err)
	}
}

func assertTargetSchemaState(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	required := []string{
		"channel_accounts",
		"operation_requests",
		"stored_objects",
		"media_assets",
		"collections",
		"collection_items",
		"selection_snapshots",
		"selection_snapshot_items",
		"analysis_runs",
		"analysis_run_steps",
		"analysis_run_step_inputs",
		"analysis_run_events",
		"artifacts",
		"artifact_subjects",
		"diagnostics",
		"channel_surfaces",
		"channel_surface_subjects",
		"channel_surface_events",
	}
	for _, table := range required {
		assertSQLBool(t, ctx, db, fmt.Sprintf("SELECT to_regclass('public.%s') IS NOT NULL", table), true)
	}
	for _, table := range []string{"sources", "media_items", "selections", "selection_items", "analysis_run_tasks", "owners", "workspaces"} {
		assertSQLBool(t, ctx, db, fmt.Sprintf("SELECT to_regclass('public.%s') IS NOT NULL", table), false)
	}
	assertSQLCount(t, ctx, db, "SELECT count(*) FROM pg_trigger WHERE tgname IN ('selection_snapshots_immutable_update_trg', 'selection_snapshots_immutable_delete_trg')", 2)
}

func assertSQLBool(t *testing.T, ctx context.Context, db *sql.DB, query string, want bool, args ...any) {
	t.Helper()
	var got bool
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query %q error = %v", query, err)
	}
	if got != want {
		t.Fatalf("query %q = %v, want %v", query, got, want)
	}
}

func assertSQLCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("query %q error = %v", query, err)
	}
	if got != want {
		t.Fatalf("query %q = %d, want %d", query, got, want)
	}
}

func must(t *testing.T, err error, label string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

const (
	targetTestTelegramChannelID        = "00000000-0000-4000-8000-000000000002"
	targetTestTelegramInboxID          = "00000000-0000-4000-8000-000000000102"
	targetTestOperationID              = "00000000-0000-4000-8000-000000000151"
	targetTestReplayOperationID        = "00000000-0000-4000-8000-000000000152"
	targetTestLocalStoredObjectID      = "00000000-0000-4000-8000-000000000201"
	targetTestTelegramStoredObjectID   = "00000000-0000-4000-8000-000000000202"
	targetTestArtifactStoredObjectID   = "00000000-0000-4000-8000-000000000203"
	targetTestLocalAssetID             = "00000000-0000-4000-8000-000000000301"
	targetTestTelegramAssetID          = "00000000-0000-4000-8000-000000000302"
	targetTestLocalCollectionItemID    = "00000000-0000-4000-8000-000000000401"
	targetTestTelegramCollectionItemID = "00000000-0000-4000-8000-000000000402"
	targetTestCollectionID             = "00000000-0000-4000-8000-000000000501"
	targetTestCollectionItemID         = "00000000-0000-4000-8000-000000000502"
	targetTestCollectionItemTwoID      = "00000000-0000-4000-8000-000000000503"
	targetTestCollectionItemThreeID    = "00000000-0000-4000-8000-000000000504"
	targetTestSnapshotID               = "00000000-0000-4000-8000-000000000601"
	targetTestSnapshotItemID           = "00000000-0000-4000-8000-000000000602"
	targetTestRunID                    = "00000000-0000-4000-8000-000000000701"
	targetTestStepID                   = "00000000-0000-4000-8000-000000000702"
	targetTestStepInputID              = "00000000-0000-4000-8000-000000000703"
	targetTestRunEventID               = "00000000-0000-4000-8000-000000000704"
	targetTestProgressEventID          = "00000000-0000-4000-8000-000000000705"
	targetTestCancelEventID            = "00000000-0000-4000-8000-000000000706"
	targetTestFinalizeEventID          = "00000000-0000-4000-8000-000000000707"
	targetTestArtifactID               = "00000000-0000-4000-8000-000000000801"
	targetTestArtifactSubjectID        = "00000000-0000-4000-8000-000000000802"
	targetTestDiagnosticID             = "00000000-0000-4000-8000-000000000901"
	targetTestSurfaceID                = "00000000-0000-4000-8000-000000001001"
	targetTestSurfaceReplayID          = "00000000-0000-4000-8000-000000001002"
	targetTestSurfaceDisplayEventID    = "00000000-0000-4000-8000-000000001003"
	targetTestSurfaceStaleEventID      = "00000000-0000-4000-8000-000000001004"
	targetTestSurfaceSupersedeEventID  = "00000000-0000-4000-8000-000000001005"
	targetTestSurfaceAddressOwnerID    = "00000000-0000-4000-8000-000000001006"
	targetTestSurfaceAddressHandoffID  = "00000000-0000-4000-8000-000000001007"
)
