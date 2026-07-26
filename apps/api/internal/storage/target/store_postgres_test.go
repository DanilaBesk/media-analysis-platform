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
	applyGovernedMediaMigration(t, ctx, db)
	applyMetadataEnrichmentMigration(t, ctx, db)
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

	t.Run("identical export outputs reuse canonical content identity", func(t *testing.T) {
		firstExpiresAt := now.Add(time.Hour)
		secondExpiresAt := now.Add(2 * time.Hour)
		first := StoredObjectRecord{
			ID: "00000000-0000-4000-8000-000000000270", ChannelAccountID: seed.ChannelAccount.ID,
			Bucket: "artifacts", ObjectKey: "transient/exports/first/result.m4a", Generation: 1,
			GenerationPublishedAt: now, ContentType: "audio/mp4", SizeBytes: 128,
			ChecksumAlgorithm: "sha256", Checksum: "sha256:identical-export-output",
			StorageStatus: "available", RetentionState: "expires_scheduled", HoldState: "none",
			CreatedAt: now, ExpiresAt: &firstExpiresAt,
		}
		second := first
		second.ID = "00000000-0000-4000-8000-000000000271"
		second.ObjectKey = "transient/exports/second/result.m4a"
		second.CreatedAt = now.Add(time.Minute)
		second.ExpiresAt = &secondExpiresAt

		var registeredFirst, registeredSecond StoredObjectRecord
		if err := store.withTx(ctx, func(tx *sql.Tx) error {
			var err error
			registeredFirst, err = registerExportOutput(ctx, tx, first)
			return err
		}); err != nil {
			t.Fatalf("register first export output: %v", err)
		}
		if err := store.withTx(ctx, func(tx *sql.Tx) error {
			var err error
			registeredSecond, err = registerExportOutput(ctx, tx, second)
			return err
		}); err != nil {
			t.Fatalf("register duplicate export output: %v", err)
		}
		if registeredFirst.ID != first.ID || registeredSecond.ID != first.ID || registeredSecond.ObjectKey != first.ObjectKey {
			t.Fatalf("registered outputs = %#v / %#v", registeredFirst, registeredSecond)
		}
		if registeredSecond.ExpiresAt == nil || !registeredSecond.ExpiresAt.Equal(*second.ExpiresAt) {
			t.Fatalf("duplicate output expiry = %v, want %v", registeredSecond.ExpiresAt, second.ExpiresAt)
		}
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE channel_account_id=$1 AND checksum=$2`, 1, seed.ChannelAccount.ID, first.Checksum)
	})

	t.Run("concurrent identical export outputs converge on the canonical row", func(t *testing.T) {
		firstExpiresAt := now.Add(3 * time.Hour)
		secondExpiresAt := now.Add(4 * time.Hour)
		first := StoredObjectRecord{
			ID: "00000000-0000-4000-8000-000000000272", ChannelAccountID: seed.ChannelAccount.ID,
			Bucket: "artifacts", ObjectKey: "transient/exports/concurrent-first/result.m4a", Generation: 1,
			GenerationPublishedAt: now, ContentType: "audio/mp4", SizeBytes: 256,
			ChecksumAlgorithm: "sha256", Checksum: "sha256:concurrent-identical-export-output",
			StorageStatus: "available", RetentionState: "expires_scheduled", HoldState: "none",
			CreatedAt: now, ExpiresAt: &firstExpiresAt,
		}
		second := first
		second.ID = "00000000-0000-4000-8000-000000000273"
		second.ObjectKey = "transient/exports/concurrent-second/result.m4a"
		second.CreatedAt = now.Add(time.Minute)
		second.ExpiresAt = &secondExpiresAt

		firstTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin first output transaction: %v", err)
		}
		registeredFirst, err := registerExportOutput(ctx, firstTx, first)
		if err != nil {
			_ = firstTx.Rollback()
			t.Fatalf("register first concurrent output: %v", err)
		}

		type concurrentResult struct {
			output StoredObjectRecord
			err    error
		}
		result := make(chan concurrentResult, 1)
		go func() {
			secondTx, beginErr := db.BeginTx(ctx, nil)
			if beginErr != nil {
				result <- concurrentResult{err: beginErr}
				return
			}
			registered, registerErr := registerExportOutput(ctx, secondTx, second)
			if registerErr == nil {
				registerErr = secondTx.Commit()
			} else {
				_ = secondTx.Rollback()
			}
			result <- concurrentResult{output: registered, err: registerErr}
		}()

		if err := firstTx.Commit(); err != nil {
			t.Fatalf("commit first output transaction: %v", err)
		}
		concurrent := <-result
		if concurrent.err != nil {
			t.Fatalf("register concurrent duplicate output: %v", concurrent.err)
		}
		if registeredFirst.ID != first.ID || concurrent.output.ID != first.ID {
			t.Fatalf("concurrent canonical outputs = %#v / %#v", registeredFirst, concurrent.output)
		}
		if concurrent.output.ExpiresAt == nil || !concurrent.output.ExpiresAt.Equal(secondExpiresAt) {
			t.Fatalf("concurrent canonical expiry = %v, want %v", concurrent.output.ExpiresAt, secondExpiresAt)
		}
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE channel_account_id=$1 AND checksum=$2`, 1, seed.ChannelAccount.ID, first.Checksum)
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

	t.Run("metadata enrichment is atomic fenced and snapshot immutable", func(t *testing.T) {
		assetID := "00000000-0000-4000-8000-000000000260"
		itemID := "00000000-0000-4000-8000-000000000261"
		enrichmentID := "00000000-0000-4000-8000-000000000262"
		snapshotID := "00000000-0000-4000-8000-000000000263"
		snapshotItemID := "00000000-0000-4000-8000-000000000264"
		canonicalURL := "https://www.youtube.com/watch?v=abc123DEF_-"
		if err := store.CreateMediaAssetWithInbox(ctx, CreateMediaAssetWithInboxParams{
			MediaAsset: MediaAssetRecord{
				ID: assetID, ChannelAccountID: targetTestTelegramChannelID, OriginType: "url",
				OriginRef: canonicalURL, Kind: "video", DisplayName: "YouTube: abc123DEF_-",
				Status: "available", MetadataJSON: []byte(`{"source":"telegram"}`), CreatedAt: now, UpdatedAt: now,
			},
			InboxCollection: CollectionRecord{
				ID: targetTestTelegramInboxID, ChannelAccountID: targetTestTelegramChannelID,
				Kind: "inbox", Name: "Inbox", Status: "active", Version: 1, CreatedAt: now, UpdatedAt: now,
			},
			CollectionItem: CollectionItemRecord{
				ID: itemID, CollectionID: targetTestTelegramInboxID, MediaAssetID: assetID,
				AddedViaChannel: targetTestTelegramChannelID, AddedAt: now,
			},
			Enrichment: MetadataEnrichmentRecord{
				ID: enrichmentID, MediaAssetID: assetID, ChannelAccountID: targetTestTelegramChannelID,
				Provider: "youtube", CanonicalURL: canonicalURL, Status: "queued", Version: 1,
				IdempotencyKey: "initial:" + assetID, MaxAttempts: 3,
				ProgressJSON: []byte(`{"stage":"queued"}`), CreatedAt: now,
			},
		}); err != nil {
			t.Fatalf("CreateMediaAssetWithInbox(metadata enrichment) error = %v", err)
		}
		queue, err := store.ListMetadataEnrichmentQueue(ctx, now, 20)
		if err != nil || len(queue) != 1 || queue[0].ID != enrichmentID {
			t.Fatalf("ListMetadataEnrichmentQueue()=%#v err=%v", queue, err)
		}
		claimAt := now.Add(time.Minute)
		claimed, ok, err := store.ClaimMetadataEnrichment(ctx, ClaimMetadataEnrichmentParams{
			EnrichmentID: enrichmentID, LeaseOwner: "metadata-worker-1", AttemptToken: "attempt-token-current",
			ClaimedAt: claimAt, LeaseExpiresAt: claimAt.Add(2 * time.Minute),
		})
		if err != nil || !ok || claimed.AttemptNo != 1 {
			t.Fatalf("ClaimMetadataEnrichment()=%#v ok=%v err=%v", claimed, ok, err)
		}
		heartbeatAt := claimAt.Add(30 * time.Second)
		if err := store.RecordMetadataEnrichmentProgress(ctx, RecordMetadataEnrichmentProgressParams{
			EnrichmentID: enrichmentID, LeaseOwner: "metadata-worker-1", AttemptToken: "attempt-token-current",
			ProgressJSON: []byte(`{"stage":"fetching","percent":50}`), HeartbeatAt: heartbeatAt,
		}); err != nil {
			t.Fatalf("RecordMetadataEnrichmentProgress() error = %v", err)
		}
		afterHeartbeat, err := store.GetMetadataEnrichmentByID(ctx, enrichmentID)
		if err != nil || afterHeartbeat.LeaseExpiresAt == nil || !afterHeartbeat.LeaseExpiresAt.Equal(heartbeatAt.Add(2*time.Minute)) {
			t.Fatalf("heartbeat lease=%#v err=%v", afterHeartbeat.LeaseExpiresAt, err)
		}
		if err := store.RecordMetadataEnrichmentProgress(ctx, RecordMetadataEnrichmentProgressParams{
			EnrichmentID: enrichmentID, LeaseOwner: "metadata-worker-1", AttemptToken: "stale-attempt-token",
			ProgressJSON: []byte(`{"stage":"stale"}`), HeartbeatAt: heartbeatAt,
		}); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("stale progress error=%v, want sql.ErrNoRows", err)
		}
		if err := store.CreateSelectionSnapshot(ctx, SelectionSnapshotRecord{
			ID: snapshotID, ChannelAccountID: targetTestTelegramChannelID, Status: "sealed",
			OptionSnapshotJSON: []byte(`{}`), DiagnosticsJSON: []byte(`[]`), CreatedAt: now, SealedAt: now,
		}, []SelectionSnapshotItemRecord{{
			ID: snapshotItemID, SelectionSnapshotID: snapshotID, MediaAssetID: assetID,
			Kind: "video", DisplayName: "YouTube: abc123DEF_-", OriginSnapshotJSON: []byte(`{"origin_type":"url"}`),
			StorageSnapshotJSON: []byte(`{}`), MetadataJSON: []byte(`{"source":"telegram"}`),
			StatusAtSelection: "available",
		}}); err != nil {
			t.Fatalf("CreateSelectionSnapshot(metadata fixture) error = %v", err)
		}
		finalized, err := store.FinalizeMetadataEnrichment(ctx, FinalizeMetadataEnrichmentParams{
			EnrichmentID: enrichmentID, LeaseOwner: "metadata-worker-1", AttemptToken: "attempt-token-current",
			Status: "succeeded", DisplayName: "Bounded title",
			ProviderMetadataJSON: []byte(`{"provider":"youtube","title":"Bounded title","thumbnail_url":"https://i.ytimg.com/demo.jpg","duration_seconds":42}`),
			CompletedAt:          heartbeatAt.Add(time.Minute),
		})
		if err != nil || finalized.Status != "succeeded" {
			t.Fatalf("FinalizeMetadataEnrichment()=%#v err=%v", finalized, err)
		}
		asset, err := store.GetMediaAsset(ctx, targetTestTelegramChannelID, assetID)
		if err != nil || asset.DisplayName != "Bounded title" || !strings.Contains(string(asset.MetadataJSON), `"provider_metadata"`) {
			t.Fatalf("enriched media asset=%#v err=%v", asset, err)
		}
		_, snapshotItems, err := store.GetSelectionSnapshot(ctx, targetTestTelegramChannelID, snapshotID)
		if err != nil || len(snapshotItems) != 1 || snapshotItems[0].DisplayName != "YouTube: abc123DEF_-" || strings.Contains(string(snapshotItems[0].MetadataJSON), "provider_metadata") {
			t.Fatalf("sealed snapshot changed after enrichment: %#v err=%v", snapshotItems, err)
		}
		if _, err := store.FinalizeMetadataEnrichment(ctx, FinalizeMetadataEnrichmentParams{
			EnrichmentID: enrichmentID, LeaseOwner: "metadata-worker-1", AttemptToken: "attempt-token-current",
			Status: "succeeded", DisplayName: "stale overwrite", ProviderMetadataJSON: []byte(`{}`),
			CompletedAt: heartbeatAt.Add(time.Minute),
		}); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("stale finalize error=%v, want sql.ErrNoRows", err)
		}

		retryID := "00000000-0000-4000-8000-000000000265"
		if _, err := store.CreateMetadataEnrichment(ctx, MetadataEnrichmentRecord{
			ID: retryID, MediaAssetID: assetID, ChannelAccountID: targetTestTelegramChannelID,
			Provider: "youtube", CanonicalURL: canonicalURL, Status: "queued", Version: 1,
			IdempotencyKey: "refresh:reclaim", MaxAttempts: 3, ProgressJSON: []byte(`{}`), CreatedAt: now,
		}); err != nil {
			t.Fatalf("CreateMetadataEnrichment(reclaim) error = %v", err)
		}
		expiresAt := now.Add(2 * time.Minute)
		if _, ok, err := store.ClaimMetadataEnrichment(ctx, ClaimMetadataEnrichmentParams{
			EnrichmentID: retryID, LeaseOwner: "metadata-worker-2", AttemptToken: "attempt-token-expired",
			ClaimedAt: now.Add(time.Minute), LeaseExpiresAt: expiresAt,
		}); err != nil || !ok {
			t.Fatalf("claim reclaim fixture ok=%v err=%v", ok, err)
		}
		reclaimed, err := store.ReclaimMetadataEnrichments(ctx, expiresAt.Add(time.Second), 10)
		if err != nil || reclaimed.Examined != 1 || reclaimed.Requeued != 1 || reclaimed.Failed != 0 {
			t.Fatalf("ReclaimMetadataEnrichments()=%#v err=%v", reclaimed, err)
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

	t.Run("processing run idempotency replays the sealed run and rejects mismatched payload", func(t *testing.T) {
		const collectionID = "00000000-0000-4000-8000-000000001100"
		const assetID = "00000000-0000-4000-8000-000000001105"
		if _, err := db.ExecContext(ctx, `INSERT INTO media_assets (
id, channel_account_id, origin_type, origin_ref, kind, display_name, status, metadata, created_at, updated_at
) VALUES ($1,$2,'telegram_file','processing-replay-file','voice','voice.ogg','available','{}',$3,$3)`, assetID, targetTestTelegramChannelID, now); err != nil {
			t.Fatalf("seed processing replay asset: %v", err)
		}
		must(t, store.CreateCollection(ctx, CollectionRecord{
			ID: collectionID, ChannelAccountID: targetTestTelegramChannelID, Kind: "user",
			Name: "Processing replay", Status: "active", Version: 1, CreatedAt: now, UpdatedAt: now,
		}, []CollectionItemRecord{{
			ID: "00000000-0000-4000-8000-000000001101", CollectionID: collectionID,
			MediaAssetID: assetID, Position: 0, AddedViaChannel: targetTestTelegramChannelID, AddedAt: now,
		}}), "create processing replay collection")
		first := processingRunReplayParams(now, collectionID, assetID, "00000000-0000-4000-8000-000000001102", "00000000-0000-4000-8000-000000001103", "00000000-0000-4000-8000-000000001104", "transcription")
		created, err := store.CreateProcessingRun(ctx, first)
		if err != nil {
			t.Fatalf("CreateProcessingRun(first) error = %v", err)
		}
		if created.Replayed || created.Run.ID != first.Graph.Run.ID || created.Snapshot.ID != first.Snapshot.ID || created.CollectionVersion != 2 {
			t.Fatalf("created processing run = %#v", created)
		}

		replay := processingRunReplayParams(now.Add(time.Second), collectionID, assetID, "00000000-0000-4000-8000-000000001112", "00000000-0000-4000-8000-000000001113", "00000000-0000-4000-8000-000000001114", "transcription")
		replayed, err := store.CreateProcessingRun(ctx, replay)
		if err != nil {
			t.Fatalf("CreateProcessingRun(replay) error = %v", err)
		}
		if !replayed.Replayed || replayed.Run.ID != first.Graph.Run.ID || replayed.Snapshot.ID != first.Snapshot.ID || replayed.CollectionVersion != 2 {
			t.Fatalf("replayed processing run = %#v", replayed)
		}
		assertSQLCount(t, ctx, db, `SELECT count(*) FROM analysis_runs WHERE channel_account_id=$1 AND idempotency_key=$2`, 1, targetTestTelegramChannelID, "telegram:process:replay")

		mismatch := processingRunReplayParams(now.Add(2*time.Second), collectionID, assetID, "00000000-0000-4000-8000-000000001122", "00000000-0000-4000-8000-000000001123", "00000000-0000-4000-8000-000000001124", "report")
		if _, err := store.CreateProcessingRun(ctx, mismatch); !errors.Is(err, ErrProcessingRunIdempotencyConflict) {
			t.Fatalf("CreateProcessingRun(mismatch) error = %v, want %v", err, ErrProcessingRunIdempotencyConflict)
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

	t.Run("reusable transcript lookup matches single stored source", func(t *testing.T) {
		completedAt := now.Add(10 * time.Second)
		must(t, store.CreateSelectionSnapshot(ctx, SelectionSnapshotRecord{
			ID:                 targetTestReusableSnapshotID,
			ChannelAccountID:   targetTestTelegramChannelID,
			SourceCollectionID: targetTestTelegramInboxID,
			Status:             "sealed",
			OptionSnapshotJSON: []byte(`{"language":"ru"}`),
			DiagnosticsJSON:    []byte(`[]`),
			CreatedViaChannel:  targetTestTelegramChannelID,
			CreatedAt:          now.Add(6 * time.Second),
			SealedAt:           now.Add(6 * time.Second),
		}, []SelectionSnapshotItemRecord{{
			ID:                  targetTestReusableSnapshotItemID,
			SelectionSnapshotID: targetTestReusableSnapshotID,
			Position:            0,
			MediaAssetID:        targetTestTelegramAssetID,
			Kind:                "voice",
			DisplayName:         "voice.ogg",
			OriginSnapshotJSON:  []byte(`{"origin_type":"telegram_file"}`),
			StorageSnapshotJSON: []byte(`{"stored_object_id":"` + targetTestTelegramStoredObjectID + `","checksum":"sha256:telegram"}`),
			MetadataJSON:        []byte(`{"fixture":"reusable"}`),
			StatusAtSelection:   "available",
			DiagnosticsJSON:     []byte(`[]`),
		}}), "create reusable transcript selection")
		must(t, store.CreateAnalysisRunGraph(ctx, AnalysisRunGraph{
			Run: AnalysisRunRecord{
				ID:                targetTestReusableRunID,
				ChannelAccountID:  targetTestTelegramChannelID,
				SelectionSnapshot: targetTestReusableSnapshotID,
				RunType:           "transcription",
				Status:            "succeeded",
				Version:           2,
				ParamsJSON:        []byte(`{}`),
				DeliveryJSON:      []byte(`{"strategy":"polling"}`),
				EvidenceGateState: "not_required",
				CreatedViaChannel: targetTestTelegramChannelID,
				CreatedAt:         now.Add(7 * time.Second),
				StartedAt:         &completedAt,
				CompletedAt:       &completedAt,
			},
			Event: AnalysisRunEventRecord{
				ID:            targetTestReusableRunEventID,
				AnalysisRunID: targetTestReusableRunID,
				EventType:     "analysis_run.created",
				Version:       1,
				Status:        "succeeded",
				PayloadJSON:   []byte(`{}`),
				CreatedAt:     now.Add(7 * time.Second),
			},
		}), "create reusable analysis run")
		must(t, store.RecordArtifacts(ctx, []StoredObjectRecord{{
			ID:             targetTestReusableArtifactStoredObjectID,
			Bucket:         "artifacts",
			ObjectKey:      "run-reusable/transcript/plain/transcript.txt",
			ContentType:    "text/plain",
			SizeBytes:      42,
			Checksum:       "sha256:transcript",
			StorageStatus:  "available",
			RetentionState: "active",
			CreatedAt:      completedAt,
		}}, []ArtifactRecord{{
			ID:               targetTestReusableArtifactID,
			ChannelAccountID: targetTestTelegramChannelID,
			AnalysisRunID:    targetTestReusableRunID,
			StoredObjectID:   targetTestReusableArtifactStoredObjectID,
			Kind:             "transcript",
			Status:           "available",
			ContentType:      "text/plain; charset=utf-8",
			Checksum:         "sha256:transcript",
			SizeBytes:        42,
			Visibility:       "channel_deliverable",
			PreviewJSON:      []byte(`{"available":true,"filename":"transcript.txt"}`),
			CreatedAt:        completedAt,
		}}, []ArtifactSubjectRecord{{
			ID:          targetTestReusableArtifactSubjectID,
			ArtifactID:  targetTestReusableArtifactID,
			SubjectType: "analysis_run",
			SubjectID:   targetTestReusableRunID,
			SubjectRole: "result",
			CreatedAt:   completedAt,
		}}), "record reusable transcript artifact")

		run, artifact, err := store.FindReusableTranscriptBySource(ctx, targetTestTelegramChannelID, targetTestTelegramStoredObjectID, "sha256:telegram")
		if err != nil {
			t.Fatalf("FindReusableTranscriptBySource() error = %v", err)
		}
		if run.ID != targetTestReusableRunID || artifact.ID != targetTestReusableArtifactID {
			t.Fatalf("reusable transcript run=%#v artifact=%#v", run, artifact)
		}
		if _, _, err := store.FindReusableTranscriptBySource(ctx, targetTestTelegramChannelID, targetTestTelegramStoredObjectID, "sha256:missing"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("FindReusableTranscriptBySource(missing checksum) error = %v, want sql.ErrNoRows", err)
		}
		if _, _, err := store.FindReusableTranscriptBySource(ctx, seed.ChannelAccount.ID, targetTestTelegramStoredObjectID, "sha256:telegram"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("FindReusableTranscriptBySource(cross channel) error = %v, want sql.ErrNoRows", err)
		}

		multiCompletedAt := completedAt.Add(time.Second)
		must(t, store.CreateSelectionSnapshot(ctx, SelectionSnapshotRecord{
			ID:                 targetTestReusableMultiSnapshotID,
			ChannelAccountID:   targetTestTelegramChannelID,
			SourceCollectionID: targetTestTelegramInboxID,
			Status:             "sealed",
			OptionSnapshotJSON: []byte(`{}`),
			DiagnosticsJSON:    []byte(`[]`),
			CreatedViaChannel:  targetTestTelegramChannelID,
			CreatedAt:          now.Add(8 * time.Second),
			SealedAt:           now.Add(8 * time.Second),
		}, []SelectionSnapshotItemRecord{{
			ID:                  targetTestReusableMultiSnapshotItemOneID,
			SelectionSnapshotID: targetTestReusableMultiSnapshotID,
			Position:            0,
			MediaAssetID:        targetTestTelegramAssetID,
			Kind:                "voice",
			DisplayName:         "ambiguous-a.ogg",
			OriginSnapshotJSON:  []byte(`{"origin_type":"telegram_file","object_ref":"ambiguous-a.ogg"}`),
			StorageSnapshotJSON: []byte(`{"stored_object_id":"` + targetTestReusableMultiStoredObjectID + `","checksum":"sha256:ambiguous"}`),
			MetadataJSON:        []byte(`{}`),
			StatusAtSelection:   "available",
			DiagnosticsJSON:     []byte(`[]`),
		}, {
			ID:                  targetTestReusableMultiSnapshotItemTwoID,
			SelectionSnapshotID: targetTestReusableMultiSnapshotID,
			Position:            1,
			MediaAssetID:        targetTestTelegramAssetID,
			Kind:                "voice",
			DisplayName:         "ambiguous-b.ogg",
			OriginSnapshotJSON:  []byte(`{"origin_type":"telegram_file","object_ref":"ambiguous-b.ogg"}`),
			StorageSnapshotJSON: []byte(`{"stored_object_id":"` + targetTestReusableMultiOtherStoredObjectID + `","checksum":"sha256:ambiguous-other"}`),
			MetadataJSON:        []byte(`{}`),
			StatusAtSelection:   "available",
			DiagnosticsJSON:     []byte(`[]`),
		}}), "create ambiguous reusable snapshot")
		must(t, store.CreateAnalysisRunGraph(ctx, AnalysisRunGraph{
			Run: AnalysisRunRecord{
				ID:                targetTestReusableMultiRunID,
				ChannelAccountID:  targetTestTelegramChannelID,
				SelectionSnapshot: targetTestReusableMultiSnapshotID,
				RunType:           "transcription",
				Status:            "succeeded",
				Version:           2,
				ParamsJSON:        []byte(`{}`),
				DeliveryJSON:      []byte(`{"strategy":"polling"}`),
				EvidenceGateState: "not_required",
				CreatedViaChannel: targetTestTelegramChannelID,
				CreatedAt:         now.Add(8 * time.Second),
				StartedAt:         &multiCompletedAt,
				CompletedAt:       &multiCompletedAt,
			},
			Event: AnalysisRunEventRecord{
				ID:            targetTestReusableMultiRunEventID,
				AnalysisRunID: targetTestReusableMultiRunID,
				EventType:     "analysis_run.created",
				Version:       1,
				Status:        "succeeded",
				PayloadJSON:   []byte(`{}`),
				CreatedAt:     now.Add(8 * time.Second),
			},
		}), "create ambiguous reusable run")
		must(t, store.RecordArtifacts(ctx, []StoredObjectRecord{{
			ID:             targetTestReusableMultiArtifactStoredObjectID,
			Bucket:         "artifacts",
			ObjectKey:      "run-reusable-multi/transcript/plain/transcript.txt",
			ContentType:    "text/plain",
			SizeBytes:      42,
			Checksum:       "sha256:ambiguous-transcript",
			StorageStatus:  "available",
			RetentionState: "active",
			CreatedAt:      multiCompletedAt,
		}}, []ArtifactRecord{{
			ID:               targetTestReusableMultiArtifactID,
			ChannelAccountID: targetTestTelegramChannelID,
			AnalysisRunID:    targetTestReusableMultiRunID,
			StoredObjectID:   targetTestReusableMultiArtifactStoredObjectID,
			Kind:             "transcript",
			Status:           "available",
			ContentType:      "text/plain; charset=utf-8",
			Checksum:         "sha256:ambiguous-transcript",
			SizeBytes:        42,
			Visibility:       "channel_deliverable",
			PreviewJSON:      []byte(`{"available":true,"filename":"ambiguous.txt"}`),
			CreatedAt:        multiCompletedAt,
		}}, []ArtifactSubjectRecord{{
			ID:          targetTestReusableMultiArtifactSubjectID,
			ArtifactID:  targetTestReusableMultiArtifactID,
			SubjectType: "analysis_run",
			SubjectID:   targetTestReusableMultiRunID,
			SubjectRole: "result",
			CreatedAt:   multiCompletedAt,
		}}), "record ambiguous transcript artifact")
		if _, _, err := store.FindReusableTranscriptBySource(ctx, targetTestTelegramChannelID, targetTestReusableMultiStoredObjectID, "sha256:ambiguous"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("FindReusableTranscriptBySource(ambiguous multi-item run) error = %v, want sql.ErrNoRows", err)
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

func TestYouTubeMetadataBackfillPostgresContracts(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyGovernedMediaMigration(t, ctx, db)
	applyMetadataEnrichmentMigration(t, ctx, db)

	const accountID = "10000000-0000-4000-8000-000000000001"
	if _, err := db.ExecContext(ctx, `INSERT INTO channel_accounts (id, channel, external_account_ref, status)
VALUES ($1, 'telegram', 'backfill-test', 'active')`, accountID); err != nil {
		t.Fatalf("seed metadata backfill account: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO media_assets (id, channel_account_id, origin_type, origin_ref, kind, display_name, status) VALUES
	('10000000-0000-4000-8000-000000000011', $1, 'url', 'https://www.youtube.com/watch?v=roEzqWv7HpI&list=RDMM', 'url', 'old watch URL', 'available'),
	('10000000-0000-4000-8000-000000000012', $1, 'url', 'https://youtu.be/goXKlOozyx8?t=2', 'url', 'old short URL', 'available'),
	('10000000-0000-4000-8000-000000000013', $1, 'url', 'https://www.youtube.com/channel/not-a-video', 'url', 'channel URL', 'available'),
	('10000000-0000-4000-8000-000000000014', $1, 'url', 'https://m.youtube.com/shorts/dQw4w9WgXcQ?feature=share', 'url', 'old Shorts URL', 'available')`, accountID); err != nil {
		t.Fatalf("seed metadata backfill fixtures: %v", err)
	}

	applyNamedTargetMigration(t, ctx, db, "0004_backfill_youtube_metadata_enrichment.sql")
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs`, 3)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs WHERE canonical_url IN (
	'https://www.youtube.com/watch?v=roEzqWv7HpI', 'https://www.youtube.com/watch?v=goXKlOozyx8',
	'https://www.youtube.com/watch?v=dQw4w9WgXcQ')`, 3)

	if _, err := db.ExecContext(ctx, `UPDATE metadata_enrichment_jobs
SET canonical_url='https://www.youtube.com/watch?v=roEzqWv7HpI&list=RDMM'
WHERE media_asset_id='10000000-0000-4000-8000-000000000011'`); err != nil {
		t.Fatalf("seed metadata repair URL: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO metadata_enrichment_jobs (
    id, media_asset_id, channel_account_id, provider, canonical_url, idempotency_key
) VALUES (
    '10000000-0000-4000-8000-000000000099',
    '10000000-0000-4000-8000-000000000013',
    $1,
    'youtube',
    'https://www.youtube.com/channel/not-a-video',
    'youtube-metadata-enrichment:backfill:invalid'
    )`, accountID); err != nil {
		t.Fatalf("seed metadata repair fixtures: %v", err)
	}

	applyNamedTargetMigration(t, ctx, db, "0005_repair_youtube_metadata_backfill_urls.sql")
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs
WHERE media_asset_id='10000000-0000-4000-8000-000000000011'
  AND canonical_url='https://www.youtube.com/watch?v=roEzqWv7HpI'`, 1)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs
	WHERE media_asset_id='10000000-0000-4000-8000-000000000013'
	  AND status='failed' AND error_code='provider_url_invalid'`, 1)

	if _, err := db.ExecContext(ctx, `UPDATE metadata_enrichment_jobs
SET canonical_url='https://m.youtube.com/shorts/dQw4w9WgXcQ?feature=share',
    status='failed', error_code='provider_url_invalid', error_message='legacy failure', completed_at=now()
WHERE media_asset_id='10000000-0000-4000-8000-000000000014'`); err != nil {
		t.Fatalf("seed historically failed Shorts backfill: %v", err)
	}
	applyNamedTargetMigration(t, ctx, db, "0006_requeue_youtube_shorts_backfill.sql")
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs
WHERE media_asset_id='10000000-0000-4000-8000-000000000014'
  AND canonical_url='https://www.youtube.com/watch?v=dQw4w9WgXcQ'
  AND status='queued' AND error_code IS NULL AND completed_at IS NULL`, 1)
}

func TestYouTubeShortsForwardRepairPostgresContracts(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyGovernedMediaMigration(t, ctx, db)
	applyMetadataEnrichmentMigration(t, ctx, db)

	const accountID = "20000000-0000-4000-8000-000000000001"
	if _, err := db.ExecContext(ctx, `INSERT INTO channel_accounts (id, channel, external_account_ref, status)
VALUES ($1, 'telegram', 'shorts-forward-repair', 'active')`, accountID); err != nil {
		t.Fatalf("seed Shorts repair account: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO media_assets (id, channel_account_id, origin_type, origin_ref, kind, display_name, status) VALUES
('20000000-0000-4000-8000-000000000011', $1, 'url', 'https://www.youtube.com/shorts/dQw4w9WgXcQ?feature=share', 'url', 'omitted by historical 0004', 'available'),
('20000000-0000-4000-8000-000000000012', $1, 'url', 'https://m.youtube.com/shorts/goXKlOozyx8', 'url', 'failed historical backfill', 'available')`, accountID); err != nil {
		t.Fatalf("seed historical Shorts assets: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO metadata_enrichment_jobs (
    id, media_asset_id, channel_account_id, provider, canonical_url, status, version,
    idempotency_key, attempt_no, max_attempts, error_code, error_message, completed_at
) VALUES (
    '20000000-0000-4000-8000-000000000021',
    '20000000-0000-4000-8000-000000000012', $1, 'youtube',
    'https://m.youtube.com/shorts/goXKlOozyx8', 'failed', 2,
    'youtube-metadata-enrichment:backfill:20000000-0000-4000-8000-000000000012',
    1, 3, 'provider_url_invalid', 'legacy failure', now()
), (
    '20000000-0000-4000-8000-000000000022',
    '20000000-0000-4000-8000-000000000012', $1, 'youtube',
    'https://www.youtube.com/watch?v=goXKlOozyx8', 'queued', 1,
    'youtube-metadata-enrichment:refresh:20000000-0000-4000-8000-000000000012',
    0, 3, NULL, NULL, NULL
)`, accountID); err != nil {
		t.Fatalf("seed historical and active Shorts jobs: %v", err)
	}

	applyNamedTargetMigration(t, ctx, db, "0006_requeue_youtube_shorts_backfill.sql")
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs
WHERE media_asset_id='20000000-0000-4000-8000-000000000011'
  AND canonical_url='https://www.youtube.com/watch?v=dQw4w9WgXcQ'
  AND status='queued'`, 1)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs
WHERE id='20000000-0000-4000-8000-000000000021'
  AND status='failed' AND error_code='provider_url_invalid'`, 1)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM metadata_enrichment_jobs
WHERE id='20000000-0000-4000-8000-000000000022' AND status='queued'`, 1)
}

func TestFinalizeExportDeliveryRejectsExpiredLeasePostgres(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyGovernedMediaMigration(t, ctx, db)
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	const accountID = "20000000-0000-4000-8000-000000000001"
	const assetID = "20000000-0000-4000-8000-000000000002"
	const jobID = "20000000-0000-4000-8000-000000000003"
	const deliveryID = "20000000-0000-4000-8000-000000000004"
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	seedStatements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO channel_accounts (id, channel, external_account_ref, status) VALUES ($1,'telegram','expired-delivery','active')`, []any{accountID}},
		{`INSERT INTO media_assets (id, channel_account_id, origin_type, origin_ref, kind, display_name, status) VALUES ($1,$2,'url','https://youtu.be/example','url','Example','available')`, []any{assetID, accountID}},
		{`INSERT INTO export_jobs (id, channel_account_id, media_asset_id, operation, delivery_channel, variant, status, version, max_attempts, progress) VALUES ($1,$2,$3,'youtube_audio','telegram','{"audio_bitrate_kbps":192}','succeeded',2,3,'{}')`, []any{jobID, accountID, assetID}},
		{`INSERT INTO export_deliveries (id, export_job_id, channel_account_id, channel, status, version, attempt_no, attempt_token, lease_owner, lease_expires_at, max_attempts, expires_at) VALUES ($1,$2,$3,'telegram','claimed',2,1,'attempt-token-current','telegram-adapter',$4,5,$5)`, []any{deliveryID, jobID, accountID, now.Add(time.Minute), now.Add(time.Hour)}},
	}
	for _, seed := range seedStatements {
		if _, err := db.ExecContext(ctx, seed.query, seed.args...); err != nil {
			t.Fatalf("seed expired delivery: %v", err)
		}
	}
	if _, err := store.FinalizeExportDelivery(ctx, FinalizeExportDeliveryParams{
		ExportJobID: jobID, ChannelAccountID: accountID, ExportDeliveryID: deliveryID,
		LeaseOwner: "telegram-adapter", AttemptToken: "attempt-token-current",
		Status: "delivered", FinalizedAt: now.Add(time.Minute + time.Second),
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("FinalizeExportDelivery(expired lease) error = %v, want sql.ErrNoRows", err)
	}
	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM export_deliveries WHERE id=$1`, deliveryID).Scan(&status); err != nil {
		t.Fatalf("read delivery status: %v", err)
	}
	if status != "claimed" {
		t.Fatalf("delivery status = %q, want claimed", status)
	}
}

func TestGovernedMediaMigrationRewritesHistoricalSnapshotAliases(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)

	accountID := "10000000-0000-4000-8000-000000000001"
	canonicalID := "10000000-0000-4000-8000-000000000002"
	aliasID := "10000000-0000-4000-8000-000000000003"
	assetID := "10000000-0000-4000-8000-000000000004"
	snapshotID := "10000000-0000-4000-8000-000000000005"
	itemID := "10000000-0000-4000-8000-000000000006"
	canonicalAssetID := "10000000-0000-4000-8000-000000000007"
	if _, err := db.ExecContext(ctx, `INSERT INTO channel_accounts (id, channel, external_account_ref, status) VALUES ($1, 'telegram', 'migration-alias-test', 'active')`, accountID); err != nil {
		t.Fatalf("seed pre-migration account: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_objects (id, bucket, object_key, content_type, size_bytes, checksum, storage_status, created_at)
VALUES
  ($1, 'sources', 'legacy/canonical', 'video/mp4', 12, 'sha256:same-body', 'available', now() - interval '2 hours'),
  ($2, 'sources', 'legacy/alias', 'video/mp4', 12, 'sha256:same-body', 'available', now() - interval '1 hour')`, canonicalID, aliasID); err != nil {
		t.Fatalf("seed pre-migration stored objects: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO media_assets (id, channel_account_id, stored_object_id, origin_type, origin_ref, kind, display_name, status)
VALUES
  ($1, $2, $3, 'upload', 'legacy/alias', 'video', 'Historical video', 'available'),
  ($4, $2, $5, 'upload', 'legacy/canonical', 'video', 'Canonical video', 'available')`,
		assetID, accountID, aliasID, canonicalAssetID, canonicalID); err != nil {
		t.Fatalf("seed pre-migration asset: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO selection_snapshots (id, channel_account_id, status) VALUES ($1, $2, 'sealed')`, snapshotID, accountID); err != nil {
		t.Fatalf("seed pre-migration snapshot: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO selection_snapshot_items (
  id, selection_snapshot_id, position, media_asset_id, kind, display_name,
  origin_snapshot, storage_snapshot, metadata_snapshot, status_at_selection, diagnostics
)
VALUES ($1, $2, 0, $3, 'video', 'Historical video', '{}', jsonb_build_object('stored_object_id', $4::text), '{}', 'available', '[]')`,
		itemID, snapshotID, assetID, aliasID); err != nil {
		t.Fatalf("seed pre-migration snapshot item: %v", err)
	}

	applyGovernedMediaMigration(t, ctx, db)

	var snapshotStoredObjectID, assetStoredObjectID string
	if err := db.QueryRowContext(ctx, `SELECT storage_snapshot->>'stored_object_id' FROM selection_snapshot_items WHERE id=$1`, itemID).Scan(&snapshotStoredObjectID); err != nil {
		t.Fatalf("read migrated snapshot: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT stored_object_id::text FROM media_assets WHERE id=$1`, assetID).Scan(&assetStoredObjectID); err != nil {
		t.Fatalf("read migrated asset: %v", err)
	}
	if snapshotStoredObjectID != canonicalID || assetStoredObjectID != canonicalID {
		t.Fatalf("migrated identities snapshot=%q asset=%q want=%q", snapshotStoredObjectID, assetStoredObjectID, canonicalID)
	}
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1`, 0, aliasID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_object_aliases WHERE alias_id=$1 AND canonical_stored_object_id=$2`, 1, aliasID, canonicalID)
}

func TestStoredObjectPublicationRefreshesAvailableRetention(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyGovernedMediaMigration(t, ctx, db)
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	now := time.Date(2026, 7, 26, 11, 0, 0, 0, time.UTC)
	channelID := "00000000-0000-4000-8000-000000006101"
	_, err = store.UpsertChannelAccount(ctx, ChannelAccountRecord{
		ID: channelID, Channel: "telegram", ExternalAccountRef: "publication-retention-refresh",
		Status: "active", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertChannelAccount() error = %v", err)
	}
	firstExpiry := now.Add(time.Hour)
	candidate := StoredObjectRecord{
		ID: "00000000-0000-4000-8000-000000006102", ChannelAccountID: channelID,
		Bucket: "sources", ObjectKey: "sources/uploads/retention/1/source", StagingKey: "staging/uploads/retention-1",
		Generation: 1, GenerationPublishedAt: now, ContentType: "video/mp4", SizeBytes: 5,
		ChecksumAlgorithm: "sha256", Checksum: "sha256:retention-refresh", StorageStatus: "publishing",
		RetentionState: "active", HoldState: "none", CreatedAt: now, ExpiresAt: &firstExpiry,
	}
	first, err := store.PrepareStoredObjectPublication(ctx, candidate)
	if err != nil || !first.Publisher {
		t.Fatalf("first publication = %#v err=%v", first, err)
	}
	if err := store.CompleteStoredObjectPublication(ctx, candidate.ID, 1, candidate.StagingKey, now.Add(time.Second)); err != nil {
		t.Fatalf("complete first publication: %v", err)
	}

	secondExpiry := now.Add(7 * 24 * time.Hour)
	candidate.ID = "00000000-0000-4000-8000-000000006103"
	candidate.ObjectKey = "sources/uploads/retention/2/source"
	candidate.StagingKey = "staging/uploads/retention-2"
	candidate.ExpiresAt = &secondExpiry
	second, err := store.PrepareStoredObjectPublication(ctx, candidate)
	if err != nil || second.Publisher {
		t.Fatalf("deduplicated publication = %#v err=%v", second, err)
	}
	if second.StoredObject.ID != first.StoredObject.ID || second.StoredObject.ExpiresAt == nil || !second.StoredObject.ExpiresAt.Equal(secondExpiry) {
		t.Fatalf("deduplicated publication retention = %#v", second.StoredObject)
	}
}

func TestExportJobReclaimReleasesTerminalSourcePin(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyGovernedMediaMigration(t, ctx, db)
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	now := time.Date(2026, 7, 26, 9, 0, 0, 0, time.UTC)
	channelID := "00000000-0000-4000-8000-000000007001"
	inboxID := "00000000-0000-4000-8000-000000007002"
	storedID := "00000000-0000-4000-8000-000000007003"
	assetID := "00000000-0000-4000-8000-000000007004"
	itemID := "00000000-0000-4000-8000-000000007005"
	jobID := "00000000-0000-4000-8000-000000007006"
	pinID := "00000000-0000-4000-8000-000000007007"
	_, err = store.UpsertChannelAccount(ctx, ChannelAccountRecord{
		ID: channelID, Channel: "telegram", ExternalAccountRef: "export-reclaim",
		Status: "active", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertChannelAccount() error = %v", err)
	}
	createTargetTestMediaAsset(t, ctx, store, targetMediaFixture{
		channelID: channelID, inboxID: inboxID, storedID: storedID, assetID: assetID,
		itemID: itemID, bucket: "sources", objectKey: "sources/reclaim/source",
		kind: "video", displayName: "reclaim.mp4", originType: "upload",
		originRef: "sources/reclaim/source", checksum: "sha256:reclaim", createdAt: now,
	})
	_, err = store.CreateExportJob(ctx, CreateExportJobParams{
		Job: ExportJobRecord{
			ID: jobID, ChannelAccountID: channelID, MediaAssetID: assetID,
			Operation: "video_to_audio", DeliveryChannel: "telegram", Status: "queued",
			Version: 1, MaxAttempts: 2, CreatedAt: now,
		},
		SourcePin: StoredObjectPinRecord{
			ID: pinID, StoredObjectID: storedID, OwnerType: "export_job",
			OwnerID: jobID, Purpose: "source", CreatedAt: now,
		},
	})
	if err != nil {
		t.Fatalf("CreateExportJob() error = %v", err)
	}

	firstLease := now.Add(time.Minute)
	if _, claimed, err := store.ClaimExportJob(ctx, ClaimExportJobParams{
		ExportJobID: jobID, LeaseOwner: "worker-a", AttemptToken: "attempt-a",
		ClaimedAt: now, LeaseExpiresAt: firstLease,
	}); err != nil || !claimed {
		t.Fatalf("first ClaimExportJob() claimed=%v err=%v", claimed, err)
	}
	first, err := store.ReclaimExportJobs(ctx, firstLease, 100)
	if err != nil {
		t.Fatalf("first ReclaimExportJobs() error = %v", err)
	}
	if first.Examined != 1 || first.Requeued != 1 || first.Failed != 0 {
		t.Fatalf("first reclaim = %#v", first)
	}
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_object_pins WHERE id=$1 AND released_at IS NULL`, 1, pinID)

	secondLease := now.Add(2 * time.Minute)
	if _, claimed, err := store.ClaimExportJob(ctx, ClaimExportJobParams{
		ExportJobID: jobID, LeaseOwner: "worker-b", AttemptToken: "attempt-b",
		ClaimedAt: firstLease, LeaseExpiresAt: secondLease,
	}); err != nil || !claimed {
		t.Fatalf("second ClaimExportJob() claimed=%v err=%v", claimed, err)
	}
	second, err := store.ReclaimExportJobs(ctx, secondLease, 100)
	if err != nil {
		t.Fatalf("second ReclaimExportJobs() error = %v", err)
	}
	if second.Examined != 1 || second.Requeued != 0 || second.Failed != 1 {
		t.Fatalf("second reclaim = %#v", second)
	}
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_object_pins WHERE id=$1 AND released_at IS NOT NULL`, 1, pinID)
}

func TestStoredObjectPublicationUsesImmutableGenerationKeysAndDeleteFence(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)
	applyGovernedMediaMigration(t, ctx, db)
	store, err := NewStore(db)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	channelID := "00000000-0000-4000-8000-000000006001"
	storedID := "00000000-0000-4000-8000-000000006002"
	_, err = store.UpsertChannelAccount(ctx, ChannelAccountRecord{
		ID: channelID, Channel: "telegram", ExternalAccountRef: "publication-generation",
		Status: "active", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertChannelAccount() error = %v", err)
	}
	candidate := StoredObjectRecord{
		ID: storedID, ChannelAccountID: channelID, Bucket: "sources",
		ObjectKey: "sources/uploads/" + storedID + "/1/source", StagingKey: "staging/uploads/attempt-1",
		Generation: 1, GenerationPublishedAt: now, ContentType: "video/mp4",
		SizeBytes: 5, ChecksumAlgorithm: "sha256", Checksum: "sha256:generation",
		StorageStatus: "publishing", RetentionState: "active", HoldState: "none", CreatedAt: now,
	}
	first, err := store.PrepareStoredObjectPublication(ctx, candidate)
	if err != nil || !first.Publisher || first.StoredObject.Generation != 1 {
		t.Fatalf("first PrepareStoredObjectPublication() = %#v err=%v", first, err)
	}
	if err := store.CompleteStoredObjectPublication(ctx, storedID, 1, candidate.StagingKey, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteStoredObjectPublication() error = %v", err)
	}
	if _, err := db.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='deleted', retention_state='expired', deleted_at=$2
WHERE id=$1`, storedID, now.Add(2*time.Second)); err != nil {
		t.Fatalf("seed deleted generation: %v", err)
	}

	candidate.StagingKey = "staging/uploads/attempt-2"
	candidate.GenerationPublishedAt = now.Add(3 * time.Second)
	second, err := store.PrepareStoredObjectPublication(ctx, candidate)
	if err != nil || !second.Publisher {
		t.Fatalf("second PrepareStoredObjectPublication() = %#v err=%v", second, err)
	}
	if second.StoredObject.Generation != 2 || second.StoredObject.ObjectKey != "sources/uploads/"+storedID+"/2/source" {
		t.Fatalf("second generation object = %#v", second.StoredObject)
	}
	leaseExpiresAt := now.Add(10 * time.Minute)
	if _, err := db.ExecContext(ctx, `
UPDATE stored_objects
SET storage_status='delete_scheduled', retention_state='expires_scheduled',
    delete_owner='sweeper', delete_token='delete-token-123456', delete_lease_expires_at=$2
WHERE id=$1`, storedID, leaseExpiresAt); err != nil {
		t.Fatalf("seed delete claim: %v", err)
	}
	if err := store.CompleteRetentionDelete(ctx, storedID, 1, "sweeper", "delete-token-123456", now.Add(4*time.Second)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale generation delete error = %v, want sql.ErrNoRows", err)
	}
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1 AND generation=2 AND storage_status='delete_scheduled'`, 1, storedID)
	if err := store.CompleteRetentionDelete(ctx, storedID, 2, "sweeper", "delete-token-123456", now.Add(5*time.Second)); err != nil {
		t.Fatalf("current generation delete error = %v", err)
	}
}

func TestGovernedMediaMigrationUpgradesLegacyDuplicates(t *testing.T) {
	ctx := context.Background()
	db := openTargetPostgresTestDB(t, ctx)
	applyTargetMigration(t, ctx, db)

	now := time.Date(2026, 7, 26, 8, 0, 0, 0, time.UTC)
	channelID := "00000000-0000-4000-8000-000000008001"
	canonicalID := "00000000-0000-4000-8000-000000008002"
	duplicateID := "00000000-0000-4000-8000-000000008003"
	assetID := "00000000-0000-4000-8000-000000008004"
	unscopedID := "00000000-0000-4000-8000-000000008005"
	canonicalAssetID := "00000000-0000-4000-8000-000000008006"
	snapshotID := "00000000-0000-4000-8000-000000008007"
	snapshotItemID := "00000000-0000-4000-8000-000000008008"
	if _, err := db.ExecContext(ctx, `
INSERT INTO channel_accounts (id, channel, external_account_ref, created_at, updated_at)
VALUES ($1,'telegram','migration-test',$2,$2)`, channelID, now); err != nil {
		t.Fatalf("seed legacy migration account: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO stored_objects (id,bucket,object_key,size_bytes,checksum,storage_status,retention_state,created_at,expires_at)
VALUES ($1,'sources','sources/legacy/a',5,'sha256:same','available','active',$4::timestamptz,$4::timestamptz + interval '1 day'),
       ($2,'sources','sources/legacy/b',5,'sha256:same','available','held',$4::timestamptz + interval '1 second',NULL),
       ($3,'sources','sources/legacy/orphan',7,'sha256:orphan','available','active',$4::timestamptz,NULL)`,
		canonicalID, duplicateID, unscopedID, now); err != nil {
		t.Fatalf("seed legacy migration objects: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO media_assets (id,channel_account_id,stored_object_id,origin_type,kind,display_name,created_at,updated_at)
VALUES ($1,$2,$3,'upload','video','legacy.mp4',$5,$5),
       ($4,$2,$6,'upload','video','canonical.mp4',$5,$5)`,
		assetID, channelID, duplicateID, canonicalAssetID, now, canonicalID); err != nil {
		t.Fatalf("seed legacy migration state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO selection_snapshots (id,channel_account_id,status,created_at,sealed_at)
VALUES ($1,$2,'sealed',$3,$3)`, snapshotID, channelID, now); err != nil {
		t.Fatalf("seed sealed legacy snapshot: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO selection_snapshot_items (
    id,selection_snapshot_id,position,media_asset_id,kind,display_name,
    origin_snapshot,storage_snapshot,status_at_selection
)
VALUES (
    $2,$1,0,$3,'video','legacy.mp4',
    jsonb_build_object('origin_type','telegram_file','object_ref','sources/legacy/b'),
    jsonb_build_object(
        'stored_object_id',$4::text,'bucket','sources','object_key','sources/legacy/b',
        'checksum','sha256:same','size_bytes',5,'content_type','video/mp4'
    ),
    'available'
)`, snapshotID, snapshotItemID, assetID, duplicateID); err != nil {
		t.Fatalf("seed sealed legacy snapshot: %v", err)
	}

	applyGovernedMediaMigration(t, ctx, db)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE channel_account_id=$1 AND checksum='sha256:same' AND size_bytes=5`, 1, channelID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM media_assets WHERE id=$1 AND stored_object_id=$2`, 1, assetID, canonicalID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_object_aliases WHERE alias_id=$1 AND canonical_stored_object_id=$2`, 1, duplicateID, canonicalID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1 AND storage_status='delete_scheduled'`, 1, unscopedID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1 AND hold_state='held' AND retention_state='held'`, 1, canonicalID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1 AND generation_published_at=$2`, 1, canonicalID, now.Add(time.Second))
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1 AND expires_at IS NULL`, 1, canonicalID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM selection_snapshot_items
WHERE id=$1
  AND storage_snapshot->>'stored_object_id'=$2
  AND storage_snapshot->>'bucket'='sources'
  AND storage_snapshot->>'object_key'='sources/legacy/a'`, 1, snapshotItemID, canonicalID)
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM stored_objects WHERE id=$1`, 0, duplicateID)

	if _, err := db.ExecContext(ctx, `UPDATE selection_snapshot_items
SET storage_snapshot=storage_snapshot || jsonb_build_object(
	'stored_object_id',$2::text,'bucket','legacy-stale','object_key','sources/legacy/b'
)
WHERE id=$1`, snapshotItemID, canonicalID); err != nil {
		t.Fatalf("seed historically partial alias repair: %v", err)
	}
	applyNamedTargetMigration(t, ctx, db, "0007_repair_snapshot_alias_locators.sql")
	assertSQLCount(t, ctx, db, `SELECT count(*) FROM selection_snapshot_items
WHERE id=$1
  AND storage_snapshot->>'stored_object_id'=$2
  AND storage_snapshot->>'object_key'='sources/legacy/a'`, 1, snapshotItemID, canonicalID)
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

func applyGovernedMediaMigration(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "migrations", "0002_governed_media_export_retention.sql"))
	if err != nil {
		t.Fatalf("read governed media migration: %v", err)
	}
	up := strings.Split(string(body), "-- +goose Down")[0]
	if _, err := db.ExecContext(ctx, "SET client_min_messages TO warning;\n"+up); err != nil {
		t.Fatalf("apply governed media migration: %v", err)
	}
}

func applyMetadataEnrichmentMigration(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "migrations", "0003_youtube_metadata_enrichment.sql"))
	if err != nil {
		t.Fatalf("read metadata enrichment migration: %v", err)
	}
	up := strings.Split(string(body), "-- +goose Down")[0]
	if _, err := db.ExecContext(ctx, "SET client_min_messages TO warning;\n"+up); err != nil {
		t.Fatalf("apply metadata enrichment migration: %v", err)
	}
}

func applyNamedTargetMigration(t *testing.T, ctx context.Context, db *sql.DB, filename string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "migrations", filename))
	if err != nil {
		t.Fatalf("read migration %s: %v", filename, err)
	}
	up := strings.Split(string(body), "-- +goose Down")[0]
	if _, err := db.ExecContext(ctx, "SET client_min_messages TO warning;\n"+up); err != nil {
		t.Fatalf("apply migration %s: %v", filename, err)
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
		"stored_object_aliases",
		"stored_object_pins",
		"export_jobs",
		"metadata_enrichment_jobs",
		"export_deliveries",
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

func processingRunReplayParams(createdAt time.Time, collectionID, assetID, snapshotID, snapshotItemID, runID, runType string) CreateProcessingRunParams {
	eventID := runID[:len(runID)-1] + "4"
	snapshot := SelectionSnapshotRecord{
		ID: snapshotID, ChannelAccountID: targetTestTelegramChannelID,
		SourceCollectionID: collectionID, Status: "sealed",
		OptionSnapshotJSON: []byte(`{"language":"ru"}`), DiagnosticsJSON: []byte(`[]`),
		CreatedViaChannel: targetTestTelegramChannelID, CreatedAt: createdAt, SealedAt: createdAt,
	}
	items := []SelectionSnapshotItemRecord{{
		ID: snapshotItemID, SelectionSnapshotID: snapshotID, Position: 0,
		MediaAssetID: assetID, Kind: "voice", DisplayName: "voice.ogg",
		OriginSnapshotJSON: []byte(`{"origin_type":"telegram_file"}`), StorageSnapshotJSON: []byte(`{}`),
		MetadataJSON: []byte(`{}`), StatusAtSelection: "available", DiagnosticsJSON: []byte(`[]`),
	}}
	graph := AnalysisRunGraph{
		Run: AnalysisRunRecord{
			ID: runID, ChannelAccountID: targetTestTelegramChannelID, SelectionSnapshot: snapshotID,
			RunType: runType, Status: "queued", Version: 1, IdempotencyKey: "telegram:process:replay",
			ParamsJSON: []byte(`{"language":"ru"}`), DeliveryJSON: []byte(`{"strategy":"polling"}`),
			EvidenceGateState: "not_required", CreatedViaChannel: targetTestTelegramChannelID, CreatedAt: createdAt,
		},
		Event: AnalysisRunEventRecord{
			ID: eventID, AnalysisRunID: runID, EventType: "analysis_run.created", Version: 1,
			Status: "queued", PayloadJSON: []byte(`{"collection_membership":"detached_at_launch"}`), CreatedAt: createdAt,
		},
	}
	return CreateProcessingRunParams{
		ChannelAccountID: targetTestTelegramChannelID, CollectionID: collectionID,
		ExpectedVersion: 1, CapturedAssetIDs: []string{assetID}, DetachedAt: createdAt,
		Snapshot: snapshot, SnapshotItems: items, Graph: graph,
	}
}

const (
	targetTestTelegramChannelID                   = "00000000-0000-4000-8000-000000000002"
	targetTestTelegramInboxID                     = "00000000-0000-4000-8000-000000000102"
	targetTestOperationID                         = "00000000-0000-4000-8000-000000000151"
	targetTestReplayOperationID                   = "00000000-0000-4000-8000-000000000152"
	targetTestLocalStoredObjectID                 = "00000000-0000-4000-8000-000000000201"
	targetTestTelegramStoredObjectID              = "00000000-0000-4000-8000-000000000202"
	targetTestArtifactStoredObjectID              = "00000000-0000-4000-8000-000000000203"
	targetTestReusableMultiStoredObjectID         = "00000000-0000-4000-8000-000000000204"
	targetTestReusableMultiOtherStoredObjectID    = "00000000-0000-4000-8000-000000000205"
	targetTestLocalAssetID                        = "00000000-0000-4000-8000-000000000301"
	targetTestTelegramAssetID                     = "00000000-0000-4000-8000-000000000302"
	targetTestLocalCollectionItemID               = "00000000-0000-4000-8000-000000000401"
	targetTestTelegramCollectionItemID            = "00000000-0000-4000-8000-000000000402"
	targetTestCollectionID                        = "00000000-0000-4000-8000-000000000501"
	targetTestCollectionItemID                    = "00000000-0000-4000-8000-000000000502"
	targetTestCollectionItemTwoID                 = "00000000-0000-4000-8000-000000000503"
	targetTestCollectionItemThreeID               = "00000000-0000-4000-8000-000000000504"
	targetTestSnapshotID                          = "00000000-0000-4000-8000-000000000601"
	targetTestSnapshotItemID                      = "00000000-0000-4000-8000-000000000602"
	targetTestReusableSnapshotID                  = "00000000-0000-4000-8000-000000000603"
	targetTestReusableSnapshotItemID              = "00000000-0000-4000-8000-000000000604"
	targetTestReusableMultiSnapshotID             = "00000000-0000-4000-8000-000000000605"
	targetTestReusableMultiSnapshotItemOneID      = "00000000-0000-4000-8000-000000000606"
	targetTestReusableMultiSnapshotItemTwoID      = "00000000-0000-4000-8000-000000000607"
	targetTestRunID                               = "00000000-0000-4000-8000-000000000701"
	targetTestStepID                              = "00000000-0000-4000-8000-000000000702"
	targetTestStepInputID                         = "00000000-0000-4000-8000-000000000703"
	targetTestRunEventID                          = "00000000-0000-4000-8000-000000000704"
	targetTestProgressEventID                     = "00000000-0000-4000-8000-000000000705"
	targetTestCancelEventID                       = "00000000-0000-4000-8000-000000000706"
	targetTestFinalizeEventID                     = "00000000-0000-4000-8000-000000000707"
	targetTestReusableRunID                       = "00000000-0000-4000-8000-000000000708"
	targetTestReusableRunEventID                  = "00000000-0000-4000-8000-000000000709"
	targetTestReusableMultiRunID                  = "00000000-0000-4000-8000-000000000710"
	targetTestReusableMultiRunEventID             = "00000000-0000-4000-8000-000000000711"
	targetTestArtifactID                          = "00000000-0000-4000-8000-000000000801"
	targetTestArtifactSubjectID                   = "00000000-0000-4000-8000-000000000802"
	targetTestReusableArtifactID                  = "00000000-0000-4000-8000-000000000803"
	targetTestReusableArtifactSubjectID           = "00000000-0000-4000-8000-000000000804"
	targetTestReusableArtifactStoredObjectID      = "00000000-0000-4000-8000-000000000805"
	targetTestReusableMultiArtifactID             = "00000000-0000-4000-8000-000000000806"
	targetTestReusableMultiArtifactSubjectID      = "00000000-0000-4000-8000-000000000807"
	targetTestReusableMultiArtifactStoredObjectID = "00000000-0000-4000-8000-000000000808"
	targetTestDiagnosticID                        = "00000000-0000-4000-8000-000000000901"
	targetTestSurfaceID                           = "00000000-0000-4000-8000-000000001001"
	targetTestSurfaceReplayID                     = "00000000-0000-4000-8000-000000001002"
	targetTestSurfaceDisplayEventID               = "00000000-0000-4000-8000-000000001003"
	targetTestSurfaceStaleEventID                 = "00000000-0000-4000-8000-000000001004"
	targetTestSurfaceSupersedeEventID             = "00000000-0000-4000-8000-000000001005"
	targetTestSurfaceAddressOwnerID               = "00000000-0000-4000-8000-000000001006"
	targetTestSurfaceAddressHandoffID             = "00000000-0000-4000-8000-000000001007"
)
