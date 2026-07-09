package api

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

func TestTargetRuntimeServicePersistsTargetOperations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		snapshotItems: []targetstore.SelectionSnapshotItemRecord{{
			ID:                  "snapshot-item-1",
			SelectionSnapshotID: "snapshot-1",
			Position:            0,
			MediaAssetID:        "media-asset-1",
			Kind:                "voice",
			DisplayName:         "voice.ogg",
			StatusAtSelection:   "available",
		}},
	}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs(
			"channel-account-1",
			"operation-1",
			"media-asset-1",
			"collection-item-1",
			"snapshot-1",
			"snapshot-item-1",
			"run-1",
			"step-1",
			"step-input-1",
			"event-1",
			"surface-1",
			"surface-event-1",
		)),
	)

	account, err := service.ResolveChannelAccount(context.Background(), TargetChannelAccountRequest{
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Danila",
	})
	if err != nil || account.ChannelAccountID != "channel-account-1" {
		t.Fatalf("ResolveChannelAccount() account=%#v err=%v", account, err)
	}
	if store.channelAccount.ID != "channel-account-1" || store.channelAccount.Channel != "telegram" {
		t.Fatalf("stored channel account = %#v", store.channelAccount)
	}

	asset, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		Origin:           TargetMediaAssetOrigin{OriginType: "telegram_file", OriginRef: "file-id"},
		Kind:             "voice",
		DisplayName:      "voice.ogg",
		IdempotencyKey:   "telegram:update:1",
	})
	if err != nil || asset.MediaAssetID != "media-asset-1" {
		t.Fatalf("CreateMediaAsset() asset=%#v err=%v", asset, err)
	}
	if store.operation.OperationType != "media_asset.create" || store.operation.IdempotencyKey != "telegram:update:1" {
		t.Fatalf("stored operation request = %#v", store.operation)
	}
	if store.mediaAssetParams.MediaAsset.ChannelAccountID != "channel-account-1" ||
		store.mediaAssetParams.InboxCollection.Kind != "inbox" ||
		store.mediaAssetParams.CollectionItem.MediaAssetID != "media-asset-1" {
		t.Fatalf("stored media asset params = %#v", store.mediaAssetParams)
	}

	snapshot, err := service.CreateSelectionSnapshot(context.Background(), TargetCreateSelectionSnapshotRequest{
		ChannelAccountID:   "channel-account-1",
		SourceCollectionID: store.mediaAssetParams.InboxCollection.ID,
		Items: []TargetSelectionSnapshotItemRequest{{
			MediaAssetID: "media-asset-1",
			Position:     0,
		}},
	})
	if err != nil || snapshot.SelectionSnapshotID != "snapshot-1" {
		t.Fatalf("CreateSelectionSnapshot() snapshot=%#v err=%v", snapshot, err)
	}
	if store.selectionSnapshot.ID != "snapshot-1" || len(store.selectionSnapshotItems) != 1 {
		t.Fatalf("stored selection snapshot = %#v items=%#v", store.selectionSnapshot, store.selectionSnapshotItems)
	}

	run, err := service.CreateAnalysisRun(context.Background(), TargetCreateAnalysisRunRequest{
		ChannelAccountID:    "channel-account-1",
		SelectionSnapshotID: "snapshot-1",
		RunType:             "transcription",
		IdempotencyKey:      "run:key",
	})
	if err != nil || run.AnalysisRunID != "run-1" {
		t.Fatalf("CreateAnalysisRun() run=%#v err=%v", run, err)
	}
	if store.analysisRunGraph.Run.SelectionSnapshot != "snapshot-1" ||
		len(store.analysisRunGraph.Steps) != 1 ||
		len(store.analysisRunGraph.StepInputs) != 1 ||
		store.analysisRunGraph.StepInputs[0].SelectionSnapshotItemID != "snapshot-item-1" {
		t.Fatalf("stored analysis run graph = %#v", store.analysisRunGraph)
	}

	claim, err := service.ClaimAnalysisRunStep(context.Background(), "run-1", TargetClaimAnalysisRunStepRequest{
		WorkerKind: "transcription",
		StepKind:   "selection.transcription",
		LeaseOwner: "worker-1",
	})
	if err != nil || claim.AnalysisRunStepID != "step-1" || len(claim.AnalysisRunStepInputs) != 1 {
		t.Fatalf("ClaimAnalysisRunStep() claim=%#v err=%v", claim, err)
	}

	surface, err := service.UpsertChannelSurface(context.Background(), TargetUpsertChannelSurfaceRequest{
		ChannelAccountID: "channel-account-1",
		Channel:          "telegram",
		SurfaceType:      "message",
		SurfaceKey:       "run:run-1",
		Subjects: []TargetChannelSurfaceSubject{{
			SubjectType: "analysis_run",
			SubjectID:   "run-1",
			SubjectRole: "primary",
		}},
	})
	if err != nil || surface.ChannelSurfaceID != "surface-1" {
		t.Fatalf("UpsertChannelSurface() surface=%#v err=%v", surface, err)
	}
	if store.surface.ID != "surface-1" || len(store.surfaceSubjects) != 1 {
		t.Fatalf("stored surface = %#v subjects=%#v", store.surface, store.surfaceSubjects)
	}

	event, err := service.SupersedeChannelSurface(context.Background(), TargetSupersedeChannelSurfaceRequest{
		SurfaceID: "surface-1",
		Reason:    "message_not_editable",
		ActorType: "telegram_adapter",
		ActorID:   "bot",
	})
	if err != nil || event.ChannelSurfaceEventID != "surface-event-1" {
		t.Fatalf("SupersedeChannelSurface() event=%#v err=%v", event, err)
	}
	if store.supersede.SurfaceID != "surface-1" || store.supersede.Event.Reason != "message_not_editable" {
		t.Fatalf("stored supersede = %#v", store.supersede)
	}
}

func TestTargetRuntimeServiceResolveChannelAccountReturnsPersistedConflictID(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 13, 15, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		channelAccount: targetstore.ChannelAccountRecord{
			ID:                 "persisted-channel-account",
			Channel:            "telegram",
			ExternalAccountRef: "chat-1",
			DisplayName:        "Existing",
			Status:             "active",
			MetadataJSON:       []byte(`{"first":true}`),
			CreatedAt:          now.Add(-time.Hour),
			UpdatedAt:          now.Add(-time.Hour),
		},
	}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs("generated-channel-account")),
	)

	account, err := service.ResolveChannelAccount(context.Background(), TargetChannelAccountRequest{
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Updated",
	})
	if err != nil {
		t.Fatalf("ResolveChannelAccount() error = %v", err)
	}
	if account.ChannelAccountID != "persisted-channel-account" {
		t.Fatalf("resolved channel account id = %q, want persisted conflict id", account.ChannelAccountID)
	}
}

func TestTargetRuntimeServiceReplaysMediaAssetIdempotencyKey(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 13, 30, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{operationsByKey: map[string]targetstore.OperationRequestRecord{}}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs(
			"operation-1",
			"media-asset-1",
			"collection-item-1",
			"operation-2",
			"media-asset-2",
			"collection-item-2",
		)),
	)

	first, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		Origin:           TargetMediaAssetOrigin{OriginType: "upload", OriginRef: "uploads/file.txt"},
		Kind:             "document",
		DisplayName:      "file.txt",
		IdempotencyKey:   "upload:stable",
	})
	if err != nil {
		t.Fatalf("CreateMediaAsset(first) error = %v", err)
	}
	replayed, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		Origin:           TargetMediaAssetOrigin{OriginType: "upload", OriginRef: "uploads/file-duplicate.txt"},
		Kind:             "document",
		DisplayName:      "file-duplicate.txt",
		IdempotencyKey:   "upload:stable",
	})
	if err != nil {
		t.Fatalf("CreateMediaAsset(replay) error = %v", err)
	}
	if replayed.MediaAssetID != first.MediaAssetID {
		t.Fatalf("replayed media asset id = %q, want original %q", replayed.MediaAssetID, first.MediaAssetID)
	}
	if store.mediaAssetCreateCalls != 1 {
		t.Fatalf("CreateMediaAssetWithInbox calls = %d, want 1", store.mediaAssetCreateCalls)
	}
}

func TestTargetRuntimeServiceReadAndLifecycleQueriesUseTargetStore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 14, 30, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		channelAccount: targetstore.ChannelAccountRecord{
			ID:                 "channel-account-1",
			Channel:            "telegram",
			ExternalAccountRef: "chat-1",
			DisplayName:        "Danila",
			Status:             "active",
			MetadataJSON:       []byte(`{"lang":"ru"}`),
			CreatedAt:          now.Add(-time.Hour),
			UpdatedAt:          now.Add(-time.Hour),
		},
		snapshotItems: []targetstore.SelectionSnapshotItemRecord{{
			ID:                  "snapshot-item-1",
			SelectionSnapshotID: "snapshot-1",
			Position:            0,
			MediaAssetID:        "media-asset-1",
			Kind:                "voice",
			DisplayName:         "voice.ogg",
			StatusAtSelection:   "available",
		}},
	}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs(
			"collection-1",
			"collection-item-1",
			"collection-item-2",
			"cancel-event-1",
			"retry-run-1",
			"retry-step-1",
			"retry-step-input-1",
			"retry-event-1",
			"surface-event-replace-1",
			"finalize-event-1",
		)),
	)
	ctx := context.Background()

	channelAccounts, err := service.ListChannelAccounts(ctx, TargetListChannelAccountsRequest{})
	if err != nil {
		t.Fatalf("ListChannelAccounts() error = %v", err)
	}
	if channelAccounts.PageSize != 20 || len(channelAccounts.Items) != 1 || channelAccounts.Items[0].ChannelAccountID != "channel-account-1" {
		t.Fatalf("ListChannelAccounts() = %#v", channelAccounts)
	}

	updatedAccount, err := service.UpdateChannelAccount(ctx, TargetUpdateChannelAccountRequest{
		ChannelAccountID: "channel-account-1",
		DisplayName:      "Danila B",
		Status:           "disabled",
	})
	if err != nil {
		t.Fatalf("UpdateChannelAccount() error = %v", err)
	}
	if updatedAccount.ChannelAccountID != "channel-account-1" || updatedAccount.DisplayName != "Danila B" || updatedAccount.Status != "disabled" {
		t.Fatalf("UpdateChannelAccount() = %#v", updatedAccount)
	}

	mediaPage, err := service.ListMediaAssets(ctx, TargetListMediaAssetsRequest{ChannelAccountID: "channel-account-1"})
	if err != nil {
		t.Fatalf("ListMediaAssets() error = %v", err)
	}
	if mediaPage.PageSize != 20 || len(mediaPage.Items) != 1 || mediaPage.Items[0].MediaAssetID != "media-asset-1" {
		t.Fatalf("ListMediaAssets() = %#v", mediaPage)
	}

	media, err := service.GetMediaAsset(ctx, TargetGetMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		MediaAssetID:     "media-asset-1",
	})
	if err != nil {
		t.Fatalf("GetMediaAsset() error = %v", err)
	}
	if media.MediaAssetID != "media-asset-1" || media.Status != "available" {
		t.Fatalf("GetMediaAsset() = %#v", media)
	}

	deleted, err := service.DeleteMediaAsset(ctx, TargetDeleteMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		MediaAssetID:     "media-asset-1",
	})
	if err != nil {
		t.Fatalf("DeleteMediaAsset() error = %v", err)
	}
	if deleted.Status != "deleted" || deleted.DeletedAt == nil {
		t.Fatalf("DeleteMediaAsset() = %#v", deleted)
	}

	inbox, err := service.GetInboxCollection(ctx, TargetGetInboxCollectionRequest{ChannelAccountID: "channel-account-1"})
	if err != nil {
		t.Fatalf("GetInboxCollection() error = %v", err)
	}
	if inbox.CollectionID != "inbox-1" || inbox.Kind != "inbox" {
		t.Fatalf("GetInboxCollection() = %#v", inbox)
	}

	createdCollection, err := service.CreateCollection(ctx, TargetCreateCollectionRequest{
		ChannelAccountID: "channel-account-1",
		Name:             "Research",
		Items:            []string{"media-asset-1"},
	})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	if createdCollection.CollectionID != "collection-1" || len(createdCollection.Items) != 1 || createdCollection.Items[0].CollectionItemID != "collection-item-1" {
		t.Fatalf("CreateCollection() = %#v", createdCollection)
	}

	collections, err := service.ListCollections(ctx, TargetListCollectionsRequest{
		ChannelAccountID: "channel-account-1",
		PageSize:         7,
	})
	if err != nil {
		t.Fatalf("ListCollections() error = %v", err)
	}
	if collections.PageSize != 7 || len(collections.Items) != 1 || collections.Items[0].CollectionID != "collection-1" {
		t.Fatalf("ListCollections() = %#v", collections)
	}

	collection, err := service.GetCollection(ctx, TargetGetCollectionRequest{
		ChannelAccountID: "channel-account-1",
		CollectionID:     "collection-1",
	})
	if err != nil {
		t.Fatalf("GetCollection() error = %v", err)
	}
	if collection.CollectionID != "collection-1" || collection.Version != 1 {
		t.Fatalf("GetCollection() = %#v", collection)
	}

	updatedCollection, err := service.UpdateCollection(ctx, TargetUpdateCollectionRequest{
		ChannelAccountID: "channel-account-1",
		CollectionID:     "collection-1",
		ExpectedVersion:  2,
		Name:             "Research v2",
		Status:           "active",
	})
	if err != nil {
		t.Fatalf("UpdateCollection() error = %v", err)
	}
	if updatedCollection.Name != "Research v2" || updatedCollection.Version != 3 {
		t.Fatalf("UpdateCollection() = %#v", updatedCollection)
	}

	replacedItems, err := service.UpdateCollectionItems(ctx, TargetUpdateCollectionItemsRequest{
		ChannelAccountID: "channel-account-1",
		CollectionID:     "collection-1",
		ExpectedVersion:  3,
		Items: []TargetCollectionItemMutationInput{{
			MediaAssetID: "media-asset-1",
			Position:     2,
		}},
	})
	if err != nil {
		t.Fatalf("UpdateCollectionItems() error = %v", err)
	}
	if replacedItems.Version != 4 || len(replacedItems.Items) != 1 || replacedItems.Items[0].Position != 2 {
		t.Fatalf("UpdateCollectionItems() = %#v", replacedItems)
	}

	removedItemCollection, err := service.RemoveCollectionItem(ctx, TargetRemoveCollectionItemRequest{
		ChannelAccountID: "channel-account-1",
		CollectionID:     "collection-1",
		MediaAssetID:     "media-asset-1",
		ExpectedVersion:  4,
	})
	if err != nil {
		t.Fatalf("RemoveCollectionItem() error = %v", err)
	}
	if removedItemCollection.Version != 5 || len(removedItemCollection.Items) != 0 {
		t.Fatalf("RemoveCollectionItem() = %#v", removedItemCollection)
	}

	snapshot, err := service.GetSelectionSnapshot(ctx, TargetGetSelectionSnapshotRequest{
		ChannelAccountID:    "channel-account-1",
		SelectionSnapshotID: "snapshot-1",
	})
	if err != nil {
		t.Fatalf("GetSelectionSnapshot() error = %v", err)
	}
	if snapshot.SelectionSnapshotID != "snapshot-1" || len(snapshot.Items) != 1 || snapshot.Items[0].SelectionSnapshotItemID != "snapshot-item-1" {
		t.Fatalf("GetSelectionSnapshot() = %#v", snapshot)
	}

	runs, err := service.ListAnalysisRuns(ctx, TargetListAnalysisRunsRequest{ChannelAccountID: "channel-account-1"})
	if err != nil {
		t.Fatalf("ListAnalysisRuns() error = %v", err)
	}
	if runs.PageSize != 20 || len(runs.Items) != 1 || runs.Items[0].AnalysisRunID != "run-1" {
		t.Fatalf("ListAnalysisRuns() = %#v", runs)
	}

	run, err := service.GetAnalysisRun(ctx, TargetGetAnalysisRunRequest{
		ChannelAccountID: "channel-account-1",
		AnalysisRunID:    "run-1",
	})
	if err != nil {
		t.Fatalf("GetAnalysisRun() error = %v", err)
	}
	if run.AnalysisRunID != "run-1" || run.Status != "queued" {
		t.Fatalf("GetAnalysisRun() = %#v", run)
	}

	canceled, err := service.CancelAnalysisRun(ctx, "run-1", TargetCancelAnalysisRunRequest{
		ChannelAccountID: "channel-account-1",
		Message:          "stop",
	})
	if err != nil {
		t.Fatalf("CancelAnalysisRun() error = %v", err)
	}
	if canceled.Status != "cancel_requested" || canceled.CancelRequestedAt == nil {
		t.Fatalf("CancelAnalysisRun() = %#v", canceled)
	}

	retried, err := service.RetryAnalysisRun(ctx, "run-1", TargetRetryAnalysisRunRequest{
		ChannelAccountID: "channel-account-1",
		IdempotencyKey:   "retry-key",
	})
	if err != nil {
		t.Fatalf("RetryAnalysisRun() error = %v", err)
	}
	if retried.AnalysisRunID != "retry-run-1" || len(retried.Steps) != 1 || retried.Steps[0].AnalysisRunStepID != "retry-step-1" {
		t.Fatalf("RetryAnalysisRun() = %#v", retried)
	}

	events, err := service.ListAnalysisRunEvents(ctx, TargetListAnalysisRunEventsRequest{
		ChannelAccountID: "channel-account-1",
		AnalysisRunID:    "run-1",
	})
	if err != nil {
		t.Fatalf("ListAnalysisRunEvents() error = %v", err)
	}
	if len(events.Items) != 1 || events.Items[0].AnalysisRunEventID != "event-1" {
		t.Fatalf("ListAnalysisRunEvents() = %#v", events)
	}

	artifacts, err := service.ListArtifacts(ctx, TargetListArtifactsRequest{
		ChannelAccountID: "channel-account-1",
		AnalysisRunID:    "run-1",
	})
	if err != nil {
		t.Fatalf("ListArtifacts() error = %v", err)
	}
	if len(artifacts.Items) != 1 || artifacts.Items[0].ArtifactID != "artifact-1" {
		t.Fatalf("ListArtifacts() = %#v", artifacts)
	}

	artifact, err := service.GetArtifact(ctx, TargetGetArtifactRequest{
		ChannelAccountID: "channel-account-1",
		ArtifactID:       "artifact-1",
	})
	if err != nil {
		t.Fatalf("GetArtifact() error = %v", err)
	}
	if artifact.ArtifactID != "artifact-1" || artifact.Visibility != "channel_deliverable" {
		t.Fatalf("GetArtifact() = %#v", artifact)
	}

	diagnostics, err := service.ListDiagnostics(ctx, TargetListDiagnosticsRequest{
		ChannelAccountID: "channel-account-1",
		SubjectType:      "analysis_run",
		SubjectID:        "run-1",
		Severity:         "warning",
	})
	if err != nil {
		t.Fatalf("ListDiagnostics() error = %v", err)
	}
	if len(diagnostics.Items) != 1 || diagnostics.Items[0].SubjectID != "run-1" {
		t.Fatalf("ListDiagnostics() = %#v", diagnostics)
	}

	queue, err := service.ListAnalysisRunStepQueue(ctx, TargetAnalysisRunStepQueueRequest{
		Status:     "queued",
		RunType:    "transcription",
		WorkerKind: "transcription",
		StepKind:   "selection.transcription",
	})
	if err != nil {
		t.Fatalf("ListAnalysisRunStepQueue() error = %v", err)
	}
	if queue.PageSize != 20 || len(queue.Items) != 1 || queue.Items[0].AnalysisRunStepID != "step-1" {
		t.Fatalf("ListAnalysisRunStepQueue() = %#v", queue)
	}

	cancelState, err := service.CheckAnalysisRunStepCancel(ctx, "run-1", TargetCheckAnalysisRunStepCancelRequest{
		AnalysisRunStepID: "step-1",
	})
	if err != nil {
		t.Fatalf("CheckAnalysisRunStepCancel() error = %v", err)
	}
	if cancelState.CancelRequested || cancelState.Status != "running" {
		t.Fatalf("CheckAnalysisRunStepCancel() = %#v", cancelState)
	}

	surfaces, err := service.ListChannelSurfaces(ctx, TargetListChannelSurfacesRequest{
		ChannelAccountID: "channel-account-1",
		SubjectType:      "analysis_run",
		SubjectID:        "run-1",
		ActiveOnly:       true,
	})
	if err != nil {
		t.Fatalf("ListChannelSurfaces() error = %v", err)
	}
	if len(surfaces.Items) != 1 || len(surfaces.Items[0].Subjects) != 1 || surfaces.Items[0].Subjects[0].SubjectID != "run-1" {
		t.Fatalf("ListChannelSurfaces() = %#v", surfaces)
	}

	replacedSurface, err := service.ReplaceChannelSurfaceDisplayState(ctx, TargetReplaceChannelSurfaceDisplayStateRequest{
		SurfaceID:       "surface-1",
		ExpectedVersion: 1,
		DisplayState:    []byte(`{"status":"running"}`),
		ActorType:       "telegram_adapter",
		ActorID:         "bot",
		Metadata:        []byte(`{"reason":"progress"}`),
	})
	if err != nil {
		t.Fatalf("ReplaceChannelSurfaceDisplayState() error = %v", err)
	}
	if replacedSurface.Version != 2 || string(replacedSurface.DisplayState) != `{"status":"running"}` {
		t.Fatalf("ReplaceChannelSurfaceDisplayState() = %#v", replacedSurface)
	}

	surfaceEvents, err := service.ListChannelSurfaceEvents(ctx, TargetListChannelSurfaceEventsRequest{SurfaceID: "surface-1"})
	if err != nil {
		t.Fatalf("ListChannelSurfaceEvents() error = %v", err)
	}
	if len(surfaceEvents.Items) != 1 || surfaceEvents.Items[0].ChannelSurfaceEventID != "surface-event-1" {
		t.Fatalf("ListChannelSurfaceEvents() = %#v", surfaceEvents)
	}

	finalized, err := service.FinalizeAnalysisRunStep(ctx, "run-1", TargetFinalizeAnalysisRunStepRequest{
		AnalysisRunStepID: "step-1",
		Outcome:           "partially_succeeded",
		Message:           "done",
	})
	if err != nil {
		t.Fatalf("FinalizeAnalysisRunStep() error = %v", err)
	}
	if finalized.Status != "partially_succeeded" || finalized.CompletedAt == nil {
		t.Fatalf("FinalizeAnalysisRunStep() = %#v", finalized)
	}
}

func TestTargetRuntimeServiceRecordsWorkerWrites(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 15, 0, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs(
			"progress-event-1",
			"artifact-1",
			"stored-object-1",
			"subject-run-1",
			"subject-step-1",
		)),
	)
	ctx := context.Background()

	if err := service.RecordAnalysisRunStepProgress(ctx, "run-1", TargetRecordAnalysisRunStepProgressRequest{
		AnalysisRunStepID: "step-1",
		ProgressStage:     "transcribing",
		ProgressMessage:   "working",
		Payload:           []byte(`{"percent":50}`),
	}); err != nil {
		t.Fatalf("RecordAnalysisRunStepProgress() error = %v", err)
	}
	if store.progress.AnalysisRunStepID != "step-1" ||
		store.progress.Event.ID != "progress-event-1" ||
		!bytes.Contains(store.progress.Event.PayloadJSON, []byte(`"progress_stage":"transcribing"`)) {
		t.Fatalf("stored progress = %#v", store.progress)
	}

	if err := service.RecordAnalysisRunArtifacts(ctx, "run-1", TargetRecordAnalysisRunArtifactsRequest{
		AnalysisRunStepID: "step-1",
		Artifacts: []workerArtifactDescriptor{{
			ArtifactKind: "summary_markdown",
			MIMEType:     "text/markdown",
			ObjectKey:    "run-1/summary.md",
			SizeBytes:    7,
			Filename:     "summary.md",
			Format:       "markdown",
		}},
	}); err != nil {
		t.Fatalf("RecordAnalysisRunArtifacts() error = %v", err)
	}
	if store.artifactCalls != 1 ||
		len(store.storedObjects) != 1 ||
		len(store.artifacts) != 1 ||
		len(store.artifactSubjects) != 2 ||
		store.artifacts[0].Kind != "summary" ||
		store.artifacts[0].StoredObjectID != "stored-object-1" {
		t.Fatalf("stored artifacts objects=%#v artifacts=%#v subjects=%#v", store.storedObjects, store.artifacts, store.artifactSubjects)
	}

	if err := service.RecordAnalysisRunDiagnostics(ctx, "run-1", TargetRecordAnalysisRunDiagnosticsRequest{
		AnalysisRunStepID: "step-1",
		Diagnostics: []workerDiagnosticDescriptor{{
			DiagnosticID:       "diagnostic-1",
			SubjectType:        "analysis_run",
			SubjectID:          "run-1",
			Severity:           "warning",
			Code:               "transcript_missing",
			Message:            "Transcript is missing",
			Context:            map[string]any{"source": "worker"},
			SafeChannelContext: map[string]any{"chat_id": "chat-1"},
			CorrelationID:      "corr-1",
			RemediationHint:    "retry",
		}},
	}); err != nil {
		t.Fatalf("RecordAnalysisRunDiagnostics() error = %v", err)
	}
	if store.diagnosticCalls != 1 ||
		len(store.diagnostics) != 1 ||
		store.diagnostics[0].ChannelAccountID != "channel-account-1" ||
		!bytes.Contains(store.diagnostics[0].ContextJSON, []byte(`"analysis_run_step_id":"step-1"`)) ||
		!bytes.Contains(store.diagnostics[0].SafeChannelContext, []byte(`"chat_id":"chat-1"`)) {
		t.Fatalf("stored diagnostics = %#v", store.diagnostics)
	}
}

func TestTargetRuntimeServiceFindsReusableTranscriptForStoredObject(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 14, 30, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		reusableRun: targetstore.AnalysisRunRecord{
			ID:                "run-reused",
			ChannelAccountID:  "channel-account-1",
			SelectionSnapshot: "snapshot-reused",
			RunType:           "transcription",
			Status:            "succeeded",
			Version:           3,
			ParamsJSON:        []byte(`{}`),
			DeliveryJSON:      []byte(`{"strategy":"polling"}`),
			EvidenceGateState: "not_required",
			CreatedAt:         now.Add(-time.Hour),
			CompletedAt:       &now,
		},
		reusableArtifact: targetstore.ArtifactRecord{
			ID:               "artifact-reused",
			ChannelAccountID: "channel-account-1",
			AnalysisRunID:    "run-reused",
			Kind:             "transcript",
			Status:           "available",
			ContentType:      "text/plain; charset=utf-8",
			Visibility:       "channel_deliverable",
			PreviewJSON:      []byte(`{"available":true,"filename":"transcript.txt"}`),
			CreatedAt:        now,
		},
	}
	service := NewTargetRuntimeService(store, WithTargetClock(func() time.Time { return now }))

	result, found, err := service.FindReusableTranscript(context.Background(), TargetReusableTranscriptRequest{
		ChannelAccountID: "channel-account-1",
		StoredObjectID:   "stored-source-1",
		Checksum:         "sha256:source",
	})

	if err != nil {
		t.Fatalf("FindReusableTranscript() error = %v", err)
	}
	if !found {
		t.Fatalf("FindReusableTranscript() found=false, want true")
	}
	if result.AnalysisRun.AnalysisRunID != "run-reused" || result.Artifact.ArtifactID != "artifact-reused" {
		t.Fatalf("FindReusableTranscript() = %#v", result)
	}
	if store.reusableTranscriptReq.StoredObjectID != "stored-source-1" || store.reusableTranscriptReq.Checksum != "sha256:source" {
		t.Fatalf("store reusable request = %#v", store.reusableTranscriptReq)
	}
}

func TestTargetRuntimeRecordHelpersCoverPayloadEdges(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 15, 30, 0, 0, time.UTC)
	expiresAt := now.Add(time.Hour)
	deletedAt := now.Add(2 * time.Hour)
	object := targetstore.StoredObjectRecord{
		ID:             "stored-object-1",
		Bucket:         "sources",
		ObjectKey:      "sources/uploads/voice.ogg",
		ContentType:    "audio/ogg",
		SizeBytes:      42,
		Checksum:       "sha256:abc",
		StorageStatus:  "available",
		RetentionState: "active",
		CreatedAt:      now,
		ExpiresAt:      &expiresAt,
		DeletedAt:      &deletedAt,
	}

	textOrigin := targetOriginSnapshotPayload(targetstore.MediaAssetRecord{OriginType: "text", OriginRef: "hello"}, targetstore.StoredObjectRecord{})
	if textOrigin["origin_type"] != "text" || textOrigin["text"] != "hello" {
		t.Fatalf("text origin payload = %#v", textOrigin)
	}
	urlOrigin := targetOriginSnapshotPayload(targetstore.MediaAssetRecord{OriginType: "url", OriginRef: "https://example.test"}, targetstore.StoredObjectRecord{})
	if urlOrigin["origin_type"] != "url" || urlOrigin["url"] != "https://example.test" {
		t.Fatalf("url origin payload = %#v", urlOrigin)
	}
	uploadOrigin := targetOriginSnapshotPayload(targetstore.MediaAssetRecord{OriginType: "upload", OriginRef: "fallback.ogg"}, object)
	if uploadOrigin["origin_type"] != "upload" || uploadOrigin["object_ref"] != "sources/uploads/voice.ogg" || uploadOrigin["content_type"] != "audio/ogg" {
		t.Fatalf("upload origin payload = %#v", uploadOrigin)
	}
	customOrigin := targetOriginSnapshotPayload(targetstore.MediaAssetRecord{OriginType: "custom", OriginRef: "custom-ref"}, targetstore.StoredObjectRecord{})
	if customOrigin["origin_type"] != "custom" || customOrigin["object_ref"] != "custom-ref" {
		t.Fatalf("custom origin payload = %#v", customOrigin)
	}

	emptyStorage := targetStorageSnapshotPayload(targetstore.StoredObjectRecord{})
	if len(emptyStorage) != 0 {
		t.Fatalf("empty storage payload = %#v", emptyStorage)
	}
	storagePayload := targetStorageSnapshotPayload(object)
	if storagePayload["stored_object_id"] != "stored-object-1" ||
		storagePayload["expires_at"] != &expiresAt ||
		storagePayload["deleted_at"] != &deletedAt {
		t.Fatalf("storage payload = %#v", storagePayload)
	}

	item := targetCollectionItemFromRecord(targetstore.CollectionItemRecord{
		ID:              "collection-item-1",
		MediaAssetID:    "media-asset-1",
		Position:        3,
		AddedViaChannel: "channel-account-1",
		AddedAt:         now,
		MediaAsset: &targetstore.MediaAssetRecord{
			ID:               "media-asset-1",
			ChannelAccountID: "channel-account-1",
			OriginType:       "upload",
			OriginRef:        "sources/uploads/voice.ogg",
			Kind:             "voice",
			DisplayName:      "voice.ogg",
			Status:           "available",
			CreatedAt:        now,
			UpdatedAt:        now,
		},
	})
	if item.MediaAsset == nil || item.MediaAsset.MediaAssetID != "media-asset-1" || item.Position != 3 {
		t.Fatalf("collection item = %#v", item)
	}

	if !isSpeechMediaKind("video") || isSpeechMediaKind("document") {
		t.Fatalf("isSpeechMediaKind returned unexpected result")
	}
	if withDefaultString("", "fallback") != "fallback" || withDefaultString("value", "fallback") != "value" {
		t.Fatalf("withDefaultString returned unexpected result")
	}
}

func TestTargetOutcomeStatusMapsKnownOutcomes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		outcome  string
		wantStep string
		wantRun  string
	}{
		{name: "default", outcome: "", wantStep: "succeeded", wantRun: "succeeded"},
		{name: "succeeded", outcome: "succeeded", wantStep: "succeeded", wantRun: "succeeded"},
		{name: "partial", outcome: "partially_succeeded", wantStep: "partially_succeeded", wantRun: "partially_succeeded"},
		{name: "failed", outcome: "failed", wantStep: "failed", wantRun: "failed"},
		{name: "canceled", outcome: "canceled", wantStep: "canceled", wantRun: "canceled"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stepStatus, runStatus, err := targetOutcomeStatus(tc.outcome)
			if err != nil {
				t.Fatalf("targetOutcomeStatus(%q) error = %v", tc.outcome, err)
			}
			if stepStatus != tc.wantStep || runStatus != tc.wantRun {
				t.Fatalf("targetOutcomeStatus(%q) = (%q, %q), want (%q, %q)", tc.outcome, stepStatus, runStatus, tc.wantStep, tc.wantRun)
			}
		})
	}

	_, _, err := targetOutcomeStatus("unexpected")
	if !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("targetOutcomeStatus(unexpected) error = %v, want ErrContractViolation", err)
	}
}

func TestTargetRuntimeServicePersistsUploadBodyToSourceObjectStore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 14, 0, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{}
	objects := &fakeTargetObjectStore{}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs("media-asset-1", "collection-item-1")),
		WithTargetObjectStore(objects),
	)

	asset, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		Origin: TargetMediaAssetOrigin{
			OriginType:       "upload",
			OriginRef:        "sources/uploads/stored-object-1/voice.ogg",
			StoredObjectID:   "stored-object-1",
			ContentType:      "audio/ogg",
			SizeBytes:        11,
			Checksum:         "sha256:007ab34004c58b28cfd2f9746c848f29e54eac1c0c4c45c6166cbf2f71217850",
			UploadBody:       []byte("voice-bytes"),
			OriginalFilename: "voice.ogg",
		},
		Kind:        "voice",
		DisplayName: "Голосовое из Telegram",
	})
	if err != nil {
		t.Fatalf("CreateMediaAsset(upload body) error = %v", err)
	}
	if asset.MediaAssetID != "media-asset-1" {
		t.Fatalf("media asset id = %q, want media-asset-1", asset.MediaAssetID)
	}
	if len(objects.puts) != 1 {
		t.Fatalf("object store puts = %d, want 1", len(objects.puts))
	}
	put := objects.puts[0]
	if put.bucket != storage.SourcesBucket || put.objectKey != "sources/uploads/stored-object-1/voice.ogg" || put.contentType != "audio/ogg" {
		t.Fatalf("object store put = %#v", put)
	}
	if !bytes.Equal(put.body, []byte("voice-bytes")) {
		t.Fatalf("object body = %q, want voice-bytes", string(put.body))
	}
	if store.mediaAssetParams.StoredObject.ObjectKey != "sources/uploads/stored-object-1/voice.ogg" {
		t.Fatalf("stored object key = %q", store.mediaAssetParams.StoredObject.ObjectKey)
	}
}

func TestTargetRuntimeServiceUploadBodyDefaultsContentTypeAndChecksum(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 14, 5, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{}
	objects := &fakeTargetObjectStore{}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs("media-asset-1", "collection-item-1")),
		WithTargetObjectStore(objects),
	)

	_, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
		ChannelAccountID: "channel-account-1",
		Origin: TargetMediaAssetOrigin{
			OriginType:     "upload",
			OriginRef:      "sources/uploads/stored-object-1/raw.bin",
			StoredObjectID: "stored-object-1",
			UploadBody:     []byte("raw-bytes"),
		},
		Kind:        "document",
		DisplayName: "raw.bin",
	})
	if err != nil {
		t.Fatalf("CreateMediaAsset(upload defaults) error = %v", err)
	}
	if len(objects.puts) != 1 {
		t.Fatalf("object store puts = %d, want 1", len(objects.puts))
	}
	if objects.puts[0].contentType != "application/octet-stream" {
		t.Fatalf("default content type = %q, want application/octet-stream", objects.puts[0].contentType)
	}
	if store.mediaAssetParams.StoredObject.SizeBytes != int64(len("raw-bytes")) {
		t.Fatalf("stored object size = %d, want uploaded body size", store.mediaAssetParams.StoredObject.SizeBytes)
	}
	if store.mediaAssetParams.StoredObject.Checksum != targetUploadChecksum([]byte("raw-bytes")) {
		t.Fatalf("stored checksum = %q, want uploaded body checksum", store.mediaAssetParams.StoredObject.Checksum)
	}
}

func TestTargetRuntimeServiceRejectsInvalidUploadBodies(t *testing.T) {
	t.Parallel()

	putErr := errors.New("put failed")
	baseOrigin := TargetMediaAssetOrigin{
		OriginType:     "upload",
		OriginRef:      "sources/uploads/stored-object-1/raw.bin",
		StoredObjectID: "stored-object-1",
		UploadBody:     []byte("raw-bytes"),
	}
	testCases := []struct {
		name    string
		objects *fakeTargetObjectStore
		mutate  func(*TargetMediaAssetOrigin)
		wantErr error
	}{
		{
			name:    "missing object store",
			objects: nil,
			wantErr: storage.ErrContractViolation,
		},
		{
			name:    "missing object ref and stored object id",
			objects: &fakeTargetObjectStore{},
			mutate: func(origin *TargetMediaAssetOrigin) {
				origin.OriginRef = ""
				origin.ObjectRef = ""
				origin.StoredObjectID = ""
			},
			wantErr: storage.ErrContractViolation,
		},
		{
			name:    "size mismatch",
			objects: &fakeTargetObjectStore{},
			mutate: func(origin *TargetMediaAssetOrigin) {
				origin.SizeBytes = 99
			},
			wantErr: storage.ErrContractViolation,
		},
		{
			name:    "checksum mismatch",
			objects: &fakeTargetObjectStore{},
			mutate: func(origin *TargetMediaAssetOrigin) {
				origin.Checksum = "sha256:not-the-upload-body"
			},
			wantErr: storage.ErrContractViolation,
		},
		{
			name:    "object store failure",
			objects: &fakeTargetObjectStore{err: putErr},
			wantErr: storage.ErrStorageUnavailable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			origin := baseOrigin
			if tc.mutate != nil {
				tc.mutate(&origin)
			}
			store := &fakeTargetRuntimeStore{}
			opts := []TargetRuntimeOption{WithTargetIDGenerator(sequenceTargetIDs("media-asset-1"))}
			if tc.objects != nil {
				opts = append(opts, WithTargetObjectStore(tc.objects))
			}
			service := NewTargetRuntimeService(store, opts...)

			_, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
				ChannelAccountID: "channel-account-1",
				Origin:           origin,
				Kind:             "document",
				DisplayName:      "raw.bin",
			})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateMediaAsset(%s) error = %v, want %v", tc.name, err, tc.wantErr)
			}
			if store.mediaAssetCreateCalls != 0 {
				t.Fatalf("CreateMediaAssetWithInbox calls = %d, want 0 for invalid upload", store.mediaAssetCreateCalls)
			}
		})
	}
}

func TestTargetRuntimeServicePlansSpeechPrerequisiteForReportRuns(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 14, 0, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		snapshotItems: []targetstore.SelectionSnapshotItemRecord{{
			ID:                  "snapshot-item-voice",
			SelectionSnapshotID: "snapshot-1",
			Position:            0,
			MediaAssetID:        "media-asset-voice",
			Kind:                "voice",
			DisplayName:         "voice.ogg",
			StatusAtSelection:   "available",
		}},
	}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs(
			"run-report-1",
			"step-transcription-1",
			"step-input-voice-1",
			"step-analysis-1",
			"event-report-1",
		)),
	)

	run, err := service.CreateAnalysisRun(context.Background(), TargetCreateAnalysisRunRequest{
		ChannelAccountID:    "channel-account-1",
		SelectionSnapshotID: "snapshot-1",
		RunType:             "report",
	})
	if err != nil {
		t.Fatalf("CreateAnalysisRun(report) error = %v", err)
	}
	if len(store.analysisRunGraph.Steps) != 2 {
		t.Fatalf("planned steps = %#v, want transcription prerequisite plus pending analysis", store.analysisRunGraph.Steps)
	}
	if store.analysisRunGraph.Steps[0].WorkerKind != "transcription" || store.analysisRunGraph.Steps[0].Status != "queued" {
		t.Fatalf("transcription prerequisite step = %#v", store.analysisRunGraph.Steps[0])
	}
	if store.analysisRunGraph.Steps[1].WorkerKind != "agent_runner" || store.analysisRunGraph.Steps[1].Status != "pending" {
		t.Fatalf("analysis step = %#v, want pending agent_runner", store.analysisRunGraph.Steps[1])
	}
	if len(store.analysisRunGraph.StepInputs) != 1 ||
		store.analysisRunGraph.StepInputs[0].AnalysisRunStepID != "step-transcription-1" ||
		store.analysisRunGraph.StepInputs[0].SelectionSnapshotItemID != "snapshot-item-voice" {
		t.Fatalf("planned step inputs = %#v", store.analysisRunGraph.StepInputs)
	}
	if len(run.Steps) != 2 || run.Steps[1].Status != "pending" {
		t.Fatalf("run response steps = %#v", run.Steps)
	}
}

func TestTargetRuntimeServicePlansDeepResearchDirectlyForTextInputs(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 14, 10, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		snapshotItems: []targetstore.SelectionSnapshotItemRecord{{
			ID:                  "snapshot-item-document",
			SelectionSnapshotID: "snapshot-1",
			Position:            0,
			MediaAssetID:        "media-asset-document",
			Kind:                "document",
			DisplayName:         "notes.txt",
			StatusAtSelection:   "available",
		}},
	}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return now }),
		WithTargetIDGenerator(sequenceTargetIDs(
			"run-deep-1",
			"step-analysis-1",
			"step-input-document-1",
			"event-deep-1",
		)),
	)

	run, err := service.CreateAnalysisRun(context.Background(), TargetCreateAnalysisRunRequest{
		ChannelAccountID:    "channel-account-1",
		SelectionSnapshotID: "snapshot-1",
		RunType:             "deep_research",
	})
	if err != nil {
		t.Fatalf("CreateAnalysisRun(deep_research) error = %v", err)
	}
	if len(store.analysisRunGraph.Steps) != 1 {
		t.Fatalf("planned steps = %#v, want direct analysis step", store.analysisRunGraph.Steps)
	}
	step := store.analysisRunGraph.Steps[0]
	if step.WorkerKind != "agent_runner" || step.StepKind != "deep_research.analysis" || step.Status != "queued" {
		t.Fatalf("analysis step = %#v, want queued deep research analysis", step)
	}
	if len(store.analysisRunGraph.StepInputs) != 1 ||
		store.analysisRunGraph.StepInputs[0].AnalysisRunStepID != "step-analysis-1" ||
		store.analysisRunGraph.StepInputs[0].SelectionSnapshotItemID != "snapshot-item-document" {
		t.Fatalf("planned inputs = %#v", store.analysisRunGraph.StepInputs)
	}
	if len(run.Steps) != 1 || run.Steps[0].Status != "queued" {
		t.Fatalf("run response steps = %#v", run.Steps)
	}
}

func TestTargetRuntimeServiceMapsCrossChannelRunMissToNotFound(t *testing.T) {
	t.Parallel()

	service := NewTargetRuntimeService(&fakeTargetRuntimeStore{getAnalysisRunErr: sql.ErrNoRows})

	_, err := service.GetAnalysisRun(context.Background(), TargetGetAnalysisRunRequest{
		ChannelAccountID: "other-channel-account",
		AnalysisRunID:    "run-1",
	})
	if !errors.Is(err, storage.ErrAnalysisRunNotFound) {
		t.Fatalf("GetAnalysisRun(cross channel) error = %v, want ErrAnalysisRunNotFound", err)
	}
}

func TestTargetRuntimeServiceRejectsWorkerWritesForUnknownStep(t *testing.T) {
	t.Parallel()

	store := &fakeTargetRuntimeStore{checkStepErr: sql.ErrNoRows}
	service := NewTargetRuntimeService(store)
	ctx := context.Background()

	if err := service.RecordAnalysisRunStepProgress(ctx, "run-1", TargetRecordAnalysisRunStepProgressRequest{
		AnalysisRunStepID: "missing-step",
		ProgressStage:     "running",
	}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
		t.Fatalf("RecordAnalysisRunStepProgress() error = %v, want ErrAnalysisRunNotFound", err)
	}
	if err := service.RecordAnalysisRunArtifacts(ctx, "run-1", TargetRecordAnalysisRunArtifactsRequest{
		AnalysisRunStepID: "missing-step",
		Artifacts: []workerArtifactDescriptor{{
			ArtifactKind: "summary_markdown",
			MIMEType:     "text/markdown",
			ObjectKey:    "run-1/summary/markdown/summary.md",
			SizeBytes:    10,
			Filename:     "summary.md",
		}},
	}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
		t.Fatalf("RecordAnalysisRunArtifacts() error = %v, want ErrAnalysisRunNotFound", err)
	}
	if err := service.RecordAnalysisRunDiagnostics(ctx, "run-1", TargetRecordAnalysisRunDiagnosticsRequest{
		AnalysisRunStepID: "missing-step",
		Diagnostics: []workerDiagnosticDescriptor{{
			DiagnosticID: "diagnostic-1",
			SubjectType:  "analysis_run",
			SubjectID:    "run-1",
			Severity:     "warning",
			Code:         "worker_warning",
			Message:      "worker warning",
		}},
	}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
		t.Fatalf("RecordAnalysisRunDiagnostics() error = %v, want ErrAnalysisRunNotFound", err)
	}
	if _, err := service.FinalizeAnalysisRunStep(ctx, "run-1", TargetFinalizeAnalysisRunStepRequest{
		AnalysisRunStepID: "missing-step",
		Outcome:           "succeeded",
	}); !errors.Is(err, storage.ErrAnalysisRunNotFound) {
		t.Fatalf("FinalizeAnalysisRunStep() error = %v, want ErrAnalysisRunNotFound", err)
	}
	if store.progressCalls != 0 || store.artifactCalls != 0 || store.diagnosticCalls != 0 || store.finalizeCalls != 0 {
		t.Fatalf("worker write reached store after unknown step: progress=%d artifacts=%d diagnostics=%d finalize=%d", store.progressCalls, store.artifactCalls, store.diagnosticCalls, store.finalizeCalls)
	}
}

func TestTargetRuntimeServiceRejectsInvalidWorkerWriteInputs(t *testing.T) {
	t.Parallel()

	service := NewTargetRuntimeService(&fakeTargetRuntimeStore{})
	ctx := context.Background()

	if err := service.RecordAnalysisRunStepProgress(ctx, "run-1", TargetRecordAnalysisRunStepProgressRequest{
		AnalysisRunStepID: "",
		ProgressStage:     "running",
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("RecordAnalysisRunStepProgress(missing step) error = %v, want ErrContractViolation", err)
	}
	if err := service.RecordAnalysisRunArtifacts(ctx, "run-1", TargetRecordAnalysisRunArtifactsRequest{
		AnalysisRunStepID: "step-1",
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("RecordAnalysisRunArtifacts(empty artifacts) error = %v, want ErrContractViolation", err)
	}
	if err := service.RecordAnalysisRunArtifacts(ctx, "run-1", TargetRecordAnalysisRunArtifactsRequest{
		AnalysisRunStepID: "step-1",
		Artifacts: []workerArtifactDescriptor{{
			ArtifactKind: "unsupported_worker_artifact",
			ObjectKey:    "run-1/unsupported.bin",
		}},
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("RecordAnalysisRunArtifacts(unsupported kind) error = %v, want ErrContractViolation", err)
	}
	if err := service.RecordAnalysisRunArtifacts(ctx, "run-1", TargetRecordAnalysisRunArtifactsRequest{
		AnalysisRunStepID: "step-1",
		Artifacts: []workerArtifactDescriptor{{
			ArtifactKind: "summary_markdown",
			ObjectKey:    " ",
		}},
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("RecordAnalysisRunArtifacts(blank object key) error = %v, want ErrContractViolation", err)
	}
	if err := service.RecordAnalysisRunDiagnostics(ctx, "run-1", TargetRecordAnalysisRunDiagnosticsRequest{
		AnalysisRunStepID: "step-1",
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("RecordAnalysisRunDiagnostics(empty diagnostics) error = %v, want ErrContractViolation", err)
	}
	if _, err := service.FinalizeAnalysisRunStep(ctx, "run-1", TargetFinalizeAnalysisRunStepRequest{
		Outcome: "succeeded",
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("FinalizeAnalysisRunStep(missing step) error = %v, want ErrContractViolation", err)
	}
	if _, err := service.FinalizeAnalysisRunStep(ctx, "run-1", TargetFinalizeAnalysisRunStepRequest{
		AnalysisRunStepID: "step-1",
		Outcome:           "unexpected",
	}); !errors.Is(err, storage.ErrContractViolation) {
		t.Fatalf("FinalizeAnalysisRunStep(invalid outcome) error = %v, want ErrContractViolation", err)
	}
}

func TestTargetRuntimeServiceMapsAndPropagatesTargetStoreFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 15, 45, 0, 0, time.UTC)
	boom := errors.New("target store failed")
	newService := func(store *fakeTargetRuntimeStore) *TargetRuntimeService {
		return NewTargetRuntimeService(store,
			WithTargetClock(func() time.Time { return now }),
			WithTargetIDGenerator(sequenceTargetIDs(
				"operation-1",
				"media-asset-1",
				"collection-item-1",
				"collection-1",
				"collection-item-2",
				"snapshot-1",
				"snapshot-item-1",
				"run-1",
				"step-1",
				"step-input-1",
				"event-1",
				"artifact-1",
				"stored-object-1",
				"subject-run-1",
				"subject-step-1",
				"surface-1",
				"surface-event-1",
			)),
		)
	}
	artifactReq := TargetRecordAnalysisRunArtifactsRequest{
		AnalysisRunStepID: "step-1",
		Artifacts: []workerArtifactDescriptor{{
			ArtifactKind: "summary_markdown",
			MIMEType:     "text/markdown",
			ObjectKey:    "run-1/summary.md",
			Filename:     "summary.md",
		}},
	}
	diagnosticsReq := TargetRecordAnalysisRunDiagnosticsRequest{
		AnalysisRunStepID: "step-1",
		Diagnostics: []workerDiagnosticDescriptor{{
			DiagnosticID: "diagnostic-1",
			SubjectType:  "analysis_run",
			SubjectID:    "run-1",
			Severity:     "warning",
			Code:         "worker_warning",
			Message:      "worker warning",
		}},
	}
	testCases := []struct {
		name    string
		store   *fakeTargetRuntimeStore
		run     func(*TargetRuntimeService) error
		wantErr error
	}{
		{
			name:  "resolve account propagates upsert failure",
			store: &fakeTargetRuntimeStore{failMethod: "UpsertChannelAccount", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ResolveChannelAccount(context.Background(), TargetChannelAccountRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "list accounts propagates list failure",
			store: &fakeTargetRuntimeStore{failMethod: "ListChannelAccounts", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ListChannelAccounts(context.Background(), TargetListChannelAccountsRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "update account maps missing row",
			store: &fakeTargetRuntimeStore{failMethod: "UpdateChannelAccount", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.UpdateChannelAccount(context.Background(), TargetUpdateChannelAccountRequest{})
				return err
			},
			wantErr: storage.ErrMediaAssetNotFound,
		},
		{
			name:  "update account propagates generic failure",
			store: &fakeTargetRuntimeStore{failMethod: "UpdateChannelAccount", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.UpdateChannelAccount(context.Background(), TargetUpdateChannelAccountRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "create media asset propagates operation failure",
			store: &fakeTargetRuntimeStore{failMethod: "RecordOperationRequest", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{IdempotencyKey: "stable"})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "create media asset propagates create failure",
			store: &fakeTargetRuntimeStore{failMethod: "CreateMediaAssetWithInbox", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "get media maps missing row",
			store: &fakeTargetRuntimeStore{failMethod: "GetMediaAsset", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.GetMediaAsset(context.Background(), TargetGetMediaAssetRequest{})
				return err
			},
			wantErr: storage.ErrMediaAssetNotFound,
		},
		{
			name:  "get inbox maps missing row",
			store: &fakeTargetRuntimeStore{failMethod: "GetInboxCollection", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.GetInboxCollection(context.Background(), TargetGetInboxCollectionRequest{})
				return err
			},
			wantErr: storage.ErrCollectionNotFound,
		},
		{
			name:  "update collection maps version conflict",
			store: &fakeTargetRuntimeStore{failMethod: "UpdateCollection", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.UpdateCollection(context.Background(), TargetUpdateCollectionRequest{})
				return err
			},
			wantErr: storage.ErrCollectionVersionConflict,
		},
		{
			name:  "remove collection item maps version conflict",
			store: &fakeTargetRuntimeStore{failMethod: "RemoveCollectionItem", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.RemoveCollectionItem(context.Background(), TargetRemoveCollectionItemRequest{})
				return err
			},
			wantErr: storage.ErrCollectionVersionConflict,
		},
		{
			name:  "selection snapshot propagates asset lookup failure",
			store: &fakeTargetRuntimeStore{failMethod: "GetMediaAsset", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CreateSelectionSnapshot(context.Background(), TargetCreateSelectionSnapshotRequest{Items: []TargetSelectionSnapshotItemRequest{{MediaAssetID: "media-asset-1"}}})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "selection snapshot propagates stored object failure",
			store: &fakeTargetRuntimeStore{failMethod: "GetStoredObject", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CreateSelectionSnapshot(context.Background(), TargetCreateSelectionSnapshotRequest{Items: []TargetSelectionSnapshotItemRequest{{MediaAssetID: "media-asset-with-object"}}})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "selection snapshot maps missing row",
			store: &fakeTargetRuntimeStore{failMethod: "GetSelectionSnapshot", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.GetSelectionSnapshot(context.Background(), TargetGetSelectionSnapshotRequest{})
				return err
			},
			wantErr: storage.ErrSelectionSnapshotNotFound,
		},
		{
			name:  "create run propagates snapshot item failure",
			store: &fakeTargetRuntimeStore{failMethod: "ListSelectionSnapshotItems", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CreateAnalysisRun(context.Background(), TargetCreateAnalysisRunRequest{SelectionSnapshotID: "snapshot-1"})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "create run propagates graph failure",
			store: &fakeTargetRuntimeStore{failMethod: "CreateAnalysisRunGraph", failErr: boom, snapshotItems: []targetstore.SelectionSnapshotItemRecord{{ID: "snapshot-item-1"}}},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CreateAnalysisRun(context.Background(), TargetCreateAnalysisRunRequest{SelectionSnapshotID: "snapshot-1"})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "retry run maps missing source run",
			store: &fakeTargetRuntimeStore{failMethod: "GetAnalysisRun", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.RetryAnalysisRun(context.Background(), "run-1", TargetRetryAnalysisRunRequest{})
				return err
			},
			wantErr: storage.ErrAnalysisRunNotFound,
		},
		{
			name:  "claim step maps unclaimed result",
			store: &fakeTargetRuntimeStore{claimUnclaimed: true},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ClaimAnalysisRunStep(context.Background(), "run-1", TargetClaimAnalysisRunStepRequest{})
				return err
			},
			wantErr: storage.ErrAnalysisRunNotFound,
		},
		{
			name:  "claim step propagates run lookup failure",
			store: &fakeTargetRuntimeStore{failMethod: "GetAnalysisRunByID", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ClaimAnalysisRunStep(context.Background(), "run-1", TargetClaimAnalysisRunStepRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "check cancel maps missing step",
			store: &fakeTargetRuntimeStore{failMethod: "CheckAnalysisRunStepCancel", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.CheckAnalysisRunStepCancel(context.Background(), "run-1", TargetCheckAnalysisRunStepCancelRequest{})
				return err
			},
			wantErr: storage.ErrAnalysisRunNotFound,
		},
		{
			name:  "record progress propagates store failure",
			store: &fakeTargetRuntimeStore{failMethod: "RecordAnalysisRunStepProgress", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				return s.RecordAnalysisRunStepProgress(context.Background(), "run-1", TargetRecordAnalysisRunStepProgressRequest{AnalysisRunStepID: "step-1", ProgressStage: "running"})
			},
			wantErr: boom,
		},
		{
			name:  "record artifacts propagates run lookup failure",
			store: &fakeTargetRuntimeStore{failMethod: "GetAnalysisRunByID", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				return s.RecordAnalysisRunArtifacts(context.Background(), "run-1", artifactReq)
			},
			wantErr: boom,
		},
		{
			name:  "record artifacts propagates record failure",
			store: &fakeTargetRuntimeStore{failMethod: "RecordArtifacts", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				return s.RecordAnalysisRunArtifacts(context.Background(), "run-1", artifactReq)
			},
			wantErr: boom,
		},
		{
			name:  "record diagnostics propagates run lookup failure",
			store: &fakeTargetRuntimeStore{failMethod: "GetAnalysisRunByID", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				return s.RecordAnalysisRunDiagnostics(context.Background(), "run-1", diagnosticsReq)
			},
			wantErr: boom,
		},
		{
			name:  "record diagnostics propagates record failure",
			store: &fakeTargetRuntimeStore{failMethod: "RecordDiagnostics", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				return s.RecordAnalysisRunDiagnostics(context.Background(), "run-1", diagnosticsReq)
			},
			wantErr: boom,
		},
		{
			name:  "finalize maps missing run",
			store: &fakeTargetRuntimeStore{failMethod: "FinalizeAnalysisRunStep", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.FinalizeAnalysisRunStep(context.Background(), "run-1", TargetFinalizeAnalysisRunStepRequest{AnalysisRunStepID: "step-1", Outcome: "succeeded"})
				return err
			},
			wantErr: storage.ErrAnalysisRunNotFound,
		},
		{
			name:  "upsert surface propagates subject lookup failure",
			store: &fakeTargetRuntimeStore{failMethod: "ListChannelSurfaceSubjects", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.UpsertChannelSurface(context.Background(), TargetUpsertChannelSurfaceRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "replace surface maps version conflict",
			store: &fakeTargetRuntimeStore{failMethod: "ReplaceChannelSurfaceDisplayState", failErr: sql.ErrNoRows},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ReplaceChannelSurfaceDisplayState(context.Background(), TargetReplaceChannelSurfaceDisplayStateRequest{})
				return err
			},
			wantErr: storage.ErrCollectionVersionConflict,
		},
		{
			name:  "replace surface propagates subject lookup failure",
			store: &fakeTargetRuntimeStore{failMethod: "ListChannelSurfaceSubjects", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ReplaceChannelSurfaceDisplayState(context.Background(), TargetReplaceChannelSurfaceDisplayStateRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "supersede surface propagates store failure",
			store: &fakeTargetRuntimeStore{failMethod: "SupersedeChannelSurface", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.SupersedeChannelSurface(context.Background(), TargetSupersedeChannelSurfaceRequest{})
				return err
			},
			wantErr: boom,
		},
		{
			name:  "list surface events propagates list failure",
			store: &fakeTargetRuntimeStore{failMethod: "ListChannelSurfaceEvents", failErr: boom},
			run: func(s *TargetRuntimeService) error {
				_, err := s.ListChannelSurfaceEvents(context.Background(), TargetListChannelSurfaceEventsRequest{})
				return err
			},
			wantErr: boom,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := tc.run(newService(tc.store)); !errors.Is(err, tc.wantErr) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

func TestTargetRuntimeServicePropagatesAdditionalTargetStoreFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 16, 15, 0, 0, time.UTC)
	boom := errors.New("target store failed")
	newService := func(store *fakeTargetRuntimeStore) *TargetRuntimeService {
		return NewTargetRuntimeService(store,
			WithTargetClock(func() time.Time { return now }),
			WithTargetIDGenerator(sequenceTargetIDs(
				"operation-1",
				"media-asset-1",
				"collection-item-1",
				"collection-1",
				"collection-item-2",
				"snapshot-1",
				"snapshot-item-1",
				"run-1",
				"step-1",
				"step-input-1",
				"event-1",
				"artifact-1",
				"stored-object-1",
				"subject-run-1",
				"subject-step-1",
				"surface-1",
				"surface-event-1",
			)),
		)
	}
	testCases := []struct {
		name    string
		store   *fakeTargetRuntimeStore
		run     func(*TargetRuntimeService) error
		wantErr error
	}{
		{name: "list media assets propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListMediaAssets", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListMediaAssets(context.Background(), TargetListMediaAssetsRequest{})
			return err
		}, wantErr: boom},
		{name: "get media propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "GetMediaAsset", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetMediaAsset(context.Background(), TargetGetMediaAssetRequest{})
			return err
		}, wantErr: boom},
		{name: "delete media propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "DeleteMediaAsset", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.DeleteMediaAsset(context.Background(), TargetDeleteMediaAssetRequest{})
			return err
		}, wantErr: boom},
		{name: "get inbox propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "GetInboxCollection", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetInboxCollection(context.Background(), TargetGetInboxCollectionRequest{})
			return err
		}, wantErr: boom},
		{name: "create collection propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "CreateCollection", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.CreateCollection(context.Background(), TargetCreateCollectionRequest{})
			return err
		}, wantErr: boom},
		{name: "list collections propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListCollections", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListCollections(context.Background(), TargetListCollectionsRequest{})
			return err
		}, wantErr: boom},
		{name: "get collection maps missing row", store: &fakeTargetRuntimeStore{failMethod: "GetCollection", failErr: sql.ErrNoRows}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetCollection(context.Background(), TargetGetCollectionRequest{})
			return err
		}, wantErr: storage.ErrCollectionNotFound},
		{name: "get collection propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "GetCollection", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetCollection(context.Background(), TargetGetCollectionRequest{})
			return err
		}, wantErr: boom},
		{name: "update collection propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "UpdateCollection", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.UpdateCollection(context.Background(), TargetUpdateCollectionRequest{})
			return err
		}, wantErr: boom},
		{name: "update collection items maps version conflict", store: &fakeTargetRuntimeStore{failMethod: "UpdateCollectionItems", failErr: sql.ErrNoRows}, run: func(s *TargetRuntimeService) error {
			_, err := s.UpdateCollectionItems(context.Background(), TargetUpdateCollectionItemsRequest{})
			return err
		}, wantErr: storage.ErrCollectionVersionConflict},
		{name: "update collection items propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "UpdateCollectionItems", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.UpdateCollectionItems(context.Background(), TargetUpdateCollectionItemsRequest{})
			return err
		}, wantErr: boom},
		{name: "remove collection item propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "RemoveCollectionItem", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.RemoveCollectionItem(context.Background(), TargetRemoveCollectionItemRequest{})
			return err
		}, wantErr: boom},
		{name: "create selection propagates create failure", store: &fakeTargetRuntimeStore{failMethod: "CreateSelectionSnapshot", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.CreateSelectionSnapshot(context.Background(), TargetCreateSelectionSnapshotRequest{Items: []TargetSelectionSnapshotItemRequest{{MediaAssetID: "media-asset-1"}}})
			return err
		}, wantErr: boom},
		{name: "get selection propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "GetSelectionSnapshot", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetSelectionSnapshot(context.Background(), TargetGetSelectionSnapshotRequest{})
			return err
		}, wantErr: boom},
		{name: "list analysis runs propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListAnalysisRuns", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListAnalysisRuns(context.Background(), TargetListAnalysisRunsRequest{})
			return err
		}, wantErr: boom},
		{name: "get analysis run propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "GetAnalysisRun", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetAnalysisRun(context.Background(), TargetGetAnalysisRunRequest{})
			return err
		}, wantErr: boom},
		{name: "cancel analysis run propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "RequestAnalysisRunCancel", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.CancelAnalysisRun(context.Background(), "run-1", TargetCancelAnalysisRunRequest{})
			return err
		}, wantErr: boom},
		{name: "retry run propagates generic source failure", store: &fakeTargetRuntimeStore{failMethod: "GetAnalysisRun", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.RetryAnalysisRun(context.Background(), "run-1", TargetRetryAnalysisRunRequest{})
			return err
		}, wantErr: boom},
		{name: "list analysis run events propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListAnalysisRunEvents", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListAnalysisRunEvents(context.Background(), TargetListAnalysisRunEventsRequest{})
			return err
		}, wantErr: boom},
		{name: "list artifacts propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListArtifacts", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListArtifacts(context.Background(), TargetListArtifactsRequest{})
			return err
		}, wantErr: boom},
		{name: "get artifact maps missing row", store: &fakeTargetRuntimeStore{failMethod: "GetArtifact", failErr: sql.ErrNoRows}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetArtifact(context.Background(), TargetGetArtifactRequest{})
			return err
		}, wantErr: storage.ErrArtifactNotFound},
		{name: "get artifact propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "GetArtifact", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.GetArtifact(context.Background(), TargetGetArtifactRequest{})
			return err
		}, wantErr: boom},
		{name: "list diagnostics propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListDiagnostics", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListDiagnostics(context.Background(), TargetListDiagnosticsRequest{})
			return err
		}, wantErr: boom},
		{name: "list step queue propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ListAnalysisRunStepQueue", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListAnalysisRunStepQueue(context.Background(), TargetAnalysisRunStepQueueRequest{})
			return err
		}, wantErr: boom},
		{name: "claim step propagates claim failure", store: &fakeTargetRuntimeStore{failMethod: "ClaimAnalysisRunStep", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ClaimAnalysisRunStep(context.Background(), "run-1", TargetClaimAnalysisRunStepRequest{})
			return err
		}, wantErr: boom},
		{name: "claim step propagates snapshot failure", store: &fakeTargetRuntimeStore{failMethod: "GetSelectionSnapshot", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ClaimAnalysisRunStep(context.Background(), "run-1", TargetClaimAnalysisRunStepRequest{})
			return err
		}, wantErr: boom},
		{name: "check cancel propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "CheckAnalysisRunStepCancel", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.CheckAnalysisRunStepCancel(context.Background(), "run-1", TargetCheckAnalysisRunStepCancelRequest{})
			return err
		}, wantErr: boom},
		{name: "finalize propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "FinalizeAnalysisRunStep", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.FinalizeAnalysisRunStep(context.Background(), "run-1", TargetFinalizeAnalysisRunStepRequest{AnalysisRunStepID: "step-1", Outcome: "succeeded"})
			return err
		}, wantErr: boom},
		{name: "upsert surface propagates upsert failure", store: &fakeTargetRuntimeStore{failMethod: "UpsertChannelSurface", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.UpsertChannelSurface(context.Background(), TargetUpsertChannelSurfaceRequest{})
			return err
		}, wantErr: boom},
		{name: "list surfaces propagates list failure", store: &fakeTargetRuntimeStore{failMethod: "ListChannelSurfaces", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListChannelSurfaces(context.Background(), TargetListChannelSurfacesRequest{})
			return err
		}, wantErr: boom},
		{name: "list surfaces propagates subject failure", store: &fakeTargetRuntimeStore{failMethod: "ListChannelSurfaceSubjects", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ListChannelSurfaces(context.Background(), TargetListChannelSurfacesRequest{})
			return err
		}, wantErr: boom},
		{name: "replace surface propagates generic failure", store: &fakeTargetRuntimeStore{failMethod: "ReplaceChannelSurfaceDisplayState", failErr: boom}, run: func(s *TargetRuntimeService) error {
			_, err := s.ReplaceChannelSurfaceDisplayState(context.Background(), TargetReplaceChannelSurfaceDisplayStateRequest{})
			return err
		}, wantErr: boom},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := tc.run(newService(tc.store)); !errors.Is(err, tc.wantErr) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, tc.wantErr)
			}
		})
	}

	t.Run("reuses orphaned idempotent operation target id when stored asset is missing", func(t *testing.T) {
		t.Parallel()

		store := &fakeTargetRuntimeStore{
			operationsByKey: map[string]targetstore.OperationRequestRecord{
				"channel-account-1\x00media_asset.create\x00stable-key": {
					TargetType: "media_asset",
					TargetID:   "orphaned-media-asset-id",
				},
			},
			failMethod: "GetMediaAsset",
			failErr:    sql.ErrNoRows,
		}
		asset, err := newService(store).CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
			ChannelAccountID: "channel-account-1",
			IdempotencyKey:   "stable-key",
		})
		if err != nil {
			t.Fatalf("CreateMediaAsset(orphaned operation) error = %v", err)
		}
		if asset.MediaAssetID != "orphaned-media-asset-id" {
			t.Fatalf("media asset id = %q, want orphaned-media-asset-id", asset.MediaAssetID)
		}
	})

	t.Run("propagates idempotent operation replay lookup failures", func(t *testing.T) {
		t.Parallel()

		store := &fakeTargetRuntimeStore{
			operationsByKey: map[string]targetstore.OperationRequestRecord{
				"channel-account-1\x00media_asset.create\x00stable-key": {
					TargetType: "media_asset",
					TargetID:   "existing-media-asset-id",
				},
			},
			failMethod: "GetMediaAsset",
			failErr:    boom,
		}
		_, err := newService(store).CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
			ChannelAccountID: "channel-account-1",
			IdempotencyKey:   "stable-key",
		})
		if !errors.Is(err, boom) {
			t.Fatalf("CreateMediaAsset(replay lookup failure) error = %v, want %v", err, boom)
		}
	})

	t.Run("ensure step rejects blank step ids before store access", func(t *testing.T) {
		t.Parallel()

		err := newService(&fakeTargetRuntimeStore{}).ensureAnalysisRunStep(context.Background(), "run-1", " ")
		if !errors.Is(err, storage.ErrContractViolation) {
			t.Fatalf("ensureAnalysisRunStep(blank) error = %v, want ErrContractViolation", err)
		}
	})
}

func TestTargetRuntimeServiceDefaultClockAndIDGeneratorAreUsable(t *testing.T) {
	t.Parallel()

	store := &fakeTargetRuntimeStore{}
	service := NewTargetRuntimeService(store)

	account, err := service.ResolveChannelAccount(context.Background(), TargetChannelAccountRequest{
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Danila",
	})
	if err != nil {
		t.Fatalf("ResolveChannelAccount(default service) error = %v", err)
	}
	if account.ChannelAccountID == "" || account.CreatedAt.IsZero() || account.UpdatedAt.IsZero() || account.LastSeenAt == nil {
		t.Fatalf("default service account = %#v", account)
	}
}

func TestMustJSONFallsBackForUnmarshalableValues(t *testing.T) {
	t.Parallel()

	if got := string(mustJSON(func() {})); got != "{}" {
		t.Fatalf("mustJSON(unmarshalable) = %s, want {}", got)
	}
}

func TestTargetRuntimeServiceRequiresStoreForAllOperations(t *testing.T) {
	t.Parallel()

	service := NewTargetRuntimeService(nil)
	ctx := context.Background()
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "resolve channel account", run: func() error {
			_, err := service.ResolveChannelAccount(ctx, TargetChannelAccountRequest{})
			return err
		}},
		{name: "list channel accounts", run: func() error {
			_, err := service.ListChannelAccounts(ctx, TargetListChannelAccountsRequest{})
			return err
		}},
		{name: "update channel account", run: func() error {
			_, err := service.UpdateChannelAccount(ctx, TargetUpdateChannelAccountRequest{})
			return err
		}},
		{name: "create media asset", run: func() error {
			_, err := service.CreateMediaAsset(ctx, TargetCreateMediaAssetRequest{})
			return err
		}},
		{name: "list media assets", run: func() error {
			_, err := service.ListMediaAssets(ctx, TargetListMediaAssetsRequest{})
			return err
		}},
		{name: "get media asset", run: func() error {
			_, err := service.GetMediaAsset(ctx, TargetGetMediaAssetRequest{})
			return err
		}},
		{name: "delete media asset", run: func() error {
			_, err := service.DeleteMediaAsset(ctx, TargetDeleteMediaAssetRequest{})
			return err
		}},
		{name: "get inbox collection", run: func() error {
			_, err := service.GetInboxCollection(ctx, TargetGetInboxCollectionRequest{})
			return err
		}},
		{name: "create collection", run: func() error {
			_, err := service.CreateCollection(ctx, TargetCreateCollectionRequest{})
			return err
		}},
		{name: "list collections", run: func() error {
			_, err := service.ListCollections(ctx, TargetListCollectionsRequest{})
			return err
		}},
		{name: "get collection", run: func() error {
			_, err := service.GetCollection(ctx, TargetGetCollectionRequest{})
			return err
		}},
		{name: "update collection", run: func() error {
			_, err := service.UpdateCollection(ctx, TargetUpdateCollectionRequest{})
			return err
		}},
		{name: "update collection items", run: func() error {
			_, err := service.UpdateCollectionItems(ctx, TargetUpdateCollectionItemsRequest{})
			return err
		}},
		{name: "remove collection item", run: func() error {
			_, err := service.RemoveCollectionItem(ctx, TargetRemoveCollectionItemRequest{})
			return err
		}},
		{name: "create selection snapshot", run: func() error {
			_, err := service.CreateSelectionSnapshot(ctx, TargetCreateSelectionSnapshotRequest{})
			return err
		}},
		{name: "get selection snapshot", run: func() error {
			_, err := service.GetSelectionSnapshot(ctx, TargetGetSelectionSnapshotRequest{})
			return err
		}},
		{name: "create analysis run", run: func() error {
			_, err := service.CreateAnalysisRun(ctx, TargetCreateAnalysisRunRequest{})
			return err
		}},
		{name: "list analysis runs", run: func() error {
			_, err := service.ListAnalysisRuns(ctx, TargetListAnalysisRunsRequest{})
			return err
		}},
		{name: "get analysis run", run: func() error {
			_, err := service.GetAnalysisRun(ctx, TargetGetAnalysisRunRequest{})
			return err
		}},
		{name: "cancel analysis run", run: func() error {
			_, err := service.CancelAnalysisRun(ctx, "run-1", TargetCancelAnalysisRunRequest{})
			return err
		}},
		{name: "retry analysis run", run: func() error {
			_, err := service.RetryAnalysisRun(ctx, "run-1", TargetRetryAnalysisRunRequest{})
			return err
		}},
		{name: "list analysis run events", run: func() error {
			_, err := service.ListAnalysisRunEvents(ctx, TargetListAnalysisRunEventsRequest{})
			return err
		}},
		{name: "list artifacts", run: func() error {
			_, err := service.ListArtifacts(ctx, TargetListArtifactsRequest{})
			return err
		}},
		{name: "get artifact", run: func() error {
			_, err := service.GetArtifact(ctx, TargetGetArtifactRequest{})
			return err
		}},
		{name: "list diagnostics", run: func() error {
			_, err := service.ListDiagnostics(ctx, TargetListDiagnosticsRequest{})
			return err
		}},
		{name: "list step queue", run: func() error {
			_, err := service.ListAnalysisRunStepQueue(ctx, TargetAnalysisRunStepQueueRequest{})
			return err
		}},
		{name: "claim step", run: func() error {
			_, err := service.ClaimAnalysisRunStep(ctx, "run-1", TargetClaimAnalysisRunStepRequest{})
			return err
		}},
		{name: "check step cancel", run: func() error {
			_, err := service.CheckAnalysisRunStepCancel(ctx, "run-1", TargetCheckAnalysisRunStepCancelRequest{})
			return err
		}},
		{name: "record progress", run: func() error {
			return service.RecordAnalysisRunStepProgress(ctx, "run-1", TargetRecordAnalysisRunStepProgressRequest{})
		}},
		{name: "record artifacts", run: func() error {
			return service.RecordAnalysisRunArtifacts(ctx, "run-1", TargetRecordAnalysisRunArtifactsRequest{})
		}},
		{name: "record diagnostics", run: func() error {
			return service.RecordAnalysisRunDiagnostics(ctx, "run-1", TargetRecordAnalysisRunDiagnosticsRequest{})
		}},
		{name: "finalize step", run: func() error {
			_, err := service.FinalizeAnalysisRunStep(ctx, "run-1", TargetFinalizeAnalysisRunStepRequest{})
			return err
		}},
		{name: "upsert surface", run: func() error {
			_, err := service.UpsertChannelSurface(ctx, TargetUpsertChannelSurfaceRequest{})
			return err
		}},
		{name: "list surfaces", run: func() error {
			_, err := service.ListChannelSurfaces(ctx, TargetListChannelSurfacesRequest{})
			return err
		}},
		{name: "replace display state", run: func() error {
			_, err := service.ReplaceChannelSurfaceDisplayState(ctx, TargetReplaceChannelSurfaceDisplayStateRequest{})
			return err
		}},
		{name: "supersede surface", run: func() error {
			_, err := service.SupersedeChannelSurface(ctx, TargetSupersedeChannelSurfaceRequest{})
			return err
		}},
		{name: "list surface events", run: func() error {
			_, err := service.ListChannelSurfaceEvents(ctx, TargetListChannelSurfaceEventsRequest{})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); err == nil {
				t.Fatalf("%s returned nil error without target store", tc.name)
			}
		})
	}
}

type fakeTargetRuntimeStore struct {
	channelAccount         targetstore.ChannelAccountRecord
	operation              targetstore.OperationRequestRecord
	operationsByKey        map[string]targetstore.OperationRequestRecord
	mediaAssetParams       targetstore.CreateMediaAssetWithInboxParams
	mediaAssetCreateCalls  int
	selectionSnapshot      targetstore.SelectionSnapshotRecord
	selectionSnapshotItems []targetstore.SelectionSnapshotItemRecord
	snapshotItems          []targetstore.SelectionSnapshotItemRecord
	analysisRunGraph       targetstore.AnalysisRunGraph
	getAnalysisRunErr      error
	checkStepErr           error
	progressCalls          int
	progress               targetstore.RecordAnalysisRunProgressParams
	artifactCalls          int
	storedObjects          []targetstore.StoredObjectRecord
	artifacts              []targetstore.ArtifactRecord
	reusableTranscriptReq  TargetReusableTranscriptRequest
	reusableRun            targetstore.AnalysisRunRecord
	reusableArtifact       targetstore.ArtifactRecord
	artifactSubjects       []targetstore.ArtifactSubjectRecord
	diagnosticCalls        int
	diagnostics            []targetstore.DiagnosticRecord
	finalizeCalls          int
	surface                targetstore.ChannelSurfaceRecord
	surfaceSubjects        []targetstore.ChannelSurfaceSubjectRecord
	supersede              targetstore.SupersedeChannelSurfaceParams
	failMethod             string
	failErr                error
	claimUnclaimed         bool
}

func (s *fakeTargetRuntimeStore) fail(method string) error {
	if s.failMethod != method {
		return nil
	}
	if s.failErr != nil {
		return s.failErr
	}
	return errors.New("forced target store failure")
}

func (s *fakeTargetRuntimeStore) UpsertChannelAccount(_ context.Context, record targetstore.ChannelAccountRecord) (targetstore.ChannelAccountRecord, error) {
	if err := s.fail("UpsertChannelAccount"); err != nil {
		return targetstore.ChannelAccountRecord{}, err
	}
	if s.channelAccount.ID != "" &&
		s.channelAccount.Channel == record.Channel &&
		s.channelAccount.ExternalAccountRef == record.ExternalAccountRef {
		s.channelAccount.DisplayName = record.DisplayName
		s.channelAccount.Status = record.Status
		s.channelAccount.MetadataJSON = record.MetadataJSON
		s.channelAccount.UpdatedAt = record.UpdatedAt
		s.channelAccount.LastSeenAt = record.LastSeenAt
		s.channelAccount.DisabledAt = record.DisabledAt
		return s.channelAccount, nil
	}
	s.channelAccount = record
	return record, nil
}

func (s *fakeTargetRuntimeStore) ListChannelAccounts(_ context.Context, _ int) ([]targetstore.ChannelAccountRecord, error) {
	if err := s.fail("ListChannelAccounts"); err != nil {
		return nil, err
	}
	return []targetstore.ChannelAccountRecord{s.channelAccount}, nil
}

func (s *fakeTargetRuntimeStore) UpdateChannelAccount(_ context.Context, params targetstore.UpdateChannelAccountParams) (targetstore.ChannelAccountRecord, error) {
	if err := s.fail("UpdateChannelAccount"); err != nil {
		return targetstore.ChannelAccountRecord{}, err
	}
	s.channelAccount.ID = params.ID
	s.channelAccount.DisplayName = params.DisplayName
	s.channelAccount.Status = params.Status
	s.channelAccount.UpdatedAt = params.UpdatedAt
	return s.channelAccount, nil
}

func (s *fakeTargetRuntimeStore) RecordOperationRequest(_ context.Context, record targetstore.OperationRequestRecord) (targetstore.OperationRequestRecord, error) {
	if err := s.fail("RecordOperationRequest"); err != nil {
		return targetstore.OperationRequestRecord{}, err
	}
	if s.operationsByKey != nil {
		key := record.ChannelAccountID + "\x00" + record.OperationType + "\x00" + record.IdempotencyKey
		if existing, ok := s.operationsByKey[key]; ok {
			s.operation = existing
			return existing, nil
		}
		s.operationsByKey[key] = record
	}
	s.operation = record
	return record, nil
}

func (s *fakeTargetRuntimeStore) CreateMediaAssetWithInbox(_ context.Context, params targetstore.CreateMediaAssetWithInboxParams) error {
	if err := s.fail("CreateMediaAssetWithInbox"); err != nil {
		return err
	}
	s.mediaAssetCreateCalls++
	s.mediaAssetParams = params
	return nil
}

func (s *fakeTargetRuntimeStore) ListMediaAssets(_ context.Context, channelAccountID string, limit int) ([]targetstore.MediaAssetRecord, error) {
	if err := s.fail("ListMediaAssets"); err != nil {
		return nil, err
	}
	return []targetstore.MediaAssetRecord{{
		ID:               "media-asset-1",
		ChannelAccountID: channelAccountID,
		OriginType:       "telegram_file",
		OriginRef:        "file-id",
		Kind:             "voice",
		DisplayName:      "voice.ogg",
		Status:           "available",
		MetadataJSON:     []byte(`{}`),
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) GetMediaAsset(_ context.Context, channelAccountID, mediaAssetID string) (targetstore.MediaAssetRecord, error) {
	if err := s.fail("GetMediaAsset"); err != nil {
		return targetstore.MediaAssetRecord{}, err
	}
	storedObjectID := ""
	if mediaAssetID == "media-asset-with-object" {
		storedObjectID = "stored-object-1"
	}
	return targetstore.MediaAssetRecord{
		ID:               mediaAssetID,
		ChannelAccountID: channelAccountID,
		StoredObjectID:   storedObjectID,
		OriginType:       "telegram_file",
		OriginRef:        "file-id",
		Kind:             "voice",
		DisplayName:      "voice.ogg",
		Status:           "available",
		MetadataJSON:     []byte(`{}`),
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (s *fakeTargetRuntimeStore) GetStoredObject(_ context.Context, storedObjectID string) (targetstore.StoredObjectRecord, error) {
	if err := s.fail("GetStoredObject"); err != nil {
		return targetstore.StoredObjectRecord{}, err
	}
	return targetstore.StoredObjectRecord{
		ID:             storedObjectID,
		Bucket:         "sources",
		ObjectKey:      "file-id",
		ContentType:    "audio/ogg",
		SizeBytes:      42,
		StorageStatus:  "available",
		RetentionState: "active",
		CreatedAt:      time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (s *fakeTargetRuntimeStore) DeleteMediaAsset(_ context.Context, channelAccountID, mediaAssetID string, deletedAt time.Time) (targetstore.MediaAssetRecord, error) {
	if err := s.fail("DeleteMediaAsset"); err != nil {
		return targetstore.MediaAssetRecord{}, err
	}
	return targetstore.MediaAssetRecord{
		ID:               mediaAssetID,
		ChannelAccountID: channelAccountID,
		OriginType:       "telegram_file",
		OriginRef:        "file-id",
		Kind:             "voice",
		DisplayName:      "voice.ogg",
		Status:           "deleted",
		MetadataJSON:     []byte(`{}`),
		CreatedAt:        deletedAt,
		UpdatedAt:        deletedAt,
		DeletedAt:        &deletedAt,
	}, nil
}

func (s *fakeTargetRuntimeStore) GetInboxCollection(_ context.Context, channelAccountID string) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error) {
	if err := s.fail("GetInboxCollection"); err != nil {
		return targetstore.CollectionRecord{}, nil, err
	}
	return targetstore.CollectionRecord{
		ID:               "inbox-1",
		ChannelAccountID: channelAccountID,
		Kind:             "inbox",
		Name:             "Inbox",
		Status:           "active",
		Version:          1,
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, []targetstore.CollectionItemRecord{}, nil
}

func (s *fakeTargetRuntimeStore) CreateCollection(_ context.Context, collection targetstore.CollectionRecord, items []targetstore.CollectionItemRecord) error {
	if err := s.fail("CreateCollection"); err != nil {
		return err
	}
	return nil
}

func (s *fakeTargetRuntimeStore) ListCollections(_ context.Context, channelAccountID string, _ int) ([]targetstore.CollectionRecord, error) {
	if err := s.fail("ListCollections"); err != nil {
		return nil, err
	}
	return []targetstore.CollectionRecord{{
		ID:               "collection-1",
		ChannelAccountID: channelAccountID,
		Kind:             "user",
		Name:             "Research",
		Status:           "active",
		Version:          1,
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) GetCollection(_ context.Context, channelAccountID, collectionID string) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error) {
	if err := s.fail("GetCollection"); err != nil {
		return targetstore.CollectionRecord{}, nil, err
	}
	return targetstore.CollectionRecord{
		ID:               collectionID,
		ChannelAccountID: channelAccountID,
		Kind:             "user",
		Name:             "Research",
		Status:           "active",
		Version:          1,
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, []targetstore.CollectionItemRecord{}, nil
}

func (s *fakeTargetRuntimeStore) UpdateCollection(_ context.Context, params targetstore.UpdateCollectionParams) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error) {
	if err := s.fail("UpdateCollection"); err != nil {
		return targetstore.CollectionRecord{}, nil, err
	}
	return targetstore.CollectionRecord{
		ID:               params.CollectionID,
		ChannelAccountID: params.ChannelAccountID,
		Kind:             "user",
		Name:             params.Name,
		Status:           "active",
		Version:          params.ExpectedVersion + 1,
		CreatedAt:        params.UpdatedAt,
		UpdatedAt:        params.UpdatedAt,
	}, []targetstore.CollectionItemRecord{}, nil
}

func (s *fakeTargetRuntimeStore) UpdateCollectionItems(_ context.Context, params targetstore.UpdateCollectionItemsParams) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error) {
	if err := s.fail("UpdateCollectionItems"); err != nil {
		return targetstore.CollectionRecord{}, nil, err
	}
	return targetstore.CollectionRecord{
		ID:               params.CollectionID,
		ChannelAccountID: params.ChannelAccountID,
		Kind:             "user",
		Name:             "Research",
		Status:           "active",
		Version:          params.ExpectedVersion + 1,
		CreatedAt:        params.UpdatedAt,
		UpdatedAt:        params.UpdatedAt,
	}, params.Items, nil
}

func (s *fakeTargetRuntimeStore) RemoveCollectionItem(_ context.Context, params targetstore.RemoveCollectionItemParams) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error) {
	if err := s.fail("RemoveCollectionItem"); err != nil {
		return targetstore.CollectionRecord{}, nil, err
	}
	return targetstore.CollectionRecord{
		ID:               params.CollectionID,
		ChannelAccountID: params.ChannelAccountID,
		Kind:             "user",
		Name:             "Research",
		Status:           "active",
		Version:          params.ExpectedVersion + 1,
		CreatedAt:        params.RemovedAt,
		UpdatedAt:        params.RemovedAt,
	}, []targetstore.CollectionItemRecord{}, nil
}

func (s *fakeTargetRuntimeStore) CreateSelectionSnapshot(_ context.Context, snapshot targetstore.SelectionSnapshotRecord, items []targetstore.SelectionSnapshotItemRecord) error {
	if err := s.fail("CreateSelectionSnapshot"); err != nil {
		return err
	}
	s.selectionSnapshot = snapshot
	s.selectionSnapshotItems = append([]targetstore.SelectionSnapshotItemRecord(nil), items...)
	return nil
}

func (s *fakeTargetRuntimeStore) GetSelectionSnapshot(_ context.Context, channelAccountID, selectionSnapshotID string) (targetstore.SelectionSnapshotRecord, []targetstore.SelectionSnapshotItemRecord, error) {
	if err := s.fail("GetSelectionSnapshot"); err != nil {
		return targetstore.SelectionSnapshotRecord{}, nil, err
	}
	return targetstore.SelectionSnapshotRecord{
		ID:                 selectionSnapshotID,
		ChannelAccountID:   channelAccountID,
		SourceCollectionID: "inbox-1",
		Status:             "sealed",
		OptionSnapshotJSON: []byte(`{}`),
		DiagnosticsJSON:    []byte(`[]`),
		CreatedAt:          time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		SealedAt:           time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, append([]targetstore.SelectionSnapshotItemRecord(nil), s.snapshotItems...), nil
}

func (s *fakeTargetRuntimeStore) ListSelectionSnapshotItems(_ context.Context, selectionSnapshotID string) ([]targetstore.SelectionSnapshotItemRecord, error) {
	if err := s.fail("ListSelectionSnapshotItems"); err != nil {
		return nil, err
	}
	if selectionSnapshotID != "snapshot-1" {
		return nil, nil
	}
	return append([]targetstore.SelectionSnapshotItemRecord(nil), s.snapshotItems...), nil
}

func (s *fakeTargetRuntimeStore) CreateAnalysisRunGraph(_ context.Context, graph targetstore.AnalysisRunGraph) error {
	if err := s.fail("CreateAnalysisRunGraph"); err != nil {
		return err
	}
	s.analysisRunGraph = graph
	return nil
}

func (s *fakeTargetRuntimeStore) ListAnalysisRuns(_ context.Context, channelAccountID string, limit int) ([]targetstore.AnalysisRunRecord, error) {
	if err := s.fail("ListAnalysisRuns"); err != nil {
		return nil, err
	}
	return []targetstore.AnalysisRunRecord{{
		ID:                "run-1",
		ChannelAccountID:  channelAccountID,
		SelectionSnapshot: "snapshot-1",
		RunType:           "transcription",
		Status:            "queued",
		Version:           1,
		ParamsJSON:        []byte(`{}`),
		DeliveryJSON:      []byte(`{"strategy":"polling"}`),
		EvidenceGateState: "not_required",
		CreatedAt:         time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) GetAnalysisRun(_ context.Context, channelAccountID, analysisRunID string) (targetstore.AnalysisRunRecord, error) {
	if err := s.fail("GetAnalysisRun"); err != nil {
		return targetstore.AnalysisRunRecord{}, err
	}
	if s.getAnalysisRunErr != nil {
		return targetstore.AnalysisRunRecord{}, s.getAnalysisRunErr
	}
	return targetstore.AnalysisRunRecord{
		ID:                analysisRunID,
		ChannelAccountID:  channelAccountID,
		SelectionSnapshot: "snapshot-1",
		RunType:           "transcription",
		Status:            "queued",
		Version:           1,
		ParamsJSON:        []byte(`{}`),
		DeliveryJSON:      []byte(`{"strategy":"polling"}`),
		EvidenceGateState: "not_required",
		CreatedAt:         time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (s *fakeTargetRuntimeStore) GetAnalysisRunByID(_ context.Context, analysisRunID string) (targetstore.AnalysisRunRecord, error) {
	if err := s.fail("GetAnalysisRunByID"); err != nil {
		return targetstore.AnalysisRunRecord{}, err
	}
	return targetstore.AnalysisRunRecord{
		ID:                analysisRunID,
		ChannelAccountID:  "channel-account-1",
		SelectionSnapshot: "snapshot-1",
		RunType:           "transcription",
		Status:            "queued",
		Version:           1,
		ParamsJSON:        []byte(`{"language":"ru"}`),
		DeliveryJSON:      []byte(`{"strategy":"polling"}`),
		EvidenceGateState: "not_required",
		CreatedAt:         time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (s *fakeTargetRuntimeStore) ListAnalysisRunStepQueue(_ context.Context, status, runType, workerKind, stepKind string, limit int) ([]targetstore.AnalysisRunStepQueueRecord, error) {
	if err := s.fail("ListAnalysisRunStepQueue"); err != nil {
		return nil, err
	}
	return []targetstore.AnalysisRunStepQueueRecord{{
		AnalysisRunID:     "run-1",
		RunType:           firstNonEmpty(runType, "transcription"),
		WorkerKind:        firstNonEmpty(workerKind, "transcription"),
		StepKind:          firstNonEmpty(stepKind, "selection.transcription"),
		Status:            firstNonEmpty(status, "queued"),
		Version:           1,
		AttemptNo:         1,
		AnalysisRunStepID: "step-1",
		CreatedAt:         time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) RequestAnalysisRunCancel(_ context.Context, channelAccountID, analysisRunID string, _ targetstore.AnalysisRunEventRecord, requestedAt time.Time) (targetstore.AnalysisRunRecord, error) {
	if err := s.fail("RequestAnalysisRunCancel"); err != nil {
		return targetstore.AnalysisRunRecord{}, err
	}
	return targetstore.AnalysisRunRecord{
		ID:                analysisRunID,
		ChannelAccountID:  channelAccountID,
		SelectionSnapshot: "snapshot-1",
		RunType:           "transcription",
		Status:            "cancel_requested",
		Version:           2,
		ParamsJSON:        []byte(`{}`),
		DeliveryJSON:      []byte(`{"strategy":"polling"}`),
		EvidenceGateState: "not_required",
		CreatedAt:         requestedAt,
		CancelRequestedAt: &requestedAt,
	}, nil
}

func (s *fakeTargetRuntimeStore) ListAnalysisRunEvents(_ context.Context, _ string, analysisRunID string, _ int) ([]targetstore.AnalysisRunEventRecord, error) {
	if err := s.fail("ListAnalysisRunEvents"); err != nil {
		return nil, err
	}
	return []targetstore.AnalysisRunEventRecord{{
		ID:            "event-1",
		AnalysisRunID: analysisRunID,
		EventType:     "analysis_run.created",
		Version:       1,
		Status:        "queued",
		PayloadJSON:   []byte(`{}`),
		CreatedAt:     time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) ListArtifacts(_ context.Context, channelAccountID, analysisRunID string, limit int) ([]targetstore.ArtifactRecord, error) {
	if err := s.fail("ListArtifacts"); err != nil {
		return nil, err
	}
	return []targetstore.ArtifactRecord{{
		ID:               "artifact-1",
		ChannelAccountID: channelAccountID,
		AnalysisRunID:    analysisRunID,
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Visibility:       "channel_deliverable",
		PreviewJSON:      []byte(`{"available":true}`),
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) FindReusableTranscriptBySource(_ context.Context, channelAccountID, storedObjectID, checksum string) (targetstore.AnalysisRunRecord, targetstore.ArtifactRecord, error) {
	if err := s.fail("FindReusableTranscriptBySource"); err != nil {
		return targetstore.AnalysisRunRecord{}, targetstore.ArtifactRecord{}, err
	}
	s.reusableTranscriptReq = TargetReusableTranscriptRequest{
		ChannelAccountID: channelAccountID,
		StoredObjectID:   storedObjectID,
		Checksum:         checksum,
	}
	if s.reusableRun.ID == "" {
		return targetstore.AnalysisRunRecord{}, targetstore.ArtifactRecord{}, sql.ErrNoRows
	}
	return s.reusableRun, s.reusableArtifact, nil
}

func (s *fakeTargetRuntimeStore) GetArtifact(_ context.Context, channelAccountID, artifactID string) (targetstore.ArtifactRecord, error) {
	if err := s.fail("GetArtifact"); err != nil {
		return targetstore.ArtifactRecord{}, err
	}
	return targetstore.ArtifactRecord{
		ID:               artifactID,
		ChannelAccountID: channelAccountID,
		AnalysisRunID:    "run-1",
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Visibility:       "channel_deliverable",
		PreviewJSON:      []byte(`{"available":true}`),
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (s *fakeTargetRuntimeStore) GetArtifactByID(_ context.Context, artifactID string) (targetstore.ArtifactRecord, error) {
	if err := s.fail("GetArtifactByID"); err != nil {
		return targetstore.ArtifactRecord{}, err
	}
	return targetstore.ArtifactRecord{
		ID:               artifactID,
		ChannelAccountID: "channel-account-1",
		AnalysisRunID:    "run-1",
		StoredObjectID:   "stored-object-1",
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		SizeBytes:        42,
		Visibility:       "channel_deliverable",
		PreviewJSON:      []byte(`{"available":true,"filename":"transcript.txt","worker_artifact_kind":"transcript_plain"}`),
		CreatedAt:        time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (s *fakeTargetRuntimeStore) ListDiagnostics(_ context.Context, query targetstore.DiagnosticQuery, limit int) ([]targetstore.DiagnosticRecord, error) {
	if err := s.fail("ListDiagnostics"); err != nil {
		return nil, err
	}
	return []targetstore.DiagnosticRecord{{
		ID:                 "diagnostic-1",
		ChannelAccountID:   query.ChannelAccountID,
		SubjectType:        query.SubjectType,
		SubjectID:          query.SubjectID,
		Severity:           "warning",
		Code:               "analysis_prerequisite_missing",
		Message:            "Transcript is missing",
		ContextJSON:        []byte(`{}`),
		SafeChannelContext: []byte(`{}`),
		CreatedAt:          time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) ClaimAnalysisRunStep(_ context.Context, analysisRunID, workerKind, stepKind, leaseOwner string, claimedAt time.Time) (targetstore.AnalysisRunStepRecord, []targetstore.AnalysisRunStepInputRecord, bool, error) {
	if err := s.fail("ClaimAnalysisRunStep"); err != nil {
		return targetstore.AnalysisRunStepRecord{}, nil, false, err
	}
	if s.claimUnclaimed {
		return targetstore.AnalysisRunStepRecord{}, nil, false, nil
	}
	return targetstore.AnalysisRunStepRecord{
			ID:            "step-1",
			AnalysisRunID: analysisRunID,
			StepKind:      stepKind,
			WorkerKind:    workerKind,
			Status:        "claimed",
			AttemptNo:     1,
			LeaseOwner:    leaseOwner,
			ClaimedAt:     &claimedAt,
			CreatedAt:     claimedAt,
		}, []targetstore.AnalysisRunStepInputRecord{{
			ID:                      "step-input-1",
			AnalysisRunStepID:       "step-1",
			InputKind:               "selection_snapshot_item",
			SelectionSnapshotItemID: "snapshot-item-1",
			Position:                0,
			Required:                true,
		}}, true, nil
}

func (s *fakeTargetRuntimeStore) CheckAnalysisRunStepCancel(_ context.Context, analysisRunID, analysisRunStepID string) (targetstore.AnalysisRunRecord, targetstore.AnalysisRunStepRecord, error) {
	if err := s.fail("CheckAnalysisRunStepCancel"); err != nil {
		return targetstore.AnalysisRunRecord{}, targetstore.AnalysisRunStepRecord{}, err
	}
	if s.checkStepErr != nil {
		return targetstore.AnalysisRunRecord{}, targetstore.AnalysisRunStepRecord{}, s.checkStepErr
	}
	now := time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC)
	return targetstore.AnalysisRunRecord{
			ID:                analysisRunID,
			ChannelAccountID:  "channel-account-1",
			SelectionSnapshot: "snapshot-1",
			RunType:           "transcription",
			Status:            "running",
			Version:           1,
			ParamsJSON:        []byte(`{}`),
			DeliveryJSON:      []byte(`{"strategy":"polling"}`),
			EvidenceGateState: "not_required",
			CreatedAt:         now,
		}, targetstore.AnalysisRunStepRecord{
			ID:            analysisRunStepID,
			AnalysisRunID: analysisRunID,
			StepKind:      "selection.transcription",
			WorkerKind:    "transcription",
			Status:        "claimed",
			AttemptNo:     1,
			CreatedAt:     now,
		}, nil
}

func (s *fakeTargetRuntimeStore) RecordAnalysisRunStepProgress(_ context.Context, params targetstore.RecordAnalysisRunProgressParams) error {
	if err := s.fail("RecordAnalysisRunStepProgress"); err != nil {
		return err
	}
	s.progressCalls++
	s.progress = params
	return nil
}

func (s *fakeTargetRuntimeStore) RecordArtifacts(_ context.Context, storedObjects []targetstore.StoredObjectRecord, artifacts []targetstore.ArtifactRecord, subjects []targetstore.ArtifactSubjectRecord) error {
	if err := s.fail("RecordArtifacts"); err != nil {
		return err
	}
	s.artifactCalls++
	s.storedObjects = append([]targetstore.StoredObjectRecord(nil), storedObjects...)
	s.artifacts = append([]targetstore.ArtifactRecord(nil), artifacts...)
	s.artifactSubjects = append([]targetstore.ArtifactSubjectRecord(nil), subjects...)
	return nil
}

func (s *fakeTargetRuntimeStore) RecordDiagnostics(_ context.Context, diagnostics []targetstore.DiagnosticRecord) error {
	if err := s.fail("RecordDiagnostics"); err != nil {
		return err
	}
	s.diagnosticCalls++
	s.diagnostics = append([]targetstore.DiagnosticRecord(nil), diagnostics...)
	return nil
}

func (s *fakeTargetRuntimeStore) FinalizeAnalysisRunStep(_ context.Context, params targetstore.FinalizeAnalysisRunStepParams) (targetstore.AnalysisRunRecord, error) {
	if err := s.fail("FinalizeAnalysisRunStep"); err != nil {
		return targetstore.AnalysisRunRecord{}, err
	}
	s.finalizeCalls++
	return targetstore.AnalysisRunRecord{
		ID:                params.AnalysisRunID,
		ChannelAccountID:  "channel-account-1",
		SelectionSnapshot: "snapshot-1",
		RunType:           "transcription",
		Status:            params.RunStatus,
		Version:           2,
		ParamsJSON:        []byte(`{}`),
		DeliveryJSON:      []byte(`{"strategy":"polling"}`),
		EvidenceGateState: "not_required",
		CreatedAt:         params.FinalizedAt,
		CompletedAt:       &params.FinalizedAt,
	}, nil
}

func (s *fakeTargetRuntimeStore) UpsertChannelSurface(_ context.Context, record targetstore.ChannelSurfaceRecord, subjects []targetstore.ChannelSurfaceSubjectRecord) (targetstore.ChannelSurfaceRecord, error) {
	if err := s.fail("UpsertChannelSurface"); err != nil {
		return targetstore.ChannelSurfaceRecord{}, err
	}
	s.surface = record
	s.surfaceSubjects = append([]targetstore.ChannelSurfaceSubjectRecord(nil), subjects...)
	return record, nil
}

func (s *fakeTargetRuntimeStore) ListChannelSurfaces(_ context.Context, query targetstore.ChannelSurfaceQuery, _ int) ([]targetstore.ChannelSurfaceRecord, error) {
	if err := s.fail("ListChannelSurfaces"); err != nil {
		return nil, err
	}
	return []targetstore.ChannelSurfaceRecord{{
		ID:                 "surface-1",
		ChannelAccountID:   query.ChannelAccountID,
		Channel:            "telegram",
		SurfaceType:        "message",
		SurfaceKey:         "run:run-1",
		DisplayStateJSON:   []byte(`{"status":"queued"}`),
		LifecycleStatus:    "active",
		Version:            1,
		AddressFingerprint: "telegram:chat-1:42",
		CreatedAt:          time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
		UpdatedAt:          time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) ListChannelSurfaceSubjects(_ context.Context, surfaceID string) ([]targetstore.ChannelSurfaceSubjectRecord, error) {
	if err := s.fail("ListChannelSurfaceSubjects"); err != nil {
		return nil, err
	}
	return []targetstore.ChannelSurfaceSubjectRecord{{
		SurfaceID:   surfaceID,
		SubjectType: "analysis_run",
		SubjectID:   "run-1",
		SubjectRole: "primary",
		CreatedAt:   time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) ReplaceChannelSurfaceDisplayState(_ context.Context, params targetstore.ReplaceChannelSurfaceDisplayStateParams) (targetstore.ChannelSurfaceRecord, error) {
	if err := s.fail("ReplaceChannelSurfaceDisplayState"); err != nil {
		return targetstore.ChannelSurfaceRecord{}, err
	}
	return targetstore.ChannelSurfaceRecord{
		ID:                 params.SurfaceID,
		ChannelAccountID:   "channel-account-1",
		Channel:            "telegram",
		SurfaceType:        "message",
		SurfaceKey:         "run:run-1",
		DisplayStateJSON:   params.DisplayStateJSON,
		LifecycleStatus:    "active",
		Version:            params.ExpectedVersion + 1,
		AddressFingerprint: "telegram:chat-1:42",
		CreatedAt:          params.UpdatedAt,
		UpdatedAt:          params.UpdatedAt,
		LastRenderedAt:     &params.UpdatedAt,
	}, nil
}

func (s *fakeTargetRuntimeStore) SupersedeChannelSurface(_ context.Context, params targetstore.SupersedeChannelSurfaceParams) error {
	if err := s.fail("SupersedeChannelSurface"); err != nil {
		return err
	}
	s.supersede = params
	return nil
}

func (s *fakeTargetRuntimeStore) ListChannelSurfaceEvents(_ context.Context, surfaceID string, _ int) ([]targetstore.ChannelSurfaceEventRecord, error) {
	if err := s.fail("ListChannelSurfaceEvents"); err != nil {
		return nil, err
	}
	return []targetstore.ChannelSurfaceEventRecord{{
		ID:        "surface-event-1",
		SurfaceID: surfaceID,
		EventType: "channel_surface.superseded",
		Reason:    "message_not_editable",
		ActorType: "telegram_adapter",
		CreatedAt: time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func sequenceTargetIDs(ids ...string) func() string {
	next := 0
	return func() string {
		if next >= len(ids) {
			return "extra-target-id"
		}
		id := ids[next]
		next++
		return id
	}
}

type fakeTargetObjectStore struct {
	puts []fakeTargetObjectPut
	err  error
}

type fakeTargetObjectPut struct {
	bucket      string
	objectKey   string
	contentType string
	body        []byte
}

func (f *fakeTargetObjectStore) PutObject(_ context.Context, bucket, objectKey, contentType string, body []byte) error {
	if f.err != nil {
		return f.err
	}
	f.puts = append(f.puts, fakeTargetObjectPut{
		bucket:      bucket,
		objectKey:   objectKey,
		contentType: contentType,
		body:        append([]byte(nil), body...),
	})
	return nil
}

func (f *fakeTargetObjectStore) PresignGetObject(_ context.Context, bucket, objectKey string, _ time.Duration) (string, time.Time, error) {
	return "http://object-store/" + bucket + "/" + objectKey, time.Time{}, nil
}
