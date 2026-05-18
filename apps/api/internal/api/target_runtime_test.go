package api

import (
	"context"
	"testing"
	"time"

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

type fakeTargetRuntimeStore struct {
	channelAccount         targetstore.ChannelAccountRecord
	operation              targetstore.OperationRequestRecord
	mediaAssetParams       targetstore.CreateMediaAssetWithInboxParams
	selectionSnapshot      targetstore.SelectionSnapshotRecord
	selectionSnapshotItems []targetstore.SelectionSnapshotItemRecord
	snapshotItems          []targetstore.SelectionSnapshotItemRecord
	analysisRunGraph       targetstore.AnalysisRunGraph
	surface                targetstore.ChannelSurfaceRecord
	surfaceSubjects        []targetstore.ChannelSurfaceSubjectRecord
	supersede              targetstore.SupersedeChannelSurfaceParams
}

func (s *fakeTargetRuntimeStore) UpsertChannelAccount(_ context.Context, record targetstore.ChannelAccountRecord) error {
	s.channelAccount = record
	return nil
}

func (s *fakeTargetRuntimeStore) ListChannelAccounts(_ context.Context, _ int) ([]targetstore.ChannelAccountRecord, error) {
	return []targetstore.ChannelAccountRecord{s.channelAccount}, nil
}

func (s *fakeTargetRuntimeStore) UpdateChannelAccount(_ context.Context, params targetstore.UpdateChannelAccountParams) (targetstore.ChannelAccountRecord, error) {
	s.channelAccount.ID = params.ID
	s.channelAccount.DisplayName = params.DisplayName
	s.channelAccount.Status = params.Status
	s.channelAccount.UpdatedAt = params.UpdatedAt
	return s.channelAccount, nil
}

func (s *fakeTargetRuntimeStore) RecordOperationRequest(_ context.Context, record targetstore.OperationRequestRecord) (targetstore.OperationRequestRecord, error) {
	s.operation = record
	return record, nil
}

func (s *fakeTargetRuntimeStore) CreateMediaAssetWithInbox(_ context.Context, params targetstore.CreateMediaAssetWithInboxParams) error {
	s.mediaAssetParams = params
	return nil
}

func (s *fakeTargetRuntimeStore) ListMediaAssets(_ context.Context, channelAccountID string, limit int) ([]targetstore.MediaAssetRecord, error) {
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
	return targetstore.MediaAssetRecord{
		ID:               mediaAssetID,
		ChannelAccountID: channelAccountID,
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
	return nil
}

func (s *fakeTargetRuntimeStore) ListCollections(_ context.Context, channelAccountID string, _ int) ([]targetstore.CollectionRecord, error) {
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
	s.selectionSnapshot = snapshot
	s.selectionSnapshotItems = append([]targetstore.SelectionSnapshotItemRecord(nil), items...)
	return nil
}

func (s *fakeTargetRuntimeStore) GetSelectionSnapshot(_ context.Context, channelAccountID, selectionSnapshotID string) (targetstore.SelectionSnapshotRecord, []targetstore.SelectionSnapshotItemRecord, error) {
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
	if selectionSnapshotID != "snapshot-1" {
		return nil, nil
	}
	return append([]targetstore.SelectionSnapshotItemRecord(nil), s.snapshotItems...), nil
}

func (s *fakeTargetRuntimeStore) CreateAnalysisRunGraph(_ context.Context, graph targetstore.AnalysisRunGraph) error {
	s.analysisRunGraph = graph
	return nil
}

func (s *fakeTargetRuntimeStore) ListAnalysisRuns(_ context.Context, channelAccountID string, limit int) ([]targetstore.AnalysisRunRecord, error) {
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

func (s *fakeTargetRuntimeStore) GetArtifact(_ context.Context, channelAccountID, artifactID string) (targetstore.ArtifactRecord, error) {
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

func (s *fakeTargetRuntimeStore) ListDiagnostics(_ context.Context, query targetstore.DiagnosticQuery, limit int) ([]targetstore.DiagnosticRecord, error) {
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

func (s *fakeTargetRuntimeStore) RecordAnalysisRunStepProgress(_ context.Context, _ targetstore.RecordAnalysisRunProgressParams) error {
	return nil
}

func (s *fakeTargetRuntimeStore) RecordArtifacts(_ context.Context, _ []targetstore.StoredObjectRecord, _ []targetstore.ArtifactRecord, _ []targetstore.ArtifactSubjectRecord) error {
	return nil
}

func (s *fakeTargetRuntimeStore) RecordDiagnostics(_ context.Context, _ []targetstore.DiagnosticRecord) error {
	return nil
}

func (s *fakeTargetRuntimeStore) FinalizeAnalysisRunStep(_ context.Context, params targetstore.FinalizeAnalysisRunStepParams) (targetstore.AnalysisRunRecord, error) {
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
	s.surface = record
	s.surfaceSubjects = append([]targetstore.ChannelSurfaceSubjectRecord(nil), subjects...)
	return record, nil
}

func (s *fakeTargetRuntimeStore) ListChannelSurfaces(_ context.Context, query targetstore.ChannelSurfaceQuery, _ int) ([]targetstore.ChannelSurfaceRecord, error) {
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
	return []targetstore.ChannelSurfaceSubjectRecord{{
		SurfaceID:   surfaceID,
		SubjectType: "analysis_run",
		SubjectID:   "run-1",
		SubjectRole: "primary",
		CreatedAt:   time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC),
	}}, nil
}

func (s *fakeTargetRuntimeStore) ReplaceChannelSurfaceDisplayState(_ context.Context, params targetstore.ReplaceChannelSurfaceDisplayStateParams) (targetstore.ChannelSurfaceRecord, error) {
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
	s.supersede = params
	return nil
}

func (s *fakeTargetRuntimeStore) ListChannelSurfaceEvents(_ context.Context, surfaceID string, _ int) ([]targetstore.ChannelSurfaceEventRecord, error) {
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
