package api

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

type TargetStateStore interface {
	UpsertChannelAccount(ctx context.Context, record targetstore.ChannelAccountRecord) error
	ListChannelAccounts(ctx context.Context, limit int) ([]targetstore.ChannelAccountRecord, error)
	UpdateChannelAccount(ctx context.Context, params targetstore.UpdateChannelAccountParams) (targetstore.ChannelAccountRecord, error)
	RecordOperationRequest(ctx context.Context, record targetstore.OperationRequestRecord) (targetstore.OperationRequestRecord, error)
	CreateMediaAssetWithInbox(ctx context.Context, params targetstore.CreateMediaAssetWithInboxParams) error
	GetInboxCollection(ctx context.Context, channelAccountID string) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error)
	CreateCollection(ctx context.Context, collection targetstore.CollectionRecord, items []targetstore.CollectionItemRecord) error
	ListCollections(ctx context.Context, channelAccountID string, limit int) ([]targetstore.CollectionRecord, error)
	GetCollection(ctx context.Context, channelAccountID, collectionID string) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error)
	UpdateCollection(ctx context.Context, params targetstore.UpdateCollectionParams) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error)
	UpdateCollectionItems(ctx context.Context, params targetstore.UpdateCollectionItemsParams) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error)
	RemoveCollectionItem(ctx context.Context, params targetstore.RemoveCollectionItemParams) (targetstore.CollectionRecord, []targetstore.CollectionItemRecord, error)
	ListMediaAssets(ctx context.Context, channelAccountID string, limit int) ([]targetstore.MediaAssetRecord, error)
	GetMediaAsset(ctx context.Context, channelAccountID, mediaAssetID string) (targetstore.MediaAssetRecord, error)
	GetStoredObject(ctx context.Context, storedObjectID string) (targetstore.StoredObjectRecord, error)
	DeleteMediaAsset(ctx context.Context, channelAccountID, mediaAssetID string, deletedAt time.Time) (targetstore.MediaAssetRecord, error)
	CreateSelectionSnapshot(ctx context.Context, snapshot targetstore.SelectionSnapshotRecord, items []targetstore.SelectionSnapshotItemRecord) error
	GetSelectionSnapshot(ctx context.Context, channelAccountID, selectionSnapshotID string) (targetstore.SelectionSnapshotRecord, []targetstore.SelectionSnapshotItemRecord, error)
	ListSelectionSnapshotItems(ctx context.Context, selectionSnapshotID string) ([]targetstore.SelectionSnapshotItemRecord, error)
	CreateAnalysisRunGraph(ctx context.Context, graph targetstore.AnalysisRunGraph) error
	ListAnalysisRuns(ctx context.Context, channelAccountID string, limit int) ([]targetstore.AnalysisRunRecord, error)
	GetAnalysisRun(ctx context.Context, channelAccountID, analysisRunID string) (targetstore.AnalysisRunRecord, error)
	GetAnalysisRunByID(ctx context.Context, analysisRunID string) (targetstore.AnalysisRunRecord, error)
	ListAnalysisRunStepQueue(ctx context.Context, status, runType, workerKind, stepKind string, limit int) ([]targetstore.AnalysisRunStepQueueRecord, error)
	ListArtifacts(ctx context.Context, channelAccountID, analysisRunID string, limit int) ([]targetstore.ArtifactRecord, error)
	GetArtifact(ctx context.Context, channelAccountID, artifactID string) (targetstore.ArtifactRecord, error)
	ListDiagnostics(ctx context.Context, query targetstore.DiagnosticQuery, limit int) ([]targetstore.DiagnosticRecord, error)
	RequestAnalysisRunCancel(ctx context.Context, channelAccountID, analysisRunID string, event targetstore.AnalysisRunEventRecord, requestedAt time.Time) (targetstore.AnalysisRunRecord, error)
	ListAnalysisRunEvents(ctx context.Context, channelAccountID, analysisRunID string, limit int) ([]targetstore.AnalysisRunEventRecord, error)
	ClaimAnalysisRunStep(ctx context.Context, analysisRunID, workerKind, stepKind, leaseOwner string, claimedAt time.Time) (targetstore.AnalysisRunStepRecord, []targetstore.AnalysisRunStepInputRecord, bool, error)
	CheckAnalysisRunStepCancel(ctx context.Context, analysisRunID, analysisRunStepID string) (targetstore.AnalysisRunRecord, targetstore.AnalysisRunStepRecord, error)
	RecordAnalysisRunStepProgress(ctx context.Context, params targetstore.RecordAnalysisRunProgressParams) error
	FinalizeAnalysisRunStep(ctx context.Context, params targetstore.FinalizeAnalysisRunStepParams) (targetstore.AnalysisRunRecord, error)
	RecordArtifacts(ctx context.Context, storedObjects []targetstore.StoredObjectRecord, artifacts []targetstore.ArtifactRecord, subjects []targetstore.ArtifactSubjectRecord) error
	RecordDiagnostics(ctx context.Context, diagnostics []targetstore.DiagnosticRecord) error
	UpsertChannelSurface(ctx context.Context, record targetstore.ChannelSurfaceRecord, subjects []targetstore.ChannelSurfaceSubjectRecord) (targetstore.ChannelSurfaceRecord, error)
	ListChannelSurfaces(ctx context.Context, query targetstore.ChannelSurfaceQuery, limit int) ([]targetstore.ChannelSurfaceRecord, error)
	ListChannelSurfaceSubjects(ctx context.Context, surfaceID string) ([]targetstore.ChannelSurfaceSubjectRecord, error)
	ReplaceChannelSurfaceDisplayState(ctx context.Context, params targetstore.ReplaceChannelSurfaceDisplayStateParams) (targetstore.ChannelSurfaceRecord, error)
	SupersedeChannelSurface(ctx context.Context, params targetstore.SupersedeChannelSurfaceParams) error
	ListChannelSurfaceEvents(ctx context.Context, surfaceID string, limit int) ([]targetstore.ChannelSurfaceEventRecord, error)
}

type TargetRuntimeService struct {
	store  TargetStateStore
	now    func() time.Time
	nextID func() string
}

type TargetRuntimeOption func(*TargetRuntimeService)

func WithTargetClock(now func() time.Time) TargetRuntimeOption {
	return func(s *TargetRuntimeService) {
		if now != nil {
			s.now = now
		}
	}
}

func WithTargetIDGenerator(nextID func() string) TargetRuntimeOption {
	return func(s *TargetRuntimeService) {
		if nextID != nil {
			s.nextID = nextID
		}
	}
}

func NewTargetRuntimeService(store TargetStateStore, opts ...TargetRuntimeOption) *TargetRuntimeService {
	service := &TargetRuntimeService{
		store:  store,
		now:    func() time.Time { return time.Now().UTC() },
		nextID: uuid.NewString,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

func (s *TargetRuntimeService) ResolveChannelAccount(ctx context.Context, req TargetChannelAccountRequest) (TargetChannelAccount, error) {
	if s.store == nil {
		return TargetChannelAccount{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	record := targetstore.ChannelAccountRecord{
		ID:                 s.nextID(),
		Channel:            req.Channel,
		ExternalAccountRef: req.ExternalAccountRef,
		DisplayName:        req.DisplayName,
		Status:             withDefaultString(req.Status, "active"),
		MetadataJSON:       jsonOrObject(req.Metadata),
		CreatedAt:          now,
		UpdatedAt:          now,
		LastSeenAt:         &now,
	}
	if err := s.store.UpsertChannelAccount(ctx, record); err != nil {
		return TargetChannelAccount{}, err
	}
	return targetChannelAccountFromRecord(record), nil
}

func (s *TargetRuntimeService) ListChannelAccounts(ctx context.Context, req TargetListChannelAccountsRequest) (TargetChannelAccountPage, error) {
	if s.store == nil {
		return TargetChannelAccountPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListChannelAccounts(ctx, limit)
	if err != nil {
		return TargetChannelAccountPage{}, err
	}
	items := make([]TargetChannelAccount, 0, len(records))
	for _, record := range records {
		items = append(items, targetChannelAccountFromRecord(record))
	}
	return TargetChannelAccountPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) UpdateChannelAccount(ctx context.Context, req TargetUpdateChannelAccountRequest) (TargetChannelAccount, error) {
	if s.store == nil {
		return TargetChannelAccount{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	params := targetstore.UpdateChannelAccountParams{
		ID:           req.ChannelAccountID,
		DisplayName:  req.DisplayName,
		Status:       req.Status,
		MetadataJSON: jsonOrObject(req.Metadata),
		LastSeenAt:   req.LastSeenAt,
		DisabledAt:   req.DisabledAt,
		UpdatedAt:    now,
	}
	record, err := s.store.UpdateChannelAccount(ctx, params)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetChannelAccount{}, storage.ErrMediaItemNotFound
	}
	if err != nil {
		return TargetChannelAccount{}, err
	}
	return targetChannelAccountFromRecord(record), nil
}

func (s *TargetRuntimeService) CreateMediaAsset(ctx context.Context, req TargetCreateMediaAssetRequest) (TargetMediaAsset, error) {
	if s.store == nil {
		return TargetMediaAsset{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	originRef := firstNonEmpty(req.Origin.OriginRef, req.Origin.ObjectRef)
	operationID := ""
	if req.IdempotencyKey != "" {
		operationID = s.nextID()
	}
	assetID := s.nextID()
	if req.IdempotencyKey != "" {
		operation, err := s.store.RecordOperationRequest(ctx, targetstore.OperationRequestRecord{
			ID:               operationID,
			ChannelAccountID: req.ChannelAccountID,
			OperationType:    "media_asset.create",
			IdempotencyKey:   req.IdempotencyKey,
			Status:           "accepted",
			TargetType:       "media_asset",
			TargetID:         assetID,
			MetadataJSON:     []byte(`{}`),
			CreatedAt:        now,
		})
		if err != nil {
			return TargetMediaAsset{}, err
		}
		if operation.TargetType == "media_asset" && operation.TargetID != "" && operation.TargetID != assetID {
			record, err := s.store.GetMediaAsset(ctx, req.ChannelAccountID, operation.TargetID)
			if errors.Is(err, sql.ErrNoRows) {
				assetID = operation.TargetID
			} else if err != nil {
				return TargetMediaAsset{}, err
			} else {
				return targetMediaAssetFromRecord(record), nil
			}
		}
	}
	params := targetstore.CreateMediaAssetWithInboxParams{
		MediaAsset: targetstore.MediaAssetRecord{
			ID:               assetID,
			ChannelAccountID: req.ChannelAccountID,
			StoredObjectID:   req.Origin.StoredObjectID,
			OriginType:       req.Origin.OriginType,
			OriginRef:        originRef,
			Kind:             req.Kind,
			DisplayName:      req.DisplayName,
			Status:           "available",
			MetadataJSON:     jsonOrObject(req.Metadata),
			CreatedAt:        now,
			UpdatedAt:        now,
		},
		InboxCollection: targetstore.CollectionRecord{
			ID:               stableTargetID("inbox:" + req.ChannelAccountID),
			ChannelAccountID: req.ChannelAccountID,
			Kind:             "inbox",
			Name:             "Inbox",
			Status:           "active",
			Version:          1,
			CreatedAt:        now,
			UpdatedAt:        now,
		},
		CollectionItem: targetstore.CollectionItemRecord{
			ID:              s.nextID(),
			CollectionID:    stableTargetID("inbox:" + req.ChannelAccountID),
			MediaAssetID:    assetID,
			AddedViaChannel: req.ChannelAccountID,
			AddedAt:         now,
		},
	}
	if req.Origin.StoredObjectID != "" {
		params.StoredObject = targetstore.StoredObjectRecord{
			ID:             req.Origin.StoredObjectID,
			Bucket:         "sources",
			ObjectKey:      originRef,
			ContentType:    req.Origin.ContentType,
			SizeBytes:      req.Origin.SizeBytes,
			Checksum:       req.Origin.Checksum,
			StorageStatus:  "available",
			RetentionState: "active",
			CreatedAt:      now,
		}
	}
	if err := s.store.CreateMediaAssetWithInbox(ctx, params); err != nil {
		return TargetMediaAsset{}, err
	}
	return TargetMediaAsset{
		MediaAssetID:     assetID,
		ChannelAccountID: req.ChannelAccountID,
		Origin:           req.Origin,
		Kind:             req.Kind,
		DisplayName:      req.DisplayName,
		Status:           "available",
		Metadata:         req.Metadata,
		CreatedAt:        now,
		UpdatedAt:        now,
	}, nil
}

func (s *TargetRuntimeService) ListMediaAssets(ctx context.Context, req TargetListMediaAssetsRequest) (TargetMediaAssetPage, error) {
	if s.store == nil {
		return TargetMediaAssetPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListMediaAssets(ctx, req.ChannelAccountID, limit)
	if err != nil {
		return TargetMediaAssetPage{}, err
	}
	items := make([]TargetMediaAsset, 0, len(records))
	for _, record := range records {
		items = append(items, targetMediaAssetFromRecord(record))
	}
	return TargetMediaAssetPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) GetMediaAsset(ctx context.Context, req TargetGetMediaAssetRequest) (TargetMediaAsset, error) {
	if s.store == nil {
		return TargetMediaAsset{}, fmt.Errorf("target storage is required")
	}
	record, err := s.store.GetMediaAsset(ctx, req.ChannelAccountID, req.MediaAssetID)
	if err != nil {
		return TargetMediaAsset{}, err
	}
	return targetMediaAssetFromRecord(record), nil
}

func (s *TargetRuntimeService) DeleteMediaAsset(ctx context.Context, req TargetDeleteMediaAssetRequest) (TargetMediaAsset, error) {
	if s.store == nil {
		return TargetMediaAsset{}, fmt.Errorf("target storage is required")
	}
	record, err := s.store.DeleteMediaAsset(ctx, req.ChannelAccountID, req.MediaAssetID, s.now())
	if err != nil {
		return TargetMediaAsset{}, err
	}
	return targetMediaAssetFromRecord(record), nil
}

func (s *TargetRuntimeService) GetInboxCollection(ctx context.Context, req TargetGetInboxCollectionRequest) (TargetCollection, error) {
	if s.store == nil {
		return TargetCollection{}, fmt.Errorf("target storage is required")
	}
	record, items, err := s.store.GetInboxCollection(ctx, req.ChannelAccountID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetCollection{}, storage.ErrCollectionNotFound
	}
	if err != nil {
		return TargetCollection{}, err
	}
	return targetCollectionFromRecord(record, items), nil
}

func (s *TargetRuntimeService) CreateCollection(ctx context.Context, req TargetCreateCollectionRequest) (TargetCollection, error) {
	if s.store == nil {
		return TargetCollection{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	collectionID := s.nextID()
	collection := targetstore.CollectionRecord{
		ID:               collectionID,
		ChannelAccountID: req.ChannelAccountID,
		Kind:             "user",
		Name:             req.Name,
		Status:           "active",
		Version:          1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	items := make([]targetstore.CollectionItemRecord, 0, len(req.Items))
	for position, mediaAssetID := range req.Items {
		items = append(items, targetstore.CollectionItemRecord{
			ID:              s.nextID(),
			CollectionID:    collectionID,
			MediaAssetID:    mediaAssetID,
			Position:        position,
			AddedViaChannel: req.ChannelAccountID,
			AddedAt:         now,
		})
	}
	if err := s.store.CreateCollection(ctx, collection, items); err != nil {
		return TargetCollection{}, err
	}
	return targetCollectionFromRecord(collection, items), nil
}

func (s *TargetRuntimeService) ListCollections(ctx context.Context, req TargetListCollectionsRequest) (TargetCollectionPage, error) {
	if s.store == nil {
		return TargetCollectionPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListCollections(ctx, req.ChannelAccountID, limit)
	if err != nil {
		return TargetCollectionPage{}, err
	}
	items := make([]TargetCollection, 0, len(records))
	for _, record := range records {
		items = append(items, targetCollectionFromRecord(record, nil))
	}
	return TargetCollectionPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) GetCollection(ctx context.Context, req TargetGetCollectionRequest) (TargetCollection, error) {
	if s.store == nil {
		return TargetCollection{}, fmt.Errorf("target storage is required")
	}
	record, items, err := s.store.GetCollection(ctx, req.ChannelAccountID, req.CollectionID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetCollection{}, storage.ErrCollectionNotFound
	}
	if err != nil {
		return TargetCollection{}, err
	}
	return targetCollectionFromRecord(record, items), nil
}

func (s *TargetRuntimeService) UpdateCollection(ctx context.Context, req TargetUpdateCollectionRequest) (TargetCollection, error) {
	if s.store == nil {
		return TargetCollection{}, fmt.Errorf("target storage is required")
	}
	record, items, err := s.store.UpdateCollection(ctx, targetstore.UpdateCollectionParams{
		ChannelAccountID: req.ChannelAccountID,
		CollectionID:     req.CollectionID,
		ExpectedVersion:  req.ExpectedVersion,
		Name:             req.Name,
		Status:           req.Status,
		UpdatedAt:        s.now(),
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetCollection{}, storage.ErrCollectionVersionConflict
	}
	if err != nil {
		return TargetCollection{}, err
	}
	return targetCollectionFromRecord(record, items), nil
}

func (s *TargetRuntimeService) UpdateCollectionItems(ctx context.Context, req TargetUpdateCollectionItemsRequest) (TargetCollection, error) {
	if s.store == nil {
		return TargetCollection{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	items := make([]targetstore.CollectionItemRecord, 0, len(req.Items))
	for _, item := range req.Items {
		items = append(items, targetstore.CollectionItemRecord{
			ID:              s.nextID(),
			CollectionID:    req.CollectionID,
			MediaAssetID:    item.MediaAssetID,
			Position:        item.Position,
			AddedViaChannel: req.ChannelAccountID,
			AddedAt:         now,
		})
	}
	record, storedItems, err := s.store.UpdateCollectionItems(ctx, targetstore.UpdateCollectionItemsParams{
		ChannelAccountID: req.ChannelAccountID,
		CollectionID:     req.CollectionID,
		ExpectedVersion:  req.ExpectedVersion,
		Items:            items,
		UpdatedAt:        now,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetCollection{}, storage.ErrCollectionVersionConflict
	}
	if err != nil {
		return TargetCollection{}, err
	}
	return targetCollectionFromRecord(record, storedItems), nil
}

func (s *TargetRuntimeService) RemoveCollectionItem(ctx context.Context, req TargetRemoveCollectionItemRequest) (TargetCollection, error) {
	if s.store == nil {
		return TargetCollection{}, fmt.Errorf("target storage is required")
	}
	record, items, err := s.store.RemoveCollectionItem(ctx, targetstore.RemoveCollectionItemParams{
		ChannelAccountID: req.ChannelAccountID,
		CollectionID:     req.CollectionID,
		MediaAssetID:     req.MediaAssetID,
		ExpectedVersion:  req.ExpectedVersion,
		RemovedAt:        s.now(),
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetCollection{}, storage.ErrCollectionVersionConflict
	}
	if err != nil {
		return TargetCollection{}, err
	}
	return targetCollectionFromRecord(record, items), nil
}

func (s *TargetRuntimeService) CreateSelectionSnapshot(ctx context.Context, req TargetCreateSelectionSnapshotRequest) (TargetSelectionSnapshot, error) {
	if s.store == nil {
		return TargetSelectionSnapshot{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	snapshotID := s.nextID()
	snapshot := targetstore.SelectionSnapshotRecord{
		ID:                 snapshotID,
		ChannelAccountID:   req.ChannelAccountID,
		SourceCollectionID: req.SourceCollectionID,
		Status:             "sealed",
		OptionSnapshotJSON: jsonOrObject(req.OptionSnapshot),
		DiagnosticsJSON:    []byte(`[]`),
		CreatedViaChannel:  req.CreatedViaChannel,
		CreatedAt:          now,
		SealedAt:           now,
	}
	items := make([]targetstore.SelectionSnapshotItemRecord, 0, len(req.Items))
	dtoItems := make([]TargetSelectionSnapshotItem, 0, len(req.Items))
	for _, item := range req.Items {
		itemID := s.nextID()
		asset, err := s.store.GetMediaAsset(ctx, req.ChannelAccountID, item.MediaAssetID)
		if err != nil {
			return TargetSelectionSnapshot{}, err
		}
		var storedObject targetstore.StoredObjectRecord
		if asset.StoredObjectID != "" {
			storedObject, err = s.store.GetStoredObject(ctx, asset.StoredObjectID)
			if err != nil {
				return TargetSelectionSnapshot{}, err
			}
		}
		record := targetstore.SelectionSnapshotItemRecord{
			ID:                  itemID,
			SelectionSnapshotID: snapshotID,
			Position:            item.Position,
			MediaAssetID:        item.MediaAssetID,
			Kind:                asset.Kind,
			DisplayName:         asset.DisplayName,
			StatusAtSelection:   asset.Status,
			OriginSnapshotJSON:  mustJSON(targetOriginSnapshotPayload(asset, storedObject)),
			StorageSnapshotJSON: mustJSON(targetStorageSnapshotPayload(storedObject)),
			MetadataJSON:        jsonOrObject(asset.MetadataJSON),
			DiagnosticsJSON:     []byte(`[]`),
		}
		items = append(items, record)
		dtoItems = append(dtoItems, targetSelectionSnapshotItemFromRecord(record))
	}
	if err := s.store.CreateSelectionSnapshot(ctx, snapshot, items); err != nil {
		return TargetSelectionSnapshot{}, err
	}
	return TargetSelectionSnapshot{
		SelectionSnapshotID: snapshotID,
		ChannelAccountID:    req.ChannelAccountID,
		SourceCollectionID:  req.SourceCollectionID,
		Status:              "sealed",
		Items:               dtoItems,
		OptionSnapshot:      req.OptionSnapshot,
		Diagnostics:         []TargetDiagnostic{},
		CreatedAt:           now,
		SealedAt:            now,
	}, nil
}

func (s *TargetRuntimeService) GetSelectionSnapshot(ctx context.Context, req TargetGetSelectionSnapshotRequest) (TargetSelectionSnapshot, error) {
	if s.store == nil {
		return TargetSelectionSnapshot{}, fmt.Errorf("target storage is required")
	}
	snapshot, items, err := s.store.GetSelectionSnapshot(ctx, req.ChannelAccountID, req.SelectionSnapshotID)
	if err != nil {
		return TargetSelectionSnapshot{}, err
	}
	return targetSelectionSnapshotFromRecord(snapshot, items), nil
}

func (s *TargetRuntimeService) CreateAnalysisRun(ctx context.Context, req TargetCreateAnalysisRunRequest) (TargetAnalysisRun, error) {
	if s.store == nil {
		return TargetAnalysisRun{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	snapshotItems, err := s.store.ListSelectionSnapshotItems(ctx, req.SelectionSnapshotID)
	if err != nil {
		return TargetAnalysisRun{}, err
	}
	runID := s.nextID()
	steps, inputs := s.planAnalysisRunSteps(runID, req.RunType, snapshotItems, now)
	eventID := s.nextID()
	graph := targetstore.AnalysisRunGraph{
		Run: targetstore.AnalysisRunRecord{
			ID:                runID,
			ChannelAccountID:  req.ChannelAccountID,
			SelectionSnapshot: req.SelectionSnapshotID,
			RunType:           req.RunType,
			Status:            "queued",
			Version:           1,
			IdempotencyKey:    req.IdempotencyKey,
			ParamsJSON:        jsonOrObject(req.Params),
			DeliveryJSON:      jsonOrDefaultRaw(req.Delivery, `{"strategy":"polling"}`),
			EvidenceGateState: "not_required",
			CreatedViaChannel: req.CreatedViaChannelID,
			CreatedAt:         now,
		},
		Steps:      steps,
		StepInputs: inputs,
		Event: targetstore.AnalysisRunEventRecord{
			ID:            eventID,
			AnalysisRunID: runID,
			EventType:     "analysis_run.created",
			Version:       1,
			Status:        "queued",
			PayloadJSON:   []byte(`{}`),
			CreatedAt:     now,
		},
	}
	if err := s.store.CreateAnalysisRunGraph(ctx, graph); err != nil {
		return TargetAnalysisRun{}, err
	}
	dtoSteps := make([]TargetAnalysisRunStep, 0, len(steps))
	for _, step := range steps {
		dtoSteps = append(dtoSteps, targetAnalysisRunStepFromRecord(step))
	}
	return TargetAnalysisRun{
		AnalysisRunID:       runID,
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: req.SelectionSnapshotID,
		RunType:             req.RunType,
		Status:              "queued",
		Version:             1,
		Params:              req.Params,
		Delivery:            req.Delivery,
		EvidenceGateState:   "not_required",
		Steps:               dtoSteps,
		CreatedAt:           now,
	}, nil
}

func (s *TargetRuntimeService) planAnalysisRunSteps(runID, runType string, snapshotItems []targetstore.SelectionSnapshotItemRecord, now time.Time) ([]targetstore.AnalysisRunStepRecord, []targetstore.AnalysisRunStepInputRecord) {
	if runType == "report" || runType == "deep_research" {
		return s.planReportLikeRunSteps(runID, runType, snapshotItems, now)
	}
	stepID := s.nextID()
	step := targetstore.AnalysisRunStepRecord{
		ID:            stepID,
		AnalysisRunID: runID,
		StepKind:      "selection.transcription",
		WorkerKind:    "transcription",
		Status:        "queued",
		AttemptNo:     1,
		MetadataJSON:  []byte(`{}`),
		CreatedAt:     now,
	}
	return []targetstore.AnalysisRunStepRecord{step}, s.selectionSnapshotStepInputs(stepID, snapshotItems, now)
}

func (s *TargetRuntimeService) planReportLikeRunSteps(runID, runType string, snapshotItems []targetstore.SelectionSnapshotItemRecord, now time.Time) ([]targetstore.AnalysisRunStepRecord, []targetstore.AnalysisRunStepInputRecord) {
	speechItems := make([]targetstore.SelectionSnapshotItemRecord, 0, len(snapshotItems))
	textReadyItems := make([]targetstore.SelectionSnapshotItemRecord, 0, len(snapshotItems))
	for _, item := range snapshotItems {
		if isSpeechMediaKind(item.Kind) {
			speechItems = append(speechItems, item)
			continue
		}
		textReadyItems = append(textReadyItems, item)
	}
	analysisStepKind := "report.analysis"
	if runType == "deep_research" {
		analysisStepKind = "deep_research.analysis"
	}
	analysisStatus := "queued"
	if len(speechItems) > 0 {
		analysisStatus = "pending"
	}
	steps := make([]targetstore.AnalysisRunStepRecord, 0, 2)
	inputs := make([]targetstore.AnalysisRunStepInputRecord, 0, len(snapshotItems))
	if len(speechItems) > 0 {
		transcriptionStepID := s.nextID()
		steps = append(steps, targetstore.AnalysisRunStepRecord{
			ID:            transcriptionStepID,
			AnalysisRunID: runID,
			StepKind:      "selection.transcription",
			WorkerKind:    "transcription",
			Status:        "queued",
			AttemptNo:     1,
			MetadataJSON:  []byte(`{"prerequisite_for":"analysis"}`),
			CreatedAt:     now,
		})
		inputs = append(inputs, s.selectionSnapshotStepInputs(transcriptionStepID, speechItems, now)...)
	}
	analysisStepID := s.nextID()
	steps = append(steps, targetstore.AnalysisRunStepRecord{
		ID:            analysisStepID,
		AnalysisRunID: runID,
		StepKind:      analysisStepKind,
		WorkerKind:    "agent_runner",
		Status:        analysisStatus,
		AttemptNo:     1,
		MetadataJSON:  jsonOrDefaultRaw(mustJSON(map[string]any{"requires_transcript_artifacts": len(speechItems) > 0}), "{}"),
		CreatedAt:     now,
	})
	if len(speechItems) == 0 {
		inputs = append(inputs, s.selectionSnapshotStepInputs(analysisStepID, textReadyItems, now)...)
	}
	return steps, inputs
}

func (s *TargetRuntimeService) selectionSnapshotStepInputs(stepID string, snapshotItems []targetstore.SelectionSnapshotItemRecord, now time.Time) []targetstore.AnalysisRunStepInputRecord {
	inputs := make([]targetstore.AnalysisRunStepInputRecord, 0, len(snapshotItems))
	for _, item := range snapshotItems {
		inputs = append(inputs, targetstore.AnalysisRunStepInputRecord{
			ID:                      s.nextID(),
			AnalysisRunStepID:       stepID,
			InputKind:               "selection_snapshot_item",
			SelectionSnapshotItemID: item.ID,
			Position:                item.Position,
			Required:                true,
			MetadataJSON:            []byte(`{}`),
			CreatedAt:               now,
		})
	}
	return inputs
}

func (s *TargetRuntimeService) ListAnalysisRuns(ctx context.Context, req TargetListAnalysisRunsRequest) (TargetAnalysisRunPage, error) {
	if s.store == nil {
		return TargetAnalysisRunPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListAnalysisRuns(ctx, req.ChannelAccountID, limit)
	if err != nil {
		return TargetAnalysisRunPage{}, err
	}
	items := make([]TargetAnalysisRun, 0, len(records))
	for _, record := range records {
		items = append(items, targetAnalysisRunFromRecord(record))
	}
	return TargetAnalysisRunPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) GetAnalysisRun(ctx context.Context, req TargetGetAnalysisRunRequest) (TargetAnalysisRun, error) {
	if s.store == nil {
		return TargetAnalysisRun{}, fmt.Errorf("target storage is required")
	}
	record, err := s.store.GetAnalysisRun(ctx, req.ChannelAccountID, req.AnalysisRunID)
	if err != nil {
		return TargetAnalysisRun{}, err
	}
	return targetAnalysisRunFromRecord(record), nil
}

func (s *TargetRuntimeService) CancelAnalysisRun(ctx context.Context, analysisRunID string, req TargetCancelAnalysisRunRequest) (TargetAnalysisRun, error) {
	if s.store == nil {
		return TargetAnalysisRun{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	payload := json.RawMessage(`{}`)
	if req.Message != "" {
		payload = mustJSON(map[string]string{"message": req.Message})
	}
	event := targetstore.AnalysisRunEventRecord{
		ID:            s.nextID(),
		AnalysisRunID: analysisRunID,
		EventType:     "analysis_run.cancel_requested",
		Status:        "cancel_requested",
		PayloadJSON:   payload,
		CreatedAt:     now,
	}
	record, err := s.store.RequestAnalysisRunCancel(ctx, req.ChannelAccountID, analysisRunID, event, now)
	if err != nil {
		return TargetAnalysisRun{}, err
	}
	return targetAnalysisRunFromRecord(record), nil
}

func (s *TargetRuntimeService) RetryAnalysisRun(ctx context.Context, analysisRunID string, req TargetRetryAnalysisRunRequest) (TargetAnalysisRun, error) {
	if s.store == nil {
		return TargetAnalysisRun{}, fmt.Errorf("target storage is required")
	}
	previous, err := s.store.GetAnalysisRun(ctx, req.ChannelAccountID, analysisRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetAnalysisRun{}, storage.ErrAnalysisRunNotFound
	}
	if err != nil {
		return TargetAnalysisRun{}, err
	}
	return s.CreateAnalysisRun(ctx, TargetCreateAnalysisRunRequest{
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: previous.SelectionSnapshot,
		RunType:             previous.RunType,
		IdempotencyKey:      req.IdempotencyKey,
		Params:              previous.ParamsJSON,
		Delivery:            previous.DeliveryJSON,
		CreatedViaChannelID: previous.CreatedViaChannel,
	})
}

func (s *TargetRuntimeService) ListAnalysisRunEvents(ctx context.Context, req TargetListAnalysisRunEventsRequest) (TargetAnalysisRunEventPage, error) {
	if s.store == nil {
		return TargetAnalysisRunEventPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListAnalysisRunEvents(ctx, req.ChannelAccountID, req.AnalysisRunID, limit)
	if err != nil {
		return TargetAnalysisRunEventPage{}, err
	}
	items := make([]TargetAnalysisRunEvent, 0, len(records))
	for _, record := range records {
		items = append(items, targetAnalysisRunEventFromRecord(record))
	}
	return TargetAnalysisRunEventPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) ListArtifacts(ctx context.Context, req TargetListArtifactsRequest) (TargetArtifactPage, error) {
	if s.store == nil {
		return TargetArtifactPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListArtifacts(ctx, req.ChannelAccountID, req.AnalysisRunID, limit)
	if err != nil {
		return TargetArtifactPage{}, err
	}
	items := make([]TargetArtifact, 0, len(records))
	for _, record := range records {
		items = append(items, targetArtifactFromRecord(record))
	}
	return TargetArtifactPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) GetArtifact(ctx context.Context, req TargetGetArtifactRequest) (TargetArtifact, error) {
	if s.store == nil {
		return TargetArtifact{}, fmt.Errorf("target storage is required")
	}
	record, err := s.store.GetArtifact(ctx, req.ChannelAccountID, req.ArtifactID)
	if err != nil {
		return TargetArtifact{}, err
	}
	return targetArtifactFromRecord(record), nil
}

func (s *TargetRuntimeService) ListDiagnostics(ctx context.Context, req TargetListDiagnosticsRequest) (TargetDiagnosticPage, error) {
	if s.store == nil {
		return TargetDiagnosticPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListDiagnostics(ctx, targetstore.DiagnosticQuery{
		ChannelAccountID: req.ChannelAccountID,
		SubjectType:      req.SubjectType,
		SubjectID:        req.SubjectID,
		Severity:         req.Severity,
		Code:             req.Code,
		CorrelationID:    req.CorrelationID,
	}, limit)
	if err != nil {
		return TargetDiagnosticPage{}, err
	}
	items := make([]TargetDiagnostic, 0, len(records))
	for _, record := range records {
		items = append(items, targetDiagnosticFromRecord(record))
	}
	return TargetDiagnosticPage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) ListAnalysisRunStepQueue(ctx context.Context, req TargetAnalysisRunStepQueueRequest) (TargetAnalysisRunStepQueueResponse, error) {
	if s.store == nil {
		return TargetAnalysisRunStepQueueResponse{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListAnalysisRunStepQueue(ctx, req.Status, req.RunType, req.WorkerKind, req.StepKind, limit)
	if err != nil {
		return TargetAnalysisRunStepQueueResponse{}, err
	}
	items := make([]TargetAnalysisRunStepQueueItem, 0, len(records))
	for _, record := range records {
		items = append(items, TargetAnalysisRunStepQueueItem{
			AnalysisRunID:     record.AnalysisRunID,
			RunType:           record.RunType,
			WorkerKind:        record.WorkerKind,
			StepKind:          record.StepKind,
			Status:            record.Status,
			Version:           record.Version,
			AttemptNo:         record.AttemptNo,
			AnalysisRunStepID: record.AnalysisRunStepID,
			CreatedAt:         record.CreatedAt,
		})
	}
	return TargetAnalysisRunStepQueueResponse{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) ClaimAnalysisRunStep(ctx context.Context, analysisRunID string, req TargetClaimAnalysisRunStepRequest) (TargetClaimAnalysisRunStepResponse, error) {
	if s.store == nil {
		return TargetClaimAnalysisRunStepResponse{}, fmt.Errorf("target storage is required")
	}
	step, inputs, claimed, err := s.store.ClaimAnalysisRunStep(ctx, analysisRunID, req.WorkerKind, req.StepKind, req.LeaseOwner, s.now())
	if err != nil {
		return TargetClaimAnalysisRunStepResponse{}, err
	}
	if !claimed {
		return TargetClaimAnalysisRunStepResponse{}, storage.ErrAnalysisRunNotFound
	}
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return TargetClaimAnalysisRunStepResponse{}, err
	}
	snapshot, snapshotItems, err := s.store.GetSelectionSnapshot(ctx, run.ChannelAccountID, run.SelectionSnapshot)
	if err != nil {
		return TargetClaimAnalysisRunStepResponse{}, err
	}
	dtoInputs := make([]TargetAnalysisRunStepInput, 0, len(inputs))
	for _, input := range inputs {
		dtoInputs = append(dtoInputs, targetAnalysisRunStepInputFromRecord(input))
	}
	claimedAt := s.now()
	if step.ClaimedAt != nil {
		claimedAt = *step.ClaimedAt
	}
	return TargetClaimAnalysisRunStepResponse{
		AnalysisRunStepID:     step.ID,
		AnalysisRunID:         step.AnalysisRunID,
		RunType:               run.RunType,
		SelectionSnapshot:     targetSelectionSnapshotFromRecord(snapshot, snapshotItems),
		AnalysisRunStepInputs: dtoInputs,
		Params:                run.ParamsJSON,
		ClaimedAt:             claimedAt,
	}, nil
}

func (s *TargetRuntimeService) CheckAnalysisRunStepCancel(ctx context.Context, analysisRunID string, req TargetCheckAnalysisRunStepCancelRequest) (TargetAnalysisRunStepCancelState, error) {
	if s.store == nil {
		return TargetAnalysisRunStepCancelState{}, fmt.Errorf("target storage is required")
	}
	run, _, err := s.store.CheckAnalysisRunStepCancel(ctx, analysisRunID, req.AnalysisRunStepID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetAnalysisRunStepCancelState{}, storage.ErrAnalysisRunNotFound
	}
	if err != nil {
		return TargetAnalysisRunStepCancelState{}, err
	}
	cancelRequested := run.Status == "cancel_requested" || run.Status == "canceled"
	return TargetAnalysisRunStepCancelState{
		CancelRequested:   cancelRequested,
		Status:            run.Status,
		CancelRequestedAt: run.CancelRequestedAt,
	}, nil
}

func (s *TargetRuntimeService) RecordAnalysisRunStepProgress(ctx context.Context, analysisRunID string, req TargetRecordAnalysisRunStepProgressRequest) error {
	if s.store == nil {
		return fmt.Errorf("target storage is required")
	}
	if req.AnalysisRunStepID == "" || req.ProgressStage == "" {
		return fmt.Errorf("%w: analysis_run_step_id and progress_stage are required", storage.ErrContractViolation)
	}
	now := s.now()
	payload := jsonOrObject(req.Payload)
	if req.ProgressMessage != "" || req.ProgressStage != "" {
		payload = mustJSON(map[string]any{
			"progress_stage":   req.ProgressStage,
			"progress_message": req.ProgressMessage,
			"payload":          json.RawMessage(payload),
		})
	}
	return s.store.RecordAnalysisRunStepProgress(ctx, targetstore.RecordAnalysisRunProgressParams{
		AnalysisRunID:     analysisRunID,
		AnalysisRunStepID: req.AnalysisRunStepID,
		HeartbeatAt:       now,
		Event: targetstore.AnalysisRunEventRecord{
			ID:            s.nextID(),
			AnalysisRunID: analysisRunID,
			EventType:     "analysis_run_step.progress",
			Status:        "running",
			PayloadJSON:   payload,
			CreatedAt:     now,
		},
	})
}

func (s *TargetRuntimeService) RecordAnalysisRunArtifacts(ctx context.Context, analysisRunID string, req TargetRecordAnalysisRunArtifactsRequest) error {
	if s.store == nil {
		return fmt.Errorf("target storage is required")
	}
	if req.AnalysisRunStepID == "" || len(req.Artifacts) == 0 {
		return fmt.Errorf("%w: analysis_run_step_id and artifacts are required", storage.ErrContractViolation)
	}
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return err
	}
	now := s.now()
	storedObjects := make([]targetstore.StoredObjectRecord, 0, len(req.Artifacts))
	artifacts := make([]targetstore.ArtifactRecord, 0, len(req.Artifacts))
	subjects := make([]targetstore.ArtifactSubjectRecord, 0, len(req.Artifacts)*2)
	for _, descriptor := range req.Artifacts {
		workerKind := strings.TrimSpace(descriptor.ArtifactKind)
		publicKind := workerDescriptorPublicArtifactKind(workerKind)
		if publicKind == "" {
			return fmt.Errorf("%w: unsupported worker artifact_kind %q", storage.ErrContractViolation, workerKind)
		}
		objectKey := strings.TrimSpace(descriptor.ObjectKey)
		if objectKey == "" {
			return fmt.Errorf("%w: worker artifact object_key is required", storage.ErrContractViolation)
		}
		artifactID := s.nextID()
		storedObjectID := s.nextID()
		storedObjects = append(storedObjects, targetstore.StoredObjectRecord{
			ID:             storedObjectID,
			Bucket:         "artifacts",
			ObjectKey:      objectKey,
			ContentType:    strings.TrimSpace(descriptor.MIMEType),
			SizeBytes:      descriptor.SizeBytes,
			StorageStatus:  "available",
			RetentionState: "active",
			CreatedAt:      now,
		})
		artifacts = append(artifacts, targetstore.ArtifactRecord{
			ID:               artifactID,
			ChannelAccountID: run.ChannelAccountID,
			AnalysisRunID:    analysisRunID,
			StoredObjectID:   storedObjectID,
			Kind:             publicKind,
			Status:           "available",
			ContentType:      strings.TrimSpace(descriptor.MIMEType),
			SizeBytes:        descriptor.SizeBytes,
			Visibility:       "channel_deliverable",
			PreviewJSON: mustJSON(map[string]any{
				"available":            true,
				"filename":             strings.TrimSpace(descriptor.Filename),
				"format":               strings.TrimSpace(descriptor.Format),
				"artifact_kind":        publicKind,
				"worker_artifact_kind": workerKind,
				"stored_object_id":     storedObjectID,
				"bucket":               "artifacts",
				"object_key":           objectKey,
			}),
			CreatedAt: now,
		})
		subjects = append(subjects,
			targetstore.ArtifactSubjectRecord{
				ID:          s.nextID(),
				ArtifactID:  artifactID,
				SubjectType: "analysis_run",
				SubjectID:   analysisRunID,
				SubjectRole: "result",
				CreatedAt:   now,
			},
			targetstore.ArtifactSubjectRecord{
				ID:          s.nextID(),
				ArtifactID:  artifactID,
				SubjectType: "analysis_run_step",
				SubjectID:   req.AnalysisRunStepID,
				SubjectRole: "source",
				CreatedAt:   now,
			},
		)
	}
	return s.store.RecordArtifacts(ctx, storedObjects, artifacts, subjects)
}

func (s *TargetRuntimeService) RecordAnalysisRunDiagnostics(ctx context.Context, analysisRunID string, req TargetRecordAnalysisRunDiagnosticsRequest) error {
	if s.store == nil {
		return fmt.Errorf("target storage is required")
	}
	if req.AnalysisRunStepID == "" || len(req.Diagnostics) == 0 {
		return fmt.Errorf("%w: analysis_run_step_id and diagnostics are required", storage.ErrContractViolation)
	}
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return err
	}
	now := s.now()
	diagnostics := make([]targetstore.DiagnosticRecord, 0, len(req.Diagnostics))
	for _, descriptor := range req.Diagnostics {
		contextJSON := jsonObjectBytes(descriptor.Context)
		contextJSON = mergeRuntimeContext(contextJSON, map[string]any{"analysis_run_step_id": req.AnalysisRunStepID})
		createdAt := descriptor.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		diagnostics = append(diagnostics, targetstore.DiagnosticRecord{
			ID:                 strings.TrimSpace(descriptor.DiagnosticID),
			ChannelAccountID:   run.ChannelAccountID,
			SubjectType:        strings.TrimSpace(descriptor.SubjectType),
			SubjectID:          strings.TrimSpace(descriptor.SubjectID),
			Severity:           strings.TrimSpace(descriptor.Severity),
			Code:               strings.TrimSpace(descriptor.Code),
			Message:            strings.TrimSpace(descriptor.Message),
			ContextJSON:        contextJSON,
			SafeChannelContext: jsonObjectBytes(descriptor.SafeAdapterContext),
			CorrelationID:      strings.TrimSpace(descriptor.CorrelationID),
			RemediationHint:    strings.TrimSpace(descriptor.RemediationHint),
			CreatedAt:          createdAt,
		})
	}
	return s.store.RecordDiagnostics(ctx, diagnostics)
}

func (s *TargetRuntimeService) FinalizeAnalysisRunStep(ctx context.Context, analysisRunID string, req TargetFinalizeAnalysisRunStepRequest) (TargetAnalysisRun, error) {
	if s.store == nil {
		return TargetAnalysisRun{}, fmt.Errorf("target storage is required")
	}
	if req.AnalysisRunStepID == "" {
		return TargetAnalysisRun{}, fmt.Errorf("%w: analysis_run_step_id is required", storage.ErrContractViolation)
	}
	stepStatus, runStatus, err := targetOutcomeStatus(req.Outcome)
	if err != nil {
		return TargetAnalysisRun{}, err
	}
	now := s.now()
	record, err := s.store.FinalizeAnalysisRunStep(ctx, targetstore.FinalizeAnalysisRunStepParams{
		AnalysisRunID:     analysisRunID,
		AnalysisRunStepID: req.AnalysisRunStepID,
		StepStatus:        stepStatus,
		RunStatus:         runStatus,
		Message:           req.Message,
		FinalizedAt:       now,
		Event: targetstore.AnalysisRunEventRecord{
			ID:            s.nextID(),
			AnalysisRunID: analysisRunID,
			EventType:     "analysis_run_step.finalized",
			PayloadJSON: mustJSON(map[string]string{
				"analysis_run_step_id": req.AnalysisRunStepID,
				"outcome":              req.Outcome,
				"message":              req.Message,
			}),
			CreatedAt: now,
		},
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetAnalysisRun{}, storage.ErrAnalysisRunNotFound
	}
	if err != nil {
		return TargetAnalysisRun{}, err
	}
	return targetAnalysisRunFromRecord(record), nil
}

func (s *TargetRuntimeService) UpsertChannelSurface(ctx context.Context, req TargetUpsertChannelSurfaceRequest) (TargetChannelSurface, error) {
	if s.store == nil {
		return TargetChannelSurface{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	surfaceID := s.nextID()
	record := targetstore.ChannelSurfaceRecord{
		ID:                 surfaceID,
		ChannelAccountID:   req.ChannelAccountID,
		Channel:            req.Channel,
		SurfaceType:        req.SurfaceType,
		SurfaceKey:         req.SurfaceKey,
		AddressJSON:        jsonOrObject(req.Address),
		AddressFingerprint: req.AddressFingerprint,
		DisplayStateJSON:   jsonOrObject(req.DisplayState),
		LifecycleStatus:    "active",
		Version:            1,
		IdempotencyKey:     req.IdempotencyKey,
		CreatedAt:          now,
		UpdatedAt:          now,
		LastRenderedAt:     &now,
	}
	subjects := make([]targetstore.ChannelSurfaceSubjectRecord, 0, len(req.Subjects))
	for _, subject := range req.Subjects {
		subjects = append(subjects, targetstore.ChannelSurfaceSubjectRecord{
			SurfaceID:   surfaceID,
			SubjectType: subject.SubjectType,
			SubjectID:   subject.SubjectID,
			SubjectRole: subject.SubjectRole,
			CreatedAt:   now,
		})
	}
	record, err := s.store.UpsertChannelSurface(ctx, record, subjects)
	if err != nil {
		return TargetChannelSurface{}, err
	}
	storedSubjects, err := s.store.ListChannelSurfaceSubjects(ctx, record.ID)
	if err != nil {
		return TargetChannelSurface{}, err
	}
	return targetChannelSurfaceFromRecord(record, targetChannelSurfaceSubjectsFromRecords(storedSubjects)), nil
}

func (s *TargetRuntimeService) ListChannelSurfaces(ctx context.Context, req TargetListChannelSurfacesRequest) (TargetChannelSurfacePage, error) {
	if s.store == nil {
		return TargetChannelSurfacePage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListChannelSurfaces(ctx, targetstore.ChannelSurfaceQuery{
		ChannelAccountID: req.ChannelAccountID,
		SubjectType:      req.SubjectType,
		SubjectID:        req.SubjectID,
		LifecycleStatus:  req.LifecycleStatus,
		ActiveOnly:       req.ActiveOnly,
	}, limit)
	if err != nil {
		return TargetChannelSurfacePage{}, err
	}
	items := make([]TargetChannelSurface, 0, len(records))
	for _, record := range records {
		subjects, err := s.store.ListChannelSurfaceSubjects(ctx, record.ID)
		if err != nil {
			return TargetChannelSurfacePage{}, err
		}
		items = append(items, targetChannelSurfaceFromRecord(record, targetChannelSurfaceSubjectsFromRecords(subjects)))
	}
	return TargetChannelSurfacePage{Items: items, Page: 1, PageSize: limit}, nil
}

func (s *TargetRuntimeService) ReplaceChannelSurfaceDisplayState(ctx context.Context, req TargetReplaceChannelSurfaceDisplayStateRequest) (TargetChannelSurface, error) {
	if s.store == nil {
		return TargetChannelSurface{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	record, err := s.store.ReplaceChannelSurfaceDisplayState(ctx, targetstore.ReplaceChannelSurfaceDisplayStateParams{
		SurfaceID:        req.SurfaceID,
		ExpectedVersion:  req.ExpectedVersion,
		DisplayStateJSON: jsonOrObject(req.DisplayState),
		UpdatedAt:        now,
		Event: targetstore.ChannelSurfaceEventRecord{
			ID:              s.nextID(),
			SurfaceID:       req.SurfaceID,
			EventType:       "channel_surface.display_state_replaced",
			Reason:          "display_state_replaced",
			PreviousVersion: req.ExpectedVersion,
			ActorType:       req.ActorType,
			ActorID:         req.ActorID,
			MetadataJSON:    jsonOrObject(req.Metadata),
			CreatedAt:       now,
		},
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetChannelSurface{}, storage.ErrCollectionVersionConflict
	}
	if err != nil {
		return TargetChannelSurface{}, err
	}
	subjects, err := s.store.ListChannelSurfaceSubjects(ctx, record.ID)
	if err != nil {
		return TargetChannelSurface{}, err
	}
	return targetChannelSurfaceFromRecord(record, targetChannelSurfaceSubjectsFromRecords(subjects)), nil
}

func (s *TargetRuntimeService) SupersedeChannelSurface(ctx context.Context, req TargetSupersedeChannelSurfaceRequest) (TargetChannelSurfaceEvent, error) {
	if s.store == nil {
		return TargetChannelSurfaceEvent{}, fmt.Errorf("target storage is required")
	}
	now := s.now()
	event := targetstore.ChannelSurfaceEventRecord{
		ID:           s.nextID(),
		SurfaceID:    req.SurfaceID,
		EventType:    "channel_surface.superseded",
		Reason:       req.Reason,
		ActorType:    req.ActorType,
		ActorID:      req.ActorID,
		MetadataJSON: jsonOrObject(req.Metadata),
		CreatedAt:    now,
	}
	if err := s.store.SupersedeChannelSurface(ctx, targetstore.SupersedeChannelSurfaceParams{
		SurfaceID:    req.SurfaceID,
		SupersededAt: now,
		Event:        event,
	}); err != nil {
		return TargetChannelSurfaceEvent{}, err
	}
	return targetChannelSurfaceEventFromRecord(event), nil
}

func (s *TargetRuntimeService) ListChannelSurfaceEvents(ctx context.Context, req TargetListChannelSurfaceEventsRequest) (TargetChannelSurfaceEventPage, error) {
	if s.store == nil {
		return TargetChannelSurfaceEventPage{}, fmt.Errorf("target storage is required")
	}
	limit := req.PageSize
	if limit <= 0 {
		limit = 20
	}
	records, err := s.store.ListChannelSurfaceEvents(ctx, req.SurfaceID, limit)
	if err != nil {
		return TargetChannelSurfaceEventPage{}, err
	}
	items := make([]TargetChannelSurfaceEvent, 0, len(records))
	for _, record := range records {
		items = append(items, targetChannelSurfaceEventFromRecord(record))
	}
	return TargetChannelSurfaceEventPage{Items: items, Page: 1, PageSize: limit}, nil
}

func stableTargetID(seed string) string {
	sum := sha1.Sum([]byte(seed))
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	return uuid.UUID(sum[:16]).String()
}

func isSpeechMediaKind(kind string) bool {
	switch kind {
	case "voice", "audio", "video":
		return true
	default:
		return false
	}
}

func jsonOrObject(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return []byte(`{}`)
	}
	return raw
}

func jsonOrDefaultRaw(raw json.RawMessage, fallback string) []byte {
	if len(raw) == 0 {
		return []byte(fallback)
	}
	return raw
}

func withDefaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func targetChannelAccountFromRecord(record targetstore.ChannelAccountRecord) TargetChannelAccount {
	return TargetChannelAccount{
		ChannelAccountID:   record.ID,
		Channel:            record.Channel,
		ExternalAccountRef: record.ExternalAccountRef,
		DisplayName:        record.DisplayName,
		Status:             record.Status,
		Metadata:           record.MetadataJSON,
		CreatedAt:          record.CreatedAt,
		UpdatedAt:          record.UpdatedAt,
		LastSeenAt:         record.LastSeenAt,
		DisabledAt:         record.DisabledAt,
	}
}

func targetMediaAssetFromRecord(record targetstore.MediaAssetRecord) TargetMediaAsset {
	return TargetMediaAsset{
		MediaAssetID:     record.ID,
		ChannelAccountID: record.ChannelAccountID,
		Origin: TargetMediaAssetOrigin{
			OriginType:     record.OriginType,
			OriginRef:      record.OriginRef,
			ObjectRef:      record.OriginRef,
			StoredObjectID: record.StoredObjectID,
		},
		Kind:        record.Kind,
		DisplayName: record.DisplayName,
		Status:      record.Status,
		Metadata:    record.MetadataJSON,
		CreatedAt:   record.CreatedAt,
		UpdatedAt:   record.UpdatedAt,
		DeletedAt:   record.DeletedAt,
	}
}

func targetOriginSnapshotPayload(asset targetstore.MediaAssetRecord, object targetstore.StoredObjectRecord) map[string]any {
	switch asset.OriginType {
	case "text":
		return map[string]any{"origin_type": "text", "text": asset.OriginRef}
	case "url":
		return map[string]any{"origin_type": "url", "url": asset.OriginRef}
	case "upload", "telegram_file":
		payload := map[string]any{
			"origin_type": asset.OriginType,
			"object_ref":  firstNonEmpty(object.ObjectKey, asset.OriginRef),
		}
		if object.ContentType != "" {
			payload["content_type"] = object.ContentType
		}
		if object.SizeBytes > 0 {
			payload["size_bytes"] = object.SizeBytes
		}
		return payload
	default:
		return map[string]any{"origin_type": asset.OriginType, "object_ref": asset.OriginRef}
	}
}

func targetStorageSnapshotPayload(object targetstore.StoredObjectRecord) map[string]any {
	if object.ID == "" {
		return map[string]any{}
	}
	payload := map[string]any{
		"stored_object_id": object.ID,
		"bucket":           object.Bucket,
		"object_key":       object.ObjectKey,
		"content_type":     object.ContentType,
		"size_bytes":       object.SizeBytes,
		"checksum":         object.Checksum,
		"storage_status":   object.StorageStatus,
		"retention_state":  object.RetentionState,
		"created_at":       object.CreatedAt,
	}
	if object.ExpiresAt != nil {
		payload["expires_at"] = object.ExpiresAt
	}
	if object.DeletedAt != nil {
		payload["deleted_at"] = object.DeletedAt
	}
	return payload
}

func targetCollectionFromRecord(record targetstore.CollectionRecord, items []targetstore.CollectionItemRecord) TargetCollection {
	dtoItems := make([]TargetCollectionItem, 0, len(items))
	for _, item := range items {
		dtoItems = append(dtoItems, targetCollectionItemFromRecord(item))
	}
	return TargetCollection{
		CollectionID:     record.ID,
		ChannelAccountID: record.ChannelAccountID,
		Kind:             record.Kind,
		Name:             record.Name,
		Status:           record.Status,
		Version:          record.Version,
		Items:            dtoItems,
		CreatedAt:        record.CreatedAt,
		UpdatedAt:        record.UpdatedAt,
		ArchivedAt:       record.ArchivedAt,
		DeletedAt:        record.DeletedAt,
	}
}

func targetCollectionItemFromRecord(record targetstore.CollectionItemRecord) TargetCollectionItem {
	item := TargetCollectionItem{
		CollectionItemID: record.ID,
		MediaAssetID:     record.MediaAssetID,
		Position:         record.Position,
		AddedBy:          record.AddedViaChannel,
		AddedAt:          record.AddedAt,
	}
	if record.MediaAsset != nil {
		asset := targetMediaAssetFromRecord(*record.MediaAsset)
		item.MediaAsset = &asset
	}
	return item
}

func targetSelectionSnapshotItemFromRecord(record targetstore.SelectionSnapshotItemRecord) TargetSelectionSnapshotItem {
	return TargetSelectionSnapshotItem{
		SelectionSnapshotItemID: record.ID,
		MediaAssetID:            record.MediaAssetID,
		Position:                record.Position,
		Kind:                    record.Kind,
		DisplayName:             record.DisplayName,
		OriginSnapshot:          record.OriginSnapshotJSON,
		StorageSnapshot:         record.StorageSnapshotJSON,
		Metadata:                record.MetadataJSON,
		StatusAtSelection:       record.StatusAtSelection,
	}
}

func targetSelectionSnapshotFromRecord(snapshot targetstore.SelectionSnapshotRecord, items []targetstore.SelectionSnapshotItemRecord) TargetSelectionSnapshot {
	dtoItems := make([]TargetSelectionSnapshotItem, 0, len(items))
	for _, item := range items {
		dtoItems = append(dtoItems, targetSelectionSnapshotItemFromRecord(item))
	}
	return TargetSelectionSnapshot{
		SelectionSnapshotID: snapshot.ID,
		ChannelAccountID:    snapshot.ChannelAccountID,
		SourceCollectionID:  snapshot.SourceCollectionID,
		Status:              snapshot.Status,
		Items:               dtoItems,
		OptionSnapshot:      snapshot.OptionSnapshotJSON,
		Diagnostics:         []TargetDiagnostic{},
		CreatedAt:           snapshot.CreatedAt,
		SealedAt:            snapshot.SealedAt,
	}
}

func targetAnalysisRunStepFromRecord(record targetstore.AnalysisRunStepRecord) TargetAnalysisRunStep {
	return TargetAnalysisRunStep{
		AnalysisRunStepID: record.ID,
		AnalysisRunID:     record.AnalysisRunID,
		StepKind:          record.StepKind,
		WorkerKind:        record.WorkerKind,
		Status:            record.Status,
		AttemptNo:         record.AttemptNo,
		ClaimedAt:         record.ClaimedAt,
		HeartbeatAt:       record.HeartbeatAt,
		FinalizedAt:       record.FinalizedAt,
	}
}

func targetAnalysisRunFromRecord(record targetstore.AnalysisRunRecord) TargetAnalysisRun {
	return TargetAnalysisRun{
		AnalysisRunID:       record.ID,
		ChannelAccountID:    record.ChannelAccountID,
		SelectionSnapshotID: record.SelectionSnapshot,
		RunType:             record.RunType,
		Status:              record.Status,
		Version:             record.Version,
		Params:              record.ParamsJSON,
		Delivery:            record.DeliveryJSON,
		EvidenceGateState:   record.EvidenceGateState,
		CreatedAt:           record.CreatedAt,
		StartedAt:           record.StartedAt,
		CompletedAt:         record.CompletedAt,
		CancelRequestedAt:   record.CancelRequestedAt,
		CanceledAt:          record.CanceledAt,
		ExpiresAt:           record.ExpiresAt,
	}
}

func targetAnalysisRunEventFromRecord(record targetstore.AnalysisRunEventRecord) TargetAnalysisRunEvent {
	return TargetAnalysisRunEvent{
		AnalysisRunEventID: record.ID,
		AnalysisRunID:      record.AnalysisRunID,
		EventType:          record.EventType,
		Version:            record.Version,
		Status:             record.Status,
		Payload:            record.PayloadJSON,
		CreatedAt:          record.CreatedAt,
	}
}

func targetArtifactFromRecord(record targetstore.ArtifactRecord) TargetArtifact {
	return TargetArtifact{
		ArtifactID:       record.ID,
		ChannelAccountID: record.ChannelAccountID,
		AnalysisRunID:    record.AnalysisRunID,
		StoredObjectID:   record.StoredObjectID,
		Kind:             record.Kind,
		Status:           record.Status,
		ContentType:      record.ContentType,
		Visibility:       record.Visibility,
		Preview:          record.PreviewJSON,
		CreatedAt:        record.CreatedAt,
	}
}

func targetDiagnosticFromRecord(record targetstore.DiagnosticRecord) TargetDiagnostic {
	return TargetDiagnostic{
		DiagnosticID:       record.ID,
		ChannelAccountID:   record.ChannelAccountID,
		SubjectType:        record.SubjectType,
		SubjectID:          record.SubjectID,
		Severity:           record.Severity,
		Code:               record.Code,
		Message:            record.Message,
		Context:            record.ContextJSON,
		SafeChannelContext: record.SafeChannelContext,
		CorrelationID:      record.CorrelationID,
		RemediationHint:    record.RemediationHint,
		CreatedAt:          record.CreatedAt,
	}
}

func targetAnalysisRunStepInputFromRecord(record targetstore.AnalysisRunStepInputRecord) TargetAnalysisRunStepInput {
	return TargetAnalysisRunStepInput{
		AnalysisRunStepInputID:  record.ID,
		AnalysisRunStepID:       record.AnalysisRunStepID,
		InputKind:               record.InputKind,
		SelectionSnapshotItemID: record.SelectionSnapshotItemID,
		ArtifactID:              record.ArtifactID,
		Position:                record.Position,
		Required:                record.Required,
		Metadata:                record.MetadataJSON,
	}
}

func targetChannelSurfaceFromRecord(record targetstore.ChannelSurfaceRecord, subjects []TargetChannelSurfaceSubject) TargetChannelSurface {
	return TargetChannelSurface{
		ChannelSurfaceID:   record.ID,
		ChannelAccountID:   record.ChannelAccountID,
		Channel:            record.Channel,
		SurfaceType:        record.SurfaceType,
		SurfaceKey:         record.SurfaceKey,
		Address:            record.AddressJSON,
		AddressFingerprint: record.AddressFingerprint,
		DisplayState:       record.DisplayStateJSON,
		LifecycleStatus:    record.LifecycleStatus,
		Version:            record.Version,
		Subjects:           subjects,
		CreatedAt:          record.CreatedAt,
		UpdatedAt:          record.UpdatedAt,
		LastRenderedAt:     record.LastRenderedAt,
		SupersededAt:       record.SupersededAt,
		DeletedAt:          record.DeletedAt,
	}
}

func targetChannelSurfaceEventFromRecord(record targetstore.ChannelSurfaceEventRecord) TargetChannelSurfaceEvent {
	return TargetChannelSurfaceEvent{
		ChannelSurfaceEventID: record.ID,
		ChannelSurfaceID:      record.SurfaceID,
		EventType:             record.EventType,
		Reason:                record.Reason,
		PreviousVersion:       record.PreviousVersion,
		NextVersion:           record.NextVersion,
		ActorType:             record.ActorType,
		ActorID:               record.ActorID,
		Metadata:              record.MetadataJSON,
		CreatedAt:             record.CreatedAt,
	}
}

func targetChannelSurfaceSubjectsFromRecords(records []targetstore.ChannelSurfaceSubjectRecord) []TargetChannelSurfaceSubject {
	subjects := make([]TargetChannelSurfaceSubject, 0, len(records))
	for _, record := range records {
		subjects = append(subjects, TargetChannelSurfaceSubject{
			SubjectType: record.SubjectType,
			SubjectID:   record.SubjectID,
			SubjectRole: record.SubjectRole,
		})
	}
	return subjects
}

func targetOutcomeStatus(outcome string) (string, string, error) {
	switch outcome {
	case "succeeded", "":
		return "succeeded", "succeeded", nil
	case "partially_succeeded":
		return "partially_succeeded", "partially_succeeded", nil
	case "failed":
		return "failed", "failed", nil
	case "canceled":
		return "canceled", "canceled", nil
	default:
		return "", "", fmt.Errorf("%w: invalid worker outcome", storage.ErrContractViolation)
	}
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}
