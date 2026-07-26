package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

type exportAccessTargetService struct {
	fakeTargetService
	publicDownloadRequests   []TargetGetExportJobRequest
	internalDownloadRequests []TargetGetExportJobRequest
	retryRequests            []TargetExportJobMutationRequest
}

func (f *fakeTargetService) ResolveInternalExportDownloadAccess(context.Context, TargetGetExportJobRequest) (TargetExportDownload, error) {
	return TargetExportDownload{}, nil
}

type privateExportObjectStore struct {
	fakeTargetObjectStore
	publicCalls   int
	internalCalls int
}

type exportCreateRecordingStore struct {
	fakeTargetRuntimeStore
	createParams []targetstore.CreateExportJobParams
}

func (s *exportCreateRecordingStore) CreateExportJob(_ context.Context, params targetstore.CreateExportJobParams) (targetstore.ExportJobRecord, error) {
	s.createParams = append(s.createParams, params)
	return params.Job, nil
}

func (s *exportCreateRecordingStore) GetMediaAsset(_ context.Context, channelAccountID, mediaAssetID string) (targetstore.MediaAssetRecord, error) {
	return targetstore.MediaAssetRecord{
		ID: mediaAssetID, ChannelAccountID: channelAccountID, StoredObjectID: "source-1",
		OriginType: "upload", Kind: "video", DisplayName: "source.mp4", Status: "available",
		MetadataJSON: []byte(`{}`), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}, nil
}

func (f *privateExportObjectStore) PresignGetObject(_ context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	f.publicCalls++
	return "http://localhost:19100/" + bucket + "/" + objectKey, time.Now().Add(expiry), nil
}

func (f *privateExportObjectStore) PresignInternalGetObject(_ context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	f.internalCalls++
	return "http://minio:9000/" + bucket + "/" + objectKey, time.Now().Add(expiry), nil
}

func (f *exportAccessTargetService) ResolveExportDownload(_ context.Context, req TargetGetExportJobRequest) (TargetExportDownload, error) {
	f.publicDownloadRequests = append(f.publicDownloadRequests, req)
	return TargetExportDownload{
		ExportJobID: req.ExportJobID,
		Filename:    "result.mp3",
		ContentType: "audio/mpeg",
		SizeBytes:   42,
		URL:         "http://public-object-store/result.mp3",
		ExpiresAt:   time.Date(2026, 7, 26, 13, 0, 0, 0, time.UTC),
	}, nil
}

func (f *exportAccessTargetService) ResolveInternalExportDownloadAccess(_ context.Context, req TargetGetExportJobRequest) (TargetExportDownload, error) {
	f.internalDownloadRequests = append(f.internalDownloadRequests, req)
	return TargetExportDownload{
		ExportJobID: req.ExportJobID,
		Filename:    "result.mp3",
		ContentType: "audio/mpeg",
		SizeBytes:   42,
		URL:         "http://minio:9000/result.mp3",
		ExpiresAt:   time.Date(2026, 7, 26, 12, 15, 0, 0, time.UTC),
	}, nil
}

func (f *exportAccessTargetService) RetryExportJob(_ context.Context, req TargetExportJobMutationRequest) (TargetExportJob, error) {
	f.retryRequests = append(f.retryRequests, req)
	return TargetExportJob{ExportJobID: req.ExportJobID, Status: "queued", Deliveries: []TargetExportDelivery{}}, nil
}

func TestInternalExportDownloadAccessUsesAuthenticatedPrivateRoute(t *testing.T) {
	t.Parallel()
	target := &exportAccessTargetService{}
	server := NewServer(Dependencies{Target: target}, WithInternalToken("internal-secret"))
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/internal/v1/export-jobs/export-1/download-access?channel_account_id=channel-account-1", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}
	if len(target.internalDownloadRequests) != 0 {
		t.Fatalf("unauthorized request reached service: %#v", target.internalDownloadRequests)
	}

	authorizedRequest := httptest.NewRequest(http.MethodGet, "/internal/v1/export-jobs/export-1/download-access?channel_account_id=channel-account-1", nil)
	authorizedRequest.Header.Set("X-Platform-Internal-Token", "internal-secret")
	authorized := httptest.NewRecorder()
	mux.ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized status = %d body=%s", authorized.Code, authorized.Body.String())
	}
	if len(target.internalDownloadRequests) != 1 || target.internalDownloadRequests[0] != (TargetGetExportJobRequest{
		ChannelAccountID: "channel-account-1",
		ExportJobID:      "export-1",
	}) {
		t.Fatalf("internal download requests = %#v", target.internalDownloadRequests)
	}
	var response TargetExportDownload
	if err := json.Unmarshal(authorized.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.URL != "http://minio:9000/result.mp3" || response.ExportJobID != "export-1" {
		t.Fatalf("internal download response = %#v", response)
	}

	public := httptest.NewRecorder()
	mux.ServeHTTP(public, httptest.NewRequest(http.MethodGet, "/v1/export-jobs/export-1/download?channel_account_id=channel-account-1", nil))
	if public.Code != http.StatusOK {
		t.Fatalf("public status = %d body=%s", public.Code, public.Body.String())
	}
	if len(target.publicDownloadRequests) != 1 {
		t.Fatalf("public download requests = %#v", target.publicDownloadRequests)
	}
}

func TestRetryExportJobPreservesIdempotencyHeader(t *testing.T) {
	t.Parallel()
	target := &exportAccessTargetService{}
	mux := newFinalMux(Dependencies{Target: target})
	req := jsonRequest(http.MethodPost, "/v1/export-jobs/export-1/retry", map[string]any{
		"channel_account_id": "channel-account-1",
	})
	req.Header.Set("Idempotency-Key", "web:export-retry:click-2")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if len(target.retryRequests) != 1 || target.retryRequests[0].IdempotencyKey != "web:export-retry:click-2" {
		t.Fatalf("retry requests = %#v", target.retryRequests)
	}
}

func TestResolveInternalExportDownloadAccessUsesPrivatePresigner(t *testing.T) {
	t.Parallel()
	objects := &privateExportObjectStore{}
	store := &fakeTargetRuntimeStore{exportJob: targetExportJobRecordForDownload()}
	service := NewTargetRuntimeService(store, WithTargetObjectStore(objects))

	internal, err := service.ResolveInternalExportDownloadAccess(context.Background(), TargetGetExportJobRequest{
		ChannelAccountID: "channel-account-1",
		ExportJobID:      "export-1",
	})
	if err != nil {
		t.Fatalf("ResolveInternalExportDownloadAccess() error = %v", err)
	}
	if internal.URL != "http://minio:9000/sources/file-id" || objects.internalCalls != 1 || objects.publicCalls != 0 {
		t.Fatalf("internal=%#v public_calls=%d internal_calls=%d", internal, objects.publicCalls, objects.internalCalls)
	}

	public, err := service.ResolveExportDownload(context.Background(), TargetGetExportJobRequest{
		ChannelAccountID: "channel-account-1",
		ExportJobID:      "export-1",
	})
	if err != nil {
		t.Fatalf("ResolveExportDownload() error = %v", err)
	}
	if public.URL != "http://localhost:19100/sources/file-id" || objects.internalCalls != 1 || objects.publicCalls != 1 {
		t.Fatalf("public=%#v public_calls=%d internal_calls=%d", public, objects.publicCalls, objects.internalCalls)
	}
}

func TestCreateExportJobScopesImplicitIdempotencyToOneUserAction(t *testing.T) {
	t.Parallel()
	store := &exportCreateRecordingStore{}
	service := NewTargetRuntimeService(store, WithTargetIDGenerator(sequenceTargetIDs(
		"export-1", "export-2",
	)))
	request := TargetCreateExportJobRequest{
		ChannelAccountID: "channel-account-1",
		MediaAssetID:     "media-1",
		Operation:        "video_to_audio",
		Variant:          []byte(`{"audio_bitrate_kbps":192}`),
		DeliveryChannel:  "telegram",
	}

	if _, err := service.CreateExportJob(context.Background(), request); err != nil {
		t.Fatalf("first CreateExportJob() error = %v", err)
	}
	if _, err := service.CreateExportJob(context.Background(), request); err != nil {
		t.Fatalf("second CreateExportJob() error = %v", err)
	}
	if len(store.createParams) != 2 {
		t.Fatalf("create params = %#v", store.createParams)
	}
	firstKey := store.createParams[0].Job.IdempotencyKey
	secondKey := store.createParams[1].Job.IdempotencyKey
	if firstKey == "" || secondKey == "" || firstKey == secondKey {
		t.Fatalf("implicit keys = %q, %q; each explicit action must have a distinct fallback key", firstKey, secondKey)
	}
}

func TestCreateExportJobReplaysBeforeMutableSourceLookupAndRejectsMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 26, 13, 0, 0, 0, time.UTC)
	store := &fakeTargetRuntimeStore{
		failMethod: "GetMediaAsset",
		failErr:    errors.New("mutable source must not be read during replay"),
		exportJob: targetstore.ExportJobRecord{
			ID: "export-original", ChannelAccountID: "channel-account-1", MediaAssetID: "media-1",
			Operation: "video_to_audio", DeliveryChannel: "telegram",
			VariantJSON: []byte(`{"audio_bitrate_kbps":192}`), Status: "queued", Version: 1,
			IdempotencyKey: "export-action-1", MaxAttempts: 3, ProgressJSON: []byte(`{}`), CreatedAt: now,
		},
	}
	service := NewTargetRuntimeService(store)
	request := TargetCreateExportJobRequest{
		ChannelAccountID: "channel-account-1", MediaAssetID: "media-1", Operation: "video_to_audio",
		Variant: []byte(`{"audio_bitrate_kbps":192}`), DeliveryChannel: "telegram", IdempotencyKey: "export-action-1",
	}

	replayed, err := service.CreateExportJob(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateExportJob(replay) error = %v", err)
	}
	if replayed.ExportJobID != "export-original" {
		t.Fatalf("CreateExportJob(replay) = %#v", replayed)
	}

	request.Variant = []byte(`{"audio_bitrate_kbps":256}`)
	if _, err := service.CreateExportJob(context.Background(), request); !errors.Is(err, storage.ErrExportJobConflict) {
		t.Fatalf("CreateExportJob(mismatched replay) error = %v, want conflict", err)
	}
}

func targetExportJobRecordForDownload() targetstore.ExportJobRecord {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	return targetstore.ExportJobRecord{
		ID:                   "export-1",
		ChannelAccountID:     "channel-account-1",
		MediaAssetID:         "media-1",
		Operation:            "youtube_audio",
		DeliveryChannel:      "telegram",
		VariantJSON:          []byte(`{"audio_bitrate_kbps":192}`),
		Status:               "succeeded",
		Version:              2,
		OutputStoredObjectID: "output-1",
		ProgressJSON:         []byte(`{"stage":"completed","percent":100}`),
		CreatedAt:            now,
		CompletedAt:          &now,
	}
}
