package api

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

func TestCreateMediaAssetAtomicallyQueuesCanonicalYouTubeEnrichment(t *testing.T) {
	t.Parallel()
	store := &fakeTargetRuntimeStore{}
	ids := []string{"media-1", "collection-item-1", "enrichment-1"}
	service := NewTargetRuntimeService(store,
		WithTargetClock(func() time.Time { return time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC) }),
		WithTargetIDGenerator(func() string { id := ids[0]; ids = ids[1:]; return id }),
	)
	asset, err := service.CreateMediaAsset(context.Background(), TargetCreateMediaAssetRequest{
		ChannelAccountID: "channel-1", Kind: "video",
		Origin: TargetMediaAssetOrigin{OriginType: "url", OriginRef: "https://youtu.be/abc123DEF_-?feature=shared"},
	})
	if err != nil {
		t.Fatalf("CreateMediaAsset() error=%v", err)
	}
	if asset.Origin.OriginRef != "https://www.youtube.com/watch?v=abc123DEF_-" {
		t.Fatalf("canonical origin=%q", asset.Origin.OriginRef)
	}
	params := store.mediaAssetParams
	if params.Enrichment.ID != "enrichment-1" || params.Enrichment.MediaAssetID != "media-1" ||
		params.Enrichment.Provider != "youtube" || params.Enrichment.CanonicalURL != asset.Origin.OriginRef ||
		params.Enrichment.IdempotencyKey != "initial:media-1" {
		t.Fatalf("atomic enrichment params=%#v", params.Enrichment)
	}
}

func TestMetadataEnrichmentRuntimeFencesSanitizesAndBacksOff(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	store := &fakeMetadataRuntimeStore{fakeTargetRuntimeStore: &fakeTargetRuntimeStore{}}
	store.record = targetstore.MetadataEnrichmentRecord{
		ID: "enrichment-1", MediaAssetID: "media-1", ChannelAccountID: "channel-1",
		Provider: "youtube", CanonicalURL: "https://www.youtube.com/watch?v=abc123DEF_-",
		Status: "running", AttemptNo: 1, MaxAttempts: 3, LeaseOwner: "worker-1",
		AttemptToken: "attempt-token-current", ProgressJSON: []byte(`{}`), CreatedAt: now,
	}
	service := NewTargetRuntimeService(store, WithTargetClock(func() time.Time { return now }))

	result, err := service.FinalizeMetadataEnrichment(context.Background(), TargetFinalizeMetadataEnrichmentRequest{
		EnrichmentID: "enrichment-1", LeaseOwner: "worker-1", AttemptToken: "attempt-token-current",
		Outcome: "succeeded", Title: "  A\n\t title \x00 with spaces  ",
		ThumbnailURL: "https://i.ytimg.com/demo.jpg", DurationSeconds: 42,
	})
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("FinalizeMetadataEnrichment()=%#v err=%v", result, err)
	}
	if store.finalize.DisplayName != "A title with spaces" ||
		string(store.finalize.ProviderMetadataJSON) != `{"duration_seconds":42,"provider":"youtube","thumbnail_url":"https://i.ytimg.com/demo.jpg","title":"A title with spaces"}` {
		t.Fatalf("sanitized finalize params=%#v metadata=%s", store.finalize, store.finalize.ProviderMetadataJSON)
	}

	store.record.Status = "running"
	store.record.LeaseOwner = "worker-2"
	store.record.AttemptToken = "attempt-token-retry"
	store.record.AttemptNo = 2
	_, err = service.FinalizeMetadataEnrichment(context.Background(), TargetFinalizeMetadataEnrichmentRequest{
		EnrichmentID: "enrichment-1", LeaseOwner: "worker-2", AttemptToken: "attempt-token-retry",
		Outcome: "failed", ErrorCode: "provider_timeout", ErrorMessage: "temporary", Retryable: true,
	})
	if err != nil || store.finalize.Status != "retry_wait" || store.finalize.RetryAt == nil || !store.finalize.RetryAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("retry finalize params=%#v err=%v", store.finalize, err)
	}

	store.record.LeaseOwner = "worker-current"
	store.record.AttemptToken = "attempt-token-current-2"
	_, err = service.FinalizeMetadataEnrichment(context.Background(), TargetFinalizeMetadataEnrichmentRequest{
		EnrichmentID: "enrichment-1", LeaseOwner: "worker-stale", AttemptToken: "attempt-token-stale",
		Outcome: "succeeded", Title: "stale overwrite",
	})
	if err == nil {
		t.Fatal("stale finalize must be rejected")
	}
}

func TestRefreshMetadataEnrichmentRejectsIdempotencyReplayForAnotherAsset(t *testing.T) {
	t.Parallel()
	store := &fakeMetadataRuntimeStore{fakeTargetRuntimeStore: &fakeTargetRuntimeStore{}}
	store.createResult = &targetstore.MetadataEnrichmentRecord{
		ID: "enrichment-existing", MediaAssetID: "media-other", ChannelAccountID: "channel-1",
		CanonicalURL: "https://www.youtube.com/watch?v=otherVideo1", IdempotencyKey: "refresh:same",
	}
	service := NewTargetRuntimeService(store)

	_, err := service.RefreshMetadataEnrichment(context.Background(), TargetRefreshMetadataRequest{
		ChannelAccountID: "channel-1", MediaAssetID: "media-1", IdempotencyKey: "refresh:same",
	})
	if !errors.Is(err, storage.ErrMetadataEnrichmentConflict) {
		t.Fatalf("RefreshMetadataEnrichment() error=%v, want conflict", err)
	}
}

type fakeMetadataRuntimeStore struct {
	*fakeTargetRuntimeStore
	record       targetstore.MetadataEnrichmentRecord
	createResult *targetstore.MetadataEnrichmentRecord
	finalize     targetstore.FinalizeMetadataEnrichmentParams
}

func (s *fakeMetadataRuntimeStore) GetMediaAsset(_ context.Context, channelAccountID, mediaAssetID string) (targetstore.MediaAssetRecord, error) {
	if mediaAssetID == "missing" {
		return targetstore.MediaAssetRecord{}, sql.ErrNoRows
	}
	return targetstore.MediaAssetRecord{
		ID: mediaAssetID, ChannelAccountID: channelAccountID, OriginType: "url",
		OriginRef: "https://www.youtube.com/watch?v=abc123DEF_-", Kind: "video", Status: "available",
	}, nil
}

func (s *fakeMetadataRuntimeStore) CreateMetadataEnrichment(_ context.Context, record targetstore.MetadataEnrichmentRecord) (targetstore.MetadataEnrichmentRecord, error) {
	if s.createResult != nil {
		return *s.createResult, nil
	}
	s.record = record
	return record, nil
}

func (s *fakeMetadataRuntimeStore) ListMetadataEnrichmentQueue(context.Context, time.Time, int) ([]targetstore.MetadataEnrichmentRecord, error) {
	return []targetstore.MetadataEnrichmentRecord{s.record}, nil
}

func (s *fakeMetadataRuntimeStore) GetMetadataEnrichmentByID(context.Context, string) (targetstore.MetadataEnrichmentRecord, error) {
	return s.record, nil
}

func (s *fakeMetadataRuntimeStore) ClaimMetadataEnrichment(_ context.Context, params targetstore.ClaimMetadataEnrichmentParams) (targetstore.MetadataEnrichmentRecord, bool, error) {
	s.record.Status = "claimed"
	s.record.LeaseOwner = params.LeaseOwner
	s.record.AttemptToken = params.AttemptToken
	s.record.LeaseExpiresAt = &params.LeaseExpiresAt
	s.record.AttemptNo++
	return s.record, true, nil
}

func (s *fakeMetadataRuntimeStore) RecordMetadataEnrichmentProgress(context.Context, targetstore.RecordMetadataEnrichmentProgressParams) error {
	return nil
}

func (s *fakeMetadataRuntimeStore) FinalizeMetadataEnrichment(_ context.Context, params targetstore.FinalizeMetadataEnrichmentParams) (targetstore.MetadataEnrichmentRecord, error) {
	s.finalize = params
	s.record.Status = params.Status
	return s.record, nil
}

func (s *fakeMetadataRuntimeStore) ReclaimMetadataEnrichments(context.Context, time.Time, int) (targetstore.MetadataEnrichmentReclaimResult, error) {
	return targetstore.MetadataEnrichmentReclaimResult{}, nil
}
