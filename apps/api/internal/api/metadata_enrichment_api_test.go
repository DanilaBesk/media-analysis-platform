package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetadataEnrichmentRoutesAndInternalToken(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	service := &fakeMetadataEnrichmentService{fakeTargetService: &fakeTargetService{now: now}, now: now}
	server := NewServer(Dependencies{Target: service}, WithInternalToken("internal-secret"))
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	refresh := httptest.NewRecorder()
	req := jsonRequest(http.MethodPost, "/v1/media-assets/media-1/refresh-metadata", map[string]any{
		"channel_account_id": "channel-1",
	})
	req.Header.Set("Idempotency-Key", "refresh-1")
	mux.ServeHTTP(refresh, req)
	assertTargetStatus(t, refresh, http.StatusAccepted)
	if service.refreshReq.ChannelAccountID != "channel-1" || service.refreshReq.MediaAssetID != "media-1" || service.refreshReq.IdempotencyKey != "refresh-1" {
		t.Fatalf("refresh request=%#v", service.refreshReq)
	}

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/internal/v1/metadata-enrichment-jobs/queue", nil))
	assertTargetStatus(t, unauthorized, http.StatusUnauthorized)

	queue := httptest.NewRecorder()
	queueReq := httptest.NewRequest(http.MethodGet, "/internal/v1/metadata-enrichment-jobs/queue?page_size=7", nil)
	queueReq.Header.Set("X-Platform-Internal-Token", "internal-secret")
	mux.ServeHTTP(queue, queueReq)
	assertTargetStatus(t, queue, http.StatusOK)
	if service.pageSize != 7 || !strings.Contains(queue.Body.String(), `"canonical_url":"https://www.youtube.com/watch?v=abc123DEF_-"`) {
		t.Fatalf("queue response=%s page_size=%d", queue.Body.String(), service.pageSize)
	}

	claim := httptest.NewRecorder()
	claimReq := jsonRequest(http.MethodPost, "/internal/v1/metadata-enrichment-jobs/enrichment-1/claim", map[string]any{
		"lease_owner": "worker-1", "lease_seconds": 120,
	})
	claimReq.Header.Set("X-Platform-Internal-Token", "internal-secret")
	mux.ServeHTTP(claim, claimReq)
	assertTargetStatus(t, claim, http.StatusOK)
	if service.claimReq.EnrichmentID != "enrichment-1" || service.claimReq.LeaseOwner != "worker-1" {
		t.Fatalf("claim request=%#v", service.claimReq)
	}

	progress := httptest.NewRecorder()
	progressReq := jsonRequest(http.MethodPost, "/internal/v1/metadata-enrichment-jobs/enrichment-1/progress", map[string]any{
		"lease_owner": "worker-1", "attempt_token": "attempt-token-current", "progress": map[string]any{"stage": "fetching"},
	})
	progressReq.Header.Set("X-Platform-Internal-Token", "internal-secret")
	mux.ServeHTTP(progress, progressReq)
	assertTargetStatus(t, progress, http.StatusNoContent)

	finalize := httptest.NewRecorder()
	finalizeReq := jsonRequest(http.MethodPost, "/internal/v1/metadata-enrichment-jobs/enrichment-1/finalize", map[string]any{
		"lease_owner": "worker-1", "attempt_token": "attempt-token-current", "outcome": "succeeded",
		"title": "Title", "thumbnail_url": "https://i.ytimg.com/demo.jpg", "duration_seconds": 42, "performer": "Performer",
	})
	finalizeReq.Header.Set("X-Platform-Internal-Token", "internal-secret")
	mux.ServeHTTP(finalize, finalizeReq)
	assertTargetStatus(t, finalize, http.StatusOK)
	if service.finalizeReq.Title != "Title" || service.finalizeReq.DurationSeconds != 42 || service.finalizeReq.Performer != "Performer" {
		t.Fatalf("finalize request=%#v", service.finalizeReq)
	}

	reclaim := httptest.NewRecorder()
	reclaimReq := jsonRequest(http.MethodPost, "/internal/v1/metadata-enrichment-jobs/reclaim", map[string]any{"batch_size": 10})
	reclaimReq.Header.Set("X-Platform-Internal-Token", "internal-secret")
	mux.ServeHTTP(reclaim, reclaimReq)
	assertTargetStatus(t, reclaim, http.StatusOK)
	if service.batchSize != 10 {
		t.Fatalf("reclaim batch size=%d", service.batchSize)
	}
}

type fakeMetadataEnrichmentService struct {
	*fakeTargetService
	now         time.Time
	refreshReq  TargetRefreshMetadataRequest
	pageSize    int
	claimReq    TargetClaimMetadataEnrichmentRequest
	progressReq TargetMetadataEnrichmentProgressRequest
	finalizeReq TargetFinalizeMetadataEnrichmentRequest
	batchSize   int
}

func (f *fakeMetadataEnrichmentService) enrichment() TargetMetadataEnrichment {
	return TargetMetadataEnrichment{
		EnrichmentID: "enrichment-1", MediaAssetID: "media-1", ChannelAccountID: "channel-1",
		Provider: "youtube", CanonicalURL: "https://www.youtube.com/watch?v=abc123DEF_-",
		Status: "queued", MaxAttempts: 3, Progress: []byte(`{"stage":"queued"}`), CreatedAt: f.now,
	}
}

func (f *fakeMetadataEnrichmentService) RefreshMetadataEnrichment(_ context.Context, req TargetRefreshMetadataRequest) (TargetMetadataEnrichment, error) {
	f.refreshReq = req
	return f.enrichment(), nil
}

func (f *fakeMetadataEnrichmentService) ListMetadataEnrichmentQueue(_ context.Context, pageSize int) (TargetMetadataEnrichmentPage, error) {
	f.pageSize = pageSize
	return TargetMetadataEnrichmentPage{Items: []TargetMetadataEnrichment{f.enrichment()}, Page: 1, PageSize: pageSize}, nil
}

func (f *fakeMetadataEnrichmentService) ClaimMetadataEnrichment(_ context.Context, req TargetClaimMetadataEnrichmentRequest) (TargetMetadataEnrichmentClaim, error) {
	f.claimReq = req
	return TargetMetadataEnrichmentClaim{Enrichment: f.enrichment(), AttemptToken: "attempt-token-current", LeaseOwner: req.LeaseOwner, LeaseExpiresAt: f.now.Add(2 * time.Minute)}, nil
}

func (f *fakeMetadataEnrichmentService) RecordMetadataEnrichmentProgress(_ context.Context, req TargetMetadataEnrichmentProgressRequest) error {
	f.progressReq = req
	return nil
}

func (f *fakeMetadataEnrichmentService) FinalizeMetadataEnrichment(_ context.Context, req TargetFinalizeMetadataEnrichmentRequest) (TargetMetadataEnrichment, error) {
	f.finalizeReq = req
	result := f.enrichment()
	result.Status = req.Outcome
	return result, nil
}

func (f *fakeMetadataEnrichmentService) ReclaimMetadataEnrichments(_ context.Context, batchSize int) (TargetMetadataEnrichmentReclaimResult, error) {
	f.batchSize = batchSize
	return TargetMetadataEnrichmentReclaimResult{}, nil
}
