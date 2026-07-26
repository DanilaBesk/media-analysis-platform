package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type TargetMetadataEnrichmentService interface {
	RefreshMetadataEnrichment(rctx context.Context, req TargetRefreshMetadataRequest) (TargetMetadataEnrichment, error)
	ListMetadataEnrichmentQueue(rctx context.Context, pageSize int) (TargetMetadataEnrichmentPage, error)
	ClaimMetadataEnrichment(rctx context.Context, req TargetClaimMetadataEnrichmentRequest) (TargetMetadataEnrichmentClaim, error)
	RecordMetadataEnrichmentProgress(rctx context.Context, req TargetMetadataEnrichmentProgressRequest) error
	FinalizeMetadataEnrichment(rctx context.Context, req TargetFinalizeMetadataEnrichmentRequest) (TargetMetadataEnrichment, error)
	ReclaimMetadataEnrichments(rctx context.Context, batchSize int) (TargetMetadataEnrichmentReclaimResult, error)
}

type TargetRefreshMetadataRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	MediaAssetID     string `json:"-"`
	IdempotencyKey   string `json:"-"`
}

type TargetMetadataEnrichment struct {
	EnrichmentID     string          `json:"enrichment_id"`
	MediaAssetID     string          `json:"media_asset_id"`
	ChannelAccountID string          `json:"channel_account_id"`
	Provider         string          `json:"provider"`
	CanonicalURL     string          `json:"canonical_url"`
	Status           string          `json:"status"`
	AttemptNo        int             `json:"attempt_no"`
	MaxAttempts      int             `json:"max_attempts"`
	Progress         json.RawMessage `json:"progress,omitempty"`
	NextAttemptAt    *time.Time      `json:"next_attempt_at,omitempty"`
	ErrorCode        string          `json:"error_code,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	StartedAt        *time.Time      `json:"started_at,omitempty"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
}

type TargetMetadataEnrichmentPage struct {
	Items    []TargetMetadataEnrichment `json:"items"`
	Page     int                        `json:"page"`
	PageSize int                        `json:"page_size"`
}

type TargetClaimMetadataEnrichmentRequest struct {
	EnrichmentID string `json:"-"`
	LeaseOwner   string `json:"lease_owner"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}

type TargetMetadataEnrichmentClaim struct {
	Enrichment     TargetMetadataEnrichment `json:"enrichment"`
	AttemptToken   string                   `json:"attempt_token"`
	LeaseOwner     string                   `json:"lease_owner"`
	LeaseExpiresAt time.Time                `json:"lease_expires_at"`
}

type TargetMetadataEnrichmentProgressRequest struct {
	EnrichmentID string          `json:"-"`
	LeaseOwner   string          `json:"lease_owner"`
	AttemptToken string          `json:"attempt_token"`
	Progress     json.RawMessage `json:"progress"`
}

type TargetFinalizeMetadataEnrichmentRequest struct {
	EnrichmentID    string `json:"-"`
	LeaseOwner      string `json:"lease_owner"`
	AttemptToken    string `json:"attempt_token"`
	Outcome         string `json:"outcome"`
	Title           string `json:"title,omitempty"`
	ThumbnailURL    string `json:"thumbnail_url,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty"`
	Performer       string `json:"performer,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	Retryable       bool   `json:"retryable,omitempty"`
}

type TargetMetadataEnrichmentReclaimResult struct {
	Examined int64 `json:"examined"`
	Requeued int64 `json:"requeued"`
	Failed   int64 `json:"failed"`
}

func (s *Server) metadataEnrichmentService() (TargetMetadataEnrichmentService, bool) {
	service, ok := s.deps.Target.(TargetMetadataEnrichmentService)
	return service, ok
}

func (s *Server) handleRefreshMetadataEnrichment(w http.ResponseWriter, r *http.Request) {
	var body TargetRefreshMetadataRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_metadata_refresh", message: "metadata refresh must be valid JSON", details: err.Error()})
		return
	}
	service, ok := s.metadataEnrichmentService()
	if !ok {
		s.writeAPIError(w, apiError{status: http.StatusInternalServerError, code: "metadata_enrichment_unavailable", message: "metadata enrichment service is unavailable"})
		return
	}
	body.MediaAssetID = r.PathValue("media_asset_id")
	body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	record, err := service.RefreshMetadataEnrichment(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"enrichment": record})
}

func (s *Server) handleListMetadataEnrichmentQueue(w http.ResponseWriter, r *http.Request) {
	service, ok := s.metadataEnrichmentService()
	if !ok {
		s.writeAPIError(w, apiError{status: http.StatusInternalServerError, code: "metadata_enrichment_unavailable", message: "metadata enrichment service is unavailable"})
		return
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	page, err := service.ListMetadataEnrichmentQueue(r.Context(), pageSize)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleClaimMetadataEnrichment(w http.ResponseWriter, r *http.Request) {
	var body TargetClaimMetadataEnrichmentRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_metadata_enrichment_claim", message: "metadata enrichment claim must be valid JSON", details: err.Error()})
		return
	}
	body.EnrichmentID = r.PathValue("enrichment_id")
	service, ok := s.metadataEnrichmentService()
	if !ok {
		s.writeAPIError(w, apiError{status: http.StatusInternalServerError, code: "metadata_enrichment_unavailable", message: "metadata enrichment service is unavailable"})
		return
	}
	claim, err := service.ClaimMetadataEnrichment(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) handleRecordMetadataEnrichmentProgress(w http.ResponseWriter, r *http.Request) {
	var body TargetMetadataEnrichmentProgressRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_metadata_enrichment_progress", message: "metadata enrichment progress must be valid JSON", details: err.Error()})
		return
	}
	body.EnrichmentID = r.PathValue("enrichment_id")
	service, ok := s.metadataEnrichmentService()
	if !ok {
		s.writeAPIError(w, apiError{status: http.StatusInternalServerError, code: "metadata_enrichment_unavailable", message: "metadata enrichment service is unavailable"})
		return
	}
	if err := service.RecordMetadataEnrichmentProgress(r.Context(), body); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleFinalizeMetadataEnrichment(w http.ResponseWriter, r *http.Request) {
	var body TargetFinalizeMetadataEnrichmentRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_metadata_enrichment_finalize", message: "metadata enrichment finalization must be valid JSON", details: err.Error()})
		return
	}
	body.EnrichmentID = r.PathValue("enrichment_id")
	service, ok := s.metadataEnrichmentService()
	if !ok {
		s.writeAPIError(w, apiError{status: http.StatusInternalServerError, code: "metadata_enrichment_unavailable", message: "metadata enrichment service is unavailable"})
		return
	}
	record, err := service.FinalizeMetadataEnrichment(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"enrichment": record})
}

func (s *Server) handleReclaimMetadataEnrichments(w http.ResponseWriter, r *http.Request) {
	var body struct {
		BatchSize int `json:"batch_size"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_metadata_enrichment_reclaim", message: "metadata enrichment reclaim must be valid JSON", details: err.Error()})
		return
	}
	service, ok := s.metadataEnrichmentService()
	if !ok {
		s.writeAPIError(w, apiError{status: http.StatusInternalServerError, code: "metadata_enrichment_unavailable", message: "metadata enrichment service is unavailable"})
		return
	}
	result, err := service.ReclaimMetadataEnrichments(r.Context(), body.BatchSize)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, result)
}
