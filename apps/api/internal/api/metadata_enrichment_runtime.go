package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

const (
	metadataTitleMaxRunes     = 200
	metadataPerformerMaxRunes = 200
	metadataErrorMaxRunes     = 1000
	metadataPayloadMaxBytes   = 16 * 1024
	metadataMaxDuration       = 31 * 24 * 60 * 60
)

type targetMetadataEnrichmentStore interface {
	CreateMetadataEnrichment(context.Context, targetstore.MetadataEnrichmentRecord) (targetstore.MetadataEnrichmentRecord, error)
	ListMetadataEnrichmentQueue(context.Context, time.Time, int) ([]targetstore.MetadataEnrichmentRecord, error)
	GetMetadataEnrichmentByID(context.Context, string) (targetstore.MetadataEnrichmentRecord, error)
	ClaimMetadataEnrichment(context.Context, targetstore.ClaimMetadataEnrichmentParams) (targetstore.MetadataEnrichmentRecord, bool, error)
	RecordMetadataEnrichmentProgress(context.Context, targetstore.RecordMetadataEnrichmentProgressParams) error
	FinalizeMetadataEnrichment(context.Context, targetstore.FinalizeMetadataEnrichmentParams) (targetstore.MetadataEnrichmentRecord, error)
	ReclaimMetadataEnrichments(context.Context, time.Time, int) (targetstore.MetadataEnrichmentReclaimResult, error)
}

func (s *TargetRuntimeService) metadataStore() (targetMetadataEnrichmentStore, error) {
	store, ok := s.store.(targetMetadataEnrichmentStore)
	if !ok {
		return nil, storage.ContractViolationf("metadata enrichment storage is unavailable")
	}
	return store, nil
}

func (s *TargetRuntimeService) RefreshMetadataEnrichment(ctx context.Context, req TargetRefreshMetadataRequest) (TargetMetadataEnrichment, error) {
	if strings.TrimSpace(req.ChannelAccountID) == "" || strings.TrimSpace(req.MediaAssetID) == "" {
		return TargetMetadataEnrichment{}, storage.ContractViolationf("channel_account_id and media_asset_id are required")
	}
	asset, err := s.store.GetMediaAsset(ctx, req.ChannelAccountID, req.MediaAssetID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetMetadataEnrichment{}, storage.ErrMediaAssetNotFound
	}
	if err != nil {
		return TargetMetadataEnrichment{}, err
	}
	canonical, eligible, err := canonicalYouTubeURLForEnrichment(asset.OriginType, asset.OriginRef)
	if err != nil || !eligible {
		return TargetMetadataEnrichment{}, storage.ContractViolationf("metadata refresh requires a canonicalizable YouTube URL")
	}
	store, err := s.metadataStore()
	if err != nil {
		return TargetMetadataEnrichment{}, err
	}
	now := s.now()
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = "refresh:" + uuid.NewString()
	} else if len(idempotencyKey) > 200 {
		return TargetMetadataEnrichment{}, storage.ContractViolationf("Idempotency-Key must not exceed 200 bytes")
	}
	record, err := store.CreateMetadataEnrichment(ctx, targetstore.MetadataEnrichmentRecord{
		ID: s.nextID(), MediaAssetID: asset.ID, ChannelAccountID: asset.ChannelAccountID,
		Provider: "youtube", CanonicalURL: canonical, Status: "queued", Version: 1,
		IdempotencyKey: idempotencyKey, MaxAttempts: 3,
		ProgressJSON: []byte(`{"stage":"queued","percent":0}`), CreatedAt: now,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TargetMetadataEnrichment{}, storage.ErrMediaAssetNotFound
	}
	if err != nil {
		return TargetMetadataEnrichment{}, err
	}
	if record.MediaAssetID != asset.ID || record.ChannelAccountID != asset.ChannelAccountID || record.CanonicalURL != canonical {
		return TargetMetadataEnrichment{}, storage.ErrMetadataEnrichmentConflict
	}
	return targetMetadataEnrichmentFromRecord(record), nil
}

func (s *TargetRuntimeService) ListMetadataEnrichmentQueue(ctx context.Context, pageSize int) (TargetMetadataEnrichmentPage, error) {
	store, err := s.metadataStore()
	if err != nil {
		return TargetMetadataEnrichmentPage{}, err
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	records, err := store.ListMetadataEnrichmentQueue(ctx, s.now(), pageSize)
	if err != nil {
		return TargetMetadataEnrichmentPage{}, err
	}
	items := make([]TargetMetadataEnrichment, 0, len(records))
	for _, record := range records {
		items = append(items, targetMetadataEnrichmentFromRecord(record))
	}
	return TargetMetadataEnrichmentPage{Items: items, Page: 1, PageSize: pageSize}, nil
}

func (s *TargetRuntimeService) ClaimMetadataEnrichment(ctx context.Context, req TargetClaimMetadataEnrichmentRequest) (TargetMetadataEnrichmentClaim, error) {
	if strings.TrimSpace(req.LeaseOwner) == "" || len(req.LeaseOwner) > 160 {
		return TargetMetadataEnrichmentClaim{}, storage.ContractViolationf("lease_owner is required")
	}
	leaseSeconds := req.LeaseSeconds
	if leaseSeconds <= 0 {
		leaseSeconds = 120
	}
	if leaseSeconds > 900 {
		return TargetMetadataEnrichmentClaim{}, storage.ContractViolationf("lease_seconds must not exceed 900")
	}
	store, err := s.metadataStore()
	if err != nil {
		return TargetMetadataEnrichmentClaim{}, err
	}
	now := s.now()
	token := strings.ReplaceAll(uuid.NewString(), "-", "")
	record, claimed, err := store.ClaimMetadataEnrichment(ctx, targetstore.ClaimMetadataEnrichmentParams{
		EnrichmentID: req.EnrichmentID, LeaseOwner: req.LeaseOwner, AttemptToken: token,
		ClaimedAt: now, LeaseExpiresAt: now.Add(time.Duration(leaseSeconds) * time.Second),
	})
	if err != nil {
		return TargetMetadataEnrichmentClaim{}, err
	}
	if !claimed || record.LeaseExpiresAt == nil {
		return TargetMetadataEnrichmentClaim{}, storage.ErrMetadataEnrichmentConflict
	}
	return TargetMetadataEnrichmentClaim{
		Enrichment: targetMetadataEnrichmentFromRecord(record), AttemptToken: token,
		LeaseOwner: req.LeaseOwner, LeaseExpiresAt: *record.LeaseExpiresAt,
	}, nil
}

func (s *TargetRuntimeService) RecordMetadataEnrichmentProgress(ctx context.Context, req TargetMetadataEnrichmentProgressRequest) error {
	var progressObject map[string]any
	if strings.TrimSpace(req.LeaseOwner) == "" || len(req.LeaseOwner) > 160 ||
		strings.TrimSpace(req.AttemptToken) == "" || len(req.AttemptToken) > 160 ||
		len(req.Progress) == 0 || !json.Valid(req.Progress) || json.Unmarshal(req.Progress, &progressObject) != nil || progressObject == nil {
		return storage.ContractViolationf("lease_owner, attempt_token, and progress object are required")
	}
	if len(req.Progress) > metadataPayloadMaxBytes {
		return storage.ContractViolationf("progress exceeds %d bytes", metadataPayloadMaxBytes)
	}
	store, err := s.metadataStore()
	if err != nil {
		return err
	}
	err = store.RecordMetadataEnrichmentProgress(ctx, targetstore.RecordMetadataEnrichmentProgressParams{
		EnrichmentID: req.EnrichmentID, LeaseOwner: req.LeaseOwner,
		AttemptToken: req.AttemptToken, ProgressJSON: req.Progress, HeartbeatAt: s.now(),
	})
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrMetadataEnrichmentConflict
	}
	return err
}

func (s *TargetRuntimeService) FinalizeMetadataEnrichment(ctx context.Context, req TargetFinalizeMetadataEnrichmentRequest) (TargetMetadataEnrichment, error) {
	if strings.TrimSpace(req.LeaseOwner) == "" || len(req.LeaseOwner) > 160 ||
		strings.TrimSpace(req.AttemptToken) == "" || len(req.AttemptToken) > 160 {
		return TargetMetadataEnrichment{}, storage.ContractViolationf("lease_owner and attempt_token are required")
	}
	store, err := s.metadataStore()
	if err != nil {
		return TargetMetadataEnrichment{}, err
	}
	current, err := store.GetMetadataEnrichmentByID(ctx, req.EnrichmentID)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetMetadataEnrichment{}, storage.ErrMetadataEnrichmentNotFound
	}
	if err != nil {
		return TargetMetadataEnrichment{}, err
	}
	if current.LeaseOwner != req.LeaseOwner || current.AttemptToken != req.AttemptToken {
		return TargetMetadataEnrichment{}, storage.ErrMetadataEnrichmentConflict
	}
	now := s.now()
	params := targetstore.FinalizeMetadataEnrichmentParams{
		EnrichmentID: req.EnrichmentID, LeaseOwner: req.LeaseOwner,
		AttemptToken: req.AttemptToken, CompletedAt: now,
	}
	switch req.Outcome {
	case "succeeded":
		title := sanitizeBoundedText(req.Title, metadataTitleMaxRunes)
		if title == "" {
			return TargetMetadataEnrichment{}, storage.ContractViolationf("a non-empty title is required for successful enrichment")
		}
		thumbnail, err := sanitizeThumbnailURL(req.ThumbnailURL)
		if err != nil {
			return TargetMetadataEnrichment{}, err
		}
		if req.DurationSeconds < 0 || req.DurationSeconds > metadataMaxDuration {
			return TargetMetadataEnrichment{}, storage.ContractViolationf("duration_seconds is outside the supported range")
		}
		performer := sanitizeBoundedText(req.Performer, metadataPerformerMaxRunes)
		var thumbnailValue any
		if thumbnail != "" {
			thumbnailValue = thumbnail
		}
		providerMetadataValue := map[string]any{
			"provider": "youtube", "title": title, "thumbnail_url": thumbnailValue,
			"duration_seconds": req.DurationSeconds,
		}
		if performer != "" {
			providerMetadataValue["performer"] = performer
		}
		providerMetadata, err := json.Marshal(providerMetadataValue)
		if err != nil || len(providerMetadata) > metadataPayloadMaxBytes {
			return TargetMetadataEnrichment{}, storage.ContractViolationf("provider metadata is invalid")
		}
		params.Status, params.DisplayName, params.ProviderMetadataJSON = "succeeded", title, providerMetadata
	case "failed":
		params.ErrorCode = sanitizeBoundedText(req.ErrorCode, 120)
		params.ErrorMessage = sanitizeBoundedText(req.ErrorMessage, metadataErrorMaxRunes)
		if params.ErrorCode == "" {
			return TargetMetadataEnrichment{}, storage.ContractViolationf("error_code is required for failed enrichment")
		}
		if req.Retryable && current.AttemptNo < current.MaxAttempts {
			params.Status = "retry_wait"
			retryAt := now.Add(metadataRetryBackoff(current.AttemptNo))
			params.RetryAt = &retryAt
		} else {
			params.Status = "failed"
		}
	default:
		return TargetMetadataEnrichment{}, storage.ContractViolationf("outcome must be succeeded or failed")
	}
	record, err := store.FinalizeMetadataEnrichment(ctx, params)
	if errors.Is(err, sql.ErrNoRows) {
		return TargetMetadataEnrichment{}, storage.ErrMetadataEnrichmentConflict
	}
	if err != nil {
		return TargetMetadataEnrichment{}, err
	}
	return targetMetadataEnrichmentFromRecord(record), nil
}

func (s *TargetRuntimeService) ReclaimMetadataEnrichments(ctx context.Context, batchSize int) (TargetMetadataEnrichmentReclaimResult, error) {
	if batchSize <= 0 || batchSize > 1000 {
		return TargetMetadataEnrichmentReclaimResult{}, storage.ContractViolationf("batch_size must be between 1 and 1000")
	}
	store, err := s.metadataStore()
	if err != nil {
		return TargetMetadataEnrichmentReclaimResult{}, err
	}
	result, err := store.ReclaimMetadataEnrichments(ctx, s.now(), batchSize)
	return TargetMetadataEnrichmentReclaimResult{
		Examined: result.Examined, Requeued: result.Requeued, Failed: result.Failed,
	}, err
}

func canonicalYouTubeURLForEnrichment(originType, raw string) (string, bool, error) {
	if originType != "url" {
		return "", false, nil
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false, storage.ContractViolationf("URL is invalid")
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "youtu.be" && host != "youtube.com" && host != "www.youtube.com" && host != "m.youtube.com" {
		return "", false, nil
	}
	canonical, err := canonicalYouTubeURL(raw)
	return canonical, true, err
}

func sanitizeThumbnailURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if len(trimmed) > 2048 {
		return "", storage.ContractViolationf("thumbnail_url is too long")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return "", storage.ContractViolationf("thumbnail_url must be an absolute HTTPS URL without credentials")
	}
	return parsed.String(), nil
}

func sanitizeBoundedText(raw string, maxRunes int) string {
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, raw)
	cleaned = strings.Join(strings.Fields(cleaned), " ")
	runes := []rune(cleaned)
	if len(runes) > maxRunes {
		cleaned = strings.TrimSpace(string(runes[:maxRunes]))
	}
	return cleaned
}

func metadataRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := 5 * time.Second * time.Duration(1<<min(attempt-1, 6))
	return min(backoff, 5*time.Minute)
}

func targetMetadataEnrichmentFromRecord(record targetstore.MetadataEnrichmentRecord) TargetMetadataEnrichment {
	return TargetMetadataEnrichment{
		EnrichmentID: record.ID, MediaAssetID: record.MediaAssetID,
		ChannelAccountID: record.ChannelAccountID, Provider: record.Provider,
		CanonicalURL: record.CanonicalURL, Status: record.Status, AttemptNo: record.AttemptNo,
		MaxAttempts: record.MaxAttempts, Progress: record.ProgressJSON,
		NextAttemptAt: record.NextAttemptAt, ErrorCode: record.ErrorCode,
		CreatedAt: record.CreatedAt, StartedAt: record.StartedAt, CompletedAt: record.CompletedAt,
	}
}
