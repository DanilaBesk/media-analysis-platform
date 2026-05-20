package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	SourcesBucket   = "sources"
	ArtifactsBucket = "artifacts"
)

var (
	ErrStorageUnavailable          = errors.New("storage_unavailable")
	ErrArtifactNotFound            = errors.New("artifact_not_found")
	ErrContractViolation           = errors.New("storage_contract_violation")
	ErrMediaAssetNotFound          = errors.New("media_asset_not_found")
	ErrCollectionNotFound          = errors.New("collection_not_found")
	ErrCollectionVersionConflict   = errors.New("collection_version_conflict")
	ErrSelectionSnapshotNotFound   = errors.New("selection_snapshot_not_found")
	ErrAnalysisRunNotFound         = errors.New("analysis_run_not_found")
	ErrArtifactResolutionFailed    = errors.New("artifact_resolution_failed")
	ErrRetryRequiresTerminalStatus = errors.New("retry_requires_terminal_status")
)

type Logger interface {
	Printf(format string, args ...any)
}

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, objectKey, contentType string, body []byte) error
	PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error)
}

type DownloadDescriptor struct {
	Provider  string    `json:"provider"`
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

func ContractViolationf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrContractViolation, fmt.Sprintf(format, args...))
}
