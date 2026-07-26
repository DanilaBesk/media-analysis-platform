package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	ErrProcessingRunConflict       = errors.New("processing_run_conflict")
	ErrSelectionSnapshotNotFound   = errors.New("selection_snapshot_not_found")
	ErrAnalysisRunNotFound         = errors.New("analysis_run_not_found")
	ErrExportJobNotFound           = errors.New("export_job_not_found")
	ErrExportJobConflict           = errors.New("export_job_conflict")
	ErrMetadataEnrichmentNotFound  = errors.New("metadata_enrichment_not_found")
	ErrMetadataEnrichmentConflict  = errors.New("metadata_enrichment_conflict")
	ErrStoredObjectUnavailable     = errors.New("stored_object_unavailable")
	ErrArtifactResolutionFailed    = errors.New("artifact_resolution_failed")
	ErrRetryRequiresTerminalStatus = errors.New("retry_requires_terminal_status")
	ErrObjectNotFound              = errors.New("object_not_found")
)

type Logger interface {
	Printf(format string, args ...any)
}

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, objectKey, contentType string, body []byte) error
	PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error)
}

type ManagedObjectInfo struct {
	SizeBytes   int64
	ContentType string
	ETag        string
	Metadata    map[string]string
}

type ManagedObjectEntry struct {
	Bucket       string
	ObjectKey    string
	SizeBytes    int64
	LastModified time.Time
}

type ManagedObjectStore interface {
	ObjectStore
	PutObjectStream(ctx context.Context, bucket, objectKey, contentType string, reader io.Reader, sizeBytes int64, metadata map[string]string) error
	PromoteObject(ctx context.Context, bucket, stagingKey, objectKey string, metadata map[string]string) error
	StatObject(ctx context.Context, bucket, objectKey string) (ManagedObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, objectKey string) error
	ListObjects(ctx context.Context, bucket, prefix, startAfter string, limit int) ([]ManagedObjectEntry, error)
}

type DownloadDescriptor struct {
	Provider  string    `json:"provider"`
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

func ContractViolationf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrContractViolation, fmt.Sprintf(format, args...))
}
