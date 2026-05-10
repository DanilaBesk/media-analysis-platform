package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	SourcesBucket   = "sources"
	ArtifactsBucket = "artifacts"
)

var (
	ErrStorageUnavailable        = errors.New("storage_unavailable")
	ErrArtifactNotFound          = errors.New("artifact_not_found")
	ErrContractViolation         = errors.New("storage_contract_violation")
	ErrExecutionNotFound         = errors.New("execution_not_found")
	ErrMediaItemNotFound         = errors.New("media_item_not_found")
	ErrCollectionNotFound        = errors.New("collection_not_found")
	ErrCollectionVersionConflict = errors.New("collection_version_conflict")
	ErrSelectionNotFound         = errors.New("selection_not_found")
	ErrSelectionInvalid          = errors.New("selection_invalid")
	ErrAnalysisRunNotFound       = errors.New("analysis_run_not_found")
	ErrOwnerMismatch             = errors.New("owner_mismatch")
	ErrRetryRequiresTerminalRun  = errors.New("retry_requires_terminal_run")
	ErrArtifactResolutionFailed  = errors.New("artifact_resolution_failed")
)

type Logger interface {
	Printf(format string, args ...any)
}

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, objectKey, contentType string, body []byte) error
	PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error)
}

type ObjectDeleter interface {
	DeleteObject(ctx context.Context, bucket, objectKey string) error
}

type internalObjectPresigner interface {
	PresignInternalGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error)
}

type Repository struct {
	state      MediaStateStore
	objects    ObjectStore
	logger     Logger
	now        func() time.Time
	nextID     func() string
	presignTTL time.Duration
}

type Option func(*Repository)

func WithLogger(logger Logger) Option {
	return func(r *Repository) {
		r.logger = logger
	}
}

func WithClock(now func() time.Time) Option {
	return func(r *Repository) {
		r.now = now
	}
}

func WithIDGenerator(nextID func() string) Option {
	return func(r *Repository) {
		r.nextID = nextID
	}
}

func WithPresignTTL(ttl time.Duration) Option {
	return func(r *Repository) {
		r.presignTTL = ttl
	}
}

func NewRepository(state MediaStateStore, objects ObjectStore, opts ...Option) (*Repository, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: media state store is required", ErrContractViolation)
	}
	if objects == nil {
		return nil, fmt.Errorf("%w: object store is required", ErrContractViolation)
	}

	repo := &Repository{
		state:      state,
		objects:    objects,
		now:        func() time.Time { return time.Now().UTC() },
		nextID:     uuid.NewString,
		presignTTL: 15 * time.Minute,
	}
	for _, opt := range opts {
		opt(repo)
	}
	return repo, nil
}

type ArtifactRecord struct {
	ID            string              `json:"artifact_id"`
	Owner         OwnerScope          `json:"owner"`
	AnalysisRunID string              `json:"analysis_run_id"`
	Kind          string              `json:"kind"`
	Status        string              `json:"status"`
	ObjectKey     string              `json:"object_key,omitempty"`
	ContentType   string              `json:"content_type"`
	Checksum      string              `json:"checksum,omitempty"`
	SizeBytes     int64               `json:"size_bytes"`
	Visibility    string              `json:"visibility"`
	PreviewJSON   []byte              `json:"preview,omitempty"`
	Download      *DownloadDescriptor `json:"download,omitempty"`
	Retention     RetentionMetadata   `json:"retention"`
	CreatedAt     time.Time           `json:"created_at"`
	ExpiresAt     *time.Time          `json:"expires_at,omitempty"`
	DeletedAt     *time.Time          `json:"deleted_at,omitempty"`
}

type DownloadDescriptor struct {
	Provider  string    `json:"provider"`
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

type ArtifactResolution struct {
	ArtifactID    string             `json:"artifact_id"`
	AnalysisRunID string             `json:"analysis_run_id"`
	Kind          string             `json:"kind"`
	ContentType   string             `json:"content_type"`
	SizeBytes     int64              `json:"size_bytes"`
	CreatedAt     time.Time          `json:"created_at"`
	Download      DownloadDescriptor `json:"download"`
}

type RetentionSweepResult struct {
	ExpiredMediaItems      int `json:"expired_media_items"`
	RemovedCollectionItems int `json:"removed_collection_items"`
	ArchivedCollections    int `json:"archived_collections"`
	InvalidatedSelections  int `json:"invalidated_selections"`
	ExpiredAnalysisRuns    int `json:"expired_analysis_runs"`
	ExpiredArtifacts       int `json:"expired_artifacts"`
	DiagnosticsRecorded    int `json:"diagnostics_recorded"`
}

type OrphanObjectRecord struct {
	SubjectType string     `json:"subject_type"`
	SubjectID   string     `json:"subject_id"`
	Owner       OwnerScope `json:"owner"`
	Bucket      string     `json:"bucket"`
	ObjectKey   string     `json:"object_key"`
	Reason      string     `json:"reason"`
}

type OrphanCleanupResult struct {
	Detected            int `json:"detected"`
	Deleted             int `json:"deleted"`
	MetadataOnly        int `json:"metadata_only"`
	DiagnosticsRecorded int `json:"diagnostics_recorded"`
	DeleteFailures      int `json:"delete_failures"`
}

type ObservabilitySnapshot struct {
	QueueTasks                 int       `json:"queue_tasks"`
	QueueLagSeconds           int64     `json:"queue_lag_seconds"`
	CleanupFailures            int       `json:"cleanup_failures"`
	ArtifactResolutionFailures int       `json:"artifact_resolution_failures"`
	GeneratedAt                time.Time `json:"generated_at"`
}

func (r *Repository) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
