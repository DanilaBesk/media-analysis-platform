package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

type TargetService interface {
	ResolveChannelAccount(ctx context.Context, req TargetChannelAccountRequest) (TargetChannelAccount, error)
	ListChannelAccounts(ctx context.Context, req TargetListChannelAccountsRequest) (TargetChannelAccountPage, error)
	UpdateChannelAccount(ctx context.Context, req TargetUpdateChannelAccountRequest) (TargetChannelAccount, error)
	CreateMediaAsset(ctx context.Context, req TargetCreateMediaAssetRequest) (TargetMediaAsset, error)
	ListMediaAssets(ctx context.Context, req TargetListMediaAssetsRequest) (TargetMediaAssetPage, error)
	GetMediaAsset(ctx context.Context, req TargetGetMediaAssetRequest) (TargetMediaAsset, error)
	DeleteMediaAsset(ctx context.Context, req TargetDeleteMediaAssetRequest) (TargetMediaAsset, error)
	GetInboxCollection(ctx context.Context, req TargetGetInboxCollectionRequest) (TargetCollection, error)
	CreateCollection(ctx context.Context, req TargetCreateCollectionRequest) (TargetCollection, error)
	ListCollections(ctx context.Context, req TargetListCollectionsRequest) (TargetCollectionPage, error)
	GetCollection(ctx context.Context, req TargetGetCollectionRequest) (TargetCollection, error)
	UpdateCollection(ctx context.Context, req TargetUpdateCollectionRequest) (TargetCollection, error)
	UpdateCollectionItems(ctx context.Context, req TargetUpdateCollectionItemsRequest) (TargetCollection, error)
	RemoveCollectionItem(ctx context.Context, req TargetRemoveCollectionItemRequest) (TargetCollection, error)
	CreateSelectionSnapshot(ctx context.Context, req TargetCreateSelectionSnapshotRequest) (TargetSelectionSnapshot, error)
	GetSelectionSnapshot(ctx context.Context, req TargetGetSelectionSnapshotRequest) (TargetSelectionSnapshot, error)
	CreateAnalysisRun(ctx context.Context, req TargetCreateAnalysisRunRequest) (TargetAnalysisRun, error)
	ListAnalysisRuns(ctx context.Context, req TargetListAnalysisRunsRequest) (TargetAnalysisRunPage, error)
	GetAnalysisRun(ctx context.Context, req TargetGetAnalysisRunRequest) (TargetAnalysisRun, error)
	CancelAnalysisRun(ctx context.Context, analysisRunID string, req TargetCancelAnalysisRunRequest) (TargetAnalysisRun, error)
	RetryAnalysisRun(ctx context.Context, analysisRunID string, req TargetRetryAnalysisRunRequest) (TargetAnalysisRun, error)
	ListAnalysisRunEvents(ctx context.Context, req TargetListAnalysisRunEventsRequest) (TargetAnalysisRunEventPage, error)
	ListArtifacts(ctx context.Context, req TargetListArtifactsRequest) (TargetArtifactPage, error)
	GetArtifact(ctx context.Context, req TargetGetArtifactRequest) (TargetArtifact, error)
	RefreshArtifactLink(ctx context.Context, req TargetRefreshArtifactRequest) (TargetArtifact, error)
	ListDiagnostics(ctx context.Context, req TargetListDiagnosticsRequest) (TargetDiagnosticPage, error)
	GetObservabilitySnapshot(ctx context.Context) (TargetObservabilitySnapshot, error)
	ListAnalysisRunStepQueue(ctx context.Context, req TargetAnalysisRunStepQueueRequest) (TargetAnalysisRunStepQueueResponse, error)
	ClaimAnalysisRunStep(ctx context.Context, analysisRunID string, req TargetClaimAnalysisRunStepRequest) (TargetClaimAnalysisRunStepResponse, error)
	CheckAnalysisRunStepCancel(ctx context.Context, analysisRunID string, req TargetCheckAnalysisRunStepCancelRequest) (TargetAnalysisRunStepCancelState, error)
	ResolveAnalysisRunStepRequestAccess(ctx context.Context, analysisRunID string, req TargetRequestAccessRequest) (RequestAccessResponse, error)
	ResolveArtifactDownloadAccess(ctx context.Context, artifactID string) (ArtifactDownloadAccessResponse, error)
	RecordAnalysisRunStepProgress(ctx context.Context, analysisRunID string, req TargetRecordAnalysisRunStepProgressRequest) error
	RecordAnalysisRunArtifacts(ctx context.Context, analysisRunID string, req TargetRecordAnalysisRunArtifactsRequest) error
	RecordAnalysisRunDiagnostics(ctx context.Context, analysisRunID string, req TargetRecordAnalysisRunDiagnosticsRequest) error
	FinalizeAnalysisRunStep(ctx context.Context, analysisRunID string, req TargetFinalizeAnalysisRunStepRequest) (TargetAnalysisRun, error)
	UpsertChannelSurface(ctx context.Context, req TargetUpsertChannelSurfaceRequest) (TargetChannelSurface, error)
	ListChannelSurfaces(ctx context.Context, req TargetListChannelSurfacesRequest) (TargetChannelSurfacePage, error)
	ReplaceChannelSurfaceDisplayState(ctx context.Context, req TargetReplaceChannelSurfaceDisplayStateRequest) (TargetChannelSurface, error)
	SupersedeChannelSurface(ctx context.Context, req TargetSupersedeChannelSurfaceRequest) (TargetChannelSurfaceEvent, error)
	ListChannelSurfaceEvents(ctx context.Context, req TargetListChannelSurfaceEventsRequest) (TargetChannelSurfaceEventPage, error)
}

type TargetChannelAccountRequest struct {
	Channel            string          `json:"channel"`
	ExternalAccountRef string          `json:"external_account_ref"`
	DisplayName        string          `json:"display_name,omitempty"`
	Status             string          `json:"status,omitempty"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
}

type TargetListChannelAccountsRequest struct {
	Cursor   string
	PageSize int
}

type TargetUpdateChannelAccountRequest struct {
	ChannelAccountID string          `json:"-"`
	DisplayName      string          `json:"display_name,omitempty"`
	Status           string          `json:"status,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	LastSeenAt       *time.Time      `json:"last_seen_at,omitempty"`
	DisabledAt       *time.Time      `json:"disabled_at,omitempty"`
}

type TargetChannelAccountPage struct {
	Items    []TargetChannelAccount `json:"items"`
	Page     int                    `json:"page"`
	PageSize int                    `json:"page_size"`
	Next     string                 `json:"next_cursor,omitempty"`
}

type TargetChannelAccount struct {
	ChannelAccountID   string          `json:"channel_account_id"`
	Channel            string          `json:"channel"`
	ExternalAccountRef string          `json:"external_account_ref"`
	DisplayName        string          `json:"display_name,omitempty"`
	Status             string          `json:"status"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
	LastSeenAt         *time.Time      `json:"last_seen_at,omitempty"`
	DisabledAt         *time.Time      `json:"disabled_at,omitempty"`
}

type TargetMediaAssetOrigin struct {
	OriginType       string `json:"origin_type"`
	OriginRef        string `json:"origin_ref,omitempty"`
	ObjectRef        string `json:"object_ref,omitempty"`
	OriginalFilename string `json:"original_filename,omitempty"`
	StoredObjectID   string `json:"stored_object_id,omitempty"`
	ContentType      string `json:"content_type,omitempty"`
	SizeBytes        int64  `json:"size_bytes,omitempty"`
	Checksum         string `json:"checksum,omitempty"`
	UploadBody       []byte `json:"-"`
}

type TargetCreateMediaAssetRequest struct {
	ChannelAccountID string                 `json:"channel_account_id"`
	Origin           TargetMediaAssetOrigin `json:"origin"`
	Kind             string                 `json:"kind"`
	DisplayName      string                 `json:"display_name"`
	CollectionID     string                 `json:"collection_id,omitempty"`
	Metadata         json.RawMessage        `json:"metadata,omitempty"`
	IdempotencyKey   string                 `json:"idempotency_key,omitempty"`
}

type TargetCreateMediaAssetMultipartMetadata struct {
	ChannelAccountID string          `json:"channel_account_id"`
	Kind             string          `json:"kind"`
	CollectionID     string          `json:"collection_id,omitempty"`
	DisplayName      string          `json:"display_name,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	IdempotencyKey   string          `json:"idempotency_key,omitempty"`
}

type TargetListMediaAssetsRequest struct {
	ChannelAccountID string
	Cursor           string
	PageSize         int
}

type TargetGetMediaAssetRequest struct {
	ChannelAccountID string
	MediaAssetID     string
}

type TargetDeleteMediaAssetRequest struct {
	ChannelAccountID string
	MediaAssetID     string
}

type TargetMediaAssetPage struct {
	Items    []TargetMediaAsset `json:"items"`
	Page     int                `json:"page"`
	PageSize int                `json:"page_size"`
	Next     string             `json:"next_cursor,omitempty"`
}

type TargetMediaAsset struct {
	MediaAssetID     string                 `json:"media_asset_id"`
	ChannelAccountID string                 `json:"channel_account_id"`
	Origin           TargetMediaAssetOrigin `json:"origin"`
	Kind             string                 `json:"kind"`
	DisplayName      string                 `json:"display_name"`
	Status           string                 `json:"status"`
	Metadata         json.RawMessage        `json:"metadata,omitempty"`
	Diagnostics      []TargetDiagnostic     `json:"diagnostics,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	DeletedAt        *time.Time             `json:"deleted_at,omitempty"`
}

type TargetGetInboxCollectionRequest struct {
	ChannelAccountID string
	Cursor           string
	PageSize         int
}

type TargetCreateCollectionRequest struct {
	ChannelAccountID string   `json:"channel_account_id"`
	Name             string   `json:"name"`
	Items            []string `json:"items,omitempty"`
	IdempotencyKey   string   `json:"idempotency_key,omitempty"`
}

type TargetListCollectionsRequest struct {
	ChannelAccountID string
	Cursor           string
	PageSize         int
}

type TargetGetCollectionRequest struct {
	ChannelAccountID string
	CollectionID     string
	Cursor           string
	PageSize         int
}

type TargetUpdateCollectionRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	CollectionID     string `json:"-"`
	ExpectedVersion  int64  `json:"expected_version"`
	Name             string `json:"name,omitempty"`
	Status           string `json:"status,omitempty"`
}

type TargetUpdateCollectionItemsRequest struct {
	ChannelAccountID string                              `json:"channel_account_id"`
	CollectionID     string                              `json:"-"`
	ExpectedVersion  int64                               `json:"expected_version"`
	Items            []TargetCollectionItemMutationInput `json:"items"`
}

type TargetCollectionItemMutationInput struct {
	MediaAssetID string `json:"media_asset_id"`
	Position     int    `json:"position"`
}

type TargetRemoveCollectionItemRequest struct {
	ChannelAccountID string
	CollectionID     string
	MediaAssetID     string
	ExpectedVersion  int64
}

type TargetCollectionPage struct {
	Items    []TargetCollection `json:"items"`
	Page     int                `json:"page"`
	PageSize int                `json:"page_size"`
	Next     string             `json:"next_cursor,omitempty"`
}

type TargetCollection struct {
	CollectionID     string                 `json:"collection_id"`
	ChannelAccountID string                 `json:"channel_account_id"`
	Kind             string                 `json:"kind"`
	Name             string                 `json:"name"`
	Status           string                 `json:"status"`
	Version          int64                  `json:"version"`
	Items            []TargetCollectionItem `json:"items"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	ArchivedAt       *time.Time             `json:"archived_at,omitempty"`
	DeletedAt        *time.Time             `json:"deleted_at,omitempty"`
}

type TargetCollectionItem struct {
	CollectionItemID string            `json:"collection_item_id,omitempty"`
	MediaAssetID     string            `json:"media_asset_id"`
	Position         int               `json:"position"`
	MediaAsset       *TargetMediaAsset `json:"media_asset,omitempty"`
	AddedBy          string            `json:"added_by,omitempty"`
	AddedAt          time.Time         `json:"added_at"`
}

type TargetCreateSelectionSnapshotRequest struct {
	ChannelAccountID   string                               `json:"channel_account_id"`
	SourceCollectionID string                               `json:"source_collection_id,omitempty"`
	Items              []TargetSelectionSnapshotItemRequest `json:"items"`
	OptionSnapshot     json.RawMessage                      `json:"option_snapshot,omitempty"`
	CreatedViaChannel  string                               `json:"created_via_channel_account_id,omitempty"`
	IdempotencyKey     string                               `json:"idempotency_key,omitempty"`
}

type TargetGetSelectionSnapshotRequest struct {
	ChannelAccountID    string
	SelectionSnapshotID string
}

type TargetSelectionSnapshotItemRequest struct {
	MediaAssetID string `json:"media_asset_id"`
	Position     int    `json:"position"`
}

type TargetSelectionSnapshot struct {
	SelectionSnapshotID string                        `json:"selection_snapshot_id"`
	ChannelAccountID    string                        `json:"channel_account_id"`
	SourceCollectionID  string                        `json:"source_collection_id,omitempty"`
	Status              string                        `json:"status"`
	Items               []TargetSelectionSnapshotItem `json:"items"`
	OptionSnapshot      json.RawMessage               `json:"option_snapshot,omitempty"`
	Diagnostics         []TargetDiagnostic            `json:"diagnostics"`
	CreatedAt           time.Time                     `json:"created_at"`
	SealedAt            time.Time                     `json:"sealed_at"`
}

type TargetSelectionSnapshotItem struct {
	SelectionSnapshotItemID string             `json:"selection_snapshot_item_id"`
	MediaAssetID            string             `json:"media_asset_id"`
	Position                int                `json:"position"`
	Kind                    string             `json:"kind"`
	DisplayName             string             `json:"display_name"`
	OriginSnapshot          json.RawMessage    `json:"origin_snapshot,omitempty"`
	StorageSnapshot         json.RawMessage    `json:"storage_snapshot,omitempty"`
	Metadata                json.RawMessage    `json:"metadata_snapshot,omitempty"`
	StatusAtSelection       string             `json:"status_at_selection"`
	Diagnostics             []TargetDiagnostic `json:"diagnostics,omitempty"`
}

type TargetCreateAnalysisRunRequest struct {
	ChannelAccountID    string          `json:"channel_account_id"`
	SelectionSnapshotID string          `json:"selection_snapshot_id"`
	RunType             string          `json:"run_type"`
	IdempotencyKey      string          `json:"idempotency_key,omitempty"`
	Params              json.RawMessage `json:"params,omitempty"`
	Delivery            json.RawMessage `json:"delivery,omitempty"`
	CreatedViaChannelID string          `json:"created_via_channel_id,omitempty"`
}

type TargetListAnalysisRunsRequest struct {
	ChannelAccountID string
	Cursor           string
	PageSize         int
}

type TargetGetAnalysisRunRequest struct {
	ChannelAccountID string
	AnalysisRunID    string
}

type TargetCancelAnalysisRunRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	Message          string `json:"message,omitempty"`
}

type TargetRetryAnalysisRunRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
}

type TargetListAnalysisRunEventsRequest struct {
	ChannelAccountID string
	AnalysisRunID    string
	Cursor           string
	PageSize         int
}

type TargetAnalysisRunPage struct {
	Items    []TargetAnalysisRun `json:"items"`
	Page     int                 `json:"page"`
	PageSize int                 `json:"page_size"`
	Next     string              `json:"next_cursor,omitempty"`
}

type TargetAnalysisRunEventPage struct {
	Items    []TargetAnalysisRunEvent `json:"items"`
	Page     int                      `json:"page"`
	PageSize int                      `json:"page_size"`
	Next     string                   `json:"next_cursor,omitempty"`
}

type TargetListArtifactsRequest struct {
	ChannelAccountID string
	AnalysisRunID    string
	Cursor           string
	PageSize         int
}

type TargetGetArtifactRequest struct {
	ChannelAccountID string
	ArtifactID       string
}

type TargetRefreshArtifactRequest struct {
	ChannelAccountID string
	ArtifactID       string
}

type TargetArtifactPage struct {
	Items    []TargetArtifact `json:"items"`
	Page     int              `json:"page"`
	PageSize int              `json:"page_size"`
	Next     string           `json:"next_cursor,omitempty"`
}

type TargetListDiagnosticsRequest struct {
	ChannelAccountID string
	SubjectType      string
	SubjectID        string
	Severity         string
	Code             string
	CorrelationID    string
	Cursor           string
	PageSize         int
}

type TargetDiagnosticPage struct {
	Items    []TargetDiagnostic `json:"items"`
	Page     int                `json:"page"`
	PageSize int                `json:"page_size"`
	Next     string             `json:"next_cursor,omitempty"`
}

type TargetAnalysisRun struct {
	AnalysisRunID       string                  `json:"analysis_run_id"`
	ChannelAccountID    string                  `json:"channel_account_id"`
	SelectionSnapshotID string                  `json:"selection_snapshot_id"`
	RunType             string                  `json:"run_type"`
	Status              string                  `json:"status"`
	Version             int64                   `json:"version"`
	Params              json.RawMessage         `json:"params,omitempty"`
	Delivery            json.RawMessage         `json:"delivery,omitempty"`
	EvidenceGateState   string                  `json:"evidence_gate_state"`
	Steps               []TargetAnalysisRunStep `json:"analysis_run_steps,omitempty"`
	Artifacts           []TargetArtifact        `json:"artifacts,omitempty"`
	Diagnostics         []TargetDiagnostic      `json:"diagnostics,omitempty"`
	CreatedAt           time.Time               `json:"created_at"`
	StartedAt           *time.Time              `json:"started_at,omitempty"`
	CompletedAt         *time.Time              `json:"completed_at,omitempty"`
	CancelRequestedAt   *time.Time              `json:"cancel_requested_at,omitempty"`
	CanceledAt          *time.Time              `json:"canceled_at,omitempty"`
	ExpiresAt           *time.Time              `json:"expires_at,omitempty"`
}

type TargetAnalysisRunEvent struct {
	AnalysisRunEventID string          `json:"analysis_run_event_id"`
	AnalysisRunID      string          `json:"analysis_run_id"`
	EventType          string          `json:"event_type"`
	Version            int64           `json:"version"`
	Status             string          `json:"status,omitempty"`
	Payload            json.RawMessage `json:"payload,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
}

type TargetClaimAnalysisRunStepRequest struct {
	WorkerKind string `json:"worker_kind"`
	StepKind   string `json:"step_kind"`
	LeaseOwner string `json:"lease_owner,omitempty"`
}

type TargetAnalysisRunStepQueueRequest struct {
	Status     string
	RunType    string
	WorkerKind string
	StepKind   string
	PageSize   int
}

type TargetAnalysisRunStepQueueItem struct {
	AnalysisRunID     string    `json:"analysis_run_id"`
	RunType           string    `json:"run_type"`
	WorkerKind        string    `json:"worker_kind"`
	StepKind          string    `json:"step_kind"`
	Status            string    `json:"status"`
	Version           int64     `json:"version"`
	AttemptNo         int       `json:"attempt_no"`
	AnalysisRunStepID string    `json:"analysis_run_step_id"`
	CreatedAt         time.Time `json:"created_at"`
}

type TargetAnalysisRunStepQueueResponse struct {
	Items    []TargetAnalysisRunStepQueueItem `json:"items"`
	Page     int                              `json:"page"`
	PageSize int                              `json:"page_size"`
}

type TargetClaimAnalysisRunStepResponse struct {
	AnalysisRunStepID     string                       `json:"analysis_run_step_id"`
	AnalysisRunID         string                       `json:"analysis_run_id"`
	RunType               string                       `json:"run_type"`
	SelectionSnapshot     TargetSelectionSnapshot      `json:"selection_snapshot"`
	AnalysisRunStepInputs []TargetAnalysisRunStepInput `json:"analysis_run_step_inputs"`
	Params                json.RawMessage              `json:"params"`
	ClaimedAt             time.Time                    `json:"claimed_at"`
}

type TargetCheckAnalysisRunStepCancelRequest struct {
	AnalysisRunStepID string
}

type TargetAnalysisRunStepCancelState struct {
	CancelRequested   bool       `json:"cancel_requested"`
	Status            string     `json:"status"`
	CancelRequestedAt *time.Time `json:"cancel_requested_at,omitempty"`
}

type TargetRequestAccessRequest struct {
	AnalysisRunStepID string
}

type TargetRecordAnalysisRunStepProgressRequest struct {
	AnalysisRunStepID string          `json:"analysis_run_step_id"`
	ProgressStage     string          `json:"progress_stage"`
	ProgressMessage   string          `json:"progress_message,omitempty"`
	Payload           json.RawMessage `json:"payload,omitempty"`
}

type workerArtifactDescriptor struct {
	ArtifactKind string `json:"artifact_kind"`
	MIMEType     string `json:"mime_type"`
	ObjectKey    string `json:"object_key"`
	SizeBytes    int64  `json:"size_bytes"`
	Filename     string `json:"filename"`
	Format       string `json:"format,omitempty"`
}

type TargetRecordAnalysisRunArtifactsRequest struct {
	AnalysisRunStepID string                     `json:"analysis_run_step_id"`
	Artifacts         []workerArtifactDescriptor `json:"artifacts"`
}

type workerDiagnosticDescriptor struct {
	DiagnosticID       string         `json:"diagnostic_id"`
	SubjectType        string         `json:"subject_type"`
	SubjectID          string         `json:"subject_id"`
	Severity           string         `json:"severity"`
	Code               string         `json:"code"`
	Message            string         `json:"message"`
	Context            map[string]any `json:"context,omitempty"`
	SafeChannelContext map[string]any `json:"safe_channel_context,omitempty"`
	CorrelationID      string         `json:"correlation_id,omitempty"`
	RemediationHint    string         `json:"remediation_hint,omitempty"`
	CreatedAt          time.Time      `json:"created_at,omitempty"`
}

type TargetRecordAnalysisRunDiagnosticsRequest struct {
	AnalysisRunStepID string                       `json:"analysis_run_step_id"`
	Diagnostics       []workerDiagnosticDescriptor `json:"diagnostics"`
}

type TargetFinalizeAnalysisRunStepRequest struct {
	AnalysisRunStepID string `json:"analysis_run_step_id"`
	Outcome           string `json:"outcome"`
	Message           string `json:"message,omitempty"`
}

type TargetAnalysisRunStep struct {
	AnalysisRunStepID string     `json:"analysis_run_step_id"`
	AnalysisRunID     string     `json:"analysis_run_id"`
	StepKind          string     `json:"step_kind"`
	WorkerKind        string     `json:"worker_kind"`
	Status            string     `json:"status"`
	AttemptNo         int        `json:"attempt_no"`
	ClaimedAt         *time.Time `json:"claimed_at,omitempty"`
	HeartbeatAt       *time.Time `json:"heartbeat_at,omitempty"`
	FinalizedAt       *time.Time `json:"finalized_at,omitempty"`
}

type TargetAnalysisRunStepInput struct {
	AnalysisRunStepInputID  string          `json:"analysis_run_step_input_id"`
	AnalysisRunStepID       string          `json:"analysis_run_step_id"`
	InputKind               string          `json:"input_kind"`
	SelectionSnapshotItemID string          `json:"selection_snapshot_item_id,omitempty"`
	ArtifactID              string          `json:"artifact_id,omitempty"`
	Position                int             `json:"position"`
	Required                bool            `json:"required"`
	Metadata                json.RawMessage `json:"metadata,omitempty"`
}

type TargetArtifact struct {
	ArtifactID       string                      `json:"artifact_id"`
	ChannelAccountID string                      `json:"channel_account_id,omitempty"`
	AnalysisRunID    string                      `json:"analysis_run_id"`
	StoredObjectID   string                      `json:"stored_object_id,omitempty"`
	Kind             string                      `json:"kind"`
	Status           string                      `json:"status"`
	ContentType      string                      `json:"content_type"`
	SizeBytes        int64                       `json:"size_bytes,omitempty"`
	ObjectKey        string                      `json:"object_key,omitempty"`
	Checksum         string                      `json:"checksum,omitempty"`
	Visibility       string                      `json:"visibility"`
	Preview          json.RawMessage             `json:"preview,omitempty"`
	Download         *storage.DownloadDescriptor `json:"download,omitempty"`
	Subjects         []TargetArtifactSubject     `json:"subjects,omitempty"`
	CreatedAt        time.Time                   `json:"created_at"`
}

type TargetArtifactSubject struct {
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	SubjectRole string `json:"subject_role"`
}

type TargetDiagnostic struct {
	DiagnosticID       string          `json:"diagnostic_id,omitempty"`
	ChannelAccountID   string          `json:"channel_account_id,omitempty"`
	SubjectType        string          `json:"subject_type,omitempty"`
	SubjectID          string          `json:"subject_id,omitempty"`
	Severity           string          `json:"severity,omitempty"`
	Code               string          `json:"code,omitempty"`
	Message            string          `json:"message,omitempty"`
	Context            json.RawMessage `json:"context,omitempty"`
	SafeChannelContext json.RawMessage `json:"safe_channel_context,omitempty"`
	CorrelationID      string          `json:"correlation_id,omitempty"`
	RemediationHint    string          `json:"remediation_hint,omitempty"`
	CreatedAt          time.Time       `json:"created_at,omitempty"`
}

type TargetUpsertChannelSurfaceRequest struct {
	ChannelAccountID   string                        `json:"channel_account_id"`
	Channel            string                        `json:"channel"`
	SurfaceType        string                        `json:"surface_type"`
	SurfaceKey         string                        `json:"surface_key"`
	Address            json.RawMessage               `json:"address,omitempty"`
	AddressFingerprint string                        `json:"address_fingerprint,omitempty"`
	DisplayState       json.RawMessage               `json:"display_state,omitempty"`
	IdempotencyKey     string                        `json:"idempotency_key,omitempty"`
	Subjects           []TargetChannelSurfaceSubject `json:"subjects,omitempty"`
}

type TargetListChannelSurfacesRequest struct {
	ChannelAccountID string
	SubjectType      string
	SubjectID        string
	LifecycleStatus  string
	ActiveOnly       bool
	Cursor           string
	PageSize         int
}

type TargetChannelSurfacePage struct {
	Items    []TargetChannelSurface `json:"items"`
	Page     int                    `json:"page"`
	PageSize int                    `json:"page_size"`
	Next     string                 `json:"next_cursor,omitempty"`
}

type TargetChannelSurface struct {
	ChannelSurfaceID   string                        `json:"channel_surface_id"`
	ChannelAccountID   string                        `json:"channel_account_id"`
	Channel            string                        `json:"channel"`
	SurfaceType        string                        `json:"surface_type"`
	SurfaceKey         string                        `json:"surface_key"`
	Address            json.RawMessage               `json:"address,omitempty"`
	AddressFingerprint string                        `json:"address_fingerprint,omitempty"`
	DisplayState       json.RawMessage               `json:"display_state,omitempty"`
	LifecycleStatus    string                        `json:"lifecycle_status"`
	Version            int64                         `json:"version"`
	Subjects           []TargetChannelSurfaceSubject `json:"subjects,omitempty"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
	LastRenderedAt     *time.Time                    `json:"last_rendered_at,omitempty"`
	SupersededAt       *time.Time                    `json:"superseded_at,omitempty"`
	DeletedAt          *time.Time                    `json:"deleted_at,omitempty"`
}

type TargetChannelSurfaceSubject struct {
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
	SubjectRole string `json:"subject_role"`
}

type TargetSupersedeChannelSurfaceRequest struct {
	SurfaceID string          `json:"channel_surface_id,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	ActorType string          `json:"actor_type"`
	ActorID   string          `json:"actor_id,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type TargetReplaceChannelSurfaceDisplayStateRequest struct {
	SurfaceID       string          `json:"-"`
	ExpectedVersion int64           `json:"expected_version"`
	DisplayState    json.RawMessage `json:"display_state"`
	ActorType       string          `json:"actor_type,omitempty"`
	ActorID         string          `json:"actor_id,omitempty"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
}

type TargetListChannelSurfaceEventsRequest struct {
	SurfaceID string
	Cursor    string
	PageSize  int
}

type TargetChannelSurfaceEvent struct {
	ChannelSurfaceEventID string          `json:"channel_surface_event_id"`
	ChannelSurfaceID      string          `json:"channel_surface_id,omitempty"`
	EventType             string          `json:"event_type"`
	Reason                string          `json:"reason,omitempty"`
	PreviousVersion       int64           `json:"previous_version,omitempty"`
	NextVersion           int64           `json:"next_version,omitempty"`
	ActorType             string          `json:"actor_type"`
	ActorID               string          `json:"actor_id,omitempty"`
	Metadata              json.RawMessage `json:"metadata,omitempty"`
	CreatedAt             time.Time       `json:"created_at"`
}

type TargetChannelSurfaceEventPage struct {
	Items    []TargetChannelSurfaceEvent `json:"items"`
	Page     int                         `json:"page"`
	PageSize int                         `json:"page_size"`
	Next     string                      `json:"next_cursor,omitempty"`
}

func (s *Server) handleResolveTargetChannelAccount(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetChannelAccountRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_channel_account", message: "channel account request must be valid JSON", details: err.Error()})
		return
	}
	account, err := s.deps.Target.ResolveChannelAccount(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_account": account})
}

func (s *Server) handleListTargetChannelAccounts(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListChannelAccounts(r.Context(), TargetListChannelAccountsRequest{Cursor: cursor, PageSize: pageSize})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetChannelAccount{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleUpdateTargetChannelAccount(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetUpdateChannelAccountRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_channel_account", message: "channel account update must be valid JSON", details: err.Error()})
		return
	}
	body.ChannelAccountID = r.PathValue("channel_account_id")
	account, err := s.deps.Target.UpdateChannelAccount(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_account": account})
}

func (s *Server) handleCreateTargetMediaAsset(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetCreateMediaAssetRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "media asset request must be valid JSON", details: err.Error()})
		return
	}
	if body.IdempotencyKey == "" {
		body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	asset, err := s.deps.Target.CreateMediaAsset(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"media_asset": asset})
}

func (s *Server) handleUploadTargetMediaAsset(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes)
	if err := r.ParseMultipartForm(s.maxRequestBytes); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "multipart media asset request must be valid form data", details: err.Error()})
		return
	}
	metadataValue := strings.TrimSpace(r.FormValue("metadata"))
	if metadataValue == "" {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "multipart media asset request must include metadata"})
		return
	}
	var metadata TargetCreateMediaAssetMultipartMetadata
	decoder := json.NewDecoder(strings.NewReader(metadataValue))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "multipart metadata must be valid JSON", details: err.Error()})
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "multipart media asset request must include a file", details: err.Error()})
		return
	}
	defer file.Close()
	body, ok := s.readTargetMultipartUploadBody(w, file)
	if !ok {
		return
	}
	contentType := strings.TrimSpace(header.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	sum := sha256.Sum256(body)
	checksum := fmt.Sprintf("sha256:%x", sum[:])
	filename := strings.TrimSpace(header.Filename)
	if filename == "" {
		filename = "upload.bin"
	}
	storedObjectID := targetUploadStoredObjectID(metadata.ChannelAccountID, filename, checksum)
	objectRef := "sources/uploads/" + storedObjectID + "/" + filename
	displayName := firstNonEmpty(metadata.DisplayName, filename)
	idempotencyKey := firstNonEmpty(metadata.IdempotencyKey, strings.TrimSpace(r.Header.Get("Idempotency-Key")))
	asset, err := s.deps.Target.CreateMediaAsset(r.Context(), TargetCreateMediaAssetRequest{
		ChannelAccountID: metadata.ChannelAccountID,
		Origin: TargetMediaAssetOrigin{
			OriginType:       "upload",
			OriginRef:        objectRef,
			ObjectRef:        objectRef,
			OriginalFilename: filename,
			StoredObjectID:   storedObjectID,
			ContentType:      contentType,
			SizeBytes:        int64(len(body)),
			Checksum:         checksum,
			UploadBody:       body,
		},
		Kind:           metadata.Kind,
		DisplayName:    displayName,
		CollectionID:   metadata.CollectionID,
		Metadata:       metadata.Metadata,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"media_asset": asset})
}

func targetUploadStoredObjectID(channelAccountID, filename, checksum string) string {
	return stableTargetID(strings.Join([]string{"target-upload-sources-v2", channelAccountID, filename, checksum}, ":"))
}

func (s *Server) handleListTargetMediaAssets(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListMediaAssets(r.Context(), TargetListMediaAssetsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetMediaAsset{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetTargetMediaAsset(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	asset, err := s.deps.Target.GetMediaAsset(r.Context(), TargetGetMediaAssetRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		MediaAssetID:     r.PathValue("media_asset_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"media_asset": asset})
}

func (s *Server) handleDeleteTargetMediaAsset(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	asset, err := s.deps.Target.DeleteMediaAsset(r.Context(), TargetDeleteMediaAssetRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		MediaAssetID:     r.PathValue("media_asset_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"media_asset": asset})
}

func (s *Server) handleGetTargetInboxCollection(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	collection, err := s.deps.Target.GetInboxCollection(r.Context(), TargetGetInboxCollectionRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if collection.Items == nil {
		collection.Items = []TargetCollectionItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleCreateTargetCollectionDecoded(w http.ResponseWriter, r *http.Request, body TargetCreateCollectionRequest) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	if body.IdempotencyKey == "" {
		body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	collection, err := s.deps.Target.CreateCollection(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if collection.Items == nil {
		collection.Items = []TargetCollectionItem{}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"collection": collection})
}

func (s *Server) handleCreateTargetCollection(w http.ResponseWriter, r *http.Request) {
	var body TargetCreateCollectionRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_collection", message: "collection request must be valid JSON", details: err.Error()})
		return
	}
	s.handleCreateTargetCollectionDecoded(w, r, body)
}

func (s *Server) handleListTargetCollections(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListCollections(r.Context(), TargetListCollectionsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetCollection{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetTargetCollection(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	collection, err := s.deps.Target.GetCollection(r.Context(), TargetGetCollectionRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		CollectionID:     r.PathValue("collection_id"),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if collection.Items == nil {
		collection.Items = []TargetCollectionItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleUpdateTargetCollectionDecoded(w http.ResponseWriter, r *http.Request, body TargetUpdateCollectionRequest) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	body.CollectionID = r.PathValue("collection_id")
	collection, err := s.deps.Target.UpdateCollection(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if collection.Items == nil {
		collection.Items = []TargetCollectionItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleUpdateTargetCollection(w http.ResponseWriter, r *http.Request) {
	var body TargetUpdateCollectionRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_collection", message: "collection update must be valid JSON", details: err.Error()})
		return
	}
	s.handleUpdateTargetCollectionDecoded(w, r, body)
}

func (s *Server) handleUpdateTargetCollectionItemsDecoded(w http.ResponseWriter, r *http.Request, body TargetUpdateCollectionItemsRequest) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	body.CollectionID = r.PathValue("collection_id")
	collection, err := s.deps.Target.UpdateCollectionItems(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if collection.Items == nil {
		collection.Items = []TargetCollectionItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleUpdateTargetCollectionItems(w http.ResponseWriter, r *http.Request) {
	var body TargetUpdateCollectionItemsRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_collection_items", message: "collection items request must be valid JSON", details: err.Error()})
		return
	}
	s.handleUpdateTargetCollectionItemsDecoded(w, r, body)
}

func (s *Server) handleRemoveTargetCollectionItem(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	expected, _ := strconv.ParseInt(r.URL.Query().Get("expected_version"), 10, 64)
	collection, err := s.deps.Target.RemoveCollectionItem(r.Context(), TargetRemoveCollectionItemRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		CollectionID:     r.PathValue("collection_id"),
		MediaAssetID:     r.PathValue("media_asset_id"),
		ExpectedVersion:  expected,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if collection.Items == nil {
		collection.Items = []TargetCollectionItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleCreateTargetSelectionSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetCreateSelectionSnapshotRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_selection_snapshot", message: "selection snapshot request must be valid JSON", details: err.Error()})
		return
	}
	if body.IdempotencyKey == "" {
		body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	snapshot, err := s.deps.Target.CreateSelectionSnapshot(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"selection_snapshot": snapshot})
}

func (s *Server) handleGetTargetSelectionSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	snapshot, err := s.deps.Target.GetSelectionSnapshot(r.Context(), TargetGetSelectionSnapshotRequest{
		ChannelAccountID:    strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		SelectionSnapshotID: r.PathValue("selection_snapshot_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"selection_snapshot": snapshot})
}

func (s *Server) handleCreateTargetAnalysisRunDecoded(w http.ResponseWriter, r *http.Request, body TargetCreateAnalysisRunRequest) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = body.IdempotencyKey
	}
	run, err := s.deps.Target.CreateAnalysisRun(r.Context(), TargetCreateAnalysisRunRequest{
		ChannelAccountID:    body.ChannelAccountID,
		SelectionSnapshotID: body.SelectionSnapshotID,
		RunType:             body.RunType,
		IdempotencyKey:      idempotencyKey,
		Params:              body.Params,
		Delivery:            body.Delivery,
		CreatedViaChannelID: body.CreatedViaChannelID,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"analysis_run": run})
}

func (s *Server) handleCreateTargetAnalysisRun(w http.ResponseWriter, r *http.Request) {
	var body TargetCreateAnalysisRunRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run", message: "analysis run request must be valid JSON", details: err.Error()})
		return
	}
	s.handleCreateTargetAnalysisRunDecoded(w, r, body)
}

func (s *Server) handleListTargetAnalysisRuns(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListAnalysisRuns(r.Context(), TargetListAnalysisRunsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetAnalysisRun{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetTargetAnalysisRun(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	run, err := s.deps.Target.GetAnalysisRun(r.Context(), TargetGetAnalysisRunRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		AnalysisRunID:    r.PathValue("analysis_run_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis_run": run})
}

func (s *Server) handleCancelTargetAnalysisRunDecoded(w http.ResponseWriter, r *http.Request, body TargetCancelAnalysisRunRequest) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	if body.ChannelAccountID == "" {
		body.ChannelAccountID = strings.TrimSpace(r.URL.Query().Get("channel_account_id"))
	}
	run, err := s.deps.Target.CancelAnalysisRun(r.Context(), r.PathValue("analysis_run_id"), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis_run": run})
}

func (s *Server) handleCancelTargetAnalysisRun(w http.ResponseWriter, r *http.Request) {
	var body TargetCancelAnalysisRunRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_cancel", message: "analysis run cancel request must be valid JSON", details: err.Error()})
		return
	}
	s.handleCancelTargetAnalysisRunDecoded(w, r, body)
}

func (s *Server) handleRetryTargetAnalysisRunDecoded(w http.ResponseWriter, r *http.Request, body TargetRetryAnalysisRunRequest) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	if body.IdempotencyKey == "" {
		body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if body.ChannelAccountID == "" {
		body.ChannelAccountID = strings.TrimSpace(r.URL.Query().Get("channel_account_id"))
	}
	run, err := s.deps.Target.RetryAnalysisRun(r.Context(), r.PathValue("analysis_run_id"), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"analysis_run": run})
}

func (s *Server) handleRetryTargetAnalysisRun(w http.ResponseWriter, r *http.Request) {
	var body TargetRetryAnalysisRunRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_retry", message: "analysis run retry request must be valid JSON", details: err.Error()})
		return
	}
	s.handleRetryTargetAnalysisRunDecoded(w, r, body)
}

func (s *Server) handleListTargetAnalysisRunEvents(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListAnalysisRunEvents(r.Context(), TargetListAnalysisRunEventsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		AnalysisRunID:    r.PathValue("analysis_run_id"),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetAnalysisRunEvent{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleListTargetArtifacts(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListArtifacts(r.Context(), TargetListArtifactsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		AnalysisRunID:    strings.TrimSpace(firstNonEmpty(r.PathValue("analysis_run_id"), r.URL.Query().Get("analysis_run_id"))),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetArtifact{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetTargetArtifact(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	artifact, err := s.deps.Target.GetArtifact(r.Context(), TargetGetArtifactRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		ArtifactID:       r.PathValue("artifact_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifact": artifact})
}

func (s *Server) handleRefreshTargetArtifact(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	artifact, err := s.deps.Target.RefreshArtifactLink(r.Context(), TargetRefreshArtifactRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		ArtifactID:       r.PathValue("artifact_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifact": artifact})
}

func (s *Server) handleListTargetDiagnostics(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListDiagnostics(r.Context(), TargetListDiagnosticsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		SubjectType:      strings.TrimSpace(r.URL.Query().Get("subject_type")),
		SubjectID:        strings.TrimSpace(r.URL.Query().Get("subject_id")),
		Severity:         strings.TrimSpace(r.URL.Query().Get("severity")),
		Code:             strings.TrimSpace(r.URL.Query().Get("code")),
		CorrelationID:    strings.TrimSpace(r.URL.Query().Get("correlation_id")),
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetDiagnostic{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetTargetObservabilitySnapshot(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	snapshot, err := s.deps.Target.GetObservabilitySnapshot(r.Context())
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"observability": snapshot})
}

func (s *Server) handleListTargetAnalysisRunStepQueue(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	response, err := s.deps.Target.ListAnalysisRunStepQueue(r.Context(), TargetAnalysisRunStepQueueRequest{
		Status:     strings.TrimSpace(r.URL.Query().Get("status")),
		RunType:    strings.TrimSpace(r.URL.Query().Get("run_type")),
		WorkerKind: strings.TrimSpace(r.URL.Query().Get("worker_kind")),
		StepKind:   strings.TrimSpace(r.URL.Query().Get("step_kind")),
		PageSize:   parsePositiveQueryInt(r.URL.Query().Get("page_size"), 20),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if response.Items == nil {
		response.Items = []TargetAnalysisRunStepQueueItem{}
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleClaimTargetAnalysisRunStep(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetClaimAnalysisRunStepRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_step_claim", message: "analysis run step claim must be valid JSON", details: err.Error()})
		return
	}
	claim, err := s.deps.Target.ClaimAnalysisRunStep(r.Context(), r.PathValue("analysis_run_id"), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) handleCheckTargetAnalysisRunStepCancel(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	response, err := s.deps.Target.CheckAnalysisRunStepCancel(r.Context(), r.PathValue("analysis_run_id"), TargetCheckAnalysisRunStepCancelRequest{
		AnalysisRunStepID: strings.TrimSpace(r.URL.Query().Get("analysis_run_step_id")),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleResolveTargetAnalysisRunStepRequestAccess(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	stepID := strings.TrimSpace(r.URL.Query().Get("analysis_run_step_id"))
	if stepID == "" {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_request_access", message: "analysis_run_step_id is required"})
		return
	}
	response, err := s.deps.Target.ResolveAnalysisRunStepRequestAccess(r.Context(), r.PathValue("analysis_run_id"), TargetRequestAccessRequest{AnalysisRunStepID: stepID})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleResolveTargetArtifactDownloadAccess(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	response, err := s.deps.Target.ResolveArtifactDownloadAccess(r.Context(), r.PathValue("artifact_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRecordTargetAnalysisRunStepProgress(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetRecordAnalysisRunStepProgressRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_step_progress", message: "analysis run step progress must be valid JSON", details: err.Error()})
		return
	}
	if err := s.deps.Target.RecordAnalysisRunStepProgress(r.Context(), r.PathValue("analysis_run_id"), body); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (s *Server) handleRecordTargetAnalysisRunArtifacts(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetRecordAnalysisRunArtifactsRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_artifacts", message: "analysis run artifacts must be valid JSON", details: err.Error()})
		return
	}
	if err := s.deps.Target.RecordAnalysisRunArtifacts(r.Context(), r.PathValue("analysis_run_id"), body); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (s *Server) handleRecordTargetAnalysisRunDiagnostics(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetRecordAnalysisRunDiagnosticsRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_diagnostics", message: "analysis run diagnostics must be valid JSON", details: err.Error()})
		return
	}
	if err := s.deps.Target.RecordAnalysisRunDiagnostics(r.Context(), r.PathValue("analysis_run_id"), body); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (s *Server) handleFinalizeTargetAnalysisRunStep(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetFinalizeAnalysisRunStepRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run_step_finalize", message: "analysis run step finalize must be valid JSON", details: err.Error()})
		return
	}
	run, err := s.deps.Target.FinalizeAnalysisRunStep(r.Context(), r.PathValue("analysis_run_id"), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis_run": run})
}

func (s *Server) handleUpsertTargetChannelSurface(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetUpsertChannelSurfaceRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_channel_surface", message: "channel surface request must be valid JSON", details: err.Error()})
		return
	}
	if body.IdempotencyKey == "" {
		body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	surface, err := s.deps.Target.UpsertChannelSurface(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_surface": surface})
}

func (s *Server) handleListTargetChannelSurfaces(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListChannelSurfaces(r.Context(), TargetListChannelSurfacesRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		SubjectType:      strings.TrimSpace(r.URL.Query().Get("subject_type")),
		SubjectID:        strings.TrimSpace(r.URL.Query().Get("subject_id")),
		LifecycleStatus:  strings.TrimSpace(r.URL.Query().Get("lifecycle_status")),
		ActiveOnly:       false,
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetChannelSurface{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleListActiveTargetChannelSurfaces(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListChannelSurfaces(r.Context(), TargetListChannelSurfacesRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		SubjectType:      strings.TrimSpace(r.URL.Query().Get("subject_type")),
		SubjectID:        strings.TrimSpace(r.URL.Query().Get("subject_id")),
		LifecycleStatus:  strings.TrimSpace(r.URL.Query().Get("lifecycle_status")),
		ActiveOnly:       true,
		Cursor:           cursor,
		PageSize:         pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetChannelSurface{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleReplaceTargetChannelSurfaceDisplayState(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetReplaceChannelSurfaceDisplayStateRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_channel_surface_display_state", message: "channel surface display state must be valid JSON", details: err.Error()})
		return
	}
	body.SurfaceID = r.PathValue("channel_surface_id")
	surface, err := s.deps.Target.ReplaceChannelSurfaceDisplayState(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_surface": surface})
}

func (s *Server) handleSupersedeTargetChannelSurface(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	var body TargetSupersedeChannelSurfaceRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_channel_surface_supersede", message: "channel surface supersede request must be valid JSON", details: err.Error()})
		return
	}
	body.SurfaceID = r.PathValue("channel_surface_id")
	event, err := s.deps.Target.SupersedeChannelSurface(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"channel_surface_event": event})
}

func (s *Server) handleListTargetChannelSurfaceEvents(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target == nil {
		s.writeAPIError(w, dependencyUnavailableError("target service is not configured"))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	page, err := s.deps.Target.ListChannelSurfaceEvents(r.Context(), TargetListChannelSurfaceEventsRequest{
		SurfaceID: r.PathValue("channel_surface_id"),
		Cursor:    cursor,
		PageSize:  pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	if page.Items == nil {
		page.Items = []TargetChannelSurfaceEvent{}
	}
	writeJSON(w, http.StatusOK, page)
}
