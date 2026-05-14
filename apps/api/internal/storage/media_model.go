package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"mime"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	MediaStatusReady       = "ready"
	MediaStatusQuarantined = "quarantined"
	MediaStatusDeleted     = "deleted"

	CollectionKindInbox = "inbox"
	CollectionKindUser  = "user"

	CollectionStatusActive   = "active"
	CollectionStatusArchived = "archived"
	CollectionStatusDeleted  = "deleted"

	SelectionStatusSealed      = "sealed"
	SelectionStatusInvalidated = "invalidated"

	AnalysisRunStatusQueued             = "queued"
	AnalysisRunStatusRunning            = "running"
	AnalysisRunStatusCancelRequested    = "cancel_requested"
	AnalysisRunStatusSucceeded          = "succeeded"
	AnalysisRunStatusPartiallySucceeded = "partially_succeeded"
	AnalysisRunStatusFailed             = "failed"
	AnalysisRunStatusCanceled           = "canceled"
	AnalysisRunStatusExpired            = "expired"

	AnalysisRunTaskStatusPendingEnqueue = "pending_enqueue"
	AnalysisRunTaskStatusQueued         = "queued"
	AnalysisRunTaskStatusClaimed        = "claimed"
	AnalysisRunTaskStatusSucceeded      = "succeeded"
	AnalysisRunTaskStatusFailed         = "failed"
	AnalysisRunTaskStatusCanceled       = "canceled"

	ArtifactStatusAvailable = "available"
	ArtifactStatusExpired   = "expired"
	ArtifactStatusDeleted   = "deleted"

	RetentionStateActive             = "active"
	RetentionStateSoftDeleted        = "soft_deleted"
	RetentionStateExpired            = "expired"
	RetentionStateHardDeleteEligible = "hard_delete_eligible"
	RetentionStateHeld               = "held"
)

type OwnerScope struct {
	OwnerType       string         `json:"owner_type"`
	OwnerID         string         `json:"owner_id"`
	TenantID        string         `json:"tenant_id,omitempty"`
	AdapterIdentity map[string]any `json:"adapter_identity,omitempty"`
}

func (o OwnerScope) normalized() OwnerScope {
	o.OwnerType = strings.TrimSpace(o.OwnerType)
	o.OwnerID = strings.TrimSpace(o.OwnerID)
	o.TenantID = strings.TrimSpace(o.TenantID)
	return o
}

func (o OwnerScope) key() string {
	n := o.normalized()
	return n.OwnerType + "\x00" + n.OwnerID + "\x00" + n.TenantID
}

func (o OwnerScope) Empty() bool {
	n := o.normalized()
	return n.OwnerType == "" || n.OwnerID == ""
}

func SameOwner(a, b OwnerScope) bool {
	return a.key() == b.key()
}

type RetentionMetadata struct {
	State      string     `json:"state"`
	PolicyID   string     `json:"policy_id,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	DeletedAt  *time.Time `json:"deleted_at,omitempty"`
	HoldReason string     `json:"hold_reason,omitempty"`
}

func activeRetention(retention RetentionMetadata) RetentionMetadata {
	if strings.TrimSpace(retention.State) == "" {
		retention.State = RetentionStateActive
	}
	return retention
}

type MediaSourceMetadata struct {
	SourceID    string     `json:"source_id"`
	OriginType  string     `json:"origin_type"`
	ExternalURI string     `json:"external_uri,omitempty"`
	ObjectKey   string     `json:"object_key,omitempty"`
	TextRef     string     `json:"text_ref,omitempty"`
	Checksum    string     `json:"checksum,omitempty"`
	SizeBytes   *int64     `json:"size_bytes,omitempty"`
	MIMEType    string     `json:"mime_type,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

type AddMediaSource struct {
	OriginType       string
	Text             string
	URL              string
	ObjectRef        string
	OriginalFilename string
	ContentType      string
	SizeBytes        int64
	Checksum         string
	UploadBody       []byte
}

type AddMediaItemRequest struct {
	Owner         OwnerScope
	Kind          string
	Source        AddMediaSource
	CollectionID  string
	DisplayName   string
	AdapterOrigin string
	MetadataJSON  []byte
	Retention     RetentionMetadata
}

type MediaItemRecord struct {
	ID            string              `json:"media_item_id"`
	Owner         OwnerScope          `json:"owner"`
	Source        MediaSourceMetadata `json:"source"`
	Kind          string              `json:"kind"`
	Status        string              `json:"status"`
	DisplayName   string              `json:"display_name"`
	AdapterOrigin string              `json:"adapter_origin,omitempty"`
	MetadataJSON  []byte              `json:"metadata,omitempty"`
	Retention     RetentionMetadata   `json:"retention"`
	Diagnostics   []DiagnosticRecord  `json:"diagnostics,omitempty"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
	DeletedAt     *time.Time          `json:"deleted_at,omitempty"`
}

type CollectionItemRecord struct {
	MediaItemID string           `json:"media_item_id"`
	Position    int              `json:"position"`
	MediaItem   *MediaItemRecord `json:"media_item,omitempty"`
	AddedBy     string           `json:"added_by,omitempty"`
	AddedAt     time.Time        `json:"added_at"`
	RemovedAt   *time.Time       `json:"removed_at,omitempty"`
}

type CollectionRecord struct {
	ID         string                 `json:"collection_id"`
	Owner      OwnerScope             `json:"owner"`
	Kind       string                 `json:"kind"`
	Name       string                 `json:"name"`
	Status     string                 `json:"status"`
	Version    int64                  `json:"version"`
	Items      []CollectionItemRecord `json:"items"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
	ArchivedAt *time.Time             `json:"archived_at,omitempty"`
	DeletedAt  *time.Time             `json:"deleted_at,omitempty"`
}

type CreateCollectionRequest struct {
	Owner OwnerScope
	Name  string
	Items []string
}

type UpdateCollectionRequest struct {
	CollectionID    string
	Owner           OwnerScope
	ExpectedVersion int64
	Name            string
	Status          string
}

type UpdateCollectionItemsRequest struct {
	CollectionID    string
	Owner           OwnerScope
	ExpectedVersion int64
	Items           []CollectionItemRecord
	AddedBy         string
}

type SelectionItemSnapshot struct {
	ID                string              `json:"selection_item_id"`
	Position          int                 `json:"position"`
	MediaItemID       string              `json:"media_item_id"`
	Kind              string              `json:"kind"`
	SourceSnapshot    MediaSourceMetadata `json:"source_snapshot"`
	DisplayName       string              `json:"display_name"`
	StatusAtSelection string              `json:"status_at_selection"`
	MetadataJSON      []byte              `json:"metadata_snapshot,omitempty"`
	RetentionSnapshot RetentionMetadata   `json:"retention_snapshot"`
	Diagnostics       []DiagnosticRecord  `json:"diagnostics,omitempty"`
}

type CreateSelectionRequest struct {
	Owner              OwnerScope
	SourceCollectionID string
	Items              []CollectionItemRecord
	OptionSnapshotJSON []byte
	DuplicatePolicy    string
	CreatedBy          string
}

type SelectionRecord struct {
	ID                 string                  `json:"selection_id"`
	Owner              OwnerScope              `json:"owner"`
	Status             string                  `json:"status"`
	SourceCollectionID string                  `json:"source_collection_id,omitempty"`
	Items              []SelectionItemSnapshot `json:"items"`
	OptionSnapshotJSON []byte                  `json:"option_snapshot"`
	CreatedBy          string                  `json:"created_by"`
	Diagnostics        []DiagnosticRecord      `json:"diagnostics,omitempty"`
	CreatedAt          time.Time               `json:"created_at"`
	SealedAt           time.Time               `json:"sealed_at"`
}

type CreateAnalysisRunRequest struct {
	Owner          OwnerScope
	SelectionID    string
	RunType        string
	ParamsJSON     []byte
	DeliveryJSON   []byte
	IdempotencyKey string
}

type AnalysisRunRecord struct {
	ID                string             `json:"analysis_run_id"`
	Owner             OwnerScope         `json:"owner"`
	SelectionID       string             `json:"selection_id"`
	Selection         SelectionRecord    `json:"selection"`
	RunType           string             `json:"run_type"`
	Status            string             `json:"status"`
	Version           int64              `json:"version"`
	ParamsJSON        []byte             `json:"params,omitempty"`
	DeliveryJSON      []byte             `json:"delivery"`
	EvidenceGateState string             `json:"evidence_gate_state"`
	Artifacts         []ArtifactRecord   `json:"artifacts"`
	Diagnostics       []DiagnosticRecord `json:"diagnostics,omitempty"`
	CreatedAt         time.Time          `json:"created_at"`
	StartedAt         *time.Time         `json:"started_at,omitempty"`
	CompletedAt       *time.Time         `json:"completed_at,omitempty"`
	CanceledAt        *time.Time         `json:"canceled_at,omitempty"`
	ExpiresAt         *time.Time         `json:"expires_at,omitempty"`
}

type AnalysisRunTaskRecord struct {
	ID            string
	AnalysisRunID string
	WorkerKind    string
	TaskType      string
	Status        string
	AttemptNo     int
	LeaseOwner    string
	ClaimedAt     *time.Time
	HeartbeatAt   *time.Time
	FinalizedAt   *time.Time
	CreatedAt     time.Time
}

type AnalysisRunQueueRecord struct {
	AnalysisRunID string    `json:"analysis_run_id"`
	RunType       string    `json:"run_type"`
	WorkerKind    string    `json:"worker_kind"`
	TaskType      string    `json:"task_type"`
	Status        string    `json:"status"`
	Version       int64     `json:"version"`
	AttemptNo     int       `json:"attempt_no"`
	CreatedAt     time.Time `json:"created_at"`
}

type RunEventRecord struct {
	ID            string          `json:"event_id"`
	AnalysisRunID string          `json:"analysis_run_id"`
	EventType     string          `json:"event_type"`
	Version       int64           `json:"version"`
	PayloadJSON   json.RawMessage `json:"payload"`
	Status        string          `json:"status,omitempty"`
	CreatedAt     time.Time       `json:"emitted_at"`
}

type DiagnosticRecord struct {
	ID              string     `json:"diagnostic_id"`
	Owner           OwnerScope `json:"owner,omitempty"`
	SubjectType     string     `json:"subject_type,omitempty"`
	SubjectID       string     `json:"subject_id,omitempty"`
	Severity        string     `json:"severity"`
	Code            string     `json:"code"`
	Message         string     `json:"message"`
	ContextJSON     []byte     `json:"context,omitempty"`
	SafeAdapterJSON []byte     `json:"safe_adapter_context,omitempty"`
	CorrelationID   string     `json:"correlation_id,omitempty"`
	RemediationHint string     `json:"remediation_hint,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

type DiagnosticQuery struct {
	SubjectType   string
	SubjectID     string
	Severity      string
	Code          string
	CorrelationID string
}

type MediaStateStore interface {
	AddMediaItem(ctx context.Context, item MediaItemRecord, inbox CollectionRecord, targetCollectionID string) (MediaItemRecord, CollectionRecord, error)
	ListMediaItems(ctx context.Context, owner OwnerScope) ([]MediaItemRecord, error)
	GetMediaItem(ctx context.Context, owner OwnerScope, mediaItemID string) (MediaItemRecord, error)
	SoftDeleteMediaItem(ctx context.Context, owner OwnerScope, mediaItemID string, deletedAt time.Time) (MediaItemRecord, error)
	CreateCollection(ctx context.Context, collection CollectionRecord, itemIDs []string) (CollectionRecord, error)
	ListCollections(ctx context.Context, owner OwnerScope) ([]CollectionRecord, error)
	GetCollection(ctx context.Context, owner OwnerScope, collectionID string) (CollectionRecord, error)
	UpdateCollection(ctx context.Context, req UpdateCollectionRequest, updatedAt time.Time) (CollectionRecord, error)
	UpdateCollectionItems(ctx context.Context, req UpdateCollectionItemsRequest, updatedAt time.Time) (CollectionRecord, error)
	CreateSelection(ctx context.Context, selection SelectionRecord, requestedItems []CollectionItemRecord) (SelectionRecord, error)
	GetSelection(ctx context.Context, owner OwnerScope, selectionID string) (SelectionRecord, error)
	CreateAnalysisRun(ctx context.Context, run AnalysisRunRecord, task AnalysisRunTaskRecord, event RunEventRecord) (AnalysisRunRecord, error)
	GetAnalysisRunByID(ctx context.Context, analysisRunID string) (AnalysisRunRecord, error)
	GetAnalysisRun(ctx context.Context, owner OwnerScope, analysisRunID string) (AnalysisRunRecord, error)
	ListAnalysisRuns(ctx context.Context, owner OwnerScope) ([]AnalysisRunRecord, error)
	ListRunEvents(ctx context.Context, owner OwnerScope, analysisRunID string) ([]RunEventRecord, error)
	RecordArtifacts(ctx context.Context, owner OwnerScope, analysisRunID string, artifacts []ArtifactRecord, createdAt time.Time) ([]ArtifactRecord, error)
	ListArtifacts(ctx context.Context, owner OwnerScope, analysisRunID string) ([]ArtifactRecord, error)
	GetArtifact(ctx context.Context, owner OwnerScope, artifactID string) (ArtifactRecord, error)
	GetArtifactByID(ctx context.Context, artifactID string) (ArtifactRecord, error)
	RecordDiagnostics(ctx context.Context, owner OwnerScope, analysisRunID string, diagnostics []DiagnosticRecord, createdAt time.Time) ([]DiagnosticRecord, error)
	ListDiagnostics(ctx context.Context, owner OwnerScope, query DiagnosticQuery) ([]DiagnosticRecord, error)
	RecordAnalysisRunProgress(ctx context.Context, owner OwnerScope, analysisRunID string, event RunEventRecord, recordedAt time.Time) (AnalysisRunRecord, error)
	FinalizeAnalysisRunTask(ctx context.Context, owner OwnerScope, analysisRunID, status string, event RunEventRecord, finalizedAt time.Time) (AnalysisRunRecord, error)
	ApplyRetentionPolicies(ctx context.Context, now time.Time) (RetentionSweepResult, error)
	DetectOrphanObjects(ctx context.Context) ([]OrphanObjectRecord, error)
	RecordOrphanObjectCleanup(ctx context.Context, orphan OrphanObjectRecord, deleted bool, message string, now time.Time) error
	ListPendingEnqueueTasks(ctx context.Context, limit int) ([]AnalysisRunTaskRecord, error)
	ListAnalysisRunQueue(ctx context.Context, status, runType, taskType string, limit int) ([]AnalysisRunQueueRecord, error)
	ListOperationalDiagnostics(ctx context.Context, codes []string) ([]DiagnosticRecord, error)
	MarkAnalysisRunTaskQueued(ctx context.Context, analysisRunID, taskType string, queuedAt time.Time) error
	ClaimAnalysisRunTask(ctx context.Context, analysisRunID, workerKind, taskType, leaseOwner string, claimedAt time.Time) (AnalysisRunRecord, bool, error)
}

func (r *Repository) AddMediaItem(ctx context.Context, req AddMediaItemRequest) (MediaItemRecord, error) {
	now := r.now()
	if req.Owner.Empty() {
		return MediaItemRecord{}, fmt.Errorf("%w: owner is required", ErrContractViolation)
	}
	kind := strings.TrimSpace(req.Kind)
	source := req.Source
	originType := strings.TrimSpace(source.OriginType)
	if kind == "" || originType == "" {
		return MediaItemRecord{}, fmt.Errorf("%w: kind and source origin_type are required", ErrContractViolation)
	}
	sourceID := r.nextID()
	sourceMeta := MediaSourceMetadata{
		SourceID:   sourceID,
		OriginType: originType,
		Checksum:   strings.TrimSpace(source.Checksum),
	}
	displayName := strings.TrimSpace(req.DisplayName)
	switch originType {
	case "text":
		if strings.TrimSpace(source.Text) == "" {
			return MediaItemRecord{}, fmt.Errorf("%w: text source is required", ErrContractViolation)
		}
		sourceMeta.TextRef = fmt.Sprintf("inline:%s", sourceID)
		if displayName == "" {
			displayName = "Text"
		}
	case "url":
		if strings.TrimSpace(source.URL) == "" {
			return MediaItemRecord{}, fmt.Errorf("%w: url source is required", ErrContractViolation)
		}
		sourceMeta.ExternalURI = strings.TrimSpace(source.URL)
		if displayName == "" {
			displayName = sourceMeta.ExternalURI
		}
	case "object":
		objectRef := strings.TrimSpace(source.ObjectRef)
		contentType := strings.TrimSpace(source.ContentType)
		if objectRef != "" && len(source.UploadBody) > 0 {
			return MediaItemRecord{}, fmt.Errorf("%w: object sources must provide object_ref or upload_body, not both", ErrContractViolation)
		}
		if objectRef == "" && len(source.UploadBody) == 0 {
			return MediaItemRecord{}, fmt.Errorf("%w: object_ref or upload_body is required", ErrContractViolation)
		}
		if strings.HasPrefix(objectRef, "telegram://file/") {
			return MediaItemRecord{}, fmt.Errorf("%w: telegram file references must be uploaded before persistence", ErrContractViolation)
		}
		if objectRef != "" {
			sourceMeta.ObjectKey = objectRef
			sourceMeta.MIMEType = contentType
			if source.SizeBytes > 0 {
				size := source.SizeBytes
				sourceMeta.SizeBytes = &size
			}
		} else {
			if contentType == "" {
				contentType = "application/octet-stream"
			}
			size := int64(len(source.UploadBody))
			if source.SizeBytes > 0 && source.SizeBytes != size {
				return MediaItemRecord{}, fmt.Errorf("%w: size_bytes does not match uploaded body", ErrContractViolation)
			}
			objectKey := buildSourceObjectKey(sourceID, source.OriginalFilename, contentType)
			if err := r.objects.PutObject(ctx, SourcesBucket, objectKey, contentType, source.UploadBody); err != nil {
				return MediaItemRecord{}, fmt.Errorf("%w: persist source object: %v", ErrStorageUnavailable, err)
			}
			sourceMeta.ObjectKey = objectKey
			sourceMeta.MIMEType = contentType
			sourceMeta.SizeBytes = &size
			if sourceMeta.Checksum == "" {
				sourceMeta.Checksum = checksumBytes(source.UploadBody)
			}
		}
		if displayName == "" {
			displayName = strings.TrimSpace(source.OriginalFilename)
		}
	default:
		return MediaItemRecord{}, fmt.Errorf("%w: unsupported source origin_type %q", ErrContractViolation, originType)
	}
	if displayName == "" {
		displayName = kind
	}
	item := MediaItemRecord{
		ID:            r.nextID(),
		Owner:         req.Owner.normalized(),
		Source:        sourceMeta,
		Kind:          kind,
		Status:        MediaStatusReady,
		DisplayName:   displayName,
		AdapterOrigin: strings.TrimSpace(req.AdapterOrigin),
		MetadataJSON:  normalizeJSON(req.MetadataJSON),
		Retention:     activeRetention(req.Retention),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	inbox := CollectionRecord{
		ID:        r.nextID(),
		Owner:     item.Owner,
		Kind:      CollectionKindInbox,
		Name:      "Inbox",
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	created, _, err := r.state.AddMediaItem(ctx, item, inbox, strings.TrimSpace(req.CollectionID))
	return created, err
}

func buildSourceObjectKey(sourceID, originalFilename, contentType string) string {
	filename := sanitizeSourceFilename(originalFilename, contentType)
	return "sources/" + sourceID + "/" + filename
}

func sanitizeSourceFilename(originalFilename, contentType string) string {
	filename := strings.TrimSpace(originalFilename)
	if filename != "" {
		filename = path.Base(strings.ReplaceAll(filename, "\\", "/"))
		filename = strings.TrimSpace(strings.NewReplacer("/", "-", "\\", "-", "\x00", "").Replace(filename))
	}
	if filename != "" && filename != "." {
		return filename
	}
	if extensions, _ := mime.ExtensionsByType(contentType); len(extensions) > 0 {
		return "source" + extensions[0]
	}
	return "source.bin"
}

func checksumBytes(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("sha256:%x", sum)
}

func artifactObjectStoreKey(objectKey string) string {
	return strings.TrimSpace(objectKey)
}

func (r *Repository) ListMediaItems(ctx context.Context, owner OwnerScope) ([]MediaItemRecord, error) {
	return r.state.ListMediaItems(ctx, owner.normalized())
}

func (r *Repository) GetMediaItem(ctx context.Context, owner OwnerScope, mediaItemID string) (MediaItemRecord, error) {
	return r.state.GetMediaItem(ctx, owner.normalized(), strings.TrimSpace(mediaItemID))
}

func (r *Repository) RemoveMediaItem(ctx context.Context, owner OwnerScope, mediaItemID string) (MediaItemRecord, error) {
	return r.state.SoftDeleteMediaItem(ctx, owner.normalized(), strings.TrimSpace(mediaItemID), r.now())
}

func (r *Repository) CancelAnalysisRun(ctx context.Context, owner OwnerScope, analysisRunID, message string) (AnalysisRunRecord, error) {
	return r.FinalizeAnalysisRunTask(ctx, owner, analysisRunID, AnalysisRunStatusCanceled, message)
}

func (r *Repository) RetryAnalysisRun(ctx context.Context, owner OwnerScope, analysisRunID, idempotencyKey string) (AnalysisRunRecord, error) {
	previous, err := r.GetAnalysisRun(ctx, owner, analysisRunID)
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	if !terminalRunStatus(previous.Status) {
		return AnalysisRunRecord{}, ErrRetryRequiresTerminalRun
	}
	return r.CreateAnalysisRun(ctx, CreateAnalysisRunRequest{
		Owner:          previous.Owner,
		SelectionID:    previous.SelectionID,
		RunType:        previous.RunType,
		ParamsJSON:     previous.ParamsJSON,
		DeliveryJSON:   previous.DeliveryJSON,
		IdempotencyKey: strings.TrimSpace(idempotencyKey),
	})
}

func (r *Repository) CreateCollection(ctx context.Context, req CreateCollectionRequest) (CollectionRecord, error) {
	if req.Owner.Empty() || strings.TrimSpace(req.Name) == "" {
		return CollectionRecord{}, fmt.Errorf("%w: owner and name are required", ErrContractViolation)
	}
	now := r.now()
	collection := CollectionRecord{
		ID:        r.nextID(),
		Owner:     req.Owner.normalized(),
		Kind:      CollectionKindUser,
		Name:      strings.TrimSpace(req.Name),
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return r.state.CreateCollection(ctx, collection, req.Items)
}

func (r *Repository) ListCollections(ctx context.Context, owner OwnerScope) ([]CollectionRecord, error) {
	return r.state.ListCollections(ctx, owner.normalized())
}

func (r *Repository) GetCollection(ctx context.Context, owner OwnerScope, collectionID string) (CollectionRecord, error) {
	return r.state.GetCollection(ctx, owner.normalized(), strings.TrimSpace(collectionID))
}

func (r *Repository) UpdateCollection(ctx context.Context, req UpdateCollectionRequest) (CollectionRecord, error) {
	req.Owner = req.Owner.normalized()
	return r.state.UpdateCollection(ctx, req, r.now())
}

func (r *Repository) UpdateCollectionItems(ctx context.Context, req UpdateCollectionItemsRequest) (CollectionRecord, error) {
	req.Owner = req.Owner.normalized()
	return r.state.UpdateCollectionItems(ctx, req, r.now())
}

func (r *Repository) CreateSelection(ctx context.Context, req CreateSelectionRequest) (SelectionRecord, error) {
	if req.Owner.Empty() || len(req.Items) == 0 {
		return SelectionRecord{}, fmt.Errorf("%w: owner and items are required", ErrContractViolation)
	}
	now := r.now()
	selection := SelectionRecord{
		ID:                 r.nextID(),
		Owner:              req.Owner.normalized(),
		Status:             SelectionStatusSealed,
		SourceCollectionID: strings.TrimSpace(req.SourceCollectionID),
		OptionSnapshotJSON: normalizeJSON(req.OptionSnapshotJSON),
		CreatedBy:          strings.TrimSpace(req.CreatedBy),
		CreatedAt:          now,
		SealedAt:           now,
	}
	if selection.CreatedBy == "" {
		selection.CreatedBy = selection.Owner.OwnerID
	}
	return r.state.CreateSelection(ctx, selection, req.Items)
}

func (r *Repository) GetSelection(ctx context.Context, owner OwnerScope, selectionID string) (SelectionRecord, error) {
	return r.state.GetSelection(ctx, owner.normalized(), strings.TrimSpace(selectionID))
}

func (r *Repository) CreateAnalysisRun(ctx context.Context, req CreateAnalysisRunRequest) (AnalysisRunRecord, error) {
	if req.Owner.Empty() || strings.TrimSpace(req.SelectionID) == "" || strings.TrimSpace(req.RunType) == "" {
		return AnalysisRunRecord{}, fmt.Errorf("%w: owner, selection_id, and run_type are required", ErrContractViolation)
	}
	selection, err := r.state.GetSelection(ctx, req.Owner.normalized(), strings.TrimSpace(req.SelectionID))
	if err != nil {
		return AnalysisRunRecord{}, err
	}
	if selection.Status != SelectionStatusSealed {
		return AnalysisRunRecord{}, ErrSelectionInvalid
	}
	now := r.now()
	runID := r.nextID()
	run := AnalysisRunRecord{
		ID:                runID,
		Owner:             req.Owner.normalized(),
		SelectionID:       strings.TrimSpace(req.SelectionID),
		RunType:           strings.TrimSpace(req.RunType),
		Status:            AnalysisRunStatusQueued,
		Version:           1,
		ParamsJSON:        normalizeJSON(req.ParamsJSON),
		DeliveryJSON:      normalizeDelivery(req.DeliveryJSON),
		EvidenceGateState: "not_required",
		Artifacts:         []ArtifactRecord{},
		CreatedAt:         now,
	}
	task := AnalysisRunTaskRecord{
		ID:            r.nextID(),
		AnalysisRunID: runID,
		WorkerKind:    workerKindForRunType(run.RunType),
		TaskType:      taskTypeForRunType(run.RunType),
		Status:        AnalysisRunTaskStatusPendingEnqueue,
		AttemptNo:     1,
		CreatedAt:     now,
	}
	payload := fmt.Sprintf(`{"analysis_run_id":%q,"status":"queued"}`, runID)
	event := RunEventRecord{
		ID:            r.nextID(),
		AnalysisRunID: runID,
		EventType:     "analysis_run.created",
		Version:       1,
		PayloadJSON:   []byte(payload),
		Status:        AnalysisRunStatusQueued,
		CreatedAt:     now,
	}
	return r.state.CreateAnalysisRun(ctx, run, task, event)
}

func (r *Repository) MarkAnalysisRunTaskQueued(ctx context.Context, analysisRunID, taskType string) error {
	return r.state.MarkAnalysisRunTaskQueued(ctx, strings.TrimSpace(analysisRunID), strings.TrimSpace(taskType), r.now())
}

func (r *Repository) ListPendingEnqueueTasks(ctx context.Context, limit int) ([]AnalysisRunTaskRecord, error) {
	return r.state.ListPendingEnqueueTasks(ctx, limit)
}

func (r *Repository) ListAnalysisRunQueue(ctx context.Context, status, runType, taskType string, limit int) ([]AnalysisRunQueueRecord, error) {
	return r.state.ListAnalysisRunQueue(
		ctx,
		strings.TrimSpace(status),
		strings.TrimSpace(runType),
		strings.TrimSpace(taskType),
		limit,
	)
}

func (r *Repository) ClaimAnalysisRunTask(ctx context.Context, analysisRunID, workerKind, taskType, leaseOwner string) (AnalysisRunRecord, bool, error) {
	return r.state.ClaimAnalysisRunTask(ctx, strings.TrimSpace(analysisRunID), strings.TrimSpace(workerKind), strings.TrimSpace(taskType), strings.TrimSpace(leaseOwner), r.now())
}

func (r *Repository) GetAnalysisRunByID(ctx context.Context, analysisRunID string) (AnalysisRunRecord, error) {
	return r.state.GetAnalysisRunByID(ctx, strings.TrimSpace(analysisRunID))
}

func (r *Repository) GetAnalysisRun(ctx context.Context, owner OwnerScope, analysisRunID string) (AnalysisRunRecord, error) {
	return r.state.GetAnalysisRun(ctx, owner.normalized(), strings.TrimSpace(analysisRunID))
}

func (r *Repository) ListAnalysisRuns(ctx context.Context, owner OwnerScope) ([]AnalysisRunRecord, error) {
	return r.state.ListAnalysisRuns(ctx, owner.normalized())
}

func (r *Repository) ListAnalysisRunEvents(ctx context.Context, owner OwnerScope, analysisRunID string) ([]RunEventRecord, error) {
	return r.state.ListRunEvents(ctx, owner.normalized(), strings.TrimSpace(analysisRunID))
}

func (r *Repository) RecordArtifacts(ctx context.Context, owner OwnerScope, analysisRunID string, artifacts []ArtifactRecord) ([]ArtifactRecord, error) {
	owner = owner.normalized()
	analysisRunID = strings.TrimSpace(analysisRunID)
	if owner.Empty() || analysisRunID == "" {
		return nil, fmt.Errorf("%w: owner and analysis_run_id are required", ErrContractViolation)
	}
	now := r.now()
	normalized := make([]ArtifactRecord, 0, len(artifacts))
	for _, artifact := range artifacts {
		artifact.ID = strings.TrimSpace(artifact.ID)
		if artifact.ID == "" {
			artifact.ID = r.nextID()
		}
		artifact.Owner = owner
		artifact.AnalysisRunID = analysisRunID
		artifact.Kind = strings.TrimSpace(artifact.Kind)
		artifact.Status = strings.TrimSpace(artifact.Status)
		artifact.ObjectKey = strings.TrimSpace(artifact.ObjectKey)
		artifact.ContentType = strings.TrimSpace(artifact.ContentType)
		artifact.Checksum = strings.TrimSpace(artifact.Checksum)
		artifact.Visibility = strings.TrimSpace(artifact.Visibility)
		if artifact.Status == "" {
			artifact.Status = "available"
		}
		if artifact.Visibility == "" {
			artifact.Visibility = "owner"
		}
		if artifact.ContentType == "" {
			artifact.ContentType = "application/octet-stream"
		}
		if len(artifact.PreviewJSON) == 0 {
			artifact.PreviewJSON = []byte(`{"available":false}`)
		}
		artifact.Retention = activeRetention(artifact.Retention)
		if artifact.CreatedAt.IsZero() {
			artifact.CreatedAt = now
		}
		normalized = append(normalized, artifact)
	}
	return r.state.RecordArtifacts(ctx, owner, analysisRunID, normalized, now)
}

func (r *Repository) ListArtifacts(ctx context.Context, owner OwnerScope, analysisRunID string) ([]ArtifactRecord, error) {
	return r.state.ListArtifacts(ctx, owner.normalized(), strings.TrimSpace(analysisRunID))
}

func (r *Repository) GetArtifact(ctx context.Context, owner OwnerScope, artifactID string) (ArtifactRecord, error) {
	artifact, err := r.state.GetArtifact(ctx, owner.normalized(), strings.TrimSpace(artifactID))
	if err != nil {
		return ArtifactRecord{}, err
	}
	if artifact.Status == "available" && artifact.ObjectKey != "" {
		url, expiresAt, err := r.objects.PresignGetObject(ctx, ArtifactsBucket, artifactObjectStoreKey(artifact.ObjectKey), r.presignTTL)
		if err != nil {
			_, _ = r.RecordDiagnostics(ctx, artifact.Owner, artifact.AnalysisRunID, []DiagnosticRecord{{
				SubjectType: "artifact",
				SubjectID:   artifact.ID,
				Severity:    "error",
				Code:        "artifact_resolution_failed",
				Message:     "artifact download link could not be resolved",
				ContextJSON: []byte(fmt.Sprintf(`{"artifact_id":%q,"analysis_run_id":%q,"object_key":%q}`, artifact.ID, artifact.AnalysisRunID, artifact.ObjectKey)),
			}})
			return ArtifactRecord{}, fmt.Errorf("%w: %v", ErrArtifactResolutionFailed, err)
		}
		artifact.Download = &DownloadDescriptor{
			Provider:  "object_store",
			URL:       url,
			ExpiresAt: expiresAt,
		}
	}
	return artifact, nil
}

func (r *Repository) GetInternalArtifactDownloadAccess(ctx context.Context, artifactID string) (ArtifactRecord, error) {
	artifact, err := r.state.GetArtifactByID(ctx, strings.TrimSpace(artifactID))
	if err != nil {
		return ArtifactRecord{}, err
	}
	if artifact.Status == "available" && artifact.ObjectKey != "" {
		internalPresigner, ok := r.objects.(internalObjectPresigner)
		if !ok {
			return ArtifactRecord{}, fmt.Errorf("%w: object store does not support internal artifact download access", ErrContractViolation)
		}
		url, expiresAt, err := internalPresigner.PresignInternalGetObject(
			ctx,
			ArtifactsBucket,
			artifactObjectStoreKey(artifact.ObjectKey),
			r.presignTTL,
		)
		if err != nil {
			_, _ = r.RecordDiagnostics(ctx, artifact.Owner, artifact.AnalysisRunID, []DiagnosticRecord{{
				SubjectType: "artifact",
				SubjectID:   artifact.ID,
				Severity:    "error",
				Code:        "artifact_resolution_failed",
				Message:     "internal artifact download link could not be resolved",
				ContextJSON: []byte(fmt.Sprintf(`{"artifact_id":%q,"analysis_run_id":%q,"object_key":%q}`, artifact.ID, artifact.AnalysisRunID, artifact.ObjectKey)),
			}})
			return ArtifactRecord{}, fmt.Errorf("%w: %v", ErrArtifactResolutionFailed, err)
		}
		artifact.Download = &DownloadDescriptor{
			Provider:  "object_store",
			URL:       url,
			ExpiresAt: expiresAt,
		}
	}
	return artifact, nil
}

func (r *Repository) ListDiagnostics(ctx context.Context, owner OwnerScope, query DiagnosticQuery) ([]DiagnosticRecord, error) {
	return r.state.ListDiagnostics(ctx, owner.normalized(), DiagnosticQuery{
		SubjectType:   strings.TrimSpace(query.SubjectType),
		SubjectID:     strings.TrimSpace(query.SubjectID),
		Severity:      strings.TrimSpace(query.Severity),
		Code:          strings.TrimSpace(query.Code),
		CorrelationID: strings.TrimSpace(query.CorrelationID),
	})
}

func (r *Repository) RefreshArtifactLink(ctx context.Context, owner OwnerScope, artifactID string) (ArtifactRecord, error) {
	return r.GetArtifact(ctx, owner, artifactID)
}

func (r *Repository) RecordDiagnostics(ctx context.Context, owner OwnerScope, analysisRunID string, diagnostics []DiagnosticRecord) ([]DiagnosticRecord, error) {
	owner = owner.normalized()
	analysisRunID = strings.TrimSpace(analysisRunID)
	if owner.Empty() || analysisRunID == "" {
		return nil, fmt.Errorf("%w: owner and analysis_run_id are required", ErrContractViolation)
	}
	if !isUUID(analysisRunID) {
		return nil, fmt.Errorf("%w: analysis_run_id must be a UUID", ErrContractViolation)
	}
	now := r.now()
	normalized := make([]DiagnosticRecord, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		workerID := strings.TrimSpace(diagnostic.ID)
		if workerID == "" || !isUUID(workerID) {
			diagnostic.ID = r.nextID()
		} else {
			diagnostic.ID = workerID
		}
		diagnostic.Owner = owner
		diagnostic.SubjectType = strings.TrimSpace(diagnostic.SubjectType)
		diagnostic.SubjectID = strings.TrimSpace(diagnostic.SubjectID)
		diagnostic.Severity = strings.TrimSpace(diagnostic.Severity)
		diagnostic.Code = strings.TrimSpace(diagnostic.Code)
		diagnostic.Message = strings.TrimSpace(diagnostic.Message)
		diagnostic.CorrelationID = strings.TrimSpace(diagnostic.CorrelationID)
		diagnostic.RemediationHint = strings.TrimSpace(diagnostic.RemediationHint)
		if diagnostic.Severity == "" {
			diagnostic.Severity = "warning"
		}
		if diagnostic.Code == "" {
			diagnostic.Code = "worker_diagnostic"
		}
		if diagnostic.Message == "" {
			diagnostic.Message = diagnostic.Code
		}
		contextFields := map[string]any{"analysis_run_id": analysisRunID}
		if workerID != "" && workerID != diagnostic.ID {
			contextFields["worker_diagnostic_id"] = workerID
		}
		if diagnostic.SubjectType == "" || diagnostic.SubjectID == "" || !isUUID(diagnostic.SubjectID) {
			if diagnostic.SubjectType != "" {
				contextFields["original_subject_type"] = diagnostic.SubjectType
			}
			if diagnostic.SubjectID != "" {
				contextFields["original_subject_id"] = diagnostic.SubjectID
			}
			diagnostic.SubjectType = "analysis_run"
			diagnostic.SubjectID = analysisRunID
		}
		diagnostic.ContextJSON = mergeJSONObject(diagnostic.ContextJSON, contextFields)
		diagnostic.SafeAdapterJSON = normalizeJSON(diagnostic.SafeAdapterJSON)
		if diagnostic.CreatedAt.IsZero() {
			diagnostic.CreatedAt = now
		}
		normalized = append(normalized, diagnostic)
	}
	return r.state.RecordDiagnostics(ctx, owner, analysisRunID, normalized, now)
}

func (r *Repository) RecordAnalysisRunProgress(ctx context.Context, owner OwnerScope, analysisRunID, stage, message string, payload json.RawMessage) (AnalysisRunRecord, error) {
	owner = owner.normalized()
	analysisRunID = strings.TrimSpace(analysisRunID)
	stage = strings.TrimSpace(stage)
	if owner.Empty() || analysisRunID == "" || stage == "" {
		return AnalysisRunRecord{}, fmt.Errorf("%w: owner, analysis_run_id, and stage are required", ErrContractViolation)
	}
	now := r.now()
	eventPayload := map[string]any{"analysis_run_id": analysisRunID, "stage": stage}
	if strings.TrimSpace(message) != "" {
		eventPayload["message"] = strings.TrimSpace(message)
	}
	if json.Valid(payload) && len(payload) > 0 {
		var raw any
		if err := json.Unmarshal(payload, &raw); err == nil {
			eventPayload["payload"] = raw
		}
	}
	data, _ := json.Marshal(eventPayload)
	event := RunEventRecord{
		ID:            r.nextID(),
		AnalysisRunID: analysisRunID,
		EventType:     "analysis_run.progress",
		PayloadJSON:   data,
		Status:        AnalysisRunStatusRunning,
		CreatedAt:     now,
	}
	return r.state.RecordAnalysisRunProgress(ctx, owner, analysisRunID, event, now)
}

func (r *Repository) FinalizeAnalysisRunTask(ctx context.Context, owner OwnerScope, analysisRunID, status, message string) (AnalysisRunRecord, error) {
	owner = owner.normalized()
	analysisRunID = strings.TrimSpace(analysisRunID)
	status = strings.TrimSpace(status)
	if owner.Empty() || analysisRunID == "" || !terminalRunStatus(status) || status == AnalysisRunStatusExpired {
		return AnalysisRunRecord{}, fmt.Errorf("%w: owner, analysis_run_id, and terminal status are required", ErrContractViolation)
	}
	now := r.now()
	eventPayload := map[string]any{"analysis_run_id": analysisRunID, "status": status}
	if strings.TrimSpace(message) != "" {
		eventPayload["message"] = strings.TrimSpace(message)
	}
	data, _ := json.Marshal(eventPayload)
	event := RunEventRecord{
		ID:            r.nextID(),
		AnalysisRunID: analysisRunID,
		EventType:     "analysis_run." + status,
		PayloadJSON:   data,
		Status:        status,
		CreatedAt:     now,
	}
	return r.state.FinalizeAnalysisRunTask(ctx, owner, analysisRunID, status, event, now)
}

func (r *Repository) ApplyRetentionPolicies(ctx context.Context) (RetentionSweepResult, error) {
	return r.state.ApplyRetentionPolicies(ctx, r.now())
}

func (r *Repository) DetectOrphanObjects(ctx context.Context) ([]OrphanObjectRecord, error) {
	return r.state.DetectOrphanObjects(ctx)
}

func (r *Repository) CleanOrphanObjects(ctx context.Context) (OrphanCleanupResult, error) {
	orphans, err := r.state.DetectOrphanObjects(ctx)
	if err != nil {
		return OrphanCleanupResult{}, err
	}
	result := OrphanCleanupResult{Detected: len(orphans)}
	deleter, canDelete := r.objects.(ObjectDeleter)
	for _, orphan := range orphans {
		deleted := false
		message := "object deletion is not supported by configured object store; metadata diagnostic recorded"
		if canDelete {
			if err := deleter.DeleteObject(ctx, orphan.Bucket, orphan.ObjectKey); err != nil {
				result.DeleteFailures++
				message = fmt.Sprintf("object delete failed: %v", err)
			} else {
				deleted = true
				message = "object deleted from object store"
			}
		}
		if err := r.state.RecordOrphanObjectCleanup(ctx, orphan, deleted, message, r.now()); err != nil {
			return result, err
		}
		result.DiagnosticsRecorded++
		if deleted {
			result.Deleted++
		} else {
			result.MetadataOnly++
		}
	}
	return result, nil
}

func (r *Repository) GetObservabilitySnapshot(ctx context.Context) (ObservabilitySnapshot, error) {
	now := r.now()
	windowStart := now.Add(-15 * time.Minute)
	queueRecords, err := r.state.ListAnalysisRunQueue(ctx, "", "", "", 1000)
	if err != nil {
		return ObservabilitySnapshot{}, err
	}
	snapshot := ObservabilitySnapshot{
		QueueTasks:                 len(queueRecords),
		ObservabilityWindowSeconds: int64((15 * time.Minute).Seconds()),
		GeneratedAt:                now,
	}
	for _, record := range queueRecords {
		if record.CreatedAt.IsZero() || record.CreatedAt.After(now) {
			continue
		}
		lag := int64(now.Sub(record.CreatedAt).Seconds())
		if lag > snapshot.QueueLagSeconds {
			snapshot.QueueLagSeconds = lag
		}
	}
	diagnostics, err := r.state.ListOperationalDiagnostics(ctx, []string{"orphan_object_cleanup_failed", "artifact_resolution_failed"})
	if err != nil {
		return ObservabilitySnapshot{}, err
	}
	for _, diagnostic := range diagnostics {
		isRecent := !diagnostic.CreatedAt.IsZero() && !diagnostic.CreatedAt.Before(windowStart) && !diagnostic.CreatedAt.After(now)
		switch diagnostic.Code {
		case "orphan_object_cleanup_failed":
			snapshot.CleanupFailures++
			if isRecent {
				snapshot.CleanupFailuresRecent++
			}
		case "artifact_resolution_failed":
			snapshot.ArtifactResolutionFailures++
			if isRecent {
				snapshot.ArtifactResolutionFailuresRecent++
			}
		}
	}
	return snapshot, nil
}

func normalizeJSON(data []byte) []byte {
	if len(data) == 0 {
		return []byte(`{}`)
	}
	if !json.Valid(data) {
		return []byte(`{}`)
	}
	return data
}

func normalizeDelivery(data []byte) []byte {
	if len(data) == 0 {
		return []byte(`{"strategy":"polling"}`)
	}
	if !json.Valid(data) {
		return []byte(`{"strategy":"polling"}`)
	}
	return data
}

func mergeJSONObject(data []byte, fields map[string]any) []byte {
	merged := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(normalizeJSON(data), &merged)
	}
	for key, value := range fields {
		if _, exists := merged[key]; !exists {
			merged[key] = value
		}
	}
	encoded, err := json.Marshal(merged)
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}

func isUUID(value string) bool {
	_, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil
}

func terminalRunStatus(status string) bool {
	switch status {
	case AnalysisRunStatusSucceeded, AnalysisRunStatusPartiallySucceeded, AnalysisRunStatusFailed, AnalysisRunStatusCanceled, AnalysisRunStatusExpired:
		return true
	default:
		return false
	}
}

func workerKindForRunType(runType string) string {
	if runType == "transcription" {
		return "transcription"
	}
	return "analysis_runner"
}

func taskTypeForRunType(runType string) string {
	if runType == "transcription" {
		return "selection.transcription"
	}
	return "selection.analysis"
}
