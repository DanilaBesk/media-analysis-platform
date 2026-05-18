package api

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
	"github.com/danila/media-analysis-platform/apps/api/internal/ws"
)

type publicRuntimeService struct {
	store finalRuntimeStorageService
	queue *queue.Publisher
}

type workerRuntimeService struct {
	store finalRuntimeStorageService
}

func NewRuntimeDependencies(storageService *storage.Repository, publisher *queue.Publisher, _ *ws.Service, websocket WebsocketAcceptor) (Dependencies, error) {
	return NewRuntimeDependenciesWithTarget(storageService, nil, publisher, nil, websocket)
}

func NewRuntimeDependenciesWithTarget(storageService *storage.Repository, targetState TargetStateStore, publisher *queue.Publisher, _ *ws.Service, websocket WebsocketAcceptor) (Dependencies, error) {
	if storageService == nil {
		return Dependencies{}, fmt.Errorf("%w: storage repository is required", storage.ErrContractViolation)
	}
	var target TargetService
	if targetState != nil {
		target = NewTargetRuntimeService(targetState)
	}
	return Dependencies{
		Public:    &publicRuntimeService{store: storageService, queue: publisher},
		Worker:    &workerRuntimeService{store: storageService},
		Target:    target,
		Websocket: websocket,
	}, nil
}

var _ TargetStateStore = (*targetstore.Store)(nil)

func (s *workerRuntimeService) ListAnalysisRunQueue(ctx context.Context, req AnalysisRunQueueRequest) (AnalysisRunQueueResponse, error) {
	records, err := s.store.ListAnalysisRunQueue(ctx, req.Status, req.RunType, req.TaskType, req.PageSize)
	if err != nil {
		return AnalysisRunQueueResponse{}, err
	}
	return AnalysisRunQueueResponse{
		Items:    records,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (s *workerRuntimeService) ClaimExecution(ctx context.Context, analysisRunID string, req ExecutionClaimRequest) (ExecutionClaimResponse, error) {
	run, claimed, err := s.store.ClaimAnalysisRunTask(ctx, analysisRunID, req.WorkerKind, req.TaskType, req.LeaseOwner)
	if err != nil {
		return ExecutionClaimResponse{}, err
	}
	claimedAt := time.Now().UTC()
	if run.StartedAt != nil {
		claimedAt = run.StartedAt.UTC()
	} else if !run.CreatedAt.IsZero() {
		claimedAt = run.CreatedAt.UTC()
	}
	_ = claimed
	return ExecutionClaimResponse{
		ExecutionID:   run.ID,
		AnalysisRunID: run.ID,
		RunType:       run.RunType,
		Selection:     toSealedSelectionInput(run.Selection),
		Params:        jsonObject(run.ParamsJSON),
		ClaimedAt:     claimedAt,
	}, nil
}

func (s *workerRuntimeService) ResolveRequestAccess(ctx context.Context, analysisRunID string, executionID string) (RequestAccessResponse, error) {
	if strings.TrimSpace(executionID) == "" {
		return RequestAccessResponse{}, fmt.Errorf("%w: execution_id is required", storage.ErrContractViolation)
	}
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return RequestAccessResponse{}, err
	}
	params := jsonObject(run.ParamsJSON)
	access, ok := params["request_access"].(map[string]any)
	if !ok {
		return RequestAccessResponse{}, fmt.Errorf("%w: request_access is not available for analysis_run", storage.ErrContractViolation)
	}
	response := RequestAccessResponse{
		Provider:            accessString(access, "provider"),
		URL:                 accessString(access, "url"),
		ExpiresAt:           accessString(access, "expires_at"),
		RequestRef:          accessString(access, "request_ref"),
		RequestDigestSHA256: accessString(access, "request_digest_sha256"),
		RequestBytes:        accessInt64(access, "request_bytes"),
	}
	if response.Provider == "" || response.URL == "" || response.ExpiresAt == "" || response.RequestRef == "" || response.RequestDigestSHA256 == "" || response.RequestBytes < 1 {
		return RequestAccessResponse{}, fmt.Errorf("%w: request_access is incomplete", storage.ErrContractViolation)
	}
	return response, nil
}

func (s *workerRuntimeService) CheckCancel(ctx context.Context, analysisRunID string, executionID string) (CancelCheckResponse, error) {
	if strings.TrimSpace(executionID) == "" {
		return CancelCheckResponse{}, fmt.Errorf("%w: execution_id is required", storage.ErrContractViolation)
	}
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return CancelCheckResponse{}, err
	}
	cancelRequested := run.Status == storage.AnalysisRunStatusCancelRequested || run.Status == storage.AnalysisRunStatusCanceled
	return CancelCheckResponse{
		CancelRequested:   cancelRequested,
		Status:            run.Status,
		CancelRequestedAt: run.CanceledAt,
	}, nil
}

func (s *workerRuntimeService) ResolveArtifactDownloadAccess(ctx context.Context, artifactID string) (ArtifactDownloadAccessResponse, error) {
	artifact, err := s.store.GetInternalArtifactDownloadAccess(ctx, artifactID)
	if err != nil {
		return ArtifactDownloadAccessResponse{}, err
	}
	if artifact.Download == nil {
		return ArtifactDownloadAccessResponse{}, fmt.Errorf("%w: artifact download is not available", storage.ErrArtifactResolutionFailed)
	}
	return ArtifactDownloadAccessResponse{
		ArtifactID:    artifact.ID,
		AnalysisRunID: artifact.AnalysisRunID,
		ArtifactKind:  internalWorkerArtifactKind(artifact),
		Filename:      internalArtifactFilename(artifact),
		MIMEType:      artifact.ContentType,
		SizeBytes:     artifact.SizeBytes,
		CreatedAt:     artifact.CreatedAt,
		Download:      *artifact.Download,
	}, nil
}

func toSealedSelectionInput(selection storage.SelectionRecord) sealedSelectionInput {
	optionSnapshot := jsonObject(selection.OptionSnapshotJSON)
	items := make([]selectionItemSnapshot, 0, len(selection.Items))
	for _, item := range selection.Items {
		metadata := jsonObject(item.MetadataJSON)
		items = append(items, selectionItemSnapshot{
			SelectionItemID:   item.ID,
			Position:          item.Position,
			MediaItemID:       item.MediaItemID,
			Kind:              item.Kind,
			MediaKind:         item.Kind,
			MIMEType:          optionalString(item.SourceSnapshot.MIMEType),
			Role:              selectionItemRole(item, metadata, optionSnapshot),
			Labels:            selectionLabels(item, metadata),
			SourceSnapshot:    item.SourceSnapshot,
			DisplayName:       item.DisplayName,
			StatusAtSelection: item.StatusAtSelection,
			MetadataSnapshot:  metadata,
			RetentionSnapshot: item.RetentionSnapshot,
			Diagnostics:       item.Diagnostics,
		})
	}
	return sealedSelectionInput{
		SelectionID:    selection.ID,
		Items:          items,
		OptionSnapshot: optionSnapshot,
		SealedAt:       selection.SealedAt,
	}
}

func jsonObject(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded == nil {
		return map[string]any{}
	}
	return decoded
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func accessString(access map[string]any, key string) string {
	value, ok := access[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func accessInt64(access map[string]any, key string) int64 {
	switch value := access[key].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case float64:
		return int64(value)
	default:
		return 0
	}
}

func internalWorkerArtifactKind(artifact storage.ArtifactRecord) string {
	preview := jsonObject(artifact.PreviewJSON)
	if workerKind := accessString(preview, "worker_artifact_kind"); workerKind != "" {
		return workerKind
	}
	return strings.TrimSpace(artifact.Kind)
}

func internalArtifactFilename(artifact storage.ArtifactRecord) string {
	preview := jsonObject(artifact.PreviewJSON)
	if filename := accessString(preview, "filename"); filename != "" {
		return filename
	}
	if base := path.Base(strings.TrimSpace(artifact.ObjectKey)); base != "." && base != "/" && base != "" {
		return base
	}
	return artifact.ID
}

func selectionLabels(item storage.SelectionItemSnapshot, metadata map[string]any) selectionItemLabels {
	displayLabel := strings.TrimSpace(item.DisplayName)
	if displayLabel == "" {
		displayLabel = strings.TrimSpace(item.MediaItemID)
	}
	return selectionItemLabels{
		DisplayLabel:     displayLabel,
		SourceLabel:      firstString(metadataString(metadata, "source_label"), sourceLabelFromSnapshot(item.SourceSnapshot)),
		OriginalFilename: firstMetadataString(metadata, "original_filename", "filename"),
	}
}

func sourceLabelFromSnapshot(source storage.MediaSourceMetadata) *string {
	for _, value := range []string{source.ExternalURI, source.ObjectKey, source.TextRef, source.SourceID} {
		if label := optionalString(value); label != nil {
			return label
		}
	}
	return nil
}

func selectionItemRole(item storage.SelectionItemSnapshot, metadata map[string]any, optionSnapshot map[string]any) string {
	if role := metadataString(metadata, "role"); role != nil {
		return *role
	}
	if itemRoles, ok := optionSnapshot["item_roles"].(map[string]any); ok {
		for _, key := range []string{item.ID, item.MediaItemID, fmt.Sprintf("%d", item.Position)} {
			if role := metadataString(itemRoles, key); role != nil {
				return *role
			}
		}
	}
	return "primary"
}

func firstMetadataString(metadata map[string]any, keys ...string) *string {
	for _, key := range keys {
		if value := metadataString(metadata, key); value != nil {
			return value
		}
	}
	return nil
}

func firstString(values ...*string) *string {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func metadataString(metadata map[string]any, key string) *string {
	value, ok := metadata[key].(string)
	if !ok {
		return nil
	}
	return optionalString(value)
}

func (s *workerRuntimeService) RecordExecutionProgress(ctx context.Context, analysisRunID string, req ExecutionProgressRequest) error {
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return err
	}
	stage := firstNonEmpty(req.ProgressStage, req.Stage)
	message := firstNonEmpty(req.ProgressMessage, req.Message)
	_, err = s.store.RecordAnalysisRunProgress(ctx, run.Owner, run.ID, stage, message, req.Payload)
	return err
}

func (s *workerRuntimeService) RecordExecutionArtifacts(ctx context.Context, analysisRunID string, req ExecutionArtifactsRequest) error {
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return err
	}
	artifacts := make([]storage.ArtifactRecord, 0, len(req.Artifacts))
	for _, descriptor := range req.Artifacts {
		workerKind := strings.TrimSpace(descriptor.ArtifactKind)
		publicKind := workerDescriptorPublicArtifactKind(workerKind)
		if publicKind == "" {
			return fmt.Errorf("%w: unsupported worker artifact_kind %q", storage.ErrContractViolation, workerKind)
		}
		preview := map[string]any{
			"available":            false,
			"filename":             strings.TrimSpace(descriptor.Filename),
			"format":               strings.TrimSpace(descriptor.Format),
			"artifact_kind":        publicKind,
			"worker_artifact_kind": workerKind,
		}
		previewJSON, _ := json.Marshal(preview)
		artifacts = append(artifacts, storage.ArtifactRecord{
			Kind:        publicKind,
			Status:      storage.ArtifactStatusAvailable,
			ObjectKey:   normalizeWorkerArtifactObjectKey(descriptor.ObjectKey),
			ContentType: strings.TrimSpace(descriptor.MIMEType),
			SizeBytes:   descriptor.SizeBytes,
			Visibility:  "owner",
			PreviewJSON: previewJSON,
		})
	}
	_, err = s.store.RecordArtifacts(ctx, run.Owner, run.ID, artifacts)
	return err
}

func workerDescriptorPublicArtifactKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "transcript_plain", "transcript_segmented_markdown", "transcript_docx":
		return "transcript"
	case "summary_markdown":
		return "summary"
	case "report_markdown", "report_docx":
		return "report"
	case "deep_research_markdown":
		return "deep_research"
	case "agent_result_json":
		return "structured_data"
	case "execution_log":
		return "execution_log"
	case "run_manifest":
		return "run_manifest"
	case "run_diagnostics":
		return "run_diagnostics"
	default:
		return ""
	}
}

func normalizeWorkerArtifactObjectKey(objectKey string) string {
	trimmed := strings.TrimSpace(objectKey)
	return strings.TrimPrefix(trimmed, "artifacts/")
}

func (s *workerRuntimeService) RecordExecutionDiagnostics(ctx context.Context, analysisRunID string, req ExecutionDiagnosticsRequest) error {
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return err
	}
	diagnostics := make([]storage.DiagnosticRecord, 0, len(req.Diagnostics))
	for _, descriptor := range req.Diagnostics {
		contextJSON := jsonObjectBytes(descriptor.Context)
		if req.ExecutionID != "" {
			contextJSON = mergeRuntimeContext(contextJSON, map[string]any{"execution_id": req.ExecutionID})
		}
		diagnostics = append(diagnostics, storage.DiagnosticRecord{
			ID:              strings.TrimSpace(descriptor.DiagnosticID),
			SubjectType:     strings.TrimSpace(descriptor.SubjectType),
			SubjectID:       strings.TrimSpace(descriptor.SubjectID),
			Severity:        strings.TrimSpace(descriptor.Severity),
			Code:            strings.TrimSpace(descriptor.Code),
			Message:         strings.TrimSpace(descriptor.Message),
			ContextJSON:     contextJSON,
			SafeAdapterJSON: jsonObjectBytes(descriptor.SafeAdapterContext),
			CorrelationID:   strings.TrimSpace(descriptor.CorrelationID),
			RemediationHint: strings.TrimSpace(descriptor.RemediationHint),
			CreatedAt:       descriptor.CreatedAt,
		})
	}
	_, err = s.store.RecordDiagnostics(ctx, run.Owner, run.ID, diagnostics)
	return err
}

func (s *workerRuntimeService) FinalizeExecution(ctx context.Context, analysisRunID string, req ExecutionFinalizeRequest) (storage.AnalysisRunRecord, error) {
	run, err := s.store.GetAnalysisRunByID(ctx, analysisRunID)
	if err != nil {
		return storage.AnalysisRunRecord{}, err
	}
	status := workerOutcomeStatus(firstNonEmpty(req.Outcome, req.Status))
	if status == "" {
		return storage.AnalysisRunRecord{}, fmt.Errorf("%w: invalid worker outcome", storage.ErrContractViolation)
	}
	if run.Status == storage.AnalysisRunStatusCancelRequested || run.Status == storage.AnalysisRunStatusCanceled {
		status = storage.AnalysisRunStatusCanceled
	}
	return s.store.FinalizeAnalysisRunTask(ctx, run.Owner, run.ID, status, req.Message)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func workerOutcomeStatus(outcome string) string {
	switch strings.TrimSpace(outcome) {
	case "succeeded":
		return storage.AnalysisRunStatusSucceeded
	case "partially_succeeded":
		return storage.AnalysisRunStatusPartiallySucceeded
	case "failed":
		return storage.AnalysisRunStatusFailed
	case "canceled":
		return storage.AnalysisRunStatusCanceled
	default:
		return ""
	}
}

func jsonObjectBytes(value map[string]any) []byte {
	if value == nil {
		return []byte(`{}`)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}
	return data
}

func mergeRuntimeContext(data []byte, fields map[string]any) []byte {
	merged := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &merged)
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
