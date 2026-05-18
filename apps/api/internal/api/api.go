package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

const (
	ValidateRequestMarker = "[ApiHttp][validateRequest][BLOCK_VALIDATE_REQUEST_AND_SHAPE_RESPONSE]"
	defaultMaxRequestBody = 1 << 30
)

type Logger interface {
	Printf(format string, args ...any)
}

type PublicService interface {
	AddMediaItem(ctx context.Context, req storage.AddMediaItemRequest) (storage.MediaItemRecord, error)
	ListMediaItems(ctx context.Context, owner storage.OwnerScope) ([]storage.MediaItemRecord, error)
	GetMediaItem(ctx context.Context, owner storage.OwnerScope, mediaItemID string) (storage.MediaItemRecord, error)
	RemoveMediaItem(ctx context.Context, owner storage.OwnerScope, mediaItemID string) (storage.MediaItemRecord, error)
	GetInboxCollection(ctx context.Context, owner storage.OwnerScope) (storage.CollectionRecord, error)
	CreateCollection(ctx context.Context, req storage.CreateCollectionRequest) (storage.CollectionRecord, error)
	ListCollections(ctx context.Context, owner storage.OwnerScope) ([]storage.CollectionRecord, error)
	GetCollection(ctx context.Context, owner storage.OwnerScope, collectionID string) (storage.CollectionRecord, error)
	UpdateCollection(ctx context.Context, req storage.UpdateCollectionRequest) (storage.CollectionRecord, error)
	UpdateCollectionItems(ctx context.Context, req storage.UpdateCollectionItemsRequest) (storage.CollectionRecord, error)
	CreateSelection(ctx context.Context, req storage.CreateSelectionRequest) (storage.SelectionRecord, error)
	GetSelection(ctx context.Context, owner storage.OwnerScope, selectionID string) (storage.SelectionRecord, error)
	CreateAnalysisRun(ctx context.Context, req storage.CreateAnalysisRunRequest) (storage.AnalysisRunRecord, error)
	CancelAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID, message string) (storage.AnalysisRunRecord, error)
	RetryAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID, idempotencyKey string) (storage.AnalysisRunRecord, error)
	ListAnalysisRuns(ctx context.Context, owner storage.OwnerScope) ([]storage.AnalysisRunRecord, error)
	GetAnalysisRun(ctx context.Context, owner storage.OwnerScope, analysisRunID string) (storage.AnalysisRunRecord, error)
	ListAnalysisRunEvents(ctx context.Context, owner storage.OwnerScope, analysisRunID string) ([]storage.RunEventRecord, error)
	ListArtifacts(ctx context.Context, owner storage.OwnerScope, analysisRunID string) ([]storage.ArtifactRecord, error)
	GetArtifact(ctx context.Context, owner storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error)
	RefreshArtifactLink(ctx context.Context, owner storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error)
	ListDiagnostics(ctx context.Context, owner storage.OwnerScope, query storage.DiagnosticQuery) ([]storage.DiagnosticRecord, error)
	ReconcileAnalysisRunQueue(ctx context.Context, limit int) (int, error)
	GetObservabilitySnapshot(ctx context.Context) (storage.ObservabilitySnapshot, error)
}

type WorkerService interface {
	ListAnalysisRunQueue(ctx context.Context, req AnalysisRunQueueRequest) (AnalysisRunQueueResponse, error)
	ClaimExecution(ctx context.Context, analysisRunID string, req ExecutionClaimRequest) (ExecutionClaimResponse, error)
	ResolveRequestAccess(ctx context.Context, analysisRunID string, executionID string) (RequestAccessResponse, error)
	CheckCancel(ctx context.Context, analysisRunID string, executionID string) (CancelCheckResponse, error)
	ResolveArtifactDownloadAccess(ctx context.Context, artifactID string) (ArtifactDownloadAccessResponse, error)
	RecordExecutionProgress(ctx context.Context, analysisRunID string, req ExecutionProgressRequest) error
	RecordExecutionArtifacts(ctx context.Context, analysisRunID string, req ExecutionArtifactsRequest) error
	RecordExecutionDiagnostics(ctx context.Context, analysisRunID string, req ExecutionDiagnosticsRequest) error
	FinalizeExecution(ctx context.Context, analysisRunID string, req ExecutionFinalizeRequest) (storage.AnalysisRunRecord, error)
}

type WebsocketAcceptor interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}

type Dependencies struct {
	Public    PublicService
	Worker    WorkerService
	Target    TargetService
	Websocket WebsocketAcceptor
}

type Server struct {
	deps            Dependencies
	logger          Logger
	maxRequestBytes int64
	readUploadBody  func(io.Reader) ([]byte, error)
}

type Option func(*Server)

func WithLogger(logger Logger) Option {
	return func(s *Server) {
		s.logger = logger
	}
}

func WithMaxRequestBytes(limit int64) Option {
	return func(s *Server) {
		if limit > 0 {
			s.maxRequestBytes = limit
		}
	}
}

func NewServer(deps Dependencies, opts ...Option) *Server {
	server := &Server{
		deps:            deps,
		maxRequestBytes: defaultMaxRequestBody,
	}
	for _, opt := range opts {
		opt(server)
	}
	return server
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	for _, path := range []string{
		"/v1/media-items",
		"/v1/media-items/{media_item_id}",
		"/v1/media-assets",
		"/v1/media-assets/upload",
		"/v1/media-assets/{media_asset_id}",
		"/v1/collections/inbox",
		"/v1/collections",
		"/v1/collections/{collection_id}",
		"/v1/collections/{collection_id}/items",
		"/v1/collections/{collection_id}/items/{media_item_id}",
		"/v1/selections",
		"/v1/selections/{selection_id}",
		"/v1/selection-snapshots",
		"/v1/selection-snapshots/{selection_snapshot_id}",
		"/v1/analysis-runs",
		"/v1/analysis-runs/{analysis_run_id}",
		"/v1/analysis-runs/{analysis_run_id}/cancel",
		"/v1/analysis-runs/{analysis_run_id}/retry",
		"/v1/analysis-runs/{analysis_run_id}/events",
		"/v1/analysis-runs/{analysis_run_id}/artifacts",
		"/v1/artifacts",
		"/v1/artifacts/{artifact_id}",
		"/v1/artifacts/{artifact_id}/refresh",
		"/v1/diagnostics",
		"/v1/admin/reconcile-queue",
		"/v1/admin/observability",
		"/internal/v1/channel-accounts",
		"/internal/v1/channel-accounts/{channel_account_id}",
		"/internal/v1/channel-surfaces",
		"/internal/v1/channel-surfaces/active",
		"/internal/v1/channel-surfaces/{channel_surface_id}/display-state",
		"/internal/v1/channel-surfaces/{channel_surface_id}/supersede",
		"/internal/v1/channel-surfaces/{channel_surface_id}/events",
		"/internal/v1/analysis-runs/queue",
		"/internal/v1/analysis-runs/{analysis_run_id}/steps/claim",
		"/internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check",
		"/internal/v1/analysis-runs/{analysis_run_id}/steps/progress",
		"/internal/v1/analysis-runs/{analysis_run_id}/steps/finalize",
		"/internal/v1/analysis-runs/{analysis_run_id}/executions/claim",
		"/internal/v1/analysis-runs/{analysis_run_id}/request-access",
		"/internal/v1/analysis-runs/{analysis_run_id}/executions/cancel-check",
		"/internal/v1/artifacts/{artifact_id}/download-access",
		"/internal/v1/analysis-runs/{analysis_run_id}/executions/progress",
		"/internal/v1/analysis-runs/{analysis_run_id}/artifacts",
		"/internal/v1/analysis-runs/{analysis_run_id}/diagnostics",
		"/internal/v1/analysis-runs/{analysis_run_id}/executions/finalize",
	} {
		mux.HandleFunc("OPTIONS "+path, s.handleCORSPreflight)
	}
	mux.HandleFunc("POST /v1/media-items", s.withCORS(s.handleAddMediaItem))
	mux.HandleFunc("GET /v1/media-items", s.withCORS(s.handleListMediaItems))
	mux.HandleFunc("GET /v1/media-items/{media_item_id}", s.withCORS(s.handleGetMediaItem))
	mux.HandleFunc("DELETE /v1/media-items/{media_item_id}", s.withCORS(s.handleRemoveMediaItem))
	mux.HandleFunc("POST /v1/media-assets", s.withCORS(s.handleCreateTargetMediaAsset))
	mux.HandleFunc("POST /v1/media-assets/upload", s.withCORS(s.handleUploadTargetMediaAsset))
	mux.HandleFunc("GET /v1/media-assets", s.withCORS(s.handleListTargetMediaAssets))
	mux.HandleFunc("GET /v1/media-assets/{media_asset_id}", s.withCORS(s.handleGetTargetMediaAsset))
	mux.HandleFunc("DELETE /v1/media-assets/{media_asset_id}", s.withCORS(s.handleDeleteTargetMediaAsset))
	mux.HandleFunc("GET /internal/v1/channel-accounts", s.withCORS(s.handleListTargetChannelAccounts))
	mux.HandleFunc("PUT /internal/v1/channel-accounts", s.withCORS(s.handleResolveTargetChannelAccount))
	mux.HandleFunc("PATCH /internal/v1/channel-accounts/{channel_account_id}", s.withCORS(s.handleUpdateTargetChannelAccount))
	mux.HandleFunc("POST /v1/selection-snapshots", s.withCORS(s.handleCreateTargetSelectionSnapshot))
	mux.HandleFunc("GET /v1/selection-snapshots/{selection_snapshot_id}", s.withCORS(s.handleGetTargetSelectionSnapshot))
	mux.HandleFunc("PUT /internal/v1/channel-surfaces", s.withCORS(s.handleUpsertTargetChannelSurface))
	mux.HandleFunc("GET /internal/v1/channel-surfaces", s.withCORS(s.handleListTargetChannelSurfaces))
	mux.HandleFunc("GET /internal/v1/channel-surfaces/active", s.withCORS(s.handleListActiveTargetChannelSurfaces))
	mux.HandleFunc("PATCH /internal/v1/channel-surfaces/{channel_surface_id}/display-state", s.withCORS(s.handleReplaceTargetChannelSurfaceDisplayState))
	mux.HandleFunc("POST /internal/v1/channel-surfaces/{channel_surface_id}/supersede", s.withCORS(s.handleSupersedeTargetChannelSurface))
	mux.HandleFunc("GET /internal/v1/channel-surfaces/{channel_surface_id}/events", s.withCORS(s.handleListTargetChannelSurfaceEvents))
	mux.HandleFunc("GET /v1/collections/inbox", s.withCORS(s.handleGetInboxCollection))
	mux.HandleFunc("POST /v1/collections", s.withCORS(s.handleCreateCollection))
	mux.HandleFunc("GET /v1/collections", s.withCORS(s.handleListCollections))
	mux.HandleFunc("GET /v1/collections/{collection_id}", s.withCORS(s.handleGetCollection))
	mux.HandleFunc("PATCH /v1/collections/{collection_id}", s.withCORS(s.handleUpdateCollection))
	mux.HandleFunc("POST /v1/collections/{collection_id}/items", s.withCORS(s.handleUpdateCollectionItems))
	mux.HandleFunc("DELETE /v1/collections/{collection_id}/items/{media_item_id}", s.withCORS(s.handleRemoveCollectionItem))
	mux.HandleFunc("POST /v1/selections", s.withCORS(s.handleCreateSelection))
	mux.HandleFunc("GET /v1/selections/{selection_id}", s.withCORS(s.handleGetSelection))
	mux.HandleFunc("POST /v1/analysis-runs", s.withCORS(s.handleCreateAnalysisRun))
	mux.HandleFunc("GET /v1/analysis-runs", s.withCORS(s.handleListAnalysisRuns))
	mux.HandleFunc("GET /v1/analysis-runs/{analysis_run_id}", s.withCORS(s.handleGetAnalysisRun))
	mux.HandleFunc("POST /v1/analysis-runs/{analysis_run_id}/cancel", s.withCORS(s.handleCancelAnalysisRun))
	mux.HandleFunc("POST /v1/analysis-runs/{analysis_run_id}/retry", s.withCORS(s.handleRetryAnalysisRun))
	mux.HandleFunc("GET /v1/analysis-runs/{analysis_run_id}/events", s.withCORS(s.handleListAnalysisRunEvents))
	mux.HandleFunc("GET /v1/analysis-runs/{analysis_run_id}/artifacts", s.withCORS(s.handleListArtifacts))
	mux.HandleFunc("GET /v1/artifacts", s.withCORS(s.handleListArtifacts))
	mux.HandleFunc("GET /v1/artifacts/{artifact_id}", s.withCORS(s.handleGetArtifact))
	mux.HandleFunc("POST /v1/artifacts/{artifact_id}/refresh", s.withCORS(s.handleRefreshArtifactLink))
	mux.HandleFunc("GET /v1/diagnostics", s.withCORS(s.handleListDiagnostics))
	mux.HandleFunc("POST /v1/admin/reconcile-queue", s.withCORS(s.handleReconcileAnalysisRunQueue))
	mux.HandleFunc("GET /v1/admin/observability", s.withCORS(s.handleGetObservabilitySnapshot))
	mux.HandleFunc("GET /v1/ws", s.withCORS(s.HandleWebsocket))
	mux.HandleFunc("GET /internal/v1/analysis-runs/queue", s.withCORS(s.handleListAnalysisRunQueue))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/steps/claim", s.withCORS(s.handleClaimTargetAnalysisRunStep))
	mux.HandleFunc("GET /internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check", s.withCORS(s.handleCheckTargetAnalysisRunStepCancel))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/steps/progress", s.withCORS(s.handleRecordTargetAnalysisRunStepProgress))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/steps/finalize", s.withCORS(s.handleFinalizeTargetAnalysisRunStep))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/executions/claim", s.withCORS(s.handleClaimExecution))
	mux.HandleFunc("GET /internal/v1/analysis-runs/{analysis_run_id}/request-access", s.withCORS(s.handleResolveRequestAccess))
	mux.HandleFunc("GET /internal/v1/analysis-runs/{analysis_run_id}/executions/cancel-check", s.withCORS(s.handleCheckCancel))
	mux.HandleFunc("GET /internal/v1/artifacts/{artifact_id}/download-access", s.withCORS(s.handleResolveArtifactDownloadAccess))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/executions/progress", s.withCORS(s.handleRecordExecutionProgress))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/artifacts", s.withCORS(s.handleRecordExecutionArtifacts))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/diagnostics", s.withCORS(s.handleRecordExecutionDiagnostics))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/executions/finalize", s.withCORS(s.handleFinalizeExecution))
}

func (s *Server) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.writeCORSHeaders(w, r)
		next(w, r)
	}
}

func (s *Server) handleCORSPreflight(w http.ResponseWriter, r *http.Request) {
	if !s.writeCORSHeaders(w, r) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) writeCORSHeaders(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	w.Header().Add("Vary", "Origin")
	if !isAllowedLocalHTTPOrigin(origin) {
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
	w.Header().Set("Access-Control-Max-Age", "600")
	return true
}

func isAllowedLocalHTTPOrigin(origin string) bool {
	parsed, err := neturl.Parse(origin)
	if err != nil {
		return false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	switch strings.ToLower(parsed.Hostname()) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func (s *Server) HandleWebsocket(w http.ResponseWriter, r *http.Request) {
	if s.deps.Websocket == nil {
		s.writeAPIError(w, dependencyUnavailableError("websocket transport is not configured"))
		return
	}
	s.deps.Websocket.ServeHTTP(w, r)
}

type ExecutionClaimRequest struct {
	WorkerKind string `json:"worker_kind"`
	TaskType   string `json:"task_type"`
	LeaseOwner string `json:"lease_owner,omitempty"`
}

type AnalysisRunQueueRequest struct {
	Status   string
	RunType  string
	TaskType string
	PageSize int
}

type AnalysisRunQueueResponse struct {
	Items    []storage.AnalysisRunQueueRecord `json:"items"`
	Page     int                              `json:"page"`
	PageSize int                              `json:"page_size"`
}

type ExecutionClaimResponse struct {
	ExecutionID   string               `json:"execution_id"`
	AnalysisRunID string               `json:"analysis_run_id"`
	RunType       string               `json:"run_type"`
	Selection     sealedSelectionInput `json:"selection"`
	Params        map[string]any       `json:"params"`
	ClaimedAt     time.Time            `json:"claimed_at"`
}

type RequestAccessResponse struct {
	Provider            string `json:"provider"`
	URL                 string `json:"url"`
	ExpiresAt           string `json:"expires_at"`
	RequestRef          string `json:"request_ref"`
	RequestDigestSHA256 string `json:"request_digest_sha256"`
	RequestBytes        int64  `json:"request_bytes"`
}

type CancelCheckResponse struct {
	CancelRequested   bool       `json:"cancel_requested"`
	Status            string     `json:"status"`
	CancelRequestedAt *time.Time `json:"cancel_requested_at,omitempty"`
}

type ArtifactDownloadAccessResponse struct {
	ArtifactID    string                     `json:"artifact_id"`
	AnalysisRunID string                     `json:"analysis_run_id"`
	ArtifactKind  string                     `json:"artifact_kind"`
	Filename      string                     `json:"filename"`
	MIMEType      string                     `json:"mime_type"`
	SizeBytes     int64                      `json:"size_bytes"`
	CreatedAt     time.Time                  `json:"created_at"`
	Download      storage.DownloadDescriptor `json:"download"`
}

type sealedSelectionInput struct {
	SelectionID    string                  `json:"selection_id"`
	Items          []selectionItemSnapshot `json:"items"`
	OptionSnapshot map[string]any          `json:"option_snapshot"`
	SealedAt       time.Time               `json:"sealed_at"`
}

type selectionItemSnapshot struct {
	SelectionItemID   string                      `json:"selection_item_id"`
	Position          int                         `json:"position"`
	MediaItemID       string                      `json:"media_item_id"`
	Kind              string                      `json:"kind"`
	MediaKind         string                      `json:"media_kind"`
	MIMEType          *string                     `json:"mime_type"`
	Role              string                      `json:"role"`
	Labels            selectionItemLabels         `json:"labels"`
	SourceSnapshot    storage.MediaSourceMetadata `json:"source_snapshot"`
	DisplayName       string                      `json:"display_name"`
	StatusAtSelection string                      `json:"status_at_selection"`
	MetadataSnapshot  map[string]any              `json:"metadata_snapshot,omitempty"`
	RetentionSnapshot storage.RetentionMetadata   `json:"retention_snapshot"`
	Diagnostics       []storage.DiagnosticRecord  `json:"diagnostics,omitempty"`
}

type selectionItemLabels struct {
	DisplayLabel     string  `json:"display_label"`
	SourceLabel      *string `json:"source_label,omitempty"`
	OriginalFilename *string `json:"original_filename,omitempty"`
}

type ExecutionProgressRequest struct {
	ExecutionID     string          `json:"execution_id"`
	ProgressStage   string          `json:"progress_stage,omitempty"`
	ProgressMessage string          `json:"progress_message,omitempty"`
	Stage           string          `json:"stage,omitempty"`
	Message         string          `json:"message,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	ItemPosition    *int            `json:"item_position,omitempty"`
}

type workerArtifactDescriptor struct {
	ArtifactKind string `json:"artifact_kind"`
	MIMEType     string `json:"mime_type"`
	ObjectKey    string `json:"object_key"`
	SizeBytes    int64  `json:"size_bytes"`
	Filename     string `json:"filename"`
	Format       string `json:"format,omitempty"`
}

type ExecutionArtifactsRequest struct {
	ExecutionID string                     `json:"execution_id"`
	Artifacts   []workerArtifactDescriptor `json:"artifacts"`
}

type workerDiagnosticDescriptor struct {
	DiagnosticID       string         `json:"diagnostic_id"`
	SubjectType        string         `json:"subject_type"`
	SubjectID          string         `json:"subject_id"`
	Severity           string         `json:"severity"`
	Code               string         `json:"code"`
	Message            string         `json:"message"`
	Context            map[string]any `json:"context,omitempty"`
	SafeAdapterContext map[string]any `json:"safe_adapter_context,omitempty"`
	CorrelationID      string         `json:"correlation_id,omitempty"`
	RemediationHint    string         `json:"remediation_hint,omitempty"`
	CreatedAt          time.Time      `json:"created_at,omitempty"`
}

type ExecutionDiagnosticsRequest struct {
	ExecutionID string                       `json:"execution_id"`
	Diagnostics []workerDiagnosticDescriptor `json:"diagnostics"`
}

type ExecutionFinalizeRequest struct {
	ExecutionID string `json:"execution_id"`
	Outcome     string `json:"outcome,omitempty"`
	Status      string `json:"status,omitempty"`
	Message     string `json:"message,omitempty"`
}

func (s *Server) handleListAnalysisRunQueue(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target != nil {
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
		writeJSON(w, http.StatusOK, response)
		return
	}
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	req := AnalysisRunQueueRequest{
		Status:   strings.TrimSpace(r.URL.Query().Get("status")),
		RunType:  strings.TrimSpace(r.URL.Query().Get("run_type")),
		TaskType: strings.TrimSpace(r.URL.Query().Get("task_type")),
		PageSize: parsePositiveQueryInt(r.URL.Query().Get("page_size"), 20),
	}
	response, err := s.deps.Worker.ListAnalysisRunQueue(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleClaimExecution(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	var req ExecutionClaimRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_claim", message: "execution claim must be valid JSON", details: err.Error()})
		return
	}
	response, err := s.deps.Worker.ClaimExecution(r.Context(), r.PathValue("analysis_run_id"), req)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleResolveRequestAccess(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	analysisRunStepID := strings.TrimSpace(r.URL.Query().Get("analysis_run_step_id"))
	if analysisRunStepID == "" {
		analysisRunStepID = strings.TrimSpace(r.URL.Query().Get("execution_id"))
	}
	if analysisRunStepID == "" {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_request_access", message: "analysis_run_step_id is required"})
		return
	}
	response, err := s.deps.Worker.ResolveRequestAccess(r.Context(), r.PathValue("analysis_run_id"), analysisRunStepID)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCheckCancel(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	executionID := strings.TrimSpace(r.URL.Query().Get("execution_id"))
	if executionID == "" {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_cancel_check", message: "execution_id is required"})
		return
	}
	response, err := s.deps.Worker.CheckCancel(r.Context(), r.PathValue("analysis_run_id"), executionID)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleResolveArtifactDownloadAccess(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	response, err := s.deps.Worker.ResolveArtifactDownloadAccess(r.Context(), r.PathValue("artifact_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRecordExecutionProgress(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	var req ExecutionProgressRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_progress", message: "execution progress must be valid JSON", details: err.Error()})
		return
	}
	if err := s.deps.Worker.RecordExecutionProgress(r.Context(), r.PathValue("analysis_run_id"), req); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (s *Server) handleRecordExecutionArtifacts(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target != nil {
		var req TargetRecordAnalysisRunArtifactsRequest
		if err := decodeJSONBody(r, &req); err != nil {
			s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_artifacts", message: "analysis run artifacts must be valid JSON", details: err.Error()})
			return
		}
		if req.AnalysisRunStepID != "" {
			if err := s.deps.Target.RecordAnalysisRunArtifacts(r.Context(), r.PathValue("analysis_run_id"), req); err != nil {
				s.writeAPIError(w, mapFinalStorageError(err))
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
			return
		}
		if s.deps.Worker == nil {
			s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
			return
		}
		if err := s.deps.Worker.RecordExecutionArtifacts(r.Context(), r.PathValue("analysis_run_id"), ExecutionArtifactsRequest{
			ExecutionID: req.ExecutionID,
			Artifacts:   req.Artifacts,
		}); err != nil {
			s.writeAPIError(w, mapFinalStorageError(err))
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
		return
	}
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	var req ExecutionArtifactsRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_artifacts", message: "execution artifacts must be valid JSON", details: err.Error()})
		return
	}
	if err := s.deps.Worker.RecordExecutionArtifacts(r.Context(), r.PathValue("analysis_run_id"), req); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (s *Server) handleRecordExecutionDiagnostics(w http.ResponseWriter, r *http.Request) {
	if s.deps.Target != nil {
		var req TargetRecordAnalysisRunDiagnosticsRequest
		if err := decodeJSONBody(r, &req); err != nil {
			s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_diagnostics", message: "analysis run diagnostics must be valid JSON", details: err.Error()})
			return
		}
		if req.AnalysisRunStepID != "" {
			if err := s.deps.Target.RecordAnalysisRunDiagnostics(r.Context(), r.PathValue("analysis_run_id"), req); err != nil {
				s.writeAPIError(w, mapFinalStorageError(err))
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
			return
		}
		if s.deps.Worker == nil {
			s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
			return
		}
		if err := s.deps.Worker.RecordExecutionDiagnostics(r.Context(), r.PathValue("analysis_run_id"), ExecutionDiagnosticsRequest{
			ExecutionID: req.ExecutionID,
			Diagnostics: req.Diagnostics,
		}); err != nil {
			s.writeAPIError(w, mapFinalStorageError(err))
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
		return
	}
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	var req ExecutionDiagnosticsRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_diagnostics", message: "execution diagnostics must be valid JSON", details: err.Error()})
		return
	}
	if err := s.deps.Worker.RecordExecutionDiagnostics(r.Context(), r.PathValue("analysis_run_id"), req); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (s *Server) handleFinalizeExecution(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker service is not configured"))
		return
	}
	var req ExecutionFinalizeRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_execution_finalize", message: "execution finalize must be valid JSON", details: err.Error()})
		return
	}
	run, err := s.deps.Worker.FinalizeExecution(r.Context(), r.PathValue("analysis_run_id"), req)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis_run": run})
}

type apiError struct {
	status  int
	code    string
	message string
	details any
}

func (e apiError) Error() string {
	if e.message != "" {
		return e.message
	}
	return e.code
}

func dependencyUnavailableError(message string) apiError {
	return apiError{status: http.StatusServiceUnavailable, code: "dependency_unavailable", message: message}
}

func parsePositiveQueryInt(raw string, fallback int) int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil || parsed <= 0 {
		return fallback
	}
	if parsed > 100 {
		return 100
	}
	return parsed
}

func decodeJSONBody(r *http.Request, dest any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, defaultMaxRequestBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		return err
	}
	return nil
}

func (s *Server) writeAPIError(w http.ResponseWriter, err error) {
	var apiErr apiError
	if !asAPIError(err, &apiErr) {
		apiErr = apiError{status: http.StatusInternalServerError, code: "internal_error", message: err.Error()}
	}
	if apiErr.status == 0 {
		apiErr.status = http.StatusInternalServerError
	}
	if apiErr.code == "" {
		apiErr.code = "internal_error"
	}
	if apiErr.message == "" {
		apiErr.message = apiErr.code
	}
	body := map[string]any{
		"error": map[string]any{
			"code":    apiErr.code,
			"message": apiErr.message,
		},
	}
	if apiErr.details != nil {
		body["error"].(map[string]any)["details"] = apiErr.details
	}
	writeJSON(w, apiErr.status, body)
}

func asAPIError(err error, target *apiError) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(apiError); ok {
		*target = apiErr
		return true
	}
	return false
}

func (s *Server) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func routeNotImplemented(name string) error {
	return fmt.Errorf("%w: %s", storage.ErrStorageUnavailable, name)
}
