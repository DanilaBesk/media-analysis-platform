// START_MODULE_CONTRACT
// PURPOSE: Expose the HTTP and WebSocket entrypoint for API-owned product contracts and internal worker control routes.
// SCOPE: Server construction, route registration, request validation, access checks, CORS, error envelopes, and transport-level response shaping.
// DEPENDS: M-CONTRACTS, M-API-DOMAIN, M-API-STORAGE, M-API-STEP-CONTROL, M-API-EVENTS
// LINKS: M-CONTRACTS, M-API-HTTP, V-M-CONTRACTS, V-M-API-HTTP
// ROLE: RUNTIME
// MAP_MODE: EXPORTS
// END_MODULE_CONTRACT

// START_MODULE_MAP
// Logger
// WebsocketAcceptor
// Dependencies
// Server
// Option
// WithLogger
// WithMaxRequestBytes
// WithStrictLocalRequests
// WithInternalToken
// NewServer
// END_MODULE_MAP

package api

import (
	"encoding/json"
	"errors"
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

type WebsocketAcceptor interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}

type Dependencies struct {
	Target    TargetService
	Websocket WebsocketAcceptor
}

type Server struct {
	deps                Dependencies
	logger              Logger
	maxRequestBytes     int64
	readUploadBody      func(io.Reader) ([]byte, error)
	strictLocalRequests bool
	internalToken       string
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

func WithStrictLocalRequests(enabled bool) Option {
	return func(s *Server) { s.strictLocalRequests = enabled }
}

func WithInternalToken(token string) Option {
	return func(s *Server) { s.internalToken = strings.TrimSpace(token) }
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
		"/v1/media-assets",
		"/v1/media-assets/upload",
		"/v1/media-assets/{media_asset_id}",
		"/v1/media-assets/{media_asset_id}/exports",
		"/v1/export-jobs",
		"/v1/export-jobs/{export_job_id}",
		"/v1/export-jobs/{export_job_id}/cancel",
		"/v1/export-jobs/{export_job_id}/retry",
		"/v1/export-jobs/{export_job_id}/deliveries/claim",
		"/v1/export-jobs/{export_job_id}/deliveries/heartbeat",
		"/v1/export-jobs/{export_job_id}/deliveries/ack",
		"/v1/export-jobs/{export_job_id}/deliveries/fail",
		"/v1/export-jobs/{export_job_id}/download",
		"/v1/collections/inbox",
		"/v1/collections",
		"/v1/collections/{collection_id}",
		"/v1/collections/{collection_id}/items",
		"/v1/collections/{collection_id}/items/{media_asset_id}",
		"/v1/collections/{collection_id}/processing-runs",
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
		"/v1/admin/observability",
		"/v1/ws",
		"/internal/v1/channel-accounts",
		"/internal/v1/channel-accounts/{channel_account_id}",
		"/internal/v1/reusable-transcripts",
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
		"/internal/v1/analysis-runs/{analysis_run_id}/request-access",
		"/internal/v1/artifacts/{artifact_id}/download-access",
		"/internal/v1/analysis-runs/{analysis_run_id}/artifacts",
		"/internal/v1/analysis-runs/{analysis_run_id}/diagnostics",
		"/internal/v1/export-jobs/queue",
		"/internal/v1/export-jobs/{export_job_id}/claim",
		"/internal/v1/export-jobs/{export_job_id}/progress",
		"/internal/v1/export-jobs/{export_job_id}/cancel-check",
		"/internal/v1/export-jobs/{export_job_id}/finalize",
		"/internal/v1/export-jobs/{export_job_id}/download-access",
		"/internal/v1/export-jobs/reclaim",
		"/v1/media-assets/{media_asset_id}/refresh-metadata",
		"/internal/v1/metadata-enrichment-jobs/queue",
		"/internal/v1/metadata-enrichment-jobs/{enrichment_id}/claim",
		"/internal/v1/metadata-enrichment-jobs/{enrichment_id}/progress",
		"/internal/v1/metadata-enrichment-jobs/{enrichment_id}/finalize",
		"/internal/v1/metadata-enrichment-jobs/reclaim",
		"/internal/v1/retention/sweep",
	} {
		mux.HandleFunc("OPTIONS "+path, s.handleCORSPreflight)
	}

	mux.HandleFunc("POST /v1/media-assets", s.withCORS(s.handleCreateTargetMediaAsset))
	mux.HandleFunc("POST /v1/media-assets/upload", s.withCORS(s.handleUploadTargetMediaAsset))
	mux.HandleFunc("GET /v1/media-assets", s.withCORS(s.handleListTargetMediaAssets))
	mux.HandleFunc("GET /v1/media-assets/{media_asset_id}", s.withCORS(s.handleGetTargetMediaAsset))
	mux.HandleFunc("DELETE /v1/media-assets/{media_asset_id}", s.withCORS(s.handleDeleteTargetMediaAsset))
	mux.HandleFunc("POST /v1/media-assets/{media_asset_id}/refresh-metadata", s.withCORS(s.handleRefreshMetadataEnrichment))
	mux.HandleFunc("POST /v1/media-assets/{media_asset_id}/exports", s.withCORS(s.handleCreateExportJob))
	mux.HandleFunc("GET /v1/export-jobs", s.withCORS(s.handleListExportJobs))
	mux.HandleFunc("GET /v1/export-jobs/{export_job_id}", s.withCORS(s.handleGetExportJob))
	mux.HandleFunc("POST /v1/export-jobs/{export_job_id}/cancel", s.withCORS(s.handleCancelExportJob))
	mux.HandleFunc("POST /v1/export-jobs/{export_job_id}/retry", s.withCORS(s.handleRetryExportJob))
	mux.HandleFunc("POST /v1/export-jobs/{export_job_id}/deliveries/claim", s.withCORS(s.handleClaimExportDelivery))
	mux.HandleFunc("POST /v1/export-jobs/{export_job_id}/deliveries/heartbeat", s.withCORS(s.handleHeartbeatExportDelivery))
	mux.HandleFunc("POST /v1/export-jobs/{export_job_id}/deliveries/ack", s.withCORS(s.handleAckExportDelivery))
	mux.HandleFunc("POST /v1/export-jobs/{export_job_id}/deliveries/fail", s.withCORS(s.handleFailExportDelivery))
	mux.HandleFunc("GET /v1/export-jobs/{export_job_id}/download", s.withCORS(s.handleResolveExportDownload))
	mux.HandleFunc("GET /v1/collections/inbox", s.withCORS(s.handleGetTargetInboxCollection))
	mux.HandleFunc("POST /v1/collections", s.withCORS(s.handleCreateTargetCollection))
	mux.HandleFunc("GET /v1/collections", s.withCORS(s.handleListTargetCollections))
	mux.HandleFunc("GET /v1/collections/{collection_id}", s.withCORS(s.handleGetTargetCollection))
	mux.HandleFunc("PATCH /v1/collections/{collection_id}", s.withCORS(s.handleUpdateTargetCollection))
	mux.HandleFunc("POST /v1/collections/{collection_id}/items", s.withCORS(s.handleUpdateTargetCollectionItems))
	mux.HandleFunc("DELETE /v1/collections/{collection_id}/items/{media_asset_id}", s.withCORS(s.handleRemoveTargetCollectionItem))
	mux.HandleFunc("POST /v1/collections/{collection_id}/processing-runs", s.withCORS(s.handleStartTargetCollectionProcessingRun))
	mux.HandleFunc("POST /v1/selection-snapshots", s.withCORS(s.handleCreateTargetSelectionSnapshot))
	mux.HandleFunc("GET /v1/selection-snapshots/{selection_snapshot_id}", s.withCORS(s.handleGetTargetSelectionSnapshot))
	mux.HandleFunc("POST /v1/analysis-runs", s.withCORS(s.handleCreateTargetAnalysisRun))
	mux.HandleFunc("GET /v1/analysis-runs", s.withCORS(s.handleListTargetAnalysisRuns))
	mux.HandleFunc("GET /v1/analysis-runs/{analysis_run_id}", s.withCORS(s.handleGetTargetAnalysisRun))
	mux.HandleFunc("POST /v1/analysis-runs/{analysis_run_id}/cancel", s.withCORS(s.handleCancelTargetAnalysisRun))
	mux.HandleFunc("POST /v1/analysis-runs/{analysis_run_id}/retry", s.withCORS(s.handleRetryTargetAnalysisRun))
	mux.HandleFunc("GET /v1/analysis-runs/{analysis_run_id}/events", s.withCORS(s.handleListTargetAnalysisRunEvents))
	mux.HandleFunc("GET /v1/analysis-runs/{analysis_run_id}/artifacts", s.withCORS(s.handleListTargetArtifacts))
	mux.HandleFunc("GET /v1/artifacts", s.withCORS(s.handleListTargetArtifacts))
	mux.HandleFunc("GET /v1/artifacts/{artifact_id}", s.withCORS(s.handleGetTargetArtifact))
	mux.HandleFunc("POST /v1/artifacts/{artifact_id}/refresh", s.withCORS(s.handleRefreshTargetArtifact))
	mux.HandleFunc("GET /v1/diagnostics", s.withCORS(s.handleListTargetDiagnostics))
	mux.HandleFunc("GET /v1/admin/observability", s.withCORS(s.handleGetTargetObservabilitySnapshot))
	mux.HandleFunc("GET /v1/ws", s.withCORS(s.HandleWebsocket))

	mux.HandleFunc("GET /internal/v1/channel-accounts", s.withCORS(s.handleListTargetChannelAccounts))
	mux.HandleFunc("PUT /internal/v1/channel-accounts", s.withCORS(s.handleResolveTargetChannelAccount))
	mux.HandleFunc("PATCH /internal/v1/channel-accounts/{channel_account_id}", s.withCORS(s.handleUpdateTargetChannelAccount))
	mux.HandleFunc("GET /internal/v1/reusable-transcripts", s.withCORS(s.handleFindTargetReusableTranscript))
	mux.HandleFunc("PUT /internal/v1/channel-surfaces", s.withCORS(s.handleUpsertTargetChannelSurface))
	mux.HandleFunc("GET /internal/v1/channel-surfaces", s.withCORS(s.handleListTargetChannelSurfaces))
	mux.HandleFunc("GET /internal/v1/channel-surfaces/active", s.withCORS(s.handleListActiveTargetChannelSurfaces))
	mux.HandleFunc("PATCH /internal/v1/channel-surfaces/{channel_surface_id}/display-state", s.withCORS(s.handleReplaceTargetChannelSurfaceDisplayState))
	mux.HandleFunc("POST /internal/v1/channel-surfaces/{channel_surface_id}/supersede", s.withCORS(s.handleSupersedeTargetChannelSurface))
	mux.HandleFunc("GET /internal/v1/channel-surfaces/{channel_surface_id}/events", s.withCORS(s.handleListTargetChannelSurfaceEvents))
	mux.HandleFunc("GET /internal/v1/analysis-runs/queue", s.withCORS(s.handleListTargetAnalysisRunStepQueue))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/steps/claim", s.withCORS(s.handleClaimTargetAnalysisRunStep))
	mux.HandleFunc("GET /internal/v1/analysis-runs/{analysis_run_id}/steps/cancel-check", s.withCORS(s.handleCheckTargetAnalysisRunStepCancel))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/steps/progress", s.withCORS(s.handleRecordTargetAnalysisRunStepProgress))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/steps/finalize", s.withCORS(s.handleFinalizeTargetAnalysisRunStep))
	mux.HandleFunc("GET /internal/v1/analysis-runs/{analysis_run_id}/request-access", s.withCORS(s.handleResolveTargetAnalysisRunStepRequestAccess))
	mux.HandleFunc("GET /internal/v1/artifacts/{artifact_id}/download-access", s.withCORS(s.handleResolveTargetArtifactDownloadAccess))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/artifacts", s.withCORS(s.handleRecordTargetAnalysisRunArtifacts))
	mux.HandleFunc("POST /internal/v1/analysis-runs/{analysis_run_id}/diagnostics", s.withCORS(s.handleRecordTargetAnalysisRunDiagnostics))
	mux.HandleFunc("GET /internal/v1/export-jobs/queue", s.withCORS(s.handleListExportQueue))
	mux.HandleFunc("POST /internal/v1/export-jobs/{export_job_id}/claim", s.withCORS(s.handleClaimExportJob))
	mux.HandleFunc("POST /internal/v1/export-jobs/{export_job_id}/progress", s.withCORS(s.handleRecordExportProgress))
	mux.HandleFunc("GET /internal/v1/export-jobs/{export_job_id}/cancel-check", s.withCORS(s.handleCheckExportCancel))
	mux.HandleFunc("POST /internal/v1/export-jobs/{export_job_id}/finalize", s.withCORS(s.handleFinalizeExportJob))
	mux.HandleFunc("GET /internal/v1/export-jobs/{export_job_id}/download-access", s.withCORS(s.handleResolveInternalExportDownloadAccess))
	mux.HandleFunc("POST /internal/v1/export-jobs/reclaim", s.withCORS(s.handleReclaimExportJobs))
	mux.HandleFunc("GET /internal/v1/metadata-enrichment-jobs/queue", s.withCORS(s.handleListMetadataEnrichmentQueue))
	mux.HandleFunc("POST /internal/v1/metadata-enrichment-jobs/{enrichment_id}/claim", s.withCORS(s.handleClaimMetadataEnrichment))
	mux.HandleFunc("POST /internal/v1/metadata-enrichment-jobs/{enrichment_id}/progress", s.withCORS(s.handleRecordMetadataEnrichmentProgress))
	mux.HandleFunc("POST /internal/v1/metadata-enrichment-jobs/{enrichment_id}/finalize", s.withCORS(s.handleFinalizeMetadataEnrichment))
	mux.HandleFunc("POST /internal/v1/metadata-enrichment-jobs/reclaim", s.withCORS(s.handleReclaimMetadataEnrichments))
	mux.HandleFunc("POST /internal/v1/retention/sweep", s.withCORS(s.handleSweepRetention))
	mux.HandleFunc("POST /internal/v1/retention/reconcile", s.withCORS(s.handleReconcileRetention))
}

func (s *Server) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.writeCORSHeaders(w, r) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/internal/") && s.internalToken != "" &&
			!constantTimeTokenEqual(r.Header.Get("X-Platform-Internal-Token"), s.internalToken) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
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
	if s.strictLocalRequests && !isAllowedLocalHTTPHost(r.Host) {
		return false
	}
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

func isAllowedLocalHTTPHost(hostport string) bool {
	parsed, err := neturl.Parse("//" + strings.TrimSpace(hostport))
	if err != nil {
		return false
	}
	switch strings.ToLower(parsed.Hostname()) {
	case "localhost", "127.0.0.1", "::1", "api":
		return true
	default:
		return false
	}
}

func constantTimeTokenEqual(provided, expected string) bool {
	provided = strings.TrimSpace(provided)
	expected = strings.TrimSpace(expected)
	if provided == "" || expected == "" || len(provided) != len(expected) {
		return false
	}
	var mismatch byte
	for index := range provided {
		mismatch |= provided[index] ^ expected[index]
	}
	return mismatch == 0
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

type RequestAccessResponse struct {
	Provider            string `json:"provider"`
	URL                 string `json:"url"`
	ExpiresAt           string `json:"expires_at"`
	RequestRef          string `json:"request_ref"`
	RequestDigestSHA256 string `json:"request_digest_sha256"`
	RequestBytes        int64  `json:"request_bytes"`
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

type TargetObservabilitySnapshot struct {
	QueueTasks                       int       `json:"queue_tasks"`
	QueueLagSeconds                  int64     `json:"queue_lag_seconds"`
	CleanupFailures                  int       `json:"cleanup_failures"`
	CleanupFailuresRecent            int       `json:"cleanup_failures_recent"`
	ArtifactResolutionFailures       int       `json:"artifact_resolution_failures"`
	ArtifactResolutionFailuresRecent int       `json:"artifact_resolution_failures_recent"`
	ObservabilityWindowSeconds       int64     `json:"observability_window_seconds"`
	GeneratedAt                      time.Time `json:"generated_at"`
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

func parsePageRequest(r *http.Request) (string, int) {
	const (
		defaultPageSize = 50
		maxPageSize     = 100
	)
	pageSize := defaultPageSize
	if raw := strings.TrimSpace(r.URL.Query().Get("page_size")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			pageSize = parsed
		}
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return strings.TrimSpace(r.URL.Query().Get("cursor")), pageSize
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

func (s *Server) readTargetMultipartUploadBody(w http.ResponseWriter, reader io.Reader) ([]byte, bool) {
	readBody := io.ReadAll
	if s.readUploadBody != nil {
		readBody = s.readUploadBody
	}
	body, err := readBody(reader)
	if err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "media asset upload body could not be read", details: err.Error()})
		return nil, false
	}
	if len(body) == 0 {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_asset", message: "media asset upload body is empty"})
		return nil, false
	}
	return body, true
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

func mapFinalStorageError(err error) apiError {
	switch {
	case errors.Is(err, storage.ErrCollectionVersionConflict):
		return apiError{status: http.StatusConflict, code: "collection_version_conflict", message: "collection version conflict"}
	case errors.Is(err, storage.ErrProcessingRunConflict):
		return apiError{status: http.StatusConflict, code: "processing_run_conflict", message: "idempotency key was already used for a different processing request"}
	case errors.Is(err, storage.ErrRetryRequiresTerminalStatus):
		return apiError{status: http.StatusConflict, code: "retry_requires_terminal_status", message: "analysis run must be terminal before retry"}
	case errors.Is(err, storage.ErrExportJobConflict):
		return apiError{status: http.StatusConflict, code: "export_job_conflict", message: "export job state conflict"}
	case errors.Is(err, storage.ErrMetadataEnrichmentConflict):
		return apiError{status: http.StatusConflict, code: "metadata_enrichment_conflict", message: "metadata enrichment state conflict"}
	case errors.Is(err, storage.ErrStoredObjectUnavailable):
		return apiError{status: http.StatusConflict, code: "stored_object_unavailable", message: "stored object is not available"}
	case errors.Is(err, storage.ErrMediaAssetNotFound),
		errors.Is(err, storage.ErrCollectionNotFound),
		errors.Is(err, storage.ErrSelectionSnapshotNotFound),
		errors.Is(err, storage.ErrAnalysisRunNotFound),
		errors.Is(err, storage.ErrExportJobNotFound),
		errors.Is(err, storage.ErrMetadataEnrichmentNotFound),
		errors.Is(err, storage.ErrArtifactNotFound):
		return apiError{status: http.StatusNotFound, code: "not_found", message: "resource was not found"}
	case errors.Is(err, storage.ErrArtifactResolutionFailed):
		return apiError{status: http.StatusBadGateway, code: "artifact_resolution_failed", message: "artifact link could not be resolved"}
	case errors.Is(err, storage.ErrContractViolation):
		return apiError{status: http.StatusBadRequest, code: "invalid_request", message: err.Error()}
	case errors.Is(err, storage.ErrStorageUnavailable):
		return apiError{status: http.StatusServiceUnavailable, code: "storage_unavailable", message: err.Error()}
	default:
		return apiError{status: http.StatusInternalServerError, code: "internal_error", message: err.Error()}
	}
}

func (s *Server) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
