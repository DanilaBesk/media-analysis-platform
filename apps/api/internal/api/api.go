package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/danila/media-analysis-platform/apps/api/internal/jobs"
	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	"github.com/danila/media-analysis-platform/apps/api/internal/ws"
)

const (
	ValidateRequestMarker           = "[ApiHttp][validateRequest][BLOCK_VALIDATE_REQUEST_AND_SHAPE_RESPONSE]"
	defaultMaxRequestBody           = 1 << 30
	batchManifestVersion            = "batch-transcription.v1"
	BatchCompletionPolicyAllSources = "succeed_when_all_sources_succeed"
	BatchCompletionPolicyAnySource  = "succeed_when_any_source_succeeds"
)

type Logger interface {
	Printf(format string, args ...any)
}

type PublicService interface {
	CreateUpload(ctx context.Context, req UploadCommand) ([]JobSnapshot, error)
	CreateCombined(ctx context.Context, req UploadCommand) (JobSnapshot, error)
	CreateBatch(ctx context.Context, req BatchCommand) (JobSnapshot, error)
	CreateFromURL(ctx context.Context, req URLCommand) (JobSnapshot, error)
	CreateAgentRun(ctx context.Context, req AgentRunCommand) (JobSnapshot, error)
	GetJob(ctx context.Context, jobID string) (JobSnapshot, error)
	ListJobs(ctx context.Context, filter ListJobsFilter) (JobListResponse, error)
	CreateReport(ctx context.Context, jobID string, req ChildCreateRequest) (JobSnapshot, error)
	CreateDeepResearch(ctx context.Context, jobID string, req ChildCreateRequest) (JobSnapshot, error)
	CancelJob(ctx context.Context, jobID string) (JobSnapshot, error)
	RetryJob(ctx context.Context, jobID string) (JobSnapshot, error)
	ResolveArtifact(ctx context.Context, artifactID string) (storage.ArtifactResolution, error)
	ListJobEvents(ctx context.Context, jobID string) ([]ws.JobEventEnvelope, error)
}

type WorkerService interface {
	Claim(ctx context.Context, jobID string, req ClaimRequest) (ClaimResponse, error)
	RecordProgress(ctx context.Context, jobID string, req ProgressRequest) error
	RecordArtifacts(ctx context.Context, jobID string, req ArtifactUpsertRequest) error
	Finalize(ctx context.Context, jobID string, req FinalizeRequest) (JobSnapshot, error)
	CancelCheck(ctx context.Context, jobID, executionID string) (CancelCheckResponse, error)
	ResolveAgentRunRequestAccess(ctx context.Context, jobID, executionID string) (storage.AgentRunRequestAccess, error)
}

type WebsocketAcceptor interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}

type Dependencies struct {
	Public    PublicService
	Worker    WorkerService
	Websocket WebsocketAcceptor
}

type Server struct {
	deps            Dependencies
	logger          Logger
	maxRequestBytes int64
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
	mux.HandleFunc("OPTIONS /v1/transcription-jobs", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/transcription-jobs/combined", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/transcription-jobs/batch", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/transcription-jobs/from-url", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/agent-runs", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/jobs/{job_id}", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/jobs", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/transcription-jobs/{job_id}/report-jobs", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/report-jobs/{job_id}/deep-research-jobs", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/jobs/{job_id}/cancel", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/jobs/{job_id}/retry", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/artifacts/{artifact_id}", s.handleCORSPreflight)
	mux.HandleFunc("OPTIONS /v1/jobs/{job_id}/events", s.handleCORSPreflight)
	mux.HandleFunc("POST /v1/transcription-jobs", s.withCORS(s.handleCreateUpload))
	mux.HandleFunc("POST /v1/transcription-jobs/combined", s.withCORS(s.handleCreateCombinedUpload))
	mux.HandleFunc("POST /v1/transcription-jobs/batch", s.withCORS(s.handleCreateBatch))
	mux.HandleFunc("POST /v1/transcription-jobs/from-url", s.withCORS(s.handleCreateFromURL))
	mux.HandleFunc("POST /v1/agent-runs", s.withCORS(s.handleCreateAgentRun))
	mux.HandleFunc("GET /v1/jobs/{job_id}", s.withCORS(s.handleGetJob))
	mux.HandleFunc("GET /v1/jobs", s.withCORS(s.handleListJobs))
	mux.HandleFunc("POST /v1/transcription-jobs/{job_id}/report-jobs", s.withCORS(s.handleCreateReport))
	mux.HandleFunc("POST /v1/report-jobs/{job_id}/deep-research-jobs", s.withCORS(s.handleCreateDeepResearch))
	mux.HandleFunc("POST /v1/jobs/{job_id}/cancel", s.withCORS(s.handleCancelJob))
	mux.HandleFunc("POST /v1/jobs/{job_id}/retry", s.withCORS(s.handleRetryJob))
	mux.HandleFunc("GET /v1/artifacts/{artifact_id}", s.withCORS(s.handleResolveArtifact))
	mux.HandleFunc("GET /v1/jobs/{job_id}/events", s.withCORS(s.handleListEvents))
	mux.HandleFunc("GET /v1/ws", s.withCORS(s.HandleWebsocket))
	mux.HandleFunc("POST /internal/v1/jobs/{job_id}/claim", s.withCORS(s.handleClaimJob))
	mux.HandleFunc("POST /internal/v1/jobs/{job_id}/progress", s.withCORS(s.handleRecordProgress))
	mux.HandleFunc("POST /internal/v1/jobs/{job_id}/artifacts", s.withCORS(s.handleRecordArtifacts))
	mux.HandleFunc("POST /internal/v1/jobs/{job_id}/finalize", s.withCORS(s.handleFinalizeJob))
	mux.HandleFunc("GET /internal/v1/jobs/{job_id}/cancel-check", s.withCORS(s.handleCancelCheck))
	mux.HandleFunc("GET /internal/v1/jobs/{job_id}/request-access", s.withCORS(s.handleResolveAgentRunRequestAccess))
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
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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

type DeliveryConfig struct {
	Strategy   string `json:"strategy"`
	WebhookURL string `json:"webhook_url,omitempty"`
}

type UploadFile struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	Body        []byte `json:"-"`
}

type BatchUploadFile struct {
	PartName string `json:"part_name"`
	UploadFile
}

type UploadCommand struct {
	IdempotencyKey string         `json:"-"`
	DisplayName    string         `json:"display_name,omitempty"`
	ClientRef      string         `json:"client_ref,omitempty"`
	Delivery       DeliveryConfig `json:"delivery"`
	Files          []UploadFile   `json:"files"`
}

type BatchCommand struct {
	IdempotencyKey string              `json:"-"`
	DisplayName    string              `json:"display_name,omitempty"`
	ClientRef      string              `json:"client_ref,omitempty"`
	Delivery       DeliveryConfig      `json:"delivery"`
	Manifest       BatchSourceManifest `json:"source_manifest"`
	ManifestJSON   []byte              `json:"-"`
	Files          []BatchUploadFile   `json:"files"`
}

type BatchSourceManifest struct {
	ManifestVersion     string                 `json:"manifest_version"`
	OrderedSourceLabels []string               `json:"ordered_source_labels"`
	Sources             map[string]BatchSource `json:"sources"`
	CompletionPolicy    string                 `json:"completion_policy"`
}

type BatchSource struct {
	SourceKind       string `json:"source_kind"`
	FilePart         string `json:"file_part,omitempty"`
	URL              string `json:"url,omitempty"`
	DisplayName      string `json:"display_name,omitempty"`
	OriginalFilename string `json:"original_filename,omitempty"`
}

type URLCommand struct {
	IdempotencyKey string         `json:"-"`
	SourceKind     string         `json:"source_kind"`
	URL            string         `json:"url"`
	DisplayName    string         `json:"display_name,omitempty"`
	ClientRef      string         `json:"client_ref,omitempty"`
	Delivery       DeliveryConfig `json:"delivery"`
}

type ChildCreateRequest struct {
	IdempotencyKey string         `json:"-"`
	ClientRef      string         `json:"client_ref,omitempty"`
	Delivery       DeliveryConfig `json:"delivery"`
}

type AgentRunCommand struct {
	IdempotencyKey string          `json:"-"`
	ParentJobID    string          `json:"-"`
	HarnessName    string          `json:"harness_name"`
	Request        AgentRunRequest `json:"request"`
	ClientRef      string          `json:"client_ref,omitempty"`
	Delivery       DeliveryConfig  `json:"delivery"`
}

type AgentRunRequest struct {
	Prompt         string                  `json:"prompt,omitempty"`
	Payload        json.RawMessage         `json:"payload,omitempty"`
	InputArtifacts []AgentRunInputArtifact `json:"input_artifacts,omitempty"`
}

type AgentRunInputArtifact struct {
	ArtifactID   string `json:"artifact_id,omitempty"`
	ArtifactKind string `json:"artifact_kind,omitempty"`
	ObjectKey    string `json:"object_key,omitempty"`
	URI          string `json:"uri,omitempty"`
	Filename     string `json:"filename,omitempty"`
}

type ListJobsFilter struct {
	Status    string `json:"status,omitempty"`
	JobType   string `json:"job_type,omitempty"`
	RootJobID string `json:"root_job_id,omitempty"`
	Page      int    `json:"page"`
	PageSize  int    `json:"page_size"`
}

type JobAcceptedEnvelope struct {
	Job JobSnapshot `json:"job"`
}

type JobsAcceptedEnvelope struct {
	Jobs []JobSnapshot `json:"jobs"`
}

type JobListResponse struct {
	Items    []JobSnapshot `json:"items"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
	NextPage *int          `json:"next_page,omitempty"`
}

type JobEventListResponse struct {
	Items []JobEventView `json:"items"`
}

type JobSnapshot struct {
	JobID             string              `json:"job_id"`
	RootJobID         string              `json:"root_job_id"`
	ParentJobID       *string             `json:"parent_job_id,omitempty"`
	RetryOfJobID      *string             `json:"retry_of_job_id,omitempty"`
	JobType           string              `json:"job_type"`
	Status            string              `json:"status"`
	Version           int64               `json:"version"`
	DisplayName       *string             `json:"display_name,omitempty"`
	ClientRef         *string             `json:"client_ref,omitempty"`
	Delivery          DeliveryConfig      `json:"delivery"`
	SourceSet         SourceSetView       `json:"source_set"`
	Artifacts         []ArtifactSummary   `json:"artifacts"`
	Children          []ChildJobReference `json:"children"`
	Progress          *ProgressState      `json:"progress,omitempty"`
	LatestError       *ErrorInfo          `json:"latest_error,omitempty"`
	CreatedAt         time.Time           `json:"created_at"`
	StartedAt         *time.Time          `json:"started_at,omitempty"`
	FinishedAt        *time.Time          `json:"finished_at,omitempty"`
	CancelRequestedAt *time.Time          `json:"cancel_requested_at,omitempty"`
}

type SourceSetView struct {
	SourceSetID string          `json:"source_set_id"`
	InputKind   string          `json:"input_kind"`
	Items       []SourceSetItem `json:"items"`
}

type SourceSetItem struct {
	Position    int             `json:"position"`
	SourceLabel *string         `json:"source_label,omitempty"`
	Source      SourceReference `json:"source"`
}

type SourceReference struct {
	SourceID         string  `json:"source_id"`
	SourceKind       string  `json:"source_kind"`
	DisplayName      *string `json:"display_name,omitempty"`
	OriginalFilename *string `json:"original_filename,omitempty"`
	SourceURL        *string `json:"source_url,omitempty"`
}

type ArtifactSummary struct {
	ArtifactID   string    `json:"artifact_id"`
	ArtifactKind string    `json:"artifact_kind"`
	Filename     string    `json:"filename"`
	MIMEType     string    `json:"mime_type"`
	SizeBytes    int64     `json:"size_bytes"`
	CreatedAt    time.Time `json:"created_at"`
}

type ChildJobReference struct {
	JobID     string `json:"job_id"`
	JobType   string `json:"job_type"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	JobURL    string `json:"job_url"`
	RootJobID string `json:"root_job_id"`
}

type ProgressState struct {
	Stage   string  `json:"stage"`
	Message *string `json:"message,omitempty"`
}

type ErrorInfo struct {
	Code    string  `json:"code"`
	Message *string `json:"message,omitempty"`
}

type ArtifactResolutionView struct {
	ArtifactID   string             `json:"artifact_id"`
	JobID        string             `json:"job_id"`
	ArtifactKind string             `json:"artifact_kind"`
	Filename     string             `json:"filename"`
	MIMEType     string             `json:"mime_type"`
	SizeBytes    int64              `json:"size_bytes"`
	CreatedAt    time.Time          `json:"created_at"`
	Download     DownloadDescriptor `json:"download"`
}

type DownloadDescriptor struct {
	Provider  string    `json:"provider"`
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

type JobEventView struct {
	EventID   string          `json:"event_id"`
	EventType string          `json:"event_type"`
	JobID     string          `json:"job_id"`
	RootJobID string          `json:"root_job_id"`
	Version   int64           `json:"version"`
	EmittedAt time.Time       `json:"emitted_at"`
	Payload   JobEventPayload `json:"payload"`
}

type JobEventPayload struct {
	Status          string  `json:"status"`
	ProgressStage   *string `json:"progress_stage,omitempty"`
	ProgressMessage *string `json:"progress_message,omitempty"`
}

type ClaimRequest struct {
	WorkerKind string `json:"worker_kind"`
	TaskType   string `json:"task_type"`
}

type ClaimResponse struct {
	ExecutionID   string         `json:"execution_id"`
	JobID         string         `json:"job_id"`
	RootJobID     string         `json:"root_job_id"`
	ParentJobID   *string        `json:"parent_job_id,omitempty"`
	RetryOfJobID  *string        `json:"retry_of_job_id,omitempty"`
	JobType       string         `json:"job_type"`
	Version       int64          `json:"version"`
	OrderedInputs []OrderedInput `json:"ordered_inputs"`
	Params        map[string]any `json:"params"`
}

type OrderedInput struct {
	Position         int     `json:"position"`
	SourceID         string  `json:"source_id"`
	SourceKind       string  `json:"source_kind"`
	DisplayName      *string `json:"display_name,omitempty"`
	OriginalFilename *string `json:"original_filename,omitempty"`
	ObjectKey        *string `json:"object_key,omitempty"`
	SourceURL        *string `json:"source_url,omitempty"`
	SHA256           *string `json:"sha256,omitempty"`
	SizeBytes        *int64  `json:"size_bytes,omitempty"`
}

type ProgressRequest struct {
	ExecutionID     string  `json:"execution_id"`
	ProgressStage   string  `json:"progress_stage"`
	ProgressMessage *string `json:"progress_message,omitempty"`
}

type ArtifactDescriptor struct {
	ArtifactKind string  `json:"artifact_kind"`
	Format       *string `json:"format,omitempty"`
	Filename     string  `json:"filename"`
	MIMEType     string  `json:"mime_type"`
	ObjectKey    string  `json:"object_key"`
	SizeBytes    int64   `json:"size_bytes"`
}

type ArtifactUpsertRequest struct {
	ExecutionID string               `json:"execution_id"`
	Artifacts   []ArtifactDescriptor `json:"artifacts"`
}

type FinalizeRequest struct {
	ExecutionID     string  `json:"execution_id"`
	Outcome         string  `json:"outcome"`
	ProgressStage   *string `json:"progress_stage,omitempty"`
	ProgressMessage *string `json:"progress_message,omitempty"`
	ErrorCode       *string `json:"error_code,omitempty"`
	ErrorMessage    *string `json:"error_message,omitempty"`
}

type CancelCheckResponse struct {
	CancelRequested   bool       `json:"cancel_requested"`
	Status            string     `json:"status"`
	CancelRequestedAt *time.Time `json:"cancel_requested_at,omitempty"`
}

type AgentRunRequestAccessResponse struct {
	Provider            string    `json:"provider"`
	URL                 string    `json:"url"`
	ExpiresAt           time.Time `json:"expires_at"`
	RequestRef          string    `json:"request_ref"`
	RequestDigestSHA256 string    `json:"request_digest_sha256"`
	RequestBytes        int64     `json:"request_bytes"`
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
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

func ValidateRequest(kind string, payload any) error {
	switch req := payload.(type) {
	case UploadCommand:
		return validateUploadCommand(kind, req)
	case BatchCommand:
		return validateBatchCommand(req)
	case URLCommand:
		return validateURLCommand(req)
	case ChildCreateRequest:
		return validateDelivery(req.Delivery)
	case AgentRunCommand:
		return validateAgentRunCommand(req)
	case ListJobsFilter:
		return validateListJobsFilter(req)
	case ClaimRequest:
		if strings.TrimSpace(req.WorkerKind) == "" || strings.TrimSpace(req.TaskType) == "" {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "worker_kind and task_type are required"}
		}
	case ProgressRequest:
		if !isUUID(req.ExecutionID) || strings.TrimSpace(req.ProgressStage) == "" {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "execution_id and progress_stage are required"}
		}
	case ArtifactUpsertRequest:
		if !isUUID(req.ExecutionID) || len(req.Artifacts) == 0 {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "execution_id and artifacts are required"}
		}
		for _, artifact := range req.Artifacts {
			if strings.TrimSpace(artifact.ArtifactKind) == "" || strings.TrimSpace(artifact.Filename) == "" || strings.TrimSpace(artifact.MIMEType) == "" || strings.TrimSpace(artifact.ObjectKey) == "" {
				return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "artifact descriptors must include kind, filename, mime_type, and object_key"}
			}
		}
	case FinalizeRequest:
		if !isUUID(req.ExecutionID) || strings.TrimSpace(req.Outcome) == "" {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "execution_id and outcome are required"}
		}
	default:
		return nil
	}
	return nil
}

func (s *Server) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	req, err := s.parseUploadCommand(w, r)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("create_upload", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	jobsAccepted, err := s.deps.Public.CreateUpload(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobsAcceptedEnvelope{Jobs: jobsAccepted})
}

func (s *Server) handleCreateCombinedUpload(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	req, err := s.parseUploadCommand(w, r)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("create_combined_upload", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.CreateCombined(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleCreateBatch(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	req, err := s.parseBatchCommand(w, r)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("create_batch", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.CreateBatch(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleCreateFromURL(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	var body struct {
		SourceKind  string      `json:"source_kind"`
		URL         string      `json:"url"`
		DisplayName string      `json:"display_name"`
		ClientRef   string      `json:"client_ref"`
		Delivery    deliveryDTO `json:"delivery"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, err)
		return
	}
	req := URLCommand{
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		SourceKind:     strings.TrimSpace(body.SourceKind),
		URL:            strings.TrimSpace(body.URL),
		DisplayName:    strings.TrimSpace(body.DisplayName),
		ClientRef:      strings.TrimSpace(body.ClientRef),
		Delivery:       body.Delivery.toDeliveryConfig(),
	}
	if err := s.validateRequest("create_from_url", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.CreateFromURL(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleCreateAgentRun(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	var body struct {
		HarnessName string          `json:"harness_name"`
		Request     AgentRunRequest `json:"request"`
		ClientRef   string          `json:"client_ref"`
		Delivery    deliveryDTO     `json:"delivery"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, err)
		return
	}
	req := AgentRunCommand{
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		HarnessName:    strings.TrimSpace(body.HarnessName),
		Request:        normalizeAgentRunRequest(body.Request),
		ClientRef:      strings.TrimSpace(body.ClientRef),
		Delivery:       body.Delivery.toDeliveryConfig(),
	}
	if req.Delivery.Strategy == "" {
		req.Delivery.Strategy = storage.DeliveryStrategyPolling
	}
	if err := s.validateRequest("create_agent_run", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.CreateAgentRun(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.GetJob(r.Context(), jobID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	filter, err := parseListJobsFilter(r)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("list_jobs", filter); err != nil {
		s.writeAPIError(w, err)
		return
	}
	response, err := s.deps.Public.ListJobs(r.Context(), filter)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCreateReport(w http.ResponseWriter, r *http.Request) {
	s.handleChildCreate(w, r, "report")
}

func (s *Server) handleCreateDeepResearch(w http.ResponseWriter, r *http.Request) {
	s.handleChildCreate(w, r, "deep_research")
}

func (s *Server) handleChildCreate(w http.ResponseWriter, r *http.Request, childType string) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	req, err := parseChildCreateRequest(r)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("create_child", req); err != nil {
		s.writeAPIError(w, err)
		return
	}

	var job JobSnapshot
	switch childType {
	case "report":
		job, err = s.deps.Public.CreateReport(r.Context(), jobID, req)
	case "deep_research":
		job, err = s.deps.Public.CreateDeepResearch(r.Context(), jobID, req)
	default:
		err = apiError{status: http.StatusInternalServerError, code: "internal_error", message: "unsupported child route"}
	}
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.CancelJob(r.Context(), jobID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Public.RetryJob(r.Context(), jobID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleResolveArtifact(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	artifactID, err := parseUUIDPathValue(r, "artifact_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	resolution, err := s.deps.Public.ResolveArtifact(r.Context(), artifactID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, artifactResolutionViewFromStorage(resolution))
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	events, err := s.deps.Public.ListJobEvents(r.Context(), jobID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	response := JobEventListResponse{Items: make([]JobEventView, 0, len(events))}
	for _, event := range events {
		response.Items = append(response.Items, jobEventViewFromEnvelope(event))
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleClaimJob(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	var req ClaimRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("claim_job", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	response, err := s.deps.Worker.Claim(r.Context(), jobID, req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRecordProgress(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	var req ProgressRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("record_progress", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.deps.Worker.RecordProgress(r.Context(), jobID, req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleRecordArtifacts(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	var req ArtifactUpsertRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("record_artifacts", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.deps.Worker.RecordArtifacts(r.Context(), jobID, req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleFinalizeJob(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	var req FinalizeRequest
	if err := decodeJSONBody(r, &req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	if err := s.validateRequest("finalize_job", req); err != nil {
		s.writeAPIError(w, err)
		return
	}
	job, err := s.deps.Worker.Finalize(r.Context(), jobID, req)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, JobAcceptedEnvelope{Job: job})
}

func (s *Server) handleCancelCheck(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	executionID := strings.TrimSpace(r.URL.Query().Get("execution_id"))
	if !isUUID(executionID) {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_query", message: "execution_id must be a valid UUID"})
		return
	}
	response, err := s.deps.Worker.CancelCheck(r.Context(), jobID, executionID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleResolveAgentRunRequestAccess(w http.ResponseWriter, r *http.Request) {
	if s.deps.Worker == nil {
		s.writeAPIError(w, dependencyUnavailableError("worker control plane is not configured"))
		return
	}
	jobID, err := parseUUIDPathValue(r, "job_id")
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	executionID := strings.TrimSpace(r.URL.Query().Get("execution_id"))
	if !isUUID(executionID) {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_query", message: "execution_id must be a valid UUID"})
		return
	}
	access, err := s.deps.Worker.ResolveAgentRunRequestAccess(r.Context(), jobID, executionID)
	if err != nil {
		s.writeAPIError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, AgentRunRequestAccessResponse{
		Provider:            access.Provider,
		URL:                 access.URL,
		ExpiresAt:           access.ExpiresAt,
		RequestRef:          access.RequestRef,
		RequestDigestSHA256: access.RequestDigestSHA256,
		RequestBytes:        access.RequestBytes,
	})
}

func (s *Server) parseUploadCommand(w http.ResponseWriter, r *http.Request) (UploadCommand, error) {
	if r.ContentLength > s.maxRequestBytes {
		return UploadCommand{}, apiError{status: http.StatusRequestEntityTooLarge, code: "request_too_large", message: "request exceeds the configured maximum size"}
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return UploadCommand{}, apiError{status: http.StatusRequestEntityTooLarge, code: "request_too_large", message: "request exceeds the configured maximum size"}
		}
		return UploadCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "request body must be valid multipart/form-data", details: err.Error()}
	}

	files := r.MultipartForm.File["files"]
	command := UploadCommand{
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		DisplayName:    strings.TrimSpace(r.FormValue("display_name")),
		ClientRef:      strings.TrimSpace(r.FormValue("client_ref")),
		Delivery: DeliveryConfig{
			Strategy:   strings.TrimSpace(r.FormValue("delivery_strategy")),
			WebhookURL: strings.TrimSpace(r.FormValue("delivery_webhook_url")),
		},
		Files: make([]UploadFile, 0, len(files)),
	}
	if command.Delivery.Strategy == "" {
		command.Delivery.Strategy = storage.DeliveryStrategyPolling
	}

	for _, header := range files {
		file, err := header.Open()
		if err != nil {
			return UploadCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "multipart file part could not be opened", details: err.Error()}
		}
		body, readErr := io.ReadAll(file)
		file.Close()
		if readErr != nil {
			return UploadCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "multipart file part could not be read", details: readErr.Error()}
		}
		command.Files = append(command.Files, UploadFile{
			Filename:    filepath.Base(header.Filename),
			ContentType: header.Header.Get("Content-Type"),
			SizeBytes:   int64(len(body)),
			SHA256:      checksum(body),
			Body:        body,
		})
	}
	return command, nil
}

func (s *Server) parseBatchCommand(w http.ResponseWriter, r *http.Request) (BatchCommand, error) {
	if r.ContentLength > s.maxRequestBytes {
		return BatchCommand{}, apiError{status: http.StatusRequestEntityTooLarge, code: "request_too_large", message: "request exceeds the configured maximum size"}
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return BatchCommand{}, apiError{status: http.StatusRequestEntityTooLarge, code: "request_too_large", message: "request exceeds the configured maximum size"}
		}
		return BatchCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "request body must be valid multipart/form-data", details: err.Error()}
	}

	rawManifest := strings.TrimSpace(r.FormValue("source_manifest"))
	if rawManifest == "" {
		return BatchCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "source_manifest is required"}
	}
	var manifest BatchSourceManifest
	if err := json.Unmarshal([]byte(rawManifest), &manifest); err != nil {
		return BatchCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "source_manifest must be valid JSON", details: err.Error()}
	}

	command := BatchCommand{
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		DisplayName:    strings.TrimSpace(r.FormValue("display_name")),
		ClientRef:      strings.TrimSpace(r.FormValue("client_ref")),
		Delivery: DeliveryConfig{
			Strategy:   strings.TrimSpace(r.FormValue("delivery_strategy")),
			WebhookURL: strings.TrimSpace(r.FormValue("delivery_webhook_url")),
		},
		Manifest:     normalizeBatchManifest(manifest),
		ManifestJSON: []byte(rawManifest),
	}
	if command.Delivery.Strategy == "" {
		command.Delivery.Strategy = storage.DeliveryStrategyPolling
	}

	for partName, headers := range r.MultipartForm.File {
		for _, header := range headers {
			file, err := header.Open()
			if err != nil {
				return BatchCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "multipart file part could not be opened", details: err.Error()}
			}
			body, readErr := io.ReadAll(file)
			file.Close()
			if readErr != nil {
				return BatchCommand{}, apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "multipart file part could not be read", details: readErr.Error()}
			}
			command.Files = append(command.Files, BatchUploadFile{
				PartName: strings.TrimSpace(partName),
				UploadFile: UploadFile{
					Filename:    filepath.Base(header.Filename),
					ContentType: header.Header.Get("Content-Type"),
					SizeBytes:   int64(len(body)),
					SHA256:      checksum(body),
					Body:        body,
				},
			})
		}
	}
	return command, nil
}

func parseChildCreateRequest(r *http.Request) (ChildCreateRequest, error) {
	req := ChildCreateRequest{IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key"))}
	if r.ContentLength == 0 {
		req.Delivery.Strategy = storage.DeliveryStrategyPolling
		return req, nil
	}

	var body struct {
		ClientRef string      `json:"client_ref"`
		Delivery  deliveryDTO `json:"delivery"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		return ChildCreateRequest{}, err
	}
	req.ClientRef = strings.TrimSpace(body.ClientRef)
	req.Delivery = body.Delivery.toDeliveryConfig()
	if req.Delivery.Strategy == "" {
		req.Delivery.Strategy = storage.DeliveryStrategyPolling
	}
	return req, nil
}

func parseListJobsFilter(r *http.Request) (ListJobsFilter, error) {
	filter := ListJobsFilter{
		Status:    strings.TrimSpace(r.URL.Query().Get("status")),
		JobType:   strings.TrimSpace(r.URL.Query().Get("job_type")),
		RootJobID: strings.TrimSpace(r.URL.Query().Get("root_job_id")),
		Page:      1,
		PageSize:  20,
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("page")); raw != "" {
		page, err := strconv.Atoi(raw)
		if err != nil {
			return ListJobsFilter{}, apiError{status: http.StatusBadRequest, code: "invalid_query", message: "page must be an integer"}
		}
		filter.Page = page
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("page_size")); raw != "" {
		pageSize, err := strconv.Atoi(raw)
		if err != nil {
			return ListJobsFilter{}, apiError{status: http.StatusBadRequest, code: "invalid_query", message: "page_size must be an integer"}
		}
		filter.PageSize = pageSize
	}
	return filter, nil
}

func validateUploadCommand(kind string, req UploadCommand) error {
	if len(req.Files) == 0 {
		return apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "at least one files part is required"}
	}
	if kind == "create_upload" && len(req.Files) > 1 && strings.TrimSpace(req.DisplayName) != "" {
		return apiError{status: http.StatusBadRequest, code: "display_name_not_allowed_for_multi_file_upload", message: "display_name is allowed only for single-file upload"}
	}
	for _, file := range req.Files {
		if strings.TrimSpace(file.Filename) == "" || len(file.Body) == 0 {
			return apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "multipart files must be non-empty and named"}
		}
	}
	return validateDelivery(req.Delivery)
}

func validateBatchCommand(req BatchCommand) error {
	if req.Manifest.ManifestVersion != batchManifestVersion {
		return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "source_manifest.manifest_version must be batch-transcription.v1"}
	}
	if len(req.Manifest.OrderedSourceLabels) == 0 || len(req.Manifest.Sources) == 0 {
		return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "source_manifest must include ordered_source_labels and sources"}
	}
	if req.Manifest.CompletionPolicy == "" {
		req.Manifest.CompletionPolicy = BatchCompletionPolicyAllSources
	}
	switch req.Manifest.CompletionPolicy {
	case BatchCompletionPolicyAllSources, BatchCompletionPolicyAnySource:
	default:
		return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "unsupported completion_policy"}
	}

	filesByPart := map[string]BatchUploadFile{}
	for _, file := range req.Files {
		if strings.TrimSpace(file.PartName) == "" || strings.TrimSpace(file.Filename) == "" || len(file.Body) == 0 {
			return apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "batch file parts must be non-empty and named"}
		}
		if _, exists := filesByPart[file.PartName]; exists {
			return apiError{status: http.StatusBadRequest, code: "invalid_multipart", message: "batch file part names must be unique"}
		}
		filesByPart[file.PartName] = file
	}

	seen := map[string]struct{}{}
	for _, label := range req.Manifest.OrderedSourceLabels {
		if !validBatchLabel(label) {
			return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "source labels must match ^[a-z][a-z0-9_-]*$"}
		}
		if _, ok := seen[label]; ok {
			return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "ordered_source_labels must be unique"}
		}
		seen[label] = struct{}{}
		source, ok := req.Manifest.Sources[label]
		if !ok {
			return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "ordered_source_labels must match sources keys"}
		}
		switch source.SourceKind {
		case storage.SourceKindUploadedFile, storage.SourceKindTelegramUpload:
			if strings.TrimSpace(source.FilePart) == "" {
				return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "uploaded batch sources require file_part"}
			}
			if _, ok := filesByPart[source.FilePart]; !ok {
				return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "uploaded batch source references missing multipart file_part"}
			}
		case storage.SourceKindYouTubeURL:
			if err := validateBatchURL(source.URL); err != nil {
				return err
			}
		default:
			return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "unsupported batch source_kind"}
		}
	}
	for label := range req.Manifest.Sources {
		if _, ok := seen[label]; !ok {
			return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "sources keys must match ordered_source_labels"}
		}
	}
	return validateDelivery(req.Delivery)
}

func validateURLCommand(req URLCommand) error {
	if req.SourceKind != storage.SourceKindYouTubeURL {
		return apiError{status: http.StatusBadRequest, code: "unsupported_source_url", message: "source_kind must be youtube_url"}
	}
	parsed, err := neturl.Parse(req.URL)
	if err != nil || parsed == nil || !parsed.IsAbs() {
		return apiError{status: http.StatusBadRequest, code: "unsupported_source_url", message: "source URL must be an absolute YouTube URL"}
	}
	host := strings.ToLower(parsed.Hostname())
	switch host {
	case "youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be":
	default:
		return apiError{status: http.StatusBadRequest, code: "unsupported_source_url", message: "source URL must use a supported YouTube host"}
	}
	return validateDelivery(req.Delivery)
}

func validateBatchURL(rawURL string) error {
	parsed, err := neturl.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil || !parsed.IsAbs() {
		return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "batch URL sources require an absolute URL"}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return apiError{status: http.StatusBadRequest, code: "invalid_source_manifest", message: "batch URL sources require http or https"}
	}
	return nil
}

func normalizeBatchManifest(manifest BatchSourceManifest) BatchSourceManifest {
	manifest.ManifestVersion = strings.TrimSpace(manifest.ManifestVersion)
	manifest.CompletionPolicy = strings.TrimSpace(manifest.CompletionPolicy)
	if manifest.CompletionPolicy == "" {
		manifest.CompletionPolicy = BatchCompletionPolicyAllSources
	}
	labels := make([]string, 0, len(manifest.OrderedSourceLabels))
	for _, label := range manifest.OrderedSourceLabels {
		labels = append(labels, strings.TrimSpace(label))
	}
	manifest.OrderedSourceLabels = labels
	sources := map[string]BatchSource{}
	for label, source := range manifest.Sources {
		normalized := BatchSource{
			SourceKind:       strings.TrimSpace(source.SourceKind),
			FilePart:         strings.TrimSpace(source.FilePart),
			URL:              strings.TrimSpace(source.URL),
			DisplayName:      strings.TrimSpace(source.DisplayName),
			OriginalFilename: strings.TrimSpace(source.OriginalFilename),
		}
		sources[strings.TrimSpace(label)] = normalized
	}
	manifest.Sources = sources
	return manifest
}

func validBatchLabel(label string) bool {
	if len(label) == 0 || len(label) > 64 {
		return false
	}
	for idx, r := range label {
		switch {
		case r >= 'a' && r <= 'z':
		case idx > 0 && r >= '0' && r <= '9':
		case idx > 0 && (r == '_' || r == '-'):
		default:
			return false
		}
	}
	return true
}

func validateAgentRunCommand(req AgentRunCommand) error {
	if strings.TrimSpace(req.HarnessName) == "" {
		return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "harness_name is required"}
	}
	if strings.TrimSpace(req.Request.Prompt) == "" && len(req.Request.Payload) == 0 && len(req.Request.InputArtifacts) == 0 {
		return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "request must include prompt, payload, or input_artifacts"}
	}
	if len(req.Request.Payload) > 0 && !json.Valid(req.Request.Payload) {
		return apiError{status: http.StatusBadRequest, code: "invalid_json", message: "request.payload must be valid JSON"}
	}
	if len(req.Request.Payload) > 0 {
		var payloadObject map[string]any
		if err := json.Unmarshal(req.Request.Payload, &payloadObject); err != nil {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "request.payload must be a JSON object"}
		}
	}
	for _, artifact := range req.Request.InputArtifacts {
		artifactID := strings.TrimSpace(artifact.ArtifactID)
		objectKey := strings.TrimSpace(artifact.ObjectKey)
		uri := strings.TrimSpace(artifact.URI)
		if artifactID == "" && objectKey == "" && uri == "" {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "input_artifacts entries require artifact_id, object_key, or uri"}
		}
		if artifactID != "" && !isUUID(artifactID) {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "input_artifacts artifact_id must be a valid UUID"}
		}
		if kind := strings.TrimSpace(artifact.ArtifactKind); kind != "" && !isKnownArtifactKind(kind) {
			return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "input_artifacts artifact_kind is not supported"}
		}
		if uri != "" {
			parsed, err := neturl.Parse(uri)
			if err != nil || parsed == nil || !parsed.IsAbs() {
				return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "input_artifacts uri must be absolute"}
			}
		}
	}
	return validateDelivery(req.Delivery)
}

func isKnownArtifactKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "transcript_plain",
		"transcript_segmented_markdown",
		"transcript_docx",
		"report_markdown",
		"report_docx",
		"deep_research_markdown",
		"execution_log",
		"agent_result_json":
		return true
	default:
		return false
	}
}

func validateDelivery(delivery DeliveryConfig) error {
	strategy := strings.TrimSpace(delivery.Strategy)
	if strategy == "" {
		strategy = storage.DeliveryStrategyPolling
	}
	if strategy != storage.DeliveryStrategyPolling && strategy != storage.DeliveryStrategyWebhook {
		return apiError{status: http.StatusBadRequest, code: "validation_failed", message: "delivery strategy must be polling or webhook"}
	}
	if strategy == storage.DeliveryStrategyWebhook && strings.TrimSpace(delivery.WebhookURL) == "" {
		return apiError{status: http.StatusBadRequest, code: "webhook_url_required", message: "delivery.webhook.url is required when strategy is webhook"}
	}
	if strings.TrimSpace(delivery.WebhookURL) != "" {
		parsed, err := neturl.Parse(delivery.WebhookURL)
		if err != nil || parsed == nil || !parsed.IsAbs() || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return apiError{status: http.StatusBadRequest, code: "invalid_webhook_url", message: "webhook URL must be an absolute http or https URL"}
		}
	}
	return nil
}

func validateListJobsFilter(filter ListJobsFilter) error {
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > 100 {
		return apiError{status: http.StatusBadRequest, code: "invalid_query", message: "page must be >= 1 and page_size must be between 1 and 100"}
	}
	if filter.RootJobID != "" && !isUUID(filter.RootJobID) {
		return apiError{status: http.StatusBadRequest, code: "invalid_query", message: "root_job_id must be a valid UUID"}
	}
	return nil
}

func parseUUIDPathValue(r *http.Request, key string) (string, error) {
	value := strings.TrimSpace(r.PathValue(key))
	if !isUUID(value) {
		return "", apiError{status: http.StatusBadRequest, code: "invalid_uuid", message: fmt.Sprintf("%s must be a valid UUID", key)}
	}
	return value, nil
}

func dependencyUnavailableError(message string) error {
	return apiError{status: http.StatusServiceUnavailable, code: "dependency_unavailable", message: message}
}

func decodeJSONBody(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return apiError{status: http.StatusBadRequest, code: "invalid_json", message: "request body must be valid JSON", details: err.Error()}
	}
	if decoder.More() {
		return apiError{status: http.StatusBadRequest, code: "invalid_json", message: "request body must contain exactly one JSON object"}
	}
	return nil
}

func (s *Server) validateRequest(kind string, payload any) error {
	s.logf("%s kind=%s", ValidateRequestMarker, kind)
	return ValidateRequest(kind, payload)
}

func (s *Server) writeAPIError(w http.ResponseWriter, err error) {
	typed := classifyError(err)
	s.writeJSON(w, typed.status, errorEnvelope{
		Error: errorBody{
			Code:    typed.code,
			Message: typed.message,
			Details: typed.details,
		},
	})
}

func classifyError(err error) apiError {
	var typed apiError
	if errors.As(err, &typed) {
		return typed
	}
	switch {
	case errors.Is(err, jobs.ErrIdempotencyConflict):
		return apiError{status: http.StatusConflict, code: "idempotency_conflict", message: "request conflicts with an existing idempotent submission"}
	case errors.Is(err, jobs.ErrMissingArtifact):
		return apiError{status: http.StatusBadRequest, code: "required_artifact_missing", message: "required artifact is missing for this operation"}
	case errors.Is(err, jobs.ErrJobNotFound):
		return apiError{status: http.StatusNotFound, code: "job_not_found", message: "job was not found"}
	case errors.Is(err, storage.ErrJobNotFound):
		return apiError{status: http.StatusNotFound, code: "job_not_found", message: "job was not found"}
	case errors.Is(err, jobs.ErrInvalidJobState):
		return apiError{status: http.StatusBadRequest, code: "invalid_job_state", message: "job state does not allow this operation"}
	case errors.Is(err, storage.ErrArtifactNotFound):
		return apiError{status: http.StatusNotFound, code: "artifact_not_found", message: "artifact was not found"}
	case errors.Is(err, storage.ErrExecutionNotFound):
		return apiError{status: http.StatusConflict, code: "execution_not_found", message: "execution_id is not active for this job"}
	case errors.Is(err, storage.ErrStorageUnavailable):
		return apiError{status: http.StatusServiceUnavailable, code: "storage_unavailable", message: "storage dependency is unavailable"}
	case errors.Is(err, queue.ErrQueueUnavailable):
		return apiError{status: http.StatusServiceUnavailable, code: "queue_unavailable", message: "queue dependency is unavailable"}
	default:
		return apiError{status: http.StatusInternalServerError, code: "internal_error", message: "internal server error", details: err.Error()}
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func isUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}

func checksum(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func artifactResolutionViewFromStorage(resolution storage.ArtifactResolution) ArtifactResolutionView {
	return ArtifactResolutionView{
		ArtifactID:   resolution.ArtifactID,
		JobID:        resolution.JobID,
		ArtifactKind: resolution.ArtifactKind,
		Filename:     resolution.Filename,
		MIMEType:     resolution.MIMEType,
		SizeBytes:    resolution.SizeBytes,
		CreatedAt:    resolution.CreatedAt,
		Download: DownloadDescriptor{
			Provider:  resolution.Download.Provider,
			URL:       resolution.Download.URL,
			ExpiresAt: resolution.Download.ExpiresAt,
		},
	}
}

func jobEventViewFromEnvelope(envelope ws.JobEventEnvelope) JobEventView {
	view := JobEventView{
		EventID:   envelope.EventID,
		EventType: envelope.EventType,
		JobID:     envelope.JobID,
		RootJobID: envelope.RootJobID,
		Version:   envelope.Version,
		EmittedAt: envelope.EmittedAt,
		Payload: JobEventPayload{
			Status: envelope.Payload.Status,
		},
	}
	if envelope.Payload.ProgressStage != "" {
		view.Payload.ProgressStage = &envelope.Payload.ProgressStage
	}
	if envelope.Payload.ProgressMessage != "" {
		view.Payload.ProgressMessage = &envelope.Payload.ProgressMessage
	}
	return view
}

type deliveryDTO struct {
	Strategy string `json:"strategy"`
	Webhook  *struct {
		URL string `json:"url"`
	} `json:"webhook"`
}

func (d deliveryDTO) toDeliveryConfig() DeliveryConfig {
	cfg := DeliveryConfig{Strategy: strings.TrimSpace(d.Strategy)}
	if d.Webhook != nil {
		cfg.WebhookURL = strings.TrimSpace(d.Webhook.URL)
	}
	if cfg.Strategy == "" {
		cfg.Strategy = storage.DeliveryStrategyPolling
	}
	return cfg
}
