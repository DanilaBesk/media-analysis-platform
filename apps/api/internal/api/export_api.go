package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type TargetCreateExportJobRequest struct {
	ChannelAccountID string          `json:"channel_account_id"`
	MediaAssetID     string          `json:"-"`
	Operation        string          `json:"operation"`
	Variant          json.RawMessage `json:"variant"`
	DeliveryChannel  string          `json:"delivery_channel,omitempty"`
	IdempotencyKey   string          `json:"-"`
}

type TargetListExportJobsRequest struct {
	ChannelAccountID string
	Status           string
	PageSize         int
}

type TargetGetExportJobRequest struct {
	ChannelAccountID string
	ExportJobID      string
}

type TargetExportJobMutationRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	ExportJobID      string `json:"-"`
	IdempotencyKey   string `json:"-"`
}

type TargetExportJobPage struct {
	Items    []TargetExportJob `json:"items"`
	Page     int               `json:"page"`
	PageSize int               `json:"page_size"`
}

type TargetExportJob struct {
	ExportJobID       string                 `json:"export_job_id"`
	ChannelAccountID  string                 `json:"channel_account_id"`
	MediaAssetID      string                 `json:"media_asset_id"`
	Operation         string                 `json:"operation"`
	Variant           json.RawMessage        `json:"variant"`
	OutputProfile     string                 `json:"output_profile"`
	Status            string                 `json:"status"`
	Version           int64                  `json:"version"`
	RetryGeneration   int                    `json:"retry_generation"`
	AttemptNo         int                    `json:"attempt_no"`
	MaxAttempts       int                    `json:"max_attempts"`
	Progress          json.RawMessage        `json:"progress"`
	Output            *TargetExportOutput    `json:"output"`
	Deliveries        []TargetExportDelivery `json:"deliveries"`
	CreatedAt         time.Time              `json:"created_at"`
	StartedAt         *time.Time             `json:"started_at,omitempty"`
	CompletedAt       *time.Time             `json:"completed_at,omitempty"`
	CancelRequestedAt *time.Time             `json:"cancel_requested_at,omitempty"`
	CanceledAt        *time.Time             `json:"canceled_at,omitempty"`
	ExpiresAt         *time.Time             `json:"expires_at,omitempty"`
}

type TargetExportOutput struct {
	ContentType string `json:"content_type"`
	Filename    string `json:"filename"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}

type TargetExportDelivery struct {
	ExportDeliveryID string     `json:"export_delivery_id"`
	ExportJobID      string     `json:"export_job_id"`
	ChannelAccountID string     `json:"channel_account_id"`
	Channel          string     `json:"channel"`
	Status           string     `json:"status"`
	Version          int64      `json:"version"`
	AttemptNo        int        `json:"attempt_no"`
	MaxAttempts      int        `json:"max_attempts"`
	LeaseExpiresAt   *time.Time `json:"lease_expires_at,omitempty"`
	NextAttemptAt    *time.Time `json:"next_attempt_at,omitempty"`
	ExpiresAt        time.Time  `json:"expires_at"`
	DeliveredAt      *time.Time `json:"delivered_at,omitempty"`
	FailureCode      string     `json:"failure_code,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

type TargetClaimExportDeliveryRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	ExportJobID      string `json:"-"`
	Channel          string `json:"channel"`
	LeaseOwner       string `json:"lease_owner"`
	LeaseSeconds     int    `json:"lease_seconds,omitempty"`
}

type TargetExportDeliveryClaim struct {
	Delivery       TargetExportDelivery `json:"delivery"`
	AttemptToken   string               `json:"attempt_token"`
	LeaseOwner     string               `json:"lease_owner"`
	LeaseExpiresAt time.Time            `json:"lease_expires_at"`
}

type TargetHeartbeatExportDeliveryRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	ExportJobID      string `json:"-"`
	ExportDeliveryID string `json:"export_delivery_id"`
	LeaseOwner       string `json:"lease_owner"`
	AttemptToken     string `json:"attempt_token"`
	LeaseSeconds     *int   `json:"lease_seconds,omitempty"`
}

type TargetFinalizeExportDeliveryRequest struct {
	ChannelAccountID string `json:"channel_account_id"`
	ExportJobID      string `json:"-"`
	ExportDeliveryID string `json:"export_delivery_id"`
	LeaseOwner       string `json:"lease_owner"`
	AttemptToken     string `json:"attempt_token"`
	FailureCode      string `json:"failure_code,omitempty"`
	Retryable        bool   `json:"retryable,omitempty"`
	Status           string `json:"-"`
}

type TargetExportDownload struct {
	ExportJobID  string                   `json:"export_job_id"`
	Filename     string                   `json:"filename"`
	ContentType  string                   `json:"content_type"`
	SizeBytes    int64                    `json:"size_bytes"`
	URL          string                   `json:"url"`
	ExpiresAt    time.Time                `json:"expires_at"`
	Presentation TargetExportPresentation `json:"presentation"`
}

type TargetExportPresentation struct {
	Kind            string `json:"kind"`
	Title           string `json:"title"`
	Performer       string `json:"performer"`
	DurationSeconds *int   `json:"duration_seconds"`
}

type TargetExportQueueRequest struct {
	PageSize int
}

type TargetClaimExportJobRequest struct {
	ExportJobID  string `json:"-"`
	LeaseOwner   string `json:"lease_owner"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}

type TargetExportJobClaim struct {
	ExportJob      TargetExportJob    `json:"export_job"`
	AttemptToken   string             `json:"attempt_token"`
	LeaseOwner     string             `json:"lease_owner"`
	LeaseExpiresAt time.Time          `json:"lease_expires_at"`
	Source         TargetExportSource `json:"source"`
}

type TargetExportSource struct {
	MediaAssetID string    `json:"media_asset_id"`
	SourceType   string    `json:"source_type"`
	URL          string    `json:"url"`
	ExpiresAt    time.Time `json:"expires_at"`
	ContentType  string    `json:"content_type,omitempty"`
	SizeBytes    int64     `json:"size_bytes,omitempty"`
}

type TargetExportAttemptRequest struct {
	ExportJobID  string `json:"-"`
	LeaseOwner   string `json:"lease_owner"`
	AttemptToken string `json:"attempt_token"`
}

type TargetExportCancelState struct {
	CancelRequested   bool       `json:"cancel_requested"`
	Status            string     `json:"status"`
	CancelRequestedAt *time.Time `json:"cancel_requested_at,omitempty"`
}

type TargetRecordExportProgressRequest struct {
	ExportJobID  string          `json:"-"`
	LeaseOwner   string          `json:"lease_owner"`
	AttemptToken string          `json:"attempt_token"`
	Progress     json.RawMessage `json:"progress"`
}

type TargetFinalizeExportJobRequest struct {
	ExportJobID       string                   `json:"-"`
	LeaseOwner        string                   `json:"lease_owner"`
	AttemptToken      string                   `json:"attempt_token"`
	Outcome           string                   `json:"outcome"`
	Output            *TargetExportPublication `json:"output,omitempty"`
	DiagnosticCode    string                   `json:"diagnostic_code,omitempty"`
	DiagnosticMessage string                   `json:"diagnostic_message,omitempty"`
}

type TargetExportPublication struct {
	ContentType     string `json:"content_type"`
	Filename        string `json:"filename"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	StagingKey      string `json:"staging_key"`
	DurationSeconds *int   `json:"duration_seconds,omitempty"`
}

type TargetExportReclaimRequest struct {
	BatchSize int `json:"batch_size"`
}

type TargetExportReclaimResult struct {
	Examined int64 `json:"examined"`
	Requeued int64 `json:"requeued"`
	Failed   int64 `json:"failed"`
}

type TargetRetentionSweepRequest struct {
	BatchSize     int    `json:"batch_size"`
	DeletionOwner string `json:"deletion_owner,omitempty"`
	ClaimSeconds  int    `json:"claim_seconds,omitempty"`
}

type TargetRetentionClaim struct {
	StoredObjectID string    `json:"stored_object_id"`
	Generation     int       `json:"generation"`
	DeletionOwner  string    `json:"deletion_owner"`
	DeletionToken  string    `json:"deletion_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type TargetRetentionSweepResult struct {
	Claimed int                    `json:"claimed"`
	Deleted int                    `json:"deleted"`
	Failed  int                    `json:"failed"`
	Claims  []TargetRetentionClaim `json:"claims"`
}

type TargetRetentionReconcileRequest struct {
	BatchSize int  `json:"batch_size"`
	DryRun    bool `json:"dry_run,omitempty"`
}

type TargetRetentionReconcileResult struct {
	Examined               int `json:"examined"`
	OrphansDeleted         int `json:"orphans_deleted"`
	PublicationsReconciled int `json:"publications_reconciled"`
	ObjectsMarkedMissing   int `json:"objects_marked_missing"`
}

func (s *Server) handleCreateExportJob(w http.ResponseWriter, r *http.Request) {
	var body TargetCreateExportJobRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_export_job", message: "export request must be valid JSON", details: err.Error()})
		return
	}
	body.MediaAssetID = r.PathValue("media_asset_id")
	body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	job, err := s.deps.Target.CreateExportJob(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"export_job": job})
}

func (s *Server) handleListExportJobs(w http.ResponseWriter, r *http.Request) {
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	page, err := s.deps.Target.ListExportJobs(r.Context(), TargetListExportJobsRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")),
		Status:           strings.TrimSpace(r.URL.Query().Get("status")), PageSize: pageSize,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleGetExportJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.deps.Target.GetExportJob(r.Context(), TargetGetExportJobRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")), ExportJobID: r.PathValue("export_job_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"export_job": job})
}

func (s *Server) handleCancelExportJob(w http.ResponseWriter, r *http.Request) {
	var body TargetExportJobMutationRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_export_job", message: "cancel request must be valid JSON"})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	job, err := s.deps.Target.CancelExportJob(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"export_job": job})
}

func (s *Server) handleRetryExportJob(w http.ResponseWriter, r *http.Request) {
	var body TargetExportJobMutationRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_export_job", message: "retry request must be valid JSON"})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	body.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	job, err := s.deps.Target.RetryExportJob(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"export_job": job})
}

func (s *Server) handleResolveInternalExportDownloadAccess(w http.ResponseWriter, r *http.Request) {
	download, err := s.deps.Target.ResolveInternalExportDownloadAccess(r.Context(), TargetGetExportJobRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")), ExportJobID: r.PathValue("export_job_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, download)
}

func (s *Server) handleResolveExportDownload(w http.ResponseWriter, r *http.Request) {
	download, err := s.deps.Target.ResolveExportDownload(r.Context(), TargetGetExportJobRequest{
		ChannelAccountID: strings.TrimSpace(r.URL.Query().Get("channel_account_id")), ExportJobID: r.PathValue("export_job_id"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, download)
}

func (s *Server) handleClaimExportDelivery(w http.ResponseWriter, r *http.Request) {
	var body TargetClaimExportDeliveryRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_delivery", message: "delivery claim must be valid JSON"})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	claim, err := s.deps.Target.ClaimExportDelivery(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) handleHeartbeatExportDelivery(w http.ResponseWriter, r *http.Request) {
	var body TargetHeartbeatExportDeliveryRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_delivery", message: "delivery heartbeat must be valid JSON"})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	claim, err := s.deps.Target.HeartbeatExportDelivery(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) handleAckExportDelivery(w http.ResponseWriter, r *http.Request) {
	s.handleFinalizeExportDelivery(w, r, "delivered")
}

func (s *Server) handleFailExportDelivery(w http.ResponseWriter, r *http.Request) {
	s.handleFinalizeExportDelivery(w, r, "failed")
}

func (s *Server) handleFinalizeExportDelivery(w http.ResponseWriter, r *http.Request, status string) {
	var body TargetFinalizeExportDeliveryRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_delivery", message: "delivery mutation must be valid JSON"})
		return
	}
	body.ExportJobID, body.Status = r.PathValue("export_job_id"), status
	delivery, err := s.deps.Target.FinalizeExportDelivery(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"delivery": delivery})
}

func (s *Server) handleListExportQueue(w http.ResponseWriter, r *http.Request) {
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	page, err := s.deps.Target.ListExportJobQueue(r.Context(), TargetExportQueueRequest{PageSize: pageSize})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleClaimExportJob(w http.ResponseWriter, r *http.Request) {
	var body TargetClaimExportJobRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_claim", message: "claim request must be valid JSON"})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	claim, err := s.deps.Target.ClaimExportJob(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) handleCheckExportCancel(w http.ResponseWriter, r *http.Request) {
	req := TargetExportAttemptRequest{ExportJobID: r.PathValue("export_job_id"), LeaseOwner: r.URL.Query().Get("lease_owner"), AttemptToken: r.URL.Query().Get("attempt_token")}
	state, err := s.deps.Target.CheckExportJobCancel(r.Context(), req)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleRecordExportProgress(w http.ResponseWriter, r *http.Request) {
	var body TargetRecordExportProgressRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_progress", message: "progress request must be valid JSON"})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	if err := s.deps.Target.RecordExportJobProgress(r.Context(), body); err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleFinalizeExportJob(w http.ResponseWriter, r *http.Request) {
	var body TargetFinalizeExportJobRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_finalize", message: "finalize request must be valid JSON", details: err.Error()})
		return
	}
	body.ExportJobID = r.PathValue("export_job_id")
	job, err := s.deps.Target.FinalizeExportJob(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"export_job": job})
}

func (s *Server) handleReclaimExportJobs(w http.ResponseWriter, r *http.Request) {
	var body TargetExportReclaimRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_export_reclaim", message: "reclaim request must be valid JSON"})
		return
	}
	result, err := s.deps.Target.ReclaimExportJobs(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleSweepRetention(w http.ResponseWriter, r *http.Request) {
	var body TargetRetentionSweepRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: 400, code: "invalid_retention_sweep", message: "retention sweep request must be valid JSON"})
		return
	}
	result, err := s.deps.Target.SweepRetention(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleReconcileRetention(w http.ResponseWriter, r *http.Request) {
	var body TargetRetentionReconcileRequest
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_retention_reconcile", message: "retention reconcile request must be valid JSON"})
		return
	}
	result, err := s.deps.Target.ReconcileRetention(r.Context(), body)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, result)
}
