package api

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/jobs"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/ws"
)

const (
	submissionKindTranscriptionUpload = "transcription_upload"
	submissionKindTranscriptionURL    = "transcription_url"
)

type runtimeJobsService interface {
	CreateTranscriptionJobs(ctx context.Context, req jobs.CreateTranscriptionRequest) (jobs.CreateJobsResult, error)
	CreateCombinedTranscriptionJob(ctx context.Context, req jobs.CreateCombinedTranscriptionRequest) (jobs.ChildResult, error)
	CreateChildJob(ctx context.Context, req jobs.CreateChildRequest) (jobs.ChildResult, error)
	CancelJob(ctx context.Context, jobID string) (storage.JobRecord, error)
	RetryJob(ctx context.Context, jobID string) (jobs.RetryResult, error)
	TransitionJob(ctx context.Context, req jobs.TransitionRequest) (storage.JobRecord, error)
}

type runtimeStorageService interface {
	GetJob(ctx context.Context, jobID string) (storage.JobRecord, error)
	ListJobs(ctx context.Context, filter storage.JobListFilter) (storage.JobListPage, error)
	GetSourceSet(ctx context.Context, sourceSetID string) (storage.SourceSetRecord, error)
	ListOrderedSources(ctx context.Context, sourceSetID string) ([]storage.OrderedSource, error)
	ListArtifactsByJob(ctx context.Context, jobID string) ([]storage.ArtifactRecord, error)
	ListChildJobs(ctx context.Context, parentJobID string) ([]storage.JobRecord, error)
	ListJobEvents(ctx context.Context, jobID string) ([]storage.JobEvent, error)
	ResolveArtifact(ctx context.Context, artifactID string) (storage.ArtifactResolution, error)
	ClaimJobExecution(ctx context.Context, req storage.ClaimJobExecutionRequest) (storage.ClaimJobExecutionResult, error)
	GetActiveJobExecution(ctx context.Context, jobID, executionID string) (storage.JobExecutionRecord, error)
	FinishJobExecution(ctx context.Context, req storage.FinishJobExecutionRequest) (storage.JobExecutionRecord, error)
	UpsertArtifacts(ctx context.Context, artifacts []storage.ArtifactRecord) error
	UpdateJob(ctx context.Context, job storage.JobRecord) error
	FindArtifactByKind(ctx context.Context, jobID, artifactKind string) (*storage.ArtifactRecord, error)
}

type runtimeEventsService interface {
	EmitJobEvent(ctx context.Context, req ws.EmitRequest) (ws.EmitResult, error)
}

type publicRuntimeService struct {
	jobs   runtimeJobsService
	store  runtimeStorageService
	events runtimeEventsService
}

type workerRuntimeService struct {
	jobs   runtimeJobsService
	store  runtimeStorageService
	events runtimeEventsService
	now    func() time.Time
	nextID func() string
}

type jobsEventBridge struct {
	store  runtimeStorageService
	events runtimeEventsService
}

func newPublicRuntimeService(jobsService runtimeJobsService, storageService runtimeStorageService, eventsService runtimeEventsService) *publicRuntimeService {
	return &publicRuntimeService{
		jobs:   jobsService,
		store:  storageService,
		events: eventsService,
	}
}

func newWorkerRuntimeService(jobsService runtimeJobsService, storageService runtimeStorageService, eventsService runtimeEventsService) *workerRuntimeService {
	return &workerRuntimeService{
		jobs:   jobsService,
		store:  storageService,
		events: eventsService,
		now:    func() time.Time { return time.Now().UTC() },
		nextID: uuid.NewString,
	}
}

func newJobsEventBridge(storageService runtimeStorageService, eventsService runtimeEventsService) *jobsEventBridge {
	if eventsService == nil {
		return nil
	}
	return &jobsEventBridge{
		store:  storageService,
		events: eventsService,
	}
}

func NewRuntimeDependencies(storageService *storage.Repository, publisher *queue.Publisher, eventsService *ws.Service, websocket WebsocketAcceptor) (Dependencies, error) {
	jobsService, err := jobs.NewService(
		storageService,
		publisher,
		jobs.WithEventEmitter(newJobsEventBridge(storageService, eventsService)),
	)
	if err != nil {
		return Dependencies{}, err
	}
	return Dependencies{
		Public:    newPublicRuntimeService(jobsService, storageService, eventsService),
		Worker:    newWorkerRuntimeService(jobsService, storageService, eventsService),
		Websocket: websocket,
	}, nil
}

func (s *publicRuntimeService) CreateUpload(ctx context.Context, req UploadCommand) ([]JobSnapshot, error) {
	sources := buildUploadedSources(req)
	result, err := s.jobs.CreateTranscriptionJobs(ctx, jobs.CreateTranscriptionRequest{
		SubmissionKind:     submissionKindTranscriptionUpload,
		IdempotencyKey:     req.IdempotencyKey,
		RequestFingerprint: fingerprintUploadCommand("upload", req),
		Sources:            sources,
		Delivery:           deliveryConfigToStorage(req.Delivery),
		ClientRef:          strings.TrimSpace(req.ClientRef),
	})
	if err != nil {
		return nil, err
	}

	snapshots := make([]JobSnapshot, 0, len(result.Jobs))
	for _, job := range result.Jobs {
		if !result.Reused {
			if err := s.emitJobEvent(ctx, job, "job.created"); err != nil {
				return nil, err
			}
		}
		snapshot, err := s.snapshotByID(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func (s *publicRuntimeService) CreateCombined(ctx context.Context, req UploadCommand) (JobSnapshot, error) {
	sources := buildUploadedSources(req)
	result, err := s.jobs.CreateCombinedTranscriptionJob(ctx, jobs.CreateCombinedTranscriptionRequest{
		SubmissionKind:     submissionKindTranscriptionUpload,
		IdempotencyKey:     req.IdempotencyKey,
		RequestFingerprint: fingerprintUploadCommand("combined_upload", req),
		Sources:            sources,
		Delivery:           deliveryConfigToStorage(req.Delivery),
		ClientRef:          strings.TrimSpace(req.ClientRef),
		DisplayName:        strings.TrimSpace(req.DisplayName),
	})
	if err != nil {
		return JobSnapshot{}, err
	}
	if !result.Reused {
		if err := s.emitJobEvent(ctx, result.Job, "job.created"); err != nil {
			return JobSnapshot{}, err
		}
	}
	return s.snapshotByID(ctx, result.Job.ID)
}

func (s *publicRuntimeService) CreateFromURL(ctx context.Context, req URLCommand) (JobSnapshot, error) {
	displayName := strings.TrimSpace(req.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(req.URL)
	}
	sourceID := uuid.NewString()
	result, err := s.jobs.CreateTranscriptionJobs(ctx, jobs.CreateTranscriptionRequest{
		SubmissionKind:     submissionKindTranscriptionURL,
		IdempotencyKey:     req.IdempotencyKey,
		RequestFingerprint: fingerprintURLCommand(req),
		Sources: []storage.SourceRecord{
			{
				ID:          sourceID,
				SourceKind:  storage.SourceKindYouTubeURL,
				DisplayName: displayName,
				SourceURL:   strings.TrimSpace(req.URL),
			},
		},
		Delivery:  deliveryConfigToStorage(req.Delivery),
		ClientRef: strings.TrimSpace(req.ClientRef),
	})
	if err != nil {
		return JobSnapshot{}, err
	}
	if len(result.Jobs) != 1 {
		return JobSnapshot{}, fmt.Errorf("expected one job for URL submission, got %d", len(result.Jobs))
	}
	if !result.Reused {
		if err := s.emitJobEvent(ctx, result.Jobs[0], "job.created"); err != nil {
			return JobSnapshot{}, err
		}
	}
	return s.snapshotByID(ctx, result.Jobs[0].ID)
}

func (s *publicRuntimeService) GetJob(ctx context.Context, jobID string) (JobSnapshot, error) {
	return s.snapshotByID(ctx, jobID)
}

func (s *publicRuntimeService) ListJobs(ctx context.Context, filter ListJobsFilter) (JobListResponse, error) {
	page, err := s.store.ListJobs(ctx, storage.JobListFilter{
		Status:    strings.TrimSpace(filter.Status),
		JobType:   strings.TrimSpace(filter.JobType),
		RootJobID: strings.TrimSpace(filter.RootJobID),
		Limit:     filter.PageSize,
		Offset:    (filter.Page - 1) * filter.PageSize,
	})
	if err != nil {
		return JobListResponse{}, err
	}

	response := JobListResponse{
		Items:    make([]JobSnapshot, 0, len(page.Jobs)),
		Page:     filter.Page,
		PageSize: filter.PageSize,
	}
	for _, job := range page.Jobs {
		snapshot, err := s.snapshotFromJob(ctx, job)
		if err != nil {
			return JobListResponse{}, err
		}
		response.Items = append(response.Items, snapshot)
	}
	if page.HasMore {
		nextPage := filter.Page + 1
		response.NextPage = &nextPage
	}
	return response, nil
}

func (s *publicRuntimeService) CreateReport(ctx context.Context, jobID string, _ ChildCreateRequest) (JobSnapshot, error) {
	result, err := s.jobs.CreateChildJob(ctx, jobs.CreateChildRequest{
		ParentJobID:  jobID,
		ChildJobType: queue.JobTypeReport,
	})
	if err != nil {
		return JobSnapshot{}, err
	}
	if !result.Reused {
		if err := s.emitJobEvent(ctx, result.Job, "job.created"); err != nil {
			return JobSnapshot{}, err
		}
	}
	return s.snapshotByID(ctx, result.Job.ID)
}

func (s *publicRuntimeService) CreateDeepResearch(ctx context.Context, jobID string, _ ChildCreateRequest) (JobSnapshot, error) {
	result, err := s.jobs.CreateChildJob(ctx, jobs.CreateChildRequest{
		ParentJobID:  jobID,
		ChildJobType: queue.JobTypeDeepResearch,
	})
	if err != nil {
		return JobSnapshot{}, err
	}
	if !result.Reused {
		if err := s.emitJobEvent(ctx, result.Job, "job.created"); err != nil {
			return JobSnapshot{}, err
		}
	}
	return s.snapshotByID(ctx, result.Job.ID)
}

func (s *publicRuntimeService) CancelJob(ctx context.Context, jobID string) (JobSnapshot, error) {
	job, err := s.jobs.CancelJob(ctx, jobID)
	if err != nil {
		return JobSnapshot{}, err
	}
	return s.snapshotFromJob(ctx, job)
}

func (s *publicRuntimeService) RetryJob(ctx context.Context, jobID string) (JobSnapshot, error) {
	result, err := s.jobs.RetryJob(ctx, jobID)
	if err != nil {
		return JobSnapshot{}, err
	}
	if err := s.emitJobEvent(ctx, result.Job, "job.created"); err != nil {
		return JobSnapshot{}, err
	}
	return s.snapshotByID(ctx, result.Job.ID)
}

func (s *publicRuntimeService) ResolveArtifact(ctx context.Context, artifactID string) (storage.ArtifactResolution, error) {
	return s.store.ResolveArtifact(ctx, artifactID)
}

func (s *publicRuntimeService) ListJobEvents(ctx context.Context, jobID string) ([]ws.JobEventEnvelope, error) {
	events, err := s.store.ListJobEvents(ctx, jobID)
	if err != nil {
		return nil, err
	}
	envelopes := make([]ws.JobEventEnvelope, 0, len(events))
	for _, event := range events {
		envelope, err := decodeStoredEnvelope(event)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func (s *publicRuntimeService) snapshotByID(ctx context.Context, jobID string) (JobSnapshot, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return JobSnapshot{}, err
	}
	return s.snapshotFromJob(ctx, job)
}

func (s *publicRuntimeService) snapshotFromJob(ctx context.Context, job storage.JobRecord) (JobSnapshot, error) {
	sourceSet, err := s.store.GetSourceSet(ctx, job.SourceSetID)
	if err != nil {
		return JobSnapshot{}, err
	}
	orderedSources, err := s.store.ListOrderedSources(ctx, job.SourceSetID)
	if err != nil {
		return JobSnapshot{}, err
	}
	artifacts, err := s.store.ListArtifactsByJob(ctx, job.ID)
	if err != nil {
		return JobSnapshot{}, err
	}
	children, err := s.store.ListChildJobs(ctx, job.ID)
	if err != nil {
		return JobSnapshot{}, err
	}

	return jobSnapshotFromRecords(job, sourceSet, orderedSources, artifacts, children), nil
}

func (s *publicRuntimeService) emitJobEvent(ctx context.Context, job storage.JobRecord, eventType string) error {
	if s.events == nil {
		return nil
	}
	_, err := s.events.EmitJobEvent(ctx, ws.EmitRequest{
		Job:       job,
		EventType: eventType,
		JobURL:    jobURL(job.ID),
		Payload: ws.EventPayload{
			Status:          job.Status,
			ProgressStage:   job.ProgressStage,
			ProgressMessage: job.ProgressMessage,
		},
	})
	return err
}

func (s *workerRuntimeService) Claim(ctx context.Context, jobID string, req ClaimRequest) (ClaimResponse, error) {
	result, err := s.store.ClaimJobExecution(ctx, storage.ClaimJobExecutionRequest{
		JobID:      jobID,
		WorkerKind: strings.TrimSpace(req.WorkerKind),
		TaskType:   strings.TrimSpace(req.TaskType),
	})
	if err != nil {
		return ClaimResponse{}, err
	}
	if !result.Claimed {
		switch result.Job.Status {
		case "succeeded", "failed", "canceled":
			return ClaimResponse{}, apiError{status: 409, code: "job_terminal", message: "job is already terminal"}
		default:
			return ClaimResponse{}, apiError{status: 409, code: "job_already_owned", message: "job already has an active execution"}
		}
	}
	if err := s.emitEvent(ctx, result.Job, "job.updated"); err != nil {
		return ClaimResponse{}, err
	}
	return s.claimResponseFromJob(ctx, result.Job, *result.Execution)
}

func (s *workerRuntimeService) RecordProgress(ctx context.Context, jobID string, req ProgressRequest) error {
	if _, err := s.store.GetActiveJobExecution(ctx, jobID, req.ExecutionID); err != nil {
		return mapExecutionLookupError(err)
	}
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job.Status != "running" && job.Status != "cancel_requested" {
		return jobs.ErrInvalidJobState
	}

	job.Version++
	job.ProgressStage = strings.TrimSpace(req.ProgressStage)
	job.ProgressMessage = optionalStringValue(req.ProgressMessage)
	if err := s.store.UpdateJob(ctx, job); err != nil {
		return err
	}
	return s.emitEvent(ctx, job, "job.updated")
}

func (s *workerRuntimeService) RecordArtifacts(ctx context.Context, jobID string, req ArtifactUpsertRequest) error {
	if _, err := s.store.GetActiveJobExecution(ctx, jobID, req.ExecutionID); err != nil {
		return mapExecutionLookupError(err)
	}
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job.Status != "running" && job.Status != "cancel_requested" {
		return jobs.ErrInvalidJobState
	}

	records := make([]storage.ArtifactRecord, 0, len(req.Artifacts))
	for _, artifact := range req.Artifacts {
		records = append(records, storage.ArtifactRecord{
			ID:           s.nextID(),
			JobID:        jobID,
			ArtifactKind: strings.TrimSpace(artifact.ArtifactKind),
			Filename:     filepath.Base(strings.TrimSpace(artifact.Filename)),
			Format:       optionalStringValue(artifact.Format),
			MIMEType:     strings.TrimSpace(artifact.MIMEType),
			ObjectKey:    strings.TrimSpace(artifact.ObjectKey),
			SizeBytes:    artifact.SizeBytes,
			CreatedAt:    s.now(),
		})
	}
	if err := s.store.UpsertArtifacts(ctx, records); err != nil {
		return err
	}

	job.Version++
	if err := s.store.UpdateJob(ctx, job); err != nil {
		return err
	}
	return s.emitEvent(ctx, job, "job.artifact_created")
}

func (s *workerRuntimeService) Finalize(ctx context.Context, jobID string, req FinalizeRequest) (JobSnapshot, error) {
	if _, err := s.store.GetActiveJobExecution(ctx, jobID, req.ExecutionID); err != nil {
		return JobSnapshot{}, mapExecutionLookupError(err)
	}
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return JobSnapshot{}, err
	}

	if strings.TrimSpace(req.Outcome) == "succeeded" {
		if err := s.ensureRequiredArtifacts(ctx, job); err != nil {
			return JobSnapshot{}, err
		}
	}

	transitionedJob, err := s.jobs.TransitionJob(ctx, jobs.TransitionRequest{
		JobID:           jobID,
		ToStatus:        statusFromOutcome(req.Outcome),
		ProgressStage:   terminalProgressStage(req),
		ProgressMessage: optionalStringValue(req.ProgressMessage),
		ErrorCode:       optionalStringValue(req.ErrorCode),
		ErrorMessage:    optionalStringValue(req.ErrorMessage),
	})
	if err != nil {
		return JobSnapshot{}, err
	}
	if _, err := s.store.FinishJobExecution(ctx, storage.FinishJobExecutionRequest{
		JobID:       jobID,
		ExecutionID: req.ExecutionID,
		Outcome:     strings.TrimSpace(req.Outcome),
	}); err != nil {
		return JobSnapshot{}, mapExecutionLookupError(err)
	}
	return newPublicRuntimeService(s.jobs, s.store, s.events).snapshotFromJob(ctx, transitionedJob)
}

func (s *workerRuntimeService) CancelCheck(ctx context.Context, jobID, executionID string) (CancelCheckResponse, error) {
	if _, err := s.store.GetActiveJobExecution(ctx, jobID, executionID); err != nil {
		return CancelCheckResponse{}, mapExecutionLookupError(err)
	}
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return CancelCheckResponse{}, err
	}
	return CancelCheckResponse{
		CancelRequested:   job.Status == "cancel_requested",
		Status:            job.Status,
		CancelRequestedAt: job.CancelRequestedAt,
	}, nil
}

func (s *workerRuntimeService) claimResponseFromJob(ctx context.Context, job storage.JobRecord, execution storage.JobExecutionRecord) (ClaimResponse, error) {
	params := map[string]any{}
	if len(job.ParamsJSON) > 0 {
		if err := json.Unmarshal(job.ParamsJSON, &params); err != nil {
			return ClaimResponse{}, fmt.Errorf("decode job params: %w", err)
		}
	}
	inputs, err := s.claimInputsForJob(ctx, job)
	if err != nil {
		return ClaimResponse{}, err
	}
	return ClaimResponse{
		ExecutionID:   execution.ExecutionID,
		JobID:         job.ID,
		RootJobID:     job.RootJobID,
		ParentJobID:   stringPtr(job.ParentJobID),
		RetryOfJobID:  stringPtr(job.RetryOfJobID),
		JobType:       job.JobType,
		Version:       job.Version,
		OrderedInputs: inputs,
		Params:        params,
	}, nil
}

func (s *workerRuntimeService) claimInputsForJob(ctx context.Context, job storage.JobRecord) ([]OrderedInput, error) {
	switch job.JobType {
	case queue.JobTypeTranscription:
		orderedSources, err := s.store.ListOrderedSources(ctx, job.SourceSetID)
		if err != nil {
			return nil, err
		}
		inputs := make([]OrderedInput, 0, len(orderedSources))
		for _, ordered := range orderedSources {
			inputs = append(inputs, orderedInputFromSource(ordered))
		}
		return inputs, nil
	case queue.JobTypeReport:
		if strings.TrimSpace(job.ParentJobID) == "" {
			return nil, fmt.Errorf("report claim requires parent transcription job")
		}
		artifact, err := s.findFirstArtifactByKind(ctx, job.ParentJobID, "transcript_segmented_markdown", "transcript_plain")
		if err != nil {
			return nil, err
		}
		return []OrderedInput{orderedInputFromArtifact(0, "Transcript", artifact)}, nil
	case queue.JobTypeDeepResearch:
		if strings.TrimSpace(job.ParentJobID) == "" {
			return nil, fmt.Errorf("deep research claim requires parent report job")
		}
		reportJob, err := s.store.GetJob(ctx, job.ParentJobID)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(reportJob.ParentJobID) == "" {
			return nil, fmt.Errorf("deep research claim requires report parent to reference transcription job")
		}
		transcriptArtifact, err := s.findFirstArtifactByKind(ctx, reportJob.ParentJobID, "transcript_segmented_markdown", "transcript_plain")
		if err != nil {
			return nil, err
		}
		reportArtifact, err := s.findFirstArtifactByKind(ctx, reportJob.ID, "report_markdown")
		if err != nil {
			return nil, err
		}
		return []OrderedInput{
			orderedInputFromArtifact(0, "Transcript", transcriptArtifact),
			orderedInputFromArtifact(1, "Report", reportArtifact),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported claim job type %q", job.JobType)
	}
}

func (s *workerRuntimeService) findFirstArtifactByKind(ctx context.Context, jobID string, artifactKinds ...string) (storage.ArtifactRecord, error) {
	for _, artifactKind := range artifactKinds {
		artifact, err := s.store.FindArtifactByKind(ctx, jobID, artifactKind)
		if err != nil {
			return storage.ArtifactRecord{}, err
		}
		if artifact != nil {
			return *artifact, nil
		}
	}
	return storage.ArtifactRecord{}, fmt.Errorf("%w: required claim input artifact missing", jobs.ErrMissingArtifact)
}

func (s *workerRuntimeService) ensureRequiredArtifacts(ctx context.Context, job storage.JobRecord) error {
	for _, artifactKind := range requiredArtifactKinds(job.JobType) {
		artifact, err := s.store.FindArtifactByKind(ctx, job.ID, artifactKind)
		if err != nil {
			return err
		}
		if artifact == nil {
			return fmt.Errorf("%w: missing %s for %s finalize", jobs.ErrMissingArtifact, artifactKind, job.JobType)
		}
	}
	return nil
}

func (s *workerRuntimeService) emitEvent(ctx context.Context, job storage.JobRecord, eventType string) error {
	if s.events == nil {
		return nil
	}
	_, err := s.events.EmitJobEvent(ctx, ws.EmitRequest{
		Job:       job,
		EventType: eventType,
		JobURL:    jobURL(job.ID),
		Payload: ws.EventPayload{
			Status:          job.Status,
			ProgressStage:   job.ProgressStage,
			ProgressMessage: job.ProgressMessage,
		},
	})
	return err
}

func (b *jobsEventBridge) Emit(ctx context.Context, event jobs.Event) error {
	if b == nil || b.events == nil {
		return nil
	}
	job, err := b.store.GetJob(ctx, event.JobID)
	if err != nil {
		return err
	}
	status := strings.TrimSpace(event.ToStatus)
	if status == "" {
		status = job.Status
	}
	_, err = b.events.EmitJobEvent(ctx, ws.EmitRequest{
		Job:       job,
		EventType: event.EventType,
		JobURL:    jobURL(job.ID),
		Payload: ws.EventPayload{
			Status:          status,
			ProgressStage:   job.ProgressStage,
			ProgressMessage: job.ProgressMessage,
		},
	})
	return err
}

func buildUploadedSources(req UploadCommand) []storage.SourceRecord {
	sources := make([]storage.SourceRecord, 0, len(req.Files))
	singleDisplayName := strings.TrimSpace(req.DisplayName)
	for idx, file := range req.Files {
		sourceID := uuid.NewString()
		displayName := strings.TrimSpace(file.Filename)
		if len(req.Files) == 1 && singleDisplayName != "" {
			displayName = singleDisplayName
		}
		if displayName == "" {
			displayName = fmt.Sprintf("upload-%d", idx+1)
		}
		sources = append(sources, storage.SourceRecord{
			ID:               sourceID,
			SourceKind:       storage.SourceKindUploadedFile,
			DisplayName:      displayName,
			OriginalFilename: filepath.Base(strings.TrimSpace(file.Filename)),
			MIMEType:         strings.TrimSpace(file.ContentType),
			ObjectKey:        sourceObjectKey(sourceID, file.Filename),
			SizeBytes:        file.SizeBytes,
			ObjectBody:       file.Body,
		})
	}
	return sources
}

func deliveryConfigToStorage(cfg DeliveryConfig) storage.Delivery {
	return storage.Delivery{
		Strategy:   strings.TrimSpace(cfg.Strategy),
		WebhookURL: strings.TrimSpace(cfg.WebhookURL),
	}
}

func fingerprintUploadCommand(kind string, req UploadCommand) string {
	type fileFingerprint struct {
		Filename    string `json:"filename"`
		ContentType string `json:"content_type"`
		SizeBytes   int64  `json:"size_bytes"`
		SHA256      string `json:"sha256"`
	}
	payload := struct {
		Kind        string            `json:"kind"`
		DisplayName string            `json:"display_name,omitempty"`
		ClientRef   string            `json:"client_ref,omitempty"`
		Delivery    DeliveryConfig    `json:"delivery"`
		Files       []fileFingerprint `json:"files"`
	}{
		Kind:        kind,
		DisplayName: strings.TrimSpace(req.DisplayName),
		ClientRef:   strings.TrimSpace(req.ClientRef),
		Delivery:    req.Delivery,
		Files:       make([]fileFingerprint, 0, len(req.Files)),
	}
	for _, file := range req.Files {
		payload.Files = append(payload.Files, fileFingerprint{
			Filename:    filepath.Base(strings.TrimSpace(file.Filename)),
			ContentType: strings.TrimSpace(file.ContentType),
			SizeBytes:   file.SizeBytes,
			SHA256:      strings.TrimSpace(file.SHA256),
		})
	}
	encoded, _ := json.Marshal(payload)
	return checksum(encoded)
}

func fingerprintURLCommand(req URLCommand) string {
	payload := struct {
		Kind        string         `json:"kind"`
		SourceKind  string         `json:"source_kind"`
		URL         string         `json:"url"`
		DisplayName string         `json:"display_name,omitempty"`
		ClientRef   string         `json:"client_ref,omitempty"`
		Delivery    DeliveryConfig `json:"delivery"`
	}{
		Kind:        submissionKindTranscriptionURL,
		SourceKind:  strings.TrimSpace(req.SourceKind),
		URL:         strings.TrimSpace(req.URL),
		DisplayName: strings.TrimSpace(req.DisplayName),
		ClientRef:   strings.TrimSpace(req.ClientRef),
		Delivery:    req.Delivery,
	}
	encoded, _ := json.Marshal(payload)
	return checksum(encoded)
}

func sourceObjectKey(sourceID, filename string) string {
	name := sanitizeFilename(filename)
	if name == "" {
		name = "source.bin"
	}
	return fmt.Sprintf("sources/%s/original/%s", sourceID, name)
}

func sanitizeFilename(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	if base == "." || base == "/" || base == "" {
		return ""
	}
	var builder strings.Builder
	for _, r := range base {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '.', r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}
	return strings.Trim(builder.String(), "_")
}

func jobSnapshotFromRecords(job storage.JobRecord, sourceSet storage.SourceSetRecord, orderedSources []storage.OrderedSource, artifacts []storage.ArtifactRecord, children []storage.JobRecord) JobSnapshot {
	snapshot := JobSnapshot{
		JobID:             job.ID,
		RootJobID:         job.RootJobID,
		ParentJobID:       stringPtr(job.ParentJobID),
		RetryOfJobID:      stringPtr(job.RetryOfJobID),
		JobType:           job.JobType,
		Status:            job.Status,
		Version:           job.Version,
		DisplayName:       inferredDisplayName(sourceSet, orderedSources),
		ClientRef:         stringPtr(job.ClientRef),
		Delivery:          deliveryConfigFromStorage(job.Delivery),
		SourceSet:         sourceSetViewFromRecords(sourceSet, orderedSources),
		Artifacts:         artifactSummariesFromRecords(artifacts),
		Children:          childReferencesFromRecords(children),
		Progress:          progressStateFromJob(job),
		LatestError:       errorInfoFromJob(job),
		CreatedAt:         job.CreatedAt,
		StartedAt:         job.StartedAt,
		FinishedAt:        job.FinishedAt,
		CancelRequestedAt: job.CancelRequestedAt,
	}
	return snapshot
}

func sourceSetViewFromRecords(sourceSet storage.SourceSetRecord, orderedSources []storage.OrderedSource) SourceSetView {
	view := SourceSetView{
		SourceSetID: sourceSet.ID,
		InputKind:   sourceSet.InputKind,
		Items:       make([]SourceSetItem, 0, len(orderedSources)),
	}
	for _, ordered := range orderedSources {
		view.Items = append(view.Items, SourceSetItem{
			Position: ordered.Position,
			Source: SourceReference{
				SourceID:         ordered.Source.ID,
				SourceKind:       ordered.Source.SourceKind,
				DisplayName:      stringPtr(ordered.Source.DisplayName),
				OriginalFilename: stringPtr(ordered.Source.OriginalFilename),
				SourceURL:        stringPtr(ordered.Source.SourceURL),
			},
		})
	}
	return view
}

func orderedInputFromSource(source storage.OrderedSource) OrderedInput {
	return OrderedInput{
		Position:         source.Position,
		SourceID:         source.Source.ID,
		SourceKind:       source.Source.SourceKind,
		DisplayName:      stringPtr(source.Source.DisplayName),
		OriginalFilename: stringPtr(source.Source.OriginalFilename),
		ObjectKey:        stringPtr(source.Source.ObjectKey),
		SourceURL:        stringPtr(source.Source.SourceURL),
		SizeBytes:        int64Ptr(source.Source.SizeBytes),
	}
}

func orderedInputFromArtifact(position int, label string, artifact storage.ArtifactRecord) OrderedInput {
	filename := filepath.Base(strings.TrimSpace(artifact.Filename))
	displayName := strings.TrimSpace(label)
	if filename != "" {
		displayName = displayName + ": " + filename
	}
	return OrderedInput{
		Position:         position,
		SourceID:         artifact.ID,
		SourceKind:       storage.SourceKindUploadedFile,
		DisplayName:      stringPtr(displayName),
		OriginalFilename: stringPtr(filename),
		ObjectKey:        stringPtr(artifact.ObjectKey),
		SizeBytes:        int64Ptr(artifact.SizeBytes),
	}
}

func artifactSummariesFromRecords(records []storage.ArtifactRecord) []ArtifactSummary {
	summaries := make([]ArtifactSummary, 0, len(records))
	for _, artifact := range records {
		summaries = append(summaries, ArtifactSummary{
			ArtifactID:   artifact.ID,
			ArtifactKind: artifact.ArtifactKind,
			Filename:     artifact.Filename,
			MIMEType:     artifact.MIMEType,
			SizeBytes:    artifact.SizeBytes,
			CreatedAt:    artifact.CreatedAt,
		})
	}
	return summaries
}

func childReferencesFromRecords(children []storage.JobRecord) []ChildJobReference {
	refs := make([]ChildJobReference, 0, len(children))
	for _, child := range children {
		refs = append(refs, ChildJobReference{
			JobID:     child.ID,
			JobType:   child.JobType,
			Status:    child.Status,
			Version:   child.Version,
			JobURL:    jobURL(child.ID),
			RootJobID: child.RootJobID,
		})
	}
	return refs
}

func progressStateFromJob(job storage.JobRecord) *ProgressState {
	if strings.TrimSpace(job.ProgressStage) == "" && strings.TrimSpace(job.ProgressMessage) == "" {
		return nil
	}
	return &ProgressState{
		Stage:   job.ProgressStage,
		Message: stringPtr(job.ProgressMessage),
	}
}

func errorInfoFromJob(job storage.JobRecord) *ErrorInfo {
	if strings.TrimSpace(job.ErrorCode) == "" && strings.TrimSpace(job.ErrorMessage) == "" {
		return nil
	}
	return &ErrorInfo{
		Code:    job.ErrorCode,
		Message: stringPtr(job.ErrorMessage),
	}
}

func deliveryConfigFromStorage(delivery storage.Delivery) DeliveryConfig {
	return DeliveryConfig{
		Strategy:   delivery.Strategy,
		WebhookURL: delivery.WebhookURL,
	}
}

func inferredDisplayName(sourceSet storage.SourceSetRecord, orderedSources []storage.OrderedSource) *string {
	if value := strings.TrimSpace(sourceSet.DisplayName); value != "" {
		return &value
	}
	if len(orderedSources) == 1 {
		if value := strings.TrimSpace(orderedSources[0].Source.DisplayName); value != "" {
			return &value
		}
	}
	return nil
}

func decodeStoredEnvelope(event storage.JobEvent) (ws.JobEventEnvelope, error) {
	var envelope ws.JobEventEnvelope
	if err := json.Unmarshal(event.Payload, &envelope); err == nil && strings.TrimSpace(envelope.EventID) != "" {
		return envelope, nil
	}

	var payload struct {
		Status          string `json:"status"`
		ProgressStage   string `json:"progress_stage"`
		ProgressMessage string `json:"progress_message"`
	}
	if len(event.Payload) != 0 {
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return ws.JobEventEnvelope{}, fmt.Errorf("decode job event payload: %w", err)
		}
	}
	return ws.JobEventEnvelope{
		EventID:   event.ID,
		EventType: event.EventType,
		JobID:     event.JobID,
		RootJobID: event.RootJobID,
		Version:   event.Version,
		EmittedAt: event.CreatedAt,
		Payload: ws.EventPayload{
			Status:          payload.Status,
			ProgressStage:   payload.ProgressStage,
			ProgressMessage: payload.ProgressMessage,
		},
	}, nil
}

func requiredArtifactKinds(jobType string) []string {
	switch jobType {
	case queue.JobTypeTranscription:
		return []string{"transcript_plain", "transcript_segmented_markdown", "transcript_docx"}
	case queue.JobTypeReport:
		return []string{"report_markdown", "report_docx"}
	case queue.JobTypeDeepResearch:
		return []string{"deep_research_markdown"}
	default:
		return nil
	}
}

func statusFromOutcome(outcome string) string {
	switch strings.TrimSpace(outcome) {
	case "succeeded":
		return "succeeded"
	case "failed":
		return "failed"
	case "canceled":
		return "canceled"
	default:
		return strings.TrimSpace(outcome)
	}
}

func terminalProgressStage(req FinalizeRequest) string {
	if req.ProgressStage != nil && strings.TrimSpace(*req.ProgressStage) != "" {
		return strings.TrimSpace(*req.ProgressStage)
	}
	switch strings.TrimSpace(req.Outcome) {
	case "succeeded":
		return "completed"
	case "failed":
		return "failed"
	case "canceled":
		return "canceled"
	default:
		return ""
	}
}

func mapExecutionLookupError(err error) error {
	if err == nil {
		return nil
	}
	if err == storage.ErrExecutionNotFound {
		return apiError{status: 409, code: "execution_not_found", message: "execution_id is not active for this job"}
	}
	return err
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func stringPtr(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func int64Ptr(value int64) *int64 {
	if value == 0 {
		return nil
	}
	copied := value
	return &copied
}

func jobURL(jobID string) string {
	return fmt.Sprintf("/v1/jobs/%s", jobID)
}
