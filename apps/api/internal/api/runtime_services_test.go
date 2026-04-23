package api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/jobs"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/ws"
)

func TestApiHttpRuntimeCreateUploadEmitsCreatedEventAndReturnsSnapshot(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", "transcription", "queued", 1)
	sourceSet := runtimeSourceSet(job.SourceSetID, storage.SourceSetInputSingleSource)
	orderedSources := []storage.OrderedSource{
		{
			Position: 0,
			Source: storage.SourceRecord{
				ID:               "22222222-2222-2222-2222-222222222222",
				SourceKind:       storage.SourceKindUploadedFile,
				DisplayName:      "Call",
				OriginalFilename: "call.ogg",
				ObjectKey:        "sources/22222222-2222-2222-2222-222222222222/original/call.ogg",
				CreatedAt:        time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			},
		},
	}

	store := &runtimeStorageDouble{
		jobs:               map[string]storage.JobRecord{job.ID: job},
		sourceSets:         map[string]storage.SourceSetRecord{sourceSet.ID: sourceSet},
		orderedSourcesByID: map[string][]storage.OrderedSource{sourceSet.ID: orderedSources},
		artifactsByJob:     map[string][]storage.ArtifactRecord{job.ID: nil},
		childrenByParent:   map[string][]storage.JobRecord{job.ID: nil},
	}
	jobsService := &runtimeJobsDouble{
		createJobsResult: jobs.CreateJobsResult{Jobs: []storage.JobRecord{job}},
	}
	eventsService := &runtimeEventsDouble{}

	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshots, err := service.CreateUpload(context.Background(), UploadCommand{
		IdempotencyKey: "idem-1",
		DisplayName:    "Call",
		ClientRef:      "client-1",
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		Files: []UploadFile{
			{
				Filename:    "call.ogg",
				ContentType: "audio/ogg",
				SizeBytes:   12,
				SHA256:      "checksum",
				Body:        []byte("hello world!"),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateUpload() error = %v", err)
	}
	if got, want := len(snapshots), 1; got != want {
		t.Fatalf("snapshot count = %d, want %d", got, want)
	}
	if snapshots[0].JobID != job.ID {
		t.Fatalf("job_id = %q, want %q", snapshots[0].JobID, job.ID)
	}
	if jobsService.createUpload.SubmissionKind != submissionKindTranscriptionUpload {
		t.Fatalf("submission kind = %q, want %q", jobsService.createUpload.SubmissionKind, submissionKindTranscriptionUpload)
	}
	if got := jobsService.createUpload.Sources[0].ObjectKey; got == "" {
		t.Fatal("expected upload source object key to be materialized")
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
	if eventsService.requests[0].EventType != "job.created" {
		t.Fatalf("event type = %q, want job.created", eventsService.requests[0].EventType)
	}
}

func TestApiHttpRuntimeWorkerLifecycleUsesAuthoritativeExecutionState(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "running", 2)
	execution := storage.JobExecutionRecord{
		ExecutionID: "33333333-3333-3333-3333-333333333333",
		JobID:       job.ID,
		WorkerKind:  "transcription",
		TaskType:    "transcription.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	sourceSet := runtimeSourceSet(job.SourceSetID, storage.SourceSetInputSingleSource)
	orderedSources := []storage.OrderedSource{
		{
			Position: 0,
			Source: storage.SourceRecord{
				ID:               "22222222-2222-2222-2222-222222222222",
				SourceKind:       storage.SourceKindUploadedFile,
				DisplayName:      "Clip",
				OriginalFilename: "clip.mp3",
				ObjectKey:        "sources/22222222-2222-2222-2222-222222222222/original/clip.mp3",
				SizeBytes:        42,
				CreatedAt:        time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			},
		},
	}

	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{job.ID: job},
		sourceSets: map[string]storage.SourceSetRecord{
			sourceSet.ID: sourceSet,
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			sourceSet.ID: orderedSources,
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			job.ID: nil,
		},
		childrenByParent: map[string][]storage.JobRecord{
			job.ID: nil,
		},
		claimResult: storage.ClaimJobExecutionResult{
			Job:       job,
			Execution: &execution,
			Claimed:   true,
		},
		activeExecutions: map[string]storage.JobExecutionRecord{
			job.ID + ":" + execution.ExecutionID: execution,
		},
	}
	jobsService := &runtimeJobsDouble{
		transitionResult: storage.JobRecord{
			ID:              job.ID,
			RootJobID:       job.RootJobID,
			SourceSetID:     job.SourceSetID,
			JobType:         job.JobType,
			Status:          "succeeded",
			Delivery:        job.Delivery,
			Version:         5,
			ProgressStage:   "completed",
			ProgressMessage: "done",
			ParamsJSON:      []byte(`{"language":"ru"}`),
			CreatedAt:       job.CreatedAt,
			StartedAt:       job.StartedAt,
			FinishedAt:      ptrTime(time.Date(2026, 4, 23, 10, 10, 0, 0, time.UTC)),
		},
	}
	eventsService := &runtimeEventsDouble{}
	service := newWorkerRuntimeService(jobsService, store, eventsService)
	service.now = func() time.Time { return time.Date(2026, 4, 23, 10, 5, 0, 0, time.UTC) }
	service.nextID = func() string { return "artifact-uuid" }

	claimResponse, err := service.Claim(context.Background(), job.ID, ClaimRequest{
		WorkerKind: "transcription",
		TaskType:   "transcription.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if claimResponse.ExecutionID != execution.ExecutionID {
		t.Fatalf("execution_id = %q, want %q", claimResponse.ExecutionID, execution.ExecutionID)
	}
	if got := len(claimResponse.OrderedInputs); got != 1 {
		t.Fatalf("ordered input count = %d, want 1", got)
	}

	progressMessage := "halfway"
	if err := service.RecordProgress(context.Background(), job.ID, ProgressRequest{
		ExecutionID:     execution.ExecutionID,
		ProgressStage:   "transcribing",
		ProgressMessage: &progressMessage,
	}); err != nil {
		t.Fatalf("RecordProgress() error = %v", err)
	}
	if got := store.jobs[job.ID].Version; got != 3 {
		t.Fatalf("job version after progress = %d, want 3", got)
	}

	if err := service.RecordArtifacts(context.Background(), job.ID, ArtifactUpsertRequest{
		ExecutionID: execution.ExecutionID,
		Artifacts: []ArtifactDescriptor{
			{ArtifactKind: "transcript_plain", Filename: "transcript.txt", MIMEType: "text/plain", ObjectKey: "artifacts/job/transcript/plain/transcript.txt", SizeBytes: 10},
			{ArtifactKind: "transcript_segmented_markdown", Filename: "transcript.md", MIMEType: "text/markdown", ObjectKey: "artifacts/job/transcript/segmented/transcript.md", SizeBytes: 20},
			{ArtifactKind: "transcript_docx", Filename: "transcript.docx", MIMEType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document", ObjectKey: "artifacts/job/transcript/docx/transcript.docx", SizeBytes: 30},
		},
	}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	if got := len(store.upsertArtifacts); got != 3 {
		t.Fatalf("upserted artifact count = %d, want 3", got)
	}
	if got := store.jobs[job.ID].Version; got != 4 {
		t.Fatalf("job version after artifact upsert = %d, want 4", got)
	}

	finalProgress := "completed"
	finalMessage := "done"
	finalSnapshot, err := service.Finalize(context.Background(), job.ID, FinalizeRequest{
		ExecutionID:     execution.ExecutionID,
		Outcome:         "succeeded",
		ProgressStage:   &finalProgress,
		ProgressMessage: &finalMessage,
	})
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if finalSnapshot.Status != "succeeded" {
		t.Fatalf("final snapshot status = %q, want succeeded", finalSnapshot.Status)
	}
	if got := len(store.finishedExecutions); got != 1 {
		t.Fatalf("finished execution count = %d, want 1", got)
	}
	if got := len(eventsService.requests); got < 3 {
		t.Fatalf("event count = %d, want at least 3", got)
	}
	if jobsService.transitionRequest.ToStatus != "succeeded" {
		t.Fatalf("transition target = %q, want succeeded", jobsService.transitionRequest.ToStatus)
	}

	store.jobs[job.ID] = storage.JobRecord{
		ID:                job.ID,
		RootJobID:         job.RootJobID,
		SourceSetID:       job.SourceSetID,
		JobType:           job.JobType,
		Status:            "cancel_requested",
		Delivery:          job.Delivery,
		Version:           6,
		ProgressStage:     "rendering_artifacts",
		CancelRequestedAt: ptrTime(time.Date(2026, 4, 23, 10, 11, 0, 0, time.UTC)),
		CreatedAt:         job.CreatedAt,
	}
	store.activeExecutions[job.ID+":"+execution.ExecutionID] = execution

	cancelState, err := service.CancelCheck(context.Background(), job.ID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("CancelCheck() error = %v", err)
	}
	if !cancelState.CancelRequested || cancelState.Status != "cancel_requested" {
		t.Fatalf("cancel state = %+v, want cancel_requested", cancelState)
	}
}

func TestApiHttpRuntimeClaimUsesTranscriptArtifactForReportChild(t *testing.T) {
	t.Parallel()

	parent := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	child := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeReport, "running", 2)
	child.RootJobID = parent.ID
	child.ParentJobID = parent.ID
	execution := storage.JobExecutionRecord{
		ExecutionID: "33333333-3333-3333-3333-333333333333",
		JobID:       child.ID,
		WorkerKind:  "report",
		TaskType:    "report.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{
			parent.ID: parent,
			child.ID:  child,
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			parent.SourceSetID: {
				{Position: 0, Source: storage.SourceRecord{ID: "source-a", SourceKind: storage.SourceKindUploadedFile}},
				{Position: 1, Source: storage.SourceRecord{ID: "source-b", SourceKind: storage.SourceKindUploadedFile}},
			},
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			parent.ID: {
				runtimeArtifact("artifact-transcript", parent.ID, "transcript_segmented_markdown", "transcript.md", "artifacts/parent/transcript/segmented/transcript.md"),
			},
		},
		claimResult: storage.ClaimJobExecutionResult{Job: child, Execution: &execution, Claimed: true},
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	claimResponse, err := service.Claim(context.Background(), child.ID, ClaimRequest{
		WorkerKind: "report",
		TaskType:   "report.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if got := len(claimResponse.OrderedInputs); got != 1 {
		t.Fatalf("ordered input count = %d, want 1", got)
	}
	input := claimResponse.OrderedInputs[0]
	if input.SourceID != "artifact-transcript" || input.ObjectKey == nil || *input.ObjectKey != "artifacts/parent/transcript/segmented/transcript.md" {
		t.Fatalf("report claim input = %#v, want transcript artifact", input)
	}
}

func TestApiHttpRuntimeClaimUsesTranscriptAndReportArtifactsForDeepResearchChild(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	report := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeReport, "succeeded", 4)
	report.RootJobID = root.ID
	report.ParentJobID = root.ID
	deepResearch := runtimeJobRecord("33333333-3333-3333-3333-333333333333", queue.JobTypeDeepResearch, "running", 2)
	deepResearch.RootJobID = root.ID
	deepResearch.ParentJobID = report.ID
	execution := storage.JobExecutionRecord{
		ExecutionID: "44444444-4444-4444-4444-444444444444",
		JobID:       deepResearch.ID,
		WorkerKind:  "deep_research",
		TaskType:    "deep_research.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{
			root.ID:         root,
			report.ID:       report,
			deepResearch.ID: deepResearch,
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			root.ID: {
				runtimeArtifact("artifact-transcript", root.ID, "transcript_segmented_markdown", "transcript.md", "artifacts/root/transcript/segmented/transcript.md"),
			},
			report.ID: {
				runtimeArtifact("artifact-report", report.ID, "report_markdown", "report.md", "artifacts/report/report/markdown/report.md"),
			},
		},
		claimResult: storage.ClaimJobExecutionResult{Job: deepResearch, Execution: &execution, Claimed: true},
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	claimResponse, err := service.Claim(context.Background(), deepResearch.ID, ClaimRequest{
		WorkerKind: "deep_research",
		TaskType:   "deep_research.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if got := len(claimResponse.OrderedInputs); got != 2 {
		t.Fatalf("ordered input count = %d, want 2", got)
	}
	if claimResponse.OrderedInputs[0].SourceID != "artifact-transcript" {
		t.Fatalf("first deep-research input = %#v, want transcript artifact", claimResponse.OrderedInputs[0])
	}
	if claimResponse.OrderedInputs[1].SourceID != "artifact-report" {
		t.Fatalf("second deep-research input = %#v, want report artifact", claimResponse.OrderedInputs[1])
	}
}

func TestApiHttpRuntimeWorkerLifecycleRejectsStaleExecution(t *testing.T) {
	t.Parallel()

	store := &runtimeStorageDouble{
		activeExecutionErr: storage.ErrExecutionNotFound,
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	err := service.RecordProgress(context.Background(), "11111111-1111-1111-1111-111111111111", ProgressRequest{
		ExecutionID:   "22222222-2222-2222-2222-222222222222",
		ProgressStage: "transcribing",
	})
	if err == nil {
		t.Fatal("expected stale execution error")
	}
	var typed apiError
	if !errors.As(err, &typed) {
		t.Fatalf("error type = %T, want apiError", err)
	}
	if typed.code != "execution_not_found" {
		t.Fatalf("error code = %q, want execution_not_found", typed.code)
	}
}

func TestApiHttpJobsEventBridgeUsesAuthoritativeJobSnapshot(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", "transcription", "running", 2)
	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{job.ID: job},
	}
	eventsService := &runtimeEventsDouble{}
	bridge := newJobsEventBridge(store, eventsService)

	if err := bridge.Emit(context.Background(), jobs.Event{
		JobID:     job.ID,
		EventType: "job.updated",
		ToStatus:  "cancel_requested",
		Version:   job.Version,
	}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
	if eventsService.requests[0].Payload.Status != "cancel_requested" {
		t.Fatalf("payload status = %q, want cancel_requested", eventsService.requests[0].Payload.Status)
	}
}

type runtimeJobsDouble struct {
	createJobsResult     jobs.CreateJobsResult
	createCombinedResult jobs.ChildResult
	createChildResult    jobs.ChildResult
	cancelResult         storage.JobRecord
	retryResult          jobs.RetryResult
	transitionResult     storage.JobRecord

	createUpload      jobs.CreateTranscriptionRequest
	createCombined    jobs.CreateCombinedTranscriptionRequest
	createChild       jobs.CreateChildRequest
	cancelJobID       string
	retryJobID        string
	transitionRequest jobs.TransitionRequest
}

func (d *runtimeJobsDouble) CreateTranscriptionJobs(_ context.Context, req jobs.CreateTranscriptionRequest) (jobs.CreateJobsResult, error) {
	d.createUpload = req
	return d.createJobsResult, nil
}

func (d *runtimeJobsDouble) CreateCombinedTranscriptionJob(_ context.Context, req jobs.CreateCombinedTranscriptionRequest) (jobs.ChildResult, error) {
	d.createCombined = req
	return d.createCombinedResult, nil
}

func (d *runtimeJobsDouble) CreateChildJob(_ context.Context, req jobs.CreateChildRequest) (jobs.ChildResult, error) {
	d.createChild = req
	return d.createChildResult, nil
}

func (d *runtimeJobsDouble) CancelJob(_ context.Context, jobID string) (storage.JobRecord, error) {
	d.cancelJobID = jobID
	return d.cancelResult, nil
}

func (d *runtimeJobsDouble) RetryJob(_ context.Context, jobID string) (jobs.RetryResult, error) {
	d.retryJobID = jobID
	return d.retryResult, nil
}

func (d *runtimeJobsDouble) TransitionJob(_ context.Context, req jobs.TransitionRequest) (storage.JobRecord, error) {
	d.transitionRequest = req
	return d.transitionResult, nil
}

type runtimeStorageDouble struct {
	jobs               map[string]storage.JobRecord
	sourceSets         map[string]storage.SourceSetRecord
	orderedSourcesByID map[string][]storage.OrderedSource
	artifactsByJob     map[string][]storage.ArtifactRecord
	childrenByParent   map[string][]storage.JobRecord
	eventsByJob        map[string][]storage.JobEvent
	activeExecutions   map[string]storage.JobExecutionRecord

	listPage           storage.JobListPage
	claimResult        storage.ClaimJobExecutionResult
	activeExecutionErr error
	finishedExecutions []storage.FinishJobExecutionRequest
	upsertArtifacts    []storage.ArtifactRecord
}

func (d *runtimeStorageDouble) GetJob(_ context.Context, jobID string) (storage.JobRecord, error) {
	job, ok := d.jobs[jobID]
	if !ok {
		return storage.JobRecord{}, storage.ErrJobNotFound
	}
	return job, nil
}

func (d *runtimeStorageDouble) ListJobs(_ context.Context, _ storage.JobListFilter) (storage.JobListPage, error) {
	if d.listPage.Jobs != nil || d.listPage.HasMore {
		return d.listPage, nil
	}
	jobsPage := make([]storage.JobRecord, 0, len(d.jobs))
	for _, job := range d.jobs {
		jobsPage = append(jobsPage, job)
	}
	return storage.JobListPage{Jobs: jobsPage}, nil
}

func (d *runtimeStorageDouble) GetSourceSet(_ context.Context, sourceSetID string) (storage.SourceSetRecord, error) {
	return d.sourceSets[sourceSetID], nil
}

func (d *runtimeStorageDouble) ListOrderedSources(_ context.Context, sourceSetID string) ([]storage.OrderedSource, error) {
	return d.orderedSourcesByID[sourceSetID], nil
}

func (d *runtimeStorageDouble) ListArtifactsByJob(_ context.Context, jobID string) ([]storage.ArtifactRecord, error) {
	return append([]storage.ArtifactRecord(nil), d.artifactsByJob[jobID]...), nil
}

func (d *runtimeStorageDouble) ListChildJobs(_ context.Context, parentJobID string) ([]storage.JobRecord, error) {
	return append([]storage.JobRecord(nil), d.childrenByParent[parentJobID]...), nil
}

func (d *runtimeStorageDouble) ListJobEvents(_ context.Context, jobID string) ([]storage.JobEvent, error) {
	return append([]storage.JobEvent(nil), d.eventsByJob[jobID]...), nil
}

func (d *runtimeStorageDouble) ResolveArtifact(_ context.Context, artifactID string) (storage.ArtifactResolution, error) {
	return storage.ArtifactResolution{ArtifactID: artifactID}, nil
}

func (d *runtimeStorageDouble) ClaimJobExecution(_ context.Context, _ storage.ClaimJobExecutionRequest) (storage.ClaimJobExecutionResult, error) {
	return d.claimResult, nil
}

func (d *runtimeStorageDouble) GetActiveJobExecution(_ context.Context, jobID, executionID string) (storage.JobExecutionRecord, error) {
	if d.activeExecutionErr != nil {
		return storage.JobExecutionRecord{}, d.activeExecutionErr
	}
	execution, ok := d.activeExecutions[jobID+":"+executionID]
	if !ok {
		return storage.JobExecutionRecord{}, storage.ErrExecutionNotFound
	}
	return execution, nil
}

func (d *runtimeStorageDouble) FinishJobExecution(_ context.Context, req storage.FinishJobExecutionRequest) (storage.JobExecutionRecord, error) {
	d.finishedExecutions = append(d.finishedExecutions, req)
	key := req.JobID + ":" + req.ExecutionID
	execution := d.activeExecutions[key]
	finishedAt := req.FinishedAt
	execution.FinishedAt = &finishedAt
	execution.Outcome = req.Outcome
	delete(d.activeExecutions, key)
	return execution, nil
}

func (d *runtimeStorageDouble) UpsertArtifacts(_ context.Context, artifacts []storage.ArtifactRecord) error {
	d.upsertArtifacts = append([]storage.ArtifactRecord(nil), artifacts...)
	if d.artifactsByJob == nil {
		d.artifactsByJob = map[string][]storage.ArtifactRecord{}
	}
	for _, artifact := range artifacts {
		jobArtifacts := d.artifactsByJob[artifact.JobID]
		replaced := false
		for idx := range jobArtifacts {
			if jobArtifacts[idx].ArtifactKind == artifact.ArtifactKind {
				jobArtifacts[idx] = artifact
				replaced = true
				break
			}
		}
		if !replaced {
			jobArtifacts = append(jobArtifacts, artifact)
		}
		d.artifactsByJob[artifact.JobID] = jobArtifacts
	}
	return nil
}

func (d *runtimeStorageDouble) UpdateJob(_ context.Context, job storage.JobRecord) error {
	if d.jobs == nil {
		d.jobs = map[string]storage.JobRecord{}
	}
	d.jobs[job.ID] = job
	return nil
}

func (d *runtimeStorageDouble) FindArtifactByKind(_ context.Context, jobID, artifactKind string) (*storage.ArtifactRecord, error) {
	for _, artifact := range d.artifactsByJob[jobID] {
		if artifact.ArtifactKind == artifactKind {
			copied := artifact
			return &copied, nil
		}
	}
	return nil, nil
}

type runtimeEventsDouble struct {
	requests []ws.EmitRequest
}

func (d *runtimeEventsDouble) EmitJobEvent(_ context.Context, req ws.EmitRequest) (ws.EmitResult, error) {
	d.requests = append(d.requests, req)
	payload, _ := json.Marshal(req.Payload)
	return ws.EmitResult{
		Event: storage.JobEvent{
			ID:        "evt-1",
			JobID:     req.Job.ID,
			RootJobID: req.Job.RootJobID,
			EventType: req.EventType,
			Version:   req.Job.Version,
			Payload:   payload,
			CreatedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		},
		Envelope: ws.JobEventEnvelope{
			EventID:   "evt-1",
			EventType: req.EventType,
			JobID:     req.Job.ID,
			RootJobID: req.Job.RootJobID,
			Version:   req.Job.Version,
			Payload:   req.Payload,
		},
	}, nil
}

func runtimeJobRecord(jobID, jobType, status string, version int64) storage.JobRecord {
	startedAt := time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC)
	return storage.JobRecord{
		ID:              jobID,
		RootJobID:       jobID,
		SourceSetID:     "44444444-4444-4444-4444-444444444444",
		JobType:         jobType,
		Status:          status,
		Delivery:        storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:         version,
		ProgressStage:   "running",
		ProgressMessage: "working",
		ParamsJSON:      []byte(`{"language":"ru"}`),
		CreatedAt:       time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		StartedAt:       &startedAt,
	}
}

func runtimeSourceSet(sourceSetID, inputKind string) storage.SourceSetRecord {
	return storage.SourceSetRecord{
		ID:        sourceSetID,
		InputKind: inputKind,
		CreatedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		Items: []storage.SourceSetItem{
			{Position: 0, SourceID: "22222222-2222-2222-2222-222222222222"},
		},
	}
}

func runtimeArtifact(id, jobID, artifactKind, filename, objectKey string) storage.ArtifactRecord {
	return storage.ArtifactRecord{
		ID:           id,
		JobID:        jobID,
		ArtifactKind: artifactKind,
		Filename:     filename,
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    objectKey,
		SizeBytes:    42,
		CreatedAt:    time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
