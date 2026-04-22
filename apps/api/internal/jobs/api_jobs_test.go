package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
)

func TestApiJobsCreateTranscriptionJobsSupportsIdempotentReplay(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	request := CreateTranscriptionRequest{
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "idem-1",
		RequestFingerprint: "upload-manifest-a",
		Sources: []storage.SourceRecord{
			source("source-1"),
			source("source-2"),
		},
		Delivery:  storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		ClientRef: "client-1",
	}

	created, err := service.CreateTranscriptionJobs(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateTranscriptionJobs() error = %v", err)
	}
	if got, want := len(created.Jobs), 2; got != want {
		t.Fatalf("created jobs = %d, want %d", got, want)
	}
	if got, want := len(enqueuer.requests), 2; got != want {
		t.Fatalf("enqueue count = %d, want %d", got, want)
	}

	replayed, err := service.CreateTranscriptionJobs(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateTranscriptionJobs(replay) error = %v", err)
	}
	if !replayed.Reused {
		t.Fatalf("expected replay to reuse original jobs")
	}
	if got, want := len(enqueuer.requests), 2; got != want {
		t.Fatalf("replay should not enqueue again, got %d requests", got)
	}

	_, err = service.CreateTranscriptionJobs(context.Background(), CreateTranscriptionRequest{
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "idem-1",
		RequestFingerprint: "upload-manifest-b",
		Sources:            request.Sources,
		Delivery:           request.Delivery,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay should fail with ErrIdempotencyConflict, got %v", err)
	}
}

func TestApiJobsCreateCombinedTranscriptionJobPreservesOneJobSemantics(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	result, err := service.CreateCombinedTranscriptionJob(context.Background(), CreateCombinedTranscriptionRequest{
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "idem-combined",
		RequestFingerprint: "combined-manifest",
		Sources: []storage.SourceRecord{
			source("source-a"),
			source("source-b"),
		},
		Delivery:    storage.Delivery{Strategy: storage.DeliveryStrategyWebhook, WebhookURL: "https://example.com/hook"},
		DisplayName: "combined-job",
	})
	if err != nil {
		t.Fatalf("CreateCombinedTranscriptionJob() error = %v", err)
	}
	if result.Reused {
		t.Fatalf("first combined create should not be reused")
	}
	if got, want := len(store.jobsBySubmission(store.lastSubmissionID)), 1; got != want {
		t.Fatalf("combined submission created %d jobs, want 1", got)
	}
	if got, want := len(store.sourceSets), 1; got != want {
		t.Fatalf("combined submission created %d source sets, want 1", got)
	}
	sourceSet := store.sourceSets[0]
	if sourceSet.InputKind != storage.SourceSetInputCombinedUpload || len(sourceSet.Items) != 2 {
		t.Fatalf("combined source set = %#v", sourceSet)
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("combined create enqueue count = %d, want 1", got)
	}
}

func TestApiJobsCreateChildJobReusesCanonicalChildAndValidatesArtifacts(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	parent := storage.JobRecord{
		ID:          "parent-transcription",
		RootJobID:   "parent-transcription",
		SourceSetID: "source-set-1",
		JobType:     queue.JobTypeTranscription,
		Status:      "succeeded",
		Delivery:    storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:     3,
	}
	store.jobs[parent.ID] = parent

	_, err := service.CreateChildJob(context.Background(), CreateChildRequest{
		ParentJobID:  parent.ID,
		ChildJobType: queue.JobTypeReport,
	})
	if !errors.Is(err, ErrMissingArtifact) {
		t.Fatalf("missing artifact should fail with ErrMissingArtifact, got %v", err)
	}

	store.artifacts[parent.ID+"::transcript_plain"] = &storage.ArtifactRecord{
		ID:           "artifact-1",
		JobID:        parent.ID,
		ArtifactKind: "transcript_plain",
	}

	created, err := service.CreateChildJob(context.Background(), CreateChildRequest{
		ParentJobID:  parent.ID,
		ChildJobType: queue.JobTypeReport,
	})
	if err != nil {
		t.Fatalf("CreateChildJob() error = %v", err)
	}
	if created.Reused {
		t.Fatalf("first child create should not be reused")
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("enqueue count = %d, want %d", got, want)
	}

	reused, err := service.CreateChildJob(context.Background(), CreateChildRequest{
		ParentJobID:  parent.ID,
		ChildJobType: queue.JobTypeReport,
	})
	if err != nil {
		t.Fatalf("CreateChildJob(reuse) error = %v", err)
	}
	if !reused.Reused {
		t.Fatalf("expected canonical child reuse on second call")
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("reused child should not enqueue again, got %d requests", got)
	}
}

func TestApiJobsRetryCreatesNewLineageLinkedRow(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	terminal := storage.JobRecord{
		ID:          "job-terminal",
		RootJobID:   "root-job",
		ParentJobID: "parent-job",
		SourceSetID: "source-set-7",
		JobType:     queue.JobTypeReport,
		Status:      "failed",
		Delivery:    storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:     5,
	}
	store.jobs[terminal.ID] = terminal
	store.artifacts[terminal.ID+"::report_markdown"] = &storage.ArtifactRecord{ID: "artifact-x", JobID: terminal.ID, ArtifactKind: "report_markdown"}

	result, err := service.RetryJob(context.Background(), terminal.ID)
	if err != nil {
		t.Fatalf("RetryJob() error = %v", err)
	}
	if result.Job.RetryOfJobID != terminal.ID {
		t.Fatalf("retry_of_job_id = %q, want %q", result.Job.RetryOfJobID, terminal.ID)
	}
	if result.Job.RootJobID != terminal.RootJobID || result.Job.ParentJobID != terminal.ParentJobID || result.Job.SourceSetID != terminal.SourceSetID {
		t.Fatalf("retry lineage = %#v, want root=%q parent=%q source_set=%q", result.Job, terminal.RootJobID, terminal.ParentJobID, terminal.SourceSetID)
	}
	if _, ok := store.artifacts[result.Job.ID+"::report_markdown"]; ok {
		t.Fatalf("retry should not copy artifacts to new row")
	}
}

func TestApiJobsCancelAndTransitionRules(t *testing.T) {
	t.Parallel()

	logger := &jobsBufferLogger{}
	store := newMemoryStore()
	emitter := &fakeEmitter{}
	service := newTestService(t, store, &fakeEnqueuer{}, WithLogger(logger), WithEventEmitter(emitter))

	queued := storage.JobRecord{ID: "queued-job", RootJobID: "queued-job", SourceSetID: "ss-1", JobType: queue.JobTypeTranscription, Status: "queued", Delivery: storage.Delivery{Strategy: storage.DeliveryStrategyPolling}, Version: 1}
	running := storage.JobRecord{ID: "running-job", RootJobID: "running-job", SourceSetID: "ss-2", JobType: queue.JobTypeTranscription, Status: "running", Delivery: storage.Delivery{Strategy: storage.DeliveryStrategyPolling}, Version: 2}
	store.jobs[queued.ID] = queued
	store.jobs[running.ID] = running

	canceled, err := service.CancelJob(context.Background(), queued.ID)
	if err != nil {
		t.Fatalf("CancelJob(queued) error = %v", err)
	}
	if canceled.Status != "canceled" || canceled.FinishedAt == nil {
		t.Fatalf("queued cancel result = %#v", canceled)
	}

	cancelRequested, err := service.CancelJob(context.Background(), running.ID)
	if err != nil {
		t.Fatalf("CancelJob(running) error = %v", err)
	}
	if cancelRequested.Status != "cancel_requested" || cancelRequested.CancelRequestedAt == nil {
		t.Fatalf("running cancel result = %#v", cancelRequested)
	}

	transitioned, err := service.TransitionJob(context.Background(), TransitionRequest{
		JobID:           running.ID,
		ToStatus:        "failed",
		ProgressStage:   "failed",
		ProgressMessage: "worker crashed",
		ErrorCode:       "pipeline_failed",
		ErrorMessage:    "boom",
	})
	if err != nil {
		t.Fatalf("TransitionJob() error = %v", err)
	}
	if transitioned.Status != "failed" || transitioned.FinishedAt == nil {
		t.Fatalf("transition result = %#v", transitioned)
	}
	if !strings.Contains(logger.String(), TransitionMarker) {
		t.Fatalf("logger output missing marker %q", TransitionMarker)
	}
	if got, want := len(emitter.events), 3; got != want {
		t.Fatalf("emitted events = %d, want %d", got, want)
	}

	_, err = service.TransitionJob(context.Background(), TransitionRequest{
		JobID:    queued.ID,
		ToStatus: "succeeded",
	})
	if !errors.Is(err, ErrInvalidJobState) {
		t.Fatalf("invalid transition should fail with ErrInvalidJobState, got %v", err)
	}
}

func TestApiJobsTransitionMatrixFixture(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("testdata", "transition_matrix.json"))
	if err != nil {
		t.Fatalf("ReadFile(testdata/transition_matrix.json) error = %v", err)
	}

	var fixture struct {
		Allowed []struct {
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"allowed"`
		Blocked []struct {
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"blocked"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("Unmarshal(transition_matrix.json) error = %v", err)
	}

	for _, tc := range fixture.Allowed {
		if !isAllowedTransition(tc.From, tc.To) {
			t.Fatalf("expected %s -> %s to be allowed", tc.From, tc.To)
		}
	}
	for _, tc := range fixture.Blocked {
		if isAllowedTransition(tc.From, tc.To) {
			t.Fatalf("expected %s -> %s to be blocked", tc.From, tc.To)
		}
	}
}

func newTestService(t *testing.T, store *memoryStore, enqueuer *fakeEnqueuer, opts ...Option) *Service {
	t.Helper()

	idIndex := 0
	base := []Option{
		WithClock(func() time.Time { return time.Date(2026, 4, 22, 13, 0, 0, 0, time.UTC) }),
		WithIDGenerator(func() string {
			idIndex++
			return fmt.Sprintf("generated-%02d", idIndex)
		}),
	}
	base = append(base, opts...)

	service, err := NewService(store, enqueuer, base...)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func source(id string) storage.SourceRecord {
	return storage.SourceRecord{
		ID:          id,
		SourceKind:  storage.SourceKindUploadedFile,
		DisplayName: id,
		ObjectKey:   "sources/" + id,
		ObjectBody:  []byte("payload"),
		MIMEType:    "audio/mpeg",
	}
}

type memoryStore struct {
	submissions      map[string]*storage.JobSubmission
	jobs             map[string]storage.JobRecord
	jobOrder         []string
	sourceSets       []storage.SourceSetRecord
	artifacts        map[string]*storage.ArtifactRecord
	lastSubmissionID string
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		submissions: map[string]*storage.JobSubmission{},
		jobs:        map[string]storage.JobRecord{},
		artifacts:   map[string]*storage.ArtifactRecord{},
	}
}

func (m *memoryStore) FindSubmission(_ context.Context, submissionKind, idempotencyKey string) (*storage.JobSubmission, error) {
	return m.submissions[submissionKind+"::"+idempotencyKey], nil
}

func (m *memoryStore) ListJobsBySubmission(_ context.Context, submissionID string) ([]storage.JobRecord, error) {
	return m.jobsBySubmission(submissionID), nil
}

func (m *memoryStore) SaveSubmissionGraph(_ context.Context, submission storage.JobSubmission, _ []storage.SourceRecord, sourceSets []storage.SourceSetRecord, jobs []storage.JobRecord) error {
	copySubmission := submission
	m.submissions[submission.SubmissionKind+"::"+submission.IdempotencyKey] = &copySubmission
	m.lastSubmissionID = submission.ID
	m.sourceSets = append(m.sourceSets, sourceSets...)
	for _, job := range jobs {
		m.jobs[job.ID] = job
		m.jobOrder = append(m.jobOrder, job.ID)
	}
	return nil
}

func (m *memoryStore) GetJob(_ context.Context, jobID string) (storage.JobRecord, error) {
	job, ok := m.jobs[jobID]
	if !ok {
		return storage.JobRecord{}, ErrJobNotFound
	}
	return job, nil
}

func (m *memoryStore) CreateJob(_ context.Context, job storage.JobRecord) error {
	m.jobs[job.ID] = job
	m.jobOrder = append(m.jobOrder, job.ID)
	return nil
}

func (m *memoryStore) UpdateJob(_ context.Context, job storage.JobRecord) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *memoryStore) FindCanonicalChild(_ context.Context, parentJobID, jobType string) (*storage.JobRecord, error) {
	for _, job := range m.jobs {
		if job.ParentJobID == parentJobID && job.JobType == jobType && job.RetryOfJobID == "" && (job.Status == "queued" || job.Status == "running" || job.Status == "cancel_requested" || job.Status == "succeeded") {
			copyJob := job
			return &copyJob, nil
		}
	}
	return nil, nil
}

func (m *memoryStore) FindArtifactByKind(_ context.Context, jobID, artifactKind string) (*storage.ArtifactRecord, error) {
	return m.artifacts[jobID+"::"+artifactKind], nil
}

func (m *memoryStore) jobsBySubmission(submissionID string) []storage.JobRecord {
	jobs := make([]storage.JobRecord, 0)
	for _, jobID := range m.jobOrder {
		job := m.jobs[jobID]
		if job.SubmissionID == submissionID {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

type fakeEnqueuer struct {
	requests []queue.EnqueueRequest
}

func (f *fakeEnqueuer) Enqueue(_ context.Context, req queue.EnqueueRequest) (queue.EnqueueResult, error) {
	f.requests = append(f.requests, req)
	return queue.EnqueueResult{}, nil
}

type fakeEmitter struct {
	events []Event
}

func (f *fakeEmitter) Emit(_ context.Context, event Event) error {
	f.events = append(f.events, event)
	return nil
}

type jobsBufferLogger struct {
	lines []string
}

func (b *jobsBufferLogger) Printf(format string, args ...any) {
	b.lines = append(b.lines, fmt.Sprintf(format, args...))
}

func (b *jobsBufferLogger) String() string {
	return strings.Join(b.lines, "\n")
}
