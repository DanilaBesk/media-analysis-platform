package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
)

const TransitionMarker = "[ApiJobs][transitionJob][BLOCK_VALIDATE_AND_TRANSITION_JOB]"

var (
	ErrInvalidJobState     = errors.New("invalid_job_state")
	ErrIdempotencyConflict = errors.New("idempotency_conflict")
	ErrMissingArtifact     = errors.New("missing_artifact_prerequisite")
	ErrJobNotFound         = errors.New("job_not_found")
)

type Logger interface {
	Printf(format string, args ...any)
}

type Store interface {
	FindSubmission(ctx context.Context, submissionKind, idempotencyKey string) (*storage.JobSubmission, error)
	ListJobsBySubmission(ctx context.Context, submissionID string) ([]storage.JobRecord, error)
	SaveSubmissionGraph(ctx context.Context, submission storage.JobSubmission, sources []storage.SourceRecord, sourceSets []storage.SourceSetRecord, jobs []storage.JobRecord) error
	GetJob(ctx context.Context, jobID string) (storage.JobRecord, error)
	CreateJob(ctx context.Context, job storage.JobRecord) error
	UpdateJob(ctx context.Context, job storage.JobRecord) error
	FindCanonicalChild(ctx context.Context, parentJobID, jobType string) (*storage.JobRecord, error)
	FindArtifactByKind(ctx context.Context, jobID, artifactKind string) (*storage.ArtifactRecord, error)
}

type Enqueuer interface {
	Enqueue(ctx context.Context, req queue.EnqueueRequest) (queue.EnqueueResult, error)
}

type EventEmitter interface {
	Emit(ctx context.Context, event Event) error
}

type Event struct {
	JobID      string
	EventType  string
	ToStatus   string
	Version    int64
	OccurredAt time.Time
}

type Service struct {
	store  Store
	queue  Enqueuer
	events EventEmitter
	logger Logger
	now    func() time.Time
	nextID func() string
}

type Option func(*Service)

func WithLogger(logger Logger) Option {
	return func(s *Service) {
		s.logger = logger
	}
}

func WithClock(now func() time.Time) Option {
	return func(s *Service) {
		s.now = now
	}
}

func WithIDGenerator(next func() string) Option {
	return func(s *Service) {
		s.nextID = next
	}
}

func WithEventEmitter(emitter EventEmitter) Option {
	return func(s *Service) {
		s.events = emitter
	}
}

func NewService(store Store, enqueuer Enqueuer, opts ...Option) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidJobState)
	}
	if enqueuer == nil {
		return nil, fmt.Errorf("%w: queue publisher is required", ErrInvalidJobState)
	}

	service := &Service{
		store:  store,
		queue:  enqueuer,
		now:    time.Now().UTC,
		nextID: uuid.NewString,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service, nil
}

type CreateTranscriptionRequest struct {
	SubmissionKind     string
	IdempotencyKey     string
	RequestFingerprint string
	Sources            []storage.SourceRecord
	Delivery           storage.Delivery
	ClientRef          string
}

type CreateCombinedTranscriptionRequest struct {
	SubmissionKind     string
	IdempotencyKey     string
	RequestFingerprint string
	Sources            []storage.SourceRecord
	Delivery           storage.Delivery
	ClientRef          string
	DisplayName        string
}

type CreateJobsResult struct {
	Jobs   []storage.JobRecord
	Reused bool
}

type CreateChildRequest struct {
	ParentJobID  string
	ChildJobType string
}

type ChildResult struct {
	Job    storage.JobRecord
	Reused bool
}

type RetryResult struct {
	Job storage.JobRecord
}

type TransitionRequest struct {
	JobID           string
	ToStatus        string
	ProgressStage   string
	ProgressMessage string
	ErrorCode       string
	ErrorMessage    string
}

func (s *Service) CreateTranscriptionJobs(ctx context.Context, req CreateTranscriptionRequest) (CreateJobsResult, error) {
	if err := validateRootCreate(req.SubmissionKind, req.RequestFingerprint, req.Sources, req.Delivery); err != nil {
		return CreateJobsResult{}, err
	}

	if reused, err := s.replaySubmissionIfPresent(ctx, req.SubmissionKind, req.IdempotencyKey, req.RequestFingerprint); reused.Jobs != nil || err != nil {
		return reused, err
	}

	submissionID := s.nextID()
	submission := storage.JobSubmission{
		ID:                 submissionID,
		SubmissionKind:     req.SubmissionKind,
		IdempotencyKey:     submissionIdempotencyKey(req.IdempotencyKey, submissionID),
		RequestFingerprint: req.RequestFingerprint,
		CreatedAt:          s.now(),
	}

	sourceSets := make([]storage.SourceSetRecord, 0, len(req.Sources))
	jobs := make([]storage.JobRecord, 0, len(req.Sources))
	for _, source := range req.Sources {
		jobID := s.nextID()
		sourceSetID := s.nextID()
		sourceSets = append(sourceSets, storage.SourceSetRecord{
			ID:        sourceSetID,
			InputKind: storage.SourceSetInputSingleSource,
			Items: []storage.SourceSetItem{
				{Position: 0, SourceID: source.ID},
			},
			CreatedAt: s.now(),
		})
		jobs = append(jobs, newRootJob(jobID, submission.ID, sourceSetID, req.ClientRef, req.Delivery, s.now()))
	}

	if err := s.store.SaveSubmissionGraph(ctx, submission, req.Sources, sourceSets, jobs); err != nil {
		return CreateJobsResult{}, err
	}

	for _, job := range jobs {
		if _, err := s.queue.Enqueue(ctx, queue.EnqueueRequest{JobID: job.ID, JobType: job.JobType, Attempt: 1}); err != nil {
			return CreateJobsResult{}, err
		}
	}

	return CreateJobsResult{Jobs: jobs}, nil
}

func (s *Service) CreateCombinedTranscriptionJob(ctx context.Context, req CreateCombinedTranscriptionRequest) (ChildResult, error) {
	if err := validateRootCreate(req.SubmissionKind, req.RequestFingerprint, req.Sources, req.Delivery); err != nil {
		return ChildResult{}, err
	}
	if len(req.Sources) < 2 {
		return ChildResult{}, fmt.Errorf("%w: combined transcription requires at least two sources", ErrInvalidJobState)
	}

	if reused, err := s.replaySubmissionIfPresent(ctx, req.SubmissionKind, req.IdempotencyKey, req.RequestFingerprint); reused.Jobs != nil || err != nil {
		if len(reused.Jobs) != 1 && err == nil {
			return ChildResult{}, fmt.Errorf("%w: combined submission replay must resolve to one job", ErrInvalidJobState)
		}
		if err != nil {
			return ChildResult{}, err
		}
		return ChildResult{Job: reused.Jobs[0], Reused: true}, nil
	}

	submissionID := s.nextID()
	submission := storage.JobSubmission{
		ID:                 submissionID,
		SubmissionKind:     req.SubmissionKind,
		IdempotencyKey:     submissionIdempotencyKey(req.IdempotencyKey, submissionID),
		RequestFingerprint: req.RequestFingerprint,
		CreatedAt:          s.now(),
	}

	sourceSetID := s.nextID()
	items := make([]storage.SourceSetItem, 0, len(req.Sources))
	for idx, source := range req.Sources {
		items = append(items, storage.SourceSetItem{Position: idx, SourceID: source.ID})
	}

	sourceSet := storage.SourceSetRecord{
		ID:          sourceSetID,
		InputKind:   storage.SourceSetInputCombinedUpload,
		DisplayName: req.DisplayName,
		Items:       items,
		CreatedAt:   s.now(),
	}

	job := newRootJob(s.nextID(), submission.ID, sourceSetID, req.ClientRef, req.Delivery, s.now())

	if err := s.store.SaveSubmissionGraph(ctx, submission, req.Sources, []storage.SourceSetRecord{sourceSet}, []storage.JobRecord{job}); err != nil {
		return ChildResult{}, err
	}
	if _, err := s.queue.Enqueue(ctx, queue.EnqueueRequest{JobID: job.ID, JobType: job.JobType, Attempt: 1}); err != nil {
		return ChildResult{}, err
	}

	return ChildResult{Job: job}, nil
}

func (s *Service) CreateChildJob(ctx context.Context, req CreateChildRequest) (ChildResult, error) {
	parent, err := s.store.GetJob(ctx, req.ParentJobID)
	if err != nil {
		return ChildResult{}, err
	}

	if existing, err := s.store.FindCanonicalChild(ctx, parent.ID, req.ChildJobType); err != nil {
		return ChildResult{}, err
	} else if existing != nil {
		return ChildResult{Job: *existing, Reused: true}, nil
	}

	if err := validateChildRequest(parent, req.ChildJobType); err != nil {
		return ChildResult{}, err
	}
	if err := s.validateChildArtifacts(ctx, parent, req.ChildJobType); err != nil {
		return ChildResult{}, err
	}

	child := storage.JobRecord{
		ID:              s.nextID(),
		RootJobID:       parent.RootJobID,
		ParentJobID:     parent.ID,
		SourceSetID:     parent.SourceSetID,
		JobType:         req.ChildJobType,
		Status:          "queued",
		Delivery:        parent.Delivery,
		Version:         1,
		ClientRef:       parent.ClientRef,
		CreatedAt:       s.now(),
		ProgressStage:   "queued",
		ProgressMessage: "",
		ParamsJSON:      []byte("{}"),
	}

	if err := s.store.CreateJob(ctx, child); err != nil {
		return ChildResult{}, err
	}
	if _, err := s.queue.Enqueue(ctx, queue.EnqueueRequest{JobID: child.ID, JobType: child.JobType, Attempt: 1}); err != nil {
		return ChildResult{}, err
	}
	return ChildResult{Job: child}, nil
}

func (s *Service) CancelJob(ctx context.Context, jobID string) (storage.JobRecord, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return storage.JobRecord{}, err
	}

	switch job.Status {
	case "succeeded", "failed", "canceled":
		return job, nil
	}

	cancelAt := s.now()
	job.CancelRequestedAt = &cancelAt

	switch job.Status {
	case "queued":
		job.Status = "canceled"
		job.FinishedAt = &cancelAt
		job.Version++
	case "running":
		job.Status = "cancel_requested"
		job.Version++
	default:
		return storage.JobRecord{}, fmt.Errorf("%w: cancel unsupported from %s", ErrInvalidJobState, job.Status)
	}

	if err := s.store.UpdateJob(ctx, job); err != nil {
		return storage.JobRecord{}, err
	}
	if err := s.emit(ctx, Event{JobID: job.ID, EventType: eventTypeForStatus(job.Status), ToStatus: job.Status, Version: job.Version, OccurredAt: cancelAt}); err != nil {
		return storage.JobRecord{}, err
	}
	return job, nil
}

func (s *Service) RetryJob(ctx context.Context, jobID string) (RetryResult, error) {
	original, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return RetryResult{}, err
	}
	if !isTerminal(original.Status) {
		return RetryResult{}, fmt.Errorf("%w: retry requires terminal job", ErrInvalidJobState)
	}

	retry := storage.JobRecord{
		ID:              s.nextID(),
		RootJobID:       original.RootJobID,
		ParentJobID:     original.ParentJobID,
		RetryOfJobID:    original.ID,
		SourceSetID:     original.SourceSetID,
		JobType:         original.JobType,
		Status:          "queued",
		Delivery:        original.Delivery,
		Version:         1,
		ClientRef:       original.ClientRef,
		CreatedAt:       s.now(),
		ProgressStage:   "queued",
		ProgressMessage: "",
		ParamsJSON:      []byte("{}"),
	}

	if err := s.store.CreateJob(ctx, retry); err != nil {
		return RetryResult{}, err
	}
	if _, err := s.queue.Enqueue(ctx, queue.EnqueueRequest{JobID: retry.ID, JobType: retry.JobType, Attempt: 1}); err != nil {
		return RetryResult{}, err
	}
	return RetryResult{Job: retry}, nil
}

func (s *Service) TransitionJob(ctx context.Context, req TransitionRequest) (storage.JobRecord, error) {
	job, err := s.store.GetJob(ctx, req.JobID)
	if err != nil {
		return storage.JobRecord{}, err
	}

	if !isAllowedTransition(job.Status, req.ToStatus) {
		return storage.JobRecord{}, fmt.Errorf("%w: invalid transition %s -> %s", ErrInvalidJobState, job.Status, req.ToStatus)
	}

	s.logf("%s job_id=%s from=%s to=%s", TransitionMarker, job.ID, job.Status, req.ToStatus)

	now := s.now()
	job.Status = req.ToStatus
	job.Version++
	if strings.TrimSpace(req.ProgressStage) != "" {
		job.ProgressStage = req.ProgressStage
	}
	if req.ProgressMessage != "" {
		job.ProgressMessage = req.ProgressMessage
	}
	if req.ErrorCode != "" {
		job.ErrorCode = req.ErrorCode
	}
	if req.ErrorMessage != "" {
		job.ErrorMessage = req.ErrorMessage
	}

	switch req.ToStatus {
	case "running":
		job.StartedAt = &now
	case "cancel_requested":
		job.CancelRequestedAt = &now
	case "succeeded", "failed", "canceled":
		job.FinishedAt = &now
	}

	if err := s.store.UpdateJob(ctx, job); err != nil {
		return storage.JobRecord{}, err
	}
	if err := s.emit(ctx, Event{JobID: job.ID, EventType: eventTypeForStatus(job.Status), ToStatus: job.Status, Version: job.Version, OccurredAt: now}); err != nil {
		return storage.JobRecord{}, err
	}
	return job, nil
}

func validateRootCreate(submissionKind, requestFingerprint string, sources []storage.SourceRecord, delivery storage.Delivery) error {
	if strings.TrimSpace(submissionKind) == "" || strings.TrimSpace(requestFingerprint) == "" {
		return fmt.Errorf("%w: submission kind and request fingerprint are required", ErrInvalidJobState)
	}
	if len(sources) == 0 {
		return fmt.Errorf("%w: at least one source is required", ErrInvalidJobState)
	}
	switch delivery.Strategy {
	case storage.DeliveryStrategyPolling:
		if delivery.WebhookURL != "" {
			return fmt.Errorf("%w: polling delivery must not set webhook url", ErrInvalidJobState)
		}
	case storage.DeliveryStrategyWebhook:
		if delivery.WebhookURL == "" {
			return fmt.Errorf("%w: webhook delivery requires webhook url", ErrInvalidJobState)
		}
	default:
		return fmt.Errorf("%w: unsupported delivery strategy %q", ErrInvalidJobState, delivery.Strategy)
	}
	return nil
}

func (s *Service) replaySubmissionIfPresent(ctx context.Context, submissionKind, idempotencyKey, requestFingerprint string) (CreateJobsResult, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return CreateJobsResult{}, nil
	}

	submission, err := s.store.FindSubmission(ctx, submissionKind, idempotencyKey)
	if err != nil || submission == nil {
		return CreateJobsResult{}, err
	}
	if submission.RequestFingerprint != requestFingerprint {
		return CreateJobsResult{}, fmt.Errorf("%w: request fingerprint differs for existing submission", ErrIdempotencyConflict)
	}
	jobs, err := s.store.ListJobsBySubmission(ctx, submission.ID)
	if err != nil {
		return CreateJobsResult{}, err
	}
	return CreateJobsResult{Jobs: jobs, Reused: true}, nil
}

func submissionIdempotencyKey(idempotencyKey, submissionID string) string {
	key := strings.TrimSpace(idempotencyKey)
	if key != "" {
		return key
	}
	return "non-idempotent:" + submissionID
}

func validateChildRequest(parent storage.JobRecord, childJobType string) error {
	switch childJobType {
	case queue.JobTypeReport:
		if parent.JobType != queue.JobTypeTranscription || parent.Status != "succeeded" {
			return fmt.Errorf("%w: report child requires succeeded transcription parent", ErrInvalidJobState)
		}
	case queue.JobTypeDeepResearch:
		if parent.JobType != queue.JobTypeReport || parent.Status != "succeeded" {
			return fmt.Errorf("%w: deep research child requires succeeded report parent", ErrInvalidJobState)
		}
	default:
		return fmt.Errorf("%w: unsupported child job type %q", ErrInvalidJobState, childJobType)
	}
	return nil
}

func (s *Service) validateChildArtifacts(ctx context.Context, parent storage.JobRecord, childJobType string) error {
	requiredArtifact := map[string]string{
		queue.JobTypeReport:       "transcript_plain",
		queue.JobTypeDeepResearch: "report_markdown",
	}[childJobType]

	artifact, err := s.store.FindArtifactByKind(ctx, parent.ID, requiredArtifact)
	if err != nil {
		return err
	}
	if artifact == nil {
		return fmt.Errorf("%w: missing %s for %s child", ErrMissingArtifact, requiredArtifact, childJobType)
	}
	return nil
}

func newRootJob(jobID, submissionID, sourceSetID, clientRef string, delivery storage.Delivery, now time.Time) storage.JobRecord {
	return storage.JobRecord{
		ID:              jobID,
		SubmissionID:    submissionID,
		RootJobID:       jobID,
		SourceSetID:     sourceSetID,
		JobType:         queue.JobTypeTranscription,
		Status:          "queued",
		Delivery:        delivery,
		Version:         1,
		ProgressStage:   "queued",
		ProgressMessage: "",
		ClientRef:       clientRef,
		ParamsJSON:      []byte("{}"),
		CreatedAt:       now,
	}
}

func isTerminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "canceled"
}

func eventTypeForStatus(status string) string {
	switch status {
	case "succeeded":
		return "job.completed"
	case "failed":
		return "job.failed"
	case "canceled":
		return "job.canceled"
	default:
		return "job.updated"
	}
}

func isAllowedTransition(from, to string) bool {
	allowed := map[string]map[string]struct{}{
		"queued": {
			"running":  {},
			"canceled": {},
		},
		"running": {
			"succeeded":        {},
			"failed":           {},
			"cancel_requested": {},
		},
		"cancel_requested": {
			"canceled": {},
			"failed":   {},
		},
	}
	next, ok := allowed[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

func (s *Service) emit(ctx context.Context, event Event) error {
	if s.events == nil {
		return nil
	}
	return s.events.Emit(ctx, event)
}

func (s *Service) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}
