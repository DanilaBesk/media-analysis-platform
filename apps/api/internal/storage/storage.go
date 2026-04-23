package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	LogPrefix                    = "[ApiStorage]"
	PersistJobArtifactMarker     = "[ApiStorage][persistJob][BLOCK_PERSIST_JOB_AND_ARTIFACT]"
	SourcesBucket                = "sources"
	ArtifactsBucket              = "artifacts"
	DownloadProviderMinIO        = "minio_presigned_url"
	DeliveryStrategyPolling      = "polling"
	DeliveryStrategyWebhook      = "webhook"
	SourceKindUploadedFile       = "uploaded_file"
	SourceKindTelegramUpload     = "telegram_upload"
	SourceKindYouTubeURL         = "youtube_url"
	SourceSetInputSingleSource   = "single_source"
	SourceSetInputCombinedUpload = "combined_upload"
	SourceSetInputCombinedTG     = "combined_telegram"
	WebhookStatePending          = "pending"
	WebhookStateDelivered        = "delivered"
	WebhookStateDead             = "dead"
)

var (
	ErrStorageUnavailable = errors.New("storage_unavailable")
	ErrArtifactNotFound   = errors.New("artifact_not_found")
	ErrJobNotFound        = errors.New("job_not_found")
	ErrContractViolation  = errors.New("storage_contract_violation")
	ErrExecutionNotFound  = errors.New("execution_not_found")
)

type Logger interface {
	Printf(format string, args ...any)
}

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, objectKey, contentType string, body []byte) error
	PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error)
}

type StateStore interface {
	PersistSource(ctx context.Context, source SourceRecord) error
	PersistJobBundle(ctx context.Context, bundle PersistedJobBundle) error
	PersistArtifacts(ctx context.Context, artifacts []ArtifactRecord) error
	LookupArtifact(ctx context.Context, artifactID string) (ArtifactRecord, error)
	FindSubmission(ctx context.Context, submissionKind, idempotencyKey string) (*JobSubmission, error)
	ListJobsBySubmission(ctx context.Context, submissionID string) ([]JobRecord, error)
	SaveSubmissionGraph(ctx context.Context, submission JobSubmission, sources []SourceRecord, sourceSets []SourceSetRecord, jobs []JobRecord) error
	GetJob(ctx context.Context, jobID string) (JobRecord, error)
	CreateJob(ctx context.Context, job JobRecord) error
	UpdateJob(ctx context.Context, job JobRecord) error
	FindCanonicalChild(ctx context.Context, parentJobID, jobType string) (*JobRecord, error)
	FindArtifactByKind(ctx context.Context, jobID, artifactKind string) (*ArtifactRecord, error)
	AppendJobEvent(ctx context.Context, event JobEvent) error
	CreateWebhookDelivery(ctx context.Context, delivery WebhookDelivery) error
	UpdateWebhookDelivery(ctx context.Context, delivery WebhookDelivery) error
	ClaimJobExecution(ctx context.Context, req ClaimJobExecutionRequest) (ClaimJobExecutionResult, error)
	GetActiveJobExecution(ctx context.Context, jobID, executionID string) (JobExecutionRecord, error)
	FinishJobExecution(ctx context.Context, req FinishJobExecutionRequest) (JobExecutionRecord, error)
}

type Repository struct {
	state      StateStore
	objects    ObjectStore
	logger     Logger
	now        func() time.Time
	nextID     func() string
	presignTTL time.Duration
}

type Option func(*Repository)

func WithLogger(logger Logger) Option {
	return func(r *Repository) {
		r.logger = logger
	}
}

func WithClock(now func() time.Time) Option {
	return func(r *Repository) {
		r.now = now
	}
}

func WithIDGenerator(nextID func() string) Option {
	return func(r *Repository) {
		r.nextID = nextID
	}
}

func WithPresignTTL(ttl time.Duration) Option {
	return func(r *Repository) {
		r.presignTTL = ttl
	}
}

func NewRepository(state StateStore, objects ObjectStore, opts ...Option) (*Repository, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: state store is required", ErrContractViolation)
	}
	if objects == nil {
		return nil, fmt.Errorf("%w: object store is required", ErrContractViolation)
	}

	repo := &Repository{
		state:      state,
		objects:    objects,
		now:        time.Now().UTC,
		nextID:     uuid.NewString,
		presignTTL: 15 * time.Minute,
	}

	for _, opt := range opts {
		opt(repo)
	}

	return repo, nil
}

type JobSubmission struct {
	ID                 string
	SubmissionKind     string
	IdempotencyKey     string
	RequestFingerprint string
	CreatedAt          time.Time
}

type Delivery struct {
	Strategy   string
	WebhookURL string
}

type SourceRecord struct {
	ID               string
	SourceKind       string
	DisplayName      string
	OriginalFilename string
	MIMEType         string
	SourceURL        string
	ObjectKey        string
	SizeBytes        int64
	CreatedAt        time.Time
	ObjectBody       []byte
}

type SourceSetItem struct {
	Position int
	SourceID string
}

type SourceSetRecord struct {
	ID          string
	InputKind   string
	DisplayName string
	Items       []SourceSetItem
	CreatedAt   time.Time
}

type JobRecord struct {
	ID                string
	SubmissionID      string
	RootJobID         string
	ParentJobID       string
	RetryOfJobID      string
	SourceSetID       string
	JobType           string
	Status            string
	Delivery          Delivery
	Version           int64
	ProgressStage     string
	ProgressMessage   string
	ParamsJSON        []byte
	ClientRef         string
	ErrorCode         string
	ErrorMessage      string
	CancelRequestedAt *time.Time
	CreatedAt         time.Time
	StartedAt         *time.Time
	FinishedAt        *time.Time
}

type ArtifactRecord struct {
	ID           string
	JobID        string
	ArtifactKind string
	Filename     string
	Format       string
	MIMEType     string
	ObjectKey    string
	SizeBytes    int64
	CreatedAt    time.Time
	Body         []byte
}

type JobEvent struct {
	ID        string
	JobID     string
	RootJobID string
	EventType string
	Version   int64
	Payload   []byte
	CreatedAt time.Time
}

type WebhookDelivery struct {
	ID             string
	JobEventID     string
	JobID          string
	TargetURL      string
	Payload        []byte
	State          string
	AttemptCount   int
	NextAttemptAt  time.Time
	LastAttemptAt  *time.Time
	DeliveredAt    *time.Time
	LastHTTPStatus *int
	LastError      string
	CreatedAt      time.Time
}

type JobExecutionRecord struct {
	ExecutionID string
	JobID       string
	WorkerKind  string
	TaskType    string
	ClaimedAt   time.Time
	FinishedAt  *time.Time
	Outcome     string
}

type ClaimJobExecutionRequest struct {
	JobID       string
	WorkerKind  string
	TaskType    string
	ClaimedAt   time.Time
	ExecutionID string
}

type ClaimJobExecutionResult struct {
	Job       JobRecord
	Execution *JobExecutionRecord
	Claimed   bool
}

type FinishJobExecutionRequest struct {
	JobID       string
	ExecutionID string
	Outcome     string
	FinishedAt  time.Time
}

type PersistedJobBundle struct {
	Submission      *JobSubmission
	Sources         []SourceRecord
	SourceSet       SourceSetRecord
	Job             JobRecord
	Event           JobEvent
	WebhookDelivery *WebhookDelivery
}

type PersistJobRequest struct {
	Submission      *JobSubmission
	Sources         []SourceRecord
	SourceSet       SourceSetRecord
	Job             JobRecord
	Event           JobEvent
	Artifacts       []ArtifactRecord
	WebhookDelivery *WebhookDelivery
}

type PersistJobResult struct {
	Job       JobRecord
	Sources   []SourceRecord
	Artifacts []ArtifactRecord
}

type DownloadDescriptor struct {
	Provider  string
	URL       string
	ExpiresAt time.Time
}

type ArtifactResolution struct {
	ArtifactID   string
	JobID        string
	ArtifactKind string
	Filename     string
	MIMEType     string
	SizeBytes    int64
	CreatedAt    time.Time
	Download     DownloadDescriptor
}

type JobListFilter struct {
	Status    string
	JobType   string
	RootJobID string
	Limit     int
	Offset    int
}

type JobListPage struct {
	Jobs    []JobRecord
	HasMore bool
}

type OrderedSource struct {
	Position int
	Source   SourceRecord
}

type runtimeJobLister interface {
	ListJobs(ctx context.Context, filter JobListFilter) (JobListPage, error)
}

type runtimeSourceSetGetter interface {
	GetSourceSet(ctx context.Context, sourceSetID string) (SourceSetRecord, error)
}

type runtimeOrderedSourceLister interface {
	ListOrderedSources(ctx context.Context, sourceSetID string) ([]OrderedSource, error)
}

type runtimeArtifactLister interface {
	ListArtifactsByJob(ctx context.Context, jobID string) ([]ArtifactRecord, error)
}

type runtimeChildJobLister interface {
	ListChildJobs(ctx context.Context, parentJobID string) ([]JobRecord, error)
}

type runtimeEventLister interface {
	ListJobEvents(ctx context.Context, jobID string) ([]JobEvent, error)
}

type runtimeArtifactUpserter interface {
	UpsertArtifacts(ctx context.Context, artifacts []ArtifactRecord) error
}

func (r *Repository) PersistSource(ctx context.Context, source SourceRecord) (SourceRecord, error) {
	source = ensureSourceDefaults(source, r.now)
	if err := validateSource(source); err != nil {
		return SourceRecord{}, err
	}

	if requiresSourceObject(source.SourceKind) {
		if err := r.objects.PutObject(ctx, SourcesBucket, source.ObjectKey, source.MIMEType, source.ObjectBody); err != nil {
			return SourceRecord{}, fmt.Errorf("%w: persist source object: %v", ErrStorageUnavailable, err)
		}
	}

	persisted := source.withoutObjectBody()
	if err := r.state.PersistSource(ctx, persisted); err != nil {
		return SourceRecord{}, fmt.Errorf("%w: persist source row: %v", ErrStorageUnavailable, err)
	}

	return persisted, nil
}

func (r *Repository) PersistJob(ctx context.Context, req PersistJobRequest) (PersistJobResult, error) {
	bundle, artifacts, err := r.preparePersistJob(req)
	if err != nil {
		return PersistJobResult{}, err
	}

	for _, source := range bundle.Sources {
		if requiresSourceObject(source.SourceKind) {
			if err := r.objects.PutObject(ctx, SourcesBucket, source.ObjectKey, source.MIMEType, source.ObjectBody); err != nil {
				return PersistJobResult{}, fmt.Errorf("%w: persist source object: %v", ErrStorageUnavailable, err)
			}
		}
	}

	if err := r.state.PersistJobBundle(ctx, bundle.withoutSourceBodies()); err != nil {
		return PersistJobResult{}, fmt.Errorf("%w: persist job bundle: %v", ErrStorageUnavailable, err)
	}

	artifactMetadata := make([]ArtifactRecord, 0, len(artifacts))
	for _, artifact := range artifacts {
		if err := r.objects.PutObject(ctx, ArtifactsBucket, artifact.ObjectKey, artifact.MIMEType, artifact.Body); err != nil {
			return PersistJobResult{}, fmt.Errorf("%w: persist artifact object: %v", ErrStorageUnavailable, err)
		}
		artifactMetadata = append(artifactMetadata, artifact.withoutBody())
	}

	r.logf("%s job_id=%s artifact_count=%d", PersistJobArtifactMarker, bundle.Job.ID, len(artifactMetadata))

	if len(artifactMetadata) > 0 {
		if err := r.state.PersistArtifacts(ctx, artifactMetadata); err != nil {
			return PersistJobResult{}, fmt.Errorf("%w: persist artifact rows: %v", ErrStorageUnavailable, err)
		}
	}

	return PersistJobResult{
		Job:       bundle.Job,
		Sources:   bundle.withoutSourceBodies().Sources,
		Artifacts: artifactMetadata,
	}, nil
}

func (r *Repository) ResolveArtifact(ctx context.Context, artifactID string) (ArtifactResolution, error) {
	if strings.TrimSpace(artifactID) == "" {
		return ArtifactResolution{}, fmt.Errorf("%w: artifact id is required", ErrContractViolation)
	}

	artifact, err := r.state.LookupArtifact(ctx, artifactID)
	if err != nil {
		if errors.Is(err, ErrArtifactNotFound) {
			return ArtifactResolution{}, err
		}
		return ArtifactResolution{}, fmt.Errorf("%w: lookup artifact: %v", ErrStorageUnavailable, err)
	}

	url, expiresAt, err := r.objects.PresignGetObject(ctx, ArtifactsBucket, artifact.ObjectKey, r.presignTTL)
	if err != nil {
		return ArtifactResolution{}, fmt.Errorf("%w: presign artifact: %v", ErrStorageUnavailable, err)
	}

	return ArtifactResolution{
		ArtifactID:   artifact.ID,
		JobID:        artifact.JobID,
		ArtifactKind: artifact.ArtifactKind,
		Filename:     artifact.Filename,
		MIMEType:     artifact.MIMEType,
		SizeBytes:    artifact.SizeBytes,
		CreatedAt:    artifact.CreatedAt,
		Download: DownloadDescriptor{
			Provider:  DownloadProviderMinIO,
			URL:       url,
			ExpiresAt: expiresAt,
		},
	}, nil
}

func (r *Repository) FindSubmission(ctx context.Context, submissionKind, idempotencyKey string) (*JobSubmission, error) {
	if strings.TrimSpace(submissionKind) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("%w: submission kind and idempotency key are required", ErrContractViolation)
	}

	submission, err := r.state.FindSubmission(ctx, submissionKind, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("%w: lookup submission: %v", ErrStorageUnavailable, err)
	}
	return submission, nil
}

func (r *Repository) ListJobsBySubmission(ctx context.Context, submissionID string) ([]JobRecord, error) {
	if strings.TrimSpace(submissionID) == "" {
		return nil, fmt.Errorf("%w: submission id is required", ErrContractViolation)
	}

	jobs, err := r.state.ListJobsBySubmission(ctx, submissionID)
	if err != nil {
		return nil, fmt.Errorf("%w: list submission jobs: %v", ErrStorageUnavailable, err)
	}
	return jobs, nil
}

func (r *Repository) SaveSubmissionGraph(ctx context.Context, submission JobSubmission, sources []SourceRecord, sourceSets []SourceSetRecord, jobs []JobRecord) error {
	submission = *ensureSubmissionDefaults(&submission, r.now)
	if err := validateSubmission(submission); err != nil {
		return err
	}
	if len(sources) == 0 || len(sourceSets) == 0 || len(jobs) == 0 {
		return fmt.Errorf("%w: submission graph requires sources, source sets, and jobs", ErrContractViolation)
	}

	normalizedSources := make([]SourceRecord, 0, len(sources))
	for _, source := range sources {
		source = ensureSourceDefaults(source, r.now)
		if err := validateSource(source); err != nil {
			return err
		}
		normalizedSources = append(normalizedSources, source)
	}

	for _, source := range normalizedSources {
		if requiresSourceObject(source.SourceKind) {
			if err := r.objects.PutObject(ctx, SourcesBucket, source.ObjectKey, source.MIMEType, source.ObjectBody); err != nil {
				return fmt.Errorf("%w: persist source object: %v", ErrStorageUnavailable, err)
			}
		}
	}

	normalizedSourceSets := make([]SourceSetRecord, 0, len(sourceSets))
	for _, sourceSet := range sourceSets {
		sourceSet = ensureSourceSetDefaults(sourceSet, r.now)
		if err := validateSourceSet(sourceSet, normalizedSources); err != nil {
			return err
		}
		normalizedSourceSets = append(normalizedSourceSets, sourceSet)
	}

	normalizedJobs := make([]JobRecord, 0, len(jobs))
	for _, job := range jobs {
		job = ensureJobDefaults(job, r.now)
		var matchedSourceSet *SourceSetRecord
		for idx := range normalizedSourceSets {
			if normalizedSourceSets[idx].ID == job.SourceSetID {
				matchedSourceSet = &normalizedSourceSets[idx]
				break
			}
		}
		if matchedSourceSet == nil {
			return fmt.Errorf("%w: job source_set_id must reference one of the persisted source sets", ErrContractViolation)
		}
		if err := validateJob(job, *matchedSourceSet); err != nil {
			return err
		}
		normalizedJobs = append(normalizedJobs, job)
	}

	if err := r.state.SaveSubmissionGraph(ctx, submission, withoutSourceBodies(normalizedSources), normalizedSourceSets, normalizedJobs); err != nil {
		return fmt.Errorf("%w: save submission graph: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) GetJob(ctx context.Context, jobID string) (JobRecord, error) {
	if strings.TrimSpace(jobID) == "" {
		return JobRecord{}, fmt.Errorf("%w: job id is required", ErrContractViolation)
	}

	job, err := r.state.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrJobNotFound) {
			return JobRecord{}, ErrJobNotFound
		}
		return JobRecord{}, fmt.Errorf("%w: get job: %v", ErrStorageUnavailable, err)
	}
	return job, nil
}

func (r *Repository) CreateJob(ctx context.Context, job JobRecord) error {
	job = ensureJobDefaults(job, r.now)
	if strings.TrimSpace(job.SourceSetID) == "" {
		return fmt.Errorf("%w: create job requires source_set_id", ErrContractViolation)
	}
	if err := r.state.CreateJob(ctx, job); err != nil {
		return fmt.Errorf("%w: create job: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) UpdateJob(ctx context.Context, job JobRecord) error {
	job = ensureJobDefaults(job, r.now)
	if strings.TrimSpace(job.ID) == "" {
		return fmt.Errorf("%w: update job requires id", ErrContractViolation)
	}
	if err := r.state.UpdateJob(ctx, job); err != nil {
		return fmt.Errorf("%w: update job: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) FindCanonicalChild(ctx context.Context, parentJobID, jobType string) (*JobRecord, error) {
	if strings.TrimSpace(parentJobID) == "" || strings.TrimSpace(jobType) == "" {
		return nil, fmt.Errorf("%w: parent job id and child job type are required", ErrContractViolation)
	}

	child, err := r.state.FindCanonicalChild(ctx, parentJobID, jobType)
	if err != nil {
		return nil, fmt.Errorf("%w: find canonical child: %v", ErrStorageUnavailable, err)
	}
	return child, nil
}

func (r *Repository) FindArtifactByKind(ctx context.Context, jobID, artifactKind string) (*ArtifactRecord, error) {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(artifactKind) == "" {
		return nil, fmt.Errorf("%w: job id and artifact kind are required", ErrContractViolation)
	}

	artifact, err := r.state.FindArtifactByKind(ctx, jobID, artifactKind)
	if err != nil {
		return nil, fmt.Errorf("%w: lookup artifact by kind: %v", ErrStorageUnavailable, err)
	}
	return artifact, nil
}

func (r *Repository) AppendJobEvent(ctx context.Context, event JobEvent) error {
	event = ensureEventDefaults(event, JobRecord{ID: event.JobID, RootJobID: event.RootJobID, Version: event.Version}, r.now)
	if strings.TrimSpace(event.ID) == "" || strings.TrimSpace(event.JobID) == "" || strings.TrimSpace(event.RootJobID) == "" {
		return fmt.Errorf("%w: append event requires ids and lineage", ErrContractViolation)
	}
	if err := r.state.AppendJobEvent(ctx, event); err != nil {
		return fmt.Errorf("%w: append job event: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) CreateWebhookDelivery(ctx context.Context, delivery WebhookDelivery) error {
	delivery = *ensureWebhookDefaults(&delivery, JobEvent{ID: delivery.JobEventID, JobID: delivery.JobID}, r.now)
	if strings.TrimSpace(delivery.ID) == "" || strings.TrimSpace(delivery.JobEventID) == "" || strings.TrimSpace(delivery.JobID) == "" || strings.TrimSpace(delivery.TargetURL) == "" {
		return fmt.Errorf("%w: webhook delivery requires ids and target url", ErrContractViolation)
	}
	if err := r.state.CreateWebhookDelivery(ctx, delivery); err != nil {
		return fmt.Errorf("%w: create webhook delivery: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) UpdateWebhookDelivery(ctx context.Context, delivery WebhookDelivery) error {
	delivery = *ensureWebhookDefaults(&delivery, JobEvent{ID: delivery.JobEventID, JobID: delivery.JobID}, r.now)
	if strings.TrimSpace(delivery.ID) == "" {
		return fmt.Errorf("%w: webhook delivery id is required", ErrContractViolation)
	}
	if err := r.state.UpdateWebhookDelivery(ctx, delivery); err != nil {
		return fmt.Errorf("%w: update webhook delivery: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) ListJobs(ctx context.Context, filter JobListFilter) (JobListPage, error) {
	lister, ok := r.state.(runtimeJobLister)
	if !ok {
		return JobListPage{}, fmt.Errorf("%w: list jobs query is not supported by the configured state store", ErrStorageUnavailable)
	}
	if filter.Limit <= 0 {
		return JobListPage{}, fmt.Errorf("%w: list jobs requires a positive limit", ErrContractViolation)
	}
	if filter.Offset < 0 {
		return JobListPage{}, fmt.Errorf("%w: list jobs offset must be non-negative", ErrContractViolation)
	}
	page, err := lister.ListJobs(ctx, filter)
	if err != nil {
		return JobListPage{}, fmt.Errorf("%w: list jobs: %v", ErrStorageUnavailable, err)
	}
	return page, nil
}

func (r *Repository) GetSourceSet(ctx context.Context, sourceSetID string) (SourceSetRecord, error) {
	getter, ok := r.state.(runtimeSourceSetGetter)
	if !ok {
		return SourceSetRecord{}, fmt.Errorf("%w: source set query is not supported by the configured state store", ErrStorageUnavailable)
	}
	if strings.TrimSpace(sourceSetID) == "" {
		return SourceSetRecord{}, fmt.Errorf("%w: source set id is required", ErrContractViolation)
	}
	sourceSet, err := getter.GetSourceSet(ctx, sourceSetID)
	if err != nil {
		return SourceSetRecord{}, fmt.Errorf("%w: get source set: %v", ErrStorageUnavailable, err)
	}
	return sourceSet, nil
}

func (r *Repository) ListOrderedSources(ctx context.Context, sourceSetID string) ([]OrderedSource, error) {
	lister, ok := r.state.(runtimeOrderedSourceLister)
	if !ok {
		return nil, fmt.Errorf("%w: ordered-source query is not supported by the configured state store", ErrStorageUnavailable)
	}
	if strings.TrimSpace(sourceSetID) == "" {
		return nil, fmt.Errorf("%w: source set id is required", ErrContractViolation)
	}
	ordered, err := lister.ListOrderedSources(ctx, sourceSetID)
	if err != nil {
		return nil, fmt.Errorf("%w: list ordered sources: %v", ErrStorageUnavailable, err)
	}
	return ordered, nil
}

func (r *Repository) ListArtifactsByJob(ctx context.Context, jobID string) ([]ArtifactRecord, error) {
	lister, ok := r.state.(runtimeArtifactLister)
	if !ok {
		return nil, fmt.Errorf("%w: artifact listing is not supported by the configured state store", ErrStorageUnavailable)
	}
	if strings.TrimSpace(jobID) == "" {
		return nil, fmt.Errorf("%w: job id is required", ErrContractViolation)
	}
	artifacts, err := lister.ListArtifactsByJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("%w: list job artifacts: %v", ErrStorageUnavailable, err)
	}
	return artifacts, nil
}

func (r *Repository) ListChildJobs(ctx context.Context, parentJobID string) ([]JobRecord, error) {
	lister, ok := r.state.(runtimeChildJobLister)
	if !ok {
		return nil, fmt.Errorf("%w: child-job listing is not supported by the configured state store", ErrStorageUnavailable)
	}
	if strings.TrimSpace(parentJobID) == "" {
		return nil, fmt.Errorf("%w: parent job id is required", ErrContractViolation)
	}
	children, err := lister.ListChildJobs(ctx, parentJobID)
	if err != nil {
		return nil, fmt.Errorf("%w: list child jobs: %v", ErrStorageUnavailable, err)
	}
	return children, nil
}

func (r *Repository) ListJobEvents(ctx context.Context, jobID string) ([]JobEvent, error) {
	lister, ok := r.state.(runtimeEventLister)
	if !ok {
		return nil, fmt.Errorf("%w: job-event listing is not supported by the configured state store", ErrStorageUnavailable)
	}
	if strings.TrimSpace(jobID) == "" {
		return nil, fmt.Errorf("%w: job id is required", ErrContractViolation)
	}
	events, err := lister.ListJobEvents(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("%w: list job events: %v", ErrStorageUnavailable, err)
	}
	return events, nil
}

func (r *Repository) ClaimJobExecution(ctx context.Context, req ClaimJobExecutionRequest) (ClaimJobExecutionResult, error) {
	req = ensureClaimJobExecutionDefaults(req, r.now, r.nextID)
	if err := validateClaimJobExecutionRequest(req); err != nil {
		return ClaimJobExecutionResult{}, err
	}

	result, err := r.state.ClaimJobExecution(ctx, req)
	if err != nil {
		return ClaimJobExecutionResult{}, fmt.Errorf("%w: claim job execution: %v", ErrStorageUnavailable, err)
	}
	if result.Claimed && result.Execution == nil {
		return ClaimJobExecutionResult{}, fmt.Errorf("%w: claimed execution result requires execution metadata", ErrContractViolation)
	}
	return result, nil
}

func (r *Repository) GetActiveJobExecution(ctx context.Context, jobID, executionID string) (JobExecutionRecord, error) {
	if err := validateActiveJobExecutionLookup(jobID, executionID); err != nil {
		return JobExecutionRecord{}, err
	}

	execution, err := r.state.GetActiveJobExecution(ctx, jobID, executionID)
	if err != nil {
		if errors.Is(err, ErrExecutionNotFound) {
			return JobExecutionRecord{}, err
		}
		return JobExecutionRecord{}, fmt.Errorf("%w: get active job execution: %v", ErrStorageUnavailable, err)
	}
	return execution, nil
}

func (r *Repository) FinishJobExecution(ctx context.Context, req FinishJobExecutionRequest) (JobExecutionRecord, error) {
	req = ensureFinishJobExecutionDefaults(req, r.now)
	if err := validateFinishJobExecutionRequest(req); err != nil {
		return JobExecutionRecord{}, err
	}

	execution, err := r.state.FinishJobExecution(ctx, req)
	if err != nil {
		if errors.Is(err, ErrExecutionNotFound) {
			return JobExecutionRecord{}, err
		}
		return JobExecutionRecord{}, fmt.Errorf("%w: finish job execution: %v", ErrStorageUnavailable, err)
	}
	return execution, nil
}

func (r *Repository) UpsertArtifacts(ctx context.Context, artifacts []ArtifactRecord) error {
	upserter, ok := r.state.(runtimeArtifactUpserter)
	if !ok {
		return fmt.Errorf("%w: artifact upsert is not supported by the configured state store", ErrStorageUnavailable)
	}
	if len(artifacts) == 0 {
		return fmt.Errorf("%w: at least one artifact is required", ErrContractViolation)
	}
	for _, artifact := range artifacts {
		if err := validateArtifactMetadata(artifact); err != nil {
			return err
		}
	}
	if err := upserter.UpsertArtifacts(ctx, artifacts); err != nil {
		return fmt.Errorf("%w: upsert artifacts: %v", ErrStorageUnavailable, err)
	}
	return nil
}

func (r *Repository) preparePersistJob(req PersistJobRequest) (PersistedJobBundle, []ArtifactRecord, error) {
	sources := make([]SourceRecord, 0, len(req.Sources))
	for _, source := range req.Sources {
		source = ensureSourceDefaults(source, r.now)
		if err := validateSource(source); err != nil {
			return PersistedJobBundle{}, nil, err
		}
		sources = append(sources, source)
	}
	if len(sources) == 0 {
		return PersistedJobBundle{}, nil, fmt.Errorf("%w: at least one source is required", ErrContractViolation)
	}

	sourceSet := ensureSourceSetDefaults(req.SourceSet, r.now)
	if err := validateSourceSet(sourceSet, sources); err != nil {
		return PersistedJobBundle{}, nil, err
	}

	job := ensureJobDefaults(req.Job, r.now)
	if err := validateJob(job, sourceSet); err != nil {
		return PersistedJobBundle{}, nil, err
	}

	event := ensureEventDefaults(req.Event, job, r.now)
	if err := validateEvent(event, job); err != nil {
		return PersistedJobBundle{}, nil, err
	}

	submission := ensureSubmissionDefaults(req.Submission, r.now)
	if submission != nil {
		if err := validateSubmission(*submission); err != nil {
			return PersistedJobBundle{}, nil, err
		}
		if job.SubmissionID != submission.ID {
			return PersistedJobBundle{}, nil, fmt.Errorf("%w: job submission id must match submission row", ErrContractViolation)
		}
	}

	webhook := ensureWebhookDefaults(req.WebhookDelivery, event, r.now)
	if err := validateWebhookDelivery(webhook, job, event); err != nil {
		return PersistedJobBundle{}, nil, err
	}

	artifacts := make([]ArtifactRecord, 0, len(req.Artifacts))
	for _, artifact := range req.Artifacts {
		artifact = ensureArtifactDefaults(artifact, job, r.now)
		if err := validateArtifact(artifact); err != nil {
			return PersistedJobBundle{}, nil, err
		}
		artifacts = append(artifacts, artifact)
	}

	return PersistedJobBundle{
		Submission:      submission,
		Sources:         sources,
		SourceSet:       sourceSet,
		Job:             job,
		Event:           event,
		WebhookDelivery: webhook,
	}, artifacts, nil
}

func ensureSubmissionDefaults(submission *JobSubmission, now func() time.Time) *JobSubmission {
	if submission == nil {
		return nil
	}
	copied := *submission
	if copied.CreatedAt.IsZero() {
		copied.CreatedAt = now()
	}
	return &copied
}

func ensureSourceDefaults(source SourceRecord, now func() time.Time) SourceRecord {
	if source.CreatedAt.IsZero() {
		source.CreatedAt = now()
	}
	if source.ObjectBody != nil && source.SizeBytes == 0 {
		source.SizeBytes = int64(len(source.ObjectBody))
	}
	return source
}

func ensureSourceSetDefaults(sourceSet SourceSetRecord, now func() time.Time) SourceSetRecord {
	if sourceSet.CreatedAt.IsZero() {
		sourceSet.CreatedAt = now()
	}
	return sourceSet
}

func ensureJobDefaults(job JobRecord, now func() time.Time) JobRecord {
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now()
	}
	if job.Version == 0 {
		job.Version = 1
	}
	if strings.TrimSpace(job.ProgressStage) == "" {
		job.ProgressStage = "queued"
	}
	if job.ParamsJSON == nil {
		job.ParamsJSON = []byte("{}")
	}
	return job
}

func ensureClaimJobExecutionDefaults(req ClaimJobExecutionRequest, now func() time.Time, nextID func() string) ClaimJobExecutionRequest {
	if req.ClaimedAt.IsZero() {
		req.ClaimedAt = now()
	}
	if strings.TrimSpace(req.ExecutionID) == "" && nextID != nil {
		req.ExecutionID = nextID()
	}
	return req
}

func ensureFinishJobExecutionDefaults(req FinishJobExecutionRequest, now func() time.Time) FinishJobExecutionRequest {
	if req.FinishedAt.IsZero() {
		req.FinishedAt = now()
	}
	return req
}

func ensureEventDefaults(event JobEvent, job JobRecord, now func() time.Time) JobEvent {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now()
	}
	if event.JobID == "" {
		event.JobID = job.ID
	}
	if event.RootJobID == "" {
		event.RootJobID = job.RootJobID
	}
	if event.Version == 0 {
		event.Version = job.Version
	}
	return event
}

func ensureWebhookDefaults(delivery *WebhookDelivery, event JobEvent, now func() time.Time) *WebhookDelivery {
	if delivery == nil {
		return nil
	}
	copied := *delivery
	if copied.State == "" {
		copied.State = WebhookStatePending
	}
	if copied.NextAttemptAt.IsZero() {
		copied.NextAttemptAt = now()
	}
	if copied.CreatedAt.IsZero() {
		copied.CreatedAt = now()
	}
	if copied.JobEventID == "" {
		copied.JobEventID = event.ID
	}
	if copied.JobID == "" {
		copied.JobID = event.JobID
	}
	return &copied
}

func ensureArtifactDefaults(artifact ArtifactRecord, job JobRecord, now func() time.Time) ArtifactRecord {
	if artifact.JobID == "" {
		artifact.JobID = job.ID
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = now()
	}
	if artifact.Body != nil && artifact.SizeBytes == 0 {
		artifact.SizeBytes = int64(len(artifact.Body))
	}
	return artifact
}

func validateSubmission(submission JobSubmission) error {
	if strings.TrimSpace(submission.ID) == "" || strings.TrimSpace(submission.SubmissionKind) == "" || strings.TrimSpace(submission.IdempotencyKey) == "" || strings.TrimSpace(submission.RequestFingerprint) == "" {
		return fmt.Errorf("%w: submission requires id, kind, idempotency key, and fingerprint", ErrContractViolation)
	}
	return nil
}

func validateSource(source SourceRecord) error {
	if strings.TrimSpace(source.ID) == "" || strings.TrimSpace(source.SourceKind) == "" || strings.TrimSpace(source.DisplayName) == "" {
		return fmt.Errorf("%w: source requires id, kind, and display name", ErrContractViolation)
	}

	switch source.SourceKind {
	case SourceKindYouTubeURL:
		if strings.TrimSpace(source.SourceURL) == "" {
			return fmt.Errorf("%w: youtube_url sources require source_url", ErrContractViolation)
		}
		if strings.TrimSpace(source.ObjectKey) != "" {
			return fmt.Errorf("%w: youtube_url sources must not carry object_key", ErrContractViolation)
		}
	case SourceKindUploadedFile, SourceKindTelegramUpload:
		if strings.TrimSpace(source.ObjectKey) == "" {
			return fmt.Errorf("%w: uploaded and telegram sources require object_key", ErrContractViolation)
		}
		if source.ObjectBody == nil {
			return fmt.Errorf("%w: object-backed sources require object payload", ErrContractViolation)
		}
	default:
		return fmt.Errorf("%w: unsupported source kind %q", ErrContractViolation, source.SourceKind)
	}

	return nil
}

func validateSourceSet(sourceSet SourceSetRecord, sources []SourceRecord) error {
	if strings.TrimSpace(sourceSet.ID) == "" || strings.TrimSpace(sourceSet.InputKind) == "" {
		return fmt.Errorf("%w: source set requires id and input kind", ErrContractViolation)
	}
	if len(sourceSet.Items) == 0 {
		return fmt.Errorf("%w: source set requires at least one item", ErrContractViolation)
	}

	sourceIDs := map[string]struct{}{}
	for _, source := range sources {
		sourceIDs[source.ID] = struct{}{}
	}

	positions := map[int]struct{}{}
	seenSourceIDs := map[string]struct{}{}
	for _, item := range sourceSet.Items {
		if item.Position < 0 {
			return fmt.Errorf("%w: source set position must be non-negative", ErrContractViolation)
		}
		if _, ok := positions[item.Position]; ok {
			return fmt.Errorf("%w: source set positions must be unique", ErrContractViolation)
		}
		positions[item.Position] = struct{}{}
		if _, ok := seenSourceIDs[item.SourceID]; ok {
			return fmt.Errorf("%w: source ids must be unique within one source set", ErrContractViolation)
		}
		seenSourceIDs[item.SourceID] = struct{}{}
		if _, ok := sourceIDs[item.SourceID]; !ok {
			return fmt.Errorf("%w: source set references unknown source %q", ErrContractViolation, item.SourceID)
		}
	}

	switch sourceSet.InputKind {
	case SourceSetInputSingleSource:
		if len(sourceSet.Items) != 1 {
			return fmt.Errorf("%w: single_source source set must contain exactly one item", ErrContractViolation)
		}
	case SourceSetInputCombinedUpload, SourceSetInputCombinedTG:
		if len(sourceSet.Items) < 2 {
			return fmt.Errorf("%w: combined source sets require at least two items", ErrContractViolation)
		}
	default:
		return fmt.Errorf("%w: unsupported source set input kind %q", ErrContractViolation, sourceSet.InputKind)
	}

	for expected := 0; expected < len(sourceSet.Items); expected++ {
		if _, ok := positions[expected]; !ok {
			return fmt.Errorf("%w: source set positions must be contiguous from zero", ErrContractViolation)
		}
	}

	return nil
}

func validateJob(job JobRecord, sourceSet SourceSetRecord) error {
	if strings.TrimSpace(job.ID) == "" || strings.TrimSpace(job.RootJobID) == "" || strings.TrimSpace(job.SourceSetID) == "" || strings.TrimSpace(job.JobType) == "" || strings.TrimSpace(job.Status) == "" {
		return fmt.Errorf("%w: job requires id, root job id, source set id, type, and status", ErrContractViolation)
	}
	if job.SourceSetID != sourceSet.ID {
		return fmt.Errorf("%w: job source_set_id must match source set", ErrContractViolation)
	}
	if job.Version < 1 {
		return fmt.Errorf("%w: job version must be >= 1", ErrContractViolation)
	}
	switch job.Delivery.Strategy {
	case DeliveryStrategyPolling:
		if strings.TrimSpace(job.Delivery.WebhookURL) != "" {
			return fmt.Errorf("%w: polling delivery must not include webhook url", ErrContractViolation)
		}
	case DeliveryStrategyWebhook:
		if strings.TrimSpace(job.Delivery.WebhookURL) == "" {
			return fmt.Errorf("%w: webhook delivery requires webhook url", ErrContractViolation)
		}
	default:
		return fmt.Errorf("%w: unsupported delivery strategy %q", ErrContractViolation, job.Delivery.Strategy)
	}
	if job.Status == "cancel_requested" && job.CancelRequestedAt == nil {
		return fmt.Errorf("%w: cancel_requested jobs require cancel_requested_at", ErrContractViolation)
	}
	return nil
}

func validateEvent(event JobEvent, job JobRecord) error {
	if strings.TrimSpace(event.ID) == "" || strings.TrimSpace(event.JobID) == "" || strings.TrimSpace(event.RootJobID) == "" || strings.TrimSpace(event.EventType) == "" {
		return fmt.Errorf("%w: event requires id, job id, root job id, and type", ErrContractViolation)
	}
	if event.JobID != job.ID || event.RootJobID != job.RootJobID {
		return fmt.Errorf("%w: event lineage must match job lineage", ErrContractViolation)
	}
	if event.Version != job.Version {
		return fmt.Errorf("%w: event version must match job version", ErrContractViolation)
	}
	if len(event.Payload) == 0 {
		return fmt.Errorf("%w: event payload is required", ErrContractViolation)
	}
	return nil
}

func validateClaimJobExecutionRequest(req ClaimJobExecutionRequest) error {
	if strings.TrimSpace(req.JobID) == "" || strings.TrimSpace(req.WorkerKind) == "" || strings.TrimSpace(req.TaskType) == "" || strings.TrimSpace(req.ExecutionID) == "" {
		return fmt.Errorf("%w: claim execution requires job id, worker kind, task type, and execution id", ErrContractViolation)
	}
	if req.ClaimedAt.IsZero() {
		return fmt.Errorf("%w: claim execution requires claimed_at", ErrContractViolation)
	}
	return nil
}

func validateActiveJobExecutionLookup(jobID, executionID string) error {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(executionID) == "" {
		return fmt.Errorf("%w: active execution lookup requires job id and execution id", ErrContractViolation)
	}
	return nil
}

func validateFinishJobExecutionRequest(req FinishJobExecutionRequest) error {
	if err := validateActiveJobExecutionLookup(req.JobID, req.ExecutionID); err != nil {
		return err
	}
	if req.FinishedAt.IsZero() {
		return fmt.Errorf("%w: finish execution requires finished_at", ErrContractViolation)
	}
	switch req.Outcome {
	case "succeeded", "failed", "canceled":
		return nil
	default:
		return fmt.Errorf("%w: unsupported execution outcome %q", ErrContractViolation, req.Outcome)
	}
}

func validateWebhookDelivery(delivery *WebhookDelivery, job JobRecord, event JobEvent) error {
	if job.Delivery.Strategy == DeliveryStrategyPolling {
		if delivery != nil {
			return fmt.Errorf("%w: polling jobs must not create webhook delivery rows", ErrContractViolation)
		}
		return nil
	}
	if delivery == nil {
		return fmt.Errorf("%w: webhook jobs require webhook delivery state", ErrContractViolation)
	}
	if strings.TrimSpace(delivery.ID) == "" || strings.TrimSpace(delivery.JobEventID) == "" || strings.TrimSpace(delivery.JobID) == "" || strings.TrimSpace(delivery.TargetURL) == "" {
		return fmt.Errorf("%w: webhook delivery requires ids and target url", ErrContractViolation)
	}
	if delivery.JobEventID != event.ID || delivery.JobID != job.ID || delivery.TargetURL != job.Delivery.WebhookURL {
		return fmt.Errorf("%w: webhook delivery must reference the persisted event and webhook target", ErrContractViolation)
	}
	switch delivery.State {
	case WebhookStatePending, WebhookStateDelivered, WebhookStateDead:
	default:
		return fmt.Errorf("%w: unsupported webhook delivery state %q", ErrContractViolation, delivery.State)
	}
	return nil
}

func validateArtifact(artifact ArtifactRecord) error {
	if strings.TrimSpace(artifact.ID) == "" || strings.TrimSpace(artifact.JobID) == "" || strings.TrimSpace(artifact.ArtifactKind) == "" || strings.TrimSpace(artifact.Filename) == "" || strings.TrimSpace(artifact.MIMEType) == "" || strings.TrimSpace(artifact.ObjectKey) == "" {
		return fmt.Errorf("%w: artifact requires ids, kind, filename, mime type, and object key", ErrContractViolation)
	}
	if artifact.Body == nil {
		return fmt.Errorf("%w: artifact body is required before metadata can be persisted", ErrContractViolation)
	}
	return nil
}

func validateArtifactMetadata(artifact ArtifactRecord) error {
	if strings.TrimSpace(artifact.ID) == "" || strings.TrimSpace(artifact.JobID) == "" || strings.TrimSpace(artifact.ArtifactKind) == "" || strings.TrimSpace(artifact.Filename) == "" || strings.TrimSpace(artifact.MIMEType) == "" || strings.TrimSpace(artifact.ObjectKey) == "" {
		return fmt.Errorf("%w: artifact metadata requires ids, kind, filename, mime type, and object key", ErrContractViolation)
	}
	return nil
}

func requiresSourceObject(sourceKind string) bool {
	return sourceKind == SourceKindUploadedFile || sourceKind == SourceKindTelegramUpload
}

func (bundle PersistedJobBundle) withoutSourceBodies() PersistedJobBundle {
	copied := bundle
	copied.Sources = make([]SourceRecord, 0, len(bundle.Sources))
	for _, source := range bundle.Sources {
		copied.Sources = append(copied.Sources, source.withoutObjectBody())
	}
	return copied
}

func (source SourceRecord) withoutObjectBody() SourceRecord {
	source.ObjectBody = nil
	return source
}

func withoutSourceBodies(sources []SourceRecord) []SourceRecord {
	stripped := make([]SourceRecord, 0, len(sources))
	for _, source := range sources {
		stripped = append(stripped, source.withoutObjectBody())
	}
	return stripped
}

func (artifact ArtifactRecord) withoutBody() ArtifactRecord {
	artifact.Body = nil
	return artifact
}

func (r *Repository) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
