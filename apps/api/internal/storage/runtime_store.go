package storage

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	neturl "net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type SQLStateStore struct {
	db *sql.DB
}

type MinioObjectStore struct {
	client        *minio.Client
	presignClient *minio.Client
}

func NewSQLStateStore(db *sql.DB) (*SQLStateStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: sql db is required", ErrContractViolation)
	}
	return &SQLStateStore{db: db}, nil
}

func OpenPostgresDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: postgres dsn is required", ErrContractViolation)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: open postgres db: %v", ErrStorageUnavailable, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: ping postgres db: %v", ErrStorageUnavailable, err)
	}
	return db, nil
}

func NewMinioClient(endpoint, accessKey, secretKey string) (*minio.Client, error) {
	parsedEndpoint, err := neturl.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return nil, fmt.Errorf("%w: parse minio endpoint: %v", ErrContractViolation, err)
	}
	if parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https" {
		return nil, fmt.Errorf("%w: minio endpoint must use http or https", ErrContractViolation)
	}
	if parsedEndpoint.Host == "" {
		return nil, fmt.Errorf("%w: minio endpoint host is required", ErrContractViolation)
	}
	if strings.TrimSpace(accessKey) == "" || strings.TrimSpace(secretKey) == "" {
		return nil, fmt.Errorf("%w: minio credentials are required", ErrContractViolation)
	}

	client, err := minio.New(parsedEndpoint.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Region: "us-east-1",
		Secure: parsedEndpoint.Scheme == "https",
	})
	if err != nil {
		return nil, fmt.Errorf("%w: create minio client: %v", ErrStorageUnavailable, err)
	}
	return client, nil
}

func NewMinioObjectStore(client *minio.Client) (*MinioObjectStore, error) {
	return NewMinioObjectStoreWithPresignClient(client, client)
}

func NewMinioObjectStoreWithPresignClient(client *minio.Client, presignClient *minio.Client) (*MinioObjectStore, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: minio client is required", ErrContractViolation)
	}
	if presignClient == nil {
		return nil, fmt.Errorf("%w: minio presign client is required", ErrContractViolation)
	}
	return &MinioObjectStore{client: client, presignClient: presignClient}, nil
}

func (s *SQLStateStore) PersistSource(ctx context.Context, source SourceRecord) error {
	return insertSource(ctx, s.db, source)
}

func (s *SQLStateStore) PersistJobBundle(ctx context.Context, bundle PersistedJobBundle) error {
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		if bundle.Submission != nil {
			if err := insertSubmission(ctx, tx, *bundle.Submission); err != nil {
				return err
			}
		}
		for _, source := range bundle.Sources {
			if err := insertSource(ctx, tx, source); err != nil {
				return err
			}
		}
		if err := insertSourceSet(ctx, tx, bundle.SourceSet); err != nil {
			return err
		}
		if err := insertSourceSetItems(ctx, tx, bundle.SourceSet); err != nil {
			return err
		}
		if err := insertJob(ctx, tx, bundle.Job); err != nil {
			return err
		}
		if err := insertJobEvent(ctx, tx, bundle.Event); err != nil {
			return err
		}
		if bundle.WebhookDelivery != nil {
			if err := insertWebhookDelivery(ctx, tx, *bundle.WebhookDelivery); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStateStore) PersistArtifacts(ctx context.Context, artifacts []ArtifactRecord) error {
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, artifact := range artifacts {
			if err := insertArtifact(ctx, tx, artifact); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStateStore) LookupArtifact(ctx context.Context, artifactID string) (ArtifactRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, job_id, artifact_kind, filename, format, mime_type, object_key, size_bytes, created_at
FROM job_artifacts
WHERE id = $1
`, artifactID)
	artifact, err := scanArtifact(row)
	if err != nil {
		return ArtifactRecord{}, err
	}
	return artifact, nil
}

func (s *SQLStateStore) FindSubmission(ctx context.Context, submissionKind, idempotencyKey string) (*JobSubmission, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, submission_kind, idempotency_key, request_fingerprint, created_at
FROM job_submissions
WHERE submission_kind = $1 AND idempotency_key = $2
`, submissionKind, idempotencyKey)
	submission, err := scanSubmission(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &submission, nil
}

func (s *SQLStateStore) ListJobsBySubmission(ctx context.Context, submissionID string) ([]JobRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
       delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
       error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
FROM jobs
WHERE submission_id = $1
ORDER BY created_at ASC, id ASC
`, submissionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []JobRecord
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *SQLStateStore) SaveSubmissionGraph(ctx context.Context, submission JobSubmission, sources []SourceRecord, sourceSets []SourceSetRecord, jobs []JobRecord) error {
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		if err := insertSubmission(ctx, tx, submission); err != nil {
			return err
		}
		for _, source := range sources {
			if err := insertSource(ctx, tx, source); err != nil {
				return err
			}
		}
		for _, sourceSet := range sourceSets {
			if err := insertSourceSet(ctx, tx, sourceSet); err != nil {
				return err
			}
			if err := insertSourceSetItems(ctx, tx, sourceSet); err != nil {
				return err
			}
		}
		for _, job := range jobs {
			if err := insertJob(ctx, tx, job); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStateStore) GetJob(ctx context.Context, jobID string) (JobRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
       delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
       error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
FROM jobs
WHERE id = $1
`, jobID)
	job, err := scanJob(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return JobRecord{}, ErrJobNotFound
		}
		return JobRecord{}, err
	}
	return job, nil
}

func (s *SQLStateStore) CreateJob(ctx context.Context, job JobRecord) error {
	return insertJob(ctx, s.db, job)
}

func (s *SQLStateStore) UpdateJob(ctx context.Context, job JobRecord) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = $2,
    version = $3,
    progress_stage = $4,
    progress_message = $5,
    params = $6,
    client_ref = $7,
    error_code = $8,
    error_message = $9,
    cancel_requested_at = $10,
    started_at = $11,
    finished_at = $12
WHERE id = $1
`, job.ID, job.Status, job.Version, job.ProgressStage, job.ProgressMessage, job.ParamsJSON, nullString(job.ClientRef), nullString(job.ErrorCode), nullString(job.ErrorMessage), job.CancelRequestedAt, job.StartedAt, job.FinishedAt)
	return err
}

func (s *SQLStateStore) FindCanonicalChild(ctx context.Context, parentJobID, jobType string) (*JobRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
       delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
       error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
FROM jobs
WHERE parent_job_id = $1
  AND job_type = $2
  AND retry_of_job_id IS NULL
  AND status IN ('queued', 'running', 'cancel_requested', 'succeeded')
ORDER BY created_at DESC, id DESC
LIMIT 1
`, parentJobID, jobType)
	job, err := scanJob(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &job, nil
}

func (s *SQLStateStore) FindArtifactByKind(ctx context.Context, jobID, artifactKind string) (*ArtifactRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, job_id, artifact_kind, filename, format, mime_type, object_key, size_bytes, created_at
FROM job_artifacts
WHERE job_id = $1 AND artifact_kind = $2
`, jobID, artifactKind)
	artifact, err := scanArtifact(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &artifact, nil
}

func (s *SQLStateStore) AppendJobEvent(ctx context.Context, event JobEvent) error {
	return insertJobEvent(ctx, s.db, event)
}

func (s *SQLStateStore) CreateWebhookDelivery(ctx context.Context, delivery WebhookDelivery) error {
	return insertWebhookDelivery(ctx, s.db, delivery)
}

func (s *SQLStateStore) UpdateWebhookDelivery(ctx context.Context, delivery WebhookDelivery) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE webhook_deliveries
SET state = $2,
    attempt_count = $3,
    next_attempt_at = $4,
    last_attempt_at = $5,
    delivered_at = $6,
    last_http_status = $7,
    last_error = $8
WHERE id = $1
`, delivery.ID, delivery.State, delivery.AttemptCount, delivery.NextAttemptAt, delivery.LastAttemptAt, delivery.DeliveredAt, delivery.LastHTTPStatus, nullString(delivery.LastError))
	return err
}

func (s *SQLStateStore) ListDueWebhookDeliveries(ctx context.Context, now time.Time, limit int) ([]WebhookDelivery, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, job_event_id, job_id, target_url, payload, state, attempt_count, next_attempt_at,
       last_attempt_at, delivered_at, last_http_status, last_error, created_at
FROM webhook_deliveries
WHERE state = 'pending'
  AND next_attempt_at <= $1
ORDER BY next_attempt_at ASC, created_at ASC, id ASC
LIMIT $2
`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []WebhookDelivery
	for rows.Next() {
		delivery, scanErr := scanWebhookDelivery(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

func (s *SQLStateStore) ClaimJobExecution(ctx context.Context, req ClaimJobExecutionRequest) (ClaimJobExecutionResult, error) {
	var result ClaimJobExecutionResult
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
SELECT id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
       delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
       error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
FROM jobs
WHERE id = $1
FOR UPDATE
`, req.JobID)
		job, err := scanJob(row)
		if err != nil {
			if err == sql.ErrNoRows {
				return ErrJobNotFound
			}
			return err
		}

		_, err = scanJobExecution(tx.QueryRowContext(ctx, `
SELECT id, job_id, worker_kind, task_type, claimed_at, finished_at, outcome
FROM job_executions
WHERE job_id = $1
  AND finished_at IS NULL
ORDER BY claimed_at DESC, id DESC
LIMIT 1
`, req.JobID))
		switch {
		case err == nil:
			result.Job = job
			result.Claimed = false
			return nil
		case err != sql.ErrNoRows:
			return err
		}

		if job.Status != "queued" {
			result.Job = job
			result.Claimed = false
			return nil
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO job_executions (id, job_id, worker_kind, task_type, claimed_at, finished_at, outcome)
VALUES ($1, $2, $3, $4, $5, NULL, NULL)
`, req.ExecutionID, req.JobID, req.WorkerKind, req.TaskType, req.ClaimedAt); err != nil {
			return err
		}

		updatedRow := tx.QueryRowContext(ctx, `
UPDATE jobs
SET status = 'running',
    version = version + 1,
    progress_stage = 'running',
    progress_message = '',
    started_at = COALESCE(started_at, $2)
WHERE id = $1
RETURNING id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
          delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
          error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
`, req.JobID, req.ClaimedAt)
		updatedJob, err := scanJob(updatedRow)
		if err != nil {
			return err
		}

		execution, err := scanJobExecution(tx.QueryRowContext(ctx, `
SELECT id, job_id, worker_kind, task_type, claimed_at, finished_at, outcome
FROM job_executions
WHERE id = $1
`, req.ExecutionID))
		if err != nil {
			return err
		}

		result.Job = updatedJob
		result.Execution = &execution
		result.Claimed = true
		return nil
	})
	if err != nil {
		return ClaimJobExecutionResult{}, err
	}
	return result, nil
}

func (s *SQLStateStore) GetActiveJobExecution(ctx context.Context, jobID, executionID string) (JobExecutionRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, job_id, worker_kind, task_type, claimed_at, finished_at, outcome
FROM job_executions
WHERE job_id = $1
  AND id = $2
  AND finished_at IS NULL
`, jobID, executionID)
	execution, err := scanJobExecution(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return JobExecutionRecord{}, ErrExecutionNotFound
		}
		return JobExecutionRecord{}, err
	}
	return execution, nil
}

func (s *SQLStateStore) ListJobs(ctx context.Context, filter JobListFilter) (JobListPage, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
       delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
       error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
FROM jobs
WHERE ($1 = '' OR status = $1)
  AND ($2 = '' OR job_type = $2)
  AND ($3 = '' OR root_job_id::text = $3)
ORDER BY created_at DESC, id DESC
LIMIT $4 OFFSET $5
`, strings.TrimSpace(filter.Status), strings.TrimSpace(filter.JobType), strings.TrimSpace(filter.RootJobID), filter.Limit+1, filter.Offset)
	if err != nil {
		return JobListPage{}, err
	}
	defer rows.Close()

	page := JobListPage{Jobs: make([]JobRecord, 0, filter.Limit)}
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return JobListPage{}, scanErr
		}
		if len(page.Jobs) == filter.Limit {
			page.HasMore = true
			continue
		}
		page.Jobs = append(page.Jobs, job)
	}
	if err := rows.Err(); err != nil {
		return JobListPage{}, err
	}
	return page, nil
}

func (s *SQLStateStore) GetSourceSet(ctx context.Context, sourceSetID string) (SourceSetRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, input_kind, display_name, created_at
FROM source_sets
WHERE id = $1
`, sourceSetID)
	return scanSourceSet(row)
}

func (s *SQLStateStore) ListOrderedSources(ctx context.Context, sourceSetID string) ([]OrderedSource, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT ssi.position,
       s.id, s.source_kind, s.display_name, s.original_filename, s.mime_type, s.source_url, s.object_key, s.size_bytes, s.created_at
FROM source_set_items ssi
JOIN sources s ON s.id = ssi.source_id
WHERE ssi.source_set_id = $1
ORDER BY ssi.position ASC, s.id ASC
`, sourceSetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ordered := make([]OrderedSource, 0)
	for rows.Next() {
		orderedSource, err := scanOrderedSource(rows)
		if err != nil {
			return nil, err
		}
		ordered = append(ordered, OrderedSource{
			Position: orderedSource.Position,
			Source:   orderedSource.Source,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ordered, nil
}

func (s *SQLStateStore) ListArtifactsByJob(ctx context.Context, jobID string) ([]ArtifactRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, job_id, artifact_kind, filename, format, mime_type, object_key, size_bytes, created_at
FROM job_artifacts
WHERE job_id = $1
ORDER BY created_at ASC, id ASC
`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var artifacts []ArtifactRecord
	for rows.Next() {
		artifact, scanErr := scanArtifact(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *SQLStateStore) ListChildJobs(ctx context.Context, parentJobID string) ([]JobRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
       delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
       error_code, error_message, cancel_requested_at, created_at, started_at, finished_at
FROM jobs
WHERE parent_job_id = $1
ORDER BY created_at ASC, id ASC
`, parentJobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []JobRecord
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		children = append(children, job)
	}
	return children, rows.Err()
}

func (s *SQLStateStore) ListJobEvents(ctx context.Context, jobID string) ([]JobEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, job_id, root_job_id, event_type, version, payload, created_at
FROM job_events
WHERE job_id = $1
ORDER BY created_at ASC, id ASC
`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []JobEvent
	for rows.Next() {
		event, scanErr := scanJobEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *SQLStateStore) UpsertArtifacts(ctx context.Context, artifacts []ArtifactRecord) error {
	return withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, artifact := range artifacts {
			_, err := tx.ExecContext(ctx, `
INSERT INTO job_artifacts (id, job_id, artifact_kind, filename, format, mime_type, object_key, size_bytes, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (job_id, artifact_kind) DO UPDATE
SET filename = EXCLUDED.filename,
    format = EXCLUDED.format,
    mime_type = EXCLUDED.mime_type,
    object_key = EXCLUDED.object_key,
    size_bytes = EXCLUDED.size_bytes,
    created_at = EXCLUDED.created_at
`, artifact.ID, artifact.JobID, artifact.ArtifactKind, artifact.Filename, artifact.Format, artifact.MIMEType, artifact.ObjectKey, nullInt64(artifact.SizeBytes), artifact.CreatedAt)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStateStore) FinishJobExecution(ctx context.Context, req FinishJobExecutionRequest) (JobExecutionRecord, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE job_executions
SET finished_at = $3,
    outcome = $4
WHERE job_id = $1
  AND id = $2
  AND finished_at IS NULL
RETURNING id, job_id, worker_kind, task_type, claimed_at, finished_at, outcome
`, req.JobID, req.ExecutionID, req.FinishedAt, req.Outcome)
	execution, err := scanJobExecution(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return JobExecutionRecord{}, ErrExecutionNotFound
		}
		return JobExecutionRecord{}, err
	}
	return execution, nil
}

func (s *MinioObjectStore) PutObject(ctx context.Context, bucket, objectKey, contentType string, body []byte) error {
	_, err := s.client.PutObject(ctx, bucket, objectKey, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}

func (s *MinioObjectStore) PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	presignedURL, err := s.presignClient.PresignedGetObject(ctx, bucket, objectKey, expiry, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	return presignedURL.String(), time.Now().UTC().Add(expiry), nil
}

func insertSubmission(ctx context.Context, execer sqlExecer, submission JobSubmission) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO job_submissions (id, submission_kind, idempotency_key, request_fingerprint, created_at)
VALUES ($1, $2, $3, $4, $5)
`, submission.ID, submission.SubmissionKind, submission.IdempotencyKey, submission.RequestFingerprint, submission.CreatedAt)
	return err
}

func insertSource(ctx context.Context, execer sqlExecer, source SourceRecord) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO sources (id, source_kind, display_name, original_filename, mime_type, source_url, object_key, size_bytes, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`, source.ID, source.SourceKind, source.DisplayName, nullString(source.OriginalFilename), nullString(source.MIMEType), nullString(source.SourceURL), nullString(source.ObjectKey), nullInt64(source.SizeBytes), source.CreatedAt)
	return err
}

func insertSourceSet(ctx context.Context, execer sqlExecer, sourceSet SourceSetRecord) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO source_sets (id, input_kind, display_name, created_at)
VALUES ($1, $2, $3, $4)
`, sourceSet.ID, sourceSet.InputKind, nullString(sourceSet.DisplayName), sourceSet.CreatedAt)
	return err
}

func insertSourceSetItems(ctx context.Context, execer sqlExecer, sourceSet SourceSetRecord) error {
	for _, item := range sourceSet.Items {
		_, err := execer.ExecContext(ctx, `
INSERT INTO source_set_items (id, source_set_id, source_id, position, created_at)
VALUES ($1, $2, $3, $4, $5)
`, uuid.NewString(), sourceSet.ID, item.SourceID, item.Position, sourceSet.CreatedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func insertJob(ctx context.Context, execer sqlExecer, job JobRecord) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO jobs (id, submission_id, root_job_id, parent_job_id, retry_of_job_id, source_set_id, job_type, status,
                  delivery_strategy, webhook_url, version, progress_stage, progress_message, params, client_ref,
                  error_code, error_message, cancel_requested_at, created_at, started_at, finished_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
        $9, $10, $11, $12, $13, $14, $15,
        $16, $17, $18, $19, $20, $21)
`, job.ID, nullString(job.SubmissionID), job.RootJobID, nullString(job.ParentJobID), nullString(job.RetryOfJobID), job.SourceSetID, job.JobType, job.Status,
		job.Delivery.Strategy, nullString(job.Delivery.WebhookURL), job.Version, job.ProgressStage, job.ProgressMessage, job.ParamsJSON, nullString(job.ClientRef),
		nullString(job.ErrorCode), nullString(job.ErrorMessage), job.CancelRequestedAt, job.CreatedAt, job.StartedAt, job.FinishedAt)
	return err
}

func insertArtifact(ctx context.Context, execer sqlExecer, artifact ArtifactRecord) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO job_artifacts (id, job_id, artifact_kind, filename, format, mime_type, object_key, size_bytes, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`, artifact.ID, artifact.JobID, artifact.ArtifactKind, artifact.Filename, artifact.Format, artifact.MIMEType, artifact.ObjectKey, nullInt64(artifact.SizeBytes), artifact.CreatedAt)
	return err
}

func insertJobEvent(ctx context.Context, execer sqlExecer, event JobEvent) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO job_events (id, job_id, root_job_id, event_type, version, payload, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`, event.ID, event.JobID, event.RootJobID, event.EventType, event.Version, event.Payload, event.CreatedAt)
	return err
}

func insertWebhookDelivery(ctx context.Context, execer sqlExecer, delivery WebhookDelivery) error {
	_, err := execer.ExecContext(ctx, `
INSERT INTO webhook_deliveries (id, job_event_id, job_id, target_url, payload, state, attempt_count, next_attempt_at,
                               last_attempt_at, delivered_at, last_http_status, last_error, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
        $9, $10, $11, $12, $13)
`, delivery.ID, delivery.JobEventID, delivery.JobID, delivery.TargetURL, delivery.Payload, delivery.State, delivery.AttemptCount, delivery.NextAttemptAt,
		delivery.LastAttemptAt, delivery.DeliveredAt, delivery.LastHTTPStatus, nullString(delivery.LastError), delivery.CreatedAt)
	return err
}

func scanSubmission(scanner interface {
	Scan(dest ...any) error
}) (JobSubmission, error) {
	var submission JobSubmission
	err := scanner.Scan(&submission.ID, &submission.SubmissionKind, &submission.IdempotencyKey, &submission.RequestFingerprint, &submission.CreatedAt)
	return submission, err
}

func scanSourceSet(scanner interface {
	Scan(dest ...any) error
}) (SourceSetRecord, error) {
	var (
		sourceSet   SourceSetRecord
		displayName sql.NullString
	)
	err := scanner.Scan(&sourceSet.ID, &sourceSet.InputKind, &displayName, &sourceSet.CreatedAt)
	if err != nil {
		return SourceSetRecord{}, err
	}
	sourceSet.DisplayName = nullStringValue(displayName)
	return sourceSet, nil
}

func scanOrderedSource(scanner interface {
	Scan(dest ...any) error
}) (OrderedSource, error) {
	var (
		ordered          OrderedSource
		originalFilename sql.NullString
		mimeType         sql.NullString
		sourceURL        sql.NullString
		objectKey        sql.NullString
		sizeBytes        sql.NullInt64
	)
	err := scanner.Scan(
		&ordered.Position,
		&ordered.Source.ID,
		&ordered.Source.SourceKind,
		&ordered.Source.DisplayName,
		&originalFilename,
		&mimeType,
		&sourceURL,
		&objectKey,
		&sizeBytes,
		&ordered.Source.CreatedAt,
	)
	if err != nil {
		return OrderedSource{}, err
	}
	ordered.Source.OriginalFilename = nullStringValue(originalFilename)
	ordered.Source.MIMEType = nullStringValue(mimeType)
	ordered.Source.SourceURL = nullStringValue(sourceURL)
	ordered.Source.ObjectKey = nullStringValue(objectKey)
	if sizeBytes.Valid {
		ordered.Source.SizeBytes = sizeBytes.Int64
	}
	return ordered, nil
}

func scanArtifact(scanner interface {
	Scan(dest ...any) error
}) (ArtifactRecord, error) {
	var (
		artifact ArtifactRecord
		size     sql.NullInt64
	)
	err := scanner.Scan(&artifact.ID, &artifact.JobID, &artifact.ArtifactKind, &artifact.Filename, &artifact.Format, &artifact.MIMEType, &artifact.ObjectKey, &size, &artifact.CreatedAt)
	if err != nil {
		return ArtifactRecord{}, err
	}
	if size.Valid {
		artifact.SizeBytes = size.Int64
	}
	return artifact, nil
}

func scanJobEvent(scanner interface {
	Scan(dest ...any) error
}) (JobEvent, error) {
	var event JobEvent
	err := scanner.Scan(&event.ID, &event.JobID, &event.RootJobID, &event.EventType, &event.Version, &event.Payload, &event.CreatedAt)
	if err != nil {
		return JobEvent{}, err
	}
	return event, nil
}

func scanWebhookDelivery(scanner interface {
	Scan(dest ...any) error
}) (WebhookDelivery, error) {
	var (
		delivery       WebhookDelivery
		lastAttemptAt  sql.NullTime
		deliveredAt    sql.NullTime
		lastHTTPStatus sql.NullInt64
		lastError      sql.NullString
	)
	err := scanner.Scan(
		&delivery.ID,
		&delivery.JobEventID,
		&delivery.JobID,
		&delivery.TargetURL,
		&delivery.Payload,
		&delivery.State,
		&delivery.AttemptCount,
		&delivery.NextAttemptAt,
		&lastAttemptAt,
		&deliveredAt,
		&lastHTTPStatus,
		&lastError,
		&delivery.CreatedAt,
	)
	if err != nil {
		return WebhookDelivery{}, err
	}
	delivery.LastAttemptAt = nullTimePtr(lastAttemptAt)
	delivery.DeliveredAt = nullTimePtr(deliveredAt)
	if lastHTTPStatus.Valid {
		status := int(lastHTTPStatus.Int64)
		delivery.LastHTTPStatus = &status
	}
	delivery.LastError = nullStringValue(lastError)
	return delivery, nil
}

func scanJobExecution(scanner interface {
	Scan(dest ...any) error
}) (JobExecutionRecord, error) {
	var (
		execution  JobExecutionRecord
		finishedAt sql.NullTime
		outcome    sql.NullString
	)
	err := scanner.Scan(
		&execution.ExecutionID,
		&execution.JobID,
		&execution.WorkerKind,
		&execution.TaskType,
		&execution.ClaimedAt,
		&finishedAt,
		&outcome,
	)
	if err != nil {
		return JobExecutionRecord{}, err
	}
	execution.FinishedAt = nullTimePtr(finishedAt)
	execution.Outcome = nullStringValue(outcome)
	return execution, nil
}

func scanJob(scanner interface {
	Scan(dest ...any) error
}) (JobRecord, error) {
	var (
		job               JobRecord
		submissionID      sql.NullString
		parentJobID       sql.NullString
		retryOfJobID      sql.NullString
		webhookURL        sql.NullString
		clientRef         sql.NullString
		errorCode         sql.NullString
		errorMessage      sql.NullString
		cancelRequestedAt sql.NullTime
		startedAt         sql.NullTime
		finishedAt        sql.NullTime
	)
	err := scanner.Scan(
		&job.ID,
		&submissionID,
		&job.RootJobID,
		&parentJobID,
		&retryOfJobID,
		&job.SourceSetID,
		&job.JobType,
		&job.Status,
		&job.Delivery.Strategy,
		&webhookURL,
		&job.Version,
		&job.ProgressStage,
		&job.ProgressMessage,
		&job.ParamsJSON,
		&clientRef,
		&errorCode,
		&errorMessage,
		&cancelRequestedAt,
		&job.CreatedAt,
		&startedAt,
		&finishedAt,
	)
	if err != nil {
		return JobRecord{}, err
	}
	job.SubmissionID = nullStringValue(submissionID)
	job.ParentJobID = nullStringValue(parentJobID)
	job.RetryOfJobID = nullStringValue(retryOfJobID)
	job.Delivery.WebhookURL = nullStringValue(webhookURL)
	job.ClientRef = nullStringValue(clientRef)
	job.ErrorCode = nullStringValue(errorCode)
	job.ErrorMessage = nullStringValue(errorMessage)
	job.CancelRequestedAt = nullTimePtr(cancelRequestedAt)
	job.StartedAt = nullTimePtr(startedAt)
	job.FinishedAt = nullTimePtr(finishedAt)
	return job, nil
}

func withTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func nullString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: strings.TrimSpace(value) != ""}
}

func nullInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: value != 0}
}

func nullStringValue(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}

func nullTimePtr(value sql.NullTime) *time.Time {
	if value.Valid {
		timestamp := value.Time
		return &timestamp
	}
	return nil
}

var _ StateStore = (*SQLStateStore)(nil)
var _ ObjectStore = (*MinioObjectStore)(nil)
