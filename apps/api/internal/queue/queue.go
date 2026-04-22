package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
)

const (
	EnqueueMarker          = "[ApiQueue][enqueue][BLOCK_ENQUEUE_JOB]"
	JobTypeTranscription   = "transcription"
	JobTypeReport          = "report"
	JobTypeDeepResearch    = "deep_research"
	QueueNameTranscription = "transcription"
	QueueNameReport        = "report"
	QueueNameDeepResearch  = "deep_research"
	TaskTypeTranscription  = "job:transcription.run"
	TaskTypeReport         = "job:report.run"
	TaskTypeDeepResearch   = "job:deep_research.run"
)

const (
	FailureCodeJobTerminal           = "job_terminal"
	FailureCodeJobAlreadyOwned       = "job_already_owned"
	FailureCodeCancelAuthoritative   = "cancel_authoritative"
	FailureCodeMissingParentArtifact = "missing_parent_artifact"
	FailureCodeInvalidRequestData    = "invalid_request_data"
	FailureCodePipelineTerminal      = "pipeline_terminal_failure"
)

var (
	ErrQueueUnavailable  = errors.New("queue_unavailable")
	ErrContractViolation = errors.New("queue_contract_violation")
)

var deterministicFailureCodes = map[string]struct{}{
	FailureCodeJobTerminal:           {},
	FailureCodeJobAlreadyOwned:       {},
	FailureCodeCancelAuthoritative:   {},
	FailureCodeMissingParentArtifact: {},
	FailureCodeInvalidRequestData:    {},
	FailureCodePipelineTerminal:      {},
}

type Logger interface {
	Printf(format string, args ...any)
}

type Policy struct {
	JobType   string
	QueueName string
	TaskType  string
	MaxRetry  int
	Timeout   time.Duration
}

type Payload struct {
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
}

type EnqueueSpec struct {
	QueueName string
	TaskType  string
	Payload   []byte
	MaxRetry  int
	Timeout   time.Duration
}

type EnqueueReceipt struct {
	ID        string
	QueueName string
	TaskType  string
}

type Client interface {
	Enqueue(ctx context.Context, spec EnqueueSpec) (EnqueueReceipt, error)
}

type AsynqClientAdapter struct {
	client *asynq.Client
}

func NewAsynqClientAdapter(redisOpt asynq.RedisClientOpt) *AsynqClientAdapter {
	return &AsynqClientAdapter{client: asynq.NewClient(redisOpt)}
}

func (a *AsynqClientAdapter) Enqueue(ctx context.Context, spec EnqueueSpec) (EnqueueReceipt, error) {
	task := asynq.NewTask(spec.TaskType, spec.Payload)
	info, err := a.client.EnqueueContext(
		ctx,
		task,
		asynq.Queue(spec.QueueName),
		asynq.MaxRetry(spec.MaxRetry),
		asynq.Timeout(spec.Timeout),
	)
	if err != nil {
		return EnqueueReceipt{}, err
	}
	return EnqueueReceipt{
		ID:        info.ID,
		QueueName: info.Queue,
		TaskType:  info.Type,
	}, nil
}

func (a *AsynqClientAdapter) Close() error {
	return a.client.Close()
}

type Publisher struct {
	client Client
	logger Logger
}

type Option func(*Publisher)

func WithLogger(logger Logger) Option {
	return func(p *Publisher) {
		p.logger = logger
	}
}

func NewPublisher(client Client, opts ...Option) (*Publisher, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: queue client is required", ErrContractViolation)
	}

	publisher := &Publisher{client: client}
	for _, opt := range opts {
		opt(publisher)
	}

	return publisher, nil
}

type EnqueueRequest struct {
	JobID   string
	JobType string
	Attempt int
}

type EnqueueResult struct {
	Receipt EnqueueReceipt
	Policy  Policy
	Payload []byte
}

func KnownPolicies() []Policy {
	return []Policy{
		policyByJobType[JobTypeTranscription],
		policyByJobType[JobTypeReport],
		policyByJobType[JobTypeDeepResearch],
	}
}

func PolicyForJobType(jobType string) (Policy, error) {
	policy, ok := policyByJobType[jobType]
	if !ok {
		return Policy{}, fmt.Errorf("%w: unsupported job type %q", ErrContractViolation, jobType)
	}
	return policy, nil
}

func (p *Publisher) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if strings.TrimSpace(req.JobID) == "" {
		return EnqueueResult{}, fmt.Errorf("%w: job_id is required", ErrContractViolation)
	}
	if req.Attempt < 1 {
		return EnqueueResult{}, fmt.Errorf("%w: attempt must be >= 1", ErrContractViolation)
	}

	policy, err := PolicyForJobType(req.JobType)
	if err != nil {
		return EnqueueResult{}, err
	}

	payload, err := json.Marshal(Payload{
		JobID:   req.JobID,
		Attempt: req.Attempt,
	})
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("%w: encode payload: %v", ErrQueueUnavailable, err)
	}

	spec := EnqueueSpec{
		QueueName: policy.QueueName,
		TaskType:  policy.TaskType,
		Payload:   payload,
		MaxRetry:  policy.MaxRetry,
		Timeout:   policy.Timeout,
	}

	p.logf("%s job_id=%s queue=%s task_type=%s", EnqueueMarker, req.JobID, policy.QueueName, policy.TaskType)

	receipt, err := p.client.Enqueue(ctx, spec)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("%w: enqueue task: %v", ErrQueueUnavailable, err)
	}

	return EnqueueResult{
		Receipt: receipt,
		Policy:  policy,
		Payload: payload,
	}, nil
}

func classifyFailure(code string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := deterministicFailureCodes[code]; ok {
		return fmt.Errorf("%w: %s: %v", asynq.SkipRetry, code, err)
	}
	return err
}

func (p *Publisher) logf(format string, args ...any) {
	if p.logger != nil {
		p.logger.Printf(format, args...)
	}
}

var policyByJobType = map[string]Policy{
	JobTypeTranscription: {
		JobType:   JobTypeTranscription,
		QueueName: QueueNameTranscription,
		TaskType:  TaskTypeTranscription,
		MaxRetry:  3,
		Timeout:   2 * time.Hour,
	},
	JobTypeReport: {
		JobType:   JobTypeReport,
		QueueName: QueueNameReport,
		TaskType:  TaskTypeReport,
		MaxRetry:  2,
		Timeout:   45 * time.Minute,
	},
	JobTypeDeepResearch: {
		JobType:   JobTypeDeepResearch,
		QueueName: QueueNameDeepResearch,
		TaskType:  TaskTypeDeepResearch,
		MaxRetry:  2,
		Timeout:   2 * time.Hour,
	},
}
