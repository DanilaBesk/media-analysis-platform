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
	EnqueueMarker                  = "[ApiQueue][enqueue][BLOCK_ENQUEUE_ANALYSIS_RUN]"
	RunTypeAnalysis                = "analysis_run"
	QueueNameTranscription         = "transcription"
	QueueNameAnalysis              = "analysis"
	TaskTypeSelectionTranscription = "selection.transcription"
	TaskTypeSelectionAnalysis      = "selection.analysis"
)

const (
	FailureCodeExecutionTerminal = "execution_terminal"
	FailureCodeAlreadyOwned      = "execution_already_owned"
	FailureCodeInvalidInput      = "invalid_execution_input"
)

var (
	ErrQueueUnavailable  = errors.New("queue_unavailable")
	ErrContractViolation = errors.New("queue_contract_violation")
)

var deterministicFailureCodes = map[string]struct{}{
	FailureCodeExecutionTerminal: {},
	FailureCodeAlreadyOwned:      {},
	FailureCodeInvalidInput:      {},
}

type Logger interface {
	Printf(format string, args ...any)
}

type Policy struct {
	RunType   string
	QueueName string
	TaskType  string
	MaxRetry  int
	Timeout   time.Duration
}

type Payload struct {
	AnalysisRunID string `json:"analysis_run_id"`
	Attempt       int    `json:"attempt"`
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
	AnalysisRunID string
	RunType       string
	TaskType      string
	Attempt       int
}

type EnqueueResult struct {
	Receipt EnqueueReceipt
	Policy  Policy
	Payload []byte
}

func KnownPolicies() []Policy {
	return []Policy{
		policyByTaskType[TaskTypeSelectionTranscription],
		policyByTaskType[TaskTypeSelectionAnalysis],
	}
}

func (p *Publisher) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if strings.TrimSpace(req.AnalysisRunID) == "" {
		return EnqueueResult{}, fmt.Errorf("%w: analysis_run_id is required", ErrContractViolation)
	}
	if req.Attempt < 1 {
		return EnqueueResult{}, fmt.Errorf("%w: attempt must be >= 1", ErrContractViolation)
	}
	policy, err := policyForRequest(req)
	if err != nil {
		return EnqueueResult{}, err
	}
	payload, err := json.Marshal(Payload{
		AnalysisRunID: strings.TrimSpace(req.AnalysisRunID),
		Attempt:       req.Attempt,
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
	p.logf("%s analysis_run_id=%s queue=%s task_type=%s", EnqueueMarker, req.AnalysisRunID, policy.QueueName, policy.TaskType)
	receipt, err := p.client.Enqueue(ctx, spec)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("%w: enqueue task: %v", ErrQueueUnavailable, err)
	}
	return EnqueueResult{Receipt: receipt, Policy: policy, Payload: payload}, nil
}

func policyForRequest(req EnqueueRequest) (Policy, error) {
	taskType := strings.TrimSpace(req.TaskType)
	if taskType == "" && strings.TrimSpace(req.RunType) == RunTypeAnalysis {
		taskType = TaskTypeSelectionAnalysis
	}
	if taskType == "" {
		return Policy{}, fmt.Errorf("%w: task_type is required", ErrContractViolation)
	}
	policy, ok := policyByTaskType[taskType]
	if !ok {
		return Policy{}, fmt.Errorf("%w: unsupported task type %q", ErrContractViolation, taskType)
	}
	return policy, nil
}

var policyByTaskType = map[string]Policy{
	TaskTypeSelectionTranscription: {
		RunType:   RunTypeAnalysis,
		QueueName: QueueNameTranscription,
		TaskType:  TaskTypeSelectionTranscription,
		MaxRetry:  3,
		Timeout:   2 * time.Hour,
	},
	TaskTypeSelectionAnalysis: {
		RunType:   RunTypeAnalysis,
		QueueName: QueueNameAnalysis,
		TaskType:  TaskTypeSelectionAnalysis,
		MaxRetry:  2,
		Timeout:   4 * time.Hour,
	},
}

func classifyFailure(code string, err error) error {
	if _, ok := deterministicFailureCodes[strings.TrimSpace(code)]; ok {
		return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
	}
	return err
}

func (p *Publisher) logf(format string, args ...any) {
	if p.logger != nil {
		p.logger.Printf(format, args...)
	}
}
