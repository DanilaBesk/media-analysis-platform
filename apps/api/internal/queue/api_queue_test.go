package queue

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

	"github.com/hibiken/asynq"
)

func TestApiQueueEnqueueUsesMinimalPayloadAndFrozenPolicies(t *testing.T) {
	t.Parallel()

	policies := loadPolicyFixtures(t)
	logger := &queueBufferLogger{}

	for _, policy := range policies {
		policy := policy
		t.Run(policy.JobType, func(t *testing.T) {
			t.Parallel()

			client := &fakeClient{}
			publisher, err := NewPublisher(client, WithLogger(logger))
			if err != nil {
				t.Fatalf("NewPublisher() error = %v", err)
			}

			req := EnqueueRequest{
				JobID:   "job-" + policy.JobType,
				JobType: policy.JobType,
				Attempt: 1,
			}
			if policy.TaskType != TaskTypeTranscription && policy.TaskType != TaskTypeAgentRun {
				req.TaskType = policy.TaskType
			}
			result, err := publisher.Enqueue(context.Background(), req)
			if err != nil {
				t.Fatalf("Enqueue() error = %v", err)
			}

			if result.Policy.QueueName != policy.QueueName || result.Policy.TaskType != policy.TaskType {
				t.Fatalf("policy = %#v, want %#v", result.Policy, policy)
			}
			if result.Policy.MaxRetry != policy.MaxRetry || result.Policy.Timeout != policy.Timeout {
				t.Fatalf("retry/timeout = (%d, %v), want (%d, %v)", result.Policy.MaxRetry, result.Policy.Timeout, policy.MaxRetry, policy.Timeout)
			}

			var payload map[string]any
			if err := json.Unmarshal(client.lastSpec.Payload, &payload); err != nil {
				t.Fatalf("Unmarshal(payload) error = %v", err)
			}
			if len(payload) != 2 {
				t.Fatalf("payload keys = %#v, want only job_id and attempt", payload)
			}
			if _, ok := payload["job_id"]; !ok {
				t.Fatalf("payload missing job_id: %#v", payload)
			}
			if _, ok := payload["attempt"]; !ok {
				t.Fatalf("payload missing attempt: %#v", payload)
			}
			if strings.Contains(string(client.lastSpec.Payload), "status") || strings.Contains(string(client.lastSpec.Payload), "webhook") {
				t.Fatalf("payload should not contain mutable job metadata: %s", string(client.lastSpec.Payload))
			}
			if !strings.Contains(logger.String(), EnqueueMarker) {
				t.Fatalf("logger output missing marker %q", EnqueueMarker)
			}
		})
	}
}

func TestApiQueueClassifiesDeterministicFailuresAsSkipRetry(t *testing.T) {
	t.Parallel()

	for _, code := range []string{
		FailureCodeJobTerminal,
		FailureCodeJobAlreadyOwned,
		FailureCodeCancelAuthoritative,
		FailureCodeMissingParentArtifact,
		FailureCodeInvalidRequestData,
		FailureCodePipelineTerminal,
	} {
		err := classifyFailure(code, errors.New("deterministic"))
		if !errors.Is(err, asynq.SkipRetry) {
			t.Fatalf("classifyFailure(%q) should wrap asynq.SkipRetry, got %v", code, err)
		}
	}

	transient := classifyFailure("redis_timeout", errors.New("temporary"))
	if errors.Is(transient, asynq.SkipRetry) {
		t.Fatalf("transient failure should not be skip-retry: %v", transient)
	}
}

func TestApiQueueRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	publisher, err := NewPublisher(&fakeClient{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	_, err = publisher.Enqueue(context.Background(), EnqueueRequest{
		JobID:   "",
		JobType: JobTypeTranscription,
		Attempt: 1,
	})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("Enqueue() error = %v, want ErrContractViolation", err)
	}

	_, err = publisher.Enqueue(context.Background(), EnqueueRequest{
		JobID:   "job-1",
		JobType: "unsupported",
		Attempt: 1,
	})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("unsupported job type should fail with ErrContractViolation, got %v", err)
	}
}

func TestApiQueueRejectsDedicatedReportAndDeepResearchTaskPolicies(t *testing.T) {
	t.Parallel()

	for _, jobType := range []string{JobTypeReport, JobTypeDeepResearch} {
		_, err := PolicyForJobType(jobType)
		if !errors.Is(err, ErrContractViolation) {
			t.Fatalf("PolicyForJobType(%q) error = %v, want ErrContractViolation", jobType, err)
		}
	}

	for _, policy := range KnownPolicies() {
		if policy.TaskType == "job:report.run" || policy.TaskType == "job:deep_research.run" {
			t.Fatalf("dedicated AI task policy should not be registered: %#v", policy)
		}
	}
}

func TestApiQueueSupportsTranscriptionAggregateTaskPolicy(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	publisher, err := NewPublisher(client)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Enqueue(context.Background(), EnqueueRequest{
		JobID:    "batch-root",
		JobType:  JobTypeTranscription,
		TaskType: TaskTypeTranscriptionAggregate,
		Attempt:  1,
	})
	if err != nil {
		t.Fatalf("Enqueue(aggregate) error = %v", err)
	}
	if result.Policy.QueueName != QueueNameTranscription || result.Policy.TaskType != TaskTypeTranscriptionAggregate {
		t.Fatalf("aggregate policy = %#v, want transcription aggregate task", result.Policy)
	}
	if client.lastSpec.TaskType != TaskTypeTranscriptionAggregate {
		t.Fatalf("task type = %q, want %q", client.lastSpec.TaskType, TaskTypeTranscriptionAggregate)
	}
}

func TestApiQueueSmokeWithRedis(t *testing.T) {
	redisAddr := strings.TrimSpace(os.Getenv("API_QUEUE_REDIS_ADDR"))
	if redisAddr == "" {
		t.Skip("set API_QUEUE_REDIS_ADDR to run real Redis smoke")
	}

	redisOpt := asynq.RedisClientOpt{Addr: redisAddr}
	client := NewAsynqClientAdapter(redisOpt)
	defer func() {
		if err := client.Close(); err != nil {
			t.Fatalf("client.Close() error = %v", err)
		}
	}()

	inspector := asynq.NewInspector(redisOpt)
	defer func() {
		if err := inspector.Close(); err != nil {
			t.Fatalf("inspector.Close() error = %v", err)
		}
	}()

	publisher, err := NewPublisher(client)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	jobID := fmt.Sprintf("job-smoke-%d", time.Now().UnixNano())
	result, err := publisher.Enqueue(context.Background(), EnqueueRequest{
		JobID:   jobID,
		JobType: JobTypeTranscription,
		Attempt: 1,
	})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	tasks, err := inspector.ListPendingTasks(QueueNameTranscription, asynq.PageSize(50))
	if err != nil {
		t.Fatalf("ListPendingTasks() error = %v", err)
	}

	found := false
	for _, task := range tasks {
		if task.ID != result.Receipt.ID {
			continue
		}
		found = true
		if task.Type != TaskTypeTranscription {
			t.Fatalf("task.Type = %q, want %q", task.Type, TaskTypeTranscription)
		}
		var payload Payload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			t.Fatalf("Unmarshal(task.Payload) error = %v", err)
		}
		if payload.JobID != jobID || payload.Attempt != 1 {
			t.Fatalf("payload = %#v, want job_id=%q attempt=1", payload, jobID)
		}
		if task.MaxRetry != 3 || task.Timeout != 2*time.Hour {
			t.Fatalf("task retry/timeout = (%d, %v), want (3, 2h)", task.MaxRetry, task.Timeout)
		}
	}

	if !found {
		t.Fatalf("queued task %q was not visible in Redis inspector output", result.Receipt.ID)
	}
}

func loadPolicyFixtures(t *testing.T) []Policy {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "policy_fixtures.json"))
	if err != nil {
		t.Fatalf("ReadFile(testdata/policy_fixtures.json) error = %v", err)
	}

	var fixtures []struct {
		JobType   string `json:"job_type"`
		QueueName string `json:"queue_name"`
		TaskType  string `json:"task_type"`
		MaxRetry  int    `json:"max_retry"`
		Timeout   string `json:"timeout"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatalf("Unmarshal(policy_fixtures.json) error = %v", err)
	}

	policies := make([]Policy, 0, len(fixtures))
	for _, fixture := range fixtures {
		timeout, err := time.ParseDuration(fixture.Timeout)
		if err != nil {
			t.Fatalf("ParseDuration(%q) error = %v", fixture.Timeout, err)
		}
		policies = append(policies, Policy{
			JobType:   fixture.JobType,
			QueueName: fixture.QueueName,
			TaskType:  fixture.TaskType,
			MaxRetry:  fixture.MaxRetry,
			Timeout:   timeout,
		})
	}
	return policies
}

type fakeClient struct {
	lastSpec EnqueueSpec
}

func (f *fakeClient) Enqueue(_ context.Context, spec EnqueueSpec) (EnqueueReceipt, error) {
	f.lastSpec = spec
	return EnqueueReceipt{
		ID:        "task-1",
		QueueName: spec.QueueName,
		TaskType:  spec.TaskType,
	}, nil
}

type queueBufferLogger struct {
	lines []string
}

func (b *queueBufferLogger) Printf(format string, args ...any) {
	b.lines = append(b.lines, fmt.Sprintf(format, args...))
}

func (b *queueBufferLogger) String() string {
	return strings.Join(b.lines, "\n")
}
