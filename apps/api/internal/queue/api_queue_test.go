package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

func TestApiQueueEnqueueUsesAnalysisRunPayload(t *testing.T) {
	t.Parallel()

	logger := &queueBufferLogger{}
	client := &fakeClient{}
	publisher, err := NewPublisher(client, WithLogger(logger))
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Enqueue(context.Background(), EnqueueRequest{
		AnalysisRunID: "run-1",
		RunType:       RunTypeAnalysis,
		TaskType:      TaskTypeSelectionTranscription,
		Attempt:       1,
	})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	if result.Policy.QueueName != QueueNameTranscription || result.Policy.TaskType != TaskTypeSelectionTranscription {
		t.Fatalf("policy = %#v, want transcription selection policy", result.Policy)
	}
	var payload Payload
	if err := json.Unmarshal(client.lastSpec.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal(payload) error = %v", err)
	}
	if payload.AnalysisRunID != "run-1" || payload.Attempt != 1 {
		t.Fatalf("payload = %#v, want run id and attempt", payload)
	}
	if strings.Contains(string(client.lastSpec.Payload), "status") || strings.Contains(string(client.lastSpec.Payload), "webhook") {
		t.Fatalf("payload should not contain mutable metadata: %s", string(client.lastSpec.Payload))
	}
	if !strings.Contains(logger.String(), EnqueueMarker) {
		t.Fatalf("logger output missing marker %q", EnqueueMarker)
	}
}

func TestApiQueueSupportsSelectionAnalysisPolicy(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	publisher, err := NewPublisher(client)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Enqueue(context.Background(), EnqueueRequest{
		AnalysisRunID: "run-2",
		RunType:       RunTypeAnalysis,
		TaskType:      TaskTypeSelectionAnalysis,
		Attempt:       2,
	})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if result.Policy.QueueName != QueueNameAnalysis || result.Policy.TaskType != TaskTypeSelectionAnalysis {
		t.Fatalf("policy = %#v, want analysis selection policy", result.Policy)
	}
}

func TestApiQueueClassifiesDeterministicFailuresAsSkipRetry(t *testing.T) {
	t.Parallel()

	for _, code := range []string{
		FailureCodeExecutionTerminal,
		FailureCodeAlreadyOwned,
		FailureCodeInvalidInput,
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
		AnalysisRunID: "",
		TaskType:      TaskTypeSelectionTranscription,
		Attempt:       1,
	})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("Enqueue() error = %v, want ErrContractViolation", err)
	}

	_, err = publisher.Enqueue(context.Background(), EnqueueRequest{
		AnalysisRunID: "run-1",
		TaskType:      "unsupported",
		Attempt:       1,
	})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("unsupported task type should fail with ErrContractViolation, got %v", err)
	}
}

type fakeClient struct {
	lastSpec EnqueueSpec
}

func (f *fakeClient) Enqueue(_ context.Context, spec EnqueueSpec) (EnqueueReceipt, error) {
	f.lastSpec = spec
	return EnqueueReceipt{ID: "task-1", QueueName: spec.QueueName, TaskType: spec.TaskType}, nil
}

type queueBufferLogger struct {
	lines []string
}

func (l *queueBufferLogger) Printf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *queueBufferLogger) String() string {
	return strings.Join(l.lines, "\n")
}

func TestApiQueueKnownPoliciesAreFrozenForFinalExecution(t *testing.T) {
	t.Parallel()

	policies := KnownPolicies()
	if len(policies) != 2 {
		t.Fatalf("KnownPolicies() = %d, want 2", len(policies))
	}
	for _, policy := range policies {
		if policy.RunType != RunTypeAnalysis || policy.Timeout <= time.Minute || policy.MaxRetry < 1 {
			t.Fatalf("policy = %#v, want final run policy", policy)
		}
	}
}
