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

	"github.com/google/uuid"

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

func TestApiJobsCreateTranscriptionJobsWithoutIdempotencyKeyCreatesIndependentSubmissions(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	request := CreateTranscriptionRequest{
		SubmissionKind:     "transcription_upload",
		RequestFingerprint: "upload-manifest-a",
		Sources:            []storage.SourceRecord{source("source-1")},
		Delivery:           storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		ClientRef:          "client-1",
	}

	first, err := service.CreateTranscriptionJobs(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateTranscriptionJobs(first) error = %v", err)
	}
	second, err := service.CreateTranscriptionJobs(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateTranscriptionJobs(second) error = %v", err)
	}
	if first.Reused || second.Reused {
		t.Fatalf("non-idempotent creates must not replay: first=%v second=%v", first.Reused, second.Reused)
	}
	if first.Jobs[0].ID == second.Jobs[0].ID {
		t.Fatalf("non-idempotent creates reused job id %q", first.Jobs[0].ID)
	}
	if got, want := len(store.submissions), 2; got != want {
		t.Fatalf("stored submissions = %d, want %d", got, want)
	}
	for key, submission := range store.submissions {
		if strings.Contains(key, "::<nil>") || strings.HasSuffix(key, "::") {
			t.Fatalf("stored empty idempotency key in submission index: %q", key)
		}
		if !strings.HasPrefix(submission.IdempotencyKey, "non-idempotent:") {
			t.Fatalf("submission idempotency key = %q, want non-idempotent internal key", submission.IdempotencyKey)
		}
	}
	if got, want := len(enqueuer.requests), 2; got != want {
		t.Fatalf("enqueue count = %d, want %d", got, want)
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

func TestApiJobsCreateCombinedTranscriptionJobWithoutIdempotencyKeyCreatesIndependentSubmissions(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	request := CreateCombinedTranscriptionRequest{
		SubmissionKind:     "transcription_upload",
		RequestFingerprint: "combined-manifest",
		Sources: []storage.SourceRecord{
			source("source-a"),
			source("source-b"),
		},
		Delivery:    storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		DisplayName: "combined-job",
	}

	first, err := service.CreateCombinedTranscriptionJob(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateCombinedTranscriptionJob(first) error = %v", err)
	}
	second, err := service.CreateCombinedTranscriptionJob(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateCombinedTranscriptionJob(second) error = %v", err)
	}
	if first.Reused || second.Reused {
		t.Fatalf("non-idempotent combined creates must not replay: first=%v second=%v", first.Reused, second.Reused)
	}
	if first.Job.ID == second.Job.ID {
		t.Fatalf("non-idempotent combined creates reused job id %q", first.Job.ID)
	}
	if got, want := len(store.submissions), 2; got != want {
		t.Fatalf("stored submissions = %d, want %d", got, want)
	}
	for _, submission := range store.submissions {
		if !strings.HasPrefix(submission.IdempotencyKey, "non-idempotent:") {
			t.Fatalf("submission idempotency key = %q, want non-idempotent internal key", submission.IdempotencyKey)
		}
	}
	if got, want := len(enqueuer.requests), 2; got != want {
		t.Fatalf("enqueue count = %d, want %d", got, want)
	}
}

func TestApiJobsCreateBatchTranscriptionGraphAndIdempotency(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	request := CreateBatchTranscriptionRequest{
		SubmissionKind:     "transcription_batch",
		IdempotencyKey:     "batch-idem",
		RequestFingerprint: "batch-fingerprint-a",
		Sources: []storage.SourceRecord{
			source("source-a"),
			{
				ID:          "source-b",
				SourceKind:  storage.SourceKindYouTubeURL,
				DisplayName: "Video",
				SourceURL:   "https://youtu.be/example",
			},
		},
		SourceLabels:     []string{"voice_a", "video_b"},
		CompletionPolicy: BatchCompletionPolicyAllSources,
		ManifestJSON:     []byte(`{"manifest_version":"batch-transcription.v1"}`),
		Delivery:         storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		ClientRef:        "client-1",
		DisplayName:      "Mixed batch",
	}

	created, err := service.CreateBatchTranscriptionJob(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateBatchTranscriptionJob() error = %v", err)
	}
	if created.Reused {
		t.Fatalf("first batch create should not be reused")
	}
	if created.Job.ParentJobID != "" || created.Job.RootJobID != created.Job.ID || created.Job.ProgressStage != "waiting_for_sources" {
		t.Fatalf("root batch job = %#v", created.Job)
	}
	if got, want := len(store.jobsBySubmission(store.lastSubmissionID)), 3; got != want {
		t.Fatalf("batch graph job count = %d, want root + 2 children", got)
	}
	if got, want := len(store.sourceSets), 3; got != want {
		t.Fatalf("batch source set count = %d, want root + 2 child sets", got)
	}
	rootSet := store.sourceSets[0]
	if rootSet.InputKind != storage.SourceSetInputBatchTranscription || rootSet.DisplayName != "Mixed batch" {
		t.Fatalf("root source set = %#v", rootSet)
	}
	if rootSet.Items[0].SourceLabel != "voice_a" || rootSet.Items[1].SourceLabel != "video_b" {
		t.Fatalf("root source labels = %#v", rootSet.Items)
	}
	if got, want := len(enqueuer.requests), 2; got != want {
		t.Fatalf("batch create enqueue count = %d, want only child transcription tasks", got)
	}
	for _, req := range enqueuer.requests {
		if req.JobType != queue.JobTypeTranscription || req.TaskType == queue.TaskTypeTranscriptionAggregate {
			t.Fatalf("child enqueue request = %#v, want transcription run task", req)
		}
	}

	replayed, err := service.CreateBatchTranscriptionJob(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateBatchTranscriptionJob(replay) error = %v", err)
	}
	if !replayed.Reused || replayed.Job.ID != created.Job.ID {
		t.Fatalf("replay = %#v, want reused root %q", replayed, created.Job.ID)
	}
	if got, want := len(enqueuer.requests), 2; got != want {
		t.Fatalf("replay should not enqueue again, got %d requests", got)
	}

	conflict := request
	conflict.RequestFingerprint = "batch-fingerprint-b"
	_, err = service.CreateBatchTranscriptionJob(context.Background(), conflict)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting batch replay error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestApiJobsBatchCancelRetryAndAggregateScheduling(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	created, err := service.CreateBatchTranscriptionJob(context.Background(), CreateBatchTranscriptionRequest{
		SubmissionKind:     "transcription_batch",
		RequestFingerprint: "batch-fingerprint",
		Sources:            []storage.SourceRecord{source("source-a"), source("source-b")},
		SourceLabels:       []string{"voice_a", "voice_b"},
		CompletionPolicy:   BatchCompletionPolicyAllSources,
		ManifestJSON:       []byte(`{"manifest_version":"batch-transcription.v1"}`),
		Delivery:           storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
	})
	if err != nil {
		t.Fatalf("CreateBatchTranscriptionJob() error = %v", err)
	}
	root := created.Job
	children := store.childrenOf(root.ID)
	if got, want := len(children), 2; got != want {
		t.Fatalf("children = %d, want 2", got)
	}

	canceled, err := service.CancelJob(context.Background(), root.ID)
	if err != nil {
		t.Fatalf("CancelJob(root) error = %v", err)
	}
	if canceled.Status != "canceled" {
		t.Fatalf("root cancel status = %q, want canceled", canceled.Status)
	}
	for _, child := range store.childrenOf(root.ID) {
		if child.Status != "canceled" {
			t.Fatalf("child %q status = %q, want canceled", child.ID, child.Status)
		}
	}

	store.jobs[root.ID] = root
	for idx, child := range children {
		child.Status = "succeeded"
		if idx == 0 {
			child.Status = "failed"
		}
		child.Version++
		store.jobs[child.ID] = child
	}
	retryChild := children[0]
	retryChild.ID = "retry-child-source-a"
	retryChild.RetryOfJobID = children[0].ID
	retryChild.Status = "succeeded"
	retryChild.Version = 1
	store.jobs[retryChild.ID] = retryChild
	store.jobOrder = append(store.jobOrder, retryChild.ID)
	enqueuer.requests = nil

	scheduled, err := service.ScheduleBatchAggregateIfReady(context.Background(), root.ID)
	if err != nil {
		t.Fatalf("ScheduleBatchAggregateIfReady() error = %v", err)
	}
	if !scheduled {
		t.Fatalf("expected aggregate task to be scheduled after all children succeeded")
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("aggregate enqueue count = %d, want 1", got)
	}
	if enqueuer.requests[0].JobID != root.ID || enqueuer.requests[0].TaskType != queue.TaskTypeTranscriptionAggregate {
		t.Fatalf("aggregate enqueue request = %#v", enqueuer.requests[0])
	}

	store.jobs[root.ID] = storage.JobRecord{
		ID:            root.ID,
		RootJobID:     root.RootJobID,
		SourceSetID:   root.SourceSetID,
		JobType:       root.JobType,
		Status:        "failed",
		Delivery:      root.Delivery,
		Version:       9,
		ProgressStage: "failed",
		ParamsJSON:    root.ParamsJSON,
		CreatedAt:     root.CreatedAt,
		FinishedAt:    ptrTime(time.Date(2026, 4, 22, 13, 30, 0, 0, time.UTC)),
	}
	enqueuer.requests = nil
	retry, err := service.RetryJob(context.Background(), root.ID)
	if err != nil {
		t.Fatalf("RetryJob(root) error = %v", err)
	}
	if retry.Job.RetryOfJobID != root.ID || retry.Job.ProgressStage != "aggregate_queued" {
		t.Fatalf("root retry = %#v, want aggregate retry linked to original", retry.Job)
	}
	if got, want := len(store.childrenOf(retry.Job.ID)), 0; got != want {
		t.Fatalf("root retry should reuse original children, got %d new children", got)
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("root retry enqueue count = %d, want aggregate task only", got)
	}
}

func TestApiJobsCreateAgentRunSupportsIdempotencyRootLineageRedactedParamsAndQueue(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	request := CreateAgentRunRequest{
		SubmissionKind:     "agent_run_create",
		IdempotencyKey:     "agent-idem",
		RequestFingerprint: "agent-fingerprint-a",
		HarnessName:        "generic",
		ParamsJSON:         []byte(`{"harness_name":"generic","request":{"prompt_sha256":"abc","prompt_bytes":5,"prompt_runes":5,"payload":{"json_type":"object","sha256":"def","bytes":24},"input_artifacts":[{"artifact_id":"artifact-1"}]}}`),
		Delivery:           storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		ClientRef:          "client-1",
	}

	created, err := service.CreateAgentRun(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateAgentRun() error = %v", err)
	}
	if created.Reused {
		t.Fatalf("first create should not reuse")
	}
	if created.Job.JobType != queue.JobTypeAgentRun {
		t.Fatalf("job_type = %q, want %q", created.Job.JobType, queue.JobTypeAgentRun)
	}
	if created.Job.RootJobID != created.Job.ID || created.Job.ParentJobID != "" || created.Job.RetryOfJobID != "" {
		t.Fatalf("agent_run root lineage = %#v", created.Job)
	}
	if created.Job.ParamsJSON == nil || !json.Valid(created.Job.ParamsJSON) {
		t.Fatalf("params json invalid: %s", string(created.Job.ParamsJSON))
	}
	paramsText := string(created.Job.ParamsJSON)
	if strings.Contains(paramsText, "raw prompt") || strings.Contains(paramsText, "secret-value") {
		t.Fatalf("agent_run params leaked raw prompt/secret: %s", paramsText)
	}
	if got, want := len(store.sourceSets), 1; got != want {
		t.Fatalf("source sets = %d, want %d", got, want)
	}
	if store.sourceSets[0].InputKind != storage.SourceSetInputAgentRun || len(store.sourceSets[0].Items) != 0 {
		t.Fatalf("source set = %#v, want empty agent_run source set", store.sourceSets[0])
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("enqueue count = %d, want %d", got, want)
	}
	if enqueuer.requests[0].JobType != queue.JobTypeAgentRun {
		t.Fatalf("enqueue job type = %q, want %q", enqueuer.requests[0].JobType, queue.JobTypeAgentRun)
	}

	replayed, err := service.CreateAgentRun(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateAgentRun(replay) error = %v", err)
	}
	if !replayed.Reused || replayed.Job.ID != created.Job.ID {
		t.Fatalf("replay = %#v, want reused job %q", replayed, created.Job.ID)
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("replay should not enqueue again, got %d requests", got)
	}

	conflict := request
	conflict.RequestFingerprint = "agent-fingerprint-b"
	_, err = service.CreateAgentRun(context.Background(), conflict)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestApiJobsCreateAgentRunChildReusesCanonicalParentLineageAndAgentQueue(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service := newTestService(t, store, enqueuer)

	parent := storage.JobRecord{
		ID:          "parent-transcription",
		RootJobID:   "root-transcription",
		SourceSetID: "source-set-1",
		JobType:     queue.JobTypeTranscription,
		Status:      "succeeded",
		Delivery:    storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:     3,
	}
	store.jobs[parent.ID] = parent

	request := CreateAgentRunRequest{
		SubmissionKind:     "agent_run_create",
		IdempotencyKey:     "report-idem",
		RequestFingerprint: "report-agent-fingerprint",
		HarnessName:        "claude-code",
		ParentJobID:        parent.ID,
		ParamsJSON:         []byte(`{"harness_name":"claude-code","request_ref":"agentreq_x","request_digest_sha256":"digest","request_bytes":123,"request":{"payload":{"json_type":"object","sha256":"abc","bytes":64}}}`),
		Delivery:           storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		ClientRef:          "client-1",
	}

	created, err := service.CreateAgentRun(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateAgentRun(child) error = %v", err)
	}
	if created.Job.JobType != queue.JobTypeAgentRun {
		t.Fatalf("job_type = %q, want agent_run", created.Job.JobType)
	}
	if created.Job.ParentJobID != parent.ID || created.Job.RootJobID != parent.RootJobID {
		t.Fatalf("child lineage = %#v, want parent=%q root=%q", created.Job, parent.ID, parent.RootJobID)
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("enqueue count = %d, want %d", got, want)
	}
	if enqueuer.requests[0].JobType == queue.JobTypeReport || enqueuer.requests[0].JobType == queue.JobTypeDeepResearch {
		t.Fatalf("dedicated AI worker job was enqueued: %#v", enqueuer.requests[0])
	}
	policy, err := queue.PolicyForJobType(enqueuer.requests[0].JobType)
	if err != nil {
		t.Fatalf("PolicyForJobType(%q) error = %v", enqueuer.requests[0].JobType, err)
	}
	if policy.TaskType != queue.TaskTypeAgentRun {
		t.Fatalf("task_type = %q, want %q", policy.TaskType, queue.TaskTypeAgentRun)
	}

	reused, err := service.CreateAgentRun(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateAgentRun(child replay) error = %v", err)
	}
	if !reused.Reused || reused.Job.ID != created.Job.ID {
		t.Fatalf("reused = %#v, want canonical child %q", reused, created.Job.ID)
	}
	if got, want := len(enqueuer.requests), 1; got != want {
		t.Fatalf("canonical reuse should not enqueue again, got %d requests", got)
	}
}

func TestApiJobsDefaultIDGeneratorUsesUUIDs(t *testing.T) {
	t.Parallel()

	store := newMemoryStore()
	enqueuer := &fakeEnqueuer{}
	service, err := NewService(store, enqueuer, WithClock(func() time.Time {
		return time.Date(2026, 4, 23, 9, 0, 0, 0, time.UTC)
	}))
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	result, err := service.CreateTranscriptionJobs(context.Background(), CreateTranscriptionRequest{
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "idem-uuid",
		RequestFingerprint: "upload-manifest-uuid",
		Sources:            []storage.SourceRecord{source("source-uuid")},
		Delivery:           storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
	})
	if err != nil {
		t.Fatalf("CreateTranscriptionJobs() error = %v", err)
	}
	if got, want := len(result.Jobs), 1; got != want {
		t.Fatalf("created jobs = %d, want %d", got, want)
	}
	if got, want := len(store.sourceSets), 1; got != want {
		t.Fatalf("source sets = %d, want %d", got, want)
	}

	for label, value := range map[string]string{
		"submission_id": store.lastSubmissionID,
		"source_set_id": store.sourceSets[0].ID,
		"job_id":        result.Jobs[0].ID,
		"root_job_id":   result.Jobs[0].RootJobID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			t.Fatalf("%s = %q, want UUID: %v", label, value, err)
		}
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
	failed := storage.JobRecord{ID: "failed-job", RootJobID: "failed-job", SourceSetID: "ss-3", JobType: queue.JobTypeTranscription, Status: "failed", Delivery: storage.Delivery{Strategy: storage.DeliveryStrategyPolling}, Version: 4, FinishedAt: ptrTime(time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC))}
	store.jobs[queued.ID] = queued
	store.jobs[running.ID] = running
	store.jobs[failed.ID] = failed

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

	unchangedTerminal, err := service.CancelJob(context.Background(), failed.ID)
	if err != nil {
		t.Fatalf("CancelJob(failed) error = %v", err)
	}
	if unchangedTerminal.CancelRequestedAt != nil {
		t.Fatalf("terminal cancel response mutated cancel_requested_at: %#v", unchangedTerminal)
	}
	if stored := store.jobs[failed.ID]; stored.CancelRequestedAt != nil {
		t.Fatalf("terminal cancel persisted cancel_requested_at: %#v", stored)
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
	for idx, want := range []string{"job.canceled", "job.updated", "job.failed"} {
		if got := emitter.events[idx].EventType; got != want {
			t.Fatalf("emitted event %d type = %q, want %q", idx, got, want)
		}
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

func ptrTime(value time.Time) *time.Time {
	return &value
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

func (m *memoryStore) ListChildJobs(_ context.Context, parentJobID string) ([]storage.JobRecord, error) {
	return m.childrenOf(parentJobID), nil
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

func (m *memoryStore) childrenOf(parentJobID string) []storage.JobRecord {
	children := make([]storage.JobRecord, 0)
	for _, jobID := range m.jobOrder {
		job := m.jobs[jobID]
		if job.ParentJobID == parentJobID {
			children = append(children, job)
		}
	}
	return children
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
