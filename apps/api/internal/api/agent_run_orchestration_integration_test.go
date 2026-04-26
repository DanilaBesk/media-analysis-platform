package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/jobs"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
)

func TestApiRuntimeAgentRunOperationsIntegrationUseRedactedStorageAndAgentQueue(t *testing.T) {
	ctx := context.Background()
	state, objects, service, queueClient := newAgentRunIntegrationRuntime(t)
	root := seedSucceededTranscriptionForAgentRun(t, state)
	seedArtifact(t, state, storage.ArtifactRecord{
		ID:           "artifact-transcript",
		JobID:        root.ID,
		ArtifactKind: "transcript_segmented_markdown",
		Filename:     "transcript.md",
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    "artifacts/root/transcript/segmented/transcript.md",
		SizeBytes:    31,
		CreatedAt:    time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC),
	})

	report, err := service.CreateReport(ctx, root.ID, ChildCreateRequest{IdempotencyKey: "report-idem"})
	if err != nil {
		t.Fatalf("CreateReport() error = %v", err)
	}
	if report.JobType != queue.JobTypeAgentRun {
		t.Fatalf("report job_type = %q, want agent_run", report.JobType)
	}
	reportJob := state.jobs[report.JobID]
	if reportJob.ParentJobID != root.ID || reportJob.RootJobID != root.ID {
		t.Fatalf("report lineage = %#v, want parent/root %q", reportJob, root.ID)
	}
	if got, want := len(queueClient.specs), 1; got != want {
		t.Fatalf("enqueue count after report = %d, want %d", got, want)
	}
	assertAgentRunQueueSpec(t, queueClient.specs[0], report.JobID)
	assertRedactedAgentRunParams(t, reportJob.ParamsJSON, []string{
		agentRunOperationReport,
		"Create report_markdown",
		"report_markdown",
		"report_docx",
		"artifacts/root/transcript/segmented/transcript.md",
		"transcript.md",
	})
	reportEnvelope := agentRunEnvelopeForJob(t, objects, reportJob)
	assertOperationEnvelope(t, reportEnvelope, "report", []string{"report_markdown", "report_docx"}, []string{"artifact-transcript"})

	reusedReport, err := service.CreateReport(ctx, root.ID, ChildCreateRequest{IdempotencyKey: "report-idem"})
	if err != nil {
		t.Fatalf("CreateReport(reuse) error = %v", err)
	}
	if reusedReport.JobID != report.JobID {
		t.Fatalf("reused report job_id = %q, want %q", reusedReport.JobID, report.JobID)
	}
	if got, want := len(queueClient.specs), 1; got != want {
		t.Fatalf("reused report should not enqueue again, got %d specs", got)
	}

	reportJob.Status = "succeeded"
	reportJob.Version = 4
	state.jobs[reportJob.ID] = reportJob
	seedArtifact(t, state, storage.ArtifactRecord{
		ID:           "artifact-report",
		JobID:        reportJob.ID,
		ArtifactKind: "report_markdown",
		Filename:     "report.md",
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    "artifacts/report/report/markdown/report.md",
		SizeBytes:    41,
		CreatedAt:    time.Date(2026, 4, 26, 12, 5, 0, 0, time.UTC),
	})

	deepResearch, err := service.CreateDeepResearch(ctx, reportJob.ID, ChildCreateRequest{IdempotencyKey: "deep-idem"})
	if err != nil {
		t.Fatalf("CreateDeepResearch() error = %v", err)
	}
	if deepResearch.JobType != queue.JobTypeAgentRun {
		t.Fatalf("deep research job_type = %q, want agent_run", deepResearch.JobType)
	}
	deepResearchJob := state.jobs[deepResearch.JobID]
	if deepResearchJob.ParentJobID != reportJob.ID || deepResearchJob.RootJobID != root.ID {
		t.Fatalf("deep research lineage = %#v, want parent=%q root=%q", deepResearchJob, reportJob.ID, root.ID)
	}
	if got, want := len(queueClient.specs), 2; got != want {
		t.Fatalf("enqueue count after deep research = %d, want %d", got, want)
	}
	assertAgentRunQueueSpec(t, queueClient.specs[1], deepResearch.JobID)
	assertRedactedAgentRunParams(t, deepResearchJob.ParamsJSON, []string{
		agentRunOperationDeepResearch,
		"Create deep_research_markdown",
		"deep_research_markdown",
		"artifacts/root/transcript/segmented/transcript.md",
		"artifacts/report/report/markdown/report.md",
		"transcript.md",
		"report.md",
	})
	deepEnvelope := agentRunEnvelopeForJob(t, objects, deepResearchJob)
	assertOperationEnvelope(t, deepEnvelope, "deep_research", []string{"deep_research_markdown"}, []string{"artifact-transcript", "artifact-report"})

	reusedDeepResearch, err := service.CreateDeepResearch(ctx, reportJob.ID, ChildCreateRequest{IdempotencyKey: "deep-idem"})
	if err != nil {
		t.Fatalf("CreateDeepResearch(reuse) error = %v", err)
	}
	if reusedDeepResearch.JobID != deepResearch.JobID {
		t.Fatalf("reused deep research job_id = %q, want %q", reusedDeepResearch.JobID, deepResearch.JobID)
	}
	if got, want := len(queueClient.specs), 2; got != want {
		t.Fatalf("reused deep research should not enqueue again, got %d specs", got)
	}
}

func TestApiRuntimeBatchAggregateRootHandoffUsesAgentRunOperationEnvelopes(t *testing.T) {
	ctx := context.Background()
	state, objects, service, queueClient := newAgentRunIntegrationRuntime(t)
	root, child := seedSucceededBatchAggregateForAgentRun(t, state)
	seedArtifact(t, state, storage.ArtifactRecord{
		ID:           "artifact-batch-aggregate-transcript",
		JobID:        root.ID,
		ArtifactKind: "transcript_segmented_markdown",
		Filename:     "batch-transcript.md",
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    "artifacts/job-batch-root/transcript/segmented/batch-transcript.md",
		SizeBytes:    47,
		CreatedAt:    time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC),
	})
	seedArtifact(t, state, storage.ArtifactRecord{
		ID:           "artifact-child-transcript",
		JobID:        child.ID,
		ArtifactKind: "transcript_segmented_markdown",
		Filename:     "child-transcript.md",
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    "artifacts/job-batch-child-a/transcript/segmented/child-transcript.md",
		SizeBytes:    23,
		CreatedAt:    time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC),
	})

	if _, err := service.CreateReport(ctx, child.ID, ChildCreateRequest{IdempotencyKey: "report-from-child"}); !errors.Is(err, jobs.ErrInvalidJobState) {
		t.Fatalf("CreateReport(batch child) error = %v, want ErrInvalidJobState", err)
	}
	if _, err := service.CreateDeepResearch(ctx, root.ID, ChildCreateRequest{IdempotencyKey: "deep-from-root"}); !errors.Is(err, jobs.ErrInvalidJobState) {
		t.Fatalf("CreateDeepResearch(batch root) error = %v, want ErrInvalidJobState", err)
	}

	report, err := service.CreateReport(ctx, root.ID, ChildCreateRequest{IdempotencyKey: "report-from-batch-root"})
	if err != nil {
		t.Fatalf("CreateReport(batch root) error = %v", err)
	}
	if report.JobType != queue.JobTypeAgentRun {
		t.Fatalf("report job_type = %q, want agent_run", report.JobType)
	}
	reportJob := state.jobs[report.JobID]
	if reportJob.ParentJobID != root.ID || reportJob.RootJobID != root.ID {
		t.Fatalf("report lineage = %#v, want parent/root %q", reportJob, root.ID)
	}
	if got, want := len(queueClient.specs), 1; got != want {
		t.Fatalf("enqueue count after report = %d, want %d", got, want)
	}
	assertAgentRunQueueSpec(t, queueClient.specs[0], report.JobID)
	assertRedactedAgentRunParams(t, reportJob.ParamsJSON, []string{
		"operation=report",
		"artifact-child-transcript",
		"batch-transcript.md",
		"child-transcript.md",
		"artifacts/job-batch-root",
		"artifacts/job-batch-child-a",
	})
	reportEnvelope := agentRunEnvelopeForJob(t, objects, reportJob)
	assertOperationEnvelope(t, reportEnvelope, "report", []string{"report_markdown", "report_docx"}, []string{"artifact-batch-aggregate-transcript"})
	assertEnvelopeInputArtifact(t, reportEnvelope, "artifact-batch-aggregate-transcript", "batch-transcript.md", "artifacts/job-batch-root/transcript/segmented/batch-transcript.md")

	reportJob.Status = "succeeded"
	reportJob.Version = 4
	state.jobs[reportJob.ID] = reportJob
	seedArtifact(t, state, storage.ArtifactRecord{
		ID:           "artifact-report-markdown",
		JobID:        reportJob.ID,
		ArtifactKind: "report_markdown",
		Filename:     "report.md",
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    "artifacts/job-report-agent-run/report/markdown/report.md",
		SizeBytes:    41,
		CreatedAt:    time.Date(2026, 4, 26, 12, 5, 0, 0, time.UTC),
	})

	deepResearch, err := service.CreateDeepResearch(ctx, reportJob.ID, ChildCreateRequest{IdempotencyKey: "deep-from-batch-report"})
	if err != nil {
		t.Fatalf("CreateDeepResearch(report from batch root) error = %v", err)
	}
	if deepResearch.JobType != queue.JobTypeAgentRun {
		t.Fatalf("deep research job_type = %q, want agent_run", deepResearch.JobType)
	}
	deepResearchJob := state.jobs[deepResearch.JobID]
	if deepResearchJob.ParentJobID != reportJob.ID || deepResearchJob.RootJobID != root.ID {
		t.Fatalf("deep research lineage = %#v, want parent=%q root=%q", deepResearchJob, reportJob.ID, root.ID)
	}
	if got, want := len(queueClient.specs), 2; got != want {
		t.Fatalf("enqueue count after deep research = %d, want %d", got, want)
	}
	assertAgentRunQueueSpec(t, queueClient.specs[1], deepResearch.JobID)
	deepEnvelope := agentRunEnvelopeForJob(t, objects, deepResearchJob)
	assertOperationEnvelope(t, deepEnvelope, "deep_research", []string{"deep_research_markdown"}, []string{"artifact-batch-aggregate-transcript", "artifact-report-markdown"})
	assertEnvelopeInputArtifact(t, deepEnvelope, "artifact-batch-aggregate-transcript", "batch-transcript.md", "artifacts/job-batch-root/transcript/segmented/batch-transcript.md")
	assertEnvelopeInputArtifact(t, deepEnvelope, "artifact-report-markdown", "report.md", "artifacts/job-report-agent-run/report/markdown/report.md")
}

func TestApiRuntimeCreateDeepResearchRequiresReportMarkdownOnReportBackedAgentRun(t *testing.T) {
	ctx := context.Background()
	state, _, service, _ := newAgentRunIntegrationRuntime(t)
	root := seedSucceededTranscriptionForAgentRun(t, state)
	seedArtifact(t, state, storage.ArtifactRecord{
		ID:           "artifact-transcript",
		JobID:        root.ID,
		ArtifactKind: "transcript_segmented_markdown",
		Filename:     "transcript.md",
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    "artifacts/root/transcript/segmented/transcript.md",
		SizeBytes:    31,
		CreatedAt:    time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC),
	})
	reportJob := storage.JobRecord{
		ID:              "job-report-agent",
		SubmissionID:    "submission-report-agent",
		RootJobID:       root.ID,
		ParentJobID:     root.ID,
		SourceSetID:     "source-set-report-agent",
		JobType:         queue.JobTypeAgentRun,
		Status:          "succeeded",
		Delivery:        storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:         4,
		ProgressStage:   "completed",
		ProgressMessage: "done",
		ParamsJSON:      []byte(`{"harness_name":"claude-code"}`),
		CreatedAt:       time.Date(2026, 4, 26, 12, 1, 0, 0, time.UTC),
	}
	state.sourceSets = append(state.sourceSets, storage.SourceSetRecord{
		ID:        reportJob.SourceSetID,
		InputKind: storage.SourceSetInputAgentRun,
		CreatedAt: reportJob.CreatedAt,
	})
	state.jobs[reportJob.ID] = reportJob
	state.jobOrder = append(state.jobOrder, reportJob.ID)

	_, err := service.CreateDeepResearch(ctx, reportJob.ID, ChildCreateRequest{IdempotencyKey: "deep-idem"})
	if !errors.Is(err, jobs.ErrMissingArtifact) {
		t.Fatalf("CreateDeepResearch() error = %v, want ErrMissingArtifact", err)
	}
}

func newAgentRunIntegrationRuntime(t *testing.T) (*agentRunIntegrationStateStore, *agentRunIntegrationObjectStore, *publicRuntimeService, *agentRunIntegrationQueueClient) {
	t.Helper()

	state := newAgentRunIntegrationStateStore()
	objects := newAgentRunIntegrationObjectStore()
	now := func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) }
	nextID := newAgentRunIntegrationIDGenerator()
	repo, err := storage.NewRepository(state, objects, storage.WithClock(now), storage.WithIDGenerator(nextID))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	queueClient := &agentRunIntegrationQueueClient{}
	publisher, err := queue.NewPublisher(queueClient)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	jobsService, err := jobs.NewService(repo, publisher, jobs.WithClock(now), jobs.WithIDGenerator(nextID))
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return state, objects, newPublicRuntimeService(jobsService, repo, nil), queueClient
}

func seedSucceededTranscriptionForAgentRun(t *testing.T, state *agentRunIntegrationStateStore) storage.JobRecord {
	t.Helper()

	source := storage.SourceRecord{
		ID:               "source-root",
		SourceKind:       storage.SourceKindUploadedFile,
		DisplayName:      "root audio",
		OriginalFilename: "root.ogg",
		MIMEType:         "audio/ogg",
		ObjectKey:        "sources/source-root/original/root.ogg",
		SizeBytes:        12,
		CreatedAt:        time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	sourceSet := storage.SourceSetRecord{
		ID:        "source-set-root",
		InputKind: storage.SourceSetInputSingleSource,
		Items:     []storage.SourceSetItem{{Position: 0, SourceID: source.ID}},
		CreatedAt: time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	job := storage.JobRecord{
		ID:              "job-root",
		SubmissionID:    "submission-root",
		RootJobID:       "job-root",
		SourceSetID:     sourceSet.ID,
		JobType:         queue.JobTypeTranscription,
		Status:          "succeeded",
		Delivery:        storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:         4,
		ProgressStage:   "completed",
		ProgressMessage: "done",
		ParamsJSON:      []byte(`{}`),
		ClientRef:       "client-root",
		CreatedAt:       time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	state.submissions["transcription_upload::root-idem"] = &storage.JobSubmission{
		ID:                 job.SubmissionID,
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "root-idem",
		RequestFingerprint: "root-fingerprint",
		CreatedAt:          job.CreatedAt,
	}
	state.sources = append(state.sources, source)
	state.sourceSets = append(state.sourceSets, sourceSet)
	state.jobs[job.ID] = job
	state.jobOrder = append(state.jobOrder, job.ID)
	return job
}

func seedSucceededBatchAggregateForAgentRun(t *testing.T, state *agentRunIntegrationStateStore) (storage.JobRecord, storage.JobRecord) {
	t.Helper()

	sourceA := storage.SourceRecord{
		ID:               "source-batch-a",
		SourceKind:       storage.SourceKindUploadedFile,
		DisplayName:      "batch audio A",
		OriginalFilename: "batch-a.ogg",
		MIMEType:         "audio/ogg",
		ObjectKey:        "sources/source-batch-a/original/batch-a.ogg",
		SizeBytes:        12,
		CreatedAt:        time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	sourceB := storage.SourceRecord{
		ID:          "source-batch-b",
		SourceKind:  storage.SourceKindYouTubeURL,
		DisplayName: "batch video B",
		SourceURL:   "https://youtu.be/example",
		CreatedAt:   time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	rootSourceSet := storage.SourceSetRecord{
		ID:        "source-set-batch-root",
		InputKind: storage.SourceSetInputBatchTranscription,
		Items: []storage.SourceSetItem{
			{Position: 0, SourceID: sourceA.ID, SourceLabel: "voice_a", SourceLabelVersion: "batch-transcription.v1"},
			{Position: 1, SourceID: sourceB.ID, SourceLabel: "video_b", SourceLabelVersion: "batch-transcription.v1"},
		},
		CreatedAt: time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	childSourceSet := storage.SourceSetRecord{
		ID:        "source-set-batch-child-a",
		InputKind: storage.SourceSetInputSingleSource,
		Items:     []storage.SourceSetItem{{Position: 0, SourceID: sourceA.ID, SourceLabel: "voice_a", SourceLabelVersion: "batch-transcription.v1"}},
		CreatedAt: time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	root := storage.JobRecord{
		ID:              "job-batch-root",
		SubmissionID:    "submission-batch",
		RootJobID:       "job-batch-root",
		SourceSetID:     rootSourceSet.ID,
		JobType:         queue.JobTypeTranscription,
		Status:          "succeeded",
		Delivery:        storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:         4,
		ProgressStage:   "completed",
		ProgressMessage: "aggregate done",
		ParamsJSON:      []byte(`{"batch":{"role":"aggregate","completion_policy":"succeed_when_all_sources_succeed","ordered_source_labels":["voice_a","video_b"]}}`),
		ClientRef:       "client-batch",
		CreatedAt:       time.Date(2026, 4, 26, 11, 55, 0, 0, time.UTC),
	}
	child := storage.JobRecord{
		ID:              "job-batch-child-a",
		SubmissionID:    "submission-batch",
		RootJobID:       root.ID,
		ParentJobID:     root.ID,
		SourceSetID:     childSourceSet.ID,
		JobType:         queue.JobTypeTranscription,
		Status:          "succeeded",
		Delivery:        storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:         4,
		ProgressStage:   "completed",
		ProgressMessage: "source done",
		ParamsJSON:      []byte(`{"batch":{"role":"source","source_label":"voice_a"}}`),
		ClientRef:       "client-batch",
		CreatedAt:       time.Date(2026, 4, 26, 11, 56, 0, 0, time.UTC),
	}
	state.submissions["transcription_batch::batch-idem"] = &storage.JobSubmission{
		ID:                 root.SubmissionID,
		SubmissionKind:     "transcription_batch",
		IdempotencyKey:     "batch-idem",
		RequestFingerprint: "batch-fingerprint",
		CreatedAt:          root.CreatedAt,
	}
	state.sources = append(state.sources, sourceA, sourceB)
	state.sourceSets = append(state.sourceSets, rootSourceSet, childSourceSet)
	state.jobs[root.ID] = root
	state.jobs[child.ID] = child
	state.jobOrder = append(state.jobOrder, root.ID, child.ID)
	return root, child
}

func seedArtifact(t *testing.T, state *agentRunIntegrationStateStore, artifact storage.ArtifactRecord) {
	t.Helper()

	state.persistedArtifacts[artifact.ID] = artifact
}

func assertAgentRunQueueSpec(t *testing.T, spec queue.EnqueueSpec, jobID string) {
	t.Helper()

	if spec.QueueName != queue.QueueNameAgentRun || spec.TaskType != queue.TaskTypeAgentRun {
		t.Fatalf("queue spec = %#v, want agent_run.run policy", spec)
	}
	var payload queue.Payload
	if err := json.Unmarshal(spec.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal(queue payload) error = %v", err)
	}
	if payload.JobID != jobID || payload.Attempt != 1 {
		t.Fatalf("queue payload = %#v, want job_id=%q attempt=1", payload, jobID)
	}
}

func assertRedactedAgentRunParams(t *testing.T, paramsJSON []byte, forbidden []string) {
	t.Helper()

	if !json.Valid(paramsJSON) {
		t.Fatalf("params_json is not valid JSON: %s", string(paramsJSON))
	}
	text := string(paramsJSON)
	for _, value := range forbidden {
		if strings.Contains(text, value) {
			t.Fatalf("params_json leaked %q: %s", value, text)
		}
	}
	for _, required := range []string{"request_ref", "request_digest_sha256", "request_bytes", "prompt_sha256", "payload", "sha256", "bytes", "object_key_sha256", "filename_sha256"} {
		if !strings.Contains(text, required) {
			t.Fatalf("params_json missing redacted metadata %q: %s", required, text)
		}
	}
}

func agentRunEnvelopeForJob(t *testing.T, objects *agentRunIntegrationObjectStore, job storage.JobRecord) map[string]any {
	t.Helper()

	var params struct {
		RequestRef          string `json:"request_ref"`
		RequestDigestSHA256 string `json:"request_digest_sha256"`
		RequestBytes        int64  `json:"request_bytes"`
	}
	if err := json.Unmarshal(job.ParamsJSON, &params); err != nil {
		t.Fatalf("Unmarshal(params_json) error = %v", err)
	}
	if params.RequestRef == "" || params.RequestDigestSHA256 == "" || params.RequestBytes <= 0 {
		t.Fatalf("params missing request pointer: %s", string(job.ParamsJSON))
	}
	objectKey := fmt.Sprintf("private/agent-runs/%s/request.json", params.RequestRef)
	body, ok := objects.bodies[storage.ArtifactsBucket+"::"+objectKey]
	if !ok {
		t.Fatalf("private envelope object %q was not persisted; keys=%v", objectKey, objects.keys())
	}
	if int64(len(body)) != params.RequestBytes {
		t.Fatalf("request_bytes = %d, want %d", params.RequestBytes, len(body))
	}
	digest := sha256.Sum256(body)
	if got := hex.EncodeToString(digest[:]); got != params.RequestDigestSHA256 {
		t.Fatalf("request digest = %q, want %q", got, params.RequestDigestSHA256)
	}
	var envelope map[string]any
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("Unmarshal(private envelope) error = %v", err)
	}
	return envelope
}

func assertOperationEnvelope(t *testing.T, envelope map[string]any, operation string, expectedArtifacts []string, inputArtifactIDs []string) {
	t.Helper()

	if envelope["schema_version"] != "agent_run_request_envelope/v1" || envelope["harness_name"] != agentRunHarnessClaudeCode {
		t.Fatalf("envelope header = %#v", envelope)
	}
	request, ok := envelope["request"].(map[string]any)
	if !ok {
		t.Fatalf("envelope request = %#v, want object", envelope["request"])
	}
	payload, ok := request["payload"].(map[string]any)
	if !ok {
		t.Fatalf("envelope payload = %#v, want object", request["payload"])
	}
	if payload["operation"] != operation {
		t.Fatalf("payload operation = %q, want %q", payload["operation"], operation)
	}
	expected := stringSet(expectedArtifacts)
	for _, raw := range payload["expected_output_artifacts"].([]any) {
		delete(expected, raw.(string))
	}
	if len(expected) != 0 {
		t.Fatalf("payload missing expected artifacts: %v in %#v", expected, payload["expected_output_artifacts"])
	}
	inputs, ok := request["input_artifacts"].([]any)
	if !ok {
		t.Fatalf("input_artifacts = %#v, want array", request["input_artifacts"])
	}
	seen := map[string]bool{}
	for _, raw := range inputs {
		item := raw.(map[string]any)
		seen[item["artifact_id"].(string)] = true
		if item["object_key"] == "" || item["filename"] == "" {
			t.Fatalf("private envelope input artifact lost locator data: %#v", item)
		}
	}
	for _, artifactID := range inputArtifactIDs {
		if !seen[artifactID] {
			t.Fatalf("input artifact %q missing from private envelope: %#v", artifactID, inputs)
		}
	}
}

func assertEnvelopeInputArtifact(t *testing.T, envelope map[string]any, artifactID, filename, objectKey string) {
	t.Helper()

	request, ok := envelope["request"].(map[string]any)
	if !ok {
		t.Fatalf("envelope request = %#v, want object", envelope["request"])
	}
	inputs, ok := request["input_artifacts"].([]any)
	if !ok {
		t.Fatalf("input_artifacts = %#v, want array", request["input_artifacts"])
	}
	for _, raw := range inputs {
		item := raw.(map[string]any)
		if item["artifact_id"] == artifactID {
			if item["filename"] != filename || item["object_key"] != objectKey {
				t.Fatalf("input artifact %q = %#v, want filename=%q object_key=%q", artifactID, item, filename, objectKey)
			}
			return
		}
	}
	t.Fatalf("input artifact %q missing from private envelope: %#v", artifactID, inputs)
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

type agentRunIntegrationStateStore struct {
	sources              []storage.SourceRecord
	sourceSets           []storage.SourceSetRecord
	submissions          map[string]*storage.JobSubmission
	jobs                 map[string]storage.JobRecord
	jobOrder             []string
	executions           map[string]storage.JobExecutionRecord
	activeExecutionByJob map[string]string
	persistedArtifacts   map[string]storage.ArtifactRecord
	events               []storage.JobEvent
	deliveries           []storage.WebhookDelivery
}

func newAgentRunIntegrationStateStore() *agentRunIntegrationStateStore {
	return &agentRunIntegrationStateStore{
		submissions:          map[string]*storage.JobSubmission{},
		jobs:                 map[string]storage.JobRecord{},
		executions:           map[string]storage.JobExecutionRecord{},
		activeExecutionByJob: map[string]string{},
		persistedArtifacts:   map[string]storage.ArtifactRecord{},
	}
}

func (s *agentRunIntegrationStateStore) PersistSource(_ context.Context, source storage.SourceRecord) error {
	s.sources = append(s.sources, source)
	return nil
}

func (s *agentRunIntegrationStateStore) PersistJobBundle(_ context.Context, bundle storage.PersistedJobBundle) error {
	if bundle.Submission != nil {
		copySubmission := *bundle.Submission
		s.submissions[bundle.Submission.SubmissionKind+"::"+bundle.Submission.IdempotencyKey] = &copySubmission
	}
	s.sources = append(s.sources, bundle.Sources...)
	s.sourceSets = append(s.sourceSets, bundle.SourceSet)
	s.jobs[bundle.Job.ID] = bundle.Job
	s.jobOrder = append(s.jobOrder, bundle.Job.ID)
	if bundle.WebhookDelivery != nil {
		s.deliveries = append(s.deliveries, *bundle.WebhookDelivery)
	}
	return nil
}

func (s *agentRunIntegrationStateStore) PersistArtifacts(_ context.Context, artifacts []storage.ArtifactRecord) error {
	for _, artifact := range artifacts {
		s.persistedArtifacts[artifact.ID] = artifact
	}
	return nil
}

func (s *agentRunIntegrationStateStore) LookupArtifact(_ context.Context, artifactID string) (storage.ArtifactRecord, error) {
	artifact, ok := s.persistedArtifacts[artifactID]
	if !ok {
		return storage.ArtifactRecord{}, storage.ErrArtifactNotFound
	}
	return artifact, nil
}

func (s *agentRunIntegrationStateStore) FindSubmission(_ context.Context, submissionKind, idempotencyKey string) (*storage.JobSubmission, error) {
	return s.submissions[submissionKind+"::"+idempotencyKey], nil
}

func (s *agentRunIntegrationStateStore) ListJobsBySubmission(_ context.Context, submissionID string) ([]storage.JobRecord, error) {
	var result []storage.JobRecord
	for _, jobID := range s.jobOrder {
		job := s.jobs[jobID]
		if job.SubmissionID == submissionID {
			result = append(result, job)
		}
	}
	return result, nil
}

func (s *agentRunIntegrationStateStore) SaveSubmissionGraph(_ context.Context, submission storage.JobSubmission, sources []storage.SourceRecord, sourceSets []storage.SourceSetRecord, jobs []storage.JobRecord) error {
	copySubmission := submission
	s.submissions[submission.SubmissionKind+"::"+submission.IdempotencyKey] = &copySubmission
	s.sources = append(s.sources, sources...)
	s.sourceSets = append(s.sourceSets, sourceSets...)
	for _, job := range jobs {
		s.jobs[job.ID] = job
		s.jobOrder = append(s.jobOrder, job.ID)
	}
	return nil
}

func (s *agentRunIntegrationStateStore) GetJob(_ context.Context, jobID string) (storage.JobRecord, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return storage.JobRecord{}, storage.ErrJobNotFound
	}
	return job, nil
}

func (s *agentRunIntegrationStateStore) CreateJob(_ context.Context, job storage.JobRecord) error {
	s.jobs[job.ID] = job
	s.jobOrder = append(s.jobOrder, job.ID)
	return nil
}

func (s *agentRunIntegrationStateStore) UpdateJob(_ context.Context, job storage.JobRecord) error {
	s.jobs[job.ID] = job
	return nil
}

func (s *agentRunIntegrationStateStore) FindCanonicalChild(_ context.Context, parentJobID, jobType string) (*storage.JobRecord, error) {
	for _, jobID := range s.jobOrder {
		job := s.jobs[jobID]
		if job.ParentJobID == parentJobID && job.JobType == jobType && job.RetryOfJobID == "" {
			copyJob := job
			return &copyJob, nil
		}
	}
	return nil, nil
}

func (s *agentRunIntegrationStateStore) FindArtifactByKind(_ context.Context, jobID, artifactKind string) (*storage.ArtifactRecord, error) {
	for _, artifact := range s.persistedArtifacts {
		if artifact.JobID == jobID && artifact.ArtifactKind == artifactKind {
			copyArtifact := artifact
			return &copyArtifact, nil
		}
	}
	return nil, nil
}

func (s *agentRunIntegrationStateStore) AppendJobEvent(_ context.Context, event storage.JobEvent) error {
	s.events = append(s.events, event)
	return nil
}

func (s *agentRunIntegrationStateStore) CreateWebhookDelivery(_ context.Context, delivery storage.WebhookDelivery) error {
	s.deliveries = append(s.deliveries, delivery)
	return nil
}

func (s *agentRunIntegrationStateStore) UpdateWebhookDelivery(_ context.Context, delivery storage.WebhookDelivery) error {
	for idx := range s.deliveries {
		if s.deliveries[idx].ID == delivery.ID {
			s.deliveries[idx] = delivery
			return nil
		}
	}
	s.deliveries = append(s.deliveries, delivery)
	return nil
}

func (s *agentRunIntegrationStateStore) ListDueWebhookDeliveries(_ context.Context, now time.Time, limit int) ([]storage.WebhookDelivery, error) {
	var result []storage.WebhookDelivery
	for _, delivery := range s.deliveries {
		if delivery.State == storage.WebhookStatePending && !delivery.NextAttemptAt.After(now) {
			result = append(result, delivery)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func (s *agentRunIntegrationStateStore) ClaimJobExecution(_ context.Context, req storage.ClaimJobExecutionRequest) (storage.ClaimJobExecutionResult, error) {
	job, ok := s.jobs[req.JobID]
	if !ok {
		return storage.ClaimJobExecutionResult{}, storage.ErrJobNotFound
	}
	if _, ok := s.activeExecutionByJob[req.JobID]; ok || job.Status != "queued" {
		return storage.ClaimJobExecutionResult{Job: job, Claimed: false}, nil
	}
	execution := storage.JobExecutionRecord{
		ExecutionID: req.ExecutionID,
		JobID:       req.JobID,
		WorkerKind:  req.WorkerKind,
		TaskType:    req.TaskType,
		ClaimedAt:   req.ClaimedAt,
	}
	s.executions[execution.ExecutionID] = execution
	s.activeExecutionByJob[req.JobID] = execution.ExecutionID
	startedAt := req.ClaimedAt
	job.Status = "running"
	job.Version++
	job.StartedAt = &startedAt
	s.jobs[job.ID] = job
	return storage.ClaimJobExecutionResult{Job: job, Execution: &execution, Claimed: true}, nil
}

func (s *agentRunIntegrationStateStore) GetActiveJobExecution(_ context.Context, jobID, executionID string) (storage.JobExecutionRecord, error) {
	activeID, ok := s.activeExecutionByJob[jobID]
	if !ok || activeID != executionID {
		return storage.JobExecutionRecord{}, storage.ErrExecutionNotFound
	}
	execution, ok := s.executions[executionID]
	if !ok || execution.FinishedAt != nil {
		return storage.JobExecutionRecord{}, storage.ErrExecutionNotFound
	}
	return execution, nil
}

func (s *agentRunIntegrationStateStore) FinishJobExecution(_ context.Context, req storage.FinishJobExecutionRequest) (storage.JobExecutionRecord, error) {
	execution, ok := s.executions[req.ExecutionID]
	if !ok || execution.JobID != req.JobID || execution.FinishedAt != nil {
		return storage.JobExecutionRecord{}, storage.ErrExecutionNotFound
	}
	finishedAt := req.FinishedAt
	execution.FinishedAt = &finishedAt
	execution.Outcome = req.Outcome
	s.executions[req.ExecutionID] = execution
	delete(s.activeExecutionByJob, req.JobID)
	return execution, nil
}

func (s *agentRunIntegrationStateStore) ListJobs(_ context.Context, filter storage.JobListFilter) (storage.JobListPage, error) {
	var result []storage.JobRecord
	for _, jobID := range s.jobOrder {
		job := s.jobs[jobID]
		if filter.Status != "" && job.Status != filter.Status {
			continue
		}
		if filter.JobType != "" && job.JobType != filter.JobType {
			continue
		}
		if filter.RootJobID != "" && job.RootJobID != filter.RootJobID {
			continue
		}
		result = append(result, job)
	}
	end := filter.Offset + filter.Limit
	if filter.Offset >= len(result) {
		return storage.JobListPage{}, nil
	}
	hasMore := end < len(result)
	if end > len(result) {
		end = len(result)
	}
	return storage.JobListPage{Jobs: result[filter.Offset:end], HasMore: hasMore}, nil
}

func (s *agentRunIntegrationStateStore) GetSourceSet(_ context.Context, sourceSetID string) (storage.SourceSetRecord, error) {
	for _, sourceSet := range s.sourceSets {
		if sourceSet.ID == sourceSetID {
			return sourceSet, nil
		}
	}
	return storage.SourceSetRecord{}, storage.ErrStorageUnavailable
}

func (s *agentRunIntegrationStateStore) ListOrderedSources(_ context.Context, sourceSetID string) ([]storage.OrderedSource, error) {
	var sourceSet storage.SourceSetRecord
	for _, candidate := range s.sourceSets {
		if candidate.ID == sourceSetID {
			sourceSet = candidate
			break
		}
	}
	if sourceSet.ID == "" {
		return nil, storage.ErrStorageUnavailable
	}
	result := make([]storage.OrderedSource, 0, len(sourceSet.Items))
	for _, item := range sourceSet.Items {
		for _, source := range s.sources {
			if source.ID == item.SourceID {
				result = append(result, storage.OrderedSource{Position: item.Position, Source: source})
				break
			}
		}
	}
	return result, nil
}

func (s *agentRunIntegrationStateStore) ListArtifactsByJob(_ context.Context, jobID string) ([]storage.ArtifactRecord, error) {
	var result []storage.ArtifactRecord
	for _, artifact := range s.persistedArtifacts {
		if artifact.JobID == jobID {
			result = append(result, artifact)
		}
	}
	return result, nil
}

func (s *agentRunIntegrationStateStore) ListChildJobs(_ context.Context, parentJobID string) ([]storage.JobRecord, error) {
	var result []storage.JobRecord
	for _, jobID := range s.jobOrder {
		job := s.jobs[jobID]
		if job.ParentJobID == parentJobID {
			result = append(result, job)
		}
	}
	return result, nil
}

func (s *agentRunIntegrationStateStore) ListJobEvents(_ context.Context, jobID string) ([]storage.JobEvent, error) {
	var result []storage.JobEvent
	for _, event := range s.events {
		if event.JobID == jobID {
			result = append(result, event)
		}
	}
	return result, nil
}

func (s *agentRunIntegrationStateStore) UpsertArtifacts(_ context.Context, artifacts []storage.ArtifactRecord) error {
	for _, artifact := range artifacts {
		s.persistedArtifacts[artifact.ID] = artifact
	}
	return nil
}

type agentRunIntegrationObjectStore struct {
	bodies map[string][]byte
}

func newAgentRunIntegrationObjectStore() *agentRunIntegrationObjectStore {
	return &agentRunIntegrationObjectStore{bodies: map[string][]byte{}}
}

func (s *agentRunIntegrationObjectStore) PutObject(_ context.Context, bucket, objectKey, _ string, body []byte) error {
	s.bodies[bucket+"::"+objectKey] = append([]byte(nil), body...)
	return nil
}

func (s *agentRunIntegrationObjectStore) PresignGetObject(_ context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	return "https://public-minio.local/" + bucket + "/" + objectKey, time.Date(2026, 4, 26, 13, 0, 0, 0, time.UTC).Add(expiry), nil
}

func (s *agentRunIntegrationObjectStore) PresignInternalGetObject(_ context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	return "https://minio.local/" + bucket + "/" + objectKey, time.Date(2026, 4, 26, 13, 0, 0, 0, time.UTC).Add(expiry), nil
}

func (s *agentRunIntegrationObjectStore) keys() []string {
	keys := make([]string, 0, len(s.bodies))
	for key := range s.bodies {
		keys = append(keys, key)
	}
	return keys
}

type agentRunIntegrationQueueClient struct {
	specs []queue.EnqueueSpec
}

func (c *agentRunIntegrationQueueClient) Enqueue(_ context.Context, spec queue.EnqueueSpec) (queue.EnqueueReceipt, error) {
	c.specs = append(c.specs, spec)
	return queue.EnqueueReceipt{
		ID:        fmt.Sprintf("task-%d", len(c.specs)),
		QueueName: spec.QueueName,
		TaskType:  spec.TaskType,
	}, nil
}

func newAgentRunIntegrationIDGenerator() func() string {
	values := []string{
		"submission-report",
		"source-set-report",
		"job-report-agent-run",
		"submission-deep-research",
		"source-set-deep-research",
		"job-deep-research-agent-run",
	}
	index := 0
	return func() string {
		if index >= len(values) {
			index++
			return fmt.Sprintf("generated-extra-%02d", index)
		}
		value := values[index]
		index++
		return value
	}
}
