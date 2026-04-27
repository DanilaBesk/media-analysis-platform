package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/jobs"
	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	"github.com/danila/media-analysis-platform/apps/api/internal/ws"
)

func TestApiHttpRuntimeCreateUploadEmitsCreatedEventAndReturnsSnapshot(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", "transcription", "queued", 1)
	sourceSet := runtimeSourceSet(job.SourceSetID, storage.SourceSetInputSingleSource)
	orderedSources := []storage.OrderedSource{
		{
			Position: 0,
			Source: storage.SourceRecord{
				ID:               "22222222-2222-2222-2222-222222222222",
				SourceKind:       storage.SourceKindUploadedFile,
				DisplayName:      "Call",
				OriginalFilename: "call.ogg",
				ObjectKey:        "sources/22222222-2222-2222-2222-222222222222/original/call.ogg",
				CreatedAt:        time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			},
		},
	}

	store := &runtimeStorageDouble{
		jobs:               map[string]storage.JobRecord{job.ID: job},
		sourceSets:         map[string]storage.SourceSetRecord{sourceSet.ID: sourceSet},
		orderedSourcesByID: map[string][]storage.OrderedSource{sourceSet.ID: orderedSources},
		artifactsByJob:     map[string][]storage.ArtifactRecord{job.ID: nil},
		childrenByParent:   map[string][]storage.JobRecord{job.ID: nil},
	}
	jobsService := &runtimeJobsDouble{
		createJobsResult: jobs.CreateJobsResult{Jobs: []storage.JobRecord{job}},
	}
	eventsService := &runtimeEventsDouble{}

	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshots, err := service.CreateUpload(context.Background(), UploadCommand{
		IdempotencyKey: "idem-1",
		DisplayName:    "Call",
		ClientRef:      "client-1",
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		Files: []UploadFile{
			{
				Filename:    "call.ogg",
				ContentType: "audio/ogg",
				SizeBytes:   12,
				SHA256:      "checksum",
				Body:        []byte("hello world!"),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateUpload() error = %v", err)
	}
	if got, want := len(snapshots), 1; got != want {
		t.Fatalf("snapshot count = %d, want %d", got, want)
	}
	if snapshots[0].JobID != job.ID {
		t.Fatalf("job_id = %q, want %q", snapshots[0].JobID, job.ID)
	}
	if jobsService.createUpload.SubmissionKind != submissionKindTranscriptionUpload {
		t.Fatalf("submission kind = %q, want %q", jobsService.createUpload.SubmissionKind, submissionKindTranscriptionUpload)
	}
	if got := jobsService.createUpload.Sources[0].ObjectKey; got == "" {
		t.Fatal("expected upload source object key to be materialized")
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
	if eventsService.requests[0].EventType != "job.created" {
		t.Fatalf("event type = %q, want job.created", eventsService.requests[0].EventType)
	}
}

func TestApiHttpRuntimeCreateBatchReturnsRootSnapshotWithSourceLabels(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "queued", 1)
	sourceSet := storage.SourceSetRecord{
		ID:        root.SourceSetID,
		InputKind: storage.SourceSetInputBatchTranscription,
		Items: []storage.SourceSetItem{
			{Position: 0, SourceID: "22222222-2222-2222-2222-222222222222", SourceLabel: "voice_a", SourceLabelVersion: "batch-transcription.v1"},
			{Position: 1, SourceID: "33333333-3333-3333-3333-333333333333", SourceLabel: "video_b", SourceLabelVersion: "batch-transcription.v1"},
		},
		CreatedAt: root.CreatedAt,
	}
	childA := runtimeJobRecord("44444444-4444-4444-4444-444444444444", queue.JobTypeTranscription, "queued", 1)
	childA.RootJobID = root.ID
	childA.ParentJobID = root.ID
	childB := runtimeJobRecord("55555555-5555-5555-5555-555555555555", queue.JobTypeTranscription, "queued", 1)
	childB.RootJobID = root.ID
	childB.ParentJobID = root.ID

	store := &runtimeStorageDouble{
		jobs:       map[string]storage.JobRecord{root.ID: root, childA.ID: childA, childB.ID: childB},
		sourceSets: map[string]storage.SourceSetRecord{sourceSet.ID: sourceSet},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			sourceSet.ID: {
				{Position: 0, SourceLabel: "voice_a", Source: storage.SourceRecord{ID: "22222222-2222-2222-2222-222222222222", SourceKind: storage.SourceKindUploadedFile, DisplayName: "Voice A"}},
				{Position: 1, SourceLabel: "video_b", Source: storage.SourceRecord{ID: "33333333-3333-3333-3333-333333333333", SourceKind: storage.SourceKindYouTubeURL, DisplayName: "Video B", SourceURL: "https://youtu.be/example"}},
			},
		},
		artifactsByJob:   map[string][]storage.ArtifactRecord{root.ID: nil},
		childrenByParent: map[string][]storage.JobRecord{root.ID: {childA, childB}},
	}
	jobsService := &runtimeJobsDouble{
		createBatchResult: jobs.ChildResult{Job: root},
	}
	eventsService := &runtimeEventsDouble{}
	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshot, err := service.CreateBatch(context.Background(), BatchCommand{
		IdempotencyKey: "batch-idem",
		DisplayName:    "Mixed batch",
		ClientRef:      "client-1",
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		Manifest: BatchSourceManifest{
			ManifestVersion:     batchManifestVersion,
			OrderedSourceLabels: []string{"voice_a", "video_b"},
			Sources: map[string]BatchSource{
				"voice_a": {SourceKind: storage.SourceKindUploadedFile, FilePart: "voice_a", DisplayName: "Voice A"},
				"video_b": {SourceKind: storage.SourceKindYouTubeURL, URL: "https://youtu.be/example", DisplayName: "Video B"},
			},
			CompletionPolicy: BatchCompletionPolicyAllSources,
		},
		Files: []BatchUploadFile{{PartName: "voice_a", UploadFile: UploadFile{Filename: "voice.ogg", ContentType: "audio/ogg", SizeBytes: 5, SHA256: "digest", Body: []byte("voice")}}},
	})
	if err != nil {
		t.Fatalf("CreateBatch() error = %v", err)
	}
	if snapshot.JobID != root.ID || snapshot.SourceSet.InputKind != storage.SourceSetInputBatchTranscription {
		t.Fatalf("snapshot = %#v, want batch root %q", snapshot, root.ID)
	}
	if got, want := len(snapshot.SourceSet.Items), 2; got != want {
		t.Fatalf("source set item count = %d, want %d", got, want)
	}
	if snapshot.SourceSet.Items[0].SourceLabel == nil || *snapshot.SourceSet.Items[0].SourceLabel != "voice_a" {
		t.Fatalf("first source label = %#v, want voice_a", snapshot.SourceSet.Items[0].SourceLabel)
	}
	if got, want := len(snapshot.Children), 2; got != want {
		t.Fatalf("child refs = %d, want %d", got, want)
	}
	if jobsService.createBatch.SubmissionKind != submissionKindTranscriptionBatch {
		t.Fatalf("submission kind = %q, want %q", jobsService.createBatch.SubmissionKind, submissionKindTranscriptionBatch)
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
}

func TestApiHttpRuntimeSubmitBatchDraftReplayReturnsExistingRoot(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "queued", 1)
	sourceSet := runtimeSourceSet(root.SourceSetID, storage.SourceSetInputBatchTranscription)
	draft := storage.BatchDraftRecord{
		DraftID:            "22222222-2222-2222-2222-222222222222",
		Version:            3,
		Owner:              storage.BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"},
		Status:             storage.BatchDraftStatusSubmitted,
		SubmittedRootJobID: root.ID,
		Items:              []storage.BatchDraftItem{},
	}
	jobsService := &runtimeJobsDouble{
		submitBatchResult: jobs.SubmitBatchDraftResult{Draft: draft, Job: root, Reused: true},
	}
	store := &runtimeStorageDouble{
		jobs:       map[string]storage.JobRecord{root.ID: root},
		sourceSets: map[string]storage.SourceSetRecord{sourceSet.ID: sourceSet},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			sourceSet.ID: {},
		},
		artifactsByJob:   map[string][]storage.ArtifactRecord{},
		childrenByParent: map[string][]storage.JobRecord{},
	}
	service := newPublicRuntimeService(jobsService, store, &runtimeEventsDouble{})

	response, err := service.SubmitBatchDraft(context.Background(), BatchDraftSubmitCommand{
		DraftID:         draft.DraftID,
		Owner:           BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"},
		ExpectedVersion: 3,
	})
	if err != nil {
		t.Fatalf("SubmitBatchDraft() error = %v", err)
	}
	if response.StaleOutcome != "draft_submitted" {
		t.Fatalf("stale_outcome = %q, want draft_submitted", response.StaleOutcome)
	}
	if response.Job == nil || response.Job.JobID != root.ID {
		t.Fatalf("job = %#v, want root %q", response.Job, root.ID)
	}
	if jobsService.submitBatch.DraftID != draft.DraftID || jobsService.submitBatch.ExpectedVersion != 3 {
		t.Fatalf("submit request = %#v", jobsService.submitBatch)
	}
	if jobsService.submitBatch.Delivery.Strategy != storage.DeliveryStrategyPolling {
		t.Fatalf("submit delivery = %#v, want default polling", jobsService.submitBatch.Delivery)
	}
}

func TestApiHttpRuntimeCreateReportAcceptsSucceededBatchAggregateRoot(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	report := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeAgentRun, "queued", 1)
	report.RootJobID = root.ID
	report.ParentJobID = root.ID
	report.SourceSetID = "55555555-5555-5555-5555-555555555555"

	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{root.ID: root, report.ID: report},
		sourceSets: map[string]storage.SourceSetRecord{
			root.SourceSetID: {
				ID:        root.SourceSetID,
				InputKind: storage.SourceSetInputBatchTranscription,
				Items: []storage.SourceSetItem{
					{Position: 0, SourceID: "source-a", SourceLabel: "voice_a", SourceLabelVersion: "batch-transcription.v1"},
				},
				CreatedAt: root.CreatedAt,
			},
			report.SourceSetID: {ID: report.SourceSetID, InputKind: storage.SourceSetInputAgentRun, CreatedAt: report.CreatedAt},
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{report.SourceSetID: nil},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			root.ID: {
				runtimeArtifact("33333333-3333-3333-3333-333333333333", root.ID, "transcript_segmented_markdown", "batch-transcript.md", "artifacts/root/transcript/segmented/batch-transcript.md"),
			},
			report.ID: nil,
		},
		childrenByParent: map[string][]storage.JobRecord{root.ID: nil, report.ID: nil},
	}
	jobsService := &runtimeJobsDouble{createAgentRunResult: jobs.ChildResult{Job: report}}
	eventsService := &runtimeEventsDouble{}
	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshot, err := service.CreateReport(context.Background(), root.ID, ChildCreateRequest{IdempotencyKey: "report-from-batch"})
	if err != nil {
		t.Fatalf("CreateReport(batch root) error = %v", err)
	}
	if snapshot.JobID != report.ID || snapshot.JobType != queue.JobTypeAgentRun {
		t.Fatalf("snapshot = %#v, want report agent_run %q", snapshot, report.ID)
	}
	if jobsService.createAgentRun.ParentJobID != root.ID {
		t.Fatalf("agent_run parent = %q, want batch root %q", jobsService.createAgentRun.ParentJobID, root.ID)
	}
}

func TestApiHttpRuntimeCreateReportCreatesClaudeCodeAgentRunWithTranscriptInput(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	agentRun := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeAgentRun, "queued", 1)
	agentRun.RootJobID = root.ID
	agentRun.ParentJobID = root.ID
	agentRun.SourceSetID = "55555555-5555-5555-5555-555555555555"

	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{root.ID: root, agentRun.ID: agentRun},
		sourceSets: map[string]storage.SourceSetRecord{
			root.SourceSetID:     runtimeSourceSet(root.SourceSetID, storage.SourceSetInputSingleSource),
			agentRun.SourceSetID: {ID: agentRun.SourceSetID, InputKind: storage.SourceSetInputAgentRun, CreatedAt: agentRun.CreatedAt},
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			root.SourceSetID:     nil,
			agentRun.SourceSetID: nil,
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			root.ID: {
				runtimeArtifact("33333333-3333-3333-3333-333333333333", root.ID, "transcript_segmented_markdown", "transcript.md", "artifacts/root/transcript/segmented/transcript.md"),
			},
			agentRun.ID: nil,
		},
		childrenByParent: map[string][]storage.JobRecord{root.ID: nil, agentRun.ID: nil},
	}
	jobsService := &runtimeJobsDouble{
		createAgentRunResult: jobs.ChildResult{Job: agentRun},
	}
	eventsService := &runtimeEventsDouble{}

	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshot, err := service.CreateReport(context.Background(), root.ID, ChildCreateRequest{IdempotencyKey: "report-idem"})
	if err != nil {
		t.Fatalf("CreateReport() error = %v", err)
	}
	if snapshot.JobID != agentRun.ID || snapshot.JobType != queue.JobTypeAgentRun {
		t.Fatalf("snapshot = %#v, want agent_run job %q", snapshot, agentRun.ID)
	}
	if jobsService.createAgentRun.ParentJobID != root.ID || jobsService.createAgentRun.HarnessName != agentRunHarnessClaudeCode {
		t.Fatalf("agent_run request = %#v, want parent %q harness %q", jobsService.createAgentRun, root.ID, agentRunHarnessClaudeCode)
	}
	if jobsService.createAgentRun.SubmissionKind != submissionKindAgentRunCreate || jobsService.createAgentRun.IdempotencyKey != "report-idem" {
		t.Fatalf("agent_run submission = %#v, want report idempotent agent_run_create", jobsService.createAgentRun)
	}
	if jobsService.createChild.ChildJobType != "" {
		t.Fatalf("CreateReport used dedicated child job path: %#v", jobsService.createChild)
	}
	paramsText := string(jobsService.createAgentRun.ParamsJSON)
	if strings.Contains(paramsText, agentRunOperationReport) || strings.Contains(paramsText, "Create report") || strings.Contains(paramsText, "artifacts/root") {
		t.Fatalf("agent_run params leaked private report request data: %s", paramsText)
	}
	envelopeText := string(store.persistedAgentRunRequestBody)
	for _, required := range []string{`"harness_name":"claude-code"`, `"operation":"report"`, `"report_markdown"`, `"report_docx"`, `"artifact_id":"33333333-3333-3333-3333-333333333333"`} {
		if !strings.Contains(envelopeText, required) {
			t.Fatalf("private envelope missing %q: %s", required, envelopeText)
		}
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
}

func TestApiHttpRuntimeCreateReportReusesCanonicalAgentRunWithoutExtraCreatedEvent(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	report := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeAgentRun, "running", 2)
	report.RootJobID = root.ID
	report.ParentJobID = root.ID
	report.SourceSetID = "55555555-5555-5555-5555-555555555555"

	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{root.ID: root, report.ID: report},
		sourceSets: map[string]storage.SourceSetRecord{
			report.SourceSetID: {ID: report.SourceSetID, InputKind: storage.SourceSetInputAgentRun, CreatedAt: report.CreatedAt},
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{report.SourceSetID: nil},
		artifactsByJob:     map[string][]storage.ArtifactRecord{report.ID: nil},
		childrenByParent:   map[string][]storage.JobRecord{root.ID: {report}, report.ID: nil},
	}
	jobsService := &runtimeJobsDouble{}
	eventsService := &runtimeEventsDouble{}

	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshot, err := service.CreateReport(context.Background(), root.ID, ChildCreateRequest{})
	if err != nil {
		t.Fatalf("CreateReport() error = %v", err)
	}
	if snapshot.JobID != report.ID || snapshot.JobType != queue.JobTypeAgentRun {
		t.Fatalf("snapshot = %#v, want reused agent_run %q", snapshot, report.ID)
	}
	if jobsService.createAgentRun.HarnessName != "" || jobsService.createChild.ChildJobType != "" {
		t.Fatalf("canonical reuse should not create jobs: agent=%#v child=%#v", jobsService.createAgentRun, jobsService.createChild)
	}
	if got := len(eventsService.requests); got != 0 {
		t.Fatalf("event count = %d, want 0 for canonical agent_run reuse", got)
	}
}

func TestApiHttpRuntimeCreateAgentRunEmitsCreatedEventAndReturnsMetadataSnapshot(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeAgentRun, "queued", 1)
	job.ParamsJSON = []byte(`{"harness_name":"generic","request":{"prompt_sha256":"abc","prompt_bytes":9,"prompt_runes":9,"payload":{"json_type":"object","sha256":"def","bytes":17},"input_artifacts":[{"artifact_id":"artifact-1"}]}}`)
	sourceSet := storage.SourceSetRecord{
		ID:        job.SourceSetID,
		InputKind: storage.SourceSetInputAgentRun,
		CreatedAt: job.CreatedAt,
	}
	store := &runtimeStorageDouble{
		jobs:               map[string]storage.JobRecord{job.ID: job},
		sourceSets:         map[string]storage.SourceSetRecord{sourceSet.ID: sourceSet},
		orderedSourcesByID: map[string][]storage.OrderedSource{sourceSet.ID: nil},
		artifactsByJob:     map[string][]storage.ArtifactRecord{job.ID: nil},
		childrenByParent:   map[string][]storage.JobRecord{job.ID: nil},
	}
	jobsService := &runtimeJobsDouble{
		createAgentRunResult: jobs.ChildResult{Job: job},
	}
	eventsService := &runtimeEventsDouble{}
	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshot, err := service.CreateAgentRun(context.Background(), AgentRunCommand{
		IdempotencyKey: "agent-idem",
		HarnessName:    "generic",
		ClientRef:      "client-1",
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		Request: AgentRunRequest{
			Prompt:  "summarize this meeting",
			Payload: json.RawMessage(`{"topic":"calls","session_token":"secret-value"}`),
			InputArtifacts: []AgentRunInputArtifact{
				{ArtifactID: "artifact-1", ArtifactKind: "transcript_plain", Filename: "../input.txt"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateAgentRun() error = %v", err)
	}
	if snapshot.JobID != job.ID || snapshot.JobType != queue.JobTypeAgentRun {
		t.Fatalf("snapshot = %#v, want agent_run job %q", snapshot, job.ID)
	}
	if snapshot.SourceSet.InputKind != storage.SourceSetInputAgentRun || len(snapshot.SourceSet.Items) != 0 {
		t.Fatalf("source set = %#v, want empty agent_run source set", snapshot.SourceSet)
	}
	if jobsService.createAgentRun.SubmissionKind != submissionKindAgentRunCreate {
		t.Fatalf("submission kind = %q, want %q", jobsService.createAgentRun.SubmissionKind, submissionKindAgentRunCreate)
	}
	if jobsService.createAgentRun.HarnessName != "generic" {
		t.Fatalf("harness name = %q, want generic", jobsService.createAgentRun.HarnessName)
	}
	if store.persistedAgentRunRequestRef == "" || len(store.persistedAgentRunRequestBody) == 0 {
		t.Fatalf("private agent_run request envelope was not persisted: ref=%q bytes=%d", store.persistedAgentRunRequestRef, len(store.persistedAgentRunRequestBody))
	}
	paramsText := string(jobsService.createAgentRun.ParamsJSON)
	if strings.Contains(paramsText, "secret-value") || strings.Contains(paramsText, "summarize this meeting") || strings.Contains(paramsText, "topic") || strings.Contains(paramsText, "calls") || strings.Contains(paramsText, "../input.txt") || strings.Contains(paramsText, "input.txt") {
		t.Fatalf("params leaked raw request content: %s", paramsText)
	}
	if !strings.Contains(paramsText, "prompt_sha256") || !strings.Contains(paramsText, "payload") || !strings.Contains(paramsText, "json_type") || !strings.Contains(paramsText, "filename_sha256") || !strings.Contains(paramsText, "request_ref") || !strings.Contains(paramsText, "request_digest_sha256") {
		t.Fatalf("params missing normalized metadata: %s", paramsText)
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
}

func TestApiHttpRuntimeAgentRunFingerprintUsesRawRequestBeforeRedaction(t *testing.T) {
	t.Parallel()

	base := AgentRunCommand{
		HarnessName: "generic",
		Delivery:    DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		Request: AgentRunRequest{
			Payload: json.RawMessage(`{"session_token":"token-a"}`),
		},
	}
	changedSecret := base
	changedSecret.Request.Payload = json.RawMessage(`{"session_token":"token-b"}`)

	if fingerprintAgentRunCommand(base) == fingerprintAgentRunCommand(changedSecret) {
		t.Fatal("fingerprint collapsed different raw requests after redaction")
	}

	paramsA, err := agentRunParamsJSON(base, "agentreq_a", "digest-a", 123)
	if err != nil {
		t.Fatalf("agentRunParamsJSON(base) error = %v", err)
	}
	paramsB, err := agentRunParamsJSON(changedSecret, "agentreq_b", "digest-b", 123)
	if err != nil {
		t.Fatalf("agentRunParamsJSON(changedSecret) error = %v", err)
	}
	if strings.Contains(string(paramsA), "token-a") || strings.Contains(string(paramsB), "token-b") {
		t.Fatalf("params leaked raw secret values: %s / %s", paramsA, paramsB)
	}
}

func TestApiHttpRuntimeAgentRunParamsHashArtifactLocators(t *testing.T) {
	t.Parallel()

	params, err := agentRunParamsJSON(AgentRunCommand{
		HarnessName: "generic",
		Request: AgentRunRequest{
			InputArtifacts: []AgentRunInputArtifact{
				{
					ObjectKey: "tenants/acme/private/input.md",
					URI:       "https://files.example.test/private/input.md?token=secret",
					Filename:  "secret-input.md",
				},
			},
		},
	}, "agentreq_x", "digest-x", 456)
	if err != nil {
		t.Fatalf("agentRunParamsJSON() error = %v", err)
	}
	text := string(params)
	for _, forbidden := range []string{"tenants/acme/private/input.md", "token=secret", "/private/input.md", "secret-input.md"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("params leaked raw artifact locator %q: %s", forbidden, text)
		}
	}
	for _, required := range []string{"object_key_sha256", "uri", "files.example.test", "filename_sha256"} {
		if !strings.Contains(text, required) {
			t.Fatalf("params missing locator metadata %q: %s", required, text)
		}
	}
}

func TestApiHttpRuntimeCreateDeepResearchCreatesAgentRunFromReportAgentRun(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	report := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeAgentRun, "succeeded", 4)
	report.RootJobID = root.ID
	report.ParentJobID = root.ID
	report.SourceSetID = "55555555-5555-5555-5555-555555555555"
	deepResearch := runtimeJobRecord("33333333-3333-3333-3333-333333333333", queue.JobTypeAgentRun, "queued", 1)
	deepResearch.RootJobID = root.ID
	deepResearch.ParentJobID = report.ID
	deepResearch.SourceSetID = "66666666-6666-6666-6666-666666666666"

	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{root.ID: root, report.ID: report, deepResearch.ID: deepResearch},
		sourceSets: map[string]storage.SourceSetRecord{
			report.SourceSetID:       {ID: report.SourceSetID, InputKind: storage.SourceSetInputAgentRun, CreatedAt: report.CreatedAt},
			deepResearch.SourceSetID: {ID: deepResearch.SourceSetID, InputKind: storage.SourceSetInputAgentRun, CreatedAt: deepResearch.CreatedAt},
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{report.SourceSetID: nil, deepResearch.SourceSetID: nil},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			root.ID: {
				runtimeArtifact("77777777-7777-7777-7777-777777777777", root.ID, "transcript_segmented_markdown", "transcript.md", "artifacts/root/transcript/segmented/transcript.md"),
			},
			report.ID: {
				runtimeArtifact("88888888-8888-8888-8888-888888888888", report.ID, "report_markdown", "report.md", "artifacts/report/report/markdown/report.md"),
			},
			deepResearch.ID: nil,
		},
		childrenByParent: map[string][]storage.JobRecord{report.ID: nil, deepResearch.ID: nil},
	}
	jobsService := &runtimeJobsDouble{
		createAgentRunResult: jobs.ChildResult{Job: deepResearch},
	}
	eventsService := &runtimeEventsDouble{}

	service := newPublicRuntimeService(jobsService, store, eventsService)

	snapshot, err := service.CreateDeepResearch(context.Background(), report.ID, ChildCreateRequest{IdempotencyKey: "deep-idem"})
	if err != nil {
		t.Fatalf("CreateDeepResearch() error = %v", err)
	}
	if snapshot.JobID != deepResearch.ID || snapshot.JobType != queue.JobTypeAgentRun {
		t.Fatalf("snapshot = %#v, want agent_run job %q", snapshot, deepResearch.ID)
	}
	if jobsService.createAgentRun.ParentJobID != report.ID || jobsService.createAgentRun.HarnessName != agentRunHarnessClaudeCode {
		t.Fatalf("agent_run request = %#v, want parent report agent_run %q", jobsService.createAgentRun, report.ID)
	}
	if jobsService.createChild.ChildJobType != "" {
		t.Fatalf("CreateDeepResearch used dedicated child job path: %#v", jobsService.createChild)
	}
	paramsText := string(jobsService.createAgentRun.ParamsJSON)
	if strings.Contains(paramsText, agentRunOperationDeepResearch) || strings.Contains(paramsText, "deep_research_markdown") || strings.Contains(paramsText, "artifacts/report") {
		t.Fatalf("agent_run params leaked private deep research request data: %s", paramsText)
	}
	envelopeText := string(store.persistedAgentRunRequestBody)
	for _, required := range []string{`"harness_name":"claude-code"`, `"operation":"deep_research"`, `"deep_research_markdown"`, `"artifact_id":"77777777-7777-7777-7777-777777777777"`, `"artifact_id":"88888888-8888-8888-8888-888888888888"`} {
		if !strings.Contains(envelopeText, required) {
			t.Fatalf("private envelope missing %q: %s", required, envelopeText)
		}
	}
}

func TestApiHttpRuntimeWorkerLifecycleUsesAuthoritativeExecutionState(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "running", 2)
	execution := storage.JobExecutionRecord{
		ExecutionID: "33333333-3333-3333-3333-333333333333",
		JobID:       job.ID,
		WorkerKind:  "transcription",
		TaskType:    "transcription.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	sourceSet := runtimeSourceSet(job.SourceSetID, storage.SourceSetInputSingleSource)
	orderedSources := []storage.OrderedSource{
		{
			Position: 0,
			Source: storage.SourceRecord{
				ID:               "22222222-2222-2222-2222-222222222222",
				SourceKind:       storage.SourceKindUploadedFile,
				DisplayName:      "Clip",
				OriginalFilename: "clip.mp3",
				ObjectKey:        "sources/22222222-2222-2222-2222-222222222222/original/clip.mp3",
				SizeBytes:        42,
				CreatedAt:        time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			},
		},
	}

	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{job.ID: job},
		sourceSets: map[string]storage.SourceSetRecord{
			sourceSet.ID: sourceSet,
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			sourceSet.ID: orderedSources,
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			job.ID: nil,
		},
		childrenByParent: map[string][]storage.JobRecord{
			job.ID: nil,
		},
		claimResult: storage.ClaimJobExecutionResult{
			Job:       job,
			Execution: &execution,
			Claimed:   true,
		},
		activeExecutions: map[string]storage.JobExecutionRecord{
			job.ID + ":" + execution.ExecutionID: execution,
		},
	}
	jobsService := &runtimeJobsDouble{
		transitionResult: storage.JobRecord{
			ID:              job.ID,
			RootJobID:       job.RootJobID,
			SourceSetID:     job.SourceSetID,
			JobType:         job.JobType,
			Status:          "succeeded",
			Delivery:        job.Delivery,
			Version:         5,
			ProgressStage:   "completed",
			ProgressMessage: "done",
			ParamsJSON:      []byte(`{"language":"ru"}`),
			CreatedAt:       job.CreatedAt,
			StartedAt:       job.StartedAt,
			FinishedAt:      ptrTime(time.Date(2026, 4, 23, 10, 10, 0, 0, time.UTC)),
		},
	}
	eventsService := &runtimeEventsDouble{}
	service := newWorkerRuntimeService(jobsService, store, eventsService)
	service.now = func() time.Time { return time.Date(2026, 4, 23, 10, 5, 0, 0, time.UTC) }
	service.nextID = func() string { return "artifact-uuid" }

	claimResponse, err := service.Claim(context.Background(), job.ID, ClaimRequest{
		WorkerKind: "transcription",
		TaskType:   "transcription.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if claimResponse.ExecutionID != execution.ExecutionID {
		t.Fatalf("execution_id = %q, want %q", claimResponse.ExecutionID, execution.ExecutionID)
	}
	if got := len(claimResponse.OrderedInputs); got != 1 {
		t.Fatalf("ordered input count = %d, want 1", got)
	}

	progressMessage := "halfway"
	if err := service.RecordProgress(context.Background(), job.ID, ProgressRequest{
		ExecutionID:     execution.ExecutionID,
		ProgressStage:   "transcribing",
		ProgressMessage: &progressMessage,
	}); err != nil {
		t.Fatalf("RecordProgress() error = %v", err)
	}
	if got := store.jobs[job.ID].Version; got != 3 {
		t.Fatalf("job version after progress = %d, want 3", got)
	}

	if err := service.RecordArtifacts(context.Background(), job.ID, ArtifactUpsertRequest{
		ExecutionID: execution.ExecutionID,
		Artifacts: []ArtifactDescriptor{
			{ArtifactKind: "transcript_plain", Filename: "transcript.txt", MIMEType: "text/plain", ObjectKey: "artifacts/job/transcript/plain/transcript.txt", SizeBytes: 10},
			{ArtifactKind: "transcript_segmented_markdown", Filename: "transcript.md", MIMEType: "text/markdown", ObjectKey: "artifacts/job/transcript/segmented/transcript.md", SizeBytes: 20},
			{ArtifactKind: "transcript_docx", Filename: "transcript.docx", MIMEType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document", ObjectKey: "artifacts/job/transcript/docx/transcript.docx", SizeBytes: 30},
		},
	}); err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	if got := len(store.upsertArtifacts); got != 3 {
		t.Fatalf("upserted artifact count = %d, want 3", got)
	}
	if got := store.jobs[job.ID].Version; got != 4 {
		t.Fatalf("job version after artifact upsert = %d, want 4", got)
	}

	finalProgress := "completed"
	finalMessage := "done"
	finalSnapshot, err := service.Finalize(context.Background(), job.ID, FinalizeRequest{
		ExecutionID:     execution.ExecutionID,
		Outcome:         "succeeded",
		ProgressStage:   &finalProgress,
		ProgressMessage: &finalMessage,
	})
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if finalSnapshot.Status != "succeeded" {
		t.Fatalf("final snapshot status = %q, want succeeded", finalSnapshot.Status)
	}
	if got := len(store.finishedExecutions); got != 1 {
		t.Fatalf("finished execution count = %d, want 1", got)
	}
	if got := len(eventsService.requests); got < 3 {
		t.Fatalf("event count = %d, want at least 3", got)
	}
	if jobsService.transitionRequest.ToStatus != "succeeded" {
		t.Fatalf("transition target = %q, want succeeded", jobsService.transitionRequest.ToStatus)
	}

	store.jobs[job.ID] = storage.JobRecord{
		ID:                job.ID,
		RootJobID:         job.RootJobID,
		SourceSetID:       job.SourceSetID,
		JobType:           job.JobType,
		Status:            "cancel_requested",
		Delivery:          job.Delivery,
		Version:           6,
		ProgressStage:     "rendering_artifacts",
		CancelRequestedAt: ptrTime(time.Date(2026, 4, 23, 10, 11, 0, 0, time.UTC)),
		CreatedAt:         job.CreatedAt,
	}
	store.activeExecutions[job.ID+":"+execution.ExecutionID] = execution

	cancelState, err := service.CancelCheck(context.Background(), job.ID, execution.ExecutionID)
	if err != nil {
		t.Fatalf("CancelCheck() error = %v", err)
	}
	if !cancelState.CancelRequested || cancelState.Status != "cancel_requested" {
		t.Fatalf("cancel state = %+v, want cancel_requested", cancelState)
	}
}

func TestApiHttpRuntimeClaimUsesTranscriptArtifactForReportChild(t *testing.T) {
	t.Parallel()

	parent := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	child := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeReport, "running", 2)
	child.RootJobID = parent.ID
	child.ParentJobID = parent.ID
	execution := storage.JobExecutionRecord{
		ExecutionID: "33333333-3333-3333-3333-333333333333",
		JobID:       child.ID,
		WorkerKind:  "report",
		TaskType:    "report.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{
			parent.ID: parent,
			child.ID:  child,
		},
		orderedSourcesByID: map[string][]storage.OrderedSource{
			parent.SourceSetID: {
				{Position: 0, Source: storage.SourceRecord{ID: "source-a", SourceKind: storage.SourceKindUploadedFile}},
				{Position: 1, Source: storage.SourceRecord{ID: "source-b", SourceKind: storage.SourceKindUploadedFile}},
			},
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			parent.ID: {
				runtimeArtifact("artifact-transcript", parent.ID, "transcript_segmented_markdown", "transcript.md", "artifacts/parent/transcript/segmented/transcript.md"),
			},
		},
		claimResult: storage.ClaimJobExecutionResult{Job: child, Execution: &execution, Claimed: true},
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	claimResponse, err := service.Claim(context.Background(), child.ID, ClaimRequest{
		WorkerKind: "report",
		TaskType:   "report.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if got := len(claimResponse.OrderedInputs); got != 1 {
		t.Fatalf("ordered input count = %d, want 1", got)
	}
	input := claimResponse.OrderedInputs[0]
	if input.SourceID != "artifact-transcript" || input.ObjectKey == nil || *input.ObjectKey != "artifacts/parent/transcript/segmented/transcript.md" {
		t.Fatalf("report claim input = %#v, want transcript artifact", input)
	}
}

func TestApiHttpRuntimeClaimUsesTranscriptAndReportArtifactsForDeepResearchChild(t *testing.T) {
	t.Parallel()

	root := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeTranscription, "succeeded", 4)
	report := runtimeJobRecord("22222222-2222-2222-2222-222222222222", queue.JobTypeReport, "succeeded", 4)
	report.RootJobID = root.ID
	report.ParentJobID = root.ID
	deepResearch := runtimeJobRecord("33333333-3333-3333-3333-333333333333", queue.JobTypeDeepResearch, "running", 2)
	deepResearch.RootJobID = root.ID
	deepResearch.ParentJobID = report.ID
	execution := storage.JobExecutionRecord{
		ExecutionID: "44444444-4444-4444-4444-444444444444",
		JobID:       deepResearch.ID,
		WorkerKind:  "deep_research",
		TaskType:    "deep_research.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{
			root.ID:         root,
			report.ID:       report,
			deepResearch.ID: deepResearch,
		},
		artifactsByJob: map[string][]storage.ArtifactRecord{
			root.ID: {
				runtimeArtifact("artifact-transcript", root.ID, "transcript_segmented_markdown", "transcript.md", "artifacts/root/transcript/segmented/transcript.md"),
			},
			report.ID: {
				runtimeArtifact("artifact-report", report.ID, "report_markdown", "report.md", "artifacts/report/report/markdown/report.md"),
			},
		},
		claimResult: storage.ClaimJobExecutionResult{Job: deepResearch, Execution: &execution, Claimed: true},
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	claimResponse, err := service.Claim(context.Background(), deepResearch.ID, ClaimRequest{
		WorkerKind: "deep_research",
		TaskType:   "deep_research.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if got := len(claimResponse.OrderedInputs); got != 2 {
		t.Fatalf("ordered input count = %d, want 2", got)
	}
	if claimResponse.OrderedInputs[0].SourceID != "artifact-transcript" {
		t.Fatalf("first deep-research input = %#v, want transcript artifact", claimResponse.OrderedInputs[0])
	}
	if claimResponse.OrderedInputs[1].SourceID != "artifact-report" {
		t.Fatalf("second deep-research input = %#v, want report artifact", claimResponse.OrderedInputs[1])
	}
}

func TestApiHttpRuntimeClaimSupportsAgentRunWithoutOrderedInputs(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", queue.JobTypeAgentRun, "running", 2)
	job.ParamsJSON = []byte(`{"harness_name":"generic","request":{"prompt_sha256":"abc","payload":{"mode":"check"}}}`)
	execution := storage.JobExecutionRecord{
		ExecutionID: "33333333-3333-3333-3333-333333333333",
		JobID:       job.ID,
		WorkerKind:  "agent_runner",
		TaskType:    "agent_run.run",
		ClaimedAt:   time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC),
	}
	store := &runtimeStorageDouble{
		jobs:        map[string]storage.JobRecord{job.ID: job},
		claimResult: storage.ClaimJobExecutionResult{Job: job, Execution: &execution, Claimed: true},
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	claimResponse, err := service.Claim(context.Background(), job.ID, ClaimRequest{
		WorkerKind: "agent_runner",
		TaskType:   "agent_run.run",
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if got := len(claimResponse.OrderedInputs); got != 0 {
		t.Fatalf("ordered input count = %d, want 0", got)
	}
	if claimResponse.Params["harness_name"] != "generic" {
		t.Fatalf("params = %#v, want harness_name generic", claimResponse.Params)
	}
}

func TestApiHttpRuntimeWorkerLifecycleRejectsStaleExecution(t *testing.T) {
	t.Parallel()

	store := &runtimeStorageDouble{
		activeExecutionErr: storage.ErrExecutionNotFound,
	}
	service := newWorkerRuntimeService(&runtimeJobsDouble{}, store, &runtimeEventsDouble{})

	err := service.RecordProgress(context.Background(), "11111111-1111-1111-1111-111111111111", ProgressRequest{
		ExecutionID:   "22222222-2222-2222-2222-222222222222",
		ProgressStage: "transcribing",
	})
	if err == nil {
		t.Fatal("expected stale execution error")
	}
	var typed apiError
	if !errors.As(err, &typed) {
		t.Fatalf("error type = %T, want apiError", err)
	}
	if typed.code != "execution_not_found" {
		t.Fatalf("error code = %q, want execution_not_found", typed.code)
	}
}

func TestApiHttpJobsEventBridgeUsesAuthoritativeJobSnapshot(t *testing.T) {
	t.Parallel()

	job := runtimeJobRecord("11111111-1111-1111-1111-111111111111", "transcription", "running", 2)
	store := &runtimeStorageDouble{
		jobs: map[string]storage.JobRecord{job.ID: job},
	}
	eventsService := &runtimeEventsDouble{}
	bridge := newJobsEventBridge(store, eventsService)

	if err := bridge.Emit(context.Background(), jobs.Event{
		JobID:     job.ID,
		EventType: "job.updated",
		ToStatus:  "cancel_requested",
		Version:   job.Version,
	}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if got, want := len(eventsService.requests), 1; got != want {
		t.Fatalf("event count = %d, want %d", got, want)
	}
	if eventsService.requests[0].Payload.Status != "cancel_requested" {
		t.Fatalf("payload status = %q, want cancel_requested", eventsService.requests[0].Payload.Status)
	}
}

type runtimeJobsDouble struct {
	createJobsResult     jobs.CreateJobsResult
	createCombinedResult jobs.ChildResult
	createBatchResult    jobs.ChildResult
	submitBatchResult    jobs.SubmitBatchDraftResult
	createAgentRunResult jobs.ChildResult
	createChildResult    jobs.ChildResult
	cancelResult         storage.JobRecord
	retryResult          jobs.RetryResult
	transitionResult     storage.JobRecord

	createUpload      jobs.CreateTranscriptionRequest
	createCombined    jobs.CreateCombinedTranscriptionRequest
	createBatch       jobs.CreateBatchTranscriptionRequest
	submitBatch       jobs.SubmitBatchDraftRequest
	createAgentRun    jobs.CreateAgentRunRequest
	createChild       jobs.CreateChildRequest
	cancelJobID       string
	retryJobID        string
	transitionRequest jobs.TransitionRequest
	scheduleRootJobID string
}

func (d *runtimeJobsDouble) CreateTranscriptionJobs(_ context.Context, req jobs.CreateTranscriptionRequest) (jobs.CreateJobsResult, error) {
	d.createUpload = req
	return d.createJobsResult, nil
}

func (d *runtimeJobsDouble) CreateCombinedTranscriptionJob(_ context.Context, req jobs.CreateCombinedTranscriptionRequest) (jobs.ChildResult, error) {
	d.createCombined = req
	return d.createCombinedResult, nil
}

func (d *runtimeJobsDouble) CreateBatchTranscriptionJob(_ context.Context, req jobs.CreateBatchTranscriptionRequest) (jobs.ChildResult, error) {
	d.createBatch = req
	return d.createBatchResult, nil
}

func (d *runtimeJobsDouble) SubmitBatchDraft(_ context.Context, req jobs.SubmitBatchDraftRequest) (jobs.SubmitBatchDraftResult, error) {
	d.submitBatch = req
	return d.submitBatchResult, nil
}

func (d *runtimeJobsDouble) CreateAgentRun(_ context.Context, req jobs.CreateAgentRunRequest) (jobs.ChildResult, error) {
	d.createAgentRun = req
	return d.createAgentRunResult, nil
}

func (d *runtimeJobsDouble) CreateChildJob(_ context.Context, req jobs.CreateChildRequest) (jobs.ChildResult, error) {
	d.createChild = req
	return d.createChildResult, nil
}

func (d *runtimeJobsDouble) CancelJob(_ context.Context, jobID string) (storage.JobRecord, error) {
	d.cancelJobID = jobID
	return d.cancelResult, nil
}

func (d *runtimeJobsDouble) RetryJob(_ context.Context, jobID string) (jobs.RetryResult, error) {
	d.retryJobID = jobID
	return d.retryResult, nil
}

func (d *runtimeJobsDouble) TransitionJob(_ context.Context, req jobs.TransitionRequest) (storage.JobRecord, error) {
	d.transitionRequest = req
	return d.transitionResult, nil
}

func (d *runtimeJobsDouble) ScheduleBatchAggregateIfReady(_ context.Context, rootJobID string) (bool, error) {
	d.scheduleRootJobID = rootJobID
	return true, nil
}

type runtimeStorageDouble struct {
	jobs               map[string]storage.JobRecord
	sourceSets         map[string]storage.SourceSetRecord
	orderedSourcesByID map[string][]storage.OrderedSource
	artifactsByJob     map[string][]storage.ArtifactRecord
	childrenByParent   map[string][]storage.JobRecord
	eventsByJob        map[string][]storage.JobEvent
	activeExecutions   map[string]storage.JobExecutionRecord

	listPage                     storage.JobListPage
	claimResult                  storage.ClaimJobExecutionResult
	activeExecutionErr           error
	finishedExecutions           []storage.FinishJobExecutionRequest
	upsertArtifacts              []storage.ArtifactRecord
	persistedAgentRunRequestRef  string
	persistedAgentRunRequestBody []byte
	agentRunRequestAccess        storage.AgentRunRequestAccess
	batchDrafts                  map[string]storage.BatchDraftRecord
}

func (d *runtimeStorageDouble) GetJob(_ context.Context, jobID string) (storage.JobRecord, error) {
	job, ok := d.jobs[jobID]
	if !ok {
		return storage.JobRecord{}, storage.ErrJobNotFound
	}
	return job, nil
}

func (d *runtimeStorageDouble) ListJobs(_ context.Context, _ storage.JobListFilter) (storage.JobListPage, error) {
	if d.listPage.Jobs != nil || d.listPage.HasMore {
		return d.listPage, nil
	}
	jobsPage := make([]storage.JobRecord, 0, len(d.jobs))
	for _, job := range d.jobs {
		jobsPage = append(jobsPage, job)
	}
	return storage.JobListPage{Jobs: jobsPage}, nil
}

func (d *runtimeStorageDouble) GetSourceSet(_ context.Context, sourceSetID string) (storage.SourceSetRecord, error) {
	return d.sourceSets[sourceSetID], nil
}

func (d *runtimeStorageDouble) ListOrderedSources(_ context.Context, sourceSetID string) ([]storage.OrderedSource, error) {
	return d.orderedSourcesByID[sourceSetID], nil
}

func (d *runtimeStorageDouble) ListArtifactsByJob(_ context.Context, jobID string) ([]storage.ArtifactRecord, error) {
	return append([]storage.ArtifactRecord(nil), d.artifactsByJob[jobID]...), nil
}

func (d *runtimeStorageDouble) ListChildJobs(_ context.Context, parentJobID string) ([]storage.JobRecord, error) {
	return append([]storage.JobRecord(nil), d.childrenByParent[parentJobID]...), nil
}

func (d *runtimeStorageDouble) ListJobEvents(_ context.Context, jobID string) ([]storage.JobEvent, error) {
	return append([]storage.JobEvent(nil), d.eventsByJob[jobID]...), nil
}

func (d *runtimeStorageDouble) ResolveArtifact(_ context.Context, artifactID string) (storage.ArtifactResolution, error) {
	return storage.ArtifactResolution{ArtifactID: artifactID}, nil
}

func (d *runtimeStorageDouble) ResolveArtifactForAudience(_ context.Context, artifactID string, audience storage.ArtifactDownloadAudience) (storage.ArtifactResolution, error) {
	return storage.ArtifactResolution{
		ArtifactID: artifactID,
		Download: storage.DownloadDescriptor{
			URL: string(audience),
		},
	}, nil
}

func (d *runtimeStorageDouble) PersistAgentRunRequest(_ context.Context, requestRef string, body []byte) error {
	d.persistedAgentRunRequestRef = requestRef
	d.persistedAgentRunRequestBody = append([]byte(nil), body...)
	return nil
}

func (d *runtimeStorageDouble) ResolveAgentRunRequestAccess(_ context.Context, _ storage.JobRecord) (storage.AgentRunRequestAccess, error) {
	return d.agentRunRequestAccess, nil
}

func (d *runtimeStorageDouble) ClaimJobExecution(_ context.Context, _ storage.ClaimJobExecutionRequest) (storage.ClaimJobExecutionResult, error) {
	return d.claimResult, nil
}

func (d *runtimeStorageDouble) GetActiveJobExecution(_ context.Context, jobID, executionID string) (storage.JobExecutionRecord, error) {
	if d.activeExecutionErr != nil {
		return storage.JobExecutionRecord{}, d.activeExecutionErr
	}
	execution, ok := d.activeExecutions[jobID+":"+executionID]
	if !ok {
		return storage.JobExecutionRecord{}, storage.ErrExecutionNotFound
	}
	return execution, nil
}

func (d *runtimeStorageDouble) FinishJobExecution(_ context.Context, req storage.FinishJobExecutionRequest) (storage.JobExecutionRecord, error) {
	d.finishedExecutions = append(d.finishedExecutions, req)
	key := req.JobID + ":" + req.ExecutionID
	execution := d.activeExecutions[key]
	finishedAt := req.FinishedAt
	execution.FinishedAt = &finishedAt
	execution.Outcome = req.Outcome
	delete(d.activeExecutions, key)
	return execution, nil
}

func (d *runtimeStorageDouble) UpsertArtifacts(_ context.Context, artifacts []storage.ArtifactRecord) error {
	d.upsertArtifacts = append([]storage.ArtifactRecord(nil), artifacts...)
	if d.artifactsByJob == nil {
		d.artifactsByJob = map[string][]storage.ArtifactRecord{}
	}
	for _, artifact := range artifacts {
		jobArtifacts := d.artifactsByJob[artifact.JobID]
		replaced := false
		for idx := range jobArtifacts {
			if jobArtifacts[idx].ArtifactKind == artifact.ArtifactKind {
				jobArtifacts[idx] = artifact
				replaced = true
				break
			}
		}
		if !replaced {
			jobArtifacts = append(jobArtifacts, artifact)
		}
		d.artifactsByJob[artifact.JobID] = jobArtifacts
	}
	return nil
}

func (d *runtimeStorageDouble) UpdateJob(_ context.Context, job storage.JobRecord) error {
	if d.jobs == nil {
		d.jobs = map[string]storage.JobRecord{}
	}
	d.jobs[job.ID] = job
	return nil
}

func (d *runtimeStorageDouble) FindArtifactByKind(_ context.Context, jobID, artifactKind string) (*storage.ArtifactRecord, error) {
	for _, artifact := range d.artifactsByJob[jobID] {
		if artifact.ArtifactKind == artifactKind {
			copied := artifact
			return &copied, nil
		}
	}
	return nil, nil
}

func (d *runtimeStorageDouble) CreateBatchDraft(_ context.Context, req storage.BatchDraftCreate) (storage.BatchDraftRecord, error) {
	draft := storage.BatchDraftRecord{
		DraftID:     "11111111-1111-1111-1111-111111111111",
		Version:     1,
		Owner:       req.Owner,
		Status:      storage.BatchDraftStatusOpen,
		DisplayName: req.DisplayName,
		ClientRef:   req.ClientRef,
		ExpiresAt:   req.ExpiresAt,
		Items:       []storage.BatchDraftItem{},
	}
	if d.batchDrafts == nil {
		d.batchDrafts = map[string]storage.BatchDraftRecord{}
	}
	d.batchDrafts[draft.DraftID] = draft
	return draft, nil
}

func (d *runtimeStorageDouble) GetBatchDraft(_ context.Context, draftID string, owner storage.BatchDraftOwner) (storage.BatchDraftRecord, error) {
	draft, ok := d.batchDrafts[draftID]
	if !ok {
		return storage.BatchDraftRecord{}, storage.ErrBatchDraftNotFound
	}
	if draft.Owner != owner {
		return storage.BatchDraftRecord{}, storage.ErrBatchDraftOwnerMismatch
	}
	return draft, nil
}

func (d *runtimeStorageDouble) AddBatchDraftItem(_ context.Context, req storage.BatchDraftItemMutation) (storage.BatchDraftRecord, error) {
	draft := d.batchDrafts[req.DraftID]
	draft.Version++
	d.batchDrafts[req.DraftID] = draft
	return draft, nil
}

func (d *runtimeStorageDouble) RemoveBatchDraftItem(_ context.Context, req storage.BatchDraftItemRemove) (storage.BatchDraftRecord, error) {
	draft := d.batchDrafts[req.DraftID]
	draft.Version++
	d.batchDrafts[req.DraftID] = draft
	return draft, nil
}

func (d *runtimeStorageDouble) ClearBatchDraft(_ context.Context, req storage.BatchDraftMutation) (storage.BatchDraftRecord, error) {
	draft := d.batchDrafts[req.DraftID]
	draft.Items = []storage.BatchDraftItem{}
	draft.Version++
	d.batchDrafts[req.DraftID] = draft
	return draft, nil
}

type runtimeEventsDouble struct {
	requests []ws.EmitRequest
}

func (d *runtimeEventsDouble) EmitJobEvent(_ context.Context, req ws.EmitRequest) (ws.EmitResult, error) {
	d.requests = append(d.requests, req)
	payload, _ := json.Marshal(req.Payload)
	return ws.EmitResult{
		Event: storage.JobEvent{
			ID:        "evt-1",
			JobID:     req.Job.ID,
			RootJobID: req.Job.RootJobID,
			EventType: req.EventType,
			Version:   req.Job.Version,
			Payload:   payload,
			CreatedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		},
		Envelope: ws.JobEventEnvelope{
			EventID:   "evt-1",
			EventType: req.EventType,
			JobID:     req.Job.ID,
			RootJobID: req.Job.RootJobID,
			Version:   req.Job.Version,
			Payload:   req.Payload,
		},
	}, nil
}

func runtimeJobRecord(jobID, jobType, status string, version int64) storage.JobRecord {
	startedAt := time.Date(2026, 4, 23, 10, 1, 0, 0, time.UTC)
	return storage.JobRecord{
		ID:              jobID,
		RootJobID:       jobID,
		SourceSetID:     "44444444-4444-4444-4444-444444444444",
		JobType:         jobType,
		Status:          status,
		Delivery:        storage.Delivery{Strategy: storage.DeliveryStrategyPolling},
		Version:         version,
		ProgressStage:   "running",
		ProgressMessage: "working",
		ParamsJSON:      []byte(`{"language":"ru"}`),
		CreatedAt:       time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		StartedAt:       &startedAt,
	}
}

func runtimeSourceSet(sourceSetID, inputKind string) storage.SourceSetRecord {
	return storage.SourceSetRecord{
		ID:        sourceSetID,
		InputKind: inputKind,
		CreatedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		Items: []storage.SourceSetItem{
			{Position: 0, SourceID: "22222222-2222-2222-2222-222222222222"},
		},
	}
}

func runtimeArtifact(id, jobID, artifactKind, filename, objectKey string) storage.ArtifactRecord {
	return storage.ArtifactRecord{
		ID:           id,
		JobID:        jobID,
		ArtifactKind: artifactKind,
		Filename:     filename,
		Format:       "markdown",
		MIMEType:     "text/markdown; charset=utf-8",
		ObjectKey:    objectKey,
		SizeBytes:    42,
		CreatedAt:    time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
