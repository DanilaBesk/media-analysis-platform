package api

import (
	"context"
	"encoding/json"
	"fmt"
	neturl "net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/jobs"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
)

func TestApiBatchTranscriptionE2EWithLivePostgresControlPlane(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("API_STORAGE_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("set API_STORAGE_POSTGRES_DSN to run live PostgreSQL batch control-plane E2E")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runtime := newLiveBatchE2ERuntime(t, ctx, dsn)

	root := runtime.createMixedBatch(t, ctx, "batch-idem-success", BatchCompletionPolicyAllSources)
	if root.Status != "queued" || root.Progress == nil || root.Progress.Stage != "waiting_for_sources" {
		t.Fatalf("root after create = status %q progress %#v, want queued waiting_for_sources", root.Status, root.Progress)
	}
	if got, want := len(root.Children), 2; got != want {
		t.Fatalf("root children = %d, want %d", got, want)
	}
	assertBatchRootSourceLabels(t, root, []string{"voice_a", "video_b"})
	assertQueuedTaskTypes(t, runtime.queueClient.specs, []string{
		queue.TaskTypeTranscription,
		queue.TaskTypeTranscription,
	})

	reused := runtime.createMixedBatch(t, ctx, "batch-idem-success", BatchCompletionPolicyAllSources)
	if reused.JobID != root.JobID {
		t.Fatalf("reused batch root = %q, want %q", reused.JobID, root.JobID)
	}
	assertQueuedTaskTypes(t, runtime.queueClient.specs, []string{
		queue.TaskTypeTranscription,
		queue.TaskTypeTranscription,
	})

	children := runtime.children(t, ctx, root.JobID)
	runtime.claimFinalizeTranscription(t, ctx, children[0].JobID, []ArtifactDescriptor{
		e2eArtifact("transcript_plain", "voice-a.txt"),
		e2eArtifact("transcript_segmented_markdown", "voice-a.md"),
		e2eArtifact("transcript_docx", "voice-a.docx"),
	})
	afterFirstChild := runtime.snapshot(t, ctx, root.JobID)
	if afterFirstChild.Progress == nil || afterFirstChild.Progress.Stage != "waiting_for_sources" {
		t.Fatalf("root progress after one child = %#v, want waiting_for_sources", afterFirstChild.Progress)
	}

	runtime.claimFinalizeTranscription(t, ctx, children[1].JobID, []ArtifactDescriptor{
		e2eArtifact("transcript_plain", "video-b.txt"),
		e2eArtifact("transcript_segmented_markdown", "video-b.md"),
		e2eArtifact("transcript_docx", "video-b.docx"),
	})
	aggregateQueued := runtime.snapshot(t, ctx, root.JobID)
	if aggregateQueued.Progress == nil || aggregateQueued.Progress.Stage != "aggregate_queued" {
		t.Fatalf("root progress after all children = %#v, want aggregate_queued", aggregateQueued.Progress)
	}
	assertQueuedTaskTypes(t, runtime.queueClient.specs, []string{
		queue.TaskTypeTranscription,
		queue.TaskTypeTranscription,
		queue.TaskTypeTranscriptionAggregate,
	})

	runtime.claimFinalizeTranscription(t, ctx, root.JobID, []ArtifactDescriptor{
		e2eArtifact("transcript_plain", "batch.txt"),
		e2eArtifact("transcript_segmented_markdown", "batch.md"),
		e2eArtifact("transcript_docx", "batch.docx"),
		e2eArtifact("source_manifest_json", "source-manifest.json"),
		e2eArtifact("batch_diagnostics_json", "batch-diagnostics.json"),
	})
	succeededRoot := runtime.snapshot(t, ctx, root.JobID)
	if succeededRoot.Status != "succeeded" || succeededRoot.Progress == nil || succeededRoot.Progress.Stage != "completed" {
		t.Fatalf("root after aggregate finalize = status %q progress %#v, want succeeded completed", succeededRoot.Status, succeededRoot.Progress)
	}
	assertArtifactKinds(t, succeededRoot.Artifacts, []string{
		"batch_diagnostics_json",
		"source_manifest_json",
		"transcript_docx",
		"transcript_plain",
		"transcript_segmented_markdown",
	})

	report := runtime.createReport(t, ctx, succeededRoot.JobID, "report-idem")
	if report.JobType != queue.JobTypeAgentRun || ptrValue(report.ParentJobID) != succeededRoot.JobID {
		t.Fatalf("report job = %#v, want agent_run child of %q", report, succeededRoot.JobID)
	}
	reportAccess := runtime.claimAgentRun(t, ctx, report.JobID)
	assertAgentRequestAccess(t, runtime.objects, reportAccess, "report", []string{"report_markdown", "report_docx"})
	runtime.recordAndFinalizeAgentRun(t, ctx, report.JobID, reportAccess.ExecutionID, []ArtifactDescriptor{
		e2eArtifact("execution_log", "report-execution.log"),
		e2eArtifact("agent_result_json", "report-result.json"),
		e2eArtifact("report_markdown", "report.md"),
		e2eArtifact("report_docx", "report.docx"),
	})

	deepResearch := runtime.createDeepResearch(t, ctx, report.JobID, "deep-idem")
	if deepResearch.JobType != queue.JobTypeAgentRun || ptrValue(deepResearch.ParentJobID) != report.JobID {
		t.Fatalf("deep research job = %#v, want agent_run child of %q", deepResearch, report.JobID)
	}
	deepAccess := runtime.claimAgentRun(t, ctx, deepResearch.JobID)
	assertAgentRequestAccess(t, runtime.objects, deepAccess, "deep_research", []string{"deep_research_markdown"})
	runtime.recordAndFinalizeAgentRun(t, ctx, deepResearch.JobID, deepAccess.ExecutionID, []ArtifactDescriptor{
		e2eArtifact("execution_log", "deep-execution.log"),
		e2eArtifact("agent_result_json", "deep-result.json"),
		e2eArtifact("deep_research_markdown", "deep-research.md"),
	})

	cancelRoot := runtime.createMixedBatch(t, ctx, "batch-idem-cancel", BatchCompletionPolicyAllSources)
	canceled, err := runtime.public.CancelJob(ctx, cancelRoot.JobID)
	if err != nil {
		t.Fatalf("CancelJob(root) error = %v", err)
	}
	if canceled.Status != "canceled" {
		t.Fatalf("canceled root status = %q, want canceled", canceled.Status)
	}
	for _, child := range runtime.children(t, ctx, cancelRoot.JobID) {
		if child.Status != "canceled" {
			t.Fatalf("cancel cascaded child %q status = %q, want canceled", child.JobID, child.Status)
		}
	}

	failedRoot := runtime.createMixedBatch(t, ctx, "batch-idem-failed", BatchCompletionPolicyAnySource)
	failedChildren := runtime.children(t, ctx, failedRoot.JobID)
	runtime.claimFailTranscription(t, ctx, failedChildren[0].JobID, "source_fetch_failed", "telegram download failed")
	runtime.claimFailTranscription(t, ctx, failedChildren[1].JobID, "source_fetch_failed", "youtube transcript unavailable")
	failedRoot = runtime.snapshot(t, ctx, failedRoot.JobID)
	if failedRoot.Status != "failed" || failedRoot.LatestError == nil || failedRoot.LatestError.Code != "batch_sources_failed" {
		t.Fatalf("failed-source root = status %q latest_error %#v, want batch_sources_failed", failedRoot.Status, failedRoot.LatestError)
	}

	retry, err := runtime.public.RetryJob(ctx, failedRoot.JobID)
	if err != nil {
		t.Fatalf("RetryJob(failed root) error = %v", err)
	}
	if ptrValue(retry.RetryOfJobID) != failedRoot.JobID || retry.Progress == nil || retry.Progress.Stage != "aggregate_queued" {
		t.Fatalf("root retry = %#v, want aggregate retry linked to %q", retry, failedRoot.JobID)
	}
}

type liveBatchE2ERuntime struct {
	public      *publicRuntimeService
	worker      *workerRuntimeService
	objects     *agentRunIntegrationObjectStore
	queueClient *agentRunIntegrationQueueClient
}

type agentRunClaimAccess struct {
	ExecutionID string
	Access      storage.AgentRunRequestAccess
}

func newLiveBatchE2ERuntime(t *testing.T, ctx context.Context, dsn string) liveBatchE2ERuntime {
	t.Helper()

	dbName := "api_batch_transcription_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	adminDSN, err := batchE2EPostgresDSNWithDatabase(dsn, "postgres")
	if err != nil {
		t.Fatalf("batchE2EPostgresDSNWithDatabase(admin) error = %v", err)
	}
	testDSN, err := batchE2EPostgresDSNWithDatabase(dsn, dbName)
	if err != nil {
		t.Fatalf("batchE2EPostgresDSNWithDatabase(test) error = %v", err)
	}

	adminDB, err := storage.OpenPostgresDB(ctx, adminDSN)
	if err != nil {
		t.Fatalf("OpenPostgresDB(admin) error = %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+batchE2EQuoteSQLIdentifier(dbName)+" WITH (FORCE)")
		_ = adminDB.Close()
	})
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+batchE2EQuoteSQLIdentifier(dbName)); err != nil {
		t.Fatalf("CREATE DATABASE test db error = %v", err)
	}

	testDB, err := storage.OpenPostgresDB(ctx, testDSN)
	if err != nil {
		t.Fatalf("OpenPostgresDB(test) error = %v", err)
	}
	t.Cleanup(func() { _ = testDB.Close() })
	upSQL := batchE2EMigrationUpSQL(t)
	if _, err := testDB.ExecContext(ctx, upSQL); err != nil {
		t.Fatalf("migration up error = %v", err)
	}

	state, err := storage.NewSQLStateStore(testDB)
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}
	objects := newAgentRunIntegrationObjectStore()
	repo, err := storage.NewRepository(state, objects)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	queueClient := &agentRunIntegrationQueueClient{}
	publisher, err := queue.NewPublisher(queueClient)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	jobsService, err := jobs.NewService(repo, publisher)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	worker := newWorkerRuntimeService(jobsService, repo, nil)
	return liveBatchE2ERuntime{
		public:      newPublicRuntimeService(jobsService, repo, nil),
		worker:      worker,
		objects:     objects,
		queueClient: queueClient,
	}
}

func (r liveBatchE2ERuntime) createMixedBatch(t *testing.T, ctx context.Context, idempotencyKey string, completionPolicy string) JobSnapshot {
	t.Helper()

	root, err := r.public.CreateBatch(ctx, BatchCommand{
		IdempotencyKey: idempotencyKey,
		DisplayName:    "Mixed Telegram + YouTube batch",
		ClientRef:      "e2e-client",
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		Manifest: BatchSourceManifest{
			ManifestVersion:     batchManifestVersion,
			OrderedSourceLabels: []string{"voice_a", "video_b"},
			Sources: map[string]BatchSource{
				"voice_a": {
					SourceKind:       storage.SourceKindTelegramUpload,
					FilePart:         "voice_a",
					DisplayName:      "Telegram voice A",
					OriginalFilename: "voice-a.ogg",
				},
				"video_b": {
					SourceKind:  storage.SourceKindYouTubeURL,
					URL:         "https://youtu.be/batch-demo",
					DisplayName: "YouTube: batch demo",
				},
			},
			CompletionPolicy: completionPolicy,
		},
		ManifestJSON: []byte(`{"manifest_version":"batch-transcription.v1","ordered_source_labels":["voice_a","video_b"],"completion_policy":"` + completionPolicy + `"}`),
		Files: []BatchUploadFile{
			{
				PartName: "voice_a",
				UploadFile: UploadFile{
					Filename:    "voice-a.ogg",
					ContentType: "audio/ogg",
					SizeBytes:   int64(len("telegram voice bytes")),
					SHA256:      "sha-voice-a",
					Body:        []byte("telegram voice bytes"),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateBatch(%s) error = %v", idempotencyKey, err)
	}
	return root
}

func (r liveBatchE2ERuntime) claimFinalizeTranscription(t *testing.T, ctx context.Context, jobID string, artifacts []ArtifactDescriptor) {
	t.Helper()

	claim, err := r.worker.Claim(ctx, jobID, ClaimRequest{
		WorkerKind: "transcription",
		TaskType:   claimTaskTypeForTranscriptionJob(t, r.snapshot(t, ctx, jobID)),
	})
	if err != nil {
		t.Fatalf("Claim(%s) error = %v", jobID, err)
	}
	if err := r.worker.RecordProgress(ctx, jobID, ProgressRequest{
		ExecutionID:     claim.ExecutionID,
		ProgressStage:   "persisting_artifacts",
		ProgressMessage: stringPtr("persisting E2E artifacts"),
	}); err != nil {
		t.Fatalf("RecordProgress(%s) error = %v", jobID, err)
	}
	if err := r.worker.RecordArtifacts(ctx, jobID, ArtifactUpsertRequest{ExecutionID: claim.ExecutionID, Artifacts: artifacts}); err != nil {
		t.Fatalf("RecordArtifacts(%s) error = %v", jobID, err)
	}
	if _, err := r.worker.Finalize(ctx, jobID, FinalizeRequest{
		ExecutionID:     claim.ExecutionID,
		Outcome:         "succeeded",
		ProgressStage:   stringPtr("completed"),
		ProgressMessage: stringPtr("E2E completed"),
	}); err != nil {
		t.Fatalf("Finalize(%s) error = %v", jobID, err)
	}
}

func (r liveBatchE2ERuntime) claimFailTranscription(t *testing.T, ctx context.Context, jobID string, code string, message string) {
	t.Helper()

	claim, err := r.worker.Claim(ctx, jobID, ClaimRequest{WorkerKind: "transcription", TaskType: queue.TaskTypeTranscription})
	if err != nil {
		t.Fatalf("Claim(%s) error = %v", jobID, err)
	}
	if _, err := r.worker.Finalize(ctx, jobID, FinalizeRequest{
		ExecutionID:     claim.ExecutionID,
		Outcome:         "failed",
		ProgressStage:   stringPtr("failed"),
		ProgressMessage: stringPtr("Transcription failed"),
		ErrorCode:       stringPtr(code),
		ErrorMessage:    stringPtr(message),
	}); err != nil {
		t.Fatalf("Finalize failed child %s error = %v", jobID, err)
	}
}

func (r liveBatchE2ERuntime) createReport(t *testing.T, ctx context.Context, jobID string, idempotencyKey string) JobSnapshot {
	t.Helper()

	snapshot, err := r.public.CreateReport(ctx, jobID, ChildCreateRequest{
		IdempotencyKey: idempotencyKey,
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
	})
	if err != nil {
		t.Fatalf("CreateReport(%s) error = %v", jobID, err)
	}
	return snapshot
}

func (r liveBatchE2ERuntime) createDeepResearch(t *testing.T, ctx context.Context, jobID string, idempotencyKey string) JobSnapshot {
	t.Helper()

	snapshot, err := r.public.CreateDeepResearch(ctx, jobID, ChildCreateRequest{
		IdempotencyKey: idempotencyKey,
		Delivery:       DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
	})
	if err != nil {
		t.Fatalf("CreateDeepResearch(%s) error = %v", jobID, err)
	}
	return snapshot
}

func (r liveBatchE2ERuntime) claimAgentRun(t *testing.T, ctx context.Context, jobID string) agentRunClaimAccess {
	t.Helper()

	claim, err := r.worker.Claim(ctx, jobID, ClaimRequest{WorkerKind: "agent_runner", TaskType: queue.TaskTypeAgentRun})
	if err != nil {
		t.Fatalf("Claim agent_run %s error = %v", jobID, err)
	}
	access, err := r.worker.ResolveAgentRunRequestAccess(ctx, jobID, claim.ExecutionID)
	if err != nil {
		t.Fatalf("ResolveAgentRunRequestAccess(%s) error = %v", jobID, err)
	}
	return agentRunClaimAccess{ExecutionID: claim.ExecutionID, Access: access}
}

func (r liveBatchE2ERuntime) recordAndFinalizeAgentRun(t *testing.T, ctx context.Context, jobID string, executionID string, artifacts []ArtifactDescriptor) {
	t.Helper()

	if err := r.worker.RecordArtifacts(ctx, jobID, ArtifactUpsertRequest{ExecutionID: executionID, Artifacts: artifacts}); err != nil {
		t.Fatalf("RecordArtifacts(agent_run %s) error = %v", jobID, err)
	}
	if _, err := r.worker.Finalize(ctx, jobID, FinalizeRequest{
		ExecutionID:     executionID,
		Outcome:         "succeeded",
		ProgressStage:   stringPtr("completed"),
		ProgressMessage: stringPtr("agent_run E2E completed"),
	}); err != nil {
		t.Fatalf("Finalize(agent_run %s) error = %v", jobID, err)
	}
}

func (r liveBatchE2ERuntime) snapshot(t *testing.T, ctx context.Context, jobID string) JobSnapshot {
	t.Helper()

	snapshot, err := r.public.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob(%s) error = %v", jobID, err)
	}
	return snapshot
}

func (r liveBatchE2ERuntime) children(t *testing.T, ctx context.Context, jobID string) []ChildJobReference {
	t.Helper()

	children := append([]ChildJobReference(nil), r.snapshot(t, ctx, jobID).Children...)
	sort.Slice(children, func(i, j int) bool { return children[i].JobID < children[j].JobID })
	return children
}

func e2eArtifact(kind string, filename string) ArtifactDescriptor {
	format := strings.TrimPrefix(filepath.Ext(filename), ".")
	if format == "" {
		format = "binary"
	}
	return ArtifactDescriptor{
		ArtifactKind: kind,
		Format:       stringPtr(format),
		Filename:     filename,
		MIMEType:     e2eMimeType(filename),
		ObjectKey:    "artifacts/e2e/" + kind + "/" + filename,
		SizeBytes:    int64(len(filename) + len(kind)),
	}
}

func e2eMimeType(filename string) string {
	switch filepath.Ext(filename) {
	case ".txt", ".log":
		return "text/plain; charset=utf-8"
	case ".md":
		return "text/markdown; charset=utf-8"
	case ".json":
		return "application/json"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	default:
		return "application/octet-stream"
	}
}

func assertBatchRootSourceLabels(t *testing.T, snapshot JobSnapshot, labels []string) {
	t.Helper()

	if got, want := len(snapshot.SourceSet.Items), len(labels); got != want {
		t.Fatalf("source set item count = %d, want %d", got, want)
	}
	for idx, label := range labels {
		if snapshot.SourceSet.Items[idx].SourceLabel == nil || *snapshot.SourceSet.Items[idx].SourceLabel != label {
			t.Fatalf("source label[%d] = %#v, want %q", idx, snapshot.SourceSet.Items[idx].SourceLabel, label)
		}
	}
}

func assertQueuedTaskTypes(t *testing.T, specs []queue.EnqueueSpec, taskTypes []string) {
	t.Helper()

	if got, want := len(specs), len(taskTypes); got != want {
		t.Fatalf("queued tasks = %d, want %d", got, want)
	}
	for idx, want := range taskTypes {
		if specs[idx].TaskType != want {
			t.Fatalf("queued task[%d] = %q, want %q", idx, specs[idx].TaskType, want)
		}
	}
}

func assertArtifactKinds(t *testing.T, artifacts []ArtifactSummary, kinds []string) {
	t.Helper()

	got := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		got = append(got, artifact.ArtifactKind)
	}
	sort.Strings(got)
	sort.Strings(kinds)
	if strings.Join(got, ",") != strings.Join(kinds, ",") {
		t.Fatalf("artifact kinds = %v, want %v", got, kinds)
	}
}

func assertAgentRequestAccess(
	t *testing.T,
	objects *agentRunIntegrationObjectStore,
	claimAccess agentRunClaimAccess,
	operation string,
	expectedArtifacts []string,
) {
	t.Helper()

	bodyKey := storage.ArtifactsBucket + "::private/agent-runs/" + claimAccess.Access.RequestRef + "/request.json"
	if claimAccess.Access.URL == "" || claimAccess.Access.RequestDigestSHA256 == "" || claimAccess.Access.RequestBytes <= 0 {
		t.Fatalf("agent request access for %s is incomplete: %#v body_key=%s", operation, claimAccess.Access, bodyKey)
	}
	body, ok := objects.bodies[bodyKey]
	if !ok {
		t.Fatalf("agent_run request body %q missing; object keys=%v", bodyKey, objects.keys())
	}
	var envelope struct {
		Request struct {
			Payload struct {
				Operation               string   `json:"operation"`
				ExpectedOutputArtifacts []string `json:"expected_output_artifacts"`
			} `json:"payload"`
		} `json:"request"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("Unmarshal(agent_run request body) error = %v", err)
	}
	if envelope.Request.Payload.Operation != operation {
		t.Fatalf("agent_run operation = %q, want %q", envelope.Request.Payload.Operation, operation)
	}
	missingArtifacts := map[string]bool{}
	for _, artifactKind := range expectedArtifacts {
		if artifactKind == "" {
			t.Fatalf("empty expected artifact for operation %s", operation)
		}
		missingArtifacts[artifactKind] = true
	}
	for _, actual := range envelope.Request.Payload.ExpectedOutputArtifacts {
		delete(missingArtifacts, actual)
	}
	if len(missingArtifacts) != 0 {
		t.Fatalf("agent_run expected output artifacts missing %v in %#v", missingArtifacts, envelope.Request.Payload.ExpectedOutputArtifacts)
	}
	if int64(len(body)) != claimAccess.Access.RequestBytes {
		t.Fatalf("agent_run request bytes = %d, want %d", claimAccess.Access.RequestBytes, len(body))
	}
	if !strings.Contains(claimAccess.Access.URL, claimAccess.Access.RequestRef) {
		t.Fatalf("agent_run access URL %q does not reference request ref %q", claimAccess.Access.URL, claimAccess.Access.RequestRef)
	}
	if !strings.Contains(claimAccess.Access.Provider, "minio") {
		t.Fatalf("agent_run access provider = %q, want minio-backed provider", claimAccess.Access.Provider)
	}
}

func claimTaskTypeForTranscriptionJob(t *testing.T, snapshot JobSnapshot) string {
	t.Helper()

	if snapshot.ParentJobID == nil && snapshot.Progress != nil && snapshot.Progress.Stage == "aggregate_queued" {
		return queue.TaskTypeTranscriptionAggregate
	}
	return queue.TaskTypeTranscription
}

func ptrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func batchE2EMigrationUpSQL(t *testing.T) string {
	t.Helper()

	migrationPaths, err := filepath.Glob(filepath.Join("..", "storage", "migrations", "*.sql"))
	if err != nil {
		t.Fatalf("Glob(storage/migrations/*.sql) error = %v", err)
	}
	sort.Strings(migrationPaths)
	parts := make([]string, 0, len(migrationPaths))
	for _, migrationPath := range migrationPaths {
		migrationBytes, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", migrationPath, err)
		}
		upDown := strings.Split(string(migrationBytes), "-- +goose Down")
		if len(upDown) != 2 {
			t.Fatalf("migration %s must contain one goose Down marker", filepath.Base(migrationPath))
		}
		upSQL := strings.TrimSpace(strings.Replace(upDown[0], "-- +goose Up", "", 1))
		if upSQL == "" {
			t.Fatalf("migration %s up section is empty", filepath.Base(migrationPath))
		}
		parts = append(parts, upSQL)
	}
	return strings.Join(parts, "\n\n")
}

func batchE2EPostgresDSNWithDatabase(rawDSN, database string) (string, error) {
	parsed, err := neturl.Parse(strings.TrimSpace(rawDSN))
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("postgres dsn must use postgres:// or postgresql://")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("postgres dsn host is required")
	}
	if strings.TrimSpace(database) == "" {
		return "", fmt.Errorf("database is required")
	}
	parsed.Path = "/" + database
	return parsed.String(), nil
}

func batchE2EQuoteSQLIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
