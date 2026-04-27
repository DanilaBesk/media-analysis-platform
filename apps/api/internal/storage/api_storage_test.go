package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	neturl "net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	semanticMigrationNamePattern = regexp.MustCompile(`^\d{4}_[a-z0-9]+(?:_[a-z0-9]+)*\.sql$`)
	phaseOnlyMigrationPattern    = regexp.MustCompile(`^\d{4}_phase\d`)
)

func TestApiStoragePersistJobPreservesCombinedSourceSetAndWebhookDelivery(t *testing.T) {
	t.Parallel()

	logger := &bufferLogger{}
	state := newMemoryStateStore()
	objects := newFakeObjectStore()
	now := func() time.Time { return time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC) }

	repo, err := NewRepository(state, objects, WithLogger(logger), WithClock(now), WithPresignTTL(10*time.Minute))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	req := PersistJobRequest{
		Submission: &JobSubmission{
			ID:                 "submission-1",
			SubmissionKind:     "transcription_upload",
			IdempotencyKey:     "idem-1",
			RequestFingerprint: "normalized-request",
		},
		Sources: []SourceRecord{
			{
				ID:               "source-1",
				SourceKind:       SourceKindUploadedFile,
				DisplayName:      "clip-1",
				OriginalFilename: "clip-1.mp3",
				MIMEType:         "audio/mpeg",
				ObjectKey:        "sources/source-1/original/clip-1.mp3",
				ObjectBody:       []byte("audio-1"),
			},
			{
				ID:               "source-2",
				SourceKind:       SourceKindUploadedFile,
				DisplayName:      "clip-2",
				OriginalFilename: "clip-2.mp3",
				MIMEType:         "audio/mpeg",
				ObjectKey:        "sources/source-2/original/clip-2.mp3",
				ObjectBody:       []byte("audio-2"),
			},
		},
		SourceSet: SourceSetRecord{
			ID:        "source-set-1",
			InputKind: SourceSetInputCombinedUpload,
			Items: []SourceSetItem{
				{Position: 0, SourceID: "source-1"},
				{Position: 1, SourceID: "source-2"},
			},
		},
		Job: JobRecord{
			ID:           "job-1",
			SubmissionID: "submission-1",
			RootJobID:    "job-1",
			SourceSetID:  "source-set-1",
			JobType:      "transcription",
			Status:       "queued",
			Delivery: Delivery{
				Strategy:   DeliveryStrategyWebhook,
				WebhookURL: "https://example.com/hook",
			},
		},
		Event: JobEvent{
			ID:        "event-1",
			EventType: "job.created",
			Payload:   []byte(`{"status":"queued"}`),
		},
		Artifacts: []ArtifactRecord{
			{
				ID:           "artifact-1",
				ArtifactKind: "transcript_plain",
				Filename:     "transcript.txt",
				Format:       "plain",
				MIMEType:     "text/plain",
				ObjectKey:    "artifacts/job-1/transcript/plain/transcript.txt",
				Body:         []byte("hello"),
			},
		},
		WebhookDelivery: &WebhookDelivery{
			ID:        "delivery-1",
			TargetURL: "https://example.com/hook",
			Payload:   []byte(`{"event":"job.created"}`),
		},
	}

	result, err := repo.PersistJob(context.Background(), req)
	if err != nil {
		t.Fatalf("PersistJob() error = %v", err)
	}

	if got, want := len(state.bundle.Sources), 2; got != want {
		t.Fatalf("persisted sources = %d, want %d", got, want)
	}
	if got := state.bundle.SourceSet.Items[0].SourceID; got != "source-1" {
		t.Fatalf("first source-set item = %q, want source-1", got)
	}
	if got := state.bundle.SourceSet.Items[1].SourceID; got != "source-2" {
		t.Fatalf("second source-set item = %q, want source-2", got)
	}
	if state.bundle.WebhookDelivery == nil {
		t.Fatal("expected webhook delivery to be persisted")
	}
	if got := len(state.persistedArtifacts); got != 1 {
		t.Fatalf("persisted artifact rows = %d, want 1", got)
	}
	if got := len(objects.puts); got != 3 {
		t.Fatalf("object puts = %d, want 3", got)
	}
	if objects.puts[0].bucket != SourcesBucket || objects.puts[1].bucket != SourcesBucket || objects.puts[2].bucket != ArtifactsBucket {
		t.Fatalf("object put ordering = %#v, want sources first then artifact", objects.puts)
	}
	if !strings.Contains(logger.String(), PersistJobArtifactMarker) {
		t.Fatalf("logger output missing marker %q: %s", PersistJobArtifactMarker, logger.String())
	}
	if result.Artifacts[0].Body != nil {
		t.Fatalf("artifact body should be stripped from persisted metadata")
	}
}

func TestApiStoragePersistJobDoesNotPersistArtifactRowsBeforeObjectWriteSucceeds(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	objects := newFakeObjectStore()
	objects.failPutFor = map[string]error{
		ArtifactsBucket + "::artifacts/job-2/transcript/plain/transcript.txt": errors.New("minio unavailable"),
	}

	repo, err := NewRepository(state, objects)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	req := PersistJobRequest{
		Sources: []SourceRecord{
			{
				ID:          "source-1",
				SourceKind:  SourceKindUploadedFile,
				DisplayName: "voice-note",
				MIMEType:    "audio/ogg",
				ObjectKey:   "sources/source-1/original/voice.ogg",
				ObjectBody:  []byte("ogg"),
			},
		},
		SourceSet: SourceSetRecord{
			ID:        "source-set-1",
			InputKind: SourceSetInputSingleSource,
			Items: []SourceSetItem{
				{Position: 0, SourceID: "source-1"},
			},
		},
		Job: JobRecord{
			ID:          "job-2",
			RootJobID:   "job-2",
			SourceSetID: "source-set-1",
			JobType:     "transcription",
			Status:      "queued",
			Delivery: Delivery{
				Strategy: DeliveryStrategyPolling,
			},
		},
		Event: JobEvent{
			ID:        "event-2",
			EventType: "job.created",
			Payload:   []byte(`{"status":"queued"}`),
		},
		Artifacts: []ArtifactRecord{
			{
				ID:           "artifact-2",
				ArtifactKind: "transcript_plain",
				Filename:     "transcript.txt",
				Format:       "plain",
				MIMEType:     "text/plain",
				ObjectKey:    "artifacts/job-2/transcript/plain/transcript.txt",
				Body:         []byte("text"),
			},
		},
	}

	_, err = repo.PersistJob(context.Background(), req)
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("PersistJob() error = %v, want ErrStorageUnavailable", err)
	}
	if state.artifactPersistCalls != 0 {
		t.Fatalf("artifact rows should not be persisted after object write failure, got %d calls", state.artifactPersistCalls)
	}
	if state.bundle.Job.ID != "job-2" {
		t.Fatalf("job bundle should persist before artifact metadata phase, got %#v", state.bundle.Job)
	}
}

func TestApiStorageResolveArtifactReturnsFreshPresignedURL(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	createdAt := time.Date(2026, 4, 22, 11, 0, 0, 0, time.UTC)
	state.persistedArtifacts["artifact-1"] = ArtifactRecord{
		ID:           "artifact-1",
		JobID:        "job-3",
		ArtifactKind: "report_markdown",
		Filename:     "report.md",
		MIMEType:     "text/markdown",
		ObjectKey:    "artifacts/job-3/report/markdown/report.md",
		SizeBytes:    17,
		CreatedAt:    createdAt,
	}

	objects := newFakeObjectStore()
	objects.presignedURL = "https://minio.local/presigned/report.md"
	now := func() time.Time { return time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC) }
	objects.presignBaseTime = now()

	repo, err := NewRepository(state, objects, WithClock(now), WithPresignTTL(30*time.Minute))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	resolution, err := repo.ResolveArtifact(context.Background(), "artifact-1")
	if err != nil {
		t.Fatalf("ResolveArtifact() error = %v", err)
	}

	if resolution.Download.Provider != DownloadProviderMinIO {
		t.Fatalf("download provider = %q, want %q", resolution.Download.Provider, DownloadProviderMinIO)
	}
	if resolution.Download.URL != objects.presignedURL {
		t.Fatalf("download url = %q, want %q", resolution.Download.URL, objects.presignedURL)
	}
	if !resolution.Download.ExpiresAt.After(now()) {
		t.Fatalf("expires_at = %v, want after %v", resolution.Download.ExpiresAt, now())
	}
	if resolution.CreatedAt != createdAt {
		t.Fatalf("created_at = %v, want %v", resolution.CreatedAt, createdAt)
	}
}

func TestApiStorageResolveAgentRunRequestAccessUsesInternalPresignedURL(t *testing.T) {
	t.Parallel()

	objects := newFakeObjectStore()
	objects.presignedURL = "https://public-minio.local/presigned/request.json"
	objects.internalPresignedURL = "https://minio:9000/presigned/request.json"
	now := func() time.Time { return time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC) }
	objects.presignBaseTime = now()

	repo, err := NewRepository(newMemoryStateStore(), objects, WithClock(now), WithPresignTTL(30*time.Minute))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	access, err := repo.ResolveAgentRunRequestAccess(context.Background(), JobRecord{
		ID: "job-agent-run",
		ParamsJSON: []byte(`{
			"request_ref": "agentreq_123",
			"request_digest_sha256": "sha256digest",
			"request_bytes": 42
		}`),
	})
	if err != nil {
		t.Fatalf("ResolveAgentRunRequestAccess() error = %v", err)
	}

	if access.URL != objects.internalPresignedURL {
		t.Fatalf("request access url = %q, want internal %q", access.URL, objects.internalPresignedURL)
	}
	if objects.internalPresignCalls != 1 {
		t.Fatalf("internal presign calls = %d, want 1", objects.internalPresignCalls)
	}
	if objects.publicPresignCalls != 0 {
		t.Fatalf("public presign calls = %d, want 0", objects.publicPresignCalls)
	}
	if access.RequestRef != "agentreq_123" || access.RequestDigestSHA256 != "sha256digest" || access.RequestBytes != 42 {
		t.Fatalf("request access metadata = %#v", access)
	}
}

func TestApiStorageMinioObjectStoreUsesPublicPresignEndpoint(t *testing.T) {
	t.Parallel()

	internalClient, err := NewMinioClient("http://minio:9000", "minioadmin", "minioadmin")
	if err != nil {
		t.Fatalf("NewMinioClient(internal) error = %v", err)
	}
	publicClient, err := NewMinioClient("http://localhost:9000", "minioadmin", "minioadmin")
	if err != nil {
		t.Fatalf("NewMinioClient(public) error = %v", err)
	}
	objects, err := NewMinioObjectStoreWithPresignClient(internalClient, publicClient)
	if err != nil {
		t.Fatalf("NewMinioObjectStoreWithPresignClient() error = %v", err)
	}

	rawURL, expiresAt, err := objects.PresignGetObject(context.Background(), ArtifactsBucket, "artifacts/job-3/report/markdown/report.md", 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignGetObject() error = %v", err)
	}
	parsedURL, err := neturl.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse presigned url: %v", err)
	}

	if parsedURL.Host != "localhost:9000" {
		t.Fatalf("presigned host = %q, want localhost:9000; url=%s", parsedURL.Host, rawURL)
	}
	if parsedURL.Query().Get("X-Amz-Signature") == "" {
		t.Fatalf("presigned url missing X-Amz-Signature: %s", rawURL)
	}
	if !expiresAt.After(time.Now().UTC()) {
		t.Fatalf("expires_at = %v, want future timestamp", expiresAt)
	}
}

func TestApiStorageMinioObjectStoreUsesInternalEndpointForInternalPresign(t *testing.T) {
	t.Parallel()

	internalClient, err := NewMinioClient("http://minio:9000", "minioadmin", "minioadmin")
	if err != nil {
		t.Fatalf("NewMinioClient(internal) error = %v", err)
	}
	publicClient, err := NewMinioClient("http://localhost:9000", "minioadmin", "minioadmin")
	if err != nil {
		t.Fatalf("NewMinioClient(public) error = %v", err)
	}
	objects, err := NewMinioObjectStoreWithPresignClient(internalClient, publicClient)
	if err != nil {
		t.Fatalf("NewMinioObjectStoreWithPresignClient() error = %v", err)
	}

	rawURL, expiresAt, err := objects.PresignInternalGetObject(context.Background(), ArtifactsBucket, "private/agent-runs/agentreq_123/request.json", 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignInternalGetObject() error = %v", err)
	}
	parsedURL, err := neturl.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse presigned url: %v", err)
	}

	if parsedURL.Host != "minio:9000" {
		t.Fatalf("internal presigned host = %q, want minio:9000; url=%s", parsedURL.Host, rawURL)
	}
	if parsedURL.Query().Get("X-Amz-Signature") == "" {
		t.Fatalf("internal presigned url missing X-Amz-Signature: %s", rawURL)
	}
	if !expiresAt.After(time.Now().UTC()) {
		t.Fatalf("expires_at = %v, want future timestamp", expiresAt)
	}
}

func TestApiStorageRejectsInvalidSourceContract(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(newMemoryStateStore(), newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	_, err = repo.PersistSource(context.Background(), SourceRecord{
		ID:          "source-youtube",
		SourceKind:  SourceKindYouTubeURL,
		DisplayName: "youtube",
		ObjectKey:   "sources/should-not-exist",
	})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("PersistSource() error = %v, want ErrContractViolation", err)
	}
}

func TestApiStorageAllowsEmptySourceSetOnlyForAgentRun(t *testing.T) {
	t.Parallel()

	if err := validateSourceSet(SourceSetRecord{
		ID:        "source-set-agent",
		InputKind: SourceSetInputAgentRun,
	}, nil); err != nil {
		t.Fatalf("agent_run source set should allow empty items: %v", err)
	}

	err := validateSourceSet(SourceSetRecord{
		ID:        "source-set-single",
		InputKind: SourceSetInputSingleSource,
	}, nil)
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("single_source empty source set error = %v, want ErrContractViolation", err)
	}

	err = validateSourceSet(SourceSetRecord{
		ID:        "source-set-agent-with-item",
		InputKind: SourceSetInputAgentRun,
		Items: []SourceSetItem{
			{Position: 0, SourceID: "source-1"},
		},
	}, []SourceRecord{{ID: "source-1"}})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("agent_run with source items error = %v, want ErrContractViolation", err)
	}
}

func TestApiStorageSaveSubmissionGraphAllowsAgentRunWithoutSources(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(newMemoryStateStore(), newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	submission := JobSubmission{
		ID:                 "submission-agent",
		SubmissionKind:     "agent_run_create",
		IdempotencyKey:     "agent-idem",
		RequestFingerprint: "agent-fingerprint",
	}
	sourceSet := SourceSetRecord{
		ID:        "source-set-agent",
		InputKind: SourceSetInputAgentRun,
	}
	job := JobRecord{
		ID:           "job-agent",
		SubmissionID: submission.ID,
		RootJobID:    "job-agent",
		SourceSetID:  sourceSet.ID,
		JobType:      "agent_run",
		Status:       "queued",
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
	}

	if err := repo.SaveSubmissionGraph(context.Background(), submission, nil, []SourceSetRecord{sourceSet}, []JobRecord{job}); err != nil {
		t.Fatalf("SaveSubmissionGraph() error = %v", err)
	}
}

func TestApiStorageSaveSubmissionGraphStillRejectsNonAgentRunWithoutSources(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(newMemoryStateStore(), newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	submission := JobSubmission{
		ID:                 "submission-upload",
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "upload-idem",
		RequestFingerprint: "upload-fingerprint",
	}
	sourceSet := SourceSetRecord{
		ID:        "source-set-upload",
		InputKind: SourceSetInputSingleSource,
		Items: []SourceSetItem{
			{Position: 0, SourceID: "missing-source"},
		},
	}
	job := JobRecord{
		ID:           "job-upload",
		SubmissionID: submission.ID,
		RootJobID:    "job-upload",
		SourceSetID:  sourceSet.ID,
		JobType:      "transcription",
		Status:       "queued",
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
	}

	err = repo.SaveSubmissionGraph(context.Background(), submission, nil, []SourceSetRecord{sourceSet}, []JobRecord{job})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("SaveSubmissionGraph() error = %v, want ErrContractViolation", err)
	}
}

func TestApiStorageSaveSubmissionGraphPreservesBatchSourceLineage(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	submission := JobSubmission{
		ID:                 "submission-batch",
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "batch-idem",
		RequestFingerprint: "batch-fingerprint",
	}
	sources := []SourceRecord{
		{
			ID:          "source-a",
			SourceKind:  SourceKindUploadedFile,
			DisplayName: "voice-a",
			MIMEType:    "audio/ogg",
			ObjectKey:   "sources/source-a/original/voice-a.ogg",
			ObjectBody:  []byte("voice-a"),
		},
		{
			ID:          "source-b",
			SourceKind:  SourceKindYouTubeURL,
			DisplayName: "video-b",
			SourceURL:   "https://youtube.example/watch?v=video-b",
		},
	}
	sourceSet := SourceSetRecord{
		ID:        "source-set-batch",
		InputKind: SourceSetInputBatchTranscription,
		Items: []SourceSetItem{
			{
				Position:           0,
				SourceID:           "source-a",
				SourceLabel:        "voice_a",
				SourceLabelVersion: "batch-transcription.v1",
				MetadataJSON:       []byte(`{"file_part":"files[0]","sha256":"sha-a"}`),
				LineageJSON:        []byte(`{"root_job_id":"job-root","child_job_id":"job-child-a"}`),
			},
			{
				Position:           1,
				SourceID:           "source-b",
				SourceLabel:        "video_b",
				SourceLabelVersion: "batch-transcription.v1",
				MetadataJSON:       []byte(`{"url":"https://youtube.example/watch?v=video-b"}`),
				LineageJSON:        []byte(`{"root_job_id":"job-root","child_job_id":"job-child-b"}`),
			},
		},
	}
	jobs := []JobRecord{
		{
			ID:           "job-root",
			SubmissionID: submission.ID,
			RootJobID:    "job-root",
			SourceSetID:  sourceSet.ID,
			JobType:      "transcription",
			Status:       "queued",
			Delivery: Delivery{
				Strategy: DeliveryStrategyPolling,
			},
		},
		{
			ID:          "job-child-a",
			RootJobID:   "job-root",
			ParentJobID: "job-root",
			SourceSetID: sourceSet.ID,
			JobType:     "transcription",
			Status:      "queued",
			Delivery: Delivery{
				Strategy: DeliveryStrategyPolling,
			},
		},
		{
			ID:          "job-child-b",
			RootJobID:   "job-root",
			ParentJobID: "job-root",
			SourceSetID: sourceSet.ID,
			JobType:     "transcription",
			Status:      "queued",
			Delivery: Delivery{
				Strategy: DeliveryStrategyPolling,
			},
		},
	}

	if err := repo.SaveSubmissionGraph(context.Background(), submission, sources, []SourceSetRecord{sourceSet}, jobs); err != nil {
		t.Fatalf("SaveSubmissionGraph() error = %v", err)
	}

	if got, want := len(state.sourceSets), 1; got != want {
		t.Fatalf("source sets = %d, want %d", got, want)
	}
	items := state.sourceSets[0].Items
	if got, want := len(items), 2; got != want {
		t.Fatalf("source set items = %d, want %d", got, want)
	}
	if items[0].SourceLabel != "voice_a" || items[1].SourceLabel != "video_b" {
		t.Fatalf("source labels = %q, %q; want voice_a, video_b", items[0].SourceLabel, items[1].SourceLabel)
	}
	if items[0].SourceLabelVersion != "batch-transcription.v1" || items[1].SourceLabelVersion != "batch-transcription.v1" {
		t.Fatalf("source label versions = %q, %q", items[0].SourceLabelVersion, items[1].SourceLabelVersion)
	}
	if string(items[0].MetadataJSON) != `{"file_part":"files[0]","sha256":"sha-a"}` {
		t.Fatalf("first metadata json = %s", items[0].MetadataJSON)
	}
	if string(items[1].LineageJSON) != `{"root_job_id":"job-root","child_job_id":"job-child-b"}` {
		t.Fatalf("second lineage json = %s", items[1].LineageJSON)
	}
}

func TestApiStorageRejectsDuplicateBatchSourceLabels(t *testing.T) {
	t.Parallel()

	err := validateSourceSet(SourceSetRecord{
		ID:        "source-set-batch",
		InputKind: SourceSetInputBatchTranscription,
		Items: []SourceSetItem{
			{Position: 0, SourceID: "source-a", SourceLabel: "same_label", SourceLabelVersion: "batch-transcription.v1"},
			{Position: 1, SourceID: "source-b", SourceLabel: "same_label", SourceLabelVersion: "batch-transcription.v1"},
		},
	}, []SourceRecord{{ID: "source-a"}, {ID: "source-b"}})
	if !errors.Is(err, ErrContractViolation) {
		t.Fatalf("duplicate source label error = %v, want ErrContractViolation", err)
	}
}

func TestApiStoragePersistsBatchManifestAndDiagnosticsArtifacts(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	req := PersistJobRequest{
		Sources: []SourceRecord{
			{
				ID:          "source-a",
				SourceKind:  SourceKindUploadedFile,
				DisplayName: "voice-a",
				MIMEType:    "audio/ogg",
				ObjectKey:   "sources/source-a/original/voice-a.ogg",
				ObjectBody:  []byte("voice-a"),
			},
		},
		SourceSet: SourceSetRecord{
			ID:        "source-set-batch-artifacts",
			InputKind: SourceSetInputBatchTranscription,
			Items: []SourceSetItem{
				{Position: 0, SourceID: "source-a", SourceLabel: "voice_a", SourceLabelVersion: "batch-transcription.v1"},
			},
		},
		Job: JobRecord{
			ID:          "job-batch-artifacts",
			RootJobID:   "job-batch-artifacts",
			SourceSetID: "source-set-batch-artifacts",
			JobType:     "transcription",
			Status:      "queued",
			Delivery: Delivery{
				Strategy: DeliveryStrategyPolling,
			},
		},
		Event: JobEvent{
			ID:        "event-batch-artifacts",
			EventType: "job.created",
			Payload:   []byte(`{"status":"queued"}`),
		},
		Artifacts: []ArtifactRecord{
			{
				ID:           "artifact-manifest",
				ArtifactKind: "source_manifest_json",
				Filename:     "source-manifest.json",
				Format:       "json",
				MIMEType:     "application/json",
				ObjectKey:    "artifacts/job-batch-artifacts/source-manifest.json",
				Body:         []byte(`{"manifest_version":"batch-transcription.v1"}`),
			},
			{
				ID:           "artifact-diagnostics",
				ArtifactKind: "batch_diagnostics_json",
				Filename:     "batch-diagnostics.json",
				Format:       "json",
				MIMEType:     "application/json",
				ObjectKey:    "artifacts/job-batch-artifacts/batch-diagnostics.json",
				Body:         []byte(`{"sources":[]}`),
			},
		},
	}

	result, err := repo.PersistJob(context.Background(), req)
	if err != nil {
		t.Fatalf("PersistJob() error = %v", err)
	}
	if got, want := len(result.Artifacts), 2; got != want {
		t.Fatalf("persisted artifacts = %d, want %d", got, want)
	}
	if _, ok := state.persistedArtifacts["artifact-manifest"]; !ok {
		t.Fatal("source_manifest_json artifact was not persisted")
	}
	if _, ok := state.persistedArtifacts["artifact-diagnostics"]; !ok {
		t.Fatal("batch_diagnostics_json artifact was not persisted")
	}
}

func TestApiStorageMigrationContract(t *testing.T) {
	t.Parallel()

	migrationPaths, err := filepath.Glob(filepath.Join("migrations", "*.sql"))
	if err != nil {
		t.Fatalf("Glob(migrations/*.sql) error = %v", err)
	}
	if len(migrationPaths) == 0 {
		t.Fatalf("migration file count = 0, want at least 1")
	}
	sort.Strings(migrationPaths)

	var combined strings.Builder
	for _, migrationPath := range migrationPaths {
		migrationName := filepath.Base(migrationPath)
		if !semanticMigrationNamePattern.MatchString(migrationName) {
			t.Fatalf("migration name %q must match zero-padded semantic snake_case rule", migrationName)
		}
		if phaseOnlyMigrationPattern.MatchString(migrationName) {
			t.Fatalf("migration name %q must be semantic, not phase-only", migrationName)
		}

		migrationBytes, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", migrationPath, err)
		}

		migration := string(migrationBytes)
		combined.WriteString(migration)
		combined.WriteString("\n")
		if !strings.Contains(migration, "-- +goose Up") || !strings.Contains(migration, "-- +goose Down") {
			t.Fatalf("migration %q must expose goose up/down sections", migrationName)
		}
	}

	combinedMigration := combined.String()
	for _, snippet := range requiredMigrationSnippets(t) {
		if !strings.Contains(combinedMigration, snippet) {
			t.Fatalf("migration set is missing required snippet %q", snippet)
		}
	}
}

func TestApiStorageMigrationAppliesAndRollsBackOnLivePostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("API_STORAGE_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("set API_STORAGE_POSTGRES_DSN to run live PostgreSQL migration smoke")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbName := "api_storage_migration_smoke_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	adminDSN, err := postgresDSNWithDatabase(dsn, "postgres")
	if err != nil {
		t.Fatalf("postgresDSNWithDatabase(admin) error = %v", err)
	}
	smokeDSN, err := postgresDSNWithDatabase(dsn, dbName)
	if err != nil {
		t.Fatalf("postgresDSNWithDatabase(smoke) error = %v", err)
	}

	adminDB, err := OpenPostgresDB(ctx, adminDSN)
	if err != nil {
		t.Fatalf("OpenPostgresDB(admin) error = %v", err)
	}
	defer adminDB.Close()
	defer func() {
		_, _ = adminDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quoteSQLIdentifier(dbName)+" WITH (FORCE)")
	}()

	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+quoteSQLIdentifier(dbName)); err != nil {
		t.Fatalf("CREATE DATABASE smoke db error = %v", err)
	}

	smokeDB, err := OpenPostgresDB(ctx, smokeDSN)
	if err != nil {
		t.Fatalf("OpenPostgresDB(smoke) error = %v", err)
	}
	defer smokeDB.Close()

	upSQL, downSQL := migrationSections(t)
	if _, err := smokeDB.ExecContext(ctx, upSQL); err != nil {
		t.Fatalf("migration up error = %v", err)
	}
	for _, table := range []string{"job_submissions", "sources", "source_sets", "source_set_items", "jobs", "job_executions", "job_artifacts", "job_events", "webhook_deliveries"} {
		if !tableExists(ctx, t, smokeDB, table) {
			t.Fatalf("table %s missing after migration up", table)
		}
	}

	if _, err := smokeDB.ExecContext(ctx, downSQL); err != nil {
		t.Fatalf("migration down error = %v", err)
	}
	for _, table := range []string{"webhook_deliveries", "job_events", "job_artifacts", "job_executions", "jobs", "source_set_items", "source_sets", "sources", "job_submissions"} {
		if tableExists(ctx, t, smokeDB, table) {
			t.Fatalf("table %s still exists after migration down", table)
		}
	}
}

func TestApiStorageInsertSourceSetItemsUsesUUIDIDs(t *testing.T) {
	t.Parallel()

	execer := &recordingExecer{}
	sourceSet := SourceSetRecord{
		ID:        uuid.NewString(),
		InputKind: SourceSetInputCombinedUpload,
		Items: []SourceSetItem{
			{
				Position:           0,
				SourceID:           uuid.NewString(),
				SourceLabel:        "first_source",
				SourceLabelVersion: "batch-transcription.v1",
				MetadataJSON:       []byte(`{"sha256":"sha-first"}`),
				LineageJSON:        []byte(`{"child_job_id":"job-child-first"}`),
			},
			{
				Position:           1,
				SourceID:           uuid.NewString(),
				SourceLabel:        "second_source",
				SourceLabelVersion: "batch-transcription.v1",
				MetadataJSON:       []byte(`{"sha256":"sha-second"}`),
				LineageJSON:        []byte(`{"child_job_id":"job-child-second"}`),
			},
		},
		CreatedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
	}

	if err := insertSourceSetItems(context.Background(), execer, sourceSet); err != nil {
		t.Fatalf("insertSourceSetItems() error = %v", err)
	}
	if got, want := len(execer.calls), 2; got != want {
		t.Fatalf("insertSourceSetItems() call count = %d, want %d", got, want)
	}

	seen := map[string]struct{}{}
	for idx, call := range execer.calls {
		if got, want := len(call.args), 9; got != want {
			t.Fatalf("call %d arg count = %d, want %d", idx, got, want)
		}
		itemID, ok := call.args[0].(string)
		if !ok {
			t.Fatalf("call %d arg[0] type = %T, want string", idx, call.args[0])
		}
		if _, err := uuid.Parse(itemID); err != nil {
			t.Fatalf("call %d item id = %q, want UUID: %v", idx, itemID, err)
		}
		if _, exists := seen[itemID]; exists {
			t.Fatalf("call %d item id = %q, want unique UUID per row", idx, itemID)
		}
		seen[itemID] = struct{}{}
		sourceLabel, ok := call.args[4].(sql.NullString)
		if !ok {
			t.Fatalf("call %d source label arg type = %T, want sql.NullString", idx, call.args[4])
		}
		if got, want := sourceLabel.String, sourceSet.Items[idx].SourceLabel; !sourceLabel.Valid || got != want {
			t.Fatalf("call %d source label arg = %#v, want %q", idx, sourceLabel, want)
		}
		sourceLabelVersion, ok := call.args[5].(sql.NullString)
		if !ok {
			t.Fatalf("call %d source label version arg type = %T, want sql.NullString", idx, call.args[5])
		}
		if got, want := sourceLabelVersion.String, sourceSet.Items[idx].SourceLabelVersion; !sourceLabelVersion.Valid || got != want {
			t.Fatalf("call %d source label version arg = %#v, want %q", idx, sourceLabelVersion, want)
		}
		if got, want := string(call.args[6].([]byte)), string(sourceSet.Items[idx].MetadataJSON); got != want {
			t.Fatalf("call %d metadata arg = %s, want %s", idx, got, want)
		}
		if got, want := string(call.args[7].([]byte)), string(sourceSet.Items[idx].LineageJSON); got != want {
			t.Fatalf("call %d lineage arg = %s, want %s", idx, got, want)
		}
	}
}

func TestApiStorageClaimJobExecutionPersistsAuthoritativeExecution(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	seedJob(state, JobRecord{
		ID:          "job-claim-1",
		RootJobID:   "job-claim-1",
		SourceSetID: "source-set-1",
		JobType:     "transcription",
		Status:      "queued",
		Version:     1,
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
		ProgressStage: "queued",
		ParamsJSON:    []byte("{}"),
	})

	claimedAt := time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC)
	repo, err := NewRepository(
		state,
		newFakeObjectStore(),
		WithClock(func() time.Time { return claimedAt }),
		WithIDGenerator(func() string { return "execution-1" }),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	result, err := repo.ClaimJobExecution(context.Background(), ClaimJobExecutionRequest{
		JobID:      "job-claim-1",
		WorkerKind: "worker-transcription",
		TaskType:   "transcription",
	})
	if err != nil {
		t.Fatalf("ClaimJobExecution() error = %v", err)
	}

	if !result.Claimed {
		t.Fatal("expected claim to succeed")
	}
	if result.Execution == nil {
		t.Fatal("expected execution metadata on successful claim")
	}
	if result.Execution.ExecutionID != "execution-1" {
		t.Fatalf("execution id = %q, want execution-1", result.Execution.ExecutionID)
	}
	if result.Job.Status != "running" {
		t.Fatalf("job status = %q, want running", result.Job.Status)
	}
	if result.Job.Version != 2 {
		t.Fatalf("job version = %d, want 2", result.Job.Version)
	}
	if result.Job.StartedAt == nil || !result.Job.StartedAt.Equal(claimedAt) {
		t.Fatalf("job started_at = %v, want %v", result.Job.StartedAt, claimedAt)
	}

	execution, err := repo.GetActiveJobExecution(context.Background(), "job-claim-1", "execution-1")
	if err != nil {
		t.Fatalf("GetActiveJobExecution() error = %v", err)
	}
	if execution.WorkerKind != "worker-transcription" || execution.TaskType != "transcription" {
		t.Fatalf("execution = %#v, want worker/task metadata", execution)
	}
}

func TestApiStorageClaimJobExecutionRejectsSecondActiveClaim(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	seedJob(state, JobRecord{
		ID:          "job-claim-2",
		RootJobID:   "job-claim-2",
		SourceSetID: "source-set-1",
		JobType:     "transcription",
		Status:      "queued",
		Version:     1,
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
		ProgressStage: "queued",
		ParamsJSON:    []byte("{}"),
	})

	repo, err := NewRepository(state, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	first, err := repo.ClaimJobExecution(context.Background(), ClaimJobExecutionRequest{
		JobID:       "job-claim-2",
		WorkerKind:  "worker-transcription",
		TaskType:    "transcription",
		ExecutionID: "execution-1",
		ClaimedAt:   time.Date(2026, 4, 23, 11, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("first ClaimJobExecution() error = %v", err)
	}
	if !first.Claimed {
		t.Fatal("expected first claim to succeed")
	}

	second, err := repo.ClaimJobExecution(context.Background(), ClaimJobExecutionRequest{
		JobID:       "job-claim-2",
		WorkerKind:  "worker-transcription",
		TaskType:    "transcription",
		ExecutionID: "execution-2",
		ClaimedAt:   time.Date(2026, 4, 23, 11, 1, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("second ClaimJobExecution() error = %v", err)
	}
	if second.Claimed {
		t.Fatal("expected second claim to be rejected while the first execution is active")
	}
	if second.Execution != nil {
		t.Fatalf("second claim execution = %#v, want nil", second.Execution)
	}

	active, err := repo.GetActiveJobExecution(context.Background(), "job-claim-2", "execution-1")
	if err != nil {
		t.Fatalf("GetActiveJobExecution() error = %v", err)
	}
	if active.ExecutionID != "execution-1" {
		t.Fatalf("active execution id = %q, want execution-1", active.ExecutionID)
	}
}

func TestApiStorageFinishJobExecutionExpiresAuthority(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	seedJob(state, JobRecord{
		ID:          "job-claim-3",
		RootJobID:   "job-claim-3",
		SourceSetID: "source-set-1",
		JobType:     "report",
		Status:      "queued",
		Version:     1,
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
		ProgressStage: "queued",
		ParamsJSON:    []byte("{}"),
	})

	repo, err := NewRepository(state, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	_, err = repo.ClaimJobExecution(context.Background(), ClaimJobExecutionRequest{
		JobID:       "job-claim-3",
		WorkerKind:  "worker-report",
		TaskType:    "report",
		ExecutionID: "execution-3",
		ClaimedAt:   time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ClaimJobExecution() error = %v", err)
	}

	finishedAt := time.Date(2026, 4, 23, 12, 30, 0, 0, time.UTC)
	execution, err := repo.FinishJobExecution(context.Background(), FinishJobExecutionRequest{
		JobID:       "job-claim-3",
		ExecutionID: "execution-3",
		Outcome:     "succeeded",
		FinishedAt:  finishedAt,
	})
	if err != nil {
		t.Fatalf("FinishJobExecution() error = %v", err)
	}
	if execution.FinishedAt == nil || !execution.FinishedAt.Equal(finishedAt) {
		t.Fatalf("finished_at = %v, want %v", execution.FinishedAt, finishedAt)
	}
	if execution.Outcome != "succeeded" {
		t.Fatalf("outcome = %q, want succeeded", execution.Outcome)
	}

	_, err = repo.GetActiveJobExecution(context.Background(), "job-claim-3", "execution-3")
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("GetActiveJobExecution() error = %v, want ErrExecutionNotFound", err)
	}
	_, err = repo.FinishJobExecution(context.Background(), FinishJobExecutionRequest{
		JobID:       "job-claim-3",
		ExecutionID: "execution-3",
		Outcome:     "succeeded",
		FinishedAt:  finishedAt.Add(time.Minute),
	})
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("second FinishJobExecution() error = %v, want ErrExecutionNotFound", err)
	}
}

func TestApiStorageRepositoryDelegatesSubmissionAndJobQueries(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	objects := newFakeObjectStore()
	repo, err := NewRepository(state, objects)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	submission := JobSubmission{
		ID:                 "submission-1",
		SubmissionKind:     "transcription_upload",
		IdempotencyKey:     "idem-1",
		RequestFingerprint: "fp-1",
	}
	sources := []SourceRecord{
		{
			ID:          "source-1",
			SourceKind:  SourceKindUploadedFile,
			DisplayName: "voice",
			MIMEType:    "audio/ogg",
			ObjectKey:   "sources/source-1/original/voice.ogg",
			ObjectBody:  []byte("ogg"),
		},
	}
	sourceSet := SourceSetRecord{
		ID:        "source-set-1",
		InputKind: SourceSetInputSingleSource,
		Items: []SourceSetItem{
			{Position: 0, SourceID: "source-1"},
		},
	}
	root := JobRecord{
		ID:           "job-root",
		SubmissionID: submission.ID,
		RootJobID:    "job-root",
		SourceSetID:  sourceSet.ID,
		JobType:      "transcription",
		Status:       "queued",
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
	}
	child := JobRecord{
		ID:          "job-child",
		RootJobID:   "job-root",
		ParentJobID: "job-root",
		SourceSetID: sourceSet.ID,
		JobType:     "report",
		Status:      "succeeded",
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
	}

	if err := repo.SaveSubmissionGraph(context.Background(), submission, sources, []SourceSetRecord{sourceSet}, []JobRecord{root, child}); err != nil {
		t.Fatalf("SaveSubmissionGraph() error = %v", err)
	}

	gotSubmission, err := repo.FindSubmission(context.Background(), submission.SubmissionKind, submission.IdempotencyKey)
	if err != nil {
		t.Fatalf("FindSubmission() error = %v", err)
	}
	if gotSubmission == nil || gotSubmission.ID != submission.ID {
		t.Fatalf("FindSubmission() = %#v, want submission %q", gotSubmission, submission.ID)
	}

	jobs, err := repo.ListJobsBySubmission(context.Background(), submission.ID)
	if err != nil {
		t.Fatalf("ListJobsBySubmission() error = %v", err)
	}
	if got, want := len(jobs), 1; got != want {
		t.Fatalf("ListJobsBySubmission() count = %d, want %d", got, want)
	}
	if jobs[0].ID != root.ID {
		t.Fatalf("ListJobsBySubmission() root job = %q, want %q", jobs[0].ID, root.ID)
	}

	gotRoot, err := repo.GetJob(context.Background(), root.ID)
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if gotRoot.ID != root.ID {
		t.Fatalf("GetJob() id = %q, want %q", gotRoot.ID, root.ID)
	}

	reportChild, err := repo.FindCanonicalChild(context.Background(), root.ID, "report")
	if err != nil {
		t.Fatalf("FindCanonicalChild() error = %v", err)
	}
	if reportChild == nil || reportChild.ID != child.ID {
		t.Fatalf("FindCanonicalChild() = %#v, want child %q", reportChild, child.ID)
	}
}

func TestApiStorageRepositoryDelegatesArtifactsAndWebhookState(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	repo, err := NewRepository(state, newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	job := JobRecord{
		ID:          "job-1",
		RootJobID:   "job-1",
		SourceSetID: "source-set-1",
		JobType:     "transcription",
		Status:      "queued",
		Delivery: Delivery{
			Strategy: DeliveryStrategyPolling,
		},
	}

	if err := repo.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	job.Status = "running"
	if err := repo.UpdateJob(context.Background(), job); err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}

	state.persistedArtifacts["artifact-1"] = ArtifactRecord{
		ID:           "artifact-1",
		JobID:        job.ID,
		ArtifactKind: "transcript_plain",
		Filename:     "transcript.txt",
		MIMEType:     "text/plain",
		ObjectKey:    "artifacts/job-1/transcript/plain/transcript.txt",
	}

	artifact, err := repo.FindArtifactByKind(context.Background(), job.ID, "transcript_plain")
	if err != nil {
		t.Fatalf("FindArtifactByKind() error = %v", err)
	}
	if artifact == nil || artifact.ID != "artifact-1" {
		t.Fatalf("FindArtifactByKind() = %#v, want artifact-1", artifact)
	}

	event := JobEvent{
		ID:        "event-1",
		JobID:     job.ID,
		RootJobID: job.RootJobID,
		EventType: "job.updated",
		Version:   2,
		Payload:   []byte(`{"status":"running"}`),
	}
	if err := repo.AppendJobEvent(context.Background(), event); err != nil {
		t.Fatalf("AppendJobEvent() error = %v", err)
	}

	delivery := WebhookDelivery{
		ID:         "delivery-1",
		JobEventID: event.ID,
		JobID:      job.ID,
		TargetURL:  "https://example.com/hook",
		Payload:    []byte(`{"status":"running"}`),
	}
	if err := repo.CreateWebhookDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("CreateWebhookDelivery() error = %v", err)
	}

	delivery.State = WebhookStateDelivered
	if err := repo.UpdateWebhookDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("UpdateWebhookDelivery() error = %v", err)
	}

	if got, want := len(state.events), 1; got != want {
		t.Fatalf("persisted events = %d, want %d", got, want)
	}
	if got, want := len(state.deliveries), 1; got != want {
		t.Fatalf("persisted deliveries = %d, want %d", got, want)
	}
	if state.deliveries[0].State != WebhookStateDelivered {
		t.Fatalf("delivery state = %q, want %q", state.deliveries[0].State, WebhookStateDelivered)
	}
}

func requiredMigrationSnippets(t *testing.T) []string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "required_schema_fragments.txt"))
	if err != nil {
		t.Fatalf("ReadFile(testdata/required_schema_fragments.txt) error = %v", err)
	}

	lines := strings.Split(string(data), "\n")
	fragments := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fragments = append(fragments, line)
	}
	return fragments
}

func migrationSections(t *testing.T) (string, string) {
	t.Helper()

	migrationPaths, err := filepath.Glob(filepath.Join("migrations", "*.sql"))
	if err != nil {
		t.Fatalf("Glob(migrations/*.sql) error = %v", err)
	}
	sort.Strings(migrationPaths)

	upParts := make([]string, 0, len(migrationPaths))
	downParts := make([]string, 0, len(migrationPaths))
	for _, migrationPath := range migrationPaths {
		migrationBytes, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", migrationPath, err)
		}
		parts := strings.Split(string(migrationBytes), "-- +goose Down")
		if len(parts) != 2 {
			t.Fatalf("migration %s must contain exactly one goose Down marker", filepath.Base(migrationPath))
		}
		upSQL := strings.TrimSpace(strings.Replace(parts[0], "-- +goose Up", "", 1))
		downSQL := strings.TrimSpace(parts[1])
		if upSQL == "" || downSQL == "" {
			t.Fatalf("migration %s up/down sections must both be non-empty", filepath.Base(migrationPath))
		}
		upParts = append(upParts, upSQL)
		downParts = append([]string{downSQL}, downParts...)
	}
	return strings.Join(upParts, "\n\n"), strings.Join(downParts, "\n\n")
}

func postgresDSNWithDatabase(rawDSN, database string) (string, error) {
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

func tableExists(ctx context.Context, t *testing.T, db *sql.DB, table string) bool {
	t.Helper()

	var relation sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)`, "public."+table).Scan(&relation); err != nil {
		t.Fatalf("to_regclass(%s) error = %v", table, err)
	}
	return relation.Valid
}

func quoteSQLIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func seedJob(state *memoryStateStore, job JobRecord) {
	state.jobs[job.ID] = job
	state.jobOrder = append(state.jobOrder, job.ID)
}

type memoryStateStore struct {
	bundle               PersistedJobBundle
	sources              []SourceRecord
	sourceSets           []SourceSetRecord
	submissions          map[string]*JobSubmission
	jobs                 map[string]JobRecord
	jobOrder             []string
	executions           map[string]JobExecutionRecord
	activeExecutionByJob map[string]string
	persistedArtifacts   map[string]ArtifactRecord
	artifactPersistCalls int
	events               []JobEvent
	deliveries           []WebhookDelivery
	batchDrafts          map[string]BatchDraftRecord
}

func newMemoryStateStore() *memoryStateStore {
	return &memoryStateStore{
		submissions:          map[string]*JobSubmission{},
		jobs:                 map[string]JobRecord{},
		executions:           map[string]JobExecutionRecord{},
		activeExecutionByJob: map[string]string{},
		persistedArtifacts:   map[string]ArtifactRecord{},
		batchDrafts:          map[string]BatchDraftRecord{},
	}
}

type recordingExecCall struct {
	query string
	args  []any
}

type recordingExecer struct {
	calls []recordingExecCall
}

func (e *recordingExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.calls = append(e.calls, recordingExecCall{
		query: query,
		args:  append([]any(nil), args...),
	})
	return recordingSQLResult{}, nil
}

func (e *recordingExecer) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("QueryContext should not be called in this test")
}

func (e *recordingExecer) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	panic("QueryRowContext should not be called in this test")
}

type recordingSQLResult struct{}

func (recordingSQLResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (recordingSQLResult) RowsAffected() (int64, error) {
	return 1, nil
}

func (m *memoryStateStore) PersistSource(_ context.Context, source SourceRecord) error {
	m.sources = append(m.sources, source)
	return nil
}

func (m *memoryStateStore) PersistJobBundle(_ context.Context, bundle PersistedJobBundle) error {
	m.bundle = bundle
	if bundle.Submission != nil {
		copySubmission := *bundle.Submission
		m.submissions[bundle.Submission.SubmissionKind+"::"+bundle.Submission.IdempotencyKey] = &copySubmission
	}
	m.sources = append(m.sources, bundle.Sources...)
	m.sourceSets = append(m.sourceSets, bundle.SourceSet)
	m.jobs[bundle.Job.ID] = bundle.Job
	m.jobOrder = append(m.jobOrder, bundle.Job.ID)
	if bundle.WebhookDelivery != nil {
		m.deliveries = append(m.deliveries, *bundle.WebhookDelivery)
	}
	return nil
}

func (m *memoryStateStore) PersistArtifacts(_ context.Context, artifacts []ArtifactRecord) error {
	m.artifactPersistCalls++
	for _, artifact := range artifacts {
		m.persistedArtifacts[artifact.ID] = artifact
	}
	return nil
}

func (m *memoryStateStore) LookupArtifact(_ context.Context, artifactID string) (ArtifactRecord, error) {
	artifact, ok := m.persistedArtifacts[artifactID]
	if !ok {
		return ArtifactRecord{}, ErrArtifactNotFound
	}
	return artifact, nil
}

func (m *memoryStateStore) FindSubmission(_ context.Context, submissionKind, idempotencyKey string) (*JobSubmission, error) {
	return m.submissions[submissionKind+"::"+idempotencyKey], nil
}

func (m *memoryStateStore) ListJobsBySubmission(_ context.Context, submissionID string) ([]JobRecord, error) {
	jobs := make([]JobRecord, 0, len(m.jobOrder))
	for _, jobID := range m.jobOrder {
		job := m.jobs[jobID]
		if job.SubmissionID == submissionID {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (m *memoryStateStore) SaveSubmissionGraph(_ context.Context, submission JobSubmission, sources []SourceRecord, sourceSets []SourceSetRecord, jobs []JobRecord) error {
	copySubmission := submission
	m.submissions[submission.SubmissionKind+"::"+submission.IdempotencyKey] = &copySubmission
	m.sources = append(m.sources, sources...)
	m.sourceSets = append(m.sourceSets, sourceSets...)
	for _, job := range jobs {
		m.jobs[job.ID] = job
		m.jobOrder = append(m.jobOrder, job.ID)
	}
	return nil
}

func (m *memoryStateStore) GetJob(_ context.Context, jobID string) (JobRecord, error) {
	job, ok := m.jobs[jobID]
	if !ok {
		return JobRecord{}, errors.New("job_not_found")
	}
	return job, nil
}

func (m *memoryStateStore) CreateJob(_ context.Context, job JobRecord) error {
	m.jobs[job.ID] = job
	m.jobOrder = append(m.jobOrder, job.ID)
	return nil
}

func (m *memoryStateStore) UpdateJob(_ context.Context, job JobRecord) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *memoryStateStore) FindCanonicalChild(_ context.Context, parentJobID, jobType string) (*JobRecord, error) {
	for _, jobID := range m.jobOrder {
		job := m.jobs[jobID]
		if job.ParentJobID == parentJobID && job.JobType == jobType && job.RetryOfJobID == "" {
			copyJob := job
			return &copyJob, nil
		}
	}
	return nil, nil
}

func (m *memoryStateStore) FindArtifactByKind(_ context.Context, jobID, artifactKind string) (*ArtifactRecord, error) {
	for _, artifact := range m.persistedArtifacts {
		if artifact.JobID == jobID && artifact.ArtifactKind == artifactKind {
			copyArtifact := artifact
			return &copyArtifact, nil
		}
	}
	return nil, nil
}

func (m *memoryStateStore) AppendJobEvent(_ context.Context, event JobEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *memoryStateStore) CreateWebhookDelivery(_ context.Context, delivery WebhookDelivery) error {
	m.deliveries = append(m.deliveries, delivery)
	return nil
}

func (m *memoryStateStore) UpdateWebhookDelivery(_ context.Context, delivery WebhookDelivery) error {
	for idx := range m.deliveries {
		if m.deliveries[idx].ID == delivery.ID {
			m.deliveries[idx] = delivery
			return nil
		}
	}
	m.deliveries = append(m.deliveries, delivery)
	return nil
}

func (m *memoryStateStore) ListDueWebhookDeliveries(_ context.Context, now time.Time, limit int) ([]WebhookDelivery, error) {
	due := make([]WebhookDelivery, 0, limit)
	for _, delivery := range m.deliveries {
		if delivery.State == WebhookStatePending && !delivery.NextAttemptAt.After(now) {
			due = append(due, delivery)
			if len(due) == limit {
				break
			}
		}
	}
	return due, nil
}

func (m *memoryStateStore) ClaimJobExecution(_ context.Context, req ClaimJobExecutionRequest) (ClaimJobExecutionResult, error) {
	job, ok := m.jobs[req.JobID]
	if !ok {
		return ClaimJobExecutionResult{}, errors.New("job_not_found")
	}
	if _, ok := m.activeExecutionByJob[req.JobID]; ok {
		return ClaimJobExecutionResult{Job: job, Claimed: false}, nil
	}
	if job.Status != "queued" {
		return ClaimJobExecutionResult{Job: job, Claimed: false}, nil
	}

	execution := JobExecutionRecord{
		ExecutionID: req.ExecutionID,
		JobID:       req.JobID,
		WorkerKind:  req.WorkerKind,
		TaskType:    req.TaskType,
		ClaimedAt:   req.ClaimedAt,
	}
	m.executions[execution.ExecutionID] = execution
	m.activeExecutionByJob[req.JobID] = execution.ExecutionID

	startedAt := req.ClaimedAt
	job.Status = "running"
	job.Version++
	job.ProgressStage = "running"
	job.ProgressMessage = ""
	job.StartedAt = &startedAt
	m.jobs[job.ID] = job

	return ClaimJobExecutionResult{
		Job:       job,
		Execution: &execution,
		Claimed:   true,
	}, nil
}

func (m *memoryStateStore) GetActiveJobExecution(_ context.Context, jobID, executionID string) (JobExecutionRecord, error) {
	activeExecutionID, ok := m.activeExecutionByJob[jobID]
	if !ok || activeExecutionID != executionID {
		return JobExecutionRecord{}, ErrExecutionNotFound
	}
	execution, ok := m.executions[executionID]
	if !ok || execution.FinishedAt != nil {
		return JobExecutionRecord{}, ErrExecutionNotFound
	}
	return execution, nil
}

func (m *memoryStateStore) FinishJobExecution(_ context.Context, req FinishJobExecutionRequest) (JobExecutionRecord, error) {
	execution, ok := m.executions[req.ExecutionID]
	if !ok || execution.JobID != req.JobID || execution.FinishedAt != nil {
		return JobExecutionRecord{}, ErrExecutionNotFound
	}
	finishedAt := req.FinishedAt
	execution.FinishedAt = &finishedAt
	execution.Outcome = req.Outcome
	m.executions[req.ExecutionID] = execution
	delete(m.activeExecutionByJob, req.JobID)
	return execution, nil
}

func (m *memoryStateStore) CreateBatchDraft(_ context.Context, draft BatchDraftRecord) (BatchDraftRecord, error) {
	draft.Items = []BatchDraftItem{}
	m.batchDrafts[draft.DraftID] = draft
	return draft, nil
}

func (m *memoryStateStore) GetBatchDraft(_ context.Context, draftID string) (BatchDraftRecord, error) {
	draft, ok := m.batchDrafts[draftID]
	if !ok {
		return BatchDraftRecord{}, ErrBatchDraftNotFound
	}
	return copyBatchDraft(draft), nil
}

func (m *memoryStateStore) AddBatchDraftItem(_ context.Context, req BatchDraftItemMutation, item BatchDraftItem) (BatchDraftRecord, error) {
	draft, err := m.lockDraftForMutation(req.DraftID, req.Owner, req.ExpectedVersion)
	if err != nil {
		return BatchDraftRecord{}, err
	}
	item.Position = len(draft.Items)
	draft.Items = append(draft.Items, item)
	draft.Version++
	draft.UpdatedAt = item.CreatedAt
	m.batchDrafts[draft.DraftID] = draft
	return copyBatchDraft(draft), nil
}

func (m *memoryStateStore) RemoveBatchDraftItem(_ context.Context, req BatchDraftItemRemove) (BatchDraftRecord, error) {
	draft, err := m.lockDraftForMutation(req.DraftID, req.Owner, req.ExpectedVersion)
	if err != nil {
		return BatchDraftRecord{}, err
	}
	next := make([]BatchDraftItem, 0, len(draft.Items))
	removed := false
	for _, item := range draft.Items {
		if item.ItemID == req.ItemID {
			removed = true
			continue
		}
		item.Position = len(next)
		next = append(next, item)
	}
	if !removed {
		return BatchDraftRecord{}, ErrBatchDraftNotFound
	}
	draft.Items = next
	draft.Version++
	m.batchDrafts[draft.DraftID] = draft
	return copyBatchDraft(draft), nil
}

func (m *memoryStateStore) ClearBatchDraft(_ context.Context, req BatchDraftMutation) (BatchDraftRecord, error) {
	draft, err := m.lockDraftForMutation(req.DraftID, req.Owner, req.ExpectedVersion)
	if err != nil {
		return BatchDraftRecord{}, err
	}
	draft.Items = []BatchDraftItem{}
	draft.Version++
	m.batchDrafts[draft.DraftID] = draft
	return copyBatchDraft(draft), nil
}

func (m *memoryStateStore) SubmitBatchDraftGraph(_ context.Context, req BatchDraftGraphSubmission) (BatchDraftRecord, error) {
	draft, ok := m.batchDrafts[req.DraftID]
	if !ok {
		return BatchDraftRecord{}, ErrBatchDraftNotFound
	}
	if !sameBatchDraftOwner(draft.Owner, req.Owner) {
		return BatchDraftRecord{}, ErrBatchDraftOwnerMismatch
	}
	if draft.Status == BatchDraftStatusSubmitted {
		return copyBatchDraft(draft), nil
	}
	if err := batchDraftTerminalError(draft); err != nil {
		return BatchDraftRecord{}, err
	}
	if draft.Version != req.ExpectedVersion {
		return BatchDraftRecord{}, ErrBatchDraftVersionConflict
	}
	if len(draft.Items) == 0 {
		return BatchDraftRecord{}, ErrBatchDraftEmpty
	}
	if len(req.Jobs) == 0 {
		return BatchDraftRecord{}, ErrContractViolation
	}
	m.SaveSubmissionGraph(context.Background(), req.Submission, req.Sources, req.SourceSets, req.Jobs)
	draft.Status = BatchDraftStatusSubmitted
	draft.Version++
	draft.SubmittedRootJobID = req.Jobs[0].ID
	m.batchDrafts[draft.DraftID] = draft
	return copyBatchDraft(draft), nil
}

func (m *memoryStateStore) lockDraftForMutation(draftID string, owner BatchDraftOwner, expectedVersion int64) (BatchDraftRecord, error) {
	draft, ok := m.batchDrafts[draftID]
	if !ok {
		return BatchDraftRecord{}, ErrBatchDraftNotFound
	}
	if !sameBatchDraftOwner(draft.Owner, owner) {
		return BatchDraftRecord{}, ErrBatchDraftOwnerMismatch
	}
	if err := batchDraftTerminalError(draft); err != nil {
		return BatchDraftRecord{}, err
	}
	if draft.Version != expectedVersion {
		return BatchDraftRecord{}, ErrBatchDraftVersionConflict
	}
	return draft, nil
}

func copyBatchDraft(draft BatchDraftRecord) BatchDraftRecord {
	draft.Items = append([]BatchDraftItem(nil), draft.Items...)
	return draft
}

type putCall struct {
	bucket      string
	objectKey   string
	contentType string
	body        []byte
}

type fakeObjectStore struct {
	puts                 []putCall
	failPutFor           map[string]error
	presignedURL         string
	internalPresignedURL string
	presignBaseTime      time.Time
	publicPresignCalls   int
	internalPresignCalls int
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{
		failPutFor:           map[string]error{},
		presignedURL:         "https://minio.local/presigned/default",
		internalPresignedURL: "https://minio.internal/presigned/default",
		presignBaseTime:      time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC),
	}
}

func (f *fakeObjectStore) PutObject(_ context.Context, bucket, objectKey, contentType string, body []byte) error {
	key := bucket + "::" + objectKey
	if err, ok := f.failPutFor[key]; ok {
		return err
	}
	f.puts = append(f.puts, putCall{bucket: bucket, objectKey: objectKey, contentType: contentType, body: append([]byte(nil), body...)})
	return nil
}

func (f *fakeObjectStore) PresignGetObject(_ context.Context, _, _ string, expiry time.Duration) (string, time.Time, error) {
	f.publicPresignCalls++
	return f.presignedURL, f.presignBaseTime.Add(expiry), nil
}

func (f *fakeObjectStore) PresignInternalGetObject(_ context.Context, _, _ string, expiry time.Duration) (string, time.Time, error) {
	f.internalPresignCalls++
	return f.internalPresignedURL, f.presignBaseTime.Add(expiry), nil
}

type bufferLogger struct {
	lines []string
}

func (b *bufferLogger) Printf(format string, args ...any) {
	b.lines = append(b.lines, fmt.Sprintf(strings.TrimSpace(format), args...))
}

func (b *bufferLogger) String() string {
	return strings.Join(b.lines, "\n")
}
