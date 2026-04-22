package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestApiStorageMigrationContract(t *testing.T) {
	t.Parallel()

	migrationPath := filepath.Join("migrations", "0001_phase2_storage.sql")
	migrationBytes, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", migrationPath, err)
	}

	migration := string(migrationBytes)
	for _, snippet := range requiredMigrationSnippets(t) {
		if !strings.Contains(migration, snippet) {
			t.Fatalf("migration is missing required snippet %q", snippet)
		}
	}

	if !strings.Contains(migration, "-- +goose Up") || !strings.Contains(migration, "-- +goose Down") {
		t.Fatalf("migration must expose goose up/down sections")
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

type memoryStateStore struct {
	bundle               PersistedJobBundle
	sources              []SourceRecord
	persistedArtifacts   map[string]ArtifactRecord
	artifactPersistCalls int
}

func newMemoryStateStore() *memoryStateStore {
	return &memoryStateStore{
		persistedArtifacts: map[string]ArtifactRecord{},
	}
}

func (m *memoryStateStore) PersistSource(_ context.Context, source SourceRecord) error {
	m.sources = append(m.sources, source)
	return nil
}

func (m *memoryStateStore) PersistJobBundle(_ context.Context, bundle PersistedJobBundle) error {
	m.bundle = bundle
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

type putCall struct {
	bucket    string
	objectKey string
}

type fakeObjectStore struct {
	puts            []putCall
	failPutFor      map[string]error
	presignedURL    string
	presignBaseTime time.Time
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{
		failPutFor:      map[string]error{},
		presignedURL:    "https://minio.local/presigned/default",
		presignBaseTime: time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC),
	}
}

func (f *fakeObjectStore) PutObject(_ context.Context, bucket, objectKey, _ string, _ []byte) error {
	key := bucket + "::" + objectKey
	if err, ok := f.failPutFor[key]; ok {
		return err
	}
	f.puts = append(f.puts, putCall{bucket: bucket, objectKey: objectKey})
	return nil
}

func (f *fakeObjectStore) PresignGetObject(_ context.Context, _, _ string, expiry time.Duration) (string, time.Time, error) {
	return f.presignedURL, f.presignBaseTime.Add(expiry), nil
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
