package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type helperFailingPutObjectStore struct {
	err error
}

func (s *helperFailingPutObjectStore) PutObject(context.Context, string, string, string, []byte) error {
	return s.err
}

func (s *helperFailingPutObjectStore) PresignGetObject(context.Context, string, string, time.Duration) (string, time.Time, error) {
	return "", time.Time{}, nil
}

func TestSanitizeSourceFilenameFallbacks(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		originalFilename string
		contentType      string
		want             string
	}{
		{
			name:             "basename strips path separators",
			originalFilename: `nested\\folder/report.txt`,
			contentType:      "text/plain",
			want:             "report.txt",
		},
		{
			name:             "dot filename falls back to extension",
			originalFilename: ".",
			contentType:      "application/pdf",
			want:             "source.pdf",
		},
		{
			name:             "empty filename falls back to binary",
			originalFilename: "   ",
			contentType:      "application/x-unknown",
			want:             "source.bin",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := sanitizeSourceFilename(tc.originalFilename, tc.contentType); got != tc.want {
				t.Fatalf("sanitizeSourceFilename(%q, %q) = %q, want %q", tc.originalFilename, tc.contentType, got, tc.want)
			}
		})
	}
}

func TestNormalizeJSONAndDeliveryFallbacks(t *testing.T) {
	t.Parallel()

	if got := string(normalizeJSON(nil)); got != "{}" {
		t.Fatalf("normalizeJSON(nil) = %q, want {}", got)
	}
	if got := string(normalizeJSON([]byte("not-json"))); got != "{}" {
		t.Fatalf("normalizeJSON(invalid) = %q, want {}", got)
	}
	if got := string(normalizeDelivery(nil)); got != `{"strategy":"polling"}` {
		t.Fatalf("normalizeDelivery(nil) = %q", got)
	}
	if got := string(normalizeDelivery([]byte("not-json"))); got != `{"strategy":"polling"}` {
		t.Fatalf("normalizeDelivery(invalid) = %q", got)
	}
	if got := artifactObjectStoreKey("artifacts/run-1/report.md"); got != "artifacts/run-1/report.md" {
		t.Fatalf("artifactObjectStoreKey(artifacts/run-1/report.md) = %q, want full stored key", got)
	}

	merged := mergeJSONObject([]byte(`{"strategy":"push"}`), map[string]any{
		"strategy": "polling",
		"topic":    "reports",
	})
	var decoded map[string]any
	if err := json.Unmarshal(merged, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(merged) error = %v", err)
	}
	if decoded["strategy"] != "push" || decoded["topic"] != "reports" {
		t.Fatalf("merged payload = %#v", decoded)
	}
	if got := string(mergeJSONObject([]byte("{"), map[string]any{"topic": "reports"})); got != `{"topic":"reports"}` {
		t.Fatalf("mergeJSONObject(invalid base) = %q, want merged fallback object", got)
	}
}

func TestMergeJSONObjectMarshalFailureReturnsEmptyObject(t *testing.T) {
	t.Parallel()

	if got := string(mergeJSONObject(nil, map[string]any{"bad": func() {}})); got != "{}" {
		t.Fatalf("mergeJSONObject(marshal failure) = %q, want {}", got)
	}
}

func TestWorkerAndTaskKindsDefaultToAnalysis(t *testing.T) {
	t.Parallel()

	if got := workerKindForRunType("summary"); got != "analysis_runner" {
		t.Fatalf("workerKindForRunType(summary) = %q, want analysis_runner", got)
	}
	if got := taskTypeForRunType("summary"); got != "selection.analysis" {
		t.Fatalf("taskTypeForRunType(summary) = %q, want selection.analysis", got)
	}
}

func TestAddMediaItemValidationRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(newMemoryStateStore(), newFakeObjectStore(), WithClock(func() time.Time {
		return time.Date(2026, 5, 11, 10, 0, 0, 0, time.UTC)
	}))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	validOwner := OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"}
	cases := []struct {
		name string
		req  AddMediaItemRequest
	}{
		{
			name: "missing owner",
			req: AddMediaItemRequest{
				Kind:   "text",
				Source: AddMediaSource{OriginType: "text", Text: "hello"},
			},
		},
		{
			name: "missing kind",
			req: AddMediaItemRequest{
				Owner:  validOwner,
				Source: AddMediaSource{OriginType: "text", Text: "hello"},
			},
		},
		{
			name: "text source requires text",
			req: AddMediaItemRequest{
				Owner:  validOwner,
				Kind:   "text",
				Source: AddMediaSource{OriginType: "text"},
			},
		},
		{
			name: "url source requires url",
			req: AddMediaItemRequest{
				Owner:  validOwner,
				Kind:   "url",
				Source: AddMediaSource{OriginType: "url"},
			},
		},
		{
			name: "object source rejects mixed ref and upload",
			req: AddMediaItemRequest{
				Owner: validOwner,
				Kind:  "file",
				Source: AddMediaSource{
					OriginType: "object",
					ObjectRef:  "sources/file.bin",
					UploadBody: []byte("payload"),
				},
			},
		},
		{
			name: "object source requires ref or body",
			req: AddMediaItemRequest{
				Owner: validOwner,
				Kind:  "file",
				Source: AddMediaSource{
					OriginType: "object",
				},
			},
		},
		{
			name: "telegram file refs are rejected",
			req: AddMediaItemRequest{
				Owner: validOwner,
				Kind:  "voice",
				Source: AddMediaSource{
					OriginType: "object",
					ObjectRef:  "telegram://file/voice-file",
				},
			},
		},
		{
			name: "uploaded body size mismatch is rejected",
			req: AddMediaItemRequest{
				Owner: validOwner,
				Kind:  "voice",
				Source: AddMediaSource{
					OriginType:  "object",
					ContentType: "audio/ogg",
					UploadBody:  []byte("voice"),
					SizeBytes:   99,
				},
			},
		},
		{
			name: "unsupported source type",
			req: AddMediaItemRequest{
				Owner:  validOwner,
				Kind:   "text",
				Source: AddMediaSource{OriginType: "ftp", URL: "ftp://example.com/a"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := repo.AddMediaItem(context.Background(), tc.req); !errors.Is(err, ErrContractViolation) {
				t.Fatalf("AddMediaItem(%s) error = %v, want ErrContractViolation", tc.name, err)
			}
		})
	}
}

func TestAddMediaItemUploadDefaultsObjectMetadata(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	objectStore := newFakeObjectStore()
	repo, err := NewRepository(
		state,
		objectStore,
		WithIDGenerator(sequenceIDs("source-1", "media-1", "inbox-1")),
		WithClock(func() time.Time { return time.Date(2026, 5, 11, 11, 0, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	item, err := repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
		Kind:  "voice",
		Source: AddMediaSource{
			OriginType: "object",
			UploadBody: []byte("voice-body"),
		},
	})
	if err != nil {
		t.Fatalf("AddMediaItem(upload defaults) error = %v", err)
	}

	if item.DisplayName != "voice" {
		t.Fatalf("display name = %q, want kind fallback", item.DisplayName)
	}
	if item.Source.MIMEType != "application/octet-stream" {
		t.Fatalf("mime type = %q, want application/octet-stream", item.Source.MIMEType)
	}
	if item.Source.Checksum == "" {
		t.Fatalf("checksum = %q, want generated checksum", item.Source.Checksum)
	}
	if item.Source.SizeBytes == nil || *item.Source.SizeBytes != int64(len("voice-body")) {
		t.Fatalf("size bytes = %#v, want %d", item.Source.SizeBytes, len("voice-body"))
	}
	if len(objectStore.puts) != 1 {
		t.Fatalf("object puts = %d, want 1", len(objectStore.puts))
	}
	if objectStore.puts[0].contentType != "application/octet-stream" {
		t.Fatalf("stored content type = %q, want application/octet-stream", objectStore.puts[0].contentType)
	}
}

func TestAddMediaItemUploadPropagatesObjectStoreFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("put failed")
	repo, err := NewRepository(
		newMemoryStateStore(),
		&helperFailingPutObjectStore{err: expectedErr},
		WithIDGenerator(sequenceIDs("source-1")),
	)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	_, err = repo.AddMediaItem(context.Background(), AddMediaItemRequest{
		Owner: OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
		Kind:  "voice",
		Source: AddMediaSource{
			OriginType: "object",
			UploadBody: []byte("voice-body"),
		},
	})
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("AddMediaItem(put failure) error = %v, want ErrStorageUnavailable", err)
	}
}
