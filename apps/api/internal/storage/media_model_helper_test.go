package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

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
