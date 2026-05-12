package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

type failingMultipartUploadReader struct{}

func (failingMultipartUploadReader) Read(_ []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

type updateCollectionItemsErrorStore struct {
	*fakePublicService
	updateErr error
}

func (s *updateCollectionItemsErrorStore) UpdateCollectionItems(_ context.Context, req storage.UpdateCollectionItemsRequest) (storage.CollectionRecord, error) {
	s.lastUpdateCollectionItems = req
	return storage.CollectionRecord{}, s.updateErr
}

func TestApiHttpFinalRoutesAddMediaWithoutStartingAnalysis(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	public := &fakePublicService{
		mediaItem: storage.MediaItemRecord{
			ID:          "11111111-1111-1111-1111-111111111111",
			Owner:       storage.OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
			Kind:        "text",
			Status:      storage.MediaStatusReady,
			DisplayName: "note",
			Source: storage.MediaSourceMetadata{
				SourceID:   "22222222-2222-2222-2222-222222222222",
				OriginType: "text",
				TextRef:    "inline:22222222-2222-2222-2222-222222222222",
			},
			Retention: storage.RetentionMetadata{State: storage.RetentionStateActive},
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/media-items", map[string]any{
		"owner": map[string]any{"owner_type": "telegram", "owner_id": "chat-1"},
		"kind":  "text",
		"source": map[string]any{
			"origin_type": "text",
			"text":        "hello",
		},
		"display_name": "note",
	}))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if public.lastAddMedia.Source.OriginType != "text" || public.createAnalysisRunCalls != 0 {
		t.Fatalf("add media should persist only, got source=%#v run_calls=%d", public.lastAddMedia.Source, public.createAnalysisRunCalls)
	}
	var body map[string]storage.MediaItemRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(response) error = %v", err)
	}
	if body["media_item"].ID != public.mediaItem.ID {
		t.Fatalf("media_item_id = %q, want %q", body["media_item"].ID, public.mediaItem.ID)
	}
}

func TestApiHttpFinalRoutesAddUploadedMediaItemWithoutPseudoObjectRef(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	public := &fakePublicService{
		mediaItem: storage.MediaItemRecord{
			ID:          "11111111-1111-1111-1111-111111111111",
			Owner:       storage.OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
			Kind:        "voice",
			Status:      storage.MediaStatusReady,
			DisplayName: "voice.ogg",
			Source: storage.MediaSourceMetadata{
				SourceID:   "22222222-2222-2222-2222-222222222222",
				OriginType: "object",
				ObjectKey:  "sources/telegram/chat-1/voice.ogg",
				MIMEType:   "audio/ogg",
			},
			Retention: storage.RetentionMetadata{State: storage.RetentionStateActive},
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metadataWriter, err := writer.CreateFormField("metadata")
	if err != nil {
		t.Fatalf("CreateFormField(metadata) error = %v", err)
	}
	if _, err := metadataWriter.Write([]byte(`{"owner":{"owner_type":"telegram","owner_id":"chat-1"},"kind":"voice","display_name":"voice.ogg","adapter_origin":"telegram"}`)); err != nil {
		t.Fatalf("Write(metadata) error = %v", err)
	}
	fileWriter, err := writer.CreateFormFile("file", "voice.ogg")
	if err != nil {
		t.Fatalf("CreateFormFile(file) error = %v", err)
	}
	if _, err := fileWriter.Write([]byte("voice-bytes")); err != nil {
		t.Fatalf("Write(file) error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/media-items", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if public.lastAddMedia.Source.OriginType != "object" {
		t.Fatalf("source origin_type = %q, want object", public.lastAddMedia.Source.OriginType)
	}
	if public.lastAddMedia.Source.ObjectRef != "" {
		t.Fatalf("multipart ingest must not forward client object_ref, got %q", public.lastAddMedia.Source.ObjectRef)
	}
	if !bytes.Equal(public.lastAddMedia.Source.UploadBody, []byte("voice-bytes")) {
		t.Fatalf("upload body = %q, want voice-bytes", string(public.lastAddMedia.Source.UploadBody))
	}
	if public.lastAddMedia.Source.OriginalFilename != "voice.ogg" {
		t.Fatalf("original filename = %q, want voice.ogg", public.lastAddMedia.Source.OriginalFilename)
	}
	if public.lastAddMedia.Source.ContentType != "application/octet-stream" {
		t.Fatalf("content type = %q, want application/octet-stream", public.lastAddMedia.Source.ContentType)
	}
}

func TestApiHttpMultipartValidationAndErrorBranches(t *testing.T) {
	t.Parallel()

	buildMultipartRequest := func(t *testing.T, metadata string, includeFile bool, fileContentType string) *http.Request {
		t.Helper()

		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		if metadata != "" {
			metadataWriter, err := writer.CreateFormField("metadata")
			if err != nil {
				t.Fatalf("CreateFormField(metadata) error = %v", err)
			}
			if _, err := metadataWriter.Write([]byte(metadata)); err != nil {
				t.Fatalf("Write(metadata) error = %v", err)
			}
		}
		if includeFile {
			header := textproto.MIMEHeader{}
			header.Set("Content-Disposition", `form-data; name="file"; filename="voice.ogg"`)
			if strings.TrimSpace(fileContentType) != "" {
				header.Set("Content-Type", fileContentType)
			}
			fileWriter, err := writer.CreatePart(header)
			if err != nil {
				t.Fatalf("CreatePart(file) error = %v", err)
			}
			if _, err := fileWriter.Write([]byte("voice-bytes")); err != nil {
				t.Fatalf("Write(file) error = %v", err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("writer.Close() error = %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/v1/media-items", &body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		return req
	}

	validMetadata := `{"owner":{"owner_type":"telegram","owner_id":"chat-1"},"kind":"voice","display_name":"voice.ogg","adapter_origin":"telegram"}`

	t.Run("rejects malformed multipart bodies", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/media-items", strings.NewReader("not-a-multipart-body"))
		req.Header.Set("Content-Type", "multipart/form-data")

		newFinalMux(Dependencies{Public: &fakePublicService{}}).ServeHTTP(rec, req)
		assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_item")
	})

	t.Run("rejects missing metadata", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{}}).ServeHTTP(rec, buildMultipartRequest(t, "", true, ""))
		assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_item")
	})

	t.Run("rejects invalid metadata json", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := buildMultipartRequest(t, `{"owner":{"owner_type":"telegram","owner_id":"chat-1"},"kind":"voice","unexpected":true}`, true, "")
		newFinalMux(Dependencies{Public: &fakePublicService{}}).ServeHTTP(rec, req)
		assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_item")
	})

	t.Run("rejects missing file", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{}}).ServeHTTP(rec, buildMultipartRequest(t, validMetadata, false, ""))
		assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_item")
	})

	t.Run("maps public service errors for multipart uploads", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := buildMultipartRequest(t, validMetadata, true, "")
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}).ServeHTTP(rec, req)
		assertErrorCode(t, rec, http.StatusNotFound, "not_found")
	})

	t.Run("preserves explicit upload content type", func(t *testing.T) {
		t.Parallel()

		public := &fakePublicService{
			mediaItem: storage.MediaItemRecord{
				ID:          "media-1",
				Owner:       storage.OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
				Kind:        "voice",
				Status:      storage.MediaStatusReady,
				DisplayName: "voice.ogg",
				Source: storage.MediaSourceMetadata{
					SourceID:   "source-1",
					OriginType: "object",
					ObjectKey:  "sources/telegram/chat-1/voice.ogg",
					MIMEType:   "audio/ogg",
				},
				Retention: storage.RetentionMetadata{State: storage.RetentionStateActive},
				CreatedAt: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
			},
		}

		rec := httptest.NewRecorder()
		req := buildMultipartRequest(t, validMetadata, true, "audio/ogg")
		newFinalMux(Dependencies{Public: public}).ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
		}
		if public.lastAddMedia.Source.ContentType != "audio/ogg" {
			t.Fatalf("content type = %q, want audio/ogg", public.lastAddMedia.Source.ContentType)
		}
	})
}

func TestReadMultipartUploadBodyPropagatesReaderErrors(t *testing.T) {
	t.Parallel()

	if _, err := readMultipartUploadBody(failingMultipartUploadReader{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readMultipartUploadBody() error = %v, want ErrUnexpectedEOF", err)
	}
}

func TestServerReadMultipartUploadBodyMapsReaderErrors(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	body, ok := (&Server{readUploadBody: func(io.Reader) ([]byte, error) {
		return nil, io.ErrUnexpectedEOF
	}}).readMultipartUploadBody(rec, strings.NewReader("ignored"))
	if ok || body != nil {
		t.Fatalf("readMultipartUploadBody(ok=%v, body=%v), want !ok nil", ok, body)
	}
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_item")
}

func TestHandleAddMediaItemMultipartMapsUnreadableUploadBodies(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("metadata", `{"owner":{"owner_type":"telegram","owner_id":"chat-1"},"kind":"voice","display_name":"voice.ogg","adapter_origin":"telegram"}`); err != nil {
		t.Fatalf("WriteField(metadata) error = %v", err)
	}
	fileWriter, err := writer.CreateFormFile("file", "voice.ogg")
	if err != nil {
		t.Fatalf("CreateFormFile(file) error = %v", err)
	}
	if _, err := fileWriter.Write([]byte("voice-bytes")); err != nil {
		t.Fatalf("Write(file) error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/media-items", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	server := &Server{
		deps: Dependencies{Public: &fakePublicService{}},
		readUploadBody: func(io.Reader) ([]byte, error) {
			return nil, io.ErrUnexpectedEOF
		},
		maxRequestBytes: defaultMaxRequestBody,
	}
	server.handleAddMediaItemMultipart(rec, req)
	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_item")
}

func TestApiHttpFinalRoutesPropagateCollectionVersionConflict(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{err: storage.ErrCollectionVersionConflict}
	mux := newFinalMux(Dependencies{Public: public})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/collections/11111111-1111-1111-1111-111111111111/items", map[string]any{
		"owner":            map[string]any{"owner_type": "web", "owner_id": "u-1"},
		"expected_version": float64(1),
		"items": []map[string]any{{
			"media_item_id": "22222222-2222-2222-2222-222222222222",
			"position":      float64(0),
		}},
	}))

	assertErrorCode(t, rec, http.StatusConflict, "collection_version_conflict")
}

func TestApiHttpFinalRoutesCreateSelectionAndRun(t *testing.T) {
	t.Parallel()

	selection := storage.SelectionRecord{
		ID:        "33333333-3333-3333-3333-333333333333",
		Owner:     storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"},
		Status:    storage.SelectionStatusSealed,
		CreatedBy: "u-1",
		Items: []storage.SelectionItemSnapshot{{
			Position:          0,
			MediaItemID:       "22222222-2222-2222-2222-222222222222",
			Kind:              "file",
			DisplayName:       "clip.mp3",
			StatusAtSelection: storage.MediaStatusReady,
			RetentionSnapshot: storage.RetentionMetadata{State: storage.RetentionStateActive},
		}},
		CreatedAt: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		SealedAt:  time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}
	public := &fakePublicService{
		selection: selection,
		run: storage.AnalysisRunRecord{
			ID:                "44444444-4444-4444-4444-444444444444",
			Owner:             selection.Owner,
			SelectionID:       selection.ID,
			Selection:         selection,
			RunType:           "transcription",
			Status:            storage.AnalysisRunStatusQueued,
			Version:           1,
			DeliveryJSON:      []byte(`{"strategy":"polling"}`),
			EvidenceGateState: "not_required",
			CreatedAt:         selection.CreatedAt,
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	selectionRec := httptest.NewRecorder()
	mux.ServeHTTP(selectionRec, jsonRequest(http.MethodPost, "/v1/selections", map[string]any{
		"owner": map[string]any{"owner_type": "web", "owner_id": "u-1"},
		"items": []map[string]any{{
			"media_item_id": "22222222-2222-2222-2222-222222222222",
			"position":      float64(0),
		}},
	}))
	if selectionRec.Code != http.StatusCreated {
		t.Fatalf("selection status = %d want 201 body=%s", selectionRec.Code, selectionRec.Body.String())
	}

	runRec := httptest.NewRecorder()
	mux.ServeHTTP(runRec, jsonRequest(http.MethodPost, "/v1/analysis-runs", map[string]any{
		"owner":        map[string]any{"owner_type": "web", "owner_id": "u-1"},
		"selection_id": selection.ID,
		"run_type":     "transcription",
	}))
	if runRec.Code != http.StatusAccepted {
		t.Fatalf("run status = %d want 202 body=%s", runRec.Code, runRec.Body.String())
	}
	if public.lastRun.SelectionID != selection.ID || public.lastRun.RunType != "transcription" {
		t.Fatalf("run request = %#v", public.lastRun)
	}
}

func TestApiHttpListEndpointsUseCursorPagedSummaries(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	public := &fakePublicService{
		mediaItems: []storage.MediaItemRecord{
			{ID: "media-1", Owner: owner, Kind: "text", Status: storage.MediaStatusReady, DisplayName: "first", Source: storage.MediaSourceMetadata{SourceID: "source-1", OriginType: "text"}, Retention: storage.RetentionMetadata{State: storage.RetentionStateActive}, CreatedAt: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)},
			{ID: "media-2", Owner: owner, Kind: "url", Status: storage.MediaStatusReady, DisplayName: "second", Source: storage.MediaSourceMetadata{SourceID: "source-2", OriginType: "url"}, Retention: storage.RetentionMetadata{State: storage.RetentionStateActive}, CreatedAt: time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC)},
		},
		runs: []storage.AnalysisRunRecord{
			{ID: "run-1", Owner: owner, SelectionID: "selection-1", RunType: "transcription", Status: storage.AnalysisRunStatusQueued, Version: 1, EvidenceGateState: "not_required", CreatedAt: time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC)},
			{ID: "run-2", Owner: owner, SelectionID: "selection-2", RunType: "report", Status: storage.AnalysisRunStatusQueued, Version: 1, EvidenceGateState: "not_required", CreatedAt: time.Date(2026, 5, 10, 12, 3, 0, 0, time.UTC)},
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	mediaRec := httptest.NewRecorder()
	mux.ServeHTTP(mediaRec, httptest.NewRequest(http.MethodGet, "/v1/media-items?owner_type=web&owner_id=u-1&page_size=1", nil))
	if mediaRec.Code != http.StatusOK {
		t.Fatalf("media list status = %d want 200 body=%s", mediaRec.Code, mediaRec.Body.String())
	}
	var mediaBody struct {
		Items []struct {
			ID               string `json:"media_item_id"`
			DisplayName      string `json:"display_name"`
			DiagnosticsCount int    `json:"diagnostics_count"`
		} `json:"items"`
		Page struct {
			PageSize   int    `json:"page_size"`
			HasMore    bool   `json:"has_more"`
			NextCursor string `json:"next_cursor"`
		} `json:"page"`
	}
	if err := json.Unmarshal(mediaRec.Body.Bytes(), &mediaBody); err != nil {
		t.Fatalf("Unmarshal(media list) error = %v", err)
	}
	if len(mediaBody.Items) != 1 || mediaBody.Items[0].ID != "media-1" || !mediaBody.Page.HasMore || mediaBody.Page.NextCursor != "media-1" {
		t.Fatalf("media page = %#v", mediaBody)
	}

	runRec := httptest.NewRecorder()
	mux.ServeHTTP(runRec, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs?owner_type=web&owner_id=u-1&page_size=1&cursor=run-1", nil))
	if runRec.Code != http.StatusOK {
		t.Fatalf("run list status = %d want 200 body=%s", runRec.Code, runRec.Body.String())
	}
	var runBody struct {
		Items []struct {
			ID               string `json:"analysis_run_id"`
			SelectionID      string `json:"selection_id"`
			ArtifactCount    int    `json:"artifact_count"`
			DiagnosticsCount int    `json:"diagnostics_count"`
		} `json:"items"`
		Page struct {
			HasMore bool `json:"has_more"`
		} `json:"page"`
	}
	if err := json.Unmarshal(runRec.Body.Bytes(), &runBody); err != nil {
		t.Fatalf("Unmarshal(run list) error = %v", err)
	}
	if len(runBody.Items) != 1 || runBody.Items[0].ID != "run-2" || runBody.Page.HasMore {
		t.Fatalf("run page = %#v", runBody)
	}
}

func TestApiHttpArtifactAccessIsOwnerScoped(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	public := &fakePublicService{
		artifact: storage.ArtifactRecord{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "transcript",
			Status:        "available",
			ContentType:   "text/plain",
			SizeBytes:     10,
			Visibility:    "owner",
			Download:      &storage.DownloadDescriptor{Provider: "object_store", URL: "https://minio.local/presigned", ExpiresAt: time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)},
			Retention:     storage.RetentionMetadata{State: storage.RetentionStateActive},
			CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/artifact-1?owner_type=web&owner_id=u-1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("artifact status = %d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Artifact storage.ArtifactRecord `json:"artifact"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(artifact) error = %v", err)
	}
	if body.Artifact.Download == nil || body.Artifact.Download.URL == "" {
		t.Fatalf("artifact download = %#v", body.Artifact.Download)
	}

	public.err = storage.ErrOwnerMismatch
	denied := httptest.NewRecorder()
	mux.ServeHTTP(denied, httptest.NewRequest(http.MethodGet, "/v1/artifacts/artifact-1?owner_type=web&owner_id=other", nil))
	assertErrorCode(t, denied, http.StatusNotFound, "not_found")
}

func TestApiHttpAnalysisRunArtifactsRouteUsesPathRunIDAndPagination(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	public := &fakePublicService{
		artifacts: []storage.ArtifactRecord{
			{
				ID:            "artifact-1",
				Owner:         owner,
				AnalysisRunID: "run-1",
				Kind:          "transcript_plain",
				Status:        storage.ArtifactStatusAvailable,
				ContentType:   "text/plain",
				SizeBytes:     10,
				Visibility:    "owner",
				Retention:     storage.RetentionMetadata{State: storage.RetentionStateActive},
				CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
			},
			{
				ID:            "artifact-2",
				Owner:         owner,
				AnalysisRunID: "run-1",
				Kind:          "summary_markdown",
				Status:        storage.ArtifactStatusAvailable,
				ContentType:   "text/markdown",
				SizeBytes:     20,
				Visibility:    "owner",
				Retention:     storage.RetentionMetadata{State: storage.RetentionStateActive},
				CreatedAt:     time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC),
			},
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/artifacts?owner_type=web&owner_id=u-1&page_size=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("artifact list status = %d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if public.listArtifactsAnalysisRunID != "run-1" {
		t.Fatalf("list artifacts analysis_run_id = %q, want run-1", public.listArtifactsAnalysisRunID)
	}

	var body struct {
		Items []struct {
			ID string `json:"artifact_id"`
		} `json:"items"`
		Page struct {
			PageSize   int    `json:"page_size"`
			HasMore    bool   `json:"has_more"`
			NextCursor string `json:"next_cursor"`
		} `json:"page"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(artifact list) error = %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].ID != "artifact-1" {
		t.Fatalf("artifact page items = %#v", body.Items)
	}
	if body.Page.PageSize != 1 || !body.Page.HasMore || body.Page.NextCursor != "artifact-1" {
		t.Fatalf("artifact page = %#v", body.Page)
	}
}

func TestApiHttpAdminLifecycleRoutesCancelRetryRefreshAndReconcile(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	public := &fakePublicService{
		run: storage.AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "transcription",
			Status:      storage.AnalysisRunStatusRunning,
			CreatedAt:   time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		},
		artifact: storage.ArtifactRecord{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "transcript",
			Status:        storage.ArtifactStatusAvailable,
			ContentType:   "text/plain",
			SizeBytes:     42,
			Visibility:    "owner",
			Download:      &storage.DownloadDescriptor{Provider: "object_store", URL: "https://minio.local/refreshed", ExpiresAt: time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)},
			CreatedAt:     time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC),
		},
		reconciled: 2,
	}
	mux := newFinalMux(Dependencies{Public: public})

	cancel := httptest.NewRecorder()
	mux.ServeHTTP(cancel, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/cancel?owner_type=web&owner_id=u-1", map[string]any{
		"message": "operator cancel",
	}))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status = %d want 200 body=%s", cancel.Code, cancel.Body.String())
	}
	if public.canceledRunID != "run-1" || public.canceledMessage != "operator cancel" {
		t.Fatalf("cancel call = id:%q message:%q", public.canceledRunID, public.canceledMessage)
	}

	retry := httptest.NewRecorder()
	mux.ServeHTTP(retry, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry?owner_type=web&owner_id=u-1", map[string]any{
		"owner": map[string]any{"owner_type": "web", "owner_id": "u-1"},
	}))
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d want 202 body=%s", retry.Code, retry.Body.String())
	}
	if public.retriedRunID != "run-1" {
		t.Fatalf("retried run id = %q", public.retriedRunID)
	}

	refresh := httptest.NewRecorder()
	mux.ServeHTTP(refresh, jsonRequest(http.MethodPost, "/v1/artifacts/artifact-1/refresh?owner_type=web&owner_id=u-1", nil))
	if refresh.Code != http.StatusOK {
		t.Fatalf("refresh status = %d want 200 body=%s", refresh.Code, refresh.Body.String())
	}
	if public.refreshedArtifactID != "artifact-1" {
		t.Fatalf("refreshed artifact id = %q", public.refreshedArtifactID)
	}

	reconcile := httptest.NewRecorder()
	mux.ServeHTTP(reconcile, jsonRequest(http.MethodPost, "/v1/admin/reconcile-queue", map[string]any{"limit": float64(10)}))
	if reconcile.Code != http.StatusAccepted {
		t.Fatalf("reconcile status = %d want 202 body=%s", reconcile.Code, reconcile.Body.String())
	}
	var reconcileBody struct {
		Reconciled int `json:"reconciled"`
	}
	if err := json.Unmarshal(reconcile.Body.Bytes(), &reconcileBody); err != nil {
		t.Fatalf("Unmarshal(reconcile) error = %v", err)
	}
	if reconcileBody.Reconciled != 2 {
		t.Fatalf("reconcile body = %#v, want 2", reconcileBody)
	}
}

func TestApiHttpObservabilitySurfacesOperationalFailuresAndQueueLag(t *testing.T) {
	t.Parallel()

		public := &fakePublicService{
			observability: storage.ObservabilitySnapshot{
				QueueTasks:                       3,
				QueueLagSeconds:                  42,
				CleanupFailures:                  1,
				CleanupFailuresRecent:            1,
				ArtifactResolutionFailures:       2,
				ArtifactResolutionFailuresRecent: 1,
				ObservabilityWindowSeconds:       900,
				GeneratedAt:                      time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
			},
		}
	mux := newFinalMux(Dependencies{Public: public})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/admin/observability", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("observability status = %d want 200 body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Observability storage.ObservabilitySnapshot `json:"observability"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(observability) error = %v", err)
	}
		if body.Observability.QueueLagSeconds != 42 ||
			body.Observability.CleanupFailures != 1 ||
			body.Observability.CleanupFailuresRecent != 1 ||
			body.Observability.ArtifactResolutionFailures != 2 ||
			body.Observability.ArtifactResolutionFailuresRecent != 1 ||
			body.Observability.ObservabilityWindowSeconds != 900 {
			t.Fatalf("observability = %#v", body.Observability)
		}
	}

func TestApiHttpPublicHandlerErrorMappings(t *testing.T) {
	t.Parallel()

	owner := map[string]any{"owner_type": "web", "owner_id": "u-1"}

	t.Run("media item dependency unavailable", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/media-items", map[string]any{
			"owner": owner,
			"kind":  "text",
			"source": map[string]any{
				"origin_type": "text",
				"text":        "hello",
			},
		}))
		assertErrorCode(t, rec, http.StatusServiceUnavailable, "dependency_unavailable")
	})

	t.Run("json media add maps owner mismatch", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/media-items", map[string]any{
			"owner": owner,
			"kind":  "text",
			"source": map[string]any{
				"origin_type": "text",
				"text":        "hello",
			},
		}))
		assertErrorCode(t, rec, http.StatusNotFound, "not_found")
	})

	t.Run("create collection maps owner mismatch", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/collections", map[string]any{
			"owner": owner,
			"name":  "Review set",
		}))
		assertErrorCode(t, rec, http.StatusNotFound, "not_found")
	})

		t.Run("update collection maps contract violation", func(t *testing.T) {
			t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrContractViolation}}).ServeHTTP(rec, jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{
			"owner":            owner,
			"expected_version": float64(3),
			"name":             "Review set v2",
		}))
			assertErrorCode(t, rec, http.StatusBadRequest, "invalid_request")
		})

		t.Run("update collection items maps owner mismatch", func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{
				"owner":            owner,
				"expected_version": float64(3),
				"items":            []map[string]any{{"media_item_id": "media-1", "position": float64(0)}},
			}))
			assertErrorCode(t, rec, http.StatusNotFound, "not_found")
		})

		t.Run("create selection maps owner mismatch", func(t *testing.T) {
			t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/selections", map[string]any{
			"owner": owner,
			"items": []map[string]any{{"media_item_id": "media-1", "position": float64(0)}},
		}))
		assertErrorCode(t, rec, http.StatusNotFound, "not_found")
	})

	t.Run("cancel run maps owner mismatch", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/cancel?owner_type=web&owner_id=u-1", map[string]any{
			"message": "stop",
		}))
		assertErrorCode(t, rec, http.StatusNotFound, "not_found")
	})

	t.Run("retry run maps terminal conflict", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrRetryRequiresTerminalRun}}).ServeHTTP(rec, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry?owner_type=web&owner_id=u-1", map[string]any{
			"owner": owner,
		}))
		assertErrorCode(t, rec, http.StatusConflict, "retry_requires_terminal_run")
	})
}

func TestApiHttpRegistersFinalInternalExecutionRoutes(t *testing.T) {
	t.Parallel()

	mux := newFinalMux(Dependencies{Public: &fakePublicService{}, Worker: &fakeWorkerService{}})
	queueRec := httptest.NewRecorder()
	mux.ServeHTTP(queueRec, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/queue?status=queued&run_type=transcription&task_type=selection.transcription&page_size=1", nil))
	if queueRec.Code != http.StatusOK {
		t.Fatalf("queue status = %d, want 200 body=%s", queueRec.Code, queueRec.Body.String())
	}
	var queueBody AnalysisRunQueueResponse
	if err := json.Unmarshal(queueRec.Body.Bytes(), &queueBody); err != nil {
		t.Fatalf("Unmarshal(queue response) error = %v", err)
	}
	if len(queueBody.Items) != 1 || queueBody.Items[0].AnalysisRunID == "" || queueBody.Items[0].TaskType != "selection.transcription" {
		t.Fatalf("queue response = %#v", queueBody)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/44444444-4444-4444-4444-444444444444/executions/claim", map[string]any{
		"worker_kind": "transcription",
		"task_type":   "selection.transcription",
		"lease_owner": "worker-1",
	}))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(claim response) error = %v", err)
	}
	for _, key := range []string{"execution_id", "analysis_run_id", "run_type", "selection", "params", "claimed_at"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("claim response missing %q: %#v", key, body)
		}
	}
	for _, stale := range []string{"analysis_run", "items", "claimed"} {
		if _, ok := body[stale]; ok {
			t.Fatalf("claim response leaked stale wrapper field %q: %#v", stale, body)
		}
	}
	selection, ok := body["selection"].(map[string]any)
	if !ok {
		t.Fatalf("selection missing or invalid: %#v", body["selection"])
	}
	items, ok := selection["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("selection.items missing or invalid: %#v", selection["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("selection item missing or invalid: %#v", items[0])
	}
	for _, key := range []string{"selection_item_id", "media_kind", "mime_type", "role", "labels"} {
		if _, ok := item[key]; !ok {
			t.Fatalf("claim selection item missing %q: %#v", key, item)
		}
	}
	if item["selection_item_id"] != "66666666-6666-6666-6666-666666666666" {
		t.Fatalf("selection_item_id = %#v, want persisted selection item id", item["selection_item_id"])
	}
	if item["media_kind"] != "audio" || item["mime_type"] != "audio/wav" || item["role"] != "primary" {
		t.Fatalf("claim selection item v2 fields mismatch: %#v", item)
	}
	labels, ok := item["labels"].(map[string]any)
	if !ok {
		t.Fatalf("labels missing or invalid: %#v", item["labels"])
	}
	if labels["display_label"] != "source.wav" || labels["source_label"] != "voice_a" || labels["original_filename"] != "source.wav" {
		t.Fatalf("labels mismatch: %#v", labels)
	}
	for _, stale := range []string{"selection_item_snapshot", "selection_item"} {
		if _, ok := item[stale]; ok {
			t.Fatalf("claim selection item leaked stale wrapper field %q: %#v", stale, item)
		}
	}

	accessRec := httptest.NewRecorder()
	mux.ServeHTTP(accessRec, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/44444444-4444-4444-4444-444444444444/request-access?execution_id=55555555-5555-5555-5555-555555555555", nil))
	if accessRec.Code != http.StatusOK {
		t.Fatalf("request-access status = %d, want 200 body=%s", accessRec.Code, accessRec.Body.String())
	}
	var accessBody RequestAccessResponse
	if err := json.Unmarshal(accessRec.Body.Bytes(), &accessBody); err != nil {
		t.Fatalf("Unmarshal(request-access response) error = %v", err)
	}
	if accessBody.RequestRef != "agentreq_digest" || accessBody.RequestDigestSHA256 == "" || accessBody.RequestBytes != 123 {
		t.Fatalf("request-access response = %#v", accessBody)
	}

	cancelRec := httptest.NewRecorder()
	mux.ServeHTTP(cancelRec, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/44444444-4444-4444-4444-444444444444/executions/cancel-check?execution_id=55555555-5555-5555-5555-555555555555", nil))
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("cancel-check status = %d, want 200 body=%s", cancelRec.Code, cancelRec.Body.String())
	}
	var cancelBody CancelCheckResponse
	if err := json.Unmarshal(cancelRec.Body.Bytes(), &cancelBody); err != nil {
		t.Fatalf("Unmarshal(cancel-check response) error = %v", err)
	}
	if cancelBody.CancelRequested || cancelBody.Status != storage.AnalysisRunStatusRunning {
		t.Fatalf("cancel-check response = %#v", cancelBody)
	}

	downloadRec := httptest.NewRecorder()
	mux.ServeHTTP(downloadRec, httptest.NewRequest(http.MethodGet, "/internal/v1/artifacts/77777777-7777-7777-7777-777777777777/download-access", nil))
	if downloadRec.Code != http.StatusOK {
		t.Fatalf("download-access status = %d, want 200 body=%s", downloadRec.Code, downloadRec.Body.String())
	}
	var downloadBody ArtifactDownloadAccessResponse
	if err := json.Unmarshal(downloadRec.Body.Bytes(), &downloadBody); err != nil {
		t.Fatalf("Unmarshal(download-access response) error = %v", err)
	}
	if downloadBody.AnalysisRunID == "" || downloadBody.Download.URL == "" {
		t.Fatalf("download-access response = %#v", downloadBody)
	}
}

func TestWorkerRuntimeRoutesPersistWorkerArtifactDiagnosticAndFinalizePayloads(t *testing.T) {
	t.Parallel()

	runID := "44444444-4444-4444-4444-444444444444"
	mediaID := "22222222-2222-2222-2222-222222222222"
	store := &fakePublicService{
		run: storage.AnalysisRunRecord{
			ID:          runID,
			Owner:       storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"},
			SelectionID: "33333333-3333-3333-3333-333333333333",
			RunType:     "transcription",
			Status:      storage.AnalysisRunStatusRunning,
			Version:     2,
		},
		artifact: storage.ArtifactRecord{
			ID:            "77777777-7777-7777-7777-777777777777",
			Owner:         storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"},
			AnalysisRunID: runID,
			Kind:          "transcript",
			Status:        storage.ArtifactStatusAvailable,
			ObjectKey:     "artifacts/" + runID + "/transcript/segmented/transcript.md",
			ContentType:   "text/markdown; charset=utf-8",
			SizeBytes:     55,
			Visibility:    "owner",
			PreviewJSON:   []byte(`{"filename":"transcript.md","worker_artifact_kind":"transcript_segmented_markdown"}`),
			CreatedAt:     time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC),
			Download:      &storage.DownloadDescriptor{Provider: "object_store", URL: "https://minio.local/artifacts/transcript.md", ExpiresAt: time.Date(2099, 4, 25, 12, 0, 0, 0, time.UTC)},
		},
	}
	mux := newFinalMux(Dependencies{Public: store, Worker: &workerRuntimeService{store: store}})

	cancel := httptest.NewRecorder()
	mux.ServeHTTP(cancel, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/"+runID+"/executions/cancel-check?execution_id="+runID, nil))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel-check status = %d, want 200 body=%s", cancel.Code, cancel.Body.String())
	}
	var cancelBody CancelCheckResponse
	if err := json.Unmarshal(cancel.Body.Bytes(), &cancelBody); err != nil {
		t.Fatalf("Unmarshal(cancel-check) error = %v", err)
	}
	if cancelBody.CancelRequested || cancelBody.Status != storage.AnalysisRunStatusRunning {
		t.Fatalf("cancel-check = %#v", cancelBody)
	}

	download := httptest.NewRecorder()
	mux.ServeHTTP(download, httptest.NewRequest(http.MethodGet, "/internal/v1/artifacts/77777777-7777-7777-7777-777777777777/download-access", nil))
	if download.Code != http.StatusOK {
		t.Fatalf("download-access status = %d, want 200 body=%s", download.Code, download.Body.String())
	}
	var downloadBody ArtifactDownloadAccessResponse
	if err := json.Unmarshal(download.Body.Bytes(), &downloadBody); err != nil {
		t.Fatalf("Unmarshal(download-access) error = %v", err)
	}
	if downloadBody.AnalysisRunID != runID || downloadBody.ArtifactKind != "transcript_segmented_markdown" || downloadBody.Filename != "transcript.md" || downloadBody.Download.URL == "" {
		t.Fatalf("download-access = %#v", downloadBody)
	}

	progress := httptest.NewRecorder()
	mux.ServeHTTP(progress, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/"+runID+"/executions/progress", map[string]any{
		"execution_id":     runID,
		"progress_stage":   "persisting_artifacts",
		"progress_message": "Uploading artifacts",
	}))
	if progress.Code != http.StatusAccepted {
		t.Fatalf("progress status = %d, want 202 body=%s", progress.Code, progress.Body.String())
	}
	if store.recordedProgressStage != "persisting_artifacts" || store.recordedProgressMsg != "Uploading artifacts" {
		t.Fatalf("progress = stage:%q message:%q", store.recordedProgressStage, store.recordedProgressMsg)
	}

	artifacts := httptest.NewRecorder()
	mux.ServeHTTP(artifacts, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/"+runID+"/artifacts", map[string]any{
		"execution_id": runID,
		"artifacts": []map[string]any{
			{"artifact_kind": "transcript_plain", "mime_type": "text/plain; charset=utf-8", "object_key": "artifacts/" + runID + "/transcript/plain/transcript.txt", "size_bytes": 42, "filename": "transcript.txt", "format": "plain_text"},
			{"artifact_kind": "transcript_segmented_markdown", "mime_type": "text/markdown; charset=utf-8", "object_key": "artifacts/" + runID + "/transcript/segmented/transcript.md", "size_bytes": 55, "filename": "transcript.md", "format": "markdown"},
			{"artifact_kind": "transcript_docx", "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "object_key": "artifacts/" + runID + "/transcript/docx/transcript.docx", "size_bytes": 66, "filename": "transcript.docx", "format": "docx"},
			{"artifact_kind": "summary_markdown", "mime_type": "text/markdown; charset=utf-8", "object_key": "artifacts/" + runID + "/summary/markdown/summary.md", "size_bytes": 77, "filename": "summary.md", "format": "markdown"},
			{"artifact_kind": "report_markdown", "mime_type": "text/markdown; charset=utf-8", "object_key": "artifacts/" + runID + "/report/markdown/report.md", "size_bytes": 88, "filename": "report.md", "format": "markdown"},
			{"artifact_kind": "report_docx", "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "object_key": "artifacts/" + runID + "/report/docx/report.docx", "size_bytes": 99, "filename": "report.docx", "format": "docx"},
			{"artifact_kind": "deep_research_markdown", "mime_type": "text/markdown; charset=utf-8", "object_key": "artifacts/" + runID + "/deep-research/markdown/deep-research.md", "size_bytes": 111, "filename": "deep-research.md", "format": "markdown"},
			{"artifact_kind": "agent_result_json", "mime_type": "application/json; charset=utf-8", "object_key": "artifacts/" + runID + "/agent/result/result.json", "size_bytes": 122, "filename": "result.json", "format": "json"},
			{"artifact_kind": "execution_log", "mime_type": "text/plain; charset=utf-8", "object_key": "artifacts/" + runID + "/logs/execution.log", "size_bytes": 12, "filename": "execution.log", "format": "plain_text"},
			{"artifact_kind": "run_manifest", "mime_type": "application/json; charset=utf-8", "object_key": "artifacts/" + runID + "/run/manifest/run-manifest.json", "size_bytes": 101, "filename": "run-manifest.json", "format": "json"},
			{"artifact_kind": "run_diagnostics", "mime_type": "application/json; charset=utf-8", "object_key": "artifacts/" + runID + "/run/diagnostics/run-diagnostics.json", "size_bytes": 77, "filename": "run-diagnostics.json", "format": "json"},
		},
	}))
	if artifacts.Code != http.StatusAccepted {
		t.Fatalf("artifacts status = %d, want 202 body=%s", artifacts.Code, artifacts.Body.String())
	}
	if got := len(store.recordedArtifacts); got != 11 {
		t.Fatalf("recorded artifacts = %d, want 11: %#v", got, store.recordedArtifacts)
	}
	wantKinds := []string{
		"transcript",
		"transcript",
		"transcript",
		"summary",
		"report",
		"report",
		"deep_research",
		"structured_data",
		"execution_log",
		"run_manifest",
		"run_diagnostics",
	}
	for idx, wantKind := range wantKinds {
		if store.recordedArtifacts[idx].Kind != wantKind {
			t.Fatalf("artifact %d kind = %q, want %q: %#v", idx, store.recordedArtifacts[idx].Kind, wantKind, store.recordedArtifacts[idx])
		}
	}
	if store.recordedArtifacts[0].ContentType != "text/plain; charset=utf-8" || store.recordedArtifacts[0].ObjectKey == "" {
		t.Fatalf("transcript mapping = %#v", store.recordedArtifacts[0])
	}
	if store.recordedArtifacts[10].SizeBytes != 77 {
		t.Fatalf("run_diagnostics mapping = %#v", store.recordedArtifacts[10])
	}
	var artifactPreview map[string]any
	if err := json.Unmarshal(store.recordedArtifacts[0].PreviewJSON, &artifactPreview); err != nil {
		t.Fatalf("artifact preview JSON error = %v", err)
	}
	if artifactPreview["worker_artifact_kind"] != "transcript_plain" || artifactPreview["artifact_kind"] != "transcript" {
		t.Fatalf("artifact preview = %#v, want public and worker artifact kinds", artifactPreview)
	}

	diagnostics := httptest.NewRecorder()
	mux.ServeHTTP(diagnostics, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/"+runID+"/diagnostics", map[string]any{
		"execution_id": runID,
		"diagnostics": []map[string]any{{
			"diagnostic_id": "55555555-5555-5555-5555-555555555555",
			"subject_type":  "media_item",
			"subject_id":    mediaID,
			"severity":      "warning",
			"code":          "source_unavailable",
			"message":       "URL source skipped",
			"context": map[string]any{
				"analysis_run_id":   runID,
				"selection_item_id": "66666666-6666-6666-6666-666666666666",
				"media_item_id":     mediaID,
				"media_kind":        "url",
				"role":              "primary",
				"labels":            map[string]any{"display_label": "source"},
			},
		}},
	}))
	if diagnostics.Code != http.StatusAccepted {
		t.Fatalf("diagnostics status = %d, want 202 body=%s", diagnostics.Code, diagnostics.Body.String())
	}
	if got := len(store.recordedDiagnostics); got != 1 {
		t.Fatalf("recorded diagnostics = %d, want 1: %#v", got, store.recordedDiagnostics)
	}
	diagnostic := store.recordedDiagnostics[0]
	if diagnostic.SubjectType != "media_item" || diagnostic.SubjectID != mediaID || diagnostic.Code != "source_unavailable" {
		t.Fatalf("diagnostic mapping = %#v", diagnostic)
	}
	var contextPayload map[string]any
	if err := json.Unmarshal(diagnostic.ContextJSON, &contextPayload); err != nil {
		t.Fatalf("diagnostic context JSON error = %v", err)
	}
	if contextPayload["execution_id"] != runID || contextPayload["selection_item_id"] != "66666666-6666-6666-6666-666666666666" {
		t.Fatalf("diagnostic context = %#v", contextPayload)
	}

	finalize := httptest.NewRecorder()
	mux.ServeHTTP(finalize, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/"+runID+"/executions/finalize", map[string]any{
		"execution_id": runID,
		"outcome":      "partially_succeeded",
		"message":      "Completed with skipped items",
	}))
	if finalize.Code != http.StatusOK {
		t.Fatalf("finalize status = %d, want 200 body=%s", finalize.Code, finalize.Body.String())
	}
	if store.finalizedStatus != storage.AnalysisRunStatusPartiallySucceeded {
		t.Fatalf("finalized status = %q, want partially_succeeded", store.finalizedStatus)
	}
}

func TestToSealedSelectionInputEmitsV2SelectionItemClaimFields(t *testing.T) {
	t.Parallel()

	sealed := toSealedSelectionInput(storage.SelectionRecord{
		ID: "33333333-3333-3333-3333-333333333333",
		Items: []storage.SelectionItemSnapshot{{
			ID:                "66666666-6666-6666-6666-666666666666",
			Position:          2,
			MediaItemID:       "22222222-2222-2222-2222-222222222222",
			Kind:              "audio",
			SourceSnapshot:    storage.MediaSourceMetadata{SourceID: "11111111-1111-1111-1111-111111111111", OriginType: "object", ObjectKey: "media/source.wav", MIMEType: "audio/wav"},
			DisplayName:       "source.wav",
			StatusAtSelection: storage.MediaStatusReady,
			MetadataJSON:      []byte(`{"source_label":"voice_a","original_filename":"source.wav"}`),
			RetentionSnapshot: storage.RetentionMetadata{State: storage.RetentionStateActive},
		}},
		OptionSnapshotJSON: []byte(`{"item_roles":{"22222222-2222-2222-2222-222222222222":"reference"}}`),
		SealedAt:           time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	})

	if len(sealed.Items) != 1 {
		t.Fatalf("sealed items len = %d, want 1", len(sealed.Items))
	}
	item := sealed.Items[0]
	if item.SelectionItemID != "66666666-6666-6666-6666-666666666666" {
		t.Fatalf("selection_item_id = %q, want persisted selection item id", item.SelectionItemID)
	}
	if item.MediaKind != "audio" || item.MIMEType == nil || *item.MIMEType != "audio/wav" {
		t.Fatalf("media fields mismatch: %#v", item)
	}
	if item.Role != "reference" {
		t.Fatalf("role = %q, want role from option_snapshot item_roles", item.Role)
	}
	if item.Labels.DisplayLabel != "source.wav" || item.Labels.SourceLabel == nil || *item.Labels.SourceLabel != "voice_a" || item.Labels.OriginalFilename == nil || *item.Labels.OriginalFilename != "source.wav" {
		t.Fatalf("labels mismatch: %#v", item.Labels)
	}

	payload, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("Marshal(selection item) error = %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatalf("Unmarshal(selection item) error = %v", err)
	}
	if _, ok := raw["mime_type"]; !ok {
		t.Fatalf("mime_type must be emitted even when nullable: %#v", raw)
	}
	for _, stale := range []string{"selection_item_snapshot", "selection_item"} {
		if _, ok := raw[stale]; ok {
			t.Fatalf("selection item leaked stale wrapper field %q: %#v", stale, raw)
		}
	}

	noMime := toSealedSelectionInput(storage.SelectionRecord{
		Items: []storage.SelectionItemSnapshot{{
			ID:                "77777777-7777-7777-7777-777777777777",
			Position:          0,
			MediaItemID:       "88888888-8888-8888-8888-888888888888",
			Kind:              "text",
			SourceSnapshot:    storage.MediaSourceMetadata{SourceID: "99999999-9999-9999-9999-999999999999", OriginType: "text", TextRef: "inline:99999999-9999-9999-9999-999999999999"},
			DisplayName:       "note",
			StatusAtSelection: storage.MediaStatusReady,
			RetentionSnapshot: storage.RetentionMetadata{State: storage.RetentionStateActive},
		}},
	})
	payload, err = json.Marshal(noMime.Items[0])
	if err != nil {
		t.Fatalf("Marshal(no mime selection item) error = %v", err)
	}
	raw = map[string]any{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatalf("Unmarshal(no mime selection item) error = %v", err)
	}
	if value, ok := raw["mime_type"]; !ok || value != nil {
		t.Fatalf("absent source MIME type must be emitted as null, got %#v in %#v", value, raw)
	}
}

func TestApiRuntimeReconcilesPersistedRunAfterEnqueueFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	public := &fakePublicService{
		run: storage.AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "transcription",
			Status:      storage.AnalysisRunStatusQueued,
			CreatedAt:   now,
		},
	}
	client := &flakyQueueClient{err: errors.New("redis unavailable")}
	publisher, err := queue.NewPublisher(client)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	service := &publicRuntimeService{store: public, queue: publisher}

	_, err = service.CreateAnalysisRun(context.Background(), storage.CreateAnalysisRunRequest{Owner: owner, SelectionID: "selection-1", RunType: "transcription"})
	if !errors.Is(err, queue.ErrQueueUnavailable) {
		t.Fatalf("CreateAnalysisRun() error = %v, want queue unavailable", err)
	}
	pending, err := public.ListPendingEnqueueTasks(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListPendingEnqueueTasks() error = %v", err)
	}
	if len(pending) != 1 || pending[0].AnalysisRunID != "run-1" {
		t.Fatalf("pending tasks = %#v, want persisted run task", pending)
	}

	client.err = nil
	recovered, err := service.ReconcileAnalysisRunQueue(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReconcileAnalysisRunQueue() error = %v", err)
	}
	if recovered != 1 || client.calls != 2 {
		t.Fatalf("recovered=%d calls=%d, want one recovery enqueue after one failed enqueue", recovered, client.calls)
	}
	pending, err = public.ListPendingEnqueueTasks(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListPendingEnqueueTasks(after) error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending tasks after reconcile = %#v, want none", pending)
	}
	_, claimed, err := public.ClaimAnalysisRunTask(context.Background(), "run-1", "transcription", "selection.transcription", "worker-1")
	if err != nil {
		t.Fatalf("ClaimAnalysisRunTask() error = %v", err)
	}
	if !claimed {
		t.Fatalf("reconciled task was not claimable")
	}
}

func TestApiHttpListDiagnosticsAppliesQueryFilters(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "owner-1"}
	public := &fakePublicService{
		diagnostics: []storage.DiagnosticRecord{
			{
				ID:            "diag-match",
				Owner:         owner,
				SubjectType:   "media_item",
				SubjectID:     "11111111-1111-1111-1111-111111111111",
				Severity:      "warning",
				Code:          "source_unavailable",
				CorrelationID: "corr-1",
				Message:       "match",
				CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
			},
			{
				ID:            "diag-severity-miss",
				Owner:         owner,
				SubjectType:   "media_item",
				SubjectID:     "11111111-1111-1111-1111-111111111111",
				Severity:      "error",
				Code:          "source_unavailable",
				CorrelationID: "corr-1",
				Message:       "severity miss",
				CreatedAt:     time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC),
			},
			{
				ID:            "diag-code-miss",
				Owner:         owner,
				SubjectType:   "media_item",
				SubjectID:     "11111111-1111-1111-1111-111111111111",
				Severity:      "warning",
				Code:          "retention_denied",
				CorrelationID: "corr-1",
				Message:       "code miss",
				CreatedAt:     time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC),
			},
			{
				ID:            "diag-correlation-miss",
				Owner:         owner,
				SubjectType:   "media_item",
				SubjectID:     "11111111-1111-1111-1111-111111111111",
				Severity:      "warning",
				Code:          "source_unavailable",
				CorrelationID: "corr-2",
				Message:       "correlation miss",
				CreatedAt:     time.Date(2026, 5, 10, 12, 3, 0, 0, time.UTC),
			},
		},
	}
	mux := newFinalMux(Dependencies{Public: public})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/diagnostics?owner_type=web&owner_id=owner-1&subject_type=media_item&subject_id=11111111-1111-1111-1111-111111111111&severity=warning&code=source_unavailable&correlation_id=corr-1", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if public.lastDiagnosticQuery != (storage.DiagnosticQuery{
		SubjectType:   "media_item",
		SubjectID:     "11111111-1111-1111-1111-111111111111",
		Severity:      "warning",
		Code:          "source_unavailable",
		CorrelationID: "corr-1",
	}) {
		t.Fatalf("diagnostic query = %#v", public.lastDiagnosticQuery)
	}

	var body struct {
		Items []struct {
			ID string `json:"diagnostic_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(diagnostics) error = %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].ID != "diag-match" {
		t.Fatalf("items = %#v, want only diag-match", body.Items)
	}
}

func TestApiHttpListDiagnosticsPaginatesAndMapsServiceErrors(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "owner-1"}
	mux := newFinalMux(Dependencies{Public: &fakePublicService{
		diagnostics: []storage.DiagnosticRecord{
			{ID: "diag-1", Owner: owner, Message: "first", CreatedAt: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)},
			{ID: "diag-2", Owner: owner, Message: "second", CreatedAt: time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC)},
		},
	}})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/diagnostics?owner_type=web&owner_id=owner-1&page_size=1", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("paginated diagnostics status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	var pagedBody struct {
		Items []struct {
			ID string `json:"diagnostic_id"`
		} `json:"items"`
		Page struct {
			HasMore    bool   `json:"has_more"`
			NextCursor string `json:"next_cursor"`
		} `json:"page"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &pagedBody); err != nil {
		t.Fatalf("Unmarshal(paginated diagnostics) error = %v", err)
	}
	if len(pagedBody.Items) != 1 || pagedBody.Items[0].ID != "diag-1" || !pagedBody.Page.HasMore || pagedBody.Page.NextCursor != "diag-1" {
		t.Fatalf("paged diagnostics = %#v", pagedBody)
	}

	errRec := httptest.NewRecorder()
	errMux := newFinalMux(Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}})
	errMux.ServeHTTP(errRec, httptest.NewRequest(http.MethodGet, "/v1/diagnostics?owner_type=web&owner_id=owner-1", nil))
	assertErrorCode(t, errRec, http.StatusNotFound, "not_found")
}

func TestHandleUpdateCollectionItemsMapsServiceErrors(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{
		"owner":            map[string]any{"owner_type": "web", "owner_id": "u-1"},
		"expected_version": float64(3),
		"items":            []map[string]any{{"media_item_id": "media-1", "position": float64(0)}},
	})
	req.SetPathValue("collection_id", "collection-1")

	(&Server{deps: Dependencies{Public: &fakePublicService{err: storage.ErrOwnerMismatch}}}).handleUpdateCollectionItems(rec, req)
	assertErrorCode(t, rec, http.StatusNotFound, "not_found")
}

func TestHandleRemoveCollectionItemMapsUpdateErrors(t *testing.T) {
	t.Parallel()

	store := &updateCollectionItemsErrorStore{
		fakePublicService: &fakePublicService{
			collection: storage.CollectionRecord{
				ID: "collection-1",
				Owner: storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"},
				Items: []storage.CollectionItemRecord{
					{MediaItemID: "media-1", Position: 0},
					{MediaItemID: "media-2", Position: 1},
				},
			},
		},
		updateErr: storage.ErrOwnerMismatch,
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-2?owner_type=web&owner_id=u-1&expected_version=3", nil)
	req.SetPathValue("collection_id", "collection-1")
	req.SetPathValue("media_item_id", "media-2")

	(&Server{deps: Dependencies{Public: store}}).handleRemoveCollectionItem(rec, req)
	assertErrorCode(t, rec, http.StatusNotFound, "not_found")
}

func TestApiHttpFinalRoutesCoverRemainingReadAndMutationEndpoints(t *testing.T) {
	t.Parallel()

	owner := storage.OwnerScope{OwnerType: "web", OwnerID: "u-1"}
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	public := &fakePublicService{
		mediaItem: storage.MediaItemRecord{
			ID:          "media-1",
			Owner:       owner,
			Kind:        "audio",
			Status:      storage.MediaStatusReady,
			DisplayName: "source.wav",
			Source: storage.MediaSourceMetadata{
				SourceID:   "source-1",
				OriginType: "object",
				ObjectKey:  "sources/source.wav",
				MIMEType:   "audio/wav",
			},
			Retention: storage.RetentionMetadata{State: storage.RetentionStateActive},
			CreatedAt: now,
			UpdatedAt: now,
		},
		collection: storage.CollectionRecord{
			ID:        "collection-1",
			Owner:     owner,
			Kind:      storage.CollectionKindUser,
			Name:      "Review set",
			Status:    storage.CollectionStatusActive,
			Version:   3,
			CreatedAt: now,
			UpdatedAt: now,
			Items: []storage.CollectionItemRecord{
				{MediaItemID: "media-1", Position: 0},
				{MediaItemID: "media-2", Position: 1},
			},
		},
		collections: []storage.CollectionRecord{
			{
				ID:        "inbox-1",
				Owner:     owner,
				Kind:      storage.CollectionKindInbox,
				Name:      "Inbox",
				Status:    storage.CollectionStatusActive,
				Version:   1,
				CreatedAt: now,
				UpdatedAt: now,
			},
			{
				ID:        "collection-1",
				Owner:     owner,
				Kind:      storage.CollectionKindUser,
				Name:      "Review set",
				Status:    storage.CollectionStatusActive,
				Version:   3,
				CreatedAt: now,
				UpdatedAt: now,
				Items: []storage.CollectionItemRecord{
					{MediaItemID: "media-1", Position: 0},
					{MediaItemID: "media-2", Position: 1},
				},
			},
		},
		selection: storage.SelectionRecord{
			ID:        "selection-1",
			Owner:     owner,
			Status:    storage.SelectionStatusSealed,
			CreatedBy: "u-1",
			CreatedAt: now,
			SealedAt:  now,
			Items: []storage.SelectionItemSnapshot{{
				ID:                "selection-item-1",
				Position:          0,
				MediaItemID:       "media-1",
				Kind:              "audio",
				DisplayName:       "source.wav",
				StatusAtSelection: storage.MediaStatusReady,
				SourceSnapshot:    storage.MediaSourceMetadata{SourceID: "source-1", OriginType: "object", MIMEType: "audio/wav"},
				RetentionSnapshot: storage.RetentionMetadata{State: storage.RetentionStateActive},
			}},
		},
		run: storage.AnalysisRunRecord{
			ID:                "run-1",
			Owner:             owner,
			SelectionID:       "selection-1",
			RunType:           "transcription",
			Status:            storage.AnalysisRunStatusQueued,
			Version:           2,
			EvidenceGateState: "not_required",
			CreatedAt:         now,
		},
		events: []storage.RunEventRecord{{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Status:        storage.AnalysisRunStatusQueued,
			CreatedAt:     now,
		}},
	}
	mux := newFinalMux(Dependencies{Public: public})

	getMedia := httptest.NewRecorder()
	mux.ServeHTTP(getMedia, httptest.NewRequest(http.MethodGet, "/v1/media-items/media-1?owner_type=web&owner_id=u-1", nil))
	if getMedia.Code != http.StatusOK {
		t.Fatalf("get media status = %d want 200 body=%s", getMedia.Code, getMedia.Body.String())
	}

	deleteMedia := httptest.NewRecorder()
	mux.ServeHTTP(deleteMedia, httptest.NewRequest(http.MethodDelete, "/v1/media-items/media-1?owner_type=web&owner_id=u-1", nil))
	if deleteMedia.Code != http.StatusOK {
		t.Fatalf("delete media status = %d want 200 body=%s", deleteMedia.Code, deleteMedia.Body.String())
	}

	inboxRec := httptest.NewRecorder()
	mux.ServeHTTP(inboxRec, httptest.NewRequest(http.MethodGet, "/v1/collections/inbox?owner_type=web&owner_id=u-1", nil))
	if inboxRec.Code != http.StatusOK {
		t.Fatalf("inbox status = %d want 200 body=%s", inboxRec.Code, inboxRec.Body.String())
	}

	createCollection := httptest.NewRecorder()
	mux.ServeHTTP(createCollection, jsonRequest(http.MethodPost, "/v1/collections", map[string]any{
		"owner": map[string]any{"owner_type": "web", "owner_id": "u-1"},
		"name":  "Review set",
		"items": []string{"media-1"},
	}))
	if createCollection.Code != http.StatusCreated {
		t.Fatalf("create collection status = %d want 201 body=%s", createCollection.Code, createCollection.Body.String())
	}

	listCollections := httptest.NewRecorder()
	mux.ServeHTTP(listCollections, httptest.NewRequest(http.MethodGet, "/v1/collections?owner_type=web&owner_id=u-1&page_size=1", nil))
	if listCollections.Code != http.StatusOK {
		t.Fatalf("list collections status = %d want 200 body=%s", listCollections.Code, listCollections.Body.String())
	}

	getCollection := httptest.NewRecorder()
	mux.ServeHTTP(getCollection, httptest.NewRequest(http.MethodGet, "/v1/collections/collection-1?owner_type=web&owner_id=u-1", nil))
	if getCollection.Code != http.StatusOK {
		t.Fatalf("get collection status = %d want 200 body=%s", getCollection.Code, getCollection.Body.String())
	}

	updateCollection := httptest.NewRecorder()
	mux.ServeHTTP(updateCollection, jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{
		"owner":            map[string]any{"owner_type": "web", "owner_id": "u-1"},
		"expected_version": float64(3),
		"name":             "Review set v2",
		"status":           storage.CollectionStatusArchived,
	}))
	if updateCollection.Code != http.StatusOK {
		t.Fatalf("update collection status = %d want 200 body=%s", updateCollection.Code, updateCollection.Body.String())
	}

	removeCollectionItem := httptest.NewRecorder()
	mux.ServeHTTP(removeCollectionItem, httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-2?owner_type=web&owner_id=u-1&expected_version=3", nil))
	if removeCollectionItem.Code != http.StatusOK {
		t.Fatalf("remove collection item status = %d want 200 body=%s", removeCollectionItem.Code, removeCollectionItem.Body.String())
	}
	if public.lastUpdateCollectionItems.CollectionID != "collection-1" || len(public.lastUpdateCollectionItems.Items) != 1 || public.lastUpdateCollectionItems.Items[0].MediaItemID != "media-1" || public.lastUpdateCollectionItems.Items[0].Position != 0 {
		t.Fatalf("remove collection item request = %#v", public.lastUpdateCollectionItems)
	}

	getSelection := httptest.NewRecorder()
	mux.ServeHTTP(getSelection, httptest.NewRequest(http.MethodGet, "/v1/selections/selection-1?owner_type=web&owner_id=u-1", nil))
	if getSelection.Code != http.StatusOK {
		t.Fatalf("get selection status = %d want 200 body=%s", getSelection.Code, getSelection.Body.String())
	}

	getRun := httptest.NewRecorder()
	mux.ServeHTTP(getRun, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1?owner_type=web&owner_id=u-1", nil))
	if getRun.Code != http.StatusOK {
		t.Fatalf("get run status = %d want 200 body=%s", getRun.Code, getRun.Body.String())
	}

	listEvents := httptest.NewRecorder()
	mux.ServeHTTP(listEvents, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/events?owner_type=web&owner_id=u-1&page_size=1", nil))
	if listEvents.Code != http.StatusOK {
		t.Fatalf("list events status = %d want 200 body=%s", listEvents.Code, listEvents.Body.String())
	}
	var eventsBody struct {
		Items []struct {
			ID string `json:"event_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listEvents.Body.Bytes(), &eventsBody); err != nil {
		t.Fatalf("Unmarshal(events) error = %v", err)
	}
	if len(eventsBody.Items) != 1 || eventsBody.Items[0].ID != "event-1" {
		t.Fatalf("events = %#v, want event-1", eventsBody.Items)
	}

	listArtifacts := httptest.NewRecorder()
	mux.ServeHTTP(listArtifacts, httptest.NewRequest(http.MethodGet, "/v1/artifacts?owner_type=web&owner_id=u-1&analysis_run_id=run-1", nil))
	if listArtifacts.Code != http.StatusOK {
		t.Fatalf("list artifacts status = %d want 200 body=%s", listArtifacts.Code, listArtifacts.Body.String())
	}
	if public.listArtifactsAnalysisRunID != "run-1" {
		t.Fatalf("list artifacts analysis_run_id = %q, want run-1", public.listArtifactsAnalysisRunID)
	}
}

func TestApiServerCORSAndWebsocketBranches(t *testing.T) {
	t.Parallel()

	blockedServer := NewServer(Dependencies{})
	blockedMux := http.NewServeMux()
	blockedServer.RegisterRoutes(blockedMux)

	preflight := httptest.NewRequest(http.MethodOptions, "/v1/media-items", nil)
	preflight.Header.Set("Origin", "http://localhost:3000")
	preflightRec := httptest.NewRecorder()
	blockedMux.ServeHTTP(preflightRec, preflight)
	if preflightRec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d want 204 body=%s", preflightRec.Code, preflightRec.Body.String())
	}
	if got := preflightRec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Fatalf("allow origin = %q, want localhost origin", got)
	}

	forbidden := httptest.NewRequest(http.MethodOptions, "/v1/media-items", nil)
	forbidden.Header.Set("Origin", "http://example.com")
	forbiddenRec := httptest.NewRecorder()
	blockedMux.ServeHTTP(forbiddenRec, forbidden)
	if forbiddenRec.Code != http.StatusForbidden {
		t.Fatalf("forbidden preflight status = %d want 403 body=%s", forbiddenRec.Code, forbiddenRec.Body.String())
	}

	noOrigin := httptest.NewRequest(http.MethodOptions, "/v1/media-items", nil)
	noOriginRec := httptest.NewRecorder()
	blockedMux.ServeHTTP(noOriginRec, noOrigin)
	if noOriginRec.Code != http.StatusNoContent {
		t.Fatalf("no origin preflight status = %d want 204 body=%s", noOriginRec.Code, noOriginRec.Body.String())
	}

	wsUnavailable := httptest.NewRecorder()
	blockedMux.ServeHTTP(wsUnavailable, httptest.NewRequest(http.MethodGet, "/v1/ws", nil))
	assertErrorCode(t, wsUnavailable, http.StatusServiceUnavailable, "dependency_unavailable")

	ws := &fakeWebsocketAcceptor{}
	allowedServer := NewServer(Dependencies{Websocket: ws})
	allowedMux := http.NewServeMux()
	allowedServer.RegisterRoutes(allowedMux)
	wsRec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
	req.Header.Set("Origin", "https://127.0.0.1:8080")
	allowedMux.ServeHTTP(wsRec, req)
	if wsRec.Code != http.StatusCreated {
		t.Fatalf("websocket status = %d want 201 body=%s", wsRec.Code, wsRec.Body.String())
	}
	if !ws.called {
		t.Fatalf("websocket acceptor was not called")
	}
	if got := wsRec.Header().Get("Access-Control-Allow-Origin"); got != "https://127.0.0.1:8080" {
		t.Fatalf("websocket allow origin = %q", got)
	}
}

func TestApiUtilityBranches(t *testing.T) {
	t.Parallel()

	if got := parsePositiveQueryInt("", 7); got != 7 {
		t.Fatalf("empty query int = %d, want fallback 7", got)
	}
	if got := parsePositiveQueryInt("bad", 7); got != 7 {
		t.Fatalf("invalid query int = %d, want fallback 7", got)
	}
	if got := parsePositiveQueryInt("101", 7); got != 100 {
		t.Fatalf("capped query int = %d, want 100", got)
	}

	if _, pageSize := parsePageRequest(httptest.NewRequest(http.MethodGet, "/v1/media-items?page_size=999", nil)); pageSize != 100 {
		t.Fatalf("parsePageRequest capped page size = %d, want 100", pageSize)
	}

	if got := mapFinalStorageError(storage.ErrContractViolation); got.code != "invalid_request" || got.status != http.StatusBadRequest {
		t.Fatalf("contract violation mapping = %#v", got)
	}
	if got := mapFinalStorageError(errors.New("boom")); got.code != "internal_error" || got.status != http.StatusInternalServerError {
		t.Fatalf("default error mapping = %#v", got)
	}

	errWithDetails := apiError{status: http.StatusBadRequest, code: "invalid_request", message: "bad request", details: map[string]any{"field": "owner"}}
	rec := httptest.NewRecorder()
	NewServer(Dependencies{}).writeAPIError(rec, errWithDetails)
	var body map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(writeAPIError) error = %v", err)
	}
	if body["error"]["code"] != "invalid_request" || body["error"]["details"] == nil {
		t.Fatalf("writeAPIError body = %#v", body)
	}

	if got := dependencyUnavailableError("missing queue"); got.status != http.StatusServiceUnavailable || got.code != "dependency_unavailable" {
		t.Fatalf("dependencyUnavailableError = %#v", got)
	}
	if (apiError{message: "boom"}).Error() != "boom" {
		t.Fatalf("apiError message branch not used")
	}
	if (apiError{code: "fallback_code"}).Error() != "fallback_code" {
		t.Fatalf("apiError code fallback branch not used")
	}
}

func newFinalMux(deps Dependencies) *http.ServeMux {
	mux := http.NewServeMux()
	NewServer(deps).RegisterRoutes(mux)
	return mux
}

func jsonRequest(method, path string, body any) *http.Request {
	data, _ := json.Marshal(body)
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, status, rec.Body.String())
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal(error) error = %v", err)
	}
	if body.Error.Code != code {
		t.Fatalf("error code = %q, want %q", body.Error.Code, code)
	}
}

type fakePublicService struct {
	mediaItem                  storage.MediaItemRecord
	mediaItems                 []storage.MediaItemRecord
	collection                 storage.CollectionRecord
	collections                []storage.CollectionRecord
	selection                  storage.SelectionRecord
	run                        storage.AnalysisRunRecord
	runs                       []storage.AnalysisRunRecord
	events                     []storage.RunEventRecord
	diagnostics                []storage.DiagnosticRecord
	artifact                   storage.ArtifactRecord
	artifacts                  []storage.ArtifactRecord
	err                        error
	lastAddMedia               storage.AddMediaItemRequest
	lastRun                    storage.CreateAnalysisRunRequest
	lastDiagnosticQuery        storage.DiagnosticQuery
	lastUpdateCollectionItems  storage.UpdateCollectionItemsRequest
	listArtifactsAnalysisRunID string
	createAnalysisRunCalls     int
	pendingTasks               []storage.AnalysisRunTaskRecord
	recordedArtifacts          []storage.ArtifactRecord
	recordedDiagnostics        []storage.DiagnosticRecord
	recordedProgressStage      string
	recordedProgressMsg        string
	finalizedStatus            string
	canceledRunID              string
	canceledMessage            string
	retriedRunID               string
	refreshedArtifactID        string
	reconciled                 int
	observability              storage.ObservabilitySnapshot
}

func (f *fakePublicService) AddMediaItem(_ context.Context, req storage.AddMediaItemRequest) (storage.MediaItemRecord, error) {
	f.lastAddMedia = req
	return f.mediaItem, f.err
}
func (f *fakePublicService) ListMediaItems(context.Context, storage.OwnerScope) ([]storage.MediaItemRecord, error) {
	if f.mediaItems != nil {
		return f.mediaItems, f.err
	}
	return []storage.MediaItemRecord{f.mediaItem}, f.err
}
func (f *fakePublicService) GetMediaItem(context.Context, storage.OwnerScope, string) (storage.MediaItemRecord, error) {
	return f.mediaItem, f.err
}
func (f *fakePublicService) RemoveMediaItem(context.Context, storage.OwnerScope, string) (storage.MediaItemRecord, error) {
	return f.mediaItem, f.err
}
func (f *fakePublicService) GetInboxCollection(context.Context, storage.OwnerScope) (storage.CollectionRecord, error) {
	return f.collection, f.err
}
func (f *fakePublicService) CreateCollection(context.Context, storage.CreateCollectionRequest) (storage.CollectionRecord, error) {
	return f.collection, f.err
}
func (f *fakePublicService) ListCollections(context.Context, storage.OwnerScope) ([]storage.CollectionRecord, error) {
	if f.collections != nil {
		return f.collections, f.err
	}
	return []storage.CollectionRecord{f.collection}, f.err
}
func (f *fakePublicService) GetCollection(context.Context, storage.OwnerScope, string) (storage.CollectionRecord, error) {
	return f.collection, f.err
}
func (f *fakePublicService) UpdateCollection(context.Context, storage.UpdateCollectionRequest) (storage.CollectionRecord, error) {
	return f.collection, f.err
}
func (f *fakePublicService) UpdateCollectionItems(_ context.Context, req storage.UpdateCollectionItemsRequest) (storage.CollectionRecord, error) {
	f.lastUpdateCollectionItems = req
	return f.collection, f.err
}
func (f *fakePublicService) CreateSelection(context.Context, storage.CreateSelectionRequest) (storage.SelectionRecord, error) {
	return f.selection, f.err
}
func (f *fakePublicService) GetSelection(context.Context, storage.OwnerScope, string) (storage.SelectionRecord, error) {
	return f.selection, f.err
}
func (f *fakePublicService) CreateAnalysisRun(_ context.Context, req storage.CreateAnalysisRunRequest) (storage.AnalysisRunRecord, error) {
	f.createAnalysisRunCalls++
	f.lastRun = req
	if f.err == nil && f.run.ID != "" {
		taskType := queue.TaskTypeSelectionAnalysis
		workerKind := "analysis_runner"
		if f.run.RunType == "transcription" {
			taskType = queue.TaskTypeSelectionTranscription
			workerKind = "transcription"
		}
		f.pendingTasks = append(f.pendingTasks, storage.AnalysisRunTaskRecord{
			ID:            "task-" + f.run.ID,
			AnalysisRunID: f.run.ID,
			WorkerKind:    workerKind,
			TaskType:      taskType,
			Status:        storage.AnalysisRunTaskStatusPendingEnqueue,
			AttemptNo:     1,
			CreatedAt:     f.run.CreatedAt,
		})
	}
	return f.run, f.err
}
func (f *fakePublicService) CancelAnalysisRun(_ context.Context, _ storage.OwnerScope, analysisRunID, message string) (storage.AnalysisRunRecord, error) {
	f.canceledRunID = analysisRunID
	f.canceledMessage = message
	f.run.Status = storage.AnalysisRunStatusCanceled
	return f.run, f.err
}
func (f *fakePublicService) RetryAnalysisRun(_ context.Context, _ storage.OwnerScope, analysisRunID, _ string) (storage.AnalysisRunRecord, error) {
	f.retriedRunID = analysisRunID
	return f.run, f.err
}
func (f *fakePublicService) GetAnalysisRunByID(context.Context, string) (storage.AnalysisRunRecord, error) {
	return f.run, f.err
}
func (f *fakePublicService) ListAnalysisRuns(context.Context, storage.OwnerScope) ([]storage.AnalysisRunRecord, error) {
	if f.runs != nil {
		return f.runs, f.err
	}
	return []storage.AnalysisRunRecord{f.run}, f.err
}
func (f *fakePublicService) GetAnalysisRun(context.Context, storage.OwnerScope, string) (storage.AnalysisRunRecord, error) {
	return f.run, f.err
}
func (f *fakePublicService) ListAnalysisRunEvents(context.Context, storage.OwnerScope, string) ([]storage.RunEventRecord, error) {
	return append([]storage.RunEventRecord(nil), f.events...), f.err
}
func (f *fakePublicService) ListArtifacts(_ context.Context, _ storage.OwnerScope, analysisRunID string) ([]storage.ArtifactRecord, error) {
	f.listArtifactsAnalysisRunID = analysisRunID
	if f.artifacts != nil {
		return f.artifacts, f.err
	}
	return nil, f.err
}
func (f *fakePublicService) GetArtifact(context.Context, storage.OwnerScope, string) (storage.ArtifactRecord, error) {
	return f.artifact, f.err
}
func (f *fakePublicService) GetInternalArtifactDownloadAccess(context.Context, string) (storage.ArtifactRecord, error) {
	return f.artifact, f.err
}
func (f *fakePublicService) RefreshArtifactLink(_ context.Context, _ storage.OwnerScope, artifactID string) (storage.ArtifactRecord, error) {
	f.refreshedArtifactID = artifactID
	return f.artifact, f.err
}
func (f *fakePublicService) ListDiagnostics(_ context.Context, owner storage.OwnerScope, query storage.DiagnosticQuery) ([]storage.DiagnosticRecord, error) {
	f.lastDiagnosticQuery = query
	diagnostics := make([]storage.DiagnosticRecord, 0, len(f.diagnostics))
	for _, diagnostic := range f.diagnostics {
		if !storage.SameOwner(diagnostic.Owner, owner) ||
			(query.SubjectType != "" && diagnostic.SubjectType != query.SubjectType) ||
			(query.SubjectID != "" && diagnostic.SubjectID != query.SubjectID) ||
			(query.Severity != "" && diagnostic.Severity != query.Severity) ||
			(query.Code != "" && diagnostic.Code != query.Code) ||
			(query.CorrelationID != "" && diagnostic.CorrelationID != query.CorrelationID) {
			continue
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	return diagnostics, f.err
}
func (f *fakePublicService) RecordArtifacts(_ context.Context, _ storage.OwnerScope, _ string, artifacts []storage.ArtifactRecord) ([]storage.ArtifactRecord, error) {
	f.recordedArtifacts = append([]storage.ArtifactRecord(nil), artifacts...)
	return artifacts, f.err
}
func (f *fakePublicService) RecordDiagnostics(_ context.Context, _ storage.OwnerScope, _ string, diagnostics []storage.DiagnosticRecord) ([]storage.DiagnosticRecord, error) {
	f.recordedDiagnostics = append([]storage.DiagnosticRecord(nil), diagnostics...)
	return diagnostics, f.err
}
func (f *fakePublicService) RecordAnalysisRunProgress(_ context.Context, _ storage.OwnerScope, _ string, stage, message string, _ json.RawMessage) (storage.AnalysisRunRecord, error) {
	f.recordedProgressStage = stage
	f.recordedProgressMsg = message
	return f.run, f.err
}
func (f *fakePublicService) FinalizeAnalysisRunTask(_ context.Context, _ storage.OwnerScope, _ string, status, _ string) (storage.AnalysisRunRecord, error) {
	f.finalizedStatus = status
	f.run.Status = status
	return f.run, f.err
}
func (f *fakePublicService) ListPendingEnqueueTasks(context.Context, int) ([]storage.AnalysisRunTaskRecord, error) {
	return append([]storage.AnalysisRunTaskRecord(nil), f.pendingTasks...), f.err
}
func (f *fakePublicService) ListAnalysisRunQueue(_ context.Context, status, runType, taskType string, limit int) ([]storage.AnalysisRunQueueRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	records := make([]storage.AnalysisRunQueueRecord, 0, limit)
	for _, task := range f.pendingTasks {
		if status != "" && task.Status != status {
			continue
		}
		if taskType != "" && task.TaskType != taskType {
			continue
		}
		runTypeForTask := f.run.RunType
		if runType != "" && runTypeForTask != runType {
			continue
		}
		records = append(records, storage.AnalysisRunQueueRecord{
			AnalysisRunID: task.AnalysisRunID,
			RunType:       runTypeForTask,
			WorkerKind:    task.WorkerKind,
			TaskType:      task.TaskType,
			Status:        task.Status,
			Version:       f.run.Version,
			AttemptNo:     task.AttemptNo,
			CreatedAt:     task.CreatedAt,
		})
		if len(records) == limit {
			break
		}
	}
	return records, f.err
}
func (f *fakePublicService) MarkAnalysisRunTaskQueued(_ context.Context, analysisRunID, taskType string) error {
	for i, task := range f.pendingTasks {
		if task.AnalysisRunID == analysisRunID && task.TaskType == taskType {
			f.pendingTasks = append(f.pendingTasks[:i], f.pendingTasks[i+1:]...)
			return f.err
		}
	}
	return storage.ErrExecutionNotFound
}
func (f *fakePublicService) ClaimAnalysisRunTask(context.Context, string, string, string, string) (storage.AnalysisRunRecord, bool, error) {
	f.run.Status = storage.AnalysisRunStatusRunning
	return f.run, true, f.err
}
func (f *fakePublicService) ReconcileAnalysisRunQueue(context.Context, int) (int, error) {
	return f.reconciled, f.err
}
func (f *fakePublicService) GetObservabilitySnapshot(context.Context) (storage.ObservabilitySnapshot, error) {
	return f.observability, f.err
}

type fakeWorkerService struct{}

func (f *fakeWorkerService) ListAnalysisRunQueue(context.Context, AnalysisRunQueueRequest) (AnalysisRunQueueResponse, error) {
	return AnalysisRunQueueResponse{
		Items: []storage.AnalysisRunQueueRecord{{
			AnalysisRunID: "44444444-4444-4444-4444-444444444444",
			RunType:       "transcription",
			WorkerKind:    "transcription",
			TaskType:      "selection.transcription",
			Status:        storage.AnalysisRunTaskStatusQueued,
			Version:       1,
			AttemptNo:     1,
			CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		}},
		Page:     1,
		PageSize: 1,
	}, nil
}

func (f *fakeWorkerService) ClaimExecution(context.Context, string, ExecutionClaimRequest) (ExecutionClaimResponse, error) {
	return ExecutionClaimResponse{
		ExecutionID:   "55555555-5555-5555-5555-555555555555",
		AnalysisRunID: "44444444-4444-4444-4444-444444444444",
		RunType:       "transcription",
		Selection: sealedSelectionInput{
			SelectionID: "33333333-3333-3333-3333-333333333333",
			Items: []selectionItemSnapshot{
				{
					SelectionItemID:   "66666666-6666-6666-6666-666666666666",
					Position:          0,
					MediaItemID:       "22222222-2222-2222-2222-222222222222",
					Kind:              "audio",
					MediaKind:         "audio",
					MIMEType:          stringPtr("audio/wav"),
					Role:              "primary",
					Labels:            selectionItemLabels{DisplayLabel: "source.wav", SourceLabel: stringPtr("voice_a"), OriginalFilename: stringPtr("source.wav")},
					SourceSnapshot:    storage.MediaSourceMetadata{SourceID: "11111111-1111-1111-1111-111111111111", OriginType: "object", ObjectKey: "media/source.wav", MIMEType: "audio/wav"},
					DisplayName:       "source.wav",
					StatusAtSelection: "ready",
					MetadataSnapshot:  map[string]any{"source_label": "voice_a", "original_filename": "source.wav"},
					RetentionSnapshot: storage.RetentionMetadata{State: "active"},
				},
			},
			OptionSnapshot: map[string]any{},
			SealedAt:       time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		},
		Params:    map[string]any{},
		ClaimedAt: time.Date(2026, 5, 10, 12, 1, 0, 0, time.UTC),
	}, nil
}

func (f *fakeWorkerService) ResolveRequestAccess(context.Context, string, string) (RequestAccessResponse, error) {
	return RequestAccessResponse{
		Provider:            "minio_presigned_url",
		URL:                 "https://minio.local/private/request.json",
		ExpiresAt:           "2099-04-25T12:00:00Z",
		RequestRef:          "agentreq_digest",
		RequestDigestSHA256: "abc123",
		RequestBytes:        123,
	}, nil
}

func (f *fakeWorkerService) CheckCancel(context.Context, string, string) (CancelCheckResponse, error) {
	return CancelCheckResponse{CancelRequested: false, Status: storage.AnalysisRunStatusRunning}, nil
}

func (f *fakeWorkerService) ResolveArtifactDownloadAccess(context.Context, string) (ArtifactDownloadAccessResponse, error) {
	return ArtifactDownloadAccessResponse{
		ArtifactID:    "77777777-7777-7777-7777-777777777777",
		AnalysisRunID: "44444444-4444-4444-4444-444444444444",
		ArtifactKind:  "transcript_segmented_markdown",
		Filename:      "transcript.md",
		MIMEType:      "text/markdown; charset=utf-8",
		SizeBytes:     55,
		CreatedAt:     time.Date(2026, 5, 10, 12, 2, 0, 0, time.UTC),
		Download: storage.DownloadDescriptor{
			Provider:  "object_store",
			URL:       "https://minio.local/artifacts/transcript.md",
			ExpiresAt: time.Date(2099, 4, 25, 12, 0, 0, 0, time.UTC),
		},
	}, nil
}

type fakeWebsocketAcceptor struct {
	called bool
}

func (f *fakeWebsocketAcceptor) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	f.called = true
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func stringPtr(value string) *string {
	return &value
}

func (f *fakeWorkerService) RecordExecutionProgress(context.Context, string, ExecutionProgressRequest) error {
	return nil
}
func (f *fakeWorkerService) RecordExecutionArtifacts(context.Context, string, ExecutionArtifactsRequest) error {
	return nil
}
func (f *fakeWorkerService) RecordExecutionDiagnostics(context.Context, string, ExecutionDiagnosticsRequest) error {
	return nil
}
func (f *fakeWorkerService) FinalizeExecution(context.Context, string, ExecutionFinalizeRequest) (storage.AnalysisRunRecord, error) {
	return storage.AnalysisRunRecord{}, nil
}

type flakyQueueClient struct {
	err   error
	calls int
}

func (f *flakyQueueClient) Enqueue(_ context.Context, spec queue.EnqueueSpec) (queue.EnqueueReceipt, error) {
	f.calls++
	if f.err != nil {
		return queue.EnqueueReceipt{}, f.err
	}
	return queue.EnqueueReceipt{ID: "task-id", QueueName: spec.QueueName, TaskType: spec.TaskType}, nil
}
