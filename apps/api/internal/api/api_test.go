package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/jobs"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/ws"
)

func TestApiHttpUploadReturnsJobsArrayAndPreservesIdempotencyKey(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		uploadJobs: []JobSnapshot{snapshot("11111111-1111-1111-1111-111111111111", "transcription", "queued")},
	}
	logger := &bufferLogger{}
	mux := newMux(t, Dependencies{Public: public}, WithLogger(logger))

	body, contentType := buildMultipart(t, multipartInput{
		Fields: map[string]string{
			"client_ref": "cli-1",
		},
		Files: []multipartFile{
			{Name: "files", Filename: "clip.mp3", ContentType: "audio/mpeg", Body: []byte("audio-data")},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Idempotency-Key", "idem-1")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var envelope JobsAcceptedEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("Unmarshal(response) error = %v", err)
	}
	if got, want := len(envelope.Jobs), 1; got != want {
		t.Fatalf("jobs len = %d, want %d", got, want)
	}
	if public.lastUpload.IdempotencyKey != "idem-1" {
		t.Fatalf("IdempotencyKey = %q, want idem-1", public.lastUpload.IdempotencyKey)
	}
	if got := public.lastUpload.Files[0].SHA256; got == "" {
		t.Fatal("expected route layer to normalize multipart manifest with sha256")
	}
	if !strings.Contains(logger.String(), ValidateRequestMarker) {
		t.Fatalf("logger output missing marker %q", ValidateRequestMarker)
	}
}

func TestApiHttpCombinedUploadReturnsSingleJobEnvelope(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		combinedJob: snapshot("22222222-2222-2222-2222-222222222222", "transcription", "queued"),
	}
	mux := newMux(t, Dependencies{Public: public})

	body, contentType := buildMultipart(t, multipartInput{
		Fields: map[string]string{
			"display_name":         "combined-batch",
			"delivery_strategy":    "webhook",
			"delivery_webhook_url": "https://example.com/hook",
		},
		Files: []multipartFile{
			{Name: "files", Filename: "clip-a.mp3", ContentType: "audio/mpeg", Body: []byte("a")},
			{Name: "files", Filename: "clip-b.mp3", ContentType: "audio/mpeg", Body: []byte("b")},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs/combined", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var envelope JobAcceptedEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("Unmarshal(response) error = %v", err)
	}
	if envelope.Job.JobID != public.combinedJob.JobID {
		t.Fatalf("job_id = %q, want %q", envelope.Job.JobID, public.combinedJob.JobID)
	}
	if public.lastCombined.Delivery.Strategy != storage.DeliveryStrategyWebhook {
		t.Fatalf("delivery strategy = %q, want webhook", public.lastCombined.Delivery.Strategy)
	}
}

func TestApiHttpRejectsInvalidInputsWithStableErrorEnvelopes(t *testing.T) {
	t.Parallel()

	fixture := loadHTTPFixture(t)
	mux := newMux(t, Dependencies{Public: &fakePublicService{}})

	t.Run("invalid uuid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/not-a-uuid", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_uuid")
	})

	t.Run("missing webhook url", func(t *testing.T) {
		body, contentType := buildMultipart(t, multipartInput{
			Fields: map[string]string{"delivery_strategy": "webhook"},
			Files: []multipartFile{
				{Name: "files", Filename: "clip.mp3", ContentType: "audio/mpeg", Body: []byte("audio")},
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs", body)
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "webhook_url_required")
	})

	t.Run("oversized upload", func(t *testing.T) {
		smallMux := newMux(t, Dependencies{Public: &fakePublicService{}}, WithMaxRequestBytes(8))
		body, contentType := buildMultipart(t, multipartInput{
			Files: []multipartFile{
				{Name: "files", Filename: "clip.mp3", ContentType: "audio/mpeg", Body: []byte("0123456789")},
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs", body)
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		smallMux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusRequestEntityTooLarge, "request_too_large")
	})

	t.Run("unsupported source url", func(t *testing.T) {
		payload := map[string]any{
			"source_kind": "youtube_url",
			"url":         fixture.UnsupportedURLs[0],
		}
		req := jsonRequest(t, http.MethodPost, "/v1/transcription-jobs/from-url", payload)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "unsupported_source_url")
	})
}

func TestApiHttpRoutesEventsArtifactAndWebsocketShapes(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		artifactResolution: storage.ArtifactResolution{
			ArtifactID:   "33333333-3333-3333-3333-333333333333",
			JobID:        "11111111-1111-1111-1111-111111111111",
			ArtifactKind: "report_markdown",
			Filename:     "report.md",
			MIMEType:     "text/markdown",
			SizeBytes:    64,
			CreatedAt:    time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC),
			Download: storage.DownloadDescriptor{
				Provider:  storage.DownloadProviderMinIO,
				URL:       "https://minio.local/report.md",
				ExpiresAt: time.Date(2026, 4, 22, 12, 15, 0, 0, time.UTC),
			},
		},
		events: []ws.JobEventEnvelope{
			{
				EventID:   "44444444-4444-4444-4444-444444444444",
				EventType: "job.updated",
				JobID:     "11111111-1111-1111-1111-111111111111",
				RootJobID: "11111111-1111-1111-1111-111111111111",
				Version:   3,
				EmittedAt: time.Date(2026, 4, 22, 12, 5, 0, 0, time.UTC),
				JobType:   "transcription",
				JobURL:    "/v1/jobs/11111111-1111-1111-1111-111111111111",
				Payload: ws.EventPayload{
					Status:          "running",
					ProgressStage:   "transcribing",
					ProgressMessage: "50%",
				},
			},
		},
	}
	websocket := &fakeWebsocket{}
	mux := newMux(t, Dependencies{Public: public, Websocket: websocket})

	t.Run("artifact resolve", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/artifacts/33333333-3333-3333-3333-333333333333", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var response ArtifactResolutionView
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("Unmarshal(response) error = %v", err)
		}
		if response.Download.Provider != storage.DownloadProviderMinIO {
			t.Fatalf("download provider = %q, want %q", response.Download.Provider, storage.DownloadProviderMinIO)
		}
	})

	t.Run("events list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/11111111-1111-1111-1111-111111111111/events", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var response JobEventListResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("Unmarshal(response) error = %v", err)
		}
		if got := len(response.Items); got != 1 {
			t.Fatalf("events len = %d, want 1", got)
		}
		if response.Items[0].Payload.ProgressStage == nil || *response.Items[0].Payload.ProgressStage != "transcribing" {
			t.Fatalf("progress stage = %#v", response.Items[0].Payload.ProgressStage)
		}
	})

	t.Run("websocket entry", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusSwitchingProtocols)
		}
		if websocket.calls != 1 {
			t.Fatalf("websocket calls = %d, want 1", websocket.calls)
		}
	})
}

func TestApiHttpInternalWorkerRoutesRemainExplicit(t *testing.T) {
	t.Parallel()

	worker := &fakeWorkerService{
		claimResponse: ClaimResponse{
			ExecutionID: "55555555-5555-5555-5555-555555555555",
			JobID:       "11111111-1111-1111-1111-111111111111",
			RootJobID:   "11111111-1111-1111-1111-111111111111",
			JobType:     "transcription",
			Version:     2,
			OrderedInputs: []OrderedInput{
				{Position: 0, SourceID: "66666666-6666-6666-6666-666666666666", SourceKind: "uploaded_file"},
			},
			Params: map[string]any{"lang": "ru"},
		},
	}
	mux := newMux(t, Dependencies{Public: &fakePublicService{}, Worker: worker})

	req := jsonRequest(t, http.MethodPost, "/internal/v1/jobs/11111111-1111-1111-1111-111111111111/claim", map[string]any{
		"worker_kind": "transcription",
		"task_type":   "job:transcription.run",
	})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if worker.claimCalls != 1 {
		t.Fatalf("claim calls = %d, want 1", worker.claimCalls)
	}

	publicReq := jsonRequest(t, http.MethodPost, "/v1/jobs/11111111-1111-1111-1111-111111111111/claim", map[string]any{
		"worker_kind": "transcription",
		"task_type":   "job:transcription.run",
	})
	publicRec := httptest.NewRecorder()
	mux.ServeHTTP(publicRec, publicReq)
	if publicRec.Code != http.StatusNotFound {
		t.Fatalf("public claim route should be absent, status=%d", publicRec.Code)
	}
}

func TestApiHttpAllowsLocalWebUiCors(t *testing.T) {
	t.Parallel()

	mux := newMux(t, Dependencies{Public: &fakePublicService{}})

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs?page=1&page_size=20", nil)
	req.Header.Set("Origin", "http://localhost:3300")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3300" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want local origin", got)
	}

	preflight := httptest.NewRequest(http.MethodOptions, "/v1/jobs?page=1&page_size=20", nil)
	preflight.Header.Set("Origin", "http://localhost:3300")
	preflight.Header.Set("Access-Control-Request-Method", http.MethodGet)
	preflight.Header.Set("Access-Control-Request-Headers", "Content-Type")
	preflightRec := httptest.NewRecorder()
	mux.ServeHTTP(preflightRec, preflight)

	if preflightRec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want %d body=%s", preflightRec.Code, http.StatusNoContent, preflightRec.Body.String())
	}
	if got := preflightRec.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, "Idempotency-Key") {
		t.Fatalf("Access-Control-Allow-Headers = %q, want Idempotency-Key", got)
	}
}

func TestApiHttpRejectsNonLocalCorsPreflight(t *testing.T) {
	t.Parallel()

	mux := newMux(t, Dependencies{Public: &fakePublicService{}})

	req := httptest.NewRequest(http.MethodOptions, "/v1/jobs", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
	}
}

func loadHTTPFixture(t *testing.T) httpFixture {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "validation_cases.json"))
	if err != nil {
		t.Fatalf("ReadFile(validation_cases.json) error = %v", err)
	}
	var fixture httpFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("Unmarshal(validation_cases.json) error = %v", err)
	}
	return fixture
}

type httpFixture struct {
	UnsupportedURLs []string `json:"unsupported_urls"`
}

type fakePublicService struct {
	uploadJobs         []JobSnapshot
	combinedJob        JobSnapshot
	lastUpload         UploadCommand
	lastCombined       UploadCommand
	artifactResolution storage.ArtifactResolution
	events             []ws.JobEventEnvelope
}

func (f *fakePublicService) CreateUpload(_ context.Context, req UploadCommand) ([]JobSnapshot, error) {
	f.lastUpload = req
	if f.uploadJobs == nil {
		return nil, errors.New("missing upload result")
	}
	return f.uploadJobs, nil
}

func (f *fakePublicService) CreateCombined(_ context.Context, req UploadCommand) (JobSnapshot, error) {
	f.lastCombined = req
	return f.combinedJob, nil
}

func (f *fakePublicService) CreateFromURL(_ context.Context, _ URLCommand) (JobSnapshot, error) {
	return JobSnapshot{}, nil
}

func (f *fakePublicService) GetJob(_ context.Context, _ string) (JobSnapshot, error) {
	return JobSnapshot{}, jobs.ErrJobNotFound
}

func (f *fakePublicService) ListJobs(_ context.Context, filter ListJobsFilter) (JobListResponse, error) {
	return JobListResponse{Items: nil, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func (f *fakePublicService) CreateReport(_ context.Context, _ string, _ ChildCreateRequest) (JobSnapshot, error) {
	return JobSnapshot{}, nil
}

func (f *fakePublicService) CreateDeepResearch(_ context.Context, _ string, _ ChildCreateRequest) (JobSnapshot, error) {
	return JobSnapshot{}, nil
}

func (f *fakePublicService) CancelJob(_ context.Context, _ string) (JobSnapshot, error) {
	return JobSnapshot{}, nil
}

func (f *fakePublicService) RetryJob(_ context.Context, _ string) (JobSnapshot, error) {
	return JobSnapshot{}, nil
}

func (f *fakePublicService) ResolveArtifact(_ context.Context, _ string) (storage.ArtifactResolution, error) {
	return f.artifactResolution, nil
}

func (f *fakePublicService) ListJobEvents(_ context.Context, _ string) ([]ws.JobEventEnvelope, error) {
	return f.events, nil
}

type fakeWorkerService struct {
	claimResponse ClaimResponse
	claimCalls    int
}

func (f *fakeWorkerService) Claim(_ context.Context, _ string, _ ClaimRequest) (ClaimResponse, error) {
	f.claimCalls++
	return f.claimResponse, nil
}

func (f *fakeWorkerService) RecordProgress(_ context.Context, _ string, _ ProgressRequest) error {
	return nil
}

func (f *fakeWorkerService) RecordArtifacts(_ context.Context, _ string, _ ArtifactUpsertRequest) error {
	return nil
}

func (f *fakeWorkerService) Finalize(_ context.Context, _ string, _ FinalizeRequest) (JobSnapshot, error) {
	return snapshot("11111111-1111-1111-1111-111111111111", "transcription", "succeeded"), nil
}

func (f *fakeWorkerService) CancelCheck(_ context.Context, _ string, _ string) (CancelCheckResponse, error) {
	return CancelCheckResponse{CancelRequested: false, Status: "running"}, nil
}

type fakeWebsocket struct {
	calls int
}

func (f *fakeWebsocket) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	f.calls++
	w.WriteHeader(http.StatusSwitchingProtocols)
}

type bufferLogger struct {
	lines []string
}

func (l *bufferLogger) Printf(format string, args ...any) {
	l.lines = append(l.lines, strings.TrimSpace(fmt.Sprintf(format, args...)))
}

func (l *bufferLogger) String() string {
	return strings.Join(l.lines, "\n")
}

func newMux(t *testing.T, deps Dependencies, opts ...Option) *http.ServeMux {
	t.Helper()
	server := NewServer(deps, opts...)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	return mux
}

type multipartInput struct {
	Fields map[string]string
	Files  []multipartFile
}

type multipartFile struct {
	Name        string
	Filename    string
	ContentType string
	Body        []byte
}

func buildMultipart(t *testing.T, input multipartInput) (io.Reader, string) {
	t.Helper()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	for key, value := range input.Fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatalf("WriteField(%s) error = %v", key, err)
		}
	}
	for _, file := range input.Files {
		part, err := writer.CreateFormFile(file.Name, file.Filename)
		if err != nil {
			t.Fatalf("CreateFormFile(%s) error = %v", file.Filename, err)
		}
		if _, err := part.Write(file.Body); err != nil {
			t.Fatalf("part.Write(%s) error = %v", file.Filename, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	return bytes.NewReader(buf.Bytes()), writer.FormDataContentType()
}

func jsonRequest(t *testing.T, method, path string, payload any) *http.Request {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal(payload) error = %v", err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func assertErrorEnvelope(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()

	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, wantStatus, rec.Body.String())
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("Unmarshal(error envelope) error = %v", err)
	}
	if envelope.Error.Code != wantCode {
		t.Fatalf("error.code = %q, want %q body=%s", envelope.Error.Code, wantCode, rec.Body.String())
	}
}

func snapshot(jobID, jobType, status string) JobSnapshot {
	displayName := "job"
	return JobSnapshot{
		JobID:     jobID,
		RootJobID: jobID,
		JobType:   jobType,
		Status:    status,
		Version:   1,
		Delivery:  DeliveryConfig{Strategy: storage.DeliveryStrategyPolling},
		SourceSet: SourceSetView{
			SourceSetID: "77777777-7777-7777-7777-777777777777",
			InputKind:   storage.SourceSetInputSingleSource,
			Items: []SourceSetItem{
				{
					Position: 0,
					Source: SourceReference{
						SourceID:    "66666666-6666-6666-6666-666666666666",
						SourceKind:  storage.SourceKindUploadedFile,
						DisplayName: &displayName,
					},
				},
			},
		},
		Artifacts: []ArtifactSummary{},
		Children:  []ChildJobReference{},
		CreatedAt: time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC),
	}
}
