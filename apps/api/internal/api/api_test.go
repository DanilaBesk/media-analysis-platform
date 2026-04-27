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

	"github.com/danila/media-analysis-platform/apps/api/internal/jobs"
	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	"github.com/danila/media-analysis-platform/apps/api/internal/ws"
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

func TestApiHttpBatchTranscriptionParsesManifestFilesURLsAndIdempotency(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		batchJob: snapshot("77777777-7777-7777-7777-777777777777", queue.JobTypeTranscription, "queued"),
	}
	mux := newMux(t, Dependencies{Public: public})

	manifest := `{
		"manifest_version":"batch-transcription.v1",
		"ordered_source_labels":["voice_a","video_b"],
		"sources":{
			"voice_a":{"source_kind":"uploaded_file","file_part":"voice_a","display_name":"Voice A"},
			"video_b":{"source_kind":"youtube_url","url":"https://youtu.be/example","display_name":"Video B"}
		},
		"completion_policy":"succeed_when_all_sources_succeed"
	}`
	body, contentType := buildMultipart(t, multipartInput{
		Fields: map[string]string{
			"source_manifest": manifest,
			"display_name":    "Mixed batch",
			"client_ref":      "client-1",
		},
		Files: []multipartFile{
			{Name: "voice_a", Filename: "voice.ogg", ContentType: "audio/ogg", Body: []byte("voice")},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs/batch", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Idempotency-Key", "batch-idem")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var envelope JobAcceptedEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("Unmarshal(response) error = %v", err)
	}
	if envelope.Job.JobID != public.batchJob.JobID {
		t.Fatalf("job_id = %q, want %q", envelope.Job.JobID, public.batchJob.JobID)
	}
	if public.lastBatch.IdempotencyKey != "batch-idem" || public.lastBatch.DisplayName != "Mixed batch" {
		t.Fatalf("batch command metadata = %#v", public.lastBatch)
	}
	if got, want := len(public.lastBatch.Manifest.OrderedSourceLabels), 2; got != want {
		t.Fatalf("manifest labels = %d, want %d", got, want)
	}
	if got, want := len(public.lastBatch.Files), 1; got != want {
		t.Fatalf("batch files = %d, want %d", got, want)
	}
	if public.lastBatch.Files[0].PartName != "voice_a" || public.lastBatch.Files[0].SHA256 == "" {
		t.Fatalf("batch file normalization = %#v", public.lastBatch.Files[0])
	}
}

func TestApiHttpBatchTranscriptionRejectsInvalidManifest(t *testing.T) {
	t.Parallel()

	mux := newMux(t, Dependencies{Public: &fakePublicService{}})
	body, contentType := buildMultipart(t, multipartInput{
		Fields: map[string]string{
			"source_manifest": `{"manifest_version":"batch-transcription.v1","ordered_source_labels":["missing"],"sources":{},"completion_policy":"succeed_when_all_sources_succeed"}`,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs/batch", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_source_manifest")
}

func TestApiHttpBatchDraftRoutesParseOwnerVersionAndReturnStaleErrors(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		batchDraft: BatchDraft{
			DraftID: "11111111-1111-1111-1111-111111111111",
			Version: 1,
			Status:  BatchDraftStatusOpen,
			Owner:   BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"},
			Items:   []BatchDraftItem{},
		},
		batchDraftErr: apiError{status: http.StatusConflict, code: "version_conflict", message: "batch draft version conflict"},
	}
	mux := newMux(t, Dependencies{Public: public})

	createReq := jsonRequest(t, http.MethodPost, "/v1/batch-drafts", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"display_name": "Telegram batch",
		"client_ref":   "client-1",
	})
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d body=%s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	if public.lastCreateBatchDraft.Owner.TelegramChatID != "chat-1" || public.lastCreateBatchDraft.DisplayName != "Telegram batch" {
		t.Fatalf("create draft request = %#v", public.lastCreateBatchDraft)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111?owner_type=telegram&telegram_chat_id=chat-1&telegram_user_id=user-1", nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if public.lastGetBatchDraft.DraftID != "11111111-1111-1111-1111-111111111111" || public.lastGetBatchDraft.Owner.TelegramUserID != "user-1" {
		t.Fatalf("get draft request = %#v", public.lastGetBatchDraft)
	}

	addReq := jsonRequest(t, http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"expected_version": 1,
		"item": map[string]any{
			"source_kind":  "youtube_url",
			"url":          "https://youtu.be/video",
			"display_name": "Video",
		},
	})
	addRec := httptest.NewRecorder()
	mux.ServeHTTP(addRec, addReq)
	assertErrorEnvelope(t, addRec, http.StatusConflict, "version_conflict")
	if public.lastAddBatchDraftItem.DraftID != "11111111-1111-1111-1111-111111111111" || public.lastAddBatchDraftItem.ExpectedVersion != 1 {
		t.Fatalf("add draft item request = %#v", public.lastAddBatchDraftItem)
	}

	public.batchDraftErr = nil
	removeReq := jsonRequest(t, http.MethodDelete, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items/22222222-2222-2222-2222-222222222222", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"expected_version": 2,
	})
	removeRec := httptest.NewRecorder()
	mux.ServeHTTP(removeRec, removeReq)
	if removeRec.Code != http.StatusOK {
		t.Fatalf("remove status = %d, want %d body=%s", removeRec.Code, http.StatusOK, removeRec.Body.String())
	}
	if public.lastRemoveBatchDraft.ItemID != "22222222-2222-2222-2222-222222222222" || public.lastRemoveBatchDraft.ExpectedVersion != 2 {
		t.Fatalf("remove draft request = %#v", public.lastRemoveBatchDraft)
	}

	clearReq := jsonRequest(t, http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/clear", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"expected_version": 3,
	})
	clearRec := httptest.NewRecorder()
	mux.ServeHTTP(clearRec, clearReq)
	if clearRec.Code != http.StatusOK {
		t.Fatalf("clear status = %d, want %d body=%s", clearRec.Code, http.StatusOK, clearRec.Body.String())
	}
	if public.lastClearBatchDraft.DraftID != "11111111-1111-1111-1111-111111111111" || public.lastClearBatchDraft.ExpectedVersion != 3 {
		t.Fatalf("clear draft request = %#v", public.lastClearBatchDraft)
	}

	submitReq := jsonRequest(t, http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/submit", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"expected_version": 4,
	})
	submitRec := httptest.NewRecorder()
	mux.ServeHTTP(submitRec, submitReq)
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("submit status = %d, want %d body=%s", submitRec.Code, http.StatusAccepted, submitRec.Body.String())
	}
	if public.lastSubmitBatchDraft.DraftID != "11111111-1111-1111-1111-111111111111" || public.lastSubmitBatchDraft.ExpectedVersion != 4 {
		t.Fatalf("submit draft request = %#v", public.lastSubmitBatchDraft)
	}
}

func TestApiHttpBatchDraftMultipartAddParsesOwnerVersionMetadataAndFile(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		batchDraft: BatchDraft{
			DraftID: "11111111-1111-1111-1111-111111111111",
			Version: 2,
			Status:  BatchDraftStatusOpen,
			Owner:   BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"},
			Items:   []BatchDraftItem{},
		},
	}
	mux := newMux(t, Dependencies{Public: public})

	body, contentType := buildMultipart(t, multipartInput{
		Fields: map[string]string{
			"owner":            `{"owner_type":"telegram","telegram_chat_id":"chat-1","telegram_user_id":"user-1"}`,
			"expected_version": "1",
			"item":             `{"source_kind":"telegram_upload","display_name":"Voice note"}`,
		},
		Files: []multipartFile{{
			Name:     "file",
			Filename: "Voice Note.ogg",
			Body:     []byte("voice-bytes"),
		}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	got := public.lastAddBatchDraftItem
	if got.DraftID != "11111111-1111-1111-1111-111111111111" || got.ExpectedVersion != 1 {
		t.Fatalf("draft mutation = %#v, want draft/version parsed", got)
	}
	if got.Owner.TelegramChatID != "chat-1" || got.Owner.TelegramUserID != "user-1" {
		t.Fatalf("owner = %#v, want telegram scope", got.Owner)
	}
	if got.Item.SourceKind != storage.SourceKindTelegramUpload || got.Item.UploadedSourceRef == "" {
		t.Fatalf("item source = %#v, want generated uploaded source ref", got.Item)
	}
	if !strings.Contains(got.Item.UploadedSourceRef, "11111111-1111-1111-1111-111111111111") || !strings.Contains(got.Item.UploadedSourceRef, "Voice_Note.ogg") {
		t.Fatalf("uploaded_source_ref = %q, want draft-scoped sanitized object key", got.Item.UploadedSourceRef)
	}
	if got.Item.DisplayName != "Voice note" || got.Item.OriginalFilename != "Voice Note.ogg" {
		t.Fatalf("metadata = %#v, want display/original filename propagated", got.Item)
	}
	if got.Item.SizeBytes != int64(len("voice-bytes")) || string(got.Item.ObjectBody) != "voice-bytes" {
		t.Fatalf("file metadata/body = size %d body %q", got.Item.SizeBytes, string(got.Item.ObjectBody))
	}
}

func TestApiHttpBatchDraftJSONURLAddStillWorks(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		batchDraft: BatchDraft{
			DraftID: "11111111-1111-1111-1111-111111111111",
			Version: 2,
			Status:  BatchDraftStatusOpen,
			Owner:   BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"},
			Items:   []BatchDraftItem{},
		},
	}
	mux := newMux(t, Dependencies{Public: public})
	req := jsonRequest(t, http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"expected_version": 1,
		"item": map[string]any{
			"source_kind":  "youtube_url",
			"url":          "https://youtu.be/video",
			"display_name": "Video",
		},
	})
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if public.lastAddBatchDraftItem.Item.SourceKind != storage.SourceKindYouTubeURL || public.lastAddBatchDraftItem.Item.URL != "https://youtu.be/video" {
		t.Fatalf("json url item = %#v, want URL item preserved", public.lastAddBatchDraftItem.Item)
	}
}

func TestApiHttpBatchDraftJSONUploadedAddRequiresMultipart(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{}
	mux := newMux(t, Dependencies{Public: public})
	req := jsonRequest(t, http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items", map[string]any{
		"owner": map[string]any{
			"owner_type":       "telegram",
			"telegram_chat_id": "chat-1",
			"telegram_user_id": "user-1",
		},
		"expected_version": 1,
		"item": map[string]any{
			"source_kind":         "telegram_upload",
			"uploaded_source_ref": "attacker-controlled-ref",
			"display_name":        "Voice",
		},
	})
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_batch_draft_item")
	if !strings.Contains(rec.Body.String(), "uploaded draft items require multipart upload") {
		t.Fatalf("error body = %s, want multipart upload guidance", rec.Body.String())
	}
	if public.lastAddBatchDraftItem.DraftID != "" {
		t.Fatalf("JSON uploaded draft item reached service: %#v", public.lastAddBatchDraftItem)
	}
}

func TestApiHttpBatchDraftMultipartAddRequiresUploadedSourceKind(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{}
	mux := newMux(t, Dependencies{Public: public})
	body, contentType := buildMultipart(t, multipartInput{
		Fields: map[string]string{
			"owner":            `{"owner_type":"telegram","telegram_chat_id":"chat-1","telegram_user_id":"user-1"}`,
			"expected_version": "1",
			"item":             `{"source_kind":"youtube_url","url":"https://youtu.be/video","display_name":"Video"}`,
		},
		Files: []multipartFile{{
			Name:     "file",
			Filename: "voice.ogg",
			Body:     []byte("voice-bytes"),
		}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/batch-drafts/11111111-1111-1111-1111-111111111111/items", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	assertErrorEnvelope(t, rec, http.StatusBadRequest, "invalid_batch_draft_item")
	if !strings.Contains(rec.Body.String(), "multipart draft uploads require uploaded_file or telegram_upload source_kind") {
		t.Fatalf("error body = %s, want uploaded source kind guidance", rec.Body.String())
	}
	if public.lastAddBatchDraftItem.DraftID != "" {
		t.Fatalf("multipart URL draft item reached service: %#v", public.lastAddBatchDraftItem)
	}
}

func TestApiHttpCreateAgentRunParsesIdempotencyHarnessAndRequest(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		agentRunJob: snapshot("33333333-3333-3333-3333-333333333333", queue.JobTypeAgentRun, "queued"),
	}
	mux := newMux(t, Dependencies{Public: public})

	req := jsonRequest(t, http.MethodPost, "/v1/agent-runs", map[string]any{
		"harness_name": "generic",
		"client_ref":   "client-1",
		"request": map[string]any{
			"prompt": "summarize",
			"payload": map[string]any{
				"topic": "calls",
			},
			"input_artifacts": []map[string]any{
				{"artifact_id": "44444444-4444-4444-4444-444444444444", "artifact_kind": "transcript_plain"},
			},
		},
	})
	req.Header.Set("Idempotency-Key", "agent-idem")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var envelope JobAcceptedEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("Unmarshal(response) error = %v", err)
	}
	if envelope.Job.JobID != public.agentRunJob.JobID {
		t.Fatalf("job_id = %q, want %q", envelope.Job.JobID, public.agentRunJob.JobID)
	}
	if public.lastAgentRun.IdempotencyKey != "agent-idem" || public.lastAgentRun.HarnessName != "generic" {
		t.Fatalf("agent run request = %#v, want idempotency+harness", public.lastAgentRun)
	}
	if string(public.lastAgentRun.Request.Payload) == "" {
		t.Fatalf("agent run payload was not parsed: %#v", public.lastAgentRun.Request)
	}
}

func TestApiHttpCreateReportAndDeepResearchRoutesReturnAgentRunJobs(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{
		reportJob:       snapshot("55555555-5555-5555-5555-555555555555", queue.JobTypeAgentRun, "queued"),
		deepResearchJob: snapshot("66666666-6666-6666-6666-666666666666", queue.JobTypeAgentRun, "queued"),
	}
	mux := newMux(t, Dependencies{Public: public})

	reportReq := jsonRequest(t, http.MethodPost, "/v1/transcription-jobs/11111111-1111-1111-1111-111111111111/report-jobs", map[string]any{
		"client_ref": "report-client",
	})
	reportReq.Header.Set("Idempotency-Key", "report-idem")
	reportRec := httptest.NewRecorder()
	mux.ServeHTTP(reportRec, reportReq)
	if reportRec.Code != http.StatusAccepted {
		t.Fatalf("report status = %d, want %d body=%s", reportRec.Code, http.StatusAccepted, reportRec.Body.String())
	}
	var reportEnvelope JobAcceptedEnvelope
	if err := json.Unmarshal(reportRec.Body.Bytes(), &reportEnvelope); err != nil {
		t.Fatalf("Unmarshal(report response) error = %v", err)
	}
	if reportEnvelope.Job.JobType != queue.JobTypeAgentRun {
		t.Fatalf("report route job_type = %q, want agent_run", reportEnvelope.Job.JobType)
	}
	if public.lastReportJobID != "11111111-1111-1111-1111-111111111111" || public.lastReportReq.IdempotencyKey != "report-idem" {
		t.Fatalf("report request = job_id %q req %#v", public.lastReportJobID, public.lastReportReq)
	}

	deepReq := jsonRequest(t, http.MethodPost, "/v1/report-jobs/22222222-2222-2222-2222-222222222222/deep-research-jobs", map[string]any{
		"client_ref": "deep-client",
	})
	deepReq.Header.Set("Idempotency-Key", "deep-idem")
	deepRec := httptest.NewRecorder()
	mux.ServeHTTP(deepRec, deepReq)
	if deepRec.Code != http.StatusAccepted {
		t.Fatalf("deep status = %d, want %d body=%s", deepRec.Code, http.StatusAccepted, deepRec.Body.String())
	}
	var deepEnvelope JobAcceptedEnvelope
	if err := json.Unmarshal(deepRec.Body.Bytes(), &deepEnvelope); err != nil {
		t.Fatalf("Unmarshal(deep response) error = %v", err)
	}
	if deepEnvelope.Job.JobType != queue.JobTypeAgentRun {
		t.Fatalf("deep route job_type = %q, want agent_run", deepEnvelope.Job.JobType)
	}
	if public.lastDeepResearchJobID != "22222222-2222-2222-2222-222222222222" || public.lastDeepResearchReq.IdempotencyKey != "deep-idem" {
		t.Fatalf("deep request = job_id %q req %#v", public.lastDeepResearchJobID, public.lastDeepResearchReq)
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

	t.Run("missing agent harness", func(t *testing.T) {
		req := jsonRequest(t, http.MethodPost, "/v1/agent-runs", map[string]any{
			"request": map[string]any{"prompt": "hello"},
		})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "validation_failed")
	})

	t.Run("missing agent request content", func(t *testing.T) {
		req := jsonRequest(t, http.MethodPost, "/v1/agent-runs", map[string]any{
			"harness_name": "generic",
			"request":      map[string]any{},
		})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "validation_failed")
	})

	t.Run("unsupported agent artifact kind", func(t *testing.T) {
		req := jsonRequest(t, http.MethodPost, "/v1/agent-runs", map[string]any{
			"harness_name": "generic",
			"request": map[string]any{
				"input_artifacts": []map[string]any{
					{"artifact_id": "44444444-4444-4444-4444-444444444444", "artifact_kind": "not_a_kind"},
				},
			},
		})
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertErrorEnvelope(t, rec, http.StatusBadRequest, "validation_failed")
	})
}

func TestApiHttpMapsQueueUnavailableToStableErrorEnvelope(t *testing.T) {
	t.Parallel()

	public := &fakePublicService{uploadErr: queue.ErrQueueUnavailable}
	mux := newMux(t, Dependencies{Public: public})

	body, contentType := buildMultipart(t, multipartInput{
		Files: []multipartFile{
			{Name: "files", Filename: "clip.mp3", ContentType: "audio/mpeg", Body: []byte("audio")},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/transcription-jobs", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	assertErrorEnvelope(t, rec, http.StatusServiceUnavailable, "queue_unavailable")
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

func TestApiHttpInternalAgentRunRequestAccessRouteReturnsOpaqueAccess(t *testing.T) {
	t.Parallel()

	worker := &fakeWorkerService{
		requestAccess: storage.AgentRunRequestAccess{
			Provider:            storage.AgentRunRequestProviderMinIO,
			URL:                 "https://minio.local/private/request.json",
			ExpiresAt:           time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC),
			RequestRef:          "agentreq_digest",
			RequestDigestSHA256: "digest",
			RequestBytes:        123,
		},
	}
	mux := newMux(t, Dependencies{Public: &fakePublicService{}, Worker: worker})

	req := httptest.NewRequest(
		http.MethodGet,
		"/internal/v1/jobs/11111111-1111-1111-1111-111111111111/request-access?execution_id=55555555-5555-5555-5555-555555555555",
		nil,
	)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response AgentRunRequestAccessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Unmarshal(response) error = %v", err)
	}
	if response.RequestRef != "agentreq_digest" || response.RequestBytes != 123 {
		t.Fatalf("response = %#v, want request-access payload", response)
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
	uploadJobs            []JobSnapshot
	uploadErr             error
	combinedJob           JobSnapshot
	batchJob              JobSnapshot
	batchDraft            BatchDraft
	batchDraftErr         error
	agentRunJob           JobSnapshot
	reportJob             JobSnapshot
	deepResearchJob       JobSnapshot
	lastUpload            UploadCommand
	lastCombined          UploadCommand
	lastBatch             BatchCommand
	lastCreateBatchDraft  BatchDraftCreateCommand
	lastGetBatchDraft     BatchDraftGetCommand
	lastAddBatchDraftItem BatchDraftItemCommand
	lastRemoveBatchDraft  BatchDraftRemoveItemCommand
	lastClearBatchDraft   BatchDraftMutateCommand
	lastSubmitBatchDraft  BatchDraftSubmitCommand
	lastAgentRun          AgentRunCommand
	lastReportJobID       string
	lastReportReq         ChildCreateRequest
	lastDeepResearchJobID string
	lastDeepResearchReq   ChildCreateRequest
	artifactResolution    storage.ArtifactResolution
	events                []ws.JobEventEnvelope
}

func (f *fakePublicService) CreateUpload(_ context.Context, req UploadCommand) ([]JobSnapshot, error) {
	f.lastUpload = req
	if f.uploadErr != nil {
		return nil, f.uploadErr
	}
	if f.uploadJobs == nil {
		return nil, errors.New("missing upload result")
	}
	return f.uploadJobs, nil
}

func (f *fakePublicService) CreateCombined(_ context.Context, req UploadCommand) (JobSnapshot, error) {
	f.lastCombined = req
	return f.combinedJob, nil
}

func (f *fakePublicService) CreateBatch(_ context.Context, req BatchCommand) (JobSnapshot, error) {
	f.lastBatch = req
	return f.batchJob, nil
}

func (f *fakePublicService) CreateBatchDraft(_ context.Context, req BatchDraftCreateCommand) (BatchDraftResponse, error) {
	f.lastCreateBatchDraft = req
	return BatchDraftResponse{Draft: f.batchDraft}, nil
}

func (f *fakePublicService) GetBatchDraft(_ context.Context, req BatchDraftGetCommand) (BatchDraftResponse, error) {
	f.lastGetBatchDraft = req
	return BatchDraftResponse{Draft: f.batchDraft}, nil
}

func (f *fakePublicService) AddBatchDraftItem(_ context.Context, req BatchDraftItemCommand) (BatchDraftResponse, error) {
	f.lastAddBatchDraftItem = req
	if f.batchDraftErr != nil {
		return BatchDraftResponse{}, f.batchDraftErr
	}
	return BatchDraftResponse{Draft: f.batchDraft}, nil
}

func (f *fakePublicService) RemoveBatchDraftItem(_ context.Context, req BatchDraftRemoveItemCommand) (BatchDraftResponse, error) {
	f.lastRemoveBatchDraft = req
	return BatchDraftResponse{Draft: f.batchDraft}, nil
}

func (f *fakePublicService) ClearBatchDraft(_ context.Context, req BatchDraftMutateCommand) (BatchDraftResponse, error) {
	f.lastClearBatchDraft = req
	return BatchDraftResponse{Draft: f.batchDraft}, nil
}

func (f *fakePublicService) SubmitBatchDraft(_ context.Context, req BatchDraftSubmitCommand) (BatchDraftSubmitResponse, error) {
	f.lastSubmitBatchDraft = req
	return BatchDraftSubmitResponse{Draft: f.batchDraft, Job: &f.batchJob}, nil
}

func (f *fakePublicService) CreateFromURL(_ context.Context, _ URLCommand) (JobSnapshot, error) {
	return JobSnapshot{}, nil
}

func (f *fakePublicService) CreateAgentRun(_ context.Context, req AgentRunCommand) (JobSnapshot, error) {
	f.lastAgentRun = req
	return f.agentRunJob, nil
}

func (f *fakePublicService) GetJob(_ context.Context, _ string) (JobSnapshot, error) {
	return JobSnapshot{}, jobs.ErrJobNotFound
}

func (f *fakePublicService) ListJobs(_ context.Context, filter ListJobsFilter) (JobListResponse, error) {
	return JobListResponse{Items: nil, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func (f *fakePublicService) CreateReport(_ context.Context, jobID string, req ChildCreateRequest) (JobSnapshot, error) {
	f.lastReportJobID = jobID
	f.lastReportReq = req
	return f.reportJob, nil
}

func (f *fakePublicService) CreateDeepResearch(_ context.Context, jobID string, req ChildCreateRequest) (JobSnapshot, error) {
	f.lastDeepResearchJobID = jobID
	f.lastDeepResearchReq = req
	return f.deepResearchJob, nil
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
	requestAccess storage.AgentRunRequestAccess
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

func (f *fakeWorkerService) ResolveAgentRunRequestAccess(_ context.Context, _ string, _ string) (storage.AgentRunRequestAccess, error) {
	return f.requestAccess, nil
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
