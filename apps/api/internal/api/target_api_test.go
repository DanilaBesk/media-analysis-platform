package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

func TestTargetApiCanonicalRoutesUseTargetVocabulary(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	target := &fakeTargetService{now: now}
	mux := newFinalMux(Dependencies{Target: target})

	channelAccount := httptest.NewRecorder()
	mux.ServeHTTP(channelAccount, jsonRequest(http.MethodPut, "/internal/v1/channel-accounts", map[string]any{
		"channel":              "telegram",
		"external_account_ref": "chat-1",
		"display_name":         "Danila",
	}))
	assertTargetStatus(t, channelAccount, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, channelAccount.Body.String())
	if target.channelAccountReq.Channel != "telegram" || target.channelAccountReq.ExternalAccountRef != "chat-1" {
		t.Fatalf("channel account request = %#v", target.channelAccountReq)
	}

	listChannelAccounts := httptest.NewRecorder()
	mux.ServeHTTP(listChannelAccounts, httptest.NewRequest(http.MethodGet, "/internal/v1/channel-accounts?page_size=10", nil))
	assertTargetStatus(t, listChannelAccounts, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listChannelAccounts.Body.String())
	if target.listChannelAccountsReq.PageSize != 10 {
		t.Fatalf("list channel accounts request = %#v", target.listChannelAccountsReq)
	}

	updateChannelAccount := httptest.NewRecorder()
	mux.ServeHTTP(updateChannelAccount, jsonRequest(http.MethodPatch, "/internal/v1/channel-accounts/channel-account-1", map[string]any{
		"display_name": "Danila B",
		"status":       "active",
	}))
	assertTargetStatus(t, updateChannelAccount, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, updateChannelAccount.Body.String())
	if target.updateChannelAccountReq.ChannelAccountID != "channel-account-1" || target.updateChannelAccountReq.DisplayName != "Danila B" {
		t.Fatalf("update channel account request = %#v", target.updateChannelAccountReq)
	}

	createMedia := httptest.NewRecorder()
	req := jsonRequest(http.MethodPost, "/v1/media-assets", map[string]any{
		"channel_account_id": "channel-account-1",
		"origin": map[string]any{
			"origin_type": "telegram_file",
			"origin_ref":  "voice-file-id",
		},
		"kind":         "voice",
		"display_name": "voice.ogg",
		"metadata":     map[string]any{"duration_seconds": 5},
	})
	req.Header.Set("Idempotency-Key", "telegram:update:1")
	mux.ServeHTTP(createMedia, req)
	assertTargetStatus(t, createMedia, http.StatusCreated)
	assertNoLegacyTargetVocabulary(t, createMedia.Body.String())
	if target.mediaAssetReq.ChannelAccountID != "channel-account-1" || target.mediaAssetReq.IdempotencyKey != "telegram:update:1" {
		t.Fatalf("media asset request = %#v", target.mediaAssetReq)
	}
	assertTargetEnvelopeID(t, createMedia.Body.Bytes(), "media_asset", "media_asset_id", "media-asset-1")

	uploadMedia := httptest.NewRecorder()
	uploadReq := multipartTargetUploadRequest(t, "/v1/media-assets/upload", map[string]any{
		"channel_account_id": "channel-account-1",
		"kind":               "document",
		"display_name":       "notes.txt",
	}, "notes.txt", "hello")
	uploadReq.Header.Set("Idempotency-Key", "upload:key")
	mux.ServeHTTP(uploadMedia, uploadReq)
	assertTargetStatus(t, uploadMedia, http.StatusCreated)
	assertNoLegacyTargetVocabulary(t, uploadMedia.Body.String())
	if target.mediaAssetReq.Origin.OriginType != "upload" ||
		target.mediaAssetReq.Origin.ObjectRef == "" ||
		target.mediaAssetReq.Origin.StoredObjectID == "" ||
		target.mediaAssetReq.IdempotencyKey != "upload:key" {
		t.Fatalf("upload media asset request = %#v", target.mediaAssetReq)
	}
	if !bytes.Equal(target.mediaAssetReq.Origin.UploadBody, []byte("hello")) {
		t.Fatalf("upload media asset body = %q, want hello", string(target.mediaAssetReq.Origin.UploadBody))
	}
	preTargetUploadID := preTargetUploadStoredObjectID("channel-account-1", "notes.txt", []byte("hello"))
	if target.mediaAssetReq.Origin.StoredObjectID == preTargetUploadID {
		t.Fatalf("upload reused pre-target stored_object_id %q; old rows with uploads/ keys can conflict with sources/uploads/ keys", preTargetUploadID)
	}
	if !strings.HasPrefix(target.mediaAssetReq.Origin.ObjectRef, "sources/uploads/"+target.mediaAssetReq.Origin.StoredObjectID+"/") {
		t.Fatalf("upload object_ref %q must be keyed by its new stored_object_id %q", target.mediaAssetReq.Origin.ObjectRef, target.mediaAssetReq.Origin.StoredObjectID)
	}

	listMedia := httptest.NewRecorder()
	mux.ServeHTTP(listMedia, httptest.NewRequest(http.MethodGet, "/v1/media-assets?channel_account_id=channel-account-1&page_size=10", nil))
	assertTargetStatus(t, listMedia, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listMedia.Body.String())
	if !strings.Contains(listMedia.Body.String(), `"items":[]`) {
		t.Fatalf("target list media assets must encode an empty items array: %s", listMedia.Body.String())
	}
	if target.listMediaAssetsReq.ChannelAccountID != "channel-account-1" || target.listMediaAssetsReq.PageSize != 10 {
		t.Fatalf("list media assets request = %#v", target.listMediaAssetsReq)
	}

	getMedia := httptest.NewRecorder()
	mux.ServeHTTP(getMedia, httptest.NewRequest(http.MethodGet, "/v1/media-assets/media-asset-1?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, getMedia, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, getMedia.Body.String())
	assertTargetEnvelopeID(t, getMedia.Body.Bytes(), "media_asset", "media_asset_id", "media-asset-1")
	if target.getMediaAssetReq.MediaAssetID != "media-asset-1" {
		t.Fatalf("get media asset request = %#v", target.getMediaAssetReq)
	}

	deleteMedia := httptest.NewRecorder()
	mux.ServeHTTP(deleteMedia, httptest.NewRequest(http.MethodDelete, "/v1/media-assets/media-asset-1?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, deleteMedia, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, deleteMedia.Body.String())
	if !strings.Contains(deleteMedia.Body.String(), `"status":"deleted"`) {
		t.Fatalf("delete media asset response = %s", deleteMedia.Body.String())
	}
	if target.deleteMediaAssetReq.MediaAssetID != "media-asset-1" {
		t.Fatalf("delete media asset request = %#v", target.deleteMediaAssetReq)
	}

	getInbox := httptest.NewRecorder()
	mux.ServeHTTP(getInbox, httptest.NewRequest(http.MethodGet, "/v1/collections/inbox?channel_account_id=channel-account-1&page_size=10", nil))
	assertTargetStatus(t, getInbox, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, getInbox.Body.String())
	assertTargetEnvelopeID(t, getInbox.Body.Bytes(), "collection", "collection_id", "inbox-1")

	createCollection := httptest.NewRecorder()
	mux.ServeHTTP(createCollection, jsonRequest(http.MethodPost, "/v1/collections", map[string]any{
		"channel_account_id": "channel-account-1",
		"name":               "Research",
		"items":              []string{"media-asset-1"},
	}))
	assertTargetStatus(t, createCollection, http.StatusCreated)
	assertNoLegacyTargetVocabulary(t, createCollection.Body.String())
	if target.createCollectionReq.Name != "Research" || target.createCollectionReq.Items[0] != "media-asset-1" {
		t.Fatalf("create collection request = %#v", target.createCollectionReq)
	}

	listCollections := httptest.NewRecorder()
	mux.ServeHTTP(listCollections, httptest.NewRequest(http.MethodGet, "/v1/collections?channel_account_id=channel-account-1&page_size=10", nil))
	assertTargetStatus(t, listCollections, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listCollections.Body.String())
	if target.listCollectionsReq.ChannelAccountID != "channel-account-1" {
		t.Fatalf("list collections request = %#v", target.listCollectionsReq)
	}

	getCollection := httptest.NewRecorder()
	mux.ServeHTTP(getCollection, httptest.NewRequest(http.MethodGet, "/v1/collections/collection-1?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, getCollection, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, getCollection.Body.String())
	assertTargetEnvelopeID(t, getCollection.Body.Bytes(), "collection", "collection_id", "collection-1")

	updateCollection := httptest.NewRecorder()
	mux.ServeHTTP(updateCollection, jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{
		"channel_account_id": "channel-account-1",
		"expected_version":   1,
		"name":               "Research v2",
	}))
	assertTargetStatus(t, updateCollection, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, updateCollection.Body.String())
	if target.updateCollectionReq.ExpectedVersion != 1 || target.updateCollectionReq.Name != "Research v2" {
		t.Fatalf("update collection request = %#v", target.updateCollectionReq)
	}

	updateCollectionItems := httptest.NewRecorder()
	mux.ServeHTTP(updateCollectionItems, jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{
		"channel_account_id": "channel-account-1",
		"expected_version":   2,
		"items": []map[string]any{{
			"media_asset_id": "media-asset-1",
			"position":       0,
		}},
	}))
	assertTargetStatus(t, updateCollectionItems, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, updateCollectionItems.Body.String())
	if target.updateCollectionItemsReq.Items[0].MediaAssetID != "media-asset-1" {
		t.Fatalf("update collection items request = %#v", target.updateCollectionItemsReq)
	}

	removeCollectionItem := httptest.NewRecorder()
	mux.ServeHTTP(removeCollectionItem, httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-asset-1?channel_account_id=channel-account-1&expected_version=3", nil))
	assertTargetStatus(t, removeCollectionItem, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, removeCollectionItem.Body.String())
	if target.removeCollectionItemReq.MediaAssetID != "media-asset-1" {
		t.Fatalf("remove collection item request = %#v", target.removeCollectionItemReq)
	}

	processingRun := httptest.NewRecorder()
	processingRequest := jsonRequest(http.MethodPost, "/v1/collections/collection-1/processing-runs", map[string]any{
		"channel_account_id":             "channel-account-1",
		"expected_version":               4,
		"selected_item_ids":              []string{"media-asset-1"},
		"run_type":                       "transcription",
		"options":                        map[string]any{"language": "ru"},
		"created_via_channel_account_id": "channel-account-1",
	})
	processingRequest.Header.Set("Idempotency-Key", "telegram:process:1")
	mux.ServeHTTP(processingRun, processingRequest)
	assertTargetStatus(t, processingRun, http.StatusCreated)
	if target.startProcessingRunReq.CollectionID != "collection-1" || target.startProcessingRunReq.ExpectedVersion != 4 || target.startProcessingRunReq.IdempotencyKey != "telegram:process:1" ||
		len(target.startProcessingRunReq.SelectedItemIDs) != 1 || target.startProcessingRunReq.SelectedItemIDs[0] != "media-asset-1" ||
		target.startProcessingRunReq.CreatedViaChannelAccountID != "channel-account-1" {
		t.Fatalf("start processing run request = %#v", target.startProcessingRunReq)
	}
	if !strings.Contains(processingRun.Body.String(), `"detached_media_asset_ids":["media-asset-1"]`) {
		t.Fatalf("start processing run response = %s", processingRun.Body.String())
	}

	createSnapshot := httptest.NewRecorder()
	mux.ServeHTTP(createSnapshot, jsonRequest(http.MethodPost, "/v1/selection-snapshots", map[string]any{
		"channel_account_id":   "channel-account-1",
		"source_collection_id": "inbox-1",
		"items": []map[string]any{{
			"media_asset_id": "media-asset-1",
			"position":       0,
		}},
		"option_snapshot": map[string]any{"language": "ru"},
	}))
	assertTargetStatus(t, createSnapshot, http.StatusCreated)
	assertNoLegacyTargetVocabulary(t, createSnapshot.Body.String())
	if target.selectionSnapshotReq.Items[0].MediaAssetID != "media-asset-1" {
		t.Fatalf("selection snapshot request = %#v", target.selectionSnapshotReq)
	}
	assertTargetEnvelopeID(t, createSnapshot.Body.Bytes(), "selection_snapshot", "selection_snapshot_id", "snapshot-1")

	getSnapshot := httptest.NewRecorder()
	mux.ServeHTTP(getSnapshot, httptest.NewRequest(http.MethodGet, "/v1/selection-snapshots/snapshot-1?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, getSnapshot, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, getSnapshot.Body.String())
	assertTargetEnvelopeID(t, getSnapshot.Body.Bytes(), "selection_snapshot", "selection_snapshot_id", "snapshot-1")
	if target.getSelectionSnapshotReq.SelectionSnapshotID != "snapshot-1" {
		t.Fatalf("get selection snapshot request = %#v", target.getSelectionSnapshotReq)
	}

	createRun := httptest.NewRecorder()
	mux.ServeHTTP(createRun, jsonRequest(http.MethodPost, "/v1/analysis-runs", map[string]any{
		"channel_account_id":     "channel-account-1",
		"selection_snapshot_id":  "snapshot-1",
		"run_type":               "transcription",
		"idempotency_key":        "run:key",
		"params":                 map[string]any{"language": "ru"},
		"delivery":               map[string]any{"strategy": "polling"},
		"created_via_channel_id": "channel-account-1",
	}))
	assertTargetStatus(t, createRun, http.StatusCreated)
	assertNoLegacyTargetVocabulary(t, createRun.Body.String())
	if target.analysisRunReq.SelectionSnapshotID != "snapshot-1" {
		t.Fatalf("analysis run request = %#v", target.analysisRunReq)
	}
	assertTargetEnvelopeID(t, createRun.Body.Bytes(), "analysis_run", "analysis_run_id", "run-1")

	listRuns := httptest.NewRecorder()
	mux.ServeHTTP(listRuns, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs?channel_account_id=channel-account-1&page_size=10", nil))
	assertTargetStatus(t, listRuns, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listRuns.Body.String())
	if target.listAnalysisRunsReq.ChannelAccountID != "channel-account-1" {
		t.Fatalf("list analysis runs request = %#v", target.listAnalysisRunsReq)
	}

	getRun := httptest.NewRecorder()
	mux.ServeHTTP(getRun, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, getRun, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, getRun.Body.String())
	assertTargetEnvelopeID(t, getRun.Body.Bytes(), "analysis_run", "analysis_run_id", "run-1")
	if target.getAnalysisRunReq.AnalysisRunID != "run-1" {
		t.Fatalf("get analysis run request = %#v", target.getAnalysisRunReq)
	}

	cancelRun := httptest.NewRecorder()
	mux.ServeHTTP(cancelRun, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/cancel", map[string]any{
		"channel_account_id": "channel-account-1",
		"message":            "stop",
	}))
	assertTargetStatus(t, cancelRun, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, cancelRun.Body.String())
	if target.cancelRunReq.Message != "stop" {
		t.Fatalf("cancel run request = %#v", target.cancelRunReq)
	}

	retryRun := httptest.NewRecorder()
	mux.ServeHTTP(retryRun, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry", map[string]any{
		"channel_account_id": "channel-account-1",
		"idempotency_key":    "retry:key",
	}))
	assertTargetStatus(t, retryRun, http.StatusAccepted)
	assertNoLegacyTargetVocabulary(t, retryRun.Body.String())
	if target.retryRunReq.IdempotencyKey != "retry:key" {
		t.Fatalf("retry run request = %#v", target.retryRunReq)
	}

	listRunEvents := httptest.NewRecorder()
	mux.ServeHTTP(listRunEvents, httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/events?channel_account_id=channel-account-1&page_size=10", nil))
	assertTargetStatus(t, listRunEvents, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listRunEvents.Body.String())
	if target.listRunEventsReq.AnalysisRunID != "run-1" {
		t.Fatalf("list run events request = %#v", target.listRunEventsReq)
	}

	listArtifacts := httptest.NewRecorder()
	mux.ServeHTTP(listArtifacts, httptest.NewRequest(http.MethodGet, "/v1/artifacts?channel_account_id=channel-account-1&analysis_run_id=run-1&page_size=10", nil))
	assertTargetStatus(t, listArtifacts, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listArtifacts.Body.String())
	if target.listArtifactsReq.AnalysisRunID != "run-1" {
		t.Fatalf("list artifacts request = %#v", target.listArtifactsReq)
	}

	getArtifact := httptest.NewRecorder()
	mux.ServeHTTP(getArtifact, httptest.NewRequest(http.MethodGet, "/v1/artifacts/artifact-1?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, getArtifact, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, getArtifact.Body.String())
	assertTargetEnvelopeID(t, getArtifact.Body.Bytes(), "artifact", "artifact_id", "artifact-1")
	if target.getArtifactReq.ArtifactID != "artifact-1" {
		t.Fatalf("get artifact request = %#v", target.getArtifactReq)
	}

	refreshArtifact := httptest.NewRecorder()
	mux.ServeHTTP(refreshArtifact, jsonRequest(http.MethodPost, "/v1/artifacts/artifact-1/refresh?channel_account_id=channel-account-1", nil))
	assertTargetStatus(t, refreshArtifact, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, refreshArtifact.Body.String())
	assertTargetEnvelopeID(t, refreshArtifact.Body.Bytes(), "artifact", "artifact_id", "artifact-1")
	if target.getArtifactReq.ChannelAccountID != "channel-account-1" || target.getArtifactReq.ArtifactID != "artifact-1" {
		t.Fatalf("refresh artifact request = %#v", target.getArtifactReq)
	}

	listDiagnostics := httptest.NewRecorder()
	mux.ServeHTTP(listDiagnostics, httptest.NewRequest(http.MethodGet, "/v1/diagnostics?channel_account_id=channel-account-1&subject_type=analysis_run&subject_id=run-1&page_size=10", nil))
	assertTargetStatus(t, listDiagnostics, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listDiagnostics.Body.String())
	if target.listDiagnosticsReq.SubjectID != "run-1" {
		t.Fatalf("list diagnostics request = %#v", target.listDiagnosticsReq)
	}

	observability := httptest.NewRecorder()
	mux.ServeHTTP(observability, httptest.NewRequest(http.MethodGet, "/v1/admin/observability", nil))
	assertTargetStatus(t, observability, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, observability.Body.String())
	if !strings.Contains(observability.Body.String(), `"observability":`) || !strings.Contains(observability.Body.String(), `"queue_tasks":1`) {
		t.Fatalf("observability response must be enveloped: %s", observability.Body.String())
	}

	listStepQueue := httptest.NewRecorder()
	mux.ServeHTTP(listStepQueue, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/queue?status=queued&run_type=transcription&worker_kind=transcription&step_kind=selection.transcription&page_size=10", nil))
	assertTargetStatus(t, listStepQueue, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listStepQueue.Body.String())
	if target.listStepQueueReq.StepKind != "selection.transcription" || target.listStepQueueReq.PageSize != 10 {
		t.Fatalf("list step queue request = %#v", target.listStepQueueReq)
	}

	claimStep := httptest.NewRecorder()
	mux.ServeHTTP(claimStep, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/claim", map[string]any{
		"worker_kind": "transcription",
		"step_kind":   "selection.transcription",
		"lease_owner": "worker-1",
	}))
	assertTargetStatus(t, claimStep, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, claimStep.Body.String())
	if target.claimStepReq.StepKind != "selection.transcription" || target.claimStepReq.LeaseOwner != "worker-1" {
		t.Fatalf("claim step request = %#v", target.claimStepReq)
	}
	if !strings.Contains(claimStep.Body.String(), `"analysis_run_step_id":"step-1"`) ||
		!strings.Contains(claimStep.Body.String(), `"analysis_run_step_inputs"`) {
		t.Fatalf("claim step response missing target step/input fields: %s", claimStep.Body.String())
	}

	checkStepCancel := httptest.NewRecorder()
	mux.ServeHTTP(checkStepCancel, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/run-1/steps/cancel-check?analysis_run_step_id=step-1", nil))
	assertTargetStatus(t, checkStepCancel, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, checkStepCancel.Body.String())
	if target.checkStepCancelReq.AnalysisRunStepID != "step-1" {
		t.Fatalf("check step cancel request = %#v", target.checkStepCancelReq)
	}

	recordStepProgress := httptest.NewRecorder()
	mux.ServeHTTP(recordStepProgress, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/progress", map[string]any{
		"analysis_run_step_id": "step-1",
		"progress_stage":       "transcribing",
		"progress_message":     "working",
	}))
	assertTargetStatus(t, recordStepProgress, http.StatusAccepted)
	assertNoLegacyTargetVocabulary(t, recordStepProgress.Body.String())
	if target.progressStepReq.ProgressStage != "transcribing" {
		t.Fatalf("progress step request = %#v", target.progressStepReq)
	}

	recordArtifacts := httptest.NewRecorder()
	mux.ServeHTTP(recordArtifacts, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/artifacts", map[string]any{
		"analysis_run_step_id": "step-1",
		"artifacts": []map[string]any{{
			"artifact_kind": "summary_markdown",
			"mime_type":     "text/markdown",
			"object_key":    "run-1/summary.md",
			"size_bytes":    7,
			"filename":      "summary.md",
			"format":        "markdown",
		}},
	}))
	assertTargetStatus(t, recordArtifacts, http.StatusAccepted)
	assertNoLegacyTargetVocabulary(t, recordArtifacts.Body.String())
	if target.recordArtifactsReq.AnalysisRunStepID != "step-1" ||
		len(target.recordArtifactsReq.Artifacts) != 1 ||
		target.recordArtifactsReq.Artifacts[0].ArtifactKind != "summary_markdown" {
		t.Fatalf("record artifacts request = %#v", target.recordArtifactsReq)
	}

	recordDiagnostics := httptest.NewRecorder()
	mux.ServeHTTP(recordDiagnostics, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/diagnostics", map[string]any{
		"analysis_run_step_id": "step-1",
		"diagnostics": []map[string]any{{
			"diagnostic_id": "diagnostic-1",
			"subject_type":  "analysis_run",
			"subject_id":    "run-1",
			"severity":      "warning",
			"code":          "transcript_missing",
			"message":       "Transcript is missing",
			"context":       map[string]any{"source": "worker"},
		}},
	}))
	assertTargetStatus(t, recordDiagnostics, http.StatusAccepted)
	assertNoLegacyTargetVocabulary(t, recordDiagnostics.Body.String())
	if target.recordDiagnosticsReq.AnalysisRunStepID != "step-1" ||
		len(target.recordDiagnosticsReq.Diagnostics) != 1 ||
		target.recordDiagnosticsReq.Diagnostics[0].Code != "transcript_missing" {
		t.Fatalf("record diagnostics request = %#v", target.recordDiagnosticsReq)
	}

	finalizeStep := httptest.NewRecorder()
	mux.ServeHTTP(finalizeStep, jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/finalize", map[string]any{
		"analysis_run_step_id": "step-1",
		"outcome":              "succeeded",
		"message":              "done",
	}))
	assertTargetStatus(t, finalizeStep, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, finalizeStep.Body.String())
	if target.finalizeStepReq.Outcome != "succeeded" {
		t.Fatalf("finalize step request = %#v", target.finalizeStepReq)
	}

	upsertSurface := httptest.NewRecorder()
	mux.ServeHTTP(upsertSurface, jsonRequest(http.MethodPut, "/internal/v1/channel-surfaces", map[string]any{
		"channel_account_id":  "channel-account-1",
		"channel":             "telegram",
		"surface_type":        "message",
		"surface_key":         "run:run-1",
		"address":             map[string]any{"chat_id": "chat-1", "message_id": 42},
		"address_fingerprint": "telegram:chat-1:42",
		"display_state":       map[string]any{"status": "queued"},
		"subjects": []map[string]any{{
			"subject_type": "analysis_run",
			"subject_id":   "run-1",
			"subject_role": "primary",
		}},
	}))
	assertTargetStatus(t, upsertSurface, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, upsertSurface.Body.String())
	if target.surfaceReq.SurfaceKey != "run:run-1" {
		t.Fatalf("channel surface request = %#v", target.surfaceReq)
	}

	listSurfaces := httptest.NewRecorder()
	mux.ServeHTTP(listSurfaces, httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces?channel_account_id=channel-account-1&subject_type=analysis_run&subject_id=run-1&page_size=10", nil))
	assertTargetStatus(t, listSurfaces, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listSurfaces.Body.String())
	if target.listSurfacesReq.SubjectID != "run-1" {
		t.Fatalf("list surfaces request = %#v", target.listSurfacesReq)
	}

	listActiveSurfaces := httptest.NewRecorder()
	mux.ServeHTTP(listActiveSurfaces, httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/active?channel_account_id=channel-account-1&subject_type=artifact&subject_id=artifact-1&page_size=10", nil))
	assertTargetStatus(t, listActiveSurfaces, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listActiveSurfaces.Body.String())
	if !target.listActiveSurfacesReq.ActiveOnly {
		t.Fatalf("list active surfaces request = %#v", target.listActiveSurfacesReq)
	}
	if target.listActiveSurfacesReq.SubjectType != "artifact" || target.listActiveSurfacesReq.SubjectID != "artifact-1" {
		t.Fatalf("list active surfaces subject filters were dropped: %#v", target.listActiveSurfacesReq)
	}

	reusableTranscript := httptest.NewRecorder()
	mux.ServeHTTP(
		reusableTranscript,
		httptest.NewRequest(
			http.MethodGet,
			"/internal/v1/reusable-transcripts?channel_account_id=channel-account-1&stored_object_id=stored-source-1&checksum=sha256%3Asource",
			nil,
		),
	)
	assertTargetStatus(t, reusableTranscript, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, reusableTranscript.Body.String())
	if target.reusableTranscriptReq.ChannelAccountID != "channel-account-1" ||
		target.reusableTranscriptReq.StoredObjectID != "stored-source-1" ||
		target.reusableTranscriptReq.Checksum != "sha256:source" {
		t.Fatalf("reusable transcript request = %#v", target.reusableTranscriptReq)
	}
	assertTargetEnvelopeID(t, reusableTranscript.Body.Bytes(), "reusable_transcript", "artifact_id", "artifact-1")

	replaceDisplay := httptest.NewRecorder()
	mux.ServeHTTP(replaceDisplay, jsonRequest(http.MethodPatch, "/internal/v1/channel-surfaces/surface-1/display-state", map[string]any{
		"expected_version": 1,
		"display_state":    map[string]any{"status": "running"},
		"actor_type":       "telegram_adapter",
	}))
	assertTargetStatus(t, replaceDisplay, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, replaceDisplay.Body.String())
	if target.displayStateReq.SurfaceID != "surface-1" || target.displayStateReq.ExpectedVersion != 1 {
		t.Fatalf("display state request = %#v", target.displayStateReq)
	}

	supersede := httptest.NewRecorder()
	mux.ServeHTTP(supersede, jsonRequest(http.MethodPost, "/internal/v1/channel-surfaces/surface-1/supersede", map[string]any{
		"reason":     "message_not_editable",
		"actor_type": "telegram_adapter",
		"actor_id":   "bot",
	}))
	assertTargetStatus(t, supersede, http.StatusAccepted)
	assertNoLegacyTargetVocabulary(t, supersede.Body.String())
	if target.supersedeReq.SurfaceID != "surface-1" || target.supersedeReq.Reason != "message_not_editable" {
		t.Fatalf("supersede request = %#v", target.supersedeReq)
	}

	listSurfaceEvents := httptest.NewRecorder()
	mux.ServeHTTP(listSurfaceEvents, httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/surface-1/events?page_size=10", nil))
	assertTargetStatus(t, listSurfaceEvents, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listSurfaceEvents.Body.String())
	if target.listSurfaceEventsReq.SurfaceID != "surface-1" {
		t.Fatalf("list surface events request = %#v", target.listSurfaceEventsReq)
	}
}

func TestProcessingRunHandlerResponseMatchesLaunchContractSurface(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	const (
		channelID  = "00000000-0000-4000-8000-000000000002"
		collection = "00000000-0000-4000-8000-000000000102"
		assetID    = "00000000-0000-4000-8000-000000000301"
		snapshotID = "00000000-0000-4000-8000-000000000401"
		itemID     = "00000000-0000-4000-8000-000000000402"
		runID      = "00000000-0000-4000-8000-000000000501"
		stepID     = "00000000-0000-4000-8000-000000000502"
	)
	target := &fakeTargetService{now: now, processingRunResult: &TargetProcessingRun{
		SelectionSnapshot: TargetSelectionSnapshot{
			SelectionSnapshotID: snapshotID, ChannelAccountID: channelID, SourceCollectionID: collection,
			Status: "sealed", OptionSnapshot: []byte(`{"language":"ru"}`), Diagnostics: []TargetDiagnostic{},
			Items: []TargetSelectionSnapshotItem{{
				SelectionSnapshotItemID: itemID, MediaAssetID: assetID, Position: 0, Kind: "voice", DisplayName: "voice.ogg",
				OriginSnapshot: []byte(`{"origin_type":"telegram_file","origin_ref":"file-1"}`), StorageSnapshot: []byte(`{}`),
				Metadata: []byte(`{}`), StatusAtSelection: "available",
			}},
			CreatedAt: now, SealedAt: now,
		},
		AnalysisRun: TargetAnalysisRun{
			AnalysisRunID: runID, ChannelAccountID: channelID, SelectionSnapshotID: snapshotID,
			RunType: "transcription", Status: "queued", Version: 1, Params: []byte(`{}`),
			Delivery: []byte(`{"strategy":"polling"}`), EvidenceGateState: "not_required",
			Steps: []TargetAnalysisRunStep{{
				AnalysisRunStepID: stepID, AnalysisRunID: runID, StepKind: "transcription",
				WorkerKind: "transcription", Status: "queued", AttemptNo: 0,
			}},
			CreatedAt: now,
		},
		DetachedMediaAssetIDs: []string{assetID}, CollectionVersion: 8,
	}}
	mux := newFinalMux(Dependencies{Target: target})
	response := httptest.NewRecorder()
	request := jsonRequest(http.MethodPost, "/v1/collections/"+collection+"/processing-runs", map[string]any{
		"channel_account_id": channelID, "expected_version": 7, "selected_item_ids": []string{assetID},
		"run_type": "transcription", "options": map[string]any{"language": "ru"},
		"created_via_channel_account_id": channelID,
	})
	request.Header.Set("Idempotency-Key", "processing:contract:1")
	mux.ServeHTTP(response, request)
	assertTargetStatus(t, response, http.StatusCreated)
	assertJSONSurfaceMatchesSchemaDefinition(t, response.Body.Bytes(), "processingRunResponse")

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode processing response: %v", err)
	}
	assertJSONSurfaceMatchesSchemaDefinition(t, payload["analysis_run"], "analysisRunLaunch")
}

func TestProcessingRunHandlerRequiresIdempotencyHeader(t *testing.T) {
	t.Parallel()
	target := &fakeTargetService{now: time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)}
	mux := newFinalMux(Dependencies{Target: target})
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, jsonRequest(http.MethodPost, "/v1/collections/collection-1/processing-runs", map[string]any{
		"channel_account_id": "channel-account-1", "expected_version": 1,
		"selected_item_ids": []string{"media-asset-1"}, "run_type": "transcription",
	}))

	assertTargetStatus(t, response, http.StatusBadRequest)
	if target.startProcessingRunReq.CollectionID != "" {
		t.Fatalf("missing idempotency header reached target service: %#v", target.startProcessingRunReq)
	}
}

func assertJSONSurfaceMatchesSchemaDefinition(t *testing.T, body []byte, definitionName string) {
	t.Helper()
	schemaBody, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "packages", "contracts", "schemas", "http", "collection.schema.json"))
	if err != nil {
		t.Fatalf("read collection contract: %v", err)
	}
	var schema struct {
		Definitions map[string]struct {
			Required   []string                   `json:"required"`
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(schemaBody, &schema); err != nil {
		t.Fatalf("decode collection contract: %v", err)
	}
	definition, ok := schema.Definitions[definitionName]
	if !ok {
		t.Fatalf("collection contract has no %q definition", definitionName)
	}
	var actual map[string]json.RawMessage
	if err := json.Unmarshal(body, &actual); err != nil {
		t.Fatalf("decode %s response surface: %v", definitionName, err)
	}
	for _, required := range definition.Required {
		if _, ok := actual[required]; !ok {
			t.Errorf("%s response is missing required field %q", definitionName, required)
		}
	}
	for field := range actual {
		if _, ok := definition.Properties[field]; !ok {
			t.Errorf("%s response has additional field %q", definitionName, field)
		}
	}
}

func TestTargetApiEdgeCoverageForValidationConflictAndPagination(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 12, 30, 0, 0, time.UTC)
	target := &fakeTargetService{now: now}
	mux := newFinalMux(Dependencies{Target: target})

	invalidMedia := httptest.NewRecorder()
	mux.ServeHTTP(invalidMedia, httptest.NewRequest(http.MethodPost, "/v1/media-assets", strings.NewReader("{")))
	assertErrorCode(t, invalidMedia, http.StatusBadRequest, "invalid_media_asset")
	assertNoLegacyTargetVocabulary(t, invalidMedia.Body.String())

	missingUploadMetadata := httptest.NewRecorder()
	uploadReq := httptest.NewRequest(http.MethodPost, "/v1/media-assets/upload", strings.NewReader(""))
	uploadReq.Header.Set("Content-Type", "multipart/form-data; boundary=missing")
	mux.ServeHTTP(missingUploadMetadata, uploadReq)
	assertErrorCode(t, missingUploadMetadata, http.StatusBadRequest, "invalid_media_asset")
	assertNoLegacyTargetVocabulary(t, missingUploadMetadata.Body.String())

	paginated := httptest.NewRecorder()
	mux.ServeHTTP(paginated, httptest.NewRequest(http.MethodGet, "/v1/diagnostics?channel_account_id=channel-account-1&page_size=999&cursor=diag-1", nil))
	assertTargetStatus(t, paginated, http.StatusOK)
	if target.listDiagnosticsReq.PageSize != 100 || target.listDiagnosticsReq.Cursor != "diag-1" {
		t.Fatalf("diagnostics pagination request = %#v", target.listDiagnosticsReq)
	}
	if !strings.Contains(paginated.Body.String(), `"items":[`) || !strings.Contains(paginated.Body.String(), `"page_size":100`) {
		t.Fatalf("diagnostics pagination response = %s", paginated.Body.String())
	}
	assertNoLegacyTargetVocabulary(t, paginated.Body.String())

	conflictTarget := &fakeTargetService{now: now, updateCollectionErr: storage.ErrCollectionVersionConflict}
	conflictMux := newFinalMux(Dependencies{Target: conflictTarget})
	conflict := httptest.NewRecorder()
	conflictMux.ServeHTTP(conflict, jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{
		"channel_account_id": "channel-account-1",
		"expected_version":   1,
		"name":               "stale",
	}))
	assertErrorCode(t, conflict, http.StatusConflict, "collection_version_conflict")
	assertNoLegacyTargetVocabulary(t, conflict.Body.String())
}

func TestTargetApiCoversInvalidJSONAndUploadEdges(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 12, 45, 0, 0, time.UTC)
	target := &fakeTargetService{now: now}
	mux := newFinalMux(Dependencies{Target: target})
	invalidJSON := []struct {
		name string
		req  *http.Request
		code string
	}{
		{name: "resolve channel account", req: invalidTargetJSONRequest(http.MethodPut, "/internal/v1/channel-accounts"), code: "invalid_channel_account"},
		{name: "update channel account", req: invalidTargetJSONRequest(http.MethodPatch, "/internal/v1/channel-accounts/channel-account-1"), code: "invalid_channel_account"},
		{name: "create selection snapshot", req: invalidTargetJSONRequest(http.MethodPost, "/v1/selection-snapshots"), code: "invalid_selection_snapshot"},
		{name: "claim step", req: invalidTargetJSONRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/claim"), code: "invalid_analysis_run_step_claim"},
		{name: "record progress", req: invalidTargetJSONRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/progress"), code: "invalid_analysis_run_step_progress"},
		{name: "finalize step", req: invalidTargetJSONRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/finalize"), code: "invalid_analysis_run_step_finalize"},
		{name: "upsert surface", req: invalidTargetJSONRequest(http.MethodPut, "/internal/v1/channel-surfaces"), code: "invalid_channel_surface"},
		{name: "replace display state", req: invalidTargetJSONRequest(http.MethodPatch, "/internal/v1/channel-surfaces/surface-1/display-state"), code: "invalid_channel_surface_display_state"},
		{name: "supersede surface", req: invalidTargetJSONRequest(http.MethodPost, "/internal/v1/channel-surfaces/surface-1/supersede"), code: "invalid_channel_surface_supersede"},
	}
	for _, tc := range invalidJSON {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, tc.req)
			assertErrorCode(t, rec, http.StatusBadRequest, tc.code)
			assertNoLegacyTargetVocabulary(t, rec.Body.String())
		})
	}

	invalidMetadata := httptest.NewRecorder()
	mux.ServeHTTP(invalidMetadata, rawMultipartTargetUploadRequest(t, "/v1/media-assets/upload", "{", "voice.ogg", "voice-bytes", "audio/ogg", true))
	assertErrorCode(t, invalidMetadata, http.StatusBadRequest, "invalid_media_asset")
	assertNoLegacyTargetVocabulary(t, invalidMetadata.Body.String())

	missingFile := httptest.NewRecorder()
	mux.ServeHTTP(missingFile, rawMultipartTargetUploadRequest(t, "/v1/media-assets/upload", `{"channel_account_id":"channel-account-1","kind":"voice"}`, "", "", "", false))
	assertErrorCode(t, missingFile, http.StatusBadRequest, "invalid_media_asset")
	assertNoLegacyTargetVocabulary(t, missingFile.Body.String())

	missingMetadata := httptest.NewRecorder()
	mux.ServeHTTP(missingMetadata, rawMultipartTargetUploadRequest(t, "/v1/media-assets/upload", "", "voice.ogg", "voice-bytes", "audio/ogg", true))
	assertErrorCode(t, missingMetadata, http.StatusBadRequest, "invalid_media_asset")
	assertNoLegacyTargetVocabulary(t, missingMetadata.Body.String())

	blankFilename := httptest.NewRecorder()
	mux.ServeHTTP(blankFilename, rawMultipartTargetUploadRequest(t, "/v1/media-assets/upload", `{"channel_account_id":"channel-account-1","kind":"voice"}`, " ", "voice-bytes", "", true))
	assertTargetStatus(t, blankFilename, http.StatusCreated)
	if target.mediaAssetReq.DisplayName != "upload.bin" ||
		target.mediaAssetReq.Origin.OriginalFilename != "upload.bin" ||
		target.mediaAssetReq.Origin.ContentType != "application/octet-stream" {
		t.Fatalf("blank filename upload request = %#v", target.mediaAssetReq)
	}
	assertNoLegacyTargetVocabulary(t, blankFilename.Body.String())

	errorTarget := &fakeTargetService{now: now, err: storage.ErrAnalysisRunNotFound}
	errorMux := newFinalMux(Dependencies{Target: errorTarget})
	uploadError := httptest.NewRecorder()
	errorMux.ServeHTTP(uploadError, multipartTargetUploadRequest(t, "/v1/media-assets/upload", map[string]any{
		"channel_account_id": "channel-account-1",
		"kind":               "document",
	}, "notes.txt", "hello"))
	assertErrorCode(t, uploadError, http.StatusNotFound, "not_found")
	assertNoLegacyTargetVocabulary(t, uploadError.Body.String())
}

func TestTargetApiUploadMapsUnreadableUploadBodies(t *testing.T) {
	t.Parallel()

	req := multipartTargetUploadRequest(t, "/v1/media-assets/upload", map[string]any{
		"channel_account_id": "channel-account-1",
		"kind":               "voice",
		"display_name":       "voice.ogg",
	}, "voice.ogg", "voice-bytes")
	rec := httptest.NewRecorder()
	server := &Server{
		deps: Dependencies{Target: &fakeTargetService{now: time.Date(2026, 5, 18, 13, 0, 0, 0, time.UTC)}},
		readUploadBody: func(io.Reader) ([]byte, error) {
			return nil, io.ErrUnexpectedEOF
		},
		maxRequestBytes: defaultMaxRequestBody,
	}

	server.handleUploadTargetMediaAsset(rec, req)

	assertErrorCode(t, rec, http.StatusBadRequest, "invalid_media_asset")
	assertNoLegacyTargetVocabulary(t, rec.Body.String())
}

func TestTargetApiNormalizesNilItemsToEmptyArrays(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 18, 13, 5, 0, 0, time.UTC)
	target := &fakeTargetService{now: now, nilItems: true}
	mux := newFinalMux(Dependencies{Target: target})

	cases := []struct {
		name       string
		req        *http.Request
		wantStatus int
	}{
		{name: "list channel accounts", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-accounts", nil), wantStatus: http.StatusOK},
		{name: "list media assets", req: httptest.NewRequest(http.MethodGet, "/v1/media-assets?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "get inbox", req: httptest.NewRequest(http.MethodGet, "/v1/collections/inbox?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "create collection", req: jsonRequest(http.MethodPost, "/v1/collections", map[string]any{"channel_account_id": "channel-account-1", "name": "Research"}), wantStatus: http.StatusCreated},
		{name: "list collections", req: httptest.NewRequest(http.MethodGet, "/v1/collections?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "get collection", req: httptest.NewRequest(http.MethodGet, "/v1/collections/collection-1?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "update collection", req: jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{"channel_account_id": "channel-account-1", "expected_version": 1, "name": "Research"}), wantStatus: http.StatusOK},
		{name: "update collection items", req: jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{"channel_account_id": "channel-account-1", "expected_version": 1, "items": []map[string]any{{"media_asset_id": "media-asset-1", "position": 0}}}), wantStatus: http.StatusOK},
		{name: "remove collection item", req: httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-asset-1?channel_account_id=channel-account-1&expected_version=1", nil), wantStatus: http.StatusOK},
		{name: "list analysis runs", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "list analysis run events", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/events?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "list artifacts", req: httptest.NewRequest(http.MethodGet, "/v1/artifacts?channel_account_id=channel-account-1&analysis_run_id=run-1", nil), wantStatus: http.StatusOK},
		{name: "list diagnostics", req: httptest.NewRequest(http.MethodGet, "/v1/diagnostics?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "list step queue", req: httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/queue", nil), wantStatus: http.StatusOK},
		{name: "list surfaces", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "list active surfaces", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/active?channel_account_id=channel-account-1", nil), wantStatus: http.StatusOK},
		{name: "list surface events", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/surface-1/events", nil), wantStatus: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, tc.req)
			assertTargetStatus(t, rec, tc.wantStatus)
			body := rec.Body.String()
			if !strings.Contains(body, `"items":[]`) {
				t.Fatalf("%s response must normalize nil items to []: %s", tc.name, body)
			}
			assertNoLegacyTargetVocabulary(t, body)
		})
	}
}

func TestTargetApiReturnsDependencyUnavailableWhenTargetMissing(t *testing.T) {
	t.Parallel()

	mux := newFinalMux(Dependencies{})
	cases := []struct {
		name string
		req  *http.Request
	}{
		{name: "resolve channel account", req: jsonRequest(http.MethodPut, "/internal/v1/channel-accounts", map[string]any{"channel": "telegram", "external_account_ref": "chat-1"})},
		{name: "list channel accounts", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-accounts", nil)},
		{name: "update channel account", req: jsonRequest(http.MethodPatch, "/internal/v1/channel-accounts/channel-account-1", map[string]any{"display_name": "Danila"})},
		{name: "create media asset", req: jsonRequest(http.MethodPost, "/v1/media-assets", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "upload media asset", req: httptest.NewRequest(http.MethodPost, "/v1/media-assets/upload", strings.NewReader(""))},
		{name: "list media assets", req: httptest.NewRequest(http.MethodGet, "/v1/media-assets?channel_account_id=channel-account-1", nil)},
		{name: "get media asset", req: httptest.NewRequest(http.MethodGet, "/v1/media-assets/media-asset-1?channel_account_id=channel-account-1", nil)},
		{name: "delete media asset", req: httptest.NewRequest(http.MethodDelete, "/v1/media-assets/media-asset-1?channel_account_id=channel-account-1", nil)},
		{name: "get inbox", req: httptest.NewRequest(http.MethodGet, "/v1/collections/inbox?channel_account_id=channel-account-1", nil)},
		{name: "create collection", req: jsonRequest(http.MethodPost, "/v1/collections", map[string]any{"channel_account_id": "channel-account-1", "name": "Research"})},
		{name: "list collections", req: httptest.NewRequest(http.MethodGet, "/v1/collections?channel_account_id=channel-account-1", nil)},
		{name: "get collection", req: httptest.NewRequest(http.MethodGet, "/v1/collections/collection-1?channel_account_id=channel-account-1", nil)},
		{name: "update collection", req: jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{"channel_account_id": "channel-account-1", "expected_version": 1})},
		{name: "update collection items", req: jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{"channel_account_id": "channel-account-1", "expected_version": 1})},
		{name: "remove collection item", req: httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-asset-1?channel_account_id=channel-account-1&expected_version=1", nil)},
		{name: "create selection snapshot", req: jsonRequest(http.MethodPost, "/v1/selection-snapshots", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "get selection snapshot", req: httptest.NewRequest(http.MethodGet, "/v1/selection-snapshots/snapshot-1?channel_account_id=channel-account-1", nil)},
		{name: "create analysis run", req: jsonRequest(http.MethodPost, "/v1/analysis-runs", map[string]any{"channel_account_id": "channel-account-1", "selection_snapshot_id": "snapshot-1", "run_type": "transcription"})},
		{name: "list analysis runs", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs?channel_account_id=channel-account-1", nil)},
		{name: "get analysis run", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1?channel_account_id=channel-account-1", nil)},
		{name: "cancel analysis run", req: jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/cancel", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "retry analysis run", req: jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "list analysis run events", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/events?channel_account_id=channel-account-1", nil)},
		{name: "list artifacts", req: httptest.NewRequest(http.MethodGet, "/v1/artifacts?channel_account_id=channel-account-1&analysis_run_id=run-1", nil)},
		{name: "get artifact", req: httptest.NewRequest(http.MethodGet, "/v1/artifacts/artifact-1?channel_account_id=channel-account-1", nil)},
		{name: "refresh artifact", req: jsonRequest(http.MethodPost, "/v1/artifacts/artifact-1/refresh?channel_account_id=channel-account-1", nil)},
		{name: "list diagnostics", req: httptest.NewRequest(http.MethodGet, "/v1/diagnostics?channel_account_id=channel-account-1", nil)},
		{name: "claim step", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/claim", map[string]any{"worker_kind": "transcription"})},
		{name: "check step cancel", req: httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/run-1/steps/cancel-check?analysis_run_step_id=step-1", nil)},
		{name: "record step progress", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/progress", map[string]any{"analysis_run_step_id": "step-1"})},
		{name: "finalize step", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/finalize", map[string]any{"analysis_run_step_id": "step-1"})},
		{name: "upsert surface", req: jsonRequest(http.MethodPut, "/internal/v1/channel-surfaces", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "list surfaces", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces?channel_account_id=channel-account-1", nil)},
		{name: "list active surfaces", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/active?channel_account_id=channel-account-1", nil)},
		{name: "replace display state", req: jsonRequest(http.MethodPatch, "/internal/v1/channel-surfaces/surface-1/display-state", map[string]any{"expected_version": 1})},
		{name: "supersede surface", req: jsonRequest(http.MethodPost, "/internal/v1/channel-surfaces/surface-1/supersede", map[string]any{"actor_type": "telegram_adapter"})},
		{name: "list surface events", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/surface-1/events", nil)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, tc.req)
			assertErrorCode(t, rec, http.StatusServiceUnavailable, "dependency_unavailable")
			assertNoLegacyTargetVocabulary(t, rec.Body.String())
		})
	}
}

func TestTargetApiCoversQueryFallbackAndTargetQueueErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)
	target := &fakeTargetService{now: now, listStepQueueErr: storage.ErrAnalysisRunNotFound}
	mux := newFinalMux(Dependencies{Target: target})

	cancel := httptest.NewRecorder()
	mux.ServeHTTP(cancel, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/cancel?channel_account_id=channel-account-1", map[string]any{
		"message": "stop",
	}))
	assertTargetStatus(t, cancel, http.StatusOK)
	if target.cancelRunReq.ChannelAccountID != "channel-account-1" || target.cancelRunReq.Message != "stop" {
		t.Fatalf("cancel request = %#v", target.cancelRunReq)
	}

	retry := httptest.NewRecorder()
	mux.ServeHTTP(retry, jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry?channel_account_id=channel-account-1", map[string]any{
		"idempotency_key": "retry-1",
	}))
	assertTargetStatus(t, retry, http.StatusAccepted)
	if target.retryRunReq.ChannelAccountID != "channel-account-1" || target.retryRunReq.IdempotencyKey != "retry-1" {
		t.Fatalf("retry request = %#v", target.retryRunReq)
	}

	queue := httptest.NewRecorder()
	mux.ServeHTTP(queue, httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/queue?page_size=10", nil))
	assertErrorCode(t, queue, http.StatusNotFound, "not_found")
}

func TestTargetApiMapsTargetServiceErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 17, 11, 0, 0, 0, time.UTC)
	target := &fakeTargetService{now: now, err: storage.ErrAnalysisRunNotFound}
	mux := newFinalMux(Dependencies{Target: target})
	cases := []struct {
		name string
		req  *http.Request
	}{
		{name: "resolve channel account", req: jsonRequest(http.MethodPut, "/internal/v1/channel-accounts", map[string]any{"channel": "telegram", "external_account_ref": "chat-1"})},
		{name: "list channel accounts", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-accounts", nil)},
		{name: "update channel account", req: jsonRequest(http.MethodPatch, "/internal/v1/channel-accounts/channel-account-1", map[string]any{"display_name": "Danila"})},
		{name: "create media asset", req: jsonRequest(http.MethodPost, "/v1/media-assets", map[string]any{
			"channel_account_id": "channel-account-1",
			"kind":               "voice",
			"origin":             map[string]any{"origin_type": "telegram_file", "origin_ref": "voice-file-id"},
		})},
		{name: "list media assets", req: httptest.NewRequest(http.MethodGet, "/v1/media-assets?channel_account_id=channel-account-1", nil)},
		{name: "get media asset", req: httptest.NewRequest(http.MethodGet, "/v1/media-assets/media-asset-1?channel_account_id=channel-account-1", nil)},
		{name: "delete media asset", req: httptest.NewRequest(http.MethodDelete, "/v1/media-assets/media-asset-1?channel_account_id=channel-account-1", nil)},
		{name: "get inbox", req: httptest.NewRequest(http.MethodGet, "/v1/collections/inbox?channel_account_id=channel-account-1", nil)},
		{name: "create collection", req: jsonRequest(http.MethodPost, "/v1/collections", map[string]any{"channel_account_id": "channel-account-1", "name": "Research"})},
		{name: "list collections", req: httptest.NewRequest(http.MethodGet, "/v1/collections?channel_account_id=channel-account-1", nil)},
		{name: "get collection", req: httptest.NewRequest(http.MethodGet, "/v1/collections/collection-1?channel_account_id=channel-account-1", nil)},
		{name: "update collection", req: jsonRequest(http.MethodPatch, "/v1/collections/collection-1", map[string]any{
			"channel_account_id": "channel-account-1",
			"expected_version":   1,
			"name":               "Research v2",
		})},
		{name: "update collection items", req: jsonRequest(http.MethodPost, "/v1/collections/collection-1/items", map[string]any{
			"channel_account_id": "channel-account-1",
			"expected_version":   1,
			"items":              []map[string]any{{"media_asset_id": "media-asset-1", "position": 0}},
		})},
		{name: "remove collection item", req: httptest.NewRequest(http.MethodDelete, "/v1/collections/collection-1/items/media-asset-1?channel_account_id=channel-account-1&expected_version=1", nil)},
		{name: "create selection snapshot", req: jsonRequest(http.MethodPost, "/v1/selection-snapshots", map[string]any{
			"channel_account_id":   "channel-account-1",
			"source_collection_id": "collection-1",
			"items":                []map[string]any{{"media_asset_id": "media-asset-1", "position": 0}},
		})},
		{name: "get selection snapshot", req: httptest.NewRequest(http.MethodGet, "/v1/selection-snapshots/snapshot-1?channel_account_id=channel-account-1", nil)},
		{name: "create analysis run", req: jsonRequest(http.MethodPost, "/v1/analysis-runs", map[string]any{
			"channel_account_id":    "channel-account-1",
			"selection_snapshot_id": "snapshot-1",
			"run_type":              "transcription",
		})},
		{name: "list analysis runs", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs?channel_account_id=channel-account-1", nil)},
		{name: "get analysis run", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1?channel_account_id=channel-account-1", nil)},
		{name: "cancel analysis run", req: jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/cancel", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "retry analysis run", req: jsonRequest(http.MethodPost, "/v1/analysis-runs/run-1/retry", map[string]any{"channel_account_id": "channel-account-1"})},
		{name: "list analysis run events", req: httptest.NewRequest(http.MethodGet, "/v1/analysis-runs/run-1/events?channel_account_id=channel-account-1", nil)},
		{name: "list artifacts", req: httptest.NewRequest(http.MethodGet, "/v1/artifacts?channel_account_id=channel-account-1&analysis_run_id=run-1", nil)},
		{name: "get artifact", req: httptest.NewRequest(http.MethodGet, "/v1/artifacts/artifact-1?channel_account_id=channel-account-1", nil)},
		{name: "refresh artifact", req: jsonRequest(http.MethodPost, "/v1/artifacts/artifact-1/refresh?channel_account_id=channel-account-1", nil)},
		{name: "list diagnostics", req: httptest.NewRequest(http.MethodGet, "/v1/diagnostics?channel_account_id=channel-account-1", nil)},
		{name: "list step queue", req: httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/queue?page_size=10", nil)},
		{name: "claim step", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/claim", map[string]any{"worker_kind": "transcription", "step_kind": "selection.transcription"})},
		{name: "check step cancel", req: httptest.NewRequest(http.MethodGet, "/internal/v1/analysis-runs/run-1/steps/cancel-check?analysis_run_step_id=step-1", nil)},
		{name: "record step progress", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/progress", map[string]any{"analysis_run_step_id": "step-1"})},
		{name: "record artifacts", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/artifacts", map[string]any{"analysis_run_step_id": "step-1", "artifacts": []map[string]any{}})},
		{name: "record diagnostics", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/diagnostics", map[string]any{"analysis_run_step_id": "step-1", "diagnostics": []map[string]any{}})},
		{name: "finalize step", req: jsonRequest(http.MethodPost, "/internal/v1/analysis-runs/run-1/steps/finalize", map[string]any{"analysis_run_step_id": "step-1", "outcome": "failed"})},
		{name: "upsert surface", req: jsonRequest(http.MethodPut, "/internal/v1/channel-surfaces", map[string]any{
			"channel_account_id": "channel-account-1",
			"channel":            "telegram",
			"surface_type":       "message",
			"surface_key":        "run:run-1",
		})},
		{name: "list surfaces", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces?channel_account_id=channel-account-1", nil)},
		{name: "list active surfaces", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/active?channel_account_id=channel-account-1", nil)},
		{name: "replace display state", req: jsonRequest(http.MethodPatch, "/internal/v1/channel-surfaces/surface-1/display-state", map[string]any{
			"expected_version": 1,
			"display_state":    map[string]any{"status": "running"},
		})},
		{name: "supersede surface", req: jsonRequest(http.MethodPost, "/internal/v1/channel-surfaces/surface-1/supersede", map[string]any{"actor_type": "telegram_adapter"})},
		{name: "list surface events", req: httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/surface-1/events", nil)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, tc.req)
			assertErrorCode(t, rec, http.StatusNotFound, "not_found")
			assertNoLegacyTargetVocabulary(t, rec.Body.String())
		})
	}
}

func assertTargetStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d want %d body=%s", rec.Code, want, rec.Body.String())
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d want %d body=%s", rec.Code, wantStatus, rec.Body.String())
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error response must be JSON: %v body=%s", err, rec.Body.String())
	}
	if body.Error.Code != wantCode {
		t.Fatalf("error.code = %q want %q body=%s", body.Error.Code, wantCode, rec.Body.String())
	}
}

func newFinalMux(deps Dependencies) *http.ServeMux {
	server := NewServer(deps)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	return mux
}

func jsonRequest(method, path string, body any) *http.Request {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		reader = bytes.NewReader(encoded)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func assertTargetEnvelopeID(t *testing.T, body []byte, envelope, idKey, want string) {
	t.Helper()
	var raw map[string]map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("response must be an object envelope: %v body=%s", err, string(body))
	}
	if raw[envelope][idKey] != want {
		t.Fatalf("%s.%s = %#v want %q body=%s", envelope, idKey, raw[envelope][idKey], want, string(body))
	}
}

func assertNoLegacyTargetVocabulary(t *testing.T, body string) {
	t.Helper()
	for _, forbidden := range []string{
		"media_item",
		"media-item",
		"owner",
		"selection_id",
		"analysis_run_task",
		"adapter_projection",
		"telegram_message_id",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("target response contains legacy vocabulary %q: %s", forbidden, body)
		}
	}
}

func invalidTargetJSONRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader("{"))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func multipartTargetUploadRequest(t *testing.T, path string, metadata map[string]any, filename, body string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("json.Marshal(metadata) error = %v", err)
	}
	if err := writer.WriteField("metadata", string(encoded)); err != nil {
		t.Fatalf("WriteField(metadata) error = %v", err)
	}
	file, err := writer.CreateFormFile("file", filename)
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err := file.Write([]byte(body)); err != nil {
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func rawMultipartTargetUploadRequest(t *testing.T, path, metadata, filename, body, contentType string, includeFile bool) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if err := writer.WriteField("metadata", metadata); err != nil {
		t.Fatalf("WriteField(metadata) error = %v", err)
	}
	if includeFile {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
		if contentType != "" {
			header.Set("Content-Type", contentType)
		}
		file, err := writer.CreatePart(header)
		if err != nil {
			t.Fatalf("CreatePart(file) error = %v", err)
		}
		if _, err := file.Write([]byte(body)); err != nil {
			t.Fatalf("file.Write() error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func preTargetUploadStoredObjectID(channelAccountID, filename string, body []byte) string {
	sum := sha256.Sum256(body)
	checksum := fmt.Sprintf("sha256:%x", sum[:])
	return stableTargetID(strings.Join([]string{channelAccountID, filename, checksum}, ":"))
}

type fakeTargetService struct {
	now                 time.Time
	err                 error
	updateCollectionErr error
	listStepQueueErr    error
	nilItems            bool
	processingRunResult *TargetProcessingRun

	channelAccountReq        TargetChannelAccountRequest
	listChannelAccountsReq   TargetListChannelAccountsRequest
	updateChannelAccountReq  TargetUpdateChannelAccountRequest
	mediaAssetReq            TargetCreateMediaAssetRequest
	listMediaAssetsReq       TargetListMediaAssetsRequest
	getMediaAssetReq         TargetGetMediaAssetRequest
	deleteMediaAssetReq      TargetDeleteMediaAssetRequest
	getInboxCollectionReq    TargetGetInboxCollectionRequest
	createCollectionReq      TargetCreateCollectionRequest
	listCollectionsReq       TargetListCollectionsRequest
	getCollectionReq         TargetGetCollectionRequest
	updateCollectionReq      TargetUpdateCollectionRequest
	updateCollectionItemsReq TargetUpdateCollectionItemsRequest
	removeCollectionItemReq  TargetRemoveCollectionItemRequest
	startProcessingRunReq    TargetStartProcessingRunRequest
	selectionSnapshotReq     TargetCreateSelectionSnapshotRequest
	getSelectionSnapshotReq  TargetGetSelectionSnapshotRequest
	analysisRunReq           TargetCreateAnalysisRunRequest
	listAnalysisRunsReq      TargetListAnalysisRunsRequest
	getAnalysisRunReq        TargetGetAnalysisRunRequest
	cancelRunReq             TargetCancelAnalysisRunRequest
	retryRunReq              TargetRetryAnalysisRunRequest
	listRunEventsReq         TargetListAnalysisRunEventsRequest
	listArtifactsReq         TargetListArtifactsRequest
	reusableTranscriptReq    TargetReusableTranscriptRequest
	getArtifactReq           TargetGetArtifactRequest
	refreshArtifactReq       TargetRefreshArtifactRequest
	listDiagnosticsReq       TargetListDiagnosticsRequest
	listStepQueueReq         TargetAnalysisRunStepQueueRequest
	claimStepReq             TargetClaimAnalysisRunStepRequest
	checkStepCancelReq       TargetCheckAnalysisRunStepCancelRequest
	requestAccessReq         TargetRequestAccessRequest
	progressStepReq          TargetRecordAnalysisRunStepProgressRequest
	recordArtifactsReq       TargetRecordAnalysisRunArtifactsRequest
	recordDiagnosticsReq     TargetRecordAnalysisRunDiagnosticsRequest
	finalizeStepReq          TargetFinalizeAnalysisRunStepRequest
	surfaceReq               TargetUpsertChannelSurfaceRequest
	listSurfacesReq          TargetListChannelSurfacesRequest
	listActiveSurfacesReq    TargetListChannelSurfacesRequest
	displayStateReq          TargetReplaceChannelSurfaceDisplayStateRequest
	supersedeReq             TargetSupersedeChannelSurfaceRequest
	listSurfaceEventsReq     TargetListChannelSurfaceEventsRequest
}

func (f *fakeTargetService) ResolveChannelAccount(_ context.Context, req TargetChannelAccountRequest) (TargetChannelAccount, error) {
	f.channelAccountReq = req
	if f.err != nil {
		return TargetChannelAccount{}, f.err
	}
	return TargetChannelAccount{
		ChannelAccountID:   "channel-account-1",
		Channel:            req.Channel,
		ExternalAccountRef: req.ExternalAccountRef,
		DisplayName:        req.DisplayName,
		Status:             "active",
		CreatedAt:          f.now,
		UpdatedAt:          f.now,
	}, nil
}

func (f *fakeTargetService) ListChannelAccounts(_ context.Context, req TargetListChannelAccountsRequest) (TargetChannelAccountPage, error) {
	f.listChannelAccountsReq = req
	if f.err != nil {
		return TargetChannelAccountPage{}, f.err
	}
	items := []TargetChannelAccount{{
		ChannelAccountID:   "channel-account-1",
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        "Danila",
		Status:             "active",
		CreatedAt:          f.now,
		UpdatedAt:          f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetChannelAccountPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) UpdateChannelAccount(_ context.Context, req TargetUpdateChannelAccountRequest) (TargetChannelAccount, error) {
	f.updateChannelAccountReq = req
	if f.err != nil {
		return TargetChannelAccount{}, f.err
	}
	return TargetChannelAccount{
		ChannelAccountID:   req.ChannelAccountID,
		Channel:            "telegram",
		ExternalAccountRef: "chat-1",
		DisplayName:        req.DisplayName,
		Status:             "active",
		CreatedAt:          f.now,
		UpdatedAt:          f.now,
	}, nil
}

func (f *fakeTargetService) CreateMediaAsset(_ context.Context, req TargetCreateMediaAssetRequest) (TargetMediaAsset, error) {
	f.mediaAssetReq = req
	if f.err != nil {
		return TargetMediaAsset{}, f.err
	}
	return TargetMediaAsset{
		MediaAssetID:     "media-asset-1",
		ChannelAccountID: req.ChannelAccountID,
		Origin:           req.Origin,
		Kind:             req.Kind,
		DisplayName:      req.DisplayName,
		Status:           "available",
		Metadata:         req.Metadata,
		CreatedAt:        f.now,
		UpdatedAt:        f.now,
	}, nil
}

func (f *fakeTargetService) UploadMediaAsset(ctx context.Context, req TargetUploadMediaAssetRequest) (TargetMediaAsset, error) {
	body, err := io.ReadAll(req.Reader)
	if err != nil {
		return TargetMediaAsset{}, err
	}
	sum := sha256.Sum256(body)
	checksum := fmt.Sprintf("sha256:%x", sum[:])
	storedObjectID := targetUploadStoredObjectID(req.Metadata.ChannelAccountID, req.Filename, checksum, int64(len(body)))
	objectRef := "sources/uploads/" + storedObjectID + "/source"
	return f.CreateMediaAsset(ctx, TargetCreateMediaAssetRequest{
		ChannelAccountID: req.Metadata.ChannelAccountID,
		Origin: TargetMediaAssetOrigin{
			OriginType: "upload", OriginRef: objectRef, ObjectRef: objectRef,
			OriginalFilename: req.Filename, StoredObjectID: storedObjectID,
			ContentType: req.ContentType, SizeBytes: int64(len(body)), Checksum: checksum, UploadBody: body,
		},
		Kind: req.Metadata.Kind, DisplayName: firstNonEmpty(req.Metadata.DisplayName, req.Filename),
		CollectionID: req.Metadata.CollectionID, Metadata: req.Metadata.Metadata,
		IdempotencyKey: req.Metadata.IdempotencyKey,
	})
}

func (f *fakeTargetService) ListMediaAssets(_ context.Context, req TargetListMediaAssetsRequest) (TargetMediaAssetPage, error) {
	f.listMediaAssetsReq = req
	if f.err != nil {
		return TargetMediaAssetPage{}, f.err
	}
	items := []TargetMediaAsset{}
	if f.nilItems {
		items = nil
	}
	return TargetMediaAssetPage{Items: items, Page: 1, PageSize: req.PageSize}, nil
}

func (f *fakeTargetService) GetMediaAsset(_ context.Context, req TargetGetMediaAssetRequest) (TargetMediaAsset, error) {
	f.getMediaAssetReq = req
	if f.err != nil {
		return TargetMediaAsset{}, f.err
	}
	return TargetMediaAsset{
		MediaAssetID:     req.MediaAssetID,
		ChannelAccountID: req.ChannelAccountID,
		Origin:           TargetMediaAssetOrigin{OriginType: "telegram_file", OriginRef: "voice-file-id"},
		Kind:             "voice",
		DisplayName:      "voice.ogg",
		Status:           "available",
		CreatedAt:        f.now,
		UpdatedAt:        f.now,
	}, nil
}

func (f *fakeTargetService) DeleteMediaAsset(_ context.Context, req TargetDeleteMediaAssetRequest) (TargetMediaAsset, error) {
	f.deleteMediaAssetReq = req
	if f.err != nil {
		return TargetMediaAsset{}, f.err
	}
	return TargetMediaAsset{
		MediaAssetID:     req.MediaAssetID,
		ChannelAccountID: req.ChannelAccountID,
		Origin:           TargetMediaAssetOrigin{OriginType: "telegram_file", OriginRef: "voice-file-id"},
		Kind:             "voice",
		DisplayName:      "voice.ogg",
		Status:           "deleted",
		CreatedAt:        f.now,
		UpdatedAt:        f.now,
		DeletedAt:        &f.now,
	}, nil
}

func (f *fakeTargetService) GetInboxCollection(_ context.Context, req TargetGetInboxCollectionRequest) (TargetCollection, error) {
	f.getInboxCollectionReq = req
	if f.err != nil {
		return TargetCollection{}, f.err
	}
	collection := fakeTargetCollection("inbox-1", req.ChannelAccountID, "inbox", "Inbox", f.now)
	if f.nilItems {
		collection.Items = nil
	}
	return collection, nil
}

func (f *fakeTargetService) CreateCollection(_ context.Context, req TargetCreateCollectionRequest) (TargetCollection, error) {
	f.createCollectionReq = req
	if f.err != nil {
		return TargetCollection{}, f.err
	}
	collection := fakeTargetCollection("collection-1", req.ChannelAccountID, "user", req.Name, f.now)
	if f.nilItems {
		collection.Items = nil
	}
	return collection, nil
}

func (f *fakeTargetService) ListCollections(_ context.Context, req TargetListCollectionsRequest) (TargetCollectionPage, error) {
	f.listCollectionsReq = req
	if f.err != nil {
		return TargetCollectionPage{}, f.err
	}
	items := []TargetCollection{fakeTargetCollection("collection-1", req.ChannelAccountID, "user", "Research", f.now)}
	if f.nilItems {
		items = nil
	}
	return TargetCollectionPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) GetCollection(_ context.Context, req TargetGetCollectionRequest) (TargetCollection, error) {
	f.getCollectionReq = req
	if f.err != nil {
		return TargetCollection{}, f.err
	}
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", "Research", f.now)
	if f.nilItems {
		collection.Items = nil
	}
	return collection, nil
}

func (f *fakeTargetService) UpdateCollection(_ context.Context, req TargetUpdateCollectionRequest) (TargetCollection, error) {
	f.updateCollectionReq = req
	if f.updateCollectionErr != nil {
		return TargetCollection{}, f.updateCollectionErr
	}
	if f.err != nil {
		return TargetCollection{}, f.err
	}
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", req.Name, f.now)
	collection.Version = req.ExpectedVersion + 1
	if f.nilItems {
		collection.Items = nil
	}
	return collection, nil
}

func (f *fakeTargetService) UpdateCollectionItems(_ context.Context, req TargetUpdateCollectionItemsRequest) (TargetCollection, error) {
	f.updateCollectionItemsReq = req
	if f.err != nil {
		return TargetCollection{}, f.err
	}
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", "Research", f.now)
	collection.Version = req.ExpectedVersion + 1
	if f.nilItems {
		collection.Items = nil
	}
	return collection, nil
}

func (f *fakeTargetService) RemoveCollectionItem(_ context.Context, req TargetRemoveCollectionItemRequest) (TargetCollection, error) {
	f.removeCollectionItemReq = req
	if f.err != nil {
		return TargetCollection{}, f.err
	}
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", "Research", f.now)
	collection.Version = req.ExpectedVersion + 1
	collection.Items = []TargetCollectionItem{}
	if f.nilItems {
		collection.Items = nil
	}
	return collection, nil
}

func (f *fakeTargetService) StartCollectionProcessingRun(_ context.Context, req TargetStartProcessingRunRequest) (TargetProcessingRun, error) {
	f.startProcessingRunReq = req
	if f.err != nil {
		return TargetProcessingRun{}, f.err
	}
	if f.processingRunResult != nil {
		return *f.processingRunResult, nil
	}
	return TargetProcessingRun{
		SelectionSnapshot:     TargetSelectionSnapshot{SelectionSnapshotID: "snapshot-atomic", ChannelAccountID: req.ChannelAccountID, SourceCollectionID: req.CollectionID, Status: "sealed", Items: []TargetSelectionSnapshotItem{}, Diagnostics: []TargetDiagnostic{}, CreatedAt: f.now, SealedAt: f.now},
		AnalysisRun:           TargetAnalysisRun{AnalysisRunID: "run-atomic", ChannelAccountID: req.ChannelAccountID, SelectionSnapshotID: "snapshot-atomic", RunType: req.RunType, Status: "queued", Version: 1, CreatedAt: f.now},
		DetachedMediaAssetIDs: append([]string(nil), req.SelectedItemIDs...),
		CollectionVersion:     req.ExpectedVersion + 1,
	}, nil
}

func (f *fakeTargetService) CreateSelectionSnapshot(_ context.Context, req TargetCreateSelectionSnapshotRequest) (TargetSelectionSnapshot, error) {
	f.selectionSnapshotReq = req
	if f.err != nil {
		return TargetSelectionSnapshot{}, f.err
	}
	return TargetSelectionSnapshot{
		SelectionSnapshotID: "snapshot-1",
		ChannelAccountID:    req.ChannelAccountID,
		SourceCollectionID:  req.SourceCollectionID,
		Status:              "sealed",
		Items: []TargetSelectionSnapshotItem{{
			SelectionSnapshotItemID: "snapshot-item-1",
			MediaAssetID:            req.Items[0].MediaAssetID,
			Position:                req.Items[0].Position,
			Kind:                    "voice",
			DisplayName:             "voice.ogg",
			StatusAtSelection:       "available",
		}},
		OptionSnapshot: req.OptionSnapshot,
		Diagnostics:    []TargetDiagnostic{},
		CreatedAt:      f.now,
		SealedAt:       f.now,
	}, nil
}

func (f *fakeTargetService) GetSelectionSnapshot(_ context.Context, req TargetGetSelectionSnapshotRequest) (TargetSelectionSnapshot, error) {
	f.getSelectionSnapshotReq = req
	if f.err != nil {
		return TargetSelectionSnapshot{}, f.err
	}
	return TargetSelectionSnapshot{
		SelectionSnapshotID: req.SelectionSnapshotID,
		ChannelAccountID:    req.ChannelAccountID,
		SourceCollectionID:  "inbox-1",
		Status:              "sealed",
		Items: []TargetSelectionSnapshotItem{{
			SelectionSnapshotItemID: "snapshot-item-1",
			MediaAssetID:            "media-asset-1",
			Position:                0,
			Kind:                    "voice",
			DisplayName:             "voice.ogg",
			StatusAtSelection:       "available",
		}},
		Diagnostics: []TargetDiagnostic{},
		CreatedAt:   f.now,
		SealedAt:    f.now,
	}, nil
}

func fakeTargetCollection(id, channelAccountID, kind, name string, now time.Time) TargetCollection {
	return TargetCollection{
		CollectionID:     id,
		ChannelAccountID: channelAccountID,
		Kind:             kind,
		Name:             name,
		Status:           "active",
		Version:          1,
		Items: []TargetCollectionItem{{
			CollectionItemID: "collection-item-1",
			MediaAssetID:     "media-asset-1",
			Position:         0,
			AddedBy:          channelAccountID,
			AddedAt:          now,
		}},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (f *fakeTargetService) CreateAnalysisRun(_ context.Context, req TargetCreateAnalysisRunRequest) (TargetAnalysisRun, error) {
	f.analysisRunReq = req
	if f.err != nil {
		return TargetAnalysisRun{}, f.err
	}
	return TargetAnalysisRun{
		AnalysisRunID:       "run-1",
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: req.SelectionSnapshotID,
		RunType:             req.RunType,
		Status:              "queued",
		Version:             1,
		Params:              req.Params,
		Delivery:            req.Delivery,
		EvidenceGateState:   "not_required",
		CreatedAt:           f.now,
	}, nil
}

func (f *fakeTargetService) ListAnalysisRuns(_ context.Context, req TargetListAnalysisRunsRequest) (TargetAnalysisRunPage, error) {
	f.listAnalysisRunsReq = req
	if f.err != nil {
		return TargetAnalysisRunPage{}, f.err
	}
	items := []TargetAnalysisRun{{
		AnalysisRunID:       "run-1",
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: "snapshot-1",
		RunType:             "transcription",
		Status:              "queued",
		Version:             1,
		EvidenceGateState:   "not_required",
		CreatedAt:           f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetAnalysisRunPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) GetAnalysisRun(_ context.Context, req TargetGetAnalysisRunRequest) (TargetAnalysisRun, error) {
	f.getAnalysisRunReq = req
	if f.err != nil {
		return TargetAnalysisRun{}, f.err
	}
	return TargetAnalysisRun{
		AnalysisRunID:       req.AnalysisRunID,
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: "snapshot-1",
		RunType:             "transcription",
		Status:              "queued",
		Version:             1,
		EvidenceGateState:   "not_required",
		CreatedAt:           f.now,
	}, nil
}

func (f *fakeTargetService) CancelAnalysisRun(_ context.Context, analysisRunID string, req TargetCancelAnalysisRunRequest) (TargetAnalysisRun, error) {
	f.cancelRunReq = req
	if f.err != nil {
		return TargetAnalysisRun{}, f.err
	}
	return TargetAnalysisRun{
		AnalysisRunID:       analysisRunID,
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: "snapshot-1",
		RunType:             "transcription",
		Status:              "cancel_requested",
		Version:             2,
		EvidenceGateState:   "not_required",
		CreatedAt:           f.now,
		CancelRequestedAt:   &f.now,
	}, nil
}

func (f *fakeTargetService) RetryAnalysisRun(_ context.Context, analysisRunID string, req TargetRetryAnalysisRunRequest) (TargetAnalysisRun, error) {
	f.retryRunReq = req
	if f.err != nil {
		return TargetAnalysisRun{}, f.err
	}
	return TargetAnalysisRun{
		AnalysisRunID:       "retry-" + analysisRunID,
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: "snapshot-1",
		RunType:             "transcription",
		Status:              "queued",
		Version:             1,
		EvidenceGateState:   "not_required",
		CreatedAt:           f.now,
	}, nil
}

func (f *fakeTargetService) ListAnalysisRunEvents(_ context.Context, req TargetListAnalysisRunEventsRequest) (TargetAnalysisRunEventPage, error) {
	f.listRunEventsReq = req
	if f.err != nil {
		return TargetAnalysisRunEventPage{}, f.err
	}
	items := []TargetAnalysisRunEvent{{
		AnalysisRunEventID: "event-1",
		AnalysisRunID:      req.AnalysisRunID,
		EventType:          "analysis_run.created",
		Version:            1,
		Status:             "queued",
		CreatedAt:          f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetAnalysisRunEventPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ListArtifacts(_ context.Context, req TargetListArtifactsRequest) (TargetArtifactPage, error) {
	f.listArtifactsReq = req
	if f.err != nil {
		return TargetArtifactPage{}, f.err
	}
	items := []TargetArtifact{{
		ArtifactID:       "artifact-1",
		ChannelAccountID: req.ChannelAccountID,
		AnalysisRunID:    req.AnalysisRunID,
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Visibility:       "channel_deliverable",
		CreatedAt:        f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetArtifactPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) FindReusableTranscript(_ context.Context, req TargetReusableTranscriptRequest) (TargetReusableTranscript, bool, error) {
	f.reusableTranscriptReq = req
	if f.err != nil {
		return TargetReusableTranscript{}, false, f.err
	}
	run := TargetAnalysisRun{
		AnalysisRunID:       "run-1",
		ChannelAccountID:    req.ChannelAccountID,
		SelectionSnapshotID: "selection-1",
		RunType:             "transcription",
		Status:              "succeeded",
		Version:             2,
		CreatedAt:           f.now,
	}
	artifact := TargetArtifact{
		ArtifactID:       "artifact-1",
		ChannelAccountID: req.ChannelAccountID,
		AnalysisRunID:    "run-1",
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Visibility:       "channel_deliverable",
		Preview:          []byte(`{"available":true}`),
		CreatedAt:        f.now,
	}
	return TargetReusableTranscript{
		AnalysisRunID:      run.AnalysisRunID,
		AnalysisRunVersion: run.Version,
		ArtifactID:         artifact.ArtifactID,
		AnalysisRun:        run,
		Artifact:           artifact,
	}, true, nil
}

func (f *fakeTargetService) GetArtifact(_ context.Context, req TargetGetArtifactRequest) (TargetArtifact, error) {
	f.getArtifactReq = req
	if f.err != nil {
		return TargetArtifact{}, f.err
	}
	return TargetArtifact{
		ArtifactID:       req.ArtifactID,
		ChannelAccountID: req.ChannelAccountID,
		AnalysisRunID:    "run-1",
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Visibility:       "channel_deliverable",
		CreatedAt:        f.now,
	}, nil
}

func (f *fakeTargetService) RefreshArtifactLink(_ context.Context, req TargetRefreshArtifactRequest) (TargetArtifact, error) {
	f.refreshArtifactReq = req
	f.getArtifactReq = TargetGetArtifactRequest{ChannelAccountID: req.ChannelAccountID, ArtifactID: req.ArtifactID}
	if f.err != nil {
		return TargetArtifact{}, f.err
	}
	return TargetArtifact{
		ArtifactID:       req.ArtifactID,
		ChannelAccountID: req.ChannelAccountID,
		AnalysisRunID:    "run-1",
		Kind:             "transcript",
		Status:           "available",
		ContentType:      "text/plain",
		Visibility:       "channel_deliverable",
		CreatedAt:        f.now,
	}, nil
}

func (f *fakeTargetService) ListDiagnostics(_ context.Context, req TargetListDiagnosticsRequest) (TargetDiagnosticPage, error) {
	f.listDiagnosticsReq = req
	if f.err != nil {
		return TargetDiagnosticPage{}, f.err
	}
	items := []TargetDiagnostic{{
		DiagnosticID:     "diagnostic-1",
		ChannelAccountID: req.ChannelAccountID,
		SubjectType:      req.SubjectType,
		SubjectID:        req.SubjectID,
		Severity:         "warning",
		Code:             "analysis_prerequisite_missing",
		Message:          "Transcript is missing",
		CreatedAt:        f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetDiagnosticPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) GetObservabilitySnapshot(context.Context) (TargetObservabilitySnapshot, error) {
	if f.err != nil {
		return TargetObservabilitySnapshot{}, f.err
	}
	return TargetObservabilitySnapshot{
		QueueTasks:                 1,
		QueueLagSeconds:            5,
		ObservabilityWindowSeconds: 900,
		GeneratedAt:                f.now,
	}, nil
}

func (f *fakeTargetService) ListAnalysisRunStepQueue(_ context.Context, req TargetAnalysisRunStepQueueRequest) (TargetAnalysisRunStepQueueResponse, error) {
	f.listStepQueueReq = req
	if f.listStepQueueErr != nil {
		return TargetAnalysisRunStepQueueResponse{}, f.listStepQueueErr
	}
	if f.err != nil {
		return TargetAnalysisRunStepQueueResponse{}, f.err
	}
	items := []TargetAnalysisRunStepQueueItem{{
		AnalysisRunID:     "run-1",
		RunType:           "transcription",
		WorkerKind:        "transcription",
		StepKind:          "selection.transcription",
		Status:            "queued",
		Version:           1,
		AttemptNo:         1,
		AnalysisRunStepID: "step-1",
		CreatedAt:         f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetAnalysisRunStepQueueResponse{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ClaimAnalysisRunStep(_ context.Context, analysisRunID string, req TargetClaimAnalysisRunStepRequest) (TargetClaimAnalysisRunStepResponse, error) {
	f.claimStepReq = req
	if f.err != nil {
		return TargetClaimAnalysisRunStepResponse{}, f.err
	}
	return TargetClaimAnalysisRunStepResponse{
		AnalysisRunStepID: "step-1",
		AnalysisRunID:     analysisRunID,
		RunType:           "transcription",
		SelectionSnapshot: TargetSelectionSnapshot{
			SelectionSnapshotID: "snapshot-1",
			ChannelAccountID:    "channel-account-1",
			Status:              "sealed",
			Items: []TargetSelectionSnapshotItem{{
				SelectionSnapshotItemID: "snapshot-item-1",
				MediaAssetID:            "asset-1",
				Position:                0,
				Kind:                    "voice",
				DisplayName:             "voice.ogg",
				OriginSnapshot:          []byte(`{"origin_type":"telegram_file","object_ref":"file-id"}`),
				StorageSnapshot:         []byte(`{}`),
				Metadata:                []byte(`{}`),
				StatusAtSelection:       "available",
			}},
			OptionSnapshot: []byte(`{}`),
			Diagnostics:    []TargetDiagnostic{},
			CreatedAt:      f.now,
			SealedAt:       f.now,
		},
		AnalysisRunStepInputs: []TargetAnalysisRunStepInput{{
			AnalysisRunStepInputID:  "input-1",
			AnalysisRunStepID:       "step-1",
			InputKind:               "selection_snapshot_item",
			SelectionSnapshotItemID: "snapshot-item-1",
			Position:                0,
			Required:                true,
		}},
		Params:    []byte(`{"language":"ru"}`),
		ClaimedAt: f.now,
	}, nil
}

func (f *fakeTargetService) CheckAnalysisRunStepCancel(_ context.Context, _ string, req TargetCheckAnalysisRunStepCancelRequest) (TargetAnalysisRunStepCancelState, error) {
	f.checkStepCancelReq = req
	if f.err != nil {
		return TargetAnalysisRunStepCancelState{}, f.err
	}
	return TargetAnalysisRunStepCancelState{CancelRequested: false, Status: "running"}, nil
}

func (f *fakeTargetService) ResolveAnalysisRunStepRequestAccess(_ context.Context, _ string, req TargetRequestAccessRequest) (RequestAccessResponse, error) {
	f.requestAccessReq = req
	if f.err != nil {
		return RequestAccessResponse{}, f.err
	}
	return RequestAccessResponse{
		Provider:            "object_store",
		URL:                 "http://minio/request.json",
		ExpiresAt:           f.now.Add(time.Minute).Format(time.RFC3339),
		RequestRef:          "request.json",
		RequestDigestSHA256: "sha256:abc",
		RequestBytes:        10,
	}, nil
}

func (f *fakeTargetService) ResolveArtifactDownloadAccess(_ context.Context, artifactID string) (ArtifactDownloadAccessResponse, error) {
	if f.err != nil {
		return ArtifactDownloadAccessResponse{}, f.err
	}
	return ArtifactDownloadAccessResponse{
		ArtifactID:    artifactID,
		AnalysisRunID: "run-1",
		ArtifactKind:  "transcript_plain",
		Filename:      "transcript.txt",
		MIMEType:      "text/plain",
		SizeBytes:     10,
		CreatedAt:     f.now,
		Download: storage.DownloadDescriptor{
			Provider:  "minio",
			URL:       "http://minio/transcript.txt",
			ExpiresAt: f.now.Add(time.Minute),
		},
	}, nil
}

func (f *fakeTargetService) RecordAnalysisRunStepProgress(_ context.Context, _ string, req TargetRecordAnalysisRunStepProgressRequest) error {
	f.progressStepReq = req
	if f.err != nil {
		return f.err
	}
	return nil
}

func (f *fakeTargetService) RecordAnalysisRunArtifacts(_ context.Context, _ string, req TargetRecordAnalysisRunArtifactsRequest) error {
	f.recordArtifactsReq = req
	if f.err != nil {
		return f.err
	}
	return nil
}

func (f *fakeTargetService) RecordAnalysisRunDiagnostics(_ context.Context, _ string, req TargetRecordAnalysisRunDiagnosticsRequest) error {
	f.recordDiagnosticsReq = req
	if f.err != nil {
		return f.err
	}
	return nil
}

func (f *fakeTargetService) FinalizeAnalysisRunStep(_ context.Context, analysisRunID string, req TargetFinalizeAnalysisRunStepRequest) (TargetAnalysisRun, error) {
	f.finalizeStepReq = req
	if f.err != nil {
		return TargetAnalysisRun{}, f.err
	}
	return TargetAnalysisRun{
		AnalysisRunID:       analysisRunID,
		ChannelAccountID:    "channel-account-1",
		SelectionSnapshotID: "snapshot-1",
		RunType:             "transcription",
		Status:              req.Outcome,
		Version:             2,
		EvidenceGateState:   "not_required",
		CreatedAt:           f.now,
		CompletedAt:         &f.now,
	}, nil
}

func (f *fakeTargetService) UpsertChannelSurface(_ context.Context, req TargetUpsertChannelSurfaceRequest) (TargetChannelSurface, error) {
	f.surfaceReq = req
	if f.err != nil {
		return TargetChannelSurface{}, f.err
	}
	return TargetChannelSurface{
		ChannelSurfaceID:   "surface-1",
		ChannelAccountID:   req.ChannelAccountID,
		Channel:            req.Channel,
		SurfaceType:        req.SurfaceType,
		SurfaceKey:         req.SurfaceKey,
		Address:            req.Address,
		AddressFingerprint: req.AddressFingerprint,
		DisplayState:       req.DisplayState,
		LifecycleStatus:    "active",
		Version:            1,
		Subjects:           req.Subjects,
		CreatedAt:          f.now,
		UpdatedAt:          f.now,
	}, nil
}

func (f *fakeTargetService) ListChannelSurfaces(_ context.Context, req TargetListChannelSurfacesRequest) (TargetChannelSurfacePage, error) {
	if req.ActiveOnly {
		f.listActiveSurfacesReq = req
	} else {
		f.listSurfacesReq = req
	}
	if f.err != nil {
		return TargetChannelSurfacePage{}, f.err
	}
	items := []TargetChannelSurface{{
		ChannelSurfaceID:   "surface-1",
		ChannelAccountID:   req.ChannelAccountID,
		Channel:            "telegram",
		SurfaceType:        "message",
		SurfaceKey:         "run:run-1",
		LifecycleStatus:    "active",
		Version:            1,
		Subjects:           []TargetChannelSurfaceSubject{{SubjectType: "analysis_run", SubjectID: "run-1", SubjectRole: "primary"}},
		CreatedAt:          f.now,
		UpdatedAt:          f.now,
		LastRenderedAt:     &f.now,
		AddressFingerprint: "telegram:chat-1:42",
	}}
	if f.nilItems {
		items = nil
	}
	return TargetChannelSurfacePage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ReplaceChannelSurfaceDisplayState(_ context.Context, req TargetReplaceChannelSurfaceDisplayStateRequest) (TargetChannelSurface, error) {
	f.displayStateReq = req
	if f.err != nil {
		return TargetChannelSurface{}, f.err
	}
	return TargetChannelSurface{
		ChannelSurfaceID: "surface-1",
		ChannelAccountID: "channel-account-1",
		Channel:          "telegram",
		SurfaceType:      "message",
		SurfaceKey:       "run:run-1",
		DisplayState:     req.DisplayState,
		LifecycleStatus:  "active",
		Version:          req.ExpectedVersion + 1,
		CreatedAt:        f.now,
		UpdatedAt:        f.now,
	}, nil
}

func (f *fakeTargetService) SupersedeChannelSurface(_ context.Context, req TargetSupersedeChannelSurfaceRequest) (TargetChannelSurfaceEvent, error) {
	f.supersedeReq = req
	if f.err != nil {
		return TargetChannelSurfaceEvent{}, f.err
	}
	return TargetChannelSurfaceEvent{
		ChannelSurfaceEventID: "surface-event-1",
		ChannelSurfaceID:      req.SurfaceID,
		EventType:             "channel_surface.superseded",
		Reason:                req.Reason,
		ActorType:             req.ActorType,
		ActorID:               req.ActorID,
		CreatedAt:             f.now,
	}, nil
}

func (f *fakeTargetService) ListChannelSurfaceEvents(_ context.Context, req TargetListChannelSurfaceEventsRequest) (TargetChannelSurfaceEventPage, error) {
	f.listSurfaceEventsReq = req
	if f.err != nil {
		return TargetChannelSurfaceEventPage{}, f.err
	}
	items := []TargetChannelSurfaceEvent{{
		ChannelSurfaceEventID: "surface-event-1",
		ChannelSurfaceID:      req.SurfaceID,
		EventType:             "channel_surface.superseded",
		Reason:                "message_not_editable",
		ActorType:             "telegram_adapter",
		CreatedAt:             f.now,
	}}
	if f.nilItems {
		items = nil
	}
	return TargetChannelSurfaceEventPage{
		Items:    items,
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) CreateExportJob(_ context.Context, req TargetCreateExportJobRequest) (TargetExportJob, error) {
	return TargetExportJob{ExportJobID: "export-1", ChannelAccountID: req.ChannelAccountID, MediaAssetID: req.MediaAssetID, Operation: req.Operation, Variant: req.Variant, Status: "queued", Version: 1, Progress: []byte(`{"stage":"queued"}`), Deliveries: []TargetExportDelivery{}, CreatedAt: time.Now()}, nil
}
func (f *fakeTargetService) ListExportJobs(context.Context, TargetListExportJobsRequest) (TargetExportJobPage, error) {
	return TargetExportJobPage{Items: []TargetExportJob{}, Page: 1, PageSize: 20}, nil
}
func (f *fakeTargetService) GetExportJob(context.Context, TargetGetExportJobRequest) (TargetExportJob, error) {
	return TargetExportJob{ExportJobID: "export-1", Deliveries: []TargetExportDelivery{}}, nil
}
func (f *fakeTargetService) CancelExportJob(context.Context, TargetExportJobMutationRequest) (TargetExportJob, error) {
	return TargetExportJob{ExportJobID: "export-1", Status: "cancel_requested", Deliveries: []TargetExportDelivery{}}, nil
}
func (f *fakeTargetService) RetryExportJob(context.Context, TargetExportJobMutationRequest) (TargetExportJob, error) {
	return TargetExportJob{ExportJobID: "export-1", Status: "queued", Deliveries: []TargetExportDelivery{}}, nil
}
func (f *fakeTargetService) ClaimExportDelivery(context.Context, TargetClaimExportDeliveryRequest) (TargetExportDeliveryClaim, error) {
	return TargetExportDeliveryClaim{}, nil
}
func (f *fakeTargetService) FinalizeExportDelivery(context.Context, TargetFinalizeExportDeliveryRequest) (TargetExportDelivery, error) {
	return TargetExportDelivery{}, nil
}
func (f *fakeTargetService) ResolveExportDownload(context.Context, TargetGetExportJobRequest) (TargetExportDownload, error) {
	return TargetExportDownload{}, nil
}
func (f *fakeTargetService) ListExportJobQueue(context.Context, TargetExportQueueRequest) (TargetExportJobPage, error) {
	return TargetExportJobPage{Items: []TargetExportJob{}, Page: 1, PageSize: 20}, nil
}
func (f *fakeTargetService) ClaimExportJob(context.Context, TargetClaimExportJobRequest) (TargetExportJobClaim, error) {
	return TargetExportJobClaim{}, nil
}
func (f *fakeTargetService) CheckExportJobCancel(context.Context, TargetExportAttemptRequest) (TargetExportCancelState, error) {
	return TargetExportCancelState{}, nil
}
func (f *fakeTargetService) RecordExportJobProgress(context.Context, TargetRecordExportProgressRequest) error {
	return nil
}
func (f *fakeTargetService) FinalizeExportJob(context.Context, TargetFinalizeExportJobRequest) (TargetExportJob, error) {
	return TargetExportJob{ExportJobID: "export-1", Deliveries: []TargetExportDelivery{}}, nil
}
func (f *fakeTargetService) ReclaimExportJobs(context.Context, TargetExportReclaimRequest) (TargetExportReclaimResult, error) {
	return TargetExportReclaimResult{}, nil
}
func (f *fakeTargetService) SweepRetention(context.Context, TargetRetentionSweepRequest) (TargetRetentionSweepResult, error) {
	return TargetRetentionSweepResult{Claims: []TargetRetentionClaim{}}, nil
}

func (f *fakeTargetService) ReconcileRetention(context.Context, TargetRetentionReconcileRequest) (TargetRetentionReconcileResult, error) {
	return TargetRetentionReconcileResult{}, nil
}
