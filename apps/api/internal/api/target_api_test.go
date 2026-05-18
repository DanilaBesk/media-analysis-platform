package api

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
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
	mux.ServeHTTP(listActiveSurfaces, httptest.NewRequest(http.MethodGet, "/internal/v1/channel-surfaces/active?channel_account_id=channel-account-1&page_size=10", nil))
	assertTargetStatus(t, listActiveSurfaces, http.StatusOK)
	assertNoLegacyTargetVocabulary(t, listActiveSurfaces.Body.String())
	if !target.listActiveSurfacesReq.ActiveOnly {
		t.Fatalf("list active surfaces request = %#v", target.listActiveSurfacesReq)
	}

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

func assertTargetStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d want %d body=%s", rec.Code, want, rec.Body.String())
	}
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

type fakeTargetService struct {
	now                 time.Time
	updateCollectionErr error

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
	selectionSnapshotReq     TargetCreateSelectionSnapshotRequest
	getSelectionSnapshotReq  TargetGetSelectionSnapshotRequest
	analysisRunReq           TargetCreateAnalysisRunRequest
	listAnalysisRunsReq      TargetListAnalysisRunsRequest
	getAnalysisRunReq        TargetGetAnalysisRunRequest
	cancelRunReq             TargetCancelAnalysisRunRequest
	retryRunReq              TargetRetryAnalysisRunRequest
	listRunEventsReq         TargetListAnalysisRunEventsRequest
	listArtifactsReq         TargetListArtifactsRequest
	getArtifactReq           TargetGetArtifactRequest
	listDiagnosticsReq       TargetListDiagnosticsRequest
	listStepQueueReq         TargetAnalysisRunStepQueueRequest
	claimStepReq             TargetClaimAnalysisRunStepRequest
	checkStepCancelReq       TargetCheckAnalysisRunStepCancelRequest
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
	return TargetChannelAccountPage{
		Items: []TargetChannelAccount{{
			ChannelAccountID:   "channel-account-1",
			Channel:            "telegram",
			ExternalAccountRef: "chat-1",
			DisplayName:        "Danila",
			Status:             "active",
			CreatedAt:          f.now,
			UpdatedAt:          f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) UpdateChannelAccount(_ context.Context, req TargetUpdateChannelAccountRequest) (TargetChannelAccount, error) {
	f.updateChannelAccountReq = req
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

func (f *fakeTargetService) ListMediaAssets(_ context.Context, req TargetListMediaAssetsRequest) (TargetMediaAssetPage, error) {
	f.listMediaAssetsReq = req
	return TargetMediaAssetPage{Items: []TargetMediaAsset{}, Page: 1, PageSize: req.PageSize}, nil
}

func (f *fakeTargetService) GetMediaAsset(_ context.Context, req TargetGetMediaAssetRequest) (TargetMediaAsset, error) {
	f.getMediaAssetReq = req
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
	return fakeTargetCollection("inbox-1", req.ChannelAccountID, "inbox", "Inbox", f.now), nil
}

func (f *fakeTargetService) CreateCollection(_ context.Context, req TargetCreateCollectionRequest) (TargetCollection, error) {
	f.createCollectionReq = req
	return fakeTargetCollection("collection-1", req.ChannelAccountID, "user", req.Name, f.now), nil
}

func (f *fakeTargetService) ListCollections(_ context.Context, req TargetListCollectionsRequest) (TargetCollectionPage, error) {
	f.listCollectionsReq = req
	return TargetCollectionPage{
		Items:    []TargetCollection{fakeTargetCollection("collection-1", req.ChannelAccountID, "user", "Research", f.now)},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) GetCollection(_ context.Context, req TargetGetCollectionRequest) (TargetCollection, error) {
	f.getCollectionReq = req
	return fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", "Research", f.now), nil
}

func (f *fakeTargetService) UpdateCollection(_ context.Context, req TargetUpdateCollectionRequest) (TargetCollection, error) {
	if f.updateCollectionErr != nil {
		return TargetCollection{}, f.updateCollectionErr
	}
	f.updateCollectionReq = req
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", req.Name, f.now)
	collection.Version = req.ExpectedVersion + 1
	return collection, nil
}

func (f *fakeTargetService) UpdateCollectionItems(_ context.Context, req TargetUpdateCollectionItemsRequest) (TargetCollection, error) {
	f.updateCollectionItemsReq = req
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", "Research", f.now)
	collection.Version = req.ExpectedVersion + 1
	return collection, nil
}

func (f *fakeTargetService) RemoveCollectionItem(_ context.Context, req TargetRemoveCollectionItemRequest) (TargetCollection, error) {
	f.removeCollectionItemReq = req
	collection := fakeTargetCollection(req.CollectionID, req.ChannelAccountID, "user", "Research", f.now)
	collection.Version = req.ExpectedVersion + 1
	collection.Items = []TargetCollectionItem{}
	return collection, nil
}

func (f *fakeTargetService) CreateSelectionSnapshot(_ context.Context, req TargetCreateSelectionSnapshotRequest) (TargetSelectionSnapshot, error) {
	f.selectionSnapshotReq = req
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
	return TargetAnalysisRunPage{
		Items: []TargetAnalysisRun{{
			AnalysisRunID:       "run-1",
			ChannelAccountID:    req.ChannelAccountID,
			SelectionSnapshotID: "snapshot-1",
			RunType:             "transcription",
			Status:              "queued",
			Version:             1,
			EvidenceGateState:   "not_required",
			CreatedAt:           f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) GetAnalysisRun(_ context.Context, req TargetGetAnalysisRunRequest) (TargetAnalysisRun, error) {
	f.getAnalysisRunReq = req
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
	return TargetAnalysisRunEventPage{
		Items: []TargetAnalysisRunEvent{{
			AnalysisRunEventID: "event-1",
			AnalysisRunID:      req.AnalysisRunID,
			EventType:          "analysis_run.created",
			Version:            1,
			Status:             "queued",
			CreatedAt:          f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ListArtifacts(_ context.Context, req TargetListArtifactsRequest) (TargetArtifactPage, error) {
	f.listArtifactsReq = req
	return TargetArtifactPage{
		Items: []TargetArtifact{{
			ArtifactID:       "artifact-1",
			ChannelAccountID: req.ChannelAccountID,
			AnalysisRunID:    req.AnalysisRunID,
			Kind:             "transcript",
			Status:           "available",
			ContentType:      "text/plain",
			Visibility:       "channel_deliverable",
			CreatedAt:        f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) GetArtifact(_ context.Context, req TargetGetArtifactRequest) (TargetArtifact, error) {
	f.getArtifactReq = req
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
	return TargetDiagnosticPage{
		Items: []TargetDiagnostic{{
			DiagnosticID:     "diagnostic-1",
			ChannelAccountID: req.ChannelAccountID,
			SubjectType:      req.SubjectType,
			SubjectID:        req.SubjectID,
			Severity:         "warning",
			Code:             "analysis_prerequisite_missing",
			Message:          "Transcript is missing",
			CreatedAt:        f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ListAnalysisRunStepQueue(_ context.Context, req TargetAnalysisRunStepQueueRequest) (TargetAnalysisRunStepQueueResponse, error) {
	f.listStepQueueReq = req
	return TargetAnalysisRunStepQueueResponse{
		Items: []TargetAnalysisRunStepQueueItem{{
			AnalysisRunID:     "run-1",
			RunType:           "transcription",
			WorkerKind:        "transcription",
			StepKind:          "selection.transcription",
			Status:            "queued",
			Version:           1,
			AttemptNo:         1,
			AnalysisRunStepID: "step-1",
			CreatedAt:         f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ClaimAnalysisRunStep(_ context.Context, analysisRunID string, req TargetClaimAnalysisRunStepRequest) (TargetClaimAnalysisRunStepResponse, error) {
	f.claimStepReq = req
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
	return TargetAnalysisRunStepCancelState{CancelRequested: false, Status: "running"}, nil
}

func (f *fakeTargetService) RecordAnalysisRunStepProgress(_ context.Context, _ string, req TargetRecordAnalysisRunStepProgressRequest) error {
	f.progressStepReq = req
	return nil
}

func (f *fakeTargetService) RecordAnalysisRunArtifacts(_ context.Context, _ string, req TargetRecordAnalysisRunArtifactsRequest) error {
	f.recordArtifactsReq = req
	return nil
}

func (f *fakeTargetService) RecordAnalysisRunDiagnostics(_ context.Context, _ string, req TargetRecordAnalysisRunDiagnosticsRequest) error {
	f.recordDiagnosticsReq = req
	return nil
}

func (f *fakeTargetService) FinalizeAnalysisRunStep(_ context.Context, analysisRunID string, req TargetFinalizeAnalysisRunStepRequest) (TargetAnalysisRun, error) {
	f.finalizeStepReq = req
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
	return TargetChannelSurfacePage{
		Items: []TargetChannelSurface{{
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
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}

func (f *fakeTargetService) ReplaceChannelSurfaceDisplayState(_ context.Context, req TargetReplaceChannelSurfaceDisplayStateRequest) (TargetChannelSurface, error) {
	f.displayStateReq = req
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
	return TargetChannelSurfaceEventPage{
		Items: []TargetChannelSurfaceEvent{{
			ChannelSurfaceEventID: "surface-event-1",
			ChannelSurfaceID:      req.SurfaceID,
			EventType:             "channel_surface.superseded",
			Reason:                "message_not_editable",
			ActorType:             "telegram_adapter",
			CreatedAt:             f.now,
		}},
		Page:     1,
		PageSize: req.PageSize,
	}, nil
}
