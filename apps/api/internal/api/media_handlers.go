package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

type addMediaItemHTTP struct {
	Owner         storage.OwnerScope        `json:"owner"`
	Kind          string                    `json:"kind"`
	Source        mediaSourceHTTP           `json:"source"`
	CollectionID  string                    `json:"collection_id,omitempty"`
	DisplayName   string                    `json:"display_name,omitempty"`
	AdapterOrigin string                    `json:"adapter_origin,omitempty"`
	Metadata      json.RawMessage           `json:"metadata,omitempty"`
	Retention     storage.RetentionMetadata `json:"retention,omitempty"`
}

type addMediaItemMultipartMetadataHTTP struct {
	Owner         storage.OwnerScope        `json:"owner"`
	Kind          string                    `json:"kind"`
	CollectionID  string                    `json:"collection_id,omitempty"`
	DisplayName   string                    `json:"display_name,omitempty"`
	AdapterOrigin string                    `json:"adapter_origin,omitempty"`
	Metadata      json.RawMessage           `json:"metadata,omitempty"`
	Retention     storage.RetentionMetadata `json:"retention,omitempty"`
}

type mediaSourceHTTP struct {
	OriginType       string `json:"origin_type"`
	Text             string `json:"text,omitempty"`
	URL              string `json:"url,omitempty"`
	ObjectRef        string `json:"object_ref,omitempty"`
	OriginalFilename string `json:"original_filename,omitempty"`
	ContentType      string `json:"content_type,omitempty"`
	SizeBytes        int64  `json:"size_bytes,omitempty"`
	Checksum         string `json:"checksum,omitempty"`
}

type collectionItemsHTTP struct {
	Owner           storage.OwnerScope `json:"owner"`
	ExpectedVersion int64              `json:"expected_version"`
	Items           []struct {
		MediaItemID string `json:"media_item_id"`
		Position    int    `json:"position"`
	} `json:"items"`
}

type createSelectionHTTP struct {
	Owner              storage.OwnerScope `json:"owner"`
	SourceCollectionID string             `json:"source_collection_id,omitempty"`
	Items              []struct {
		MediaItemID string `json:"media_item_id"`
		Position    int    `json:"position"`
	} `json:"items"`
	OptionSnapshot  json.RawMessage `json:"option_snapshot,omitempty"`
	DuplicatePolicy string          `json:"duplicate_policy,omitempty"`
	CreatedBy       string          `json:"created_by,omitempty"`
}

type createAnalysisRunHTTP struct {
	Owner       storage.OwnerScope `json:"owner"`
	SelectionID string             `json:"selection_id"`
	RunType     string             `json:"run_type"`
	Params      json.RawMessage    `json:"params,omitempty"`
	Delivery    json.RawMessage    `json:"delivery,omitempty"`
}

type cancelAnalysisRunHTTP struct {
	Message string `json:"message,omitempty"`
}

type retryAnalysisRunHTTP struct {
	Owner          storage.OwnerScope `json:"owner,omitempty"`
	IdempotencyKey string             `json:"idempotency_key,omitempty"`
}

type reconcileQueueHTTP struct {
	Limit int `json:"limit,omitempty"`
}

func (s *Server) handleAddMediaItem(w http.ResponseWriter, r *http.Request) {
	if s.deps.Public == nil {
		s.writeAPIError(w, dependencyUnavailableError("public service is not configured"))
		return
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "multipart/form-data") {
		s.handleAddMediaItemMultipart(w, r)
		return
	}
	var body addMediaItemHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_item", message: "media item request must be valid JSON", details: err.Error()})
		return
	}
	item, err := s.deps.Public.AddMediaItem(r.Context(), storage.AddMediaItemRequest{
		Owner: body.Owner,
		Kind:  body.Kind,
		Source: storage.AddMediaSource{
			OriginType:       body.Source.OriginType,
			Text:             body.Source.Text,
			URL:              body.Source.URL,
			ObjectRef:        body.Source.ObjectRef,
			OriginalFilename: body.Source.OriginalFilename,
			ContentType:      body.Source.ContentType,
			SizeBytes:        body.Source.SizeBytes,
			Checksum:         body.Source.Checksum,
		},
		CollectionID:  body.CollectionID,
		DisplayName:   body.DisplayName,
		AdapterOrigin: body.AdapterOrigin,
		MetadataJSON:  body.Metadata,
		Retention:     body.Retention,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"media_item": item})
}

func (s *Server) handleAddMediaItemMultipart(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes)
	if err := r.ParseMultipartForm(s.maxRequestBytes); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_item", message: "multipart media item request must be valid form data", details: err.Error()})
		return
	}
	metadataValue := strings.TrimSpace(r.FormValue("metadata"))
	if metadataValue == "" {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_item", message: "multipart media item request must include metadata"})
		return
	}
	var metadata addMediaItemMultipartMetadataHTTP
	decoder := json.NewDecoder(strings.NewReader(metadataValue))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_item", message: "multipart metadata must be valid JSON", details: err.Error()})
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_item", message: "multipart media item request must include a file", details: err.Error()})
		return
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_media_item", message: "multipart upload body could not be read", details: err.Error()})
		return
	}
	contentType := strings.TrimSpace(header.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	item, err := s.deps.Public.AddMediaItem(r.Context(), storage.AddMediaItemRequest{
		Owner: metadata.Owner,
		Kind:  metadata.Kind,
		Source: storage.AddMediaSource{
			OriginType:       "object",
			OriginalFilename: header.Filename,
			ContentType:      contentType,
			SizeBytes:        int64(len(body)),
			UploadBody:       body,
		},
		CollectionID:  metadata.CollectionID,
		DisplayName:   metadata.DisplayName,
		AdapterOrigin: metadata.AdapterOrigin,
		MetadataJSON:  metadata.Metadata,
		Retention:     metadata.Retention,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"media_item": item})
}

func (s *Server) handleListMediaItems(w http.ResponseWriter, r *http.Request) {
	items, err := s.deps.Public.ListMediaItems(r.Context(), ownerFromQuery(r))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	summaries := make([]mediaItemSummary, 0, len(items))
	for _, item := range items {
		summaries = append(summaries, summarizeMediaItem(item))
	}
	pageItems, page := cursorPage(summaries, cursor, pageSize, func(item mediaItemSummary) string { return item.ID })
	writeJSON(w, http.StatusOK, paged(pageItems, page))
}

func (s *Server) handleGetMediaItem(w http.ResponseWriter, r *http.Request) {
	item, err := s.deps.Public.GetMediaItem(r.Context(), ownerFromQuery(r), r.PathValue("media_item_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"media_item": item})
}

func (s *Server) handleRemoveMediaItem(w http.ResponseWriter, r *http.Request) {
	item, err := s.deps.Public.RemoveMediaItem(r.Context(), ownerFromQuery(r), r.PathValue("media_item_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"media_item": item})
}

func (s *Server) handleGetInboxCollection(w http.ResponseWriter, r *http.Request) {
	collection, err := s.deps.Public.GetInboxCollection(r.Context(), ownerFromQuery(r))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleCreateCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Owner storage.OwnerScope `json:"owner"`
		Name  string             `json:"name"`
		Items []string           `json:"items,omitempty"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_collection", message: "collection request must be valid JSON", details: err.Error()})
		return
	}
	collection, err := s.deps.Public.CreateCollection(r.Context(), storage.CreateCollectionRequest{Owner: body.Owner, Name: body.Name, Items: body.Items})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"collection": collection})
}

func (s *Server) handleListCollections(w http.ResponseWriter, r *http.Request) {
	collections, err := s.deps.Public.ListCollections(r.Context(), ownerFromQuery(r))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	pageItems, page := cursorPage(collections, cursor, pageSize, func(collection storage.CollectionRecord) string { return collection.ID })
	writeJSON(w, http.StatusOK, paged(pageItems, page))
}

func (s *Server) handleGetCollection(w http.ResponseWriter, r *http.Request) {
	collection, err := s.deps.Public.GetCollection(r.Context(), ownerFromQuery(r), r.PathValue("collection_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleUpdateCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Owner           storage.OwnerScope `json:"owner"`
		ExpectedVersion int64              `json:"expected_version"`
		Name            string             `json:"name,omitempty"`
		Status          string             `json:"status,omitempty"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_collection", message: "collection update must be valid JSON", details: err.Error()})
		return
	}
	collection, err := s.deps.Public.UpdateCollection(r.Context(), storage.UpdateCollectionRequest{
		CollectionID:    r.PathValue("collection_id"),
		Owner:           body.Owner,
		ExpectedVersion: body.ExpectedVersion,
		Name:            body.Name,
		Status:          body.Status,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleUpdateCollectionItems(w http.ResponseWriter, r *http.Request) {
	var body collectionItemsHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_collection_items", message: "collection item update must be valid JSON", details: err.Error()})
		return
	}
	items := make([]storage.CollectionItemRecord, 0, len(body.Items))
	for _, item := range body.Items {
		items = append(items, storage.CollectionItemRecord{MediaItemID: item.MediaItemID, Position: item.Position})
	}
	collection, err := s.deps.Public.UpdateCollectionItems(r.Context(), storage.UpdateCollectionItemsRequest{
		CollectionID:    r.PathValue("collection_id"),
		Owner:           body.Owner,
		ExpectedVersion: body.ExpectedVersion,
		Items:           items,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleRemoveCollectionItem(w http.ResponseWriter, r *http.Request) {
	expected, _ := strconv.ParseInt(r.URL.Query().Get("expected_version"), 10, 64)
	owner := ownerFromQuery(r)
	collectionID := r.PathValue("collection_id")
	removeID := r.PathValue("media_item_id")
	current, err := s.deps.Public.GetCollection(r.Context(), owner, collectionID)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	items := make([]storage.CollectionItemRecord, 0, len(current.Items))
	for _, item := range current.Items {
		if item.MediaItemID == removeID {
			continue
		}
		item.Position = len(items)
		items = append(items, item)
	}
	collection, err := s.deps.Public.UpdateCollectionItems(r.Context(), storage.UpdateCollectionItemsRequest{
		CollectionID:    collectionID,
		Owner:           owner,
		ExpectedVersion: expected,
		Items:           items,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": collection})
}

func (s *Server) handleCreateSelection(w http.ResponseWriter, r *http.Request) {
	var body createSelectionHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_selection", message: "selection request must be valid JSON", details: err.Error()})
		return
	}
	items := make([]storage.CollectionItemRecord, 0, len(body.Items))
	for _, item := range body.Items {
		items = append(items, storage.CollectionItemRecord{MediaItemID: item.MediaItemID, Position: item.Position})
	}
	selection, err := s.deps.Public.CreateSelection(r.Context(), storage.CreateSelectionRequest{
		Owner:              body.Owner,
		SourceCollectionID: body.SourceCollectionID,
		Items:              items,
		OptionSnapshotJSON: body.OptionSnapshot,
		DuplicatePolicy:    body.DuplicatePolicy,
		CreatedBy:          body.CreatedBy,
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"selection": selection})
}

func (s *Server) handleGetSelection(w http.ResponseWriter, r *http.Request) {
	selection, err := s.deps.Public.GetSelection(r.Context(), ownerFromQuery(r), r.PathValue("selection_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"selection": selection})
}

func (s *Server) handleCreateAnalysisRun(w http.ResponseWriter, r *http.Request) {
	var body createAnalysisRunHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_analysis_run", message: "analysis run request must be valid JSON", details: err.Error()})
		return
	}
	run, err := s.deps.Public.CreateAnalysisRun(r.Context(), storage.CreateAnalysisRunRequest{
		Owner:          body.Owner,
		SelectionID:    body.SelectionID,
		RunType:        body.RunType,
		ParamsJSON:     body.Params,
		DeliveryJSON:   body.Delivery,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"analysis_run": run})
}

func (s *Server) handleListAnalysisRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.deps.Public.ListAnalysisRuns(r.Context(), ownerFromQuery(r))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	summaries := make([]analysisRunSummary, 0, len(runs))
	for _, run := range runs {
		summaries = append(summaries, summarizeAnalysisRun(run))
	}
	pageItems, page := cursorPage(summaries, cursor, pageSize, func(run analysisRunSummary) string { return run.ID })
	writeJSON(w, http.StatusOK, paged(pageItems, page))
}

func (s *Server) handleGetAnalysisRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.deps.Public.GetAnalysisRun(r.Context(), ownerFromQuery(r), r.PathValue("analysis_run_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis_run": run})
}

func (s *Server) handleCancelAnalysisRun(w http.ResponseWriter, r *http.Request) {
	var body cancelAnalysisRunHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_cancel_request", message: "cancel request must be valid JSON", details: err.Error()})
		return
	}
	run, err := s.deps.Public.CancelAnalysisRun(r.Context(), ownerFromQuery(r), r.PathValue("analysis_run_id"), body.Message)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis_run": run})
}

func (s *Server) handleRetryAnalysisRun(w http.ResponseWriter, r *http.Request) {
	var body retryAnalysisRunHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_retry_request", message: "retry request must be valid JSON", details: err.Error()})
		return
	}
	owner := ownerFromQuery(r)
	if owner.Empty() {
		owner = body.Owner
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = body.IdempotencyKey
	}
	run, err := s.deps.Public.RetryAnalysisRun(r.Context(), owner, r.PathValue("analysis_run_id"), idempotencyKey)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"analysis_run": run})
}

func (s *Server) handleListAnalysisRunEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.deps.Public.ListAnalysisRunEvents(r.Context(), ownerFromQuery(r), r.PathValue("analysis_run_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	pageItems, page := cursorPage(events, cursor, pageSize, func(event storage.RunEventRecord) string { return event.ID })
	writeJSON(w, http.StatusOK, paged(pageItems, page))
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	artifacts, err := s.deps.Public.ListArtifacts(r.Context(), ownerFromQuery(r), r.URL.Query().Get("analysis_run_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	summaries := make([]artifactSummary, 0, len(artifacts))
	for _, artifact := range artifacts {
		summaries = append(summaries, summarizeArtifact(artifact))
	}
	pageItems, page := cursorPage(summaries, cursor, pageSize, func(artifact artifactSummary) string { return artifact.ID })
	writeJSON(w, http.StatusOK, paged(pageItems, page))
}

func (s *Server) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	artifact, err := s.deps.Public.GetArtifact(r.Context(), ownerFromQuery(r), r.PathValue("artifact_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifact": artifact})
}

func (s *Server) handleRefreshArtifactLink(w http.ResponseWriter, r *http.Request) {
	artifact, err := s.deps.Public.RefreshArtifactLink(r.Context(), ownerFromQuery(r), r.PathValue("artifact_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifact": artifact})
}

func (s *Server) handleListDiagnostics(w http.ResponseWriter, r *http.Request) {
	diagnostics, err := s.deps.Public.ListDiagnostics(r.Context(), ownerFromQuery(r), r.URL.Query().Get("subject_type"), r.URL.Query().Get("subject_id"))
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	cursor, pageSize := parsePageRequest(r)
	pageItems, page := cursorPage(diagnostics, cursor, pageSize, func(diagnostic storage.DiagnosticRecord) string { return diagnostic.ID })
	writeJSON(w, http.StatusOK, paged(pageItems, page))
}

func (s *Server) handleReconcileAnalysisRunQueue(w http.ResponseWriter, r *http.Request) {
	var body reconcileQueueHTTP
	if err := decodeJSONBody(r, &body); err != nil {
		s.writeAPIError(w, apiError{status: http.StatusBadRequest, code: "invalid_reconcile_request", message: "reconcile request must be valid JSON", details: err.Error()})
		return
	}
	limit := body.Limit
	if limit <= 0 {
		limit = 100
	}
	reconciled, err := s.deps.Public.ReconcileAnalysisRunQueue(r.Context(), limit)
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"reconciled": reconciled})
}

func (s *Server) handleGetObservabilitySnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.deps.Public.GetObservabilitySnapshot(r.Context())
	if err != nil {
		s.writeAPIError(w, mapFinalStorageError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"observability": snapshot})
}

func ownerFromQuery(r *http.Request) storage.OwnerScope {
	return storage.OwnerScope{
		OwnerType: strings.TrimSpace(r.URL.Query().Get("owner_type")),
		OwnerID:   strings.TrimSpace(r.URL.Query().Get("owner_id")),
		TenantID:  strings.TrimSpace(r.URL.Query().Get("tenant_id")),
	}
}

type sourceSummary struct {
	SourceID   string `json:"source_id"`
	OriginType string `json:"origin_type"`
	MIMEType   string `json:"mime_type,omitempty"`
	SizeBytes  *int64 `json:"size_bytes,omitempty"`
}

type mediaItemSummary struct {
	ID               string                    `json:"media_item_id"`
	Owner            storage.OwnerScope        `json:"owner"`
	Kind             string                    `json:"kind"`
	Status           string                    `json:"status"`
	DisplayName      string                    `json:"display_name"`
	AdapterOrigin    string                    `json:"adapter_origin,omitempty"`
	Source           sourceSummary             `json:"source"`
	DiagnosticsCount int                       `json:"diagnostics_count"`
	Retention        storage.RetentionMetadata `json:"retention"`
	CreatedAt        time.Time                 `json:"created_at"`
	UpdatedAt        time.Time                 `json:"updated_at"`
}

type analysisRunSummary struct {
	ID                string             `json:"analysis_run_id"`
	Owner             storage.OwnerScope `json:"owner"`
	SelectionID       string             `json:"selection_id"`
	RunType           string             `json:"run_type"`
	Status            string             `json:"status"`
	Version           int64              `json:"version"`
	EvidenceGateState string             `json:"evidence_gate_state"`
	ArtifactCount     int                `json:"artifact_count"`
	DiagnosticsCount  int                `json:"diagnostics_count"`
	CreatedAt         time.Time          `json:"created_at"`
	StartedAt         *time.Time         `json:"started_at,omitempty"`
	CompletedAt       *time.Time         `json:"completed_at,omitempty"`
	CanceledAt        *time.Time         `json:"canceled_at,omitempty"`
	ExpiresAt         *time.Time         `json:"expires_at,omitempty"`
}

type artifactSummary struct {
	ID            string    `json:"artifact_id"`
	AnalysisRunID string    `json:"analysis_run_id"`
	Kind          string    `json:"kind"`
	Status        string    `json:"status"`
	ContentType   string    `json:"content_type"`
	SizeBytes     int64     `json:"size_bytes"`
	Visibility    string    `json:"visibility"`
	CreatedAt     time.Time `json:"created_at"`
}

type pageMetadata struct {
	PageSize   int    `json:"page_size"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func summarizeMediaItem(item storage.MediaItemRecord) mediaItemSummary {
	return mediaItemSummary{
		ID:            item.ID,
		Owner:         item.Owner,
		Kind:          item.Kind,
		Status:        item.Status,
		DisplayName:   item.DisplayName,
		AdapterOrigin: item.AdapterOrigin,
		Source: sourceSummary{
			SourceID:   item.Source.SourceID,
			OriginType: item.Source.OriginType,
			MIMEType:   item.Source.MIMEType,
			SizeBytes:  item.Source.SizeBytes,
		},
		DiagnosticsCount: len(item.Diagnostics),
		Retention:        item.Retention,
		CreatedAt:        item.CreatedAt,
		UpdatedAt:        item.UpdatedAt,
	}
}

func summarizeAnalysisRun(run storage.AnalysisRunRecord) analysisRunSummary {
	return analysisRunSummary{
		ID:                run.ID,
		Owner:             run.Owner,
		SelectionID:       run.SelectionID,
		RunType:           run.RunType,
		Status:            run.Status,
		Version:           run.Version,
		EvidenceGateState: run.EvidenceGateState,
		ArtifactCount:     len(run.Artifacts),
		DiagnosticsCount:  len(run.Diagnostics),
		CreatedAt:         run.CreatedAt,
		StartedAt:         run.StartedAt,
		CompletedAt:       run.CompletedAt,
		CanceledAt:        run.CanceledAt,
		ExpiresAt:         run.ExpiresAt,
	}
}

func summarizeArtifact(artifact storage.ArtifactRecord) artifactSummary {
	return artifactSummary{
		ID:            artifact.ID,
		AnalysisRunID: artifact.AnalysisRunID,
		Kind:          artifact.Kind,
		Status:        artifact.Status,
		ContentType:   artifact.ContentType,
		SizeBytes:     artifact.SizeBytes,
		Visibility:    artifact.Visibility,
		CreatedAt:     artifact.CreatedAt,
	}
}

func parsePageRequest(r *http.Request) (string, int) {
	const (
		defaultPageSize = 50
		maxPageSize     = 100
	)
	pageSize := defaultPageSize
	if raw := strings.TrimSpace(r.URL.Query().Get("page_size")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			pageSize = parsed
		}
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return strings.TrimSpace(r.URL.Query().Get("cursor")), pageSize
}

func cursorPage[T any](items []T, cursor string, pageSize int, cursorOf func(T) string) ([]T, pageMetadata) {
	start := 0
	if cursor != "" {
		for idx, item := range items {
			if cursorOf(item) == cursor {
				start = idx + 1
				break
			}
		}
	}
	if start > len(items) {
		start = len(items)
	}
	end := start + pageSize
	hasMore := false
	if end < len(items) {
		hasMore = true
	} else {
		end = len(items)
	}
	pageItems := items[start:end]
	page := pageMetadata{PageSize: pageSize, HasMore: hasMore}
	if hasMore && len(pageItems) > 0 {
		page.NextCursor = cursorOf(pageItems[len(pageItems)-1])
	}
	return pageItems, page
}

func paged[T any](items []T, page pageMetadata) map[string]any {
	return map[string]any{
		"items": items,
		"page":  page,
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func mapFinalStorageError(err error) apiError {
	switch {
	case errors.Is(err, storage.ErrCollectionVersionConflict):
		return apiError{status: http.StatusConflict, code: "collection_version_conflict", message: "collection version conflict"}
	case errors.Is(err, storage.ErrMediaItemNotFound), errors.Is(err, storage.ErrCollectionNotFound), errors.Is(err, storage.ErrSelectionNotFound), errors.Is(err, storage.ErrAnalysisRunNotFound), errors.Is(err, storage.ErrArtifactNotFound):
		return apiError{status: http.StatusNotFound, code: "not_found", message: "resource was not found"}
	case errors.Is(err, storage.ErrOwnerMismatch):
		return apiError{status: http.StatusNotFound, code: "not_found", message: "resource was not found"}
	case errors.Is(err, storage.ErrRetryRequiresTerminalRun):
		return apiError{status: http.StatusConflict, code: "retry_requires_terminal_run", message: "analysis run must be terminal before retry"}
	case errors.Is(err, storage.ErrArtifactResolutionFailed):
		return apiError{status: http.StatusBadGateway, code: "artifact_resolution_failed", message: "artifact link could not be resolved"}
	case errors.Is(err, storage.ErrContractViolation):
		return apiError{status: http.StatusBadRequest, code: "invalid_request", message: err.Error()}
	default:
		return apiError{status: http.StatusInternalServerError, code: "internal_error", message: err.Error()}
	}
}
