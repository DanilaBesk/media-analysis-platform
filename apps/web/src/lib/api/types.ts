export type OwnerType = "user" | "telegram" | "web" | "mcp" | "service";
export type MediaKind =
  | "text"
  | "url"
  | "file"
  | "photo"
  | "image"
  | "audio"
  | "voice"
  | "video"
  | "document";
export type SourceOriginType = "text" | "url" | "object";
export type MediaItemStatus = "validating" | "ready" | "quarantined" | "deleted";
export type CollectionKind = "inbox" | "user";
export type CollectionStatus = "active" | "archived" | "deleted";
export type SelectionStatus = "sealed" | "invalidated";
export type AnalysisRunStatus =
  | "queued"
  | "running"
  | "cancel_requested"
  | "partially_succeeded"
  | "succeeded"
  | "failed"
  | "canceled"
  | "expired";
export type RunType = "transcription" | "summary" | "report" | "deep_research" | "custom";
export type DeliveryStrategy = "polling" | "webhook";
export type ArtifactKind =
  | "transcript"
  | "summary"
  | "report"
  | "deep_research"
  | "structured_data"
  | "source_manifest"
  | "run_manifest"
  | "execution_log"
  | "diagnostic_bundle"
  | "run_diagnostics"
  | "preview";
export type ArtifactStatus = "pending" | "available" | "failed" | "expired" | "deleted";
export type DiagnosticSeverity = "info" | "warning" | "error";
export type DiagnosticSubjectType =
  | "media_item"
  | "source"
  | "collection"
  | "selection"
  | "analysis_run"
  | "artifact"
  | "adapter"
  | "retention";

export interface OwnerScope {
  owner_type: OwnerType;
  owner_id: string;
  tenant_id?: string;
  adapter_identity?: Record<string, unknown>;
}

export interface PageMetadata {
  page_size: number;
  has_more: boolean;
  next_cursor?: string;
}

export interface PaginatedResponse<TItem> {
  items: TItem[];
  page: PageMetadata;
}

export interface RetentionMetadata {
  state: string;
  policy_id?: string;
  expires_at?: string | null;
  deleted_at?: string | null;
  hold_reason?: string;
}

export interface MediaSourceMetadata {
  source_id: string;
  origin_type: SourceOriginType;
  external_uri?: string | null;
  object_key?: string | null;
  text_ref?: string | null;
  checksum?: string | null;
  size_bytes?: number | null;
  mime_type?: string | null;
  expires_at?: string | null;
}

export interface MediaItemSummary {
  media_item_id: string;
  owner: OwnerScope;
  kind: MediaKind;
  status: MediaItemStatus;
  display_name: string;
  adapter_origin?: string | null;
  source: MediaSourceMetadata;
  diagnostics_count?: number;
  retention: RetentionMetadata;
  created_at: string;
  updated_at: string;
}

export interface DiagnosticSummary {
  diagnostic_id: string;
  severity: DiagnosticSeverity;
  code: string;
  message: string;
  correlation_id?: string | null;
  created_at: string;
}

export interface Diagnostic extends DiagnosticSummary {
  owner: OwnerScope;
  subject: {
    subject_type: DiagnosticSubjectType;
    subject_id: string;
  };
  context?: Record<string, unknown>;
  safe_adapter_context?: Record<string, unknown>;
  remediation_hint?: string | null;
}

export interface MediaItem extends MediaItemSummary {
  diagnostics?: DiagnosticSummary[];
  deleted_at?: string | null;
}

export interface CollectionItem {
  media_item_id: string;
  position: number;
  media_item?: MediaItem;
  added_by?: string | null;
  added_at: string;
}

export interface Collection {
  collection_id: string;
  owner: OwnerScope;
  kind: CollectionKind;
  name: string;
  status: CollectionStatus;
  version: number;
  items: CollectionItem[];
  created_at: string;
  updated_at: string;
  archived_at?: string | null;
  deleted_at?: string | null;
}

export interface SelectionItemSnapshot {
  position: number;
  media_item_id: string;
  kind: MediaKind;
  source_snapshot: MediaSourceMetadata;
  display_name: string;
  status_at_selection: MediaItemStatus;
  metadata_snapshot?: Record<string, unknown>;
  retention_snapshot: RetentionMetadata;
  diagnostics?: DiagnosticSummary[];
}

export interface Selection {
  selection_id: string;
  owner: OwnerScope;
  status: SelectionStatus;
  source_collection_id?: string | null;
  items: SelectionItemSnapshot[];
  option_snapshot?: Record<string, unknown>;
  created_by: string;
  diagnostics?: DiagnosticSummary[];
  created_at: string;
  sealed_at: string;
}

export interface DeliveryPreference {
  strategy: DeliveryStrategy;
  webhook?: {
    url: string;
  };
}

export interface ArtifactPreview {
  available: boolean;
  kind?: "text" | "image" | "table" | "none" | null;
  content_type?: string | null;
  text_excerpt?: string | null;
  thumbnail_url?: string | null;
  expires_at?: string | null;
  filename?: string | null;
  format?: string | null;
  artifact_kind?: string | null;
  worker_artifact_kind?: string | null;
}

export interface ArtifactSummary {
  artifact_id: string;
  analysis_run_id: string;
  kind: ArtifactKind;
  status: ArtifactStatus;
  content_type: string;
  size_bytes: number;
  visibility?: string;
  preview?: ArtifactPreview;
  created_at: string;
}

export interface Artifact extends ArtifactSummary {
  owner: OwnerScope;
  object_key?: string | null;
  checksum?: string | null;
  visibility: string;
  download: {
    available: boolean;
    provider?: "minio_presigned_url" | null;
    url?: string | null;
    expires_at?: string | null;
    filename?: string | null;
  };
  retention: RetentionMetadata;
  diagnostics?: DiagnosticSummary[];
  expires_at?: string | null;
}

export interface RunProgressPayload {
  analysis_run_id?: string;
  stage?: string;
  message?: string;
  payload?: Record<string, unknown>;
}

export interface AnalysisRunSummary {
  analysis_run_id: string;
  owner: OwnerScope;
  selection_id: string;
  run_type: RunType;
  status: AnalysisRunStatus;
  version: number;
  evidence_gate_state: "not_required" | "waiting" | "passed" | "failed";
  artifact_count?: number;
  diagnostics_count?: number;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  canceled_at?: string | null;
  expires_at?: string | null;
}

export interface AnalysisRun extends AnalysisRunSummary {
  selection: Selection;
  params?: Record<string, unknown>;
  delivery: DeliveryPreference;
  artifacts: ArtifactSummary[];
  diagnostics: DiagnosticSummary[];
}

export interface RunEvent {
  event_id: string;
  analysis_run_id: string;
  event_type: string;
  version: number;
  emitted_at: string;
  status?: AnalysisRunStatus;
  payload: RunProgressPayload | Record<string, unknown>;
  artifact?: ArtifactSummary;
  diagnostic?: DiagnosticSummary;
}

export interface ReconcileQueueResponse {
  reconciled: number;
}

export interface ObservabilitySnapshot {
  queue_tasks: number;
  queue_lag_seconds: number;
  cleanup_failures: number;
  artifact_resolution_failures: number;
  generated_at: string;
}

export interface AddMediaItemDraft {
  kind: MediaKind;
  displayName: string;
  adapterOrigin: string;
  source:
    | { origin_type: "text"; text: string; language_hint?: string }
    | { origin_type: "url"; url: string }
    | {
        origin_type: "object";
        object_ref: string;
        original_filename?: string;
        content_type?: string;
        size_bytes?: number;
      };
}

export interface CollectionDraft {
  name: string;
  items: string[];
}

export interface SelectionDraft {
  sourceCollectionId?: string;
  items: Array<{ media_item_id: string; position: number }>;
  optionSnapshot?: Record<string, unknown>;
  duplicatePolicy?: "preserve" | "deduplicate";
  createdBy?: string;
}

export interface RunDraft {
  selectionId: string;
  runType: RunType;
  params?: Record<string, unknown>;
  delivery: DeliveryPreference;
}

export interface UpdateCollectionDraft {
  expectedVersion: number;
  name?: string;
  status?: CollectionStatus;
}

export interface ReplaceCollectionItemsDraft {
  expectedVersion: number;
  items: Array<{ media_item_id: string; position: number }>;
}
