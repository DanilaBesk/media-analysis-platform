export type ChannelAccountId = string;
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
export type MediaAssetOriginType = "text" | "url" | "upload" | "telegram_file" | "object";
export type MediaAssetStatus = "validating" | "ready" | "quarantined" | "deleted";
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
  | "media_asset"
  | "stored_object"
  | "collection"
  | "selection_snapshot"
  | "analysis_run"
  | "analysis_run_step"
  | "artifact"
  | "artifact_subject"
  | "diagnostic"
  | "channel_account"
  | "channel_surface"
  | "operation_request";

export interface PageMetadata {
  page_size: number;
  has_more?: boolean;
  next_cursor?: string;
}

export interface PaginatedResponse<TItem> {
  items: TItem[];
  page?: PageMetadata;
  page_size?: number;
  next_cursor?: string;
}

export interface RetentionMetadata {
  state: string;
  policy_id?: string;
  expires_at?: string | null;
  deleted_at?: string | null;
  hold_reason?: string;
}

export interface MediaAssetOrigin {
  origin_type: MediaAssetOriginType;
  origin_ref?: string | null;
  object_ref?: string | null;
  original_filename?: string | null;
  stored_object_id?: string | null;
  content_type?: string | null;
  size_bytes?: number | null;
  checksum?: string | null;
  text?: string;
  url?: string;
  language_hint?: string;
}

export interface MediaAssetSummary {
  media_asset_id: string;
  channel_account_id: ChannelAccountId;
  kind: MediaKind;
  status: MediaAssetStatus;
  display_name: string;
  origin: MediaAssetOrigin;
  diagnostics_count?: number;
  created_at: string;
  updated_at: string;
  deleted_at?: string | null;
}

export interface MediaAsset extends MediaAssetSummary {
  diagnostics?: DiagnosticSummary[];
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
  channel_account_id?: ChannelAccountId;
  subject?: {
    subject_type: DiagnosticSubjectType;
    subject_id: string;
  };
  context?: Record<string, unknown>;
  safe_channel_context?: Record<string, unknown>;
  remediation_hint?: string | null;
}

export interface CollectionItem {
  media_asset_id: string;
  position: number;
  media_asset?: MediaAsset;
  added_by?: string | null;
  added_at: string;
}

export interface Collection {
  collection_id: string;
  channel_account_id: ChannelAccountId;
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

export interface SelectionSnapshotItem {
  selection_snapshot_item_id: string;
  position: number;
  media_asset_id: string;
  kind: MediaKind;
  display_name: string;
  origin_snapshot: MediaAssetOrigin;
  storage_snapshot?: Record<string, unknown>;
  metadata_snapshot?: Record<string, unknown>;
  status_at_selection: MediaAssetStatus;
  diagnostics?: DiagnosticSummary[];
}

export interface SelectionSnapshot {
  selection_snapshot_id: string;
  channel_account_id: ChannelAccountId;
  status: SelectionStatus;
  source_collection_id?: string | null;
  items: SelectionSnapshotItem[];
  option_snapshot?: Record<string, unknown>;
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
  channel_account_id?: ChannelAccountId;
  stored_object_id?: string | null;
  object_key?: string | null;
  checksum?: string | null;
  visibility: string;
  download?: {
    available: boolean;
    provider?: "minio_presigned_url" | null;
    url?: string | null;
    expires_at?: string | null;
    filename?: string | null;
  };
  retention?: RetentionMetadata;
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
  channel_account_id: ChannelAccountId;
  selection_snapshot_id: string;
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
  selection_snapshot: SelectionSnapshot;
  params?: Record<string, unknown>;
  delivery: DeliveryPreference;
  artifacts: ArtifactSummary[];
  diagnostics: DiagnosticSummary[];
}

export interface RunEvent {
  event_id?: string;
  analysis_run_event_id?: string;
  analysis_run_id: string;
  event_type: string;
  version: number;
  emitted_at?: string;
  created_at?: string;
  status?: AnalysisRunStatus;
  payload: RunProgressPayload | Record<string, unknown>;
  artifact?: ArtifactSummary;
  diagnostic?: DiagnosticSummary;
}

export interface ObservabilitySnapshot {
  queue_tasks: number;
  queue_lag_seconds: number;
  cleanup_failures: number;
  artifact_resolution_failures: number;
  generated_at: string;
}

export interface AddMediaAssetInput {
  kind: MediaKind;
  displayName: string;
  origin: MediaAssetOrigin;
}

export interface CollectionInput {
  name: string;
  items: string[];
}

export interface SelectionSnapshotInput {
  sourceCollectionId?: string;
  items: Array<{ media_asset_id: string; position: number }>;
  optionSnapshot?: Record<string, unknown>;
}

export interface AnalysisRunInput {
  selectionSnapshotId: string;
  runType: RunType;
  params?: Record<string, unknown>;
  delivery: DeliveryPreference;
}

export interface UpdateCollectionInput {
  expectedVersion: number;
  name?: string;
  status?: CollectionStatus;
}

export interface ReplaceCollectionItemsInput {
  expectedVersion: number;
  items: Array<{ media_asset_id: string; position: number }>;
}
