// FILE: apps/web/src/lib/api/types.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Define the canonical packet-local DTO surface that the Web UI consumes from the HTTP and WebSocket APIs.
// SCOPE: Model job snapshots, actions, events, artifacts, delivery settings, and request payloads without embedding business logic in the client.
// DEPENDS: M-WEB-UI, M-API-HTTP, M-API-EVENTS, M-CONTRACTS
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: TYPES
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the packet-local API and event DTO surface for the Web UI implementation packet.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   define-http-dtos - Mirror the job, artifact, and action response shapes consumed through the HTTP API.
//   define-event-dtos - Mirror the list-event and websocket-event surfaces used for polling-truth reconciliation.
// END_MODULE_MAP

export type JobStatus =
  | "queued"
  | "running"
  | "cancel_requested"
  | "succeeded"
  | "failed"
  | "canceled";

export type JobType = "transcription" | "report" | "deep_research";

export type SourceKind = "uploaded_file" | "telegram_upload" | "youtube_url";

export type ArtifactKind =
  | "transcript_plain"
  | "transcript_segmented_markdown"
  | "transcript_docx"
  | "report_markdown"
  | "report_docx"
  | "deep_research_markdown"
  | "execution_log";

export type DeliveryStrategy = "polling" | "webhook";

export interface DeliveryConfig {
  strategy: DeliveryStrategy;
  webhook_url?: string | null;
}

export interface SourceReference {
  source_id: string;
  source_kind: SourceKind;
  display_name?: string | null;
  original_filename?: string | null;
  source_url?: string | null;
  size_bytes?: number | null;
  sha256?: string | null;
}

export interface SourceSetItem {
  position: number;
  source: SourceReference;
}

export interface SourceSetView {
  source_set_id: string;
  input_kind: "single_source" | "combined_upload" | "combined_telegram";
  items: SourceSetItem[];
}

export interface ArtifactSummary {
  artifact_id: string;
  artifact_kind: ArtifactKind;
  filename: string;
  mime_type: string;
  size_bytes: number;
  created_at: string;
}

export interface ChildJobReference {
  job_id: string;
  job_type: JobType;
  status: JobStatus;
  version: number;
  job_url: string;
  root_job_id: string;
}

export interface ProgressState {
  stage: string;
  message?: string | null;
}

export interface ErrorInfo {
  code: string;
  message?: string | null;
}

export interface JobSnapshot {
  job_id: string;
  root_job_id: string;
  parent_job_id?: string | null;
  retry_of_job_id?: string | null;
  job_type: JobType;
  status: JobStatus;
  version: number;
  display_name?: string | null;
  client_ref?: string | null;
  delivery: DeliveryConfig;
  source_set: SourceSetView;
  artifacts: ArtifactSummary[];
  children: ChildJobReference[];
  progress?: ProgressState | null;
  latest_error?: ErrorInfo | null;
  created_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  cancel_requested_at?: string | null;
}

export interface JobAcceptedEnvelope {
  job: JobSnapshot;
}

export interface JobsAcceptedEnvelope {
  jobs: JobSnapshot[];
}

export interface JobListResponse {
  items: JobSnapshot[];
  page: number;
  page_size: number;
  next_page?: number | null;
}

export interface ArtifactResolution {
  artifact_id: string;
  job_id: string;
  artifact_kind: ArtifactKind;
  filename: string;
  mime_type: string;
  size_bytes: number;
  created_at: string;
  download: {
    provider: "minio_presigned_url";
    url: string;
    expires_at: string;
  };
}

export interface JobEventPayload {
  status: JobStatus;
  progress_stage?: string | null;
  progress_message?: string | null;
}

export interface JobEventView {
  event_id: string;
  event_type:
    | "job.created"
    | "job.updated"
    | "job.artifact_created"
    | "job.completed"
    | "job.failed"
    | "job.canceled";
  job_id: string;
  root_job_id: string;
  version: number;
  emitted_at: string;
  payload: JobEventPayload;
}

export interface JobEventListResponse {
  items: JobEventView[];
}

export interface JobEventStreamEnvelope extends JobEventView {
  job_type: JobType;
  job_url: string;
}

export interface ListJobsFilter {
  status?: JobStatus | "";
  job_type?: JobType | "";
  root_job_id?: string;
  page?: number;
  page_size?: number;
}

export interface DeliveryDraft {
  strategy: DeliveryStrategy;
  webhookUrl: string;
}

export interface UploadDraft {
  files: File[];
  displayName: string;
  clientRef: string;
  delivery: DeliveryDraft;
}

export interface UrlDraft {
  url: string;
  displayName: string;
  clientRef: string;
  delivery: DeliveryDraft;
}

export interface ChildActionDraft {
  clientRef: string;
  delivery: DeliveryDraft;
}
