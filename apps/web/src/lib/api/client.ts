// FILE: apps/web/src/lib/api/client.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Provide a thin, mockable HTTP and WebSocket boundary for the Web UI while preserving API-owned business logic and polling truth.
// SCOPE: Normalize request URLs, send job or artifact actions to the API, subscribe to event updates, and reconcile transport state only at the boundary.
// DEPENDS: M-WEB-UI, M-API-HTTP, M-API-EVENTS, M-CONTRACTS
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Expanded the packet-local boundary to cover job actions, artifact resolve, and websocket event reconciliation helpers.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   normalize-transport-targets - Resolve REST request targets and preserve the declared WebSocket event endpoint inside the client boundary.
//   send-api-requests - Execute submission, read, action, and artifact requests while preserving API error envelopes.
//   subscribe-to-events - Parse websocket event messages into packet-local DTOs for polling-truth reconciliation.
// END_MODULE_MAP

import type {
  ArtifactResolution,
  ChildActionDraft,
  DeliveryDraft,
  JobAcceptedEnvelope,
  JobEventListResponse,
  JobEventStreamEnvelope,
  JobListResponse,
  JobSnapshot,
  JobsAcceptedEnvelope,
  ListJobsFilter,
  UploadDraft,
  UrlDraft,
} from "./types";

export const RECONCILE_STATE_MARKER = "[WebUi][reconcileState][BLOCK_RECONCILE_POLLING_AND_WS_STATE]";

export interface JobEventSubscription {
  close(): void;
}

export interface SubscribeToJobEventsOptions {
  onMessage: (event: JobEventStreamEnvelope) => void;
  onOpen?: () => void;
  onClose?: () => void;
  onError?: (error: Event | Error) => void;
}

export interface WebSocketLike {
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent<string>) => void) | null;
  onerror: ((event: Event) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  close(): void;
}

export interface WebUiApiClient {
  listJobs(filter: ListJobsFilter): Promise<JobListResponse>;
  getJob(jobId: string): Promise<JobSnapshot>;
  createUpload(draft: UploadDraft): Promise<JobSnapshot[]>;
  createCombinedUpload(draft: UploadDraft): Promise<JobSnapshot>;
  createFromUrl(draft: UrlDraft): Promise<JobSnapshot>;
  createReport(jobId: string, draft: ChildActionDraft): Promise<JobSnapshot>;
  createDeepResearch(jobId: string, draft: ChildActionDraft): Promise<JobSnapshot>;
  cancelJob(jobId: string): Promise<JobSnapshot>;
  retryJob(jobId: string): Promise<JobSnapshot>;
  resolveArtifact(artifactId: string): Promise<ArtifactResolution>;
  listJobEvents(jobId: string): Promise<JobEventListResponse["items"]>;
  subscribeToJobEvents(options: SubscribeToJobEventsOptions): JobEventSubscription;
}

export interface CreateWebUiApiClientOptions {
  baseUrl: string;
  wsUrl: string;
  fetchImpl?: typeof fetch;
  webSocketFactory?: (url: string) => WebSocketLike;
}

export class WebUiApiClientError extends Error {
  readonly status: number;
  readonly path: string;
  readonly code?: string;

  constructor(path: string, status: number, message: string, code?: string) {
    super(message);
    this.name = "WebUiApiClientError";
    this.path = path;
    this.status = status;
    this.code = code;
  }
}

function toRequestUrl(baseUrl: string, path: string): URL {
  const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const normalizedPath = path.startsWith("/") ? path.slice(1) : path;
  return new URL(normalizedPath, normalizedBaseUrl);
}

function buildDeliveryFields(delivery: DeliveryDraft): Record<string, string> {
  const fields: Record<string, string> = {
    delivery_strategy: delivery.strategy,
  };
  if (delivery.strategy === "webhook" && delivery.webhookUrl.trim() !== "") {
    fields.delivery_webhook_url = delivery.webhookUrl.trim();
  }
  return fields;
}

function toChildPayload(draft: ChildActionDraft): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  if (draft.clientRef.trim() !== "") {
    payload.client_ref = draft.clientRef.trim();
  }
  if (draft.delivery.strategy === "webhook" || draft.delivery.webhookUrl.trim() !== "") {
    payload.delivery = {
      strategy: draft.delivery.strategy,
      webhook:
        draft.delivery.strategy === "webhook" && draft.delivery.webhookUrl.trim() !== ""
          ? { url: draft.delivery.webhookUrl.trim() }
          : undefined,
    };
  }
  return payload;
}

function appendUploadDraft(formData: FormData, draft: UploadDraft): void {
  draft.files.forEach((file) => {
    formData.append("files", file, file.name);
  });
  if (draft.displayName.trim() !== "") {
    formData.append("display_name", draft.displayName.trim());
  }
  if (draft.clientRef.trim() !== "") {
    formData.append("client_ref", draft.clientRef.trim());
  }
  const deliveryFields = buildDeliveryFields(draft.delivery);
  Object.entries(deliveryFields).forEach(([key, value]) => {
    formData.append(key, value);
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function extractJobSnapshot(payload: unknown): JobSnapshot {
  if (!isRecord(payload)) {
    throw new Error("Job response is not an object");
  }
  if (isRecord(payload.job)) {
    return payload.job as JobSnapshot;
  }
  return payload as JobSnapshot;
}

function extractJobs(payload: unknown): JobSnapshot[] {
  if (!isRecord(payload) || !Array.isArray(payload.jobs)) {
    throw new Error("Jobs response does not include jobs[]");
  }
  return payload.jobs as JobSnapshot[];
}

function extractError(payload: unknown): { code?: string; message?: string } {
  if (!isRecord(payload) || !isRecord(payload.error)) {
    return {};
  }
  return {
    code: typeof payload.error.code === "string" ? payload.error.code : undefined,
    message: typeof payload.error.message === "string" ? payload.error.message : undefined,
  };
}

function isJobEventStreamEnvelope(payload: unknown): payload is JobEventStreamEnvelope {
  return (
    isRecord(payload) &&
    typeof payload.event_id === "string" &&
    typeof payload.event_type === "string" &&
    typeof payload.job_id === "string" &&
    typeof payload.root_job_id === "string" &&
    typeof payload.job_type === "string" &&
    typeof payload.job_url === "string" &&
    typeof payload.version === "number" &&
    typeof payload.emitted_at === "string" &&
    isRecord(payload.payload)
  );
}

export function requiresRestReconciliation(lastSeenVersion: number, incomingVersion: number): boolean {
  if (incomingVersion <= lastSeenVersion) {
    return true;
  }
  return incomingVersion !== lastSeenVersion + 1;
}

function defaultWebSocketFactory(url: string): WebSocketLike {
  return new WebSocket(url);
}

// START_BLOCK_BLOCK_CREATE_WEB_UI_API_CLIENT
export function createWebUiApiClient({
  baseUrl,
  wsUrl,
  fetchImpl = fetch,
  webSocketFactory = defaultWebSocketFactory,
}: CreateWebUiApiClientOptions): WebUiApiClient {
  const requestJson = async <TResponse>(path: string, init?: RequestInit): Promise<TResponse> => {
    const response = await fetchImpl(toRequestUrl(baseUrl, path), {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init?.headers ?? {}),
      },
    });

    const contentType = response.headers.get("Content-Type") ?? "";
    const payload =
      response.status === 204
        ? undefined
        : contentType.includes("application/json")
          ? await response.json()
          : await response.text();

    if (!response.ok) {
      const apiError = extractError(payload);
      throw new WebUiApiClientError(
        path,
        response.status,
        apiError.message ?? `API request failed for ${path}`,
        apiError.code,
      );
    }

    return payload as TResponse;
  };

  const postJson = <TResponse>(path: string, payload?: unknown): Promise<TResponse> =>
    requestJson<TResponse>(path, {
      method: "POST",
      headers: payload ? { "Content-Type": "application/json" } : undefined,
      body: payload ? JSON.stringify(payload) : undefined,
    });

  return {
    async listJobs(filter) {
      const search = new URLSearchParams();
      if (filter.status) {
        search.set("status", filter.status);
      }
      if (filter.job_type) {
        search.set("job_type", filter.job_type);
      }
      if (filter.root_job_id?.trim()) {
        search.set("root_job_id", filter.root_job_id.trim());
      }
      search.set("page", String(filter.page ?? 1));
      search.set("page_size", String(filter.page_size ?? 20));
      return requestJson<JobListResponse>(`/v1/jobs?${search.toString()}`);
    },

    async getJob(jobId) {
      const payload = await requestJson<JobAcceptedEnvelope | JobSnapshot>(`/v1/jobs/${jobId}`);
      return extractJobSnapshot(payload);
    },

    async createUpload(draft) {
      const formData = new FormData();
      appendUploadDraft(formData, draft);
      const payload = await requestJson<JobsAcceptedEnvelope>("/v1/transcription-jobs", {
        method: "POST",
        body: formData,
      });
      return extractJobs(payload);
    },

    async createCombinedUpload(draft) {
      const formData = new FormData();
      appendUploadDraft(formData, draft);
      const payload = await requestJson<JobAcceptedEnvelope>("/v1/transcription-jobs/combined", {
        method: "POST",
        body: formData,
      });
      return extractJobSnapshot(payload);
    },

    async createFromUrl(draft) {
      const payload = await postJson<JobAcceptedEnvelope>("/v1/transcription-jobs/from-url", {
        source_kind: "youtube_url",
        url: draft.url.trim(),
        display_name: draft.displayName.trim() || undefined,
        client_ref: draft.clientRef.trim() || undefined,
        delivery: {
          strategy: draft.delivery.strategy,
          webhook:
            draft.delivery.strategy === "webhook" && draft.delivery.webhookUrl.trim() !== ""
              ? { url: draft.delivery.webhookUrl.trim() }
              : undefined,
        },
      });
      return extractJobSnapshot(payload);
    },

    async createReport(jobId, draft) {
      const payload = await postJson<JobAcceptedEnvelope>(
        `/v1/transcription-jobs/${jobId}/report-jobs`,
        toChildPayload(draft),
      );
      return extractJobSnapshot(payload);
    },

    async createDeepResearch(jobId, draft) {
      const payload = await postJson<JobAcceptedEnvelope>(
        `/v1/report-jobs/${jobId}/deep-research-jobs`,
        toChildPayload(draft),
      );
      return extractJobSnapshot(payload);
    },

    async cancelJob(jobId) {
      const payload = await postJson<JobAcceptedEnvelope>(`/v1/jobs/${jobId}/cancel`);
      return extractJobSnapshot(payload);
    },

    async retryJob(jobId) {
      const payload = await postJson<JobAcceptedEnvelope>(`/v1/jobs/${jobId}/retry`);
      return extractJobSnapshot(payload);
    },

    resolveArtifact(artifactId) {
      return requestJson<ArtifactResolution>(`/v1/artifacts/${artifactId}`);
    },

    async listJobEvents(jobId) {
      const payload = await requestJson<JobEventListResponse>(`/v1/jobs/${jobId}/events`);
      return payload.items;
    },

    subscribeToJobEvents(options) {
      const socket = webSocketFactory(wsUrl);

      socket.onopen = (event) => {
        options.onOpen?.();
      };
      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data) as unknown;
          if (isJobEventStreamEnvelope(payload)) {
            options.onMessage(payload);
          }
        } catch (error) {
          options.onError?.(error as Error);
        }
      };
      socket.onerror = (event) => {
        options.onError?.(event);
      };
      socket.onclose = () => {
        options.onClose?.();
      };

      return {
        close() {
          socket.close();
        },
      };
    },
  };
}
// END_BLOCK_BLOCK_CREATE_WEB_UI_API_CLIENT
