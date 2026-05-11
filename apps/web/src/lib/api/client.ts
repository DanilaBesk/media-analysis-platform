import type {
  AddMediaItemDraft,
  AnalysisRun,
  AnalysisRunSummary,
  Artifact,
  ArtifactSummary,
  Collection,
  CollectionDraft,
  Diagnostic,
  DiagnosticSeverity,
  DiagnosticSubjectType,
  MediaItem,
  MediaItemSummary,
  ObservabilitySnapshot,
  OwnerScope,
  PaginatedResponse,
  ReconcileQueueResponse,
  ReplaceCollectionItemsDraft,
  RunDraft,
  RunEvent,
  RunType,
  Selection,
  SelectionDraft,
  UpdateCollectionDraft,
} from "./types";

export const RECONCILE_STATE_MARKER = "[WebUi][reconcileRunState]";

export interface RunEventSubscription {
  close(): void;
}

export interface SubscribeToRunEventsOptions {
  onMessage: (event: RunEvent) => void;
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
  listMediaItems(owner: OwnerScope, filter?: ListMediaItemsFilter): Promise<PaginatedResponse<MediaItemSummary>>;
  getMediaItem(owner: OwnerScope, mediaItemId: string): Promise<MediaItem>;
  addMediaItem(owner: OwnerScope, draft: AddMediaItemDraft, collectionId?: string): Promise<MediaItem>;
  removeMediaItem(owner: OwnerScope, mediaItemId: string): Promise<MediaItem>;
  getInboxCollection(owner: OwnerScope): Promise<Collection>;
  listCollections(owner: OwnerScope, page?: PageRequest): Promise<PaginatedResponse<Collection>>;
  getCollection(owner: OwnerScope, collectionId: string, page?: PageRequest): Promise<Collection>;
  createCollection(owner: OwnerScope, draft: CollectionDraft): Promise<Collection>;
  updateCollection(owner: OwnerScope, collectionId: string, draft: UpdateCollectionDraft): Promise<Collection>;
  replaceCollectionItems(owner: OwnerScope, collectionId: string, draft: ReplaceCollectionItemsDraft): Promise<Collection>;
  removeCollectionItem(
    owner: OwnerScope,
    collectionId: string,
    mediaItemId: string,
    expectedVersion: number,
  ): Promise<Collection>;
  createSelection(owner: OwnerScope, draft: SelectionDraft): Promise<Selection>;
  getSelection(owner: OwnerScope, selectionId: string): Promise<Selection>;
  createAnalysisRun(owner: OwnerScope, draft: RunDraft): Promise<AnalysisRun>;
  listAnalysisRuns(owner: OwnerScope, filter?: ListAnalysisRunsFilter): Promise<PaginatedResponse<AnalysisRunSummary>>;
  getAnalysisRun(owner: OwnerScope, analysisRunId: string): Promise<AnalysisRun>;
  cancelAnalysisRun(owner: OwnerScope, analysisRunId: string): Promise<AnalysisRun>;
  retryAnalysisRun(owner: OwnerScope, analysisRunId: string): Promise<AnalysisRun>;
  listAnalysisRunEvents(owner: OwnerScope, analysisRunId: string, page?: PageRequest): Promise<PaginatedResponse<RunEvent>>;
  listArtifacts(owner: OwnerScope, filter?: ListArtifactsFilter): Promise<PaginatedResponse<ArtifactSummary>>;
  getArtifact(owner: OwnerScope, artifactId: string): Promise<Artifact>;
  refreshArtifact(owner: OwnerScope, artifactId: string): Promise<Artifact>;
  listDiagnostics(owner: OwnerScope, filter?: ListDiagnosticsFilter): Promise<PaginatedResponse<Diagnostic>>;
  reconcileAnalysisRunQueue(limit?: number): Promise<ReconcileQueueResponse>;
  getObservabilitySnapshot(): Promise<ObservabilitySnapshot>;
  subscribeToRunEvents(options: SubscribeToRunEventsOptions): RunEventSubscription;
}

export interface PageRequest {
  cursor?: string;
  pageSize?: number;
}

export interface ListMediaItemsFilter extends PageRequest {
  query?: string;
  kind?: string;
  status?: string;
}

export interface ListAnalysisRunsFilter extends PageRequest {
  status?: string;
  runType?: RunType | "";
}

export interface ListArtifactsFilter extends PageRequest {
  analysisRunId?: string;
}

export interface ListDiagnosticsFilter extends PageRequest {
  subjectType?: DiagnosticSubjectType | "";
  subjectId?: string;
  severity?: DiagnosticSeverity | "";
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
  return new URL(path.replace(/^\/+/, ""), normalizedBaseUrl);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function extractEnvelope<TValue>(payload: unknown, key: string): TValue {
  if (!isRecord(payload) || !isRecord(payload[key])) {
    throw new Error(`API response does not include ${key}`);
  }
  return payload[key] as TValue;
}

function extractError(payload: unknown): { code?: string; message?: string } {
  if (!isRecord(payload) || !isRecord(payload.error)) {
    return {};
  }
  const error: { code?: string; message?: string } = {};
  if (typeof payload.error.code === "string") {
    error.code = payload.error.code;
  }
  if (typeof payload.error.message === "string") {
    error.message = payload.error.message;
  }
  return error;
}

function appendOwner(search: URLSearchParams, owner: OwnerScope): void {
  search.set("owner_type", owner.owner_type);
  search.set("owner_id", owner.owner_id);
  if (owner.tenant_id?.trim()) {
    search.set("tenant_id", owner.tenant_id.trim());
  }
}

function pageSearch(owner: OwnerScope, page?: PageRequest): URLSearchParams {
  const search = new URLSearchParams();
  appendOwner(search, owner);
  if (page?.cursor?.trim()) {
    search.set("cursor", page.cursor.trim());
  }
  if (page?.pageSize) {
    search.set("page_size", String(page.pageSize));
  }
  return search;
}

function ownerBody(owner: OwnerScope): OwnerScope {
  return {
    owner_type: owner.owner_type,
    owner_id: owner.owner_id,
    ...(owner.tenant_id?.trim() ? { tenant_id: owner.tenant_id.trim() } : {}),
    ...(owner.adapter_identity ? { adapter_identity: owner.adapter_identity } : {}),
  };
}

function isRunEvent(payload: unknown): payload is RunEvent {
  return (
    isRecord(payload) &&
    typeof payload.event_id === "string" &&
    typeof payload.analysis_run_id === "string" &&
    typeof payload.event_type === "string" &&
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

  const sendJson = <TResponse>(
    method: "POST" | "PATCH",
    path: string,
    payload: unknown,
  ): Promise<TResponse> =>
    requestJson<TResponse>(path, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

  const postJson = <TResponse>(path: string, payload: unknown): Promise<TResponse> =>
    sendJson<TResponse>("POST", path, payload);

  const patchJson = <TResponse>(path: string, payload: unknown): Promise<TResponse> =>
    sendJson<TResponse>("PATCH", path, payload);

  return {
    listMediaItems(owner, filter = {}) {
      const search = pageSearch(owner, filter);
      if (filter.query?.trim()) {
        search.set("query", filter.query.trim());
      }
      if (filter.kind) {
        search.set("kind", filter.kind);
      }
      if (filter.status) {
        search.set("status", filter.status);
      }
      return requestJson<PaginatedResponse<MediaItemSummary>>(`/v1/media-items?${search.toString()}`);
    },

    async getMediaItem(owner, mediaItemId) {
      const search = pageSearch(owner);
      const payload = await requestJson<unknown>(`/v1/media-items/${mediaItemId}?${search.toString()}`);
      return extractEnvelope<MediaItem>(payload, "media_item");
    },

    async addMediaItem(owner, draft, collectionId) {
      const payload = await postJson<unknown>("/v1/media-items", {
        owner: ownerBody(owner),
        kind: draft.kind,
        source: draft.source,
        collection_id: collectionId || undefined,
        display_name: draft.displayName.trim() || undefined,
        adapter_origin: draft.adapterOrigin.trim() || "web",
      });
      return extractEnvelope<MediaItem>(payload, "media_item");
    },

    async removeMediaItem(owner, mediaItemId) {
      const search = pageSearch(owner);
      const payload = await requestJson<unknown>(`/v1/media-items/${mediaItemId}?${search.toString()}`, {
        method: "DELETE",
      });
      return extractEnvelope<MediaItem>(payload, "media_item");
    },

    async getInboxCollection(owner) {
      const search = pageSearch(owner);
      const payload = await requestJson<unknown>(`/v1/collections/inbox?${search.toString()}`);
      return extractEnvelope<Collection>(payload, "collection");
    },

    listCollections(owner, page) {
      const search = pageSearch(owner, page);
      return requestJson<PaginatedResponse<Collection>>(`/v1/collections?${search.toString()}`);
    },

    async getCollection(owner, collectionId, page) {
      const search = pageSearch(owner, page);
      const payload = await requestJson<unknown>(`/v1/collections/${collectionId}?${search.toString()}`);
      return extractEnvelope<Collection>(payload, "collection");
    },

    async createCollection(owner, draft) {
      const payload = await postJson<unknown>("/v1/collections", {
        owner: ownerBody(owner),
        name: draft.name.trim(),
        items: draft.items,
      });
      return extractEnvelope<Collection>(payload, "collection");
    },

    async updateCollection(owner, collectionId, draft) {
      const payload = await patchJson<unknown>(`/v1/collections/${collectionId}`, {
        owner: ownerBody(owner),
        expected_version: draft.expectedVersion,
        name: draft.name?.trim() || undefined,
        status: draft.status || undefined,
      });
      return extractEnvelope<Collection>(payload, "collection");
    },

    async replaceCollectionItems(owner, collectionId, draft) {
      const payload = await postJson<unknown>(`/v1/collections/${collectionId}/items`, {
        owner: ownerBody(owner),
        expected_version: draft.expectedVersion,
        items: draft.items,
      });
      return extractEnvelope<Collection>(payload, "collection");
    },

    async removeCollectionItem(owner, collectionId, mediaItemId, expectedVersion) {
      const search = pageSearch(owner);
      search.set("expected_version", String(expectedVersion));
      const payload = await requestJson<unknown>(
        `/v1/collections/${collectionId}/items/${mediaItemId}?${search.toString()}`,
        { method: "DELETE" },
      );
      return extractEnvelope<Collection>(payload, "collection");
    },

    async createSelection(owner, draft) {
      const payload = await postJson<unknown>("/v1/selections", {
        owner: ownerBody(owner),
        source_collection_id: draft.sourceCollectionId || undefined,
        items: draft.items,
        option_snapshot: draft.optionSnapshot,
        duplicate_policy: draft.duplicatePolicy,
        created_by: draft.createdBy?.trim() || "web",
      });
      return extractEnvelope<Selection>(payload, "selection");
    },

    async getSelection(owner, selectionId) {
      const search = pageSearch(owner);
      const payload = await requestJson<unknown>(`/v1/selections/${selectionId}?${search.toString()}`);
      return extractEnvelope<Selection>(payload, "selection");
    },

    async createAnalysisRun(owner, draft) {
      const payload = await postJson<unknown>("/v1/analysis-runs", {
        owner: ownerBody(owner),
        selection_id: draft.selectionId,
        run_type: draft.runType,
        params: draft.params,
        delivery: draft.delivery,
      });
      return extractEnvelope<AnalysisRun>(payload, "analysis_run");
    },

    listAnalysisRuns(owner, filter = {}) {
      const search = pageSearch(owner, filter);
      if (filter.status) {
        search.set("status", filter.status);
      }
      if (filter.runType) {
        search.set("run_type", filter.runType);
      }
      return requestJson<PaginatedResponse<AnalysisRunSummary>>(`/v1/analysis-runs?${search.toString()}`);
    },

    async getAnalysisRun(owner, analysisRunId) {
      const search = pageSearch(owner);
      const payload = await requestJson<unknown>(`/v1/analysis-runs/${analysisRunId}?${search.toString()}`);
      return extractEnvelope<AnalysisRun>(payload, "analysis_run");
    },

    async cancelAnalysisRun(owner, analysisRunId) {
      const search = pageSearch(owner);
      const payload = await postJson<unknown>(`/v1/analysis-runs/${analysisRunId}/cancel?${search.toString()}`, {});
      return extractEnvelope<AnalysisRun>(payload, "analysis_run");
    },

    async retryAnalysisRun(owner, analysisRunId) {
      const search = pageSearch(owner);
      const payload = await postJson<unknown>(`/v1/analysis-runs/${analysisRunId}/retry?${search.toString()}`, {
        owner: ownerBody(owner),
      });
      return extractEnvelope<AnalysisRun>(payload, "analysis_run");
    },

    listAnalysisRunEvents(owner, analysisRunId, page) {
      const search = pageSearch(owner, page);
      return requestJson<PaginatedResponse<RunEvent>>(
        `/v1/analysis-runs/${analysisRunId}/events?${search.toString()}`,
      );
    },

    listArtifacts(owner, filter = {}) {
      const search = pageSearch(owner, filter);
      if (filter.analysisRunId?.trim()) {
        search.set("analysis_run_id", filter.analysisRunId.trim());
      }
      return requestJson<PaginatedResponse<ArtifactSummary>>(`/v1/artifacts?${search.toString()}`);
    },

    async getArtifact(owner, artifactId) {
      const search = pageSearch(owner);
      const payload = await requestJson<unknown>(`/v1/artifacts/${artifactId}?${search.toString()}`);
      return extractEnvelope<Artifact>(payload, "artifact");
    },

    async refreshArtifact(owner, artifactId) {
      const search = pageSearch(owner);
      const payload = await postJson<unknown>(`/v1/artifacts/${artifactId}/refresh?${search.toString()}`, {});
      return extractEnvelope<Artifact>(payload, "artifact");
    },

    listDiagnostics(owner, filter = {}) {
      const search = pageSearch(owner, filter);
      if (filter.subjectType) {
        search.set("subject_type", filter.subjectType);
      }
      if (filter.subjectId?.trim()) {
        search.set("subject_id", filter.subjectId.trim());
      }
      if (filter.severity) {
        search.set("severity", filter.severity);
      }
      return requestJson<PaginatedResponse<Diagnostic>>(`/v1/diagnostics?${search.toString()}`);
    },

    reconcileAnalysisRunQueue(limit = 100) {
      return postJson<ReconcileQueueResponse>("/v1/admin/reconcile-queue", { limit });
    },

    async getObservabilitySnapshot() {
      const payload = await requestJson<unknown>("/v1/admin/observability");
      return extractEnvelope<ObservabilitySnapshot>(payload, "observability");
    },

    subscribeToRunEvents(options) {
      const socket = webSocketFactory(wsUrl);

      socket.onopen = (event) => {
        options.onOpen?.();
      };
      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data) as unknown;
          if (isRunEvent(payload)) {
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
