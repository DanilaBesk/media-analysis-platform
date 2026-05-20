import type {
  AddMediaAssetInput,
  AnalysisRunInput,
  AnalysisRun,
  AnalysisRunSummary,
  Artifact,
  ArtifactSummary,
  ChannelAccountId,
  Collection,
  CollectionInput,
  Diagnostic,
  DiagnosticSeverity,
  DiagnosticSubjectType,
  MediaAsset,
  MediaAssetSummary,
  ObservabilitySnapshot,
  PaginatedResponse,
  ReplaceCollectionItemsInput,
  RunEvent,
  RunType,
  SelectionSnapshot,
  SelectionSnapshotInput,
  UpdateCollectionInput,
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
  listMediaAssets(channelAccountId: ChannelAccountId, filter?: ListMediaAssetsFilter): Promise<PaginatedResponse<MediaAssetSummary>>;
  getMediaAsset(channelAccountId: ChannelAccountId, mediaAssetId: string): Promise<MediaAsset>;
  addMediaAsset(channelAccountId: ChannelAccountId, input: AddMediaAssetInput, collectionId?: string): Promise<MediaAsset>;
  removeMediaAsset(channelAccountId: ChannelAccountId, mediaAssetId: string): Promise<MediaAsset>;
  getInboxCollection(channelAccountId: ChannelAccountId): Promise<Collection>;
  listCollections(channelAccountId: ChannelAccountId, page?: PageRequest): Promise<PaginatedResponse<Collection>>;
  getCollection(channelAccountId: ChannelAccountId, collectionId: string, page?: PageRequest): Promise<Collection>;
  createCollection(channelAccountId: ChannelAccountId, input: CollectionInput): Promise<Collection>;
  updateCollection(channelAccountId: ChannelAccountId, collectionId: string, input: UpdateCollectionInput): Promise<Collection>;
  replaceCollectionItems(channelAccountId: ChannelAccountId, collectionId: string, input: ReplaceCollectionItemsInput): Promise<Collection>;
  removeCollectionItem(
    channelAccountId: ChannelAccountId,
    collectionId: string,
    mediaAssetId: string,
    expectedVersion: number,
  ): Promise<Collection>;
  createSelectionSnapshot(channelAccountId: ChannelAccountId, input: SelectionSnapshotInput): Promise<SelectionSnapshot>;
  getSelectionSnapshot(channelAccountId: ChannelAccountId, selectionSnapshotId: string): Promise<SelectionSnapshot>;
  createAnalysisRun(channelAccountId: ChannelAccountId, input: AnalysisRunInput): Promise<AnalysisRun>;
  listAnalysisRuns(channelAccountId: ChannelAccountId, filter?: ListAnalysisRunsFilter): Promise<PaginatedResponse<AnalysisRunSummary>>;
  getAnalysisRun(channelAccountId: ChannelAccountId, analysisRunId: string): Promise<AnalysisRun>;
  cancelAnalysisRun(channelAccountId: ChannelAccountId, analysisRunId: string): Promise<AnalysisRun>;
  retryAnalysisRun(channelAccountId: ChannelAccountId, analysisRunId: string): Promise<AnalysisRun>;
  listAnalysisRunEvents(channelAccountId: ChannelAccountId, analysisRunId: string, page?: PageRequest): Promise<PaginatedResponse<RunEvent>>;
  listArtifacts(channelAccountId: ChannelAccountId, filter?: ListArtifactsFilter): Promise<PaginatedResponse<ArtifactSummary>>;
  getArtifact(channelAccountId: ChannelAccountId, artifactId: string): Promise<Artifact>;
  refreshArtifact(channelAccountId: ChannelAccountId, artifactId: string): Promise<Artifact>;
  listDiagnostics(channelAccountId: ChannelAccountId, filter?: ListDiagnosticsFilter): Promise<PaginatedResponse<Diagnostic>>;
  getObservabilitySnapshot(): Promise<ObservabilitySnapshot>;
  subscribeToRunEvents(options: SubscribeToRunEventsOptions): RunEventSubscription;
}

export interface PageRequest {
  cursor?: string;
  pageSize?: number;
}

export interface ListMediaAssetsFilter extends PageRequest {
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

function channelSearch(channelAccountID: ChannelAccountId, page?: PageRequest): URLSearchParams {
  const search = new URLSearchParams();
  search.set("channel_account_id", channelAccountID);
  if (page?.cursor?.trim()) {
    search.set("cursor", page.cursor.trim());
  }
  if (page?.pageSize) {
    search.set("page_size", String(page.pageSize));
  }
  return search;
}

function normalizeCollection(collection: Collection): Collection {
  return {
    ...collection,
    items: collection.items ?? [],
  };
}

function emptySelectionSnapshot(run: AnalysisRun): SelectionSnapshot {
  return {
    selection_snapshot_id: run.selection_snapshot_id,
    channel_account_id: run.channel_account_id,
    status: "sealed",
    items: [],
    created_at: run.created_at,
    sealed_at: run.created_at,
  };
}

function normalizeAnalysisRun(run: AnalysisRun): AnalysisRun {
  return {
    ...run,
    selection_snapshot: run.selection_snapshot ?? emptySelectionSnapshot(run),
    artifacts: run.artifacts ?? [],
    diagnostics: run.diagnostics ?? [],
  };
}

function normalizeRunEvent(event: RunEvent): RunEvent {
  return {
    ...event,
    event_id: event.event_id ?? event.analysis_run_event_id,
    emitted_at: event.emitted_at ?? event.created_at,
  };
}

function isRunEvent(payload: unknown): payload is RunEvent {
  if (!isRecord(payload)) {
    return false;
  }
  const hasEventID = typeof payload.event_id === "string" || typeof payload.analysis_run_event_id === "string";
  const hasTimestamp = typeof payload.emitted_at === "string" || typeof payload.created_at === "string";
  return (
    hasEventID &&
    typeof payload.analysis_run_id === "string" &&
    typeof payload.event_type === "string" &&
    typeof payload.version === "number" &&
    hasTimestamp &&
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
    listMediaAssets(channelAccountID, filter = {}) {
      const search = channelSearch(channelAccountID, filter);
      if (filter.query?.trim()) {
        search.set("query", filter.query.trim());
      }
      if (filter.kind) {
        search.set("kind", filter.kind);
      }
      if (filter.status) {
        search.set("status", filter.status);
      }
      return requestJson<PaginatedResponse<MediaAssetSummary>>(`/v1/media-assets?${search.toString()}`);
    },

    async getMediaAsset(channelAccountID, mediaAssetId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/media-assets/${mediaAssetId}?${search.toString()}`);
      return extractEnvelope<MediaAsset>(payload, "media_asset");
    },

    async addMediaAsset(channelAccountID, input, collectionId) {
      const payload = await postJson<unknown>("/v1/media-assets", {
        channel_account_id: channelAccountID,
        kind: input.kind,
        origin: input.origin,
        collection_id: collectionId || undefined,
        display_name: input.displayName.trim() || undefined,
      });
      return extractEnvelope<MediaAsset>(payload, "media_asset");
    },

    async removeMediaAsset(channelAccountID, mediaAssetId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/media-assets/${mediaAssetId}?${search.toString()}`, {
        method: "DELETE",
      });
      return extractEnvelope<MediaAsset>(payload, "media_asset");
    },

    async getInboxCollection(channelAccountID) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/collections/inbox?${search.toString()}`);
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    listCollections(channelAccountID, page) {
      const search = channelSearch(channelAccountID, page);
      return requestJson<PaginatedResponse<Collection>>(`/v1/collections?${search.toString()}`).then((response) => ({
        ...response,
        items: response.items.map(normalizeCollection),
      }));
    },

    async getCollection(channelAccountID, collectionId, page) {
      const search = channelSearch(channelAccountID, page);
      const payload = await requestJson<unknown>(`/v1/collections/${collectionId}?${search.toString()}`);
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async createCollection(channelAccountID, input) {
      const payload = await postJson<unknown>("/v1/collections", {
        channel_account_id: channelAccountID,
        name: input.name.trim(),
        items: input.items,
      });
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async updateCollection(channelAccountID, collectionId, input) {
      const payload = await patchJson<unknown>(`/v1/collections/${collectionId}`, {
        channel_account_id: channelAccountID,
        expected_version: input.expectedVersion,
        name: input.name?.trim() || undefined,
        status: input.status || undefined,
      });
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async replaceCollectionItems(channelAccountID, collectionId, input) {
      const payload = await postJson<unknown>(`/v1/collections/${collectionId}/items`, {
        channel_account_id: channelAccountID,
        expected_version: input.expectedVersion,
        items: input.items,
      });
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async removeCollectionItem(channelAccountID, collectionId, mediaAssetId, expectedVersion) {
      const search = channelSearch(channelAccountID);
      search.set("expected_version", String(expectedVersion));
      const payload = await requestJson<unknown>(
        `/v1/collections/${collectionId}/items/${mediaAssetId}?${search.toString()}`,
        { method: "DELETE" },
      );
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async createSelectionSnapshot(channelAccountID, input) {
      const payload = await postJson<unknown>("/v1/selection-snapshots", {
        channel_account_id: channelAccountID,
        source_collection_id: input.sourceCollectionId || undefined,
        items: input.items,
        option_snapshot: input.optionSnapshot,
        created_via_channel_account_id: channelAccountID,
      });
      return extractEnvelope<SelectionSnapshot>(payload, "selection_snapshot");
    },

    async getSelectionSnapshot(channelAccountID, selectionSnapshotId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/selection-snapshots/${selectionSnapshotId}?${search.toString()}`);
      return extractEnvelope<SelectionSnapshot>(payload, "selection_snapshot");
    },

    async createAnalysisRun(channelAccountID, input) {
      const payload = await postJson<unknown>("/v1/analysis-runs", {
        channel_account_id: channelAccountID,
        selection_snapshot_id: input.selectionSnapshotId,
        run_type: input.runType,
        params: input.params,
        delivery: input.delivery,
        created_via_channel_id: channelAccountID,
      });
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    listAnalysisRuns(channelAccountID, filter = {}) {
      const search = channelSearch(channelAccountID, filter);
      if (filter.status) {
        search.set("status", filter.status);
      }
      if (filter.runType) {
        search.set("run_type", filter.runType);
      }
      return requestJson<PaginatedResponse<AnalysisRunSummary>>(`/v1/analysis-runs?${search.toString()}`);
    },

    async getAnalysisRun(channelAccountID, analysisRunId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/analysis-runs/${analysisRunId}?${search.toString()}`);
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    async cancelAnalysisRun(channelAccountID, analysisRunId) {
      const payload = await postJson<unknown>(`/v1/analysis-runs/${analysisRunId}/cancel`, {
        channel_account_id: channelAccountID,
      });
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    async retryAnalysisRun(channelAccountID, analysisRunId) {
      const payload = await postJson<unknown>(`/v1/analysis-runs/${analysisRunId}/retry`, {
        channel_account_id: channelAccountID,
      });
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    listAnalysisRunEvents(channelAccountID, analysisRunId, page) {
      const search = channelSearch(channelAccountID, page);
      return requestJson<PaginatedResponse<RunEvent>>(
        `/v1/analysis-runs/${analysisRunId}/events?${search.toString()}`,
      ).then((response) => ({ ...response, items: response.items.map(normalizeRunEvent) }));
    },

    listArtifacts(channelAccountID, filter = {}) {
      const search = channelSearch(channelAccountID, filter);
      if (filter.analysisRunId?.trim()) {
        search.set("analysis_run_id", filter.analysisRunId.trim());
      }
      return requestJson<PaginatedResponse<ArtifactSummary>>(`/v1/artifacts?${search.toString()}`);
    },

    async getArtifact(channelAccountID, artifactId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/artifacts/${artifactId}?${search.toString()}`);
      return extractEnvelope<Artifact>(payload, "artifact");
    },

    async refreshArtifact(channelAccountID, artifactId) {
      const search = channelSearch(channelAccountID);
      const payload = await postJson<unknown>(`/v1/artifacts/${artifactId}/refresh?${search.toString()}`, {});
      return extractEnvelope<Artifact>(payload, "artifact");
    },

    listDiagnostics(channelAccountID, filter = {}) {
      const search = channelSearch(channelAccountID, filter);
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

    async getObservabilitySnapshot() {
      const payload = await requestJson<unknown>("/v1/admin/observability");
      return extractEnvelope<ObservabilitySnapshot>(payload, "observability");
    },

    subscribeToRunEvents(options) {
      const socket = webSocketFactory(wsUrl);

      socket.onopen = () => {
        options.onOpen?.();
      };
      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data) as unknown;
          if (isRunEvent(payload)) {
            options.onMessage(normalizeRunEvent(payload));
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
