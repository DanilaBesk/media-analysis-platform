import type {
  AddMediaItemDraft,
  AddMediaAssetDraft,
  AnalysisRun,
  AnalysisRunSummary,
  Artifact,
  ArtifactSummary,
  ChannelAccountId,
  Collection,
  CollectionDraft,
  Diagnostic,
  DiagnosticSeverity,
  DiagnosticSubjectType,
  MediaAsset,
  MediaAssetSummary,
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
  SelectionSnapshot,
  SelectionSnapshotDraft,
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
  listMediaAssets(channelAccountId: ChannelAccountId, filter?: ListMediaItemsFilter): Promise<PaginatedResponse<MediaAssetSummary>>;
  getMediaAsset(channelAccountId: ChannelAccountId, mediaAssetId: string): Promise<MediaAsset>;
  addMediaAsset(channelAccountId: ChannelAccountId, draft: AddMediaAssetDraft, collectionId?: string): Promise<MediaAsset>;
  removeMediaAsset(channelAccountId: ChannelAccountId, mediaAssetId: string): Promise<MediaAsset>;
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
  createSelectionSnapshot(channelAccountId: ChannelAccountId, draft: SelectionSnapshotDraft): Promise<SelectionSnapshot>;
  getSelectionSnapshot(channelAccountId: ChannelAccountId, selectionSnapshotId: string): Promise<SelectionSnapshot>;
  createSelection(owner: OwnerScope, draft: SelectionDraft): Promise<Selection>;
  getSelection(owner: OwnerScope, selectionId: string): Promise<Selection>;
  createAnalysisRun(owner: OwnerScope | ChannelAccountId, draft: RunDraft): Promise<AnalysisRun>;
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

function extractAnyEnvelope<TValue>(payload: unknown, keys: string[]): TValue {
  if (isRecord(payload)) {
    for (const key of keys) {
      if (isRecord(payload[key])) {
        return payload[key] as TValue;
      }
    }
  }
  throw new Error(`API response does not include ${keys[0]}`);
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

function channelAccountId(scope: OwnerScope | ChannelAccountId): ChannelAccountId {
  return typeof scope === "string" ? scope : scope.owner_id;
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

function mediaSourceFromAsset(asset: MediaAssetSummary): MediaItemSummary["source"] {
  const legacy = asset as MediaAssetSummary & { media_item_id?: string; source?: MediaItemSummary["source"] };
  if (legacy.source) {
    return legacy.source;
  }
  const origin = asset.origin ?? { origin_type: "text" };
  const assetID = asset.media_asset_id ?? legacy.media_item_id ?? "";
  return {
    source_id: origin.stored_object_id ?? origin.origin_ref ?? origin.object_ref ?? assetID,
    origin_type: origin.origin_type === "upload" || origin.origin_type === "telegram_file" ? "object" : (origin.origin_type as never),
    external_uri: origin.url ?? origin.origin_ref ?? null,
    object_key: origin.object_ref ?? null,
    text_ref: origin.origin_type === "text" ? origin.origin_ref ?? assetID : null,
    checksum: origin.checksum ?? null,
    size_bytes: origin.size_bytes ?? null,
    mime_type: origin.content_type ?? null,
  };
}

function mediaAssetToMediaItem<TAsset extends MediaAssetSummary>(asset: TAsset): MediaItemSummary & Partial<MediaItem> {
  const diagnostics = (asset as MediaAsset).diagnostics;
  const legacy = asset as TAsset & { media_item_id?: string; owner?: OwnerScope; retention?: MediaItemSummary["retention"] };
  const assetID = asset.media_asset_id ?? legacy.media_item_id ?? "";
  return {
    media_item_id: assetID,
    owner: legacy.owner ?? { owner_type: "web", owner_id: asset.channel_account_id },
    kind: asset.kind,
    status: asset.status,
    display_name: asset.display_name,
    source: mediaSourceFromAsset(asset),
    diagnostics_count: asset.diagnostics_count ?? diagnostics?.length ?? 0,
    retention: legacy.retention ?? { state: asset.deleted_at ? "soft_deleted" : "active", deleted_at: asset.deleted_at },
    diagnostics,
    created_at: asset.created_at,
    updated_at: asset.updated_at,
    deleted_at: asset.deleted_at,
  };
}

function normalizeCollection(collection: Collection): Collection {
  return {
    ...collection,
    owner: collection.owner ?? { owner_type: "web", owner_id: collection.channel_account_id ?? "" },
    items: (collection.items ?? []).map((item) => ({
      ...item,
      media_item_id: item.media_item_id ?? item.media_asset_id ?? "",
      media_item: item.media_item ?? (item.media_asset ? mediaAssetToMediaItem(item.media_asset) as MediaItem : undefined),
    })),
  };
}

function selectionFromSnapshot(snapshot: SelectionSnapshot): Selection {
  const legacy = snapshot as SelectionSnapshot & Selection;
  if (!snapshot.selection_snapshot_id && legacy.selection_id) {
    return legacy;
  }
  return {
    selection_id: snapshot.selection_snapshot_id,
    selection_snapshot_id: snapshot.selection_snapshot_id,
    owner: { owner_type: "web", owner_id: snapshot.channel_account_id },
    channel_account_id: snapshot.channel_account_id,
    status: snapshot.status,
    source_collection_id: snapshot.source_collection_id,
    items: snapshot.items.map((item) => ({
      selection_snapshot_item_id: item.selection_snapshot_item_id,
      position: item.position,
      media_item_id: item.media_asset_id,
      media_asset_id: item.media_asset_id,
      kind: item.kind,
      display_name: item.display_name,
      source_snapshot: {
        source_id: item.origin_snapshot.stored_object_id ?? item.origin_snapshot.object_ref ?? item.media_asset_id,
        origin_type:
          item.origin_snapshot.origin_type === "upload" || item.origin_snapshot.origin_type === "telegram_file"
            ? "object"
            : (item.origin_snapshot.origin_type as never),
        external_uri: item.origin_snapshot.url ?? item.origin_snapshot.origin_ref ?? null,
        object_key: item.origin_snapshot.object_ref ?? null,
        text_ref: item.origin_snapshot.origin_type === "text" ? item.media_asset_id : null,
        size_bytes: item.origin_snapshot.size_bytes ?? null,
        mime_type: item.origin_snapshot.content_type ?? null,
        checksum: item.origin_snapshot.checksum ?? null,
      },
      origin_snapshot: item.origin_snapshot,
      storage_snapshot: item.storage_snapshot,
      metadata_snapshot: item.metadata_snapshot,
      status_at_selection: item.status_at_selection,
      retention_snapshot: { state: "active" },
      diagnostics: item.diagnostics,
    })),
    option_snapshot: snapshot.option_snapshot,
    diagnostics: snapshot.diagnostics,
    created_by: snapshot.channel_account_id,
    created_at: snapshot.created_at,
    sealed_at: snapshot.sealed_at,
  };
}

function normalizeAnalysisRun(run: AnalysisRun): AnalysisRun {
  return {
    ...run,
    owner: run.owner ?? { owner_type: "web", owner_id: run.channel_account_id ?? "" },
    selection_id: run.selection_id ?? run.selection_snapshot_id ?? "",
    selection_snapshot_id: run.selection_snapshot_id ?? run.selection_id,
    selection:
      run.selection ??
      ({
        selection_id: run.selection_snapshot_id ?? run.selection_id ?? "",
        selection_snapshot_id: run.selection_snapshot_id ?? run.selection_id ?? "",
        owner: { owner_type: "web", owner_id: run.channel_account_id ?? "" },
        channel_account_id: run.channel_account_id,
        status: "sealed",
        items: [],
        created_by: run.channel_account_id ?? "web",
        created_at: run.created_at,
        sealed_at: run.created_at,
      } satisfies Selection),
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
      return extractAnyEnvelope<MediaAsset>(payload, ["media_asset", "media_item"]);
    },

    async addMediaAsset(channelAccountID, draft, collectionId) {
      const payload = await postJson<unknown>("/v1/media-assets", {
        channel_account_id: channelAccountID,
        kind: draft.kind,
        origin: draft.origin,
        collection_id: collectionId || undefined,
        display_name: draft.displayName.trim() || undefined,
      });
      return extractAnyEnvelope<MediaAsset>(payload, ["media_asset", "media_item"]);
    },

    async removeMediaAsset(channelAccountID, mediaAssetId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/media-assets/${mediaAssetId}?${search.toString()}`, {
        method: "DELETE",
      });
      return extractAnyEnvelope<MediaAsset>(payload, ["media_asset", "media_item"]);
    },

    listMediaItems(owner, filter = {}) {
      return this.listMediaAssets(channelAccountId(owner), filter).then((response) => ({
        ...response,
        items: response.items.map((item) => mediaAssetToMediaItem(item)),
      }));
    },

    async getMediaItem(owner, mediaItemId) {
      return mediaAssetToMediaItem(await this.getMediaAsset(channelAccountId(owner), mediaItemId)) as MediaItem;
    },

    async addMediaItem(owner, draft, collectionId) {
      const origin =
        draft.source.origin_type === "text"
          ? { origin_type: "text" as const, text: draft.source.text, language_hint: draft.source.language_hint }
          : draft.source.origin_type === "url"
            ? { origin_type: "url" as const, url: draft.source.url }
            : {
                origin_type: "upload" as const,
                object_ref: draft.source.object_ref,
                original_filename: draft.source.original_filename,
                content_type: draft.source.content_type,
                size_bytes: draft.source.size_bytes,
              };
      const asset = await this.addMediaAsset(channelAccountId(owner), {
        kind: draft.kind,
        displayName: draft.displayName,
        origin,
      }, collectionId);
      return mediaAssetToMediaItem(asset) as MediaItem;
    },

    async removeMediaItem(owner, mediaItemId) {
      return mediaAssetToMediaItem(await this.removeMediaAsset(channelAccountId(owner), mediaItemId)) as MediaItem;
    },

    async getInboxCollection(owner) {
      const search = channelSearch(channelAccountId(owner));
      const payload = await requestJson<unknown>(`/v1/collections/inbox?${search.toString()}`);
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    listCollections(owner, page) {
      const search = channelSearch(channelAccountId(owner), page);
      return requestJson<PaginatedResponse<Collection>>(`/v1/collections?${search.toString()}`).then((response) => ({
        ...response,
        items: response.items.map(normalizeCollection),
      }));
    },

    async getCollection(owner, collectionId, page) {
      const search = channelSearch(channelAccountId(owner), page);
      const payload = await requestJson<unknown>(`/v1/collections/${collectionId}?${search.toString()}`);
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async createCollection(owner, draft) {
      const payload = await postJson<unknown>("/v1/collections", {
        channel_account_id: channelAccountId(owner),
        name: draft.name.trim(),
        items: draft.items,
      });
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async updateCollection(owner, collectionId, draft) {
      const payload = await patchJson<unknown>(`/v1/collections/${collectionId}`, {
        channel_account_id: channelAccountId(owner),
        expected_version: draft.expectedVersion,
        name: draft.name?.trim() || undefined,
        status: draft.status || undefined,
      });
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async replaceCollectionItems(owner, collectionId, draft) {
      const payload = await postJson<unknown>(`/v1/collections/${collectionId}/items`, {
        channel_account_id: channelAccountId(owner),
        expected_version: draft.expectedVersion,
        items: draft.items.map((item) => ({
          media_asset_id: item.media_asset_id ?? item.media_item_id,
          position: item.position,
        })),
      });
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async removeCollectionItem(owner, collectionId, mediaItemId, expectedVersion) {
      const search = channelSearch(channelAccountId(owner));
      search.set("expected_version", String(expectedVersion));
      const payload = await requestJson<unknown>(
        `/v1/collections/${collectionId}/items/${mediaItemId}?${search.toString()}`,
        { method: "DELETE" },
      );
      return normalizeCollection(extractEnvelope<Collection>(payload, "collection"));
    },

    async createSelectionSnapshot(channelAccountID, draft) {
      const payload = await postJson<unknown>("/v1/selection-snapshots", {
        channel_account_id: channelAccountID,
        source_collection_id: draft.sourceCollectionId || undefined,
        items: draft.items,
        option_snapshot: draft.optionSnapshot,
        created_via_channel_account_id: channelAccountID,
      });
      return extractAnyEnvelope<SelectionSnapshot>(payload, ["selection_snapshot", "selection"]);
    },

    async getSelectionSnapshot(channelAccountID, selectionSnapshotId) {
      const search = channelSearch(channelAccountID);
      const payload = await requestJson<unknown>(`/v1/selection-snapshots/${selectionSnapshotId}?${search.toString()}`);
      return extractAnyEnvelope<SelectionSnapshot>(payload, ["selection_snapshot", "selection"]);
    },

    async createSelection(owner, draft) {
      const snapshot = await this.createSelectionSnapshot(channelAccountId(owner), {
        sourceCollectionId: draft.sourceCollectionId,
        items: draft.items.map((item) => ({ media_asset_id: item.media_item_id, position: item.position })),
        optionSnapshot: draft.optionSnapshot,
      });
      return selectionFromSnapshot(snapshot);
    },

    async getSelection(owner, selectionId) {
      return selectionFromSnapshot(await this.getSelectionSnapshot(channelAccountId(owner), selectionId));
    },

    async createAnalysisRun(owner, draft) {
      const channelID = channelAccountId(owner);
      const payload = await postJson<unknown>("/v1/analysis-runs", {
        channel_account_id: channelID,
        selection_snapshot_id: draft.selectionSnapshotId ?? draft.selectionId,
        run_type: draft.runType,
        params: draft.params,
        delivery: draft.delivery,
        created_via_channel_id: channelID,
      });
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    listAnalysisRuns(owner, filter = {}) {
      const search = channelSearch(channelAccountId(owner), filter);
      if (filter.status) {
        search.set("status", filter.status);
      }
      if (filter.runType) {
        search.set("run_type", filter.runType);
      }
      return requestJson<PaginatedResponse<AnalysisRunSummary>>(`/v1/analysis-runs?${search.toString()}`);
    },

    async getAnalysisRun(owner, analysisRunId) {
      const search = channelSearch(channelAccountId(owner));
      const payload = await requestJson<unknown>(`/v1/analysis-runs/${analysisRunId}?${search.toString()}`);
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    async cancelAnalysisRun(owner, analysisRunId) {
      const payload = await postJson<unknown>(`/v1/analysis-runs/${analysisRunId}/cancel`, {
        channel_account_id: channelAccountId(owner),
      });
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    async retryAnalysisRun(owner, analysisRunId) {
      const payload = await postJson<unknown>(`/v1/analysis-runs/${analysisRunId}/retry`, {
        channel_account_id: channelAccountId(owner),
      });
      return normalizeAnalysisRun(extractEnvelope<AnalysisRun>(payload, "analysis_run"));
    },

    listAnalysisRunEvents(owner, analysisRunId, page) {
      const search = channelSearch(channelAccountId(owner), page);
      return requestJson<PaginatedResponse<RunEvent>>(
        `/v1/analysis-runs/${analysisRunId}/events?${search.toString()}`,
      );
    },

    listArtifacts(owner, filter = {}) {
      const search = channelSearch(channelAccountId(owner), filter);
      if (filter.analysisRunId?.trim()) {
        search.set("analysis_run_id", filter.analysisRunId.trim());
      }
      return requestJson<PaginatedResponse<ArtifactSummary>>(`/v1/artifacts?${search.toString()}`);
    },

    async getArtifact(owner, artifactId) {
      const search = channelSearch(channelAccountId(owner));
      const payload = await requestJson<unknown>(`/v1/artifacts/${artifactId}?${search.toString()}`);
      return extractEnvelope<Artifact>(payload, "artifact");
    },

    async refreshArtifact(owner, artifactId) {
      const search = channelSearch(channelAccountId(owner));
      const payload = await postJson<unknown>(`/v1/artifacts/${artifactId}/refresh?${search.toString()}`, {});
      return extractEnvelope<Artifact>(payload, "artifact");
    },

    listDiagnostics(owner, filter = {}) {
      const search = channelSearch(channelAccountId(owner), filter);
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
