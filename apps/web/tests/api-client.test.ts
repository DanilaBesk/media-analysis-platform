import { describe, expect, it, vi } from "vitest";

import {
  WebUiApiClientError,
  createWebUiApiClient,
  requiresRestReconciliation,
} from "../src/lib/api/client";
import type { ChannelAccountId } from "../src/lib/api/types";

const channelAccountId: ChannelAccountId = "web-console";
const tenantChannelAccountId: ChannelAccountId = "web-console";

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function textResponse(payload: string, status = 500): Response {
  return new Response(payload, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}

describe("createWebUiApiClient", () => {
  it("uses target media asset and selection snapshot endpoints with channel identity", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          items: [{ media_asset_id: "asset-1", display_name: "Inbox note" }],
          page: { page_size: 25, has_more: false },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          media_asset: {
            media_asset_id: "asset-2",
            channel_account_id: "web-console",
            kind: "text",
            status: "ready",
            display_name: "Note",
            origin: { origin_type: "text", text: "Meeting note" },
            created_at: "2026-05-18T00:00:00Z",
            updated_at: "2026-05-18T00:00:00Z",
          },
        }, 201),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          selection_snapshot: {
            selection_snapshot_id: "snapshot-1",
            channel_account_id: "web-console",
            status: "sealed",
            items: [],
            option_snapshot: {},
            created_at: "2026-05-18T00:00:00Z",
            sealed_at: "2026-05-18T00:00:00Z",
          },
        }, 201),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-1",
            channel_account_id: "web-console",
            selection_snapshot_id: "snapshot-1",
            run_type: "summary",
            status: "queued",
            version: 1,
            evidence_gate_state: "not_required",
            artifacts: [],
            diagnostics: [],
            created_at: "2026-05-18T00:00:00Z",
          },
        }, 201),
      );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(
      (client as any).listMediaAssets("web-console", { pageSize: 25, kind: "text", status: "ready" }),
    ).resolves.toMatchObject({ items: [expect.objectContaining({ media_asset_id: "asset-1" })] });
    await expect(
      (client as any).addMediaAsset("web-console", {
        kind: "text",
        displayName: "Note",
        origin: { origin_type: "text", text: "Meeting note" },
      }),
    ).resolves.toMatchObject({ media_asset_id: "asset-2" });
    const snapshot = await (client as any).createSelectionSnapshot("web-console", {
      sourceCollectionId: "collection-1",
      items: [{ media_asset_id: "asset-2", position: 0 }],
      optionSnapshot: { language: "ru" },
    });
    await expect(
      (client as any).createAnalysisRun("web-console", {
        selectionSnapshotId: snapshot.selection_snapshot_id,
        runType: "summary",
        delivery: { strategy: "polling" },
      }),
    ).resolves.toMatchObject({ analysis_run_id: "run-1" });

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), (call[1] as RequestInit | undefined)?.body])).toEqual([
      [
        "http://localhost:8080/v1/media-assets?channel_account_id=web-console&page_size=25&kind=text&status=ready",
        undefined,
      ],
      [
        "http://localhost:8080/v1/media-assets",
        JSON.stringify({
          channel_account_id: "web-console",
          kind: "text",
          origin: { origin_type: "text", text: "Meeting note" },
          collection_id: undefined,
          display_name: "Note",
        }),
      ],
      [
        "http://localhost:8080/v1/selection-snapshots",
        JSON.stringify({
          channel_account_id: "web-console",
          source_collection_id: "collection-1",
          items: [{ media_asset_id: "asset-2", position: 0 }],
          option_snapshot: { language: "ru" },
          created_via_channel_account_id: "web-console",
        }),
      ],
      [
        "http://localhost:8080/v1/analysis-runs",
        JSON.stringify({
          channel_account_id: "web-console",
          selection_snapshot_id: "snapshot-1",
          run_type: "summary",
          params: undefined,
          delivery: { strategy: "polling" },
          created_via_channel_id: "web-console",
        }),
      ],
    ]);
  });

  it("adds text media through the target media asset endpoint", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        media_asset: {
          media_asset_id: "media-1",
          channel_account_id: "web-console",
          kind: "text",
          status: "ready",
          display_name: "Note",
          origin: { origin_type: "text", text: "Meeting note" },
          retention: { state: "active" },
          created_at: "2026-05-10T00:00:00Z",
          updated_at: "2026-05-10T00:00:00Z",
        },
      }, 201),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/api",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(
      client.addMediaAsset(channelAccountId, {
        kind: "text",
        displayName: "Note",
        origin: { origin_type: "text", text: "Meeting note" },
      }),
    ).resolves.toEqual(expect.objectContaining({ media_asset_id: "media-1" }));

    expect(fetchImpl).toHaveBeenCalledWith(
      new URL("v1/media-assets", "http://localhost:8080/api/"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          channel_account_id: "web-console",
          kind: "text",
          origin: { origin_type: "text", text: "Meeting note" },
          display_name: "Note",
        }),
      }),
    );
  });

  it("creates a selection and queues an analysis run from that selection", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          selection_snapshot: {
            selection_snapshot_id: "selection-1",
            channel_account_id: "web-console",
            status: "sealed",
            items: [],
            option_snapshot: {},
            created_by: "web",
            created_at: "2026-05-10T00:00:00Z",
            sealed_at: "2026-05-10T00:00:00Z",
          },
        }, 201),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-1",
            channel_account_id: "web-console",
            selection_snapshot_id: "selection-1",
            selection_snapshot: { selection_snapshot_id: "selection-1", channel_account_id: "web-console", status: "sealed", items: [] },
            run_type: "summary",
            status: "queued",
            version: 1,
            delivery: { strategy: "polling" },
            evidence_gate_state: "not_required",
            artifacts: [],
            diagnostics: [],
            created_at: "2026-05-10T00:00:00Z",
          },
        }, 202),
      );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    const selection = await client.createSelectionSnapshot(channelAccountId, {
      items: [{ media_asset_id: "media-1", position: 0 }],
      sourceCollectionId: "collection-1",
      optionSnapshot: { language: "ru" },
    });
    await expect(
      client.createAnalysisRun(channelAccountId, {
        selectionSnapshotId: selection.selection_snapshot_id,
        runType: "summary",
        params: { tone: "brief" },
        delivery: { strategy: "polling" },
      }),
    ).resolves.toEqual(expect.objectContaining({ analysis_run_id: "run-1", run_type: "summary" }));

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/v1/selection-snapshots",
      "http://localhost:8080/v1/analysis-runs",
    ]);
    expect(fetchImpl).toHaveBeenNthCalledWith(
      1,
      new URL("v1/selection-snapshots", "http://localhost:8080/"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          channel_account_id: "web-console",
          source_collection_id: "collection-1",
          items: [{ media_asset_id: "media-1", position: 0 }],
          option_snapshot: { language: "ru" },
          created_via_channel_account_id: "web-console",
        }),
      }),
    );
  });

  it("uses channel-scoped collection, artifact, and diagnostic reads", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ items: [], page: { page_size: 50, has_more: false } }))
      .mockResolvedValueOnce(jsonResponse({ artifact: { artifact_id: "artifact-1" } }))
      .mockResolvedValueOnce(jsonResponse({ items: [], page: { page_size: 50, has_more: false } }));
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/root",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await client.listCollections(channelAccountId, { pageSize: 50 });
    await client.getArtifact(channelAccountId, "artifact-1");
    await client.listDiagnostics(channelAccountId, {
      subjectType: "analysis_run",
      subjectId: "run-1",
      severity: "warning",
      pageSize: 50,
    });

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/root/v1/collections?channel_account_id=web-console&page_size=50",
      "http://localhost:8080/root/v1/artifacts/artifact-1?channel_account_id=web-console",
      "http://localhost:8080/root/v1/diagnostics?channel_account_id=web-console&page_size=50&subject_type=analysis_run&subject_id=run-1&severity=warning",
    ]);
  });

  it("preserves run event stream transport and reconciliation checks", () => {
    const socket: {
      onopen: ((event: Event) => void) | null;
      onmessage: ((event: MessageEvent<string>) => void) | null;
      onerror: ((event: Event) => void) | null;
      onclose: ((event: CloseEvent) => void) | null;
      close: ReturnType<typeof vi.fn>;
    } = {
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      close: vi.fn(),
    };
    const webSocketFactory = vi.fn().mockReturnValue(socket);
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl: vi.fn(),
      webSocketFactory,
    });
    const onMessage = vi.fn();

    client.subscribeToRunEvents({ onMessage });
    socket.onmessage?.({
      data: JSON.stringify({
        event_id: "event-1",
        analysis_run_id: "run-1",
        event_type: "analysis_run.progress",
        version: 2,
        emitted_at: "2026-05-10T00:00:00Z",
        status: "running",
        payload: {
          stage: "transcribing",
          message: "Running transcription pipeline",
        },
      }),
    } as MessageEvent<string>);

    expect(webSocketFactory).toHaveBeenCalledWith("ws://localhost:8080/v1/ws");
    expect(onMessage).toHaveBeenCalledWith(expect.objectContaining({ analysis_run_id: "run-1" }));
    expect(requiresRestReconciliation(1, 3)).toBe(true);
    expect(requiresRestReconciliation(1, 2)).toBe(false);
  });

  it("normalizes optional run and websocket event fallback fields", async () => {
    const socket: {
      onopen: ((event: Event) => void) | null;
      onmessage: ((event: MessageEvent<string>) => void) | null;
      onerror: ((event: Event) => void) | null;
      onclose: ((event: CloseEvent) => void) | null;
      close: ReturnType<typeof vi.fn>;
    } = {
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      close: vi.fn(),
    };
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        analysis_run: {
          analysis_run_id: "run-minimal",
          channel_account_id: "web-console",
          selection_snapshot_id: "snapshot-minimal",
          run_type: "summary",
          status: "queued",
          version: 1,
          evidence_gate_state: "not_required",
          created_at: "2026-05-10T00:00:00Z",
        },
      }),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
      webSocketFactory: vi.fn().mockReturnValue(socket),
    });
    const onMessage = vi.fn();

    await expect(client.getAnalysisRun(channelAccountId, "run-minimal")).resolves.toMatchObject({
      selection_snapshot: {
        selection_snapshot_id: "snapshot-minimal",
        items: [],
      },
      artifacts: [],
      diagnostics: [],
    });
    client.subscribeToRunEvents({ onMessage });
    socket.onmessage?.({
      data: JSON.stringify({
        analysis_run_event_id: "event-alt-id",
        analysis_run_id: "run-minimal",
        event_type: "analysis_run.progress",
        version: 2,
        created_at: "2026-05-10T00:01:00Z",
        status: "running",
        payload: {},
      }),
    } as MessageEvent<string>);

    expect(onMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        event_id: "event-alt-id",
        emitted_at: "2026-05-10T00:01:00Z",
      }),
    );
  });

  it("calls final admin lifecycle and artifact access endpoints", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-1",
            channel_account_id: "web-console",
            selection_snapshot_id: "selection-1",
            selection_snapshot: { selection_snapshot_id: "selection-1", channel_account_id: "web-console", status: "sealed", items: [] },
            run_type: "summary",
            status: "cancel_requested",
            version: 3,
            delivery: { strategy: "polling" },
            evidence_gate_state: "not_required",
            artifacts: [],
            diagnostics: [],
            created_at: "2026-05-10T00:00:00Z",
          },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          artifact: {
            artifact_id: "artifact-1",
            channel_account_id: "web-console",
            analysis_run_id: "run-1",
            kind: "summary",
            status: "available",
            content_type: "text/plain",
            checksum: null,
            size_bytes: 42,
            visibility: "channel_deliverable",
            preview: { available: true, kind: "text", text_excerpt: "ready" },
            download: {
              available: true,
              provider: "minio_presigned_url",
              url: "https://minio.local/refreshed",
            },
            retention: { state: "active" },
            created_at: "2026-05-10T00:00:00Z",
          },
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          observability: {
            queue_tasks: 3,
            queue_lag_seconds: 42,
            cleanup_failures: 1,
            artifact_resolution_failures: 2,
            generated_at: "2026-05-10T00:00:00Z",
          },
        }),
      );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await client.cancelAnalysisRun(channelAccountId, "run-1");
    await client.refreshArtifact(channelAccountId, "artifact-1");
    await expect(client.getObservabilitySnapshot()).resolves.toMatchObject({ queue_lag_seconds: 42 });

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), (call[1] as RequestInit | undefined)?.body])).toEqual([
      [
        "http://localhost:8080/v1/analysis-runs/run-1/cancel",
        JSON.stringify({ channel_account_id: "web-console" }),
      ],
      [
        "http://localhost:8080/v1/artifacts/artifact-1/refresh?channel_account_id=web-console",
        JSON.stringify({}),
      ],
      ["http://localhost:8080/v1/admin/observability", undefined],
    ]);
  });

  it("surfaces API error envelopes with status and code", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse(
        {
          error: {
            code: "invalid_request",
            message: "invalid channel account",
          },
        },
        400,
      ),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(client.listMediaAssets(channelAccountId)).rejects.toMatchObject({
      name: "WebUiApiClientError",
      path: "/v1/media-assets?channel_account_id=web-console",
      status: 400,
      code: "invalid_request",
      message: "invalid channel account",
    });
  });

  it("lists media assets with trimmed filters and channel scope", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        items: [{ media_asset_id: "media-1", display_name: "Inbox note" }],
        page: { cursor: "cursor-2", page_size: 25, has_more: true },
      }),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/root",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(
      client.listMediaAssets(tenantChannelAccountId, {
        cursor: " cursor-1 ",
        pageSize: 25,
        query: "  note search  ",
        kind: "text",
        status: "ready",
      }),
    ).resolves.toMatchObject({
      items: [expect.objectContaining({ media_asset_id: "media-1" })],
      page: expect.objectContaining({ cursor: "cursor-2", has_more: true }),
    });

    expect(fetchImpl).toHaveBeenCalledWith(
      new URL(
        "v1/media-assets?channel_account_id=web-console&cursor=cursor-1&page_size=25&query=note+search&kind=text&status=ready",
        "http://localhost:8080/root/",
      ),
      expect.objectContaining({
        headers: { Accept: "application/json" },
      }),
    );
  });

  it("reads media and collection envelopes through channel-scoped GET requests", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ media_asset: { media_asset_id: "media-1", display_name: "Inbox note" } }))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "inbox", name: "Inbox" } }))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", name: "Research set" } }))
      .mockResolvedValueOnce(jsonResponse({ selection_snapshot: { selection_snapshot_id: "selection-1", channel_account_id: "web-console", status: "sealed", items: [] } }))
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-1",
            channel_account_id: "web-console",
            selection_snapshot_id: "selection-1",
            selection_snapshot: { selection_snapshot_id: "selection-1", channel_account_id: "web-console", status: "sealed", items: [] },
            run_type: "summary",
            status: "running",
            version: 7,
            delivery: { strategy: "polling" },
            evidence_gate_state: "not_required",
            artifacts: [],
            diagnostics: [],
            created_at: "2026-05-10T00:00:00Z",
          },
        }),
      );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(client.getMediaAsset(channelAccountId, "media-1")).resolves.toMatchObject({ media_asset_id: "media-1" });
    await expect(client.getInboxCollection(channelAccountId)).resolves.toMatchObject({ collection_id: "inbox" });
    await expect(client.getCollection(channelAccountId, "collection-1", { cursor: " cursor-2 ", pageSize: 10 })).resolves.toMatchObject({
      collection_id: "collection-1",
    });
    await expect(client.getSelectionSnapshot(channelAccountId, "selection-1")).resolves.toMatchObject({ selection_snapshot_id: "selection-1" });
    await expect(client.getAnalysisRun(channelAccountId, "run-1")).resolves.toMatchObject({ analysis_run_id: "run-1", version: 7 });

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/v1/media-assets/media-1?channel_account_id=web-console",
      "http://localhost:8080/v1/collections/inbox?channel_account_id=web-console",
      "http://localhost:8080/v1/collections/collection-1?channel_account_id=web-console&cursor=cursor-2&page_size=10",
      "http://localhost:8080/v1/selection-snapshots/selection-1?channel_account_id=web-console",
      "http://localhost:8080/v1/analysis-runs/run-1?channel_account_id=web-console",
    ]);
  });

  it("creates, updates, and mutates collections with trimmed bodies", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", name: "Alpha" } }, 201))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", name: "Beta", status: "archived" } }))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", items: [] } }))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", items: [] } }));
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await client.createCollection(tenantChannelAccountId, {
      name: "  Alpha  ",
      items: ["media-1"],
    });
    await client.updateCollection(tenantChannelAccountId, "collection-1", {
      expectedVersion: 3,
      name: "  Beta  ",
      status: "archived",
    });
    await client.replaceCollectionItems(tenantChannelAccountId, "collection-1", {
      expectedVersion: 4,
      items: [{ media_asset_id: "media-2", position: 0 }],
    });
    await client.removeCollectionItem(channelAccountId, "collection-1", "media-2", 5);

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), call[1]])).toEqual([
      [
        "http://localhost:8080/v1/collections",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            channel_account_id: "web-console",
            name: "Alpha",
            items: ["media-1"],
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/collections/collection-1",
        expect.objectContaining({
          method: "PATCH",
          body: JSON.stringify({
            channel_account_id: "web-console",
            expected_version: 3,
            name: "Beta",
            status: "archived",
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/collections/collection-1/items",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            channel_account_id: "web-console",
            expected_version: 4,
            items: [{ media_asset_id: "media-2", position: 0 }],
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/collections/collection-1/items/media-2?channel_account_id=web-console&expected_version=5",
        expect.objectContaining({
          method: "DELETE",
        }),
      ],
    ]);
  });

  it("lists and retries analysis runs with filters and channel body", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ items: [{ analysis_run_id: "run-1" }], page: { page_size: 25, has_more: false } }))
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-2",
            channel_account_id: "web-console",
            selection_snapshot_id: "selection-2",
            selection_snapshot: { selection_snapshot_id: "selection-2", channel_account_id: "web-console", status: "sealed", items: [] },
            run_type: "report",
            status: "queued",
            version: 1,
            delivery: { strategy: "polling" },
            evidence_gate_state: "not_required",
            artifacts: [],
            diagnostics: [],
            created_at: "2026-05-10T00:00:00Z",
          },
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ items: [{ event_id: "event-1" }], page: { cursor: "next", page_size: 10, has_more: true } }));
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(
      client.listAnalysisRuns(channelAccountId, { status: "running", runType: "summary", cursor: " cursor-3 ", pageSize: 25 }),
    ).resolves.toMatchObject({
      items: [expect.objectContaining({ analysis_run_id: "run-1" })],
    });
    await expect(client.retryAnalysisRun(tenantChannelAccountId, "run-2")).resolves.toMatchObject({ analysis_run_id: "run-2" });
    await expect(client.listAnalysisRunEvents(channelAccountId, "run-2", { cursor: "cursor-4", pageSize: 10 })).resolves.toMatchObject({
      page: expect.objectContaining({ cursor: "next" }),
    });

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), (call[1] as RequestInit | undefined)?.body])).toEqual([
      [
        "http://localhost:8080/v1/analysis-runs?channel_account_id=web-console&cursor=cursor-3&page_size=25&status=running&run_type=summary",
        undefined,
      ],
      [
        "http://localhost:8080/v1/analysis-runs/run-2/retry",
        JSON.stringify({ channel_account_id: "web-console" }),
      ],
      [
        "http://localhost:8080/v1/analysis-runs/run-2/events?channel_account_id=web-console&cursor=cursor-4&page_size=10",
        undefined,
      ],
    ]);
  });

  it("lists artifacts and diagnostics with optional filters", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ items: [{ artifact_id: "artifact-1" }], page: { page_size: 50, has_more: false } }))
      .mockResolvedValueOnce(jsonResponse({ items: [{ diagnostic_id: "diag-1" }], page: { page_size: 50, has_more: false } }))
      .mockResolvedValueOnce(jsonResponse({ items: [], page: { page_size: 50, has_more: false } }));
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/api",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(client.listArtifacts(channelAccountId, { analysisRunId: " run-1 ", pageSize: 50 })).resolves.toMatchObject({
      items: [expect.objectContaining({ artifact_id: "artifact-1" })],
    });
    await expect(
      client.listDiagnostics(channelAccountId, {
        subjectType: "artifact",
        subjectId: " artifact-1 ",
        severity: "error",
        cursor: " cursor-5 ",
        pageSize: 50,
      }),
    ).resolves.toMatchObject({
      items: [expect.objectContaining({ diagnostic_id: "diag-1" })],
    });
    await expect(client.listDiagnostics(channelAccountId, { subjectId: "   " })).resolves.toMatchObject({
      items: [],
    });

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/api/v1/artifacts?channel_account_id=web-console&page_size=50&analysis_run_id=run-1",
      "http://localhost:8080/api/v1/diagnostics?channel_account_id=web-console&cursor=cursor-5&page_size=50&subject_type=artifact&subject_id=artifact-1&severity=error",
      "http://localhost:8080/api/v1/diagnostics?channel_account_id=web-console",
    ]);
  });

  it("raises clear errors for missing envelopes and plain-text failures", async () => {
    const envelopeFetch = vi.fn().mockResolvedValue(jsonResponse({ items: [] }));
    const failingFetch = vi.fn().mockResolvedValue(textResponse("broken upstream", 502));
    const envelopeClient = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl: envelopeFetch,
    });
    const failingClient = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl: failingFetch,
    });

    await expect(envelopeClient.getMediaAsset(channelAccountId, "media-1")).rejects.toThrowError(
      "API response does not include media_asset",
    );
    await expect(failingClient.listCollections(channelAccountId)).rejects.toMatchObject({
      name: "WebUiApiClientError",
      status: 502,
      path: "/v1/collections?channel_account_id=web-console",
      message: "API request failed for /v1/collections?channel_account_id=web-console",
    });
  });

  it("normalizes blank optional payload fields and supports media deletion", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ media_asset: { media_asset_id: "media-1", display_name: "Inbox note" } }, 201))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", name: "Inbox" } }))
      .mockResolvedValueOnce(jsonResponse({ selection_snapshot: { selection_snapshot_id: "selection-1", channel_account_id: "web-console", status: "sealed", items: [] } }, 201))
      .mockResolvedValueOnce(jsonResponse({ media_asset: { media_asset_id: "media-1", status: "deleted" } }));
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await client.addMediaAsset(channelAccountId, {
      kind: "text",
      displayName: "   ",
      origin: { origin_type: "text", text: "Meeting note" },
    });
    await client.updateCollection(channelAccountId, "collection-1", {
      expectedVersion: 4,
      name: "   ",
    });
    await client.createSelectionSnapshot(channelAccountId, {
      items: [{ media_asset_id: "media-1", position: 0 }],
      sourceCollectionId: "",
    });
    await client.removeMediaAsset(channelAccountId, "media-1");

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), call[1]])).toEqual([
      [
        "http://localhost:8080/v1/media-assets",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            channel_account_id: "web-console",
            kind: "text",
            origin: { origin_type: "text", text: "Meeting note" },
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/collections/collection-1",
        expect.objectContaining({
          method: "PATCH",
          body: JSON.stringify({
            channel_account_id: "web-console",
            expected_version: 4,
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/selection-snapshots",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            channel_account_id: "web-console",
            items: [{ media_asset_id: "media-1", position: 0 }],
            created_via_channel_account_id: "web-console",
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/media-assets/media-1?channel_account_id=web-console",
        expect.objectContaining({
          method: "DELETE",
        }),
      ],
    ]);
  });

  it("falls back for missing content type, non-string API errors, and 204 responses", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(
          {
            error: {
              code: 404,
              message: false,
            },
          },
          400,
        ),
      )
      .mockResolvedValueOnce(
        new Response("upstream unavailable", {
          status: 400,
        }),
      )
      .mockResolvedValueOnce(
        new Response(null, {
          status: 204,
        }),
      );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(client.listMediaAssets(channelAccountId)).rejects.toMatchObject({
      name: "WebUiApiClientError",
      status: 400,
      path: "/v1/media-assets?channel_account_id=web-console",
      code: undefined,
      message: "API request failed for /v1/media-assets?channel_account_id=web-console",
    });
    await expect(client.listCollections(channelAccountId)).rejects.toMatchObject({
      name: "WebUiApiClientError",
      status: 400,
      path: "/v1/collections?channel_account_id=web-console",
      code: undefined,
      message: "API request failed for /v1/collections?channel_account_id=web-console",
    });
  });

  it("uses slash-normalized base URLs and the default websocket transport", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({ items: [], page: { page_size: 25, has_more: false } }),
    );
    const socket = {
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      close: vi.fn(),
    };
    const webSocketSpy = vi.fn(() => socket);
    const originalWebSocket = globalThis.WebSocket;
    vi.stubGlobal("WebSocket", webSocketSpy as unknown as typeof WebSocket);

    try {
      const client = createWebUiApiClient({
        baseUrl: "http://localhost:8080/root/",
        wsUrl: "ws://localhost:8080/v1/ws",
        fetchImpl,
      });

      await client.listCollections(channelAccountId);
      const subscription = client.subscribeToRunEvents({ onMessage: vi.fn() });
      subscription.close();

      expect(fetchImpl).toHaveBeenCalledWith(
        new URL("v1/collections?channel_account_id=web-console", "http://localhost:8080/root/"),
        expect.any(Object),
      );
      expect(webSocketSpy).toHaveBeenCalledWith("ws://localhost:8080/v1/ws");
      expect(socket.close).toHaveBeenCalledTimes(1);
    } finally {
      vi.unstubAllGlobals();
      globalThis.WebSocket = originalWebSocket;
    }
  });

  it("wires websocket open, error, invalid payload, close, and manual shutdown paths", () => {
    const socket: {
      onopen: ((event: Event) => void) | null;
      onmessage: ((event: MessageEvent<string>) => void) | null;
      onerror: ((event: Event) => void) | null;
      onclose: ((event: CloseEvent) => void) | null;
      close: ReturnType<typeof vi.fn>;
    } = {
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      close: vi.fn(),
    };
    const webSocketFactory = vi.fn().mockReturnValue(socket);
    const onOpen = vi.fn();
    const onMessage = vi.fn();
    const onError = vi.fn();
    const onClose = vi.fn();
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl: vi.fn(),
      webSocketFactory,
    });

    const subscription = client.subscribeToRunEvents({ onOpen, onMessage, onError, onClose });
    socket.onopen?.({} as Event);
    socket.onerror?.({ type: "error" } as Event);
    socket.onmessage?.({ data: "not json" } as MessageEvent<string>);
    socket.onmessage?.({ data: JSON.stringify("not an event object") } as MessageEvent<string>);
    socket.onmessage?.({ data: JSON.stringify({ event_id: "event-ignored" }) } as MessageEvent<string>);
    socket.onclose?.({} as CloseEvent);
    subscription.close();

    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledTimes(2);
    expect(onMessage).not.toHaveBeenCalled();
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(socket.close).toHaveBeenCalledTimes(1);
    expect(requiresRestReconciliation(4, 4)).toBe(true);
    expect(requiresRestReconciliation(4, 3)).toBe(true);
  });
});
