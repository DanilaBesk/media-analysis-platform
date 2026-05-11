import { describe, expect, it, vi } from "vitest";

import {
  WebUiApiClientError,
  createWebUiApiClient,
  requiresRestReconciliation,
} from "../src/lib/api/client";
import type { OwnerScope } from "../src/lib/api/types";

const owner: OwnerScope = {
  owner_type: "web",
  owner_id: "web-console",
};

const tenantOwner: OwnerScope = {
  owner_type: "web",
  owner_id: "web-console",
  tenant_id: " tenant-a ",
  adapter_identity: "web-ui",
};

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
  it("adds text media through the final media item endpoint", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        media_item: {
          media_item_id: "media-1",
          owner,
          kind: "text",
          status: "ready",
          display_name: "Note",
          source: { source_id: "source-1", origin_type: "text" },
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
      client.addMediaItem(owner, {
        kind: "text",
        displayName: "Note",
        adapterOrigin: "web",
        source: { origin_type: "text", text: "Meeting note" },
      }),
    ).resolves.toEqual(expect.objectContaining({ media_item_id: "media-1" }));

    expect(fetchImpl).toHaveBeenCalledWith(
      new URL("v1/media-items", "http://localhost:8080/api/"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          owner,
          kind: "text",
          source: { origin_type: "text", text: "Meeting note" },
          collection_id: undefined,
          display_name: "Note",
          adapter_origin: "web",
        }),
      }),
    );
  });

  it("creates a selection and queues an analysis run from that selection", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          selection: {
            selection_id: "selection-1",
            owner,
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
            owner,
            selection_id: "selection-1",
            selection: { selection_id: "selection-1", owner, status: "sealed", items: [] },
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

    const selection = await client.createSelection(owner, {
      items: [{ media_item_id: "media-1", position: 0 }],
      sourceCollectionId: "collection-1",
      duplicatePolicy: "reject",
      createdBy: "web",
    });
    await expect(
      client.createAnalysisRun(owner, {
        selectionId: selection.selection_id,
        runType: "summary",
        params: { tone: "brief" },
        delivery: { strategy: "polling" },
      }),
    ).resolves.toEqual(expect.objectContaining({ analysis_run_id: "run-1", run_type: "summary" }));

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/v1/selections",
      "http://localhost:8080/v1/analysis-runs",
    ]);
    expect(fetchImpl).toHaveBeenNthCalledWith(
      1,
      new URL("v1/selections", "http://localhost:8080/"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          owner,
          source_collection_id: "collection-1",
          items: [{ media_item_id: "media-1", position: 0 }],
          duplicate_policy: "reject",
          created_by: "web",
        }),
      }),
    );
  });

  it("uses owner-scoped collection, artifact, and diagnostic reads", async () => {
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

    await client.listCollections(owner, { pageSize: 50 });
    await client.getArtifact(owner, "artifact-1");
    await client.listDiagnostics(owner, {
      subjectType: "analysis_run",
      subjectId: "run-1",
      severity: "warning",
      pageSize: 50,
    });

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/root/v1/collections?owner_type=web&owner_id=web-console&page_size=50",
      "http://localhost:8080/root/v1/artifacts/artifact-1?owner_type=web&owner_id=web-console",
      "http://localhost:8080/root/v1/diagnostics?owner_type=web&owner_id=web-console&page_size=50&subject_type=analysis_run&subject_id=run-1&severity=warning",
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

  it("calls final admin lifecycle and artifact access endpoints", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-1",
            owner,
            selection_id: "selection-1",
            selection: { selection_id: "selection-1", owner, status: "sealed", items: [] },
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
            owner,
            analysis_run_id: "run-1",
            kind: "summary",
            status: "available",
            content_type: "text/plain",
            checksum: null,
            size_bytes: 42,
            visibility: "owner",
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
      .mockResolvedValueOnce(jsonResponse({ reconciled: 2 }, 202))
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

    await client.cancelAnalysisRun(owner, "run-1");
    await client.refreshArtifact(owner, "artifact-1");
    await expect(client.reconcileAnalysisRunQueue(10)).resolves.toEqual({ reconciled: 2 });
    await expect(client.getObservabilitySnapshot()).resolves.toMatchObject({ queue_lag_seconds: 42 });

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), (call[1] as RequestInit | undefined)?.body])).toEqual([
      [
        "http://localhost:8080/v1/analysis-runs/run-1/cancel?owner_type=web&owner_id=web-console",
        JSON.stringify({}),
      ],
      [
        "http://localhost:8080/v1/artifacts/artifact-1/refresh?owner_type=web&owner_id=web-console",
        JSON.stringify({}),
      ],
      ["http://localhost:8080/v1/admin/reconcile-queue", JSON.stringify({ limit: 10 })],
      ["http://localhost:8080/v1/admin/observability", undefined],
    ]);
  });

  it("surfaces API error envelopes with status and code", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse(
        {
          error: {
            code: "invalid_request",
            message: "invalid owner",
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

    await expect(client.listMediaItems(owner)).rejects.toMatchObject({
      name: "WebUiApiClientError",
      path: "/v1/media-items?owner_type=web&owner_id=web-console",
      status: 400,
      code: "invalid_request",
      message: "invalid owner",
    });
  });

  it("lists media items with trimmed filters and tenant scope", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        items: [{ media_item_id: "media-1", display_name: "Inbox note" }],
        page: { cursor: "cursor-2", page_size: 25, has_more: true },
      }),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/root",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(
      client.listMediaItems(tenantOwner, {
        cursor: " cursor-1 ",
        pageSize: 25,
        query: "  note search  ",
        kind: "text",
        status: "ready",
      }),
    ).resolves.toMatchObject({
      items: [expect.objectContaining({ media_item_id: "media-1" })],
      page: expect.objectContaining({ cursor: "cursor-2", has_more: true }),
    });

    expect(fetchImpl).toHaveBeenCalledWith(
      new URL(
        "v1/media-items?owner_type=web&owner_id=web-console&tenant_id=tenant-a&cursor=cursor-1&page_size=25&query=note+search&kind=text&status=ready",
        "http://localhost:8080/root/",
      ),
      expect.objectContaining({
        headers: { Accept: "application/json" },
      }),
    );
  });

  it("reads media and collection envelopes through owner-scoped GET requests", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ media_item: { media_item_id: "media-1", display_name: "Inbox note" } }))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "inbox", name: "Inbox" } }))
      .mockResolvedValueOnce(jsonResponse({ collection: { collection_id: "collection-1", name: "Research set" } }))
      .mockResolvedValueOnce(jsonResponse({ selection: { selection_id: "selection-1", status: "sealed", items: [] } }))
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-1",
            owner,
            selection_id: "selection-1",
            selection: { selection_id: "selection-1", owner, status: "sealed", items: [] },
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

    await expect(client.getMediaItem(owner, "media-1")).resolves.toMatchObject({ media_item_id: "media-1" });
    await expect(client.getInboxCollection(owner)).resolves.toMatchObject({ collection_id: "inbox" });
    await expect(client.getCollection(owner, "collection-1", { cursor: " cursor-2 ", pageSize: 10 })).resolves.toMatchObject({
      collection_id: "collection-1",
    });
    await expect(client.getSelection(owner, "selection-1")).resolves.toMatchObject({ selection_id: "selection-1" });
    await expect(client.getAnalysisRun(owner, "run-1")).resolves.toMatchObject({ analysis_run_id: "run-1", version: 7 });

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/v1/media-items/media-1?owner_type=web&owner_id=web-console",
      "http://localhost:8080/v1/collections/inbox?owner_type=web&owner_id=web-console",
      "http://localhost:8080/v1/collections/collection-1?owner_type=web&owner_id=web-console&cursor=cursor-2&page_size=10",
      "http://localhost:8080/v1/selections/selection-1?owner_type=web&owner_id=web-console",
      "http://localhost:8080/v1/analysis-runs/run-1?owner_type=web&owner_id=web-console",
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

    await client.createCollection(tenantOwner, {
      name: "  Alpha  ",
      items: ["media-1"],
    });
    await client.updateCollection(tenantOwner, "collection-1", {
      expectedVersion: 3,
      name: "  Beta  ",
      status: "archived",
    });
    await client.replaceCollectionItems(tenantOwner, "collection-1", {
      expectedVersion: 4,
      items: [{ media_item_id: "media-2", position: 0 }],
    });
    await client.removeCollectionItem(owner, "collection-1", "media-2", 5);

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), call[1]])).toEqual([
      [
        "http://localhost:8080/v1/collections",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            owner: {
              owner_type: "web",
              owner_id: "web-console",
              tenant_id: "tenant-a",
              adapter_identity: "web-ui",
            },
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
            owner: {
              owner_type: "web",
              owner_id: "web-console",
              tenant_id: "tenant-a",
              adapter_identity: "web-ui",
            },
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
            owner: {
              owner_type: "web",
              owner_id: "web-console",
              tenant_id: "tenant-a",
              adapter_identity: "web-ui",
            },
            expected_version: 4,
            items: [{ media_item_id: "media-2", position: 0 }],
          }),
        }),
      ],
      [
        "http://localhost:8080/v1/collections/collection-1/items/media-2?owner_type=web&owner_id=web-console&expected_version=5",
        expect.objectContaining({
          method: "DELETE",
        }),
      ],
    ]);
  });

  it("lists and retries analysis runs with filters and owner body", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ items: [{ analysis_run_id: "run-1" }], page: { page_size: 25, has_more: false } }))
      .mockResolvedValueOnce(
        jsonResponse({
          analysis_run: {
            analysis_run_id: "run-2",
            owner,
            selection_id: "selection-2",
            selection: { selection_id: "selection-2", owner, status: "sealed", items: [] },
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
      client.listAnalysisRuns(owner, { status: "running", runType: "summary", cursor: " cursor-3 ", pageSize: 25 }),
    ).resolves.toMatchObject({
      items: [expect.objectContaining({ analysis_run_id: "run-1" })],
    });
    await expect(client.retryAnalysisRun(tenantOwner, "run-2")).resolves.toMatchObject({ analysis_run_id: "run-2" });
    await expect(client.listAnalysisRunEvents(owner, "run-2", { cursor: "cursor-4", pageSize: 10 })).resolves.toMatchObject({
      page: expect.objectContaining({ cursor: "next" }),
    });

    expect(fetchImpl.mock.calls.map((call) => [String(call[0]), (call[1] as RequestInit | undefined)?.body])).toEqual([
      [
        "http://localhost:8080/v1/analysis-runs?owner_type=web&owner_id=web-console&cursor=cursor-3&page_size=25&status=running&run_type=summary",
        undefined,
      ],
      [
        "http://localhost:8080/v1/analysis-runs/run-2/retry?owner_type=web&owner_id=web-console&tenant_id=tenant-a",
        JSON.stringify({
          owner: {
            owner_type: "web",
            owner_id: "web-console",
            tenant_id: "tenant-a",
            adapter_identity: "web-ui",
          },
        }),
      ],
      [
        "http://localhost:8080/v1/analysis-runs/run-2/events?owner_type=web&owner_id=web-console&cursor=cursor-4&page_size=10",
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

    await expect(client.listArtifacts(owner, { analysisRunId: " run-1 ", pageSize: 50 })).resolves.toMatchObject({
      items: [expect.objectContaining({ artifact_id: "artifact-1" })],
    });
    await expect(
      client.listDiagnostics(owner, {
        subjectType: "artifact",
        subjectId: " artifact-1 ",
        severity: "error",
        cursor: " cursor-5 ",
        pageSize: 50,
      }),
    ).resolves.toMatchObject({
      items: [expect.objectContaining({ diagnostic_id: "diag-1" })],
    });
    await expect(client.listDiagnostics(owner, { subjectId: "   " })).resolves.toMatchObject({
      items: [],
    });

    expect(fetchImpl.mock.calls.map((call) => String(call[0]))).toEqual([
      "http://localhost:8080/api/v1/artifacts?owner_type=web&owner_id=web-console&page_size=50&analysis_run_id=run-1",
      "http://localhost:8080/api/v1/diagnostics?owner_type=web&owner_id=web-console&cursor=cursor-5&page_size=50&subject_type=artifact&subject_id=artifact-1&severity=error",
      "http://localhost:8080/api/v1/diagnostics?owner_type=web&owner_id=web-console",
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

    await expect(envelopeClient.getMediaItem(owner, "media-1")).rejects.toThrowError(
      "API response does not include media_item",
    );
    await expect(failingClient.listCollections(owner)).rejects.toMatchObject({
      name: "WebUiApiClientError",
      status: 502,
      path: "/v1/collections?owner_type=web&owner_id=web-console",
      message: "API request failed for /v1/collections?owner_type=web&owner_id=web-console",
    });
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
