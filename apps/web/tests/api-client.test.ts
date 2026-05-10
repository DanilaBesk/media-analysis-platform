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

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
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
});
