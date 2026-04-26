// FILE: apps/web/tests/api-client.test.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the Web UI API boundary preserves canonical job, action, and event transport semantics without leaking contract drift into UI layers.
// SCOPE: Verify envelope normalization, multipart payload shaping, error handling, canonical websocket event transport, and reconciliation helpers.
// DEPENDS: M-WEB-UI, M-API-HTTP
// LINKS: V-M-WEB-UI
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Expanded the client tests to cover job envelopes, multipart create flows, canonical websocket transport, and reconciliation helpers.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-client-request-shape - Confirm the client resolves job reads and envelopes against the actual API surface.
//   verify-multipart-shape - Confirm upload and combined create paths preserve multipart and delivery fields.
//   verify-event-boundary - Confirm websocket subscription parses canonical messages and preserves the declared event stream endpoint.
//   verify-client-error-surface - Confirm failed responses surface as transport errors instead of client-side guesses.
// END_MODULE_MAP

import { describe, expect, it, vi } from "vitest";

import {
  WebUiApiClientError,
  createWebUiApiClient,
  requiresRestReconciliation,
} from "../src/lib/api/client";

describe("createWebUiApiClient", () => {
  // START_BLOCK_BLOCK_VERIFY_CLIENT_REQUEST_SHAPE
  it("unwraps job envelopes returned by the current server implementation", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          job: {
            job_id: "job-1",
            root_job_id: "job-1",
            job_type: "transcription",
            status: "queued",
            version: 1,
            delivery: { strategy: "polling" },
            source_set: {
              source_set_id: "source-set-1",
              input_kind: "single_source",
              items: [
                {
                  position: 0,
                  source: {
                    source_id: "source-1",
                    source_kind: "uploaded_file",
                  },
                },
              ],
            },
            artifacts: [],
            children: [],
            created_at: "2026-04-23T00:00:00Z",
          },
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/api",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(client.getJob("job-1")).resolves.toEqual(
      expect.objectContaining({
        job_id: "job-1",
        status: "queued",
      }),
    );
    expect(fetchImpl).toHaveBeenCalledWith(
      new URL("v1/jobs/job-1", "http://localhost:8080/api/"),
      expect.objectContaining({
        headers: expect.objectContaining({
          Accept: "application/json",
        }),
      }),
    );
  });
  // END_BLOCK_BLOCK_VERIFY_CLIENT_REQUEST_SHAPE

  it("tolerates agent_run snapshots from report and deep-research create endpoints", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            job: {
              job_id: "agent-report-1",
              root_job_id: "transcription-1",
              parent_job_id: "transcription-1",
              job_type: "agent_run",
              status: "queued",
              version: 1,
              delivery: { strategy: "polling" },
              source_set: { source_set_id: "source-set-1", input_kind: "single_source", items: [] },
              artifacts: [],
              children: [],
              created_at: "2026-04-23T00:00:00Z",
            },
          }),
          {
            status: 202,
            headers: { "Content-Type": "application/json" },
          },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            job: {
              job_id: "agent-deep-1",
              root_job_id: "transcription-1",
              parent_job_id: "agent-report-1",
              job_type: "agent_run",
              status: "queued",
              version: 1,
              delivery: { strategy: "polling" },
              source_set: { source_set_id: "source-set-1", input_kind: "single_source", items: [] },
              artifacts: [],
              children: [],
              created_at: "2026-04-23T00:00:00Z",
            },
          }),
          {
            status: 202,
            headers: { "Content-Type": "application/json" },
          },
        ),
      );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/api",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(
      client.createReport("transcription-1", {
        clientRef: "",
        delivery: { strategy: "polling", webhookUrl: "" },
      }),
    ).resolves.toEqual(expect.objectContaining({ job_id: "agent-report-1", job_type: "agent_run" }));
    await expect(
      client.createDeepResearch("agent-report-1", {
        clientRef: "",
        delivery: { strategy: "polling", webhookUrl: "" },
      }),
    ).resolves.toEqual(expect.objectContaining({ job_id: "agent-deep-1", job_type: "agent_run" }));
  });

  // START_BLOCK_BLOCK_VERIFY_MULTIPART_SHAPE
  it("shapes multipart upload requests through the packet-local boundary", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ jobs: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/api",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });
    const file = new File(["audio-data"], "clip.mp3", { type: "audio/mpeg" });

    await client.createUpload({
      files: [file],
      displayName: "Clip",
      clientRef: "client-1",
      delivery: {
        strategy: "webhook",
        webhookUrl: "https://example.com/hook",
      },
    });

    const request = fetchImpl.mock.calls[0][1];
    const formData = request.body as FormData;
    expect(formData.get("display_name")).toBe("Clip");
    expect(formData.get("client_ref")).toBe("client-1");
    expect(formData.get("delivery_strategy")).toBe("webhook");
    expect(formData.get("delivery_webhook_url")).toBe("https://example.com/hook");
    expect(formData.getAll("files")).toHaveLength(1);
  });
  // END_BLOCK_BLOCK_VERIFY_MULTIPART_SHAPE

  // START_BLOCK_BLOCK_VERIFY_EVENT_BOUNDARY
  it("preserves websocket urls and forwards canonical event envelopes", () => {
    const fetchImpl = vi.fn();
    const socket = {
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
      fetchImpl,
      webSocketFactory,
    });
    const onMessage = vi.fn();

    client.subscribeToJobEvents({ onMessage });

    expect(webSocketFactory).toHaveBeenCalledWith("ws://localhost:8080/v1/ws");
    socket.onmessage?.({
      data: JSON.stringify({
        event_id: "event-1",
        event_type: "job.updated",
        job_id: "job-1",
        root_job_id: "job-1",
        job_type: "transcription",
        job_url: "/v1/jobs/job-1",
        version: 2,
        emitted_at: "2026-04-23T00:00:00Z",
        payload: {
          status: "running",
        },
      }),
    } as MessageEvent<string>);

    expect(onMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        event_id: "event-1",
        job_id: "job-1",
        version: 2,
      }),
    );
    expect(requiresRestReconciliation(1, 3)).toBe(true);
    expect(requiresRestReconciliation(1, 2)).toBe(false);
  });
  // END_BLOCK_BLOCK_VERIFY_EVENT_BOUNDARY

  // START_BLOCK_BLOCK_VERIFY_CLIENT_ERROR_SURFACE
  it("surfaces non-ok responses as transport errors", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          error: {
            code: "dependency_unavailable",
            message: "API dependency unavailable",
          },
        }),
        {
          status: 503,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );
    const client = createWebUiApiClient({
      baseUrl: "http://localhost:8080/api",
      wsUrl: "ws://localhost:8080/v1/ws",
      fetchImpl,
    });

    await expect(client.listJobs({})).rejects.toEqual(
      expect.objectContaining<WebUiApiClientError>({
        name: "WebUiApiClientError",
        path: "/v1/jobs?page=1&page_size=20",
        status: 503,
        code: "dependency_unavailable",
      }),
    );
  });
  // END_BLOCK_BLOCK_VERIFY_CLIENT_ERROR_SURFACE
});
