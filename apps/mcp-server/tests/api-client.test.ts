// FILE: apps/mcp-server/tests/api-client.test.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the MCP adapter boundary preserves JSON and multipart transport semantics without absorbing business logic.
// SCOPE: Verify request URL normalization, JSON transport behavior, multipart passthrough, and upstream error-envelope preservation.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Aligned transport-boundary tests with inbox-first media paths.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-request-shape - Confirm the thin client normalizes paths and JSON payloads.
//   verify-multipart-passthrough - Confirm multipart bodies stay multipart through the transport boundary.
//   verify-error-surface - Confirm upstream API error envelopes remain visible at the client boundary.
// END_MODULE_MAP

import test from "node:test";
import assert from "node:assert/strict";

import {
  McpAdapterApiClientError,
  createMcpAdapterApiClient,
} from "../src/client/api-client.ts";

test("createMcpAdapterApiClient normalizes request targets and JSON payloads", async () => {
  // START_BLOCK_BLOCK_VERIFY_REQUEST_SHAPE
  const calls: Array<{
    url: string;
    init: RequestInit | undefined;
  }> = [];
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test/root",
    fetchImpl: async (input, init) => {
      calls.push({
        url: input instanceof URL ? input.toString() : String(input),
        init,
      });
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          "content-type": "application/json",
        },
      });
    },
  });

  const response = await client.request<{ ok: boolean }>({
    path: "/v1/media-items",
    method: "POST",
    body: { kind: "text" },
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.url, "https://api.example.test/root/v1/media-items");
  assert.equal(calls[0]?.init?.method, "POST");
  assert.equal(calls[0]?.init?.headers instanceof Object, true);
  assert.equal(
    (calls[0]?.init?.headers as Record<string, string>)["Content-Type"],
    "application/json",
  );
  assert.equal(calls[0]?.init?.body, JSON.stringify({ kind: "text" }));
  assert.deepEqual(response, {
    status: 200,
    data: { ok: true },
  });
  // END_BLOCK_BLOCK_VERIFY_REQUEST_SHAPE
});

test("createMcpAdapterApiClient preserves multipart bodies without forcing JSON headers", async () => {
  // START_BLOCK_BLOCK_VERIFY_MULTIPART_PASSTHROUGH
  const calls: Array<{
    url: string;
    init: RequestInit | undefined;
  }> = [];
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test/root",
    fetchImpl: async (input, init) => {
      calls.push({
        url: input instanceof URL ? input.toString() : String(input),
        init,
      });
      return new Response(JSON.stringify({ media_item: { media_item_id: "media-1" } }), {
        status: 201,
        headers: {
          "content-type": "application/json",
        },
      });
    },
  });
  const body = new FormData();
  body.append("file", new Blob(["audio-data"], { type: "audio/mpeg" }), "clip.mp3");

  await client.request({
    path: "/v1/media-items",
    method: "POST",
    body,
  });

  assert.equal(calls[0]?.url, "https://api.example.test/root/v1/media-items");
  assert.equal(calls[0]?.init?.body, body);
  assert.equal(
    Object.hasOwn((calls[0]?.init?.headers as Record<string, string>) ?? {}, "Content-Type"),
    false,
  );
  // END_BLOCK_BLOCK_VERIFY_MULTIPART_PASSTHROUGH
});

test("createMcpAdapterApiClient preserves upstream error envelopes", async () => {
  // START_BLOCK_BLOCK_VERIFY_ERROR_SURFACE
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test",
    fetchImpl: async () =>
      new Response(
        JSON.stringify({
          error: {
            code: "upstream_failure",
            message: "upstream rejected request",
            correlation_id: "corr-123",
            details: {
              field: "media_item_id",
            },
            diagnostics: [
              {
                diagnostic_id: "diagnostic-1",
                severity: "error",
                code: "artifact_resolution_failed",
                message: "preview object missing",
              },
            ],
          },
        }),
        {
          status: 409,
          headers: {
            "content-type": "application/json",
          },
        },
      ),
  });

  await assert.rejects(
    () =>
      client.request({
        path: "/v1/media-items/00000000-0000-4000-8000-000000000001",
      }),
    (error: unknown) => {
      assert.ok(error instanceof McpAdapterApiClientError);
      const apiError = error as McpAdapterApiClientError;
      assert.equal(apiError.path, "/v1/media-items/00000000-0000-4000-8000-000000000001");
      assert.equal(apiError.status, 409);
      assert.equal(apiError.code, "upstream_failure");
      assert.equal(apiError.message, "upstream rejected request");
      assert.equal((apiError as any).correlationId, "corr-123");
      assert.deepEqual((apiError as any).details, {
        field: "media_item_id",
      });
      assert.deepEqual((apiError as any).diagnostics, [
        {
          diagnostic_id: "diagnostic-1",
          severity: "error",
          code: "artifact_resolution_failed",
          message: "preview object missing",
        },
      ]);
      return true;
    },
  );
  // END_BLOCK_BLOCK_VERIFY_ERROR_SURFACE
});
