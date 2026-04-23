// FILE: apps/mcp-server/tests/api-client.test.ts
// VERSION: 1.0.0
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
//   LAST_CHANGE: v2.0.0 - Expanded the transport-boundary verification to cover multipart passthrough for MCP file-create mapping.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-request-shape - Confirm the thin client normalizes packet-local paths and JSON payloads.
//   verify-multipart-passthrough - Confirm multipart bodies stay multipart through the packet-local transport boundary.
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
    path: "/v1/jobs",
    method: "POST",
    body: { kind: "scaffold-check" },
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.url, "https://api.example.test/root/v1/jobs");
  assert.equal(calls[0]?.init?.method, "POST");
  assert.equal(calls[0]?.init?.headers instanceof Object, true);
  assert.equal(
    (calls[0]?.init?.headers as Record<string, string>)["Content-Type"],
    "application/json",
  );
  assert.equal(calls[0]?.init?.body, JSON.stringify({ kind: "scaffold-check" }));
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
      return new Response(JSON.stringify({ jobs: [] }), {
        status: 202,
        headers: {
          "content-type": "application/json",
        },
      });
    },
  });
  const body = new FormData();
  body.append("files", new Blob(["audio-data"], { type: "audio/mpeg" }), "clip.mp3");

  await client.request({
    path: "/v1/transcription-jobs",
    method: "POST",
    body,
  });

  assert.equal(calls[0]?.url, "https://api.example.test/root/v1/transcription-jobs");
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
        path: "/v1/jobs/job-123",
      }),
    (error: unknown) => {
      assert.ok(error instanceof McpAdapterApiClientError);
      assert.equal(error.path, "/v1/jobs/job-123");
      assert.equal(error.status, 409);
      assert.equal(error.code, "upstream_failure");
      assert.equal(error.message, "upstream rejected request");
      return true;
    },
  );
  // END_BLOCK_BLOCK_VERIFY_ERROR_SURFACE
});
