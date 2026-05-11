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

test("createMcpAdapterApiClient handles 204 and text payload variants", async () => {
  // START_BLOCK_BLOCK_VERIFY_TEXT_AND_EMPTY_PAYLOADS
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test",
    fetchImpl: async (input) => {
      const url = input instanceof URL ? input.toString() : String(input);
      if (url.endsWith("/no-content")) {
        return new Response(null, { status: 204 });
      }
      if (url.endsWith("/empty-text")) {
        return new Response("", {
          status: 200,
          headers: {
            "content-type": "text/plain",
          },
        });
      }
      return new Response("accepted", {
        status: 202,
        headers: {
          "content-type": "text/plain",
        },
      });
    },
  });

  assert.deepEqual(
    await client.request({
      path: "/no-content",
    }),
    {
      status: 204,
      data: null,
    },
  );
  assert.deepEqual(
    await client.request({
      path: "/empty-text",
    }),
    {
      status: 200,
      data: null,
    },
  );
  assert.deepEqual(
    await client.request({
      path: "/plain-text",
    }),
    {
      status: 202,
      data: "accepted",
    },
  );
  // END_BLOCK_BLOCK_VERIFY_TEXT_AND_EMPTY_PAYLOADS
});

test("createMcpAdapterApiClient treats missing content-type headers as plain text payloads", async () => {
  // START_BLOCK_BLOCK_VERIFY_MISSING_CONTENT_TYPE_FALLBACK
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test",
    fetchImpl: async (input) => {
      const url = input instanceof URL ? input.toString() : String(input);
      if (url.endsWith("/empty-no-header")) {
        return new Response("", {
          status: 200,
        });
      }
      return new Response("accepted-without-header", {
        status: 202,
      });
    },
  });

  assert.deepEqual(
    await client.request({
      path: "/empty-no-header",
    }),
    {
      status: 200,
      data: null,
    },
  );
  assert.deepEqual(
    await client.request({
      path: "/plain-no-header",
    }),
    {
      status: 202,
      data: "accepted-without-header",
    },
  );
  // END_BLOCK_BLOCK_VERIFY_MISSING_CONTENT_TYPE_FALLBACK
});

test("createMcpAdapterApiClient falls back when the error payload is not an envelope object", async () => {
  // START_BLOCK_BLOCK_VERIFY_FALLBACK_ERROR_ENVELOPE
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test",
    fetchImpl: async () =>
      new Response("bad gateway", {
        status: 502,
        headers: {
          "content-type": "text/plain",
        },
      }),
  });

  await assert.rejects(
    () =>
      client.request({
        path: "/v1/media-items",
      }),
    (error: unknown) => {
      assert.ok(error instanceof McpAdapterApiClientError);
      const apiError = error as McpAdapterApiClientError;
      assert.equal(apiError.path, "/v1/media-items");
      assert.equal(apiError.status, 502);
      assert.equal(apiError.code, undefined);
      assert.equal(apiError.message, "API request failed with status 502");
      assert.equal((apiError as any).correlationId, undefined);
      assert.equal((apiError as any).details, undefined);
      assert.equal((apiError as any).diagnostics, undefined);
      return true;
    },
  );
  // END_BLOCK_BLOCK_VERIFY_FALLBACK_ERROR_ENVELOPE
});

test("createMcpAdapterApiClient falls back when payload.error is not an object", async () => {
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test",
    fetchImpl: async () =>
      new Response(
        JSON.stringify({
          error: "bad gateway",
        }),
        {
          status: 502,
          headers: {
            "content-type": "application/json",
          },
        },
      ),
  });

  await assert.rejects(
    () =>
      client.request({
        path: "/v1/media-items/error-envelope",
      }),
    (error: unknown) => {
      assert.ok(error instanceof McpAdapterApiClientError);
      const apiError = error as McpAdapterApiClientError;
      assert.equal(apiError.path, "/v1/media-items/error-envelope");
      assert.equal(apiError.status, 502);
      assert.equal(apiError.code, undefined);
      assert.equal(apiError.message, "API request failed with status 502");
      assert.equal((apiError as any).correlationId, undefined);
      assert.equal((apiError as any).details, undefined);
      assert.equal((apiError as any).diagnostics, undefined);
      assert.equal((apiError as any).conflict, undefined);
      return true;
    },
  );
});

test("createMcpAdapterApiClient preserves BodyInit variants and ignores malformed error-envelope fields", async () => {
  const calls: Array<{
    url: string;
    init: RequestInit | undefined;
  }> = [];
  const client = createMcpAdapterApiClient({
    baseUrl: "https://api.example.test/base/",
    fetchImpl: async (input, init) => {
      calls.push({
        url: input instanceof URL ? input.toString() : String(input),
        init,
      });
      if (calls.length === 3) {
        return new Response(
          JSON.stringify({
            error: {
              code: 409,
              message: { detail: "wrong type" },
              correlation_id: ["corr"],
              details: ["invalid"],
              diagnostics: {
                severity: "error",
              },
              conflict: {
                expected_version: 3,
              },
            },
          }),
          {
            status: 409,
            headers: {
              "content-type": "application/json",
            },
          },
        );
      }

      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: {
          "content-type": "application/json",
        },
      });
    },
  });

  const searchParams = new URLSearchParams({
    cursor: "c1",
  });
  const binaryBody = new Uint8Array([1, 2, 3]);

  await client.request({
    path: "v1/media-items",
    method: "POST",
    body: searchParams,
  });
  await client.request({
    path: "/v1/media-items/binary",
    method: "PUT",
    body: binaryBody,
    headers: {
      "content-type": "application/octet-stream",
    },
  });

  assert.equal(calls[0]?.url, "https://api.example.test/base/v1/media-items");
  assert.equal(calls[0]?.init?.body, searchParams);
  assert.equal(
    Object.hasOwn((calls[0]?.init?.headers as Record<string, string>) ?? {}, "Content-Type"),
    false,
  );
  assert.equal(calls[1]?.url, "https://api.example.test/base/v1/media-items/binary");
  assert.equal(calls[1]?.init?.body, binaryBody);
  assert.equal(
    (calls[1]?.init?.headers as Record<string, string>)["content-type"],
    "application/octet-stream",
  );

  await assert.rejects(
    () =>
      client.request({
        path: "/v1/media-items/error",
      }),
    (error: unknown) => {
      assert.ok(error instanceof McpAdapterApiClientError);
      const apiError = error as McpAdapterApiClientError;
      assert.equal(apiError.status, 409);
      assert.equal(apiError.code, undefined);
      assert.equal(apiError.message, "API request failed with status 409");
      assert.equal((apiError as any).correlationId, undefined);
      assert.equal((apiError as any).details, undefined);
      assert.equal((apiError as any).diagnostics, undefined);
      assert.deepEqual((apiError as any).conflict, {
        expected_version: 3,
      });
      return true;
    },
  );
});

test("createMcpAdapterApiClient preserves direct string blob and ArrayBuffer bodies", async () => {
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

  const blobBody = new Blob(["clip-bytes"], { type: "application/octet-stream" });
  const bufferBody = new ArrayBuffer(4);
  new Uint8Array(bufferBody).set([9, 8, 7, 6]);

  await client.request({
    path: "/v1/media-items/raw-text",
    method: "POST",
    body: "raw body",
  });
  await client.request({
    path: "/v1/media-items/blob",
    method: "PUT",
    body: blobBody,
  });
  await client.request({
    path: "/v1/media-items/bytes",
    method: "PATCH",
    body: bufferBody,
  });

  assert.equal(calls[0]?.url, "https://api.example.test/root/v1/media-items/raw-text");
  assert.equal(calls[0]?.init?.body, "raw body");
  assert.equal(
    Object.hasOwn((calls[0]?.init?.headers as Record<string, string>) ?? {}, "Content-Type"),
    false,
  );
  assert.equal(calls[1]?.url, "https://api.example.test/root/v1/media-items/blob");
  assert.equal(calls[1]?.init?.body, blobBody);
  assert.equal(
    Object.hasOwn((calls[1]?.init?.headers as Record<string, string>) ?? {}, "Content-Type"),
    false,
  );
  assert.equal(calls[2]?.url, "https://api.example.test/root/v1/media-items/bytes");
  assert.equal(calls[2]?.init?.body, bufferBody);
  assert.equal(
    Object.hasOwn((calls[2]?.init?.headers as Record<string, string>) ?? {}, "Content-Type"),
    false,
  );
});
