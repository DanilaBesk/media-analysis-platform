// FILE: apps/mcp-server/tests/index.test.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the MCP adapter bootstrap wires env, local client boundary, and SDK-backed domain runtime into one bounded surface.
// SCOPE: Verify bootstrap composition, tool exposure, marker logging, and runtime description without starting stdio.
// DEPENDS: M-MCP-ADAPTER
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: LOCALS
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Updated bootstrap verification for the real MCP runtime and final domain tool surface.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   CHANNEL_ACCOUNT_ID - Supplies the channel account fixture for runtime tool calls.
//   MEDIA_ID - Supplies the media fixture for runtime tool calls.
//   verify-runtime-bootstrap - Confirm env, client, and SDK-backed runtime composition.
//   verify-tool-entrypoints - Confirm direct test entrypoints invoke domain tools and emit the mapping marker.
//   verify-runtime-description - Confirm readiness metadata reflects registered tools and connection state.
// END_MODULE_MAP

import test from "node:test";
import assert from "node:assert/strict";

import {
  MCP_TOOL_MAPPING_MARKER,
  bootstrapMcpServerRuntime,
  callMcpTool,
  describeMcpServerRuntime,
  listMcpTools,
} from "../src/index.ts";
import type {
  McpAdapterApiClient,
  McpAdapterApiRequest,
} from "../src/client/api-client.ts";

const CHANNEL_ACCOUNT_ID = "00000000-0000-4000-8000-000000000010";
const MEDIA_ID = "00000000-0000-4000-8000-000000000001";

test("bootstrapMcpServerRuntime composes the bounded SDK runtime surface", () => {
  // START_BLOCK_BLOCK_VERIFY_RUNTIME_BOOTSTRAP
  const apiClient = {
    request: async () => ({
      status: 200,
      data: null,
    }),
  };

  const runtime = bootstrapMcpServerRuntime({
    env: {
      API_BASE_URL: "https://api.example.test",
    },
    apiClient,
  });

  assert.equal(runtime.env.apiBaseUrl, "https://api.example.test");
  assert.equal(runtime.apiClient, apiClient);
  assert.equal(runtime.domainRuntime.server.isConnected(), false);
  assert.equal(runtime.domainRuntime.listTools().length, 24);
  // END_BLOCK_BLOCK_VERIFY_RUNTIME_BOOTSTRAP
});

test("bootstrapMcpServerRuntime exposes domain tool entrypoints and mapping marker", async () => {
  // START_BLOCK_BLOCK_VERIFY_TOOL_ENTRYPOINTS
  const logs: string[] = [];
  const apiClient: McpAdapterApiClient = {
    request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
      assert.deepEqual(request, {
        path: `/v1/media-assets/${MEDIA_ID}?channel_account_id=${CHANNEL_ACCOUNT_ID}`,
      });
      return {
        status: 200,
        data: {
          media_asset: {
            media_asset_id: MEDIA_ID,
            status: "ready",
          },
        } as TPayload,
      };
    },
  };
  const runtime = bootstrapMcpServerRuntime({
    env: {
      API_BASE_URL: "https://api.example.test",
    },
    apiClient,
    logger: {
      log(message) {
        logs.push(message);
      },
    },
  });

  assert.equal(listMcpTools(runtime).length, 24);
  const result = await callMcpTool(runtime, {
    name: "get_media_asset",
    arguments: {
      channel_account_id: CHANNEL_ACCOUNT_ID,
      media_asset_id: MEDIA_ID,
    },
  });

  assert.deepEqual(result.structuredContent, {
    media_asset: {
      media_asset_id: MEDIA_ID,
      status: "ready",
    },
  });
  assert.equal(logs.length, 1);
  assert.match(logs[0] ?? "", /BLOCK_MAP_MCP_TOOL_TO_API_CALL/);
  assert.ok((logs[0] ?? "").includes(MCP_TOOL_MAPPING_MARKER));
  // END_BLOCK_BLOCK_VERIFY_TOOL_ENTRYPOINTS
});

test("describeMcpServerRuntime exposes readiness with registered domain tools", () => {
  // START_BLOCK_BLOCK_VERIFY_RUNTIME_DESCRIPTION
  const runtime = bootstrapMcpServerRuntime({
    env: {
      API_BASE_URL: "https://api.example.test",
    },
  });

  assert.deepEqual(describeMcpServerRuntime(runtime), {
    apiBaseUrl: "https://api.example.test",
    toolCount: 24,
    isConnected: false,
  });
  // END_BLOCK_BLOCK_VERIFY_RUNTIME_DESCRIPTION
});

test("bootstrapMcpServerRuntime reads API_BASE_URL from process env when options are omitted", () => {
  // START_BLOCK_BLOCK_VERIFY_PROCESS_ENV_BOOTSTRAP
  const runtimeGlobal = globalThis as unknown as {
    process?: { env?: Record<string, string | undefined> };
  };
  const originalProcess = runtimeGlobal.process;

  runtimeGlobal.process = {
    env: {
      API_BASE_URL: " https://process-env.example.test/root ",
    },
  };

  try {
    const runtime = bootstrapMcpServerRuntime();

    assert.equal(runtime.env.apiBaseUrl, "https://process-env.example.test/root");
    assert.equal(typeof runtime.apiClient.request, "function");
    assert.equal(runtime.domainRuntime.listTools().length, 24);
  } finally {
    runtimeGlobal.process = originalProcess;
  }
  // END_BLOCK_BLOCK_VERIFY_PROCESS_ENV_BOOTSTRAP
});

test("bootstrapMcpServerRuntime falls back to packet defaults when process is missing", () => {
  const runtimeGlobal = globalThis as unknown as {
    process?: { env?: Record<string, string | undefined> };
  };
  const originalProcess = runtimeGlobal.process;

  runtimeGlobal.process = undefined;

  try {
    const runtime = bootstrapMcpServerRuntime();

    assert.equal(runtime.env.apiBaseUrl, "http://localhost:8080");
    assert.equal(typeof runtime.apiClient.request, "function");
    assert.equal(listMcpTools(runtime).length, 24);
  } finally {
    runtimeGlobal.process = originalProcess;
  }
});
