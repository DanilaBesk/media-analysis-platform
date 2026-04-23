// FILE: apps/mcp-server/tests/index.test.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the MCP adapter bootstrap wires env, local client boundary, and mapped registry into one bounded shell surface.
// SCOPE: Verify bootstrap composition, tool exposure, marker logging, and shell description without introducing direct infra ownership.
// DEPENDS: M-MCP-ADAPTER
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Expanded shell-bootstrap verification to cover mapped tools and packet-local tool entrypoints.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-shell-bootstrap - Confirm the shell composes env, client, and mapped registry without direct infra access.
//   verify-tool-entrypoints - Confirm the shell exposes tool listing and tool invocation with the required marker.
//   verify-shell-description - Confirm the shell can be described for packet-level readiness checks.
// END_MODULE_MAP

import test from "node:test";
import assert from "node:assert/strict";

import {
  MCP_TOOL_MAPPING_MARKER,
  bootstrapMcpServerShell,
  callMcpTool,
  describeMcpServerShell,
  listMcpTools,
} from "../src/index.ts";

test("bootstrapMcpServerShell composes the bounded shell surface", () => {
  // START_BLOCK_BLOCK_VERIFY_SHELL_BOOTSTRAP
  const apiClient = {
    request: async () => ({
      status: 200,
      data: null,
    }),
  };

  const shell = bootstrapMcpServerShell({
    env: {
      API_BASE_URL: "https://api.example.test",
    },
    apiClient,
  });

  assert.equal(shell.env.apiBaseUrl, "https://api.example.test");
  assert.equal(shell.apiClient, apiClient);
  assert.equal(shell.toolRegistry.hasTools(), true);
  assert.equal(shell.toolRegistry.listTools().length, 10);
  // END_BLOCK_BLOCK_VERIFY_SHELL_BOOTSTRAP
});

test("bootstrapMcpServerShell exposes tool entrypoints and mapping marker", async () => {
  // START_BLOCK_BLOCK_VERIFY_TOOL_ENTRYPOINTS
  const logs: string[] = [];
  const shell = bootstrapMcpServerShell({
    env: {
      API_BASE_URL: "https://api.example.test",
    },
    apiClient: {
      request: async () => ({
        status: 200,
        data: {
          job: {
            job_id: "job-1",
            status: "queued",
          },
        },
      }),
    },
    logger: {
      log(message) {
        logs.push(message);
      },
    },
  });

  assert.equal(listMcpTools(shell).length, 10);
  const result = await callMcpTool(shell, {
    name: "get_job",
    arguments: {
      job_id: "job-1",
    },
  });

  assert.deepEqual(result.structuredContent, {
    job: {
      job_id: "job-1",
      status: "queued",
    },
  });
  assert.equal(logs.length, 1);
  assert.match(logs[0] ?? "", /BLOCK_MAP_MCP_TOOL_TO_API_CALL/);
  assert.ok((logs[0] ?? "").includes(MCP_TOOL_MAPPING_MARKER));
  // END_BLOCK_BLOCK_VERIFY_TOOL_ENTRYPOINTS
});

test("describeMcpServerShell exposes shell readiness with mapped tools", () => {
  // START_BLOCK_BLOCK_VERIFY_SHELL_DESCRIPTION
  const shell = bootstrapMcpServerShell({
    env: {
      API_BASE_URL: "https://api.example.test",
    },
  });

  assert.deepEqual(describeMcpServerShell(shell), {
    apiBaseUrl: "https://api.example.test",
    toolCount: 10,
    hasRegisteredTools: true,
  });
  // END_BLOCK_BLOCK_VERIFY_SHELL_DESCRIPTION
});
