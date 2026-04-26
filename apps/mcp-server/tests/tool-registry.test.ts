// FILE: apps/mcp-server/tests/tool-registry.test.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the MCP adapter registry exposes the required tools, emits the required mapping marker, and preserves contract-shaped failures.
// SCOPE: Verify registry listing, tool dispatch, marker logging, and deterministic error shaping without depending on the external MCP SDK.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Replaced the empty-registry check with required tool, marker, and deterministic error verification.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-required-tool-list - Confirm the registry exposes the GRACE-required tool set.
//   verify-tool-dispatch - Confirm representative tool calls map onto the existing HTTP API.
//   verify-error-shaping - Confirm adapter validation and upstream errors remain contract-shaped.
// END_MODULE_MAP

import test from "node:test";
import assert from "node:assert/strict";

import { McpAdapterApiClientError } from "../src/client/api-client.ts";
import {
  MCP_TOOL_MAPPING_MARKER,
  createMcpToolRegistryShell,
} from "../src/tools/registry.ts";

test("createMcpToolRegistryShell exposes the required MCP tools", () => {
  // START_BLOCK_BLOCK_VERIFY_REQUIRED_TOOL_LIST
  const registry = createMcpToolRegistryShell({
    apiClient: {
      request: async () => ({
        status: 200,
        data: null,
      }),
    },
  });

  assert.equal(registry.hasTools(), true);
  assert.deepEqual(
    registry.listTools().map((tool) => tool.name),
    [
      "create_transcription_job_from_file",
      "create_transcription_job_from_url",
      "get_job",
      "list_jobs",
      "create_report_job",
      "create_deep_research_job",
      "cancel_job",
      "retry_job",
      "list_job_artifacts",
      "get_artifact_download_info",
    ],
  );
  // END_BLOCK_BLOCK_VERIFY_REQUIRED_TOOL_LIST
});

test("createMcpToolRegistryShell maps representative tools to API calls and logs the marker", async () => {
  // START_BLOCK_BLOCK_VERIFY_TOOL_DISPATCH
  const requests: unknown[] = [];
  const logs: string[] = [];
  const registry = createMcpToolRegistryShell({
    apiClient: {
      request: async (request) => {
        requests.push(request);
        if (request.path === "/v1/transcription-jobs/from-url") {
          return {
            status: 202,
            data: {
              job: {
                job_id: "job-url-1",
                status: "queued",
              },
            },
          };
        }
        if (request.path === "/v1/jobs/job-1") {
          return {
            status: 200,
            data: {
              job: {
                job_id: "job-1",
                artifacts: [
                  {
                    artifact_id: "artifact-1",
                  },
                ],
              },
            },
          };
        }
        return {
          status: 200,
          data: {
            items: [],
            page: 1,
            page_size: 20,
          },
        };
      },
    },
    logger: {
      log(message) {
        logs.push(message);
      },
    },
  });

  const createResult = await registry.callTool({
    name: "create_transcription_job_from_url",
    arguments: {
      url: "https://youtube.com/watch?v=demo",
    },
  });
  const artifactsResult = await registry.callTool({
    name: "list_job_artifacts",
    arguments: {
      job_id: "job-1",
    },
  });

  assert.equal(logs.length, 2);
  assert.ok((logs[0] ?? "").startsWith(MCP_TOOL_MAPPING_MARKER));
  assert.deepEqual(createResult.structuredContent, {
    job: {
      job_id: "job-url-1",
      status: "queued",
    },
  });
  assert.deepEqual(artifactsResult.structuredContent, {
    job_id: "job-1",
    artifacts: [{ artifact_id: "artifact-1" }],
  });
  assert.deepEqual(requests, [
    {
      path: "/v1/transcription-jobs/from-url",
      method: "POST",
      body: {
        source_kind: "youtube_url",
        url: "https://youtube.com/watch?v=demo",
        delivery: {
          strategy: "polling",
        },
      },
    },
    {
      path: "/v1/jobs/job-1",
    },
  ]);
  // END_BLOCK_BLOCK_VERIFY_TOOL_DISPATCH
});

test("createMcpToolRegistryShell tolerates agent_run snapshots for report operations", async () => {
  const registry = createMcpToolRegistryShell({
    apiClient: {
      request: async (request) => {
        if (request.path === "/v1/transcription-jobs/transcription-1/report-jobs") {
          return {
            status: 202,
            data: {
              job: {
                job_id: "agent-report-1",
                job_type: "agent_run",
                status: "queued",
                artifacts: [],
              },
            },
          };
        }
        if (request.path === "/v1/report-jobs/agent-report-1/deep-research-jobs") {
          return {
            status: 202,
            data: {
              job: {
                job_id: "agent-deep-1",
                job_type: "agent_run",
                status: "queued",
                artifacts: [],
              },
            },
          };
        }
        throw new Error(`unexpected request path ${request.path}`);
      },
    },
  });

  const reportResult = await registry.callTool({
    name: "create_report_job",
    arguments: {
      job_id: "transcription-1",
    },
  });
  const deepResearchResult = await registry.callTool({
    name: "create_deep_research_job",
    arguments: {
      job_id: "agent-report-1",
    },
  });

  assert.deepEqual(reportResult.structuredContent, {
    job: {
      job_id: "agent-report-1",
      job_type: "agent_run",
      status: "queued",
      artifacts: [],
    },
  });
  assert.deepEqual(deepResearchResult.structuredContent, {
    job: {
      job_id: "agent-deep-1",
      job_type: "agent_run",
      status: "queued",
      artifacts: [],
    },
  });
});

test("createMcpToolRegistryShell keeps adapter validation and upstream failures contract-shaped", async () => {
  // START_BLOCK_BLOCK_VERIFY_ERROR_SHAPING
  const registry = createMcpToolRegistryShell({
    apiClient: {
      request: async () => {
        throw new McpAdapterApiClientError(
          "/v1/jobs/job-1/retry",
          409,
          "upstream rejected retry",
          "retry_not_allowed",
        );
      },
    },
  });

  const validationResult = await registry.callTool({
    name: "get_job",
    arguments: {},
  });
  const upstreamResult = await registry.callTool({
    name: "retry_job",
    arguments: {
      job_id: "job-1",
    },
  });
  const unknownToolResult = await registry.callTool({
    name: "missing_tool",
  });

  assert.deepEqual(validationResult.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "job_id must be a string",
      details: {
        field: "job_id",
      },
    },
  });
  assert.equal(validationResult.isError, true);
  assert.deepEqual(upstreamResult.structuredContent, {
    error: {
      code: "retry_not_allowed",
      message: "upstream rejected retry",
      details: {
        path: "/v1/jobs/job-1/retry",
        status: 409,
      },
    },
  });
  assert.deepEqual(unknownToolResult.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "Unknown MCP tool: missing_tool",
      details: {
        tool: "missing_tool",
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_ERROR_SHAPING
});
