// FILE: apps/mcp-server/src/tools/registry.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Register domain-first tools on the official MCP SDK server while preserving a testable builder surface.
// SCOPE: Create the MCP server instance, register tools with SDK metadata, expose direct test calls, and shape known adapter errors.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Moved registration onto McpServer.registerTool while keeping deterministic unit-test access to domain handlers.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   register-sdk-tools - Register every domain tool on the official MCP SDK server.
//   expose-test-builder - Let tests inspect definitions and call handlers without starting stdio.
//   shape-known-errors - Convert adapter validation and upstream API failures into structured MCP error results.
// END_MODULE_MAP

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { ZodError } from "zod/v4";

import {
  McpAdapterApiClientError,
  type McpAdapterApiClient,
} from "../client/api-client.ts";
import {
  createDomainMcpTools,
  listDomainMcpToolDefinitions,
  type DomainMcpTool,
  type DomainMcpToolDefinition,
} from "./mapped-tools.ts";
import {
  McpAdapterContractError,
  McpAdapterToolError,
  createErrorToolResult,
  type JsonObject,
  type McpAdapterLogger,
} from "./protocol.ts";

export interface McpDomainRuntime {
  server: McpServer;
  tools: readonly DomainMcpTool[];
  listTools(): readonly DomainMcpToolDefinition[];
  callTool(name: string, arguments_?: JsonObject): Promise<CallToolResult>;
}

export interface CreateMcpDomainRuntimeOptions {
  apiClient: McpAdapterApiClient;
  logger?: McpAdapterLogger;
}

export const MCP_TOOL_MAPPING_MARKER =
  "[McpAdapter][mapDomainToolToApi][BLOCK_MAP_MCP_TOOL_TO_API_CALL]";

function describeMapping(tool: DomainMcpTool): string {
  return `${MCP_TOOL_MAPPING_MARKER} tool=${tool.name} api=${tool.apiPathHint}`;
}

function zodErrorDetails(error: ZodError): JsonObject {
  return {
    issues: error.issues.map((issue) => ({
      path: issue.path.join("."),
      message: issue.message,
    })),
  };
}

function apiErrorHint(error: McpAdapterApiClientError): {
  retryable: boolean;
  action: string;
} {
  if (error.code === "artifact_resolution_failed") {
    return {
      retryable: true,
      action: "refresh_artifact_then_retry_preview",
    };
  }
  if (error.code === "run_cancel_not_allowed") {
    return {
      retryable: false,
      action: "inspect_run_state_before_retry",
    };
  }
  if (error.code === "retry_requires_terminal_run") {
    return {
      retryable: false,
      action: "wait_for_terminal_run_before_retry",
    };
  }
  if (error.code === "collection_version_conflict") {
    return {
      retryable: false,
      action: "reload_resource_and_retry_with_latest_version",
    };
  }
  if (error.code === "not_found") {
    return {
      retryable: false,
      action: "check_resource_id_owner_scope",
    };
  }
  if (error.code === "invalid_request") {
    return {
      retryable: false,
      action: "fix_request",
    };
  }
  if ([408, 425, 429, 500, 502, 503, 504].includes(error.status)) {
    return {
      retryable: true,
      action: "retry_later",
    };
  }
  return {
    retryable: false,
    action: "inspect_upstream_error",
  };
}

function apiErrorDetails(error: McpAdapterApiClientError): JsonObject {
  return {
    path: error.path,
    status: error.status,
    ...(error.details ? { upstream_details: error.details } : {}),
  };
}

function shapeKnownError(error: unknown): CallToolResult | undefined {
  if (error instanceof ZodError) {
    return createErrorToolResult({
      code: "mcp_contract_violation",
      message: "Tool input did not match the domain contract.",
      category: "adapter_contract",
      retryable: false,
      action: "fix_tool_input",
      details: zodErrorDetails(error),
    });
  }

  if (error instanceof McpAdapterContractError) {
    return createErrorToolResult({
      code: error.code,
      message: error.message,
      category: "adapter_contract",
      retryable: false,
      action: "fix_tool_input",
      details: error.details,
    });
  }

  if (error instanceof McpAdapterToolError) {
    return createErrorToolResult({
      code: error.code,
      message: error.message,
      category: error.category,
      retryable: error.retryable,
      action: error.action,
      details: error.details,
    });
  }

  if (error instanceof McpAdapterApiClientError) {
    const hint = apiErrorHint(error);
    return createErrorToolResult({
      code: error.code ?? "api_request_failed",
      message: error.message,
      category: "upstream_api",
      retryable: hint.retryable,
      action: hint.action,
      correlationId: error.correlationId,
      details: apiErrorDetails(error),
      diagnostics: error.diagnostics,
      conflict: error.conflict,
    });
  }

  return undefined;
}

async function executeDomainTool(
  tool: DomainMcpTool,
  arguments_: JsonObject | undefined,
  logger?: McpAdapterLogger,
): Promise<CallToolResult> {
  try {
    const parsedArgs = tool.inputSchema.parse(arguments_ ?? {});
    logger?.log(describeMapping(tool));
    return await tool.execute(parsedArgs);
  } catch (error) {
    const shapedError = shapeKnownError(error);
    if (shapedError) {
      return shapedError;
    }
    throw error;
  }
}

function registerToolsOnServer(
  server: McpServer,
  tools: readonly DomainMcpTool[],
  logger?: McpAdapterLogger,
): void {
  for (const tool of tools) {
    server.registerTool(
      tool.name,
      {
        title: tool.title,
        description: tool.description,
        inputSchema: tool.inputSchema,
        outputSchema: tool.outputSchema,
        annotations: tool.annotations,
        _meta: {
          api_path_hint: tool.apiPathHint,
          examples: tool.examples,
        },
      },
      async (args) => executeDomainTool(tool, args as JsonObject, logger),
    );
  }
}

// START_BLOCK_BLOCK_CREATE_MCP_DOMAIN_RUNTIME
export function createMcpDomainRuntime({
  apiClient,
  logger,
}: CreateMcpDomainRuntimeOptions): McpDomainRuntime {
  const server = new McpServer({
    name: "media-analysis-platform",
    version: "0.1.0",
  });
  const tools = createDomainMcpTools(apiClient);
  const toolIndex = new Map(tools.map((tool) => [tool.name, tool]));

  registerToolsOnServer(server, tools, logger);

  return {
    server,
    tools,
    listTools() {
      return listDomainMcpToolDefinitions(tools);
    },
    async callTool(name, arguments_) {
      const tool = toolIndex.get(name);
      if (!tool) {
        return createErrorToolResult({
          code: "mcp_contract_violation",
          message: `Unknown MCP tool: ${name}`,
          category: "adapter_contract",
          retryable: false,
          action: "check_tool_name",
          details: {
            tool: name,
          },
        });
      }
      return executeDomainTool(tool, arguments_, logger);
    },
  };
}
// END_BLOCK_BLOCK_CREATE_MCP_DOMAIN_RUNTIME
