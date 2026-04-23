// FILE: apps/mcp-server/src/tools/registry.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Provide the thin MCP tool registry that maps required tool calls onto the existing HTTP API without adding local business logic.
// SCOPE: Register required MCP tools, expose their stable definitions, emit the required mapping marker, and preserve contract-shaped failures.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Replaced the empty shell with the required MCP tool registry and marker-aware dispatch surface.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   register-required-tools - Materialize the required MCP tool definitions for the adapter packet.
//   dispatch-tool-calls - Route packet-local tool calls to thin API mappings and shape deterministic errors.
// END_MODULE_MAP

import {
  McpAdapterApiClientError,
  type McpAdapterApiClient,
} from "../client/api-client.ts";
import { createMappedMcpTools } from "./mapped-tools.ts";
import {
  McpAdapterContractError,
  createErrorToolResult,
  type JsonObject,
  type McpAdapterLogger,
  type McpMappedTool,
  type McpToolCall,
  type McpToolDefinition,
  type McpToolResult,
} from "./protocol.ts";

export interface McpToolRegistryShell {
  listTools(): readonly McpToolDefinition[];
  callTool(call: McpToolCall): Promise<McpToolResult>;
  hasTools(): boolean;
}

export interface CreateMcpToolRegistryOptions {
  apiClient: McpAdapterApiClient;
  logger?: McpAdapterLogger;
}

export const MCP_TOOL_MAPPING_MARKER =
  "[McpAdapter][mapToolToApi][BLOCK_MAP_MCP_TOOL_TO_API_CALL]";

function toToolDefinitions(tools: McpMappedTool[]): McpToolDefinition[] {
  return tools.map((tool) => ({
    name: tool.name,
    description: tool.description,
    inputSchema: tool.inputSchema,
  }));
}

function toToolIndex(tools: McpMappedTool[]): Map<string, McpMappedTool> {
  return new Map(tools.map((tool) => [tool.name, tool]));
}

function describeMapping(tool: McpMappedTool): string {
  return `${MCP_TOOL_MAPPING_MARKER} tool=${tool.name} api=${tool.apiPathHint}`;
}

function shapeKnownError(error: unknown): McpToolResult | undefined {
  if (error instanceof McpAdapterContractError) {
    return createErrorToolResult({
      code: error.code,
      message: error.message,
      details: error.details,
    });
  }

  if (error instanceof McpAdapterApiClientError) {
    return createErrorToolResult({
      code: error.code ?? "api_request_failed",
      message: error.message,
      details: {
        path: error.path,
        status: error.status,
      },
    });
  }

  return undefined;
}

// START_BLOCK_BLOCK_CREATE_MCP_TOOL_REGISTRY
export function createMcpToolRegistryShell({
  apiClient,
  logger,
}: CreateMcpToolRegistryOptions): McpToolRegistryShell {
  const tools = createMappedMcpTools(apiClient);
  const definitions = toToolDefinitions(tools);
  const toolIndex = toToolIndex(tools);

  return {
    listTools(): readonly McpToolDefinition[] {
      return definitions;
    },
    async callTool(call: McpToolCall): Promise<McpToolResult> {
      const arguments_ = (call.arguments ?? {}) as JsonObject;
      const tool = toolIndex.get(call.name);
      if (!tool) {
        return createErrorToolResult({
          code: "mcp_contract_violation",
          message: `Unknown MCP tool: ${call.name}`,
          details: {
            tool: call.name,
          },
        });
      }

      logger?.log(describeMapping(tool));

      try {
        return await tool.execute(arguments_);
      } catch (error) {
        const shapedError = shapeKnownError(error);
        if (shapedError) {
          return shapedError;
        }
        throw error;
      }
    },
    hasTools(): boolean {
      return tools.length > 0;
    },
  };
}
// END_BLOCK_BLOCK_CREATE_MCP_TOOL_REGISTRY
