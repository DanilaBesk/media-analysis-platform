// FILE: apps/mcp-server/src/tools/protocol.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Define the packet-local MCP tool protocol surface that the adapter exposes without depending on the external MCP SDK.
// SCOPE: Describe tool definitions, tool calls, result envelopes, logger hooks, and contract-shaped adapter errors for the MCP packet.
// DEPENDS: M-MCP-ADAPTER, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: TYPES
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the packet-local MCP tool protocol types and contract error surface.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   define-tool-protocol - Describe packet-local tool definitions, calls, and structured results.
//   define-contract-error - Keep adapter-side validation failures contract-shaped without embedding business logic.
// END_MODULE_MAP

export type JsonObject = Record<string, unknown>;

export interface McpToolTextContent {
  type: "text";
  text: string;
}

export interface McpToolResult {
  content: McpToolTextContent[];
  structuredContent: JsonObject;
  isError?: boolean;
}

export interface McpToolCall {
  name: string;
  arguments?: JsonObject;
}

export interface McpToolDefinition {
  name: string;
  description: string;
  inputSchema: JsonObject;
}

export interface McpMappedTool extends McpToolDefinition {
  apiPathHint: string;
  execute(arguments_: JsonObject): Promise<McpToolResult>;
}

export interface McpAdapterLogger {
  log(message: string): void;
}

export class McpAdapterContractError extends Error {
  readonly code = "mcp_contract_violation";
  readonly details?: JsonObject;

  constructor(message: string, details?: JsonObject) {
    super(message);
    this.name = "McpAdapterContractError";
    this.details = details;
  }
}

export function createSuccessToolResult(structuredContent: JsonObject): McpToolResult {
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(structuredContent, null, 2),
      },
    ],
    structuredContent,
  };
}

export function createErrorToolResult(error: {
  code: string;
  message: string;
  details?: JsonObject;
}): McpToolResult {
  const structuredContent: JsonObject = {
    error: {
      code: error.code,
      message: error.message,
      ...(error.details ? { details: error.details } : {}),
    },
  };

  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(structuredContent, null, 2),
      },
    ],
    structuredContent,
    isError: true,
  };
}
