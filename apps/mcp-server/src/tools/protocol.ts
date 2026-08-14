// FILE: apps/mcp-server/src/tools/protocol.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Define shared MCP tool result helpers and adapter-side contract errors for the SDK-backed server.
// SCOPE: Keep structuredContent/text result shaping and deterministic adapter validation failures reusable across domain tools.
// DEPENDS: M-MCP-ADAPTER, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: TYPES
// MAP_MODE: EXPORTS
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Updated result helpers for the real MCP SDK runtime and domain-first tool surface.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   JsonObject - Defines structured JSON object payloads used by MCP results.
//   McpAdapterContractError - Represents deterministic adapter validation failures.
//   McpAdapterLogger - Defines the adapter logging boundary.
//   McpAdapterToolError - Describes structured tool error payloads.
//   createErrorToolResult - Shapes SDK-compatible failed tool results.
//   createSuccessToolResult - Shapes SDK-compatible successful tool results.
//   define-json-protocol - Describe JSON object payloads used by structured MCP results.
//   define-contract-error - Keep adapter-side validation failures contract-shaped without embedding business logic.
//   shape-sdk-results - Return SDK-compatible content and structuredContent envelopes.
// END_MODULE_MAP

import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";

export type JsonObject = Record<string, unknown>;

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

export class McpAdapterToolError extends Error {
  readonly code: string;
  readonly category: string;
  readonly retryable: boolean;
  readonly action: string;
  readonly details?: JsonObject;

  constructor(error: {
    code: string;
    message: string;
    category: string;
    retryable: boolean;
    action: string;
    details?: JsonObject;
  }) {
    super(error.message);
    this.name = "McpAdapterToolError";
    this.code = error.code;
    this.category = error.category;
    this.retryable = error.retryable;
    this.action = error.action;
    this.details = error.details;
  }
}

export function createSuccessToolResult(structuredContent: JsonObject): CallToolResult {
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
  category?: string;
  retryable?: boolean;
  action?: string;
  details?: JsonObject;
  correlationId?: string;
  diagnostics?: unknown[];
  conflict?: unknown;
}): CallToolResult {
  const structuredContent: JsonObject = {
    error: {
      code: error.code,
      message: error.message,
      ...(error.category ? { category: error.category } : {}),
      ...(error.retryable !== undefined ? { retryable: error.retryable } : {}),
      ...(error.action ? { action: error.action } : {}),
      ...(error.correlationId ? { correlation_id: error.correlationId } : {}),
      ...(error.details ? { details: error.details } : {}),
      ...(error.diagnostics ? { diagnostics: error.diagnostics } : {}),
      ...(error.conflict ? { conflict: error.conflict } : {}),
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
