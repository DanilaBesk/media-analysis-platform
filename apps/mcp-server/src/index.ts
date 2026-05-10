// FILE: apps/mcp-server/src/index.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Bootstrap the real MCP runtime for the thin domain-first adapter surface.
// SCOPE: Resolve env, instantiate the HTTP API boundary, register final domain tools on McpServer, and expose testable entrypoints.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: SCRIPT
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Replaced the local-only adapter surface with an SDK-backed MCP runtime builder.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   bootstrap-mcp-runtime - Resolve env and create the API client plus SDK-backed domain runtime.
//   expose-tool-entrypoints - Surface tool listing and direct invocation for tests without starting stdio.
//   describe-mcp-runtime - Expose readiness state for packet-level verification.
// END_MODULE_MAP

import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";

import {
  createMcpAdapterApiClient,
  type McpAdapterApiClient,
} from "./client/api-client.ts";
import { resolveMcpServerEnv, type McpServerEnv } from "./client/runtime.ts";
import {
  MCP_TOOL_MAPPING_MARKER,
  createMcpDomainRuntime,
  type McpDomainRuntime,
} from "./tools/registry.ts";
import type { DomainMcpToolDefinition } from "./tools/mapped-tools.ts";
import type { JsonObject, McpAdapterLogger } from "./tools/protocol.ts";

export interface McpServerRuntime {
  env: McpServerEnv;
  apiClient: McpAdapterApiClient;
  domainRuntime: McpDomainRuntime;
}

export interface BootstrapMcpServerRuntimeOptions {
  env?: Record<string, string | undefined>;
  apiClient?: McpAdapterApiClient;
  logger?: McpAdapterLogger;
}

export interface McpServerRuntimeDescription {
  apiBaseUrl: string;
  toolCount: number;
  isConnected: boolean;
}

export interface McpToolCall {
  name: string;
  arguments?: JsonObject;
}

function getProcessEnv(): Record<string, string | undefined> {
  const runtime = globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  };
  return runtime.process?.env ?? {};
}

// START_BLOCK_BLOCK_BOOTSTRAP_MCP_SERVER_RUNTIME
export function bootstrapMcpServerRuntime(
  options: BootstrapMcpServerRuntimeOptions = {},
): McpServerRuntime {
  const env = resolveMcpServerEnv(options.env ?? getProcessEnv());
  const apiClient =
    options.apiClient ?? createMcpAdapterApiClient({ baseUrl: env.apiBaseUrl });

  return {
    env,
    apiClient,
    domainRuntime: createMcpDomainRuntime({
      apiClient,
      logger: options.logger,
    }),
  };
}
// END_BLOCK_BLOCK_BOOTSTRAP_MCP_SERVER_RUNTIME

// START_BLOCK_BLOCK_EXPOSE_MCP_TOOL_ENTRYPOINTS
export function listMcpTools(
  runtime: McpServerRuntime,
): readonly DomainMcpToolDefinition[] {
  return runtime.domainRuntime.listTools();
}

export function callMcpTool(
  runtime: McpServerRuntime,
  call: McpToolCall,
): Promise<CallToolResult> {
  return runtime.domainRuntime.callTool(call.name, call.arguments);
}
// END_BLOCK_BLOCK_EXPOSE_MCP_TOOL_ENTRYPOINTS

// START_BLOCK_BLOCK_DESCRIBE_MCP_SERVER_RUNTIME
export function describeMcpServerRuntime(
  runtime: McpServerRuntime,
): McpServerRuntimeDescription {
  return {
    apiBaseUrl: runtime.env.apiBaseUrl,
    toolCount: runtime.domainRuntime.listTools().length,
    isConnected: runtime.domainRuntime.server.isConnected(),
  };
}
// END_BLOCK_BLOCK_DESCRIBE_MCP_SERVER_RUNTIME

export { MCP_TOOL_MAPPING_MARKER };
