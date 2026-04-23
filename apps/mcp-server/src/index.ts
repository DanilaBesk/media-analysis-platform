// FILE: apps/mcp-server/src/index.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Bootstrap and expose the thin MCP adapter surface that maps required tool calls onto the existing HTTP API.
// SCOPE: Resolve the packet-local env contract, instantiate the local API client boundary, create the mapped tool registry, and expose packet-local tool entrypoints for tests.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: SCRIPT
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Wired the MCP app shell to the required tool registry and packet-local call surface.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   bootstrap-mcp-shell - Resolve env and instantiate the local API boundary plus required MCP tool registry.
//   expose-tool-entrypoints - Surface tool listing and tool invocation without bringing in external SDK ownership.
//   describe-mcp-shell - Expose shell readiness state for tests and packet-level verification.
// END_MODULE_MAP

import {
  createMcpAdapterApiClient,
  type McpAdapterApiClient,
} from "./client/api-client.ts";
import { resolveMcpServerEnv, type McpServerEnv } from "./client/runtime.ts";
import {
  MCP_TOOL_MAPPING_MARKER,
  createMcpToolRegistryShell,
  type CreateMcpToolRegistryOptions,
  type McpToolRegistryShell,
} from "./tools/registry.ts";
import type {
  McpAdapterLogger,
  McpToolCall,
  McpToolDefinition,
  McpToolResult,
} from "./tools/protocol.ts";

export interface McpServerShell {
  env: McpServerEnv;
  apiClient: McpAdapterApiClient;
  toolRegistry: McpToolRegistryShell;
}

export interface BootstrapMcpServerShellOptions {
  env?: Record<string, string | undefined>;
  apiClient?: McpAdapterApiClient;
  logger?: McpAdapterLogger;
}

export interface McpServerShellDescription {
  apiBaseUrl: string;
  toolCount: number;
  hasRegisteredTools: boolean;
}

function getProcessEnv(): Record<string, string | undefined> {
  const runtime = globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  };
  return runtime.process?.env ?? {};
}

// START_BLOCK_BLOCK_BOOTSTRAP_MCP_SERVER_SHELL
export function bootstrapMcpServerShell(
  options: BootstrapMcpServerShellOptions = {},
): McpServerShell {
  const env = resolveMcpServerEnv(options.env ?? getProcessEnv());
  const apiClient =
    options.apiClient ?? createMcpAdapterApiClient({ baseUrl: env.apiBaseUrl });
  const registryOptions: CreateMcpToolRegistryOptions = {
    apiClient,
    logger: options.logger,
  };

  return {
    env,
    apiClient,
    toolRegistry: createMcpToolRegistryShell(registryOptions),
  };
}
// END_BLOCK_BLOCK_BOOTSTRAP_MCP_SERVER_SHELL

// START_BLOCK_BLOCK_EXPOSE_MCP_TOOL_ENTRYPOINTS
export function listMcpTools(shell: McpServerShell): readonly McpToolDefinition[] {
  return shell.toolRegistry.listTools();
}

export function callMcpTool(
  shell: McpServerShell,
  call: McpToolCall,
): Promise<McpToolResult> {
  return shell.toolRegistry.callTool(call);
}
// END_BLOCK_BLOCK_EXPOSE_MCP_TOOL_ENTRYPOINTS

// START_BLOCK_BLOCK_DESCRIBE_MCP_SERVER_SHELL
export function describeMcpServerShell(
  shell: McpServerShell,
): McpServerShellDescription {
  const tools = shell.toolRegistry.listTools();
  return {
    apiBaseUrl: shell.env.apiBaseUrl,
    toolCount: tools.length,
    hasRegisteredTools: shell.toolRegistry.hasTools(),
  };
}
// END_BLOCK_BLOCK_DESCRIBE_MCP_SERVER_SHELL

export { MCP_TOOL_MAPPING_MARKER };
