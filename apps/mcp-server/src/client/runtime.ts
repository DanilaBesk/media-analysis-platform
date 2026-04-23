// FILE: apps/mcp-server/src/client/runtime.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Define the app-local MCP server env contract that the shell bootstrap consumes before tool-mapping work begins.
// SCOPE: Normalize packet-local env inputs for the API base URL and expose the stable runtime shape for the shell packet and its tests.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: TYPES
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the bounded MCP app-shell env contract with packet-local API base URL defaults.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   resolve-env-contract - Normalize the packet-local API base URL used by the shell bootstrap and tests.
//   runtime-shape - Describe the server-shell runtime object that the follow-on packet will extend with real tools.
// END_MODULE_MAP

export interface McpServerEnv {
  apiBaseUrl: string;
}

export interface McpServerRuntime {
  env: McpServerEnv;
}

const DEFAULT_API_BASE_URL = "http://localhost:8080";

// START_BLOCK_BLOCK_RESOLVE_MCP_SERVER_ENV
export function resolveMcpServerEnv(
  rawEnv: Record<string, string | undefined>,
): McpServerEnv {
  return {
    apiBaseUrl: rawEnv.API_BASE_URL?.trim() || DEFAULT_API_BASE_URL,
  };
}
// END_BLOCK_BLOCK_RESOLVE_MCP_SERVER_ENV
