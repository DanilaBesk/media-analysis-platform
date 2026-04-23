// FILE: apps/mcp-server/src/main.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Provide the compose-ready executable launcher for the MCP adapter shell.
// SCOPE: Bootstrap the mapped MCP shell, log readiness, and keep the process alive for runtime health convergence.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-INFRA-COMPOSE
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: SCRIPT
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the compose-ready MCP adapter launcher for runtime health convergence.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   launch-mcp-adapter - Bootstrap the MCP shell, log readiness, and keep the process alive under compose.
// END_MODULE_MAP

import { bootstrapMcpServerShell, describeMcpServerShell } from "./index.ts";

const MARKER = "[McpAdapter][main][BLOCK_LAUNCH_MCP_ADAPTER]";

export function main(): void {
  const shell = bootstrapMcpServerShell();
  const description = describeMcpServerShell(shell);
  console.info(`${MARKER} api_base_url=${description.apiBaseUrl} tool_count=${description.toolCount}`);
}

main();
setInterval(() => undefined, 60_000);
