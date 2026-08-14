// FILE: apps/mcp-server/src/main.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Provide the executable stdio launcher for the real MCP protocol runtime.
// SCOPE: Bootstrap the domain-first MCP server, connect StdioServerTransport, and keep all protocol traffic on stdio.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: SCRIPT
// MAP_MODE: LOCALS
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Replaced the keep-alive launcher with official MCP SDK stdio transport connection.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   MARKER - Identifies the MCP stdio launcher in runtime logs.
//   isDirectRun - Detects execution as the process entrypoint.
//   main - Connects the bootstrapped MCP server to stdio transport.
//   launch-mcp-stdio - Bootstrap the SDK server and connect it to StdioServerTransport.
// END_MODULE_MAP

import { pathToFileURL } from "node:url";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { bootstrapMcpServerRuntime, describeMcpServerRuntime } from "./index.ts";

const MARKER = "[McpAdapter][main][BLOCK_LAUNCH_MCP_STDIO]";

function isDirectRun(): boolean {
  const entrypoint = process.argv[1];
  return Boolean(entrypoint && import.meta.url === pathToFileURL(entrypoint).href);
}

// START_BLOCK_BLOCK_LAUNCH_MCP_STDIO
export async function main(): Promise<void> {
  const runtime = bootstrapMcpServerRuntime();
  const transport = new StdioServerTransport();
  await runtime.domainRuntime.server.connect(transport);

  const description = describeMcpServerRuntime(runtime);
  console.error(
    `${MARKER} api_base_url=${description.apiBaseUrl} tool_count=${description.toolCount}`,
  );
}
// END_BLOCK_BLOCK_LAUNCH_MCP_STDIO

if (isDirectRun()) {
  main().catch((error: unknown) => {
    console.error("[McpAdapter][main][ERROR]", error);
    process.exit(1);
  });
}
