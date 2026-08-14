// FILE: apps/mcp-server/vitest.config.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Reserve the app-local Vitest configuration surface that the follow-on MCP adapter packet may adopt without reopening shell plumbing.
// SCOPE: Declare the bounded MCP app-shell test roots only; this scaffold does not require Vitest to execute.
// DEPENDS: M-MCP-ADAPTER
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: CONFIG
// MAP_MODE: NONE
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the packet-local test-config placeholder alongside the standalone Node test harness.
// END_CHANGE_SUMMARY
const config = {
  test: {
    include: ["tests/*.test.ts"],
  },
};

export default config;
