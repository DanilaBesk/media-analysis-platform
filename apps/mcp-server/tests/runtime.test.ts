// FILE: apps/mcp-server/tests/runtime.test.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the MCP app-shell packet leaves behind the declared API base URL env contract.
// SCOPE: Verify packet-local defaulting and explicit env preservation for the shell runtime helper only.
// DEPENDS: M-MCP-ADAPTER
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added env contract verification for the MCP app-shell packet.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-default-env-contract - Confirm the shell remains runnable when API_BASE_URL is absent.
//   verify-explicit-env-contract - Confirm packet-local API_BASE_URL values are preserved for the follow-on packet.
// END_MODULE_MAP

import test from "node:test";
import assert from "node:assert/strict";

import { resolveMcpServerEnv } from "../src/client/runtime.ts";

test("resolveMcpServerEnv falls back to the packet-local default", () => {
  // START_BLOCK_BLOCK_VERIFY_DEFAULT_ENV_CONTRACT
  assert.deepEqual(resolveMcpServerEnv({ API_BASE_URL: undefined }), {
    apiBaseUrl: "http://localhost:8080",
  });
  // END_BLOCK_BLOCK_VERIFY_DEFAULT_ENV_CONTRACT
});

test("resolveMcpServerEnv preserves explicit API_BASE_URL values", () => {
  // START_BLOCK_BLOCK_VERIFY_EXPLICIT_ENV_CONTRACT
  assert.deepEqual(
    resolveMcpServerEnv({ API_BASE_URL: " https://api.example.test/base " }),
    {
      apiBaseUrl: "https://api.example.test/base",
    },
  );
  // END_BLOCK_BLOCK_VERIFY_EXPLICIT_ENV_CONTRACT
});
