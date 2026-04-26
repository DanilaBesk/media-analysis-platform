// FILE: apps/web/tests/env.test.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the shell packet leaves behind the declared REST and WebSocket env contract coverage.
// SCOPE: Verify env normalization for explicit values and the packet-local fallback defaults.
// DEPENDS: M-WEB-UI
// LINKS: V-M-WEB-UI
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added env contract verification for the Web UI shell packet.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-default-env-contract - Confirm the shell remains runnable when explicit Vite env values are absent.
//   verify-explicit-env-contract - Confirm packet-local env values are preserved exactly for the follow-on packet.
// END_MODULE_MAP

import { describe, expect, it } from "vitest";

import { resolveWebUiRuntimeEnv } from "../src/app/runtime";

describe("resolveWebUiRuntimeEnv", () => {
  // START_BLOCK_BLOCK_VERIFY_DEFAULT_ENV_CONTRACT
  it("falls back to the packet-local defaults when env values are absent", () => {
    expect(
      resolveWebUiRuntimeEnv({
        VITE_API_BASE_URL: undefined,
        VITE_WS_URL: undefined,
      }),
    ).toEqual({
      apiBaseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
    });
  });
  // END_BLOCK_BLOCK_VERIFY_DEFAULT_ENV_CONTRACT

  // START_BLOCK_BLOCK_VERIFY_EXPLICIT_ENV_CONTRACT
  it("preserves explicitly configured env values", () => {
    expect(
      resolveWebUiRuntimeEnv({
        VITE_API_BASE_URL: " https://api.example.test ",
        VITE_WS_URL: " wss://ws.example.test/live ",
      }),
    ).toEqual({
      apiBaseUrl: "https://api.example.test",
      wsUrl: "wss://ws.example.test/live",
    });
  });
  // END_BLOCK_BLOCK_VERIFY_EXPLICIT_ENV_CONTRACT

});
