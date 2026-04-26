// FILE: apps/web/src/app/runtime.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Define the runtime contract and env normalization rules that the Web UI implementation packet consumes.
// SCOPE: Normalize REST and WebSocket env inputs and describe the runtime object shared by routes and tests.
// DEPENDS: M-WEB-UI, M-API-HTTP, M-API-EVENTS
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: TYPES
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Simplified the runtime env helper to preserve explicit WebSocket inputs and default to the canonical API stream path.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   normalize-env - Resolve the packet-local env contract for REST and WebSocket endpoints.
//   runtime-shape - Provide the app shell runtime shape used by the provider and route bootstrap.
// END_MODULE_MAP

import type { WebUiApiClient } from "../lib/api/client";

export interface WebUiRuntimeEnv {
  apiBaseUrl: string;
  wsUrl: string;
}

export interface WebUiRuntime {
  env: WebUiRuntimeEnv;
  apiClient: WebUiApiClient;
}

const DEFAULT_API_BASE_URL = "http://localhost:8080";
const DEFAULT_WS_URL = "ws://localhost:8080/v1/ws";

// START_BLOCK_BLOCK_RESOLVE_WEB_UI_RUNTIME_ENV
export function resolveWebUiRuntimeEnv(
  rawEnv: Pick<ImportMetaEnv, "VITE_API_BASE_URL" | "VITE_WS_URL">,
): WebUiRuntimeEnv {
  const apiBaseUrl = rawEnv.VITE_API_BASE_URL?.trim() || DEFAULT_API_BASE_URL;
  const wsUrl = rawEnv.VITE_WS_URL?.trim() || DEFAULT_WS_URL;

  return {
    apiBaseUrl,
    wsUrl,
  };
}
// END_BLOCK_BLOCK_RESOLVE_WEB_UI_RUNTIME_ENV
