// FILE: apps/web/src/main.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Bootstrap the standalone Web UI app shell with the packet-local runtime env and API client boundary.
// SCOPE: Resolve env values, construct the shell runtime, create the browser router, and mount the React root.
// DEPENDS: M-WEB-UI, M-API-HTTP
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: SCRIPT
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the standalone Web UI app-shell bootstrap entrypoint.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   bootstrap-runtime - Resolve env values and instantiate the thin local API boundary.
//   mount-router - Mount the route and layout shell that the follow-on UI packet will extend.
// END_MODULE_MAP

import React from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider, createBrowserRouter } from "react-router-dom";

import { createWebUiRoutes } from "./app/routes";
import "./app/styles.css";
import { resolveWebUiRuntimeEnv } from "./app/runtime";
import { createWebUiApiClient } from "./lib/api/client";

// START_BLOCK_BLOCK_BOOTSTRAP_APP_RUNTIME
const runtimeEnv = resolveWebUiRuntimeEnv(import.meta.env);
const runtime = {
  env: runtimeEnv,
  apiClient: createWebUiApiClient({
    baseUrl: runtimeEnv.apiBaseUrl,
    wsUrl: runtimeEnv.wsUrl,
  }),
};
const router = createBrowserRouter(createWebUiRoutes(runtime), {
  future: {
    v7_fetcherPersist: true,
    v7_normalizeFormMethod: true,
    v7_partialHydration: true,
    v7_relativeSplatPath: true,
    v7_skipActionErrorRevalidation: true,
    v7_startTransition: true,
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <RouterProvider
      future={{
        v7_fetcherPersist: true,
        v7_normalizeFormMethod: true,
        v7_partialHydration: true,
        v7_relativeSplatPath: true,
        v7_skipActionErrorRevalidation: true,
        v7_startTransition: true,
      }}
      router={router}
    />
  </React.StrictMode>,
);
// END_BLOCK_BLOCK_BOOTSTRAP_APP_RUNTIME
