// FILE: apps/web/src/app/routes.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Define the minimal route tree that lets the Web UI implementation packet start inside an existing app shell.
// SCOPE: Wire the root layout, placeholder jobs routes, and a scoped fallback route without starting feature-level behavior.
// DEPENDS: M-WEB-UI
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the minimal route tree for the Web UI app shell packet.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   provide-route-runtime - Attach the app runtime to the route tree.
//   register-shell-routes - Reserve jobs and job-details paths for the follow-on packet.
// END_MODULE_MAP

import { Outlet, type RouteObject } from "react-router-dom";

import { ArtifactsRouteShell } from "../features/artifacts/artifacts-shell";
import {
  JobDetailsRouteShell,
  JobsRouteShell,
  RouteNotFoundShell,
} from "../features/jobs/jobs-shell";
import { AppShell } from "./app-shell";
import { WebUiRuntimeProvider } from "./runtime-context";
import type { WebUiRuntime } from "./runtime";

interface WebUiShellRootProps {
  runtime: WebUiRuntime;
}

function WebUiShellRoot({ runtime }: WebUiShellRootProps): JSX.Element {
  return (
    <WebUiRuntimeProvider runtime={runtime}>
      <AppShell>
        <Outlet />
      </AppShell>
    </WebUiRuntimeProvider>
  );
}

// START_BLOCK_BLOCK_CREATE_WEB_UI_ROUTES
export function createWebUiRoutes(runtime: WebUiRuntime): RouteObject[] {
  return [
    {
      path: "/",
      element: <WebUiShellRoot runtime={runtime} />,
      children: [
        {
          index: true,
          element: <JobsRouteShell />,
        },
        {
          path: "jobs/:jobId",
          element: <JobDetailsRouteShell artifactsSlot={<ArtifactsRouteShell />} />,
        },
        {
          path: "*",
          element: <RouteNotFoundShell />,
        },
      ],
    },
  ];
}
// END_BLOCK_BLOCK_CREATE_WEB_UI_ROUTES
