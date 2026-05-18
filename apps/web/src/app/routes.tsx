import { Outlet, type RouteObject } from "react-router-dom";

import {
  ArtifactsRouteShell,
  CollectionsRouteShell,
  DiagnosticsRouteShell,
  InboxRouteShell,
  MediaAssetDetailRouteShell,
  RouteNotFoundShell,
  RunDetailRouteShell,
  RunsRouteShell,
} from "../features/media/media-workspace";
import { AppShell } from "./app-shell";
import type { WebUiRuntime } from "./runtime";
import { WebUiRuntimeProvider } from "./runtime-context";

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

export function createWebUiRoutes(runtime: WebUiRuntime): RouteObject[] {
  return [
    {
      path: "/",
      element: <WebUiShellRoot runtime={runtime} />,
      children: [
        {
          index: true,
          element: <InboxRouteShell />,
        },
        {
          path: "inbox/:mediaAssetId",
          element: <MediaAssetDetailRouteShell />,
        },
        {
          path: "collections",
          element: <CollectionsRouteShell />,
        },
        {
          path: "runs",
          element: <RunsRouteShell />,
        },
        {
          path: "runs/:analysisRunId",
          element: <RunDetailRouteShell />,
        },
        {
          path: "artifacts",
          element: <ArtifactsRouteShell />,
        },
        {
          path: "artifacts/:artifactId",
          element: <ArtifactsRouteShell />,
        },
        {
          path: "diagnostics",
          element: <DiagnosticsRouteShell />,
        },
        {
          path: "*",
          element: <RouteNotFoundShell />,
        },
      ],
    },
  ];
}
