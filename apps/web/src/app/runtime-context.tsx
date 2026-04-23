// FILE: apps/web/src/app/runtime-context.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Keep the shell-local runtime boundary injectable so follow-on packets can extend UI behavior without inventing client-side truth.
// SCOPE: Provide the app runtime through React context and expose a guarded hook for route shells and tests.
// DEPENDS: M-WEB-UI
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the injectable shell runtime provider and guarded hook.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   provide-runtime - Share the env contract and packet-local API client boundary across the route shell.
//   guard-runtime-access - Fail fast if route shells render without the expected app runtime.
// END_MODULE_MAP

import { createContext, useContext } from "react";
import type { PropsWithChildren } from "react";

import type { WebUiRuntime } from "./runtime";

const WebUiRuntimeContext = createContext<WebUiRuntime | null>(null);

interface WebUiRuntimeProviderProps extends PropsWithChildren {
  runtime: WebUiRuntime;
}

// START_BLOCK_BLOCK_PROVIDE_WEB_UI_RUNTIME
export function WebUiRuntimeProvider({
  runtime,
  children,
}: WebUiRuntimeProviderProps): JSX.Element {
  return (
    <WebUiRuntimeContext.Provider value={runtime}>
      {children}
    </WebUiRuntimeContext.Provider>
  );
}
// END_BLOCK_BLOCK_PROVIDE_WEB_UI_RUNTIME

// START_BLOCK_BLOCK_USE_WEB_UI_RUNTIME
export function useWebUiRuntime(): WebUiRuntime {
  const runtime = useContext(WebUiRuntimeContext);

  if (!runtime) {
    throw new Error("WebUiRuntimeProvider is required before rendering app shell routes.");
  }

  return runtime;
}
// END_BLOCK_BLOCK_USE_WEB_UI_RUNTIME
