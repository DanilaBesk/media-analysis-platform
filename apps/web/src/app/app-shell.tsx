// FILE: apps/web/src/app/app-shell.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Render the minimal route and layout shell that reserves UI ownership for follow-on jobs and artifact features.
// SCOPE: Provide a stable frame, packet boundary copy, and env visibility without starting jobs, artifact, or admin behavior.
// DEPENDS: M-WEB-UI
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the packet-scoped layout shell with env contract visibility.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   render-shell-frame - Provide header, navigation, and packet-boundary messaging for the app shell.
//   expose-runtime-contract - Surface the packet-local env contract without triggering feature-level API work.
// END_MODULE_MAP

import type { PropsWithChildren } from "react";
import { NavLink } from "react-router-dom";

import { useWebUiRuntime } from "./runtime-context";

// START_BLOCK_BLOCK_RENDER_APP_SHELL
export function AppShell({ children }: PropsWithChildren): JSX.Element {
  const { env } = useWebUiRuntime();

  return (
    <div className="app-shell">
      <header className="app-shell__header">
        <div>
          <p className="app-shell__eyebrow">M-WEB-UI-wave-5</p>
          <h1>Telegram Transcriber Web UI</h1>
          <p className="app-shell__copy">
            This packet consumes the API and event surfaces for create, monitor, artifact, and lineage
            flows while keeping polling authoritative and WebSocket limited to low-latency refresh
            triggers.
          </p>
        </div>
        <dl className="app-shell__env">
          <div>
            <dt>API</dt>
            <dd>{env.apiBaseUrl}</dd>
          </div>
          <div>
            <dt>WS</dt>
            <dd>{env.wsUrl}</dd>
          </div>
        </dl>
      </header>

      <nav className="app-shell__nav" aria-label="Primary">
        <NavLink to="/" end>
          Jobs workspace
        </NavLink>
        <span className="muted-text">Open job details from the authoritative jobs list.</span>
      </nav>

      <main className="app-shell__content">{children}</main>
    </div>
  );
}
// END_BLOCK_BLOCK_RENDER_APP_SHELL
