import type { PropsWithChildren } from "react";
import { NavLink } from "react-router-dom";

import { useWebUiRuntime } from "./runtime-context";

const NAV_ITEMS = [
  { to: "/", label: "Inbox", end: true },
  { to: "/collections", label: "Collections" },
  { to: "/runs", label: "Run builder" },
  { to: "/artifacts", label: "Artifacts" },
  { to: "/diagnostics", label: "Admin" },
];

export function AppShell({ children }: PropsWithChildren): JSX.Element {
  const { env } = useWebUiRuntime();

  return (
    <div className="app-shell">
      <header className="app-shell__header">
        <div>
          <p className="app-shell__eyebrow">Media Analysis</p>
          <h1>Inbox</h1>
        </div>
        <dl className="app-shell__env">
          <div>
            <dt>API</dt>
            <dd>{env.apiBaseUrl}</dd>
          </div>
          <div>
            <dt>Events</dt>
            <dd>{env.wsUrl}</dd>
          </div>
        </dl>
      </header>

      <nav className="app-shell__nav" aria-label="Primary">
        {NAV_ITEMS.map((item) => (
          <NavLink end={item.end} key={item.to} to={item.to}>
            {item.label}
          </NavLink>
        ))}
      </nav>

      <main className="app-shell__content">{children}</main>
    </div>
  );
}
