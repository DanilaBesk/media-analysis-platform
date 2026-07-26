import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { AppShell } from "../src/app/app-shell";
import type { WebUiRuntime } from "../src/app/runtime";
import { WebUiRuntimeProvider, useWebUiRuntime } from "../src/app/runtime-context";

const runtime: WebUiRuntime = {
  env: {
    apiBaseUrl: "http://localhost:8080",
    wsUrl: "ws://localhost:8080/v1/ws",
    channelAccountId: "55555555-5555-4555-8555-555555555555",
  },
  apiClient: {} as never,
};
const routerFuture = { v7_startTransition: true, v7_relativeSplatPath: true } as const;

function RuntimeProbe() {
  const value = useWebUiRuntime();
  return <p>{value.env.apiBaseUrl}</p>;
}

describe("WebUiRuntimeProvider", () => {
  it("provides the runtime to consumers", () => {
    render(
      <WebUiRuntimeProvider runtime={runtime}>
        <RuntimeProbe />
      </WebUiRuntimeProvider>,
    );

    expect(screen.getByText("http://localhost:8080")).toBeVisible();
  });

  it("throws when the runtime hook is used without a provider", () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    expect(() => render(<RuntimeProbe />)).toThrowError(
      "WebUiRuntimeProvider is required before rendering app shell routes.",
    );
    consoleError.mockRestore();
  });

  it("renders the shell without leaking runtime endpoints", () => {
    render(
      <MemoryRouter initialEntries={["/"]} future={routerFuture}>
        <WebUiRuntimeProvider runtime={runtime}>
          <AppShell>
            <p>Child content</p>
          </AppShell>
        </WebUiRuntimeProvider>
      </MemoryRouter>,
    );

    expect(screen.getByText("Анализ медиа")).toBeVisible();
    expect(screen.queryByText("http://localhost:8080")).toBeNull();
    expect(screen.queryByText("ws://localhost:8080/v1/ws")).toBeNull();
    expect(screen.getByText("Child content")).toBeVisible();
  });
});
