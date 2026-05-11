import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { AppShell } from "../src/app/app-shell";
import type { WebUiRuntime } from "../src/app/runtime";
import { WebUiRuntimeProvider, useWebUiRuntime } from "../src/app/runtime-context";

const runtime: WebUiRuntime = {
  env: {
    apiBaseUrl: "http://localhost:8080",
    wsUrl: "ws://localhost:8080/v1/ws",
  },
  apiClient: {} as never,
};

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
    expect(() => render(<RuntimeProbe />)).toThrowError(
      "WebUiRuntimeProvider is required before rendering app shell routes.",
    );
  });

  it("renders the shell env block from provided runtime", () => {
    render(
      <MemoryRouter initialEntries={["/"]}>
        <WebUiRuntimeProvider runtime={runtime}>
          <AppShell>
            <p>Child content</p>
          </AppShell>
        </WebUiRuntimeProvider>
      </MemoryRouter>,
    );

    expect(screen.getByText("Media Analysis")).toBeVisible();
    expect(screen.getByText("http://localhost:8080")).toBeVisible();
    expect(screen.getByText("ws://localhost:8080/v1/ws")).toBeVisible();
    expect(screen.getByText("Child content")).toBeVisible();
  });
});
