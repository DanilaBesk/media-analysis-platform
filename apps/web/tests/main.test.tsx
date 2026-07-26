import { beforeEach, describe, expect, it, vi } from "vitest";

const createRoot = vi.fn();
const render = vi.fn();
const createBrowserRouter = vi.fn();
const createWebUiApiClient = vi.fn();
const createWebUiRoutes = vi.fn();
const resolveWebUiRuntimeEnv = vi.fn();

vi.mock("react-dom/client", () => ({
  default: {
    createRoot,
  },
}));

vi.mock("react-router-dom", () => ({
  RouterProvider: () => null,
  createBrowserRouter,
}));

vi.mock("../src/app/routes", () => ({
  createWebUiRoutes,
}));

vi.mock("../src/app/runtime", () => ({
  resolveWebUiRuntimeEnv,
}));

vi.mock("../src/lib/api/client", () => ({
  createWebUiApiClient,
}));

describe("main bootstrap", () => {
  beforeEach(() => {
    vi.resetModules();
    createRoot.mockReset();
    render.mockReset();
    createBrowserRouter.mockReset();
    createWebUiApiClient.mockReset();
    createWebUiRoutes.mockReset();
    resolveWebUiRuntimeEnv.mockReset();
    document.body.innerHTML = '<div id="root"></div>';

    createRoot.mockReturnValue({ render });
    resolveWebUiRuntimeEnv.mockReturnValue({
      apiBaseUrl: "http://api.example.test",
      wsUrl: "ws://events.example.test/v1/ws",
      channelAccountId: "44444444-4444-4444-8444-444444444444",
    });
    createWebUiApiClient.mockReturnValue({ client: "api" });
    createWebUiRoutes.mockReturnValue([{ path: "/" }]);
    createBrowserRouter.mockReturnValue({ router: "browser" });
  });

  it("bootstraps runtime env, api client, routes, and root render", async () => {
    await import("../src/main");

    expect(resolveWebUiRuntimeEnv).toHaveBeenCalledWith(import.meta.env, window.__WEB_UI_RUNTIME__);
    expect(createWebUiApiClient).toHaveBeenCalledWith({
      baseUrl: "http://api.example.test",
      wsUrl: "ws://events.example.test/v1/ws",
    });
    expect(createWebUiRoutes).toHaveBeenCalledWith({
      env: {
        apiBaseUrl: "http://api.example.test",
        wsUrl: "ws://events.example.test/v1/ws",
        channelAccountId: "44444444-4444-4444-8444-444444444444",
      },
      apiClient: { client: "api" },
    });
    expect(createBrowserRouter).toHaveBeenCalledWith([{ path: "/" }]);
    expect(createRoot).toHaveBeenCalledWith(document.getElementById("root"));
    expect(render).toHaveBeenCalledTimes(1);
  });
});
