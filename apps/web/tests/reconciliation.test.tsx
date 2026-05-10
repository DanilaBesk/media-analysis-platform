import { act, render, screen, waitFor } from "@testing-library/react";
import { RouterProvider, createMemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { createWebUiRoutes } from "../src/app/routes";
import type { WebUiRuntime } from "../src/app/runtime";
import { RECONCILE_STATE_MARKER } from "../src/lib/api/client";
import type { RunEvent } from "../src/lib/api/types";

const owner = {
  owner_type: "web" as const,
  owner_id: "web-console",
};

function runSnapshot(version = 2) {
  return {
    analysis_run_id: "run-1",
    owner,
    selection_id: "selection-1",
    selection: {
      selection_id: "selection-1",
      owner,
      status: "sealed",
      items: [
        {
          position: 0,
          media_item_id: "media-1",
          kind: "text",
          source_snapshot: { source_id: "source-1", origin_type: "text" },
          display_name: "Call note",
          status_at_selection: "ready",
          retention_snapshot: { state: "active" },
        },
      ],
      option_snapshot: {},
      created_by: "web",
      created_at: "2026-05-10T00:00:00Z",
      sealed_at: "2026-05-10T00:00:00Z",
    },
    run_type: "summary",
    status: "running",
    version,
    delivery: { strategy: "polling" },
    evidence_gate_state: "not_required",
    artifacts: [],
    diagnostics: [],
    created_at: "2026-05-10T00:00:00Z",
  };
}

function renderDetail(overrides: Partial<WebUiRuntime["apiClient"]>) {
  const runtime: WebUiRuntime = {
    env: {
      apiBaseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
    },
    apiClient: {
      listMediaItems: vi.fn(),
      getMediaItem: vi.fn(),
      addMediaItem: vi.fn(),
      removeMediaItem: vi.fn(),
      getInboxCollection: vi.fn(),
      listCollections: vi.fn(),
      getCollection: vi.fn(),
      createCollection: vi.fn(),
      updateCollection: vi.fn(),
      replaceCollectionItems: vi.fn(),
      removeCollectionItem: vi.fn(),
      createSelection: vi.fn(),
      getSelection: vi.fn(),
      createAnalysisRun: vi.fn(),
      listAnalysisRuns: vi.fn(),
      getAnalysisRun: vi.fn().mockResolvedValue(runSnapshot()),
      cancelAnalysisRun: vi.fn(),
      retryAnalysisRun: vi.fn(),
      listAnalysisRunEvents: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listArtifacts: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      getArtifact: vi.fn(),
      refreshArtifact: vi.fn(),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      reconcileAnalysisRunQueue: vi.fn(),
      getObservabilitySnapshot: vi.fn().mockResolvedValue({
        queue_tasks: 0,
        queue_lag_seconds: 0,
        cleanup_failures: 0,
        artifact_resolution_failures: 0,
        generated_at: "2026-05-10T00:00:00Z",
      }),
      subscribeToRunEvents: vi.fn().mockReturnValue({ close: vi.fn() }),
      ...overrides,
    },
  };
  const router = createMemoryRouter(createWebUiRoutes(runtime), {
    initialEntries: ["/runs/run-1"],
  });

  render(<RouterProvider router={router} />);
  return runtime;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("run detail reconciliation", () => {
  it("re-reads the run when the event stream jumps ahead", async () => {
    const consoleSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let emit: ((event: RunEvent) => void) | undefined;
    const runtime = renderDetail({
      subscribeToRunEvents: vi.fn().mockImplementation((options) => {
        emit = options.onMessage;
        return { close: vi.fn() };
      }),
    });

    expect(await screen.findByRole("heading", { name: "summary" })).toBeVisible();
    expect(runtime.apiClient.getAnalysisRun).toHaveBeenCalledTimes(1);

    act(() => {
      emit?.({
        event_id: "event-1",
        analysis_run_id: "run-1",
        event_type: "analysis_run.progress",
        version: 5,
        status: "running",
        emitted_at: "2026-05-10T00:00:01Z",
        payload: { stage: "transcribing" },
      });
    });

    await waitFor(() => {
      expect(runtime.apiClient.getAnalysisRun).toHaveBeenCalledTimes(2);
    });
    expect(consoleSpy).toHaveBeenCalledWith("%s analysis_run_id=%s", RECONCILE_STATE_MARKER, "run-1");
  });

  it("ignores stream events for another run", async () => {
    let emit: ((event: RunEvent) => void) | undefined;
    const runtime = renderDetail({
      subscribeToRunEvents: vi.fn().mockImplementation((options) => {
        emit = options.onMessage;
        return { close: vi.fn() };
      }),
    });

    expect(await screen.findByRole("heading", { name: "summary" })).toBeVisible();
    act(() => {
      emit?.({
        event_id: "event-2",
        analysis_run_id: "run-2",
        event_type: "analysis_run.progress",
        version: 9,
        status: "running",
        emitted_at: "2026-05-10T00:00:02Z",
        payload: { stage: "transcribing" },
      });
    });

    expect(runtime.apiClient.getAnalysisRun).toHaveBeenCalledTimes(1);
  });
});
