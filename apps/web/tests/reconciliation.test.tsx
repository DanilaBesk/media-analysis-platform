// FILE: apps/web/tests/reconciliation.test.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the Web UI details route keeps polling authoritative while websocket updates only trigger REST reconciliation.
// SCOPE: Verify reconnect and version-gap flows emit the reconcile marker and re-read details through the API boundary.
// DEPENDS: M-WEB-UI, M-API-EVENTS
// LINKS: V-M-WEB-UI
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added packet-local coverage for websocket-triggered REST reconciliation.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-version-gap-reconcile - Confirm a version gap emits the marker and triggers a REST refresh.
//   verify-reconnect-reconcile - Confirm websocket reconnect triggers a REST refresh even without trusting stale client state.
// END_MODULE_MAP

import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { RouterProvider, createMemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { createWebUiRoutes } from "../src/app/routes";
import type { WebUiRuntime } from "../src/app/runtime";
import { RECONCILE_STATE_MARKER } from "../src/lib/api/client";

function makeJob() {
  return {
    job_id: "job-1",
    root_job_id: "job-1",
    job_type: "transcription",
    status: "running",
    version: 4,
    delivery: { strategy: "polling" },
    source_set: {
      source_set_id: "source-set-1",
      input_kind: "single_source",
      items: [
        {
          position: 0,
          source: {
            source_id: "source-1",
            source_kind: "uploaded_file",
            original_filename: "clip.mp3",
          },
        },
      ],
    },
    artifacts: [],
    children: [],
    created_at: "2026-04-23T00:00:00Z",
  };
}

function createDeferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((nextResolve) => {
    resolve = nextResolve;
  });
  return {
    promise,
    resolve,
  };
}

function renderDetails(runtimeOverrides: Partial<WebUiRuntime["apiClient"]>) {
  const runtime: WebUiRuntime = {
    env: {
      apiBaseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
    },
    apiClient: {
      listJobs: vi.fn(),
      getJob: vi.fn().mockResolvedValue(makeJob()),
      createUpload: vi.fn(),
      createCombinedUpload: vi.fn(),
      createFromUrl: vi.fn(),
      createReport: vi.fn(),
      createDeepResearch: vi.fn(),
      cancelJob: vi.fn(),
      retryJob: vi.fn(),
      resolveArtifact: vi.fn(),
      listJobEvents: vi.fn().mockResolvedValue([]),
      subscribeToJobEvents: vi.fn().mockReturnValue({ close: vi.fn() }),
      ...runtimeOverrides,
    },
  };
  const router = createMemoryRouter(createWebUiRoutes(runtime), {
    initialEntries: ["/jobs/job-1"],
    future: {
      v7_fetcherPersist: true,
      v7_normalizeFormMethod: true,
      v7_partialHydration: true,
      v7_relativeSplatPath: true,
      v7_skipActionErrorRevalidation: true,
      v7_startTransition: true,
    },
  });

  render(
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
    />,
  );
  return runtime;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("job details reconciliation", () => {
  // START_BLOCK_BLOCK_VERIFY_VERSION_GAP_RECONCILE
  it("re-fetches from REST when websocket version jumps ahead", async () => {
    const consoleSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    let handlers:
      | {
          onMessage: (event: {
            event_id: string;
            event_type: "job.updated";
            job_id: string;
            root_job_id: string;
            job_type: "transcription";
            job_url: string;
            version: number;
            emitted_at: string;
            payload: { status: "running" };
          }) => void;
        }
      | undefined;

    const runtime = renderDetails({
      subscribeToJobEvents: vi.fn().mockImplementation((options) => {
        handlers = {
          onMessage: options.onMessage,
        };
        return { close: vi.fn() };
      }),
    });

    expect(await screen.findByRole("heading", { name: "Job details" })).toBeVisible();
    expect(runtime.apiClient.getJob).toHaveBeenCalledTimes(1);

    act(() => {
      handlers?.onMessage({
        event_id: "event-1",
        event_type: "job.updated",
        job_id: "job-1",
        root_job_id: "job-1",
        job_type: "transcription",
        job_url: "/v1/jobs/job-1",
        version: 6,
        emitted_at: "2026-04-23T00:00:01Z",
        payload: { status: "running" },
      });
    });

    await waitFor(() => {
      expect(runtime.apiClient.getJob).toHaveBeenCalledTimes(2);
    });
    expect(consoleSpy).toHaveBeenCalledWith(
      "%s reason=%s route_job_id=%s event_job_id=%s last_seen_version=%d incoming_version=%d",
      RECONCILE_STATE_MARKER,
      "version_gap",
      "job-1",
      "job-1",
      4,
      6,
    );
  });
  // END_BLOCK_BLOCK_VERIFY_VERSION_GAP_RECONCILE

  // START_BLOCK_BLOCK_VERIFY_RECONNECT_RECONCILE
  it("re-fetches from REST on websocket reconnect", async () => {
    const consoleSpy = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const handlers: Array<{ onClose?: () => void; onOpen?: () => void }> = [];

    const runtime = renderDetails({
      getJob: vi.fn().mockResolvedValue({
        ...makeJob(),
        status: "succeeded",
      }),
      subscribeToJobEvents: vi.fn().mockImplementation((options) => {
        handlers.push({
          onClose: options.onClose,
          onOpen: options.onOpen,
        });
        return { close: vi.fn() };
      }),
    });

    expect(await screen.findByRole("heading", { name: "Job details" })).toBeVisible();
    vi.useFakeTimers();
    await act(async () => {
      handlers[0]?.onClose?.();
      vi.advanceTimersByTime(1_000);
      handlers[1]?.onOpen?.();
      await Promise.resolve();
      handlers[1]?.onClose?.();
      vi.advanceTimersByTime(1_000);
      handlers[2]?.onOpen?.();
      await Promise.resolve();
    });

    vi.useRealTimers();
    expect(runtime.apiClient.subscribeToJobEvents).toHaveBeenCalledTimes(3);
    await waitFor(() => {
      expect(runtime.apiClient.getJob).toHaveBeenCalledTimes(3);
    });
    expect(consoleSpy).toHaveBeenCalledWith("%s reason=reconnect route_job_id=%s", RECONCILE_STATE_MARKER, "job-1");
  });
  // END_BLOCK_BLOCK_VERIFY_RECONNECT_RECONCILE

  // START_BLOCK_BLOCK_VERIFY_STALE_REFRESH_IS_IGNORED
  it("keeps the newest authoritative refresh when an older request resolves later", async () => {
    const staleSnapshot = createDeferred();
    const staleEvents = createDeferred();

    renderDetails({
      getJob: vi
        .fn()
        .mockResolvedValueOnce(makeJob())
        .mockReturnValueOnce(staleSnapshot.promise)
        .mockReturnValueOnce(
          Promise.resolve({
            ...makeJob(),
            version: 6,
            status: "succeeded",
            progress: {
              stage: "completed",
              message: "Fresh authoritative snapshot",
            },
          }),
        ),
      listJobEvents: vi
        .fn()
        .mockResolvedValueOnce([])
        .mockReturnValueOnce(staleEvents.promise)
        .mockReturnValueOnce(
          Promise.resolve([
            {
              event_id: "event-2",
              event_type: "job.completed",
              job_id: "job-1",
              root_job_id: "job-1",
              version: 6,
              emitted_at: "2026-04-23T00:00:02Z",
              payload: {
                status: "succeeded",
              },
            },
          ]),
        ),
    });

    expect(await screen.findByRole("heading", { name: "Job details" })).toBeVisible();
    expect(await screen.findByText("clip.mp3")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Refresh now" }));
    fireEvent.click(screen.getByRole("button", { name: "Refresh now" }));

    await waitFor(() => {
      expect(screen.getByText(/Fresh authoritative snapshot/)).toBeVisible();
    });

    await act(async () => {
      staleSnapshot.resolve({
        ...makeJob(),
        version: 5,
        progress: {
          stage: "processing",
          message: "Stale authoritative snapshot",
        },
      });
      staleEvents.resolve([
        {
          event_id: "event-stale",
          event_type: "job.updated",
          job_id: "job-1",
          root_job_id: "job-1",
          version: 5,
          emitted_at: "2026-04-23T00:00:01Z",
          payload: {
            status: "running",
          },
        },
      ]);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(screen.getByText(/Fresh authoritative snapshot/)).toBeVisible();
      expect(screen.queryByText(/Stale authoritative snapshot/)).not.toBeInTheDocument();
    });
  });
  // END_BLOCK_BLOCK_VERIFY_STALE_REFRESH_IS_IGNORED
});
