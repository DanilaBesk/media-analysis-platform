// FILE: apps/web/tests/routes.test.tsx
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the Web UI routes exercise create, details, artifact, and action flows while keeping API ownership external.
// SCOPE: Render the jobs workspace and job details route against a mock runtime, then verify create, child-action, and artifact-resolve behavior.
// DEPENDS: M-WEB-UI
// LINKS: V-M-WEB-UI
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Replaced placeholder route checks with concrete jobs and details route interaction coverage.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-jobs-route-shell - Confirm the jobs route renders list data and submits create-from-url through the API boundary.
//   verify-job-details-route-shell - Confirm the details route renders artifacts and child actions through the API boundary.
// END_MODULE_MAP

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { RouterProvider, createMemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { createWebUiRoutes } from "../src/app/routes";
import type { WebUiRuntime } from "../src/app/runtime";

function makeJob(overrides: Record<string, unknown> = {}) {
  return {
    job_id: "11111111-1111-1111-1111-111111111111",
    root_job_id: "11111111-1111-1111-1111-111111111111",
    job_type: "transcription",
    status: "running",
    version: 2,
    display_name: "Example transcription",
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
    artifacts: [
      {
        artifact_id: "artifact-1",
        artifact_kind: "transcript_plain",
        filename: "transcript.txt",
        mime_type: "text/plain",
        size_bytes: 512,
        created_at: "2026-04-23T00:00:00Z",
      },
    ],
    children: [],
    created_at: "2026-04-23T00:00:00Z",
    ...overrides,
  };
}

function renderRoute(initialEntry: string, runtimeOverrides?: Partial<WebUiRuntime["apiClient"]>) {
  const runtime: WebUiRuntime = {
    env: {
      apiBaseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
    },
    apiClient: {
      listJobs: vi.fn().mockResolvedValue({
        items: [makeJob()],
        page: 1,
        page_size: 20,
      }),
      getJob: vi.fn().mockResolvedValue(
        makeJob({
          children: [
            {
              job_id: "child-1",
              job_type: "report",
              status: "queued",
              version: 1,
              job_url: "/v1/jobs/child-1",
              root_job_id: "11111111-1111-1111-1111-111111111111",
            },
          ],
        }),
      ),
      createUpload: vi.fn().mockResolvedValue([makeJob()]),
      createCombinedUpload: vi.fn().mockResolvedValue(makeJob()),
      createFromUrl: vi.fn().mockResolvedValue(
        makeJob({
          job_id: "url-job-1",
          root_job_id: "url-job-1",
          source_set: {
            source_set_id: "source-set-2",
            input_kind: "single_source",
            items: [
              {
                position: 0,
                source: {
                  source_id: "source-2",
                  source_kind: "youtube_url",
                  source_url: "https://youtu.be/example",
                },
              },
            ],
          },
        }),
      ),
      createReport: vi.fn().mockResolvedValue(
        makeJob({
          job_id: "report-job-1",
          job_type: "report",
          status: "queued",
          root_job_id: "11111111-1111-1111-1111-111111111111",
          parent_job_id: "11111111-1111-1111-1111-111111111111",
        }),
      ),
      createDeepResearch: vi.fn().mockResolvedValue(
        makeJob({
          job_id: "deep-job-1",
          job_type: "deep_research",
          status: "queued",
          root_job_id: "11111111-1111-1111-1111-111111111111",
          parent_job_id: "11111111-1111-1111-1111-111111111111",
        }),
      ),
      cancelJob: vi.fn().mockResolvedValue(makeJob({ status: "cancel_requested" })),
      retryJob: vi.fn().mockResolvedValue(
        makeJob({
          job_id: "retry-job-1",
          retry_of_job_id: "11111111-1111-1111-1111-111111111111",
          status: "queued",
        }),
      ),
      resolveArtifact: vi.fn().mockResolvedValue({
        artifact_id: "artifact-1",
        job_id: "11111111-1111-1111-1111-111111111111",
        artifact_kind: "transcript_plain",
        filename: "transcript.txt",
        mime_type: "text/plain",
        size_bytes: 512,
        created_at: "2026-04-23T00:00:00Z",
        download: {
          provider: "minio_presigned_url",
          url: "https://minio.local/transcript.txt",
          expires_at: "2026-04-23T00:10:00Z",
        },
      }),
      listJobEvents: vi.fn().mockResolvedValue([
        {
          event_id: "event-1",
          event_type: "job.updated",
          job_id: "11111111-1111-1111-1111-111111111111",
          root_job_id: "11111111-1111-1111-1111-111111111111",
          version: 2,
          emitted_at: "2026-04-23T00:00:00Z",
          payload: {
            status: "running",
            progress_stage: "transcribing",
            progress_message: "50%",
          },
        },
      ]),
      subscribeToJobEvents: vi.fn().mockReturnValue({
        close: vi.fn(),
      }),
      ...runtimeOverrides,
    },
  };
  const router = createMemoryRouter(createWebUiRoutes(runtime), {
    initialEntries: [initialEntry],
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

describe("createWebUiRoutes", () => {
  // START_BLOCK_BLOCK_VERIFY_JOBS_ROUTE_SHELL
  it("renders the jobs route and submits the url create flow through the API boundary", async () => {
    const runtime = renderRoute("/");

    expect(await screen.findByRole("heading", { name: "Telegram Transcriber Web UI" })).toBeVisible();
    expect(await screen.findByText("Example transcription")).toBeVisible();
    expect(screen.getByRole("option", { name: "Agent run" })).toBeVisible();

    fireEvent.change(screen.getByLabelText("Submission mode"), {
      target: { value: "url" },
    });
    fireEvent.change(screen.getByLabelText("YouTube URL"), {
      target: { value: "https://youtu.be/example" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create job" }));

    await waitFor(() => {
      expect(runtime.apiClient.createFromUrl).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "https://youtu.be/example",
        }),
      );
    });
  });
  // END_BLOCK_BLOCK_VERIFY_JOBS_ROUTE_SHELL

  // START_BLOCK_BLOCK_VERIFY_JOB_DETAILS_ROUTE_SHELL
  it("renders the job details route and resolves artifact plus child actions through the API boundary", async () => {
    const runtime = renderRoute("/jobs/job-123", {
      getJob: vi.fn().mockResolvedValue(
        makeJob({
          status: "succeeded",
          children: [],
        }),
      ),
    });

    expect(await screen.findByRole("heading", { name: "Job details" })).toBeVisible();
    expect(screen.getByText("Artifacts")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Resolve download" }));
    await waitFor(() => {
      expect(runtime.apiClient.resolveArtifact).toHaveBeenCalledWith("artifact-1");
    });

    fireEvent.click(screen.getByRole("button", { name: "Create report job" }));
    await waitFor(() => {
      expect(runtime.apiClient.createReport).toHaveBeenCalled();
    });
  });

  it("allows deep research from an agent_run report job with a report markdown artifact", async () => {
    const runtime = renderRoute("/jobs/agent-report-1", {
      getJob: vi.fn().mockResolvedValue(
        makeJob({
          job_id: "agent-report-1",
          root_job_id: "11111111-1111-1111-1111-111111111111",
          parent_job_id: "11111111-1111-1111-1111-111111111111",
          job_type: "agent_run",
          status: "succeeded",
          display_name: "Report agent run",
          artifacts: [
            {
              artifact_id: "report-md-1",
              artifact_kind: "report_markdown",
              filename: "report.md",
              mime_type: "text/markdown",
              size_bytes: 1024,
              created_at: "2026-04-23T00:00:00Z",
            },
          ],
          children: [],
        }),
      ),
    });

    expect(await screen.findByRole("heading", { name: "Job details" })).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Create deep research job" }));
    await waitFor(() => {
      expect(runtime.apiClient.createDeepResearch).toHaveBeenCalledWith(
        "agent-report-1",
        expect.objectContaining({
          delivery: expect.objectContaining({ strategy: "polling" }),
        }),
      );
    });
  });
  // END_BLOCK_BLOCK_VERIFY_JOB_DETAILS_ROUTE_SHELL
});
