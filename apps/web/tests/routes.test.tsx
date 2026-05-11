import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { RouterProvider, createMemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { createWebUiRoutes } from "../src/app/routes";
import type { WebUiRuntime } from "../src/app/runtime";
import type { WebUiApiClient } from "../src/lib/api/client";

const owner = {
  owner_type: "web" as const,
  owner_id: "web-console",
};

function mediaItem(overrides = {}) {
  return {
    media_item_id: "media-1",
    owner,
    kind: "text",
    status: "ready",
    display_name: "Call note",
    source: {
      source_id: "source-1",
      origin_type: "text",
      text_ref: "text:source-1",
    },
    diagnostics_count: 0,
    retention: { state: "active" },
    created_at: "2026-05-10T00:00:00Z",
    updated_at: "2026-05-10T00:00:00Z",
    ...overrides,
  };
}

function secondMediaItem() {
  return mediaItem({
    media_item_id: "media-2",
    kind: "audio",
    display_name: "Interview audio",
    source: {
      source_id: "source-2",
      origin_type: "object",
      object_key: "web-local://interview.wav",
      mime_type: "audio/wav",
      size_bytes: 2048,
    },
  });
}

function collection(overrides = {}) {
  return {
    collection_id: "collection-1",
    owner,
    kind: "user",
    name: "Research set",
    status: "active",
    version: 3,
    items: [
      {
        media_item_id: "media-1",
        position: 0,
        media_item: mediaItem(),
        added_at: "2026-05-10T00:00:00Z",
      },
    ],
    created_at: "2026-05-10T00:00:00Z",
    updated_at: "2026-05-10T00:00:00Z",
    ...overrides,
  };
}

function analysisRun(overrides = {}) {
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
    version: 2,
    delivery: { strategy: "polling" },
    evidence_gate_state: "not_required",
    artifact_count: 1,
    diagnostics_count: 2,
    artifacts: [
      {
        artifact_id: "artifact-1",
        analysis_run_id: "run-1",
        kind: "summary",
        status: "available",
        content_type: "text/markdown; charset=utf-8",
        size_bytes: 128,
        preview: { available: true, kind: "text", text_excerpt: "## Summary\n\nInterview notes" },
        created_at: "2026-05-10T00:00:00Z",
      },
      {
        artifact_id: "artifact-manifest",
        analysis_run_id: "run-1",
        kind: "run_manifest",
        status: "available",
        content_type: "application/json; charset=utf-8",
        size_bytes: 512,
        preview: {
          available: true,
          kind: "text",
          format: "json",
          text_excerpt: JSON.stringify({
            schema_version: "analysis_run_manifest/v2",
            summary: { included_count: 1, skipped_count: 0, failed_count: 0 },
            items: [
              {
                selection_item_id: "selection-item-1",
                media_item_id: "media-1",
                position: 0,
                outcome: "succeeded",
                included: true,
                lineage: { source_id: "source-1", role: "primary" },
                artifact_kinds: ["summary", "run_manifest"],
                diagnostic_ids: [],
              },
            ],
          }),
        },
        created_at: "2026-05-10T00:00:00Z",
      },
      {
        artifact_id: "artifact-diagnostics",
        analysis_run_id: "run-1",
        kind: "run_diagnostics",
        status: "available",
        content_type: "application/json; charset=utf-8",
        size_bytes: 256,
        preview: { available: true, kind: "text", format: "json", text_excerpt: "{\"diagnostics\":[]}" },
        created_at: "2026-05-10T00:00:00Z",
      },
    ],
    diagnostics: [],
    created_at: "2026-05-10T00:00:00Z",
    ...overrides,
  };
}

function makeRuntime(overrides: Partial<WebUiApiClient> = {}) {
  const softDeletedAt = "2026-05-10T01:00:00Z";
  const apiClient: WebUiApiClient = {
    listMediaItems: vi.fn().mockResolvedValue({
      items: [mediaItem(), secondMediaItem()],
      page: { page_size: 50, has_more: false },
    }),
    getMediaItem: vi.fn().mockResolvedValue(mediaItem()),
    addMediaItem: vi.fn().mockResolvedValue(mediaItem({ media_item_id: "media-2", display_name: "Fresh note" })),
    removeMediaItem: vi.fn().mockResolvedValue(
      mediaItem({
        status: "deleted",
        retention: { state: "soft_deleted", deleted_at: softDeletedAt },
        deleted_at: softDeletedAt,
      }),
    ),
    getInboxCollection: vi.fn().mockResolvedValue(collection({ kind: "inbox", name: "Inbox" })),
    listCollections: vi.fn().mockResolvedValue({
      items: [collection()],
      page: { page_size: 50, has_more: false },
    }),
    getCollection: vi.fn().mockResolvedValue(collection()),
    createCollection: vi.fn().mockResolvedValue(collection({ collection_id: "collection-2", name: "Created set" })),
    updateCollection: vi.fn().mockResolvedValue(collection({ name: "Renamed set" })),
    replaceCollectionItems: vi.fn().mockResolvedValue(collection()),
    removeCollectionItem: vi.fn().mockResolvedValue(collection({ items: [] })),
    createSelection: vi.fn().mockResolvedValue({
      selection_id: "selection-2",
      owner,
      status: "sealed",
      items: [],
      created_by: "web",
      created_at: "2026-05-10T00:00:00Z",
      sealed_at: "2026-05-10T00:00:00Z",
    }),
    getSelection: vi.fn(),
    createAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ analysis_run_id: "run-2", status: "queued" })),
    listAnalysisRuns: vi.fn().mockResolvedValue({
      items: [analysisRun()],
      page: { page_size: 25, has_more: false },
    }),
    getAnalysisRun: vi.fn().mockResolvedValue(analysisRun()),
    cancelAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ status: "cancel_requested" })),
    retryAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ analysis_run_id: "run-3", status: "queued" })),
    listAnalysisRunEvents: vi.fn().mockResolvedValue({
      items: [
        {
          event_id: "event-1",
          analysis_run_id: "run-1",
          event_type: "analysis_run.progress",
          version: 2,
          status: "running",
          emitted_at: "2026-05-10T00:00:00Z",
          payload: {
            stage: "transcribing",
            message: "Running transcription pipeline",
          },
        },
      ],
      page: { page_size: 50, has_more: false },
    }),
    listArtifacts: vi.fn().mockResolvedValue({
      items: analysisRun().artifacts,
      page: { page_size: 50, has_more: false },
    }),
    getArtifact: vi.fn().mockImplementation(async (_owner, artifactId) => {
      const found = analysisRun().artifacts.find((candidate) => candidate.artifact_id === artifactId) ?? analysisRun().artifacts[0];
      const embeddedDiagnostics =
        artifactId === "artifact-manifest"
          ? [
              {
                diagnostic_id: "diagnostic-artifact",
                severity: "info",
                code: "artifact_preview_ready",
                message: "Preview generated",
                created_at: "2026-05-10T00:00:00Z",
              },
            ]
          : [];
      return {
        ...found,
        owner,
        visibility: "owner",
        download: {
          available: true,
          provider: "minio_presigned_url",
          url: `https://minio.local/${artifactId}.txt`,
          filename: `${artifactId}.txt`,
        },
        retention: { state: "active" },
        diagnostics: embeddedDiagnostics,
      };
    }),
    refreshArtifact: vi.fn().mockResolvedValue({
      ...analysisRun().artifacts[0],
      owner,
      visibility: "owner",
      download: {
        available: true,
        provider: "minio_presigned_url",
        url: "https://minio.local/refreshed-artifact-1.txt",
        filename: "refreshed-artifact-1.txt",
      },
      retention: { state: "active" },
      diagnostics: [],
    }),
    listDiagnostics: vi.fn().mockImplementation(async (_owner, filter) => {
      const diagnostics = [
        {
          diagnostic_id: "diagnostic-run",
          owner,
          subject: { subject_type: "analysis_run", subject_id: "run-1" },
          severity: "warning",
          code: "worker_failed",
          message: "Worker reported a bounded warning",
          created_at: "2026-05-10T00:00:00Z",
        },
        {
          diagnostic_id: "diagnostic-source",
          owner,
          subject: { subject_type: "source", subject_id: "source-1" },
          severity: "warning",
          code: "source_unavailable",
          message: "Source warning kept with lineage",
          created_at: "2026-05-10T00:00:00Z",
        },
        {
          diagnostic_id: "diagnostic-artifact",
          owner,
          subject: { subject_type: "artifact", subject_id: "artifact-manifest" },
          severity: "info",
          code: "artifact_preview_ready",
          message: "Preview generated",
          created_at: "2026-05-10T00:00:00Z",
        },
        {
          diagnostic_id: "diagnostic-retention",
          owner,
          subject: { subject_type: "retention", subject_id: "media-1" },
          severity: "error",
          code: "retention_hold_pending",
          message: "Retention hold prevents final cleanup",
          created_at: "2026-05-10T00:00:00Z",
        },
      ];
      return {
        items: diagnostics.filter((diagnostic) => {
          if (filter?.subjectType && diagnostic.subject.subject_type !== filter.subjectType) {
            return false;
          }
          if (filter?.subjectId && diagnostic.subject.subject_id !== filter.subjectId) {
            return false;
          }
          return true;
        }),
        page: { page_size: 50, has_more: false },
      };
    }),
    reconcileAnalysisRunQueue: vi.fn().mockResolvedValue({ reconciled: 2 }),
    getObservabilitySnapshot: vi.fn().mockResolvedValue({
      queue_tasks: 3,
      queue_lag_seconds: 42,
      cleanup_failures: 1,
      artifact_resolution_failures: 2,
      generated_at: "2026-05-10T00:00:00Z",
    }),
    subscribeToRunEvents: vi.fn().mockReturnValue({ close: vi.fn() }),
    ...overrides,
  };
  const runtime: WebUiRuntime = {
    env: {
      apiBaseUrl: "http://localhost:8080",
      wsUrl: "ws://localhost:8080/v1/ws",
    },
    apiClient,
  };
  return runtime;
}

function renderRoute(path: string, overrides?: Partial<WebUiApiClient>) {
  const runtime = makeRuntime(overrides);
  const router = createMemoryRouter(createWebUiRoutes(runtime), {
    initialEntries: [path],
  });
  const renderResult = render(<RouterProvider router={router} />);
  return { ...runtime, ...renderResult };
}

describe("createWebUiRoutes", () => {
  it("renders the inbox-first surface and adds text media through the API boundary", async () => {
    const runtime = renderRoute("/");

    expect(await screen.findByRole("heading", { name: "Inbox" })).toBeVisible();
    const primaryNav = within(screen.getByRole("navigation", { name: "Primary" }));
    expect(primaryNav.getAllByRole("link")).toHaveLength(5);
    expect(primaryNav.getByRole("link", { name: "Inbox" })).toHaveAttribute("href", "/");
    expect(primaryNav.getByRole("link", { name: "Collections" })).toHaveAttribute("href", "/collections");
    expect(primaryNav.getByRole("link", { name: "Run builder" })).toHaveAttribute("href", "/runs");
    expect(primaryNav.getByRole("link", { name: "Artifacts" })).toHaveAttribute("href", "/artifacts");
    expect(primaryNav.getByRole("link", { name: "Admin" })).toHaveAttribute("href", "/diagnostics");
    expect(await screen.findByText("Call note")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Display name"), { target: { value: "Fresh note" } });
    fireEvent.change(screen.getByLabelText("Text"), { target: { value: "New meeting note" } });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));

    await waitFor(() => {
      expect(runtime.apiClient.addMediaItem).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          kind: "text",
          displayName: "Fresh note",
        }),
      );
    });
  });

  it("exposes explicit soft-delete from the inbox surface", async () => {
    const runtime = renderRoute("/");
    fireEvent.click(await within(runtime.container).findByRole("button", { name: "Soft delete Call note" }));

    await waitFor(() => {
      expect(runtime.apiClient.removeMediaItem).toHaveBeenCalledWith(owner, "media-1");
    });
    expect(await within(runtime.container).findByText("Soft-deleted Call note")).toBeVisible();
  });

  it("shows the retained soft-delete outcome on the media detail surface", async () => {
    const runtime = renderRoute("/inbox/media-1");
    fireEvent.click(await within(runtime.container).findByRole("button", { name: "Soft delete Call note" }));

    await waitFor(() => {
      expect(runtime.apiClient.removeMediaItem).toHaveBeenCalledWith(owner, "media-1");
    });
    expect(await within(runtime.container).findByText("Soft-deleted Call note")).toBeVisible();
    expect(within(runtime.container).getByText("soft_deleted")).toBeVisible();
    expect(within(runtime.container).getByText("10 May 2026, 04:00")).toBeVisible();
  });

  it("creates a collection from selected inbox items", async () => {
    const runtime = renderRoute("/");

    fireEvent.click(await screen.findByLabelText("Select Call note"));
    fireEvent.change(screen.getByLabelText("New collection"), { target: { value: "Important set" } });
    fireEvent.click(screen.getByRole("button", { name: "Create collection" }));

    await waitFor(() => {
      expect(runtime.apiClient.createCollection).toHaveBeenCalledWith(owner, {
        name: "Important set",
        items: ["media-1"],
      });
    });
  });

  it("offers keyboard-reachable bulk selection controls for selection-heavy flows", async () => {
    renderRoute("/runs");

    const selectAll = await screen.findByRole("button", { name: "Select all" });
    selectAll.focus();
    expect(selectAll).toHaveFocus();
    fireEvent.click(selectAll);

    expect(screen.getByRole("button", { name: "Create run from 2 items" })).toBeEnabled();

    const clearSelection = screen.getByRole("button", { name: "Clear selection" });
    clearSelection.focus();
    expect(clearSelection).toHaveFocus();
    fireEvent.click(clearSelection);

    expect(screen.getByRole("button", { name: "Create run from 0 items" })).toBeDisabled();
  });

  it("edits a collection by adding an inbox item", async () => {
    const runtime = renderRoute("/collections");

    fireEvent.change(await screen.findByLabelText("Add inbox item"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Add item" }));

    await waitFor(() => {
      expect(runtime.apiClient.replaceCollectionItems).toHaveBeenCalledWith(owner, "collection-1", {
        expectedVersion: 3,
        items: [
          { media_item_id: "media-1", position: 0 },
          { media_item_id: "media-2", position: 1 },
        ],
      });
    });
  });

  it("keeps run detail pinned to the sealed snapshot after collection mutation", async () => {
    const runtime = makeRuntime();
    let mutableCollection = collection();
    runtime.apiClient.listCollections = vi.fn().mockImplementation(async () => ({
      items: [mutableCollection],
      page: { page_size: 50, has_more: false },
    }));
    runtime.apiClient.getCollection = vi.fn().mockImplementation(async () => mutableCollection);
    runtime.apiClient.replaceCollectionItems = vi.fn().mockImplementation(async (_owner, _collectionId, draft) => {
      mutableCollection = collection({
        version: draft.expectedVersion + 1,
        items: [
          {
            media_item_id: "media-1",
            position: 0,
            media_item: mediaItem(),
            added_at: "2026-05-10T00:00:00Z",
          },
          {
            media_item_id: "media-2",
            position: 1,
            media_item: secondMediaItem(),
            added_at: "2026-05-10T00:00:00Z",
          },
        ],
      });
      return mutableCollection;
    });
    runtime.apiClient.getAnalysisRun = vi.fn().mockResolvedValue(analysisRun());

    const router = createMemoryRouter(createWebUiRoutes(runtime), {
      initialEntries: ["/collections"],
    });
    const collectionView = render(<RouterProvider router={router} />);

    fireEvent.change(await screen.findByLabelText("Add inbox item"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Add item" }));

    await waitFor(() => {
      expect(runtime.apiClient.replaceCollectionItems).toHaveBeenCalledWith(owner, "collection-1", {
        expectedVersion: 3,
        items: [
          { media_item_id: "media-1", position: 0 },
          { media_item_id: "media-2", position: 1 },
        ],
      });
    });

    collectionView.unmount();
    const detailRouter = createMemoryRouter(createWebUiRoutes(runtime), {
      initialEntries: ["/runs/run-1"],
    });
    render(<RouterProvider router={detailRouter} />);

    expect(await screen.findByRole("heading", { name: "summary" })).toBeVisible();
    expect(screen.getByText("#1 Call note")).toBeVisible();
    expect(screen.queryByText("Interview audio")).toBeNull();
  });

  it("creates a sealed selection before queuing a run", async () => {
    const runtime = renderRoute("/runs");

    fireEvent.click(await screen.findByLabelText("Select Call note"));
    fireEvent.change(screen.getByLabelText("Run type"), { target: { value: "summary" } });
    fireEvent.click(screen.getByRole("button", { name: "Create run from 1 items" }));

    await waitFor(() => {
      expect(runtime.apiClient.createSelection).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          items: [{ media_item_id: "media-1", position: 0 }],
        }),
      );
      expect(runtime.apiClient.createAnalysisRun).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          runType: "summary",
          selectionId: "selection-2",
        }),
      );
    });
  });

  it("preloads run planning from a collection link", async () => {
    renderRoute("/runs?collection=collection-1");

    expect(await screen.findByText("Research set")).toBeVisible();
    expect(await screen.findByText("#1 Call note")).toBeVisible();
  });

  it("renders run detail with events, artifacts, and diagnostics", async () => {
    renderRoute("/runs/run-1");

    expect(await screen.findByRole("heading", { name: "summary" })).toBeVisible();
    expect(await screen.findByText("analysis_run.progress")).toBeVisible();
    expect(await screen.findByText("transcribing: Running transcription pipeline")).toBeVisible();
    expect(await screen.findByText("worker_failed")).toBeVisible();
    expect(await screen.findAllByText("source_unavailable")).toHaveLength(2);
    expect(await screen.findByText("succeeded")).toBeVisible();
    expect(await screen.findByRole("link", { name: "summary" })).toHaveAttribute("href", "/artifacts/artifact-1");
  });

  it("opens markdown artifact previews from the artifact browser", async () => {
    const runtime = renderRoute("/artifacts/artifact-1");

    expect(await screen.findByRole("heading", { name: "Artifact browser" })).toBeVisible();
    expect(await screen.findByText(/Interview notes/)).toBeVisible();
    expect(await screen.findByRole("link", { name: "Open artifact" })).toHaveAttribute(
      "href",
      "https://minio.local/artifact-1.txt",
    );

    fireEvent.click(screen.getByRole("button", { name: "Refresh access" }));

    await waitFor(() => {
      expect(runtime.apiClient.refreshArtifact).toHaveBeenCalledWith(owner, "artifact-1");
    });
    await waitFor(() => {
      expect(screen.getByRole("link", { name: "Open artifact" })).toHaveAttribute(
        "href",
        "https://minio.local/refreshed-artifact-1.txt",
      );
    });
  });

  it("opens json artifact previews and artifact diagnostics", async () => {
    renderRoute("/artifacts/artifact-manifest");

    expect(await screen.findByText(/analysis_run_manifest\/v2/)).toBeVisible();
    expect(await screen.findByText("artifact_preview_ready")).toBeVisible();
    expect(await screen.findAllByText("artifact_preview_ready")).toHaveLength(1);
    expect(await screen.findByRole("link", { name: "run_manifest" })).toHaveAttribute(
      "href",
      "/artifacts/artifact-manifest",
    );
  });

  it("does not register the old jobs entrypoint", async () => {
    renderRoute("/jobs/job-123");

    expect(await screen.findByRole("heading", { name: "Surface not found" })).toBeVisible();
    expect(screen.getByRole("link", { name: "Open inbox" })).toHaveAttribute("href", "/");
  });

  it("exposes final admin lifecycle operations and observability", async () => {
    const runtime = renderRoute("/diagnostics");

    expect(await screen.findByText("42s")).toBeVisible();
    fireEvent.change(screen.getByLabelText("Limit"), { target: { value: "10" } });
    fireEvent.click(screen.getByRole("button", { name: "Reconcile queue" }));

    await waitFor(() => {
      expect(runtime.apiClient.reconcileAnalysisRunQueue).toHaveBeenCalledWith(10);
    });
    expect(await screen.findByText("Reconciled 2 run tasks")).toBeVisible();
  });

  it("filters retention diagnostics through the existing admin contract", async () => {
    const runtime = renderRoute("/diagnostics");

    fireEvent.change(await screen.findByLabelText("Subject"), { target: { value: "retention" } });
    fireEvent.change(screen.getByLabelText("Severity"), { target: { value: "error" } });

    await waitFor(() => {
      expect(runtime.apiClient.listDiagnostics).toHaveBeenLastCalledWith(owner, {
        subjectType: "retention",
        severity: "error",
        pageSize: 50,
      });
    });
    expect(await screen.findByText("retention_hold_pending")).toBeVisible();
  });

  it("covers inbox validation and alternate ingest modes", async () => {
    const runtime = renderRoute("/");

    fireEvent.click(await screen.findByRole("button", { name: "Add to inbox" }));
    expect(await screen.findByText("Text is required.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "URL" }));
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    expect(await screen.findByText("URL is required.")).toBeVisible();

    fireEvent.change(screen.getByLabelText("URL"), { target: { value: "https://example.test/source" } });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaItem).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          kind: "url",
          displayName: "https://example.test/source",
          source: { origin_type: "url", url: "https://example.test/source" },
        }),
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "File/media" }));
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    expect(await screen.findByText("Choose a file first.")).toBeVisible();

    const fileInput = screen.getByLabelText("File");
    fireEvent.change(fileInput, {
      target: {
        files: [new File(["voice"], "sample.wav", { type: "audio/wav", lastModified: 1700000000000 })],
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaItem).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          kind: "audio",
          displayName: "sample.wav",
          source: expect.objectContaining({
            origin_type: "object",
            original_filename: "sample.wav",
            content_type: "audio/wav",
            size_bytes: 5,
          }),
        }),
      );
    });
  });

  it("covers collection management no-op and archive branches", async () => {
    const runtime = renderRoute("/collections");

    const renameInput = await screen.findByLabelText("Rename Research set");
    fireEvent.blur(renameInput, { target: { value: "Research set" } });
    fireEvent.blur(renameInput, { target: { value: "   " } });
    expect(runtime.apiClient.updateCollection).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Archive" }));
    await waitFor(() => {
      expect(runtime.apiClient.updateCollection).toHaveBeenCalledWith(owner, "collection-1", {
        expectedVersion: 3,
        status: "archived",
      });
    });

    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    await waitFor(() => {
      expect(runtime.apiClient.removeCollectionItem).toHaveBeenCalledWith(owner, "collection-1", "media-1", 3);
    });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => {
      expect(runtime.apiClient.createCollection).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          name: "Collection 2",
          items: [],
        }),
      );
    });
  });

  it("covers run-builder validation and run-detail lifecycle branches", async () => {
    const runtime = renderRoute("/runs");

    fireEvent.click(await screen.findByLabelText("Select Call note"));
    fireEvent.change(screen.getByLabelText("Params"), { target: { value: "{not-json" } });
    fireEvent.click(screen.getByRole("button", { name: "Create run from 1 items" }));
    expect(await screen.findByText(/Expected property name/)).toBeVisible();

    const detailRuntime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          status: "succeeded",
          completed_at: "2026-05-10T00:05:00Z",
          artifacts: [],
        }),
      ),
      listAnalysisRunEvents: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listArtifacts: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    const cancelButton = await within(detailRuntime.container).findByRole("button", { name: "Cancel" });
    expect(cancelButton).toBeDisabled();

    expect(await within(detailRuntime.container).findByText("No events recorded.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("No artifacts available.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("No diagnostics.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("No source-level diagnostics.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("Selected as")).toBeVisible();

    fireEvent.click(within(detailRuntime.container).getByRole("button", { name: "Retry" }));
    await waitFor(() => {
      expect(detailRuntime.apiClient.retryAnalysisRun).toHaveBeenCalledWith(owner, "run-1");
    });
    expect(await within(detailRuntime.container).findByText(/Retry queued/)).toBeVisible();
  });

  it("covers artifact and diagnostics fallback surfaces", async () => {
    const runtime = renderRoute("/artifacts", {
      listArtifacts: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      getObservabilitySnapshot: vi.fn().mockResolvedValue(null),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("No artifacts available.")).toBeVisible();
    expect(screen.getByText("Choose an artifact from the list.")).toBeVisible();

    runtime.unmount();

    renderRoute("/diagnostics", {
      getObservabilitySnapshot: vi.fn().mockResolvedValue(null),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("Observability snapshot is not loaded.")).toBeVisible();
    expect(screen.getByText("No diagnostics.")).toBeVisible();
  });

  it("covers inbox action fallback errors", async () => {
    const runtime = renderRoute("/", {
      addMediaItem: vi.fn().mockRejectedValue("boom"),
      createCollection: vi.fn().mockRejectedValue("boom"),
    });

    fireEvent.change(await screen.findByLabelText("Text"), { target: { value: "error case" } });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    expect(await screen.findByText("Unable to add media.")).toBeVisible();

    fireEvent.click(screen.getByLabelText("Select Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Create collection" }));
    expect(await screen.findByText("Unable to create collection.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Add selected" }));
    expect(await screen.findByText("Choose a target collection.")).toBeVisible();
  });

  it("covers collection management fallback errors", async () => {
    const runtime = renderRoute("/collections", {
      createCollection: vi.fn().mockRejectedValue("boom"),
      updateCollection: vi.fn().mockRejectedValue("boom"),
      removeCollectionItem: vi.fn().mockRejectedValue("boom"),
      replaceCollectionItems: vi.fn().mockRejectedValue("boom"),
    });

    fireEvent.click(await screen.findByRole("button", { name: "Create" }));
    expect(await screen.findByText("Unable to create collection.")).toBeVisible();

    fireEvent.blur(screen.getByLabelText("Rename Research set"), { target: { value: "Renamed" } });
    expect(await screen.findByText("Unable to rename collection.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    expect(await screen.findByText("Unable to remove item.")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Add inbox item"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Add item" }));
    expect(await screen.findByText("Unable to add item.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Archive" }));
    expect(await screen.findByText("Unable to update collection.")).toBeVisible();
  });

  it("covers run, artifact, diagnostics, and media-detail fallback errors", async () => {
    renderRoute("/runs", {
      createSelection: vi.fn().mockRejectedValue("boom"),
    });
    fireEvent.click(await screen.findByLabelText("Select Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Create run from 1 items" }));
    expect(await screen.findByText("Unable to create run.")).toBeVisible();

    const detailRuntime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(analysisRun()),
      cancelAnalysisRun: vi.fn().mockRejectedValue("boom"),
      retryAnalysisRun: vi.fn().mockRejectedValue("boom"),
    });
    fireEvent.click(await within(detailRuntime.container).findByRole("button", { name: "Cancel" }));
    expect(await within(detailRuntime.container).findByText("Unable to cancel run.")).toBeVisible();
    fireEvent.click(within(detailRuntime.container).getByRole("button", { name: "Retry" }));
    expect(await within(detailRuntime.container).findByText("Unable to retry run.")).toBeVisible();
    detailRuntime.unmount();

    renderRoute("/artifacts/artifact-1", {
      listArtifacts: vi.fn().mockRejectedValue("boom"),
      getArtifact: vi.fn().mockRejectedValue("boom"),
    });
    expect(await screen.findByText("Unable to load artifacts.")).toBeVisible();

    const diagnosticsRuntime = renderRoute("/diagnostics", {
      listDiagnostics: vi.fn().mockRejectedValue("boom"),
      getObservabilitySnapshot: vi.fn().mockRejectedValue("boom"),
      reconcileAnalysisRunQueue: vi.fn().mockRejectedValue("boom"),
    });
    expect(await within(diagnosticsRuntime.container).findByText("Unable to load diagnostics.")).toBeVisible();
    fireEvent.click(within(diagnosticsRuntime.container).getByRole("button", { name: "Reconcile queue" }));
    expect(await within(diagnosticsRuntime.container).findByText("Unable to reconcile queue.")).toBeVisible();
    diagnosticsRuntime.unmount();

    renderRoute("/inbox/media-1", {
      getMediaItem: vi.fn().mockRejectedValue("boom"),
      removeMediaItem: vi.fn().mockRejectedValue("boom"),
    });
    expect(await screen.findByText("Unable to load media item.")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Soft delete media item" }));
    expect(await screen.findByText("Unable to remove media item.")).toBeVisible();
  });
});
