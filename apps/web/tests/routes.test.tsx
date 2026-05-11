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

  it("covers inbox helper fallbacks, selection toggles, and source labels", async () => {
    const runtime = renderRoute("/", {
      listMediaItems: vi.fn().mockResolvedValue({
        items: [
          mediaItem({
            media_item_id: "media-url",
            display_name: "URL source",
            source: { source_id: "source-url", origin_type: "url", external_uri: "https://example.test/file" },
          }),
          mediaItem({
            media_item_id: "media-raw",
            display_name: "Raw source",
            source: { source_id: "source-raw", origin_type: "binary" },
          }),
        ],
        page: { page_size: 50, has_more: false },
      }),
      getInboxCollection: vi.fn().mockResolvedValue(null),
      listCollections: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listAnalysisRuns: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 25, has_more: false },
      }),
    });

    expect(await screen.findByText("URL source")).toBeVisible();
    expect(screen.getByText("https://example.test/file")).toBeVisible();
    expect(screen.getByText("binary")).toBeVisible();
    expect(screen.queryByText("in inbox")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Select all" }));
    expect(screen.getByRole("button", { name: "Clear selection" })).toBeEnabled();
    fireEvent.click(screen.getByLabelText("Select URL source"));
    expect(screen.getByRole("button", { name: "Create collection" })).toHaveTextContent("Create collection");
    fireEvent.click(screen.getByRole("button", { name: "Clear selection" }));
    expect(screen.getByRole("button", { name: "Clear selection" })).toBeDisabled();

    runtime.unmount();

    renderRoute("/", {
      listMediaItems: vi.fn().mockRejectedValue("boom"),
      getInboxCollection: vi.fn().mockResolvedValue(collection({ kind: "inbox", name: "Inbox" })),
      listCollections: vi.fn().mockResolvedValue({ items: [], page: { page_size: 50, has_more: false } }),
      listAnalysisRuns: vi.fn().mockResolvedValue({ items: [], page: { page_size: 25, has_more: false } }),
    });

    expect(await screen.findByText("Unable to load the workspace.")).toBeVisible();
  });

  it("covers inbox add-to-collection success and file-kind fanout", async () => {
    const runtime = renderRoute("/");

    fireEvent.click(await screen.findByLabelText("Select Interview audio"));
    fireEvent.change(screen.getByLabelText("Existing collection"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Add selected" }));

    await waitFor(() => {
      expect(runtime.apiClient.replaceCollectionItems).toHaveBeenCalledWith(owner, "collection-1", {
        expectedVersion: 3,
        items: [
          { media_item_id: "media-1", position: 0 },
          { media_item_id: "media-2", position: 1 },
        ],
      });
    });
    expect(await screen.findByText("Updated Research set")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "File/media" }));
    fireEvent.change(screen.getByLabelText("File"), {
      target: { files: [new File(["video"], "clip.mp4", { type: "video/mp4" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaItem).toHaveBeenNthCalledWith(
        1,
        owner,
        expect.objectContaining({ kind: "video", displayName: "clip.mp4" }),
      );
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Add to inbox" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("File"), {
      target: { files: [new File(["image"], "cover.png", { type: "image/png" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaItem).toHaveBeenNthCalledWith(
        2,
        owner,
        expect.objectContaining({ kind: "image", displayName: "cover.png" }),
      );
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Add to inbox" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("File"), {
      target: { files: [new File(["data"], "blob.bin", { type: "" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));

    await waitFor(() => {
      expect(runtime.apiClient.addMediaItem).toHaveBeenNthCalledWith(
        3,
        owner,
        expect.objectContaining({ kind: "file", displayName: "blob.bin" }),
      );
    });
  });

  it("covers manifest, legacy diagnostics, and non-progress event branches", async () => {
    renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          artifacts: [
            {
              artifact_id: "artifact-manifest",
              analysis_run_id: "run-1",
              kind: "run_manifest",
              status: "available",
              content_type: "application/json",
              size_bytes: 256,
              preview: {
                available: true,
                kind: "text",
                format: "json",
                text_excerpt: JSON.stringify({
                  items: [
                    {
                      media_item_id: "media-1",
                      position: 0,
                      outcome: "failed",
                      selection_item_id: "selection-item-1",
                    },
                  ],
                }),
              },
              created_at: "2026-05-10T00:00:00Z",
            },
          ],
        }),
      ),
      listArtifacts: vi.fn().mockResolvedValue({
        items: [
          {
            artifact_id: "artifact-manifest",
            analysis_run_id: "run-1",
            kind: "run_manifest",
            status: "available",
            content_type: "application/json",
            size_bytes: 256,
            preview: {
              available: true,
              kind: "text",
              format: "json",
              text_excerpt: "{not-json",
            },
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
      listAnalysisRunEvents: vi.fn().mockResolvedValue({
        items: [
          {
            event_id: "event-plain",
            analysis_run_id: "run-1",
            event_type: "analysis_run.note",
            version: 3,
            emitted_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [
          {
            diagnostic_id: "legacy-source",
            owner,
            subject_type: "source",
            subject_id: "source-1",
            severity: "warning",
            code: "legacy_source_warning",
            message: "Legacy payload kept readable.",
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText(/selection .*item-1/)).toBeVisible();
    expect(screen.getByText("failed")).toBeVisible();
    expect(screen.getByText("None")).toBeVisible();
    expect(screen.getAllByText("legacy_source_warning")).toHaveLength(2);
    expect(screen.queryByText(/analysis_run.progress/)).toBeNull();
  });

  it("covers artifact grouping labels and preview fallbacks", async () => {
    renderRoute("/artifacts/artifact-json-invalid", {
      listArtifacts: vi.fn().mockResolvedValue({
        items: [
          {
            artifact_id: "artifact-structured",
            analysis_run_id: "run-1",
            kind: "structured_data",
            status: "available",
            content_type: "application/json",
            size_bytes: 128,
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            artifact_id: "artifact-log",
            analysis_run_id: "run-1",
            kind: "execution_log",
            status: "available",
            content_type: "text/plain",
            size_bytes: 128,
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            artifact_id: "artifact-custom",
            analysis_run_id: "run-2",
            kind: "custom_blob",
            status: "available",
            content_type: "application/octet-stream",
            size_bytes: 2_500_000,
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
      getArtifact: vi.fn().mockResolvedValue({
        artifact_id: "artifact-json-invalid",
        analysis_run_id: "run-1",
        kind: "structured_data",
        status: "available",
        content_type: "application/octet-stream",
        size_bytes: 2_500_000,
        preview: {
          available: true,
          kind: "text",
          format: "json",
          text_excerpt: "{invalid-json",
        },
        created_at: "2026-05-10T00:00:00Z",
        diagnostics: [],
        download: { available: false, url: "" },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("Structured data")).toBeVisible();
    expect(screen.getByText("Execution logs")).toBeVisible();
    expect(screen.getByText("custom blob")).toBeVisible();
    expect(screen.getByText("{invalid-json")).toBeVisible();
    expect(screen.getAllByText("2.4 MB")).toHaveLength(2);
    expect(screen.queryByRole("link", { name: "Open artifact" })).toBeNull();
  });

  it("covers non-record manifest previews, missing collection labels, and generic run load fallback", async () => {
    const runRuntime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockRejectedValue("boom"),
      listAnalysisRunEvents: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listArtifacts: vi.fn().mockResolvedValue({
        items: [
          {
            artifact_id: "artifact-manifest",
            analysis_run_id: "run-1",
            kind: "run_manifest",
            status: "available",
            content_type: "application/json",
            size_bytes: 64,
            preview: { available: true, kind: "text", format: "json", text_excerpt: "[]" },
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await within(runRuntime.container).findByText("Unable to load run.")).toBeVisible();
    runRuntime.unmount();
  });

  it("covers collection, runs, artifact, admin, and media refresh callbacks", async () => {
    const collectionsRuntime = renderRoute("/collections", {
      listCollections: vi.fn().mockResolvedValue({
        items: [
          collection({
            items: [{ media_item_id: "media-opaque", position: 0, added_at: "2026-05-10T00:00:00Z" }],
          }),
        ],
        page: { page_size: 50, has_more: false },
      }),
      getCollection: vi.fn().mockResolvedValue(
        collection({
          items: [{ media_item_id: "media-opaque", position: 0, added_at: "2026-05-10T00:00:00Z" }],
        }),
      ),
    });

    fireEvent.change(await screen.findByLabelText("Name"), { target: { value: "Curated set" } });
    fireEvent.change(screen.getByLabelText("First item"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => {
      expect(collectionsRuntime.apiClient.createCollection).toHaveBeenCalledWith(owner, {
        name: "Curated set",
        items: ["media-2"],
      });
    });
    expect(screen.getByText("media-opaque")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(collectionsRuntime.apiClient.listCollections.mock.calls.length).toBeGreaterThanOrEqual(2);
    });
    collectionsRuntime.unmount();

    const runsRuntime = renderRoute("/runs");
    fireEvent.change(await screen.findByLabelText("Collection"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByLabelText("Select Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(runsRuntime.apiClient.listAnalysisRuns).toHaveBeenCalledTimes(2);
    });
    runsRuntime.unmount();

    const artifactRuntime = renderRoute("/artifacts/artifact-1", {
      getArtifact: vi.fn().mockResolvedValue({
        artifact_id: "artifact-1",
        analysis_run_id: "run-1",
        kind: "execution_log",
        status: "available",
        content_type: "application/octet-stream",
        size_bytes: 42,
        preview: { available: false, kind: "binary" },
        created_at: "2026-05-10T00:00:00Z",
        diagnostics: [],
        download: { available: true, url: "https://minio.local/artifact-1.bin" },
      }),
      refreshArtifact: vi.fn().mockRejectedValue("boom"),
    });

    expect(await screen.findByText("No inline preview is available for this artifact.")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(artifactRuntime.apiClient.listArtifacts).toHaveBeenCalledTimes(2);
    });
    fireEvent.click(screen.getByRole("button", { name: "Refresh access" }));
    expect(await screen.findByText("Unable to refresh artifact access.")).toBeVisible();
    artifactRuntime.unmount();

    const diagnosticsRuntime = renderRoute("/diagnostics");
    fireEvent.change(await screen.findByLabelText("Limit"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(diagnosticsRuntime.apiClient.listDiagnostics).toHaveBeenCalledTimes(2);
    });
    fireEvent.click(screen.getByRole("button", { name: "Reconcile queue" }));
    await waitFor(() => {
      expect(diagnosticsRuntime.apiClient.reconcileAnalysisRunQueue).toHaveBeenCalledWith(1);
    });
    diagnosticsRuntime.unmount();

    const mediaRuntime = renderRoute("/inbox/media-1");
    fireEvent.click(await screen.findByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(mediaRuntime.apiClient.getMediaItem).toHaveBeenCalledTimes(2);
    });
  });

  it("covers inbox refresh, archived collection activation, and successful rename refresh", async () => {
    const inboxRuntime = renderRoute("/");

    fireEvent.click(await screen.findByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(inboxRuntime.apiClient.listMediaItems).toHaveBeenCalledTimes(2);
      expect(inboxRuntime.apiClient.getInboxCollection).toHaveBeenCalledTimes(2);
      expect(inboxRuntime.apiClient.listCollections).toHaveBeenCalledTimes(2);
      expect(inboxRuntime.apiClient.listAnalysisRuns).toHaveBeenCalledTimes(2);
    });
    inboxRuntime.unmount();

    const archivedRuntime = renderRoute("/collections", {
      listCollections: vi.fn().mockResolvedValue({
        items: [
          collection({
            collection_id: "collection-archived",
            name: "Archived set",
            status: "archived",
          }),
        ],
        page: { page_size: 50, has_more: false },
      }),
      updateCollection: vi.fn().mockResolvedValue(
        collection({
          collection_id: "collection-archived",
          name: "Reactivated set",
          status: "active",
          version: 4,
        }),
      ),
    });

    const archivedRename = await screen.findByLabelText("Rename Archived set");
    fireEvent.blur(archivedRename, { target: { value: "Reactivated set" } });
    await waitFor(() => {
      expect(archivedRuntime.apiClient.updateCollection).toHaveBeenCalledWith(owner, "collection-archived", {
        expectedVersion: 3,
        name: "Reactivated set",
      });
    });
    await waitFor(() => {
      expect(archivedRuntime.apiClient.listCollections).toHaveBeenCalledTimes(2);
    });

    fireEvent.click(screen.getByRole("button", { name: "Activate" }));
    await waitFor(() => {
      expect(archivedRuntime.apiClient.updateCollection).toHaveBeenCalledWith(owner, "collection-archived", {
        expectedVersion: 3,
        status: "active",
      });
    });
  });

  it("covers run-detail refresh and successful cancel lifecycle", async () => {
    const runtime = renderRoute("/runs/run-1", {
      cancelAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ status: "cancel_requested" })),
    });

    fireEvent.click(await within(runtime.container).findByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(runtime.apiClient.getAnalysisRun).toHaveBeenCalledTimes(2);
    });

    fireEvent.click(within(runtime.container).getByRole("button", { name: "Cancel" }));
    await waitFor(() => {
      expect(runtime.apiClient.cancelAnalysisRun).toHaveBeenCalledWith(owner, "run-1");
    });
    expect(await within(runtime.container).findByText("Cancel requested")).toBeVisible();
    expect(within(runtime.container).getByText("cancel_requested")).toBeVisible();
  });

  it("covers text preview fallback, kilobyte sizing, and nullable download availability", async () => {
    renderRoute("/artifacts/artifact-text-preview", {
      listArtifacts: vi.fn().mockResolvedValue({
        items: [
          {
            artifact_id: "artifact-kb",
            analysis_run_id: "run-1",
            kind: "execution_log",
            status: "available",
            content_type: "text/plain",
            size_bytes: 2048,
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
      getArtifact: vi.fn().mockResolvedValue({
        artifact_id: "artifact-text-preview",
        analysis_run_id: "run-1",
        kind: "execution_log",
        status: "available",
        content_type: "text/plain",
        size_bytes: 2048,
        created_at: "2026-05-10T00:00:00Z",
        diagnostics: [],
        download: { url: "https://minio.local/artifact-text-preview.log" },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("2.0 KB")).toBeVisible();
    expect(screen.getByText("No inline preview is available for this artifact.")).toBeVisible();
    expect(screen.getByRole("link", { name: "Open artifact" })).toHaveAttribute(
      "href",
      "https://minio.local/artifact-text-preview.log",
    );
  });

  it("covers collection-sourced run creation with empty params and artifact-count fallback", async () => {
    const runtime = renderRoute("/runs?collection=collection-1", {
      listAnalysisRuns: vi.fn().mockResolvedValue({
        items: [analysisRun({ artifact_count: undefined })],
        page: { page_size: 25, has_more: false },
      }),
    });

    expect(await screen.findByText("Research set")).toBeVisible();
    expect(await screen.findByText("#1 Call note")).toBeVisible();
    expect(screen.getByText("0")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Params"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Create run from 1 items" }));

    await waitFor(() => {
      expect(runtime.apiClient.createSelection).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          sourceCollectionId: "collection-1",
          optionSnapshot: { source: "collection" },
        }),
      );
    });
    await waitFor(() => {
      expect(runtime.apiClient.createAnalysisRun).toHaveBeenCalledWith(
        owner,
        expect.objectContaining({
          selectionId: "selection-2",
          params: undefined,
        }),
      );
    });
  });

  it("covers manifest zero-summary defaults and selection-item outcome fallback", async () => {
    const runtime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          artifacts: [
            {
              artifact_id: "artifact-manifest-zero",
              analysis_run_id: "run-1",
              kind: "run_manifest",
              status: "available",
              content_type: "application/json",
              size_bytes: 64,
              preview: {
                available: true,
                kind: "text",
                format: "json",
                text_excerpt: JSON.stringify({
                  summary: {},
                  items: [
                    {
                      media_item_id: "media-1",
                      position: 0,
                      outcome: "skipped",
                    },
                  ],
                }),
              },
              created_at: "2026-05-10T00:00:00Z",
            },
          ],
        }),
      ),
      listArtifacts: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    const outcomeTable = await within(runtime.container).findByText("included");
    const metrics = within(outcomeTable.closest(".metric-strip") as HTMLElement);
    expect(metrics.getAllByText("0")).toHaveLength(3);
    const selectionItem = screen.getByText("selection item");
    expect(selectionItem).toBeVisible();
    const outcomeRow = selectionItem.closest(".outcome-row") as HTMLElement;
    expect(within(outcomeRow).getByText("skipped")).toBeVisible();
  });

  it("covers concrete Error message branches across route surfaces", async () => {
    renderRoute("/collections", {
      createCollection: vi.fn().mockRejectedValue(new Error("Collection create exploded")),
      updateCollection: vi.fn().mockRejectedValue(new Error("Collection update exploded")),
      removeCollectionItem: vi.fn().mockRejectedValue(new Error("Collection remove exploded")),
      replaceCollectionItems: vi.fn().mockRejectedValue(new Error("Collection add exploded")),
    });

    fireEvent.click(await screen.findByRole("button", { name: "Create" }));
    expect(await screen.findByText("Collection create exploded")).toBeVisible();

    fireEvent.blur(screen.getByLabelText("Rename Research set"), { target: { value: "Renamed with error" } });
    expect(await screen.findByText("Collection update exploded")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    expect(await screen.findByText("Collection remove exploded")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Add inbox item"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Add item" }));
    expect(await screen.findByText("Collection add exploded")).toBeVisible();
  });

  it("covers concrete Error object branches for inbox and collection actions", async () => {
    const inboxLoaderRuntime = renderRoute("/", {
      listMediaItems: vi.fn().mockRejectedValue(new Error("Workspace exploded")),
      getInboxCollection: vi.fn().mockResolvedValue(collection({ kind: "inbox", name: "Inbox" })),
      listCollections: vi.fn().mockResolvedValue({
        items: [collection()],
        page: { page_size: 50, has_more: false },
      }),
      listAnalysisRuns: vi.fn().mockResolvedValue({
        items: [analysisRun()],
        page: { page_size: 25, has_more: false },
      }),
    });

    expect(await within(inboxLoaderRuntime.container).findByText("Workspace exploded")).toBeVisible();
    inboxLoaderRuntime.unmount();

    const inboxRuntime = renderRoute("/", {
      createCollection: vi.fn().mockRejectedValue(new Error("Inbox collection create exploded")),
      replaceCollectionItems: vi.fn().mockRejectedValue(new Error("Inbox collection update exploded")),
      removeMediaItem: vi.fn().mockRejectedValue(new Error("Inbox removal exploded")),
    });

    fireEvent.click(await screen.findByLabelText("Select Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Create collection" }));
    expect(await screen.findByText("Inbox collection create exploded")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Existing collection"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Add selected" }));
    expect(await screen.findByText("Inbox collection update exploded")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Soft delete Call note" }));
    expect(await screen.findByText("Inbox removal exploded")).toBeVisible();
    inboxRuntime.unmount();

    const collectionsLoaderRuntime = renderRoute("/collections", {
      listCollections: vi.fn().mockRejectedValue(new Error("Collections loader exploded")),
    });

    expect(await within(collectionsLoaderRuntime.container).findByText("Collections loader exploded")).toBeVisible();
    collectionsLoaderRuntime.unmount();

    const collectionsRuntime = renderRoute("/collections", {
      updateCollection: vi.fn().mockRejectedValue(new Error("Archive update exploded")),
    });

    expect(await screen.findByLabelText("Rename Research set")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Archive" }));
    expect(await screen.findByText("Archive update exploded")).toBeVisible();
  });

  it("covers run and artifact concrete Error branches plus manifest and event fallbacks", async () => {
    const runsLoaderRuntime = renderRoute("/runs", {
      listAnalysisRuns: vi.fn().mockRejectedValue(new Error("Runs loader exploded")),
    });

    expect(await within(runsLoaderRuntime.container).findByText("Runs loader exploded")).toBeVisible();
    runsLoaderRuntime.unmount();

    const manifestFallbackRuntime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          artifacts: [
            {
              artifact_id: "artifact-manifest-array",
              analysis_run_id: "run-1",
              kind: "run_manifest",
              status: "available",
              content_type: "application/json",
              size_bytes: 32,
              preview: {
                available: true,
                kind: "text",
                format: "json",
                text_excerpt: "[]",
              },
              created_at: "2026-05-10T00:00:00Z",
            },
          ],
        }),
      ),
      listAnalysisRunEvents: vi.fn().mockResolvedValue({
        items: [
          {
            event_id: "event-non-string-progress",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 2,
            emitted_at: "2026-05-10T00:00:00Z",
            payload: {
              stage: 7,
              message: false,
            },
          },
        ],
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

    expect(await within(manifestFallbackRuntime.container).findByText("Selected as")).toBeVisible();
    const progressEvent = await within(manifestFallbackRuntime.container).findByText("analysis_run.progress");
    const progressEntry = progressEvent.closest(".timeline-entry") as HTMLElement;
    expect(progressEntry).toBeVisible();
    expect(progressEntry).not.toHaveTextContent("false");
    manifestFallbackRuntime.unmount();

    const runDetailErrorRuntime = renderRoute("/runs/run-1", {
      cancelAnalysisRun: vi.fn().mockRejectedValue(new Error("Run cancel exploded")),
      retryAnalysisRun: vi.fn().mockRejectedValue(new Error("Run retry exploded")),
      listDiagnostics: vi.fn().mockRejectedValue(new Error("Run diagnostics exploded")),
    });

    expect(await within(runDetailErrorRuntime.container).findByText("Run diagnostics exploded")).toBeVisible();
    runDetailErrorRuntime.unmount();

    const runActionRuntime = renderRoute("/runs/run-1", {
      cancelAnalysisRun: vi.fn().mockRejectedValue(new Error("Run cancel exploded")),
      retryAnalysisRun: vi.fn().mockRejectedValue(new Error("Run retry exploded")),
    });

    fireEvent.click(await within(runActionRuntime.container).findByRole("button", { name: "Cancel" }));
    expect(await within(runActionRuntime.container).findByText("Run cancel exploded")).toBeVisible();
    fireEvent.click(within(runActionRuntime.container).getByRole("button", { name: "Retry" }));
    expect(await within(runActionRuntime.container).findByText("Run retry exploded")).toBeVisible();
    runActionRuntime.unmount();

    const artifactLoaderRuntime = renderRoute("/artifacts/artifact-1", {
      listArtifacts: vi.fn().mockRejectedValue(new Error("Artifact loader exploded")),
    });

    expect(await within(artifactLoaderRuntime.container).findByText("Artifact loader exploded")).toBeVisible();
    artifactLoaderRuntime.unmount();

    const artifactRefreshRuntime = renderRoute("/artifacts/artifact-1", {
      getArtifact: vi.fn().mockResolvedValue({
        ...analysisRun().artifacts[0],
        owner,
        visibility: "owner",
        diagnostics: undefined,
      }),
      refreshArtifact: vi.fn().mockRejectedValue(new Error("Artifact refresh exploded")),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await within(artifactRefreshRuntime.container).findByText("No diagnostics.")).toBeVisible();
    fireEvent.click(within(artifactRefreshRuntime.container).getByRole("button", { name: "Refresh access" }));
    expect(await within(artifactRefreshRuntime.container).findByText("Artifact refresh exploded")).toBeVisible();
  });

  it("covers file-reset, diagnostics, and media-detail concrete Error branches", async () => {
    const inboxRuntime = renderRoute("/");

    fireEvent.click(await screen.findByRole("button", { name: "File/media" }));
    fireEvent.change(screen.getByLabelText("File"), {
      target: {
        files: [],
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add to inbox" }));
    expect(await screen.findByText("Choose a file first.")).toBeVisible();
    inboxRuntime.unmount();

    const diagnosticsLoaderRuntime = renderRoute("/diagnostics", {
      listDiagnostics: vi.fn().mockRejectedValue(new Error("Diagnostics loader exploded")),
    });

    expect(await within(diagnosticsLoaderRuntime.container).findByText("Diagnostics loader exploded")).toBeVisible();
    diagnosticsLoaderRuntime.unmount();

    const diagnosticsRuntime = renderRoute("/diagnostics", {
      reconcileAnalysisRunQueue: vi.fn().mockRejectedValue(new Error("Queue reconcile exploded")),
    });

    fireEvent.click(await within(diagnosticsRuntime.container).findByRole("button", { name: "Reconcile queue" }));
    expect(await within(diagnosticsRuntime.container).findByText("Queue reconcile exploded")).toBeVisible();
    diagnosticsRuntime.unmount();

    const mediaLoaderRuntime = renderRoute("/inbox/media-1", {
      getMediaItem: vi.fn().mockRejectedValue(new Error("Media detail exploded")),
    });

    expect(await within(mediaLoaderRuntime.container).findByText("Media detail exploded")).toBeVisible();
    mediaLoaderRuntime.unmount();

    const mediaRuntime = renderRoute("/inbox/media-1", {
      removeMediaItem: vi.fn().mockRejectedValue(new Error("Media removal exploded")),
    });

    fireEvent.click(await within(mediaRuntime.container).findByRole("button", { name: "Soft delete Call note" }));
    expect(await within(mediaRuntime.container).findByText("Media removal exploded")).toBeVisible();
  });

  it("covers generic inbox fallback messages and stale run-builder source labels", async () => {
    const inboxRuntime = renderRoute("/", {
      replaceCollectionItems: vi.fn().mockRejectedValue("boom"),
      removeMediaItem: vi.fn().mockRejectedValue("boom"),
    });

    fireEvent.click(await screen.findByLabelText("Select Call note"));
    fireEvent.change(screen.getByLabelText("Existing collection"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Add selected" }));
    expect(await screen.findByText("Unable to update collection.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Soft delete Call note" }));
    expect(await screen.findByText("Unable to remove media.")).toBeVisible();
    inboxRuntime.unmount();

    const listCollections = vi
      .fn()
      .mockResolvedValueOnce({
        items: [collection()],
        page: { page_size: 50, has_more: false },
      })
      .mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      });
    const runBuilderRuntime = renderRoute("/runs?collection=collection-1", {
      listCollections,
      listAnalysisRuns: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 25, has_more: false },
      }),
    });

    expect(await screen.findByText("Research set")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => {
      expect(runBuilderRuntime.apiClient.listCollections).toHaveBeenCalledTimes(2);
    });
    expect(await screen.findByText("collection-1")).toBeVisible();
  });

  it("covers non-object manifest payload fallback", async () => {
    const runtime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          artifacts: [
            {
              artifact_id: "artifact-manifest-scalar",
              analysis_run_id: "run-1",
              kind: "run_manifest",
              status: "available",
              content_type: "application/json",
              size_bytes: 16,
              preview: {
                available: true,
                kind: "text",
                format: "json",
                text_excerpt: "\"not-an-object\"",
              },
              created_at: "2026-05-10T00:00:00Z",
            },
          ],
        }),
      ),
      listArtifacts: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await within(runtime.container).findByText("Selected as")).toBeVisible();
    expect(within(runtime.container).queryByText("Outcome")).toBeNull();
  });
});
