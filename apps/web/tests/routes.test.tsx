import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { RouterProvider, createMemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { createWebUiRoutes } from "../src/app/routes";
import type { WebUiRuntime } from "../src/app/runtime";
import type { WebUiApiClient } from "../src/lib/api/client";

const routerFuture = {
  v7_fetcherPersist: true,
  v7_normalizeFormMethod: true,
  v7_partialHydration: true,
  v7_relativeSplatPath: true,
  v7_skipActionErrorRevalidation: true,
} as const;
const routerProviderFuture = { v7_startTransition: true } as const;
const channelAccountId = "web-console";

function secondMediaAsset() {
  return mediaAsset({
    media_asset_id: "media-2",
    kind: "audio",
    display_name: "Interview audio",
    origin: {
      origin_type: "object",
      object_key: "web-local://interview.wav",
      object_ref: "web-local://interview.wav",
      content_type: "audio/wav",
      size_bytes: 2048,
    },
  });
}

function mediaAsset(overrides = {}) {
  return {
    media_asset_id: "media-1",
    channel_account_id: "web-console",
    kind: "text",
    status: "ready",
    display_name: "Call note",
    origin: { origin_type: "text", text: "Call note" },
    diagnostics_count: 0,
    created_at: "2026-05-10T00:00:00Z",
    updated_at: "2026-05-10T00:00:00Z",
    ...overrides,
  };
}

function selectionSnapshot(overrides = {}) {
  return {
    selection_snapshot_id: "snapshot-1",
    channel_account_id: "web-console",
    status: "sealed",
    items: [],
    option_snapshot: {},
    created_at: "2026-05-10T00:00:00Z",
    sealed_at: "2026-05-10T00:00:00Z",
    ...overrides,
  };
}

function collection(overrides = {}) {
  return {
    collection_id: "collection-1",
    channel_account_id: "web-console",
    kind: "user",
    name: "Research set",
    status: "active",
    version: 3,
    items: [
      {
        media_asset_id: "media-1",
        position: 0,
        media_asset: mediaAsset(),
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
    channel_account_id: "web-console",
    selection_snapshot_id: "snapshot-1",
    selection_snapshot: {
      selection_snapshot_id: "snapshot-1",
      channel_account_id: "web-console",
      status: "sealed",
      items: [
        {
          selection_snapshot_item_id: "snapshot-item-1",
          position: 0,
          media_asset_id: "media-1",
          kind: "text",
          origin_snapshot: { origin_type: "text", stored_object_id: "origin-1", text: "Call note" },
          display_name: "Call note",
          status_at_selection: "ready",
        },
      ],
      option_snapshot: {},
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
            transcription_backend: {
              provider: "copperasr",
              model: "Copperside/CoppersideASR",
              duration_seconds: 42.5,
            },
            summary: { included_count: 1, skipped_count: 0, failed_count: 0 },
            items: [
              {
                selection_snapshot_item_id: "snapshot-item-1",
                media_asset_id: "media-1",
                position: 0,
                outcome: "succeeded",
                included: true,
                lineage: { media_asset_id: "media-1", role: "primary" },
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
    listMediaAssets: vi.fn().mockResolvedValue({
      items: [mediaAsset(), secondMediaAsset()],
      page: { page_size: 50, has_more: false },
    }),
    getMediaAsset: vi.fn().mockResolvedValue(mediaAsset()),
    addMediaAsset: vi.fn().mockResolvedValue(mediaAsset({ media_asset_id: "media-2", display_name: "Fresh note" })),
    removeMediaAsset: vi.fn().mockResolvedValue(
      mediaAsset({
        status: "deleted",
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
    createSelectionSnapshot: vi.fn().mockResolvedValue(selectionSnapshot({ selection_snapshot_id: "snapshot-2" })),
    getSelectionSnapshot: vi.fn().mockResolvedValue(selectionSnapshot()),
    createAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ analysis_run_id: "run-2", status: "queued" })),
    listAnalysisRuns: vi.fn().mockResolvedValue({
      items: [analysisRun()],
      page: { page_size: 25, has_more: false },
    }),
    getAnalysisRun: vi.fn().mockResolvedValue(analysisRun()),
    cancelAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ status: "Остановка" })),
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
    getArtifact: vi.fn().mockImplementation(async (_channelAccountId, artifactId) => {
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
        channel_account_id: channelAccountId,
        visibility: "channel_deliverable",
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
      channel_account_id: channelAccountId,
      visibility: "channel_deliverable",
      download: {
        available: true,
        provider: "minio_presigned_url",
        url: "https://minio.local/refreshed-artifact-1.txt",
        filename: "refreshed-artifact-1.txt",
      },
      retention: { state: "active" },
      diagnostics: [],
    }),
    listDiagnostics: vi.fn().mockImplementation(async (_channelAccountId, filter) => {
      const diagnostics = [
        {
          diagnostic_id: "diagnostic-run",
          channel_account_id: channelAccountId,
          subject: { subject_type: "analysis_run", subject_id: "run-1" },
          severity: "warning",
          code: "worker_failed",
          message: "Worker reported a bounded warning",
          created_at: "2026-05-10T00:00:00Z",
        },
        {
          diagnostic_id: "diagnostic-origin",
          channel_account_id: channelAccountId,
          subject: { subject_type: "stored_object", subject_id: "origin-1" },
          severity: "warning",
          code: "origin_unavailable",
          message: "Origin warning kept with lineage",
          created_at: "2026-05-10T00:00:00Z",
        },
        {
          diagnostic_id: "diagnostic-artifact",
          channel_account_id: channelAccountId,
          subject: { subject_type: "artifact", subject_id: "artifact-manifest" },
          severity: "info",
          code: "artifact_preview_ready",
          message: "Preview generated",
          created_at: "2026-05-10T00:00:00Z",
        },
        {
          diagnostic_id: "diagnostic-channel-surface",
          channel_account_id: channelAccountId,
          subject: { subject_type: "channel_surface", subject_id: "surface-1" },
          severity: "error",
          code: "adapter_conflict",
          message: "Channel surface conflict requires refresh",
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
    future: routerFuture,
  });
  const renderResult = render(<RouterProvider router={router} future={routerProviderFuture} />);
  return { ...runtime, ...renderResult };
}

describe("createWebUiRoutes", () => {
  it("renders the inbox-first surface and adds text media through the API boundary", async () => {
    const runtime = renderRoute("/");

    expect(await screen.findByRole("heading", { name: "Материалы" })).toBeVisible();
    const primaryNav = within(screen.getByRole("navigation", { name: "Основная навигация" }));
    expect(primaryNav.getAllByRole("link")).toHaveLength(5);
    expect(primaryNav.getByRole("link", { name: "Материалы" })).toHaveAttribute("href", "/");
    expect(primaryNav.getByRole("link", { name: "Группы" })).toHaveAttribute("href", "/collections");
    expect(primaryNav.getByRole("link", { name: "Подборка" })).toHaveAttribute("href", "/runs");
    expect(primaryNav.getByRole("link", { name: "Результаты" })).toHaveAttribute("href", "/artifacts");
    expect(primaryNav.getByRole("link", { name: "Проверки" })).toHaveAttribute("href", "/diagnostics");
    expect(primaryNav.queryByText(/Inbox|Collections|Run builder|Artifacts|Admin/i)).toBeNull();
    expect(screen.queryByText("http://localhost:8080")).toBeNull();
    expect(screen.queryByText("ws://localhost:8080/v1/ws")).toBeNull();
    expect(await screen.findByText("Call note")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Название"), { target: { value: "Fresh note" } });
    fireEvent.change(screen.getByLabelText("Текст"), { target: { value: "New meeting note" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));

    await waitFor(() => {
      expect(runtime.apiClient.addMediaAsset).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          kind: "text",
          displayName: "Fresh note",
        }),
      );
    });
  });

  it("exposes explicit soft-delete from the inbox surface", async () => {
    const runtime = renderRoute("/");
    fireEvent.click(await within(runtime.container).findByRole("button", { name: "Удалить Call note" }));

    await waitFor(() => {
      expect(runtime.apiClient.removeMediaAsset).toHaveBeenCalledWith(channelAccountId, "media-1");
    });
    expect(await within(runtime.container).findByText("Удалено: Call note")).toBeVisible();
  });

  it("shows the retained soft-delete outcome on the media detail surface", async () => {
    const runtime = renderRoute("/inbox/media-1");
    fireEvent.click(await within(runtime.container).findByRole("button", { name: "Удалить Call note" }));

    await waitFor(() => {
      expect(runtime.apiClient.removeMediaAsset).toHaveBeenCalledWith(channelAccountId, "media-1");
    });
    expect(await within(runtime.container).findByText("Удалено: Call note")).toBeVisible();
    expect(within(runtime.container).getAllByText("Удалено").length).toBeGreaterThan(0);
    expect(within(runtime.container).getByText("10 мая 2026 г., 04:00")).toBeVisible();
  });

  it("creates a collection from selected inbox items", async () => {
    const runtime = renderRoute("/");

    fireEvent.click(await screen.findByLabelText("Выбрать Call note"));
    fireEvent.change(screen.getByLabelText("Новая группа"), { target: { value: "Important set" } });
    fireEvent.click(screen.getByRole("button", { name: "Создать группу" }));

    await waitFor(() => {
      expect(runtime.apiClient.createCollection).toHaveBeenCalledWith(channelAccountId, {
        name: "Important set",
        items: ["media-1"],
      });
    });
  });

  it("offers keyboard-reachable bulk selection controls for selection-heavy flows", async () => {
    renderRoute("/runs");

    const selectAll = await screen.findByRole("button", { name: "Выбрать все" });
    selectAll.focus();
    expect(selectAll).toHaveFocus();
    fireEvent.click(selectAll);

    expect(screen.getByRole("button", { name: "Запустить: 2" })).toBeEnabled();

    const clearSelection = screen.getByRole("button", { name: "Очистить" });
    clearSelection.focus();
    expect(clearSelection).toHaveFocus();
    fireEvent.click(clearSelection);

    expect(screen.getByRole("button", { name: "Запустить: 0" })).toBeDisabled();
  });

  it("edits a collection by adding an inbox item", async () => {
    const runtime = renderRoute("/collections");

    fireEvent.change(await screen.findByLabelText("Добавить материал"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));

    await waitFor(() => {
      expect(runtime.apiClient.replaceCollectionItems).toHaveBeenCalledWith(channelAccountId, "collection-1", {
        expectedVersion: 3,
        items: [
          { media_asset_id: "media-1", position: 0 },
          { media_asset_id: "media-2", position: 1 },
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
    runtime.apiClient.replaceCollectionItems = vi.fn().mockImplementation(async (_channelAccountId, _collectionId, draft) => {
      mutableCollection = collection({
        version: draft.expectedVersion + 1,
        items: [
          {
            media_asset_id: "media-1",
            position: 0,
            media_asset: mediaAsset(),
            added_at: "2026-05-10T00:00:00Z",
          },
          {
            media_asset_id: "media-2",
            position: 1,
            media_asset: secondMediaAsset(),
            added_at: "2026-05-10T00:00:00Z",
          },
        ],
      });
      return mutableCollection;
    });
    runtime.apiClient.getAnalysisRun = vi.fn().mockResolvedValue(analysisRun());

    const router = createMemoryRouter(createWebUiRoutes(runtime), {
      initialEntries: ["/collections"],
      future: routerFuture,
    });
    const collectionView = render(<RouterProvider router={router} future={routerProviderFuture} />);

    fireEvent.change(await screen.findByLabelText("Добавить материал"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));

    await waitFor(() => {
      expect(runtime.apiClient.replaceCollectionItems).toHaveBeenCalledWith(channelAccountId, "collection-1", {
        expectedVersion: 3,
        items: [
          { media_asset_id: "media-1", position: 0 },
          { media_asset_id: "media-2", position: 1 },
        ],
      });
    });

    collectionView.unmount();
    const detailRouter = createMemoryRouter(createWebUiRoutes(runtime), {
      initialEntries: ["/runs/run-1"],
      future: routerFuture,
    });
    render(<RouterProvider router={detailRouter} future={routerProviderFuture} />);

    expect(await screen.findByRole("heading", { name: "Краткое содержание" })).toBeVisible();
    expect(screen.getByText("#1 Call note")).toBeVisible();
    expect(screen.queryByText("Interview audio")).toBeNull();
  });

  it("creates a sealed selection before queuing a run", async () => {
    const runtime = renderRoute("/runs");

    fireEvent.click(await screen.findByLabelText("Выбрать Call note"));
    fireEvent.change(screen.getByLabelText("Что сделать"), { target: { value: "summary" } });
    fireEvent.click(screen.getByRole("button", { name: "Запустить: 1" }));

    await waitFor(() => {
      expect(runtime.apiClient.createSelectionSnapshot).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          items: [{ media_asset_id: "media-1", position: 0 }],
        }),
      );
      expect(runtime.apiClient.createAnalysisRun).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          runType: "summary",
          selectionSnapshotId: "snapshot-2",
        }),
      );
    });
  });

  it("preloads run planning from a collection link", async () => {
    renderRoute("/runs?collection=collection-1");

    expect(await screen.findByText("Research set")).toBeVisible();
    expect(await screen.findByText("#1 Call note")).toBeVisible();
  });

  it("renders non-summary run type labels in the run list", async () => {
    renderRoute("/runs", {
      listAnalysisRuns: vi.fn().mockResolvedValue({
        items: [
          analysisRun({ analysis_run_id: "run-report", run_type: "report" }),
          analysisRun({ analysis_run_id: "run-research", run_type: "deep_research" }),
          analysisRun({ analysis_run_id: "run-custom", run_type: "custom" }),
          analysisRun({ analysis_run_id: "run-special", run_type: "custom_pipeline" }),
        ],
        page: { page_size: 25, has_more: false },
      }),
    });

    expect((await screen.findAllByText("Отчет")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("Глубокое исследование")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("Свой сценарий")).length).toBeGreaterThan(0);
    expect(await screen.findByText("custom pipeline")).toBeVisible();
  });

  it("renders run detail with events, artifacts, and diagnostics", async () => {
    renderRoute("/runs/run-1");

    expect(await screen.findByRole("heading", { name: "Краткое содержание" })).toBeVisible();
    expect(await screen.findByText("Прогресс")).toBeVisible();
    expect(await screen.findByText("Расшифровка: Running transcription pipeline")).toBeVisible();
    expect(await screen.findByText("Сбой обработки")).toBeVisible();
    expect(await screen.findAllByText("Материал недоступен")).toHaveLength(2);
    expect((await screen.findAllByText("Готово")).length).toBeGreaterThan(0);
    expect(await screen.findByRole("link", { name: "Краткое содержание" })).toHaveAttribute("href", "/artifacts/artifact-1");
    expect(screen.queryByText(/copperasr|Copperside|CopperASR/i)).toBeNull();
  });

  it("renders localized progress labels for remaining run stages", async () => {
    renderRoute("/runs/run-1", {
      listAnalysisRunEvents: vi.fn().mockResolvedValue({
        items: [
          {
            event_id: "event-created",
            analysis_run_id: "run-1",
            event_type: "analysis_run.created",
            version: 0,
            emitted_at: "2026-05-10T00:00:00Z",
            payload: {},
          },
          {
            event_id: "event-queued",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 1,
            emitted_at: "2026-05-10T00:00:00Z",
            status: "running",
            payload: { stage: "queued", message: "Waiting for worker" },
          },
          {
            event_id: "event-summarizing",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 2,
            emitted_at: "2026-05-10T00:00:30Z",
            status: "running",
            payload: { stage: "summarizing", message: "Drafting summary" },
          },
          {
            event_id: "event-reporting",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 3,
            emitted_at: "2026-05-10T00:01:00Z",
            status: "running",
            payload: { stage: "reporting", message: "Building report" },
          },
          {
            event_id: "event-research",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 4,
            emitted_at: "2026-05-10T00:02:00Z",
            status: "running",
            payload: { stage: "deep_research", message: "Expanding sources" },
          },
          {
            event_id: "event-upload",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 5,
            emitted_at: "2026-05-10T00:03:00Z",
            status: "running",
            payload: { stage: "artifact_upload", message: "Uploading artifact" },
          },
          {
            event_id: "event-custom",
            analysis_run_id: "run-1",
            event_type: "analysis_run.progress",
            version: 6,
            emitted_at: "2026-05-10T00:04:00Z",
            status: "running",
            payload: { stage: "custom_stage", message: "Custom stage" },
          },
          {
            event_id: "event-completed",
            analysis_run_id: "run-1",
            event_type: "analysis_run.completed",
            version: 7,
            emitted_at: "2026-05-10T00:05:00Z",
            payload: {},
          },
          {
            event_id: "event-failed",
            analysis_run_id: "run-1",
            event_type: "analysis_run.failed",
            version: 8,
            emitted_at: "2026-05-10T00:06:00Z",
            payload: {},
          },
          {
            event_id: "event-diagnostic",
            analysis_run_id: "run-1",
            event_type: "diagnostic.recorded",
            version: 9,
            emitted_at: "2026-05-10T00:07:00Z",
            payload: {},
          },
          {
            event_id: "event-artifact",
            analysis_run_id: "run-1",
            event_type: "artifact.available",
            version: 10,
            emitted_at: "2026-05-10T00:08:00Z",
            payload: {},
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("Запуск создан")).toBeVisible();
    expect(await screen.findByText("Ожидает очереди: Waiting for worker")).toBeVisible();
    expect(await screen.findByText("Краткое содержание: Drafting summary")).toBeVisible();
    expect(await screen.findByText("Отчет: Building report")).toBeVisible();
    expect(await screen.findByText("Глубокое исследование: Expanding sources")).toBeVisible();
    expect(await screen.findByText("Сохранение результата: Uploading artifact")).toBeVisible();
    expect(await screen.findByText("Прогресс: Custom stage")).toBeVisible();
    expect(await screen.findByText("Запуск завершен")).toBeVisible();
    expect(await screen.findByText("Ошибка запуска")).toBeVisible();
    expect(await screen.findByText("Проверка записана")).toBeVisible();
    expect(await screen.findByText("Результат готов")).toBeVisible();
  });

  it("renders pending, passed, failed, and fallback evidence gate labels", async () => {
    const cases = [
      ["pending", "Ожидает проверки"],
      ["passed", "Пройдена"],
      ["failed", "Есть ошибки"],
      ["manual_review", "manual review"],
    ] as const;

    for (const [state, label] of cases) {
      const runtime = renderRoute("/runs/run-1", {
        getAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ evidence_gate_state: state })),
      });

      expect(await within(runtime.container).findByText(label)).toBeVisible();
      runtime.unmount();
    }
  });

  it("opens markdown artifact previews from the artifact browser", async () => {
    const runtime = renderRoute("/artifacts/artifact-1");

    expect(await screen.findByRole("heading", { name: "Файлы и отчеты" })).toBeVisible();
    expect(await screen.findByText(/Interview notes/)).toBeVisible();
    expect(await screen.findByRole("link", { name: "Открыть результат" })).toHaveAttribute(
      "href",
      "https://minio.local/artifact-1.txt",
    );

    fireEvent.click(screen.getByRole("button", { name: "Обновить ссылку" }));

    await waitFor(() => {
      expect(runtime.apiClient.refreshArtifact).toHaveBeenCalledWith(channelAccountId, "artifact-1");
    });
    await waitFor(() => {
      expect(screen.getByRole("link", { name: "Открыть результат" })).toHaveAttribute(
        "href",
        "https://minio.local/refreshed-artifact-1.txt",
      );
    });
  });

  it("hides service artifact previews from normal results", async () => {
    renderRoute("/artifacts/artifact-manifest");

    expect(await screen.findByText("Этот служебный файл не показывается в обычных результатах.")).toBeVisible();
    expect(await screen.findByRole("link", { name: "Краткое содержание" })).toHaveAttribute(
      "href",
      "/artifacts/artifact-1",
    );
    expect(screen.queryByText(/analysis_run_manifest\/v2/)).toBeNull();
    expect(screen.queryByRole("link", { name: "План запуска" })).toBeNull();
  });

  it("does not register the old jobs entrypoint", async () => {
    renderRoute("/jobs/job-123");

    expect(await screen.findByRole("heading", { name: "Страница не найдена" })).toBeVisible();
    expect(screen.getByRole("link", { name: "К материалам" })).toHaveAttribute("href", "/");
  });

  it("exposes final admin observability", async () => {
    renderRoute("/diagnostics");

    expect(await screen.findByText("42s")).toBeVisible();
  });

  it("filters channel surface diagnostics through the admin contract", async () => {
    const runtime = renderRoute("/diagnostics");

    fireEvent.change(await screen.findByLabelText("Объект"), { target: { value: "channel_surface" } });
    fireEvent.change(screen.getByLabelText("Уровень"), { target: { value: "error" } });

    await waitFor(() => {
      expect(runtime.apiClient.listDiagnostics).toHaveBeenLastCalledWith(channelAccountId, {
        subjectType: "channel_surface",
        severity: "error",
        pageSize: 50,
      });
    });
    expect(await screen.findByText("Channel surface conflict requires refresh")).toBeVisible();
  });

  it("renders collection, selection snapshot, and channel diagnostic fallback labels", async () => {
    renderRoute("/diagnostics", {
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [
          {
            diagnostic_id: "diagnostic-collection",
            channel_account_id: channelAccountId,
            subject: { subject_type: "collection", subject_id: "collection-1" },
            severity: "warning",
            code: "origin_warning",
            message: "Collection warning kept visible",
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            diagnostic_id: "diagnostic-selection-snapshot",
            channel_account_id: channelAccountId,
            subject: { subject_type: "selection_snapshot", subject_id: "snapshot-1" },
            severity: "warning",
            code: "origin_warning",
            message: "Selection snapshot warning kept visible",
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            diagnostic_id: "diagnostic-media-asset",
            channel_account_id: channelAccountId,
            subject: { subject_type: "media_asset", subject_id: "media-1" },
            severity: "warning",
            code: "origin_warning",
            message: "Media asset warning kept visible",
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            diagnostic_id: "diagnostic-channel-account",
            channel_account_id: channelAccountId,
            subject: { subject_type: "channel_account", subject_id: "telegram-main" },
            severity: "info",
            code: "adapter_conflict",
            message: "Channel account conflict stayed visible",
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            diagnostic_id: "diagnostic-channel-surface",
            channel_account_id: channelAccountId,
            subject: { subject_type: "channel_surface", subject_id: "telegram-surface-main" },
            severity: "info",
            code: "adapter_conflict",
            message: "Channel surface fallback label stayed visible",
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            diagnostic_id: "diagnostic-artifact-target",
            channel_account_id: channelAccountId,
            subject: { subject_type: "artifact", subject_id: "artifact-target" },
            severity: "info",
            code: "artifact_preview_ready",
            message: "Artifact diagnostic subject stayed visible",
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            diagnostic_id: "diagnostic-missing-subject",
            channel_account_id: channelAccountId,
            severity: "info",
            code: "retention_denied",
            message: "Missing subject falls back to run",
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("Collection warning kept visible")).toBeVisible();
    expect(await screen.findByText("Selection snapshot warning kept visible")).toBeVisible();
    expect(await screen.findByText("Media asset warning kept visible")).toBeVisible();
    expect(await screen.findByText("Channel account conflict stayed visible")).toBeVisible();
    expect(await screen.findByText("Channel surface fallback label stayed visible")).toBeVisible();
    expect(await screen.findByText("Artifact diagnostic subject stayed visible")).toBeVisible();
    expect(await screen.findByText("Missing subject falls back to run")).toBeVisible();
    expect(screen.getAllByText("группа").length).toBeGreaterThan(0);
    expect(screen.getAllByText("подборка").length).toBeGreaterThan(0);
    expect(screen.getAllByText("канал").length).toBeGreaterThan(0);
    expect(screen.getByText("результат")).toBeVisible();
    expect(screen.getByText("запуск")).toBeVisible();
    expect(screen.getAllByText("Проверка").length).toBeGreaterThan(0);
  });

  it("covers inbox validation and alternate ingest modes", async () => {
    const runtime = renderRoute("/");

    fireEvent.click(await screen.findByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Добавьте текст.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Ссылка" }));
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Добавьте ссылку.")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Ссылка"), { target: { value: "https://example.test/origin" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaAsset).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          kind: "url",
          displayName: "https://example.test/origin",
          origin: { origin_type: "url", url: "https://example.test/origin", origin_ref: "https://example.test/origin" },
        }),
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "Файл" }));
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Выберите файл.")).toBeVisible();

    const fileInput = screen.getByLabelText("Файл");
    fireEvent.change(fileInput, {
      target: {
        files: [new File(["voice"], "sample.wav", { type: "audio/wav", lastModified: 1700000000000 })],
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaAsset).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          kind: "audio",
          displayName: "sample.wav",
          origin: expect.objectContaining({
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

    const renameInput = await screen.findByLabelText("Переименовать Research set");
    fireEvent.blur(renameInput, { target: { value: "Research set" } });
    fireEvent.blur(renameInput, { target: { value: "   " } });
    expect(runtime.apiClient.updateCollection).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "В архив" }));
    await waitFor(() => {
      expect(runtime.apiClient.updateCollection).toHaveBeenCalledWith(channelAccountId, "collection-1", {
        expectedVersion: 3,
        status: "archived",
      });
    });

    fireEvent.click(screen.getByRole("button", { name: "Убрать" }));
    await waitFor(() => {
      expect(runtime.apiClient.removeCollectionItem).toHaveBeenCalledWith(channelAccountId, "collection-1", "media-1", 3);
    });

    fireEvent.click(screen.getByRole("button", { name: "Создать" }));
    await waitFor(() => {
      expect(runtime.apiClient.createCollection).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          name: "Группа 2",
          items: [],
        }),
      );
    });
  });

  it("covers run-builder creation and run-detail lifecycle branches", async () => {
    const runtime = renderRoute("/runs");

    fireEvent.click(await screen.findByLabelText("Выбрать Call note"));
    expect(screen.queryByLabelText("Параметры")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Запустить: 1" }));
    await waitFor(() => {
      expect(runtime.apiClient.createAnalysisRun).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          params: undefined,
          runType: "transcription",
        }),
      );
    });

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

    const cancelButton = await within(detailRuntime.container).findByRole("button", { name: "Остановить" });
    expect(cancelButton).toBeDisabled();

    expect(await within(detailRuntime.container).findByText("Событий пока нет.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("Результатов пока нет.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("Проверок пока нет.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("Проверок по материалам нет.")).toBeVisible();
    expect(within(detailRuntime.container).getByText("Выбран в подборке")).toBeVisible();

    fireEvent.click(within(detailRuntime.container).getByRole("button", { name: "Повторить" }));
    await waitFor(() => {
      expect(detailRuntime.apiClient.retryAnalysisRun).toHaveBeenCalledWith(channelAccountId, "run-1");
    });
    expect(await within(detailRuntime.container).findByText(/Повторный запуск добавлен в очередь/)).toBeVisible();
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

    expect(await screen.findByText("Результатов пока нет.")).toBeVisible();
    expect(screen.getByText("Выберите результат из списка.")).toBeVisible();

    runtime.unmount();

    renderRoute("/diagnostics", {
      getObservabilitySnapshot: vi.fn().mockResolvedValue(null),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("Снимок состояния не загружен.")).toBeVisible();
    expect(screen.getByText("Проверок пока нет.")).toBeVisible();
  });

  it("covers inbox action fallback errors", async () => {
    const runtime = renderRoute("/", {
      addMediaAsset: vi.fn().mockRejectedValue("boom"),
      createCollection: vi.fn().mockRejectedValue("boom"),
    });

    fireEvent.change(await screen.findByLabelText("Текст"), { target: { value: "error case" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Не удалось добавить материал.")).toBeVisible();

    fireEvent.click(screen.getByLabelText("Выбрать Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Создать группу" }));
    expect(await screen.findByText("Не удалось создать группу.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Добавить выбранное" }));
    expect(await screen.findByText("Выберите группу.")).toBeVisible();
  });

  it("covers collection management fallback errors", async () => {
    const runtime = renderRoute("/collections", {
      createCollection: vi.fn().mockRejectedValue("boom"),
      updateCollection: vi.fn().mockRejectedValue("boom"),
      removeCollectionItem: vi.fn().mockRejectedValue("boom"),
      replaceCollectionItems: vi.fn().mockRejectedValue("boom"),
    });

    fireEvent.click(await screen.findByRole("button", { name: "Создать" }));
    expect(await screen.findByText("Не удалось создать группу.")).toBeVisible();

    fireEvent.blur(screen.getByLabelText("Переименовать Research set"), { target: { value: "Renamed" } });
    expect(await screen.findByText("Не удалось переименовать группу.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Убрать" }));
    expect(await screen.findByText("Не удалось убрать материал.")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Добавить материал"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Не удалось добавить материал.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "В архив" }));
    expect(await screen.findByText("Не удалось обновить группу.")).toBeVisible();
  });

  it("covers run, artifact, diagnostics, and media-detail fallback errors", async () => {
    renderRoute("/runs", {
      createSelectionSnapshot: vi.fn().mockRejectedValue("boom"),
    });
    fireEvent.click(await screen.findByLabelText("Выбрать Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Запустить: 1" }));
    expect(await screen.findByText("Не удалось запустить обработку.")).toBeVisible();

    const detailRuntime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(analysisRun()),
      cancelAnalysisRun: vi.fn().mockRejectedValue("boom"),
      retryAnalysisRun: vi.fn().mockRejectedValue("boom"),
    });
    fireEvent.click(await within(detailRuntime.container).findByRole("button", { name: "Остановить" }));
    expect(await within(detailRuntime.container).findByText("Не удалось остановить запуск.")).toBeVisible();
    fireEvent.click(within(detailRuntime.container).getByRole("button", { name: "Повторить" }));
    expect(await within(detailRuntime.container).findByText("Не удалось повторить запуск.")).toBeVisible();
    detailRuntime.unmount();

    renderRoute("/artifacts/artifact-1", {
      listArtifacts: vi.fn().mockRejectedValue("boom"),
      getArtifact: vi.fn().mockRejectedValue("boom"),
    });
    expect(await screen.findByText("Не удалось загрузить результаты.")).toBeVisible();

    const diagnosticsRuntime = renderRoute("/diagnostics", {
      listDiagnostics: vi.fn().mockRejectedValue("boom"),
      getObservabilitySnapshot: vi.fn().mockRejectedValue("boom"),
    });
    expect(await within(diagnosticsRuntime.container).findByText("Не удалось загрузить проверки.")).toBeVisible();
    diagnosticsRuntime.unmount();

    renderRoute("/inbox/media-1", {
      getMediaAsset: vi.fn().mockRejectedValue("boom"),
      removeMediaAsset: vi.fn().mockRejectedValue("boom"),
    });
    expect(await screen.findByText("Не удалось загрузить материал.")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Удалить материал" }));
    expect(await screen.findByText("Не удалось удалить материал.")).toBeVisible();
  });

  it("covers inbox helper fallbacks, selection toggles, and origin labels", async () => {
    const runtime = renderRoute("/", {
      listMediaAssets: vi.fn().mockResolvedValue({
        items: [
          mediaAsset({
            media_asset_id: "media-url",
            kind: "url",
            display_name: "URL origin",
            origin: { origin_type: "url", url: "https://example.test/file", origin_ref: "https://example.test/file" },
          }),
          mediaAsset({
            media_asset_id: "media-url-ref",
            kind: "url",
            display_name: "URL ref origin",
            origin: { origin_type: "url", origin_ref: "https://example.test/from-ref" },
          }),
          mediaAsset({
            media_asset_id: "media-url-empty",
            kind: "url",
            display_name: "URL empty origin",
            origin: { origin_type: "url" },
          }),
          mediaAsset({
            media_asset_id: "media-raw",
            display_name: "Raw origin",
            origin: { origin_type: "object", origin_ref: "web-local://raw", object_ref: "web-local://raw" },
          }),
          mediaAsset({
            media_asset_id: "media-object-kind",
            kind: "object",
            display_name: "Object kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-document-kind",
            kind: "document",
            display_name: "Document kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-telegram-file-kind",
            kind: "telegram_file",
            display_name: "Telegram file kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-remote",
            display_name: "Remote origin",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-upload-kind",
            kind: "upload",
            display_name: "Upload kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-binary-kind",
            kind: "binary",
            display_name: "Binary kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-video-kind",
            kind: "video",
            display_name: "Video kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-image-kind",
            kind: "image",
            display_name: "Image kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-file-kind",
            kind: "file",
            display_name: "File kind",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-partial-status",
            display_name: "Partial status",
            status: "partially_succeeded",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-canceled-status",
            display_name: "Canceled status",
            status: "canceled",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-expired-status",
            display_name: "Expired status",
            status: "expired",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-quarantined-status",
            display_name: "Quarantined status",
            status: "quarantined",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-queued-status",
            display_name: "Queued status",
            status: "queued",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-cancel-requested-status",
            display_name: "Cancel requested status",
            status: "cancel_requested",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-pending-status",
            display_name: "Pending status",
            status: "pending",
            origin: { origin_type: "remote_ref" },
          }),
          mediaAsset({
            media_asset_id: "media-validating-status",
            display_name: "Validating status",
            status: "validating",
            origin: { origin_type: "remote_ref" },
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

    expect(await screen.findByText("URL origin")).toBeVisible();
    expect(screen.getByText("https://example.test/file")).toBeVisible();
    expect(screen.getByText("https://example.test/from-ref")).toBeVisible();
    expect(screen.getAllByText("Ссылка").length).toBeGreaterThan(0);
    expect(screen.getByText("Загруженный файл")).toBeVisible();
    expect(screen.getAllByText("remote ref").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Ссылка").length).toBeGreaterThan(0);
    expect(screen.getByText("Видео")).toBeVisible();
    expect(screen.getByText("Изображение")).toBeVisible();
    expect(screen.getAllByText("Файл").length).toBeGreaterThan(0);
    expect(screen.getByText("Данные")).toBeVisible();
    expect(screen.getByText("Частично готово")).toBeVisible();
    expect(screen.getByText("Отменено")).toBeVisible();
    expect(screen.getByText("Истекло")).toBeVisible();
    expect(screen.getByText("На проверке")).toBeVisible();
    expect(screen.getByText("В очереди")).toBeVisible();
    expect(screen.getByText("Остановка")).toBeVisible();
    expect(screen.getByText("Готовится")).toBeVisible();
    expect(screen.getByText("Проверяется")).toBeVisible();
    expect(screen.queryByText("in inbox")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Выбрать все" }));
    expect(screen.getByRole("button", { name: "Очистить" })).toBeEnabled();
    fireEvent.click(screen.getByLabelText("Выбрать URL origin"));
    expect(screen.getByRole("button", { name: "Создать группу" })).toHaveTextContent("Создать группу");
    fireEvent.click(screen.getByRole("button", { name: "Очистить" }));
    expect(screen.getByRole("button", { name: "Очистить" })).toBeDisabled();

    runtime.unmount();

    renderRoute("/", {
      listMediaAssets: vi.fn().mockRejectedValue("boom"),
      getInboxCollection: vi.fn().mockResolvedValue(collection({ kind: "inbox", name: "Inbox" })),
      listCollections: vi.fn().mockResolvedValue({ items: [], page: { page_size: 50, has_more: false } }),
      listAnalysisRuns: vi.fn().mockResolvedValue({ items: [], page: { page_size: 25, has_more: false } }),
    });

    expect(await screen.findByText("Не удалось загрузить рабочую область.")).toBeVisible();
  });

  it("covers inbox add-to-collection success and file-kind fanout", async () => {
    const runtime = renderRoute("/");

    fireEvent.click(await screen.findByLabelText("Выбрать Interview audio"));
    fireEvent.change(screen.getByLabelText("Существующая группа"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить выбранное" }));

    await waitFor(() => {
      expect(runtime.apiClient.replaceCollectionItems).toHaveBeenCalledWith(channelAccountId, "collection-1", {
        expectedVersion: 3,
        items: [
          { media_asset_id: "media-1", position: 0 },
          { media_asset_id: "media-2", position: 1 },
        ],
      });
    });
    expect(await screen.findByText("Обновлена группа: Research set")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Файл" }));
    fireEvent.change(screen.getByLabelText("Файл"), {
      target: { files: [new File(["video"], "clip.mp4", { type: "video/mp4" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaAsset).toHaveBeenNthCalledWith(
        1,
        channelAccountId,
        expect.objectContaining({ kind: "video", displayName: "clip.mp4" }),
      );
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Добавить" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("Файл"), {
      target: { files: [new File(["image"], "cover.png", { type: "image/png" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    await waitFor(() => {
      expect(runtime.apiClient.addMediaAsset).toHaveBeenNthCalledWith(
        2,
        channelAccountId,
        expect.objectContaining({ kind: "image", displayName: "cover.png" }),
      );
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Добавить" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("Файл"), {
      target: { files: [new File(["data"], "blob.bin", { type: "" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));

    await waitFor(() => {
      expect(runtime.apiClient.addMediaAsset).toHaveBeenNthCalledWith(
        3,
        channelAccountId,
        expect.objectContaining({ kind: "file", displayName: "blob.bin" }),
      );
    });
  });

  it("covers manifest, material diagnostics, and non-progress event branches", async () => {
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
                      media_asset_id: "media-1",
                      position: 0,
                      outcome: "failed",
                      selection_snapshot_item_id: "selection-item-1",
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
            diagnostic_id: "origin-diagnostic",
            channel_account_id: channelAccountId,
            subject: { subject_type: "stored_object", subject_id: "origin-1" },
            severity: "warning",
            code: "origin_warning",
            message: "Origin payload kept readable.",
            created_at: "2026-05-10T00:00:00Z",
          },
        ],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await screen.findByText("Выбран в подборке")).toBeVisible();
    expect(screen.getByText("Ошибка")).toBeVisible();
    expect(screen.getByText("Нет")).toBeVisible();
    expect(screen.getAllByText("Предупреждение по материалу")).toHaveLength(2);
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
            artifact_id: "artifact-transcript",
            analysis_run_id: "run-1",
            kind: "transcript",
            status: "available",
            content_type: "text/plain",
            size_bytes: 128,
            created_at: "2026-05-10T00:00:00Z",
          },
          {
            artifact_id: "artifact-research",
            analysis_run_id: "run-1",
            kind: "deep_research",
            status: "available",
            content_type: "text/markdown",
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

    expect((await screen.findAllByText("Данные")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("Расшифровка").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Глубокое исследование").length).toBeGreaterThan(0);
    expect(screen.queryByText("Журнал")).toBeNull();
    expect(screen.getAllByText("custom blob").length).toBeGreaterThan(0);
    expect(screen.getByText("{invalid-json")).toBeVisible();
    expect(screen.getAllByText("2.4 MB")).toHaveLength(2);
    expect(screen.queryByRole("link", { name: "Открыть результат" })).toBeNull();
  });

  it("renders valid JSON artifact previews with formatted object text", async () => {
    renderRoute("/artifacts/artifact-json-valid", {
      getArtifact: vi.fn().mockResolvedValue({
        artifact_id: "artifact-json-valid",
        analysis_run_id: "run-1",
        kind: "structured_data",
        status: "available",
        content_type: "application/octet-stream",
        size_bytes: 128,
        preview: {
          available: true,
          kind: "text",
          format: "json",
          text_excerpt: "{\"nested\":{\"value\":1}}",
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

    const preview = await screen.findByText(/"nested":/);
    expect(preview).toBeVisible();
    expect(preview.textContent).toContain('"value": 1');
  });

  it("renders direct source and preview artifact labels", async () => {
    const cases = [
      ["artifact-source", "source_manifest", "Список материалов"],
      ["artifact-preview", "preview", "Предпросмотр"],
    ] as const;

    for (const [artifactId, kind, label] of cases) {
      const runtime = renderRoute(`/artifacts/${artifactId}`, {
        listArtifacts: vi.fn().mockResolvedValue({
          items: [],
          page: { page_size: 50, has_more: false },
        }),
        getArtifact: vi.fn().mockResolvedValue({
          artifact_id: artifactId,
          analysis_run_id: "run-1",
          kind,
          status: "available",
          content_type: "application/json",
          size_bytes: 128,
          preview: { available: false, kind: "none" },
          created_at: "2026-05-10T00:00:00Z",
          diagnostics: [],
          download: { available: false, url: "" },
        }),
        listDiagnostics: vi.fn().mockResolvedValue({
          items: [],
          page: { page_size: 50, has_more: false },
        }),
      });

      expect(await within(runtime.container).findByRole("heading", { name: label })).toBeVisible();
      runtime.unmount();
    }
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

    expect(await within(runRuntime.container).findByText("Не удалось загрузить запуск.")).toBeVisible();
    runRuntime.unmount();
  });

  it("covers collection, runs, artifact, admin, and media refresh callbacks", async () => {
    const collectionsRuntime = renderRoute("/collections", {
      listCollections: vi.fn().mockResolvedValue({
        items: [
          collection({
            items: [{ media_asset_id: "media-opaque", position: 0, added_at: "2026-05-10T00:00:00Z" }],
          }),
        ],
        page: { page_size: 50, has_more: false },
      }),
      getCollection: vi.fn().mockResolvedValue(
        collection({
          items: [{ media_asset_id: "media-opaque", position: 0, added_at: "2026-05-10T00:00:00Z" }],
        }),
      ),
    });

    fireEvent.change(await screen.findByLabelText("Название"), { target: { value: "Curated set" } });
    fireEvent.change(screen.getByLabelText("Первый материал"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Создать" }));
    await waitFor(() => {
      expect(collectionsRuntime.apiClient.createCollection).toHaveBeenCalledWith(channelAccountId, {
        name: "Curated set",
        items: ["media-2"],
      });
    });
    expect(screen.getByText("Материал")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(vi.mocked(collectionsRuntime.apiClient.listCollections).mock.calls.length).toBeGreaterThanOrEqual(2);
    });
    collectionsRuntime.unmount();

    const runsRuntime = renderRoute("/runs");
    fireEvent.change(await screen.findByLabelText("Группа"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByLabelText("Выбрать Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(runsRuntime.apiClient.listAnalysisRuns).toHaveBeenCalledTimes(2);
    });
    runsRuntime.unmount();

    const artifactRuntime = renderRoute("/artifacts/artifact-1", {
      getArtifact: vi.fn().mockResolvedValue({
        artifact_id: "artifact-1",
        analysis_run_id: "run-1",
        kind: "summary",
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

    expect(await screen.findByText("Предпросмотр недоступен.")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(artifactRuntime.apiClient.listArtifacts).toHaveBeenCalledTimes(2);
    });
    fireEvent.click(screen.getByRole("button", { name: "Обновить ссылку" }));
    expect(await screen.findByText("Не удалось обновить ссылку.")).toBeVisible();
    artifactRuntime.unmount();

    const diagnosticsRuntime = renderRoute("/diagnostics");
    fireEvent.click(screen.getByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(diagnosticsRuntime.apiClient.listDiagnostics).toHaveBeenCalledTimes(2);
    });
    diagnosticsRuntime.unmount();

    const mediaRuntime = renderRoute("/inbox/media-1");
    fireEvent.click(await screen.findByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(mediaRuntime.apiClient.getMediaAsset).toHaveBeenCalledTimes(2);
    });
  });

  it("covers inbox refresh, archived collection activation, and successful rename refresh", async () => {
    const inboxRuntime = renderRoute("/");

    fireEvent.click(await screen.findByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(inboxRuntime.apiClient.listMediaAssets).toHaveBeenCalledTimes(2);
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

    const archivedRename = await screen.findByLabelText("Переименовать Archived set");
    fireEvent.blur(archivedRename, { target: { value: "Reactivated set" } });
    await waitFor(() => {
      expect(archivedRuntime.apiClient.updateCollection).toHaveBeenCalledWith(channelAccountId, "collection-archived", {
        expectedVersion: 3,
        name: "Reactivated set",
      });
    });
    await waitFor(() => {
      expect(archivedRuntime.apiClient.listCollections).toHaveBeenCalledTimes(2);
    });

    fireEvent.click(screen.getByRole("button", { name: "Вернуть" }));
    await waitFor(() => {
      expect(archivedRuntime.apiClient.updateCollection).toHaveBeenCalledWith(channelAccountId, "collection-archived", {
        expectedVersion: 3,
        status: "active",
      });
    });
  });

  it("covers run-detail refresh and successful cancel lifecycle", async () => {
    const runtime = renderRoute("/runs/run-1", {
      cancelAnalysisRun: vi.fn().mockResolvedValue(analysisRun({ status: "Остановка" })),
    });

    fireEvent.click(await within(runtime.container).findByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(runtime.apiClient.getAnalysisRun).toHaveBeenCalledTimes(2);
    });

    fireEvent.click(within(runtime.container).getByRole("button", { name: "Остановить" }));
    await waitFor(() => {
      expect(runtime.apiClient.cancelAnalysisRun).toHaveBeenCalledWith(channelAccountId, "run-1");
    });
    expect(await within(runtime.container).findByText("Остановка запрошена")).toBeVisible();
    expect(within(runtime.container).getByText("Остановка")).toBeVisible();
  });

  it("covers text preview fallback, kilobyte sizing, and nullable download availability", async () => {
    renderRoute("/artifacts/artifact-text-preview", {
      listArtifacts: vi.fn().mockResolvedValue({
        items: [
          {
            artifact_id: "artifact-kb",
            analysis_run_id: "run-1",
            kind: "report",
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
        kind: "report",
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
    expect(screen.getByText("Предпросмотр недоступен.")).toBeVisible();
    expect(screen.getByRole("link", { name: "Открыть результат" })).toHaveAttribute(
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

    expect(screen.queryByLabelText("Параметры")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Запустить: 1" }));

    await waitFor(() => {
      expect(runtime.apiClient.createSelectionSnapshot).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          sourceCollectionId: "collection-1",
          optionSnapshot: { basis: "collection" },
        }),
      );
    });
    await waitFor(() => {
      expect(runtime.apiClient.createAnalysisRun).toHaveBeenCalledWith(
        channelAccountId,
        expect.objectContaining({
          selectionSnapshotId: "snapshot-2",
          params: undefined,
        }),
      );
    });
  });

  it("shows concrete and generic run-builder errors", async () => {
    const errorRuntime = renderRoute("/runs", {
      createAnalysisRun: vi.fn().mockRejectedValue(new Error("Run creation exploded")),
    });

    fireEvent.click(await within(errorRuntime.container).findByLabelText("Выбрать Call note"));
    fireEvent.click(within(errorRuntime.container).getByRole("button", { name: "Запустить: 1" }));

    expect(await within(errorRuntime.container).findByText("Run creation exploded")).toBeVisible();
    errorRuntime.unmount();

    const genericRuntime = renderRoute("/runs", {
      createSelectionSnapshot: vi.fn().mockRejectedValue("selection failed without Error"),
    });

    fireEvent.click(await within(genericRuntime.container).findByLabelText("Выбрать Call note"));
    fireEvent.click(within(genericRuntime.container).getByRole("button", { name: "Запустить: 1" }));

    expect(await within(genericRuntime.container).findByText("Не удалось запустить обработку.")).toBeVisible();
  });

  it("renders localized selection counts for several selected materials", async () => {
    const baseRun = analysisRun();
    const twoItemRuntime = renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          selection_snapshot: {
            ...baseRun.selection_snapshot,
            items: [
              ...baseRun.selection_snapshot.items,
              {
                selection_snapshot_item_id: "snapshot-item-2",
                position: 1,
                media_asset_id: "media-2",
                kind: "audio",
                origin_snapshot: { origin_type: "object", object_ref: "web-local://interview.wav" },
                display_name: "Interview audio",
                status_at_selection: "ready",
              },
            ],
          },
        }),
      ),
    });

    expect(await within(twoItemRuntime.container).findByText("2 материала")).toBeVisible();
    twoItemRuntime.unmount();

    renderRoute("/runs/run-1", {
      getAnalysisRun: vi.fn().mockResolvedValue(
        analysisRun({
          selection_snapshot: {
            ...baseRun.selection_snapshot,
            items: Array.from({ length: 5 }, (_, index) => ({
              selection_snapshot_item_id: `snapshot-item-${index + 1}`,
              position: index,
              media_asset_id: `media-${index + 1}`,
              kind: "text",
              origin_snapshot: { origin_type: "text", text: `Note ${index + 1}` },
              display_name: `Note ${index + 1}`,
              status_at_selection: "ready",
            })),
          },
        }),
      ),
    });

    expect(await screen.findByText("5 материалов")).toBeVisible();
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
                      media_asset_id: "media-1",
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

    const outcomeTable = await within(runtime.container).findByText("включено");
    const metrics = within(outcomeTable.closest(".metric-strip") as HTMLElement);
    expect(metrics.getAllByText("0")).toHaveLength(3);
    const selectionItem = screen.getByText("Элемент подборки");
    expect(selectionItem).toBeVisible();
    const outcomeRow = selectionItem.closest(".outcome-row") as HTMLElement;
    expect(within(outcomeRow).getByText("Пропущено")).toBeVisible();
  });

  it("covers concrete Error message branches across route surfaces", async () => {
    renderRoute("/collections", {
      createCollection: vi.fn().mockRejectedValue(new Error("Collection create exploded")),
      updateCollection: vi.fn().mockRejectedValue(new Error("Collection update exploded")),
      removeCollectionItem: vi.fn().mockRejectedValue(new Error("Collection remove exploded")),
      replaceCollectionItems: vi.fn().mockRejectedValue(new Error("Collection add exploded")),
    });

    fireEvent.click(await screen.findByRole("button", { name: "Создать" }));
    expect(await screen.findByText("Collection create exploded")).toBeVisible();

    fireEvent.blur(screen.getByLabelText("Переименовать Research set"), { target: { value: "Renamed with error" } });
    expect(await screen.findByText("Collection update exploded")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Убрать" }));
    expect(await screen.findByText("Collection remove exploded")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Добавить материал"), { target: { value: "media-2" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Collection add exploded")).toBeVisible();
  });

  it("covers concrete Error object branches for inbox and collection actions", async () => {
    const inboxLoaderRuntime = renderRoute("/", {
      listMediaAssets: vi.fn().mockRejectedValue(new Error("Workspace exploded")),
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
      removeMediaAsset: vi.fn().mockRejectedValue(new Error("Inbox removal exploded")),
    });

    fireEvent.click(await screen.findByLabelText("Выбрать Call note"));
    fireEvent.click(screen.getByRole("button", { name: "Создать группу" }));
    expect(await screen.findByText("Inbox collection create exploded")).toBeVisible();

    fireEvent.change(screen.getByLabelText("Существующая группа"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить выбранное" }));
    expect(await screen.findByText("Inbox collection update exploded")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Удалить Call note" }));
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

    expect(await screen.findByLabelText("Переименовать Research set")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "В архив" }));
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

    expect(await within(manifestFallbackRuntime.container).findByText("Выбран в подборке")).toBeVisible();
    const progressEvent = await within(manifestFallbackRuntime.container).findByText("Прогресс");
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

    fireEvent.click(await within(runActionRuntime.container).findByRole("button", { name: "Остановить" }));
    expect(await within(runActionRuntime.container).findByText("Run cancel exploded")).toBeVisible();
    fireEvent.click(within(runActionRuntime.container).getByRole("button", { name: "Повторить" }));
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
        channel_account_id: channelAccountId,
        visibility: "channel_deliverable",
        diagnostics: undefined,
      }),
      refreshArtifact: vi.fn().mockRejectedValue(new Error("Artifact refresh exploded")),
      listDiagnostics: vi.fn().mockResolvedValue({
        items: [],
        page: { page_size: 50, has_more: false },
      }),
    });

    expect(await within(artifactRefreshRuntime.container).findByText("Проверок пока нет.")).toBeVisible();
    fireEvent.click(within(artifactRefreshRuntime.container).getByRole("button", { name: "Обновить ссылку" }));
    expect(await within(artifactRefreshRuntime.container).findByText("Artifact refresh exploded")).toBeVisible();
  });

  it("covers file-reset, diagnostics, and media-detail concrete Error branches", async () => {
    const inboxRuntime = renderRoute("/");

    fireEvent.click(await screen.findByRole("button", { name: "Файл" }));
    fireEvent.change(screen.getByLabelText("Файл"), {
      target: {
        files: [],
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Добавить" }));
    expect(await screen.findByText("Выберите файл.")).toBeVisible();
    inboxRuntime.unmount();

    const diagnosticsLoaderRuntime = renderRoute("/diagnostics", {
      listDiagnostics: vi.fn().mockRejectedValue(new Error("Diagnostics loader exploded")),
    });

    expect(await within(diagnosticsLoaderRuntime.container).findByText("Diagnostics loader exploded")).toBeVisible();
    diagnosticsLoaderRuntime.unmount();

    const mediaLoaderRuntime = renderRoute("/inbox/media-1", {
      getMediaAsset: vi.fn().mockRejectedValue(new Error("Media detail exploded")),
    });

    expect(await within(mediaLoaderRuntime.container).findByText("Media detail exploded")).toBeVisible();
    mediaLoaderRuntime.unmount();

    const mediaRuntime = renderRoute("/inbox/media-1", {
      removeMediaAsset: vi.fn().mockRejectedValue(new Error("Media removal exploded")),
    });

    fireEvent.click(await within(mediaRuntime.container).findByRole("button", { name: "Удалить Call note" }));
    expect(await within(mediaRuntime.container).findByText("Media removal exploded")).toBeVisible();
  });

  it("covers generic inbox fallback messages and stale run-builder origin labels", async () => {
    const inboxRuntime = renderRoute("/", {
      replaceCollectionItems: vi.fn().mockRejectedValue("boom"),
      removeMediaAsset: vi.fn().mockRejectedValue("boom"),
    });

    fireEvent.click(await screen.findByLabelText("Выбрать Call note"));
    fireEvent.change(screen.getByLabelText("Существующая группа"), { target: { value: "collection-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Добавить выбранное" }));
    expect(await screen.findByText("Не удалось обновить группу.")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Удалить Call note" }));
    expect(await screen.findByText("Не удалось удалить материал.")).toBeVisible();
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
    fireEvent.click(screen.getByRole("button", { name: "Обновить" }));
    await waitFor(() => {
      expect(runBuilderRuntime.apiClient.listCollections).toHaveBeenCalledTimes(2);
    });
    expect(await screen.findByText("Выбранная группа")).toBeVisible();
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

    expect(await within(runtime.container).findByText("Выбран в подборке")).toBeVisible();
    expect(within(runtime.container).queryByText("Outcome")).toBeNull();
  });
});
