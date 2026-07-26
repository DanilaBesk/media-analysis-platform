import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { MediaAssetList } from "../src/features/media/media-workspace";
import type { MediaAssetSummary } from "../src/lib/api/types";

const items: MediaAssetSummary[] = [
  {
    media_asset_id: "media-1",
    channel_account_id: "web-console",
    kind: "text",
    status: "ready",
    display_name: "First note",
    origin: {
      origin_type: "text",
      text: "First note",
    },
    diagnostics_count: 0,
    created_at: "2026-05-10T00:00:00Z",
    updated_at: "2026-05-10T00:00:00Z",
  },
];
const routerFuture = { v7_startTransition: true, v7_relativeSplatPath: true } as const;

describe("MediaAssetList", () => {
  it("renders without selection controls when toggle callbacks are absent", () => {
    render(
      <MemoryRouter future={routerFuture}>
        <MediaAssetList items={items} selected={new Set()} />
      </MemoryRouter>,
    );

    expect(screen.queryByLabelText("Управление подборкой")).toBeNull();
    expect(screen.queryByLabelText("Выбрать First note")).toBeNull();
    expect(screen.getByRole("link", { name: "First note" })).toBeVisible();
  });

  it("renders an empty state when there are no items", () => {
    render(
      <MemoryRouter future={routerFuture}>
        <MediaAssetList items={[]} selected={new Set()} onToggle={vi.fn()} onSelectAll={vi.fn()} onClearSelection={vi.fn()} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Материалов пока нет.")).toBeVisible();
  });

  it("keeps a pending YouTube URL out of the row while enrichment is running", () => {
    const sourceUrl = "https://www.youtube.com/watch?v=pending-video";
    render(
      <MemoryRouter future={routerFuture}>
        <MediaAssetList
          items={[{
            ...items[0],
            media_asset_id: "youtube-pending",
            kind: "url",
            display_name: sourceUrl,
            origin: { origin_type: "url", url: sourceUrl },
            metadata: { provider_metadata: { provider: "youtube", status: "pending" } },
          }]}
          selected={new Set()}
        />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: "Видео YouTube" })).toBeVisible();
    expect(screen.getByText("Метаданные YouTube загружаются")).toBeVisible();
    expect(screen.queryByText(sourceUrl)).toBeNull();
  });

  it("uses top-level YouTube metadata before the legacy metadata envelope", () => {
    render(
      <MemoryRouter future={routerFuture}>
        <MediaAssetList
          items={[{
            ...items[0],
            media_asset_id: "youtube-ready",
            kind: "url",
            display_name: "YouTube: ready-video",
            origin: { origin_type: "url", url: "https://youtu.be/ready-video" },
            provider_metadata: {
              provider: "youtube",
              status: "succeeded",
              title: "Public API walkthrough",
              duration_seconds: 3723.4,
            },
            metadata: { provider_metadata: { provider: "youtube", title: "Older cached title", duration_seconds: "invalid" } },
          }]}
          selected={new Set()}
        />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: "Public API walkthrough" })).toBeVisible();
    expect(screen.getByText("YouTube · 1:02:03")).toBeVisible();
    expect(screen.queryByText("Older cached title")).toBeNull();
  });

  it("falls back to the pending label when optional provider metadata is malformed", () => {
    render(
      <MemoryRouter future={routerFuture}>
        <MediaAssetList
          items={[{
            ...items[0],
            media_asset_id: "youtube-malformed",
            kind: "url",
            display_name: "YouTube: malformed-video",
            origin: { origin_type: "url", url: "https://youtu.be/malformed-video" },
            provider_metadata: { provider: "youtube", title: 42, duration_seconds: "not-a-number", status: "pending" },
          }]}
          selected={new Set()}
        />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: "YouTube: malformed-video" })).toBeVisible();
    expect(screen.getByText("Метаданные YouTube загружаются")).toBeVisible();
  });

  it("uses an optional enrichment envelope when provider metadata is not present", () => {
    render(
      <MemoryRouter future={routerFuture}>
        <MediaAssetList
          items={[{
            ...items[0],
            media_asset_id: "youtube-enrichment",
            kind: "url",
            display_name: "YouTube: enriched-video",
            origin: { origin_type: "url", url: "https://youtu.be/enriched-video" },
            enrichment: { provider: "youtube", status: "succeeded", title: "Enriched title", duration_seconds: 61 },
          }]}
          selected={new Set()}
        />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: "Enriched title" })).toBeVisible();
    expect(screen.getByText("YouTube · 1:01")).toBeVisible();
  });
});
