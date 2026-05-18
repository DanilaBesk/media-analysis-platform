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

describe("MediaAssetList", () => {
  it("renders without selection controls when toggle callbacks are absent", () => {
    render(
      <MemoryRouter>
        <MediaAssetList items={items} selected={new Set()} />
      </MemoryRouter>,
    );

    expect(screen.queryByLabelText("Управление подборкой")).toBeNull();
    expect(screen.queryByLabelText("Выбрать First note")).toBeNull();
    expect(screen.getByRole("link", { name: "First note" })).toBeVisible();
  });

  it("renders an empty state when there are no items", () => {
    render(
      <MemoryRouter>
        <MediaAssetList items={[]} selected={new Set()} onToggle={vi.fn()} onSelectAll={vi.fn()} onClearSelection={vi.fn()} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Материалов пока нет.")).toBeVisible();
  });
});
