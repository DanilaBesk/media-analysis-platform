import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { MediaItemList } from "../src/features/media/media-workspace";
import type { MediaItemSummary } from "../src/lib/api/types";

const items: MediaItemSummary[] = [
  {
    media_item_id: "media-1",
    owner: { owner_type: "web", owner_id: "u-1" },
    kind: "text",
    status: "ready",
    display_name: "First note",
    source: {
      source_id: "source-1",
      origin_type: "text",
      text_ref: "inline:source-1",
    },
    diagnostics_count: 0,
    retention: { state: "active" },
    created_at: "2026-05-10T00:00:00Z",
    updated_at: "2026-05-10T00:00:00Z",
  },
];

describe("MediaItemList", () => {
  it("renders without selection controls when toggle callbacks are absent", () => {
    render(
      <MemoryRouter>
        <MediaItemList items={items} selected={new Set()} />
      </MemoryRouter>,
    );

    expect(screen.queryByLabelText("Управление подборкой")).toBeNull();
    expect(screen.queryByLabelText("Выбрать First note")).toBeNull();
    expect(screen.getByRole("link", { name: "First note" })).toBeVisible();
  });

  it("renders an empty state when there are no items", () => {
    render(
      <MemoryRouter>
        <MediaItemList items={[]} selected={new Set()} onToggle={vi.fn()} onSelectAll={vi.fn()} onClearSelection={vi.fn()} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Материалов пока нет.")).toBeVisible();
  });
});
