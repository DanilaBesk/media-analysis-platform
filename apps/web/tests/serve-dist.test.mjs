import { describe, expect, it, vi } from "vitest";

import { createWebUiServer, resolveChannelAccountId, runtimeConfigScript } from "../src/serve-dist.mjs";

const DISCOVERED_CHANNEL_ACCOUNT_ID = "88888888-8888-4888-8888-888888888888";

describe("web static server channel account bootstrap", () => {
  it("uses a valid explicit override without calling the internal API", async () => {
    const fetchImpl = vi.fn();

    await expect(
      resolveChannelAccountId({ WEB_CHANNEL_ACCOUNT_ID: DISCOVERED_CHANNEL_ACCOUNT_ID }, fetchImpl),
    ).resolves.toBe(DISCOVERED_CHANNEL_ACCOUNT_ID);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("discovers the most recently active Telegram UUID account through the internal API", async () => {
    const olderTelegramId = "77777777-7777-4777-8777-777777777777";
    const fetchImpl = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [
          { channel_account_id: "not-a-uuid", channel: "telegram", status: "active" },
          { channel_account_id: "99999999-9999-4999-8999-999999999999", channel: "mcp", status: "active", last_seen_at: "2026-07-26T12:00:00Z" },
          { channel_account_id: olderTelegramId, channel: "telegram", status: "active", last_seen_at: "2026-07-25T12:00:00Z" },
          { channel_account_id: DISCOVERED_CHANNEL_ACCOUNT_ID, channel: "telegram", status: "active", last_seen_at: "2026-07-26T11:00:00Z" },
        ],
      }),
    });

    await expect(
      resolveChannelAccountId(
        { API_INTERNAL_BASE_URL: "http://api:8080/", PLATFORM_INTERNAL_TOKEN: "server-only-token" },
        fetchImpl,
      ),
    ).resolves.toBe(DISCOVERED_CHANNEL_ACCOUNT_ID);
    expect(fetchImpl).toHaveBeenCalledWith(
      "http://api:8080/internal/v1/channel-accounts?page_size=50",
      { headers: { "X-Platform-Internal-Token": "server-only-token" } },
    );
  });

  it("rejects invalid overrides and never serializes the internal token", async () => {
    await expect(
      resolveChannelAccountId({ WEB_CHANNEL_ACCOUNT_ID: "web-console" }, vi.fn()),
    ).rejects.toThrow("WEB_CHANNEL_ACCOUNT_ID must be a UUID");

    const script = runtimeConfigScript(DISCOVERED_CHANNEL_ACCOUNT_ID);
    expect(script).toContain(DISCOVERED_CHANNEL_ACCOUNT_ID);
    expect(script).not.toContain("PLATFORM_INTERNAL_TOKEN");
    expect(script).not.toContain("server-only-token");
  });

  it("returns a client error for a malformed encoded request path", async () => {
    const server = createWebUiServer(DISCOVERED_CHANNEL_ACCOUNT_ID);
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    expect(address).toBeTypeOf("object");
    try {
      const response = await fetch(`http://127.0.0.1:${address.port}/%`);
      expect(response.status).toBe(400);
      expect(await response.text()).toBe("invalid request path");
    } finally {
      await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    }
  });
});
