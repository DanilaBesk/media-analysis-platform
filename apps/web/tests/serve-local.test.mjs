import { describe, expect, it } from "vitest";

import { resolveLocalServerEnv } from "../src/serve-local.mjs";

describe("local Web bootstrap", () => {
  it("keeps explicit server credentials and defaults only the internal API address", () => {
    expect(resolveLocalServerEnv({ PLATFORM_INTERNAL_TOKEN: "explicit" }, () => "")).toMatchObject({
      API_INTERNAL_BASE_URL: "http://127.0.0.1:8080",
      PLATFORM_INTERNAL_TOKEN: "explicit",
    });
  });

  it("loads the shared server token without placing it in browser runtime config", () => {
    expect(resolveLocalServerEnv({}, () => "PLATFORM_INTERNAL_TOKEN=shared-only\n")).toEqual({
      API_INTERNAL_BASE_URL: "http://127.0.0.1:8080",
      PLATFORM_INTERNAL_TOKEN: "shared-only",
    });
  });
});
