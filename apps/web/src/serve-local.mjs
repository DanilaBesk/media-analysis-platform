import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { startServer } from "./serve-dist.mjs";

const SHARED_ENV = new URL("../../../infra/env/shared.env.example", import.meta.url);

export function resolveLocalServerEnv(env = process.env, readFile = readFileSync) {
  const resolved = { ...env };
  resolved.API_INTERNAL_BASE_URL ||= "http://127.0.0.1:8080";
  if (!resolved.PLATFORM_INTERNAL_TOKEN) {
    const shared = readFile(SHARED_ENV, "utf8");
    const tokenLine = shared.split(/\r?\n/).find((line) => line.startsWith("PLATFORM_INTERNAL_TOKEN="));
    resolved.PLATFORM_INTERNAL_TOKEN = tokenLine?.slice("PLATFORM_INTERNAL_TOKEN=".length).trim() ?? "";
  }
  if (!resolved.PLATFORM_INTERNAL_TOKEN) {
    throw new Error("PLATFORM_INTERNAL_TOKEN is required for local Web bootstrap");
  }
  return resolved;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  Object.assign(process.env, resolveLocalServerEnv());
  startServer().catch((error) => {
    console.error(`[WebUi][serveLocal][BLOCK_BOOTSTRAP] ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  });
}
