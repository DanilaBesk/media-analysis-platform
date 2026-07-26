// FILE: apps/web/src/serve-dist.mjs
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Serve the built Web UI bundle as a compose-ready runtime entrypoint.
// SCOPE: Static dist file serving, SPA fallback, and a lightweight health endpoint.
// DEPENDS: M-WEB-UI, M-INFRA-COMPOSE
// LINKS: M-WEB-UI, V-M-WEB-UI
// ROLE: SCRIPT
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added a compose-ready static Web UI server with health endpoint.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   serve-web-ui-dist - Serve dist assets, SPA fallback responses, and /healthz for compose convergence.
// END_MODULE_MAP

import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { extname, isAbsolute, join, normalize, relative, resolve, sep } from "node:path";

const HOST = process.env.HOST ?? "0.0.0.0";
const PORT = Number(process.env.PORT ?? "3201");
const DIST_DIR = resolve(process.env.WEB_DIST_DIR ?? new URL("../dist", import.meta.url).pathname);
const MARKER = "[WebUi][serveDist][BLOCK_SERVE_WEB_UI_DIST]";
const CHANNEL_ACCOUNT_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const contentTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
]);

export function isChannelAccountId(value) {
  return typeof value === "string" && CHANNEL_ACCOUNT_ID_PATTERN.test(value.trim());
}

export async function resolveChannelAccountId(
  env = process.env,
  fetchImpl = fetch,
) {
  const override = env.WEB_CHANNEL_ACCOUNT_ID?.trim();
  if (override && isChannelAccountId(override)) {
    return override;
  }
  if (override) {
    throw new Error("WEB_CHANNEL_ACCOUNT_ID must be a UUID");
  }

  const apiBaseUrl = env.API_INTERNAL_BASE_URL?.trim();
  const internalToken = env.PLATFORM_INTERNAL_TOKEN?.trim();
  if (!apiBaseUrl || !internalToken) {
    throw new Error("API_INTERNAL_BASE_URL and PLATFORM_INTERNAL_TOKEN are required when WEB_CHANNEL_ACCOUNT_ID is not set");
  }
  const response = await fetchImpl(`${apiBaseUrl.replace(/\/$/, "")}/internal/v1/channel-accounts?page_size=50`, {
    headers: { "X-Platform-Internal-Token": internalToken },
  });
  if (!response.ok) {
    throw new Error(`channel account discovery failed with status ${response.status}`);
  }
  const payload = await response.json();
  const active = Array.isArray(payload?.items)
    ? payload.items
      .filter((account) => (
        account?.channel === "telegram"
        && account?.status === "active"
        && isChannelAccountId(account.channel_account_id)
      ))
      .sort((left, right) => accountActivityTime(right) - accountActivityTime(left))[0]
    : undefined;
  if (!active) {
    throw new Error("channel account discovery returned no active Telegram UUID account");
  }
  return active.channel_account_id.trim();
}

function accountActivityTime(account) {
  const value = account?.last_seen_at ?? account?.updated_at ?? account?.created_at;
  const parsed = typeof value === "string" ? Date.parse(value) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : 0;
}

export function runtimeConfigScript(channelAccountId) {
  return `window.__WEB_UI_RUNTIME__=${JSON.stringify({ channelAccountId })};`;
}

function resolveAssetPath(requestUrl) {
  const parsed = new URL(requestUrl, `http://${HOST}:${PORT}`);
  if (parsed.pathname === "/healthz") {
    return null;
  }
  let decodedPathname;
  try {
    decodedPathname = decodeURIComponent(parsed.pathname);
  } catch {
    return undefined;
  }
  const normalized = normalize(decodedPathname).replace(/^(\.\.[/\\])+/, "");
  const candidate = resolve(join(DIST_DIR, normalized));
  const relativePath = relative(DIST_DIR, candidate);
  const insideDist = relativePath === "" || (
    relativePath !== ".."
    && !relativePath.startsWith(`..${sep}`)
    && !isAbsolute(relativePath)
  );
  if (insideDist && existsSync(candidate) && statSync(candidate).isFile()) {
    return candidate;
  }
  return join(DIST_DIR, "index.html");
}

export function createWebUiServer(channelAccountId) {
  return createServer((request, response) => {
    if (request.url === "/healthz") {
      response.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      response.end(JSON.stringify({ ok: true }));
      return;
    }
    if (request.url === "/runtime-config.js") {
      response.writeHead(200, {
        "Cache-Control": "no-store",
        "Content-Type": "text/javascript; charset=utf-8",
      });
      response.end(runtimeConfigScript(channelAccountId));
      return;
    }

    const assetPath = resolveAssetPath(request.url ?? "/");
    if (assetPath === undefined) {
      response.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
      response.end("invalid request path");
      return;
    }
    if (!assetPath || !existsSync(assetPath)) {
      response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      response.end("not found");
      return;
    }

    const stream = createReadStream(assetPath);
    stream.once("open", () => {
      response.writeHead(200, {
        "Content-Type": contentTypes.get(extname(assetPath)) ?? "application/octet-stream",
      });
      stream.pipe(response);
    });
    stream.once("error", (error) => {
      console.error(`${MARKER} asset_read_failed=${error.code ?? "unknown"}`);
      if (!response.headersSent) {
        response.writeHead(503, { "Content-Type": "text/plain; charset=utf-8", "Retry-After": "1" });
        response.end("web assets are being updated");
        return;
      }
      response.destroy();
    });
  });
}

export async function startServer() {
  const channelAccountId = await resolveChannelAccountId();
  const server = createWebUiServer(channelAccountId);
  server.listen(PORT, HOST, () => {
    console.info(`${MARKER} host=${HOST} port=${PORT} dist=${DIST_DIR} channel_account_id=${channelAccountId}`);
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  startServer().catch((error) => {
    console.error(`${MARKER} startup_failed=${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  });
}
