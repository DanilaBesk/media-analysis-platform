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
import { extname, join, normalize, resolve } from "node:path";

const HOST = process.env.HOST ?? "0.0.0.0";
const PORT = Number(process.env.PORT ?? "3201");
const DIST_DIR = resolve(process.env.WEB_DIST_DIR ?? new URL("../dist", import.meta.url).pathname);
const MARKER = "[WebUi][serveDist][BLOCK_SERVE_WEB_UI_DIST]";

const contentTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
]);

function resolveAssetPath(requestUrl) {
  const parsed = new URL(requestUrl, `http://${HOST}:${PORT}`);
  if (parsed.pathname === "/healthz") {
    return null;
  }
  const normalized = normalize(decodeURIComponent(parsed.pathname)).replace(/^(\.\.[/\\])+/, "");
  const candidate = resolve(join(DIST_DIR, normalized));
  if (candidate.startsWith(DIST_DIR) && existsSync(candidate) && statSync(candidate).isFile()) {
    return candidate;
  }
  return join(DIST_DIR, "index.html");
}

const server = createServer((request, response) => {
  if (request.url === "/healthz") {
    response.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    response.end(JSON.stringify({ ok: true }));
    return;
  }

  const assetPath = resolveAssetPath(request.url ?? "/");
  if (!assetPath || !existsSync(assetPath)) {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("not found");
    return;
  }

  response.writeHead(200, {
    "Content-Type": contentTypes.get(extname(assetPath)) ?? "application/octet-stream",
  });
  createReadStream(assetPath).pipe(response);
});

server.listen(PORT, HOST, () => {
  console.info(`${MARKER} host=${HOST} port=${PORT} dist=${DIST_DIR}`);
});
