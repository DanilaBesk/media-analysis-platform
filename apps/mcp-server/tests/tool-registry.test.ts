// FILE: apps/mcp-server/tests/tool-registry.test.ts
// VERSION: 3.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove the MCP adapter exposes final domain tools, registers them on the SDK runtime, and preserves contract-shaped failures.
// SCOPE: Verify tool listing, representative API mappings, SDK registration state, marker logging, and deterministic error shaping.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v3.0.0 - Replaced old execution assertions with inbox-first domain tool and SDK runtime verification.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   verify-domain-tool-list - Confirm the runtime exposes the final media, collection, selection, run, artifact, and diagnostic tools.
//   verify-tool-dispatch - Confirm representative tools call only inbox-first HTTP API paths.
//   verify-sdk-registration - Confirm tools are registered on a real McpServer instance.
//   verify-error-shaping - Confirm adapter validation and upstream failures remain structured.
// END_MODULE_MAP

import test from "node:test";
import assert from "node:assert/strict";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

import {
  McpAdapterApiClientError,
  type McpAdapterApiClient,
  type McpAdapterApiRequest,
} from "../src/client/api-client.ts";
import {
  MCP_TOOL_MAPPING_MARKER,
  createMcpDomainRuntime,
} from "../src/tools/registry.ts";

const OWNER = {
  owner_type: "mcp",
  owner_id: "assistant",
  adapter_identity: {
    mcp_caller_id: "codex",
  },
};
const OWNER_QUERY = "owner_type=mcp&owner_id=assistant";

function withOwnerQuery(path: string): string {
  if (!path.includes("?")) {
    return `${path}?${OWNER_QUERY}`;
  }
  const [basePath, query] = path.split("?", 2);
  return `${basePath}?${OWNER_QUERY}&${query}`;
}

const MEDIA_ID = "00000000-0000-4000-8000-000000000001";
const COLLECTION_ID = "00000000-0000-4000-8000-000000000002";
const SELECTION_ID = "00000000-0000-4000-8000-000000000003";
const RUN_ID = "00000000-0000-4000-8000-000000000004";
const ARTIFACT_ID = "00000000-0000-4000-8000-000000000005";
const JSON_ARTIFACT_ID = "00000000-0000-4000-8000-000000000006";
const TEXT_ARTIFACT_ID = "00000000-0000-4000-8000-000000000007";

const REQUIRED_TOOLS = [
  "add_media",
  "list_media",
  "search_media",
  "get_media",
  "remove_media",
  "get_inbox",
  "create_collection",
  "list_collections",
  "get_collection",
  "update_collection",
  "update_collection_items",
  "create_selection",
  "get_selection",
  "run_analysis",
  "list_runs",
  "get_run",
  "cancel_run",
  "retry_run",
  "list_run_events",
  "list_artifacts",
  "get_artifact",
  "get_artifact_preview",
  "refresh_artifact",
  "get_diagnostics",
] as const;

test("createMcpDomainRuntime exposes the required final domain tools", () => {
  // START_BLOCK_BLOCK_VERIFY_DOMAIN_TOOL_LIST
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async () => ({
        status: 200,
        data: null,
      }),
    },
  });

  assert.ok(runtime.server instanceof McpServer);
  assert.deepEqual(
    runtime.listTools().map((tool) => tool.name),
    [...REQUIRED_TOOLS],
  );
  assert.equal(runtime.listTools().every((tool) => tool.description.length > 20), true);
  assert.equal(
    runtime.listTools().every((tool) => ((tool as any).examples ?? []).length > 0),
    true,
  );
  assert.equal(
    runtime.listTools().every((tool) => Boolean((tool as any).outputSchema)),
    true,
  );
  assert.equal(
    runtime
      .listTools()
      .some((tool) => /transcription|batch|old route|dual path/i.test(tool.name + tool.description)),
    false,
  );
  // END_BLOCK_BLOCK_VERIFY_DOMAIN_TOOL_LIST
});

test("createMcpDomainRuntime maps representative domain tools to inbox-first API calls", async () => {
  // START_BLOCK_BLOCK_VERIFY_TOOL_DISPATCH
  const requests: unknown[] = [];
  const logs: string[] = [];
  const apiClient: McpAdapterApiClient = {
    request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
      requests.push(request);
      if (request.path === "/v1/media-items") {
        return {
          status: 201,
          data: {
            media_item: {
              media_item_id: MEDIA_ID,
              status: "ready",
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery("/v1/media-items?query=meeting&cursor=c1&page_size=10&kind=text&status=ready")) {
        return {
          status: 200,
          data: {
            items: [
              {
                media_item_id: MEDIA_ID,
              },
            ],
            page: {
              page_size: 10,
              next_cursor: "c2",
              has_more: true,
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/collections/${COLLECTION_ID}`)) {
        return {
          status: 200,
          data: {
            collection: {
              collection_id: COLLECTION_ID,
              name: "Research clips",
              version: 2,
            },
          } as TPayload,
        };
      }
      if (request.path === `/v1/collections/${COLLECTION_ID}` && request.method === "PATCH") {
        return {
          status: 200,
          data: {
            collection: {
              collection_id: COLLECTION_ID,
              name: "Research clips",
              version: 2,
            },
          } as TPayload,
        };
      }
      if (request.path === `/v1/collections/${COLLECTION_ID}/items`) {
        return {
          status: 200,
          data: {
            collection: {
              collection_id: COLLECTION_ID,
              version: 2,
            },
          } as TPayload,
        };
      }
      if (request.path === "/v1/analysis-runs") {
        return {
          status: 202,
          data: {
            analysis_run: {
              analysis_run_id: RUN_ID,
              status: "queued",
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/retry`)) {
        return {
          status: 202,
          data: {
            analysis_run: {
              analysis_run_id: "00000000-0000-4000-8000-000000000006",
              status: "queued",
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/events?cursor=e1&page_size=3`)) {
        return {
          status: 200,
          data: {
            items: [
              {
                event_id: "event-1",
                analysis_run_id: RUN_ID,
                event_type: "diagnostic.recorded",
              },
            ],
            page: {
              page_size: 3,
              has_more: false,
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/artifacts?cursor=next&page_size=5`)) {
        return {
          status: 200,
          data: {
            items: [
              {
                artifact_id: ARTIFACT_ID,
              },
            ],
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`)) {
        return {
          status: 200,
          data: {
            artifact: {
              artifact_id: ARTIFACT_ID,
              preview: {
                available: true,
                kind: "text",
                content_type: "text/markdown",
                text_excerpt: "# Refreshed preview",
              },
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery("/v1/diagnostics?cursor=d1&page_size=2&subject_type=analysis_run&subject_id=00000000-0000-4000-8000-000000000004&severity=warning")) {
        return {
          status: 200,
          data: {
            items: [
              {
                diagnostic_id: "diagnostic-1",
                code: "run_failed",
              },
            ],
            page: {
              page_size: 2,
              has_more: false,
            },
          } as TPayload,
        };
      }
      throw new Error(`unexpected request path ${request.path}`);
    },
  };
  const runtime = createMcpDomainRuntime({
    apiClient,
    logger: {
      log(message) {
        logs.push(message);
      },
    },
  });

  const addResult = await runtime.callTool("add_media", {
    owner: OWNER,
    kind: "text",
    source: {
      origin_type: "text",
      text: "Meeting transcript fragment",
    },
    display_name: "Meeting notes",
    idempotency_key: "add-media-1",
  });
  const searchResult = await runtime.callTool("search_media", {
    owner: OWNER,
    query: "meeting",
    cursor: "c1",
    page_size: 10,
    kind: "text",
    status: "ready",
  });
  const updateCollectionResult = await runtime.callTool("update_collection", {
    collection_id: COLLECTION_ID,
    owner: OWNER,
    expected_version: 1,
    name: "Research clips",
  });
  const updateItemsResult = await runtime.callTool("update_collection_items", {
    collection_id: COLLECTION_ID,
    owner: OWNER,
    expected_version: 1,
    items: [
      {
        media_item_id: MEDIA_ID,
        position: 0,
      },
    ],
  });
  const runResult = await runtime.callTool("run_analysis", {
    owner: OWNER,
    selection_id: SELECTION_ID,
    run_type: "summary",
  });
  const retryResult = await runtime.callTool("retry_run", {
    analysis_run_id: RUN_ID,
    owner: OWNER,
    idempotency_key: "retry-run-1",
  });
  const eventsResult = await runtime.callTool("list_run_events", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
    cursor: "e1",
    page_size: 3,
  });
  const artifactsResult = await runtime.callTool("list_artifacts", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
    cursor: "next",
    page_size: 5,
  });
  const refreshArtifactResult = await runtime.callTool("refresh_artifact", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
  });
  const diagnosticsResult = await runtime.callTool("get_diagnostics", {
    owner: OWNER,
    subject_type: "analysis_run",
    subject_id: RUN_ID,
    severity: "warning",
    cursor: "d1",
    page_size: 2,
  });

  assert.equal(logs.length, 10);
  assert.ok((logs[0] ?? "").startsWith(MCP_TOOL_MAPPING_MARKER));
  assert.deepEqual(addResult.structuredContent, {
    media_item: {
      media_item_id: MEDIA_ID,
      status: "ready",
    },
  });
  assert.deepEqual(searchResult.structuredContent, {
    items: [
      {
        media_item_id: MEDIA_ID,
      },
    ],
    page: {
      page_size: 10,
      next_cursor: "c2",
      has_more: true,
    },
  });
  assert.deepEqual(updateCollectionResult.structuredContent, {
    collection: {
      collection_id: COLLECTION_ID,
      name: "Research clips",
      version: 2,
    },
  });
  assert.deepEqual(updateItemsResult.structuredContent, {
    collection: {
      collection_id: COLLECTION_ID,
      version: 2,
    },
  });
  assert.deepEqual(runResult.structuredContent, {
    analysis_run: {
      analysis_run_id: RUN_ID,
      status: "queued",
    },
  });
  assert.deepEqual(retryResult.structuredContent, {
    analysis_run: {
      analysis_run_id: "00000000-0000-4000-8000-000000000006",
      status: "queued",
    },
  });
  assert.deepEqual(eventsResult.structuredContent, {
    items: [
      {
        event_id: "event-1",
        analysis_run_id: RUN_ID,
        event_type: "diagnostic.recorded",
      },
    ],
    page: {
      page_size: 3,
      has_more: false,
    },
  });
  assert.deepEqual(artifactsResult.structuredContent, {
    items: [
      {
        artifact_id: ARTIFACT_ID,
      },
    ],
  });
  assert.deepEqual(refreshArtifactResult.structuredContent, {
    artifact: {
      artifact_id: ARTIFACT_ID,
      preview: {
        available: true,
        kind: "text",
        content_type: "text/markdown",
        text_excerpt: "# Refreshed preview",
      },
    },
  });
  assert.deepEqual(diagnosticsResult.structuredContent, {
    items: [
      {
        diagnostic_id: "diagnostic-1",
        code: "run_failed",
      },
    ],
    page: {
      page_size: 2,
      has_more: false,
    },
  });
  assert.deepEqual(requests, [
    {
      path: "/v1/media-items",
      method: "POST",
      headers: {
        "Idempotency-Key": "add-media-1",
      },
      body: {
        owner: OWNER,
        kind: "text",
        source: {
          origin_type: "text",
          text: "Meeting transcript fragment",
        },
        display_name: "Meeting notes",
      },
    },
    {
      path: withOwnerQuery("/v1/media-items?query=meeting&cursor=c1&page_size=10&kind=text&status=ready"),
    },
    {
      path: `/v1/collections/${COLLECTION_ID}`,
      method: "PATCH",
      body: {
        owner: OWNER,
        expected_version: 1,
        name: "Research clips",
      },
    },
    {
      path: `/v1/collections/${COLLECTION_ID}/items`,
      method: "POST",
      body: {
        owner: OWNER,
        expected_version: 1,
        items: [
          {
            media_item_id: MEDIA_ID,
            position: 0,
          },
        ],
      },
    },
    {
      path: "/v1/analysis-runs",
      method: "POST",
      headers: undefined,
      body: {
        owner: OWNER,
        selection_id: SELECTION_ID,
        run_type: "summary",
      },
    },
    {
      path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/retry`),
      method: "POST",
      headers: {
        "Idempotency-Key": "retry-run-1",
      },
      body: {
        owner: OWNER,
      },
    },
    {
      path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/events?cursor=e1&page_size=3`),
    },
    {
      path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/artifacts?cursor=next&page_size=5`),
    },
    {
      path: withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`),
      method: "POST",
    },
    {
      path: withOwnerQuery(
        "/v1/diagnostics?cursor=d1&page_size=2&subject_type=analysis_run&subject_id=00000000-0000-4000-8000-000000000004&severity=warning",
      ),
    },
  ]);
  // END_BLOCK_BLOCK_VERIFY_TOOL_DISPATCH
});

test("createMcpDomainRuntime covers the full inbox-first media lifecycle without execution wrappers", async () => {
  // START_BLOCK_BLOCK_VERIFY_FULL_INBOX_FIRST_LIFECYCLE
  const requests: McpAdapterApiRequest[] = [];
  const responseByPath = new Map<string, unknown>([
    [
      "/v1/media-items",
      {
        media_item: {
          media_item_id: MEDIA_ID,
          status: "ready",
        },
      },
    ],
    [
      withOwnerQuery("/v1/media-items?cursor=m1&page_size=25&kind=text&status=ready"),
      {
        items: [{ media_item_id: MEDIA_ID }],
        page: { page_size: 25, has_more: false },
      },
    ],
    [
      withOwnerQuery("/v1/media-items?query=meeting&kind=url"),
      {
        items: [{ media_item_id: MEDIA_ID }],
        page: { page_size: 50, has_more: false },
      },
    ],
    [withOwnerQuery(`/v1/media-items/${MEDIA_ID}`), { media_item: { media_item_id: MEDIA_ID } }],
    [
      withOwnerQuery("/v1/collections/inbox?cursor=i1&page_size=10"),
      {
        collection: {
          collection_id: COLLECTION_ID,
          kind: "inbox",
          items: [{ media_item_id: MEDIA_ID, position: 0 }],
        },
      },
    ],
    [
      "/v1/collections",
      {
        collection: {
          collection_id: COLLECTION_ID,
          name: "Research clips",
          version: 1,
        },
      },
    ],
    [
      withOwnerQuery("/v1/collections?cursor=c1&page_size=10"),
      {
        items: [{ collection_id: COLLECTION_ID }],
        page: { page_size: 10, has_more: false },
      },
    ],
    [
      withOwnerQuery(`/v1/collections/${COLLECTION_ID}?cursor=ci1&page_size=5`),
      {
        collection: {
          collection_id: COLLECTION_ID,
          items: [{ media_item_id: MEDIA_ID, position: 0 }],
        },
      },
    ],
    [
      `/v1/collections/${COLLECTION_ID}`,
      {
        collection: {
          collection_id: COLLECTION_ID,
          name: "Research clips v2",
          version: 2,
        },
      },
    ],
    [
      `/v1/collections/${COLLECTION_ID}/items`,
      {
        collection: {
          collection_id: COLLECTION_ID,
          version: 3,
        },
      },
    ],
    [
      "/v1/selections",
      {
        selection: {
          selection_id: SELECTION_ID,
          source_collection_id: COLLECTION_ID,
        },
      },
    ],
    [withOwnerQuery(`/v1/selections/${SELECTION_ID}`), { selection: { selection_id: SELECTION_ID } }],
    [
      "/v1/analysis-runs",
      {
        analysis_run: {
          analysis_run_id: RUN_ID,
          status: "queued",
        },
      },
    ],
    [
      withOwnerQuery("/v1/analysis-runs?cursor=r1&page_size=10&status=queued"),
      {
        items: [{ analysis_run_id: RUN_ID }],
        page: { page_size: 10, has_more: false },
      },
    ],
    [withOwnerQuery(`/v1/analysis-runs/${RUN_ID}`), { analysis_run: { analysis_run_id: RUN_ID } }],
    [
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/cancel`),
      {
        analysis_run: {
          analysis_run_id: RUN_ID,
          status: "cancel_requested",
        },
      },
    ],
    [
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/retry`),
      {
        analysis_run: {
          analysis_run_id: "00000000-0000-4000-8000-000000000006",
          status: "queued",
        },
      },
    ],
    [
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/events?cursor=e1&page_size=3`),
      {
        items: [{ event_id: "event-1", event_type: "run.started" }],
        page: { page_size: 3, has_more: false },
      },
    ],
    [
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/artifacts?cursor=a1&page_size=3`),
      {
        items: [{ artifact_id: ARTIFACT_ID }],
        page: { page_size: 3, has_more: false },
      },
    ],
    [withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}`), { artifact: { artifact_id: ARTIFACT_ID } }],
    [
      withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`),
      {
        artifact: {
          artifact_id: ARTIFACT_ID,
          preview: {
            available: true,
            kind: "text",
            content_type: "text/markdown",
            text_excerpt: "# Refreshed preview",
          },
        },
      },
    ],
    [
      withOwnerQuery(`/v1/diagnostics?subject_type=media_item&subject_id=${MEDIA_ID}&severity=info`),
      {
        items: [{ diagnostic_id: "diagnostic-1" }],
        page: { page_size: 50, has_more: false },
      },
    ],
  ]);
  const apiClient: McpAdapterApiClient = {
    request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
      requests.push(request);
      if (request.body instanceof FormData) {
        return {
          status: 201,
          data: {
            media_item: {
              media_item_id: "00000000-0000-4000-8000-000000000007",
              status: "ready",
            },
          } as TPayload,
        };
      }
      const response = responseByPath.get(request.path);
      if (response === undefined) {
        throw new Error(`unexpected request path ${request.path}`);
      }
      return {
        status: request.method === "POST" ? 201 : 200,
        data: response as TPayload,
      };
    },
  };
  const runtime = createMcpDomainRuntime({ apiClient });

  await runtime.callTool("add_media", {
    owner: OWNER,
    kind: "text",
    source: {
      origin_type: "text",
      text: "Meeting transcript fragment",
      language_hint: "en",
    },
    metadata: {
      source: "mcp-lifecycle-test",
    },
  });
  await runtime.callTool("add_media", {
    owner: OWNER,
    kind: "url",
    source: {
      origin_type: "url",
      url: "https://example.test/research-note",
    },
    display_name: "Research note",
  });
  await runtime.callTool("add_media", {
    owner: OWNER,
    kind: "audio",
    file: {
      filename: "clip.mp3",
      content_type: "audio/mpeg",
      content_base64: Buffer.from("audio-data").toString("base64"),
    },
    collection_id: COLLECTION_ID,
    display_name: "Interview clip",
  });
  await runtime.callTool("list_media", {
    owner: OWNER,
    cursor: "m1",
    page_size: 25,
    kind: "text",
    status: "ready",
  });
  await runtime.callTool("search_media", {
    owner: OWNER,
    query: "meeting",
    kind: "url",
  });
  await runtime.callTool("get_media", {
    owner: OWNER,
    media_item_id: MEDIA_ID,
  });
  await runtime.callTool("get_inbox", {
    owner: OWNER,
    cursor: "i1",
    page_size: 10,
  });
  await runtime.callTool("create_collection", {
    owner: OWNER,
    name: "Research clips",
    items: [MEDIA_ID],
  });
  await runtime.callTool("list_collections", {
    owner: OWNER,
    cursor: "c1",
    page_size: 10,
  });
  await runtime.callTool("get_collection", {
    owner: OWNER,
    collection_id: COLLECTION_ID,
    cursor: "ci1",
    page_size: 5,
  });
  await runtime.callTool("update_collection", {
    collection_id: COLLECTION_ID,
    owner: OWNER,
    expected_version: 1,
    name: "Research clips v2",
  });
  await runtime.callTool("update_collection_items", {
    collection_id: COLLECTION_ID,
    owner: OWNER,
    expected_version: 2,
    items: [{ media_item_id: MEDIA_ID, position: 0 }],
  });
  await runtime.callTool("create_selection", {
    owner: OWNER,
    source_collection_id: COLLECTION_ID,
    items: [{ media_item_id: MEDIA_ID, position: 0 }],
  });
  await runtime.callTool("get_selection", {
    owner: OWNER,
    selection_id: SELECTION_ID,
  });
  await runtime.callTool("run_analysis", {
    owner: OWNER,
    selection_id: SELECTION_ID,
    run_type: "summary",
  });
  await runtime.callTool("list_runs", {
    owner: OWNER,
    cursor: "r1",
    page_size: 10,
    status: "queued",
  });
  await runtime.callTool("get_run", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
  });
  await runtime.callTool("cancel_run", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
    message: "user requested stop",
  });
  await runtime.callTool("retry_run", {
    analysis_run_id: RUN_ID,
    owner: OWNER,
  });
  await runtime.callTool("list_run_events", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
    cursor: "e1",
    page_size: 3,
  });
  await runtime.callTool("list_artifacts", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
    cursor: "a1",
    page_size: 3,
  });
  await runtime.callTool("get_artifact", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
  });
  await runtime.callTool("refresh_artifact", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
  });
  await runtime.callTool("get_diagnostics", {
    owner: OWNER,
    subject_type: "media_item",
    subject_id: MEDIA_ID,
    severity: "info",
  });
  await runtime.callTool("remove_media", {
    owner: OWNER,
    media_item_id: MEDIA_ID,
  });

  assert.equal(requests.length, 25);
  const fileRequest = requests[2];
  assert.equal(fileRequest?.path, "/v1/media-items");
  assert.equal(fileRequest?.method, "POST");
  assert.ok(fileRequest?.body instanceof FormData);
  assert.deepEqual(JSON.parse(String(fileRequest.body.get("metadata"))), {
    owner: OWNER,
    kind: "audio",
    collection_id: COLLECTION_ID,
    display_name: "Interview clip",
  });
  assert.ok(fileRequest.body.get("file") instanceof File);
  assert.deepEqual(
    requests.map((request) => request.path),
    [
      "/v1/media-items",
      "/v1/media-items",
      "/v1/media-items",
      withOwnerQuery("/v1/media-items?cursor=m1&page_size=25&kind=text&status=ready"),
      withOwnerQuery("/v1/media-items?query=meeting&kind=url"),
      withOwnerQuery(`/v1/media-items/${MEDIA_ID}`),
      withOwnerQuery("/v1/collections/inbox?cursor=i1&page_size=10"),
      "/v1/collections",
      withOwnerQuery("/v1/collections?cursor=c1&page_size=10"),
      withOwnerQuery(`/v1/collections/${COLLECTION_ID}?cursor=ci1&page_size=5`),
      `/v1/collections/${COLLECTION_ID}`,
      `/v1/collections/${COLLECTION_ID}/items`,
      "/v1/selections",
      withOwnerQuery(`/v1/selections/${SELECTION_ID}`),
      "/v1/analysis-runs",
      withOwnerQuery("/v1/analysis-runs?cursor=r1&page_size=10&status=queued"),
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}`),
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/cancel`),
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/retry`),
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/events?cursor=e1&page_size=3`),
      withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/artifacts?cursor=a1&page_size=3`),
      withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}`),
      withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`),
      withOwnerQuery(`/v1/diagnostics?subject_type=media_item&subject_id=${MEDIA_ID}&severity=info`),
      withOwnerQuery(`/v1/media-items/${MEDIA_ID}`),
    ],
  );
  assert.deepEqual(requests[0]?.body, {
    owner: OWNER,
    kind: "text",
    source: {
      origin_type: "text",
      text: "Meeting transcript fragment",
      language_hint: "en",
    },
    metadata: {
      source: "mcp-lifecycle-test",
    },
  });
  assert.deepEqual(requests[1]?.body, {
    owner: OWNER,
    kind: "url",
    source: {
      origin_type: "url",
      url: "https://example.test/research-note",
    },
    display_name: "Research note",
  });
  assert.deepEqual(requests[17], {
    path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/cancel`),
    method: "POST",
    body: {
      message: "user requested stop",
    },
  });
  assert.equal(requests[24]?.method, "DELETE");
  // END_BLOCK_BLOCK_VERIFY_FULL_INBOX_FIRST_LIFECYCLE
});

test("createMcpDomainRuntime returns text, markdown, and json artifact previews", async () => {
  // START_BLOCK_BLOCK_VERIFY_ARTIFACT_PREVIEWS
  const requests: McpAdapterApiRequest[] = [];
  const artifactByPath = new Map<string, unknown>([
    [
      withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}`),
      {
        artifact: {
          artifact_id: ARTIFACT_ID,
          analysis_run_id: RUN_ID,
          kind: "summary",
          content_type: "text/markdown; charset=utf-8",
          preview: {
            available: true,
            kind: "text",
            content_type: "text/markdown; charset=utf-8",
            text_excerpt: "# Summary\n\nImportant result",
          },
        },
      },
    ],
    [
      withOwnerQuery(`/v1/artifacts/${TEXT_ARTIFACT_ID}`),
      {
        artifact: {
          artifact_id: TEXT_ARTIFACT_ID,
          analysis_run_id: RUN_ID,
          kind: "transcript",
          content_type: "text/plain; charset=utf-8",
          preview: {
            available: true,
            kind: "text",
            content_type: "text/plain; charset=utf-8",
            text_excerpt: "Plain transcript excerpt",
          },
        },
      },
    ],
    [
      withOwnerQuery(`/v1/artifacts/${JSON_ARTIFACT_ID}`),
      {
        artifact: {
          artifact_id: JSON_ARTIFACT_ID,
          analysis_run_id: RUN_ID,
          kind: "run_manifest",
          content_type: "application/json; charset=utf-8",
          preview: {
            available: true,
            kind: "text",
            content_type: "application/json; charset=utf-8",
            text_excerpt: "{\"segments\":[{\"speaker\":\"A\",\"text\":\"hello\"}]}",
          },
        },
      },
    ],
  ]);
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
        requests.push(request);
        const response = artifactByPath.get(request.path);
        if (response === undefined) {
          throw new Error(`unexpected request path ${request.path}`);
        }
        return {
          status: 200,
          data: response as TPayload,
        };
      },
    },
  });

  const markdownResult = await runtime.callTool("get_artifact_preview", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
    format: "markdown",
    max_chars: 20,
  });
  const textResult = await runtime.callTool("get_artifact_preview", {
    owner: OWNER,
    artifact_id: TEXT_ARTIFACT_ID,
    format: "text",
  });
  const jsonResult = await runtime.callTool("get_artifact_preview", {
    owner: OWNER,
    artifact_id: JSON_ARTIFACT_ID,
    format: "json",
  });

  assert.deepEqual(markdownResult.structuredContent, {
    artifact_preview: {
      artifact_id: ARTIFACT_ID,
      analysis_run_id: RUN_ID,
      artifact_kind: "summary",
      content_type: "text/markdown; charset=utf-8",
      format: "markdown",
      available: true,
      text: "# Summary\n\nImportant",
      markdown: "# Summary\n\nImportant",
      truncated: true,
      max_chars: 20,
      source: "artifact.preview.text_excerpt",
    },
  });
  assert.deepEqual(textResult.structuredContent, {
    artifact_preview: {
      artifact_id: TEXT_ARTIFACT_ID,
      analysis_run_id: RUN_ID,
      artifact_kind: "transcript",
      content_type: "text/plain; charset=utf-8",
      format: "text",
      available: true,
      text: "Plain transcript excerpt",
      truncated: false,
      max_chars: 4000,
      source: "artifact.preview.text_excerpt",
    },
  });
  assert.deepEqual(jsonResult.structuredContent, {
    artifact_preview: {
      artifact_id: JSON_ARTIFACT_ID,
      analysis_run_id: RUN_ID,
      artifact_kind: "run_manifest",
      content_type: "application/json; charset=utf-8",
      format: "json",
      available: true,
      text: "{\"segments\":[{\"speaker\":\"A\",\"text\":\"hello\"}]}",
      json: {
        segments: [
          {
            speaker: "A",
            text: "hello",
          },
        ],
      },
      truncated: false,
      max_chars: 4000,
      source: "artifact.preview.text_excerpt",
    },
  });
  assert.deepEqual(requests.map((request) => request.path), [
    withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}`),
    withOwnerQuery(`/v1/artifacts/${TEXT_ARTIFACT_ID}`),
    withOwnerQuery(`/v1/artifacts/${JSON_ARTIFACT_ID}`),
  ]);
  // END_BLOCK_BLOCK_VERIFY_ARTIFACT_PREVIEWS
});

test("createMcpDomainRuntime supports real SDK listTools and callTool protocol flow", async () => {
  // START_BLOCK_BLOCK_VERIFY_SDK_PROTOCOL_FLOW
  const requests: McpAdapterApiRequest[] = [];
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
        requests.push(request);
        return {
          status: 200,
          data: {
            media_item: {
              media_item_id: MEDIA_ID,
              status: "ready",
            },
          } as TPayload,
        };
      },
    },
  });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const client = new Client({
    name: "media-analysis-platform-test-client",
    version: "0.1.0",
  });

  try {
    await runtime.server.connect(serverTransport);
    await client.connect(clientTransport);

    const toolsResult = await client.listTools();
    const callResult = await client.callTool({
      name: "get_media",
      arguments: {
        owner: OWNER,
        media_item_id: MEDIA_ID,
      },
    });

    assert.equal(toolsResult.tools.length, REQUIRED_TOOLS.length);
    assert.deepEqual(
      toolsResult.tools.map((tool) => tool.name),
      [...REQUIRED_TOOLS],
    );
    assert.equal(
      toolsResult.tools.some((tool) => /job|batch|source_set|legacy/i.test(tool.name)),
      false,
    );
    assert.equal(toolsResult.tools.every((tool) => Boolean(tool.outputSchema)), true);
    assert.equal(
      toolsResult.tools.every((tool) => Array.isArray(tool._meta?.examples)),
      true,
    );
    assert.ok(
      toolsResult.tools.find((tool) => tool.name === "get_artifact_preview")
        ?.outputSchema?.properties?.artifact_preview,
    );
    assert.deepEqual(callResult.structuredContent, {
      media_item: {
        media_item_id: MEDIA_ID,
        status: "ready",
      },
    });
    assert.deepEqual(requests, [
      {
        path: withOwnerQuery(`/v1/media-items/${MEDIA_ID}`),
      },
    ]);
  } finally {
    await client.close();
    await runtime.server.close();
  }
  // END_BLOCK_BLOCK_VERIFY_SDK_PROTOCOL_FLOW
});

test("createMcpDomainRuntime preserves explicit retention-touching contract parity", async () => {
  // START_BLOCK_BLOCK_VERIFY_RETENTION_TOUCHING_PARITY
  const requests: McpAdapterApiRequest[] = [];
  const apiClient: McpAdapterApiClient = {
    request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
      requests.push(request);
      if (request.path === withOwnerQuery(`/v1/media-items/${MEDIA_ID}`) && request.method === "DELETE") {
        return {
          status: 200,
          data: {
            media_item: {
              media_item_id: MEDIA_ID,
              status: "deleted",
              retention: {
                state: "soft_deleted",
                deleted_at: "2026-05-10T01:00:00Z",
              },
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`) && request.method === "POST") {
        return {
          status: 200,
          data: {
            artifact: {
              artifact_id: ARTIFACT_ID,
              download: {
                available: true,
                url: "https://minio.local/refreshed-artifact-1.txt",
              },
              preview: {
                available: true,
                kind: "text",
                content_type: "text/markdown",
                text_excerpt: "# Refreshed preview",
              },
              retention: {
                state: "active",
              },
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery("/v1/diagnostics?page_size=5&subject_type=retention&severity=error")) {
        return {
          status: 200,
          data: {
            items: [
              {
                diagnostic_id: "diagnostic-retention",
                severity: "error",
                code: "retention_hold_pending",
                subject: {
                  subject_type: "retention",
                  subject_id: MEDIA_ID,
                },
              },
            ],
            page: {
              page_size: 5,
              has_more: false,
            },
          } as TPayload,
        };
      }
      throw new Error(`unexpected request path ${request.path}`);
    },
  };
  const runtime = createMcpDomainRuntime({ apiClient });

  const removeResult = await runtime.callTool("remove_media", {
    owner: OWNER,
    media_item_id: MEDIA_ID,
  });
  const refreshResult = await runtime.callTool("refresh_artifact", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
  });
  const diagnosticsResult = await runtime.callTool("get_diagnostics", {
    owner: OWNER,
    page_size: 5,
    subject_type: "retention",
    severity: "error",
  });

  assert.deepEqual(removeResult.structuredContent, {
    media_item: {
      media_item_id: MEDIA_ID,
      status: "deleted",
      retention: {
        state: "soft_deleted",
        deleted_at: "2026-05-10T01:00:00Z",
      },
    },
  });
  assert.deepEqual(refreshResult.structuredContent, {
    artifact: {
      artifact_id: ARTIFACT_ID,
      download: {
        available: true,
        url: "https://minio.local/refreshed-artifact-1.txt",
      },
      preview: {
        available: true,
        kind: "text",
        content_type: "text/markdown",
        text_excerpt: "# Refreshed preview",
      },
      retention: {
        state: "active",
      },
    },
  });
  assert.deepEqual(diagnosticsResult.structuredContent, {
    items: [
      {
        diagnostic_id: "diagnostic-retention",
        severity: "error",
        code: "retention_hold_pending",
        subject: {
          subject_type: "retention",
          subject_id: MEDIA_ID,
        },
      },
    ],
    page: {
      page_size: 5,
      has_more: false,
    },
  });
  assert.deepEqual(requests, [
    {
      path: withOwnerQuery(`/v1/media-items/${MEDIA_ID}`),
      method: "DELETE",
    },
    {
      path: withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`),
      method: "POST",
    },
    {
      path: withOwnerQuery("/v1/diagnostics?page_size=5&subject_type=retention&severity=error"),
    },
  ]);
  // END_BLOCK_BLOCK_VERIFY_RETENTION_TOUCHING_PARITY
});

test("createMcpDomainRuntime keeps adapter validation and upstream failures structured", async () => {
  // START_BLOCK_BLOCK_VERIFY_ERROR_SHAPING
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async () => {
        throw new McpAdapterApiClientError(
          withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/cancel`),
          409,
          "upstream rejected cancellation",
          "run_cancel_not_allowed",
        );
      },
    },
  });

  const validationResult = await runtime.callTool("get_media", {
    owner: OWNER,
  });
  const ownerValidationResult = await runtime.callTool("list_media", {});
  const upstreamResult = await runtime.callTool("cancel_run", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
  });
  const unknownToolResult = await runtime.callTool("missing_tool");

  assert.equal(validationResult.isError, true);
  assert.deepEqual(validationResult.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "Tool input did not match the domain contract.",
      category: "adapter_contract",
      retryable: false,
      action: "fix_tool_input",
      details: {
        issues: [
          {
            path: "media_item_id",
            message: "Invalid input: expected string, received undefined",
          },
        ],
      },
    },
  });
  assert.deepEqual(ownerValidationResult.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "Tool input did not match the domain contract.",
      category: "adapter_contract",
      retryable: false,
      action: "fix_tool_input",
      details: {
        issues: [
          {
            path: "owner",
            message: "Invalid input: expected object, received undefined",
          },
        ],
      },
    },
  });
  assert.deepEqual(upstreamResult.structuredContent, {
    error: {
      code: "run_cancel_not_allowed",
      message: "upstream rejected cancellation",
      category: "upstream_api",
      retryable: false,
      action: "inspect_run_state_before_retry",
      details: {
        path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/cancel`),
        status: 409,
      },
    },
  });
  assert.deepEqual(unknownToolResult.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "Unknown MCP tool: missing_tool",
      category: "adapter_contract",
      retryable: false,
      action: "check_tool_name",
      details: {
        tool: "missing_tool",
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_ERROR_SHAPING
});

test("createMcpDomainRuntime keeps denied retention-touching paths structured", async () => {
  // START_BLOCK_BLOCK_VERIFY_RETENTION_TOUCHING_DENIALS
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async (request: McpAdapterApiRequest) => {
        if (request.path === withOwnerQuery(`/v1/media-items/${MEDIA_ID}`) && request.method === "DELETE") {
          throw new McpAdapterApiClientError(
            withOwnerQuery(`/v1/media-items/${MEDIA_ID}`),
            404,
            "media item not found for this owner scope",
            "not_found",
          );
        }
        if (request.path === withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`) && request.method === "POST") {
          throw new McpAdapterApiClientError(
            withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`),
            404,
            "artifact not found for this owner scope",
            "not_found",
          );
        }
        throw new Error(`unexpected request path ${request.path}`);
      },
    },
  });

  const removeResult = await runtime.callTool("remove_media", {
    owner: OWNER,
    media_item_id: MEDIA_ID,
  });
  const refreshResult = await runtime.callTool("refresh_artifact", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
  });

  assert.deepEqual(removeResult.structuredContent, {
    error: {
      code: "not_found",
      message: "media item not found for this owner scope",
      category: "upstream_api",
      retryable: false,
      action: "check_resource_id_owner_scope",
      details: {
        path: withOwnerQuery(`/v1/media-items/${MEDIA_ID}`),
        status: 404,
      },
    },
  });
  assert.deepEqual(refreshResult.structuredContent, {
    error: {
      code: "not_found",
      message: "artifact not found for this owner scope",
      category: "upstream_api",
      retryable: false,
      action: "check_resource_id_owner_scope",
      details: {
        path: withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}/refresh`),
        status: 404,
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_RETENTION_TOUCHING_DENIALS
});

test("createMcpDomainRuntime returns actionable retry hints for preview failures", async () => {
  // START_BLOCK_BLOCK_VERIFY_PREVIEW_ERROR_HINTS
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async <TPayload = unknown>() => ({
        status: 200,
        data: {
          artifact: {
            artifact_id: ARTIFACT_ID,
            analysis_run_id: RUN_ID,
            kind: "summary",
            content_type: "text/markdown; charset=utf-8",
            preview: {
              available: false,
              kind: "none",
            },
          },
        } as TPayload,
      }),
    },
  });

  const result = await runtime.callTool("get_artifact_preview", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
    format: "markdown",
  });

  assert.deepEqual(result.structuredContent, {
    error: {
      code: "artifact_preview_unavailable",
      message: "Artifact preview is not available.",
      category: "resource_state",
      retryable: true,
      action: "refresh_artifact_then_retry_preview",
      details: {
        artifact_id: ARTIFACT_ID,
        preview_available: false,
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_PREVIEW_ERROR_HINTS
});

test("createMcpDomainRuntime covers the remaining upstream API hint branches", async () => {
  // START_BLOCK_BLOCK_VERIFY_REMAINING_API_HINT_BRANCHES
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async (request: McpAdapterApiRequest) => {
        if (request.path === withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}`)) {
          throw new McpAdapterApiClientError(
            request.path,
            424,
            "artifact bytes are still materializing",
            "artifact_resolution_failed",
          );
        }
        if (request.path === withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/retry`)) {
          throw new McpAdapterApiClientError(
            request.path,
            409,
            "run is not terminal yet",
            "retry_requires_terminal_run",
          );
        }
        if (request.path === `/v1/collections/${COLLECTION_ID}` && request.method === "PATCH") {
          throw new McpAdapterApiClientError(
            request.path,
            409,
            "collection version mismatch",
            "collection_version_conflict",
          );
        }
        if (request.path === withOwnerQuery("/v1/media-items")) {
          throw new McpAdapterApiClientError(
            request.path,
            400,
            "owner scope is invalid",
            "invalid_request",
          );
        }
        if (request.path === withOwnerQuery("/v1/analysis-runs")) {
          throw new McpAdapterApiClientError(
            request.path,
            503,
            "analysis service is unavailable",
          );
        }
        if (request.path === withOwnerQuery(`/v1/analysis-runs/${RUN_ID}`)) {
          throw new McpAdapterApiClientError(
            request.path,
            418,
            "unexpected upstream status",
          );
        }
        throw new Error(`unexpected request path ${request.path}`);
      },
    },
  });

  const artifactResult = await runtime.callTool("get_artifact", {
    owner: OWNER,
    artifact_id: ARTIFACT_ID,
  });
  const retryResult = await runtime.callTool("retry_run", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
  });
  const conflictResult = await runtime.callTool("update_collection", {
    collection_id: COLLECTION_ID,
    owner: OWNER,
    expected_version: 1,
    name: "Renamed collection",
  });
  const invalidRequestResult = await runtime.callTool("list_media", {
    owner: OWNER,
  });
  const retryLaterResult = await runtime.callTool("list_runs", {
    owner: OWNER,
  });
  const inspectResult = await runtime.callTool("get_run", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
  });

  assert.deepEqual(artifactResult.structuredContent, {
    error: {
      code: "artifact_resolution_failed",
      message: "artifact bytes are still materializing",
      category: "upstream_api",
      retryable: true,
      action: "refresh_artifact_then_retry_preview",
      details: {
        path: withOwnerQuery(`/v1/artifacts/${ARTIFACT_ID}`),
        status: 424,
      },
    },
  });
  assert.deepEqual(retryResult.structuredContent, {
    error: {
      code: "retry_requires_terminal_run",
      message: "run is not terminal yet",
      category: "upstream_api",
      retryable: false,
      action: "wait_for_terminal_run_before_retry",
      details: {
        path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/retry`),
        status: 409,
      },
    },
  });
  assert.deepEqual(conflictResult.structuredContent, {
    error: {
      code: "collection_version_conflict",
      message: "collection version mismatch",
      category: "upstream_api",
      retryable: false,
      action: "reload_resource_and_retry_with_latest_version",
      details: {
        path: `/v1/collections/${COLLECTION_ID}`,
        status: 409,
      },
    },
  });
  assert.deepEqual(invalidRequestResult.structuredContent, {
    error: {
      code: "invalid_request",
      message: "owner scope is invalid",
      category: "upstream_api",
      retryable: false,
      action: "fix_request",
      details: {
        path: withOwnerQuery("/v1/media-items"),
        status: 400,
      },
    },
  });
  assert.deepEqual(retryLaterResult.structuredContent, {
    error: {
      code: "api_request_failed",
      message: "analysis service is unavailable",
      category: "upstream_api",
      retryable: true,
      action: "retry_later",
      details: {
        path: withOwnerQuery("/v1/analysis-runs"),
        status: 503,
      },
    },
  });
  assert.deepEqual(inspectResult.structuredContent, {
    error: {
      code: "api_request_failed",
      message: "unexpected upstream status",
      category: "upstream_api",
      retryable: false,
      action: "inspect_upstream_error",
      details: {
        path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}`),
        status: 418,
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_REMAINING_API_HINT_BRANCHES
});

test("createMcpDomainRuntime shapes malformed success envelopes as contract errors", async () => {
  // START_BLOCK_BLOCK_VERIFY_MALFORMED_SUCCESS_ENVELOPE
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async <TPayload = unknown>() => ({
        status: 200,
        data: [] as TPayload,
      }),
    },
  });

  const result = await runtime.callTool("get_media", {
    owner: OWNER,
    media_item_id: MEDIA_ID,
  });

  assert.deepEqual(result.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "response must be an object",
      category: "adapter_contract",
      retryable: false,
      action: "fix_tool_input",
      details: {
        field: "response",
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_MALFORMED_SUCCESS_ENVELOPE
});

test("createMcpDomainRuntime rethrows unknown execution errors", async () => {
  // START_BLOCK_BLOCK_VERIFY_UNKNOWN_ERROR_RETHROW
  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async () => {
        throw new Error("socket closed");
      },
    },
  });

  await assert.rejects(
    () =>
      runtime.callTool("get_media", {
        owner: OWNER,
        media_item_id: MEDIA_ID,
      }),
    /socket closed/,
  );
  // END_BLOCK_BLOCK_VERIFY_UNKNOWN_ERROR_RETHROW
});

test("createMcpDomainRuntime shapes missing and invalid artifact preview text branches", async () => {
  // START_BLOCK_BLOCK_VERIFY_ARTIFACT_PREVIEW_EDGE_BRANCHES
  const missingTextArtifactID = "00000000-0000-4000-8000-000000000008";
  const invalidJsonArtifactID = "00000000-0000-4000-8000-000000000009";

  const runtime = createMcpDomainRuntime({
    apiClient: {
      request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
        if (request.path === withOwnerQuery(`/v1/artifacts/${missingTextArtifactID}`)) {
          return {
            status: 200,
            data: {
              artifact: {
                artifact_id: missingTextArtifactID,
                preview: {
                  available: true,
                  content_type: "text/plain",
                },
              },
            } as TPayload,
          };
        }
        if (request.path === withOwnerQuery(`/v1/artifacts/${invalidJsonArtifactID}`)) {
          return {
            status: 200,
            data: {
              artifact: {
                artifact_id: invalidJsonArtifactID,
                preview: {
                  available: true,
                  text_excerpt: "{not-json}",
                  content_type: "application/json",
                },
              },
            } as TPayload,
          };
        }
        throw new Error(`unexpected request path ${request.path}`);
      },
    },
  });

  const missingTextResult = await runtime.callTool("get_artifact_preview", {
    owner: OWNER,
    artifact_id: missingTextArtifactID,
  });
  const invalidJsonResult = await runtime.callTool("get_artifact_preview", {
    owner: OWNER,
    artifact_id: invalidJsonArtifactID,
    format: "json",
  });

  assert.deepEqual(missingTextResult.structuredContent, {
    error: {
      code: "artifact_preview_unavailable",
      message: "Artifact preview is not available.",
      category: "resource_state",
      retryable: true,
      action: "refresh_artifact_then_retry_preview",
      details: {
        artifact_id: missingTextArtifactID,
        preview_available: true,
      },
    },
  });
  assert.deepEqual(invalidJsonResult.structuredContent, {
    error: {
      code: "artifact_preview_json_invalid",
      message: "Artifact preview is not valid JSON.",
      category: "resource_state",
      retryable: false,
      action: "request_text_preview_or_refresh_artifact",
      details: {
        artifact_id: invalidJsonArtifactID,
        content_type: "application/json",
      },
    },
  });
  // END_BLOCK_BLOCK_VERIFY_ARTIFACT_PREVIEW_EDGE_BRANCHES
});

test("createMcpDomainRuntime omits optional contract fields on minimal calls and preserves tenant-scoped queries", async () => {
  const tenantOwner = {
    ...OWNER,
    tenant_id: "tenant-1",
  };
  const requests: McpAdapterApiRequest[] = [];
  const apiClient: McpAdapterApiClient = {
    request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
      requests.push(request);
      return {
        status: request.method === "POST" || request.method === "PATCH" ? 202 : 200,
        data: {
          ok: true,
          path: request.path,
          method: request.method ?? "GET",
        } as TPayload,
      };
    },
  };
  const runtime = createMcpDomainRuntime({
    apiClient,
  });

  const addFileResult = await runtime.callTool("add_media", {
    owner: OWNER,
    kind: "document",
    file: {
      filename: "brief.txt",
      content_type: "text/plain",
      content_base64: Buffer.from("brief").toString("base64"),
    },
  });
  await runtime.callTool("list_media", {
    owner: tenantOwner,
  });
  await runtime.callTool("get_inbox", {
    owner: OWNER,
  });
  await runtime.callTool("create_collection", {
    owner: OWNER,
    name: "Minimal collection",
  });
  await runtime.callTool("update_collection", {
    owner: OWNER,
    collection_id: COLLECTION_ID,
    expected_version: 2,
    status: "archived",
  });
  await runtime.callTool("create_selection", {
    owner: OWNER,
    items: [
      {
        media_item_id: MEDIA_ID,
        position: 0,
      },
    ],
  });
  await runtime.callTool("run_analysis", {
    owner: OWNER,
    selection_id: SELECTION_ID,
    run_type: "summary",
  });
  await runtime.callTool("cancel_run", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
  });
  await runtime.callTool("list_artifacts", {
    owner: OWNER,
    analysis_run_id: RUN_ID,
  });
  await runtime.callTool("get_diagnostics", {
    owner: OWNER,
  });

  assert.deepEqual(addFileResult.structuredContent, {
    ok: true,
    path: "/v1/media-items",
    method: "POST",
  });
  assert.equal(requests.length, 10);
  assert.equal(requests[0]?.path, "/v1/media-items");
  assert.equal(requests[0]?.method, "POST");
  assert.equal(requests[0]?.headers, undefined);
  assert.ok(requests[0]?.body instanceof FormData);
  const metadataPayload = JSON.parse(String((requests[0]?.body as FormData).get("metadata")));
  assert.deepEqual(metadataPayload, {
    owner: OWNER,
    kind: "document",
  });
  assert.equal(
    requests[1]?.path,
    "/v1/media-items?owner_type=mcp&owner_id=assistant&tenant_id=tenant-1",
  );
  assert.equal(requests[2]?.path, withOwnerQuery("/v1/collections/inbox"));
  assert.deepEqual(requests[3], {
    path: "/v1/collections",
    method: "POST",
    headers: undefined,
    body: {
      owner: OWNER,
      name: "Minimal collection",
    },
  });
  assert.deepEqual(requests[4], {
    path: `/v1/collections/${COLLECTION_ID}`,
    method: "PATCH",
    body: {
      owner: OWNER,
      expected_version: 2,
      status: "archived",
    },
  });
  assert.deepEqual(requests[5], {
    path: "/v1/selections",
    method: "POST",
    headers: undefined,
    body: {
      owner: OWNER,
      items: [
        {
          media_item_id: MEDIA_ID,
          position: 0,
        },
      ],
    },
  });
  assert.deepEqual(requests[6], {
    path: "/v1/analysis-runs",
    method: "POST",
    headers: undefined,
    body: {
      owner: OWNER,
      selection_id: SELECTION_ID,
      run_type: "summary",
    },
  });
  assert.deepEqual(requests[7], {
    path: withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/cancel`),
    method: "POST",
    body: {},
  });
  assert.equal(requests[8]?.path, withOwnerQuery(`/v1/analysis-runs/${RUN_ID}/artifacts`));
  assert.equal(requests[9]?.path, withOwnerQuery("/v1/diagnostics"));
});

test("createMcpDomainRuntime preserves fully populated optional payload branches", async () => {
  const richOwner = {
    ...OWNER,
    adapter_identity: {
      ...OWNER.adapter_identity,
      service_name: "mcp-suite",
    },
  };
  const requests: McpAdapterApiRequest[] = [];
  const richArtifactID = "00000000-0000-4000-8000-000000000008";
  const apiClient: McpAdapterApiClient = {
    request: async <TPayload = unknown>(request: McpAdapterApiRequest) => {
      requests.push(request);
      if (request.path === "/v1/media-items") {
        return {
          status: 201,
          data: {
            media_item: {
              media_item_id: MEDIA_ID,
              status: "ready",
            },
          } as TPayload,
        };
      }
      if (request.path === "/v1/selections") {
        return {
          status: 201,
          data: {
            selection: {
              selection_id: SELECTION_ID,
              duplicate_policy: "allow",
            },
          } as TPayload,
        };
      }
      if (request.path === "/v1/analysis-runs") {
        return {
          status: 202,
          data: {
            analysis_run: {
              analysis_run_id: RUN_ID,
              status: "queued",
            },
          } as TPayload,
        };
      }
      if (request.path === withOwnerQuery(`/v1/artifacts/${richArtifactID}`)) {
        return {
          status: 200,
          data: {
            artifact: {
              artifact_id: richArtifactID,
              content_type: "text/plain",
              preview: {
                available: true,
                kind: "text",
                text_excerpt: "hello world!",
              },
            },
          } as TPayload,
        };
      }
      throw new Error(`unexpected request path ${request.path}`);
    },
  };
  const runtime = createMcpDomainRuntime({ apiClient });

  const addObjectResult = await runtime.callTool("add_media", {
    owner: richOwner,
    kind: "document",
    source: {
      origin_type: "object",
      object_ref: "minio://bucket/reports/brief.json",
      original_filename: "brief.json",
      content_type: "application/json",
      size_bytes: 128,
    },
    collection_id: COLLECTION_ID,
    display_name: "Quarterly brief",
    adapter_origin: "mcp-import",
    metadata: {
      source: "suite",
    },
    retention: {
      policy: "keep",
    },
    idempotency_key: "media-rich-1",
  });
  const selectionResult = await runtime.callTool("create_selection", {
    owner: richOwner,
    source_collection_id: COLLECTION_ID,
    items: [
      {
        media_item_id: MEDIA_ID,
        position: 0,
      },
    ],
    option_snapshot: {
      mode: "full",
    },
    duplicate_policy: "allow",
    created_by: "codex",
    idempotency_key: "selection-rich-1",
  });
  const runResult = await runtime.callTool("run_analysis", {
    owner: richOwner,
    selection_id: SELECTION_ID,
    run_type: "custom",
    params: {
      harness_name: "mcp-suite",
    },
    delivery: {
      strategy: "webhook",
      webhook: {
        url: "https://example.test/hooks/run",
      },
    },
    idempotency_key: "run-rich-1",
  });
  const previewResult = await runtime.callTool("get_artifact_preview", {
    owner: richOwner,
    artifact_id: richArtifactID,
    format: "text",
    max_chars: 12,
  });

  assert.deepEqual(addObjectResult.structuredContent, {
    media_item: {
      media_item_id: MEDIA_ID,
      status: "ready",
    },
  });
  assert.deepEqual(selectionResult.structuredContent, {
    selection: {
      selection_id: SELECTION_ID,
      duplicate_policy: "allow",
    },
  });
  assert.deepEqual(runResult.structuredContent, {
    analysis_run: {
      analysis_run_id: RUN_ID,
      status: "queued",
    },
  });
  assert.deepEqual(previewResult.structuredContent, {
    artifact_preview: {
      artifact_id: richArtifactID,
      available: true,
      content_type: "text/plain",
      format: "text",
      max_chars: 12,
      source: "artifact.preview.text_excerpt",
      text: "hello world!",
      truncated: false,
    },
  });
  assert.deepEqual(requests, [
    {
      path: "/v1/media-items",
      method: "POST",
      headers: {
        "Idempotency-Key": "media-rich-1",
      },
      body: {
        owner: richOwner,
        kind: "document",
        source: {
          origin_type: "object",
          object_ref: "minio://bucket/reports/brief.json",
          original_filename: "brief.json",
          content_type: "application/json",
          size_bytes: 128,
        },
        collection_id: COLLECTION_ID,
        display_name: "Quarterly brief",
        adapter_origin: "mcp-import",
        metadata: {
          source: "suite",
        },
        retention: {
          policy: "keep",
        },
      },
    },
    {
      path: "/v1/selections",
      method: "POST",
      headers: {
        "Idempotency-Key": "selection-rich-1",
      },
      body: {
        owner: richOwner,
        source_collection_id: COLLECTION_ID,
        items: [
          {
            media_item_id: MEDIA_ID,
            position: 0,
          },
        ],
        option_snapshot: {
          mode: "full",
        },
        duplicate_policy: "allow",
        created_by: "codex",
      },
    },
    {
      path: "/v1/analysis-runs",
      method: "POST",
      headers: {
        "Idempotency-Key": "run-rich-1",
      },
      body: {
        owner: richOwner,
        selection_id: SELECTION_ID,
        run_type: "custom",
        params: {
          harness_name: "mcp-suite",
        },
        delivery: {
          strategy: "webhook",
          webhook: {
            url: "https://example.test/hooks/run",
          },
        },
      },
    },
    {
      path: withOwnerQuery(`/v1/artifacts/${richArtifactID}`),
    },
  ]);
});
