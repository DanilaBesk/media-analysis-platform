// FILE: apps/mcp-server/src/tools/mapped-tools.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Materialize the domain-first MCP tool-to-API mapping layer for API-owned media workflows.
// SCOPE: Define final media, collection, selection, run, artifact, and diagnostic MCP tools, validate adapter inputs, and call only inbox-first HTTP API paths.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Replaced the old execution vocabulary with inbox-first domain tools backed by the real MCP SDK runtime.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   validate-domain-inputs - Use Zod schemas aligned with the public inbox-first contracts.
//   map-tools-to-api - Route domain tools to /v1/media-items, /v1/collections, /v1/selections, /v1/analysis-runs, /v1/artifacts, and /v1/diagnostics.
//   shape-tool-results - Preserve API envelopes in structured MCP responses without adding adapter-side orchestration.
// END_MODULE_MAP

import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod/v4";

import type {
  McpAdapterApiClient,
  McpAdapterApiResponse,
} from "../client/api-client.ts";
import {
  McpAdapterContractError,
  McpAdapterToolError,
  createSuccessToolResult,
  type JsonObject,
} from "./protocol.ts";

type DomainToolInput = JsonObject;

export interface DomainMcpTool {
  name: string;
  title: string;
  description: string;
  inputSchema: z.ZodType<DomainToolInput>;
  outputSchema: z.ZodType<JsonObject>;
  apiPathHint: string;
  examples: readonly DomainMcpToolExample[];
  annotations: {
    readOnlyHint: boolean;
    destructiveHint: boolean;
    idempotentHint: boolean;
    openWorldHint: boolean;
  };
  execute(arguments_: DomainToolInput): Promise<CallToolResult>;
}

export interface DomainMcpToolDefinition {
  name: string;
  title: string;
  description: string;
  inputSchema: z.ZodType<DomainToolInput>;
  outputSchema: z.ZodType<JsonObject>;
  apiPathHint: string;
  examples: readonly DomainMcpToolExample[];
  annotations: DomainMcpTool["annotations"];
}

export interface DomainMcpToolExample {
  description: string;
  arguments: JsonObject;
}

const jsonObjectSchema = z.record(z.string(), z.unknown());
const genericOutputSchema = z.object({}).catchall(z.unknown());

const artifactPreviewOutputSchema = z
  .object({
    artifact_preview: z
      .object({
        artifact_id: z.string(),
        analysis_run_id: z.string().optional(),
        artifact_kind: z.string().optional(),
        content_type: z.string().optional(),
        format: z.enum(["text", "markdown", "json"]),
        available: z.boolean(),
        text: z.string(),
        markdown: z.string().optional(),
        json: z.unknown().optional(),
        truncated: z.boolean(),
        max_chars: z.number().int().min(1),
        source: z.string(),
      })
      .strict(),
  })
  .strict();

const EXAMPLE_OWNER = {
  owner_type: "mcp",
  owner_id: "assistant",
  adapter_identity: {
    mcp_caller_id: "codex",
  },
};

const EXAMPLE_MEDIA_ID = "00000000-0000-4000-8000-000000000001";
const EXAMPLE_COLLECTION_ID = "00000000-0000-4000-8000-000000000002";
const EXAMPLE_SELECTION_ID = "00000000-0000-4000-8000-000000000003";
const EXAMPLE_RUN_ID = "00000000-0000-4000-8000-000000000004";
const EXAMPLE_ARTIFACT_ID = "00000000-0000-4000-8000-000000000005";

const ownerSchema = z
  .object({
    owner_type: z
      .enum(["user", "telegram", "web", "mcp", "service"])
      .describe("Contract owner scope type for the stored media or workflow object."),
    owner_id: z.string().min(1).describe("Stable owner identifier in the adapter domain."),
    tenant_id: z.string().min(1).nullable().optional(),
    adapter_identity: z
      .object({
        telegram_chat_id: z.string().min(1).optional(),
        telegram_user_id: z.string().min(1).optional(),
        web_session_id: z.string().min(1).optional(),
        mcp_caller_id: z.string().min(1).optional(),
        service_name: z.string().min(1).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

const sourceSchema = z.discriminatedUnion("origin_type", [
  z
    .object({
      origin_type: z.literal("text"),
      text: z.string().min(1).describe("Text content to persist before analysis."),
      language_hint: z.string().min(2).optional(),
    })
    .strict(),
  z
    .object({
      origin_type: z.literal("url"),
      url: z.url().regex(/^https?:\/\//).describe("HTTP(S) source URL to persist before analysis."),
    })
    .strict(),
  z
    .object({
      origin_type: z.literal("object"),
      object_ref: z.string().min(1).describe("Object-store reference already available to the API."),
      original_filename: z.string().min(1).optional(),
      content_type: z.string().min(1).optional(),
      size_bytes: z.number().int().min(1).optional(),
    })
    .strict(),
]);

const fileUploadSchema = z
  .object({
    filename: z.string().min(1).describe("Original file name for multipart media upload."),
    content_type: z.string().min(1).describe("MIME type for the uploaded file bytes."),
    content_base64: z.string().min(1).describe("Base64-encoded file bytes."),
  })
  .strict();

const collectionItemInputSchema = z
  .object({
    media_item_id: z.uuid(),
    position: z.number().int().min(0),
  })
  .strict();

const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(160)
  .describe("Optional Idempotency-Key header value for create operations.");

const paginationInputShape = {
  cursor: z.string().min(1).optional(),
  page_size: z.number().int().min(1).optional(),
};

const ownerScopedPaginationInputShape = {
  owner: ownerSchema,
  ...paginationInputShape,
};

const mediaFilterInputShape = {
  kind: z.enum(["text", "url", "file", "photo", "image", "audio", "voice", "video", "document"]).optional(),
  status: z.enum(["validating", "ready", "quarantined", "deleted"]).optional(),
};

const collectionStatusSchema = z.enum(["active", "archived", "deleted"]);

function defaultExamplesForTool(name: string): readonly DomainMcpToolExample[] {
  const argsByTool: Record<string, JsonObject> = {
    add_media: {
      owner: EXAMPLE_OWNER,
      kind: "text",
      source: {
        origin_type: "text",
        text: "Meeting transcript fragment",
      },
      display_name: "Meeting notes",
    },
    list_media: {
      owner: EXAMPLE_OWNER,
      page_size: 25,
      kind: "text",
      status: "ready",
    },
    search_media: {
      owner: EXAMPLE_OWNER,
      query: "meeting",
      kind: "text",
    },
    get_media: {
      owner: EXAMPLE_OWNER,
      media_item_id: EXAMPLE_MEDIA_ID,
    },
    remove_media: {
      owner: EXAMPLE_OWNER,
      media_item_id: EXAMPLE_MEDIA_ID,
    },
    get_inbox: {
      owner: EXAMPLE_OWNER,
      page_size: 10,
    },
    create_collection: {
      owner: EXAMPLE_OWNER,
      name: "Research clips",
      items: [EXAMPLE_MEDIA_ID],
    },
    list_collections: {
      owner: EXAMPLE_OWNER,
      page_size: 10,
    },
    get_collection: {
      owner: EXAMPLE_OWNER,
      collection_id: EXAMPLE_COLLECTION_ID,
      page_size: 10,
    },
    update_collection: {
      collection_id: EXAMPLE_COLLECTION_ID,
      owner: EXAMPLE_OWNER,
      expected_version: 1,
      name: "Research clips v2",
    },
    update_collection_items: {
      collection_id: EXAMPLE_COLLECTION_ID,
      owner: EXAMPLE_OWNER,
      expected_version: 1,
      items: [{ media_item_id: EXAMPLE_MEDIA_ID, position: 0 }],
    },
    create_selection: {
      owner: EXAMPLE_OWNER,
      source_collection_id: EXAMPLE_COLLECTION_ID,
      items: [{ media_item_id: EXAMPLE_MEDIA_ID, position: 0 }],
    },
    get_selection: {
      owner: EXAMPLE_OWNER,
      selection_id: EXAMPLE_SELECTION_ID,
    },
    run_analysis: {
      owner: EXAMPLE_OWNER,
      selection_id: EXAMPLE_SELECTION_ID,
      run_type: "summary",
    },
    list_runs: {
      owner: EXAMPLE_OWNER,
      page_size: 10,
      status: "queued",
    },
    get_run: {
      owner: EXAMPLE_OWNER,
      analysis_run_id: EXAMPLE_RUN_ID,
    },
    cancel_run: {
      owner: EXAMPLE_OWNER,
      analysis_run_id: EXAMPLE_RUN_ID,
      message: "user requested stop",
    },
    retry_run: {
      analysis_run_id: EXAMPLE_RUN_ID,
      owner: EXAMPLE_OWNER,
    },
    list_run_events: {
      owner: EXAMPLE_OWNER,
      analysis_run_id: EXAMPLE_RUN_ID,
      page_size: 10,
    },
    list_artifacts: {
      owner: EXAMPLE_OWNER,
      analysis_run_id: EXAMPLE_RUN_ID,
      page_size: 10,
    },
    get_artifact: {
      owner: EXAMPLE_OWNER,
      artifact_id: EXAMPLE_ARTIFACT_ID,
    },
    get_artifact_preview: {
      owner: EXAMPLE_OWNER,
      artifact_id: EXAMPLE_ARTIFACT_ID,
      format: "markdown",
      max_chars: 4000,
    },
    refresh_artifact: {
      owner: EXAMPLE_OWNER,
      artifact_id: EXAMPLE_ARTIFACT_ID,
    },
    get_diagnostics: {
      owner: EXAMPLE_OWNER,
      subject_type: "analysis_run",
      subject_id: EXAMPLE_RUN_ID,
      severity: "warning",
    },
  };

  return [
    {
      description: `Example arguments for ${name}.`,
      arguments: argsByTool[name] ?? {},
    },
  ];
}

function asRecord(value: unknown, fieldName: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new McpAdapterContractError(`${fieldName} must be an object`, {
      field: fieldName,
    });
  }
  return value as JsonObject;
}

function responseEnvelope(response: McpAdapterApiResponse<unknown>): JsonObject {
  return asRecord(response.data, "response");
}

function successFromResponse(response: McpAdapterApiResponse<unknown>): CallToolResult {
  return createSuccessToolResult(responseEnvelope(response));
}

function optionalString(record: JsonObject, fieldName: string): string | undefined {
  const value = record[fieldName];
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function artifactFromResponse(response: McpAdapterApiResponse<unknown>): JsonObject {
  const envelope = responseEnvelope(response);
  return asRecord(envelope.artifact, "artifact");
}

function previewText(artifact: JsonObject): {
  preview: JsonObject;
  text: string;
  source: string;
} {
  const artifactID = optionalString(artifact, "artifact_id") ?? "unknown";
  const preview = asRecord(artifact.preview, "artifact.preview");

  if (preview.available !== true) {
    throw new McpAdapterToolError({
      code: "artifact_preview_unavailable",
      message: "Artifact preview is not available.",
      category: "resource_state",
      retryable: true,
      action: "refresh_artifact_then_retry_preview",
      details: {
        artifact_id: artifactID,
        preview_available: preview.available === true,
      },
    });
  }

  const text = optionalString(preview, "text_excerpt");
  if (!text) {
    throw new McpAdapterToolError({
      code: "artifact_preview_unavailable",
      message: "Artifact preview is not available.",
      category: "resource_state",
      retryable: true,
      action: "refresh_artifact_then_retry_preview",
      details: {
        artifact_id: artifactID,
        preview_available: true,
      },
    });
  }

  return {
    preview,
    text,
    source: "artifact.preview.text_excerpt",
  };
}

function shapeArtifactPreview(args: JsonObject, artifact: JsonObject): CallToolResult {
  const format = typeof args.format === "string" ? args.format : "text";
  const maxChars =
    typeof args.max_chars === "number" && Number.isInteger(args.max_chars)
      ? args.max_chars
      : 4000;
  const { preview, text, source } = previewText(artifact);
  const clipped = text.slice(0, maxChars);
  const contentType =
    optionalString(preview, "content_type") ?? optionalString(artifact, "content_type");
  const artifactID = optionalString(artifact, "artifact_id") ?? String(args.artifact_id);
  const artifactPreview: JsonObject = {
    artifact_id: artifactID,
    ...(optionalString(artifact, "analysis_run_id")
      ? { analysis_run_id: optionalString(artifact, "analysis_run_id") }
      : {}),
    ...(optionalString(artifact, "kind")
      ? { artifact_kind: optionalString(artifact, "kind") }
      : {}),
    ...(contentType ? { content_type: contentType } : {}),
    format,
    available: true,
    text: clipped,
    truncated: clipped.length < text.length,
    max_chars: maxChars,
    source,
  };

  if (format === "markdown") {
    artifactPreview.markdown = clipped;
  }

  if (format === "json") {
    try {
      artifactPreview.json = JSON.parse(text);
    } catch {
      throw new McpAdapterToolError({
        code: "artifact_preview_json_invalid",
        message: "Artifact preview is not valid JSON.",
        category: "resource_state",
        retryable: false,
        action: "request_text_preview_or_refresh_artifact",
        details: {
          artifact_id: artifactID,
          content_type: contentType ?? null,
        },
      });
    }
  }

  return createSuccessToolResult({
    artifact_preview: artifactPreview,
  });
}

function pathSegment(value: string): string {
  return encodeURIComponent(value);
}

function queryPath(path: string, params: Record<string, unknown>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      search.set(key, String(value));
    }
  }
  const query = search.toString();
  return query ? `${path}?${query}` : path;
}

function ownerQueryPath(path: string, owner: JsonObject, params: Record<string, unknown> = {}): string {
  return queryPath(path, {
    owner_type: owner.owner_type,
    owner_id: owner.owner_id,
    ...(typeof owner.tenant_id === "string" && owner.tenant_id.length > 0 ? { tenant_id: owner.tenant_id } : {}),
    ...params,
  });
}

function idempotencyHeaders(args: JsonObject): Record<string, string> | undefined {
  return typeof args.idempotency_key === "string"
    ? { "Idempotency-Key": args.idempotency_key }
    : undefined;
}

function toFormData(args: JsonObject): FormData {
  const metadata = {
    owner: args.owner,
    kind: args.kind,
    ...(args.collection_id ? { collection_id: args.collection_id } : {}),
    ...(args.display_name ? { display_name: args.display_name } : {}),
    ...(args.adapter_origin ? { adapter_origin: args.adapter_origin } : {}),
  };
  const file = asRecord(args.file, "file");
  const formData = new FormData();

  formData.append("metadata", JSON.stringify(metadata));
  formData.append(
    "file",
    new Blob([Buffer.from(String(file.content_base64), "base64")], {
      type: String(file.content_type),
    }),
    String(file.filename),
  );
  return formData;
}

function toolDefinition(tool: DomainMcpTool): DomainMcpToolDefinition {
  return {
    name: tool.name,
    title: tool.title,
    description: tool.description,
    inputSchema: tool.inputSchema,
    outputSchema: tool.outputSchema,
    apiPathHint: tool.apiPathHint,
    examples: tool.examples,
    annotations: tool.annotations,
  };
}

function makeTool(
  apiClient: McpAdapterApiClient,
  definition: Omit<DomainMcpTool, "execute" | "examples" | "outputSchema"> & {
    outputSchema?: z.ZodType<JsonObject>;
    examples?: readonly DomainMcpToolExample[];
    call(client: McpAdapterApiClient, args: DomainToolInput): Promise<CallToolResult>;
  },
): DomainMcpTool {
  const { call, ...tool } = definition;
  return {
    ...tool,
    outputSchema: tool.outputSchema ?? genericOutputSchema,
    examples: tool.examples ?? defaultExamplesForTool(tool.name),
    execute(args) {
      return call(apiClient, args);
    },
  };
}

// START_BLOCK_BLOCK_CREATE_DOMAIN_MCP_TOOLS
export function createDomainMcpTools(apiClient: McpAdapterApiClient): DomainMcpTool[] {
  return [
    makeTool(apiClient, {
      name: "add_media",
      title: "Add Media",
      description:
        "Persist one owner-scoped text, URL, object-backed, or multipart file media item without starting analysis.",
      apiPathHint: "POST /v1/media-items",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          kind: z.enum(["text", "url", "file", "photo", "image", "audio", "voice", "video", "document"]),
          source: sourceSchema.optional(),
          file: fileUploadSchema.optional(),
          collection_id: z.uuid().optional(),
          display_name: z.string().min(1).optional(),
          adapter_origin: z.string().min(1).optional(),
          metadata: jsonObjectSchema.optional(),
          retention: jsonObjectSchema.optional(),
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict()
        .refine((args) => Boolean(args.source) !== Boolean(args.file), {
          message: "Exactly one of source or file is required",
        }),
      async call(client, args) {
        const response = await client.request({
          path: "/v1/media-items",
          method: "POST",
          headers: idempotencyHeaders(args),
          body: args.file
            ? toFormData(args)
            : {
                owner: args.owner,
                kind: args.kind,
                source: args.source,
                ...(args.collection_id ? { collection_id: args.collection_id } : {}),
                ...(args.display_name ? { display_name: args.display_name } : {}),
                ...(args.adapter_origin ? { adapter_origin: args.adapter_origin } : {}),
                ...(args.metadata ? { metadata: args.metadata } : {}),
                ...(args.retention ? { retention: args.retention } : {}),
              },
        });
        return successFromResponse(response);
      },
    }),
    makeTool(apiClient, {
      name: "list_media",
      title: "List Media",
      description: "List persisted media items with contract filters and cursor pagination.",
      apiPathHint: "GET /v1/media-items",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          ...paginationInputShape,
          ...mediaFilterInputShape,
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath("/v1/media-items", owner, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "search_media",
      title: "Search Media",
      description: "Search persisted media items through owner-scoped media filters and cursor pagination.",
      apiPathHint: "GET /v1/media-items",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          query: z.string().min(1),
          ...paginationInputShape,
          ...mediaFilterInputShape,
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath("/v1/media-items", owner, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_media",
      title: "Get Media",
      description: "Read one media item with safe source metadata and diagnostics summary.",
      apiPathHint: "GET /v1/media-items/{media_item_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, media_item_id: z.uuid() }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/media-items/${pathSegment(String(args.media_item_id))}`, owner),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "remove_media",
      title: "Remove Media",
      description: "Soft-delete one media item through the API retention contract.",
      apiPathHint: "DELETE /v1/media-items/{media_item_id}",
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, media_item_id: z.uuid() }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/media-items/${pathSegment(String(args.media_item_id))}`, owner),
            method: "DELETE",
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_inbox",
      title: "Get Inbox",
      description: "Read the caller's default inbox collection with ordered media item membership.",
      apiPathHint: "GET /v1/collections/inbox",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object(ownerScopedPaginationInputShape).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath("/v1/collections/inbox", owner, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "create_collection",
      title: "Create Collection",
      description: "Create a mutable owner-scoped collection, optionally seeded with media item ids.",
      apiPathHint: "POST /v1/collections",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          name: z.string().min(1),
          items: z.array(z.uuid()).optional(),
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: "/v1/collections",
            method: "POST",
            headers: idempotencyHeaders(args),
            body: {
              owner: args.owner,
              name: args.name,
              ...(args.items ? { items: args.items } : {}),
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "list_collections",
      title: "List Collections",
      description: "List inbox and user collections ordered by update time.",
      apiPathHint: "GET /v1/collections",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object(ownerScopedPaginationInputShape).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath("/v1/collections", owner, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_collection",
      title: "Get Collection",
      description: "Read one collection and its current paginated item membership.",
      apiPathHint: "GET /v1/collections/{collection_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, collection_id: z.uuid(), ...paginationInputShape }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, collection_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/collections/${pathSegment(String(collection_id))}`, owner, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "update_collection",
      title: "Update Collection",
      description: "Rename, archive, restore, or delete one collection with optimistic version checks.",
      apiPathHint: "PATCH /v1/collections/{collection_id}",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          collection_id: z.uuid(),
          owner: ownerSchema,
          expected_version: z.number().int().min(0),
          name: z.string().min(1).optional(),
          status: collectionStatusSchema.optional(),
        })
        .strict()
        .refine((args) => args.name !== undefined || args.status !== undefined, {
          message: "At least one of name or status is required",
        }),
      async call(client, args) {
        const { collection_id, owner, expected_version, name, status } = args;
        return successFromResponse(
          await client.request({
            path: `/v1/collections/${pathSegment(String(collection_id))}`,
            method: "PATCH",
            body: {
              owner,
              expected_version,
              ...(name ? { name } : {}),
              ...(status ? { status } : {}),
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "update_collection_items",
      title: "Update Collection Items",
      description: "Replace or reorder collection item membership with optimistic version checks.",
      apiPathHint: "POST /v1/collections/{collection_id}/items",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          collection_id: z.uuid(),
          owner: ownerSchema,
          expected_version: z.number().int().min(0),
          items: z.array(collectionItemInputSchema),
        })
        .strict(),
      async call(client, args) {
        const { collection_id, owner, expected_version, items } = args;
        return successFromResponse(
          await client.request({
            path: `/v1/collections/${pathSegment(String(collection_id))}/items`,
            method: "POST",
            body: {
              owner,
              expected_version,
              items,
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "create_selection",
      title: "Create Selection",
      description: "Create and seal an immutable selection snapshot from explicit media item references.",
      apiPathHint: "POST /v1/selections",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          source_collection_id: z.uuid().optional(),
          items: z.array(collectionItemInputSchema).min(1),
          option_snapshot: jsonObjectSchema.optional(),
          duplicate_policy: z.enum(["reject", "allow"]).optional(),
          created_by: z.string().min(1).optional(),
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: "/v1/selections",
            method: "POST",
            headers: idempotencyHeaders(args),
            body: {
              owner: args.owner,
              ...(args.source_collection_id ? { source_collection_id: args.source_collection_id } : {}),
              items: args.items,
              ...(args.option_snapshot ? { option_snapshot: args.option_snapshot } : {}),
              ...(args.duplicate_policy ? { duplicate_policy: args.duplicate_policy } : {}),
              ...(args.created_by ? { created_by: args.created_by } : {}),
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_selection",
      title: "Get Selection",
      description: "Read an immutable selection snapshot exactly as sealed by the API.",
      apiPathHint: "GET /v1/selections/{selection_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, selection_id: z.uuid() }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/selections/${pathSegment(String(args.selection_id))}`, owner),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "run_analysis",
      title: "Run Analysis",
      description: "Queue analysis from a sealed selection using the final analysis-run contract.",
      apiPathHint: "POST /v1/analysis-runs",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          selection_id: z.uuid(),
          run_type: z.enum(["transcription", "summary", "report", "deep_research", "custom"]),
          params: jsonObjectSchema.optional(),
          delivery: z
            .object({
              strategy: z.enum(["polling", "webhook"]).optional(),
              webhook: z.object({ url: z.url() }).strict().optional(),
            })
            .strict()
            .optional(),
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: "/v1/analysis-runs",
            method: "POST",
            headers: idempotencyHeaders(args),
            body: {
              owner: args.owner,
              selection_id: args.selection_id,
              run_type: args.run_type,
              ...(args.params ? { params: args.params } : {}),
              ...(args.delivery ? { delivery: args.delivery } : {}),
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "list_runs",
      title: "List Runs",
      description: "List owner-scoped analysis runs with status filtering and pagination.",
      apiPathHint: "GET /v1/analysis-runs",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          ...paginationInputShape,
          status: z
            .enum([
              "queued",
              "running",
              "cancel_requested",
              "partially_succeeded",
              "succeeded",
              "failed",
              "canceled",
              "expired",
            ])
            .optional(),
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath("/v1/analysis-runs", owner, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_run",
      title: "Get Run",
      description: "Read analysis run status, diagnostics summary, and artifact summary.",
      apiPathHint: "GET /v1/analysis-runs/{analysis_run_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, analysis_run_id: z.uuid() }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/analysis-runs/${pathSegment(String(args.analysis_run_id))}`, owner),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "cancel_run",
      title: "Cancel Run",
      description: "Request cooperative cancellation for one analysis run.",
      apiPathHint: "POST /v1/analysis-runs/{analysis_run_id}/cancel",
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          analysis_run_id: z.uuid(),
          message: z.string().min(1).optional(),
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/analysis-runs/${pathSegment(String(args.analysis_run_id))}/cancel`, owner),
            method: "POST",
            body: args.message ? { message: args.message } : {},
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "retry_run",
      title: "Retry Run",
      description: "Create a new analysis run attempt from the same sealed selection as a previous run.",
      apiPathHint: "POST /v1/analysis-runs/{analysis_run_id}/retry",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          analysis_run_id: z.uuid(),
          owner: ownerSchema,
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/analysis-runs/${pathSegment(String(args.analysis_run_id))}/retry`, owner),
            method: "POST",
            headers: idempotencyHeaders(args),
            body: {
              owner,
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "list_run_events",
      title: "List Run Events",
      description: "Poll the append-only timeline for run status, artifact, and diagnostic events.",
      apiPathHint: "GET /v1/analysis-runs/{analysis_run_id}/events",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, analysis_run_id: z.uuid(), ...paginationInputShape }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, analysis_run_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(
              `/v1/analysis-runs/${pathSegment(String(analysis_run_id))}/events`,
              owner,
              query,
            ),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "list_artifacts",
      title: "List Artifacts",
      description: "List artifact summaries produced by one analysis run, including preview metadata.",
      apiPathHint: "GET /v1/artifacts",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, analysis_run_id: z.uuid(), ...paginationInputShape }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, analysis_run_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(
              `/v1/analysis-runs/${pathSegment(String(analysis_run_id))}/artifacts`,
              owner,
              query,
            ),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_artifact",
      title: "Get Artifact",
      description: "Resolve artifact metadata plus preview and download handles.",
      apiPathHint: "GET /v1/artifacts/{artifact_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, artifact_id: z.uuid() }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/artifacts/${pathSegment(String(args.artifact_id))}`, owner),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_artifact_preview",
      title: "Get Artifact Preview",
      description:
        "Return a bounded text, markdown, or JSON preview from artifact preview metadata without downloading the artifact.",
      apiPathHint: "GET /v1/artifacts/{artifact_id}",
      outputSchema: artifactPreviewOutputSchema,
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          artifact_id: z.uuid(),
          format: z.enum(["text", "markdown", "json"]).default("text"),
          max_chars: z.number().int().min(1).max(20000).default(4000),
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const response = await client.request({
          path: ownerQueryPath(`/v1/artifacts/${pathSegment(String(args.artifact_id))}`, owner),
        });
        return shapeArtifactPreview(args, artifactFromResponse(response));
      },
    }),
    makeTool(apiClient, {
      name: "refresh_artifact",
      title: "Refresh Artifact",
      description: "Refresh owner-scoped artifact preview and download handles before retrying preview or download access.",
      apiPathHint: "POST /v1/artifacts/{artifact_id}/refresh",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ owner: ownerSchema, artifact_id: z.uuid() }).strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        return successFromResponse(
          await client.request({
            path: ownerQueryPath(`/v1/artifacts/${pathSegment(String(args.artifact_id))}/refresh`, owner),
            method: "POST",
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_diagnostics",
      title: "Get Diagnostics",
      description: "Query owner-scoped diagnostics by subject, severity, and cursor pagination.",
      apiPathHint: "GET /v1/diagnostics",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          owner: ownerSchema,
          ...paginationInputShape,
          subject_type: z
            .enum([
              "media_item",
              "source",
              "collection",
              "selection",
              "analysis_run",
              "artifact",
              "adapter",
              "retention",
            ])
            .optional(),
          subject_id: z.uuid().optional(),
          severity: z.enum(["info", "warning", "error"]).optional(),
        })
        .strict(),
      async call(client, args) {
        const owner = asRecord(args.owner, "owner");
        const { owner: _owner, ...query } = args;
        return successFromResponse(
          await client.request({
            path: ownerQueryPath("/v1/diagnostics", owner, query),
          }),
        );
      },
    }),
  ];
}
// END_BLOCK_BLOCK_CREATE_DOMAIN_MCP_TOOLS

export function listDomainMcpToolDefinitions(
  tools: readonly DomainMcpTool[],
): DomainMcpToolDefinition[] {
  return tools.map(toolDefinition);
}
