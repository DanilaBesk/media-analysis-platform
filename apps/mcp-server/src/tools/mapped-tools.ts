// FILE: apps/mcp-server/src/tools/mapped-tools.ts
// VERSION: 2.0.0
// START_MODULE_CONTRACT
// PURPOSE: Materialize the domain-first MCP tool-to-API mapping layer for API-owned media workflows.
// SCOPE: Define final media, collection, selection, run, artifact, and diagnostic MCP tools, validate adapter inputs, and call only inbox-first HTTP API paths.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: RUNTIME
// MAP_MODE: EXPORTS
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v2.0.0 - Replaced the old execution vocabulary with inbox-first domain tools backed by the real MCP SDK runtime.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   DomainMcpTool - Defines one executable domain tool.
//   DomainMcpToolDefinition - Defines public MCP tool metadata and schema.
//   DomainMcpToolExample - Describes one tool invocation example.
//   createDomainMcpTools - Creates the domain-first tool handlers over the API client.
//   listDomainMcpToolDefinitions - Returns stable public tool definitions.
//   validate-domain-inputs - Use Zod schemas aligned with the public inbox-first contracts.
//   map-tools-to-api - Route domain tools to /v1/media-assets, /v1/collections, /v1/selection-snapshots, /v1/analysis-runs, /v1/artifacts, and /v1/diagnostics.
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

const EXAMPLE_CHANNEL_ACCOUNT_ID = "00000000-0000-4000-8000-000000000010";

const EXAMPLE_MEDIA_ASSET_ID = "00000000-0000-4000-8000-000000000001";
const EXAMPLE_COLLECTION_ID = "00000000-0000-4000-8000-000000000002";
const EXAMPLE_SELECTION_SNAPSHOT_ID = "00000000-0000-4000-8000-000000000003";
const EXAMPLE_RUN_ID = "00000000-0000-4000-8000-000000000004";
const EXAMPLE_ARTIFACT_ID = "00000000-0000-4000-8000-000000000005";

const channelAccountIdSchema = z
  .uuid()
  .describe("Stable channel_account_id resolved by the API for this MCP caller.");

const originSchema = z.discriminatedUnion("origin_type", [
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
      origin_type: z.enum(["upload", "telegram_file"]),
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
    media_asset_id: z.uuid(),
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

const channelScopedPaginationInputShape = {
  channel_account_id: channelAccountIdSchema,
  ...paginationInputShape,
};

const mediaFilterInputShape = {
  kind: z.enum(["text", "url", "file", "photo", "image", "audio", "voice", "video", "document"]).optional(),
  status: z.enum(["validating", "ready", "quarantined", "deleted"]).optional(),
};

const collectionStatusSchema = z.enum(["active", "archived", "deleted"]);

function defaultExamplesForTool(name: string): readonly DomainMcpToolExample[] {
  const argsByTool: Record<string, JsonObject> = {
    create_media_asset: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      kind: "text",
      origin: {
        origin_type: "text",
        text: "Meeting transcript fragment",
      },
      display_name: "Meeting notes",
    },
    list_media_assets: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      page_size: 25,
      kind: "text",
      status: "ready",
    },
    search_media_assets: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      query: "meeting",
      kind: "text",
    },
    get_media_asset: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      media_asset_id: EXAMPLE_MEDIA_ASSET_ID,
    },
    delete_media_asset: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      media_asset_id: EXAMPLE_MEDIA_ASSET_ID,
    },
    get_inbox: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      page_size: 10,
    },
    create_collection: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      name: "Research clips",
      items: [EXAMPLE_MEDIA_ASSET_ID],
    },
    list_collections: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      page_size: 10,
    },
    get_collection: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      collection_id: EXAMPLE_COLLECTION_ID,
      page_size: 10,
    },
    update_collection: {
      collection_id: EXAMPLE_COLLECTION_ID,
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      expected_version: 1,
      name: "Research clips v2",
    },
    update_collection_items: {
      collection_id: EXAMPLE_COLLECTION_ID,
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      expected_version: 1,
      items: [{ media_asset_id: EXAMPLE_MEDIA_ASSET_ID, position: 0 }],
    },
    create_selection_snapshot: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      source_collection_id: EXAMPLE_COLLECTION_ID,
      items: [{ media_asset_id: EXAMPLE_MEDIA_ASSET_ID, position: 0 }],
    },
    get_selection_snapshot: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      selection_snapshot_id: EXAMPLE_SELECTION_SNAPSHOT_ID,
    },
    run_analysis: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      selection_snapshot_id: EXAMPLE_SELECTION_SNAPSHOT_ID,
      run_type: "summary",
    },
    list_runs: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      page_size: 10,
      status: "queued",
    },
    get_run: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      analysis_run_id: EXAMPLE_RUN_ID,
    },
    cancel_run: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      analysis_run_id: EXAMPLE_RUN_ID,
      message: "user requested stop",
    },
    retry_run: {
      analysis_run_id: EXAMPLE_RUN_ID,
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
    },
    list_run_events: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      analysis_run_id: EXAMPLE_RUN_ID,
      page_size: 10,
    },
    list_artifacts: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      analysis_run_id: EXAMPLE_RUN_ID,
      page_size: 10,
    },
    get_artifact: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      artifact_id: EXAMPLE_ARTIFACT_ID,
    },
    get_artifact_preview: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      artifact_id: EXAMPLE_ARTIFACT_ID,
      format: "markdown",
      max_chars: 4000,
    },
    refresh_artifact: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      artifact_id: EXAMPLE_ARTIFACT_ID,
    },
    get_diagnostics: {
      channel_account_id: EXAMPLE_CHANNEL_ACCOUNT_ID,
      subject_type: "analysis_run",
      subject_id: EXAMPLE_RUN_ID,
      severity: "warning",
    },
  };

  return [
    {
      description: `Example arguments for ${name}.`,
      arguments: Object.assign({}, argsByTool[name]),
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
  return [path, query].filter(Boolean).join("?");
}

function channelQueryPath(path: string, channelAccountID: unknown, params: Record<string, unknown> = {}): string {
  return queryPath(path, {
    channel_account_id: channelAccountID,
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
    channel_account_id: args.channel_account_id,
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
      name: "create_media_asset",
      title: "Create Media Asset",
      description:
        "Persist one channel-account-scoped text, URL, object-backed, or multipart file media asset without starting analysis.",
      apiPathHint: "POST /v1/media-assets",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
          kind: z.enum(["text", "url", "file", "photo", "image", "audio", "voice", "video", "document"]),
          origin: originSchema.optional(),
          file: fileUploadSchema.optional(),
          collection_id: z.uuid().optional(),
          display_name: z.string().min(1).optional(),
          adapter_origin: z.string().min(1).optional(),
          metadata: jsonObjectSchema.optional(),
          retention: jsonObjectSchema.optional(),
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict()
        .refine((args) => Boolean(args.origin) !== Boolean(args.file), {
          message: "Exactly one of origin or file is required",
        }),
      async call(client, args) {
        const response = await client.request({
          path: "/v1/media-assets",
          method: "POST",
          headers: idempotencyHeaders(args),
          body: args.file
            ? toFormData(args)
            : {
                channel_account_id: args.channel_account_id,
                kind: args.kind,
                origin: args.origin,
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
      name: "list_media_assets",
      title: "List Media Assets",
      description: "List persisted media assets with contract filters and cursor pagination.",
      apiPathHint: "GET /v1/media-assets",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
          ...paginationInputShape,
          ...mediaFilterInputShape,
        })
        .strict(),
      async call(client, args) {
        const { channel_account_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath("/v1/media-assets", channel_account_id, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "search_media_assets",
      title: "Search Media Assets",
      description: "Search persisted media assets through channel-scoped filters and cursor pagination.",
      apiPathHint: "GET /v1/media-assets",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
          query: z.string().min(1),
          ...paginationInputShape,
          ...mediaFilterInputShape,
        })
        .strict(),
      async call(client, args) {
        const { channel_account_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath("/v1/media-assets", channel_account_id, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_media_asset",
      title: "Get Media Asset",
      description: "Read one media asset with safe origin metadata and diagnostics summary.",
      apiPathHint: "GET /v1/media-assets/{media_asset_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, media_asset_id: z.uuid() }).strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: channelQueryPath(`/v1/media-assets/${pathSegment(String(args.media_asset_id))}`, args.channel_account_id),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "delete_media_asset",
      title: "Delete Media Asset",
      description: "Mark one media asset deleted through the API retention contract.",
      apiPathHint: "DELETE /v1/media-assets/{media_asset_id}",
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, media_asset_id: z.uuid() }).strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: channelQueryPath(`/v1/media-assets/${pathSegment(String(args.media_asset_id))}`, args.channel_account_id),
            method: "DELETE",
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_inbox",
      title: "Get Inbox",
      description: "Read the caller's default inbox collection with ordered media asset membership.",
      apiPathHint: "GET /v1/collections/inbox",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object(channelScopedPaginationInputShape).strict(),
      async call(client, args) {
        const { channel_account_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath("/v1/collections/inbox", channel_account_id, query),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "create_collection",
      title: "Create Collection",
      description: "Create a mutable channel-scoped collection, optionally seeded with media asset ids.",
      apiPathHint: "POST /v1/collections",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
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
              channel_account_id: args.channel_account_id,
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
      inputSchema: z.object(channelScopedPaginationInputShape).strict(),
      async call(client, args) {
        const { channel_account_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath("/v1/collections", channel_account_id, query),
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
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, collection_id: z.uuid(), ...paginationInputShape }).strict(),
      async call(client, args) {
        const { channel_account_id, collection_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath(`/v1/collections/${pathSegment(String(collection_id))}`, channel_account_id, query),
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
          channel_account_id: channelAccountIdSchema,
          expected_version: z.number().int().min(0),
          name: z.string().min(1).optional(),
          status: collectionStatusSchema.optional(),
        })
        .strict()
        .refine((args) => args.name !== undefined || args.status !== undefined, {
          message: "At least one of name or status is required",
        }),
      async call(client, args) {
        const { collection_id, channel_account_id, expected_version, name, status } = args;
        return successFromResponse(
          await client.request({
            path: `/v1/collections/${pathSegment(String(collection_id))}`,
            method: "PATCH",
            body: {
              channel_account_id,
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
          channel_account_id: channelAccountIdSchema,
          expected_version: z.number().int().min(0),
          items: z.array(collectionItemInputSchema),
        })
        .strict(),
      async call(client, args) {
        const { collection_id, channel_account_id, expected_version, items } = args;
        return successFromResponse(
          await client.request({
            path: `/v1/collections/${pathSegment(String(collection_id))}/items`,
            method: "POST",
            body: {
              channel_account_id,
              expected_version,
              items,
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "create_selection_snapshot",
      title: "Create Selection Snapshot",
      description: "Create and seal an immutable selection snapshot from explicit media asset references.",
      apiPathHint: "POST /v1/selection-snapshots",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
          source_collection_id: z.uuid().optional(),
          items: z.array(collectionItemInputSchema).min(1),
          option_snapshot: jsonObjectSchema.optional(),
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: "/v1/selection-snapshots",
            method: "POST",
            headers: idempotencyHeaders(args),
            body: {
              channel_account_id: args.channel_account_id,
              ...(args.source_collection_id ? { source_collection_id: args.source_collection_id } : {}),
              items: args.items,
              ...(args.option_snapshot ? { option_snapshot: args.option_snapshot } : {}),
              created_via_channel_account_id: args.channel_account_id,
            },
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_selection_snapshot",
      title: "Get Selection Snapshot",
      description: "Read an immutable selection snapshot exactly as sealed by the API.",
      apiPathHint: "GET /v1/selection-snapshots/{selection_snapshot_id}",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, selection_snapshot_id: z.uuid() }).strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: channelQueryPath(
              `/v1/selection-snapshots/${pathSegment(String(args.selection_snapshot_id))}`,
              args.channel_account_id,
            ),
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "run_analysis",
      title: "Run Analysis",
      description: "Queue analysis from a sealed selection snapshot using the target analysis-run contract.",
      apiPathHint: "POST /v1/analysis-runs",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
          selection_snapshot_id: z.uuid(),
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
              channel_account_id: args.channel_account_id,
              selection_snapshot_id: args.selection_snapshot_id,
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
      description: "List channel-scoped analysis runs with status filtering and pagination.",
      apiPathHint: "GET /v1/analysis-runs",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
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
        const { channel_account_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath("/v1/analysis-runs", channel_account_id, query),
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
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, analysis_run_id: z.uuid() }).strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: channelQueryPath(`/v1/analysis-runs/${pathSegment(String(args.analysis_run_id))}`, args.channel_account_id),
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
          channel_account_id: channelAccountIdSchema,
          analysis_run_id: z.uuid(),
          message: z.string().min(1).optional(),
        })
        .strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: `/v1/analysis-runs/${pathSegment(String(args.analysis_run_id))}/cancel`,
            method: "POST",
            body: {
              channel_account_id: args.channel_account_id,
              ...(args.message ? { message: args.message } : {}),
            },
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
          channel_account_id: channelAccountIdSchema,
          idempotency_key: idempotencyKeySchema.optional(),
        })
        .strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: `/v1/analysis-runs/${pathSegment(String(args.analysis_run_id))}/retry`,
            method: "POST",
            headers: idempotencyHeaders(args),
            body: {
              channel_account_id: args.channel_account_id,
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
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, analysis_run_id: z.uuid(), ...paginationInputShape }).strict(),
      async call(client, args) {
        const { channel_account_id, analysis_run_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath(
              `/v1/analysis-runs/${pathSegment(String(analysis_run_id))}/events`,
              channel_account_id,
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
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, analysis_run_id: z.uuid(), ...paginationInputShape }).strict(),
      async call(client, args) {
        const { channel_account_id, analysis_run_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath(
              `/v1/analysis-runs/${pathSegment(String(analysis_run_id))}/artifacts`,
              channel_account_id,
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
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, artifact_id: z.uuid() }).strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: channelQueryPath(`/v1/artifacts/${pathSegment(String(args.artifact_id))}`, args.channel_account_id),
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
          channel_account_id: channelAccountIdSchema,
          artifact_id: z.uuid(),
          format: z.enum(["text", "markdown", "json"]).default("text"),
          max_chars: z.number().int().min(1).max(20000).default(4000),
        })
        .strict(),
      async call(client, args) {
        const response = await client.request({
          path: channelQueryPath(`/v1/artifacts/${pathSegment(String(args.artifact_id))}`, args.channel_account_id),
        });
        return shapeArtifactPreview(args, artifactFromResponse(response));
      },
    }),
    makeTool(apiClient, {
      name: "refresh_artifact",
      title: "Refresh Artifact",
      description: "Refresh channel-scoped artifact preview and download handles before retrying preview or download access.",
      apiPathHint: "POST /v1/artifacts/{artifact_id}/refresh",
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z.object({ channel_account_id: channelAccountIdSchema, artifact_id: z.uuid() }).strict(),
      async call(client, args) {
        return successFromResponse(
          await client.request({
            path: channelQueryPath(`/v1/artifacts/${pathSegment(String(args.artifact_id))}/refresh`, args.channel_account_id),
            method: "POST",
          }),
        );
      },
    }),
    makeTool(apiClient, {
      name: "get_diagnostics",
      title: "Get Diagnostics",
      description: "Query channel-scoped diagnostics by subject, severity, and cursor pagination.",
      apiPathHint: "GET /v1/diagnostics",
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
      inputSchema: z
        .object({
          channel_account_id: channelAccountIdSchema,
          ...paginationInputShape,
          subject_type: z
            .enum([
              "media_asset",
              "stored_object",
              "collection",
              "selection_snapshot",
              "analysis_run",
              "analysis_run_step",
              "artifact",
              "artifact_subject",
              "diagnostic",
              "channel_account",
              "channel_surface",
              "operation_request",
            ])
            .optional(),
          subject_id: z.uuid().optional(),
          severity: z.enum(["info", "warning", "error"]).optional(),
        })
        .strict(),
      async call(client, args) {
        const { channel_account_id, ...query } = args;
        return successFromResponse(
          await client.request({
            path: channelQueryPath("/v1/diagnostics", channel_account_id, query),
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
