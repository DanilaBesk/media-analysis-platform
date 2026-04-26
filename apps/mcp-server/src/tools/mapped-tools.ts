// FILE: apps/mcp-server/src/tools/mapped-tools.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Materialize the thin MCP tool-to-API mapping layer for the adapter packet while preserving API-owned semantics.
// SCOPE: Define required MCP tools, validate packet-local arguments, map each tool to the existing HTTP API, and shape results without adding business logic.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the required MCP tool mappings and packet-local argument validation over the existing HTTP API.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   validate-tool-inputs - Enforce packet-local required fields and deterministic contract errors before calling the API.
//   map-tools-to-api - Route required MCP tools to the existing HTTP endpoints without taking ownership of business logic.
//   shape-tool-results - Preserve API envelopes and artifact visibility in structured MCP responses.
// END_MODULE_MAP

import type {
  McpAdapterApiClient,
  McpAdapterApiResponse,
} from "../client/api-client.ts";
import {
  McpAdapterContractError,
  createSuccessToolResult,
  type JsonObject,
  type McpMappedTool,
  type McpToolResult,
} from "./protocol.ts";

interface DeliveryFields {
  strategy: "polling" | "webhook";
  webhookUrl?: string;
}

interface FileInput {
  filename: string;
  contentType: string;
  contentBase64: string;
}

function asRecord(value: unknown, fieldName: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new McpAdapterContractError(`${fieldName} must be an object`, {
      field: fieldName,
    });
  }
  return value as JsonObject;
}

function asString(value: unknown, fieldName: string): string {
  if (typeof value !== "string") {
    throw new McpAdapterContractError(`${fieldName} must be a string`, {
      field: fieldName,
    });
  }
  const normalized = value.trim();
  if (normalized === "") {
    throw new McpAdapterContractError(`${fieldName} must not be empty`, {
      field: fieldName,
    });
  }
  return normalized;
}

function asOptionalString(value: unknown, fieldName: string): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  return asString(value, fieldName);
}

function asOptionalNumber(value: unknown, fieldName: string): number | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new McpAdapterContractError(`${fieldName} must be a finite number`, {
      field: fieldName,
    });
  }
  return value;
}

function asObjectArray(value: unknown, fieldName: string): JsonObject[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new McpAdapterContractError(`${fieldName} must be a non-empty array`, {
      field: fieldName,
    });
  }
  return value.map((item, index) => asRecord(item, `${fieldName}[${index}]`));
}

function extractDeliveryFields(arguments_: JsonObject): DeliveryFields {
  const strategyValue = arguments_.delivery_strategy ?? "polling";
  if (strategyValue !== "polling" && strategyValue !== "webhook") {
    throw new McpAdapterContractError("delivery_strategy must be polling or webhook", {
      field: "delivery_strategy",
    });
  }

  const webhookUrl = asOptionalString(arguments_.delivery_webhook_url, "delivery_webhook_url");
  if (strategyValue === "webhook" && !webhookUrl) {
    throw new McpAdapterContractError("delivery_webhook_url is required for webhook delivery", {
      field: "delivery_webhook_url",
    });
  }

  return {
    strategy: strategyValue,
    webhookUrl,
  };
}

function buildChildPayload(arguments_: JsonObject): JsonObject {
  const delivery = extractDeliveryFields(arguments_);
  return {
    ...(asOptionalString(arguments_.client_ref, "client_ref")
      ? { client_ref: asOptionalString(arguments_.client_ref, "client_ref") }
      : {}),
    delivery: {
      strategy: delivery.strategy,
      ...(delivery.webhookUrl
        ? {
            webhook: {
              url: delivery.webhookUrl,
            },
          }
        : {}),
    },
  };
}

function decodeBase64(value: string, fieldName: string): Uint8Array {
  try {
    return Buffer.from(value, "base64");
  } catch {
    throw new McpAdapterContractError(`${fieldName} must be valid base64`, {
      field: fieldName,
    });
  }
}

function buildUploadFormData(arguments_: JsonObject): FormData {
  const files = asObjectArray(arguments_.files, "files").map((item): FileInput => ({
    filename: asString(item.filename, "filename"),
    contentType: asString(item.content_type, "content_type"),
    contentBase64: asString(item.content_base64, "content_base64"),
  }));
  const delivery = extractDeliveryFields(arguments_);
  const formData = new FormData();

  files.forEach((file) => {
    const contentBytes = decodeBase64(file.contentBase64, "content_base64");
    formData.append(
      "files",
      new Blob([contentBytes], { type: file.contentType }),
      file.filename,
    );
  });

  const displayName = asOptionalString(arguments_.display_name, "display_name");
  const clientRef = asOptionalString(arguments_.client_ref, "client_ref");
  if (displayName) {
    formData.append("display_name", displayName);
  }
  if (clientRef) {
    formData.append("client_ref", clientRef);
  }
  formData.append("delivery_strategy", delivery.strategy);
  if (delivery.webhookUrl) {
    formData.append("delivery_webhook_url", delivery.webhookUrl);
  }

  return formData;
}

function unwrapJobPayload(response: McpAdapterApiResponse<unknown>): JsonObject {
  const payload = response.data;
  const record = asRecord(payload, "response");
  if (typeof record.job === "object" && record.job !== null && !Array.isArray(record.job)) {
    return record.job as JsonObject;
  }
  return record;
}

function unwrapJobsPayload(response: McpAdapterApiResponse<unknown>): JsonObject[] {
  const payload = asRecord(response.data, "response");
  if (!Array.isArray(payload.jobs)) {
    throw new McpAdapterContractError("response.jobs must be an array", {
      field: "jobs",
    });
  }
  return payload.jobs.map((item, index) => asRecord(item, `jobs[${index}]`));
}

function unwrapRecord(response: McpAdapterApiResponse<unknown>): JsonObject {
  return asRecord(response.data, "response");
}

function unwrapArtifactList(job: JsonObject): JsonObject[] {
  if (!Array.isArray(job.artifacts)) {
    throw new McpAdapterContractError("job.artifacts must be an array", {
      field: "artifacts",
    });
  }
  return job.artifacts.map((item, index) => asRecord(item, `artifacts[${index}]`));
}

function createUrlPayload(arguments_: JsonObject): JsonObject {
  const delivery = extractDeliveryFields(arguments_);
  const payload: JsonObject = {
    source_kind: "youtube_url",
    url: asString(arguments_.url, "url"),
    delivery: {
      strategy: delivery.strategy,
      ...(delivery.webhookUrl
        ? {
            webhook: {
              url: delivery.webhookUrl,
            },
          }
        : {}),
    },
  };

  const displayName = asOptionalString(arguments_.display_name, "display_name");
  const clientRef = asOptionalString(arguments_.client_ref, "client_ref");
  if (displayName) {
    payload.display_name = displayName;
  }
  if (clientRef) {
    payload.client_ref = clientRef;
  }
  return payload;
}

function listJobsPath(arguments_: JsonObject): string {
  const search = new URLSearchParams();
  const status = asOptionalString(arguments_.status, "status");
  const jobType = asOptionalString(arguments_.job_type, "job_type");
  const rootJobId = asOptionalString(arguments_.root_job_id, "root_job_id");
  const page = asOptionalNumber(arguments_.page, "page") ?? 1;
  const pageSize = asOptionalNumber(arguments_.page_size, "page_size") ?? 20;

  if (status) {
    search.set("status", status);
  }
  if (jobType) {
    search.set("job_type", jobType);
  }
  if (rootJobId) {
    search.set("root_job_id", rootJobId);
  }
  search.set("page", String(page));
  search.set("page_size", String(pageSize));
  return `/v1/jobs?${search.toString()}`;
}

function toolWithApiCall(
  definition: Omit<McpMappedTool, "execute"> & {
    executeWithClient(
      apiClient: McpAdapterApiClient,
      arguments_: JsonObject,
    ): Promise<McpToolResult>;
  },
  apiClient: McpAdapterApiClient,
): McpMappedTool {
  return {
    name: definition.name,
    description: definition.description,
    inputSchema: definition.inputSchema,
    apiPathHint: definition.apiPathHint,
    execute(arguments_) {
      return definition.executeWithClient(apiClient, arguments_);
    },
  };
}

// START_BLOCK_BLOCK_CREATE_MAPPED_MCP_TOOLS
export function createMappedMcpTools(apiClient: McpAdapterApiClient): McpMappedTool[] {
  return [
    toolWithApiCall(
      {
        name: "create_transcription_job_from_file",
        description: "Create transcription jobs from uploaded file payloads through the HTTP API.",
        apiPathHint: "/v1/transcription-jobs",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["files"],
          properties: {
            files: {
              type: "array",
            },
            display_name: { type: "string" },
            client_ref: { type: "string" },
            delivery_strategy: { type: "string", enum: ["polling", "webhook"] },
            delivery_webhook_url: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const response = await client.request({
            path: "/v1/transcription-jobs",
            method: "POST",
            body: buildUploadFormData(arguments_),
          });

          return createSuccessToolResult({
            jobs: unwrapJobsPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "create_transcription_job_from_url",
        description: "Create one transcription job from a supported source URL through the HTTP API.",
        apiPathHint: "/v1/transcription-jobs/from-url",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["url"],
          properties: {
            url: { type: "string" },
            display_name: { type: "string" },
            client_ref: { type: "string" },
            delivery_strategy: { type: "string", enum: ["polling", "webhook"] },
            delivery_webhook_url: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const response = await client.request({
            path: "/v1/transcription-jobs/from-url",
            method: "POST",
            body: createUrlPayload(arguments_),
          });

          return createSuccessToolResult({
            job: unwrapJobPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "get_job",
        description: "Read one job snapshot through the HTTP API.",
        apiPathHint: "/v1/jobs/{job_id}",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["job_id"],
          properties: {
            job_id: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const jobId = asString(arguments_.job_id, "job_id");
          const response = await client.request({
            path: `/v1/jobs/${jobId}`,
          });

          return createSuccessToolResult({
            job: unwrapJobPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "list_jobs",
        description: "List jobs through the HTTP API with packet-local filter passthrough.",
        apiPathHint: "/v1/jobs",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          properties: {
            status: { type: "string" },
            job_type: { type: "string" },
            root_job_id: { type: "string" },
            page: { type: "number" },
            page_size: { type: "number" },
          },
        },
        async executeWithClient(client, arguments_) {
          const response = await client.request({
            path: listJobsPath(arguments_),
          });

          return createSuccessToolResult(unwrapRecord(response));
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "create_report_job",
        description: "Create a report operation job through the HTTP API.",
        apiPathHint: "/v1/transcription-jobs/{job_id}/report-jobs",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["job_id"],
          properties: {
            job_id: { type: "string" },
            client_ref: { type: "string" },
            delivery_strategy: { type: "string", enum: ["polling", "webhook"] },
            delivery_webhook_url: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const jobId = asString(arguments_.job_id, "job_id");
          const response = await client.request({
            path: `/v1/transcription-jobs/${jobId}/report-jobs`,
            method: "POST",
            body: buildChildPayload(arguments_),
          });

          return createSuccessToolResult({
            job: unwrapJobPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "create_deep_research_job",
        description: "Create a deep-research operation job through the HTTP API.",
        apiPathHint: "/v1/report-jobs/{job_id}/deep-research-jobs",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["job_id"],
          properties: {
            job_id: { type: "string" },
            client_ref: { type: "string" },
            delivery_strategy: { type: "string", enum: ["polling", "webhook"] },
            delivery_webhook_url: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const jobId = asString(arguments_.job_id, "job_id");
          const response = await client.request({
            path: `/v1/report-jobs/${jobId}/deep-research-jobs`,
            method: "POST",
            body: buildChildPayload(arguments_),
          });

          return createSuccessToolResult({
            job: unwrapJobPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "cancel_job",
        description: "Request cooperative cancellation through the HTTP API.",
        apiPathHint: "/v1/jobs/{job_id}/cancel",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["job_id"],
          properties: {
            job_id: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const jobId = asString(arguments_.job_id, "job_id");
          const response = await client.request({
            path: `/v1/jobs/${jobId}/cancel`,
            method: "POST",
          });

          return createSuccessToolResult({
            job: unwrapJobPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "retry_job",
        description: "Create a retry job through the HTTP API.",
        apiPathHint: "/v1/jobs/{job_id}/retry",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["job_id"],
          properties: {
            job_id: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const jobId = asString(arguments_.job_id, "job_id");
          const response = await client.request({
            path: `/v1/jobs/${jobId}/retry`,
            method: "POST",
          });

          return createSuccessToolResult({
            job: unwrapJobPayload(response),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "list_job_artifacts",
        description: "List artifact metadata from a job snapshot through the HTTP API.",
        apiPathHint: "/v1/jobs/{job_id}",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["job_id"],
          properties: {
            job_id: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const jobId = asString(arguments_.job_id, "job_id");
          const response = await client.request({
            path: `/v1/jobs/${jobId}`,
          });
          const job = unwrapJobPayload(response);

          return createSuccessToolResult({
            job_id: jobId,
            artifacts: unwrapArtifactList(job),
          });
        },
      },
      apiClient,
    ),
    toolWithApiCall(
      {
        name: "get_artifact_download_info",
        description: "Resolve artifact download metadata through the HTTP API.",
        apiPathHint: "/v1/artifacts/{artifact_id}",
        inputSchema: {
          type: "object",
          additionalProperties: false,
          required: ["artifact_id"],
          properties: {
            artifact_id: { type: "string" },
          },
        },
        async executeWithClient(client, arguments_) {
          const artifactId = asString(arguments_.artifact_id, "artifact_id");
          const response = await client.request({
            path: `/v1/artifacts/${artifactId}`,
          });

          return createSuccessToolResult({
            artifact: unwrapRecord(response),
          });
        },
      },
      apiClient,
    ),
  ];
}
// END_BLOCK_BLOCK_CREATE_MAPPED_MCP_TOOLS
