// FILE: apps/mcp-server/src/client/api-client.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Provide a thin, mockable HTTP boundary for the MCP app shell without taking ownership of API business behavior.
// SCOPE: Normalize request targets, execute transport-level JSON requests, and preserve upstream error envelopes for the follow-on packet.
// DEPENDS: M-MCP-ADAPTER, M-API-HTTP, M-CONTRACTS
// LINKS: M-MCP-ADAPTER, V-M-MCP-ADAPTER
// ROLE: RUNTIME
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT
//
// START_CHANGE_SUMMARY
//   LAST_CHANGE: v1.0.0 - Added the packet-local HTTP client boundary required by the MCP shell unblock packet.
// END_CHANGE_SUMMARY
//
// START_MODULE_MAP
//   normalize-request-targets - Resolve packet-local request URLs against API_BASE_URL only inside the client boundary.
//   send-json-requests - Execute generic JSON requests without introducing tool semantics or business logic ownership.
// END_MODULE_MAP

export interface McpAdapterApiRequest {
  path: string;
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  body?: BodyInit | Record<string, unknown>;
  headers?: Record<string, string>;
}

export interface McpAdapterApiResponse<TPayload = unknown> {
  status: number;
  data: TPayload | null;
}

export interface McpAdapterApiClient {
  request<TPayload = unknown>(
    request: McpAdapterApiRequest,
  ): Promise<McpAdapterApiResponse<TPayload>>;
}

export interface CreateMcpAdapterApiClientOptions {
  baseUrl: string;
  fetchImpl?: typeof fetch;
}

export class McpAdapterApiClientError extends Error {
  readonly status: number;
  readonly path: string;
  readonly code?: string;

  constructor(path: string, status: number, message: string, code?: string) {
    super(message);
    this.name = "McpAdapterApiClientError";
    this.status = status;
    this.path = path;
    this.code = code;
  }
}

function toRequestUrl(baseUrl: string, path: string): URL {
  const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const normalizedPath = path.startsWith("/") ? path.slice(1) : path;
  return new URL(normalizedPath, normalizedBaseUrl);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function extractErrorEnvelope(payload: unknown): { code?: string; message?: string } {
  if (!isRecord(payload) || !isRecord(payload.error)) {
    return {};
  }

  return {
    code: typeof payload.error.code === "string" ? payload.error.code : undefined,
    message: typeof payload.error.message === "string" ? payload.error.message : undefined,
  };
}

async function readResponsePayload(response: Response): Promise<unknown> {
  if (response.status === 204) {
    return null;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return response.json();
  }

  const text = await response.text();
  return text === "" ? null : text;
}

function isBodyInitLike(body: BodyInit | Record<string, unknown>): body is BodyInit {
  return (
    typeof body === "string" ||
    body instanceof Blob ||
    body instanceof FormData ||
    body instanceof URLSearchParams ||
    body instanceof ArrayBuffer ||
    ArrayBuffer.isView(body)
  );
}

// START_BLOCK_BLOCK_CREATE_MCP_ADAPTER_API_CLIENT
export function createMcpAdapterApiClient({
  baseUrl,
  fetchImpl = fetch,
}: CreateMcpAdapterApiClientOptions): McpAdapterApiClient {
  return {
    async request<TPayload>({
      path,
      method = "GET",
      body,
      headers,
    }: McpAdapterApiRequest): Promise<McpAdapterApiResponse<TPayload>> {
      const requestHeaders: Record<string, string> = {
        Accept: "application/json",
        ...(headers ?? {}),
      };

      let requestBody: BodyInit | undefined;
      if (body !== undefined) {
        if (isBodyInitLike(body)) {
          requestBody = body;
        } else {
          requestHeaders["Content-Type"] = "application/json";
          requestBody = JSON.stringify(body);
        }
      }

      const response = await fetchImpl(toRequestUrl(baseUrl, path), {
        method,
        headers: requestHeaders,
        body: requestBody,
      });

      const payload = await readResponsePayload(response);
      if (!response.ok) {
        const errorEnvelope = extractErrorEnvelope(payload);
        throw new McpAdapterApiClientError(
          path,
          response.status,
          errorEnvelope.message ?? `API request failed with status ${response.status}`,
          errorEnvelope.code,
        );
      }

      return {
        status: response.status,
        data: payload as TPayload | null,
      };
    },
  };
}
// END_BLOCK_BLOCK_CREATE_MCP_ADAPTER_API_CLIENT
