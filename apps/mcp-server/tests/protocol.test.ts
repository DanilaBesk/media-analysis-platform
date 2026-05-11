// FILE: apps/mcp-server/tests/protocol.test.ts
// VERSION: 1.0.0
// START_MODULE_CONTRACT
// PURPOSE: Prove MCP result helpers keep success and error payload shaping deterministic for SDK consumers.
// SCOPE: Verify success text rendering plus minimal and fully populated error envelopes without touching domain logic.
// DEPENDS: M-MCP-ADAPTER
// LINKS: V-M-MCP-ADAPTER
// ROLE: TEST
// MAP_MODE: SUMMARY
// END_MODULE_CONTRACT

import test from "node:test";
import assert from "node:assert/strict";

import {
  createErrorToolResult,
  createSuccessToolResult,
} from "../src/tools/protocol.ts";

test("createSuccessToolResult mirrors structured content into text output", () => {
  const result = createSuccessToolResult({
    status: "ok",
    count: 2,
  });

  assert.equal(result.isError, undefined);
  assert.deepEqual(result.structuredContent, {
    status: "ok",
    count: 2,
  });
  assert.deepEqual(result.content, [
    {
      type: "text",
      text: JSON.stringify(
        {
          status: "ok",
          count: 2,
        },
        null,
        2,
      ),
    },
  ]);
});

test("createErrorToolResult omits optional fields when they are not provided", () => {
  const result = createErrorToolResult({
    code: "mcp_contract_violation",
    message: "bad input",
  });

  assert.equal(result.isError, true);
  assert.deepEqual(result.structuredContent, {
    error: {
      code: "mcp_contract_violation",
      message: "bad input",
    },
  });
});

test("createErrorToolResult preserves all supported optional error fields", () => {
  const result = createErrorToolResult({
    code: "artifact_resolution_failed",
    message: "refresh first",
    category: "upstream_api",
    retryable: false,
    action: "refresh_artifact_then_retry_preview",
    correlationId: "corr-42",
    details: {
      artifact_id: "artifact-1",
    },
    diagnostics: [{ code: "object_missing" }],
    conflict: {
      expected_version: 2,
    },
  });

  assert.equal(result.isError, true);
  assert.deepEqual(result.structuredContent, {
    error: {
      code: "artifact_resolution_failed",
      message: "refresh first",
      category: "upstream_api",
      retryable: false,
      action: "refresh_artifact_then_retry_preview",
      correlation_id: "corr-42",
      details: {
        artifact_id: "artifact-1",
      },
      diagnostics: [{ code: "object_missing" }],
      conflict: {
        expected_version: 2,
      },
    },
  });
});
