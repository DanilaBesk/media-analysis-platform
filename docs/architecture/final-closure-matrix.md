# Final Closure Matrix

This document is the canonical ordered runbook for final repo closure against the inbox-first GRACE plan.

It records the exact commands that prove the current state and the metrics each surface can actually emit.
All declared percentage-emitting surfaces in the current tree are now at literal `100%`.

## Preconditions

- `docker compose`, `uv`, `pnpm`, `python3`, and `xmllint` are available.
- Compose env examples under `infra/env/*.env.example` are present and unchanged from the checked-in runtime contract.
- For Python suites, use the exact `PYTHONPATH` values below instead of relying on ambient shell state.

## Ordered Gates

Run the gates in this order. Do not skip ahead and do not collapse a later green gate into proof for an earlier one.

### 1. GRACE XML Integrity

```bash
xmllint --noout \
  docs/requirements.xml \
  docs/technology.xml \
  docs/development-plan.xml \
  docs/verification-plan.xml \
  docs/knowledge-graph.xml \
  docs/operational-packets.xml
```

Expected gate:
- XML parses cleanly with exit code `0`.

### 2. Contract Surface

```bash
uv run pytest packages/contracts/tests/test_contract_surfaces.py
```

Expected gate:
- Public contract tests pass.
- Removed vocabulary checks stay green.

### 3. Go API And Storage

```bash
cd apps/api
go test ./internal/api ./internal/storage
cd ../..
```

Expected gate:
- API route, orchestration, diagnostics, retention, and artifact tests pass.

### 4. Python Worker Common

```bash
PYTHONPATH=workers/common/src \
uv run pytest workers/common/tests
```

Expected gate:
- Shared worker transport, object store, artifact, document, and runtime tests pass.

### 5. Python Transcription Worker

```bash
PYTHONPATH=workers/common/src:workers/transcription/src \
uv run pytest workers/transcription/tests
```

Expected gate:
- Transcription worker tests pass, including materialization and partial-success behavior.

### 6. Python Agent Runner Worker

```bash
PYTHONPATH=workers/common/src:workers/agent-runner/src \
uv run pytest workers/agent-runner/tests
```

Expected gate:
- Agent-runner tests pass, including diagnostics emission and partial-success policy.

### 7. Telegram Adapter

```bash
PYTHONPATH=apps/telegram-bot/src \
uv run pytest apps/telegram-bot/tests
```

Expected gate:
- Telegram adapter tests pass with explicit selection/run callbacks and stable API identifiers.

### 8. Web Adapter

```bash
cd apps/web
pnpm test -- --run tests/routes.test.tsx
pnpm test -- --run tests/api-client.test.ts
cd ../..
```

Expected gate:
- Web route and client suites pass.
- Sealed-selection immutability regression stays green.

### 9. MCP Adapter

```bash
cd apps/mcp-server
pnpm --filter @media-analysis-platform/mcp-server exec node --test --experimental-strip-types tests/tool-registry.test.ts
pnpm --filter @media-analysis-platform/mcp-server exec node --test --experimental-strip-types tests/index.test.ts
pnpm --filter @media-analysis-platform/mcp-server exec node --test --experimental-strip-types tests/api-client.test.ts
cd ../..
```

Expected gate:
- MCP tool registry, request shaping, owner-scope parity, and runtime bootstrap tests pass.

### 10. Runtime Final E2E

```bash
bash infra/scripts/compose-smoke.sh --check-config
bash infra/scripts/compose-smoke.sh --live-smoke
```

Expected gate:
- Live compose proof succeeds end to end.
- The runtime proof script verifies:
  - media ingest
  - inbox presence
  - sealed selection
  - terminal analysis run
  - events
  - artifacts
  - diagnostics endpoint readability
  - cross-owner denial
  - retention-preserved run lineage

### 11. Coverage Inventory

```bash
bash infra/scripts/coverage-inventory.sh
```

Expected gate:
- Script runs end to end.
- Every declared measurable surface emits `100%`.
- Any missing metric or non-`100%` regression remains an explicit failing gap instead of being hidden.

## Current Measurable Baselines

Latest measured baselines from the active tree:

- Go `apps/api/internal/api`: `100%` statements
- Go `apps/api/internal/storage`: `100%` statements
- Python `workers/common/src/transcriber_workers_common`: `100%`
- Python `workers/transcription/src/transcriber_worker_transcription.py`: `100%`
- Python `workers/agent-runner/src/transcriber_worker_agent_runner.py`: `100%`
- Python `apps/telegram-bot/src/telegram_adapter`: `100%`
- Node `apps/mcp-server/src`: `100%` lines, `100%` branches, `100%` functions
- Web `apps/web/src`: `100%` lines, `100%` branches, `100%` functions

## Remaining Closure Truth

The repo now satisfies a literal `100%` measured coverage claim for every declared percentage-emitting surface.

Remaining truth:

- XML integrity, contract tests, and runtime-final compose proof remain separate acceptance gates and must still be cited alongside the coverage inventory.
- Any future claim of full closure must cite this runbook and the actual command outputs, not proxy signals.
