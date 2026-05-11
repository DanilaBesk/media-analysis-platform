# Final Closure Matrix

This document is the canonical ordered runbook for final repo closure against the inbox-first GRACE plan.

It does not pretend the repository is at literal `100% coverage`.
It records the exact commands that prove the current state, the metrics each surface can actually emit, and the remaining gaps that must be closed before making stronger claims.

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
- Surfaces with measurable coverage emit their current baseline.
- Any missing metric remains an explicit failing gap instead of being hidden.

## Current Measurable Baselines

Latest measured baselines from the active tree:

- Go `apps/api/internal/api`: `49.5%` statements
- Go `apps/api/internal/storage`: `31.7%` statements
- Python `workers/common/src/transcriber_workers_common`: `95%`
- Python `workers/transcription/src/transcriber_worker_transcription.py`: `91%`
- Python `workers/agent-runner/src/transcriber_worker_agent_runner.py`: `86%`
- Python `apps/telegram-bot/src/telegram_adapter`: `66%`
- Node `apps/mcp-server/src`: `100%` lines, `79.91%` branches, `100%` functions
- Web `apps/web/src`: `84.39%` lines, `73.2%` branches, `65.78%` functions

## Remaining Closure Truth

The repo does **not** currently satisfy a literal `100% coverage` claim.

Remaining explicit gap:

- Several measured surfaces still remain below a literal `100%` bar, especially Go API/storage, Telegram adapter, and agent-runner/transcription runtime code.

Any future claim of full closure must cite this runbook and the actual command outputs, not proxy signals.
