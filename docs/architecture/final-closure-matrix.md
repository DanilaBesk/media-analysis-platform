# Final Closure Matrix

This document is the canonical ordered runbook for final repo closure against the single-user channel-aware target plan.

It records the exact commands that prove the current state and the metrics each surface can actually emit. The traceability source for target rebuild coverage is `docs/architecture/target-coverage-matrix.md`; this runbook is command evidence, not a substitute for the matrix.

## Preconditions

- `docker compose`, `uv`, `pnpm`, `python3`, and `xmllint` are available.
- Compose env examples under `infra/env/*.env.example` are present and unchanged from the checked-in runtime contract.
- For Python suites, use the exact `PYTHONPATH` values below instead of relying on ambient shell state.
- The deterministic target fixture manifest validates with `uv run pytest packages/contracts/tests/test_target_fixtures.py -q`.
- A disposable target Postgres reset can be checked with `bash infra/scripts/target-reset-smoke.sh`.

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
uv run pytest packages/contracts/tests/test_target_fixtures.py -q
```

Expected gate:
- Public contract tests pass.
- Removed vocabulary checks stay green.
- Deterministic fixture ids, object-store bytes, hashes, and target vocabulary validate.

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
- MCP tool registry, request shaping, channel-account scope parity, and runtime bootstrap tests pass.

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
  - cross-channel-account denial
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

The executable percentage baselines are owned by `bash infra/scripts/coverage-inventory.sh`. Refresh them from command output during closure instead of copying older values into this document.

## Remaining Closure Truth

- XML integrity, contract tests, fixture validation, target reset smoke, and runtime-final compose proof remain separate acceptance gates and must still be cited alongside the coverage inventory.
- `media-7f3.10` and `media-7f3.11` remain open until the target coverage matrix rows have actual proof, gaps, accepted risks, or blockers.
- Any future claim of full closure must cite this runbook and the actual command outputs, not proxy signals.
