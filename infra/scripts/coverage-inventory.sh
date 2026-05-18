#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "${ROOT_DIR}"

echo "[CoverageInventory] metric surfaces use each tool's native coverage output; pass/fail-only surfaces are listed separately."
echo

echo "[CoverageInventory][metric=go-statements] Go API/storage"
(
  cd apps/api
  go test -cover ./internal/api ./internal/storage
)

echo
echo "[CoverageInventory][metric=python-statements] Python worker-common"
PYTHONPATH=workers/common/src uv run pytest \
  --cov=workers/common/src/transcriber_workers_common \
  --cov-report=term \
  workers/common/tests

echo
echo "[CoverageInventory][metric=python-statements] Python agent-runner"
PYTHONPATH=workers/common/src:workers/agent-runner/src uv run pytest \
  --cov=transcriber_worker_agent_runner \
  --cov-report=term \
  workers/agent-runner/tests

echo
echo "[CoverageInventory][metric=python-statements] Python transcription"
PYTHONPATH=workers/common/src:workers/transcription/src uv run pytest \
  --cov=transcriber_worker_transcription \
  --cov-report=term \
  workers/transcription/tests

echo
echo "[CoverageInventory][metric=python-statements] Python telegram adapter"
PYTHONPATH=apps/telegram-bot/src uv run --with aiogram --with python-dotenv pytest \
  --cov=apps/telegram-bot/src/telegram_adapter \
  --cov-report=term \
  apps/telegram-bot/tests

echo
echo "[CoverageInventory][metric=node-line-branch-function] Node MCP adapter"
(
  cd apps/mcp-server
  node --test --experimental-strip-types --experimental-test-coverage tests/*.test.ts
)

echo
echo "[CoverageInventory][metric=v8-statements-branches-functions-lines] Web UI"
pnpm --dir apps/web exec vitest run --coverage --coverage.reporter=text-summary

echo
echo "[CoverageInventory][pass-fail] Contract surfaces and deterministic fixtures"
uv run pytest packages/contracts/tests/test_contract_surfaces.py packages/contracts/tests/test_target_fixtures.py -q

echo
echo "[CoverageInventory][pass-fail] Target no-legacy gate"
bash infra/scripts/no-legacy-target-gate.sh

echo
echo "[CoverageInventory][pass-fail] Compose topology and runtime-proof wiring"
bash infra/scripts/compose-smoke.sh --check-config

echo
echo "[CoverageInventory][pass-fail] MCP TypeScript typecheck"
pnpm --dir apps/mcp-server typecheck

echo
echo "[CoverageInventory] completed all metric and pass/fail-only surfaces"
