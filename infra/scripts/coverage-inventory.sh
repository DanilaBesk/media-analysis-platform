#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "${ROOT_DIR}"

echo "[CoverageInventory] Go API/storage"
(
  cd apps/api
  go test -cover ./internal/api ./internal/storage
)

echo
echo "[CoverageInventory] Python worker-common"
PYTHONPATH=workers/common/src uv run pytest \
  --cov=workers/common/src/transcriber_workers_common \
  --cov-report=term \
  workers/common/tests

echo
echo "[CoverageInventory] Python agent-runner"
PYTHONPATH=workers/common/src:workers/agent-runner/src uv run pytest \
  --cov=transcriber_worker_agent_runner \
  --cov-report=term \
  workers/agent-runner/tests

echo
echo "[CoverageInventory] Python transcription"
PYTHONPATH=workers/common/src:workers/transcription/src uv run pytest \
  --cov=transcriber_worker_transcription \
  --cov-report=term \
  workers/transcription/tests

echo
echo "[CoverageInventory] Python telegram adapter"
PYTHONPATH=apps/telegram-bot/src uv run --with aiogram --with python-dotenv pytest \
  --cov=apps/telegram-bot/src/telegram_adapter \
  --cov-report=term \
  apps/telegram-bot/tests

echo
echo "[CoverageInventory] Node MCP adapter"
(
  cd apps/mcp-server
  node --test --experimental-strip-types --experimental-test-coverage tests/*.test.ts
)

echo
echo "[CoverageInventory] Web UI"
pnpm --dir apps/web coverage
