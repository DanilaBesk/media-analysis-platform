#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "${ROOT_DIR}"

MARKER='[NoLegacyTargetGate]'

fail() {
  printf '%s %s\n' "${MARKER}" "$1" >&2
  exit 1
}

require_file_snippet() {
  local path="$1"
  local snippet="$2"
  grep -F -- "${snippet}" "${path}" >/dev/null || fail "file ${path} is missing required target snippet: ${snippet}"
}

reject_fixed() {
  local description="$1"
  local snippet="$2"
  shift 2

  if rg -n -F -- "${snippet}" "$@"; then
    fail "${description}: forbidden snippet still present: ${snippet}"
  fi
  printf '%s ok %s excludes %s\n' "${MARKER}" "${description}" "${snippet}"
}

reject_regex() {
  local description="$1"
  local pattern="$2"
  shift 2

  if rg -n --pcre2 -- "${pattern}" "$@"; then
    fail "${description}: forbidden pattern still present: ${pattern}"
  fi
  printf '%s ok %s excludes %s\n' "${MARKER}" "${description}" "${pattern}"
}

active_target_paths=(
  apps/telegram-bot/src
  apps/web/src
  apps/mcp-server/src
  workers/common/src
  workers/agent-runner/src
  workers/transcription/src
  infra/scripts/runtime-final-e2e.py
)

non_telegram_target_paths=(
  apps/web/src
  apps/mcp-server/src
  workers/common/src
  workers/agent-runner/src
  workers/transcription/src
  infra/scripts/runtime-final-e2e.py
)

strict_target_paths=(
  apps/web/src
  apps/mcp-server/src
  workers/agent-runner/src
  workers/transcription/src
  infra/scripts/runtime-final-e2e.py
)

runtime_proof=infra/scripts/runtime-final-e2e.py
require_file_snippet "${runtime_proof}" "/internal/v1/channel-accounts"
require_file_snippet "${runtime_proof}" "channel_account_id"
require_file_snippet "${runtime_proof}" "/v1/media-assets"
require_file_snippet "${runtime_proof}" "/v1/selection-snapshots"
require_file_snippet "${runtime_proof}" "/internal/v1/artifacts/"

for snippet in "/v1/media-items" "/v1/selections" "analysis_run_tasks" "adapter_projection" "telegram_message_id"; do
  reject_fixed "active target code" "${snippet}" "${active_target_paths[@]}"
done

for snippet in "owner_type" "owner_id"; do
  reject_fixed "non-telegram target code" "${snippet}" "${non_telegram_target_paths[@]}"
done

for snippet in "media_item_id" "selection_id"; do
  reject_fixed "strict target code" "${snippet}" "${strict_target_paths[@]}"
done

reject_regex \
  "target storage implementation" \
  '\b(media_items|selection_items|analysis_run_tasks|owner_type|owner_id|tenant_id|safe_adapter_context)\b' \
  apps/api/internal/storage/target/fixtures.go \
  apps/api/internal/storage/target/model.go \
  apps/api/internal/storage/target/store.go \
  apps/api/internal/api/target_runtime.go

reject_regex \
  "target reset migration recreated legacy tables" \
  'CREATE TABLE (IF NOT EXISTS )?(sources|media_items|selections|selection_items|analysis_run_tasks)\b' \
  apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql

reject_regex \
  "target reset migration active legacy columns" \
  '\b(owner_type|owner_id|tenant_id|safe_adapter_context)\b' \
  apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql

uv run pytest packages/contracts/tests/test_contract_surfaces.py::test_target_operations_do_not_reintroduce_compatibility_names -q
uv run pytest workers/common/tests/test_no_legacy_asr_runtime.py -q

printf '%s target no-legacy gate completed successfully\n' "${MARKER}"
