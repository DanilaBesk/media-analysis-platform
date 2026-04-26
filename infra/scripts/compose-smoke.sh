#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
COMPOSE_FILE="${ROOT_DIR}/infra/docker-compose.yml"
MARKER='[InfraCompose][verifyLocalStack][BLOCK_VERIFY_LOCAL_STACK_HEALTH]'
RUNTIME_SERVICES=(
  api
  web
  telegram-bot
  mcp-server
  worker-transcription
  worker-agent-runner
  postgres
  redis
  minio
  minio-init
)
FORBIDDEN_SERVICES=(
  worker-report
  worker-deep-research
)

fail() {
  printf '%s %s\n' "${MARKER}" "$1" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "${path}" ]] || fail "missing required file: ${path#${ROOT_DIR}/}"
}

require_file_snippet() {
  local path="$1"
  local snippet="$2"
  require_file "${path}"
  grep -F -- "${snippet}" "${path}" >/dev/null || fail "file ${path#${ROOT_DIR}/} is missing snippet: ${snippet}"
}

reject_file_snippet() {
  local path="$1"
  local snippet="$2"
  require_file "${path}"
  if grep -F -- "${snippet}" "${path}" >/dev/null; then
    fail "file ${path#${ROOT_DIR}/} still contains forbidden snippet: ${snippet}"
  fi
}

require_compose_snippet() {
  local snippet="$1"
  grep -F -- "${snippet}" "${COMPOSE_FILE}" >/dev/null || fail "missing compose snippet: ${snippet}"
}

reject_compose_snippet() {
  local snippet="$1"
  if grep -F -- "${snippet}" "${COMPOSE_FILE}" >/dev/null; then
    fail "compose still contains forbidden snippet: ${snippet}"
  fi
}

require_service() {
  local service="$1"
  grep -Eq "^  ${service}:" "${COMPOSE_FILE}" || fail "missing service definition: ${service}"
}

reject_service() {
  local service="$1"
  if grep -Eq "^  ${service}:" "${COMPOSE_FILE}"; then
    fail "forbidden service definition still exists: ${service}"
  fi
}

service_block_from_content() {
  local service="$1"
  local content="$2"

  printf '%s\n' "${content}" | sed -n "/^  ${service}:/,/^  [a-z0-9][a-z0-9-]*:/p" | sed '$d'
}

require_service_block_snippet() {
  local service="$1"
  local snippet="$2"
  local block

  block=$(service_block_from_content "${service}" "$(cat "${COMPOSE_FILE}")")
  grep -F -- "${snippet}" <<<"${block}" >/dev/null || fail "service ${service} is missing snippet: ${snippet}"
}

validate_static_contract() {
  local service

  command -v docker >/dev/null || fail "docker is required for compose validation"

  require_file "${COMPOSE_FILE}"
  docker compose -f "${COMPOSE_FILE}" config >/dev/null

  require_file "${ROOT_DIR}/infra/env/postgres.env.example"
  require_file "${ROOT_DIR}/infra/env/minio.env.example"
  require_file "${ROOT_DIR}/infra/env/shared.env.example"
  require_file "${ROOT_DIR}/infra/env/api.env.example"
  require_file "${ROOT_DIR}/infra/env/worker-transcription.env.example"
  require_file "${ROOT_DIR}/infra/env/worker-agent-runner.env.example"
  require_file "${ROOT_DIR}/infra/env/web.env.example"
  require_file "${ROOT_DIR}/infra/env/telegram-bot.env.example"
  require_file "${ROOT_DIR}/infra/env/mcp-server.env.example"
  require_file "${ROOT_DIR}/infra/init/minio/bootstrap-buckets.sh"
  require_file "${ROOT_DIR}/infra/images/worker-transcription/Dockerfile"
  require_file "${ROOT_DIR}/infra/images/worker-agent-runner/Dockerfile"

  for service in "${RUNTIME_SERVICES[@]}"; do
    require_service "${service}"
  done
  for service in "${FORBIDDEN_SERVICES[@]}"; do
    reject_service "${service}"
  done

  require_compose_snippet "- ./env/postgres.env.example"
  require_compose_snippet "- ./env/minio.env.example"
  require_compose_snippet "- ./env/shared.env.example"
  require_compose_snippet "- ./env/api.env.example"
  require_compose_snippet "- ./env/worker-transcription.env.example"
  require_compose_snippet "- ./env/worker-agent-runner.env.example"
  require_compose_snippet "- ./env/web.env.example"
  require_compose_snippet "- ./env/telegram-bot.env.example"
  require_compose_snippet "- ./env/mcp-server.env.example"
  reject_compose_snippet "- ./env/worker-report.env.example"
  reject_compose_snippet "- ./env/worker-deep-research.env.example"
  reject_compose_snippet "infra/images/worker-report/Dockerfile"
  reject_compose_snippet "infra/images/worker-deep-research/Dockerfile"

  require_compose_snippet "postgres-data:"
  require_compose_snippet "minio-data:"
  require_compose_snippet "whisper-model-cache:"
  require_compose_snippet "agent-runner-runtime:"
  require_compose_snippet "retained-log-volume:"
  reject_compose_snippet "report-and-deep-research-temp-space:"
  reject_compose_snippet "report-llm-cache:"
  reject_compose_snippet "report-llm-config:"
  reject_compose_snippet "deep-research-llm-cache:"
  reject_compose_snippet "deep-research-llm-config:"

  require_service_block_snippet "postgres" "healthcheck:"
  require_service_block_snippet "redis" "healthcheck:"
  require_service_block_snippet "minio" "healthcheck:"
  require_service_block_snippet "minio-init" "volumes:"
  require_service_block_snippet "minio-init" "./init/minio:/init:ro"
  require_service_block_snippet "minio-init" "/init/bootstrap-buckets.sh"
  require_service_block_snippet "worker-transcription" "dockerfile: infra/images/worker-transcription/Dockerfile"
  require_service_block_snippet "worker-transcription" "image: media-analysis-worker-transcription:local"
  require_service_block_snippet "worker-transcription" "PYTHONPATH: /workspace/workers/transcription/src:/workspace/workers/common/src"
  require_service_block_snippet "worker-transcription" "- transcriber_worker_transcription_main"
  require_service_block_snippet "worker-agent-runner" "dockerfile: infra/images/worker-agent-runner/Dockerfile"
  require_service_block_snippet "worker-agent-runner" "image: media-analysis-worker-agent-runner:local"
  require_service_block_snippet "worker-agent-runner" "PYTHONPATH: /workspace/workers/agent-runner/src:/workspace/workers/common/src"
  require_service_block_snippet "worker-agent-runner" "WORKER_WORKSPACE_ROOT: /tmp/runtime/agent-runner"
  require_service_block_snippet "worker-agent-runner" "./env/worker-agent-runner.env.example"
  require_service_block_snippet "worker-agent-runner" "- transcriber_worker_agent_runner_main"
  require_file_snippet "${ROOT_DIR}/infra/env/worker-agent-runner.env.example" "AGENT_RUNNER_HARNESS_CONCURRENCY=fixture=1,test_fixture=1,claude-code=1"
  require_file_snippet "${ROOT_DIR}/infra/env/worker-agent-runner.env.example" "AGENT_RUNNER_CLAUDE_CODE_PROVIDER_API_KEY_FILE="
  require_file_snippet "${ROOT_DIR}/infra/env/worker-agent-runner.env.example" "AGENT_RUNNER_CLAUDE_CODE_BASE_URL=https://api.z.ai/api/anthropic"
  require_service_block_snippet "web" '${WEB_HOST_PORT:-3201}:3201'
  reject_file_snippet "${ROOT_DIR}/workers/transcription/src/transcriber_worker_transcription.py" "_ensure_worker_dependency_paths"
  reject_file_snippet "${ROOT_DIR}/workers/transcription/src/transcriber_worker_transcription.py" "sys.path.insert"
  reject_file_snippet "${ROOT_DIR}/workers/transcription/src/transcriber_worker_transcription_main.py" "_ensure_worker_dependency_paths"
  reject_file_snippet "${ROOT_DIR}/workers/transcription/src/transcriber_worker_transcription_main.py" "sys.path.insert"
  reject_file_snippet "${ROOT_DIR}/workers/agent-runner/src/transcriber_worker_agent_runner_main.py" "_ensure_worker_dependency_paths"
  reject_file_snippet "${ROOT_DIR}/workers/agent-runner/src/transcriber_worker_agent_runner_main.py" "sys.path.insert"
  require_compose_snippet 'condition: service_healthy'
  require_compose_snippet 'condition: service_completed_successfully'
  require_compose_snippet 'driver: bridge'
}

run_check_config() {
  printf '%s validating compose config and topology scaffolding\n' "${MARKER}"

  validate_static_contract
  printf '%s compose topology scaffolding is internally consistent\n' "${MARKER}"
}

require_default_runtime_services_enabled() {
  local services="$1"
  local service

  for service in "${RUNTIME_SERVICES[@]}"; do
    grep -Fx -- "${service}" <<<"${services}" >/dev/null || fail \
      "default compose stack excludes runtime service ${service}; first divergent block is profile-gated or missing runtime wiring"
  done
}

require_materialized_runtime_service() {
  local service="$1"
  local rendered_compose="$2"
  local block

  block=$(service_block_from_content "${service}" "${rendered_compose}")
  [[ -n "${block}" ]] || fail "rendered compose config is missing runtime service block: ${service}"
  grep -F -- "image: busybox:1.36" <<<"${block}" >/dev/null && fail \
    "runtime service ${service} still uses the phase-1 busybox placeholder"
  grep -F -- "phase-1 placeholder runtime slot" <<<"${block}" >/dev/null && fail \
    "runtime service ${service} still uses the phase-1 placeholder command"
  return 0
}

run_live_smoke() {
  local default_services
  local rendered_compose
  local service

  printf '%s validating compose live-stack readiness\n' "${MARKER}"

  validate_static_contract

  default_services=$(docker compose -f "${COMPOSE_FILE}" config --services)
  require_default_runtime_services_enabled "${default_services}"

  rendered_compose=$(docker compose -f "${COMPOSE_FILE}" config)
  for service in "${RUNTIME_SERVICES[@]}"; do
    require_materialized_runtime_service "${service}" "${rendered_compose}"
  done

  printf '%s starting compose stack and waiting for health convergence\n' "${MARKER}"
  docker compose -f "${COMPOSE_FILE}" up -d --wait >/dev/null
  printf '%s compose live smoke completed successfully\n' "${MARKER}"
}

main() {
  case "${1:-}" in
    --check-config)
      run_check_config
      ;;
    --live-smoke)
      run_live_smoke
      ;;
    *)
      fail "unsupported mode: ${1:-<none>} (expected --check-config or --live-smoke)"
      ;;
  esac
}

main "$@"
