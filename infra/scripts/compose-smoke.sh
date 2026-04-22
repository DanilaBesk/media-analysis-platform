#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
COMPOSE_FILE="${ROOT_DIR}/infra/docker-compose.yml"
MARKER='[InfraCompose][verifyLocalStack][BLOCK_VERIFY_LOCAL_STACK_HEALTH]'

fail() {
  printf '%s %s\n' "${MARKER}" "$1" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "${path}" ]] || fail "missing required file: ${path#${ROOT_DIR}/}"
}

require_compose_snippet() {
  local snippet="$1"
  grep -F -- "${snippet}" "${COMPOSE_FILE}" >/dev/null || fail "missing compose snippet: ${snippet}"
}

require_service() {
  local service="$1"
  grep -Eq "^  ${service}:" "${COMPOSE_FILE}" || fail "missing service definition: ${service}"
}

require_service_block_snippet() {
  local service="$1"
  local snippet="$2"
  local block

  block=$(sed -n "/^  ${service}:/,/^  [a-z0-9][a-z0-9-]*:/p" "${COMPOSE_FILE}" | sed '$d')
  grep -F -- "${snippet}" <<<"${block}" >/dev/null || fail "service ${service} is missing snippet: ${snippet}"
}

run_check_config() {
  printf '%s validating compose config and topology scaffolding\n' "${MARKER}"

  command -v docker >/dev/null || fail "docker is required for compose validation"

  require_file "${COMPOSE_FILE}"
  docker compose -f "${COMPOSE_FILE}" config >/dev/null

  require_file "${ROOT_DIR}/infra/env/postgres.env.example"
  require_file "${ROOT_DIR}/infra/env/minio.env.example"
  require_file "${ROOT_DIR}/infra/env/shared.env.example"
  require_file "${ROOT_DIR}/infra/env/api.env.example"
  require_file "${ROOT_DIR}/infra/env/worker-transcription.env.example"
  require_file "${ROOT_DIR}/infra/env/worker-report.env.example"
  require_file "${ROOT_DIR}/infra/env/worker-deep-research.env.example"
  require_file "${ROOT_DIR}/infra/env/web.env.example"
  require_file "${ROOT_DIR}/infra/env/telegram-bot.env.example"
  require_file "${ROOT_DIR}/infra/env/mcp-server.env.example"

  for service in \
    postgres \
    redis \
    minio \
    minio-init \
    api \
    worker-transcription \
    worker-report \
    worker-deep-research \
    web \
    telegram-bot \
    mcp-server
  do
    require_service "${service}"
  done

  require_compose_snippet "- ./env/postgres.env.example"
  require_compose_snippet "- ./env/minio.env.example"
  require_compose_snippet "- ./env/shared.env.example"
  require_compose_snippet "- ./env/api.env.example"
  require_compose_snippet "- ./env/worker-transcription.env.example"
  require_compose_snippet "- ./env/worker-report.env.example"
  require_compose_snippet "- ./env/worker-deep-research.env.example"
  require_compose_snippet "- ./env/web.env.example"
  require_compose_snippet "- ./env/telegram-bot.env.example"
  require_compose_snippet "- ./env/mcp-server.env.example"

  require_compose_snippet "postgres-data:"
  require_compose_snippet "minio-data:"
  require_compose_snippet "whisper-model-cache:"
  require_compose_snippet "report-and-deep-research-temp-space:"
  require_compose_snippet "retained-log-volume:"

  require_service_block_snippet "postgres" "healthcheck:"
  require_service_block_snippet "redis" "healthcheck:"
  require_service_block_snippet "minio" "healthcheck:"
  require_compose_snippet "x-placeholder-service:"
  require_compose_snippet "test -f /tmp/service-ready"
  require_compose_snippet "x-runtime-infra-depends:"
  require_compose_snippet "minio-init:"
  require_service_block_snippet "api" "<<: *placeholder-service"
  require_service_block_snippet "worker-transcription" "<<: *placeholder-service"
  require_service_block_snippet "worker-report" "<<: *placeholder-service"
  require_service_block_snippet "worker-deep-research" "<<: *placeholder-service"
  require_service_block_snippet "web" "<<: *placeholder-service"
  require_service_block_snippet "telegram-bot" "<<: *placeholder-service"
  require_service_block_snippet "mcp-server" "<<: *placeholder-service"

  require_service_block_snippet "api" "depends_on: *runtime-infra-depends"
  require_service_block_snippet "worker-transcription" "api:"
  require_service_block_snippet "worker-report" "api:"
  require_service_block_snippet "worker-deep-research" "api:"
  require_service_block_snippet "web" "api:"
  require_service_block_snippet "telegram-bot" "api:"
  require_service_block_snippet "mcp-server" "api:"

  require_compose_snippet 'condition: service_healthy'
  require_compose_snippet 'condition: service_completed_successfully'
  require_compose_snippet 'future-runtime'
  require_compose_snippet 'driver: bridge'

  printf '%s compose topology scaffolding is internally consistent\n' "${MARKER}"
}

main() {
  case "${1:-}" in
    --check-config)
      run_check_config
      ;;
    *)
      fail "unsupported mode: ${1:-<none>} (expected --check-config)"
      ;;
  esac
}

main "$@"
