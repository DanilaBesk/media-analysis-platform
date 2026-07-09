#!/usr/bin/env bash
set -euo pipefail

MARKER='[TelegramLocalBotApi][preflight]'
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/infra/docker-compose.yml"
ENV_FILE="${ROOT_DIR}/.env"
MODE="config"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--runtime]

Checks local Telegram Bot API cutover prerequisites without printing secret values.

Options:
  --runtime   Also verify the currently running compose containers use local Bot API mode.
USAGE
}

while (($#)); do
  case "$1" in
    --runtime)
      MODE="runtime"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "${MARKER} unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

failures=0
compose_ready=0

fail() {
  echo "${MARKER} ERROR: $*" >&2
  failures=1
}

ok() {
  echo "${MARKER} OK: $*"
}

read_env_value() {
  local key="$1"
  local value="${!key:-}"
  if [[ -n "${value}" ]]; then
    printf '%s' "${value}"
    return
  fi
  if [[ -f "${ENV_FILE}" ]]; then
    awk -F= -v key="${key}" '$1 == key {print substr($0, index($0, "=") + 1)}' "${ENV_FILE}" | tail -n 1
  fi
}

require_secret() {
  local key="$1"
  local value
  value="$(read_env_value "${key}")"
  if [[ -n "${value}" ]]; then
    ok "${key}=SET"
  else
    fail "${key}=UNSET; set it in .env before starting the local Telegram Bot API runtime"
  fi
}

require_secret "TELEGRAM_BOT_TOKEN"
require_secret "TELEGRAM_API_ID"
require_secret "TELEGRAM_API_HASH"

if ! command -v docker >/dev/null; then
  fail "docker is not available"
elif ! docker compose -f "${COMPOSE_FILE}" config >/dev/null; then
  fail "docker compose config is invalid"
else
  compose_ready=1
  services="$(docker compose -f "${COMPOSE_FILE}" config --services)"
  if grep -qx "telegram-bot-api" <<<"${services}"; then
    ok "compose service telegram-bot-api is present"
  else
    fail "compose service telegram-bot-api is missing"
  fi
  if grep -qx "telegram-bot" <<<"${services}"; then
    ok "compose service telegram-bot is present"
  else
    fail "compose service telegram-bot is missing"
  fi
fi

if [[ "${MODE}" == "runtime" && ${compose_ready} -eq 1 ]]; then
  bot_api_container="$(docker compose -f "${COMPOSE_FILE}" ps -q telegram-bot-api || true)"
  bot_container="$(docker compose -f "${COMPOSE_FILE}" ps -q telegram-bot || true)"

  if [[ -z "${bot_api_container}" ]]; then
    fail "telegram-bot-api container is not running; run docker compose -f infra/docker-compose.yml up -d --build --force-recreate --wait telegram-bot-api telegram-bot"
  else
    bot_api_health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${bot_api_container}" 2>/dev/null || true)"
    if [[ "${bot_api_health}" == "healthy" ]]; then
      ok "telegram-bot-api container is healthy"
    else
      fail "telegram-bot-api health is ${bot_api_health:-unknown}"
    fi
  fi

  if [[ -z "${bot_container}" ]]; then
    fail "telegram-bot container is not running"
  else
    bot_env="$(docker inspect "${bot_container}" --format '{{range .Config.Env}}{{println .}}{{end}}')"
    if grep -qx "TELEGRAM_BOT_API_BASE_URL=http://telegram-bot-api:8081" <<<"${bot_env}"; then
      ok "telegram-bot uses TELEGRAM_BOT_API_BASE_URL=http://telegram-bot-api:8081"
    else
      fail "telegram-bot container was not recreated with local TELEGRAM_BOT_API_BASE_URL"
    fi
    if grep -qx "TELEGRAM_BOT_API_IS_LOCAL=true" <<<"${bot_env}"; then
      ok "telegram-bot uses TELEGRAM_BOT_API_IS_LOCAL=true"
    else
      fail "telegram-bot container was not recreated with TELEGRAM_BOT_API_IS_LOCAL=true"
    fi
    bot_logs="$(docker logs "${bot_container}" 2>/dev/null || true)"
    if grep -q "telegram_bot_api_mode=local" <<<"${bot_logs}"; then
      ok "telegram-bot logs show telegram_bot_api_mode=local"
    else
      ok "telegram-bot runtime env confirms local mode; startup log marker is unavailable"
    fi
  fi
elif [[ "${MODE}" == "runtime" ]]; then
  fail "runtime container checks skipped because docker compose config is not ready"
fi

if [[ ${failures} -ne 0 ]]; then
  echo "${MARKER} local Telegram Bot API cutover is not ready" >&2
  exit 1
fi

ok "local Telegram Bot API cutover checks passed"
