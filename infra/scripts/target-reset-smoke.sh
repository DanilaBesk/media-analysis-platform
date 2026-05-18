#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
MIGRATION="${ROOT_DIR}/apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql"

REQUIRED_TABLES=(
  channel_accounts
  operation_requests
  stored_objects
  media_assets
  collections
  collection_items
  selection_snapshots
  selection_snapshot_items
  analysis_runs
  analysis_run_steps
  analysis_run_step_inputs
  analysis_run_events
  artifacts
  artifact_subjects
  diagnostics
  channel_surfaces
  channel_surface_subjects
  channel_surface_events
)

FORBIDDEN_TABLES=(
  sources
  media_items
  selections
  selection_items
  analysis_run_tasks
  owners
  workspaces
  workspace_members
)

CONTAINER_NAME=""

cleanup() {
  if [[ -n "${CONTAINER_NAME}" ]]; then
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

run_psql() {
  if [[ -n "${TARGET_DATABASE_URL:-}" ]]; then
    psql "${TARGET_DATABASE_URL}" -v ON_ERROR_STOP=1 "$@"
  else
    docker exec -i "${CONTAINER_NAME}" psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
  fi
}

run_migration_up() {
  {
    echo "SET client_min_messages TO warning;"
    awk '/^-- \+goose Down/{exit} {print}' "${MIGRATION}"
  } | run_psql >/dev/null
}

if [[ -z "${TARGET_DATABASE_URL:-}" ]]; then
  CONTAINER_NAME="map-target-reset-${RANDOM}-$$"
  docker run --rm -d --name "${CONTAINER_NAME}" -e POSTGRES_PASSWORD=postgres postgres:16-alpine >/dev/null
  until docker exec "${CONTAINER_NAME}" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do
    sleep 1
  done
fi

run_migration_up
run_migration_up

for table in "${REQUIRED_TABLES[@]}"; do
  exists=$(run_psql -Atqc "SELECT to_regclass('public.${table}') IS NOT NULL")
  if [[ "${exists}" != "t" ]]; then
    echo "[target-reset-smoke] missing required table: ${table}" >&2
    exit 1
  fi
done

for table in "${FORBIDDEN_TABLES[@]}"; do
  exists=$(run_psql -Atqc "SELECT to_regclass('public.${table}') IS NOT NULL")
  if [[ "${exists}" != "f" ]]; then
    echo "[target-reset-smoke] forbidden legacy table exists: ${table}" >&2
    exit 1
  fi
done

trigger_count=$(run_psql -Atqc "SELECT count(*) FROM pg_trigger WHERE tgname IN ('selection_snapshots_immutable_update_trg', 'selection_snapshots_immutable_delete_trg')")
if [[ "${trigger_count}" != "2" ]]; then
  echo "[target-reset-smoke] immutable selection_snapshot triggers missing" >&2
  exit 1
fi

run_psql >/dev/null <<'SQL'
INSERT INTO channel_accounts (
  id, channel, external_account_ref, display_name, status, metadata, created_at, updated_at
) VALUES (
  '00000000-0000-4000-8000-000000000001',
  'local',
  'single-user',
  'Single user',
  'active',
  '{"seed":"target-reset-smoke"}',
  '2026-05-18T00:00:00Z',
  '2026-05-18T00:00:00Z'
);

INSERT INTO collections (
  id, channel_account_id, kind, name, status, version, created_at, updated_at
) VALUES (
  '00000000-0000-4000-8000-000000000101',
  '00000000-0000-4000-8000-000000000001',
  'inbox',
  'Inbox',
  'active',
  1,
  '2026-05-18T00:00:00Z',
  '2026-05-18T00:00:00Z'
);
SQL

seed_count=$(run_psql -Atqc "SELECT count(*) FROM channel_accounts ca JOIN collections c ON c.channel_account_id = ca.id WHERE ca.id = '00000000-0000-4000-8000-000000000001' AND c.kind = 'inbox'")
if [[ "${seed_count}" != "1" ]]; then
  echo "[target-reset-smoke] deterministic seed channel_account/inbox not visible" >&2
  exit 1
fi

echo "[target-reset-smoke] target schema reset and deterministic seed smoke passed"
