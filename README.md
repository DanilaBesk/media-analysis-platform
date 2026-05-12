# Media Analysis Platform

Local-first платформа для накопления медиа в API-owned inbox, сборки явных selections и запуска analysis runs через worker stack. Telegram, Web и MCP являются thin adapters: они управляют одним и тем же серверным состоянием, но не владеют бизнес-логикой.

## Что умеет

- принимает через API и адаптеры media items:
  - одну YouTube-ссылку;
  - текст с одной или несколькими ссылками;
  - аудио/видео/voice/video note/document с media mime type;
- хранит media items в inbox collection до явного запуска обработки;
- создает immutable selection snapshot из выбранных media items;
- запускает analysis run по selection и публикует artifacts/diagnostics через API;
- сохраняет границы источников и stable source labels в artifact metadata;
- дает адаптерам единый restore/status flow после перезапуска.

## Ограничения MVP

- Поддерживаемый URL-источник сейчас сфокусирован на YouTube.
- Явная diarization не обещается всегда. Speaker labels сохраняются только если их можно честно извлечь из исходного transcript. Иначе сегменты идут без фиктивных спикеров.
- Первый запуск transcription worker может скачать модель.

## Быстрый старт

Локальный runtime запускается через Docker Compose.

1. Выполните статический preflight:

```bash
bash infra/scripts/compose-smoke.sh --check-config
```

2. Поднимите compose stack:

```bash
docker compose -f infra/docker-compose.yml up -d --build --wait
```

3. Для deterministic smoke используйте `worker-agent-runner` fixture/test-fixture lanes из `infra/env/worker-agent-runner.env.example`. Report/deep-research AI execution больше не запускается отдельными LLM worker services.

4. Для runtime/runbook follow-up используйте:

```bash
docs/architecture/cutover-checklist.md
docs/architecture/runtime-ops.md
```

Root package entrypoint intentionally does not exist. Runtime code lives under `apps/*`, `workers/*`, and `packages/*`.

## Переменные окружения

- `API_BASE_URL` — HTTP endpoint API для thin adapters.
- `TELEGRAM_BOT_TOKEN` — обязателен для Telegram adapter.
- `ALLOWED_USER_IDS` — optional allow-list для Telegram adapter.
- worker/model/storage переменные задаются package-local env examples и compose files.

## Что должно быть в системе

- `uv`
- `docker compose`
- `ffmpeg`

## Архитектура

- `apps/api` — Go control plane for media items, collections, selections, analysis runs, artifacts, diagnostics, retry/cancel/progress, and adapter restore state.
- `apps/telegram-bot/src/telegram_adapter` — compose-owned Telegram adapter over the API.
- `apps/web` — Web UI over the same media and run APIs.
- `apps/mcp-server` — MCP adapter over the same media and run APIs.
- `packages/contracts` — OpenAPI and JSON schema contracts for public, internal, webhook, and websocket surfaces.
- `workers/transcription/src` — transcription runtime, local source materialization, and transcript artifact persistence.
- `workers/agent-runner/src` — AI-model runtime for report/deep-research execution.
- `workers/common/src/transcriber_workers_common` — shared worker helpers for API transport, artifacts, source materialization, transcription, and document rendering.

Final media, selection, run, artifact, and diagnostic state is owned by API PostgreSQL + MinIO boundaries; worker workspace remains execution-local.

## Тесты

```bash
PYTHONPATH=apps/telegram-bot/src uv run pytest apps/telegram-bot/tests
PYTHONPATH=workers/common/src uv run pytest workers/common/tests
PYTHONPATH=workers/common/src:workers/transcription/src uv run pytest workers/transcription/tests
PYTHONPATH=workers/common/src:workers/agent-runner/src uv run pytest workers/agent-runner/tests
```

Покрыты:

- media item ingestion and restore flows;
- explicit rejected-record diagnostics for unsupported inputs;
- selection creation and analysis run launch;
- worker-local source materialization and transcript artifact generation;
- thin adapter callbacks, paging, removal, and artifact/diagnostic display.

## Coverage Snapshot

Current executable coverage inventory is collected by:

```bash
bash infra/scripts/coverage-inventory.sh
```

Measured baselines from the current tree:

- Go `apps/api/internal/api`: `100%` statement coverage.
- Go `apps/api/internal/storage`: `100%` statement coverage.
- Python `workers/common/src/transcriber_workers_common`: `100%` line coverage.
- Python `workers/agent-runner/src/transcriber_worker_agent_runner.py`: `100%` line coverage.
- Python `workers/transcription/src/transcriber_worker_transcription.py`: `100%` line coverage.
- Python `apps/telegram-bot/src/telegram_adapter`: `100%` aggregate line coverage.
- Node `apps/mcp-server/src`: `100%` line, branch, and function coverage from `node --test --experimental-test-coverage`.
- Web `apps/web/src`: `100%` line, branch, and function coverage from `vitest run --coverage`.

Current truth:

- All declared measurable coverage surfaces now emit `100%`.
- Contract, XML, and runtime-final e2e remain separate acceptance gates and are not folded into a fake percentage.

## Final Verification

The canonical ordered closure runbook lives here:

```bash
docs/architecture/final-closure-matrix.md
```

## Executable coverage inventory

The repo still measures coverage per surface with the metric each tool can actually emit:

- Go API packages: statement coverage from focused `go test -cover` commands.
- Python adapter and worker packages: line coverage from isolated `pytest-cov` runs.
- MCP server: line, branch, and function coverage from Node's built-in test coverage.
- Web UI: line, branch, and function coverage from `vitest run --coverage` with the V8 provider.
- Compose/runtime smoke and XML validation remain acceptance gates, not percentage coverage gates.

Run the inventory with:

```bash
bash infra/scripts/coverage-inventory.sh
```

The command is intentionally a gate, not a vanity report: it exits non-zero while any declared surface lacks a real metric, falls below `100%`, or any runtime/adapter probe fails.

Current baseline snapshot from `2026-05-11`:

| Surface | Metric | Current baseline | Status |
| --- | --- | --- | --- |
| `apps/api/internal/storage` | statement coverage | `100%` | measured |
| `apps/api/internal/api` | statement coverage | `100%` | measured |
| `apps/telegram-bot/src/telegram_adapter` | line coverage | `100%` | measured |
| `workers/common/src/transcriber_workers_common` | line coverage | `100%` | measured |
| `workers/transcription/src` | line coverage | `100%` | measured |
| `workers/agent-runner/src` | line coverage | `100%` | measured |
| `apps/mcp-server/src` | line / branch / function coverage | `100% / 100% / 100%` | measured |
| `apps/web/src` | line / branch / function coverage | `100% / 100% / 100%` | measured |

The repo can now truthfully claim full measured coverage closure for all declared percentage-emitting surfaces, while still treating contracts, XML integrity, and runtime-final e2e as separate acceptance gates.

## Telegram Ops

When diagnosing Telegram adapter incidents, distinguish external polling degradation from internal product failures:

- External Telegram upstream flaps are logged with `[TelegramAdapter][bot][BLOCK_TRACK_TELEGRAM_POLLING_STATE] classification=telegram_upstream_failure` and later `classification=telegram_upstream_recovered`.
- Internal message/callback/status handler failures are logged with `[TelegramAdapter][bot][BLOCK_HANDLE_TELEGRAM_HANDLER_ERROR]` plus normalized error metadata.
- Generic user-facing outage copy must not be treated as proof of a platform outage by itself; check the structured log marker first.

Observability endpoint semantics:

- `/v1/admin/observability` exposes cumulative counters like `artifact_resolution_failures` plus recent-window counters like `artifact_resolution_failures_recent`.
- `observability_window_seconds` defines the current-window size used by the recent counters.
- Use the recent counters to judge active breakage; use the cumulative counters to understand historical residue.

Artifact object-key semantics:

- New worker-generated artifact `object_key` values are artifact-bucket-relative, for example `<analysis_run_id>/agent/result/result.json`.
- Legacy rows that still store `artifacts/...` keys remain readable for compatibility.
