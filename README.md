# Media Analysis Platform

Local-first платформа для накопления media_assets в API-owned inbox, сборки immutable selection_snapshots и запуска analysis_runs через worker stack. Telegram, Web и MCP являются thin adapters: они управляют одним и тем же серверным состоянием, но не владеют бизнес-логикой.

## Что умеет

- принимает через API и адаптеры media_assets:
  - одну YouTube-ссылку;
  - текст с одной или несколькими ссылками;
  - аудио/видео/voice/video note/document с media mime type;
- хранит media_assets в inbox collection до явного запуска обработки;
- создает immutable selection_snapshot из выбранных media_assets;
- запускает analysis_run по selection_snapshot и публикует artifacts/diagnostics через API;
- сохраняет границы origin и stable origin labels в artifact metadata;
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

## CopperASR submodule

CopperASR is consumed as a pinned source submodule for the ASR migration plan in
`docs/plans/2026-05-18-copper-asr-submodule-migration.md`.

Bootstrap after cloning or switching branches:

```bash
git submodule update --init --recursive
git -C vendor/copper-asr rev-parse HEAD
```

Current pin:

```text
remote https://copperside.gitlab.yandexcloud.net/clara/copper-asr.git
vendor/copper-asr f2a8278fb236b2ba471083ca2debcc3e9052cd64
```

Local install path for CopperASR development:

```bash
python -m pip install -e "vendor/copper-asr[server,cpu]"
```

Container build path for the dedicated runtime source:

```bash
docker build -f vendor/copper-asr/Dockerfile vendor/copper-asr
```

The active migration target is a dedicated CopperASR runtime service consumed
by the transcription worker. Do not add a Whisper/faster-whisper fallback while
working through `media-b8s.*`.

## Архитектура

- `apps/api` — Go control plane for media_assets, collections, selection_snapshots, analysis_runs, artifacts, diagnostics, retry/cancel/progress, and channel surface recovery state.
- `apps/telegram-bot/src/telegram_adapter` — compose-owned Telegram adapter over the API.
- `apps/web` — Web UI over the same media and run APIs.
- `apps/mcp-server` — MCP adapter over the same media and run APIs.
- `packages/contracts` — OpenAPI and JSON schema contracts for public, internal, webhook, and websocket surfaces.
- `workers/transcription/src` — transcription runtime, local input materialization, and transcript artifact persistence.
- `workers/agent-runner/src` — AI-model runtime for report/deep-research execution.
- `workers/common/src/transcriber_workers_common` — shared worker helpers for API transport, artifacts, input materialization, transcription, and document rendering.
- `vendor/copper-asr` — pinned CopperASR source submodule for the dedicated ASR runtime service.

Final media_asset, selection_snapshot, analysis_run, artifact, and diagnostic state is owned by API PostgreSQL + MinIO boundaries; worker workspace remains execution-local.

## Тесты

```bash
PYTHONPATH=apps/telegram-bot/src uv run pytest apps/telegram-bot/tests
PYTHONPATH=workers/common/src uv run pytest workers/common/tests
PYTHONPATH=workers/common/src:workers/transcription/src uv run pytest workers/transcription/tests
PYTHONPATH=workers/common/src:workers/agent-runner/src uv run pytest workers/agent-runner/tests
```

Покрыты:

- media_asset ingestion and restore flows;
- explicit rejected-record diagnostics for unsupported inputs;
- selection_snapshot creation and analysis_run launch;
- worker-local input materialization and transcript artifact generation;
- thin adapter callbacks, paging, removal, and artifact/diagnostic display.

## Coverage Snapshot

Target rebuild coverage is tracked in:

```bash
docs/architecture/target-coverage-matrix.md
```

That matrix maps source-plan requirements to implementation evidence, current tests, deterministic fixtures, and the remaining QA Beads that must challenge matrix rows before final readiness. It is the traceability artifact for `media-7f3.10`; percentage coverage alone is not target rebuild closure.

Current executable percentage inventory is collected by:

```bash
bash infra/scripts/coverage-inventory.sh
```

Measured baselines from the latest inventory run recorded in the current tree:

- Go `apps/api/internal/api`: `67.0%` statement coverage.
- Go `apps/api/internal/storage`: `98.5%` statement coverage.
- Python `workers/common/src/transcriber_workers_common`: `99%` statement coverage.
- Python `workers/agent-runner/src/transcriber_worker_agent_runner.py`: `97%` statement coverage.
- Python `workers/transcription/src/transcriber_worker_transcription.py`: `99%` statement coverage.
- Python `apps/telegram-bot/src/telegram_adapter`: `89%` statement coverage.
- Node `apps/mcp-server/src`: `100%` line, branch, and function coverage from `node --test --experimental-test-coverage`.
- Web `apps/web/src`: `97.86%` statements/lines, `91.86%` branches, and `100%` functions from `vitest run --coverage`.

Current truth:

- Percentage-emitting surfaces are measured per tool through `infra/scripts/coverage-inventory.sh`.
- Contract, XML, fixture, target reset, stale vocabulary, and runtime-final E2E gates remain separate acceptance evidence.
- The coverage epic `media-7f3.10` is closed with committed proof; the target rebuild is not fully closed until `media-7f3.11` completes against the coverage matrix and source plan.

## Final Verification

The canonical ordered closure runbook lives here:

```bash
docs/architecture/final-closure-matrix.md
```

## Executable coverage inventory

The repo measures coverage per surface with the metric each tool can actually emit:

- Go API packages: statement coverage from focused `go test -cover` commands.
- Python adapter and worker packages: line coverage from isolated `pytest-cov` runs.
- MCP server: line, branch, and function coverage from Node's built-in test coverage.
- Web UI: line, branch, and function coverage from `vitest run --coverage` with the V8 provider.
- Compose/runtime smoke and XML validation remain acceptance gates, not percentage coverage gates.

Run the inventory with:

```bash
bash infra/scripts/coverage-inventory.sh
```

The command is intentionally a gate, not a vanity report: it exits non-zero when a declared command or pass/fail-only probe fails. Numeric percentages are interpreted per surface and must not be collapsed into a fake repo-wide `100%` claim.

The deterministic target test environment starts with:

- `infra/fixtures/target/manifest.json` for stable channel accounts, media assets, object-store objects, selection snapshot, run, and artifact ids.
- `infra/scripts/target-reset-smoke.sh` for a fresh Postgres schema reset smoke.
- `packages/contracts/tests/test_target_fixtures.py` for fixture hash and vocabulary validation.

Latest measured percentage baselines should be refreshed by running the inventory, not copied from older closure notes:

| Surface | Metric | Current baseline | Status |
| --- | --- | --- | --- |
| `apps/api/internal/storage` | statement coverage | `98.5%` | measured |
| `apps/api/internal/api` | statement coverage | `67.0%` | measured |
| `apps/telegram-bot/src/telegram_adapter` | statement coverage | `89%` | measured |
| `workers/common/src/transcriber_workers_common` | statement coverage | `99%` | measured |
| `workers/transcription/src` | statement coverage | `99%` | measured |
| `workers/agent-runner/src` | statement coverage | `97%` | measured |
| `apps/mcp-server/src` | line / branch / function coverage | `100% / 100% / 100%` | measured |
| `apps/web/src` | statements / branches / functions / lines | `97.86% / 91.86% / 100% / 97.86%` | measured |

Do not claim full target closure from this table by itself. Use the target coverage matrix plus the ordered gates in `docs/architecture/final-closure-matrix.md`.

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
