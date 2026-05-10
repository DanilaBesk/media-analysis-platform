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
