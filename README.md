# Media Analysis Platform

Local-first платформа для сборки источников, транскрибации медиа/ссылок, генерации отчетов и deep research через API-owned control plane и worker stack. Telegram теперь только один из thin adapters: он собирает пользовательский ввод и передает операции в API, но не владеет бизнес-логикой.

## Что умеет

- принимает через API и адаптеры:
  - одну YouTube-ссылку;
  - текст с одной или несколькими ссылками;
  - аудио/видео/voice/video note/document с media mime type;
  - batch/source set из нескольких файлов и ссылок;
- сохраняет границы источников и stable source labels в итоговых artifact metadata;
- извлекает все поддерживаемые источники и даёт adapter-level выбор, если кандидатов несколько;
- для YouTube сначала пытается взять готовые субтитры;
- если субтитров нет, либо источник пришёл как файл, делает транскрибацию через `faster-whisper`;
- Telegram adapter после завершения показывает кнопки:
  - `Получить`
  - `Создать исследовательский отчет`
- по `Получить` отдаёт `.docx` с транскрибацией;
- по `Создать исследовательский отчет` создаёт API-owned report/deep-research цепочку через `agent_run` / `worker-agent-runner`.

## Ограничения MVP

- Поддерживаемый URL-источник сейчас сфокусирован на YouTube.
- Явная diarization не обещается всегда. Speaker labels сохраняются только если их можно честно извлечь из исходного transcript. Иначе сегменты идут без фиктивных спикеров.
- Первый запуск Whisper fallback может скачать модель.

## Быстрый старт

Локальный runtime теперь считается cutover-complete только через Docker Compose.

1. Выполните статический preflight:

```bash
bash infra/scripts/compose-smoke.sh --check-config
```

2. Поднимите compose stack:

```bash
docker compose -f infra/docker-compose.yml up -d --build --wait
```

3. Для deterministic smoke используйте `worker-agent-runner` fixture/test-fixture lanes из `infra/env/worker-agent-runner.env.example`. Report/deep-research AI execution больше не запускается отдельными LLM worker services.

4. Для точного cutover/runbook follow-up используйте:

```bash
docs/architecture/cutover-checklist.md
docs/architecture/runtime-ops.md
```

`uv run media-analysis-platform` больше не запускает прежний локальный runtime и теперь только указывает на compose cutover path.

## Переменные окружения

- `TELEGRAM_BOT_TOKEN` — обязателен.
- `WHISPER_MODEL` — по умолчанию `turbo`.
- `WHISPER_DEVICE` — по умолчанию `auto`.
- `WHISPER_COMPUTE_TYPE` — по умолчанию `default`.
- `YOUTUBE_TRANSCRIPT_LANGUAGES` — по умолчанию `ru,en`.
- `MEDIA_GROUP_WINDOW_SECONDS` — по умолчанию `2.5`.
- `DATA_DIR` — по умолчанию `.data`.
- `REPORT_PROMPT_SUFFIX` — необязательное дополнение к prompt для report harness.

## Что должно быть в системе

- `uv`
- `docker compose`
- `ffmpeg`

## Архитектура

- `apps/api` — Go control plane для jobs, source/source_set lineage, artifacts, retry/cancel/progress и child operations.
- `src/media_analysis_platform/bot.py` — legacy Telegram adapter runtime, inline keyboards, media group buffer, callback handlers.
- `apps/telegram-bot/src/telegram_adapter` — compose-owned Telegram adapter boundary over the API.
- `workers/transcription/src/transcriber_worker_transcription.py` — transcription worker runtime, local source materialization, combined input handling, and transcript artifact persistence.
- `workers/common/src/transcriber_workers_common/transcribers.py` — shared YouTube/subtitles/Whisper helpers consumed by the transcription worker.
- `workers/agent-runner/src` — единый AI-model worker runtime для report/deep-research execution.
- `workers/common/src/transcriber_workers_common/documents.py` — генерация transcript/report `.md` и `.docx`.

Финальные source/artifact объекты хранятся через API-owned PostgreSQL + MinIO boundary; worker workspace остаётся execution-local.

## Тесты

```bash
.venv/bin/pytest
```

Покрыты:

- extraction links и media attachments;
- unsupported URL filtering;
- media group accumulation;
- transcript/report document rendering;
- worker-local source materialization и transcript artifact generation;
- thin adapter callbacks и artifact delivery.
