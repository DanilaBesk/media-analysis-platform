# Telegram Transcriber Bot

Telegram-бот для расшифровки YouTube-ссылок и загруженных через Telegram аудио/видео. После транскрибации бот отдаёт документ с расшифровкой и умеет строить исследовательский отчёт через настроенный LLM provider.

## Что умеет

- принимает:
  - одну YouTube-ссылку;
  - текст с одной или несколькими ссылками;
  - аудио/видео/voice/video note/document с media mime type из Telegram;
  - media group из нескольких файлов;
- извлекает все поддерживаемые источники и даёт выбор через inline keyboard, если кандидатов несколько;
- для YouTube сначала пытается взять готовые субтитры;
- если субтитров нет, либо источник пришёл как файл, делает транскрибацию через `faster-whisper`;
- после завершения показывает кнопки:
  - `Получить`
  - `Создать исследовательский отчет`
- по `Получить` отдаёт `.docx` с транскрибацией;
- по `Создать исследовательский отчет` создаёт дочернюю report/deep-research цепочку и отправляет `.docx` с исследовательским отчётом.

## Ограничения MVP

- Поддерживаемый URL-источник сейчас только YouTube.
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

`uv run telegram-transcriber-bot` больше не запускает прежний локальный runtime и теперь только указывает на compose cutover path.

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

- `src/telegram_transcriber_bot/bot.py` — aiogram runtime, inline keyboards, media group buffer, callback handlers.
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
- thin Telegram adapter callbacks и artifact delivery.
