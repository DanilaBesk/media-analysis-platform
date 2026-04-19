# Telegram Transcriber Bot

Telegram-бот для расшифровки YouTube-ссылок и загруженных через Telegram аудио/видео. После транскрибации бот отдаёт документ с расшифровкой и умеет строить исследовательский отчёт через обязательный CLI `cglm`.

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
- по `Создать исследовательский отчет` вызывает `cglm` и отправляет `.docx` с исследовательским отчётом.

## Ограничения MVP

- Поддерживаемый URL-источник сейчас только YouTube.
- Явная diarization не обещается всегда. Speaker labels сохраняются только если их можно честно извлечь из исходного transcript. Иначе сегменты идут без фиктивных спикеров.
- Первый запуск Whisper fallback может скачать модель.

## Быстрый старт

1. Скопируйте `.env.example` в `.env`.
2. Укажите `TELEGRAM_BOT_TOKEN`.
3. Запустите:

```bash
uv run telegram-transcriber-bot
```

`uv` сам создаст окружение по `pyproject.toml`, если оно ещё не создано.

## Переменные окружения

- `TELEGRAM_BOT_TOKEN` — обязателен.
- `WHISPER_MODEL` — по умолчанию `turbo`.
- `WHISPER_DEVICE` — по умолчанию `auto`.
- `WHISPER_COMPUTE_TYPE` — по умолчанию `default`.
- `YOUTUBE_TRANSCRIPT_LANGUAGES` — по умолчанию `ru,en`.
- `MEDIA_GROUP_WINDOW_SECONDS` — по умолчанию `2.5`.
- `DATA_DIR` — по умолчанию `.data`.
- `REPORT_PROMPT_SUFFIX` — необязательное дополнение к prompt для `cglm`.

## Что должно быть в системе

- `uv`
- `cglm`
- `ffmpeg`

## Архитектура

- `src/telegram_transcriber_bot/bot.py` — aiogram runtime, inline keyboards, media group buffer, callback handlers.
- `src/telegram_transcriber_bot/source_extractor.py` — извлечение и нормализация источников из текста и вложений.
- `src/telegram_transcriber_bot/transcribers.py` — YouTube subtitles fast-path и Whisper fallback.
- `src/telegram_transcriber_bot/service.py` — job/artifact layer, транскрибация и отчёты.
- `src/telegram_transcriber_bot/cglm_runner.py` — безопасный subprocess-адаптер для `cglm`.
- `src/telegram_transcriber_bot/documents.py` — генерация `.md` и `.docx`.

Артефакты пишутся в `DATA_DIR/jobs/<job_id>/`.

## Тесты

```bash
.venv/bin/pytest
```

Покрыты:

- extraction links и media attachments;
- unsupported URL filtering;
- media group accumulation;
- transcript/report document rendering;
- `cglm` command builder;
- processing service и идемпотентность report generation.
