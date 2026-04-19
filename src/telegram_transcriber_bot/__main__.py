from __future__ import annotations

import asyncio
import logging

from telegram_transcriber_bot.bot import TelegramTranscriberApp
from telegram_transcriber_bot.config import load_settings
from telegram_transcriber_bot.service import ProcessingService
from telegram_transcriber_bot.transcribers import DefaultTranscriber

LOGGER = logging.getLogger(__name__)


async def _run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings()
    transcriber = DefaultTranscriber(
        youtube_languages=settings.youtube_languages,
        whisper_model=settings.whisper_model,
        whisper_device=settings.whisper_device,
        whisper_compute_type=settings.whisper_compute_type,
    )
    processing_service = ProcessingService(storage_dir=settings.data_dir, transcriber=transcriber)
    app = TelegramTranscriberApp(settings=settings, processing_service=processing_service)
    LOGGER.info("telegram-transcriber-bot started; data_dir=%s", settings.data_dir)
    await app.run()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
