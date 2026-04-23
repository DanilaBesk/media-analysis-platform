from __future__ import annotations

import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

import telegram_transcriber_bot.__main__ as main_module


@pytest.mark.asyncio
async def test_run_wires_settings_transcriber_service_and_app(monkeypatch) -> None:
    created = {}
    settings = SimpleNamespace(
        youtube_languages=("ru", "en"),
        whisper_model="turbo",
        whisper_device="auto",
        whisper_compute_type="default",
        data_dir="/tmp/data",
    )

    class FakeApp:
        def __init__(self, settings, processing_service):
            created["app"] = (settings, processing_service)

        async def run(self) -> None:
            created["ran"] = True

    monkeypatch.setattr(main_module, "load_settings", lambda: settings)
    monkeypatch.setattr(
        main_module,
        "DefaultTranscriber",
        lambda **kwargs: created.setdefault("transcriber", kwargs) or object(),
    )
    monkeypatch.setattr(
        main_module,
        "ProcessingService",
        lambda storage_dir, transcriber: created.setdefault("service", (storage_dir, transcriber)) or object(),
    )
    monkeypatch.setattr(main_module, "TelegramTranscriberApp", FakeApp)

    await main_module._run()

    assert created["transcriber"]["youtube_languages"] == ("ru", "en")
    assert created["service"][0] == "/tmp/data"
    assert created["ran"] is True


def test_main_uses_asyncio_run(monkeypatch) -> None:
    calls = []

    def fake_run(coro):
        calls.append(coro)
        coro.close()

    monkeypatch.setattr(main_module.asyncio, "run", fake_run)

    main_module.main()

    assert len(calls) == 1


def test_app_local_telegram_adapter_client_defaults_to_polling() -> None:
    adapter_src = Path(__file__).resolve().parents[1] / "apps" / "telegram-bot" / "src"
    sys.path.insert(0, str(adapter_src))
    try:
        from telegram_adapter.api_client import build_delivery_payload

        assert build_delivery_payload() == {"strategy": "polling"}
    finally:
        sys.path.pop(0)
