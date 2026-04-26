from __future__ import annotations

import sys
from pathlib import Path

import telegram_transcriber_bot.__main__ as main_module


def test_main_returns_fail_fast_exit_code(capsys) -> None:
    exit_code = main_module.main()

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "legacy single-process runtime was replaced during cutover" in captured.err
    assert "compose-smoke.sh --live-smoke" in captured.err


def test_app_local_telegram_adapter_client_defaults_to_polling() -> None:
    adapter_src = Path(__file__).resolve().parents[1] / "apps" / "telegram-bot" / "src"
    sys.path.insert(0, str(adapter_src))
    try:
        from telegram_adapter.api_client import build_delivery_payload

        assert build_delivery_payload() == {"strategy": "polling"}
    finally:
        sys.path.pop(0)
