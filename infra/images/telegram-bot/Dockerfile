FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN uv pip install --system \
    "aiogram>=3.22.0" \
    "httpx>=0.28.1,<1" \
    "python-dotenv>=1.1.1" \
    && mkdir -p /tmp/runtime/telegram-bot /var/log/platform
