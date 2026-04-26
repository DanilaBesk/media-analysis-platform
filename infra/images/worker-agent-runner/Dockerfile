FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm \
    && npm install -g @anthropic-ai/claude-code \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/runtime/agent-runner /var/log/platform
