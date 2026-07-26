FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && uv tool install --python /usr/local/bin/python yt-dlp==2026.3.17 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/runtime/media-export /var/log/platform
