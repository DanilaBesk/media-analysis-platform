FROM python:3.12-slim

ENV COPPER_ASR_HOST=0.0.0.0 \
    COPPER_ASR_PORT=8000 \
    COPPER_ASR_DEVICE=auto \
    COPPER_ASR_PRELOAD_MODEL=true \
    COPPER_ASR_REQUEST_TIMEOUT_S=28800 \
    COPPER_ASR_ACQUIRE_TIMEOUT_S=30 \
    COPPER_ASR_MAX_UPLOAD_MB=4096 \
    COPPER_ASR_UPLOAD_CHUNK_BYTES=1048576 \
    COPPER_ASR_MAX_CONCURRENT_REQUESTS=1 \
    COPPER_ASR_ONNX_NUM_THREADS=2 \
    COPPER_ASR_TORCH_NUM_THREADS=2 \
    COPPER_ASR_TORCH_INTEROP_THREADS=1 \
    COPPER_ASR_FFMPEG_THREADS=1 \
    COPPER_ASR_LOG_LEVEL=info \
    COPPER_ASR_CACHE_DIR=/var/cache/copper-asr \
    COPPER_ASR_TMP_DIR=/tmp/copper-asr \
    TORCH_HOME=/var/cache/copper-asr/torch

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/copper-asr

COPY external/copper-asr ./

# Keep the local compose runtime CPU-only: CopperASR VAD delegates ONNX provider
# choice to the installed runtime, and this image intentionally installs cpu.
RUN python -m pip install --no-cache-dir torch==2.5.1 torchaudio==2.5.1 \
    && python -m pip install --no-cache-dir ".[server,cpu]" \
    && useradd --create-home --shell /usr/sbin/nologin copperasr \
    && mkdir -p /var/cache/copper-asr/torch /tmp/copper-asr /var/log/platform \
    && chown -R copperasr:copperasr /var/cache/copper-asr /tmp/copper-asr /var/log/platform

USER copperasr

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=10)"

CMD ["copper-asr-server"]
