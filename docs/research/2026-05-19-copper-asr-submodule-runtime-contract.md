# CopperASR submodule and runtime contract research

Date: 2026-05-19
Bead: `media-b8s.1.1`
Scope: research only. No production implementation code is changed by this artifact.

## Decision summary

`media-analysis-platform` should integrate CopperASR as a pinned external
module and use the CopperASR HTTP runtime as the primary compose boundary for
transcription. This keeps the heavy Python ASR model in one dedicated runtime
process/container, gives the worker a narrow provider client, and avoids
mixing the current worker dependency set with CopperASR's Torch/ONNX stack.

CopperASR must stay consumer-agnostic. It owns ASR model lifecycle, inference,
temporary upload handling, sanitized runtime errors, `/health`, `/transcribe`,
and ASR result shape. This application continues to own Telegram/Web/MCP/API
workflows, uploads, object storage, analysis runs, queues, retries, progress,
cancellation, permissions, artifacts, diagnostics, and user-visible status.

The migration must remove the active faster-whisper/Whisper runtime. It may
preserve the current YouTube-subtitle fast path only if the source plan
explicitly treats that as a non-ASR source shortcut. For speech files, voice,
audio, and video transcription, CopperASR becomes the only ASR provider.

## CopperASR source reviewed

Local checkout:

- Path: `/Users/danila/Documents/CopperSide/copper-asr`
- Branch: `feature/copper-asr-http-runtime`
- HEAD: `f2a8278fb236b2ba471083ca2debcc3e9052cd64`
- Remote: `https://copperside.gitlab.yandexcloud.net/clara/copper-asr`
- Dirty state: untracked `excalidraw.log`; not part of the contract.

Primary files reviewed:

- `README.md`
- `docs/HTTP_RUNTIME.md`
- `docs/superpowers/plans/2026-05-17-http-runtime-api.md`
- `pyproject.toml`
- `Dockerfile`
- `copper_asr/core/asr.py`
- `copper_asr/core/cli.py`
- `copper_asr/core/dtypes.py`
- `copper_asr/runtime/config.py`
- `copper_asr/runtime/service.py`
- `copper_asr/runtime/app.py`
- `copper_asr/runtime/contract.py`
- `copper_asr/runtime/errors.py`
- `copper_asr/models/utils/model_downloader.py`
- `copper_asr/models/onnx_model.py`
- `copper_asr/utils/audio_utils.py`
- `copper_asr/utils/vad_utils.py`
- `copper_asr/diarization/factory.py`
- `copper_asr/diarization/options.py`
- `docs/THIRD_PARTY_LICENSES.md`
- `tests/test_http_runtime_config.py`
- `tests/test_http_runtime_app.py`
- `tests/test_http_runtime_integration.py`

## Public library API

Package entrypoint:

- `from copper_asr import CopperASR`
- Exported through `copper_asr/__init__.py` and `copper_asr/core/__init__.py`.

Constructor:

```python
CopperASR(
    model_path: str | None = None,
    device: str = "auto",
    force_download: bool = False,
)
```

Supported calls:

- `transcribe(wav_file=..., audio_array=..., sample_rate=..., pause_threshold=2.0, diarization=False, diarization_options=None)`
- `transcribe_batch(wav_files, continue_on_error=True, pause_threshold=2.0, diarization=False, diarization_options=None)`
- `transcribe_chunk(audio_array, sample_rate=None, use_vad=False, pause_threshold=2.0)`
- `transcribe_iterative(..., return_sentences=False, with_progress=False, pause_threshold=2.0)`

Current output DTO:

- `TranscriptionResult.full_text`
- `TranscriptionResult.words[]` with `text`, `start`, `end`, optional `speaker`
- `TranscriptionResult.sentences[]` with `text`, `start`, `end`, optional `speaker`
- optional speaker timelines and `DiarizationDiagnostics`

Important limitation: `transcribe_chunk(...)` and `transcribe_iterative(...)`
are not a true online microphone/network streaming API. They operate on
already-available audio data. Product progress, cancellation, and durable job
state must remain in `media-analysis-platform`.

## HTTP runtime contract

Runtime entrypoints:

- console script: `copper-asr-server`
- module: `python -m copper_asr.runtime`
- app factory: `copper_asr.runtime.app:create_app`

Routes:

- `GET /health`
- `POST /transcribe`

The runtime deliberately does not expose `/jobs`, progress streaming,
cancellation, durable status, object-storage reads, database state, product
metadata, or permission checks.

`POST /transcribe` request:

- multipart field `file`: audio bytes
- multipart field `params`: JSON string

Accepted params:

- `language`
- `temperature`
- `beam_size`
- `initial_prompt`
- `pause_threshold`
- `diarization`
- `num_speakers`
- `min_speakers`
- `max_speakers`

`temperature`, `beam_size`, and `initial_prompt` are accepted compatibility
no-ops and reported in `metadata.ignored_params`. Unknown params are ignored
safely and also reported.

Stable response shape:

```json
{
  "text": "recognized text",
  "language": "ru",
  "segments": [],
  "words": [],
  "provider": "copperasr",
  "model": "Copperside/CoppersideASR",
  "revision": null,
  "duration": null,
  "metadata": {
    "speakers": null,
    "diarization": {"enabled": false},
    "ignored_params": []
  }
}
```

Sanitized error classes:

- `400 invalid_params`
- `400 upload_too_large`
- `422 invalid_audio`
- `502 empty_transcript`
- `503 runtime_unavailable`
- `503 busy_runtime_unavailable`
- `504 request_timeout`
- `500 unexpected_runtime_error`

Runtime implementation notes:

- One reusable `CopperASR` instance is kept per runtime process.
- Inference is guarded by `asyncio.BoundedSemaphore`.
- `COPPER_ASR_MAX_CONCURRENT_REQUESTS` defaults to `1`.
- Semaphore acquire timeout returns `503 busy_runtime_unavailable`.
- Model call is executed through `asyncio.to_thread(...)`.
- Request timeout returns `504 request_timeout`, but the native/model call may
  not be hard-cancelable once running.
- Uploads are streamed to a temp file in bounded chunks and cleaned up after
  success and failure.

## Runtime env and model cache

HTTP runtime env:

- `COPPER_ASR_HOST=0.0.0.0`
- `COPPER_ASR_PORT=8000`
- `COPPER_ASR_MODEL_PATH`
- `COPPER_ASR_CACHE_DIR`
- `COPPER_ASR_DEVICE=auto`
- `COPPER_ASR_TMP_DIR`
- `COPPER_ASR_PRELOAD_MODEL=true`
- `COPPER_ASR_REQUEST_TIMEOUT_S=28800`
- `COPPER_ASR_ACQUIRE_TIMEOUT_S=30`
- `COPPER_ASR_MAX_UPLOAD_MB=4096`
- `COPPER_ASR_UPLOAD_CHUNK_BYTES=1048576`
- `COPPER_ASR_MAX_CONCURRENT_REQUESTS=1`
- `COPPER_ASR_LOG_LEVEL=info`

Additional ASR engine knob:

- `COPPER_ASR_ONNX_NUM_THREADS` controls ONNX Runtime CPU threads. If unset,
  CopperASR uses `min(16, os.cpu_count())` for CPU-like execution.

The runtime maps `COPPER_ASR_CACHE_DIR` to `HF_HOME` before creating
`CopperASR`. A complete local model bundle through `COPPER_ASR_MODEL_PATH` is
preferred for deterministic compose operation and for avoiding runtime network
downloads.

## ONNX model contract

CopperASR currently supports one normalized model bundle:

```text
Copperside/CoppersideASR
encoder.onnx
decoder.onnx
joint.onnx
tokens.txt
model_manifest.json
```

`ModelDownloader` rejects local bundles without `model_manifest.json` and
rejects incomplete bundles missing any required file. If `model_path` is not
provided, it resolves files from Hugging Face cache or downloads them from
`Copperside/CoppersideASR`.

The default manifest contract includes:

- tokenizer type: `vocab`
- tokenizer file key: `tokens`
- blank token: `<blk>`
- blank id: `1024`
- max tokens per decoder step: `3`
- encoder output layout: `BDT`
- log-Mel feature config: 64 features, 320 win length, 160 hop length, 320 FFT,
  HTK mel scale, no center padding.

Device/provider behavior:

- `auto` selects CUDA only when ONNX Runtime exposes `CUDAExecutionProvider`.
- macOS `mps` and `metal` are forced to `CPUExecutionProvider`.
- CPU-like sessions set intra-op threads from `COPPER_ASR_ONNX_NUM_THREADS`,
  inter-op threads to `1`, sequential execution mode, memory pattern, and CPU
  arena.
- CUDA uses `CUDAExecutionProvider` with CPU fallback and optional IO binding.

## Audio, VAD, segmentation, and CPU implications

Audio input is decoded by `ffmpeg` into 16 kHz mono PCM. The command uses
`-threads 0`, so ffmpeg may use multiple CPU threads.

Long audio is loaded as a full torch tensor, then segmented by Silero VAD.
Silero VAD is explicitly CPU-only. Default VAD segmentation constants:

- VAD window duration: `300s`
- max segment duration: `22s`
- min segment duration: `15s`
- strict segment limit: `30s`
- speech threshold: `0.5`
- min speech duration: `250ms`
- min silence duration: `100ms`

This means CopperASR is optimized for a better ASR path than the current
Whisper runtime, but it can still legitimately load CPU heavily: ffmpeg,
Silero VAD, Torch tensor work, and ONNX CPU inference all run locally unless a
CUDA-capable runtime is provided. The observed media-analysis-platform incident
still came from faster-whisper CPU, not CopperASR; the migration must add
benchmark proof before promising exact latency for a 16-minute voice message.

## Diarization contract and gates

Diarization is optional and disabled by default.

Public controls:

- Python/API: `diarization=True`, `diarization_options`
- CLI: `--diarization`, `--num-speakers`, `--min-speakers`, `--max-speakers`
- HTTP params: `diarization`, `num_speakers`, `min_speakers`, `max_speakers`

Default backend is `pyannote_reference`, built only when diarization is
requested. Access gates:

- `COPPER_ASR_DIARIZATION_LOCAL_MODEL_DIR`, or
- `COPPER_ASR_DIARIZATION_ALLOW_DOWNLOAD=true`
- `COPPER_ASR_DIARIZATION_HF_USER_CONDITIONS_ACCEPTED=true`
- `HF_TOKEN` for remote gated access

Other diarization env:

- `COPPER_ASR_DIARIZATION_MODEL_ID`
- `COPPER_ASR_DIARIZATION_DEVICE`
- `COPPER_ASR_DIARIZATION_USE_SPEECH_REGIONS`
- `COPPER_ASR_DIARIZATION_SPEECH_REGION_PADDING_S`
- `COPPER_ASR_DIARIZATION_MAX_COMPACTED_DURATION_S`
- `COPPER_ASR_DIARIZATION_WINDOW_DURATION_S`
- `COPPER_ASR_DIARIZATION_WINDOW_OVERLAP_S`

For the first media-analysis-platform migration, diarization should remain
off unless the source plan explicitly adds operator model gates and E2E proof.
The current product README already says explicit diarization is not guaranteed.

## Packaging and submodule implications

CopperASR package dependencies:

- base: `huggingface_hub`, `numpy`, `torch`, `torchaudio`, `scipy`
- `cpu`: `onnxruntime`
- `gpu`: `onnxruntime-gpu`
- `metal`: `onnxruntime` on Darwin
- `diarization`: `pyannote.audio`
- `server`: `fastapi`, `uvicorn[standard]`, `python-multipart`, `httpx`

`media-analysis-platform` currently runs Python `>=3.12`, has root
dependencies for faster-whisper/Whisper-era runtime, and runs the transcription
worker inside the main workspace environment. Embedding CopperASR directly into
that worker would mix heavyweight ML dependency resolution with the worker and
would make removal of old Whisper deps harder to prove.

Recommended migration shape:

1. Add CopperASR as a pinned git submodule. The reviewed ownership boundary is
   `external/copper-asr`.
2. Build a dedicated compose service from the CopperASR Dockerfile or an
   application-local wrapper image that uses the submodule as build context.
3. Add internal service URL/config for the transcription worker, for example
   `COPPER_ASR_BASE_URL=http://copper-asr:8000`.
4. Replace `WhisperTranscriber` with a CopperASR provider client that maps the
   HTTP response into the current `TranscriptResult` and `TranscriptSegment`
   domain objects.
5. Keep all API-owned analysis_run, artifact, diagnostic, retry, progress, and
   cancellation state unchanged at the platform boundary.

## Current media-analysis-platform integration surfaces

The active repo has no `.gitmodules` entry yet.

Current Whisper-era active surfaces include:

- `pyproject.toml`: `faster-whisper`, `accelerate`, `transformers`,
  `onnxruntime`, `torch`
- `workers/common/src/transcriber_workers_common/transcribers.py`:
  `WhisperTranscriber`, `DefaultTranscriber`, `PODLODKA_WHISPER_MODEL`
- `workers/transcription/src/transcriber_worker_transcription_main.py`:
  builds `DefaultTranscriber` from `WHISPER_*` env
- `infra/env/worker-transcription.env.example`: `WHISPER_MODEL`,
  `WHISPER_DEVICE`, `WHISPER_COMPUTE_TYPE`, `WHISPER_MODEL_CACHE_DIR`
- `infra/docker-compose.yml`: `whisper-model-cache` volume and worker mount
- `infra/images/worker-transcription/Dockerfile`: worker image installs only
  ffmpeg and relies on root uv dependencies.

The full inventory and deletion order belongs to `media-b8s.1.2`, but this
research confirms the migration should not be modeled as a small env switch.
It is a runtime boundary replacement.

## Performance proof gaps

These are not yet proved for this application:

- end-to-end latency for the user's representative 16-minute Telegram voice;
- latency/RTF for 2+ hour files under compose resource limits;
- CPU usage with `COPPER_ASR_ONNX_NUM_THREADS` values such as 2, 4, 8, and 16;
- effect of ffmpeg `-threads 0` and Silero VAD CPU usage on total load;
- temp-disk footprint for uploaded file plus decoded/intermediate data;
- behavior under queue concurrency greater than runtime concurrency;
- retry behavior when CopperASR returns `503 busy_runtime_unavailable`;
- whether local model bundle is already present on target hosts;
- Docker build time and image size for the CopperASR runtime image.

The benchmark task must measure from the media-analysis worker perspective,
not only by calling CopperASR in isolation.

## Integration risks

- The current user-visible "service temporarily unavailable" copy can mask
  different failure classes. CopperASR errors should be mapped to diagnostics
  with provider/error code and should keep Telegram/Web/MCP status visible.
- A blocking long HTTP call must sit behind the existing worker queue, not
  behind a user-facing HTTP request.
- If `COPPER_ASR_MAX_CONCURRENT_REQUESTS=1`, worker queue concurrency must be
  sized so long files do not pile up inside CopperASR instead of in the durable
  application queue.
- Runtime timeout does not guarantee hard cancellation of a native/model call.
  Platform cancellation remains a product-level state and may require killing
  or isolating a worker/runtime process in later hard-cancel work.
- Local model downloads require Hugging Face access unless a complete model
  bundle is mounted. Production-like compose should prefer mounted local model
  artifacts.
- Diarization brings pyannote gates and legal/quality/performance risks. Keep
  it disabled unless explicitly planned and tested.
- macOS `metal` install does not mean ONNX will use MPS/CoreML for this graph;
  current CopperASR forces CPU provider for `mps`/`metal`.
- The worker's current transcript artifact contract must stay stable:
  transcript text, markdown, DOCX, source manifest/run policy artifacts,
  diagnostics, and item outcomes.

## Required next decisions for the source plan

`media-b8s.1.2` should inventory every Whisper reference and separate active
production surfaces from tests/docs/history.

`media-b8s.1.3` should turn this research into an implementation source plan
with these decisions:

- exact submodule path and pinned commit;
- HTTP runtime service name, image/build context, internal URL, healthcheck,
  volumes, model cache, tmp dir, and resource/thread env;
- worker provider interface and `TranscriptResult` mapping;
- dependency deletion order for faster-whisper/Whisper-era packages;
- diagnostics mapping for CopperASR HTTP errors;
- fake CopperASR runtime harness for deterministic E2E tests;
- optional live CopperASR benchmark gate with the 16-minute voice fixture;
- explicit no-Whisper regression gate.

## Evidence commands

Commands used for this research:

```bash
bd context
bd ready --json
bd update media-b8s.1.1 --claim --json
git status --short --branch
git rev-parse HEAD
git remote -v
rg --files
rg -n "CopperASR|CoppersideASR|GigaAM|ONNX|COPPER_ASR|FastAPI|transcribe"
sed -n '1,230p' README.md
sed -n '1,240p' docs/HTTP_RUNTIME.md
sed -n '1,620p' docs/superpowers/plans/2026-05-17-http-runtime-api.md
sed -n '1,260p' pyproject.toml
sed -n '1,980p' copper_asr/core/asr.py
sed -n '1,260p' copper_asr/core/dtypes.py
sed -n '1,340p' copper_asr/runtime/app.py
sed -n '1,320p' copper_asr/runtime/service.py
sed -n '1,260p' copper_asr/runtime/contract.py
sed -n '1,380p' copper_asr/models/utils/model_downloader.py
sed -n '1,260p' copper_asr/models/onnx_model.py
sed -n '1,300p' copper_asr/utils/vad_utils.py
sed -n '1,260p' copper_asr/diarization/factory.py
rg -n "WHISPER|Whisper|faster-whisper|DefaultTranscriber|transcription" docs apps workers infra pyproject.toml README.md
```
