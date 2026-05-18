# Whisper Removal Surface Inventory

Date: 2026-05-19

Bead: `media-b8s.1.2`

Status: research-only inventory for the CopperASR migration source plan. No runtime code is changed by this artifact.

## Executive Summary

The current transcription runtime is still `faster-whisper`/Whisper, not CopperASR. A complete migration is therefore not an env switch. It must replace the active worker transcriber boundary, remove Whisper dependencies and cache volumes, update compose/env and smoke gates, rewrite Whisper-specific tests, and keep the API-owned media flow intact.

The user-visible trigger is `media-11o`: the observed slow/high-CPU transcription path was caused by the compose worker running `faster-whisper` on CPU through `WHISPER_DEVICE=cpu`, while no `copper_asr` or GigaAM ONNX runtime was installed in the active worker. Existing GRACE docs also record the same runtime mismatch as completed bead `media-l5w`.

The next source plan must treat CopperASR as the sole active ASR backend. It must not leave Whisper as fallback, compatibility mode, dormant provider switch, or documented active path.

## Evidence Commands

Inventory command:

```bash
rg -n -i "whisper|faster[-_]?whisper|ctranslate|podlodka|DefaultTranscriber|WhisperTranscriber|WHISPER" \
  --glob '!apps/web/dist/**' \
  --glob '!docs/research/2026-05-19-copper-asr-submodule-runtime-contract.md'
```

Runtime cache command:

```bash
find . -maxdepth 4 \( -iname '*whisper*' -o -iname '*podlodka*' -o -iname '*ctranslate*' \) -print
```

Tracked filename command:

```bash
git ls-files | rg -i "whisper|podlodka|ctranslate"
```

The tracked filename command produced no tracked file paths named after Whisper, Podlodka, or CTranslate2. The remaining tracked usage is content-level references inside code, tests, compose/env, dependency manifests, and docs.

## Active Production Runtime

### Shared transcriber module

Path: `workers/common/src/transcriber_workers_common/transcribers.py`

Active Whisper surfaces:

- Imports `WhisperModel` from `faster_whisper`.
- Exposes `PODLODKA_WHISPER_MODEL = "bond005/whisper-podlodka-turbo"`.
- Exposes `_PODLODKA_CTRANSLATE2_DIR = "bond005-whisper-podlodka-turbo-ct2"`.
- Defines `WhisperTranscriber`, which loads a shared `faster-whisper` model and calls `model.transcribe(..., vad_filter=True, beam_size=5)`.
- Defines `DefaultTranscriber`, which tries YouTube subtitles first and then falls back to `WhisperTranscriber`.
- Uses `WHISPER_MODEL_CACHE_DIR` to choose a local model cache root.
- Contains Podlodka-specific Hugging Face snapshot download and CTranslate2 conversion helpers:
  - `_ensure_podlodka_ctranslate2_model`
  - `_download_podlodka_snapshot`
  - `_build_podlodka_converter`

Removal requirement:

- Replace `WhisperTranscriber` with the CopperASR adapter/service client decided by the source plan.
- Delete Podlodka/Whisper/CTranslate2 conversion code completely.
- Keep `YouTubeTranscriptTranscriber` only if the source plan explicitly preserves the subtitle fast path as a non-ASR optimization. If preserved, it must no longer be named as Whisper fallback behavior.
- Preserve canonical output objects: `TranscriptResult`, `TranscriptSegment`, language, source label, segment timestamps, and raw text.

### Transcription worker launcher

Path: `workers/transcription/src/transcriber_worker_transcription_main.py`

Active Whisper surfaces:

- Imports `DefaultTranscriber` and `PODLODKA_WHISPER_MODEL`.
- Builds `DefaultTranscriber` from `WHISPER_MODEL`, `WHISPER_DEVICE`, and `WHISPER_COMPUTE_TYPE`.
- Defaults to CPU through `WHISPER_DEVICE=cpu`.

Removal requirement:

- Replace `_build_transcriber` with a CopperASR runtime builder.
- Replace `WHISPER_*` env variables with CopperASR runtime env variables.
- Preserve `WorkerRuntimeConfig`, object store wiring, API client wiring, and `run_worker_loop`.

### Transcription worker orchestration

Path: `workers/transcription/src/transcriber_worker_transcription.py`

This module is provider-agnostic at the orchestration layer. It claims `selection.transcription` steps, materializes source media, invokes `transcriber.transcribe(materialized_source, workspace_dir)`, writes artifacts, registers diagnostics, publishes progress, and finalizes the run.

Behavior to preserve:

- Step claim uses `worker_kind="transcription"` and `step_kind="selection.transcription"`.
- Progress stages stay stable: `materializing_sources`, `transcribing`, `persisting_artifacts`, `completed`, `failed`, `canceled`.
- Transcribable media kinds remain `audio`, `voice`, and `video`.
- Single direct YouTube URL can be converted to a `youtube_url` source.
- Multiple media inputs are concatenated through ffmpeg into `combined.wav`.
- Text media is skipped with a diagnostic instead of being sent to ASR.
- Object/materialization failures produce diagnostics and `source_unavailable` style failure evidence.
- Successful runs persist `transcript_plain`, `transcript_segmented_markdown`, `transcript_docx`, `run_manifest`, and `run_diagnostics`.
- Cancellation checks remain before materialization, before ASR, before artifact upload, and before finalization. CopperASR may need cooperative cancellation at the adapter boundary or best-effort cancellation around the service call.

## Dependencies And Lockfile

Active dependency surfaces:

- `pyproject.toml` declares `faster-whisper>=1.2.0`.
- `uv.lock` contains `faster-whisper 1.2.1`.
- `uv.lock` contains `ctranslate2`.
- `uv.lock` ties `faster-whisper` to `av`, `ctranslate2`, `huggingface-hub`, `onnxruntime`, `tokenizers`, and `tqdm`.

Related dependencies that must be reviewed before deletion:

- `torch`
- `transformers`
- `accelerate`
- `onnxruntime`
- `huggingface-hub`

The current code only uses `torch`, `transformers`, and CTranslate2 through the Podlodka conversion path. `onnxruntime` may be needed by CopperASR depending on whether this repository embeds CopperASR directly or talks to a CopperASR service container. The source plan must decide this boundary before dependency cleanup.

## Compose, Env, Volumes, And Smoke Gates

Active compose/env surfaces:

- `infra/docker-compose.yml` sets `WHISPER_MODEL_CACHE_DIR: /var/cache/whisper`.
- `infra/docker-compose.yml` mounts `whisper-model-cache:/var/cache/whisper`.
- `infra/docker-compose.yml` declares the `whisper-model-cache` volume.
- `infra/env/worker-transcription.env.example` defines:
  - `WHISPER_MODEL=bond005/whisper-podlodka-turbo`
  - `WHISPER_DEVICE=cpu`
  - `WHISPER_COMPUTE_TYPE=default`
  - `WHISPER_MODEL_CACHE_DIR=/var/cache/whisper`
- `infra/scripts/compose-smoke.sh` requires the `whisper-model-cache:` compose snippet.

Removal requirement:

- Replace worker env examples with CopperASR env.
- Remove `WHISPER_*` variables.
- Remove `whisper-model-cache` volume and mount.
- Update compose smoke checks so they reject Whisper snippets and require the CopperASR runtime contract.
- If CopperASR runs as a separate service/submodule container, add health/dependency checks for that service instead of hiding it inside the worker.

## API, Telegram, Web, And MCP Flow

No direct Whisper references were found in API, Telegram, Web, or MCP runtime code outside generic user text and docs. These surfaces are provider-agnostic today and must remain so.

Current flow:

- Telegram starts runs through `apps/telegram-bot/src/telegram_adapter/gateway.py` by calling `create_analysis_run(..., run_type="transcription", delivery={"strategy":"polling"})`.
- Web starts runs through `apps/web/src/features/media/media-workspace.tsx` with `runType` including `transcription`.
- MCP starts runs through `apps/mcp-server/src/tools/mapped-tools.ts` with the same `run_type` enum.
- API queues `selection.transcription` for transcription runs in `apps/api/internal/api/media_runtime.go`.
- API creates `selection.transcription` worker steps in `apps/api/internal/api/target_runtime.go`.
- Report and deep research runs create prerequisite transcription steps for speech media, then pass transcript artifacts to the agent-runner.
- The transcription worker claims the step, materializes ordered inputs, calls the transcriber, persists artifacts, and finalizes the run.

Preserved user-visible behavior:

- Telegram, Web, and MCP continue to create `analysis_run` records rather than calling an ASR backend directly.
- Existing run statuses, cancellation, retry, diagnostics, materials/result buttons, and artifact listing remain API-owned.
- Transcript artifact kinds and delivery behavior remain stable.
- Existing Telegram logic that chooses transcript artifacts and sends inline/file results should not need provider awareness.

## Tests To Rewrite Or Replace

Active Whisper-specific test surfaces:

- `workers/common/tests/test_worker_common_transcribers.py`
  - Verifies subtitle behavior and Whisper fallback behavior.
  - Imports `DefaultTranscriber`, `WhisperTranscriber`, `_download_podlodka_snapshot`, and `_build_podlodka_converter`.
  - Tests Podlodka snapshot and converter helpers.
- `workers/common/tests/test_worker_common_transcribers_runtime.py`
  - Covers `WhisperTranscriber`, Podlodka conversion, cache recovery, model cache root, and shared model serialization.
- `workers/transcription/tests/test_transcriber_worker_transcription.py`
  - Contains generic worker orchestration tests but uses `"whisper crashed"` as a test error message.
- `workers/common/tests/test_api.py`
  - Uses `"running whisper"` as a generic progress message fixture.

Test migration requirement:

- Replace Whisper implementation tests with CopperASR adapter/service tests.
- Keep worker orchestration tests provider-agnostic.
- Add regression tests that reject `WHISPER_*`, `faster_whisper`, `WhisperTranscriber`, and `whisper-model-cache` from active runtime files.
- Add end-to-end proof for Telegram/Web/MCP/API worker path after CopperASR is active.
- Rename generic fixture messages away from Whisper wording unless the test is explicitly historical.

## Docs, GRACE, And Historical References

Active docs and GRACE references:

- `AGENTS.md` says `workers/common/src/transcriber_workers_common/transcribers.py` contains shared YouTube/subtitles/Whisper helpers.
- `CLAUDE.md` says transcript acquisition uses YouTube subtitles first, then Whisper fallback.
- `docs/operational-packets.xml` records that current compose runs `faster-whisper` with `WHISPER_DEVICE=cpu`.
- `docs/requirements.xml` records the same mismatch as completed bead `media-l5w`.
- `docs/architecture/runtime-ops.md` mentions `Whisper returned an empty transcript`.

Historical research:

- `docs/research/huggingface-russian-asr-self-hosted.html` contains many Whisper, Podlodka, CTranslate2, and whisper.cpp comparisons. It can remain only as historical research if clearly not referenced as the active architecture baseline.

Docs migration requirement:

- Update AGENTS/CLAUDE code maps after implementation.
- Update GRACE docs alongside implementation:
  - `docs/requirements.xml`
  - `docs/technology.xml`
  - `docs/development-plan.xml`
  - `docs/verification-plan.xml`
  - `docs/knowledge-graph.xml`
  - `docs/operational-packets.xml`
- Preserve the historical mismatch evidence, but make the active target CopperASR-only.
- Update runtime-ops troubleshooting to use CopperASR error wording and health checks.

## Local Runtime Caches And Untracked Data

Current local Whisper cache paths found under `.data/models`:

- `.data/models/bond005-whisper-podlodka-turbo-ct2`
- `.data/models/models--bond005--whisper-podlodka-turbo`
- `.data/models/.locks/models--bond005--whisper-podlodka-turbo`

Cleanup requirement:

- Do not delete local caches as part of research.
- During implementation/ops cleanup, remove or ignore these caches only after CopperASR runtime proof is complete and there is no active code path that can reuse them.
- If Docker volumes exist locally, remove the `whisper-model-cache` volume only with explicit runtime cleanup instructions, not as a silent code migration side effect.

## Removal Order

Recommended source-plan order:

1. Define the CopperASR boundary: submodule path, runtime package/service command, model location, env, health check, concurrency, timeout, and transcript schema mapping.
2. Add CopperASR adapter tests against the chosen boundary without deleting the old path yet.
3. Implement the CopperASR transcriber boundary in `workers/common` or the worker package.
4. Wire `workers/transcription/src/transcriber_worker_transcription_main.py` to build CopperASR only.
5. Update compose/env/Dockerfile/runtime installation and health/smoke checks.
6. Preserve worker orchestration and artifact contract tests.
7. Remove `WhisperTranscriber`, `DefaultTranscriber` Whisper fallback, Podlodka conversion helpers, `faster-whisper`, CTranslate2, and `WHISPER_*` env.
8. Replace Whisper-specific tests with CopperASR tests and add no-legacy guard tests.
9. Update docs/GRACE/AGENTS/CLAUDE/runtime-ops.
10. Run focused unit/integration checks, then full E2E for Telegram, Web, MCP, API, worker, artifacts, diagnostics, retry, and cancellation.

## Risks And Open Gaps

- CopperASR source currently needs a firm source-plan decision: embedded Python package in the worker, separate service container, or library-first package plus optional HTTP runtime.
- The model contract must be validated against the actual CopperASR repository and model manifest before implementation.
- Performance claims require runtime proof with representative audio. The high-CPU issue is explained by current Whisper CPU runtime, but CopperASR speed must still be measured locally after integration.
- True streaming may not be available in CopperASR today. The app should keep the existing queued artifact workflow unless CopperASR proves a stable streaming API.
- YouTube subtitle fast path is not Whisper, but it changes ASR usage semantics. The source plan must explicitly decide whether it stays as a non-ASR optimization or is removed for strict ASR consistency.
- `onnxruntime`, `torch`, `transformers`, and `huggingface-hub` cleanup depends on the final CopperASR packaging and must not be deleted blindly.
- E2E coverage cannot be claimed until the stack is run with CopperASR active and transcript artifacts are observed through Telegram/Web/MCP surfaces.

## No-Whisper Guard Proposal

After implementation, add a focused guard test or script that scans active runtime files and fails if these tokens remain outside explicitly historical docs:

- `faster_whisper`
- `faster-whisper`
- `WhisperTranscriber`
- `DefaultTranscriber`
- `PODLODKA_WHISPER_MODEL`
- `WHISPER_MODEL`
- `WHISPER_DEVICE`
- `WHISPER_COMPUTE_TYPE`
- `WHISPER_MODEL_CACHE_DIR`
- `whisper-model-cache`
- `bond005/whisper-podlodka-turbo`
- `ctranslate2.converters.transformers`

Allowed exceptions should be narrowly scoped to this inventory, historical research archives, and migration notes that are explicitly not active runtime documentation.
