# CopperASR Submodule Migration Source Plan

Date: 2026-05-19

Root Bead: `media-b8s`

Planning Bead: `media-b8s.1.3`

Source anchors:

- `docs/research/2026-05-19-copper-asr-submodule-runtime-contract.md`
- `docs/research/2026-05-19-whisper-removal-surface-inventory.md`
- `docs/operational-packets.xml`
- `docs/requirements.xml`
- Beads graph `media-b8s.*`

## Product Goal

Make `media-analysis-platform` use CopperASR as the only active ASR backend for audio, voice, and video transcription while preserving the API-owned product model:

`media_asset -> collection/inbox -> selection_snapshot -> analysis_run -> analysis_run_step -> artifacts/diagnostics`.

The migration is complete only when the old `faster-whisper`/Whisper/Podlodka runtime is removed from active production code, compose, env, dependencies, active tests, runtime docs, and smoke gates.

## Final Decisions

### Runtime boundary

Use CopperASR through a dedicated internal HTTP runtime service, not as an embedded import inside the transcription worker.

Decision:

- Add CopperASR as a pinned git submodule at `vendor/copper-asr`.
- Add a compose service named `copper-asr`.
- Build the service from a repo-local wrapper Dockerfile under `infra/images/copper-asr/Dockerfile`.
- The transcription worker calls `http://copper-asr:8000` through a narrow `CopperAsrHttpTranscriber`.
- The worker remains responsible for queue claims, progress, cancellation checks, object materialization, artifact persistence, diagnostics, and user-visible run state.
- CopperASR owns only model lifecycle, audio inference, sanitized ASR errors, `/health`, and `/transcribe`.

Rationale:

- Keeps heavy Torch/ONNX runtime dependencies isolated from the worker environment.
- Makes backpressure and model health visible as service-level behavior.
- Avoids dual-provider code and makes no-Whisper verification practical.

### Submodule provenance

Initial source pin:

- Source reviewed locally: `/Users/danila/Documents/CopperSide/copper-asr`
- Reviewed branch: `feature/copper-asr-http-runtime`
- Reviewed HEAD: `f2a8278fb236b2ba471083ca2debcc3e9052cd64`
- Reviewed remote: `https://copperside.gitlab.yandexcloud.net/clara/copper-asr.git`

Implementation rule:

- `media-b8s.1.4` must pin the submodule to an exact commit and record `git -C vendor/copper-asr rev-parse HEAD`.
- Do not track a floating branch for runtime builds.
- Do not copy generated CopperASR source into this repo.
- Do not modify the submodule directly from this repo except for an explicit upstream CopperASR change followed by a submodule pointer bump.
- If the reviewed remote is not accessible from a fresh checkout or CI, `media-b8s.1.4` must either configure the approved remote URL or open a blocking Bead before implementation continues.

### ASR behavior

CopperASR is the only active ASR provider for speech media.

Allowed non-ASR shortcut:

- The existing YouTube subtitle fast path may remain only as a subtitle/source shortcut.
- If subtitles are unavailable or unsuitable, the fallback must materialize YouTube audio and call CopperASR.
- No fallback may call Whisper, `faster-whisper`, Podlodka, CTranslate2, whisper.cpp, or a dormant provider switch.

### Rollback

Rollback is a git/deploy rollback to a previous release, not a runtime config fallback.

Do not keep:

- `TRANSCRIPTION_PROVIDER=whisper|copperasr`
- `WHISPER_*` env
- hidden `DefaultTranscriber` compatibility
- Whisper dependency kept for emergency fallback
- docs that describe Whisper as an active backup path

## Non-Goals

- Do not add diarization in the first CopperASR migration. Keep diarization disabled unless a later Bead adds model gates, legal notes, performance proof, and E2E coverage.
- Do not introduce true streaming or live microphone UX. Current CopperASR chunk/iterative APIs are not a durable online streaming contract.
- Do not redesign Telegram/Web/MCP product flows except where CopperASR output, status, or errors require small behavior/copy changes.
- Do not migrate or preserve local model caches from Whisper.
- Do not claim a performance fix until `media-b8s.2.7` records representative runtime measurements.

## Target Topology

Compose services:

- `worker-transcription`: materializes API-owned inputs, calls `copper-asr`, writes artifacts.
- `copper-asr`: internal ASR runtime exposing `/health` and `/transcribe`.

Internal URL:

- `COPPER_ASR_BASE_URL=http://copper-asr:8000`

Worker env:

- `COPPER_ASR_BASE_URL`
- `COPPER_ASR_CLIENT_TIMEOUT_S`
- `COPPER_ASR_LANGUAGE`
- `COPPER_ASR_PAUSE_THRESHOLD_S`
- `COPPER_ASR_DIARIZATION=false`

Runtime env:

- `COPPER_ASR_HOST=0.0.0.0`
- `COPPER_ASR_PORT=8000`
- `COPPER_ASR_MODEL_PATH`
- `COPPER_ASR_CACHE_DIR=/var/cache/copper-asr`
- `COPPER_ASR_TMP_DIR=/tmp/copper-asr`
- `COPPER_ASR_DEVICE=auto`
- `COPPER_ASR_PRELOAD_MODEL=true`
- `COPPER_ASR_REQUEST_TIMEOUT_S=28800`
- `COPPER_ASR_ACQUIRE_TIMEOUT_S=30`
- `COPPER_ASR_MAX_UPLOAD_MB=4096`
- `COPPER_ASR_UPLOAD_CHUNK_BYTES=1048576`
- `COPPER_ASR_MAX_CONCURRENT_REQUESTS=1`
- `COPPER_ASR_ONNX_NUM_THREADS=4`
- `COPPER_ASR_LOG_LEVEL=info`

Volumes:

- `copper-asr-cache:/var/cache/copper-asr`
- `copper-asr-tmp:/tmp/copper-asr`

Removed volumes:

- `whisper-model-cache`

Health:

- `copper-asr` must expose a healthcheck against `GET /health`.
- `worker-transcription` must depend on `copper-asr` health in local compose.
- Compose smoke must require CopperASR snippets and reject Whisper snippets.

## Worker Contract

Add a CopperASR provider boundary, likely in `workers/common/src/transcriber_workers_common/transcribers.py` or a sibling module if the file is split during cleanup.

Required public shape:

```python
class CopperAsrHttpTranscriber:
    def transcribe(self, source: SourceCandidate, workspace_dir: Path) -> TranscriptResult: ...
```

Transport:

- Use a testable transport abstraction.
- Add `httpx>=0.28` as an explicit dependency if the implementation uses `httpx`; otherwise keep a fully tested stdlib multipart transport.
- The transcriber must support local audio/video files and a materialized YouTube audio fallback.

Request mapping:

- Send multipart `file` with the materialized source bytes.
- Send multipart `params` JSON with supported params only.
- Default diarization to false.
- Include pause threshold from env.

Response mapping:

- `text` maps to `TranscriptResult.raw_text`.
- `language` maps to `TranscriptResult.language`, default `unknown`.
- `provider`, `model`, `revision`, `duration`, and CopperASR metadata go into run manifest/diagnostics, not into visible transcript text.
- If response has `segments`, map them directly to `TranscriptSegment`.
- If response has no `segments` but has `words`, create stable segments from word timestamps.
- If response has text only, create one segment with timestamp `00:00 - 00:00` so markdown/DOCX renderers keep their current contract.
- Empty text is a provider failure, not a successful empty transcript.

Error mapping:

| CopperASR error | Worker diagnostic code | Terminal behavior |
| --- | --- | --- |
| `400 invalid_params` | `asr_invalid_params` | failed |
| `400 upload_too_large` | `asr_upload_too_large` | failed |
| `422 invalid_audio` | `asr_invalid_audio` | failed |
| `502 empty_transcript` | `asr_empty_transcript` | failed |
| `503 runtime_unavailable` | `asr_runtime_unavailable` | failed, retryable by user/API |
| `503 busy_runtime_unavailable` | `asr_runtime_busy` | failed, retryable by user/API |
| `504 request_timeout` | `asr_request_timeout` | failed, retryable by user/API |
| malformed response | `asr_bad_response` | failed |
| transport timeout/connect error | `asr_transport_unavailable` | failed, retryable by user/API |

The user-facing Telegram/Web copy must not collapse all of these into a misleading "saved/result already sent" state. The run must expose diagnostics and remain terminal or retryable according to API state.

## Existing Behavior To Preserve

- Telegram, Web, and MCP create `analysis_run` records through the API; they never call CopperASR directly.
- API planning keeps `selection.transcription` for transcription runs.
- Report/deep research runs continue to create prerequisite transcription steps for speech media.
- Worker progress stages stay stable: `materializing_sources`, `transcribing`, `persisting_artifacts`, `completed`, `failed`, `canceled`.
- Artifacts stay stable: `transcript_plain`, `transcript_segmented_markdown`, `transcript_docx`, `run_manifest`, `run_diagnostics`.
- Artifact lineage through `artifact_subject` remains API-owned.
- Cancellation checks remain before materialization, before ASR, before artifact upload, and before finalization.
- Telegram result delivery, result buttons, duplicate prevention, and active card lifecycle remain channel-surface based.
- Web and MCP see provider-agnostic run/artifact/diagnostic contracts.

## Implementation Dependency Graph

1. `media-b8s.1.4` pins the CopperASR source and makes it reproducible.
2. `media-b8s.1.5` implements the worker CopperASR runtime boundary.
3. `media-b8s.1.6` maps CopperASR output, metadata, artifacts, diagnostics, and manifests.
4. `media-b8s.1.7` updates compose, Docker, env, health, model cache, and resource knobs.
5. `media-b8s.1.8` validates Telegram/Web/MCP behavior and adjusts provider-specific copy/status only where needed.
6. `media-b8s.1.9` removes all active Whisper runtime surfaces and adds strict no-legacy guards.
7. `media-b8s.1.10` refreshes GRACE and operational docs to match the final implemented contract.
8. `media-b8s.2.*` builds deterministic fixtures, E2E proof, no-legacy proof, and benchmark evidence.
9. `media-b8s.3.*` performs QA audits, runtime soak, and final readiness packaging.

## Execution Packets

### media-b8s.1.4 - Submodule and provenance

Write scope:

- `.gitmodules`
- `vendor/copper-asr`
- `README.md` or local bootstrap docs if needed
- `docs/technology.xml`
- `docs/knowledge-graph.xml`
- this source plan if the final submodule URL differs from the reviewed URL

Acceptance gates:

- `git submodule status --recursive`
- `git -C vendor/copper-asr rev-parse HEAD`
- `git -C vendor/copper-asr status --short`
- fresh checkout instructions can run `git submodule update --init --recursive`
- no Whisper dependency or fallback is added

### media-b8s.1.5 - Worker runtime boundary

Write scope:

- `workers/common/src/transcriber_workers_common/transcribers.py` or split provider files
- `workers/transcription/src/transcriber_worker_transcription_main.py`
- worker-common tests
- transcription worker tests
- `pyproject.toml` and `uv.lock` only for the new worker client dependency if needed

Acceptance gates:

- Unit tests for CopperASR response mapping.
- Unit tests for CopperASR HTTP error mapping.
- Unit tests proving `_build_transcriber` builds CopperASR from `COPPER_ASR_*`.
- Worker orchestration tests still pass with a fake CopperASR transcriber.
- No active `faster_whisper` import remains in the worker path.

### media-b8s.1.6 - Artifacts and diagnostics

Write scope:

- transcript mapping/rendering tests
- run manifest or diagnostics helpers
- API/worker tests that assert artifact subjects and finalization

Acceptance gates:

- Plain text, markdown, and DOCX transcript artifacts are produced from CopperASR-like responses.
- Manifest includes `provider=copperasr`, model id, revision, request timing, source duration when available, and ignored params.
- Diagnostics use stable `asr_*` codes and do not leak tokens, local absolute model paths, or raw private object URLs.

### media-b8s.1.7 - Compose, Docker, env, model cache

Write scope:

- `infra/docker-compose.yml`
- `infra/images/copper-asr/Dockerfile`
- `infra/env/worker-transcription.env.example`
- new `infra/env/copper-asr.env.example`
- `infra/scripts/compose-smoke.sh`
- compose/runtime docs

Acceptance gates:

- `docker compose -f infra/docker-compose.yml config` succeeds.
- `compose-smoke.sh` requires `copper-asr` and rejects `whisper-model-cache`, `WHISPER_*`, and `faster-whisper` snippets.
- Worker starts with `COPPER_ASR_BASE_URL`.
- CopperASR runtime healthcheck is part of local compose.

### media-b8s.1.8 - Telegram, Web, MCP behavior

Write scope:

- Telegram adapter tests and copy only where provider-specific failure/status appears.
- Web run/artifact/diagnostic tests if output metadata changes visible state.
- MCP tool tests if diagnostics/artifact metadata shape changes.

Acceptance gates:

- User-facing copy does not mention Whisper or internal provider technicals.
- Failed ASR runs expose diagnostics instead of "result already sent" or generic backend-unavailable loops.
- Existing result delivery, cancellation, active cards, Web run details, and MCP artifact/diagnostic access do not regress.

### media-b8s.1.9 - Legacy removal

Write scope:

- `workers/common/src/transcriber_workers_common/transcribers.py`
- old Whisper tests
- `pyproject.toml`
- `uv.lock`
- compose/env/docs runtime references
- no-legacy guard script/test

Deletion rules:

- Delete `WhisperTranscriber`.
- Delete `DefaultTranscriber` if its only purpose is Whisper fallback; replace with CopperASR-oriented naming.
- Delete `PODLODKA_WHISPER_MODEL`.
- Delete Podlodka snapshot and CTranslate2 conversion helpers.
- Delete `faster-whisper` from production dependencies.
- Delete `ctranslate2` if no longer pulled by non-Whisper code.
- Remove `WHISPER_MODEL`, `WHISPER_DEVICE`, `WHISPER_COMPUTE_TYPE`, `WHISPER_MODEL_CACHE_DIR`.
- Remove `whisper-model-cache`.
- Rename generic fixture strings such as `"running whisper"` or `"whisper crashed"`.

Allowed historical references:

- `docs/research/2026-05-19-whisper-removal-surface-inventory.md`
- archived research docs explicitly marked historical
- final readiness/QA docs that state Whisper was removed

### media-b8s.1.10 - GRACE and operations

Write scope:

- `docs/requirements.xml`
- `docs/technology.xml`
- `docs/development-plan.xml`
- `docs/verification-plan.xml`
- `docs/knowledge-graph.xml`
- `docs/operational-packets.xml`
- `AGENTS.md`
- `CLAUDE.md`
- `docs/architecture/runtime-ops.md`

Acceptance gates:

- `xmllint --noout docs/requirements.xml docs/technology.xml docs/development-plan.xml docs/verification-plan.xml docs/knowledge-graph.xml docs/operational-packets.xml`
- GRACE names CopperASR as the sole active ASR runtime.
- Whisper references are removed from active runtime docs or explicitly historical.
- Operational packets list implementation, cleanup, coverage, and QA evidence.

## Traceability Matrix

| Requirement | Implementation Bead | Cleanup Bead | Test/QA Bead | Proof |
| --- | --- | --- | --- | --- |
| CopperASR is a pinned submodule | `media-b8s.1.4` | `media-b8s.1.9` | `media-b8s.3.2` | submodule status, commit pin, fresh checkout |
| Dedicated CopperASR HTTP runtime | `media-b8s.1.7` | `media-b8s.1.9` | `media-b8s.3.3` | compose config, healthcheck, runtime logs |
| Worker calls CopperASR only | `media-b8s.1.5` | `media-b8s.1.9` | `media-b8s.2.2`, `media-b8s.2.3` | worker tests, E2E artifacts, backend metadata |
| API-owned media/run/artifact contract stays stable | `media-b8s.1.6` | none | `media-b8s.2.3` | API/worker tests, artifact lineage |
| Telegram voice delivers transcript | `media-b8s.1.8` | none | `media-b8s.2.2` | Telegram E2E, channel surface proof |
| Web and MCP read CopperASR artifacts | `media-b8s.1.8` | none | `media-b8s.2.3` | Web/MCP E2E or deterministic integration harness |
| Failure/retry/cancel/backpressure are visible | `media-b8s.1.5`, `media-b8s.1.8` | none | `media-b8s.2.4` | terminal states, diagnostics, no stuck cards |
| Whisper runtime is gone | `media-b8s.1.9` | `media-b8s.1.9` | `media-b8s.2.5`, `media-b8s.3.1` | no-Whisper gate, dependency scan |
| GRACE and ops docs match final state | `media-b8s.1.10` | `media-b8s.1.10` | `media-b8s.3.4` | XML validation, readiness packet |
| Representative long voice performance is measured | none | none | `media-b8s.2.7`, `media-b8s.3.3` | benchmark artifact with wall time and resources |
| 100% migration coverage is auditable | all implementation beads | `media-b8s.1.9` | `media-b8s.2.6` | coverage inventory with no missing proof |

## 100% E2E Coverage Definition

For this migration, "100% E2E" means every declared CopperASR migration requirement and every critical transcription user flow has automated or runtime proof. It does not mean pretending every tool reports one universal line-coverage metric.

Required coverage:

- Telegram voice upload to transcript delivery in chat or document.
- API upload/list/selection/run/artifact/diagnostic path.
- Worker queue claim, materialization, CopperASR call, artifact registration, finalization.
- Web run creation/detail/result browsing.
- MCP `run_analysis`, `get_run`, `list_artifacts`, `get_artifact`, and `get_diagnostics` behavior.
- Corrupt audio, unsupported media, runtime unavailable, runtime busy, timeout, retry, cancellation before ASR, cancellation around ASR, and long-audio backpressure.
- Cross-channel/account isolation remains covered.
- No active Whisper runtime references remain.
- Runtime compose proof starts the actual CopperASR service and does not install or call `faster-whisper`.
- Benchmark proof records long voice duration, wall time, CPU, memory, concurrency, ONNX thread setting, model warm/cold state, and result delivery latency.

Missing proof is a finding. If an item cannot be covered in the current Bead, open or link a blocking Bead before closing coverage.

## No-Whisper Cleanup Gate

Add one local/CI command under `media-b8s.2.5` that scans active runtime files.

The gate must reject these tokens outside approved historical docs:

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

The gate must scan at least:

- `pyproject.toml`
- `uv.lock`
- `workers`
- `infra`
- `apps`
- `packages`
- active docs and GRACE XML

The gate may allow explicitly historical files listed in an allowlist with comments. The allowlist must not include production code, compose, env examples, or active tests.

## Acceptance Gates By Phase

Planning gate:

- `test -f docs/plans/2026-05-18-copper-asr-submodule-migration.md`
- `rg -n "Final Decisions|Traceability Matrix|No-Whisper Cleanup Gate|100% E2E Coverage|GRACE" docs/plans/2026-05-18-copper-asr-submodule-migration.md`
- `git diff --check`

Implementation gate:

- Focused Python tests for worker-common and transcription worker.
- Focused API tests for analysis run step finalization and artifact lineage.
- Compose config and smoke checks.
- No-Whisper active runtime gate.
- GRACE XML validation after docs refresh.

Coverage gate:

- Deterministic E2E fixtures and reset harness.
- Telegram E2E.
- API/worker/Web/MCP E2E.
- Failure/retry/cancel/backpressure E2E.
- Performance benchmark.
- Coverage inventory mapping every source-plan requirement to proof.

QA gate:

- No legacy Whisper QA audit.
- CopperASR contract/security/ops/submodule QA audit.
- Compose soak and full runtime E2E.
- Final readiness packet with Beads graph, commit range, test commands, runtime proof, benchmark, no-Whisper scan, residual risks, and git/Beads sync state.

## Explicit Blocker Decisions

No blocker prevents starting implementation after this plan, but these items must be resolved in their assigned Beads before the migration can close:

- Submodule remote/credential accessibility: resolve in `media-b8s.1.4`.
- Exact CopperASR Docker install command and extras: resolve in `media-b8s.1.7` against the pinned submodule.
- Actual model artifact location for production-like local compose: resolve in `media-b8s.1.7` and record in GRACE.
- Representative long voice fixture availability: resolve in `media-b8s.2.1` and benchmark in `media-b8s.2.7`.
- Performance threshold: do not assert until `media-b8s.2.7` records measured wall time/resource data.
- Hard cancellation of native ASR inference: out of scope for this pass unless CopperASR exposes a proven interrupt; prove product-level cancellation state and document native-call limitation.

## Session Rule

Execute this plan through Beads in dependency order. Close a Bead only after:

1. Its artifact/code change is committed.
2. Its focused verification commands have run.
3. Any missing proof is filed as a Bead or recorded as an explicit blocker.
4. Beads/Dolt state is committed locally.
5. Git is pushed when the repository remote is reachable.
