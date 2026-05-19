# CopperASR Runtime Soak QA

Owner Bead: `media-b8s.3.3`

Date: 2026-05-19

## Scope

This packet proves the compose runtime after the CopperASR migration. It covers
compose health convergence, Telegram/API/Web/MCP CopperASR flows, corrupt audio
classification, long voice resource usage, log scans, and startup/recovery
stability.

## Commands

```bash
RUNTIME_E2E_POLL_TIMEOUT_SECONDS=300 bash infra/scripts/compose-smoke.sh --live-smoke
python3 infra/scripts/copper-asr-telegram-e2e.py --json
python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json
python3 infra/scripts/copper-asr-failure-e2e.py --json --require-invalid-audio
python3 infra/scripts/copper-asr-benchmark-e2e.py --json --write-artifact docs/benchmarks/copper-asr-long-voice-benchmark-latest.json --blocker-issue-id media-b8s.2.10
uv run --project apps/telegram-bot pytest apps/telegram-bot/tests -q
uv run pytest packages/contracts/tests/test_target_fixtures.py -q
bash infra/scripts/no-legacy-asr-gate.sh
bash infra/scripts/compose-smoke.sh --check-config
```

## Runtime Results

- Compose live smoke completed successfully after a full recreate.
- After adding the API healthcheck and changing API consumers to
  `condition: service_healthy`, `telegram-bot`, Web, MCP, and both workers
  started with `restart_count=0`.
- Startup logs after the final recreate contain no `backend_unavailable`,
  `Connection refused`, `Traceback`, `TypeError`, `panic`, or `fatal` entries
  for API consumers.
- ASR logs after the final recreate contain no legacy `whisper`,
  `faster-whisper`, `ctranslate`, `podlodka`, or `backend_unavailable`
  markers.

## CopperASR E2E

- Telegram E2E succeeded with `provider=copperasr`,
  `model=Copperside/CoppersideASR`, document delivery, duplicate delivery
  prevention, and inbox clear.
- API/Web/MCP E2E succeeded with channel-scoped artifact/run denial,
  public `transcript` surface, and MCP-visible `run_manifest` provider
  metadata.
- Failure E2E with `--require-invalid-audio` succeeded: corrupt and retry runs
  both finalized as `failed` with `diagnostic_codes=["asr_invalid_audio"]`,
  and cancellation finalized as `canceled` with no artifacts.

## Long Voice Soak

Latest benchmark artifact:
`docs/benchmarks/copper-asr-long-voice-benchmark-latest.json`.

- Input duration: `960.006` seconds.
- `run_wall_seconds=200.28`.
- `speedup_vs_realtime=4.793`.
- `thresholds.passed=true`.
- CopperASR `max_cpu_percent=409.15 <= 450.0`.
- CopperASR `max_memory_mib=1781.76 <= 4096.0`.
- Runtime VAD/ASR metadata is present:
  `vad_segment_count=56`, `chunk_count=56`, `vad_s=120.219`,
  `asr_inference_s=76.999`.

## Runtime Bugs Closed During QA

`media-b8s.3.3.1` was discovered and closed during this packet.

Root cause: Telegram adapter startup recovered channel surfaces before polling.
A stale Telegram `current_materials_panel` surface could raise
`TelegramBadRequest: chat not found`; the previous fallback tried to send a
replacement message into the same unreachable chat and crashed the whole
adapter process.

Fix:

- Telegram surface failures are classified per operation.
- Stale/unreachable addresses are superseded with
  `telegram_address_unreachable` or `telegram_message_unavailable`.
- Duplicate result delivery guard remains first and does not probe Telegram
  when an active result surface already has an address.
- Failed new result delivery does not create a result surface and does not
  clear the inbox.
- Gateway status recovery now tolerates the runtime API flat page metadata
  shape (`page: 1`, `page_size: n`) for orphan/stale channel accounts.

Runtime proof seeded a stale Telegram surface, restarted `telegram-bot`, and
observed:

- `[TelegramAdapter][bot][BLOCK_HANDLE_TELEGRAM_SURFACE_FAILURE]`
  with `classification=telegram_address_unreachable`.
- The affected `channel_surface` moved to `lifecycle_status=superseded`.
- A `channel_surface.superseded` event was written with
  `reason=telegram_address_unreachable`.
- The adapter reached `Start polling` and `Run polling` without a restart loop.

## Known Log Semantics

The strict corrupt-audio/cancellation E2E intentionally produces failed and
canceled runs. Worker logs still include stack traces for these controlled
paths (`CopperAsrTranscriptionError invalid_audio` and
`WorkerCancellationRequested`). The API state and artifacts are correct, but
the log shape is noisy; this should be tracked separately if production log
cleanliness for controlled failures becomes a release gate.
