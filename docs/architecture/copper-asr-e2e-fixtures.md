# CopperASR E2E Fixtures

This packet owns deterministic fixture inputs for `media-b8s.2.*` CopperASR E2E proof.

## Source

The speech fixtures are synthetic and do not come from private Telegram history.

- Voice: macOS `say` voice `Milena` (`ru_RU`).
- Audio encoding: `ffmpeg` Opus in Ogg container, mono, 16 kHz, 24 kbit/s.
- Runtime model: resolved by the `copper-asr` service env/cache at execution time. Model weights are not embedded in the fixture files.
- Fixture manifest: `infra/fixtures/target/manifest.json`.
- Fixture harness: `python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --json`.
- Telegram E2E harness: `python3 infra/scripts/copper-asr-telegram-e2e.py --json`.
- API/Web/MCP E2E harness: `python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json`.
- Failure E2E harness: `python3 infra/scripts/copper-asr-failure-e2e.py --json`.

## Cases

- `short_voice`: short Russian voice input for Telegram transcript delivery and basic artifact assertions.
- `representative_long_voice`: 960 second synthetic voice input for performance and backpressure proof.
- `corrupt_audio`: invalid Ogg-named bytes for failed-run diagnostics. Target diagnostic is `asr_invalid_audio`; the live CopperASR runtime currently returns provider code `unexpected_runtime_error` for this fixture, so the harness accepts `asr_unexpected_runtime_error` until follow-up `media-b8s.2.9` normalizes corrupt-audio classification.
- `cancellation_voice`: long voice input reused for cancellation before or during ASR.
- `artifact_download`: deterministic transcript artifact bytes for download-path assertions.

## Commands

```bash
uv run pytest packages/contracts/tests/test_target_fixtures.py -q
python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --json
python3 infra/scripts/copper-asr-telegram-e2e.py --json
python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json
python3 infra/scripts/copper-asr-failure-e2e.py --json
bash infra/scripts/target-reset-smoke.sh
bash infra/scripts/compose-smoke.sh --check-config
```

The Telegram E2E command requires the local compose API, transcription worker, and CopperASR service to be running. It creates a throwaway Telegram channel account, uploads the `short_voice` fixture as a voice message, asserts inbox materialization and current materials surface persistence, creates a selection snapshot and transcription run, verifies worker progress/finalize events, downloads transcript and run manifest bytes, records a result artifact surface, proves duplicate delivery is blocked by that surface, and clears the inbox collection membership after successful delivery.

The API/Web/MCP E2E command requires the local compose API, transcription worker, and CopperASR service to be running. It creates throwaway Web and MCP channel accounts, uploads the `short_voice` fixture through the public API, starts a transcription run, asserts transcript plus policy artifacts, downloads transcript and manifest bytes, checks diagnostics filtering, proves cross-channel denial, and verifies the run/artifact history survives source media deletion.

The failure E2E command requires the local compose API, transcription worker, and CopperASR service to be running. It creates throwaway channel accounts and analysis runs, then asserts corrupt-audio failure, retry failure, cancellation, policy artifact publication, absence of transcript artifacts on failed runs, and the CopperASR resource-limit env knobs.

The harness can also copy fixture bytes into a bucket/object-key directory tree for later MinIO seed work:

```bash
python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --copy-object-store /tmp/copper-asr-e2e-object-store --json
```

## Limits

These fixtures prove deterministic E2E wiring and provide benchmark input. They do not define a production performance threshold. `media-b8s.2.7` must record measured wall time, CPU, memory, ONNX thread setting, warm/cold model state, and delivery latency before any performance claim is made.
