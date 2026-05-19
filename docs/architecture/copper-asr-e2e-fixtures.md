# CopperASR E2E Fixtures

This packet owns deterministic fixture inputs for `media-b8s.2.*` CopperASR E2E proof.

## Source

The speech fixtures are synthetic and do not come from private Telegram history.

- Voice: macOS `say` voice `Milena` (`ru_RU`).
- Audio encoding: `ffmpeg` Opus in Ogg container, mono, 16 kHz, 24 kbit/s.
- Runtime model: resolved by the `copper-asr` service env/cache at execution time. Model weights are not embedded in the fixture files.
- Fixture manifest: `infra/fixtures/target/manifest.json`.
- Harness: `python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --json`.

## Cases

- `short_voice`: short Russian voice input for Telegram transcript delivery and basic artifact assertions.
- `representative_long_voice`: 960 second synthetic voice input for performance and backpressure proof.
- `corrupt_audio`: invalid Ogg-named bytes for `asr_invalid_audio` diagnostics.
- `cancellation_voice`: long voice input reused for cancellation before or during ASR.
- `artifact_download`: deterministic transcript artifact bytes for download-path assertions.

## Commands

```bash
uv run pytest packages/contracts/tests/test_target_fixtures.py -q
python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --json
bash infra/scripts/target-reset-smoke.sh
bash infra/scripts/compose-smoke.sh --check-config
```

The harness can also copy fixture bytes into a bucket/object-key directory tree for later MinIO seed work:

```bash
python3 infra/scripts/copper-asr-e2e-harness.py --check-fixtures --copy-object-store /tmp/copper-asr-e2e-object-store --json
```

## Limits

These fixtures prove deterministic E2E wiring and provide benchmark input. They do not define a production performance threshold. `media-b8s.2.7` must record measured wall time, CPU, memory, ONNX thread setting, warm/cold model state, and delivery latency before any performance claim is made.
