# Runtime Ops

## Startup

Normal compose runtime is container-native. `copper-asr` owns ASR inference, `worker-transcription` owns transcript step orchestration and artifact publication, and report/deep-research AI execution is routed through the single `worker-agent-runner` model worker. Dedicated report/deep-research LLM worker services are no longer part of the compose topology.

The local compose service caps `copper-asr` with `COPPER_ASR_LOCAL_CPUS` and defaults it to `4.0` CPUs. The matching local ASR profile defaults to `COPPER_ASR_ONNX_NUM_THREADS=4`, `COPPER_ASR_TORCH_NUM_THREADS=4`, `COPPER_ASR_TORCH_INTEROP_THREADS=1`, and `COPPER_ASR_FFMPEG_THREADS=2`. This is a consumer-side Mac/local-stack guardrail: it does not edit CopperASR, does not change CopperASR production defaults, and can be overridden from the shell or `.env` before running compose.

`copper-asr` mounts `COPPER_ASR_CACHE_DIR=/var/cache/copper-asr` on the `copper-asr-cache` volume. CopperASR owns the internal cache fanout for model and VAD assets, so this consumer stack does not configure provider-specific cache internals.

```bash
bash infra/scripts/compose-smoke.sh --check-config
docker compose -f infra/docker-compose.yml up -d --build --wait
```

For a deterministic worker run, keep fixture/test-fixture agent-runner concurrency enabled in `infra/env/worker-agent-runner.env.example`. For a real provider run, configure the provider through the agent-runner env or secret mechanism. Do not route normal worker execution through a machine-local CLI bridge.

## Restart

```bash
docker compose -f infra/docker-compose.yml up -d --build --force-recreate copper-asr worker-transcription worker-agent-runner
docker compose -f infra/docker-compose.yml up -d api web telegram-bot mcp-server
```

## Logs

```bash
docker compose -f infra/docker-compose.yml logs --tail=120 api
docker compose -f infra/docker-compose.yml logs --tail=120 copper-asr
docker compose -f infra/docker-compose.yml logs --tail=120 worker-transcription
docker compose -f infra/docker-compose.yml logs --tail=120 worker-agent-runner
```

## Common Failures

- Provider command missing:
  the agent-runner image does not include the selected provider launcher. Use fixture/test-fixture provider lanes for compose smoke, or rebuild a provider-specific image layer with the launcher installed in the image.

- Missing provider credentials:
  real provider mode must receive credentials through explicit worker configuration. Fixture mode should not require credentials.

- Host `.venv` drift leaks into uv workers:
  compose workers set `UV_PROJECT_ENVIRONMENT=/tmp/uv-project-env` so container Python packages do not depend on the host `.venv`.

- Empty transcription smoke input:
  use speech-like audio for acceptance runs; a pure sine wave can legitimately produce an empty-transcript ASR diagnostic.
