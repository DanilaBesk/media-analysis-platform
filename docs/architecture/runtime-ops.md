# Runtime Ops

## Startup

Normal compose runtime is container-native. `worker-transcription` handles transcript generation, and report/deep-research AI execution is routed through the single `worker-agent-runner` model worker. Dedicated report/deep-research LLM worker services are no longer part of the compose topology.

```bash
bash infra/scripts/compose-smoke.sh --check-config
docker compose -f infra/docker-compose.yml up -d --build --wait
```

For a deterministic worker run, keep fixture/test-fixture agent-runner concurrency enabled in `infra/env/worker-agent-runner.env.example`. For a real provider run, configure the provider through the agent-runner env or secret mechanism. Do not route normal worker execution through a machine-local CLI bridge.

## Restart

```bash
docker compose -f infra/docker-compose.yml up -d --build --force-recreate worker-transcription worker-agent-runner
docker compose -f infra/docker-compose.yml up -d api web telegram-bot mcp-server
```

## Logs

```bash
docker compose -f infra/docker-compose.yml logs --tail=120 api
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
  use speech-like audio for acceptance runs; a pure sine wave can legitimately produce `Whisper returned an empty transcript`.
