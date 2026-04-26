# Cutover Checklist

## Preconditions

- Use `bash infra/scripts/compose-smoke.sh --check-config` as the static topology preflight.
- Keep report/deep-research AI execution routed through `worker-agent-runner` for deterministic compose smoke.
- For real provider acceptance, use the agent-runner image/configuration that installs or exposes the provider launcher in the container and supplies credentials/config through explicit worker configuration.

## Final Acceptance Matrix

Run these commands in order:

```bash
bash infra/scripts/compose-smoke.sh --check-config
docker compose -f infra/docker-compose.yml config --services
docker compose -f infra/docker-compose.yml config
docker compose -f infra/docker-compose.yml build worker-transcription worker-agent-runner

cd apps/api
go test ./internal/api -run 'TestApiHttpRuntimeCreateReportReusesCanonicalChildWithoutExtraCreatedEvent|TestApiHttpRuntimeClaimUsesTranscriptAndReportArtifactsForDeepResearchChild' -count=1
go test ./internal/jobs -run 'TestApiJobsCreateChildJobReusesCanonicalChildAndValidatesArtifacts|TestApiJobsRetryCreatesNewLineageLinkedRow' -count=1
go test ./internal/storage -run TestApiStorageRepositoryDelegatesSubmissionAndJobQueries -count=1

cd ../..
uv run pytest --no-cov tests/test_bot.py -k 'handle_generate_report'
uv run pytest --no-cov tests/test_main.py
```

Container-native live e2e is accepted only after report and deep-research child jobs succeed through API claim/finalize and MinIO artifact persistence using `worker-agent-runner` for AI execution. The earlier bridge-based replay remains historical evidence only; it is not sufficient for this gate.

## Legacy Removal Gate

The old single-process runtime is considered replaced only when all of the following are true:

- compose live smoke passes with fixture transcription plus agent-runner images
- report child reuse is proven both by tests and by a repeated live create against the same transcription job
- Telegram deep callback uses `ReportArtifacts.job_id` only
- the legacy entrypoint prints the cutover message instead of starting local orchestration
- no open GRACE cutover items remain except future conditional notes
