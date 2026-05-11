# Cutover Checklist

This checklist tracks acceptance for the final inbox-first architecture. The former public job control plane is retired as target guidance; acceptance is based on API-owned media accumulation, immutable selections, analysis runs, artifacts, diagnostics, and thin adapters.

## Preconditions

- `bash infra/scripts/compose-smoke.sh --check-config` passes as the static topology preflight.
- PostgreSQL exposes the final media model: sources, media_items, collections, collection_items, selections, analysis_runs, artifacts, diagnostics, and internal execution rows.
- Public contracts expose inbox-first routes only: media ingestion, collection management, selection creation/read, analysis_run lifecycle, artifact access, and diagnostics query.
- Worker execution is internal to analysis_run and consumes sealed selection snapshots rather than mutable inbox or collection state.

## Final Acceptance Matrix

Run focused checks in this order:

```bash
bash infra/scripts/compose-smoke.sh --check-config
docker compose -f infra/docker-compose.yml config --services
docker compose -f infra/docker-compose.yml config

cd apps/api
go test ./internal/storage -run 'Media|Selection|AnalysisRun|Artifact|Diagnostic|Retention' -count=1
go test ./internal/api -run 'Media|Collection|Selection|AnalysisRun|Artifact|Diagnostic|Retention' -count=1
go test ./internal/queue ./internal/ws -run 'AnalysisRun|RunEvent|CollectionEvent' -count=1

cd ../..
uv run pytest --no-cov apps/telegram-bot/tests -k 'media or collection or selection or analysis_run or diagnostic'
pnpm --filter web test
pnpm --filter mcp-server test
```

For the authoritative final order, exact `PYTHONPATH` values, runtime gate, and coverage truth, use:

```bash
docs/architecture/final-closure-matrix.md
```

## Coverage Inventory Gate

Use the executable inventory before calling the repo "fully covered":

```bash
bash infra/scripts/coverage-inventory.sh
```

Interpretation rules:

- A numeric coverage percentage only counts when the underlying tool can emit it for that surface.
- Pass/fail-only suites are still useful, but they do not satisfy a "100% coverage" claim by themselves.
- A missing coverage provider or unwired command is a gate gap, not a silent exclusion.
- Runtime smoke, compose boot, and GRACE/XML validation stay as separate acceptance gates and must not be rolled into a fake global coverage percentage.

## Live Acceptance

Container-native live smoke is accepted only after all of the following pass through the public API and shared adapter state:

- media can be added from text, URL, file, image/photo, audio/voice, video, and document inputs;
- accepted media appears in the owner inbox without requiring a user-facing execution mode;
- collections can be created, version-checked, mutated, archived, and restored;
- selections are immutable snapshots and do not change when the source inbox or collection changes later;
- analysis_run creation requires a sealed selection and records lifecycle state, diagnostics, and artifact summaries;
- cancellation is cooperative and visible through analysis_run state and diagnostics;
- artifacts can be previewed or resolved through owner-scoped artifact routes;
- Telegram, Web, and MCP operate as thin clients over the same API-owned state.

## Coverage Inventory

Use one executable inventory command before making any claim about test closure:

```bash
bash infra/scripts/coverage-inventory.sh
```

Current measurable baselines from the active tree:

- Go `apps/api/internal/api`: `49.5%`
- Go `apps/api/internal/storage`: `31.7%`
- Python `workers/common/src/transcriber_workers_common`: `95%`
- Python `workers/agent-runner/src/transcriber_worker_agent_runner.py`: `86%`
- Python `workers/transcription/src/transcriber_worker_transcription.py`: `91%`
- Python `apps/telegram-bot/src/telegram_adapter`: `81%`
- Node `apps/mcp-server/src`: `95.67%`

Known unmeasured gap:

- `apps/web` has passing suites but no configured line-coverage provider, so a repo-wide `100% coverage` statement remains unproven.

## Legacy Removal Gate

Legacy job-based product guidance is considered removed only when:

- GRACE XML docs and active architecture docs reference the inbox-first model as the target;
- stale scans show no active public job-route, job identifier, or job module target references;
- remaining historical notes, if any, are explicitly marked superseded and excluded from implementation guidance;
- adapter tests prove the UX centers on accumulated media, explicit selections, and analysis runs.
