# Cutover Checklist

This historical checklist tracks the earlier inbox-first acceptance model. Current target closure is governed by the GRACE XML docs plus `docs/architecture/single-user-channel-aware-target-architecture.md`; when terms conflict, the target media_asset, selection_snapshot, analysis_run, artifact, diagnostic, and channel_account vocabulary wins.

## Preconditions

- `bash infra/scripts/compose-smoke.sh --check-config` passes as the static topology preflight.
- PostgreSQL exposes the target media model: stored_objects, media_assets, collections, collection_items, selection_snapshots, analysis_runs, analysis_run_steps, artifact_subjects, artifacts, diagnostics, channel_accounts, and channel_surfaces.
- Public contracts expose target routes for media_asset ingestion, collection management, selection_snapshot creation/read, analysis_run lifecycle, artifact access, and diagnostics query.
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

Use the executable inventory before claiming the current coverage evidence is fresh:

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

- media_asset records can be added from text, URL, file, image/photo, audio/voice, video, and document inputs;
- accepted media appears in the channel-account inbox without requiring a user-facing execution mode;
- collections can be created, version-checked, mutated, archived, and restored;
- selection_snapshots are immutable and do not change when the origin inbox or collection changes later;
- analysis_run creation requires a sealed selection and records lifecycle state, diagnostics, and artifact summaries;
- cancellation is cooperative and visible through analysis_run state and diagnostics;
- artifacts can be previewed or resolved through channel-account artifact routes;
- Telegram, Web, and MCP operate as thin clients over the same API-owned state.

## Coverage Inventory

Use one executable inventory command before making any claim about test closure:

```bash
bash infra/scripts/coverage-inventory.sh
```

Current measurable baselines from the latest inventory run recorded in the active tree:

- Go `apps/api/internal/api`: `67.0%` statement coverage.
- Go `apps/api/internal/storage`: `98.5%` statement coverage.
- Python `workers/common/src/transcriber_workers_common`: `99%` statement coverage.
- Python `workers/agent-runner/src/transcriber_worker_agent_runner.py`: `97%` statement coverage.
- Python `workers/transcription/src/transcriber_worker_transcription.py`: `99%` statement coverage.
- Python `apps/telegram-bot/src/telegram_adapter`: `89%` statement coverage.
- Node `apps/mcp-server/src`: `100%`
- Web `apps/web/src`: `97.86%` statements/lines, `91.86%` branches, and `100%` functions

Current truth:

- `media-7f3.10` is closed with per-surface coverage proof; not every percentage-emitting surface is at `100%`, and the semantic coverage claim is backed by source-plan proof rather than a single line-coverage threshold.
- Runtime smoke, contract verification, and XML integrity remain separate non-percentage gates.

## Legacy Removal Gate

Legacy job-based product guidance is considered removed only when:

- GRACE XML docs and active architecture docs reference the inbox-first model as the target;
- stale scans show no active public job-route, job identifier, or job module target references;
- remaining historical notes, if any, are explicitly marked superseded and excluded from implementation guidance;
- adapter tests prove the UX centers on accumulated media, explicit selections, and analysis runs.
