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

## Legacy Removal Gate

Legacy job-based product guidance is considered removed only when:

- GRACE XML docs and active architecture docs reference the inbox-first model as the target;
- stale scans show no active public job-route, job identifier, or job module target references;
- remaining historical notes, if any, are explicitly marked superseded and excluded from implementation guidance;
- adapter tests prove the UX centers on accumulated media, explicit selections, and analysis runs.
