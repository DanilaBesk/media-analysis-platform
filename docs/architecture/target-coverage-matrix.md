# Target Coverage Matrix And Deterministic Test Environment

Status: coverage epic `media-7f3.10` closed; QA readiness remains in `media-7f3.11`
Source plan: `docs/architecture/single-user-channel-aware-target-architecture.md`  
Fixture manifest: `infra/fixtures/target/manifest.json`
CopperASR E2E fixture provenance: `docs/architecture/copper-asr-e2e-fixtures.md`

This document is the target rebuild coverage map. It is not a final readiness claim. It names every source-plan area that must be proven, points to the current implementation or fixture evidence, and routes remaining proof into the coverage and QA Beads.

## 10.1 Acceptance Map

| Requirement | Evidence now | Remaining gate |
| --- | --- | --- |
| Coverage matrix maps each source-plan requirement to implementation proof, test proof, and QA proof. | This file maps data reset, tables, API operations, user flows, failure modes, compatibility rules, non-goals, and app boundaries. `media-7f3.10.2` added storage/API proof. `media-7f3.10.3` added adapter, worker, and compose runtime proof. `media-7f3.10.4` added runnable inventory/no-legacy gates. | `media-7f3.11.*` must challenge any stale row assignments before final readiness. |
| Deterministic DB reset can drop/recreate target schema. | `apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql`; `infra/scripts/target-reset-smoke.sh`; `TestTargetStorePostgresContracts` applies it twice to fresh Postgres and validates target/legacy table state; `bash infra/scripts/compose-smoke.sh --live-smoke` now force-recreates the compose stack before runtime proof; `bash infra/scripts/no-legacy-target-gate.sh` rejects recreated legacy target tables and columns. | QA must keep reset proof in the final audit. |
| Deterministic seed/channel fixtures exist. | `apps/api/internal/storage/target/fixtures.go`; `infra/fixtures/target/manifest.json`; `packages/contracts/tests/test_target_fixtures.py`; `TestTargetStorePostgresContracts` consumes the deterministic local channel/inbox seed; `runtime-final-e2e.py` uses unique runtime channel_accounts with deterministic fixture harness output to avoid stale idempotency collisions. | QA should decide whether future CI needs fixed manifest ids for the live smoke instead of unique throwaway runtime ids. |
| Object-store fixture bytes are known. | `infra/fixtures/target/object-store/media-inputs/document-note.txt` and `infra/fixtures/target/object-store/artifacts/run-summary/report.md` with size and SHA-256 in the manifest; `runtime-final-e2e.py` resolves internal artifact download access and downloads non-empty MinIO bytes from the real compose object path; the coverage inventory now runs the compose topology/runtime-proof wiring check. | QA must keep artifact/download proof visible in the final audit. |
| Blockers are recorded honestly. | Open rows below are explicitly assigned to future Beads instead of hidden behind percentage coverage. `coverage-inventory.sh` reports native per-surface metrics and pass/fail-only surfaces separately instead of claiming a fake repo-wide 100%. | QA must challenge these assignments in `media-7f3.11.1`. |

## 10.2 Storage And API Coverage Evidence

`media-7f3.10.2` turns the storage/API rows below into runnable proof. The new evidence is intentionally split between live Postgres storage constraints and API/service edge behavior:

| Requirement | Evidence |
| --- | --- |
| Clean schema reset/recreate and disposable local rows. | `TestTargetStorePostgresContracts` applies `0001_final_inbox_analysis_run_schema.sql` twice to disposable `postgres:16-alpine`, verifies all 18 target tables, forbidden legacy tables, and immutable snapshot triggers. |
| Deterministic seed fixtures are consumed by storage tests. | `TestTargetStorePostgresContracts` inserts `target.DeterministicSeedFixtures()` and uses stable channel/inbox ids as the local channel fixture. |
| Success, validation failure, conflict, empty array, and pagination API behavior. | `TestTargetApiCanonicalRoutesUseTargetVocabulary` and `TestTargetApiEdgeCoverageForValidationConflictAndPagination` cover target route success, invalid JSON/form errors, empty arrays, page-size clamping, and collection conflict mapping. |
| Operation idempotency replay. | `target.Store.RecordOperationRequest` now preserves the first operation target on replay; `TestTargetRuntimeServiceReplaysMediaAssetIdempotencyKey` proves duplicate media_asset create requests return the original target and do not create a second inbox item. |
| Channel-account isolation. | `TestTargetStorePostgresContracts` proves cross-channel `media_asset` and `artifact` access returns no rows before exposing data. |
| Collection lifecycle and optimistic conflicts. | `TestTargetStorePostgresContracts` proves collection version increments, stale version rejection, and duplicate active collection positions are rejected by target constraints. |
| Immutable selection snapshots and copied item facts. | `TestTargetStorePostgresContracts` proves update triggers reject snapshot mutation and later `media_assets` edits do not change `selection_snapshot_items`. |
| analysis_run, step, input, events, cancellation, artifacts, diagnostics, and retention. | `TestTargetStorePostgresContracts` creates run graph, claims declared inputs, records progress/cancel/finalize events, forces late finalize to canceled, records artifacts plus `artifact_subjects`, records diagnostics, and verifies stored object `retention_state`. |
| channel_surface uniqueness, subject rebinding, version conflict, supersede, and events. | `TestTargetStorePostgresContracts` proves active key uniqueness, one primary subject, display-state expected-version conflicts, supersede removal from active recovery, and append-only surface events. |

Validation commands:

```bash
(cd apps/api && go test ./internal/storage/target -run TestTargetStorePostgresContracts -count=1 -v)
(cd apps/api && go test ./internal/api -count=1)
(cd apps/api && go test ./internal/storage/target -count=1)
(cd apps/api && go test ./internal/storage -count=1)
```

## 10.3 Adapter, Worker, And Runtime Evidence

`media-7f3.10.3` converts the adapter/worker/runtime rows into executable proof without reopening the target architecture. The evidence is split by surface so missing proof does not hide behind a single green result:

| Surface | Evidence |
| --- | --- |
| Target compose runtime | `bash infra/scripts/compose-smoke.sh --live-smoke` builds and force-recreates the compose stack, waits for health convergence, and runs `infra/scripts/runtime-final-e2e.py` against target-only public/internal routes. The runtime proof creates channel_accounts, creates a text media_asset, verifies inbox listing, seals a selection_snapshot, starts a report analysis_run, observes terminal success, resolves run events, resolves artifacts, fetches internal download access, downloads MinIO bytes, checks diagnostics shape, denies cross-channel run access with `not_found`, soft-deletes the media_asset, and proves the sealed selection_snapshot/run history remains intact. |
| Runtime target vocabulary | `infra/scripts/compose-smoke.sh --check-config` requires `/internal/v1/channel-accounts`, `channel_account_id`, `/v1/media-assets`, `/v1/selection-snapshots`, `selection_snapshot_id`, and `/internal/v1/artifacts/` in the runtime proof script, and rejects `/v1/media-items`, `/v1/selections`, `owner_type`, `owner_id`, and `media_item_id`. |
| Worker claim DTO compatibility | `workers/common/tests/test_api.py::test_claim_analysis_run_accepts_target_selection_snapshot_metadata` proves worker claim responses accept target selection_snapshot metadata fields returned by the live API. |
| Internal artifact download path | `apps/api/internal/storage/runtime_store_media_test.go` covers fallback from the legacy artifact query shape to target artifact/stored_object rows, so `/internal/v1/artifacts/{artifact_id}/download-access` can resolve artifacts produced by target workers. |
| Cross-channel miss semantics | `apps/api/internal/api/target_runtime_test.go::TestTargetRuntimeServiceMapsCrossChannelRunMissToNotFound` proves target runtime storage misses map to `not_found` instead of leaking `sql.ErrNoRows` as HTTP 500. |
| Telegram adapter | `PYTHONPATH=apps/telegram-bot/src uv run --with aiogram --with python-dotenv pytest apps/telegram-bot/tests -q` covers restart recovery, duplicate-result prevention, active-run/no-dead-end behavior, cancel behavior, result actions, artifact delivery, diagnostics, and clear-after-result flows against the target adapter layer. |
| Worker suites | `PYTHONPATH=workers/common/src uv run pytest workers/common/tests -q`, `PYTHONPATH=workers/common/src:workers/agent-runner/src uv run pytest workers/agent-runner/tests -q`, and `PYTHONPATH=workers/common/src:workers/transcription/src uv run pytest workers/transcription/tests -q` cover sealed selection_snapshot consumption, declared step inputs, transcript/text-corpus prerequisite materialization, partial diagnostics, artifact registration, and cancellation behavior. |
| Web and MCP | `pnpm --dir apps/web test`, `pnpm --dir apps/mcp-server test`, and `pnpm --dir apps/mcp-server typecheck` cover human-facing Web target flows and MCP target tool vocabulary, lifecycle, artifacts, diagnostics, and structured error behavior. |

## 10.4 Coverage Inventory And No-Legacy Gate Evidence

`media-7f3.10.4` wires the proof from this matrix into executable gates and fixes the target-reset admin runtime drift found after `10.3`. The inventory now separates native coverage metrics from pass/fail-only probes so the project does not imply a synthetic repo-wide 100% number.

| Surface | Evidence |
| --- | --- |
| Coverage inventory | `bash infra/scripts/coverage-inventory.sh` reports native metrics for Go API/storage, worker-common, agent-runner, transcription, Telegram adapter, MCP, and Web, then runs pass/fail probes for contracts/fixtures, ASR no-legacy, target no-legacy vocabulary, compose topology/runtime-proof wiring, and MCP typecheck. |
| Latest inventory run | Go API/storage: 67.7% and 98.5% statements. Python worker-common, agent-runner, transcription, Telegram: 96%, 97%, 99%, and 89% statements. MCP: 100% line/branch/function. Web V8: 97.86% statements/lines, 90.96% branches, 100% functions. Pass/fail probes also passed, including the dedicated ASR no-legacy gate. |
| No-legacy ASR gate | `bash infra/scripts/no-legacy-asr-gate.sh` runs `workers/common/tests/test_no_legacy_asr_runtime.py`, which scans active runtime code, env, compose, dependency manifests, and runtime docs for removed ASR runtime terms while leaving historical research/planning documents outside the active scan. |
| No-legacy target gate | `bash infra/scripts/no-legacy-target-gate.sh` requires target snippets in `runtime-final-e2e.py`, rejects deprecated active route/table/DTO terms from target adapter/worker/Web/MCP/runtime paths, rejects legacy table/column recreation in the target reset migration, and runs `test_target_operations_do_not_reintroduce_compatibility_names`. |
| CopperASR Telegram E2E | `python3 infra/scripts/copper-asr-telegram-e2e.py --json` uses the short voice fixture through a live Telegram-shaped channel account, public upload, inbox collection, current materials surface, selection_snapshot, analysis_run, transcription worker, and CopperASR runtime. It verifies analysis_run_step planning, progress/finalize events, transcript plus policy artifacts, run_manifest provider metadata, artifact download bytes, diagnostics filtering, result_artifact_surface delivery recording, duplicate delivery prevention, and inbox collection clearing after successful delivery. |
| CopperASR API/worker/Web/MCP E2E | `python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json` uses the short voice fixture through the live API, transcription worker, and CopperASR runtime. It verifies upload, inbox listing, selection_snapshot creation, successful transcription, transcript artifacts, run_manifest provider metadata, artifact download access, diagnostics filtering, cross-channel run/artifact denial, and history preservation after source media deletion. Web and MCP tests cover the client/tool surfaces over these same artifact and diagnostic API shapes. |
| CopperASR failure/retry/cancel E2E | `python3 infra/scripts/copper-asr-failure-e2e.py --json` creates live API runs through the transcription worker and CopperASR service. It proves corrupt-audio failed runs and retries emit analysis_run diagnostics plus `run_manifest`/`run_diagnostics` policy artifacts, failed runs do not publish transcript artifacts, cancellation reaches `canceled` without artifacts, and resource-limit knobs are present. |
| CopperASR corrupt-audio classification follow-up | The target corrupt-audio diagnostic is `asr_invalid_audio`, but the rebuilt CopperASR runtime image from `vendor/copper-asr` commit `5184cd4452ac45f0d93fb3e00b6bae005cb597e5` still returns HTTP 500 provider code `unexpected_runtime_error` for the corrupt fixture after the platform preflight workaround was removed, including after the local compose CPU quota was applied. Follow-up source checks inspected CopperASR `origin/main`, all visible GitLab MR head/merge refs, tag refs, and the separate local CopperASR checkout; no hidden corrupt-audio classification fix was present, and every visible ref keeps the ffmpeg `CalledProcessError` to `RuntimeError` path. Consumer-side preflight is not approved for this migration cycle, so the manifest accepts both `asr_invalid_audio` and `asr_unexpected_runtime_error` for live failure E2E only until `media-b8s.2.11` fixes provider classification; final closure must use `python3 infra/scripts/copper-asr-failure-e2e.py --json --require-invalid-audio`. |
| Admin/runtime after target reset | `apps/api/internal/storage/runtime_store_media.go` treats missing legacy tables or columns as empty/no-op results for retention sweeps, orphan detection, legacy queue inventory, operational diagnostics, and admin runtime queue views. `TestRuntimeStoreRetentionAndOrphanErrorBranches` covers SQLSTATE `42P01` and `42703` target-reset drift. |
| Live admin probe | After API restart against the target-reset compose stack, `/v1/admin/observability` returned HTTP 200 with observability counters and `POST /v1/admin/reconcile-queue` returned HTTP 202 with `{"reconciled":0}`. A fresh log tail after restart had no new legacy table errors. |

## Deterministic Fixture Catalog

The fixture manifest uses only target vocabulary and stable ids:

- channel accounts: `local`, `telegram`, `web`, `mcp`;
- default inbox: `00000000-0000-4000-8000-000000000101`;
- media assets: one inline text material and one uploaded document material;
- object-store buckets: `media-inputs` and `artifacts`;
- selection snapshot, analysis run, and report artifact ids for end-to-end tests.

Validation command:

```bash
uv run pytest packages/contracts/tests/test_target_fixtures.py -q
```

Fresh Postgres reset smoke:

```bash
bash infra/scripts/target-reset-smoke.sh
```

If `TARGET_DATABASE_URL` is set, the reset smoke uses that database. Otherwise it starts a disposable `postgres:16-alpine` container, applies the migration twice, verifies the 18 target tables, verifies forbidden legacy tables are absent, checks immutable selection_snapshot triggers, and inserts the deterministic channel/inbox seed.

## Data Reset And Environment Matrix

| Source-plan rule | Implementation proof | Test or fixture proof | Remaining proof owner |
| --- | --- | --- | --- |
| Current local database rows are disposable. | Migration begins with target and legacy `DROP TABLE IF EXISTS` statements. | `target-reset-smoke.sh` and `TestTargetStorePostgresContracts` apply the migration twice against fresh Postgres instances. | Runtime compose reset in `media-7f3.10.3`. |
| Clean schema initializes from empty database. | `apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql`. | `apps/api/internal/storage/target_schema_test.go`; `target-reset-smoke.sh`; `TestTargetStorePostgresContracts`. | Runtime compose reset in `media-7f3.10.3`. |
| Deterministic seed data exists. | `apps/api/internal/storage/target/fixtures.go`. | `TestDeterministicSeedFixturesAreStable`; manifest validator; `TestTargetStorePostgresContracts` seed insertion. | Adapter/runtime E2E in `media-7f3.10.3` must consume it. |
| Object-store fixtures are deterministic. | Manifest declares bucket, object key, content type, size, and SHA-256. | `test_target_fixture_manifest_has_stable_channel_accounts_and_media_bytes`. | `media-7f3.10.3` artifact/download runtime tests. |
| CopperASR E2E fixtures are deterministic. | Manifest declares short voice, representative long voice, corrupt audio, cancellation voice, and artifact download cases under `copper_asr_e2e`; `infra/scripts/copper-asr-e2e-harness.py` validates hashes and can copy object-store bytes into a deterministic bucket/key tree; `infra/scripts/copper-asr-telegram-e2e.py` and `infra/scripts/copper-asr-api-web-mcp-e2e.py` consume the short voice case against the live stack; `infra/scripts/copper-asr-failure-e2e.py` consumes the corrupt and cancellation cases; `infra/scripts/copper-asr-benchmark-e2e.py` consumes the representative long voice and writes `docs/benchmarks/copper-asr-long-voice-benchmark-latest.json`. | `test_copper_asr_e2e_fixture_manifest_covers_required_inputs_and_hashes`; `test_copper_asr_e2e_harness_reports_deterministic_fixture_plan`; `test_copper_asr_failure_e2e_exposes_strict_invalid_audio_gate`; `test_copper_asr_long_voice_benchmark_artifact_records_runtime_thresholds`; `python3 infra/scripts/copper-asr-telegram-e2e.py --json`; `python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json`; `python3 infra/scripts/copper-asr-failure-e2e.py --json`; `python3 infra/scripts/copper-asr-failure-e2e.py --json --require-invalid-audio` after provider fix; `python3 infra/scripts/copper-asr-benchmark-e2e.py --json --write-artifact docs/benchmarks/copper-asr-long-voice-benchmark-latest.json --blocker-issue-id media-b8s.2.10`. | `media-b8s.2.7` now passes the long-voice benchmark with compose `COPPER_ASR_LOCAL_CPUS=4.0`: `run_wall_seconds=175.232`, `speedup_vs_realtime=5.478`, `max_cpu_percent=408.78 <= 450.0`, and `thresholds.passed=true`; `media-b8s.2.6` remains blocked only on `media-b8s.2.9` / invalid-audio classification. |
| Channel identities are stable across adapters. | Manifest declares `local`, `telegram`, `web`, and `mcp` channel accounts. | Manifest validator enforces ids and refs. | Adapter/E2E tests in `media-7f3.10.3`. |

## Table Coverage Matrix

| Table | Required invariant | Implementation proof | Current automated proof | Remaining proof |
| --- | --- | --- | --- | --- |
| `channel_accounts` | Unique channel identity, active/disabled lifecycle, no product ownership. | migration, target store, channel account API. | schema fragments, target store SQL recording, deterministic seed smoke, `TestTargetStorePostgresContracts`. | Runtime adapter identity proof in `media-7f3.10.3`. |
| `operation_requests` | Idempotent mutation record scoped by channel account, operation type, key. | migration unique index; target store `RecordOperationRequest`. | target store SQL recording, `TestTargetStorePostgresContracts`, `TestTargetRuntimeServiceReplaysMediaAssetIdempotencyKey`. | Runtime duplicate-delivery proof in `media-7f3.10.3`. |
| `stored_objects` | Bucket/key uniqueness, checksum/size/content metadata, retention state. | migration; artifact/media store methods. | schema fragments, target store SQL recording, fixture manifest hashes, live retention-state storage proof. | Runtime download in `media-7f3.10.3`. |
| `media_assets` | Channel-account-scoped material, accepted/quarantined/deleted lifecycle, inbox insertion. | target store and API media asset handlers. | target API tests, Web/MCP/Telegram client tests, Telegram adapter tests, live channel isolation and delete lifecycle proof. | Runtime ingestion proof in `media-7f3.10.3`. |
| `collections` | One active inbox per channel, versioned user collections. | migration unique index; target collection routes. | schema fragments, route tests, target store SQL recording, live version conflict proof. | Runtime collection mutation proof in `media-7f3.10.3`. |
| `collection_items` | Ordered active membership, no duplicate active media asset per collection. | migration indexes; collection item handlers. | schema fragments, Web route and API tests, live duplicate active position rejection. | Runtime reorder/remove proof in `media-7f3.10.3`. |
| `selection_snapshots` | Immutable sealed execution input, copied options and access scope. | migration triggers; target selection service. | schema fragments, target store SQL recording, route tests, live trigger mutation proof. | Runtime snapshot creation proof in `media-7f3.10.3`. |
| `selection_snapshot_items` | Ordered copied media facts, stable after collection/media edits. | migration; target store `CreateSelectionSnapshot`. | schema fragments, implementation tests, live later media edit regression. | Runtime worker input proof in `media-7f3.10.3`. |
| `analysis_runs` | Public run lifecycle from one sealed snapshot. | target runtime service and analysis run routes. | target API/Web/MCP/Telegram tests, live run graph/cancel/finalize event proof. | Runtime active-run proof in `media-7f3.10.3`. |
| `analysis_run_steps` | Internal worker-claimable execution state behind analysis_run. | target runtime service, queue/claim/finalize handlers. | worker-common/transcription/agent-runner tests, live queue/claim/cancel/finalize proof. | Worker runtime E2E in `media-7f3.10.3`. |
| `analysis_run_step_inputs` | Declared selection item or artifact inputs; no mutable collection reads. | target runtime planning and worker claim contracts. | worker tests for declared inputs and artifact input materialization, live declared selection item claim proof. | Worker runtime E2E in `media-7f3.10.3`. |
| `analysis_run_events` | Append-only status/progress/user-visible event stream. | target runtime event records and `/events` route. | route tests, worker progress tests, live ordered created/progress/cancel/finalize event proof. | Reconciliation/runtime event proof in `media-7f3.10.3`. |
| `artifacts` | Channel-scoped output metadata, preview/download, retention state. | target artifact service and worker artifact registration. | worker/common artifact tests, Web artifact tests, API target tests, live channel-scoped artifact access and retained stored_object proof. | Real download/access path in `media-7f3.10.3`. |
| `artifact_subjects` | Lineage from artifact to run/step/snapshot/item/media/diagnostic. | target worker artifact registration. | target store SQL recording, worker tests, live artifact_subject insert proof. | End-to-end lineage assertion in `media-7f3.10.3`. |
| `diagnostics` | Stable code/message/context/remediation without secret leakage. | target diagnostic service and worker/adapter diagnostics. | contract failure taxonomy, worker tests, Web diagnostics tests, live subject/severity/code/correlation query proof. | Runtime diagnostic proof in `media-7f3.10.3`. |
| `channel_surfaces` | Restart-safe external presentation mapping, active uniqueness and supersede. | migration indexes; Telegram surface recovery; internal routes. | channel surface tests from implementation slices, live active uniqueness/version/supersede proof. | Runtime restart proof in `media-7f3.10.3`. |
| `channel_surface_subjects` | Surface-to-domain subject links, one primary subject per surface. | target store upsert and subject rebinding. | target store SQL recording, Telegram tests, live primary uniqueness and rebind proof. | Runtime restart proof in `media-7f3.10.3`. |
| `channel_surface_events` | Append-only surface lifecycle and recovery history. | target store supersede/event APIs. | target store SQL recording, Telegram tests, live display-state and supersede event history proof. | Event history runtime proof in `media-7f3.10.3`. |

## API Operation Matrix

| Operation group | Paths | Required proof | Current proof | Remaining proof |
| --- | --- | --- | --- | --- |
| Media assets | `POST /v1/media-assets`, `POST /v1/media-assets/upload`, `GET /v1/media-assets`, `GET/DELETE /v1/media-assets/{media_asset_id}` | Success, validation failure, empty/paginated list, delete lifecycle, channel-account mismatch. | OpenAPI contract tests, Web/MCP/Telegram client tests, target API tests, live channel isolation/delete/idempotency proof. | Runtime ingestion proof in `media-7f3.10.3`. |
| Collections | `GET /v1/collections/inbox`, `POST/GET /v1/collections`, `GET/PATCH /v1/collections/{collection_id}`, `PUT /items`, `DELETE /items/{media_asset_id}` | Inbox creation, list empty arrays, add/remove/reorder/archive/delete, version conflict. | Web route tests, OpenAPI route checks, live optimistic version and active-position conflict proof. | Runtime mutation proof in `media-7f3.10.3`. |
| Selection snapshots | `POST /v1/selection-snapshots`, `GET /v1/selection-snapshots/{selection_snapshot_id}` | Ordered item validation, sealed immutability, option snapshot, invalid media diagnostics. | Web/MCP/Telegram tests, schema checks, live immutability and copied item fact proof. | Runtime creation proof in `media-7f3.10.3`. |
| Analysis runs | `POST/GET /v1/analysis-runs`, `GET /v1/analysis-runs/{analysis_run_id}`, `POST /cancel`, `POST /retry`, `GET /events` | Idempotency, run planning, prerequisite diagnostics, cancellation lifecycle, pagination/events. | target runtime tests, Web route tests, worker tests, live graph/queue/claim/progress/cancel/finalize/event proof. | Runtime active-run proof in `media-7f3.10.3`. |
| Artifacts | `GET /v1/artifacts`, `GET /v1/artifacts/{artifact_id}`, `POST /refresh`, `GET /analysis-runs/{id}/artifacts`, internal download access | Preview/download metadata, unavailable states, retention and channel access. | Web artifact tests, worker artifact tests, OpenAPI tests. | Real object access in `media-7f3.10.3`. |
| Diagnostics | `GET /v1/diagnostics`, internal run diagnostics | Subject filters, severity/code/correlation, safe context, stable failure taxonomy. | contract tests, UI diagnostics tests, live diagnostic query matrix. | Runtime diagnostic proof in `media-7f3.10.3`. |
| Worker control plane | `/internal/v1/analysis-runs/queue`, `/steps/claim`, `/steps/progress`, `/steps/finalize`, `/steps/cancel-check`, artifact/diagnostic registration | Step-kind polling, claim leases, declared inputs, progress, cancellation, artifact_subject lineage. | worker-common/transcription/agent-runner tests, live API storage edge proof. | E2E in `media-7f3.10.3`. |
| Channel account and surfaces | `/internal/v1/channel-accounts`, `/internal/v1/channel-surfaces`, `/active`, `/display-state`, `/events`, `/supersede` | Deterministic identity, active recovery, display-state replacement, supersede, event history. | Telegram/channel surface implementation tests. | Restart and duplicate-delivery E2E in `media-7f3.10.3`. |
| Admin/runtime | `/v1/admin/observability`, `/v1/admin/reconcile-queue`, `/v1/ws` | Observability semantics, queue recovery, websocket/event stream sanity. | Existing tests and docs; target-reset legacy schema mismatch regression; live admin observability/reconcile probe after API restart. | QA runtime audit. |
| Deprecated compatibility | `/v1/media-items`, `/v1/selections`, legacy internal execution routes | Explicit deprecation, one-to-one mapping, excluded from target clients and normal UI. | contract staleness tests and implementation stale scans; `no-legacy-target-gate.sh` regression gate. | QA traceability. |

## User Flow Matrix

| Flow | Required proof | Current proof | Remaining proof |
| --- | --- | --- | --- |
| Add media from Telegram | Resolve channel_account, create stored_object/media_asset, append inbox, update current_materials_panel. | Telegram adapter tests from `media-7f3.9.5`; `python3 infra/scripts/copper-asr-telegram-e2e.py --json` proves the live voice-upload materialization and current materials surface path. | QA audit only. |
| Start transcription | Create selection_snapshot, analysis_run, step, task surface, clear current collection. | Telegram/Web route tests and worker queue tests; `python3 infra/scripts/copper-asr-telegram-e2e.py --json` proves live selection_snapshot, analysis_run_step planning, and analysis_task_surface persistence. | QA audit only. |
| Worker completes run | Claim step, read snapshot, write artifact, publish artifact_subject, finalize, deliver result surface. | worker tests and Telegram result surface tests; `python3 infra/scripts/copper-asr-telegram-e2e.py --json` proves live progress/finalize events, artifact downloads, result_artifact_surface recording, duplicate prevention, and inbox clearing. | QA audit only. |
| Report/research from transcript | API ensures transcript/text-corpus prerequisite, agent-runner consumes artifacts only, missing prerequisites emit diagnostics. | worker agent-runner tests and target API planning tests. | Speech prerequisite E2E in `media-7f3.10.3`. |
| Restart recovery | List active surfaces, resume watchers, restore current materials, supersede uneditable messages. | Telegram channel surface tests. | Runtime restart proof in `media-7f3.10.3`. |

## Failure Mode Matrix

| Failure code or class | Required target behavior | Current proof | Remaining proof |
| --- | --- | --- | --- |
| `media_asset_invalid` | Rejected input remains visible via diagnostic or error envelope. | contract taxonomy, adapter tests, API invalid JSON/form error tests, storage constraint failures. | Runtime invalid-input proof in `media-7f3.10.3`. |
| `channel_account_mismatch` | Cross-channel read/mutation fails before storage mutation. | target route tests, contract docs, live media_asset/artifact channel isolation proof. | Runtime denial in `media-7f3.10.3`. |
| `collection_version_conflict` | Stale collection mutation returns conflict and preserves state. | storage/API tests, live stale version rejection, API 409 mapping. | Runtime conflict proof in `media-7f3.10.3`. |
| `selection_snapshot_invalid` and `selection_snapshot_not_sealed` | Run creation rejects invalid or unsealed snapshots with diagnostics. | target API tests, live immutable trigger and copied fact proof. | Runtime invalid snapshot proof in `media-7f3.10.3`. |
| `stored_object_unavailable` | Run/artifact access records unavailable state, not silent disappearance. | worker/materialization tests. | Runtime artifact proof in `media-7f3.10.3`. |
| `analysis_prerequisite_missing`, `analysis_prerequisite_failed`, `analysis_prerequisite_unavailable` | Prerequisite gaps surface in run diagnostics and block unsafe agent-runner raw speech processing. | worker/API planning tests. | E2E prerequisite matrix in `media-7f3.10.3`. |
| `asr_invalid_audio` | Invalid or unsupported audio must fail the transcription run with analysis_run diagnostics and run policy artifacts, never transcript artifacts or a stuck active card. | worker RED/GREEN regression; `python3 infra/scripts/copper-asr-failure-e2e.py --json` covers live corrupt-audio failed run and retry with accepted live diagnostic codes; `--require-invalid-audio` is the strict final gate. | `media-b8s.2.9` is blocked by `media-b8s.2.11` until temporary acceptance of `asr_unexpected_runtime_error` is removed and the strict gate passes. |
| `asr_unexpected_runtime_error` | Unexpected CopperASR provider failures must still finalize the run, record ASR diagnostics, and publish only policy artifacts instead of surfacing generic backend-unavailable loops. | live corrupt-audio E2E currently observes this code from CopperASR HTTP 500 and verifies no transcript artifacts are published. | Provider classification follow-up in `media-b8s.2.11`; consumer preflight is not approved for this migration cycle. |
| `worker_failed` | Worker failure creates diagnostic and preserves partial evidence. | worker tests. | Cross-worker E2E in `media-7f3.10.3`. |
| `artifact_unavailable` | Artifact preview/download reflects failed/expired/deleted state. | Web and contract tests. | Real object-store path in `media-7f3.10.3`. |
| `retention_denied` | Retention refuses unsafe deletion and preserves run lineage. | existing retention tests, live target stored_object retention-state proof. | Runtime retention proof in `media-7f3.10.3`. |
| Adapter conflict/restart errors | Channel surfaces supersede or recover without domain corruption. | Telegram surface tests. | Restart runtime proof in `media-7f3.10.3`. |

## Boundary, Compatibility, And Non-Goal Matrix

| Source-plan rule | Current evidence | Future proof |
| --- | --- | --- |
| API owns product state; adapters are thin. | Target runtime service, Telegram/Web/MCP clients, GRACE docs. | Adapter E2E in `media-7f3.10.3`; QA in `media-7f3.11.3`. |
| Workers consume sealed snapshots and declared step inputs, not mutable collections. | Worker tests and API step input contracts. | E2E worker proof in `media-7f3.10.3`; backend QA in `media-7f3.11.2`. |
| Web normal copy avoids load-bearing internal terms. | Web route tests, stale scans from `media-7f3.9.7`/`9.8`, and the no-legacy gate from `media-7f3.10.4`. | Channel UX QA in `media-7f3.11.3`. |
| MCP may use technical target vocabulary only. | MCP tool registry tests, target schemas, and the no-legacy gate from `media-7f3.10.4`. | QA traceability in `media-7f3.11.1`. |
| Deprecated names appear only in explicit compatibility/migration/historical contexts. | OpenAPI compatibility tests, cleanup stale scans, and the no-legacy regression gate from `media-7f3.10.4`. | QA traceability in `media-7f3.11.1`. |
| No owners/workspaces/team authorization. | Requirements and migration tests forbid target storage owners/workspaces; no-legacy gate rejects owner/tenant columns from target paths. | QA traceability in `media-7f3.11.1`. |
| No public Telegram-specific API. | Channel surfaces are internal; public API uses channel_account/product terms. | QA API review in `media-7f3.11.2`. |
| No local durable Telegram state outside API. | Telegram surface recovery tests. | Runtime restart proof in `media-7f3.10.3`. |
| No current local DB preservation/backfill requirement. | Migration reset policy and target reset smoke. | Coverage and QA must not require old rows. |

## Bead Handoff

`media-7f3.10.2` has converted this matrix into storage/API tests for clean reset, constraints, idempotency, pagination, channel isolation, lifecycle, diagnostics, and retention.

`media-7f3.10.3` has converted this matrix into adapter/worker/runtime E2E proof for Telegram, worker-common, transcription, agent-runner, Web, MCP, API artifact/download access, and compose runtime target flow.

`media-7f3.10.4` has wired the inventory/no-legacy gates so this matrix cannot drift silently, including the observed legacy admin observability/reconcile/retention table references after target reset.

`media-7f3.10` is closed after a completion audit confirmed `media-7f3.10.1` through `media-7f3.10.4` are closed and the final coverage proof commit is pushed to `origin/master`.

`media-7f3.11.*` must audit that every "remaining proof" row either became evidence, was fixed, was accepted, or is a named blocker.
