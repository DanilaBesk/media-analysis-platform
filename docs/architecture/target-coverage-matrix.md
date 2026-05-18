# Target Coverage Matrix And Deterministic Test Environment

Status: active coverage seed for `media-7f3.10.1`  
Source plan: `docs/architecture/single-user-channel-aware-target-architecture.md`  
Fixture manifest: `infra/fixtures/target/manifest.json`

This document is the target rebuild coverage map. It is not a final readiness claim. It names every source-plan area that must be proven, points to the current implementation or fixture evidence, and routes remaining proof into the coverage and QA Beads.

## 10.1 Acceptance Map

| Requirement | Evidence now | Remaining gate |
| --- | --- | --- |
| Coverage matrix maps each source-plan requirement to implementation proof, test proof, and QA proof. | This file maps data reset, tables, API operations, user flows, failure modes, compatibility rules, non-goals, and app boundaries. | `media-7f3.10.2`, `media-7f3.10.3`, `media-7f3.10.4`, `media-7f3.11.*` must turn open rows into runnable proof. |
| Deterministic DB reset can drop/recreate target schema. | `apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql`; `infra/scripts/target-reset-smoke.sh` applies it twice to fresh Postgres and validates target/legacy table state. | `media-7f3.10.2` must extend this into committed storage/API coverage for constraints and repository behavior. |
| Deterministic seed/channel fixtures exist. | `apps/api/internal/storage/target/fixtures.go`; `infra/fixtures/target/manifest.json`; `packages/contracts/tests/test_target_fixtures.py`. | `media-7f3.10.2` and `media-7f3.10.3` should use these identifiers in storage/API/E2E tests instead of ad hoc ids. |
| Object-store fixture bytes are known. | `infra/fixtures/target/object-store/media-inputs/document-note.txt` and `infra/fixtures/target/object-store/artifacts/run-summary/report.md` with size and SHA-256 in the manifest. | Runtime artifact/download tests in `media-7f3.10.3`. |
| Blockers are recorded honestly. | Open rows below are explicitly assigned to future Beads instead of hidden behind percentage coverage. | QA must challenge these assignments in `media-7f3.11.1`. |

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
| Current local database rows are disposable. | Migration begins with target and legacy `DROP TABLE IF EXISTS` statements. | `target-reset-smoke.sh` applies the migration twice against a fresh Postgres instance. | `media-7f3.10.2` for repository constraints after reset. |
| Clean schema initializes from empty database. | `apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql`. | `apps/api/internal/storage/target_schema_test.go`; `target-reset-smoke.sh`. | `media-7f3.10.2`. |
| Deterministic seed data exists. | `apps/api/internal/storage/target/fixtures.go`. | `TestDeterministicSeedFixturesAreStable`; manifest validator. | `media-7f3.10.2` and `media-7f3.10.3` must consume it. |
| Object-store fixtures are deterministic. | Manifest declares bucket, object key, content type, size, and SHA-256. | `test_target_fixture_manifest_has_stable_channel_accounts_and_media_bytes`. | `media-7f3.10.3` artifact/download runtime tests. |
| Channel identities are stable across adapters. | Manifest declares `local`, `telegram`, `web`, and `mcp` channel accounts. | Manifest validator enforces ids and refs. | Adapter/E2E tests in `media-7f3.10.3`. |

## Table Coverage Matrix

| Table | Required invariant | Implementation proof | Current automated proof | Remaining proof |
| --- | --- | --- | --- | --- |
| `channel_accounts` | Unique channel identity, active/disabled lifecycle, no product ownership. | migration, target store, channel account API. | schema fragments, target store SQL recording, deterministic seed smoke. | Cross-channel access matrix in `media-7f3.10.2`. |
| `operation_requests` | Idempotent mutation record scoped by channel account, operation type, key. | migration unique index; target store `RecordOperationRequest`. | target store SQL recording. | Replay/conflict tests in `media-7f3.10.2`. |
| `stored_objects` | Bucket/key uniqueness, checksum/size/content metadata, retention state. | migration; artifact/media store methods. | schema fragments, target store SQL recording, fixture manifest hashes. | Stored object lifecycle tests in `media-7f3.10.2` and runtime download in `media-7f3.10.3`. |
| `media_assets` | Channel-account-scoped material, accepted/quarantined/deleted lifecycle, inbox insertion. | target store and API media asset handlers. | target API tests, Web/MCP client tests, Telegram adapter tests from implementation slices. | Exhaustive validation and channel isolation in `media-7f3.10.2`. |
| `collections` | One active inbox per channel, versioned user collections. | migration unique index; target collection routes. | schema fragments, route tests, target store SQL recording. | Archive/restore/delete and conflict table in `media-7f3.10.2`. |
| `collection_items` | Ordered active membership, no duplicate active media asset per collection. | migration indexes; collection item handlers. | schema fragments, Web route and API tests. | Reorder/remove/pagination cases in `media-7f3.10.2`. |
| `selection_snapshots` | Immutable sealed execution input, copied options and access scope. | migration triggers; target selection service. | schema fragments, target store SQL recording, route tests. | Live trigger mutation proof and invalidation cases in `media-7f3.10.2`. |
| `selection_snapshot_items` | Ordered copied media facts, stable after collection/media edits. | migration; target store `CreateSelectionSnapshot`. | schema fragments, implementation tests. | Later collection mutation regression in `media-7f3.10.2`. |
| `analysis_runs` | Public run lifecycle from one sealed snapshot. | target runtime service and analysis run routes. | target API/Web/MCP/Telegram tests from implementation slices. | Success/error/idempotency/cancel matrix in `media-7f3.10.2`. |
| `analysis_run_steps` | Internal worker-claimable execution state behind analysis_run. | target runtime service, queue/claim/finalize handlers. | worker-common/transcription/agent-runner tests. | API lifecycle/cancel races in `media-7f3.10.2` and E2E in `media-7f3.10.3`. |
| `analysis_run_step_inputs` | Declared selection item or artifact inputs; no mutable collection reads. | target runtime planning and worker claim contracts. | worker tests for declared inputs and artifact input materialization. | Prerequisite planning proof in `media-7f3.10.2` and `media-7f3.10.3`. |
| `analysis_run_events` | Append-only status/progress/user-visible event stream. | target runtime event records and `/events` route. | route tests and worker progress tests. | Pagination/reconciliation/runtime event proof in `media-7f3.10.2`/`10.3`. |
| `artifacts` | Channel-scoped output metadata, preview/download, retention state. | target artifact service and worker artifact registration. | worker/common artifact tests, Web artifact tests, API target tests. | Real download/access path in `media-7f3.10.3`. |
| `artifact_subjects` | Lineage from artifact to run/step/snapshot/item/media/diagnostic. | target worker artifact registration. | target store SQL recording and worker tests. | End-to-end lineage assertion in `media-7f3.10.3`. |
| `diagnostics` | Stable code/message/context/remediation without secret leakage. | target diagnostic service and worker/adapter diagnostics. | contract failure taxonomy, worker tests, Web diagnostics tests. | Subject/severity/correlation matrix in `media-7f3.10.2`. |
| `channel_surfaces` | Restart-safe external presentation mapping, active uniqueness and supersede. | migration indexes; Telegram surface recovery; internal routes. | channel surface tests from implementation slices. | Runtime restart proof in `media-7f3.10.3`. |
| `channel_surface_subjects` | Surface-to-domain subject links, one primary subject per surface. | target store upsert and subject rebinding. | target store SQL recording, Telegram tests. | Duplicate/rebind matrix in `media-7f3.10.2`. |
| `channel_surface_events` | Append-only surface lifecycle and recovery history. | target store supersede/event APIs. | target store SQL recording, Telegram tests. | Event history runtime proof in `media-7f3.10.3`. |

## API Operation Matrix

| Operation group | Paths | Required proof | Current proof | Remaining proof |
| --- | --- | --- | --- | --- |
| Media assets | `POST /v1/media-assets`, `POST /v1/media-assets/upload`, `GET /v1/media-assets`, `GET/DELETE /v1/media-assets/{media_asset_id}` | Success, validation failure, empty/paginated list, delete lifecycle, channel-account mismatch. | OpenAPI contract tests, Web/MCP/Telegram client tests, target API tests. | Full handler/service matrix in `media-7f3.10.2`. |
| Collections | `GET /v1/collections/inbox`, `POST/GET /v1/collections`, `GET/PATCH /v1/collections/{collection_id}`, `PUT /items`, `DELETE /items/{media_asset_id}` | Inbox creation, list empty arrays, add/remove/reorder/archive/delete, version conflict. | Web route tests, OpenAPI route checks. | Complete mutation/conflict coverage in `media-7f3.10.2`. |
| Selection snapshots | `POST /v1/selection-snapshots`, `GET /v1/selection-snapshots/{selection_snapshot_id}` | Ordered item validation, sealed immutability, option snapshot, invalid media diagnostics. | Web/MCP/Telegram tests, schema checks. | Invalid/cross-channel cases in `media-7f3.10.2`. |
| Analysis runs | `POST/GET /v1/analysis-runs`, `GET /v1/analysis-runs/{analysis_run_id}`, `POST /cancel`, `POST /retry`, `GET /events` | Idempotency, run planning, prerequisite diagnostics, cancellation lifecycle, pagination/events. | target runtime tests, Web route tests, worker tests. | Full lifecycle and prerequisite matrix in `media-7f3.10.2`. |
| Artifacts | `GET /v1/artifacts`, `GET /v1/artifacts/{artifact_id}`, `POST /refresh`, `GET /analysis-runs/{id}/artifacts`, internal download access | Preview/download metadata, unavailable states, retention and channel access. | Web artifact tests, worker artifact tests, OpenAPI tests. | Real object access in `media-7f3.10.3`. |
| Diagnostics | `GET /v1/diagnostics`, internal run diagnostics | Subject filters, severity/code/correlation, safe context, stable failure taxonomy. | contract tests and UI diagnostics tests. | Exhaustive query matrix in `media-7f3.10.2`. |
| Worker control plane | `/internal/v1/analysis-runs/queue`, `/steps/claim`, `/steps/progress`, `/steps/finalize`, `/steps/cancel-check`, artifact/diagnostic registration | Step-kind polling, claim leases, declared inputs, progress, cancellation, artifact_subject lineage. | worker-common/transcription/agent-runner tests. | API service edge cases in `media-7f3.10.2`; E2E in `media-7f3.10.3`. |
| Channel account and surfaces | `/internal/v1/channel-accounts`, `/internal/v1/channel-surfaces`, `/active`, `/display-state`, `/events`, `/supersede` | Deterministic identity, active recovery, display-state replacement, supersede, event history. | Telegram/channel surface implementation tests. | Restart and duplicate-delivery E2E in `media-7f3.10.3`. |
| Admin/runtime | `/v1/admin/observability`, `/v1/admin/reconcile-queue`, `/v1/ws` | Observability semantics, queue recovery, websocket/event stream sanity. | Existing tests and docs. | Coverage inventory/no-legacy gate in `media-7f3.10.4`; QA runtime audit. |
| Deprecated compatibility | `/v1/media-items`, `/v1/selections`, legacy internal execution routes | Explicit deprecation, one-to-one mapping, excluded from target clients and normal UI. | contract staleness tests and implementation stale scans. | No-legacy regression gate in `media-7f3.10.4` and QA traceability. |

## User Flow Matrix

| Flow | Required proof | Current proof | Remaining proof |
| --- | --- | --- | --- |
| Add media from Telegram | Resolve channel_account, create stored_object/media_asset, append inbox, update current_materials_panel. | Telegram adapter tests from `media-7f3.9.5`. | Runtime with deterministic fixtures in `media-7f3.10.3`. |
| Start transcription | Create selection_snapshot, analysis_run, step, task surface, clear current collection. | Telegram/Web route tests and worker queue tests. | Active-run/no-dead-end E2E in `media-7f3.10.3`. |
| Worker completes run | Claim step, read snapshot, write artifact, publish artifact_subject, finalize, deliver result surface. | worker tests and Telegram result surface tests. | Real artifact/download path and duplicate prevention in `media-7f3.10.3`. |
| Report/research from transcript | API ensures transcript/text-corpus prerequisite, agent-runner consumes artifacts only, missing prerequisites emit diagnostics. | worker agent-runner tests and target API planning tests. | Speech prerequisite E2E in `media-7f3.10.3`. |
| Restart recovery | List active surfaces, resume watchers, restore current materials, supersede uneditable messages. | Telegram channel surface tests. | Runtime restart proof in `media-7f3.10.3`. |

## Failure Mode Matrix

| Failure code or class | Required target behavior | Current proof | Remaining proof |
| --- | --- | --- | --- |
| `media_asset_invalid` | Rejected input remains visible via diagnostic or error envelope. | contract taxonomy and adapter tests. | API/storage invalid-input matrix in `media-7f3.10.2`. |
| `channel_account_mismatch` | Cross-channel read/mutation fails before storage mutation. | target route tests and contract docs. | Exhaustive entity matrix in `media-7f3.10.2`; runtime denial in `media-7f3.10.3`. |
| `collection_version_conflict` | Stale collection mutation returns conflict and preserves state. | storage/API tests. | All collection mutations in `media-7f3.10.2`. |
| `selection_snapshot_invalid` and `selection_snapshot_not_sealed` | Run creation rejects invalid or unsealed snapshots with diagnostics. | target API tests. | Full validation matrix in `media-7f3.10.2`. |
| `stored_object_unavailable` | Run/artifact access records unavailable state, not silent disappearance. | worker/materialization tests. | Runtime artifact proof in `media-7f3.10.3`. |
| `analysis_prerequisite_missing`, `analysis_prerequisite_failed`, `analysis_prerequisite_unavailable` | Prerequisite gaps surface in run diagnostics and block unsafe agent-runner raw speech processing. | worker/API planning tests. | E2E prerequisite matrix in `media-7f3.10.3`. |
| `worker_failed` | Worker failure creates diagnostic and preserves partial evidence. | worker tests. | Cross-worker E2E in `media-7f3.10.3`. |
| `artifact_unavailable` | Artifact preview/download reflects failed/expired/deleted state. | Web and contract tests. | Real object-store path in `media-7f3.10.3`. |
| `retention_denied` | Retention refuses unsafe deletion and preserves run lineage. | existing retention tests. | Target storage/API retention matrix in `media-7f3.10.2`; runtime in `10.3`. |
| Adapter conflict/restart errors | Channel surfaces supersede or recover without domain corruption. | Telegram surface tests. | Restart runtime proof in `media-7f3.10.3`. |

## Boundary, Compatibility, And Non-Goal Matrix

| Source-plan rule | Current evidence | Future proof |
| --- | --- | --- |
| API owns product state; adapters are thin. | Target runtime service, Telegram/Web/MCP clients, GRACE docs. | Adapter E2E in `media-7f3.10.3`; QA in `media-7f3.11.3`. |
| Workers consume sealed snapshots and declared step inputs, not mutable collections. | Worker tests and API step input contracts. | E2E worker proof in `media-7f3.10.3`; backend QA in `media-7f3.11.2`. |
| Web normal copy avoids load-bearing internal terms. | Web route tests and stale scans from `media-7f3.9.7`/`9.8`. | No-legacy gate in `media-7f3.10.4`; channel UX QA in `media-7f3.11.3`. |
| MCP may use technical target vocabulary only. | MCP tool registry tests and target schemas. | No-legacy gate in `media-7f3.10.4`. |
| Deprecated names appear only in explicit compatibility/migration/historical contexts. | OpenAPI compatibility tests and cleanup stale scans. | Regression gate in `media-7f3.10.4`; QA traceability in `media-7f3.11.1`. |
| No owners/workspaces/team authorization. | Requirements and migration tests forbid target storage owners/workspaces. | Staleness and schema gates in `media-7f3.10.4`. |
| No public Telegram-specific API. | Channel surfaces are internal; public API uses channel_account/product terms. | QA API review in `media-7f3.11.2`. |
| No local durable Telegram state outside API. | Telegram surface recovery tests. | Runtime restart proof in `media-7f3.10.3`. |
| No current local DB preservation/backfill requirement. | Migration reset policy and target reset smoke. | Coverage and QA must not require old rows. |

## Bead Handoff

`media-7f3.10.2` must convert this matrix into storage/API tests for clean reset, constraints, idempotency, pagination, channel isolation, lifecycle, diagnostics, and retention.

`media-7f3.10.3` must convert this matrix into adapter/worker/runtime E2E tests using the deterministic channel and object-store fixture catalog.

`media-7f3.10.4` must wire the inventory/no-legacy gates so this matrix cannot drift silently.

`media-7f3.11.*` must audit that every "remaining proof" row either became evidence, was fixed, was accepted, or is a named blocker.
