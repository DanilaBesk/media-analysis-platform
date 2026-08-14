# Target QA Traceability Audit

Status: completed evidence for `media-7f3.11.1`
Source plan: `docs/architecture/single-user-channel-aware-target-architecture.md`
Coverage map: `docs/architecture/target-coverage-matrix.md`

This historical audit checks whether the source plan, Beads graph, GRACE evidence, implementation files, and verification commands described one target rebuild. Its archived pre-migration baseline is retained in the external migration backup and Git history; current state is GRACE 4 under `.grace/`. It is not the final MR readiness packet. Backend/security/worker review remains in `media-7f3.11.2`, channel UX/Web/MCP/runtime review remains in `media-7f3.11.3`, and final readiness packaging remains in `media-7f3.11.4`.

## Executive Result

`media-7f3.9` and `media-7f3.10` are closed with committed proof. The Beads graph now has one active QA epic, `media-7f3.11`, with `media-7f3.11.1` in progress and `media-7f3.11.2` through `media-7f3.11.4` open. Traceability is coherent after fixing stale closure documentation found during this audit.

No source-plan requirement was found orphaned from implementation, coverage proof, or an explicit remaining QA owner. The remaining work is QA depth, not missing implementation decomposition.

## Prompt-To-Artifact Checklist

| Explicit requirement | Evidence inspected | Audit result |
| --- | --- | --- |
| Execute strictly from the source plan. | `docs/architecture/single-user-channel-aware-target-architecture.md` remains the source plan; the archived pre-migration baseline and target coverage matrix point back to it. Current GRACE 4 context, graph, verification, and active changes live under `.grace/`. | Pass. |
| Close implementation epic `media-7f3.9` before coverage. | `bd list --all` shows `media-7f3.9` and `media-7f3.9.1` through `media-7f3.9.8` closed. The archived pre-migration baseline records those slices as completed. | Pass. |
| Close full coverage epic `media-7f3.10` before QA. | `bd list --all` shows `media-7f3.10` and `media-7f3.10.1` through `media-7f3.10.4` closed. `docs/architecture/target-coverage-matrix.md` records coverage closure and the remaining QA stage. | Pass. |
| Continue with pre-MR QA epic `media-7f3.11`. | `bd ready --json` exposes `media-7f3.11.1`; it is now claimed. Dependent QA tasks `11.2`, `11.3`, and `11.4` remain open. | Pass. |
| Use Beads/GRACE rather than ad-hoc TODOs. | Beads state was refreshed with `bd context`, `bd ready --json`, `bd show`, and `bd list --all`; current planning and verification state is GRACE 4 under `.grace/`. | Pass. |
| Keep blockers honest. | The stale documentation drift below was fixed in this audit instead of being hidden. Remaining backend/channel/final readiness checks stay assigned to open QA Beads. | Pass. |

## Source-Plan Coverage Map

| Source-plan area | Implementation or artifact evidence | Test or gate evidence | Remaining QA owner |
| --- | --- | --- | --- |
| Purpose and data reset policy | Archived pre-migration requirements/development baseline, target reset migration, `infra/scripts/target-reset-smoke.sh`. Current context is `.grace/context/`. | `target-reset-smoke.sh`, `TestTargetStorePostgresContracts`, coverage inventory pass/fail probes. | `media-7f3.11.2` validates no hidden current-DB dependency. |
| FPF domains and target vocabulary | Archived pre-migration requirements baseline, `packages/contracts`, `apps/api/internal/api/target_runtime.go`, adapter clients. Current context is `.grace/context/requirements.xml`. | `packages/contracts/tests/test_contract_surfaces.py`, `infra/scripts/no-legacy-target-gate.sh`. | `media-7f3.11.1` traceability plus `11.2` and `11.3` semantic reviews. |
| Target table set and table contracts | `apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql`, `apps/api/internal/storage/target`. | `apps/api/internal/storage/target/store_postgres_test.go`, storage tests, target reset smoke. | `media-7f3.11.2`. |
| API operation groups | `apps/api/internal/api`, `packages/contracts/openapi/openapi.yaml`. | API tests, contract tests, runtime-final E2E, no-legacy gate. | `media-7f3.11.2`. |
| DTO and type naming | Go target types, shared contract schemas, worker DTO parsing, Web/MCP client types. | Contract tests, worker-common tests, no-legacy gate, MCP typecheck. | `media-7f3.11.1` for removed-surface isolation; `11.2` and `11.3` for code review depth. |
| App responsibilities: API | Target runtime service, target store, artifact/download and admin runtime fixes. | Go API/storage tests, runtime-final E2E, admin observability/reconcile live proof from `10.4`. | `media-7f3.11.2`. |
| App responsibilities: Telegram | `apps/telegram-bot/src/telegram_adapter` over target media_asset, collection, selection_snapshot, analysis_run, artifact, diagnostic, channel_surface APIs. | Telegram adapter tests and runtime proof. | `media-7f3.11.3`. |
| App responsibilities: Web | `apps/web/src` target client and human-facing routes. | Web tests, coverage inventory, no-legacy gate. | `media-7f3.11.3` challenges visible copy and user-flow ergonomics. |
| App responsibilities: MCP | `apps/mcp-server/src` target tool registry and API client. | MCP node tests, MCP typecheck, no-legacy gate. | `media-7f3.11.3`. |
| App responsibilities: workers | `workers/common`, `workers/transcription`, `workers/agent-runner` use analysis_run_step and declared inputs. | Worker-common, transcription, and agent-runner tests in coverage inventory. | `media-7f3.11.2`. |
| User flows | Target coverage matrix maps add media, start processing, worker completion, report/research prerequisites, and restart recovery. | Runtime-final E2E, Telegram/Web/MCP/worker suites. | `media-7f3.11.3` does exploratory UX/runtime challenge. |
| Implementation stages | Archived pre-migration execution packets covered implementation and coverage slices; current approved changes are in `.grace/changes/active/`. Beads graph has closed `9` and `10` epics. | `bd list --all` status audit. | `media-7f3.11.4` packages final readiness. |
| Verification matrix | Archived pre-migration verification baseline, `docs/architecture/final-closure-matrix.md`, `infra/scripts/coverage-inventory.sh`; current verification is `.grace/verification/{index,main}.xml`. | Coverage inventory, GRACE 4 validation, no-legacy gate, compose smoke, focused unit/integration suites. | `media-7f3.11.*` reviews depth and final evidence freshness. |
| Non-goals | Source plan and GRACE forbid owners/workspaces, public Telegram-specific API, local durable Telegram state outside API, user-facing job/task aliases, adapter projection aliases, and workers reading mutable collections. | Contract tests, no-legacy target gate, target reset migration checks, runtime and adapter suites. | `media-7f3.11.1` traceability; `11.2`/`11.3` deeper code/runtime review. |

## Special Traceability Checks

| Check | Evidence | Result |
| --- | --- | --- |
| Target contract proof | `packages/contracts/tests/test_contract_surfaces.py`, OpenAPI target operations, target coverage matrix. | Covered. |
| Removed-surface isolation proof | `infra/scripts/no-legacy-target-gate.sh`, contract negative tests, target coverage matrix removed-surface rows. | Covered; removed names remain only in negative assertions, guard patterns, or historical audit notes. |
| Web human-language proof | Web route tests, no-legacy gate, target coverage matrix boundary row. | Covered for committed tests; exploratory UX challenge remains in `media-7f3.11.3`. |
| Worker step-input and prerequisite proof | Worker-common claim parsing, transcription and agent-runner tests, runtime-final proof, target coverage matrix worker rows. | Covered for committed tests; backend/worker review remains in `media-7f3.11.2`. |
| Beads graph contradiction check | `bd list --all` shows superseded flat `media-7f3.1` through `media-7f3.8` closed, replacement epics `9` and `10` closed, QA epic `11` open. | Covered. |

## Findings

### QA-TRACE-001: stale closure and coverage wording

Status: fixed in this audit.

Evidence:

- `README.md` still listed older `100%` percentage baselines and said the target rebuild was not closed until both `media-7f3.10` and `media-7f3.11` complete.
- `docs/architecture/final-closure-matrix.md` still expected every measurable surface to emit `100%` and said `media-7f3.10` remained open.
- `docs/architecture/cutover-checklist.md` still listed older `100%` baselines.

Resolution:

- Replaced stale baselines with the latest `coverage-inventory.sh` baselines from `10.4`.
- Replaced fake `100%` threshold language with per-surface metric and pass/fail-only gate semantics.
- Updated closure wording to say `media-7f3.10` is closed and final target closure remains gated by `media-7f3.11`.

## Accepted Traceability Notes

- The live runtime proof uses unique throwaway channel_accounts instead of only fixed manifest ids. This is accepted for runtime E2E because it avoids stale idempotency collisions while deterministic fixture ids and object bytes are still validated separately.
- Line/statement coverage percentages are not the semantic definition of target rebuild closure. The semantic closure target is source-plan proof for table invariants, API operations, DTO naming, user flows, worker steps, channel recovery, diagnostics, retention, vocabulary, and reset behavior.

## Remaining QA Work

| Bead | Scope left |
| --- | --- |
| `media-7f3.11.2` | Backend, storage, security, worker, diagnostics redaction, retention, channel_account boundaries, and hidden current-DB preservation review. |
| `media-7f3.11.3` | Telegram/Web/MCP/runtime behavior, visible copy, stale callbacks, restart recovery, duplicate delivery, artifact access, and UX/runtime drift review. |
| `media-7f3.11.4` | Final MR readiness packet with source plan, implemented scope, commands, evidence, docs/GRACE state, Beads closure state, no-regression proof, and remaining risks. |

## Audit Commands

The following commands were run for `media-7f3.11.1` traceability:

```bash
bd context
bd ready --json
bd list --all --limit 0 --json
bash infra/scripts/no-legacy-target-gate.sh
uv run pytest packages/contracts/tests/test_contract_surfaces.py packages/contracts/tests/test_target_fixtures.py -q
git diff --check
```

Observed results:

- `bd context`: Dolt server reachable at `127.0.0.1:54711`.
- `bd ready --json`: next QA work was `media-7f3.11.1`, then it was claimed.
- `bd list --all --limit 0 --json`: `media-7f3.9` and `media-7f3.10` closed; `media-7f3.11.1` in progress; `media-7f3.11.2`, `media-7f3.11.3`, and `media-7f3.11.4` open.
- The historical predecessor XML validation had exit code `0`; it is retained in the external migration backup and Git history, not rerun as GRACE 4 validation.
- `bash infra/scripts/no-legacy-target-gate.sh`: passed, including target runtime snippets, active target vocabulary scans, target reset migration scans, and the focused contract test.
- `uv run pytest packages/contracts/tests/test_contract_surfaces.py packages/contracts/tests/test_target_fixtures.py -q`: `17 passed`.
- `rg` stale wording check for old `media-7f3.10` open and fake `100%` closure wording: no matches after fixes.
- `git diff --check`: exit code `0`.

Full coverage and runtime gates remain owned by the already closed `media-7f3.10` proof and by the final readiness packet, but QA may rerun them when evidence freshness is needed.
