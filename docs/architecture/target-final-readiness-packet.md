# Target Rebuild Final Readiness Packet

Status: final readiness evidence for `media-7f3.11.4`
Source plan: `docs/architecture/single-user-channel-aware-target-architecture.md`
Prepared on: 2026-05-18

## Decision

The target rebuild is ready for MR/review from the repository evidence available in this branch.

The implemented product model is the single-user, channel-aware target architecture: API owns product state, Telegram/Web/MCP are adapters over that state, channels own presentation/recovery state, workers consume immutable `selection_snapshot` data through `analysis_run_step` contracts, and target public vocabulary excludes the old owner/workspace/job/batch/source-set model from active contracts and runtime paths.

No target-readiness blocker remains in `media-7f3.11`. Remaining unrelated Beads do not block this target rebuild readiness packet.

## Branch And Commit State

| Item | State |
| --- | --- |
| Branch | `master` |
| Git remote | `origin git@github.com:DanilaBesk/media-analysis-platform.git` |
| Code readiness baseline | `4a1c4194b9fcef041ad3ca61c6395e7d807d0fa4` (`Harden channel UX QA surfaces`) |
| Baseline push state | `master` was up to date with `origin/master` after pushing `4a1c419` |
| Final packet commit | This document and GRACE final-state updates are the only expected changes after `4a1c419` |
| Beads backend | Dolt server mode on local server |
| Dolt remote | Not configured; `bd dolt push` remains intentionally blocked until a real Dolt remote URL exists |

## Implemented Scope

| Area | Evidence |
| --- | --- |
| Target architecture | `docs/architecture/single-user-channel-aware-target-architecture.md` defines the final tables, API operations, adapter responsibilities, worker responsibilities, reset policy, and verification decomposition. |
| Traceability | `docs/architecture/target-qa-traceability-audit.md` maps source-plan areas to implementation, tests, historical GRACE evidence, Beads state, findings, and remaining owners. Current state is GRACE 4 under `.grace/`. |
| Coverage matrix | `docs/architecture/target-coverage-matrix.md` maps target requirements to implementation proof, automated proof, QA proof, and remaining/closed Beads. |
| Backend/storage/worker QA | `docs/architecture/target-backend-storage-worker-qa.md` records backend, storage, security, object-store, diagnostics, cancellation, and worker findings plus fixes. |
| Channel UX/runtime QA | `docs/architecture/target-channel-ux-runtime-qa.md` records Telegram, Web, MCP, artifact, prerequisite, and runtime evidence plus Web UX drift fixes. |
| Final readiness | This packet packages the final branch, evidence, Beads, GRACE, verification, no-regression proof, and residual risk state. |

## Code Changes Since QA Started

| Commit | Purpose |
| --- | --- |
| `405c7a3` | Added target QA traceability audit and corrected stale closure wording. |
| `b9e081b` | Hardened backend/worker boundaries: workspace sanitization, object-key validation, worker-step validation, and HTTP server limits. |
| `4a1c419` | Hardened channel UX surfaces: hid Web runtime endpoints, removed raw JSON launch params, hid service artifacts in normal results, and published channel runtime QA. |

The final packet itself is documentation and GRACE state only.

## Verification Summary

Fresh final gate run:

```bash
bash infra/scripts/coverage-inventory.sh
```

Result: passed. Native metrics reported:

| Surface | Result |
| --- | --- |
| Go API/storage | `./internal/api` 67.6 percent statements; `./internal/storage` 98.5 percent statements |
| Python worker-common | 90 passed, 99 percent statements |
| Python agent-runner | 41 passed, 97 percent statements |
| Python transcription | 111 passed, 99 percent statements |
| Python Telegram adapter | 86 passed, 1 known runpy warning, 89 percent statements |
| MCP adapter | 42 passed, 100 percent line/branch/function coverage |
| Web UI | 67 passed, 97.86 percent statements/lines, 90.96 percent branches, 100 percent functions |
| Contracts and target fixtures | 17 passed |
| No-legacy target gate | passed |
| Compose topology and runtime-proof wiring | passed |
| MCP TypeScript typecheck | passed |

Focused QA gates already run in this final QA wave:

```bash
cd apps/web && pnpm test
cd apps/mcp-server && pnpm test
cd apps/api && go test ./internal/api -run 'TestTargetRuntimeServicePlansSpeechPrerequisiteForReportRuns|TestTargetRuntimeServiceRejectsWorkerWritesForUnknownStep' -count=1
PYTHONPATH=workers/common/src:workers/agent-runner/src uv run pytest workers/agent-runner/tests/test_transcriber_worker_agent_runner.py -q
cd apps/telegram-bot && uv run pytest tests/test_bot_runtime.py -q
cd apps/telegram-bot && uv run pytest tests/test_gateway.py -q
python3 infra/scripts/runtime-final-e2e.py
bash infra/scripts/compose-smoke.sh --check-config
bash infra/scripts/target-reset-smoke.sh
bash infra/scripts/no-legacy-target-gate.sh
git diff --check
```

The predecessor XML validation recorded in this completed readiness wave is retained
in the external migration backup and Git history. It was not rerun for GRACE 4;
current validation is `grace lint --path . --assertions current` and `grace status --path . --json`.

Observed results from the focused QA wave:

- Web tests: 67 passed.
- MCP tests: 42 passed.
- API prerequisite/worker-write focused tests: passed.
- Agent-runner focused suite: 38 passed before the later full inventory confirmed 41 passed.
- Telegram runtime tests: 33 passed with the known runpy warning.
- Telegram gateway tests: 25 passed.
- Runtime final E2E: `terminal_status=succeeded`, `diagnostic_count=0`, `download_bytes=325`.
- Compose config smoke, target reset smoke, no-legacy target gate, the recorded historical predecessor validation, and git diff whitespace check: passed.

## No-Regression Proof

| Critical flow | Proof |
| --- | --- |
| Clean target DB reset/recreate | `infra/scripts/target-reset-smoke.sh`; coverage inventory no-legacy/reset checks; target storage tests from earlier coverage slices. |
| Target vocabulary and deleted legacy public paths | `infra/scripts/no-legacy-target-gate.sh`; contracts tests; Web/MCP route/schema tests. |
| Channel-account isolation | storage/API coverage, runtime-final E2E cross-channel denial, MCP/Web/Telegram channel-account-scoped tests. |
| Telegram current materials, task cards, cancellation, restart recovery, duplicate result prevention | Telegram runtime/gateway tests and channel QA artifact. |
| Web normal UX copy and artifact visibility | Web route/runtime-context tests and channel QA artifact. |
| MCP tools, idempotency, structured errors, artifact preview/refresh | MCP tests and coverage inventory. |
| Worker sealed input and step lifecycle | worker-common, transcription, agent-runner tests; API worker-step validation tests. |
| Report/research prerequisite behavior | API planning tests and agent-runner transcript/text-corpus input materialization tests. |
| Runtime artifact/download path | runtime-final E2E and artifact/object-store tests. |

## Beads State

Target QA epic state after this packet:

- `media-7f3.11.1`: closed, traceability audit complete.
- `media-7f3.11.2`: closed, backend/storage/security/worker QA complete.
- `media-7f3.11.3`: closed, channel UX/Web/MCP/runtime QA complete.
- `media-7f3.11.4`: closed, final readiness packet complete.
- `media-7f3.11`: closed after all four QA/readiness children were verified closed.

Unrelated open work observed during readiness packaging:

- `media-swt`: warning-cleanup chore for `telegram_adapter.__main__` runpy warning. This is not a target readiness blocker; the warning is already visible and tracked.
- Older unrelated Beads outside `media-7f3.11` may remain in the ledger and are not part of the target rebuild readiness gate.

## Risks And Accepted Notes

- The local target database is intentionally disposable under the target architecture. The reset smoke proves recreate behavior; preserving current local rows is not a requirement.
- The runtime-final E2E proof uses a text/report flow. Speech prerequisite behavior is covered by API planning and worker input-contract tests.
- MCP exposes technical target vocabulary by design because the consumer is a tool/agent context.
- Telegram test coverage still emits one known runpy warning; this is tracked by `media-swt` and does not affect runtime behavior.
- No Dolt remote is configured. Git was pushed to the public repository; Beads changes are locally committed in Dolt but cannot be pushed to a Dolt remote until a real remote is provided.

## Final Gate

The target rebuild QA gate is closed in Beads. Completion protocol status:

1. This packet and the historical GRACE final-state updates were staged for the final Git commit.
2. The recorded historical predecessor validation and whitespace checks passed after the packet changes; current state is GRACE 4 under `.grace/`.
3. `media-7f3.11.4` is closed with this packet as evidence.
4. Parent epic `media-7f3.11` is closed after all four children closed.
5. Git push to `origin/master` is required after the final packet commit.
