# Target Channel UX, Web, MCP, And Runtime QA

Status: completed evidence for `media-7f3.11.3`
Source plan: `docs/architecture/single-user-channel-aware-target-architecture.md`
Previous QA gates:

- `docs/architecture/target-qa-traceability-audit.md`
- `docs/architecture/target-backend-storage-worker-qa.md`

This review challenges the Telegram, Web, MCP, artifact, diagnostic, and runtime behavior after the target rebuild. It is not the final MR readiness packet; final release packaging remains in `media-7f3.11.4`.

## Executive Result

The reviewed channel architecture remains aligned with the source plan: the API owns product state, Telegram/Web/MCP are adapters over the same state, Telegram presentation/recovery state lives in `channel_surfaces`, MCP may expose target technical vocabulary, and report/research execution is planned around ready transcript or text-corpus inputs rather than raw speech handoff to agent-runner.

The review found real Web UX drift in the normal user surface. All Web drift found in this slice was fixed with regression tests:

- the app shell no longer displays API or WebSocket endpoint URLs to ordinary users;
- the run builder no longer exposes a raw JSON parameter textarea as the normal launch path;
- normal result lists and direct artifact previews hide service artifacts such as run manifests, run diagnostics, diagnostic bundles, and execution logs.

No follow-up Bead was needed from this review because every concrete finding was either fixed or already covered by committed tests. The final readiness packet remains open in `media-7f3.11.4`.

## Review Surface

| Surface | Evidence inspected | Result |
| --- | --- | --- |
| Web app shell and run builder | `apps/web/src/app/app-shell.tsx`, `apps/web/src/features/media/media-workspace.tsx`, Web route/runtime tests. | Fixed runtime endpoint copy and raw JSON launch parameters in normal UI. |
| Web result and artifact views | artifact grouping, run outcome labels, direct artifact route, Web route tests. | Fixed normal result visibility so service artifacts stay hidden outside admin/debug contexts. |
| Telegram runtime behavior | `apps/telegram-bot/src/telegram_adapter/bot.py`, `apps/telegram-bot/tests/test_bot_runtime.py`, `apps/telegram-bot/tests/test_gateway.py`. | Pass; restart recovery, active task cards, stale callbacks, cancellation, result delivery, and duplicate-delivery prevention are covered. |
| MCP tool contract | `apps/mcp-server/src/tools/mapped-tools.ts`, `apps/mcp-server/tests/tool-registry.test.ts`. | Pass; technical target vocabulary is intentional for tool users, with idempotency and structured error coverage. |
| Report/research prerequisite planning | `apps/api/internal/api/target_runtime.go`, `apps/api/internal/api/target_runtime_test.go`. | Pass; speech report runs plan transcription prerequisites before agent-runner work. |
| Agent-runner input contract | `workers/agent-runner/src/transcriber_worker_agent_runner.py`, `workers/agent-runner/tests/test_transcriber_worker_agent_runner.py`. | Pass; agent-runner materializes declared transcript/text-corpus artifacts as harness inputs. |
| Runtime proof | `infra/scripts/runtime-final-e2e.py`, live compose stack on `localhost:8080`. | Pass; live text/report E2E produced a succeeded analysis_run, artifact download, diagnostics query, isolation denial, and retention check. |

## Findings

### QA-UX-001: Web shell leaked runtime endpoints

Status: fixed.

Evidence: the normal app shell rendered API and WebSocket endpoint URLs from runtime env.

Resolution: the shell now shows only product navigation and product identity. Runtime env remains available through `WebUiRuntimeProvider` for API clients and tests, but it is not ordinary page copy.

Regression proof: Web runtime-context and route tests assert the endpoint URLs are not rendered in the shell.

### QA-UX-002: Web run builder exposed raw JSON launch parameters

Status: fixed.

Evidence: the normal run builder exposed a `Параметры` textarea with raw JSON and surfaced JSON parse errors to users.

Resolution: the normal run builder now launches with the selected run type and explicit polling delivery only; advanced structured parameters are not a load-bearing normal UI field.

Regression proof: Web route tests assert the `Параметры` field is absent and run creation sends no raw params.

### QA-UX-003: Web normal results exposed service artifacts

Status: fixed.

Evidence: normal Web result views listed or previewed `run_manifest`, `run_diagnostics`, `diagnostic_bundle`, and `execution_log` artifacts as ordinary files, including direct manifest JSON preview.

Resolution: normal artifact lists and result labels filter service artifact kinds. Direct navigation to a service artifact shows a human message instead of the service JSON preview. Run manifests may still be parsed internally to render per-material outcome summaries.

Regression proof: Web route tests assert the summary result remains visible while the manifest preview and `План запуска` link are absent.

## Accepted Notes

- MCP tool schemas intentionally keep technical target vocabulary because MCP consumers are tools and agents, not the normal Web user surface.
- Web diagnostics remains an explicit admin/debug view and may expose diagnostic subject categories needed for operation.
- The runtime-final proof uses a text/report flow. Speech prerequisite behavior is covered by API planning tests and agent-runner artifact-input tests in this QA slice.
- The first Telegram check attempted from the root `uv` environment failed because that environment does not include the Telegram app's `aiogram` dependency. The correct app-local Telegram `uv.lock` environment passed.

## Verification

Commands run for this Bead:

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

The predecessor XML validation recorded for this completed QA wave is retained in
the external migration backup and Git history. It is not evidence that GRACE 4
was rerun. Current GRACE 4 validation is performed separately with
`grace lint --path . --assertions current` and `grace status --path . --json`.

Observed results:

- Web tests: `7 passed`, `67 passed`.
- MCP tests: `42 passed`.
- API prerequisite/worker-write focused tests: passed for `./internal/api`.
- Agent-runner tests: `38 passed`.
- Telegram runtime tests: `33 passed`, `1 warning`.
- Telegram gateway tests: `25 passed`.
- Runtime final E2E: succeeded with terminal analysis run status `succeeded`, `diagnostic_count: 0`, and artifact download bytes `325`.
- Compose config smoke: passed.
- Target reset smoke: passed.
- No-legacy target gate: passed.
- The recorded predecessor XML validation passed at the time; current GRACE 4 validation is separate.
- Git diff whitespace check: passed.
