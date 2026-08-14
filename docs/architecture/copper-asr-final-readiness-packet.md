# CopperASR Final Readiness Packet

Owner Bead: `media-b8s.3.4`

Date: 2026-05-19

Status: ready with non-blocking follow-ups.

## Decision

The CopperASR migration is ready to close from the application side.

The repo now consumes CopperASR as an external upstream runtime source at
`external/copper-asr`, pinned to:

```text
bc3c0da673ee8a7eabb82e2a1530ddf7d5e9bd01 external/copper-asr
```

The old Whisper/faster-whisper runtime is not active in compose, env templates,
runtime workers, dependency gates, or ASR docs. CopperASR is exposed only as an
internal compose service; Telegram/Web/MCP/API flows use API-owned artifacts and
run state.

## Evidence Packets

- Source plan:
  `docs/plans/2026-05-18-copper-asr-submodule-migration.md`.
- Source boundary:
  `external/copper-asr`, `.gitmodules`, and
  `infra/images/copper-asr/Dockerfile`.
- No-legacy QA:
  `docs/architecture/copper-asr-no-legacy-whisper-qa.md`.
- Contracts/security/ops QA:
  `docs/architecture/copper-asr-contracts-security-ops-qa.md`.
- Runtime soak QA:
  `docs/architecture/copper-asr-runtime-soak-qa.md`.
- Benchmark artifact:
  `docs/benchmarks/copper-asr-long-voice-benchmark-latest.json`.

## Verification Commands

Last successful verification set:

```bash
git diff --check
uv run --project apps/telegram-bot pytest apps/telegram-bot/tests -q
uv run pytest packages/contracts/tests/test_target_fixtures.py -q
bash infra/scripts/no-legacy-asr-gate.sh
bash infra/scripts/compose-smoke.sh --check-config
RUNTIME_E2E_POLL_TIMEOUT_SECONDS=300 bash infra/scripts/compose-smoke.sh --live-smoke
python3 infra/scripts/copper-asr-telegram-e2e.py --json
python3 infra/scripts/copper-asr-api-web-mcp-e2e.py --json
python3 infra/scripts/copper-asr-failure-e2e.py --json --require-invalid-audio
python3 infra/scripts/copper-asr-benchmark-e2e.py --json --write-artifact docs/benchmarks/copper-asr-long-voice-benchmark-latest.json --blocker-issue-id media-b8s.2.10
```

The predecessor XML validation from this historical readiness packet is retained in
the external migration backup and Git history. It was not rerun for GRACE 4;
validate current state separately with `grace lint --path . --assertions current` and `grace status --path . --json`.

Additional earlier full coverage inventory evidence:

```bash
bash infra/scripts/coverage-inventory.sh
```

That inventory covered Go API/storage, Python common/agent/transcription,
Telegram adapter, Node MCP, Web UI, contract surfaces, no-legacy gates, compose
topology, and MCP typecheck.

## Runtime Proof

Final compose live-smoke passed after a full recreate. After adding the API
healthcheck and API-consumer `service_healthy` dependencies, the final startup
had `restart_count=0` for:

- `telegram-bot`
- `worker-transcription`
- `worker-agent-runner`
- `web`
- `mcp-server`

Final API-consumer startup logs had no `backend_unavailable`, `Connection
refused`, `Traceback`, `TypeError`, `panic`, or `fatal` markers.

Final ASR logs had no old `whisper`, `faster-whisper`, `ctranslate`,
`podlodka`, or `backend_unavailable` markers.

## E2E Results

- Telegram E2E: succeeded with `provider=copperasr`, transcript delivery,
  duplicate delivery prevention, and inbox clear.
- API/Web/MCP E2E: succeeded with channel-scoped run/artifact denial and
  `run_manifest` provider metadata.
- Failure E2E: corrupt and retry runs finalized as `failed` with
  `asr_invalid_audio`; cancellation finalized as `canceled` with zero artifacts.
- Long voice benchmark: `960.006` second input, `run_wall_seconds=165.319`,
  `speedup_vs_realtime=5.807`, `max_cpu_percent=413.73 <= 450.0`,
  `max_memory_mib=1921.024 <= 4096.0`, `thresholds.passed=true`.

## Bugs Closed During Final QA

`media-b8s.3.3.1` closed a critical Telegram adapter recovery bug:
one stale or unreachable `channel_surface` can no longer crash the whole
adapter loop. Runtime proof seeded a stale Telegram surface and verified:

- stable log anchor
  `[TelegramAdapter][bot][BLOCK_HANDLE_TELEGRAM_SURFACE_FAILURE]`;
- `classification=telegram_address_unreachable`;
- `channel_surface.lifecycle_status=superseded`;
- `channel_surface.superseded` event with
  `reason=telegram_address_unreachable`;
- adapter reached `Start polling` and `Run polling`.

The same fix preserves the result duplicate guard and prevents failed new
delivery from creating a result surface or clearing inbox state.

## Open Follow-Ups

Non-blocking:

- `media-swt`: remove the existing `telegram_adapter.__main__` runpy warning in
  tests.
- `media-b8s.3.4.1`: reduce stacktrace noise for controlled transcription
  failure paths. This is not a migration blocker because invalid-audio and
  cancellation final states, diagnostics, and artifacts are correct.

## Delivery Notes

- Beads/Dolt remote sync remains not available: this repo has no configured
  Dolt remote, so `bd dolt push` is intentionally not run.
- `.beads/issues.jsonl` is exported locally after Beads updates, but `.beads`
  is ignored by git in this repo.
- Git remote exists for the code repo; the final patchset should be committed
  and pushed through the normal git remote flow.

## Final State

Application readiness: ready with the two non-blocking follow-ups above.

Migration blockers: none on the application/CopperASR runtime path.
