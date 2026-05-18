# Target Backend, Storage, Security, And Worker QA

Status: completed evidence for `media-7f3.11.2`
Source plan: `docs/architecture/single-user-channel-aware-target-architecture.md`
Previous QA gate: `docs/architecture/target-qa-traceability-audit.md`

This review challenges the backend, storage, security, object storage, diagnostics, cancellation, and worker boundaries behind the single-user channel-aware target rebuild. It is not the final MR readiness packet; channel UX/Web/MCP/runtime review remains in `media-7f3.11.3`, and final packaging remains in `media-7f3.11.4`.

## Executive Result

The reviewed target architecture is still coherent: API owns product state, target storage is disposable and resettable, workers consume sealed `selection_snapshot` data through `analysis_run_step` contracts, and compatibility vocabulary remains isolated to explicit deprecated or historical surfaces.

The review found four real backend/worker hardening issues. All four were fixed with regression tests:

- worker workspace paths now sanitize control-plane `analysis_run_id` values before creating local directories;
- worker artifact object keys now sanitize path segments, and MinIO object fetch/upload rejects absolute, backslash, parent-directory, and whitespace-padded object keys before network IO;
- worker internal write APIs now validate `analysis_run_step_id` before progress, artifact, diagnostic, and finalize writes;
- the Go HTTP server now has explicit read, write, idle, header, and max-header limits instead of only `ReadHeaderTimeout`.

No follow-up Bead was needed from this review because the concrete findings were resolved in this slice.

## Review Surface

| Surface | Evidence inspected | Result |
| --- | --- | --- |
| API entrypoint and request limits | `apps/api/cmd/api/main.go`, `apps/api/internal/api/api.go`, upload handlers. | Fixed server timeout/header hardening; JSON and multipart bodies remain bounded by configured request limits. |
| Target storage reset and disposable DB policy | `apps/api/internal/storage/migrations/0001_final_inbox_analysis_run_schema.sql`, `infra/scripts/target-reset-smoke.sh`, target store tests. | Pass; reset migration is destructive for local data and recreates target tables without preserving current DB rows. |
| Channel-account boundaries | target store queries and target runtime service lookup paths. | Pass; public reads/mutations retain channel_account predicates, and worker writes now validate step ownership before mutation. |
| Worker step lifecycle | `TargetRuntimeService`, `apps/api/internal/storage/target.Store`, worker-control DTOs. | Fixed missing-step write hole for progress/artifacts/diagnostics/finalize. |
| Object storage | `workers/common/src/transcriber_workers_common/object_store.py`, artifact writer helpers, worker tests. | Fixed object key validation and artifact key segment sanitization. |
| Worker local filesystem | transcription and agent-runner workspace setup. | Fixed workspace path traversal through untrusted control-plane IDs. |
| Diagnostics and redaction | target diagnostics storage, `safe_channel_context`, agent-runner redaction tests. | Pass; existing redaction coverage remains, and missing-step diagnostics are now rejected before storage mutation. |
| Cancellation and finalization races | target store cancellation/finalize paths and worker cancel-check usage. | Pass; late finalize still resolves canceled runs as canceled, and wrong-step finalize is rejected. |
| Current DB preservation | source plan, reset migration, reset smoke. | Pass; no code path reviewed required preserving existing local rows. |

## Findings

### QA-BSW-001: worker workspace path traversal from control-plane run ids

Status: fixed.

Evidence: both `runTranscription` and `runAgentHarness` used `Path(workspace_root) / execution.analysis_run_id`. The worker DTO parser accepted any non-empty string for `analysis_run_id`, so a malformed or compromised internal response could write outside the worker workspace.

Resolution: both workers now derive workspace directories through a safe token that preserves normal IDs, sanitizes path separators and relative segments, and verifies the resolved path stays under `workspace_root`.

Regression proof: transcription and agent-runner tests now execute runs with `analysis_run_id="../escape/run"` and assert the workspace remains under the test root.

### QA-BSW-002: object-store key path escape before MinIO IO

Status: fixed.

Evidence: worker object-store URL construction joined `bucket` and `object_key` with `PurePosixPath`; parent or absolute object keys could alter the generated path before the HTTP request. Artifact keys also used raw `analysis_run_id` and filename segments.

Resolution: object-store fetch/upload now rejects non bucket-relative keys before transport calls. Artifact object keys now sanitize path segments while preserving normal canonical keys.

Regression proof: worker-common object-store tests reject `../escape.txt`, `/absolute.txt`, `dir/../escape.txt`, backslash keys, and whitespace-padded keys with no transport call. Artifact tests prove malicious run/file segments cannot emit `..`.

### QA-BSW-003: worker internal write APIs accepted missing step ids

Status: fixed.

Evidence: target progress/finalize storage updated `analysis_run_steps` but did not check affected rows before mutating the parent `analysis_run`. Runtime artifact and diagnostic registration accepted an `analysis_run_step_id` without first proving that the step belongs to the run.

Resolution: `TargetRuntimeService` now validates the run/step pair before progress, artifact, diagnostic, and finalize writes. Target storage progress/finalize also checks `RowsAffected` and returns `sql.ErrNoRows` when the step row is missing.

Regression proof: API runtime tests prove unknown-step worker writes return `analysis_run_not_found` and do not reach store write methods. Target store tests prove missing progress/finalize step updates return `sql.ErrNoRows`.

### QA-BSW-004: incomplete HTTP server timeout surface

Status: fixed.

Evidence: the API HTTP server configured `ReadHeaderTimeout` but not read, write, idle, or max-header limits.

Resolution: `cmd/api` now configures explicit read-header, read, write, idle, and max-header limits. The read timeout is intentionally long enough for local large media uploads while still bounding connection lifetime.

Regression proof: covered by Go package build/test gates in this review.

## Accepted Notes

- The current local database remains disposable by target policy. This review did not add preservation, migration, or backfill work for existing rows.
- Compatibility names still exist in deprecated routes, historical docs, and explicit tests by design; the active target gate remains `infra/scripts/no-legacy-target-gate.sh`.
- Worker DTO parsing still accepts non-UUID strings because older tests and compatibility fixtures use readable ids. The filesystem and object-store boundaries now sanitize those ids before side effects.

## Verification

Focused checks already run during the fix loop:

```bash
uv run pytest workers/common/tests/test_artifacts.py workers/common/tests/test_object_store.py workers/transcription/tests/test_transcriber_worker_transcription.py::test_run_transcription_sanitizes_workspace_and_artifact_prefix_for_control_plane_ids workers/agent-runner/tests/test_transcriber_worker_agent_runner.py::test_run_agent_harness_sanitizes_workspace_and_artifact_prefix_for_control_plane_ids -q
cd apps/api && go test ./internal/api ./internal/storage/target -run 'TestTargetRuntimeServiceRejectsWorkerWritesForUnknownStep|TestStoreRejectsWorkerStepWritesWhenStepRowIsMissing' -count=1
```

Observed results:

- Python focused worker/common tests: `21 passed`.
- Go focused API/storage tests: passed for `./internal/api` and `./internal/storage/target`.

The wider closure commands for this Bead are recorded in the Bead close reason after final verification.
