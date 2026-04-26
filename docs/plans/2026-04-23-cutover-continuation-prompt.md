# Next Session Prompt: Cutover Parity

Use this prompt for a fresh Codex session in `/Users/danila/work/my/media-analysis-platform`.

```text
Продолжи media-analysis-platform строго по GRACE XML и Beads.

Preflight:
1. Run `bd context`.
2. Run `bd ready --json`.
3. Run `git status --short`.
4. Run `grace lint --path .`.
5. Run `grace module show M-API-HTTP --with verification --path .`.
6. Read `docs/operational-packets.xml` packet `M-API-HTTP-wave-6`.

Current state:
- Last known committed runtime-stack sync: `89b5faf grace(M-INFRA-COMPOSE): materialize runtime stack`.
- Worker extraction packets are implemented and reflected in GRACE:
  `M-WORKER-COMMON`, `M-WORKER-TRANSCRIPTION`, `M-WORKER-REPORT`, `M-WORKER-DEEP-RESEARCH`.
- `M-INFRA-COMPOSE-wave-6` step 1 is complete: `bash infra/scripts/compose-smoke.sh --live-smoke` passed with all runtime services healthy.
- The next open execution packet is `M-API-HTTP-wave-6`.
- Beads follow-up issues:
  - `media-1t4`: Run M-API-HTTP-wave-6 cutover parity replay.
  - `media-j3u`: Prove worker E2E against live MinIO and API control plane.
  - `media-8l5`: Replay Web UI Telegram and MCP parity on live stack.
  - `media-pkn`: Close storage queue events failure-path smokes before legacy removal.
  - `media-dgf`: Remove legacy single-process runtime after cutover evidence. This one is blocked and must not be started first.

Primary task:
Claim and execute `media-1t4` / `M-API-HTTP-wave-6`.
Run live-stack route regression for:
- multipart upload;
- combined upload;
- URL submission;
- child report job creation;
- child deep-research job creation;
- artifact resolution;
- cancel;
- retry;
- WebSocket/timeline behavior;
- internal worker-control routes: claim, progress, artifacts, finalize, cancel-check.

Rules:
- Do not rerun `grace-init`.
- Do not start from architecture brainstorming.
- Do not remove the old single-process runtime path until all cutover evidence exists.
- If a failure is downstream of HTTP, stop and hand off to the owning module instead of broadening the HTTP packet.
- If a route-level mismatch is proven, patch only inside the packet write scope from `docs/operational-packets.xml`.
- Preserve polling as authoritative; WebSocket/webhook are delivery layers only.
- Record any new follow-up in Beads, not in ad-hoc TODO notes.

Verification target for this session:
- `grace lint --path .`
- `xmllint --noout docs/requirements.xml docs/technology.xml docs/development-plan.xml docs/verification-plan.xml docs/knowledge-graph.xml docs/operational-packets.xml`
- `go test ./... -run ApiHttp` from `apps/api`
- live-stack route evidence for the listed HTTP/API surfaces
- update Beads issue status with exact commands and evidence

Expected output:
- concise symptom -> cause -> fix notes for any mismatch;
- exact commands run and whether each passed;
- explicit list of remaining cutover gates, if any;
- no claim that legacy cutover is ready unless all gates are proven.
```
