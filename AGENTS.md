# AGENTS.md

## Start Here

- Use `bd` for task tracking in this repo.
- This workspace is configured for Beads with a local Dolt SQL server, not embedded mode.
- Start each session with `bd context` and `bd ready --json`.
- If the server is not reachable, check `bd dolt status` and start it with `bd dolt start`.

## Working Rules

- Keep `AGENTS.md` short. Put longer process notes in `README.md` or docs instead of expanding this file.
- Git remote is configured for the public GitHub repository `DanilaBesk/media-analysis-platform`.
- Beads/Dolt still has no configured Dolt remote; do not invent one. Treat `bd dolt push` as blocked until a real Dolt remote URL is provided.
- When you discover follow-up work, record it in Beads instead of ad-hoc markdown notes.

## GRACE Protocol

- This repo is now GRACE-initialized. Treat these files as first-class engineering artifacts:
  - `docs/requirements.xml`
  - `docs/technology.xml`
  - `docs/development-plan.xml`
  - `docs/verification-plan.xml`
  - `docs/knowledge-graph.xml`
  - `docs/operational-packets.xml`
- For architecture or implementation work, update GRACE artifacts before or alongside code changes rather than letting design drift live only in chat or ad-hoc markdown.
- The current large migration brief in `docs/plans/2026-04-19-media-analysis-platform-monorepo-migration.md` remains the detailed architecture baseline, but the GRACE XML docs are now the canonical structure for future planning, execution packets, verification, and graph updates.
- New modules should carry GRACE-style module contracts and stable semantic/log anchors when they are implemented.
- For implementation from this point forward, default to GRACE packet-driven execution rather than freeform refactoring.
- Compose cutover packets are already reflected in GRACE docs; use `bd ready --json` plus `docs/operational-packets.xml` to find the remaining approved execution slice instead of restarting early waves.
- Recommended next GRACE steps:
  - use `$grace-execute` to execute the next approved packet;
  - use `$grace-reviewer` for scoped gate reviews after each packet;
  - use `$grace-refresh` only when shared GRACE artifacts must be synchronized to implemented code;
  - use `docs/operational-packets.xml` as the canonical packet and delta schema during execution.

## Repo Basics

- Primary runtime: Docker Compose local stack with Python `3.12` components managed inside the compose topology.
- Local cutover entrypoint: `uv run media-analysis-platform`
- Tests: `uv run pytest`
- Required local tools: `docker compose`, `uv`; `ffmpeg` is still needed when media-processing paths are exercised on the host.
- Required env setup: copy `.env.example` to `.env` and set `TELEGRAM_BOT_TOKEN`

## Code Map

- `apps/api` - Go API control plane
- `src/media_analysis_platform/bot.py` - legacy Telegram adapter runtime and handlers
- `apps/telegram-bot/src/telegram_adapter` - compose-owned Telegram adapter over the API
- `workers/transcription/src/transcriber_worker_transcription.py` - transcription worker runtime and local source materialization
- `workers/agent-runner/src/transcriber_worker_agent_runner.py` - report/deep-research agent_run worker runtime
- `workers/common/src/transcriber_workers_common/transcribers.py` - shared YouTube/subtitles/Whisper helpers
- `workers/common/src/transcriber_workers_common/documents.py` - transcript/report document rendering helpers

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, complete all applicable steps below. Push-to-remote steps are mandatory only if a git remote is configured for this repo.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - Mandatory only when a git remote is configured:
   ```bash
   git pull --rebase
   bd dolt push  # only if a Beads/Dolt remote is configured
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- If a git remote is configured, work is NOT complete until `git push` succeeds
- If no git remote is configured, do not invent one and do not claim remote-sync steps were completed
- NEVER say "ready to push when you are" when remote push is required - YOU must push
- If push fails and a remote exists, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
