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

- GRACE 4 state lives under `.grace`: context in `.grace/context/`, graph in `.grace/graph/{index,main}.xml`, verification in `.grace/verification/{index,main}.xml`, and approved change bundles in `.grace/changes/active/` (with completed bundles in `.grace/changes/archive/`).
- For architecture or implementation work, keep that state aligned with code and use `grace status --path . --json`, `grace lint --path . --assertions current`, and GRACE navigation commands to inspect it.
- Use `$grace-spec` and `$grace-plan` to create new `C-*` changes; use `$grace-execute`, `$grace-reviewer`, and `$grace-refresh` for approved execution, scoped review, and shared-state synchronization as applicable.
- Beads selects and tracks work; `.grace/changes/active/` holds the approved GRACE change bundles. Do not create retroactive bundles for completed work.

## Repo Basics

- Primary runtime: Docker Compose local stack with Python `3.12` components managed inside the compose topology.
- Local cutover entrypoint: `uv run media-analysis-platform`
- Tests: `uv run pytest`
- Required local tools: `docker compose`, `uv`; `ffmpeg` is still needed when media-processing paths are exercised on the host.
- Required env setup: copy `.env.example` to `.env` and set `TELEGRAM_BOT_TOKEN`

## Code Map

- `apps/api` - Go API control plane
- `apps/telegram-bot/src/telegram_adapter` - compose-owned Telegram adapter over the API
- `workers/transcription/src/transcriber_worker_transcription.py` - transcription worker runtime and local source materialization
- `workers/agent-runner/src/transcriber_worker_agent_runner.py` - report/deep-research agent_run worker runtime
- `workers/common/src/transcriber_workers_common/copper_asr.py` - shared CopperASR HTTP client and response normalization
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
