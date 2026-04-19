# AGENTS.md

## Start Here

- Use `bd` for task tracking in this repo.
- This workspace is configured for Beads with a local Dolt SQL server, not embedded mode.
- Start each session with `bd context` and `bd ready --json`.
- If the server is not reachable, check `bd dolt status` and start it with `bd dolt start`.

## Working Rules

- Keep `AGENTS.md` short. Put longer process notes in `README.md` or docs instead of expanding this file.
- `bd init` bootstrapped a local git repository here. No remote is configured right now, so do not assume branch or PR workflows exist yet.
- Until a git remote is configured, treat the Beads push-to-remote checklist as conditional rather than immediately runnable in this repo.
- When you discover follow-up work, record it in Beads instead of ad-hoc markdown notes.

## Repo Basics

- Runtime: Python `3.12` with `uv`.
- App entrypoint: `uv run telegram-transcriber-bot`
- Tests: `uv run pytest`
- Required local tools: `cglm`, `ffmpeg`
- Required env setup: copy `.env.example` to `.env` and set `TELEGRAM_BOT_TOKEN`

## Code Map

- `src/telegram_transcriber_bot/bot.py` - Telegram runtime and handlers
- `src/telegram_transcriber_bot/service.py` - job orchestration and artifacts
- `src/telegram_transcriber_bot/transcribers.py` - YouTube/subtitles/Whisper pipeline
- `src/telegram_transcriber_bot/cglm_runner.py` - `cglm` subprocess adapter
- `src/telegram_transcriber_bot/documents.py` - transcript/report document rendering

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

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
