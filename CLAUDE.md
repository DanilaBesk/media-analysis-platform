# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

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
   bd dolt push
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


## Build & Test

```bash
uv run telegram-transcriber-bot
uv run pytest
```

## Architecture Overview

- `src/telegram_transcriber_bot/bot.py` contains the aiogram runtime, handlers, callbacks, and media-group flow.
- `workers/transcription/src/transcriber_worker_transcription.py` owns transcription worker execution, local source materialization, and transcript artifact persistence.
- `workers/common/src/transcriber_workers_common/transcribers.py` handles transcript acquisition via YouTube subtitles first, then Whisper fallback.
- `workers/report/src/transcriber_worker_report.py` executes report jobs through the configured report harness.
- `workers/common/src/transcriber_workers_common/documents.py` renders transcript/report outputs.

## Conventions & Patterns

- Use `bd` for task tracking; do not keep parallel markdown TODO lists.
- Keep `AGENTS.md` short and repo-specific.
- Primary runtime is the compose stack; `uv run telegram-transcriber-bot` is only a cutover pointer, and `ffmpeg` is still required for local media-processing flows exercised on the host.
- `.env` stays local; configure the bot via `.env.example` and `TELEGRAM_BOT_TOKEN`.
- This repository currently has no configured git remote, so push-related checklist items apply only after remote setup.
