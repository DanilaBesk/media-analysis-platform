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


## Build & Test

```bash
uv run telegram-transcriber-bot
uv run pytest
```

## Architecture Overview

- `src/telegram_transcriber_bot/bot.py` contains the aiogram runtime, handlers, callbacks, and media-group flow.
- `src/telegram_transcriber_bot/service.py` owns job orchestration and artifact lifecycle under `.data/jobs/<job_id>/`.
- `src/telegram_transcriber_bot/transcribers.py` handles transcript acquisition via YouTube subtitles first, then Whisper fallback.
- `src/telegram_transcriber_bot/cglm_runner.py` wraps the mandatory `cglm` CLI used for research report generation.
- `src/telegram_transcriber_bot/documents.py` renders transcript/report outputs.

## Conventions & Patterns

- Use `bd` for task tracking; do not keep parallel markdown TODO lists.
- Keep `AGENTS.md` short and repo-specific.
- Local runtime dependencies are `uv`, `cglm`, and `ffmpeg`.
- `.env` stays local; configure the bot via `.env.example` and `TELEGRAM_BOT_TOKEN`.
- This repository currently has no configured git remote, so push-related checklist items apply only after remote setup.
