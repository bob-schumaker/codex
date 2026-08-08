# Memory Bank

This directory stores durable project-local context for future Codex sessions.
It is not a task transcript and should stay concise.

## Core Files

- `projectbrief.md` — project purpose and current durable goals.
- `productContext.md` — user/product behavior and why the work matters.
- `systemPatterns.md` — architecture patterns and design constraints.
- `techContext.md` — repo layout, tooling, and validation commands.
- `activeContext.md` — current work horizon and immediate next steps.
- `progress.md` — milestone state, working behavior, and remaining work.

## Repository-specific conventions

- Use `memory-bank/` for project-local memory.
- Use `obsidian-memory` only when the user explicitly asks for Obsidian-vault memory.
- Update `activeContext.md` and `progress.md` after meaningful state changes.
- Keep external-controller design facts grounded in `docs/external-controllers.md`
  and `docs/external-controllers-implementation-plan.md`.
- If `memory-bank/notes/historical-user-prompts.txt` is added later, update it
  only for substantive prompts not already captured elsewhere.
