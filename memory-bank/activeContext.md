# Active Context

- Current focus: external-controller spec consistency, local-controller
  discovery semantics, and downstream display correctness.

## Current Status

- Done:
  - Added controller-side discovery and presentation requirements to
    `docs/external-controllers.md`.
  - Corrected the endpoint directory in the spec to
    `$CODEX_HOME/local-controllers`.
  - Added downstream discovery/health/auto-assignment slices to
    `docs/external-controllers-implementation-plan.md`.
  - Replaced stale durable-enrollment implementation-plan language with native,
    connection-bound participation grants.
  - Added repository-local memory-bank guidance through `AGENTS.local.md`.
  - Initialized `memory-bank/`.
- In progress:
  - Commit the consistent spec and memory-bank corpus.
- Not started:
  - Downstream controller-host implementation for file-watch discovery, health
    model separation, and deterministic auto-assignment.

## Next Steps

- Review and commit the documentation and memory-bank changes.
- Implement downstream discovery as metadata-directory watch plus full rescan.
- Fix downstream status mapping so pending/unapproved/released live launches do
  not display as offline.
