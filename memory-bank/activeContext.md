# Active Context

- Current focus: external-controller normal-interface parity in Codex and
  downstream controller discovery/display follow-up.

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
  - Converted the in-process local-controller socket parity test to use the
    native TUI approval path instead of credential-enrollment test plumbing.
  - Updated stale controller-enrollment module commentary so it no longer claims
    the local endpoint/RPC path is unwired.
  - Validated the focused app-server local-controller tests with
    `just test -p codex-app-server local_controller`.
  - Expanded external-controller exact-thread target extraction so admitted
    normal-interface methods can reach their handlers instead of failing at the
    pre-dispatch gate.
  - Changed primary input reclaim so thread-scoped TUI-only mutations also
    cancel an active controller lease.
  - Validated the app-server controller gate with
    `just test -p codex-app-server controller`.
  - Built the `test_stdio_server` helper and reran
    `just test -p codex-app-server`; the full run reached 1172/1174 passing
    with two zsh-fork timeout failures that both passed when rerun
    individually.
- In progress:
  - Downstream controller-host implementation for file-watch discovery, health
    model separation, and deterministic auto-assignment.
- Not started:
  - Downstream acceptance rerun across multiple live Codex launches after the
    controller host consumes the metadata-directory discovery contract.

## Next Steps

- Implement downstream discovery as metadata-directory watch plus full rescan.
- Fix downstream status mapping so pending/unapproved/released live launches do
  not display as offline.
- Rerun the downstream multi-launch controller smoke once the downstream host
  work is in place.
