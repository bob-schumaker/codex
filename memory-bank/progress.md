# Progress

## Working

- Codex-side local-controller metadata now publishes enough launch information
  for controller discovery, including process ID, endpoint URI, nonce, and
  `mainThreadId` once available.
- The design preserves the TUI as primary input owner while allowing an approved
  controller to acquire and release control.
- The spec corpus now states that controller discovery uses
  `$CODEX_HOME/local-controllers`, not generic Codex hooks or Herdr metadata.
- In-process local-controller socket coverage now exercises the native TUI
  approval path rather than durable enrollment test credentials.
- External-controller admission now extracts exact main-thread targets from the
  admitted normal app-server interface, including serialized thread mutations
  and concurrent read methods.
- Thread-scoped TUI-only mutations now reclaim primary input ownership from an
  active controller lease before dispatch.
- Collection-filtered controller reads now align admission, target extraction,
  and dispatch for `thread/list`, `thread/search`, `thread/loaded/list`, and
  `threadSection/list`; controller collection reads are scoped to the immutable
  main thread instead of rejecting or exposing broad runtime state.
- The current branch head includes coherent Codex-side commits through
  `e0a50da` for launch metadata publication, native approval coverage, exact
  target extraction, TUI reclaim, and collection-filtered reads.
- Focused app-server controller tests pass. The full app-server suite still
  shows zsh-fork timeout failures; the latest run failed the zsh-fork cluster
  even when sampled individually, so that fixture is currently unhealthy outside
  the controller slice.

## In Flight

- Codex-side review for the next normal-interface parity slice: continuation
  binding, subscription binding, resume-token binding, and prompt/egress
  transactionality.
- Downstream controller-host discovery and display behavior for all live Codex
  launches, including non-Herdr launches.

## Remaining

- Codex app-server should explicitly validate server-side binding for cursors,
  subscriptions, resume tokens, implicit targets, and prompt responses across
  ownership changes.
- Downstream controller host should discover all live launches through
  local-controller metadata watching/rescanning.
- Downstream display should separate:
  - launch liveness,
  - participation/authorization state,
  - active input ownership, and
  - product slot assignment.
- Downstream auto-assignment should preserve explicit slots and fill free slots
  deterministically for newly discovered live launches.

## Risks or Follow-ups

- A controller product that gates visibility on Herdr `agent_session` can hide
  valid Codex launches.
- A product that maps "not approved" or "not active owner" to offline will
  misrepresent live Codex sessions.
- The implementation plan must continue to avoid durable enrollment or reusable
  controller credentials unless a future design explicitly changes that
  decision.
- Keep Codex native local-controller tests on the TUI approval path so stale
  credential-enrollment assumptions do not mask launch behavior regressions.
- Keep controller admission and target-extraction tables aligned when opening
  additional normal app-server methods to approved controllers.
- Continue validating collection continuation behavior: cursors and
  subscriptions still need explicit connection/main-thread binding coverage.
- Treat the current zsh-fork timeout failures as a separate fixture health issue
  unless a future controller change newly affects that cluster.
