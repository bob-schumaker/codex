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
- Successful `controller/signOff` now revokes the controller session, rebinds
  prompts to the TUI, waits for the sign-off response to queue and write, and
  then disconnects the controller socket.
- After `controller/signOff` starts final-response teardown, subsequent
  external-controller ingress on that same connection is rejected with typed
  `transport-closing` while the final response flushes and the socket closes.
- `controller/signOff` now requires an existing standing controller session
  before revocation, and successful sign-off immediately unsubscribes the
  external-controller connection from the TUI main thread before final
  response/disconnect teardown continues.
- External-controller `thread/resume` now remains `ExactThread + ActiveOwner`
  for main-thread rejoin, but controller-origin history, path, configuration,
  instruction, approval, sandbox, permission, and personality overrides are
  rejected before handler dispatch.
- Exact-thread pagination cursors returned to an approved controller are now
  connection-bound and main-thread-bound before reuse. The binding covers
  `thread/resume` cursor fields, `thread/turns/list`, `thread/items/list`,
  `thread/searchOccurrences`, and `thread/backgroundTerminals/list`.
- `controller/signOff` now preserves the expected external-origin error for
  non-controller callers before any main-thread admission check.
- Authorization expiry now revokes the controller's standing session with a
  typed `authorization-expired` error and removes the external-controller
  connection from the live TUI main-thread subscription without closing the
  socket.
- `controller/acquireControl` is now idempotent for the controller that already
  owns the active lease; a second controller still receives the existing
  ownership-conflict behavior.
- Controller prompt/server-request replies delivered through the
  owner-aware recipient path are now bound to the interactive owner epoch that
  delivered the prompt; the same controller cannot resolve an old prompt after
  release/reacquire under a newer lease.
- The latest Codex-side implementation commit is
  `e1463ef` for launch metadata publication, native approval coverage, exact
  target extraction, TUI reclaim, collection-filtered reads, and backpressure-
  aware sign-off teardown with ingress fencing, resume-override gating, and
  sign-off subscription cleanup plus exact-thread cursor binding and
  authorization-expiry subscription cleanup plus idempotent active-owner
  acquire plus prompt owner-epoch reply binding.
- Focused app-server controller tests pass. The full app-server suite still
  shows zsh-fork timeout failures; the latest run failed the zsh-fork cluster
  even when sampled individually, so that fixture is currently unhealthy outside
  the controller slice.

## In Flight

- Codex-side implementation for the selected current-time/direct
  server-request parity slice: `currentTime/read` is already classified as
  `ExactThread + ActiveOwner`, but `current_time.rs` still bypasses the
  controller owner-aware recipient path and therefore does not route to the
  active external controller or bind the pending response to the owner epoch.
- Downstream controller-host discovery and display behavior for all live Codex
  launches, including non-Herdr launches.

## Remaining

- Codex app-server should route and validate remaining server-side binding for
  long-lived subscriptions, implicit targets, and direct server-request paths.
  Exact-thread pagination cursors are now connection-bound for controllers,
  controller-origin `thread/resume` override fields now have a pre-dispatch
  gate, owner-aware prompt replies are owner-epoch-bound, and sign-off plus
  authorization expiry now clean up the controller's main-thread subscription.
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
- Continue validating long-lived subscription behavior; exact-thread
  pagination cursors now have explicit connection/main-thread binding coverage,
  while any remaining subscription work is outside the committed sign-off and
  authorization-expiry cleanup paths.
- Treat the current zsh-fork timeout failures as a separate fixture health issue
  unless a future controller change newly affects that cluster.
