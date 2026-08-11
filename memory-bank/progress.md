# Progress

## Working behavior

- The TUI exposes a same-user local WebSocket endpoint and metadata under
  `$CODEX_HOME/local-controllers` for each embedded launch.
- Native TUI approval grants a connection-bound, per-launch controller session;
  an active lease is required for mutations and prompt responses.
- Controller reads, subscriptions, cursor continuations, and prompt delivery
  are bound to the authorized immutable main thread and current ownership epoch.
- The TUI remains the default input owner and reclaims control for its own
  thread-affecting actions.
- Startup cleans stale metadata/socket artifacts only when the recorded process
  is definitely dead; cleanup retains ambiguous or live records.

## Validation

- Focused `codex-app-server` controller and local-controller coverage has been
  exercised during implementation.
- Two- and five-launch downstream native-approval smoke runs passed, including
  discovery, acquire, exact-thread resume, release, sign-off, and launch
  removal reconciliation.

## Remaining work

- Treat downstream V7 as status-only until physical tap input is explicitly
  designed and accepted as a separate capability.
- Use focused controller tests after behavior changes; the broader app-server
  suite has independent zsh-fork timeout flakes.

Authoritative design facts remain in
`docs/external-controllers.md` and
`docs/external-controllers-implementation-plan.md`.
