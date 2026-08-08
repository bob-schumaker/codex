# System Patterns

## Local external-controller model

- One embedded TUI launch owns one app-server runtime.
- The TUI is a synthetic in-process connection using typed requests/events.
- External controllers use the existing app-server JSON-RPC protocol over a
  same-user local WebSocket endpoint.
- The shared app-server runtime owns request dispatch, thread serialization,
  outgoing routing, and authorization checks.

## Authorization and ownership

- Every controller connection starts default-denied.
- `controller/requestParticipation` is the native TUI-mediated authorization
  entry point.
- Approval creates a connection-bound grant for one launch and the immutable TUI
  main thread.
- At most one external controller may hold the active `interactive-control`
  lease for that main thread.
- `controller/releaseControl` preserves read/subscription access while ceding
  input ownership back to the TUI.
- Reconnect or disconnect loses grant state and requires a new native request.

## Discovery and presentation

- Codex publishes local-controller metadata as
  `$CODEX_HOME/local-controllers/launch-<launch-id>.json`.
- Unix socket endpoints use
  `$CODEX_HOME/local-controllers/codex-<launch-id>.sock`.
- Controller products should watch the metadata directory and perform full
  rescans after file events.
- `mainThreadId: null` is a starting state, not an offline state.
- Herdr or terminal inventory can enrich labels, but must not be required for
  Codex launch discovery.
