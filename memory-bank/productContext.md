# Product Context

## User problem

Users may operate an active Codex TUI through another same-user control surface,
such as a smart input device or companion controller, while still expecting the
TUI display to stay current and authoritative.

## Desired behavior

- A plain embedded `codex` launch publishes a local-controller endpoint.
- A controller discovers live launches from local metadata and asks the owning
  TUI for participation.
- User approval means the external controller is allowed to become the current
  input method for the TUI main thread.
- The controller can acquire control, perform an action, and release control
  without requiring another TUI prompt while its connection-bound authorization
  remains live.
- Any thread-affecting TUI input cancels the controller's active lease and
  returns input ownership to the TUI.
- Controller-originated actions are reflected in the TUI as normal app-server
  events, not as a separate transcript.

## Current downstream product issue

The downstream controller display was showing only three sessions and marking
them offline. The design now records that discovery should watch and rescan
`$CODEX_HOME/local-controllers`, and that a live metadata/process/socket/main
thread is online even before controller approval or active ownership.
