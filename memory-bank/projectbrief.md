# Project Brief

## Project

This repository is a Codex checkout focused on Rust `codex-rs` development and
app-server/TUI integration work.

## Current durable goal

The active design thread is local external controllers for embedded interactive
Codex TUI launches. A normal `codex` TUI launch should expose its live embedded
app-server runtime to same-user local controller processes without replacing the
TUI's efficient in-process channel.

## Design outcomes to preserve

- The TUI remains the primary control surface and can reclaim input with any
  thread-affecting action.
- An approved external controller sees the same app-server v2 interface that the
  TUI sees for the TUI main thread.
- Controller authority is native, connection-bound, per-launch, and ephemeral.
  It does not rely on durable enrollment, reusable bearer tokens, or external
  client credentials.
- Local-controller discovery is based on owner-private metadata under
  `$CODEX_HOME/local-controllers`.
- Controller products must distinguish launch liveness, controller
  authorization, and slot assignment.
