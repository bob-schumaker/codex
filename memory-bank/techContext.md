# Tech Context

## Repository layout

- Rust implementation lives under `codex-rs/`.
- App-server protocol work belongs in `codex-rs/app-server-protocol`.
- App-server runtime and admission work belongs in `codex-rs/app-server`.
- Local-controller transport work belongs in `codex-rs/app-server-transport`.
- TUI integration belongs in `codex-rs/tui` and app-server client plumbing in
  `codex-rs/app-server-client`.

## Current authoritative docs

- `docs/external-controllers.md`
- `docs/external-controllers-implementation-plan.md`

These are internal app-server/controller design specs and are allowed under the
repo's app-server documentation exception.

## Validation conventions

- For Rust code changes, run `just fmt` in `codex-rs`.
- Run scoped tests with `just test -p <crate>`.
- Use app-server protocol schema generation when API shapes change.
- Markdown-only spec and memory-bank updates generally use review plus
  `git diff --check`; there is no Sphinx/docs build in this checkout.
