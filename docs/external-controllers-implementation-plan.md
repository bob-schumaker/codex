# External controllers implementation plan

## Status

Internal implementation plan for `docs/external-controllers.md`.

## Goal

Implement the external-controller design as a staged vertical slice. The first
half proves controller policy inside the existing app-server runtime before any
new per-launch endpoint is discoverable. The second half exposes the local
endpoint from embedded TUI launches and wires TUI reclaim/reflection behavior.

Keep new logic out of `codex-core`. The natural owners are:

- `codex-rs/app-server-protocol`
- `codex-rs/app-server`
- `codex-rs/app-server-transport`
- `codex-rs/app-server-client`
- `codex-rs/tui`

## Design pressure

The risky part is ownership, not the socket. The implementation must centralize
connection-bound authorization, the single main-thread controller lease, prompt
rebinding, TUI reclaim, and method admission. Publishing a socket before those
rules are enforceable would expose a controller path that looks like normal
app-server but cannot yet preserve the TUI priority and transaction guarantees.

## Milestones and commit slices

### 1. Protocol contract

Commit 1: Add v2 `controller/*` DTOs, notifications, capability fields,
canonical error codes, and generated schema/TypeScript fixtures. Do not add
server behavior yet.

- Primary files:
  - `codex-rs/app-server-protocol/src/protocol/v2/controller.rs`
  - `codex-rs/app-server-protocol/src/protocol/v2/mod.rs`
  - `codex-rs/app-server-protocol/src/protocol/common.rs`, if field-level
    experimental gating needs registration
  - generated app-server schema fixtures
  - generated TypeScript protocol files
- Protocol checklist:
  - no new v1 API surface
  - method names use singular `controller/*` resource naming
  - payload types use `*Params`, `*Response`, and `*Notification`
  - v2 wire fields and string enum values are camelCase in serde and TypeScript
  - v2 request/response/notification types use `#[ts(export_to = "v2/")]`
  - no `#[serde(skip_serializing_if = "Option::is_none")]` on v2 payload fields,
    except for no-params request wrappers
  - optional client request params use `Option<...>` plus
    `#[ts(optional = nullable)]`; do not apply that TypeScript annotation to
    response or notification fields
  - absolute timestamp fields, if any are introduced, are integer Unix seconds
    named `*_at`; lease/session durations remain advisory relative durations
  - experimental methods use `#[experimental("controller/...")]`; field-level
    experimental additions derive `ExperimentalApi` and use `inspect_params:
    true` when only some fields of a method are experimental
- Validation:
  - `just write-app-server-schema`
  - `just write-app-server-schema --experimental`
  - review generated diff as mechanical-only
  - `just test -p codex-app-server-protocol`

Commit 2: Update app-server API documentation and examples for the controller
surface.

- Primary files:
  - `codex-rs/app-server/README.md`
- Scope:
  - document experimental opt-in for controller methods
  - document participation, acquire/release/sign-off semantics at the API level
  - show that external controllers use the same app-server v2 method shapes
    after authorization
- Validation:
  - read the updated examples against the generated schema names
  - no separate docs build is expected unless a docs build target is added

### 2. Admission foundation

Commit 3: Add the generated two-axis admission registry.

- Scope:
  - define target extraction: `none`, `mainThreadOnly`, `exactThread`,
    `collectionFiltered`
  - define required authority: `preParticipation`, `standingSession`,
    `activeOwner`, `tuiOnly`
  - classify existing methods, server-request responses, implicit targets,
    cursors, resume tokens, and subscriptions
  - default unknown or unclassified methods to denied
- Suggested owner:
  - new `codex-rs/app-server/src/controller_admission.rs`
- Validation:
  - unit tests for representative method classifications
  - generated registry completeness test, if practical

Commit 4: Wire admission before initialized dispatch with controller origins
still disabled.

- Primary files:
  - `codex-rs/app-server/src/message_processor.rs`
  - `codex-rs/app-server/src/transport.rs`
  - `codex-rs/app-server/src/connection_rpc_gate.rs`, only if the current gate
    needs a small extension
- Validation:
  - app-server request validation tests through the public JSON-RPC API
  - existing non-controller API behavior remains unchanged

### 3. Ownership model

Commit 5: Add controller session and ownership domain state.

- Scope:
  - `ControllerEnrollmentGrant`
  - `ControllerSession`
  - `InteractiveOwner`
  - lease IDs, owner epochs, monotonic deadlines
  - injectable clock for deterministic tests
- Suggested owner:
  - new `codex-rs/app-server/src/controller_session.rs`
- Validation:
  - deterministic unit tests for `TuiOwned`, `TransferPending`,
    `ControllerOwned`, `TuiUnavailable`, and `Closed` transitions
  - no socket or TUI wiring yet

Commit 6: Implement `controller/requestParticipation`,
`controller/acquireControl`, `controller/releaseControl`, and
`controller/signOff` against test connections.

- Primary files:
  - new controller request processor under `codex-rs/app-server/src/request_processors/`
  - `codex-rs/app-server/src/message_processor.rs`
- Validation:
  - JSON-RPC integration tests for approved session, `activeLease: null`,
    idempotent release, sign-off, and canonical errors
  - use `TestAppServer::builder().build()` and
    `TestAppServer::send_thread_start_request_with_auto_env()` by default for
    app-server tests that need a thread, so foreign app/exec OS coverage remains
    viable

### 4. Normal interface gating

Commit 7: Enforce main-thread filtering and owner-required mutation checks.

- Scope:
  - standing sessions can read/subscribe to the immutable main thread
  - active owner can perform owner-required main-thread mutations
  - wrong-thread targets use non-enumerating not-found or typed target errors
    per the design
- Validation:
  - integration tests for `thread/list`, main-thread reads, wrong-thread target,
    mutation without lease, mutation with lease

Commit 8: Add priority and stale-epoch behavior around serialized requests.

- Scope:
  - TUI thread-affecting input wins acquire-versus-input races
  - owner epoch advances before queued controller work can start
  - one already-fenced controller step may finish; queued work gets exactly one
    stale-ownership result
  - non-interactive fairness remains bounded by the documented eight-request
    rule
- Suggested owner:
  - new scheduler/coordinator module near
    `codex-rs/app-server/src/request_serialization.rs`
- Validation:
  - deterministic tests for acquire-versus-TUI-input, expiry-versus-dequeue,
    queued stale result, and fairness that does not override ownership

### 5. Prompt and egress transactionality

Commit 9: Add prompt owner binding, rebinding, and controller prompt rules.

- Scope:
  - prompt binding by request ID, recipient `ConnectionId`, prompt binding, and
    owner epoch
  - reject controller `acceptForSession`
  - rebind pending prompts on release, reclaim, expiry, disconnect, and
    pre-`externalDelivery` egress failure
  - post-`externalDelivery` revocation relies on stale response rejection, not
    prompt redelivery
- Primary files:
  - `codex-rs/app-server/src/outgoing_message.rs`
  - `codex-rs/app-server/src/transport.rs`
  - controller session/coordinator module
- Validation:
  - prompt redelivery tests
  - write-failure and stale resolver tests
  - disconnect/revocation egress-fencing tests

### 6. Local controller endpoint

Commit 10: Add `LocalControllerEndpoint` transport scaffold, disabled by
default.

- Scope:
  - metadata path and launch ID
  - launch nonce
  - private controller directory
  - cleanup guard
  - Unix-domain socket binding
  - Windows `codex-uds` adapter with equivalent same-user endpoint semantics
  - same-user peer/nonce checks
  - if a platform adapter cannot satisfy peer verification, nonce validation, and
    cleanup guarantees, expose controllers as unavailable on that platform
    rather than shipping a partial stub
- Primary files:
  - new `codex-rs/app-server-transport/src/transport/local_controller.rs`
  - `codex-rs/app-server-transport/src/transport/mod.rs`
- Validation:
  - tempdir metadata/socket tests
  - nonce rejection tests
  - stale/foreign resource cleanup tests

Commit 11: Start the endpoint from embedded TUI launches only.

- Scope:
  - expose controller availability states
  - embedded launches may publish endpoint metadata
  - daemon-backed and explicit remote launches report unsupported
  - `TuiUnavailable` remains terminal for the launch
- Primary files:
  - `codex-rs/tui/src/lib.rs`
  - `codex-rs/app-server-client/src/lib.rs`, if startup args need a typed hook
- Validation:
  - app-server/TUI startup tests
  - TUI snapshot tests only if user-visible text changes

### 7. TUI reclaim and reflection

Commit 12: Add a TUI command classifier and reclaim hook.

- Scope:
  - classify every TUI-originated command that can reach the coordinator as
    `threadAffecting` or `displayOnly`
  - new commands default to `threadAffecting` until reviewed
  - display-only actions do not reclaim control
  - keep high-touch TUI files as orchestration glue; place classifier and
    reclaim policy in focused modules
- Primary files:
  - new `codex-rs/tui/src/controller_reclaim.rs`, or an equivalent focused
    module
  - `codex-rs/tui/src/app_command.rs`
  - `codex-rs/tui/src/app.rs`, only for narrow integration wiring
- Validation:
  - classifier tests
  - reclaim tests for submit/resume/cancel/interrupt, approvals, user-input
    replies, mutating slash commands, and display-only actions

Commit 13: Ensure controller-originated work is reflected through the canonical
TUI event stream only.

- Scope:
  - TUI renders controller-originated events from normal app-server events
  - no hidden controller transcript
  - gap handling resynchronizes from atomic snapshot-at-sequence with owner and
    prompt state
  - keep reflection/recovery rules in reducer or focused helper modules rather
    than growing `app.rs`
- Primary files:
  - `codex-rs/app-server-client/src/lib.rs`
  - new or existing focused TUI event/reducer modules
  - `codex-rs/tui/src/app.rs`, only for narrow integration wiring
- Validation:
  - end-to-end controller turn reflected in TUI state
  - gapped-stream recovery test where feasible

### 8. Hardening and cleanup

Commit 14: Add the full end-to-end scenario.

- Scenario:
  - launch embedded TUI runtime
  - discover local controller endpoint
  - authorize a Codex Micro-style controller
  - verify normal app-server parity for the granted main thread
  - resolve an approval through the controller
  - send thread-affecting TUI input that reclaims control
  - verify prompt ownership and final thread state
- Validation:
  - `just test -p codex-app-server`
  - `just test -p codex-tui`

Commit 15: Remove temporary compatibility shims and duplicated routing only
after behavioral parity is proven.

- Scope:
  - cleanup-only
  - no new behavior
  - keep public API and generated schemas stable
- Validation:
  - scoped app-server and TUI tests

## Commit-size guidance

Keep behavioral commits under roughly 500 changed lines when practical. The
protocol contract commit may exceed that when it includes generated
schema/TypeScript fixtures; keep the generated portion mechanical and easy to
review. Avoid combining protocol shape, runtime policy, endpoint publication,
and TUI behavior in one commit.

If a slice grows past 800 changed lines and is not purely generated output,
split it by owner:

- protocol shape
- domain state
- dispatch/admission wiring
- transport adapter
- TUI behavior
- tests

## Validation cadence

After code changes in `codex-rs`, run:

```text
just fmt
```

For protocol commits:

```text
just write-app-server-schema
just write-app-server-schema --experimental
just test -p codex-app-server-protocol
```

For app-server behavior:

```text
just test -p codex-app-server
```

For TUI-visible changes:

```text
just test -p codex-tui
cargo insta pending-snapshots -p codex-tui
```

Accept snapshot updates only after reviewing the generated `*.snap.new` files.

Before a broad shared-runtime merge, ask before running the complete
workspace-wide `just test`, because that is intentionally heavier than the
project-scoped checks.

Before finalizing a large crate slice, run the scoped lint fixer:

```text
just fix -p <crate>
```

Use `just fix` without `-p` only when the change truly spans shared crates. Do
not run `cargo test` directly; use the repository `just test` targets.

## Repository-maintenance triggers

- If `ConfigToml` or nested config types change, run `just write-config-schema`
  and include the schema update.
- If `Cargo.toml` or `Cargo.lock` changes, run `just bazel-lock-update` from the
  repo root and include `MODULE.bazel.lock`.
- If implementation adds `include_str!`, `include_bytes!`, migrations, or other
  compile-time source-tree reads, update the owning crate's Bazel data entries.
- Keep new controller logic out of `codex-core` unless a later design explicitly
  justifies a new core boundary.
- New Rust test modules should live in sibling `*_tests.rs` files with an
  explicit `#[path = "..._tests.rs"]` module declaration when introducing a new
  test module.
- TUI-visible text or UI changes require `codex-tui` snapshot coverage and
  reviewed `*.snap.new` files before accepting snapshots.

## Landing order guardrails

- Do not publish the local controller endpoint until the default-deny gate,
  admission registry, ownership model, and revocation path are in place.
- Do not allow controller mutation routing until main-thread filtering and owner
  epochs are enforced at admission and execution fence.
- Do not wire TUI reclaim through ad hoc call sites; use the command classifier
  as the single source of truth.
- Do not make `leaseId` a bearer token. Authority remains bound to
  `ConnectionId`, session state, owner epoch, and server-owned deadlines.
- Do not add controller behavior to v1 APIs.
