# External controllers implementation plan

## Status

Internal implementation plan for `docs/external-controllers.md`.

## Goal

Implement the external-controller design as a staged vertical slice. The first
half proves controller policy inside the existing app-server runtime before any
new per-launch endpoint is discoverable. The second half exposes the local
endpoint from embedded TUI launches, wires TUI reclaim/reflection behavior, and
defines the downstream controller-host discovery and presentation work needed to
consume those launches correctly.

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
  - classify model-context-changing and history-rewriting methods or params, such
    as resume-history injection, rollback, compaction, and instruction/config
    overrides; deny controller-origin fields that replace or inject history, or
    mark the method TUI-only until a manual model-context review approves a
    bounded contract
  - default unknown or unclassified methods to denied
- Suggested owner:
  - new `codex-rs/app-server/src/controller_admission.rs`
- Validation:
  - exhaustive generated registry completeness test for every method,
    server-request response, implicit target, cursor, resume token, and
    subscription
  - targeted tests for context-safety classification of history-rewriting and
    context-injecting surfaces
  - continuation tests proving cursors, subscriptions, and resume tokens remain
    bound to the same `ConnectionId` and main thread and cannot be replayed
    across connections or threads
  - implemented checkpoint `fee11eb`:
    - explicit `thread/unsubscribe` has exact-thread target-extraction coverage;
      and
    - an approved controller with a standing read/subscription session can
      unsubscribe from the main thread after releasing input ownership, while a
      wrong-thread unsubscribe is rejected before the handler can mutate
      subscriptions

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

Commit 6: Add native controller participation grants and revocation.

- Scope:
  - `ControllerEnrollmentGrant` source-of-truth boundary owned by the live
    embedded TUI request processor
  - native TUI approval creates a grant only for the live `ConnectionId`, launch
    ID, and immutable main thread
  - no external client credential, durable enrollment record, controller
    registry, host-defined authority, or platform credential storage
  - authorization epoch, revocation epoch, expiry, policy disabled, and policy
    required handling
  - no socket publication or controller mutation routing yet
- Suggested owner:
  - focused controller enrollment module near `controller_session.rs`
- Validation:
  - native approval, native rejection, policy disabled, policy required, grant
    expiry, disconnect, and revocation epoch tests
  - tests proving reconnect loses grant state and requires a new native
    participation request
  - tests proving display claims such as controller name or description never
    satisfy authorization

Commit 7: Implement `controller/requestParticipation`,
`controller/acquireControl`, `controller/releaseControl`, and
`controller/signOff` through the native participation grant verifier and test
connections.

- Primary files:
  - new controller request processor under `codex-rs/app-server/src/request_processors/`
  - `codex-rs/app-server/src/message_processor.rs`
- Validation:
  - JSON-RPC integration tests for pre-participation denial, experimental opt-in
    gating, enrollment rejection, approved session, `activeLease: null`,
    retryable `main-thread-unavailable`, terminal main-thread and TUI errors,
    no-events-before-approval, idempotent release, sign-off, and canonical
    errors
  - use `TestAppServer::builder().build()` and
    `TestAppServer::send_thread_start_request_with_auto_env()` by default for
    app-server tests that need a thread, so foreign app/exec OS coverage remains
    viable

### 4. Normal interface gating

Commit 8: Enforce main-thread filtering and owner-required mutation checks.

- Scope:
  - standing sessions can read/subscribe to the immutable main thread
  - active owner can perform owner-required main-thread mutations
  - wrong-thread targets use non-enumerating not-found or typed target errors
    per the design
- Validation:
  - integration tests for `thread/list`, main-thread reads, wrong-thread target,
    mutation without lease, mutation with lease
  - regression tests that existing non-controller thread resume/session behavior
    still works and that controller-bound continuations cannot alter stored
    rollouts outside the main-thread authorization boundary

Commit 9: Add priority and stale-epoch behavior around serialized requests.

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

Commit 10: Add prompt owner binding and controller prompt decision rules.

- Scope:
  - prompt binding by request ID, recipient `ConnectionId`, prompt binding, and
    owner epoch
  - reject controller `acceptForSession`
- Primary files:
  - `codex-rs/app-server/src/outgoing_message.rs`
  - `codex-rs/app-server/src/transport.rs`
  - controller session/coordinator module
- Validation:
  - prompt binding and single-authorized-resolver tests
  - controller `accept`/`cancel` acceptance and `acceptForSession` rejection
    tests
  - stale resolver tests

Commit 11: Add prompt rebinding on ownership transitions.

- Scope:
  - rebind pending prompts on release, TUI reclaim, lease expiry, authorization
    revocation, controller disconnect, and sign-off
  - preserve coordinator-owned `pending`, `resolved`, and `cancelled` prompt
    state
  - publish ownership-status changes in canonical order
- Validation:
  - prompt redelivery tests for each rebinding trigger
  - disconnect/revocation egress-fencing tests
  - deterministic race tests for response-versus-revoke and expiry-versus-reply

Commit 12: Add controller egress delivery and backpressure semantics.

- Scope:
  - pre-`externalDelivery` egress validation and write-failure handling
  - post-`externalDelivery` stale response rejection without redelivery
  - isolated controller egress queues and reserved TUI capacity
  - saturated controller ingress overload responses
  - separate bounded controller control-plane ingress so saturated normal
    controller traffic cannot block participation, acquire, release, or sign-off
- Primary files:
  - `codex-rs/app-server/src/outgoing_message.rs`
  - `codex-rs/app-server/src/transport.rs`
  - controller session/coordinator module
- Validation:
  - implemented checkpoint `7359445`:
    - queued controller-bound prompts carry a connection-bound write permit;
    - all outbound writers revalidate the permit at begin-write before
      serialization/send;
    - automatic redelivery/replay treats a begun controller write as already
      committed for duplicate-prevention, while a failed write removes that
      in-flight marker and falls back to the TUI when the prompt is still
      pending; and
    - post-delivery and begin-write controller paths avoid duplicate prompt
      redelivery while retaining the TUI-primary prompt-reclaim path.
  - implemented checkpoint `9bdbda6`:
    - slow external-controller queue overflow disconnects only that external
      connection and preserves subsequent primary delivery; and
    - controller-bound prompt overflow through the real outbound router drops
      the controller delivery before `externalDelivery` and rebinds the still
      pending prompt to the TUI.
  - write-failure and pre-/post-`externalDelivery` tests
  - saturated controller ingress returns `-32001` or typed
    `controller-overloaded` according to the design path being exercised
  - saturated normal controller ingress still allows `controller/releaseControl`
    and `controller/signOff` to reach dispatch through their separate bounded
    control-plane reservation
  - slow or disconnected controller cannot block TUI ingress, TUI egress, or the
    runtime dispatcher
  - isolated egress queue overflow disconnects or drops only explicitly lossy
    notifications

### 6. Local controller endpoint

Commit 13: Add portable local-controller endpoint metadata and cleanup,
disabled by default.

- Scope:
  - metadata path and launch ID
  - launch nonce
  - private controller directory
  - cleanup guard
  - atomic metadata publication and stale resource validation
  - no socket listener yet
- Primary files:
  - new `codex-rs/app-server-transport/src/transport/local_controller.rs`
  - `codex-rs/app-server-transport/src/transport/mod.rs`
- Validation:
  - tempdir metadata tests
  - stale/foreign resource cleanup tests
  - tests proving cleanup never follows symlinks or deletes resources not owned
    by the launch ID and nonce

Commit 14: Add the Unix local-controller WebSocket transport, still disabled by
default.

- Scope:
  - Unix-domain socket binding
  - same-user peer/nonce checks
  - HTTP Upgrade nonce validation
  - keep the persistent daemon/control socket and standalone
    `codex app-server --listen unix://...` behavior unchanged
- Primary files:
  - new `codex-rs/app-server-transport/src/transport/local_controller.rs`
  - `codex-rs/app-server-transport/src/transport/mod.rs`
- Validation:
  - socket binding tests
  - same-user peer credential tests and credential-missing rejection tests
  - nonce rejection tests
  - standalone app-server listener regression for `--listen unix://...`

Commit 15: Add Windows `codex-uds` support or explicit unavailable fallback.

- Scope:
  - Windows `codex-uds` adapter with equivalent same-user endpoint semantics, or
    an explicit unavailable state when the platform adapter cannot satisfy peer
    verification, nonce validation, and cleanup guarantees
  - no partial security stub that accepts controller connections without those
    guarantees
- Validation:
  - Windows adapter tests where the platform endpoint is supported
  - unsupported-platform fallback tests where peer verification is unavailable

### 7. TUI reclaim, reflection, and endpoint publication

Commit 16: Add a TUI command classifier and reclaim hook.

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

Commit 17: Ensure controller-originated work is reflected through the canonical
TUI event stream only.

- Scope:
  - TUI renders controller-originated events from normal app-server events
  - no hidden controller transcript
  - gap handling resynchronizes from atomic snapshot-at-sequence with owner and
    prompt state
  - keep reflection/recovery rules in reducer or focused helper modules rather
    than growing `app.rs`
  - ownership, provenance, and controller-status metadata must remain out of
    model-visible history unless a later implementation explicitly introduces a
    bounded `core/context` fragment implementing `ContextualUserFragment`
- Primary files:
  - `codex-rs/app-server-client/src/lib.rs`
  - new or existing focused TUI event/reducer modules
  - `codex-rs/tui/src/app.rs`, only for narrow integration wiring
- Validation:
  - end-to-end controller turn reflected in TUI state
  - deterministic reducer/recovery tests for gapped-stream resynchronization
  - embedded runtime and app-server-client lossless-delivery classifier tests
    for controller-visible lifecycle/state notifications
  - tests or code review evidence that ownership/provenance/status events do not
    mutate model-visible history
  - implemented checkpoint `55c979b`:
    - the typed in-process `ControllerOwnershipStatus` event is handled through
      the real TUI app-server event handler without inserting transcript history
      or creating an active transcript cell; and
    - the related validation set covers that path with the existing JSON-RPC
      controller control-plane history exclusion and lag snapshot recovery tests
  - open checkpoint after `55c979b`:
    - the production implementation currently provides lossless in-process
      event classes, `Lagged` signaling, and active-thread
      `thread/read(includeTurns=true)` recovery, but does not expose the formal
      `threadSequence` / `lastSequence` surface or an atomic
      snapshot-at-sequence containing interactive owner and prompt-binding
      state; do not mark this commit complete until that gap is implemented or
      the design is explicitly amended

Commit 18: Start the endpoint from embedded TUI launches only, after admission,
ownership, enrollment, prompt fencing, reclaim, and reflection are in place.

- Scope:
  - expose controller availability states
  - embedded launches may publish endpoint metadata under
    `$CODEX_HOME/local-controllers`
  - daemon-backed and explicit remote launches report unsupported
  - `TuiUnavailable` remains terminal for the launch
- Primary files:
  - `codex-rs/tui/src/lib.rs`
  - `codex-rs/app-server-client/src/lib.rs`, if startup args need a typed hook
- Validation:
  - startup tests for `embedded-supported`, `daemon-unsupported`,
    `remote-unsupported`, `policy-disabled`, `embedded-unavailable`, and
    `launch-failed`
  - acceptor-failure transition tests that revoke established controller
    sessions and update the availability state
  - standalone `codex app-server --listen unix://...` and daemon/control-socket
    regression tests
  - TUI snapshot tests only if user-visible text changes

### 8. Downstream controller-host discovery and presentation

This work belongs in controller products such as Codex Waveshare rather than in
the Codex app-server protocol. The Codex-side discovery contract is the
owner-private local-controller metadata directory; generic Codex hooks and
Herdr metadata are not launch-discovery authorities.

External slice A: Watch and rescan the local-controller metadata directory.

- Scope:
  - watch `$CODEX_HOME/local-controllers` with the host OS file-watch facility
  - perform a full directory rescan after every create, modify, delete, or
    overflow event; polling is an acceptable fallback
  - validate candidate `launch-*.json` records against directory privacy,
    metadata version, filename launch ID, endpoint URI, nonce-bearing socket
    path, live `processId`, and socket existence
  - treat `mainThreadId: null` as a starting launch, not an offline launch
  - remove candidates from presentation only when metadata disappears, the
    process dies, the socket becomes invalid, validation fails, or Codex returns
    a terminal no-main-thread state
- Validation:
  - launch multiple standalone TUIs and verify every live metadata record is
    discovered after one rescan
  - kill one TUI and verify only that launch becomes offline/removed
  - verify a metadata update that fills `mainThreadId` transitions from
    `starting` to discovered without requiring a controller reconnect

External slice B: Decouple launch health, authorization, and slot assignment.

- Scope:
  - model launch liveness from metadata/process/socket/main-thread validation
  - model controller authorization separately from participation state,
    standing read session, and active input lease
  - model product slot assignment separately from both launch liveness and
    authorization
  - do not report a live launch as offline merely because participation is
    pending, TUI approval has not been granted, the controller has released
    control, or another surface owns input
- Validation:
  - approved, awaiting-approval, connected-read-only, and released-control
    launches all present as online/non-offline states
  - only dead process, missing metadata, invalid socket, or terminal
    `tui-unavailable`/`main-thread-closed` states present as offline

External slice C: Auto-assign newly discovered launches when the product wants a
slot-backed display.

- Scope:
  - preserve explicit user slot assignments
  - fill free slots deterministically for discovered live launches not already
    assigned
  - avoid requiring Herdr `agent_session` or terminal metadata for discovery;
    use Herdr only for optional labels, grouping, or focus/status enrichment
- Validation:
  - with five live Codex launches, four under Herdr and one plain external
    `codex`, all five are assigned or offered for assignment after discovery
  - a Herdr pane missing `agent_session` does not hide a validated Codex
    local-controller launch
  - restart of the controller host preserves explicit assignments and assigns
    only newly discovered unassigned launches to free slots

### 9. Hardening and cleanup

Commit 19: Add the full end-to-end scenario.

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

Commit 20: Remove temporary compatibility shims and duplicated routing only
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

For each implementation slice, run the project test for every changed crate. For
example:

```text
just test -p codex-app-server-protocol
just test -p codex-app-server
just test -p codex-app-server-transport
just test -p codex-app-server-client
just test -p codex-tui
```

Choose the subset that matches the files changed by the slice.

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
