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
- Interim boundary:
  - until Commit 10 lands the prompt binding and response gate, eligible prompt
    creation/delivery and responses remain TUI-only even when a controller has
    an active lease; no controller-owned prompt may exist without its request
    ID, recipient connection, and owner epoch binding
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

#### Current delivery status

The current worktree implements the core of Commits 10 and 11: eligible
pending-prompt transfer, recipient/epoch fencing, typed stale rejection,
sequenced in-process ownership updates, TUI remote-control presentation,
controller egress recovery, embedded-runtime deadline expiry, native
thread-scoped controller revocation, and deterministic test-only TUI egress
failure controls. It also covers the live local-controller WebSocket/TUI
command-approval flow.

The remaining work is deliberately not represented as complete: add public
transport deterministic barriers for every required interleaving, complete the
full per-variant WebSocket acceptance matrix, and run/resolve the complete
crate-suite validation before landing. The detailed gaps remain listed under
Commit 11 and Commit 18 rather than being hidden by the implemented focused
tests.

#### Implementation-ready remaining slices (ROI order)

1. **Terminal TUI recovery proof:** in
   `codex-rs/app-server/src/in_process.rs`, extend the live embedded WebSocket
   fixture to transfer an eligible command prompt, trigger controller ownership
   loss with independently addressable one-shot hooks already armed for (a) prompt
   redelivery enqueue rejection and (b) server-request-consumer closure. The
   former, while its event sink remains live, also asserts the sequenced
   `TuiUnavailable` event; the closed-consumer case instead asserts terminal
   coordinator/runtime state. Both assert the prompt callback is
   failed/cancelled, controller binding and write permit are gone, all leases
   are revoked, the old controller reply is JSON-RPC `stale-ownership`, and
   subsequent controller participation/mutation receives JSON-RPC
   `tui-unavailable`. Do not add a third ownership-bundle hook until an actual
   lossless ownership-bundle sink exists; these hooks are `cfg(test)` only and
   do not cover controller egress faults.
2. **Native TUI revocation affordance:** in the existing main-thread ownership
   UI module and `app_server_requests.rs`, add a labeled, no-confirmation
   “Revoke controller access” action. Enable it whenever the main thread has
   an active controller owner *or* a read-capable standing controller session.
   Add a sequenced native `hasControllerSession` state bit with ownership,
   disconnect, and revocation updates; it exposes no controller identity or
   control-plane notification to the TUI. The action is absent/disabled when no
   session exists and remains idempotent if it disappears before dispatch. Route it to
   `AppServerClient::revoke_controller_access`, report transport/terminal
   failure through the existing status surface, and snapshot active-owner,
   standing-only, and no-session states without transcript mutation.
3. **Public race barriers:** in `outgoing_message.rs`, controller processor
   tests, and the local-controller test fixture, use the smallest test-local
   `cfg(test)` gate needed by each race: pre-transfer linearization for TUI
   reply, pre-consumption for controller reply versus reclaim, and recovery
   entry after completed/possibly disclosed delivery for controller reply
   versus recovery. Each gate asserts its current binding/delivery lifecycle
   and no gate exposes a partial ownership/binding transition. Drive one
   WebSocket controller plus one in-process TUI through: TUI reply versus
   transfer (TUI stale, no reclaim); controller reply versus thread-affecting
   TUI reclaim (reply-win resolves, reclaim-win makes TUI actionable); and
   controller reply versus recovery (exactly one binding consumer, stale
   loser). No barrier is reachable through public RPC in production.
4. **Public variant/loss matrix:** in the same live fixture, give each of the
   four eligible variants one public transfer-and-normal-response case using
   the existing mock-response producers for command execution, file change,
   permission, and tool user-input server requests; each asserts the original
   public request ID and normal response shape. Prove
   dynamic tool and MCP elicitation stay TUI-only under controller ownership.
   Keep the other excluded families on the existing static classifier path; do
   not duplicate that classification in another unit table. Use one command-approval representative for
   controller egress/ownership loss: queue rejection, write-start/write
   failure, connection-closed, completed-delivery then disconnect, release,
   sign-off, explicit policy revocation, manual-clock expiry, and a separate
   thread-affecting TUI reclaim. Each successful fallback is locally actionable
   with stale old-controller reply; TUI fallback failure belongs only to slice
   1. This is deliberately pairwise, not a Cartesian product.
5. **Selection and release audit:** in `outgoing_message`/controller tests,
   reuse slice 3's pre-transfer gate to establish resolved (consume response),
   cancelled (cancel callback), and superseded (use the real producer
   cancellation/supersession signal to mark
   the original request cancelled/failed and create any later request under a
   distinct request ID/binding); use an auth/attestation process-wide request
   for non-thread-scoped. Assert
   the original and later IDs independently; neither terminal original is
   transferred or resolvable. For each pending prompt, assert exactly one controller
   redelivery per *new transfer epoch*, including after successful recovery and
   reacquire; never more than one in an epoch and never simultaneous active
   resolvers. After these code slices, run scoped app-server/TUI suites and ask
   before the complete workspace suite.

Commit 10: Add prompt owner binding and controller prompt decision rules.

- Scope:
  - bind eligible thread-scoped prompt variants by original request ID,
    recipient `ConnectionId`, and owner epoch
  - include `CommandExecutionRequestApproval`, `FileChangeRequestApproval`,
    `PermissionsRequestApproval`, and `ToolRequestUserInput`; keep MCP
    elicitation, dynamic-tool, auth, attestation, time, patch, and legacy exec
    requests out of this transfer slice
  - authorize responses by server-stored `(request ID, receiving connection,
    owner epoch)` rather than changing existing response payloads
  - map stale responses to typed in-process rejection for the TUI and the
    existing JSON-RPC `ControllerErrorData.code: stale-ownership` for external
    controllers
  - reject every controller decision that outlives its lease: `acceptForSession`,
    exec-policy amendments, network-policy amendments, and session-scoped
    permission grants
- Primary files:
  - `codex-rs/app-server/src/outgoing_message.rs`
  - `codex-rs/app-server/src/transport.rs`
  - controller session/coordinator module
- Validation:
  - prompt binding and single-authorized-resolver tests
  - controller `accept`/`cancel` acceptance and lifetime-extending-decision
    rejection tests
  - stale resolver tests

Commit 11: Add atomic pending-prompt transfer, egress fencing, and recovery.

- Scope:
  - extend the existing per-thread coordinator/transition barrier and outgoing
    write permit rather than adding a parallel prompt coordinator or event path
  - inside one barrier spanning owner state and pending-request bindings, select
    every eligible still-pending main-thread prompt, advance the owner epoch,
    commit controller ownership, and invalidate every former TUI reply binding
    before either state becomes externally visible
  - publish the existing sequenced in-process ownership update before releasing
    that barrier, then enqueue controller prompt egress; while controller-owned,
    the TUI disables local actions for eligible still-pending prompts
  - allow at most one controller redelivery for each prompt in a transfer epoch;
    reclaim/revocation tombstones stale queued envelopes before a later transfer
    can redeliver
  - carry the current prompt binding and owner epoch through controller prompt
    egress and revalidate the existing revocable write permit immediately before
    writing
  - define `write_started` as possibly disclosed and record `externalDelivery`
    on successful frame completion; neither state blocks prompt recovery
  - rebind still-pending prompts to the TUI on release, separate thread-affecting
    TUI reclaim, lease expiry, authorization revocation, controller disconnect,
    and sign-off
  - add the native, thread-scoped TUI
    `revokeControllerAccess(mainThreadId)` action; it is idempotent and revokes
    every controller session for that main thread, including read-capable
    standing sessions, rather than merely releasing the active lease
  - on controller egress failure or ownership loss, atomically fence stale queued
    egress and recover the still-pending prompt to the viable TUI recipient;
    if recovery cannot reach the TUI, cancel/fail the prompt and enter or retain
    terminal `TuiUnavailable`
  - preserve coordinator-owned `pending`, `resolved`, and `cancelled` prompt
    state
- Validation:
  - TUI display/reducer tests showing transferred prompts are remotely controlled
    and no longer locally actionable
  - deterministic barriers for TUI-reply-versus-transfer,
    controller-reply-versus-TUI-reclaim, and controller-reply-versus-recovery
    races; competing replies resolve once and the losing reply is stale, while a
    reclaim-winning race leaves the prompt TUI-actionable
  - deterministic pre-linearization tests proving resolved, cancelled,
    superseded, and no-longer-thread-scoped requests are not transferred
  - acquire/reclaim/reacquire tests proving at most one redelivery per transfer
    epoch and no duplicate active resolver
  - update the historical checkpoint `7359445`: retain its connection-bound
    write permit and pre-write revalidation, but replace the old no-rebind
    behavior after begun/completed controller delivery with the recovery rule
    above
  - implemented checkpoint `9bdbda6`:
    - slow external-controller queue overflow disconnects only that external
      connection and preserves subsequent primary delivery; and
    - controller-bound prompt overflow through the real outbound router drops
      the controller delivery before `externalDelivery` and rebinds the still
      pending prompt to the TUI.
  - deterministic enqueue, write-start/write-failure, connection-closed, and
    post-delivery disconnect/revocation/expiry/sign-off tests proving recovery,
    stale old controller replies, and no double authorization
  - terminal `TuiUnavailable` tests for missing or failed TUI fallback
  - add a test-only in-process TUI egress-sink fault hook with two deterministic
    modes: reject the next server-request enqueue, or close the server-request
    consumer. It is unavailable in production builds and verifies that either
    failed recovery path cancels/fails the prompt and enters terminal
    `TuiUnavailable` without leaving a controller binding live.

Implementation note: the current worktree covers transfer/reply and
recovery/reply races with deterministic internal barriers and exercises the
former TUI stale result through the live in-process/WebSocket path. It does not
yet supply public-transport deterministic barriers for every listed race or a
complete post-delivery ownership-loss matrix for every loss cause.

Deferred follow-up: keep generic controller ingress reservations, queue sizing,
and lossy-notification policy in a separately reviewable hardening slice. This
feature changes only prompt-bound egress/recovery and reuses the existing
queues and control-plane reservation behavior.

### 6. Local controller endpoint

Commit 12: Add portable local-controller endpoint metadata and cleanup,
disabled by default.

- Scope:
  - metadata path and launch ID
  - launch nonce
  - private controller directory
  - cleanup guard
  - atomic metadata publication and stale resource validation
  - startup pruning for dead-process launch metadata and socket artifacts left
    by abnormal exits, with ambiguous process liveness preserved
  - no socket listener yet
- Primary files:
  - new `codex-rs/app-server-transport/src/transport/local_controller.rs`
  - `codex-rs/app-server-transport/src/transport/mod.rs`
- Validation:
  - tempdir metadata tests
  - stale/foreign resource cleanup tests
  - concurrent stale-prune tests proving `NotFound` races are tolerated and
    live-process records are preserved
  - tests proving cleanup never follows symlinks or deletes resources not owned
    by the launch ID and nonce

Commit 13: Add the Unix local-controller WebSocket transport, still disabled by
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

Commit 14: Add Windows `codex-uds` support or explicit unavailable fallback.

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

Commit 15: Add a TUI command classifier and reclaim hook.

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
  - reclaim tests for submit/resume/cancel/interrupt, mutating slash commands,
    and display-only actions
  - tests proving a TUI approval or user-input reply to a still-pending
    controller-transferred prompt fails stale and does not reclaim control

Commit 16: Ensure controller-originated work is reflected through the canonical
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
  - implemented checkpoint `dd2c93e`:
    - the TUI `ThreadEventStore` now snapshots a local monotonic
      `last_sequence`, advancing it for session refreshes, inbound
      notifications, inbound requests, and controller-ownership status updates;
    - snapshots retain the latest typed controller-ownership status alongside
      normal thread state; and
    - replay logging records the recovered sequence and whether ownership state
      was present without adding ownership/status data to transcript history.
  - implemented checkpoint `809b9e1`:
    - the app-server protocol and runtime now expose the formal per-thread
      `threadSequence` event-envelope field and `ThreadReadResponse.lastSequence`
      snapshot field; and
    - app-server-owned thread dispatch assigns canonical monotonic sequence
      values to thread-scoped notifications and server requests.
  - implemented checkpoint in the current in-process TUI sequence-preservation
    slice:
    - in-process app-server events preserve sequenced server notifications and
      server requests instead of flattening them to legacy unsequenced events;
    - `codex-app-server-client` exposes the sequenced event variants to embedded
      TUI consumers while keeping the legacy event shape for unsequenced events;
    - TUI lag recovery reads `ThreadReadResponse.lastSequence`, seeds the
      `ThreadEventStore` with that authoritative app-server sequence, and drops
      stale sequenced events at or below the refreshed snapshot sequence; and
    - `codex-exec` normalizes sequenced in-process events with the legacy
      request/notification variants.
  - implemented checkpoint in the current in-process recovery-snapshot slice:
    - embedded TUI lag recovery now requests an internal app-server-owned
      snapshot, not a public JSON-RPC method;
    - the snapshot packages the normal `thread/read` view, authoritative
      `lastSequence`, current controller ownership status, owner epoch, and
      pending server requests for TUI replay; and
    - remote/daemon TUI sessions continue to fall back to the public
      `thread/read` path.

Commit 17: Start the endpoint from embedded TUI launches only, after admission,
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
  - implemented checkpoint in the current availability reconciliation slice:
    - embedded TUI startup requests a best-effort local-controller endpoint by
      default, can disable or require it by policy, and reports supported,
      unavailable, disabled, required-failure, daemon-unsupported, and
      remote-unsupported availability states;
    - best-effort acceptor startup failure allows embedded app-server startup,
      required acceptor startup failure aborts launch, late terminal acceptor
      failure closes controller sessions through the normal revocation path and
      reports `embedded-unavailable`, and metadata publication updates
      `mainThreadId` once without replacing the immutable main-thread binding;
      and
    - local-controller transport coverage preserves nonce-gated metadata
      publication and existing Unix control-socket/default `unix://` behavior.

Commit 17a: Publish the session working directory in local-controller
metadata.

- Scope:
  - add optional `sessionWorkingDirectory` to the existing `launch-*.json`
    payload, derived from the launch CWD and omitted when it is not valid UTF-8
  - keep app-server v2 methods, WebSocket/JSON-RPC payloads, TUI events, and
    in-process interaction contracts unchanged
  - inject the field only in the metadata-file writer; leave
    `LocalControllerEndpointMetadata` and its in-process/public re-exports
    unchanged, retaining the value privately only for the `mainThreadId` rewrite
  - capture the CWD once at endpoint startup; do not re-read the
    process CWD during later metadata writes
- Primary files:
  - `codex-rs/app-server-transport/src/transport/local_controller.rs`
  - existing local-controller metadata tests
- Validation:
  - inspect initial `launch-*.json` for a representative CWD and assert it
    contains `sessionWorkingDirectory` with that value while the
    in-process metadata object retains its current shape
  - verify an unavailable or non-UTF-8 CWD omits the field
  - assert the field remains present and unchanged after the `mainThreadId`
    rewrite
  - `just fmt`
  - `just test -p codex-app-server-transport local_controller`

Commit 18: Validate pending-prompt transfer through the published local
controller endpoint.

- Scope:
  - use the public local-controller WebSocket protocol against a live embedded
    in-process TUI; do not substitute internal rebind helpers for this slice
  - transfer already TUI-delivered command, file-change, permissions, and
    user-input prompts with their original request IDs and normal response
    shapes
  - retain MCP elicitation and dynamic-tool calls as TUI-only while a controller
    holds the lease
- Validation:
  - controller acquire causes exactly one additional redelivery per transfer
    epoch; the TUI marks the prompt remotely controlled and controller `accept`
    resolves the original request
  - disconnect, sign-off, revocation, expiry, and enqueue/write failure recover
    the still-pending prompt to the TUI, including after completed controller
    delivery
  - a controller reply versus TUI reclaim is deterministic: a reclaim win leaves
    the prompt locally actionable, while a reply win resolves it; stale replies
    use the transport-appropriate error form
  - deterministic public-protocol barriers cover former-TUI-reply versus
    transfer, controller-reply versus TUI reclaim, and controller-reply versus
    recovery; verify the former TUI reply is typed in-process stale without
    reclaim and exactly one reply can resolve
  - resolved, cancelled, superseded, and non-thread-scoped requests are never
    replayed

Implementation note: the current local-controller e2e covers a TUI-originated
command approval, native participation/acquisition, stale former-TUI reply,
and controller resolution. File-change, permissions, user-input, and every
ownership-loss case still need equivalent public-WebSocket/live-TUI coverage.

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
  - use optional `sessionWorkingDirectory` when it is a string; otherwise
    retain the existing fallback label
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
  - verify a present session working directory remains stable across that
    update and an absent value does not suppress the existing fallback label
  - implemented checkpoint in downstream commit `508880c`
    (`fix(host): filter stale Codex controller launches`):
    - downstream `LocalControllerDiscovery` decodes `processId` and filters
      metadata whose process is no longer live before producing an endpoint;
    - focused regression coverage creates owner-private launch metadata plus
      real AF_UNIX socket files and proves a dead-process record is ignored; and
    - downstream `swift test --package-path host/FirstVerticalSliceHost`,
      `swift build --package-path host/FirstVerticalSliceHost`, and
      `pre-commit run --files ...` passed.
  - implemented checkpoint in downstream commit `d43fcfb`
    (`fix(host): watch Codex launches and route taps`):
    - downstream `LocalControllerDiscovery` now exposes a retained OS file
      watch on `$CODEX_HOME/local-controllers`;
    - `ExternalControllerRegistry` keeps that watch alive for the default
      discovery path and publishes discovery-change notifications;
    - `HostSessionBridge` responds to discovery-change notifications with full
      inventory refreshes, so watch events are hints rather than trusted
      incremental inventory state; and
    - focused watch/full-rescan coverage plus the full downstream host test
      suite passed.

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
  - implemented checkpoint in downstream commit `5a963b4`
    (`fix(host): preserve discovered Codex slot state`):
    - downstream `HostSessionBridge` maps live non-connected launch states to
      `needsApproval` or `unknown` slot states instead of `unavailable`;
    - terminal/lost launch state remains `unavailable`; and
    - focused downstream operational-state coverage plus the full downstream
      host test suite passed.

External slice C: Auto-assign newly discovered launches when the product wants a
slot-backed display.

- Scope:
  - preserve explicit user slot assignments
  - fill free slots deterministically for discovered live launches not already
    assigned
  - avoid requiring Herdr `agent_session` or terminal metadata for discovery;
    use Herdr only for optional labels, grouping, or focus/status enrichment
- Validation:
  - with five live Codex launches discovered through validated
    local-controller metadata, all five are assigned or offered for assignment
    after discovery without requiring Herdr `agent_session` metadata
  - a Herdr pane missing `agent_session` does not hide a validated Codex
    local-controller launch
  - restart of the controller host preserves explicit assignments and assigns
    only newly discovered unassigned launches to free slots
  - implemented checkpoint in downstream commit `5a963b4`
    (`fix(host): preserve discovered Codex slot state`):
    - `TwelveSlotMap.assignDiscovered` preserves already assigned routes,
      skips duplicates, and fills free slots in launch/thread order; and
    - `BLECentral` now uses that merge operation instead of replacing the whole
      slot map from the latest inventory projection.
  - live five-launch evidence now exists from temporary Herdr workspace `w1D`:
    five fresh plain `codex --no-alt-screen` launches published
    `$CODEX_HOME/local-controllers` metadata with non-null `mainThreadId`, the
    owning TUI approval prompts were accepted, and the downstream
    `first-vertical-slice-external-controller-smoke --all-discovered
    --expected-launches 5` passed with five launch-scoped routes.

External slice D: Resume an assigned Codex session through the active controller
lease.

- Scope:
  - when the selected slot maps to a live Codex launch/thread route, acquire
    control before issuing the normal app-server `thread/resume` request
  - send `thread/resume` with the exact selected `threadId`; do not synthesize a
    different target or add protocol-specific resume authority
  - release control after the resume request settles so read access and future
    participation remain connection-bound but input ownership is ceded
  - release control even when `thread/resume` returns an error
- Validation:
  - route a selected slot through `controller/acquireControl`, `thread/resume`,
    and `controller/releaseControl` in that order
  - prove the resumed thread ID is the selected launch/thread route's thread ID
  - prove a failed resume still releases control
  - implemented checkpoint in downstream commit `7fb328f`
    (`fix(host): resume Codex sessions through controller lease`):
    - `ExternalControllerConnection.resume` now ensures participation, acquires
      control, sends exact-thread `thread/resume`, and releases control;
    - the downstream smoke source now calls `HostSessionBridge.handleTap` so a
      smoke run can exercise resume instead of stopping after inventory; and
    - focused downstream resume coverage, the full downstream host test suite,
      downstream build, and downstream pre-commit passed.

External slice E: Defer physical slot input until V7 is an input-capable product
surface.

- Current boundary:
  - the current downstream V7 controller is status-only; it displays
    launch-scoped state but is not an acceptance surface for controller input
    or `thread/resume`
  - therefore, physical-device tap evidence is not required to complete the
    external-controller runtime design
  - downstream source has a preparatory `.slotTap` →
    `HostSessionBridge.handleTap` path from checkpoint `d43fcfb`, but that path
    is not a claim that the deployed V7 product accepts controller actions
- Future scope, only when V7 input is explicitly enabled:
  - route an assigned physical slot tap through the same persisted
    launch/thread assignment as the status display
  - write the resulting status snapshot after bridge resolution and reject
    invalid taps without mutating the bridge
  - obtain separate physical-device evidence for acquire, exact-thread resume,
    and release before advertising V7 as an input controller

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
  - implemented checkpoint in the current controller participation
    auto-subscribe and e2e approval slice:
    - approved `controller/requestParticipation` with
      `subscribeMainThread` now immediately subscribes the controller
      connection to the granted main-thread listener, so a controller that
      connects after the thread already exists receives the same thread
      notifications as the TUI-facing app-server interface;
    - the local-controller socket e2e launches an embedded runtime, discovers
      the endpoint, approves a Codex Micro-style controller, verifies normal
      app-server parity for the granted main thread, resolves a command
      approval through the controller, observes listener-ordered
      `serverRequest/resolved`, command completion, and turn completion,
      performs thread-affecting TUI reclaim, verifies stale controller mutation
      rejection plus standing read access after reclaim, reacquires control, and
      verifies final thread state and sign-off; and
    - `just test -p codex-app-server local_controller controller` passed
      114/114 controller and local-controller tests for this slice.
    - The full `just test -p codex-app-server` run still had unrelated
      fixture failures outside the touched controller path: two
      remote-thread-store deadline tests and three zsh-fork deadline/mock-request
      tests reproduced on exact rerun.
    - `just fix -p codex-app-server` passed after unrelated fixer hunks were
      reviewed and reverted, `git diff --check` passed, and
      `cargo build -p codex-cli --bin codex` rebuilt
      `codex-rs/target/debug/codex` as `codex-cli 0.147.1`.
    - The downstream
      `first-vertical-slice-external-controller-smoke --application-support <isolated-empty-temp-dir>`
      passed after native TUI approval against two live Codex launches, proving
      downstream discovery, participation, aggregate inventory, and isolated
      assignment persistence. That earlier run reported `no Codex mutation
      requested`, so it remains non-mutating evidence.
    - A repeat run used a temporary Herdr workspace with two Codex TUI panes and
      one smoke pane, then drove the native `Allow codex-waveshare to control
      this session?` prompts by sending Enter to each owning TUI pane. The first
      attempt crossed the smoke deadline while approval was still pending; the
      rerun passed in 4s with two launches and exact launch-scoped route
      persistence.
    - Downstream commit `7fb328f` updates the smoke source to call
      `HostSessionBridge.handleTap`, so live smoke runs can exercise
      `controller/acquireControl`, exact-thread `thread/resume`, and
      `controller/releaseControl`.
    - Downstream commit `d43fcfb` adds metadata directory watching with full
      inventory refresh on change and a preparatory V7 slot-tap bridge path.
      Focused downstream discovery/bridge tests, full downstream tests,
      downstream build, and downstream pre-commit passed. The currently
      deployed V7 controller remains status-only, so physical-device tap
      evidence is intentionally deferred rather than a pending acceptance gate.
    - Current Codex-side follow-up fixes cover two launch/runtime gaps found
      by live controller validation:
      - controller-enabled startup now prunes stale local-controller metadata
        and socket artifacts for definitely dead `processId` values while
        preserving live or ambiguous records; and
      - `thread/resume` now returns the live loaded main-thread snapshot when a
        fresh paginated TUI thread has not yet materialized rollout storage.
    - Focused Codex validation passed with
      `just test -p codex-app-server-transport local_controller_acceptor_prunes_dead_launch_artifacts local_controller_stale_pruning_tolerates_concurrent_cleanup local_controller_acceptor_publishes_metadata_and_forwards_websocket_messages_with_nonce local_controller_acceptor_republishes_metadata_with_main_thread_id`
      passing 4/4 tests in nextest run
      `ddc4b599-caf2-465f-9b41-efceec19c5aa`,
      `just test -p codex-app-server thread_resume_loaded_unmaterialized_paginated_thread_returns_live_snapshot thread_resume_rejects_unmaterialized_unloaded_thread local_controller_socket_uses_main_thread_interface_and_tui_reclaim controller_thread_resume_allows_read_shape_params_only thread_resume_extracts_exact_controller_thread_target`,
      passing 5/5 tests in nextest run
      `6bbc984c-4e74-4b70-8e20-d89db8f21705`,
      `just fix -p codex-app-server-transport`, and
      `cargo build -p codex-cli --bin codex` rebuilding
      `codex-rs/target/debug/codex` in 17.03s with the known `__eh_frame`
      linker warning and `proc-macro-error2` future-incompatibility warning.
    - Live Herdr validation with two debug Codex TUI panes confirmed each
      metadata record published `mainThreadId`, each native
      `Allow codex-waveshare to control this session?` prompt appeared in the
      owning TUI, and a direct local-controller diagnostic completed
      `initialize`, `controller/requestParticipation`, `thread/list`,
      `controller/acquireControl`, exact-thread `thread/resume`,
      `controller/releaseControl`, and `controller/signOff` successfully
      against both launches.
    - The updated downstream mutating smoke originally returned
      `resume through controller unavailable` after both prompts were approved
      because the smoke blocked the main thread on a semaphore while
      `HostSessionBridge` intentionally posts tap completion back to the main
      run loop. A temporary diagnostic using the same bridge path and a
      run-loop wait returned `BridgeTapResult.selected(ordinal: 0)`, proving
      the Codex controller path was healthy.
    - Downstream commit `df843d4`
      (`fix(host): pump run loop during controller smoke tap`) updates the
      smoke harness to run the main loop while waiting for tap completion.
      After rebuilding, the actual
      `first-vertical-slice-external-controller-smoke --application-support /private/tmp/codex-extctl-smoke-fixed.XaEmW5`
      passed against two live debug Codex TUI launches after native approval:
      `external-controller smoke: pass (launches: 2, exact launch-scoped route
      persisted, resume requested and control released)`.
    - Codex checkpoint `4972b62`
      (`fix(app-server): reject stale controller approvals after disconnect`)
      fixes the native approval/disconnect race found during five-launch live
      validation. A late approval for a closed external-controller connection
      now fails with typed `transport-closing` instead of granting stale
      ownership to the disconnected connection.
    - Focused Codex validation for checkpoint `4972b62` passed with
      `just test -p codex-app-server request_processors::controller_processor`
      passing 3/3 tests, `just test -p codex-app-server
      local_controller_socket_sessions_are_isolated_per_launch` passing 1/1,
      `git diff --check`, and `cargo build -p codex-cli --bin codex`
      rebuilding `codex-rs/target/debug/codex` in 24.34s. `pre-commit
      run --files ...` could not run because this checkout has no
      `.pre-commit-config.yaml`. The attempted full
      `just test -p codex-app-server` run completed with 1257 passed, 2 flaky
      passed on retry, 4 failed, and 1 skipped; the remaining failures were
      outside the controller path in remote-thread-store and zsh-fork/dotslash
      tests.
    - Downstream commit `7424849`
      (`test(host): support all-discovered controller smoke`) adds the
      reproducible all-discovered smoke mode used for the five-launch run and
      accepts equivalent `/tmp` and `/private/tmp` socket paths by resolving
      symlinks before endpoint validation. Focused
      `LocalControllerDiscoveryTests` passed 3/3, the full downstream Swift
      suite passed 63/63, downstream `swift build` passed in 0.16s, and
      downstream `pre-commit run --files ...` passed.
    - Using downstream commit `7424849`, temporary Herdr workspace `w1D`
      launched five fresh plain `codex --no-alt-screen` TUI panes and a smoke
      pane. Each owning TUI approved the native `codex-waveshare` prompt, and
      `first-vertical-slice-external-controller-smoke --application-support
      /tmp/cdx5.kjpUTg/app-support --all-discovered --expected-launches 5`
      passed: `external-controller smoke: pass (launches: 5, exact
      launch-scoped route persisted, resume requested and control released)`.
      Herdr reported the passing run at 18s.
    - Downstream commit `bf445d8`
      (`test(host): verify removed controller launch reconciliation`) adds
      `--verify-removal` smoke mode. It keeps the selected launch set in sync
      with live metadata, waits for the selected launch to disappear, verifies
      that its persisted route is unavailable and rejects without resume, and
      verifies that a survivor route remains resumable. Validation passed with
      downstream `swift build` in 1.46s (with existing unrelated SwiftUI
      warnings), focused `LocalControllerDiscoveryTests` 3/3, full downstream
      `swift test` 63/63, and downstream `pre-commit run --files ...`.
    - Temporary Herdr workspace `w1E` validated that removal mode against two
      fresh plain debug Codex TUIs. After both native participation prompts were
      approved, closing selected pane `w1E:p2` (launch
      `019fea0b-c9eb-7de1-9a99-f6caf0eb63f9`) produced:
      `external-controller smoke: pass (launches: 2, exact launch-scoped route
      persisted, resume requested and control released, removed launch
      reconciled and survivor resumed)`. Herdr reported the pass at 32s.

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
