# External controllers for the interactive TUI

## Status

Internal design and implementation-tracking specification for experimental local external-controller support. The public controller surface remains experimental and gated by app-server v2 experimental API opt-in.

## Purpose

An embedded normal interactive `codex` launch can make its active app-server runtime available to local external controllers. Controllers can inspect the running UI session without making the TUI itself pay JSON serialization or socket transport costs.

The design has two goals:

- retain the efficient typed, in-process path between the TUI and app-server; and
- expose the same runtime through the existing JSON-RPC app-server protocol over a per-launch, same-user local endpoint.

"External" means another process running under the same operating-system account. This design does not expose the server on the network.

## Non-goals

- Replacing the TUI's in-process communication with a socket connection.
- Starting a second app-server runtime for a controller.
- Making the socket a network or cross-user API.
- Letting a controller receive TUI prompts, approvals, or user-input requests without explicit transfer of input ownership.

## Feature request: transfer an already-delivered pending prompt to the active controller

### Problem observed in live downstream validation

An external controller can successfully acquire a TUI main-thread lease and
resolve an approval that is created while that controller owns the thread. This
has been verified end to end with a raw local-controller client: acquire the
exact temporary launch, start a command requiring approval, receive
`item/commandExecution/requestApproval`, reply `{"decision":"accept"}`, and
release the lease.

The corresponding human workflow still fails when Codex created and delivered
the approval to the TUI before the external controller acquired control. The
controller obtains the lease but receives no pending server request, so its
explicit affirmative action cannot resolve the TUI-visible prompt. A downstream
device controller consequently waits for the request until its bounded timeout,
then reports failure while the original TUI remains at the approval UI.

The present implementation explains the distinction: pending-request rebinding
only selects entries for which `has_external_delivery_or_started_write()` is
false. A request that has already been delivered to the TUI is therefore not
eligible for the ownership handoff described below, even after the controller
becomes the active interactive owner.

### Requested behavior

Implement the explicit-transfer case already intended by this specification:
when an authorized controller successfully acquires `interactive-control` for
the main thread, every still-`pending` eligible thread-scoped request must be
atomically rebound from the TUI to that controller, even when the TUI
previously received it. This is not concurrent authorized delivery: the
earlier TUI delivery becomes stale, and each committed transfer epoch produces
at most one additional controller redelivery.

The eligible protocol variants are `CommandExecutionRequestApproval`,
`FileChangeRequestApproval`, `PermissionsRequestApproval`, and
`ToolRequestUserInput`. `McpServerElicitationRequest`, `DynamicToolCall`,
`ChatgptAuthTokensRefresh`, `AttestationGenerate`, `CurrentTimeRead`,
`ApplyPatchApproval`, and `ExecCommandApproval` are out of scope until their
interaction and ownership semantics are specified separately. Process-wide and
otherwise non-thread-scoped requests remain TUI-only.

The handoff must use the existing per-thread transition barrier and be
linearized as follows:

1. At one per-thread snapshot/linearization point that covers both the
   interactive owner and pending-request bindings, select every eligible,
   still-`pending` request for the granted main thread. Requests resolved,
   cancelled, or superseded before that point are skipped; the remaining batch
   transfers together.
2. Advance/record the interactive-owner epoch, commit controller ownership,
   invalidate the TUI recipient bindings and all TUI reply permits for every
   selected request, and bind each original server-request ID to the acquiring
   controller connection and owner epoch. No response authorization or
   ownership reclaim may observe the new owner with an old binding still valid.
3. Before releasing the transition barrier, publish the existing sequenced
   in-process ownership update. While the owner is external, the TUI disables
   local action affordances for every still-pending eligible prompt in that
   thread and visibly marks it as remotely controlled (for example, "Awaiting
   external controller") without pretending it was resolved. Recipient-binding
   authorization remains authoritative if this display update is delayed, lost,
   or processed out of order.
4. After the transfer commit, enqueue at most one redelivery per selected
   request to the controller. Each queued controller write carries the prompt
   binding and owner epoch and checks its revocable write permit immediately
   before writing. Reclaim/revocation atomically revokes that permit and
   tombstones stale queued envelopes.
5. Look up every reply by the original request ID and receiving connection, then
   check the server-stored prompt binding and owner epoch; unchanged response
   payloads do not carry these fields. Unknown, consumed, or nonmatching
   bindings are rejected. A former TUI recipient receives the equivalent typed
   in-process `stale-ownership` rejection; an external controller receives the
   normal JSON-RPC typed controller error whose `ControllerErrorData.code` is
   `stale-ownership` if its reply loses a transfer or reclaim race. A response
   to a transferred prompt must not invoke
   the normal TUI-input reclaim path: only a separate thread-affecting TUI
   action may reclaim ownership, and that reclaim must first invalidate the
   controller binding under the same transfer critical section.

If the controller disconnects, signs off, is revoked, expires, or encounters
controller-redelivery failure, the runtime must rebind the still-pending prompt
to the viable TUI recipient under the same critical section. A redelivery is
successfully enqueued only after it carries that fence. `write_started` means
the frame may be externally disclosed, while `write_completed` records
successful frame completion; neither is a recovery cutoff. Any enqueue, write,
or connection-closed failure triggers serialized TUI recovery.
After a write starts, recovery and an in-flight controller reply race by
atomically consuming the same binding and epoch: if the reply wins it resolves
the prompt; otherwise the reply gets the applicable typed `stale-ownership`
rejection and recovery proceeds. A successful recovery may redeliver the
still-pending original request to the TUI. If no viable TUI recipient exists or
that redelivery fails, the coordinator cancels/fails the prompt and enters or
retains terminal `TuiUnavailable`; it never leaves a pending prompt owned by a
dead controller. This is a sequential
ownership transfer, not simultaneous delivery to two authorized resolvers. A
later reacquisition after recovery is a new epoch and can produce one new
controller redelivery. It must never silently resolve, discard, or leave the
prompt owned by a dead controller.
Persistent approval decisions remain forbidden for a controller-owned prompt;
only the existing non-persistent decisions are valid.

### Why this is required

This preserves the ownership safety model while making a physical controller a
usable explicit input method. The device cannot know whether a TUI created the
prompt before a human tapped it. Once the human explicitly transfers input to
the controller, they need a safe way to make the same affirmative decision
there. Requiring the controller to have originated the command is not a
workable interaction contract for status/control hardware.

### Acceptance criteria

- A TUI-originated command approval is shown in the TUI; an approved external
  controller acquires control without any new TUI mutation; at most one
  additional copy of the original approval is delivered to the controller per
  transfer epoch, the TUI marks it
  remotely controlled, and the controller's `accept` resolves the original
  command.
- The equivalent file-change and user-input requests transfer with their
  original request IDs and normal response shapes; permissions approvals are
  covered as well. MCP elicitation and dynamic-tool calls remain TUI-only.
- Competing TUI and controller replies produce exactly one successful resolution
  and one transport-appropriate stale-ownership rejection; no double execution
  is possible. A controller reply racing a non-response TUI reclaim either
  resolves before reclaim or is stale after it; in the latter case the prompt is
  TUI-actionable rather than spuriously resolved.
- A disconnect, explicit sign-off, lease expiry, controller egress failure, or
  controller redelivery failure restores the pending prompt to the TUI and
  leaves it locally actionable, with recovery serialized against an in-flight
  controller reply. If the TUI is unavailable or cannot accept recovery, the
  prompt instead fails terminally and the launch enters or retains
  `TuiUnavailable`.
- An already resolved, cancelled, or no-longer-thread-scoped request is never
  replayed; acquiring control does not create duplicate prompt notifications.
- Integration coverage exercises this through the public local-controller
  WebSocket protocol with a live in-process TUI, not only by calling internal
  rebinding helpers. Use deterministic test-only barriers for the transfer/reply
  and recovery/reply races so those assertions do not depend on timing.
- Reusing or changing the existing app-server remote-control service. Local external controllers are a separate transport origin and never enroll in, publish to, or inherit lifecycle from remote control.

## Scope

The first delivery applies when the TUI selects an embedded app-server runtime. It does not create a second endpoint when the TUI attaches to an existing local daemon or an explicit remote app server; those modes require a separate controller-session registration design because the TUI does not own that runtime or its listener lifecycle.

Plain `codex` selects the embedded runtime by default when controllers are `best-effort` or `required`, so the per-launch local-controller endpoint is the normal interactive launch behavior. Reusing an implicit local daemon is allowed only when controllers are disabled by policy; explicit `--remote` continues to attach to the requested external app-server endpoint.

The TUI reports one of these controller-availability states for the current launch:

- `embedded-supported` — endpoint published and ready for controllers.
- `daemon-unsupported` — attached to a local daemon; no per-TUI endpoint in this delivery.
- `remote-unsupported` — attached to a remote app server; no local endpoint in this delivery.
- `policy-disabled` — a managed policy disabled controllers.
- `embedded-unavailable` — best-effort endpoint setup failed; the TUI continues without controllers.
- `launch-failed` — a policy required controllers and endpoint setup failed, so no TUI is running.

The TUI must not imply that a daemon-backed or remote launch has a per-TUI endpoint when it does not.

## Architecture

One embedded TUI launch owns one app-server runtime. It starts both adapters below against that runtime.

```text
                         typed requests and events
TUI  ----------------------------------------------------+
                                                        |
                                                        v
                                             shared app-server runtime
                                             (MessageProcessor and state)
                                                        ^
                                                        |
                 JSON-RPC over WebSocket/local endpoint  |
external controller  -----------------------------------+
```

The TUI is represented internally as a synthetic, in-process connection. It uses typed `ClientRequest` and `InProcessServerEvent` values and never serializes them to JSON.

External controllers connect through a `LocalControllerEndpoint`. On Unix this is a Unix-domain socket; on Windows it is the `codex-uds` platform-native same-user endpoint. Both use the app-server's existing HTTP Upgrade and WebSocket framing, with one JSON-RPC message in each WebSocket text frame. WebSocket/JSON-RPC is the portable protocol layer; the local endpoint is a platform binding, not a claim that Windows uses a Unix-domain socket.

Both adapters feed a common connection model:

- a stable `ConnectionId`;
- initialization state and client capabilities;
- bounded ingress and egress queues; and
- per-connection routing for responses and server requests.

The existing `MessageProcessor`, request-serialization queues, and outgoing-envelope router are the shared runtime boundary. The current parallel in-process and socket routers should be consolidated behind that boundary rather than duplicated.

The shared runtime is the sole source of thread state. Controller origin changes admission and interactive ownership only; it does not create a controller-only session view. Every exposed thread has a canonical monotonically increasing `threadSequence`, delivered unconditionally to the TUI synthetic connection regardless of controller subscriptions. It includes admitted user messages/turn starts, streaming/progress, approvals, interrupts, completion, errors, and ownership changes. The TUI applies only this stream, keyed by stable turn/item IDs, through its normal reducer; it resynchronizes a single thread on a sequence gap rather than rendering out of order. The original JSON-RPC response is routed only to the controller that made the request, while the TUI receives the resulting state/event stream; it may show a control-owner indicator but must not hide or duplicate the action.

Every request passes through one central connection-authorization gate before dispatch. The gate enforces the connection role and thread-scoped lease before any existing or new app-server RPC handler runs; controllers cannot bypass it by calling an existing mutation RPC directly.

Every controller connection is default-denied from its first byte. Before binding or accepting, the runtime creates and installs the default-deny authorization gate, disconnect-revocation path, and peer-credential verifier. Startup order is: install those handlers; bind a non-accepting endpoint; register the acceptor; enable accepts; atomically publish metadata; then report `embedded-supported`. If the acceptor fails later, the runtime stops accepting, closes established controller sessions through the same revocation path, removes discovery metadata, and reports `embedded-unavailable`.

## Local-endpoint discovery and lifecycle

The listener is created for each controller-enabled embedded interactive TUI launch. Its lifetime is owned by the TUI process and runtime: acceptor failure updates the availability state. An abnormal process exit can leave filesystem socket and metadata artifacts behind, so every later controller-enabled launch performs conservative startup pruning for records whose `processId` is definitely no longer running. Pruning removes only metadata proved to match the filename launch ID plus launch nonce, removes only socket filesystem nodes, tolerates `NotFound` races, and preserves entries when process liveness is ambiguous, including possible PID reuse.

The Unix endpoint path uses a short launch identifier created before app-server startup:

```text
$CODEX_HOME/local-controllers/codex-<launch-id>.sock  # Unix only
$CODEX_HOME/local-controllers/launch-<launch-id>.json
```

`launch-id` is a random, collision-resistant value. It is not a thread ID.

A thread ID cannot safely be used in the endpoint name: the primary thread is created only after the TUI connects to the runtime. Renaming a live endpoint would also make discovery racy. The metadata file records a platform-neutral endpoint URI, process ID, creation time, protocol version, and primary thread ID once one exists. Unix uses the path above; Windows uses the URI and access-control representation defined by the `codex-uds` adapter, with no `.sock` filesystem claim.

The listener and metadata file are private to the account that launched Codex. The controller directory is created and validated as a user-owned, non-symlink private directory. A supported platform adapter must expose native peer credentials and verify that every peer belongs to the launching account; missing, mismatched, or unavailable credentials reject the connection. A platform that cannot meet this contract reports controllers unavailable rather than exposing an endpoint. On Unix, the socket is owner-only (`0600`) and its parent directory is private. On Windows, `codex-uds` supplies the supported local endpoint and its platform-native access control; `0600` is not a Windows security claim.

Metadata is a versioned, regular, owner-only file containing the launch ID, a random launch nonce, endpoint URI, process ID, creation time, protocol version, and primary thread ID once one exists. The first created primary thread becomes the immutable `mainThreadId` for this launch; if it closes, no replacement inherits controller authority. The server publishes metadata atomically only after accepts are enabled. On Unix, a controller validates that the metadata path remains below the controller directory and that the launch ID matches its filename. Before WebSocket acceptance, the controller sends the nonce in the dedicated HTTP Upgrade header `X-Codex-Launch-Nonce`; the endpoint compares it to metadata and rejects a mismatch. The nonce is never echoed in JSON-RPC messages, events, or ordinary RPC payloads. The Windows adapter provides equivalent metadata validation, nonce checking, same-user peer verification, and cleanup semantics.

On normal TUI shutdown, the runtime closes controller connections, removes the socket and metadata file, and stops the listener. Cleanup only removes resources proved to belong to this launch by their launch ID and nonce; it never relies on a PID alone, follows a symlink, or deletes an arbitrary file. A failed or partially published launch removes its own resources before reporting failure.

Controllers must still treat launch discovery as an eventually consistent filesystem inventory. A metadata file may disappear between rescan and connect because the owning TUI exited or another Codex launch pruned a stale record. That is not a protocol error; the controller should refresh inventory and retry against the current candidate set.

### Controller-side launch discovery and presentation

#### Follow-up feature request: session working directory

Controller products need the session working directory before they attach to a
launch and can read thread metadata. Add optional `sessionWorkingDirectory` to
the existing metadata JSON payload. It is launch metadata only and must not
affect endpoint discovery, launch identity, authorization, or control
ownership. This is an optional metadata-v1 extension: readers must ignore
unrecognized optional members.

This feature changes only the external-controller discovery metadata JSON
payload (`launch-*.json`). It must not add or alter app-server v2 methods,
WebSocket/JSON-RPC payloads, TUI events, or in-process interaction contracts.

Capture the full launch CWD when the local controller endpoint starts. Publish
it when it is valid UTF-8; otherwise omit the field. Preserve the captured
value when metadata is updated with `mainThreadId`.

The implementation must test initial metadata publication and the later
`mainThreadId` update.

The local-controller metadata directory is the launch-discovery contract for controller products. Generic Codex lifecycle hooks such as `SessionStart` are not controller-inventory hooks: they run inside an agent/thread workflow, are subject to hook configuration and trust policy, and do not publish the per-launch socket, nonce, process ID, or immutable main-thread binding as their contract. The app-server `fs/watch` API is also not a bootstrap mechanism because a controller must already have chosen and connected to a launch before it can call any app-server RPC.

A controller that wants to maintain a live inventory should watch `$CODEX_HOME/local-controllers` with the host operating system's file-watch facility and perform a full directory rescan after every create, modify, delete, or overflow event. Polling is acceptable as a fallback; the authoritative operation is always a rescan of the directory, not trusting an individual watch event. The controller treats `launch-*.json` files as candidates only after validating the owner-private directory/file/socket invariants, metadata version, filename launch ID, endpoint URI, and nonce-bearing socket path. It must ignore or remove from presentation any candidate whose `processId` is no longer live, whose socket is missing or invalid, or whose metadata fails validation. A candidate with `mainThreadId: null` is a starting launch, not an offline launch; it should remain pending until metadata is updated or the process exits.

Launch health, controller authorization, and product slot assignment are separate axes. A live metadata record with a live process, valid endpoint, and non-null `mainThreadId` means the Codex launch is online/discovered even if the controller has not yet connected, has not been approved, is awaiting native TUI approval, has released control, or currently has read access without an active input lease. A product UI must not display such a launch as offline solely because `controller/requestParticipation` has not completed or because the controller is not the active owner. Suggested presentation states are:

- `unassigned` — no launch is assigned to this product slot.
- `starting` — metadata exists and the process/socket are live, but `mainThreadId` is not yet published.
- `discovered` — metadata, process, socket, and `mainThreadId` are live; no controller session is connected for that launch.
- `awaitingApproval` — the controller has sent `controller/requestParticipation` and the owning TUI has not answered.
- `connected` — participation is approved and the controller has read/subscription access, with `activeLease: null`.
- `activeOwner` — participation is approved and the controller currently holds the `interactive-control` lease.
- `offline` — metadata is gone, the process is dead, the socket is invalid/missing, or the launch returned a terminal no-main-thread state such as `tui-unavailable` or `main-thread-closed`.

Slot assignment is a controller-product layer, not Codex authorization. If a controller product auto-populates visible controls, it should preserve explicit user assignments, remove dead assignments only under its own product policy, and assign newly discovered unassigned launches to free slots deterministically. Herdr, terminal metadata, or other host inventory may enrich labels and grouping, but they must not be required for discovering Codex launches or for deciding that a validated local-controller launch is online.

## Controller roles and ownership

The first delivery exposes only the embedded TUI's main thread. The TUI and an external controller swap input ownership of that same thread; external controllers cannot create, attach to, or control any other thread. A later design may add multi-thread tenancy separately.

### Participation

Opening the local socket proves only same-account transport access; it does not grant controller authority or thread access. The restricted pre-participation protocol allows only `initialize` (with `capabilities.experimentalApi: true`), `controller/requestParticipation`, transport close, and protocol keepalives. Every other request receives an experimental-not-enabled or participation-required error, and no controller receives runtime events before approval.

`controller/requestParticipation` is rate-limited by global unauthenticated-connection caps. For the embedded TUI local-controller endpoint, it surfaces a native participation request in the owning TUI with the controller's requested name (for example, `codex-waveshare`) and human-readable description. Both are untrusted display claims, not an identity or authorization principal. The act of the TUI user approving that request creates the out-of-band, connection-bound `ControllerEnrollmentGrant` for this launch, this live `ConnectionId`, and the TUI main thread. This first delivery does not require an external client credential, durable enrollment record, controller registry, or additional host-defined authority. Same-account socket access and the discovery nonce prove only that a controller reached the intended local endpoint; they do not satisfy the grant without native user approval.

The request returns `approved` only after the owning TUI approves the native participation request and the runtime creates the live connection-bound grant. If the main thread is `TuiOwned` and TUI-viable, approval also issues the initial lease and transfers ownership. If another controller owns it, approval creates a read-capable standing session with `activeLease: null`; it never displaces that controller, and the caller may later use `controller/acquireControl`. If the main thread does not yet exist, the request returns a retryable typed `main-thread-unavailable` error with `retryAfterMs` and `launchState: starting`; the controller may repeat `controller/requestParticipation` on the same initialized connection or reconnect after the hint. If main-thread creation fails, the immutable main thread closed, or the launch entered terminal `TuiUnavailable`, the request returns a non-retryable typed error such as `main-thread-closed` or `tui-unavailable`. Disconnect, sign-off, authorization expiry, explicit TUI revocation, or main-thread close deactivates the grant. A rejected connection has no thread access and can only close, use protocol keepalives, or repeat `controller/requestParticipation`.

The native grant means “authorize this controller to be the input method” for the TUI main thread; it is not read-only observation consent or a one-shot handoff. It records an approved, connection-bound controller session with standing control-switch authorization; the session can be read-capable with no active lease or can own the main thread through an `interactive-control` lease. A server-monotonic lease deadline ends ownership and returns prompts to the TUI while preserving the session; a server-monotonic authorization deadline destroys the session and its read access. The wire response exposes only advisory `leaseExpiresInMs` and `authorizationExpiresInMs`, sampled with the response; client clocks never enforce them.

The controller session is bound to its live `ConnectionId`. A lease ID is an opaque audit and revocation identifier returned in controller-session status, not a reusable bearer credential: no existing app-server RPC gains a token field, and copying a lease ID to another connection grants nothing. Disconnecting or reconnecting loses the native grant and requires a new participation request and TUI approval.

Leases are server-side and globally exclusive in this delivery: at most one external controller connection may hold an active `interactive-control` lease for the TUI main thread at a time. A second controller may stay enrolled and connected, but `controller/acquireControl` returns an ownership-conflict error until the active lease is released, expires, disconnects, or is superseded by thread-affecting TUI input. The authorization gate derives that lease from `ConnectionId` and main-thread target, then validates its server-owned monotonic expiry deadline and owner epoch. Client clocks and timestamps never extend authority.

Each exposed thread has one `InteractiveOwner` state:

- `TuiOwned(epoch)` — the default; the TUI owns interactive requests and any thread-affecting TUI input is primary.
- `TransferPending(epoch)` — a serialized handoff/revocation transition; no new interactive request is delivered until it resolves.
- `ControllerOwned(connection_id, lease_id, epoch)` — only while an `interactive-control` lease is live for that exact connection.
- `TuiUnavailable(epoch)` — the synthetic TUI connection cannot consume the canonical stream; the launch terminates after leases are revoked and pending prompts fail.
- `Closed` — the thread is no longer controllable.

The per-thread coordinator is the sole transition authority. It serializes `TuiOwned → TransferPending → ControllerOwned` when the native grant activates or the authorized controller acquires control, and `ControllerOwned → TransferPending → TuiOwned` on expiry, revocation, controller disconnect, explicit controller release, or any thread-affecting TUI input. `TransferPending` is a no-recipient barrier: interactive requests are queued and controller mutations receive retry/stale-ownership according to their epoch. Exactly one interactive owner exists only in stable `TuiOwned` and `ControllerOwned` states; `TuiUnavailable` is terminal for the launch.

The interactive owner receives and resolves server requests that require human interaction, including command approvals, file-change approvals, permission approvals, and user-input requests. With no active interactive-control lease, that owner is always the TUI.

Server requests that are not thread-scoped, including account/authentication, attestation, and process-wide requests, are TUI-only in this delivery. They are neither exposed to nor resolved by controllers.

An approved controller sees the same app-server v2 interface as the TUI's synthetic connection for the TUI main thread, scoped by its session and current ownership: the same existing methods, request and response shapes, subscriptions, notifications, raw thread data, cursors, error conventions, and thread-scoped mutation surface. With `activeLease: null`, it is a read/subscription client for that normal interface and owner-required mutations fail at the authorization gate. While it holds an active `interactive-control` lease, it is the current app-server input client for the main thread. There is no controller-specific read DTO, status projection, parallel observer protocol, or controller-specific mutation protocol. `ControllerSession.effectiveCapabilities` reports the current coarse-grained rights for client UX only: `readMainThread`, `subscribeMainThread`, `acquireControl`, `releaseControl`, `mutateMainThread`, and `answerPrompts`. The authorization gate remains authoritative, and a client must still handle typed authorization errors because capabilities can change after the session response is sampled.

The converse is also required: controller-originated work is visible to the TUI through the same app-server event types and state transitions as TUI-originated work. The TUI must remain an up-to-date display/control surface while it is not the input owner.

The authorization gate applies that normal interface to the TUI main thread. A controller may use normal reads and subscriptions for that thread and receives the same data the TUI would receive. `thread/list` is filtered to the main thread, and a different target has the normal non-enumerating not-found behavior. Cursors, subscriptions, resume tokens, and implicit targets are bound server-side to the main thread and `ConnectionId`; any continuation resolving elsewhere is denied. It may perform every existing thread-scoped operation the TUI connection may perform for the main thread while it owns it, and it receives only the eligible approval and user-input variants defined above. Process-wide, non-thread-scoped, MCP elicitation, dynamic-tool, and the other excluded server requests remain TUI-only. Thread-affecting TUI operations are always primary: when one targets the controller-owned main thread, admission first invalidates that controller's active input lease and transfers the thread to `TuiOwned`, then admits the TUI input. Reclaiming input means a TUI-originated thread-affecting operation: submit/resume/cancel/interrupt, a slash command that mutates the main thread, or composer text accepted as a pending user turn. An approval or user-input response first checks that prompt's current recipient binding: a response to a still-pending controller-transferred prompt fails `stale-ownership` and does not reclaim; a separate thread-affecting TUI operation may reclaim before resolving a subsequently TUI-bound prompt. Pure display actions such as scrolling, focus changes, pane navigation, and draft text editing without submission do not reclaim control. The controller remains connected with standing authorization and can later call `controller/acquireControl`; it cannot send thread mutations or resolve prompts until then.

### Control ownership

Approval of participation grants a connection-bound control-switch authorization and, only when the main thread is TUI-owned and viable, an initial handoff. A lease is bound to one live `ConnectionId`, the TUI main thread ID, an unguessable lease ID, a server-owned monotonic expiry deadline, and owner epoch. It is non-transferable and non-resumable: reconnecting has no session authority and must request participation again.

- An `interactive-control` lease transfers full app-server input ownership of the granted main thread. Participation approval grants this lease only when the coordinator can perform the initial handoff, and it is the only lease that permits a controller to answer approvals or user-input requests.
- The controller may relinquish its active `interactive-control` lease with `controller/releaseControl`. If the connection still has a standing controller session but no active lease because ownership was already released, expired, or reclaimed by thread-affecting TUI input, `controller/releaseControl` is idempotent and returns the current `ControllerSession` with `activeLease: null`. If the active lease is still held by the caller, the thread coordinator invalidates it, rejects queued mutations, transfers the main thread back to `TuiOwned`, and rebinds prompts before reporting success. The controller session, read/subscription access, and standing control-switch authorization remain live.
- A connected, authorized controller may later call `controller/acquireControl` without TUI interaction. The thread coordinator validates that the main thread is live, `TuiOwned`, and TUI-viable, then issues a fresh lease and transfers ownership. Otherwise it returns an ownership-conflict or stale-authorization error; it never displaces another controller. TUI revocation, controller sign-off/disconnect, or session expiry destroys this standing authorization, so reacquisition then requires a new grant.
- Thread-affecting TUI input is an automatic per-thread reclaim: it cancels the active controller input lease but preserves the connection's standing authorization and emits `controller/controlOwnershipChanged`. The owning TUI also exposes a native, thread-scoped `revokeControllerAccess(mainThreadId)` action. It is not a public app-server RPC: it atomically revokes every external-controller session for that main thread (the active owner and any read-capable standing sessions), fences their egress/subscriptions, and rebinds each still-pending eligible prompt to the TUI. It is idempotent when no controller session remains. A controller can never grant or extend its own authorization.

Every controller mutation is authorized against `(thread ID, connection ID, lease ID, owner epoch)` at admission and at the per-thread coordinator's execution fence. Thread-affecting TUI input, release, or revocation advances the owner epoch and prevents any further controller irreversible step from starting. A step already holding the fence finishes; the triggering TUI input is queued immediately behind it and runs next. Queued controller work receives exactly one stale-ownership result.

The controller uses the existing app-server mutation RPC shapes rather than a parallel control protocol. The generated v2 registry must enumerate every method, server-request response, implicit target, cursor, resume token, and subscription on two independent axes:

- target extraction: `none`, `mainThreadOnly`, `exactThread`, or `collectionFiltered`; and
- required authority: `preParticipation`, `standingSession`, `activeOwner`, or `tuiOnly`.

The first axis identifies which thread, if any, the handler may touch. The second axis identifies which connection state is required before dispatch. Examples: `controller/requestParticipation` is `none` + `preParticipation`; `thread/list` is `collectionFiltered` + `standingSession`; a main-thread read is `exactThread` + `standingSession`; a main-thread mutation or prompt response is `exactThread` + `activeOwner`; account/authentication and process-wide requests are `none` + `tuiOnly`. Cursors, resume tokens, subscriptions, and implicit targets must carry enough server-side binding to re-run both checks on continuation. New methods default to denied until registered on both axes. The priority-aware admission scheduler routes mutations to the current `InteractiveOwner`; it does not allow the non-owner to compete with the owner.

Approval decisions are responses to server-to-client requests, not new client RPCs. Their existing decision shapes remain unchanged for the TUI. A controller-owned prompt is delivered with the same server-request shape as the TUI receives, but this first delivery rejects any decision whose effect would outlive the connection-bound lease, including `acceptForSession`, exec-policy amendments, network-policy amendments, and session-scoped permission grants. Non-persistent decisions such as `accept`, `decline`, or `cancel` remain valid when the controller is the active interactive owner. The runtime authorizes each response by its original outbound request ID, recipient `ConnectionId`, and interactive-owner epoch before accepting it.

Controller input and mutation requests are eligible only while the controller is the current `InteractiveOwner`. Thread-affecting TUI input is always eligible and atomically reclaims an owned thread before it is scheduled, so it wins any acquire-versus-input race at the coordinator's linearization point. Display-only TUI interactions do not enter this scheduler and do not reclaim ownership. Priority is applied at dequeue boundaries only for concurrently admissible non-interactive work, such as reads and subscriptions. Those requests are FIFO within the TUI and controller classes. TUI work wins the next dequeue when both classes are eligible, but after eight consecutive eligible TUI dequeues the coordinator must run one valid controller request. This bound does not override TUI input reclamation, ownership, revocation, expiry, or a serialization lock held by in-flight work.

Every eligible approval or input request is atomically bound at creation to one recipient connection and, when controller-owned, its ownership epoch. Excluded TUI-only requests are bound to the viable TUI recipient even while a controller owns the thread. Inbound responses atomically consume the applicable binding; revocation/disconnect/reclaim and redelivery are serialized through the per-thread coordinator: exactly one wins. A losing reply receives a deterministic transport-appropriate `stale-ownership` rejection; a losing recovery observes that the prompt is already resolved. A new controller binding cannot be delivered until the old binding is invalidated and all queued egress under it is fenced.

Interactive handoff enters `TransferPending` behind an atomic transition barrier. At one per-thread sequence point it invalidates the old prompt binding, sets the new owner/epoch, commits local ownership, admits a pending TUI input if applicable, assigns and publishes the ownership/state bundle, then enqueues prompt redelivery in that order. The TUI reclaim never waits for a UI egress queue; if its local reducer cannot accept the bundle, the runtime enters terminal `TuiUnavailable` and closes the launch. Prompts have coordinator-owned `pending`, `resolved`, or `cancelled` state and are rechecked at delivery dequeue; only `pending` prompts are redelivered. `write_started` means a controller frame may have been disclosed; `write_completed` records `externalDelivery` after the WebSocket writer successfully writes the frame. Enqueue success only reserves delivery capacity. Neither delivery state is a recovery cutoff: when controller ownership is lost or controller egress fails, the coordinator atomically invalidates the controller binding and rebinds a still-pending prompt to the viable TUI before any later resolver can act. An old controller reply then resolves only if it consumed the binding first; otherwise it is stale. If no viable TUI recipient exists or TUI redelivery fails, the coordinator cancels/fails the prompt and enters or retains terminal `TuiUnavailable`. Committed controller progress/results remain in the canonical `threadSequence`; only stale prompt/control deliveries and replies are discarded. Terminal revocation cancels subscriptions/cursors and fences queued controller egress before emitting its change notification. The TUI event stream is lossless to its reserved queue; overflow marks the thread desynchronized and obtains an atomic snapshot-at-sequence containing thread state, `InteractiveOwner`, owner epoch, prompt bindings, and `lastSequence`. The reducer replaces state at that sequence, drops buffered events at or below it, applies later events exactly once, and enters terminal `TuiUnavailable` if recovery fails. Normal TUI shutdown closes the runtime.

An approved controller may call experimental `controller/signOff` to relinquish its controller session. Sign-off and unexpected socket disconnect use the same revocation path: invalidate every connection-bound lease, reject queued controller work, restore prompt ownership to the TUI, and emit a typed in-process TUI ownership-status event containing main thread ID, owner, owner epoch, and reason. For sign-off, the connection teardown barrier rejects new ingress, lets already-admitted requests receive their normal response or one `transport-closing` result, fences controller egress, flushes only the sign-off response, then closes the socket. Any later operation requires a new connection and enrollment.

### Experimental protocol surface

The new public controller methods use the v2 singular-resource naming convention and are all gated by `initialize.capabilities.experimentalApi`:

- `controller/requestParticipation` takes `ControllerRequestParticipationParams { controllerName, description }` and returns `ControllerRequestParticipationResponse { status, session, denial }`, where `status` is `approved` or `rejected`, `session` is a required nullable `ControllerSession`, and `denial` is required nullable typed denial data. `ControllerSession` contains session ID, main thread ID, explicitly nullable `activeLease`, authorization epoch, `effectiveCapabilities`, and advisory lease/session expiry durations. Rejection has `session: null` and typed `denial` data. `main-thread-unavailable` is not a rejection status; it is a typed retryable error until the launch reaches a terminal no-main-thread state.
- `controller/authorizationChanged` and `controller/controlOwnershipChanged` each include session ID, main thread ID, reason enum, authorization/owner epochs, and a monotonically increasing controller-session sequence. They are controller control-plane notifications, never TUI state inputs. The TUI receives the corresponding typed in-process ownership-status event in `threadSequence` order. Terminal revocation fences queued events before its notification.
- `controller/acquireControl` and `controller/releaseControl` take no payload and return the updated `ControllerSession`. Acquire returns only after its ownership transition completes. Release returns after its ownership transition completes, or immediately with `activeLease: null` when the live session already has no active lease. `controller/signOff` returns only after terminal revocation completes; its response is exempted from teardown.
- Canonical experimental errors are: `experimental-not-enabled`, `participation-required`, `enrollment-denied`, `main-thread-unavailable`, `main-thread-closed`, `tui-unavailable`, `ownership-conflict`, `stale-ownership`, `controller-not-allowed`, `transport-closing`, `different-thread-target`, `authorization-expired`, `lease-expired`, and `controller-overloaded`. Each error includes typed data sufficient to decide whether retry on the same connection is allowed.

Native TUI approval, `revokeControllerAccess(mainThreadId)`, and single-thread assignment are controller-authorization mechanisms, not public app-server RPCs. `revokeControllerAccess` enters through the embedded TUI client/runtime boundary and is serialized by the same per-thread transition barrier as TUI reclaim, expiry, disconnect, and sign-off. The public v2 types use `*Params`/`*Response`/`*Notification`, camelCase serde and TypeScript names, `#[ts(export_to = "v2/")]`, and the normal experimental annotations/schema generation workflow.

This adds only a participation/control-session wrapper around the normal app-server interface. Once its native grant is active, the controller has standing authority to acquire or release input ownership for the granted main thread. It is the normal input client while it owns that thread; any thread-affecting TUI input automatically cancels that active input authorization and reclaims the thread, while TUI policy can explicitly revoke access at any time.

## Concurrency and backpressure

The runtime retains the app-server's existing request policy:

- Requests enter bounded queues. Saturated request ingress receives JSON-RPC error `-32001` (`"Server overloaded; retry later."`).
- Responses and server requests are routed to the originating or owning connection using `ConnectionId`.
- Requests with a thread serialization scope are queued exclusively by thread ID across all connections.
- Global mutations are serialized globally. Consecutive shared reads may run concurrently.
- Unscoped requests and requests for different threads may run concurrently.

Serialization orders competing operations; it does not by itself define product ownership. The controller-role and control-lease policy above supplies that ownership boundary.

External ingress is quota-limited per connection before shared runtime admission. Normal controller app-server RPCs and controller control-plane RPCs use separate bounded reservations, so saturated normal controller traffic returns typed overload errors without preventing `controller/requestParticipation`, `controller/acquireControl`, `controller/releaseControl`, or `controller/signOff` from reaching dispatch. External fanout uses isolated bounded egress queues and never awaits controller I/O on the TUI/runtime dispatcher path. Every sensitive egress envelope carries connection, owner epoch, and prompt binding; it is revalidated immediately before socket write, and revocation closes or write-fences the connection before publishing the change. Frames already written are already disclosed. On egress overflow, the server may drop only existing app-server notifications that are explicitly designated lossy; otherwise it disconnects that controller. A failed response delivery is recorded as a terminal transport-closed result for that connection; it is not silently dropped. A controller-owned interactive request is externally delivered only at the pre-write/write boundary defined in the prompt lifecycle above; enqueue failure, stale pre-write validation, or write failure first runs the disconnect/revocation or stale-egress path and then rebinds the request to the TUI. The in-process TUI has independently reserved capacity.

## Compatibility and failure behavior

- Existing `codex app-server --listen unix://...` behavior remains unchanged.
- Existing app-server JSON-RPC schemas remain the external wire format. New controller-control APIs are additive v2 APIs and experimental until stabilized.
- Controller setup is best-effort by default. An unexpected listener or metadata-publication failure changes the running TUI state to `embedded-unavailable`, emits a clear diagnostic, cleans up partial resources, and allows the TUI to start without controllers. A managed policy may require controller availability; only then does setup failure produce `launch-failed` and abort startup. A managed policy may also explicitly disable controllers, producing `policy-disabled` without a listener or discovery metadata.
- A controller disconnect must not interrupt the TUI or its active thread. A TUI shutdown closes all attached controller connections.
- The listener must not interfere with the persistent app-server daemon or its control socket. Per-launch controller sockets live in a separate directory and are never used as daemon discovery endpoints.

## Implementation plan

1. Extract a shared runtime bootstrap that owns connection registration, request dispatch, serialization, and outgoing routing.
2. Adapt the current typed in-process client to register a synthetic connection with that runtime.
3. Define the `LocalControllerEndpoint` platform contract, including Unix/Windows peer verification, nonce handshake, discovery metadata, cleanup, and acceptor failure semantics.
4. Design the experimental v2 participation state machine, single-thread control-handoff contract, exhaustive two-axis admission registry, and TUI command classifier; generate and validate the protocol schema.
5. Implement the default-deny participation gate before endpoint acceptance, normal-interface routing, connection teardown/revocation path, and non-thread TUI-only routing.
6. Add the priority-aware admission scheduler before the existing FIFO serialization queues, with injectable clock and deterministic scheduler tests.
7. Start and publish the per-launch local-endpoint WebSocket acceptor only after steps 3–6 are ready.
8. Add per-connection external quotas, isolated egress routing, controllable egress-failure tests, and TUI-reserved capacity.
9. Implement and surface the controller-availability states for all TUI launch modes.
10. Document and validate the controller-side discovery contract: metadata-directory watch plus full rescan, liveness validation, separate launch-health/authorization/slot-assignment state, and no dependency on generic Codex hooks or Herdr metadata for launch inventory.
11. Add a TUI command classification table for every TUI-originated action that can reach the coordinator. Each command is classified as `threadAffecting` or `displayOnly`; unclassified commands default to `threadAffecting` until explicitly reviewed.
12. Retire the duplicated in-process/out-of-process routing code only after behavioral parity is verified.

## Acceptance criteria

- A pre-participation connection must opt into experimental APIs during `initialize`; it can otherwise only close or use protocol keepalives, and no runtime events are delivered.
- Participation activation is rate-limited and TUI-mediated: the runtime surfaces the native participation request in the owning TUI, creates a live connection-bound grant only after approval, and returns `approved` or `rejected`. An asserted controller name never supplies identity or authority.
- A controller enrolling while another owns the main thread receives a read-capable session with no active lease; it cannot displace the owner and acquires only after the thread returns to `TuiOwned`.
- Lease expiry returns input ownership while preserving the session; authorization expiry removes the session and read access. Participation before main-thread readiness returns retryable `main-thread-unavailable` with `retryAfterMs` and `launchState`; terminal no-main-thread states return non-retryable typed errors.
- An unapproved same-account connection cannot inspect threads or invoke handlers beyond the restricted participation protocol.
- Controller enrollment explicitly targets the immutable TUI main thread and grants standing control-switch authorization; it grants an initial lease only when the coordinator sees a TUI-viable `TuiOwned` thread.
- An approved controller sees the same v2 methods, request/response types, subscriptions, notifications, raw thread data, cursors, and errors that the TUI would see for the main thread. With no active lease it can read and subscribe; while it owns the lease it can use owner-required mutations. `ControllerSession.effectiveCapabilities` reflects the sampled coarse-grained rights, but the authorization gate remains authoritative. No controller-specific read or mutation protocol exists.
- Controller-originated mutations and their resulting thread/turn events update the TUI through the normal in-process event path, with no separate controller transcript or hidden state.
- The generated controller admission registry covers every method, server-request response, implicit target, cursor, resume token, and subscription on both axes: target extraction and required authority. Unknown or unclassified methods are denied.
- Thread-affecting TUI operations are always primary: input against a controller-owned thread cancels its active input lease, rebinds any outstanding prompt to the TUI, and then executes. Display-only TUI interactions do not reclaim control. Every TUI-originated command has an explicit `threadAffecting` or `displayOnly` classifier; new commands default to `threadAffecting` until reviewed. Non-thread server requests remain TUI-only.
- Reclaim advances the owner epoch immediately, lets only an already-fenced controller step finish, then runs the TUI input next; queued controller work receives exactly one stale-ownership result.
- TUI approvals and user-input prompts are delivered to the current interactive owner; every request has exactly one authorized resolver and deterministic TUI fallback.
- A controller can release input ownership and later reacquire it without TUI input while its connection-bound standing authorization remains live; release is idempotent for a live standing session that already has no active lease, and the controller cannot displace another controller.
- Only one external controller can hold a valid active lease for the TUI main thread at a time; expiry, controller release, revocation, disconnect, or thread-affecting TUI input reject queued controller work and restore TUI ownership.
- Ownership transitions, revoke-versus-response races, and expiry during queued or long-running controller work have one coordinator-defined outcome.
- A controller receives the normal approval server-request shape, but any reply with a session-scoped or persistent effect is rejected with a typed controller-not-allowed error; controller approval effects cannot survive the associated lease.
- Lease expiry uses a server monotonic deadline; overlapping leases are rejected and expiry-versus-dequeue is deterministic.
- A controller without ownership attempting a thread mutation is rejected without changing thread state.
- Competing requests against one thread preserve the request-serialization ordering across in-process and socket connections.
- A controller request queued before a TUI burst observes the documented dequeue-priority and eight-request fairness bound only when both requests are non-interactive and concurrently admissible; ownership never yields to that fairness bound.
- Priority/fairness is verified through the new scheduler rather than assuming the existing FIFO serialization queue provides it.
- A disconnected or reconnected controller cannot retain or recover a prior lease or standing authorization without a new native participation grant.
- `controller/releaseControl` returns the main thread and outstanding prompt to the TUI while preserving the controller's read/subscription session and standing authorization; if there is no active lease, it returns the current `ControllerSession` with `activeLease: null`. `controller/acquireControl` can later issue a fresh lease without TUI input when no other controller holds it.
- A thread-affecting TUI action and a controller `acquireControl` race have one coordinator-defined result; TUI input wins and preserves the controller connection for a later request.
- Terminal authorization revocation and prompt ownership changes fence queued controller egress and subscriptions before their ordered change notification.
- The TUI consumes a canonical `threadSequence` event stream, renders controller input only from that stream, and resynchronizes a gapped thread from an atomic snapshot-at-sequence that includes ownership and pending prompts. `TuiUnavailable` is terminal for the launch and revokes controller input leases.
- Prompt redelivery uses coordinator-owned pending/resolved/cancelled state, and TUI ownership-status changes are typed in-process events ordered with `threadSequence`.
- Schema/golden tests cover every controller response, error, and notification; deterministic tests cover acquire-versus-TUI-input, prompt rebinding, and terminal-revocation egress fencing.
- Participation rejection, `controller/signOff`, and unexpected disconnect revoke every connection-bound lease and restore TUI prompt ownership.
- Saturated normal controller ingress returns `-32001` with typed overload data; a slow, saturated, or disconnected controller cannot block the TUI or prevent controller participation, acquire, release, or sign-off requests from using their separate bounded control-plane ingress.
- Controller prompt `externalDelivery` is recorded after successful frame completion, while a begun write is treated as potentially disclosed. Neither state prevents recovery: enqueue/write failure, release, reclaim, expiry, revocation, disconnect, and sign-off atomically fence the controller binding and recover a still-pending eligible prompt to the viable TUI. The old controller reply resolves only if it already consumed the binding; otherwise it is stale. If TUI recovery is unavailable or fails, the prompt fails terminally and the launch enters or retains `TuiUnavailable`.
- The authorization gate is ready before any endpoint accepts connections; every controller is default-denied from its first byte.
- The public experimental surface enumerates canonical error codes for pre-participation denial, enrollment denial, main-thread readiness/closure, TUI unavailability, ownership conflicts, stale ownership, forbidden controller decisions, transport teardown, target mismatch, expiry, and overload, with typed retryability data.
- Unix-socket and Windows local-endpoint creation, validation, peer verification, nonce handshake, and cleanup reject foreign resources and do not delete a live launch's endpoint.
- Embedded, daemon-backed, and explicit-remote TUI launch modes report whether external controllers are available; only the embedded mode enables this first delivery.
- Listener readiness and failure follow the documented authorize/bind/register/enable/publish order; an acceptor failure revokes established controller sessions and updates `embedded-unavailable` or `launch-failed` correctly.
- A controller inventory implementation uses the local-controller metadata directory as the discovery source, watches or polls it with full rescans, and discovers every live embedded TUI launch whose metadata validates and whose `mainThreadId` is published. Codex hooks are not required or relied on for launch discovery.
- Controller presentation distinguishes launch liveness from controller authorization and slot assignment. A live launch with valid metadata, a live process, a valid endpoint, and a non-null `mainThreadId` is not shown as offline merely because participation is pending, approval has not been granted, the controller has released control, or the controller lacks the active input lease.
- Auto-assignment, when implemented by a controller product, preserves existing user assignments and fills free slots deterministically for newly discovered live launches; Herdr or other terminal inventory may enrich labels but cannot be a required discovery authority.
- An end-to-end scenario launches the TUI, discovers the endpoint, approves a Codex Micro as the current input method, verifies normal app-server parity for the granted main thread, resolves an approval through the controller, sends thread-affecting TUI input that automatically reclaims control, and verifies TUI prompt ownership and final thread state.
- Existing TUI startup and standalone `codex app-server` transport tests continue to pass.

## Build and test validation

Validation for the staged implementation was recorded on branch
`cobblers/control-is-mine`. The broad parity checkpoint was commit `a36bf85`
(`refactor(app-server): centralize controller thread list filtering`). Later
Codex-side hardening has continued through commit `809b9e1`
(`feat(app-server): add thread sequence envelopes`), the in-process TUI
sequence-preservation slice, and the current in-process recovery-snapshot
slice. The recorded
implementation goal cost at the broad checkpoint was 7,828,188 tokens and
44,738 seconds (approximately 12h 25m 38s). At the experimental opt-in typed
error slice, the cumulative goal cost was 16,417,727 tokens and 49,993 seconds
(approximately 13h 53m 13s). At the TUI participation-decision coverage slice,
the cumulative goal cost was 16,583,966 tokens and 50,378 seconds
(approximately 13h 59m 38s). At the controller permission-approval scope
coverage slice, the cumulative goal cost was 16,809,460 tokens and 50,781
seconds (approximately 14h 06m 21s). At the controller file-change approval
scope coverage slice, the cumulative goal cost was 17,263,388 tokens and 71,702
seconds (approximately 19h 55m 02s). At the persistent controller approval
rejection slice, the cumulative goal cost was 17,496,049 tokens and 72,273
seconds (approximately 20h 04m 33s). At the controller control-plane overload
coverage slice, the cumulative goal cost was 17,786,148 tokens and 73,074
seconds (approximately 20h 17m 54s). At the primary prompt-reply reclaim slice,
the cumulative goal cost was 18,050,045 tokens and 73,904 seconds
(approximately 20h 31m 44s). At the reasoning-summary-part lossless delivery
slice, the cumulative goal cost was 18,403,089 tokens and 74,556 seconds
(approximately 20h 42m 36s). At the realtime controller delivery slice, the
cumulative goal cost was 18,710,228 tokens and 75,136 seconds (approximately
20h 52m 16s). At the TUI realtime-error rendering slice, the cumulative goal
cost was 19,227,569 tokens and 80,402 seconds (approximately 22h 20m 02s). At
the TUI realtime-transcript rendering slice, the cumulative goal cost was
19,543,576 tokens and 81,503 seconds (approximately 22h 38m 23s). At the TUI
app-server lag recovery slice, the cumulative goal cost was 19,860,366 tokens
and 82,340 seconds (approximately 22h 52m 20s). At the thread-started
lossless-delivery slice, the cumulative goal cost was 20,165,824 tokens and
83,365 seconds (approximately 23h 09m 25s). At the controller-visible state
lossless-delivery slice, the cumulative goal cost was 20,492,362 tokens and
84,213 seconds (approximately 23h 23m 33s). At the controller prompt
egress-fencing slice, the cumulative goal cost was 21,272,170 tokens and
87,013 seconds (approximately 24h 10m 13s). At the controller egress-overflow
coverage slice, the cumulative goal cost was 21,524,837 tokens and 87,580
seconds (approximately 24h 19m 40s). At the explicit controller
`thread/unsubscribe` coverage slice, the cumulative goal cost was 21,794,287
tokens and 88,185 seconds (approximately 24h 29m 45s). At the TUI
ownership-status history-exclusion slice, the cumulative goal cost was
22,166,703 tokens and 88,906 seconds (approximately 24h 41m 46s). At the TUI
snapshot sequence/ownership-state slice, the cumulative goal cost was
22,555,737 tokens and 89,939 seconds (approximately 24h 58m 59s).
At the in-process TUI sequence-preservation slice, the cumulative goal cost was
24,089,821 tokens and 96,994 seconds (approximately 26h 56m 34s).
At the in-process recovery-snapshot slice, the cumulative goal cost was
24,507,618 tokens and 98,385 seconds (approximately 27h 19m 45s).
At the availability reconciliation slice, the cumulative goal cost was
24,982,209 tokens and 100,159 seconds (approximately 27h 49m 19s).
At the controller participation auto-subscribe and e2e approval slice, the
cumulative goal cost was 25,650,619 tokens and 102,284 seconds (approximately
28h 24m 44s).
At the downstream inventory/persistence smoke slice, the cumulative goal cost
was 25,828,117 tokens and 102,665 seconds (approximately 28h 31m 05s).
At the downstream process-liveness discovery filter slice, the cumulative goal
cost was 26,215,467 tokens and 104,044 seconds (approximately 28h 54m 04s).
At the Herdr-driven native approval smoke slice, the cumulative goal cost was
26,440,707 tokens and 104,451 seconds (approximately 29h 00m 51s).
At the downstream presentation/slot-preservation slice, the cumulative goal cost
was 26,608,921 tokens and 104,835 seconds (approximately 29h 07m 15s).
At the downstream controller-resume lease slice, the cumulative goal cost was
26,997,994 tokens and 109,944 seconds (approximately 30h 32m 24s).
At the downstream discovery-watch and physical-tap routing slice, the
cumulative goal cost was 27,361,643 tokens and 111,767 seconds (approximately
31h 02m 47s).
At the Codex stale-launch cleanup and loaded-unmaterialized resume follow-up
slice, the cumulative goal cost was 28,330,459 tokens and 116,642 seconds
(approximately 32h 24m 02s).
At the downstream smoke run-loop wait fix and passing mutating smoke slice, the
cumulative goal cost was 28,642,039 tokens and 119,235 seconds (approximately
33h 07m 15s).
These costs include implementation, review, validation, and commit preparation
across the staged slices; they are not limited to build/test subprocess runtime.

The repository `docs/` tree is plain authored Markdown for this spec. No
`docs/Makefile`, Sphinx `conf.py`, or docs index file was present, so there was
no repository docs build target to run for this page.

Recorded build and validation evidence:

| Check | Result | Reported cost |
| --- | --- | --- |
| `just test -p codex-app-server local_controller_socket_uses_main_thread_interface_and_tui_reclaim` | Passed: 1 test run, 1 passed, 1093 skipped. This covers the local-controller socket parity path, including filtered main-thread `thread/list`, controller mutation, TUI reclaim, stale ownership, read-after-reclaim, reacquire, and final state. | Compile reported 28.92s; nextest reported 3.779s. |
| `just test -p codex-app-server` | Passed: 1093/1093 tests passed, 1 skipped. Two unrelated tests were flaky and passed on retry: `login_account_chatgpt_redirects_to_hosted_success_page` and `plugin_list_honors_global_remote_catalog_cache_ttl`. | Compile reported 15.89s; nextest reported 111.479s. |
| `just fmt` | Blocked before Rust formatting because `dotslash` was not installed: `[Errno 2] No such file or directory: 'dotslash'`. | Failed after the formatter wrapper reached the missing tool. |
| `cargo fmt -p codex-app-server` | Passed as the scoped Rust formatting fallback for the changed crate. | Shell wall time was approximately 0.6s. |
| `just fix -p codex-app-server` | Passed. It auto-fixed two unrelated lint sites; those hunks were reviewed and reverted so the cleanup commit remained scoped to controller routing. | Cargo reported 30.18s. |
| `git diff --check` and `git diff --cached --check` | Passed. | Subsecond. |
| `pre-commit run --all-files` | Could not run repository hooks because `.pre-commit-config.yaml` is absent. | Failed fast with `InvalidConfigError`. |
| `just test -p codex-app-server connection_rpc_gate saturated_external_controller` | Passed: 10/10 focused tests after the separate control-plane ingress hardening. | Prior run reported 35.07s compile and 0.781s nextest. |
| `just test -p codex-app-server controller` | Passed: 93/93 controller-filtered tests after the separate control-plane ingress hardening. | Prior run reported 23.363s nextest. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the separate control-plane ingress hardening. | Cargo reported 1.47s. |
| `just fmt` | Passed after the local-socket native rejection coverage slice. | Shell wall time was 6.543s. |
| `just test -p codex-app-server local_controller_socket_rejected_participation_stays_denied_until_reapproved` | Passed: 1 test run, 1 passed, 1237 skipped. This covers TUI native rejection over the published local-controller socket, preserved denial of normal thread reads, repeat participation on the same initialized connection, and later approval. | Compile reported 19.56s; nextest reported 7.467s. |
| `just test -p codex-app-server local_controller_socket_` | Passed: 5 test runs, 5 passed, 1233 skipped after adding the native rejection socket test. | Compile reported 1.07s; nextest reported 23.922s. |
| `just test -p codex-app-server local_controller_` | Passed: 9 test runs, 9 passed, 1229 skipped across local-controller startup, native approval, notification suppression, socket parity, single-lease, launch isolation, reconnect, and native rejection coverage. | Compile reported 0.76s; nextest reported 29.220s. |
| `git diff --check` | Passed after the local-socket native rejection coverage slice. | Subsecond. |
| `just fmt` | Passed after the local-socket starting-readiness coverage slice. | Shell wall time was 6.368s. |
| `just test -p codex-app-server local_controller_socket_retries_participation_after_main_thread_publish` | Passed: 1 test run, 1 passed, 1238 skipped. This covers retryable `main-thread-unavailable` over the published socket before `mainThreadId` exists, and successful repeat participation on the same initialized connection after the TUI main thread is created. | Compile reported 20.29s; nextest reported 7.051s. |
| `just test -p codex-app-server local_controller_socket_` | Passed: 6 test runs, 6 passed, 1233 skipped after adding the starting-readiness socket test. | Compile reported 1.04s; nextest reported 27.787s. |
| `just test -p codex-app-server local_controller_` | Passed: 10 test runs, 10 passed, 1229 skipped across local-controller startup, native approval, notification suppression, socket parity, single-lease, launch isolation, reconnect, native rejection, and starting-readiness coverage. | Compile reported 0.74s; nextest reported 33.439s. |
| `git diff --check` | Passed after the local-socket starting-readiness coverage slice. | Subsecond. |
| `just fmt` | Passed after the in-process metadata publish route coverage slice. | Shell wall time was 6.520s. |
| `just test -p codex-app-server local_controller_main_thread_publish_updates_discovery_metadata` | Passed: 1 test run, 1 passed, 1239 skipped. This covers the in-process TUI publish command updating the actual local-controller discovery metadata file, and verifies a later publish does not replace the immutable main-thread ID. | Compile reported 20.53s; nextest reported 0.595s. |
| `just test -p codex-app-server local_controller_` | Passed: 11 test runs, 11 passed, 1229 skipped after adding the metadata publish route test. | Compile reported 1.10s; nextest reported 36.461s. |
| `git diff --check` | Passed after the in-process metadata publish route coverage slice. | Subsecond. |
| `just fmt` | Passed after the embedded nonce rejection coverage slice. | Shell wall time was 6.274s on the final run. |
| `just test -p codex-app-server local_controller_endpoint_rejects_missing_or_wrong_launch_nonce` | First attempt failed to compile because the new test helper used the module's protocol `Result` alias instead of `std::result::Result`; the helper signature was corrected. Final run passed: 1 test run, 1 passed, 1240 skipped. This covers missing and wrong launch nonce rejection over the embedded endpoint, followed by a valid nonce connection. | Final compile reported 17.10s; nextest reported 0.557s. |
| `just test -p codex-app-server local_controller_` | Passed: 12 test runs, 12 passed, 1229 skipped after adding the embedded nonce rejection test. | Compile reported 0.98s; nextest reported 35.925s. |
| `git diff --check` | Passed after the embedded nonce rejection coverage slice. | Subsecond. |
| `just fmt` | Passed after the experimental opt-in typed error slice. | Shell wall time was 6.368s. |
| `just test -p codex-app-server local_controller_endpoint_requires_experimental_api_opt_in` | Passed: 1 test run, 1 passed, 1241 skipped. This covers an initialized local-controller socket without experimental API opt-in receiving typed `experimental-not-enabled` controller error data, then a reconnect with opt-in successfully reaching native TUI approval. | Compile reported 24.92s; nextest reported 7.116s. |
| `just test -p codex-app-server local_controller_` | Passed: 13 test runs, 13 passed, 1229 skipped after adding the experimental opt-in typed error path. | Compile reported 1.07s; nextest reported 38.642s. |
| `just test -p codex-app-server controller` | Passed: 103 test runs, 103 passed, 1139 skipped after the pre-dispatch controller error change. | Compile reported 0.77s; nextest reported 54.176s. |
| `git diff --check` | Passed after the experimental opt-in typed error slice. | Subsecond. |
| `just fmt` | Passed after the TUI participation-decision coverage slice. | Shell wall time was 6.216s. |
| `just test -p codex-tui controller_participation` | Passed: 4 test runs, 4 passed, 3455 skipped. This covers the controller participation prompt snapshot plus the owning TUI emitting approved, denied, and dismissed native participation decisions to the app-layer responder. | Compile reported 40.67s; nextest reported 0.119s. |
| `git diff --check` | Passed after the TUI participation-decision coverage slice. | Subsecond. |
| `just fmt` | The first sandboxed run failed because `uv` could not initialize its cache under `~/.cache/uv`; rerunning with cache access passed after the controller permission-approval scope slice. | Passing run shell wall time was 6.436s. |
| `just test -p codex-app-server controller_rejects_session_scoped_permission_approval` | Passed: 1 test run, 1 passed, 1242 skipped. This covers a controller-owned `item/permissions/requestApproval` rejecting a session-scoped response with typed `controller-not-allowed`, keeping the prompt pending, and then accepting a turn-scoped response on the same active controller connection. | Final compile reported 15.68s; nextest reported 0.583s. |
| `just test -p codex-app-server controller` | Passed: 104 test runs, 104 passed, 1139 skipped after adding permission-approval scope coverage. | Compile reported 0.98s; nextest reported 59.317s. |
| `git diff --check` | Passed after the controller permission-approval scope slice. | Subsecond. |
| `just fmt` | Passed after the controller file-change approval scope slice. | Shell wall time was 6.905s. |
| `just test -p codex-app-server controller_rejects_session_scoped_file_change_approval` | Passed: 1 test run, 1 passed, 1243 skipped. This covers a controller-owned `item/fileChange/requestApproval` rejecting `acceptForSession` with typed `controller-not-allowed`, keeping the prompt pending, and then accepting a non-session-scoped response on the same active controller connection. | Compile reported 16.67s; nextest reported 0.594s. |
| `just test -p codex-app-server controller` | Passed: 105 test runs, 105 passed, 1139 skipped after adding file-change approval scope coverage. | Compile reported 1.02s; nextest reported 58.822s. |
| `git diff --check` | Passed after the controller file-change approval scope slice. | Subsecond. |
| `just fmt` | Passed after the persistent controller approval rejection slice. | Shell wall time was 6.811s. |
| `just test -p codex-app-server controller_rejects_persistent_command_approval_decisions` | Passed: 1 test run, 1 passed, 1244 skipped. This covers an active controller rejecting command approval decisions that would persist beyond the connection-bound lease: exec-policy amendments and network-policy amendments. Each rejected prompt remained pending and then resolved with a non-persistent `decline`. | Compile reported 1m 09s; nextest reported 0.659s. |
| `just test -p codex-app-server controller` | Passed: 106 test runs, 106 passed, 1139 skipped after adding persistent command-approval rejection coverage. | Compile reported 1.14s; nextest reported 62.721s. |
| `git diff --check` | Passed after the persistent controller approval rejection slice. | Subsecond. |
| `just fmt` | Passed after the controller control-plane overload coverage slice. | Shell wall time was 7.627s. |
| `just test -p codex-app-server saturated_external_controller_control_ingress_returns_typed_overload` | Passed: 1 test run, 1 passed, 1245 skipped. This covers an initialized external controller receiving typed `controller-overloaded` data when the controller control-plane ingress reservation pool is exhausted before `controller/requestParticipation` dispatch. | Compile reported 1m 03s; nextest reported 0.546s. |
| `just test -p codex-app-server saturated_external_controller` | Passed: 3 test runs, 3 passed, 1243 skipped across normal ingress overload, control-plane ingress overload, and normal-saturation allowing release/acquire/sign-off control-plane dispatch. | Compile reported 1.06s; nextest reported 1.212s. |
| `git diff --check` | Passed after the controller control-plane overload coverage slice. | Subsecond. |
| `just fmt` | Passed after the primary prompt-reply reclaim slice. | Shell wall time was 6.454s on the final run. |
| `just test -p codex-app-server primary_prompt_response_reclaims_controller_owned_prompt` | Initial attempts caught a compile-time integration issue: the implementation first tried to call a non-existent `ServerRequest::method()` helper, then the expanded test needed an explicit `JSONRPCErrorError` import. Final run passed: 1 test run, 1 passed, 1246 skipped. This covers primary/TUI prompt responses and errors to externally delivered controller-owned prompts reclaiming ownership, resolving or rejecting the prompt, and leaving subsequent controller mutations stale. | Final compile reported 9.72s; nextest reported 0.634s. |
| `just test -p codex-app-server controller_prompt_response_is_bound_to_owner_epoch` | Passed: 1 test run, 1 passed, 1246 skipped after the shared server-request mapping change. | Compile reported 1.52s; nextest reported 0.761s. |
| `just test -p codex-app-server controller_current_time_request_is_bound_to_owner_epoch` | Passed: 1 test run, 1 passed, 1246 skipped after the shared server-request mapping change. | Compile reported 1.71s; nextest reported 0.710s. |
| `git diff --check` | Passed after the primary prompt-reply reclaim slice. | Subsecond. |
| `just fmt` | Passed after the reasoning-summary-part lossless delivery slice. | Shell wall time was 6.429s. |
| `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications` | Passed: 1 test run, 1 passed, 1246 skipped. This covers the embedded in-process lossless classifier preserving `item/reasoning/summaryPartAdded` with other transcript/reasoning/terminal notifications. | Compile reported 27.13s; nextest reported 0.060s. |
| `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events` | Passed: 1 test run, 1 passed, 28 skipped. This covers the app-server-client bridge preserving `item/reasoning/summaryPartAdded` through the shared classifier. | Compile reported 15.63s; nextest reported 0.060s. |
| `git diff --check` and `git diff --cached --check` | Passed after the reasoning-summary-part lossless delivery slice. | Subsecond. |
| `just fmt` | Passed after the realtime controller delivery slice at commit `b2738fb`. | Shell wall time was 6.736s. |
| `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications` | Passed: 1 test run, 1 passed, 1246 skipped. This covers the embedded in-process lossless classifier preserving realtime started, transcript delta/done, error, and closed notifications with other transcript/reasoning/terminal notifications. | Compile reported 45.55s; nextest reported 0.047s. |
| `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events` | Passed: 1 test run, 1 passed, 28 skipped. This covers the app-server-client bridge preserving the same realtime transcript/lifecycle notifications through the shared classifier. | Compile reported 13.85s; nextest reported 0.044s. |
| `git diff --check` and `git diff --cached --check` | Passed after the realtime controller delivery source slice. | Subsecond. |
| `just fmt` | Passed after the TUI realtime-error rendering slice. | Shell wall time was 7.242s. |
| `just test -p codex-tui live_app_server_realtime_error_notification_renders_warning` | Passed: 1 test run, 1 passed, 3459 skipped. This covers rendering `thread/realtime/error` as a visible TUI warning instead of dropping it in the app-server notification handler. | Cargo reported 3m 45s after a cold rebuild; nextest reported the focused test passed. |
| `just fix -p codex-tui` | Passed after the TUI realtime-error rendering slice. | Cargo reported 2m 18s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the TUI realtime-error rendering slice. | Cargo reported 1m 34s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` | Passed after the TUI realtime-error rendering source slice. | Subsecond. |
| `just fmt` | Passed after the TUI realtime-transcript rendering slice. | Shell wall time was 6.523s on the final run. |
| `just test -p codex-tui live_app_server_realtime` | Passed: 3 test runs, 3 passed, 3459 skipped. This covers realtime error warning rendering, final user transcript rendering, and assistant transcript streaming/consolidation. | Cargo reported 11.99s; nextest reported 0.089s. |
| `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` | Passed: no pending snapshots. | Installed `cargo-insta` first in 20.61s; the scoped check then passed in 8.06s after rerunning with cargo-cache access. |
| `just fix -p codex-tui` | Passed after the TUI realtime-transcript rendering slice. | Cargo reported 23.88s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the TUI realtime-transcript rendering slice. | Cargo reported 14.53s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` and `git diff --cached --check` | Passed after the TUI realtime-transcript rendering source slice. | Subsecond. |
| `just fmt` | Passed after the TUI app-server lag recovery slice. | Shell wall time was 6.162s. |
| `just test -p codex-tui lag_refresh_replays_authoritative_active_thread_snapshot` | Passed: 1 test run, 1 passed, 3462 skipped. This covers active-thread resynchronization from an authoritative `thread/read(includeTurns=true)` snapshot after app-server event lag, replayed through normal TUI history rendering. | Compile reported 27.72s; nextest reported 0.103s. |
| `just test -p codex-tui mcp_startup app_scoped_mcp_startup_notifications_do_not_render_in_active_thread active_side_thread_renders_live_mcp_startup_notifications` | Passed: 38 test runs, 38 passed, 3425 skipped. This preserves existing app-server event routing call sites after the mutable session handler signature change and keeps MCP startup lag behavior covered. | Compile reported 1.06s; nextest reported 0.609s. |
| `just fix -p codex-tui` | Passed after the TUI app-server lag recovery slice. | Cargo reported 22.94s. |
| `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` | Passed: no pending snapshots. | Shell wall time was 0.779s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the TUI app-server lag recovery slice. | Cargo reported 13.03s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` and `git diff --cached --check` | Passed after the TUI app-server lag recovery docs/source slice. | Subsecond. |
| `just fmt` | Passed after the thread-started lossless-delivery slice; rerun after correcting the test fixture. | Final shell wall time was 6.677s. |
| `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications` | Passed: 1 test run, 1 passed, 1246 skipped. This covers the embedded in-process lossless classifier preserving `thread/started` along with other transcript, lifecycle, reasoning, and terminal notifications. | Final compile reported 1.54s; nextest reported 0.039s. An earlier compile-heavy run also passed after 4m 12s. |
| `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events` | Final run passed: 1 test run, 1 passed, 28 skipped. The first run failed to compile because the new test fixture used nonexistent `ThreadHistoryMode::Full`; the fixture now uses the default history mode. | Final compile reported 11.13s; nextest reported 0.060s. |
| `just fix -p codex-app-server` | Passed after the thread-started lossless-delivery slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the source commit stayed scoped. | Cargo reported 1m 50s. |
| `just fix -p codex-app-server-client` | Passed after the thread-started lossless-delivery slice. | Cargo reported 2m 41s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the thread-started lossless-delivery slice. | Cargo reported 19.27s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` and `git diff --cached --check` | Passed after the thread-started lossless-delivery source slice. | Subsecond. |
| `just fmt` | Passed after the controller-visible state lossless-delivery slice; rerun after correcting the initially omitted `turn/diff/updated` classifier arm. | Final shell wall time was 6.766s. |
| `just test -p codex-app-server guaranteed_delivery_helpers_cover` | Final run passed: 2 test runs, 2 passed, 1246 skipped. This covers the embedded in-process lossless classifier preserving transcript/reasoning/realtime notifications plus controller-visible lifecycle/state notifications such as prompt resolution, warnings, goal/token status, MCP startup, turn start/diff/plan, hooks, item start, Guardian review, terminal interaction, and model safety/verification state. The first run failed because the new `turn/diff/updated` assertion exposed a missing classifier arm; the arm was added and the focused test was rerun. | Final compile reported 20.71s; nextest reported 0.043s. |
| `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events` | Passed: 1 test run, 1 passed, 28 skipped. This covers the app-server-client bridge preserving representative controller-visible lifecycle/state notifications through the shared classifier. | Compile reported 12.09s; nextest reported 0.036s. |
| `just fix -p codex-app-server` | Passed after the controller-visible state lossless-delivery slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the source commit stayed scoped. | Cargo reported 30.99s. |
| `just fix -p codex-app-server-client` | Passed after the controller-visible state lossless-delivery slice. | Cargo reported 6.74s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the controller-visible state lossless-delivery slice. | Cargo reported 21.47s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` | Passed after the controller-visible state lossless-delivery source slice. | Subsecond. |
| `just fmt` | Passed after the controller prompt egress-fencing slice. | Final shell wall time was 6.610s. |
| `just test -p codex-app-server-transport outgoing_message` | Passed: 4 test runs, 4 passed, 157 skipped. This covers controller write permits, revoked pre-write permits, and begin-write marking for queued external-controller egress. | Compile reported 6.14s; nextest reported 0.061s. |
| `just test -p codex-app-server outgoing_message in_process::tests::local_controller_socket_uses_main_thread_interface_and_tui_reclaim controller` | Historical result: passed before the pending-prompt transfer request. Its assertion of no automatic TUI redelivery after external delivery or write-begin is superseded by this feature request and must be replaced with post-delivery recovery coverage. | Compile reported 27.69s; nextest reported 27.903s. |
| `just test -p codex-app-server-transport` | Passed: 161 test runs, 161 passed, 0 skipped. This covers the changed transport crate across stdio, websocket, remote-control, local-controller, and Unix-socket transport tests. | Compile reported 1.02s; nextest reported 9.481s. |
| `just fix -p codex-app-server-transport` | Passed after the controller prompt egress-fencing slice. | Cargo reported 4.63s. |
| `just fix -p codex-app-server` | Passed after the controller prompt egress-fencing slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the source commit stayed scoped. | Cargo reported 35.10s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the controller prompt egress-fencing slice. | Cargo reported 41.15s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` and `git diff --cached --check` | Passed after the controller prompt egress-fencing source slice. | Subsecond. |
| `just fmt` | Passed after the controller egress-overflow coverage slice. | Final shell wall time was 6.602s. |
| `just test -p codex-app-server external_controller_queue_overflow` | Passed: 2 test runs, 2 passed, 1249 skipped. This covers a slow external-controller queue disconnecting only that external connection and a controller-bound prompt falling back to the TUI when queue overflow drops the controller egress before `externalDelivery`. | Compile reported 19.42s; nextest reported 0.079s. |
| `just test -p codex-app-server outgoing_message transport` | Passed: 58 test runs, 58 passed, 1193 skipped. This covers the new overflow cases plus the existing outgoing-message prompt ownership and transport routing behavior around targeted egress, broadcast filtering, final response flush, and disconnectable versus stdio queue handling. | Compile reported 1.04s; nextest reported 15.422s. |
| `just fix -p codex-app-server` | Passed after the controller egress-overflow coverage slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the test commit stayed scoped. | Cargo reported 25.52s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the controller egress-overflow coverage slice. | Cargo reported 24.24s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` and `git diff --cached --check` | Passed after the controller egress-overflow coverage slice. | Subsecond. |
| `just fmt` | Passed after the explicit controller `thread/unsubscribe` coverage slice. | Shell wall time was 6.852s. |
| `just test -p codex-app-server controller_thread_unsubscribe_is_bound_to_standing_main_thread_session thread_resume_extracts_exact_controller_thread_target exact_controller_thread_target_uses_serialization_scope` | Passed: 3 test runs, 3 passed, 1249 skipped. This covers explicit controller `thread/unsubscribe` target extraction, released-controller standing-session unsubscribe of the authorized main thread, and wrong-thread rejection before the unsubscribe handler mutates subscriptions. | Compile reported 19.12s; nextest reported 0.420s. |
| `just test -p codex-app-server controller` | Passed: 113 test runs, 113 passed, 1139 skipped after adding explicit controller `thread/unsubscribe` coverage. | Compile reported 1.07s; nextest reported 29.573s. |
| `just fix -p codex-app-server` | Passed after the explicit controller `thread/unsubscribe` coverage slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the test commit stayed scoped. | Cargo reported 29.50s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the explicit controller `thread/unsubscribe` coverage slice. | Cargo reported 17.41s with the known `__eh_frame section too large` linker warning. |
| `just fmt` | Passed after the TUI ownership-status history-exclusion slice. | Shell wall time was 6.655s. |
| `just test -p codex-tui controller_ownership_status_event_does_not_write_transcript_history` | Passed: 1 test run, 1 passed, 3463 skipped. This covers the typed in-process `ControllerOwnershipStatus` event entering the real TUI app-server event handler without writing transcript history or creating an active transcript cell. | Compile reported 1m 05s; nextest reported 0.370s. |
| `just fix -p codex-tui` | Passed after the TUI ownership-status history-exclusion slice. | Cargo reported 49.82s. |
| `just test -p codex-tui controller_ownership_status_event_does_not_write_transcript_history controller_control_plane_notifications_do_not_write_transcript_history lag_refresh_replays_authoritative_active_thread_snapshot` | Passed: 3 test runs, 3 passed, 3461 skipped. This covers the new typed ownership-status history exclusion, existing JSON-RPC controller control-plane history exclusion, and lag snapshot recovery together. | Compile reported 1.07s; nextest reported 0.340s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the TUI ownership-status history-exclusion slice. | Cargo reported 0.85s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` | Passed before committing the TUI snapshot sequence/ownership-state source slice. | Subsecond. |
| `just test -p codex-tui thread_event_store_snapshots_monotonic_sequence thread_event_store_snapshots_controller_ownership_status controller_ownership_status_event_does_not_write_transcript_history lag_refresh_replays_authoritative_active_thread_snapshot` | Passed: 4 test runs, 4 passed, 3462 skipped. This covers local TUI snapshot sequence advancement, controller-ownership status snapshotting, ownership-status non-history handling, and lag snapshot recovery together. | Compile reported 36.80s; nextest reported 0.372s. |
| `just fmt` | Passed after the TUI snapshot sequence/ownership-state slice. | Shell wall time was 6.495s. |
| `just fix -p codex-tui` | Final run passed after narrowing a test-only `MutexGuard` lifetime that the first fixer run reported as `await_holding_invalid_type`. | Final cargo run reported 13.13s; the initial diagnostic fixer run reported 25.69s. |
| `cargo build -p codex-cli -j 4` | Passed and rebuilt `codex-rs/target/debug/codex` after the TUI snapshot sequence/ownership-state slice. | Cargo reported 21.35s with the known `__eh_frame section too large` linker warning. |
| `just test -p codex-app-server in_process_thread_notifications_preserve_thread_sequence` | Passed: 1 test run, 1 passed, 1258 skipped. This covers the embedded in-process event path preserving the app-server-assigned per-thread sequence for thread notifications. | Compile reported 48.36s; nextest reported 3.796s. |
| `just test -p codex-app-server-client app_server_event_preserves_in_process_thread_sequence forward_in_process_event_rejects_dropped_sequenced_server_requests` | Passed: 2 test runs, 2 passed, 29 skipped. This covers the app-server-client bridge preserving sequenced in-process notifications/requests and rejecting a dropped sequenced server request under backpressure. | Compile reported 1.00s; nextest reported 0.061s. |
| `just test -p codex-tui thread_event_store_adopts_authoritative_server_sequence thread_event_store_drops_stale_events_after_authoritative_snapshot` | Passed: 2 test runs, 2 passed, 3466 skipped. This covers TUI adoption of the first authoritative app-server sequence and stale sequenced event suppression after a refreshed snapshot. | Compile reported 33.52s; nextest reported 0.091s. |
| `just test -p codex-app-server-client` | Passed: 31 test runs, 31 passed. This covers the full app-server-client crate after adding sequenced in-process event variants. | Nextest reported 45.078s. |
| `just test -p codex-exec` | Passed: 136 test runs, 136 passed. This covers the exec crate after normalizing sequenced in-process events with the legacy variants. | Compile reported 1m 08s; nextest reported 80.651s. |
| `just test -p codex-tui changing_cyber_model_reasoning_preserves_selected_permissions handle_start_side_seeds_navigation_before_thread_started override_turn_context_sends_thread_settings_update selecting_cyber_model_defaults_active_thread_to_auto_review selecting_cyber_model_respects_auto_review_requirements active_turn_interrupt_is_nonblocking_and_coalesces_repeated_requests safety_retry_can_retry_a_first_turn_a_second_time safety_retry_branch_failure_preserves_unsent_draft safety_retry_forks_after_the_previous_turn_and_uses_faster_settings safety_retry_forks_first_turn_and_continues_without_duplicating_prompt safety_retry_preserves_a_committed_steer_from_the_interrupted_turn safety_retry_replays_older_interruption_notices in_app_resume_uses_configured_or_explicit_cwd` | Passed: 13 test runs, 13 passed, 3455 skipped after updating TUI test helpers to accept sequenced in-process notifications. This covers the previously failing event-routing helpers and in-app resume path. | Compile reported 57.23s; nextest reported 36.084s. |
| `just test -p codex-tui` | Mostly passed on the full crate rerun: 3462 passed, 1 timed out, 5 skipped. The only timeout was `app::tests::in_app_resume_uses_configured_or_explicit_cwd`, which passed in the focused command above; no functional failure remained in the sequence-preservation paths. | Nextest reported 171.884s before the single test timeout. |
| `just fmt` | Passed after the in-process TUI sequence-preservation slice. | Shell wall time was 6.431s on the final run. |
| `just fix -p codex-app-server` | Passed after the in-process TUI sequence-preservation slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the source commit stayed scoped. | Cargo reported 1m 27s. |
| `just fix -p codex-app-server-client` | Passed after the in-process TUI sequence-preservation slice. | Cargo reported 37.14s. |
| `just fix -p codex-tui` | Passed after the in-process TUI sequence-preservation slice. | Cargo reported 1m 20s. |
| `just fix -p codex-exec` | Passed after the in-process TUI sequence-preservation slice. | Cargo reported 47.97s. |
| `cargo build -p codex-cli --bin codex` | Passed and rebuilt `codex-rs/target/debug/codex`; final binary size was 756,912,216 bytes. | Cargo reported 48.32s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` | Passed after the in-process TUI sequence-preservation slice. | Subsecond. |
| `just fmt` | Passed after the in-process recovery-snapshot slice. | Shell wall time was 6.568s on the final run. |
| `just test -p codex-app-server in_process_thread_snapshot_reads_main_thread_state pending_requests_for_thread_returns_thread_requests_in_request_id_order tracker_advances_per_thread` | Passed: 3 test runs, 3 passed, 1257 skipped after changing the focused fixture to request `includeTurns=false` for its ephemeral thread. This covers an embedded app-server-owned recovery snapshot returning the started main thread, authoritative sequence, current TUI ownership, owner epoch, and no pending prompts, plus the outgoing helper returning a coherent sequence with pending prompts and the synchronous thread-sequence tracker. | Final compile reported 25.53s; nextest reported 3.647s. |
| `just test -p codex-app-server-client in_process_thread_snapshot_reads_started_thread` | Passed: 1 test run, 1 passed, 31 skipped. This covers the app-server-client worker command path for in-process recovery snapshots. | Final compile reported 14.46s; nextest reported 4.991s. |
| `just test -p codex-tui thread_event_store_adopts_in_process_snapshot_prompt_and_owner_state lag_refresh_replays_authoritative_active_thread_snapshot` | Passed: 2 test runs, 2 passed, 3467 skipped. This covers TUI replacement of stale prompt buffer state with app-server-owned pending prompts plus ownership status and sequence, and preserves existing lag-refresh behavior after preferring the in-process recovery snapshot for embedded sessions. | Final compile reported 54.45s after waiting on the app-server-client build lock; nextest reported 0.108s. |
| `just fix -p codex-app-server` | Passed after the in-process recovery-snapshot slice. It rewrote unrelated `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks; those were reviewed and reverted so the source commit stayed scoped. | Final cargo run reported 30.89s. |
| `just fix -p codex-app-server-client` | Passed after the in-process recovery-snapshot slice. | Cargo reported 5.83s. |
| `just fix -p codex-tui` | Passed after the in-process recovery-snapshot slice. | Cargo reported 27.68s. |
| `cargo build -p codex-cli --bin codex` | Passed and rebuilt `codex-rs/target/debug/codex`; final binary size was 757,129,192 bytes. | Cargo reported 21.72s with the known `__eh_frame section too large` linker warning. |
| `git diff --check` | Passed after the in-process recovery-snapshot source/docs slice. | Subsecond. |
| `just test -p codex-tui external_controller_availability embedded_app_server_requests_best_effort_controller_endpoint embedded_app_server_can_disable_controller_endpoint_by_policy embedded_app_server_can_require_controller_endpoint_by_policy embedded_app_server_start_failure_is_returned` | Passed: 9 test runs, 9 passed, 3460 skipped. This covers embedded-supported, embedded-unavailable, policy-disabled, launch-failed, daemon-unsupported, remote-unsupported, best-effort startup, disabled startup, required startup, and startup failure propagation. | Compile reported 24.32s; nextest reported 1.082s. |
| `just test -p codex-app-server best_effort_local_controller_endpoint_failure_allows_startup enabled_local_controller_endpoint_failure_fails_startup local_controller_main_thread_publish_updates_discovery_metadata local_controller_socket_retries_participation_after_main_thread_publish` | Passed: 4 test runs, 4 passed, 1256 skipped. This covers best-effort endpoint failure continuing startup, required endpoint failure aborting startup, metadata `mainThreadId` publication without replacing the immutable main-thread binding, and retryable same-connection participation before main-thread readiness. | Compile reported 20.10s; nextest reported 5.850s. |
| `just test -p codex-app-server-transport local_controller control_socket listen_unix_socket` | Passed: 18 test runs, 18 passed, 143 skipped. This covers local-controller metadata, nonce, peer-credential, cleanup, acceptor-failure reporting, main-thread metadata republication, and existing Unix control-socket/default `unix://` parsing and WebSocket upgrade behavior. | Compile reported 1.02s; nextest reported 0.300s. |
| `just fmt` | Passed after the controller participation auto-subscribe and e2e approval slice, including the final rerun after removing unrelated fixer hunks. | Final shell wall time was 6.442s. |
| `just test -p codex-app-server local_controller_socket_uses_main_thread_interface_and_tui_reclaim` | Passed: 1 test run, 1 passed, 1259 skipped after adding controller-resolved approval coverage and subscribing approved controllers to the granted main-thread listener during participation. This now covers local-controller socket parity, controller `turn/start`, controller receipt and resolution of `item/commandExecution/requestApproval`, listener-ordered `serverRequest/resolved`, command completion, turn completion, TUI reclaim, stale ownership, read-after-reclaim, reacquire, and final state. | Final compile reported 15.21s; nextest reported 7.009s. |
| `just test -p codex-app-server local_controller controller` | Passed: 114 test runs, 114 passed, 1146 skipped after the controller participation auto-subscribe runtime fix. This covers the local-controller socket/native approval paths together with controller admission, lease ownership, prompt response, notification targeting, subscription cleanup, and bounded ingress/egress behavior. | Compile reported 0.77s; nextest reported 47.336s. |
| `just test -p codex-app-server` | Attempted after the controller participation auto-subscribe slice. Controller/local-controller coverage passed, but the full crate run still reported 1254 passed, 2 flaky passed on retry, 5 failed, and 1 skipped. Rerunning the five failures reproduced the same failures, all outside the touched controller path: two remote-thread-store deadline tests and three zsh-fork deadline/mock-request tests. | Full run compile reported 0.81s; nextest reported 217.442s. Exact failure rerun returned 0 passed, 5 failed, 1255 skipped. |
| `just fix -p codex-app-server` | Passed after the controller participation auto-subscribe slice. It auto-fixed two unrelated lint sites; those hunks were reviewed and reverted so the commit stayed scoped to controller behavior. | Cargo reported 36.09s. |
| `git diff --check` | Passed after the controller participation auto-subscribe source slice. | Subsecond. |
| `cargo build -p codex-cli --bin codex` | Passed and rebuilt `codex-rs/target/debug/codex`; `./codex-rs/target/debug/codex --version` reported `codex-cli 0.147.1`, and the final binary size was 757,129,080 bytes. | Cargo reported 29.76s with the known `__eh_frame section too large` linker warning. |
| `/Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost/.build/arm64-apple-macosx/debug/first-vertical-slice-external-controller-smoke --application-support <isolated-empty-temp-dir>` | Passed after the owning TUIs approved the native `codex-waveshare` participation prompts: `external-controller smoke: pass (launches: 2, exact launch-scoped route persisted, no Codex mutation requested)`. The first run timed out while participation was pending because the prompts were missed. This earlier smoke revision is downstream inventory/persistence evidence only; it did not perform Codex mutation, `thread/resume`, or removal reconciliation. | Passing run wall time was 7.710s. Isolated root was `/private/tmp/codex-external-smoke.gvtgFu`. |
| Downstream commit `508880c` (`fix(host): filter stale Codex controller launches`) | Passed focused and full downstream host validation after adding `processId` decoding/liveness filtering to `LocalControllerDiscovery` and regression coverage using real AF_UNIX socket metadata. This implements the discovery-contract requirement to ignore dead process records before launch presentation or controller connection. | `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost --filter LocalControllerDiscoveryTests` passed 1/1 in 5.115s; full `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed 56/56 in 3.087s; `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed in 0.475s; downstream `pre-commit run --files ...` passed. |
| Temporary Herdr workspace `w16` with Codex panes `w16:p1` and `w16:p2` plus downstream smoke pane `w16:p3` | Passed after driving each native `Allow codex-waveshare to control this session?` prompt by sending Enter to the owning Codex TUI pane. The first Herdr-driven attempt timed out at the smoke deadline while approval was still pending; the rerun passed: `external-controller smoke: pass (launches: 2, exact launch-scoped route persisted, no Codex mutation requested)`. This validates terminal-driven native approval plus connection/inventory/persistence against two temporary live Codex TUIs, and remains non-mutating downstream evidence. | Passing run was reported by Herdr at 4s. Passing isolated root was `/private/tmp/codex-external-herdr-smoke.oNh2Hi`; the timed-out root was `/private/tmp/codex-external-herdr-smoke.baEaLc`. |
| Downstream commit `5a963b4` (`fix(host): preserve discovered Codex slot state`) | Passed after adding downstream host logic that maps live but not-yet-approved/connected launches to non-offline slot statuses and preserves existing slot assignments while filling free slots deterministically for newly discovered Codex sessions. This covers the basic presentation-state and auto-assignment preservation rules; it is not file-watch/full-rescan or mutating `thread/resume` evidence. | Focused `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost --filter 'ProtocolTests/testV7MapPreservesExistingRoutesAndFillsFreeSlotsDeterministically\|OperationalStateTests/testLiveButUnapprovedLaunchesDoNotRenderOfflineStatus'` passed 2/2; full `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed 58/58; `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed; downstream `pre-commit run --files ...` passed. |
| Downstream commit `7fb328f` (`fix(host): resume Codex sessions through controller lease`) | Passed after changing downstream resume from unavailable to `controller/acquireControl` → exact-thread `thread/resume` → `controller/releaseControl`, and after updating the downstream smoke source to exercise `HostSessionBridge.handleTap`. This proves the downstream host requests resume through the approved controller lease and releases control afterward; it is not yet live native smoke evidence against running Codex TUIs. | Focused `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost --filter 'ExternalControllerRegistryTests/testResumeAcquiresControlResumesExactThreadAndReleasesControl\|ExternalControllerRegistryTests/testFailedResumeStillReleasesControl\|ExternalControllerRegistryTests/testClosedLaunchCannotResumeOrReconnectUntilNextRefresh'` passed 3/3; full `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed 60/60; `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed; downstream `pre-commit run --files ...` passed. |
| Downstream commit `d43fcfb` (`fix(host): watch Codex launches and route taps`) | Passed after adding a retained OS file-system watch on `$CODEX_HOME/local-controllers`, routing discovery-change notifications through `HostSessionBridge.refreshInventory()` for full rescans, and adding a preparatory V7 `.slotTap` → `HostSessionBridge.handleTap` source path. This proves the downstream source path no longer depends only on the timer/poll path or a manual smoke call; the deployed V7 product remains status-only, so this is not physical input-runtime evidence. | Focused `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost --filter 'LocalControllerDiscoveryTests/testSnapshotFiltersDeadProcessMetadata\|LocalControllerDiscoveryTests/testWatchReportsDirectoryChangesForFullRescanTrigger\|OperationalStateTests/testDiscoveryChangeTriggersFullInventoryRefresh\|ExternalControllerRegistryTests/testResumeAcquiresControlResumesExactThreadAndReleasesControl'` passed 4/4; full `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed 62/62; `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed; downstream `pre-commit run --files ...` passed. Afterward, generated build products were removed by request, so the downstream smoke binary must be rebuilt before live rerun. |
| `just test -p codex-app-server-transport local_controller_acceptor_prunes_dead_launch_artifacts local_controller_stale_pruning_tolerates_concurrent_cleanup local_controller_acceptor_publishes_metadata_and_forwards_websocket_messages_with_nonce local_controller_acceptor_republishes_metadata_with_main_thread_id` | Passed: 4 test runs, 4 passed, 159 skipped. This covers stale local-controller startup pruning, concurrent cleanup tolerance, live-record preservation, socket/metadata publication, WebSocket forwarding, and immutable `mainThreadId` republication. | Compile reported 6.27s; nextest reported 0.068s. Nextest run `ddc4b599-caf2-465f-9b41-efceec19c5aa`. |
| `just test -p codex-app-server thread_resume_loaded_unmaterialized_paginated_thread_returns_live_snapshot thread_resume_rejects_unmaterialized_unloaded_thread local_controller_socket_uses_main_thread_interface_and_tui_reclaim controller_thread_resume_allows_read_shape_params_only thread_resume_extracts_exact_controller_thread_target` | Passed: 5 test runs, 5 passed, 1256 skipped. This covers fresh loaded paginated TUI `thread/resume` before rollout materialization, missing unloaded-thread rejection, native local-controller socket parity/TUI reclaim, and controller `thread/resume` admission/target extraction. | Compile reported 1.03s; nextest reported 8.320s with the known `__eh_frame` linker warning. Nextest run `6bbc984c-4e74-4b70-8e20-d89db8f21705`. |
| `cargo build -p codex-cli --bin codex` | Passed and rebuilt `codex-rs/target/debug/codex` after the stale-cleanup and loaded-unmaterialized resume follow-up. | Cargo reported 17.03s with the known `__eh_frame section too large` linker warning and `proc-macro-error2` future-incompatibility warning. |
| Downstream commit `df843d4` (`fix(host): pump run loop during controller smoke tap`) | Passed after replacing the smoke's main-thread `DispatchSemaphore.wait` tap wait with a `RunLoop.main.run` wait. This fixes the harness deadlock where `HostSessionBridge` posted tap completion back to main but the smoke blocked main before the completion could run. | `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed in 3.62s with existing unrelated Swift warnings; `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed 62/62 after an 8.10s test build and 2.55s test run; `pre-commit run --files host/FirstVerticalSliceHost/Sources/FirstVerticalSliceExternalControllerSmoke/main.swift` passed. |
| `/Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost/.build/arm64-apple-macosx/debug/first-vertical-slice-external-controller-smoke --application-support /private/tmp/codex-extctl-smoke-fixed.XaEmW5` | Passed after the owning TUIs approved two native `codex-waveshare` participation prompts in temporary Herdr workspace `w1A`: `external-controller smoke: pass (launches: 2, exact launch-scoped route persisted, resume requested and control released)`. This is live mutating evidence for downstream discovery, native participation, exact launch-scoped assignment persistence, controller acquire, exact-thread `thread/resume`, and controller release against the rebuilt debug Codex binary. | Herdr reported the passing run at 15s. |
| Downstream commit `7424849` (`test(host): support all-discovered controller smoke`) | Passed after making the downstream smoke capable of selecting every discovered Codex launch with `--all-discovered --expected-launches N` and accepting equivalent `/tmp` and `/private/tmp` local-controller socket paths by resolving symlinks before validation. | Focused `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost --filter LocalControllerDiscoveryTests` passed 3/3; full `swift test --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed 63/63; `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed in 0.16s; downstream `pre-commit run --files ...` passed. |
| Codex commit `4972b62` (`fix(app-server): reject stale controller approvals after disconnect`) | Passed after fixing a native approval/disconnect race found by five-launch live validation. If an external-controller websocket disconnects while `controller/requestParticipation` is still pending in the TUI, the late approval now returns typed `transport-closing` and cannot create a stale controller owner. A fresh controller connection can request participation and acquire control normally. | Focused `just test -p codex-app-server request_processors::controller_processor` passed 3/3; `just test -p codex-app-server local_controller_socket_sessions_are_isolated_per_launch` passed 1/1; `git diff --check` passed; `pre-commit run --files ...` could not run because this checkout has no `.pre-commit-config.yaml`; `cargo build -p codex-cli --bin codex` rebuilt `codex-rs/target/debug/codex` in 24.34s with the known `__eh_frame` linker warning. The attempted full `just test -p codex-app-server` run completed with 1257 passed, 2 flaky passed on retry, 4 failed, and 1 skipped; failures were outside the controller path: two remote-thread-store deadline tests and two zsh-fork/dotslash tests. |
| `/Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost/.build/arm64-apple-macosx/debug/first-vertical-slice-external-controller-smoke --application-support /tmp/cdx5.kjpUTg/app-support --all-discovered --expected-launches 5` | Passed against five fresh plain `codex --no-alt-screen` launches in temporary Herdr workspace `w1D` after each owning TUI approved the native `codex-waveshare` prompt, using downstream commit `7424849`. This validates five live launch metadata discovery, five `mainThreadId` publications, native approval, aggregate inventory, exact launch-scoped assignment persistence, controller acquire, exact-thread `thread/resume`, and controller release using the rebuilt debug Codex binary. | Downstream `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` rebuilt the smoke in 0.16s. Herdr reported the passing smoke run at 18s: `external-controller smoke: pass (launches: 5, exact launch-scoped route persisted, resume requested and control released)`. |
| Downstream commit `bf445d8` (`test(host): verify removed controller launch reconciliation`) | Passed after adding `--verify-removal` smoke mode, which rescans live metadata for the selected launches, waits for the selected launch to disappear, verifies its persisted route becomes unavailable and rejects without resume, then verifies a surviving route remains resumable. | `swift build --package-path /Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost` passed in 1.46s with existing unrelated SwiftUI warnings; focused `LocalControllerDiscoveryTests` passed 3/3; full downstream `swift test` passed 63/63; downstream `pre-commit run --files ...` passed. |
| `/Users/roschuma/Personal/codex-waveshare/host/FirstVerticalSliceHost/.build/arm64-apple-macosx/debug/first-vertical-slice-external-controller-smoke --application-support /tmp/cdxrm.sJJeLg/app-support-removal2 --all-discovered --expected-launches 2 --verify-removal` | Passed in temporary Herdr workspace `w1E` with two fresh plain debug Codex TUIs. Both owning TUIs approved native participation; after closing the selected launch pane `w1E:p2` (launch `019fea0b-c9eb-7de1-9a99-f6caf0eb63f9`), the smoke verified that its route was unavailable with no resume and that the survivor resumed. | Herdr reported `external-controller smoke: pass (launches: 2, exact launch-scoped route persisted, resume requested and control released, removed launch reconciled and survivor resumed)` at 32s. |

Implementation audit note: checkpoint `809b9e1` added the formal app-server
`threadSequence` / `lastSequence` protocol surface and runtime sequence
assignment. The in-process TUI sequence-preservation slice carries
sequenced server requests and notifications through `codex-app-server-client`
to the TUI, seeds lag recovery from `ThreadReadResponse.lastSequence`, and
drops stale sequenced events at or below the refreshed snapshot sequence. The
in-process recovery-snapshot slice closes the remaining recovery gap for
embedded TUI sessions: the app-server now builds an internal snapshot containing
the normal thread view, authoritative sequence, current interactive ownership
status/epoch, and pending prompt requests for TUI replay. Remote/daemon TUI
sessions retain the public `thread/read` fallback.

Availability reconciliation note: the current source satisfies Commit 18's
Codex-side launch-mode scope. Embedded TUI launches request the local-controller
endpoint by default and publish `$CODEX_HOME/local-controllers` metadata when
the endpoint is available. Policy-disabled, best-effort unavailable,
policy-required launch failure, daemon-backed, and explicit remote launch modes
produce the documented availability states. The remaining discovery,
presentation, and auto-assignment work is downstream controller-host product
work against the published metadata contract.

End-to-end controller note: the current source satisfies Commit 19's Codex-side
embedded-runtime scenario. Approved controller participation now subscribes the
controller connection to the granted main-thread listener immediately when the
session advertises `subscribeMainThread`, so a controller that connects after
the TUI main thread already exists receives the same listener-ordered
notifications for its main-thread actions as the TUI-facing app-server
interface. The focused socket e2e validates normal app-server parity,
controller-resolved command approval, command and turn completion visibility,
TUI reclaim, stale controller mutation rejection, standing read access after
reclaim, controller reacquire, final thread state, and sign-off.

Downstream smoke note: the previous downstream live smoke runs passed bounded
two-launch discovery, native participation, aggregate inventory, and isolated
assignment persistence against live local Codex metadata. Those earlier runs
printed `no Codex mutation requested`, so they remain inventory/persistence
evidence only. Downstream commit `7fb328f` updates the current smoke source and
rebuilt host binary to request resume through the controller lease. The first
live mutating smoke run reached native approval in two owning TUIs but returned
generic `resume through controller unavailable`; direct Codex diagnostics
against fresh approved sockets completed acquire, exact-thread `thread/resume`,
release, and sign-off successfully. The confirmed downstream root cause was the
smoke blocking the main thread on a semaphore while `HostSessionBridge` posts
tap completion to the main run loop. Downstream commit `df843d4` replaces that
wait with a run-loop wait, and the actual mutating smoke now passes against two
live debug Codex TUIs after native approval.

Downstream discovery note: downstream commit `508880c` now decodes
`processId` from Codex local-controller metadata and filters records whose
process is no longer live. This closes the stale metadata/process-liveness
portion of the external discovery contract for the current downstream host.
Downstream commit `d43fcfb` adds the product-side watch/full-rescan trigger:
the host watches the metadata directory with the OS file-watch facility, treats
events as hints, and performs full inventory refreshes through the same bridge
path. Runtime evidence with five live launches now exists from temporary Herdr
workspace `w1D`: five fresh plain `codex --no-alt-screen` launches published
metadata with `mainThreadId`, were approved through their owning TUI prompts,
and passed the downstream all-discovered mutating smoke with five launch-scoped
routes.

Herdr validation note: a temporary Herdr workspace can drive the same native
TUI participation UI a user sees. Herdr validation covers the earlier two-launch
inventory/persistence smoke, five-launch mutating smoke, and two-launch
removed-launch reconciliation. The repeatable procedure launches plain
`codex --no-alt-screen` TUI panes, waits for `$CODEX_HOME/local-controllers`
metadata with non-null `mainThreadId`, runs the downstream all-discovered
smoke, and sends Enter to approve each owning TUI prompt. The removal mode then
closes the selected TUI after approval and verifies its persisted route rejects
while another route remains resumable.

Codex cleanup/resume follow-up note: controller-enabled startup now prunes
stale local-controller metadata/socket artifacts for definitely dead
`processId` values, while preserving live or ambiguous records and tolerating
concurrent cleanup `NotFound` races. Running `thread/resume` now returns the
live loaded main-thread snapshot for a fresh paginated TUI thread whose rollout
storage has not yet materialized. Focused validation passed with `just fmt`,
`just test -p codex-app-server-transport local_controller_acceptor_prunes_dead_launch_artifacts local_controller_stale_pruning_tolerates_concurrent_cleanup local_controller_acceptor_publishes_metadata_and_forwards_websocket_messages_with_nonce local_controller_acceptor_republishes_metadata_with_main_thread_id`
(4/4; nextest run `ddc4b599-caf2-465f-9b41-efceec19c5aa`),
`just test -p codex-app-server thread_resume_loaded_unmaterialized_paginated_thread_returns_live_snapshot thread_resume_rejects_unmaterialized_unloaded_thread local_controller_socket_uses_main_thread_interface_and_tui_reclaim controller_thread_resume_allows_read_shape_params_only thread_resume_extracts_exact_controller_thread_target`
(5/5; nextest run `6bbc984c-4e74-4b70-8e20-d89db8f21705`),
`just fix -p codex-app-server-transport`, `just fix -p codex-app-server`
after reverting unrelated fixer hunks, and
`cargo build -p codex-cli --bin codex` rebuilding
`codex-rs/target/debug/codex` in 17.03s.

Codex stale-approval follow-up note: live five-launch validation exposed a race
where a websocket disconnect during pending native participation could be
processed before the controller session existed; a later TUI approval then
granted ownership to the already-disconnected connection. Checkpoint `4972b62`
records closed external-controller connection IDs and rejects late
participation grants with the existing typed `transport-closing` controller
error. Focused controller validation and the five-launch downstream smoke both
pass after rebuilding the debug Codex binary.

Downstream presentation note: downstream commit `5a963b4` now keeps product slot
assignment separate from launch authorization by preserving existing assignments
and deterministically filling free slots for newly discovered sessions. It also
maps live non-connected launch states such as `awaitingApproval`,
`approvalUnavailable`, `discovered`, and `connectionUnavailable` to non-offline
physical slot states rather than `unavailable`. Five-launch live discovery
and mutating resume evidence now exists for five approved debug Codex TUIs in
temporary Herdr workspace `w1D`; removed-launch reconciliation now also has
live native evidence from workspace `w1E`. The current downstream V7 controller
is status-only, so physical-device tap evidence is outside this implementation
scope rather than a remaining acceptance gate.

Downstream input note: downstream commit `d43fcfb` includes a preparatory
physical V7 `.slotTap` → `HostSessionBridge.handleTap` source path. It does not
make the current status-only V7 device an input controller. Enable and validate
that product capability separately before treating a physical tap as controller
input or advertising acquire/resume/release behavior from V7.

## Relevant implementation seams

- `codex-rs/app-server/src/in_process.rs` — typed in-process client and runtime bootstrap.
- `codex-rs/app-server/src/message_processor.rs` — request dispatch and per-connection initialization.
- `codex-rs/app-server/src/request_serialization.rs` — global and thread-scoped request ordering.
- `codex-rs/app-server/src/transport.rs` — outgoing routing by connection.
- `codex-rs/app-server-transport/src/transport/` — Unix-socket WebSocket acceptor and bounded transport ingress.
- `codex-rs/app-server-transport/src/transport/remote_control/` — existing remote-control transport, intentionally separate from this design.
- `codex-rs/tui/src/lib.rs` — normal TUI app-server startup and shutdown lifecycle.
