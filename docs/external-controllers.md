# External controllers for the interactive TUI

## Status

Internal design specification. This document describes the intended architecture; it does not describe an available CLI or app-server API.

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
- Reusing or changing the existing app-server remote-control service. Local external controllers are a separate transport origin and never enroll in, publish to, or inherit lifecycle from remote control.

## Scope

The first delivery applies when the TUI selects an embedded app-server runtime. It does not create a second endpoint when the TUI attaches to an existing local daemon or an explicit remote app server; those modes require a separate controller-session registration design because the TUI does not own that runtime or its listener lifecycle.

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

The listener is created for each controller-enabled embedded interactive TUI launch. Its lifetime is owned by the TUI process and runtime: acceptor failure updates the availability state, and an abnormal process exit relies on operating-system socket cleanup rather than promising orderly metadata removal.

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

### Controller-side launch discovery and presentation

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

The authorization gate applies that normal interface to the TUI main thread. A controller may use normal reads and subscriptions for that thread and receives the same data the TUI would receive. `thread/list` is filtered to the main thread, and a different target has the normal non-enumerating not-found behavior. Cursors, subscriptions, resume tokens, and implicit targets are bound server-side to the main thread and `ConnectionId`; any continuation resolving elsewhere is denied. It may perform every existing thread-scoped operation the TUI connection may perform for the main thread while it owns it, and it receives that thread's approvals and user-input requests. Process-wide and non-thread-scoped server requests remain TUI-only. Thread-affecting TUI operations are always primary: when one targets the controller-owned main thread, admission first invalidates that controller's active input lease and transfers the thread to `TuiOwned`, then admits the TUI input. Reclaiming input means a TUI-originated thread-affecting operation: submit/resume/cancel/interrupt, approval or user-input response, slash command that mutates the main thread, or composer text accepted as a pending user turn. Pure display actions such as scrolling, focus changes, pane navigation, and draft text editing without submission do not reclaim control. The controller remains connected with standing authorization and can later call `controller/acquireControl`; it cannot send thread mutations or resolve prompts until then.

### Control ownership

Approval of participation grants a connection-bound control-switch authorization and, only when the main thread is TUI-owned and viable, an initial handoff. A lease is bound to one live `ConnectionId`, the TUI main thread ID, an unguessable lease ID, a server-owned monotonic expiry deadline, and owner epoch. It is non-transferable and non-resumable: reconnecting has no session authority and must request participation again.

- An `interactive-control` lease transfers full app-server input ownership of the granted main thread. Participation approval grants this lease only when the coordinator can perform the initial handoff, and it is the only lease that permits a controller to answer approvals or user-input requests.
- The controller may relinquish its active `interactive-control` lease with `controller/releaseControl`. If the connection still has a standing controller session but no active lease because ownership was already released, expired, or reclaimed by thread-affecting TUI input, `controller/releaseControl` is idempotent and returns the current `ControllerSession` with `activeLease: null`. If the active lease is still held by the caller, the thread coordinator invalidates it, rejects queued mutations, transfers the main thread back to `TuiOwned`, and rebinds prompts before reporting success. The controller session, read/subscription access, and standing control-switch authorization remain live.
- A connected, authorized controller may later call `controller/acquireControl` without TUI interaction. The thread coordinator validates that the main thread is live, `TuiOwned`, and TUI-viable, then issues a fresh lease and transfers ownership. Otherwise it returns an ownership-conflict or stale-authorization error; it never displaces another controller. TUI revocation, controller sign-off/disconnect, or session expiry destroys this standing authorization, so reacquisition then requires a new grant.
- Thread-affecting TUI input is an automatic per-thread reclaim: it cancels the active controller input lease but preserves the connection's standing authorization and emits `controller/controlOwnershipChanged`. An explicit TUI policy revocation destroys the standing authorization and main-thread read access. A controller can never grant or extend its own authorization.

Every controller mutation is authorized against `(thread ID, connection ID, lease ID, owner epoch)` at admission and at the per-thread coordinator's execution fence. Thread-affecting TUI input, release, or revocation advances the owner epoch and prevents any further controller irreversible step from starting. A step already holding the fence finishes; the triggering TUI input is queued immediately behind it and runs next. Queued controller work receives exactly one stale-ownership result.

The controller uses the existing app-server mutation RPC shapes rather than a parallel control protocol. The generated v2 registry must enumerate every method, server-request response, implicit target, cursor, resume token, and subscription on two independent axes:

- target extraction: `none`, `mainThreadOnly`, `exactThread`, or `collectionFiltered`; and
- required authority: `preParticipation`, `standingSession`, `activeOwner`, or `tuiOnly`.

The first axis identifies which thread, if any, the handler may touch. The second axis identifies which connection state is required before dispatch. Examples: `controller/requestParticipation` is `none` + `preParticipation`; `thread/list` is `collectionFiltered` + `standingSession`; a main-thread read is `exactThread` + `standingSession`; a main-thread mutation or prompt response is `exactThread` + `activeOwner`; account/authentication and process-wide requests are `none` + `tuiOnly`. Cursors, resume tokens, subscriptions, and implicit targets must carry enough server-side binding to re-run both checks on continuation. New methods default to denied until registered on both axes. The priority-aware admission scheduler routes mutations to the current `InteractiveOwner`; it does not allow the non-owner to compete with the owner.

Approval decisions are responses to server-to-client requests, not new client RPCs. Their existing `accept`, `acceptForSession`, and `cancel` decisions remain unchanged for the TUI. A controller-owned prompt is delivered with the same server-request shape as the TUI receives, but this first delivery rejects an `acceptForSession` decision from a controller-owned prompt with a typed controller-not-allowed error, because its persistent effect would outlive the connection-bound lease; controller-owned prompts may use only `accept` or `cancel`. The runtime authorizes each response by its original outbound request ID, recipient `ConnectionId`, and interactive-owner epoch before accepting it.

Controller input and mutation requests are eligible only while the controller is the current `InteractiveOwner`. Thread-affecting TUI input is always eligible and atomically reclaims an owned thread before it is scheduled, so it wins any acquire-versus-input race at the coordinator's linearization point. Display-only TUI interactions do not enter this scheduler and do not reclaim ownership. Priority is applied at dequeue boundaries only for concurrently admissible non-interactive work, such as reads and subscriptions. Those requests are FIFO within the TUI and controller classes. TUI work wins the next dequeue when both classes are eligible, but after eight consecutive eligible TUI dequeues the coordinator must run one valid controller request. This bound does not override TUI input reclamation, ownership, revocation, expiry, or a serialization lock held by in-flight work.

Every approval or input request is atomically bound at creation to one owner connection and ownership epoch. Inbound responses, revocation/disconnect, and redelivery are all serialized through the per-thread coordinator: exactly one wins, and the loser receives a deterministic stale-ownership error. A new binding cannot be delivered until the old binding is invalidated.

Interactive handoff enters `TransferPending` behind an atomic transition barrier. At one per-thread sequence point it invalidates the old prompt binding, sets the new owner/epoch, commits local ownership, admits a pending TUI input if applicable, then enqueues prompt redelivery and publishes the ownership/state bundle in that order. The TUI reclaim never waits for a UI egress queue; if its local reducer cannot accept the bundle, the runtime enters terminal `TuiUnavailable` and closes the launch. Prompts have coordinator-owned `pending`, `resolved`, or `cancelled` state and are rechecked at delivery dequeue; only `pending` prompts are redelivered. A controller-bound prompt crosses the `externalDelivery` boundary only after the egress item passes immediate pre-write revalidation for connection, owner epoch, and prompt binding and the WebSocket writer successfully writes the frame. Enqueue success only reserves delivery capacity. If enqueue fails, pre-write revalidation fails, or the socket write fails before `externalDelivery` is recorded, the coordinator records a terminal transport result for that controller path, runs the disconnect or stale-egress handling required by the failure, and rebinds the still-pending prompt to the TUI before any later resolver can act on it. If ownership changes after `externalDelivery`, the prompt is not redelivered merely because of that later revocation; any later controller reply is accepted or rejected by the original request ID, recipient `ConnectionId`, prompt binding, and interactive-owner epoch. Committed controller progress/results remain in the canonical `threadSequence`; only stale prompt/control deliveries and replies are discarded. Terminal revocation cancels subscriptions/cursors and fences queued controller egress before emitting its change notification. The TUI event stream is lossless to its reserved queue; overflow marks the thread desynchronized and obtains an atomic snapshot-at-sequence containing thread state, `InteractiveOwner`, owner epoch, prompt bindings, and `lastSequence`. The reducer replaces state at that sequence, drops buffered events at or below it, applies later events exactly once, and enters terminal `TuiUnavailable` if recovery fails. Normal TUI shutdown closes the runtime.

An approved controller may call experimental `controller/signOff` to relinquish its controller session. Sign-off and unexpected socket disconnect use the same revocation path: invalidate every connection-bound lease, reject queued controller work, restore prompt ownership to the TUI, and emit a typed in-process TUI ownership-status event containing main thread ID, owner, owner epoch, and reason. For sign-off, the connection teardown barrier rejects new ingress, lets already-admitted requests receive their normal response or one `transport-closing` result, fences controller egress, flushes only the sign-off response, then closes the socket. Any later operation requires a new connection and enrollment.

### Experimental protocol surface

The new public controller methods use the v2 singular-resource naming convention and are all gated by `initialize.capabilities.experimentalApi`:

- `controller/requestParticipation` takes `ControllerRequestParticipationParams { controllerName, description }` and returns `ControllerRequestParticipationResponse { status, session }`, where `status` is `approved` or `rejected` and `session` is a required nullable `ControllerSession`. `ControllerSession` contains session ID, main thread ID, explicitly nullable `activeLease`, authorization epoch, `effectiveCapabilities`, and advisory lease/session expiry durations. Rejection has `session: null` and typed denial data. `main-thread-unavailable` is not a rejection status; it is a typed retryable error until the launch reaches a terminal no-main-thread state.
- `controller/authorizationChanged` and `controller/controlOwnershipChanged` each include session ID, main thread ID, reason enum, authorization/owner epochs, and a monotonically increasing controller-session sequence. They are controller control-plane notifications, never TUI state inputs. The TUI receives the corresponding typed in-process ownership-status event in `threadSequence` order. Terminal revocation fences queued events before its notification.
- `controller/acquireControl` and `controller/releaseControl` take no payload and return the updated `ControllerSession`. Acquire returns only after its ownership transition completes. Release returns after its ownership transition completes, or immediately with `activeLease: null` when the live session already has no active lease. `controller/signOff` returns only after terminal revocation completes; its response is exempted from teardown.
- Canonical experimental errors are: `experimental-not-enabled`, `participation-required`, `enrollment-denied`, `main-thread-unavailable`, `main-thread-closed`, `tui-unavailable`, `ownership-conflict`, `stale-ownership`, `controller-not-allowed`, `transport-closing`, `different-thread-target`, `authorization-expired`, `lease-expired`, and `controller-overloaded`. Each error includes typed data sufficient to decide whether retry on the same connection is allowed.

Native TUI approval, policy revocation, and single-thread assignment are controller-authorization mechanisms, not public app-server RPCs. The public v2 types use `*Params`/`*Response`/`*Notification`, camelCase serde and TypeScript names, `#[ts(export_to = "v2/")]`, and the normal experimental annotations/schema generation workflow.

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
- A controller receives the normal approval server-request shape, but an `acceptForSession` reply is rejected with a typed controller-not-allowed error; controller approval effects cannot survive the associated lease.
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
- Controller prompt `externalDelivery` is recorded only after pre-write validation succeeds and the WebSocket writer successfully writes the frame. Enqueue failure, stale pre-write validation, or write failure before `externalDelivery` records a terminal controller-path result and rebinds the still-pending prompt to the TUI before any controller resolver can act. Revocation after `externalDelivery` does not redeliver the prompt; the later resolver path accepts or rejects by request ID, recipient connection, prompt binding, and owner epoch.
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
Codex-side hardening has continued through commit `55a86e3`
(`test(app-server): cover native controller rejection over socket`). The recorded
implementation goal cost at the broad checkpoint was 7,828,188 tokens and
44,738 seconds (approximately 12h 25m 38s). At the local-socket rejection
validation slice, the cumulative goal cost was 16,068,831 tokens and 48,479
seconds (approximately 13h 27m 59s). These costs include implementation, review,
validation, and commit preparation across the staged slices; they are not limited
to build/test subprocess runtime.

The repository `docs/` tree is plain authored Markdown for this spec. No
`docs/Makefile`, Sphinx `conf.py`, or docs index file was present, so there was
no repository docs build target to run for this page.

Final build and validation evidence:

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

## Relevant implementation seams

- `codex-rs/app-server/src/in_process.rs` — typed in-process client and runtime bootstrap.
- `codex-rs/app-server/src/message_processor.rs` — request dispatch and per-connection initialization.
- `codex-rs/app-server/src/request_serialization.rs` — global and thread-scoped request ordering.
- `codex-rs/app-server/src/transport.rs` — outgoing routing by connection.
- `codex-rs/app-server-transport/src/transport/` — Unix-socket WebSocket acceptor and bounded transport ingress.
- `codex-rs/app-server-transport/src/transport/remote_control/` — existing remote-control transport, intentionally separate from this design.
- `codex-rs/tui/src/lib.rs` — normal TUI app-server startup and shutdown lifecycle.
