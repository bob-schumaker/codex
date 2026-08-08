# Active Context

- Current focus: finish the current-time/direct server-request owner-routing
  parity gap for external controllers, while downstream controller
  discovery/display consumes the published local-controller metadata contract.

## Current Status

- Done:
  - Added controller-side discovery and presentation requirements to
    `docs/external-controllers.md`.
  - Corrected the endpoint directory in the spec to
    `$CODEX_HOME/local-controllers`.
  - Added downstream discovery/health/auto-assignment slices to
    `docs/external-controllers-implementation-plan.md`.
  - Replaced stale durable-enrollment implementation-plan language with native,
    connection-bound participation grants.
  - Added repository-local memory-bank guidance through `AGENTS.local.md`.
  - Initialized `memory-bank/`.
  - Converted the in-process local-controller socket parity test to use the
    native TUI approval path instead of credential-enrollment test plumbing.
  - Updated stale controller-enrollment module commentary so it no longer claims
    the local endpoint/RPC path is unwired.
  - Validated the focused app-server local-controller tests with
    `just test -p codex-app-server local_controller`.
  - Expanded external-controller exact-thread target extraction so admitted
    normal-interface methods can reach their handlers instead of failing at the
    pre-dispatch gate.
  - Changed primary input reclaim so thread-scoped TUI-only mutations also
    cancel an active controller lease.
  - Validated the app-server controller gate with
    `just test -p codex-app-server controller`.
  - Built the `test_stdio_server` helper and reran
    `just test -p codex-app-server`; the full run reached 1172/1174 passing
    with two zsh-fork timeout failures that both passed when rerun
    individually.
  - Aligned controller collection-filter admission with dispatch so
    `thread/list`, `thread/search`, `thread/loaded/list`, and
    `threadSection/list` all pass target extraction and are scoped to the
    immutable main thread.
  - Validated the controller slice with
    `just test -p codex-app-server controller` passing 41/41 tests.
  - Reran `just test -p codex-app-server`; the local-controller and collection
    filtering coverage passed, while the run ended 1171/1175 passing with the
    current zsh-fork timeout cluster failing. Sampled zsh-fork tests also failed
    in isolation, so that fixture remains unhealthy independently of this
    controller change.
  - Implemented ordered `controller/signOff` transport teardown: successful
    sign-off now waits for the final response to queue and write, then
    disconnects the controller socket.
  - Validated sign-off teardown with focused app-server tests:
    `just test -p codex-app-server to_connection_then_disconnect_waits_for_final_write`,
    `just test -p codex-app-server local_controller_socket_uses_main_thread_interface_and_tui_reclaim`,
    and
    `just test -p codex-app-server controller_control_plane_round_trips_after_enrollment`.
  - Added backpressure coverage so a full outbound writer queue does not drop
    the final sign-off response before disconnecting.
  - Added sign-off ingress fencing: after successful `controller/signOff`
    starts final-response teardown, new external-controller requests on that
    connection receive typed `transport-closing` instead of re-entering normal
    admission while the socket is closing.
  - Validated the sign-off ingress fence with
    `just test -p codex-app-server controller_control_plane_round_trips_after_enrollment`.
  - Restricted external-controller `thread/resume` to the authorized main-thread
    rejoin/read-hydration shape. History, path, configuration, instruction,
    approval, sandbox, permission, and personality overrides now fail with typed
    `controller-not-allowed`.
  - Validated the controller resume override gate with
    `just test -p codex-app-server controller_thread_resume_allows_read_shape_params_only`
    and
    `just test -p codex-app-server controller_control_plane_round_trips_after_enrollment`.
  - Tightened `controller/signOff` so it requires an existing standing
    controller session before revocation.
  - After successful `controller/signOff`, the external-controller connection
    is immediately unsubscribed from the TUI main thread before the final
    response/disconnect teardown continues.
  - Validated the sign-off authorization and subscription cleanup with
    `just test -p codex-app-server controller_control_plane_round_trips_after_enrollment`
    and
    `just test -p codex-app-server controller_participation_rejects_unproven_display_claims`.
  - Added controller-bound exact-thread pagination cursors for controller
    `thread/resume` cursor fields, `thread/turns/list`, `thread/items/list`,
    `thread/searchOccurrences`, and `thread/backgroundTerminals/list`; replay
    now requires the same controller connection and authorized main thread.
  - Fixed `controller/signOff` origin checking so non-controller origins still
    receive the external-controller-origin error before main-thread admission.
  - Validated the cursor-binding slice with
    `just test -p codex-app-server controller_cursor`,
    `just test -p codex-app-server controller_control_plane_round_trips_after_enrollment`,
    and `just test -p codex-app-server controller` passing 46/46 tests.
  - Cleaned up controller main-thread subscriptions when a live controller
    loses standing authorization on expiry; the normal-interface and
    acquire/release paths preserve typed `authorization-expired` while removing
    the stale external-controller subscription from the TUI main thread.
  - Validated the authorization-expiry cleanup with
    `just test -p codex-app-server controller_authorization_expiry_removes_main_thread_subscription`
    and `just test -p codex-app-server controller` passing 47/47 tests.
  - Made `controller/acquireControl` idempotent for the controller that already
    holds the active lease, matching its advertised `acquireControl`
    capability without weakening the single-owner conflict rule for other
    controllers.
  - Validated the idempotent-acquire slice with
    `just test -p codex-app-server ownership_lifecycle_preserves_standing_authorization controller_control_plane_round_trips_after_enrollment`
    and `just test -p codex-app-server controller` passing 47/47 tests.
  - Bound controller prompt/server-request replies to the owner epoch captured
    when the prompt is delivered to the external controller, so a stale reply
    from the same controller after release/reacquire no longer resolves the
    old prompt under a new lease.
  - Validated the prompt owner-epoch binding with
    `just test -p codex-app-server controller_prompt_response_is_bound_to_owner_epoch controller_control_plane_round_trips_after_enrollment`
    and `just test -p codex-app-server controller` passing 48/48 tests.
- In progress:
  - Next Codex-side parity slice is selected: `currentTime/read` is admitted as
    `ExactThread + ActiveOwner`, but `current_time.rs` still sends direct server
    requests to subscribed connections without using the controller
    owner-aware recipient path or recording the owner epoch on the pending
    server request.
  - Downstream controller-host implementation for file-watch discovery, health
    model separation, and deterministic auto-assignment.
- Not started:
  - Downstream acceptance rerun across multiple live Codex launches after the
    controller host consumes the metadata-directory discovery contract.

## Next Steps

- Route `currentTime/read` through the controller owner-aware recipient path so
  an active external controller receives the same direct server request the TUI
  would receive, with owner-epoch binding on the pending reply.
- Keep tightening app-server normal-interface parity around any remaining
  implicit targets, egress transactionality, and subscription edges beyond the
  committed cursor-binding, sign-off cleanup, authorization-expiry cleanup,
  idempotent acquire, prompt owner-epoch binding, and resume-override gates.
- Implement downstream discovery as metadata-directory watch plus full rescan.
- Fix downstream status mapping so pending/unapproved/released live launches do
  not display as offline.
- Rerun the downstream multi-launch controller smoke once the downstream host
  work is in place.
