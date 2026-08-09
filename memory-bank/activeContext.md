# Active Context

- Current focus: continue external-controller normal-interface parity after
  binding collection-filtered controller cursors, gating controller-origin turn
  input shapes, binding internally-sent controller `thread/resume` response
  cursors, making native TUI-unavailable launch state terminal, and fencing
  terminal TUI-unavailable main-thread egress, and rebinding controller-owned
  prompts plus subscriptions before disconnect RPC drain, plus publishing
  controller authorization/control-ownership notifications from the session
  transition boundary, and prioritizing queued TUI thread input over queued
  controller work with dequeue-time reclaim, and filtering automatic listener
  attachment so external controllers only auto-subscribe to their authorized
  main thread, and removing controller main-thread subscriptions before
  terminal sign-off/disconnect revocation events, and filtering generic
  broadcasts away from external controllers while explicitly targeting
  authorized main-thread lifecycle notifications and status-change
  notifications, and preserving global primary thread notifications while
  targeting external-controller copies, including listener-command thread goal
  updates, resume snapshots, listener warning notifications, and extension
  no-listener goal-update fallback, plus app-server `thread/goal` update, clear,
  and snapshot fallbacks, plus thread-scoped MCP OAuth completion
  notifications, plus fencing controller-origin detached reviews so
  `review/start` stays on the authorized main thread, plus gating
  controller-origin realtime startup context/configuration overrides and
  realtime text role injection, plus fencing controller-origin section moves
  from ordering the main thread relative to another thread, plus focused TUI
  reclaim coverage for Guardian-denied approvals, plus making TUI command
  reclaim classification exhaustive, plus fencing
  controller-origin archive/delete from spawned-descendant subtree targets, plus
  skipping running-thread resume replay for controller prompts after external
  delivery, plus routing app-server extension fallback goal/warning egress
  through controller-aware recipient computation, plus routing listener-ordered
  `serverRequest/resolved` egress through the controller-aware thread sender,
  plus preserving embedded in-process transcript/item delivery before the
  client-side lossless bridge sees reflected controller work, plus centralizing
  the embedded app-server/app-server-client lossless delivery classifier, plus
  closing established external controllers and removing discovery metadata when
  the local-controller acceptor hits a terminal failure, plus reporting late
  local-controller endpoint failure to the TUI as `embedded-unavailable`, plus
  bounding initialized external-controller ingress per connection and returning
  typed `controller-overloaded` responses before queued app-server work can grow
  without limit, plus marking the controller launch terminal when the immutable
  main thread is closed or unloaded, plus preserving controller-relevant thread
  lifecycle/state notifications across the in-process TUI delivery bridge under
  backpressure, plus preserving reasoning summary section boundaries across the
  same lossless bridge, plus reflecting realtime transcript notifications in
  normal TUI history paths, plus surfacing typed controller prompt-reply
  rejections back to the external-controller connection without resolving the
  pending prompt, plus adding protocol/export coverage for controller
  notification schemas and all canonical controller error-code wire names, plus
  TUI app-server lag snapshot recovery;
  remaining Codex-side review is centered on any other implicit
  targets, egress transactionality, and subscription edges while downstream
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
  - Routed `currentTime/read` through the controller owner-aware recipient path
    so an active external controller receives the same current-time server
    request the TUI would receive, with the pending reply bound to the
    interactive owner epoch.
  - Validated the current-time owner-routing slice with
    `just test -p codex-app-server controller_current_time_request_is_bound_to_owner_epoch`,
    `just test -p codex-app-server current_time`, and
    `just test -p codex-app-server controller` passing 49/49 controller tests.
  - Rebuilt the debug CLI binary with `cargo build -p codex-cli`.
  - Bound collection-filtered controller pagination cursors for
    `thread/list`, `thread/search`, `thread/loaded/list`, and
    `threadSection/list`; replay now requires the same controller connection
    and authorized main thread, matching exact-thread cursor behavior.
  - Validated collection cursor binding with
    `just test -p codex-app-server controller_cursor` passing 8/8 tests and
    `just test -p codex-app-server controller` passing 53/53 tests.
  - Reran `just test -p codex-app-server`; the full app-server run ended 1185
    passed, 1 flaky passed on retry, 4 zsh-fork failures after retry, and 1
    skipped. The failing zsh-fork fixture cluster remains separate from the
    controller cursor slice.
  - Rebuilt the debug CLI binary again with `cargo build -p codex-cli`.
  - Restricted external-controller `turn/start` and `turn/steer` to the
    authorized main-thread input shape. `turn/start` may submit input and
    client metadata, but controller-origin additional context, environment,
    cwd/workspace-root, approval, sandbox, permission, model/service-tier,
    reasoning, personality, output-schema, collaboration-mode, and
    multi-agent-mode overrides now fail with typed `controller-not-allowed`.
    `turn/steer` similarly rejects controller-origin additional context.
  - Validated the turn override gate with
    `just test -p codex-app-server controller` passing 55/55 tests.
  - Rebuilt the debug CLI binary again with `cargo build -p codex-cli`.
  - Bound controller `thread/resume` response cursors on the internal response
    path used by both cold and already-running resume handling. Initial-turn
    page cursors, turns-backward cursors, and items-backward cursors are now
    connection-bound and main-thread-bound before the response is sent.
  - Validated the resume cursor-binding slice with
    `just test -p codex-app-server controller_cursor` passing 9/9 tests and
    `just test -p codex-app-server controller` passing 56/56 tests.
  - Ran `just fix -p codex-app-server`; unrelated fixer hunks were reverted and
    the remaining zsh-fork fixture lint warning stays outside the controller
    slice.
  - Rebuilt the debug CLI binary again with `cargo build -p codex-cli`.
  - Made native approval `TuiUnavailable` terminal for the local-controller
    launch. When the TUI approval bridge reports the owning TUI unavailable,
    app-server now marks the launch/coordinator `TuiUnavailable`, rejects later
    participation without re-prompting, and rejects normal-interface reads with
    typed `tui-unavailable`.
  - Fenced terminal TUI-unavailable main-thread egress. The terminal transition
    now cancels pending main-thread server requests with typed
    `tui-unavailable` and suppresses future main-thread notifications for
    existing subscribers while leaving unrelated thread notifications intact.
  - Validated the terminal TUI-unavailable slice with
    `just test -p codex-app-server native_tui_unavailable_marks_controller_launch_terminal`
    passing 1/1 focused test and
    `just test -p codex-app-server controller` passing 57/57 controller tests.
  - Reran `just test -p codex-app-server`; the full app-server run ended 1189
    passed, 1 flaky passed on retry, 4 zsh-fork failures after retry, and 1
    skipped. The failing zsh-fork fixture cluster remains separate from the
    controller TUI-unavailable slice.
  - Rebuilt the debug CLI binary again with `cargo build -p codex-cli -j 4`
    after removing generated debug build intermediates while preserving the
    existing top-level debug binaries.
  - Moved external-controller disconnect revocation before RPC-gate drain so
    connection-bound leases are invalidated and pre-externalDelivery prompts
    are rebound to the TUI immediately, even while an already-running RPC is
    still draining.
  - Validated the disconnect revocation slice with
    `just test -p codex-app-server controller_disconnect_rebinds_prompts_before_rpc_drain`
    passing 1/1 focused test and
    `just test -p codex-app-server controller` passing 58/58 controller tests.
  - Reran `just test -p codex-app-server`; the full app-server run ended 1192
    passed, 1 flaky passed on retry, 2 zsh-fork failures after retry, and 1
    skipped. The failing zsh-fork fixture cluster remains separate from the
    controller disconnect revocation slice.
  - Rebuilt the debug CLI binary again with `cargo build -p codex-cli -j 4`.
  - Removed a disconnected controller's main-thread subscription before
    RPC-gate drain, but only when the closed connection had a live controller
    session. This closes the remaining window where main-thread notifications
    could still target a disconnecting controller while one of its RPCs was
    draining.
  - Extended
    `controller_disconnect_rebinds_prompts_before_rpc_drain` to prove the
    disconnected controller is unsubscribed before drain completion.
  - Validated the subscription cleanup slice with
    `just test -p codex-app-server controller_disconnect_rebinds_prompts_before_rpc_drain`
    passing 1/1 focused test and
    `just test -p codex-app-server controller` passing 58/58 controller tests.
  - Reran `just test -p codex-app-server`; the full app-server run ended 1192
    passed, 3 flaky passed on retry, 1 leaky, 2 zsh-fork failures after retry,
    and 1 skipped. The failing zsh-fork fixture cluster remains separate from
    the controller disconnect subscription cleanup.
  - Ran `just fix -p codex-app-server`; it rewrote unrelated
    `config_manager_service.rs` and `turn_start_zsh_fork.rs` hunks, which were
    reviewed and reverted to keep the source commit scoped.
  - Rebuilt the debug CLI binary again with `cargo build -p codex-cli -j 4`.
  - Implemented controller authorization and control-ownership notifications
    from the `ControllerSessionCoordinator` transition boundary. The app-server
    now emits `controller/authorizationChanged` and
    `controller/controlOwnershipChanged` for participation approval, initial
    lease grant, acquire/release, TUI reclaim, deadline expiry, terminal
    TUI-unavailable/main-thread-close state, explicit revocation, sign-off, and
    disconnect paths while preserving prompt rebind/cancel ordering.
  - Validated the notification slice with focused transition tests,
    `just test -p codex-app-server controller` passing 61/61 controller tests,
    `just fix -p codex-app-server` passing for this slice after reverting
    known unrelated fixer hunks, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Added serialized-request priority for app-server queues so primary/TUI
    work wins over queued external-controller work, with the documented
    eight-dequeue fairness bound for valid controller work.
  - Added dequeue-time TUI reclaim for thread-scoped primary input so a
    controller cannot reacquire control while TUI thread work is queued and
    then keep ownership when that TUI work finally runs.
  - Validated the queued-priority/reclaim slice with
    `just test -p codex-app-server request_serialization`,
    `just test -p codex-app-server queued_primary_thread_input_reclaims_after_controller_reacquires`,
    `just test -p codex-app-server controller`,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Filtered automatic thread-created listener attachment so normal initialized
    clients keep the existing auto-subscribe behavior, while external
    controllers auto-attach only when the created thread is their authorized
    main thread.
  - Validated the auto-subscription filter with
    `just test -p codex-app-server auto_attach_filters_external_controller_subscriptions_to_main_thread`,
    `just test -p codex-app-server controller` passing 67/67 with one flaky
    retry in `controller_control_plane_round_trips_after_enrollment`,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Moved external-controller main-thread subscription cleanup ahead of
    terminal sign-off and disconnect revocation events, while preserving the
    existing post-revocation cleanup as an idempotent safety net.
  - Added deterministic bounded-egress coverage with
    `controller_signoff_unsubscribes_before_terminal_notification`, proving
    sign-off unsubscribes the controller before terminal notification emission
    can proceed.
  - Validated the sign-off/disconnect subscription fence with
    `just test -p codex-app-server controller_signoff_unsubscribes_before_terminal_notification`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 68/68 controller tests, `just fix -p codex-app-server` after
    reverting unrelated fixer hunks, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Filtered generic broadcast and initialization notifications away from
    external-controller origins so pre-participation controllers and approved
    controllers do not receive process-wide or unrelated-thread broadcasts.
  - Added controller-aware lifecycle notification targeting for
    thread archive, delete, rename, and unarchive updates so authorized
    external subscribers still receive the granted main thread's normal
    lifecycle notifications after generic broadcasts are filtered.
  - Validated the broadcast-filter slice with
    `just test -p codex-app-server lifecycle_notification_recipients_include_only_authorized_main_thread_controllers`
    passing 1/1 focused test, the focused transport filter tests
    `broadcast_skips_external_controller_connections` and
    `targeted_messages_reach_external_controller_connections` passing 2/2,
    `just test -p codex-app-server transport` passing 28/28,
    `just test -p codex-app-server controller` passing 71/71,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Added controller-aware `thread/status/changed` targeting so authorized
    external subscribers receive the granted main thread's status updates even
    though generic broadcasts no longer reach external-controller origins.
  - Validated the status-targeting slice with
    `just test -p codex-app-server status_change_targets_authorized_main_thread_external_controller`
    passing 1/1 focused test, `just test -p codex-app-server thread_status`
    passing 15/15, `just test -p codex-app-server controller` passing 72/72,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Added thread-scoped global notification targeting for external-controller
    recipients. The primary/TUI side still receives the existing broadcast,
    while authorized external subscribers receive targeted copies for
    controller-visible main-thread global events such as thread goal updates.
  - Validated the global notification targeting slice with
    `just test -p codex-app-server thread_scoped_global_notifications_target_external_controllers`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 73/73, `just test -p codex-app-server thread_goal` passing 7/7,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Routed live listener-command thread goal update, clear, and snapshot egress
    through the controller-aware thread sender. Running-thread resume goal
    snapshots now preserve the primary broadcast and also send targeted copies
    to authorized external-controller recipients.
  - Validated the thread-goal listener egress slice with
    `just test -p codex-app-server listener_goal_update_targets_external_controller_recipients`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 74/74, `just test -p codex-app-server thread_goal` passing 7/7,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Routed live listener-command warning egress through the controller-aware
    thread sender so extension warnings keep normal TUI delivery and include
    authorized external-controller recipients without relying on raw subscriber
    targeting.
  - Validated the listener-warning egress slice with
    `just test -p codex-app-server listener_warning_targets_thread_notification_recipients listener_goal_update_targets_external_controller_recipients`
    passing 2/2 focused tests, `just test -p codex-app-server controller`
    passing 75/75, `just test -p codex-app-server extension` passing 16/16,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Routed extension no-listener `ThreadGoalUpdated` fallback through
    thread-subscriber targeting instead of raw broadcast, so approved
    subscribed external controllers can observe extension goal updates even
    though generic broadcasts skip external-controller origins.
  - Validated the extension goal fallback slice with
    `just test -p codex-app-server app_server_event_sink_targets_goal_subscriber_without_listener`
    passing 1/1 focused test, `just test -p codex-app-server extensions::tests`
    passing 7/7, `just test -p codex-app-server controller` passing 75/75,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`. A broad
    `just test -p codex-app-server extension` run passed the in-crate
    extension tests but timed out in
    `suite::v2::imagegen_extension::standalone_image_edit_uses_attached_model_visible_image`
    after sandbox image-read failure, which remains separate from this
    controller egress slice.
  - Routed app-server `thread/goal` update, clear, and snapshot fallbacks
    through the controller-aware thread sender. These no-listener and
    closed-listener paths now preserve primary/TUI broadcast behavior while
    sending targeted copies to authorized external-controller subscribers.
  - Validated the app-server thread-goal fallback slice with
    `just test -p codex-app-server thread_goal_update_fallback_targets_external_controller_recipients thread_goal_clear_fallback_targets_external_controller_recipients`
    passing 2/2 focused tests, `just test -p codex-app-server controller`
    passing 77/77, `just test -p codex-app-server thread_goal` passing 9/9,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Rejected controller-origin `review/start` requests with
    `delivery: detached` before dispatch. Approved active controllers may still
    use the normal inline/default `review/start` shape on the authorized main
    thread, but cannot create a detached secondary review thread outside the
    single-main-thread controller scope.
  - Validated the review-start gate with
    `just test -p codex-app-server controller_review_start_rejects_detached_delivery`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 78/78, `just test -p codex-app-server review` passing 36/36 after
    one unrelated flaky retry, `just fix -p codex-app-server` after reverting
    unrelated fixer hunks, `just fmt`, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Rejected controller-origin `thread/realtime/start` context/configuration
    overrides before dispatch while keeping the realtime input/transport shape
    available to an active controller. Realtime controller startup may still
    provide the granted main-thread ID, output modality, transport,
    realtime-session ID, and voice, but not model, prompt, startup-context,
    initial-item, instruction, protocol-version, transcript-tail, or Codex
    response-handoff overrides.
  - Validated the realtime-start gate with
    `just test -p codex-app-server controller_realtime_start_allows_input_transport_shape_only`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 79/79, `just test -p codex-app-server realtime` passing 31/31,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Rejected controller-origin `thread/realtime/appendText` with non-user
    roles before dispatch. Active controllers may append user-role realtime
    text to the authorized main thread, but cannot inject developer or
    assistant-role realtime text into the session.
  - Validated the realtime append-text role gate with
    `just test -p codex-app-server controller_realtime_append_text_allows_user_role_only`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 80/80, `just test -p codex-app-server realtime` passing 32/32,
    `just fix -p codex-app-server` after reverting unrelated fixer hunks,
    `just fmt`, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Rejected controller-origin `thread/section/move` requests that set
    `beforeThreadId`, because that field is an implicit target for ordering the
    authorized main thread relative to another thread. Active controllers may
    still move the authorized main thread into a section or remove it from a
    section without naming another thread.
  - Validated the section-move implicit-target gate with
    `just test -p codex-app-server controller_thread_section_move_rejects_before_thread_target`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 81/81 with one unrelated flaky retry in
    `controller_control_plane_round_trips_after_enrollment`,
    `just test -p codex-app-server thread_section` passing 8/8 with four
    unrelated startup-timeout flaky retries, `just fix -p codex-app-server`
    after reverting unrelated fixer hunks, `just fmt`, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Added focused TUI reclaim-classifier coverage proving
    `AppCommand::ApproveGuardianDeniedAction` is treated as a thread-affecting
    approval reply, matching the controller design's TUI-primary reclaim
    invariant for approval input.
  - Validated the TUI reclaim coverage with
    `just test -p codex-tui controller_reclaim` passing 4/4 focused tests, then
    ran `just fmt`.
  - Fenced active external-controller `thread/archive` and `thread/delete`
    before handler dispatch when the authorized main thread has spawned
    descendants, preserving TUI subtree archive/delete behavior while blocking
    controller implicit secondary-thread targets.
  - Validated the archive/delete subtree fence with
    `just test -p codex-app-server active_controller_archive_delete_reject_spawned_descendant_targets`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 82/82 controller tests,
    `just test -p codex-app-server thread_archive` passing 6/6 archive tests,
    `just test -p codex-app-server thread_delete` passing 5/5 delete tests,
    `just fix -p codex-app-server` completing after unrelated fixer hunks were
    reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Fenced running-thread resume replay so pending prompts already delivered to
    an external controller are not replayed to a TUI or other connection after
    the external-delivery boundary.
  - Validated the delivered-prompt replay fence with
    `just test -p codex-app-server external_controller_request_is_not_replayed_after_external_delivery`
    passing 1/1 focused test,
    `just test -p codex-app-server external_controller_request` passing 3/3,
    `just test -p codex-app-server outgoing_message` passing 26/26,
    `just test -p codex-app-server controller` passing 83/83,
    `just test -p codex-app-server thread_resume` passing 59/59,
    `just fix -p codex-app-server` completing after unrelated fixer hunks were
    reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Routed thread-scoped MCP OAuth completion notifications through the
    controller-aware global notification path so authorized main-thread
    controller subscribers receive targeted copies while normal TUI broadcasts
    are preserved.
  - Validated the MCP OAuth completion targeting slice with
    `just test -p codex-app-server thread_scoped_mcp_oauth_completion_targets_external_controller_subscriber`
    passing 1/1 focused test,
    `just test -p codex-app-server auto_attach_filters_external_controller_subscriptions_to_main_thread`
    passing 1/1 focused test,
    `just test -p codex-app-server selected_executor_plugin_exposes_its_mcps_only_to_that_thread`
    passing 1/1 real MCP OAuth integration test,
    `just fix -p codex-app-server` completing after unrelated fixer hunks were
    reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Routed app-server extension no-listener goal and warning fallback egress
    through controller-aware thread recipient computation. Approved subscribed
    external controllers now receive fallback thread-goal notifications through
    the same main-thread subscription path as the TUI, while terminal
    `TuiUnavailable` still suppresses main-thread fallback notifications.
  - Validated the extension fallback targeting slice with
    `just test -p codex-app-server controller_targeted_goal_fallback`
    passing 2/2 focused tests, `just test -p codex-app-server extensions::tests`
    passing 9/9 in-crate extension tests,
    `just test -p codex-app-server controller` passing 86/86 controller tests,
    `just fix -p codex-app-server` completing after unrelated fixer hunks were
    reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Routed listener-ordered `serverRequest/resolved` notifications through the
    controller-aware thread sender instead of rebuilding a raw thread sender
    from subscribed connection IDs.
  - Validated the server-request resolution egress slice with
    `just test -p codex-app-server listener_server_request_resolved_targets_thread_notification_recipients`
    passing 1/1 focused test, `just test -p codex-app-server listener_`
    passing 8/8 listener-filtered tests,
    `just test -p codex-app-server controller` passing 87/87 controller tests,
    `just fix -p codex-app-server` completing after unrelated fixer hunks were
    reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex`.
  - Made terminal local-controller acceptor failure stop accepting, report a
    one-shot endpoint failure to the embedded runtime, drop the socket and
    metadata guards, avoid republishing `mainThreadId` after acceptor exit, and
    close existing external-controller connections through the normal
    connection-closed revocation path.
  - Validated the endpoint-failure slice with
    `just test -p codex-app-server-transport local_controller`,
    `just test -p codex-app-server-transport`,
    `just test -p codex-app-server in_process::tests`,
    `just test -p codex-app-server controller`, scoped `just fix` runs for
    `codex-app-server-transport` and `codex-app-server`, `just fmt`, `git diff
    --check`, and `cargo build -p codex-cli -j 4` after freeing
    `target/debug/incremental` build cache.
  - Added an in-process `LocalControllerEndpointUnavailable` event so late
    local-controller endpoint failure reaches the TUI after established
    controller connections are closed through the normal revocation path. The
    TUI now reports `embedded-unavailable` from that event while onboarding and
    `codex exec` ignore it as non-interactive controller state.
  - Validated the late endpoint-unavailable slice with
    `just test -p codex-app-server in_process::tests` passing 11/11,
    `just test -p codex-tui external_controller_availability` passing 5/5,
    `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events`
    passing 1/1, `just test -p codex-exec` passing 136/136, scoped `just fix`
    runs for `codex-app-server`, `codex-app-server-client`, `codex-tui`, and
    `codex-exec`, `just fmt`, `just build-code-mode-host`, and
    `cargo build -p codex-cli -j 4`.
  - Added a per-connection external-controller initialized-RPC reservation in
    the app-server request gate. Saturated controller ingress now receives
    JSON-RPC `-32001` with typed `ControllerErrorData { code:
    controller-overloaded, retry: sameConnection }` instead of allowing
    unbounded serialized app-server queue growth.
  - Validated the controller ingress overload slice with focused
    `just test -p codex-app-server connection_rpc_gate request_serialization controller_overload saturated_external_controller_ingress_returns_typed_overload`
    passing 19/19 tests, `just test -p codex-app-server` reaching 1227/1229
    passing with only the known zsh-fork timeout fixture cluster failing,
    `just fix -p codex-app-server` completing after unrelated fixer hunks were
    reverted, `just fmt` passing, `git diff --check` passing, and
    `cargo build -p codex-cli -j 4` rebuilding `codex-rs/target/debug/codex`.
  - Split external-controller control-plane RPC ingress from normal
    external-controller RPC ingress. Saturated normal controller requests no
    longer prevent `controller/requestParticipation`,
    `controller/acquireControl`, `controller/releaseControl`, or
    `controller/signOff` from reaching dispatch, while both queues remain
    per-connection bounded.
  - Validated the controller control-plane ingress slice at commit `bcdbdff`
    with `just test -p codex-app-server connection_rpc_gate saturated_external_controller`
    passing 10/10 focused tests, `just test -p codex-app-server controller`
    passing 93/93 controller tests, `just fix -p codex-app-server` completing
    after unrelated fixer hunks were reverted, `just fmt` passing, `git diff
    --check` and `git diff --cached --check` passing, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex` in 1.47s.
  - Added focused in-process local-controller coverage for the
    pre-participation default-deny invariant: even when the embedded runtime has
    startup config warnings queued, an initialized external-controller socket
    receives only the `initialize` response and no runtime notifications before
    participation is approved.
  - Validated the pre-participation silence coverage at commit `251372e` with
    `just test -p codex-app-server local_controller_initialize_suppresses_pre_participation_notifications in_process::tests::local_controller_socket_uses_main_thread_interface_and_tui_reclaim`
    passing 2/2 focused tests, `just test -p codex-app-server controller`
    passing 94/94 controller-filtered tests, `just fix -p codex-app-server`
    completing after unrelated fixer hunks were reverted, `just fmt` passing,
    `git diff --check` and `git diff --cached --check` passing, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex` in 21.75s.
  - Made the TUI reclaim classifier exhaustive for all current
    `AppCommand` variants, so newly added coordinator-facing commands require
    an explicit reclaim decision instead of inheriting a wildcard default.
  - Validated the exhaustive reclaim classifier at commit `ae608c3` with
    `just test -p codex-tui controller_reclaim` passing 4/4 focused tests,
    `just fix -p codex-tui` passing, `just fmt` passing, `git diff --check`
    passing, `pre-commit run --files codex-rs/tui/src/app_command.rs` failing
    only because `.pre-commit-config.yaml` is not present, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex` in 15.25s.
  - Marked the native external-controller launch terminal when the immutable
    main thread closes. The app-server controller processor now closes the
    coordinator, emits main-thread-closed authorization/control notifications,
    cancels pending main-thread server requests with typed
    `main-thread-closed`, and rejects later normal-interface reads with typed
    `main-thread-closed`.
  - Wired the terminal main-thread-close path into active thread archive/delete
    teardown and idle thread unload before generic request cancellation.
  - Validated the main-thread-close slice at commit `02d3d1c` with
    `just test -p codex-app-server controller_main_thread_close_marks_launch_closed`
    passing 1/1 focused test, `just test -p codex-app-server controller`
    passing 95/95 controller-filtered tests, `just test -p codex-app-server`
    ending 1230 passed, 2 flaky passed on retry, 3 failed, and 1 skipped due
    to the unrelated hosted-login callback and zsh-fork fixture failures,
    `just fmt` passing, `just fix -p codex-app-server` completing after
    unrelated fixer hunks were reverted, `git diff --check` passing,
    `pre-commit run --all-files` failing only because
    `.pre-commit-config.yaml` is not present, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Attempted to refresh `codex-rs/target/debug/codex-code-mode-host` with
    `cargo build -p codex-code-mode-host -j 4`; it failed before a fresh host
    binary was produced because the `v8` build script could not download the
    `rusty_v8` sandbox archive after Python TLS certificate verification
    failed. The existing debug host binary remains present.
  - Marked thread lifecycle/state notifications as lossless for the embedded
    in-process TUI delivery path and the app-server-client bridge. The
    classifier now preserves `thread/status/changed`, `thread/archived`,
    `thread/deleted`, `thread/unarchived`, `thread/closed`, and
    `thread/name/updated` under queue pressure so controller-originated normal
    interface actions are reflected through the same event path the TUI uses.
  - Validated the lifecycle-lossless slice at commit `3bf6c0d` with
    `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications`
    passing 1/1 focused test,
    `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events`
    passing 1/1 focused test, `just test -p codex-app-server in_process::tests`
    passing 12/12 tests, `just test -p codex-app-server-client` passing 29/29
    tests, `just fmt` passing, scoped `just fix` runs for
    `codex-app-server` and `codex-app-server-client` completing after the known
    unrelated app-server fixer hunks were reverted, `git diff --check` and
    `git diff --cached --check` passing, `pre-commit run --all-files` failing
    only because `.pre-commit-config.yaml` is not present, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex`.
  - Preserved reasoning summary section-boundary notifications across the
    embedded in-process TUI delivery path and the app-server-client bridge.
    The shared lossless classifier now includes
    `item/reasoning/summaryPartAdded`, matching the existing lossless handling
    for reasoning summary text deltas.
  - Validated the reasoning-summary-part lossless slice at commit `aac4b99`
    with `just fmt` passing,
    `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications`
    passing 1/1 focused test,
    `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events`
    passing 1/1 focused test, `git diff --check` and
    `git diff --cached --check` passing.
  - Preserved realtime started, transcript delta/done, error, and closed
    notifications across the same embedded in-process TUI delivery path and
    app-server-client bridge. Realtime audio, SDP, and raw realtime items remain
    best-effort.
  - Validated the realtime controller delivery slice at commit `b2738fb` with
    `just fmt` passing,
    `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript_and_terminal_server_notifications`
    passing 1/1 focused test,
    `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events`
    passing 1/1 focused test, `git diff --check` and
    `git diff --cached --check` passing.
  - Surfaced external-controller server-request reply rejections back to the
    originating controller connection without consuming the pending prompt
    callback. Controller-owned approval prompts now reject `acceptForSession`
    with typed `controller-not-allowed` / `DoNotRetry`, then remain pending so
    a valid `accept` or `cancel` from the current owner can still resolve the
    prompt.
  - Validated the prompt-rejection surfacing slice at commit `ca5186e` with
    `just test -p codex-app-server controller_control_plane_round_trips_after_enrollment`
    passing 1/1 focused test, `just fmt` passing, scoped
    `just fix -p codex-app-server` completing after the known unrelated fixer
    hunks were reverted, `git diff --check` and `git diff --cached --check`
    passing, and `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex` in 23.66s.
  - Added controller protocol/export coverage for the two controller
    notification schemas, the authorization notification wire shape, and every
    canonical controller error-code wire name.
  - Validated the protocol coverage slice at commit `3d96c79` with
    `just test -p codex-app-server-protocol controller` passing 7/7 focused
    tests, `just test -p codex-app-server-protocol stable_schema_filter_removes_mock_thread_start_field`
    passing 1/1 focused test, `just fmt` passing, scoped
    `just fix -p codex-app-server-protocol` passing, `git diff --check` and
    `git diff --cached --check` passing. No schema regeneration or debug binary
    rebuild was required because the protocol shape and production code did not
    change.
  - Rendered app-server `thread/realtime/error` notifications as visible TUI
    warning history cells instead of dropping them in the live app-server
    notification handler.
  - Validated the TUI realtime-error rendering slice at commit `b773bfe` with
    `just fmt` passing,
    `just test -p codex-tui live_app_server_realtime_error_notification_renders_warning`
    passing 1/1 focused test after a cold rebuild, `just fix -p codex-tui`
    passing, `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex` in 1m34s with the known `__eh_frame` linker
    warning, and `git diff --check` passing.
  - Rendered app-server `thread/realtime/transcriptDelta` and
    `thread/realtime/transcriptDone` notifications through normal TUI history
    paths: final user transcripts render as user messages, and assistant
    transcript deltas/done use the existing assistant stream/consolidation
    path.
  - Validated the TUI realtime-transcript rendering slice at commit `67a259f`
    with `just fmt` passing,
    `just test -p codex-tui live_app_server_realtime` passing 3/3 focused
    tests, `cargo insta pending-snapshots --manifest-path tui/Cargo.toml`
    passing with no pending snapshots after installing `cargo-insta`,
    `just fix -p codex-tui` passing, `cargo build -p codex-cli -j 4`
    rebuilding `codex-rs/target/debug/codex` in 14.53s with the known
    `__eh_frame` linker warning, and `git diff --check` plus
    `git diff --cached --check` passing.
  - Refreshed the active TUI thread from app-server
    `thread/read(includeTurns=true)` after in-process app-server event lag, then
    replayed the authoritative snapshot through the existing normal history
    rendering path while preserving current input state.
  - Validated the TUI app-server lag recovery slice at commit `ff0dd36` with
    `just fmt` passing,
    `just test -p codex-tui lag_refresh_replays_authoritative_active_thread_snapshot`
    passing 1/1 focused test,
    `just test -p codex-tui mcp_startup app_scoped_mcp_startup_notifications_do_not_render_in_active_thread active_side_thread_renders_live_mcp_startup_notifications`
    passing 38/38 focused tests,
    `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` passing with
    no pending snapshots, `just fix -p codex-tui` passing, and
    `cargo build -p codex-cli -j 4` rebuilding
    `codex-rs/target/debug/codex` in 13.03s with the known `__eh_frame` linker
    warning.
- In progress:
  - Selecting the next Codex-side parity slice around any remaining implicit
    targets, egress transactionality, and long-lived subscription edges not
    covered by sign-off, authorization-expiry cleanup, exact-thread and
    collection-filtered cursor binding, prompt owner-epoch binding,
    current-time owner routing, resume/turn override gating, internally-sent
    resume cursor binding, idempotent acquire, controller notifications, or
    serialized-request priority/dequeue reclaim, or controller auto-subscribe
    filtering, or terminal sign-off/disconnect subscription fencing, or
    generic broadcast filtering plus targeted main-thread lifecycle and status
    delivery, or thread-scoped global and listener-command thread-goal
    notification targeting, listener-warning targeting, or extension
    no-listener goal/warning fallback controller targeting, or app-server
    thread-goal fallback targeting, or listener server-request resolution
    targeting, or thread-scoped MCP OAuth completion targeting, or
    controller-origin detached review fencing, or
    controller-origin realtime context/configuration override gating, or
    controller-origin realtime text role fencing, or controller-origin
    section-move implicit target fencing, or controller-origin archive/delete
    spawned-descendant subtree fencing, or delivered controller prompt replay
    fencing, or terminal local-controller acceptor failure handling, or late
    endpoint-unavailable TUI reporting, or bounded controller ingress overload,
    or separate controller control-plane ingress, or pre-participation
    initialize-notification suppression, or exhaustive TUI command reclaim
    classification, or terminal main-thread-close launch handling, or
    in-process lifecycle/state notification preservation, or controller
    prompt-rejection error surfacing, or controller notification schema
    coverage, or realtime transcript/lifecycle delivery preservation, or TUI
    realtime-error rendering, or TUI realtime transcript rendering, or TUI
    app-server lag snapshot recovery.
  - Downstream controller-host implementation for file-watch discovery, health
    model separation, and deterministic auto-assignment.
- Not started:
  - Downstream acceptance rerun across multiple live Codex launches after the
    controller host consumes the metadata-directory discovery contract.

## Next Steps

- Keep tightening app-server normal-interface parity around any remaining
  implicit targets, egress transactionality, and subscription edges beyond the
  committed exact-thread/collection cursor binding, sign-off cleanup,
  authorization-expiry cleanup, idempotent acquire, prompt owner-epoch binding,
  current-time owner routing, resume/turn override gates, and internally-sent
  resume cursor binding, plus terminal TUI-unavailable launch handling and
  controller authorization/ownership notification emission, plus
  serialized-request priority and dequeue-time TUI reclaim, plus controller
  auto-subscribe filtering, plus terminal sign-off/disconnect subscription
  fencing, plus generic broadcast filtering and targeted main-thread lifecycle
  and status delivery, plus thread-scoped global and listener-command
  thread-goal notification targeting, plus listener-warning targeting and
  extension no-listener goal/warning fallback controller targeting, plus app-server
  thread-goal fallback targeting, plus listener server-request resolution
  targeting, plus thread-scoped MCP OAuth completion targeting, plus embedded
  in-process transcript/item delivery preservation and centralized lossless
  delivery classification, including reasoning summary part-added notifications,
  plus realtime transcript/lifecycle delivery preservation, plus bounded
  per-connection external-controller ingress overload and separate controller
  control-plane ingress, plus pre-participation initialize-notification
  suppression coverage, plus exhaustive TUI command reclaim classification, plus terminal
  main-thread-close launch handling, plus in-process lifecycle/state
  notification preservation, plus controller prompt-rejection error surfacing,
  plus controller notification schema coverage, plus reasoning-summary-part
  lossless delivery, plus TUI realtime-error rendering, plus TUI realtime
  transcript rendering, plus TUI app-server lag snapshot recovery.
- Treat the source tree as ready for the next narrow implementation slice after
  commit `ff0dd36`.
- Implement downstream discovery as metadata-directory watch plus full rescan.
- Fix downstream status mapping so pending/unapproved/released live launches do
  not display as offline.
- Rerun the downstream multi-launch controller smoke once the downstream host
  work is in place.
