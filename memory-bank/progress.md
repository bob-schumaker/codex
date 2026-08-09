# Progress

## Working

- Codex-side local-controller metadata now publishes enough launch information
  for controller discovery, including process ID, endpoint URI, nonce, and
  `mainThreadId` once available.
- The design preserves the TUI as primary input owner while allowing an approved
  controller to acquire and release control.
- The spec corpus now states that controller discovery uses
  `$CODEX_HOME/local-controllers`, not generic Codex hooks or Herdr metadata.
- In-process local-controller socket coverage now exercises the native TUI
  approval path rather than durable enrollment test credentials.
- External-controller admission now extracts exact main-thread targets from the
  admitted normal app-server interface, including serialized thread mutations
  and concurrent read methods.
- Thread-scoped TUI-only mutations now reclaim primary input ownership from an
  active controller lease before dispatch.
- Collection-filtered controller reads now align admission, target extraction,
  and dispatch for `thread/list`, `thread/search`, `thread/loaded/list`, and
  `threadSection/list`; controller collection reads are scoped to the immutable
  main thread instead of rejecting or exposing broad runtime state.
- Successful `controller/signOff` now revokes the controller session, rebinds
  prompts to the TUI, waits for the sign-off response to queue and write, and
  then disconnects the controller socket.
- After `controller/signOff` starts final-response teardown, subsequent
  external-controller ingress on that same connection is rejected with typed
  `transport-closing` while the final response flushes and the socket closes.
- `controller/signOff` now requires an existing standing controller session
  before revocation, and successful sign-off immediately unsubscribes the
  external-controller connection from the TUI main thread before final
  response/disconnect teardown continues.
- External-controller `thread/resume` now remains `ExactThread + ActiveOwner`
  for main-thread rejoin, but controller-origin history, path, configuration,
  instruction, approval, sandbox, permission, and personality overrides are
  rejected before handler dispatch.
- Exact-thread pagination cursors returned to an approved controller are now
  connection-bound and main-thread-bound before reuse. The binding covers
  `thread/resume` cursor fields, including the internal cold/running resume
  response send paths, `thread/turns/list`, `thread/items/list`,
  `thread/searchOccurrences`, and `thread/backgroundTerminals/list`.
- Collection-filtered pagination cursors returned to an approved controller are
  now also connection-bound and main-thread-bound before reuse. The binding
  covers `thread/list`, `thread/search`, `thread/loaded/list`, and
  `threadSection/list`.
- Controller-origin `turn/start` and `turn/steer` now preserve the active-owner
  normal input path while blocking pre-dispatch context/configuration override
  fields. `turn/start` allows input and client metadata, but rejects additional
  context, environment/cwd/workspace-root, approval/sandbox/permission,
  model/service-tier, reasoning, personality, output-schema, collaboration-mode,
  and multi-agent-mode overrides. `turn/steer` rejects additional context.
- `controller/signOff` now preserves the expected external-origin error for
  non-controller callers before any main-thread admission check.
- Authorization expiry now revokes the controller's standing session with a
  typed `authorization-expired` error and removes the external-controller
  connection from the live TUI main-thread subscription without closing the
  socket.
- `controller/acquireControl` is now idempotent for the controller that already
  owns the active lease; a second controller still receives the existing
  ownership-conflict behavior.
- Controller prompt/server-request replies delivered through the
  owner-aware recipient path are now bound to the interactive owner epoch that
  delivered the prompt; the same controller cannot resolve an old prompt after
  release/reacquire under a newer lease.
- `currentTime/read` now uses the same owner-aware recipient path as other
  thread-scoped interactive server requests. Active external controllers receive
  the current-time request for the TUI main thread, and replies are bound to the
  owner epoch captured when the request was delivered.
- Native approval `TuiUnavailable` is now terminal for a local-controller
  launch. Once the owning TUI reports unavailable, app-server marks the
  launch/coordinator `TuiUnavailable`, does not re-prompt on later
  participation attempts, and rejects normal-interface reads with typed
  `tui-unavailable`.
- Terminal TUI-unavailable launches now also fence main-thread egress. Pending
  main-thread server requests are canceled with typed `tui-unavailable`, future
  main-thread notifications are suppressed for existing subscribers, and
  unrelated thread notifications continue unchanged.
- External-controller disconnect now revokes connection-bound controller
  sessions and rebinds pre-externalDelivery prompts before waiting for
  in-flight connection RPCs to drain, so prompt ownership returns to the TUI
  promptly on unexpected disconnect.
- External-controller disconnect now also removes that controller's main-thread
  subscription before RPC drain, and only for connections that actually held a
  controller session.
- Terminal sign-off/disconnect paths now remove controller main-thread
  subscriptions before terminal revocation events or notifications can be
  emitted, preserving existing post-revocation cleanup as an idempotent safety
  net.
- App-server now emits controller authorization and control-ownership
  notifications from the `ControllerSessionCoordinator` transition boundary.
  `controller/authorizationChanged` and
  `controller/controlOwnershipChanged` cover participation approval, initial
  lease grant, acquire/release, TUI reclaim, deadline expiry, terminal
  TUI-unavailable/main-thread-close, explicit revocation, sign-off, and
  disconnect paths with coordinator-owned reason, owner epoch, session sequence,
  and session/lease snapshots.
- App-server serialized request queues now prioritize primary/TUI work over
  queued external-controller work, while preserving a bounded eight-dequeue
  fairness rule for valid controller work.
- Thread-scoped primary input now rechecks TUI reclaim immediately before a
  queued request executes, so a controller that reacquires while TUI input is
  waiting cannot keep ownership after that TUI input runs.
- Automatic thread-created listener attachment now preserves normal initialized
  client behavior while filtering external controllers to their authorized main
  thread only, preventing controller subscription bleed to secondary threads.
- Generic broadcast and initialization notifications are now filtered away from
  external-controller origins. Controller-visible thread archive/delete/name
  and unarchive lifecycle notifications are explicitly retargeted to authorized
  external subscribers for the granted main thread.
- `thread/status/changed` notifications are now also controller-targeted for
  authorized external subscribers to the granted main thread, preserving normal
  main-thread status visibility after generic broadcasts are filtered away from
  external-controller origins.
- Thread-scoped global notifications now preserve the existing primary/TUI
  broadcast while also targeting authorized external-controller recipients for
  the same main-thread events.
- Live listener-command thread goal update, clear, and snapshot egress now uses
  the controller-aware thread sender. Running-thread resume goal snapshots also
  target authorized external-controller recipients while preserving the primary
  broadcast.
- Live listener-command warning egress now uses the controller-aware thread
  sender, preserving normal TUI warning delivery while including authorized
  external-controller recipients and avoiding raw subscriber targeting.
- Listener-ordered `serverRequest/resolved` egress now uses the
  controller-aware thread sender, preserving listener ordering while avoiding
  raw subscriber targeting for request-resolution notifications.
- Extension no-listener `ThreadGoalUpdated` fallback now targets thread
  subscribers instead of raw broadcast, preserving visibility for approved
  subscribed external controllers after generic broadcasts were filtered away
  from external-controller origins.
- Extension no-listener goal and warning fallback egress now uses
  controller-aware thread recipient computation in the production app-server
  sink. Approved subscribed external controllers receive fallback goal
  notifications for the TUI main thread, while terminal `TuiUnavailable`
  suppresses main-thread fallback notifications.
- App-server `thread/goal` update, clear, and snapshot fallbacks now use the
  controller-aware thread sender, preserving primary/TUI broadcasts while adding
  targeted copies for authorized external-controller recipients.
- Controller-origin `review/start` now stays inside the authorized main-thread
  controller contract: inline/default review delivery remains allowed, while
  `delivery: detached` is rejected before dispatch so an active controller
  cannot create a secondary detached review thread.
- Controller-origin `thread/realtime/start` now preserves realtime
  input/transport startup for active controllers while rejecting context and
  configuration override fields that would replace model, prompt, instructions,
  startup context, initial session history, protocol version, transcript-tail
  handling, or Codex response-handoff behavior.
- Controller-origin `thread/realtime/appendText` now accepts only user-role
  realtime text. Developer and assistant roles are rejected before dispatch so
  external input controllers cannot inject non-user realtime session items.
- Controller-origin `thread/section/move` now rejects `beforeThreadId`, keeping
  section placement changes scoped to the authorized main thread instead of
  allowing the controller to order it relative to another thread.
- TUI reclaim-classifier tests now cover Guardian-denied approval input as a
  thread-affecting approval path, so the documented TUI-primary reclaim
  invariant includes `AppCommand::ApproveGuardianDeniedAction`.
- TUI command reclaim classification is now exhaustive for all current
  `AppCommand` variants, so adding a coordinator-facing command requires an
  explicit thread-affecting or display-only decision during compilation.
- Late local-controller endpoint failure now reaches the TUI through a
  lossless in-process event after established controller connections are closed
  through the normal revocation path. The TUI reports
  `embedded-unavailable`; onboarding and `codex exec` ignore the event as
  non-interactive controller state.
- Closing or unloading the immutable main thread now marks the native
  external-controller launch terminal. The controller coordinator is closed,
  pending main-thread server requests are canceled with typed
  `main-thread-closed`, authorization/control notifications are emitted with
  the main-thread-closed reason, and later normal-interface reads receive typed
  `main-thread-closed`.
- The embedded in-process TUI delivery path and app-server-client bridge now
  preserve controller-relevant thread lifecycle/state notifications under
  backpressure: `thread/status/changed`, `thread/archived`, `thread/deleted`,
  `thread/unarchived`, `thread/closed`, and `thread/name/updated`.
- The latest Codex-side source checkpoint is
  `3bf6c0d` for launch metadata publication, native approval coverage, exact
  target extraction, TUI reclaim, collection-filtered reads, sign-off teardown
  and cleanup, resume-override gating, exact-thread and collection-filtered
  cursor binding, authorization-expiry cleanup, idempotent active-owner acquire,
  prompt owner-epoch reply binding, current-time owner routing, and controller
  turn override gating, internal `thread/resume` response cursor binding, plus
  terminal native TUI-unavailable launch handling, main-thread egress fencing,
  early controller disconnect revocation, and early disconnect subscription
  cleanup, plus controller authorization/control-ownership notification
  emission, plus serialized-request priority, dequeue-time TUI reclaim, and
  controller auto-subscribe filtering, plus terminal sign-off/disconnect
  subscription fencing, plus external-controller broadcast filtering and
  targeted main-thread lifecycle delivery, plus targeted main-thread status
  delivery, plus thread-scoped global notification targeting and
  listener-command thread-goal egress targeting, plus listener-warning
  targeting, extension no-listener goal-update fallback targeting, and
  app-server thread-goal fallback targeting, plus thread-scoped MCP OAuth
  completion targeting, plus controller-origin detached review fencing, realtime
  context/configuration override gating, and realtime text role fencing, plus
  section-move implicit-target fencing, Guardian-denied TUI approval reclaim
  coverage, exhaustive TUI command reclaim classification, archive/delete
  spawned-descendant subtree fencing, delivered controller prompt replay
  fencing, and extension fallback goal/warning controller targeting, plus
  listener server-request resolution targeting, plus terminal local-controller
  acceptor failure reporting, metadata/socket cleanup, external-controller
  connection closure through the normal revocation path, and bounded
  per-connection external-controller ingress with typed `controller-overloaded`
  retry guidance, plus terminal controller launch closure when the immutable
  main thread closes or unloads, plus lossless in-process delivery for
  controller-relevant thread lifecycle/state notifications.
- Focused app-server controller tests pass. The latest full
  `just test -p codex-app-server` run ended 1230 passed, 2 flaky passed on
  retry, 3 failed, and 1 skipped due to the unrelated hosted-login callback and
  zsh-fork fixture failures; those failures remain separate fixture health
  issues outside the controller slice.
- The latest focused validation for commit `c267e17` is
  `just test -p codex-app-server notifications_track_authorization_and_ownership_transitions notifications_track_deadline_and_terminal_revocation controller_control_notifications_are_emitted_for_session_transitions`,
  passing 3/3 focused tests, `just test -p codex-app-server controller`,
  passing 61/61 controller tests, `just fix -p codex-app-server` passing for
  the source slice after reverting known unrelated fixer hunks, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `2bce609` is
  `just test -p codex-app-server request_serialization` passing 9/9 focused
  tests,
  `just test -p codex-app-server queued_primary_thread_input_reclaims_after_controller_reacquires`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 66/66 controller tests, `just fix -p codex-app-server` completing
  after unrelated fixer hunks were reverted, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `93cd090` is
  `just test -p codex-app-server auto_attach_filters_external_controller_subscriptions_to_main_thread`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 67/67 controller tests with one flaky retry in
  `controller_control_plane_round_trips_after_enrollment`,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, and `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `354c4dc` is
  `just test -p codex-app-server controller_signoff_unsubscribes_before_terminal_notification`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 68/68 controller tests, `just fix -p codex-app-server` completing
  after unrelated fixer hunks were reverted, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `5d54a58` is
  `just test -p codex-app-server lifecycle_notification_recipients_include_only_authorized_main_thread_controllers`
  passing 1/1 focused test, the focused transport filter tests
  `broadcast_skips_external_controller_connections` and
  `targeted_messages_reach_external_controller_connections` passing 2/2,
  `just test -p codex-app-server transport` passing 28/28,
  `just test -p codex-app-server controller` passing 71/71,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, and `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `280c679` is
  `just test -p codex-app-server status_change_targets_authorized_main_thread_external_controller`
  passing 1/1 focused test, `just test -p codex-app-server thread_status`
  passing 15/15, `just test -p codex-app-server controller` passing 72/72,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `bc9abac` is
  `just test -p codex-app-server thread_scoped_global_notifications_target_external_controllers`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 73/73, `just test -p codex-app-server thread_goal` passing 7/7,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `6c29759` is
  `just test -p codex-app-server listener_goal_update_targets_external_controller_recipients`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 74/74, `just test -p codex-app-server thread_goal` passing 7/7,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `0b33c0f` is
  `just test -p codex-app-server listener_warning_targets_thread_notification_recipients listener_goal_update_targets_external_controller_recipients`
  passing 2/2 focused tests, `just test -p codex-app-server controller`
  passing 75/75, `just test -p codex-app-server extension` passing 16/16,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `9b59a8c` is
  `just test -p codex-app-server app_server_event_sink_targets_goal_subscriber_without_listener`
  passing 1/1 focused test, `just test -p codex-app-server extensions::tests`
  passing 7/7, `just test -p codex-app-server controller` passing 75/75,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`. A broader
  `just test -p codex-app-server extension` run timed out in
  `suite::v2::imagegen_extension::standalone_image_edit_uses_attached_model_visible_image`
  after sandbox image-read failure, outside the controller egress slice.
- The latest focused validation for commit `e5d3141` is
  `just test -p codex-app-server thread_goal_update_fallback_targets_external_controller_recipients thread_goal_clear_fallback_targets_external_controller_recipients`
  passing 2/2 focused tests, `just test -p codex-app-server controller`
  passing 77/77, `just test -p codex-app-server thread_goal` passing 9/9,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `af4d172` is
  `just test -p codex-app-server controller_review_start_rejects_detached_delivery`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 78/78, `just test -p codex-app-server review` passing 36/36 after
  one unrelated flaky retry,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `e436e75` is
  `just test -p codex-app-server controller_realtime_start_allows_input_transport_shape_only`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 79/79, `just test -p codex-app-server realtime` passing 31/31,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `ea756d4` is
  `just test -p codex-app-server controller_realtime_append_text_allows_user_role_only`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 80/80, `just test -p codex-app-server realtime` passing 32/32,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `9b6d3a2` is
  `just test -p codex-app-server controller_thread_section_move_rejects_before_thread_target`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 81/81 with one unrelated flaky retry in
  `controller_control_plane_round_trips_after_enrollment`,
  `just test -p codex-app-server thread_section` passing 8/8 with four
  unrelated startup-timeout flaky retries,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `002f2cd` is
  `just test -p codex-tui controller_reclaim` passing 4/4 focused tests, and
  `just fmt` passing.
- The latest focused validation for commit `45806b1` is
  `just test -p codex-app-server active_controller_archive_delete_reject_spawned_descendant_targets`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 82/82 controller tests,
  `just test -p codex-app-server thread_archive` passing 6/6 archive tests,
  `just test -p codex-app-server thread_delete` passing 5/5 delete tests,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `b0ca0bc` is
  `just test -p codex-app-server external_controller_request_is_not_replayed_after_external_delivery`
  passing 1/1 focused test,
  `just test -p codex-app-server external_controller_request` passing 3/3,
  `just test -p codex-app-server outgoing_message` passing 26/26,
  `just test -p codex-app-server controller` passing 83/83,
  `just test -p codex-app-server thread_resume` passing 59/59,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `776fa13` is
  `just test -p codex-app-server thread_scoped_mcp_oauth_completion_targets_external_controller_subscriber`
  passing 1/1 focused test,
  `just test -p codex-app-server auto_attach_filters_external_controller_subscriptions_to_main_thread`
  passing 1/1 focused test,
  `just test -p codex-app-server selected_executor_plugin_exposes_its_mcps_only_to_that_thread`
  passing 1/1 real MCP OAuth integration test,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, and `cargo build -p codex-cli -j 4`
  rebuilding `codex-rs/target/debug/codex`.
- The latest focused validation for commit `09768dc` is
  `just test -p codex-app-server controller_targeted_goal_fallback` passing
  2/2 focused tests, `just test -p codex-app-server extensions::tests` passing
  9/9 in-crate extension tests, `just test -p codex-app-server controller`
  passing 86/86 controller tests, `just fix -p codex-app-server` completing
  after unrelated fixer hunks were reverted, `just fmt` passing, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `e17994d` is
  `just test -p codex-app-server listener_server_request_resolved_targets_thread_notification_recipients`
  passing 1/1 focused test, `just test -p codex-app-server listener_` passing
  8/8 listener-filtered tests, `just test -p codex-app-server controller`
  passing 87/87 controller tests, `just fix -p codex-app-server` completing
  after unrelated fixer hunks were reverted, `just fmt` passing, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `2c27d3f` is
  `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript`
  passing 1/1 focused test, `just test -p codex-app-server in_process::tests`
  passing 10/10 in-process tests, `just test -p codex-app-server controller`
  passing 87/87 controller tests, `just fix -p codex-app-server` completing
  after unrelated fixer hunks were reverted, `just fmt` passing, `git diff
  --check` and `git diff --cached --check` passing, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `c1b4a2a` is
  `just test -p codex-app-server-client event_requires_delivery_marks_transcript`
  passing 1/1 focused client test,
  `just test -p codex-app-server guaranteed_delivery_helpers_cover_transcript`
  passing 1/1 focused app-server test,
  `just test -p codex-app-server-client` passing 29/29 client tests,
  `just fix -p codex-app-server-client` passing, `just fix -p
  codex-app-server` completing after unrelated fixer hunks were reverted,
  `just fmt` passing, `git diff --check` and `git diff --cached --check`
  passing, and `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `cfa8ea6` is
  `just test -p codex-app-server-transport terminal_accept_error_reports_endpoint_failure`
  passing 1/1 focused transport test,
  `just test -p codex-app-server in_process_outbound_router_disconnect_and_close_requests_disconnect`
  passing 1/1 focused app-server test,
  `just test -p codex-app-server-transport local_controller` passing 13/13,
  `just test -p codex-app-server in_process::tests` passing 11/11,
  `just test -p codex-app-server-transport` passing 158/158, and
  `just test -p codex-app-server controller` passing 87/87. Scoped
  `just fix -p codex-app-server-transport` and `just fix -p codex-app-server`
  completed, with the known unrelated app-server fixer hunks reverted; `just
  fmt` and `git diff --check` passed. The first
  `cargo build -p codex-cli -j 4` failed at link with `errno=28` because the
  filesystem had 823 MiB free; removing `codex-rs/target/debug/incremental`
  freed enough cache space while preserving debug binaries, and the rerun
  rebuilt `codex-rs/target/debug/codex`.
- The latest focused validation for commit `6e69a87` is
  `just test -p codex-app-server in_process::tests` passing 11/11,
  `just test -p codex-tui external_controller_availability` passing 5/5,
  `just test -p codex-app-server-client event_requires_delivery_marks_transcript_and_terminal_events`
  passing 1/1, and `just test -p codex-exec` passing 136/136. Scoped
  `just fix -p codex-app-server`, `just fix -p codex-app-server-client`,
  `just fix -p codex-tui`, and `just fix -p codex-exec` completed, with the
  known unrelated app-server fixer hunks reverted; `just fmt` passed. A direct
  `cargo build -p codex-cli -p codex-code-mode-host -j 4` failed at the
  upstream `rusty_v8` archive download due Python TLS certificate verification,
  so the successful host build used the repo wrapper `just build-code-mode-host`
  and the successful CLI build used `cargo build -p codex-cli -j 4`.
- The latest focused validation for commit `0e9b265` is
  `just test -p codex-app-server connection_rpc_gate request_serialization controller_overload saturated_external_controller_ingress_returns_typed_overload`
  passing 19/19 focused tests, `just test -p codex-app-server` reaching 1227
  passed, 2 zsh-fork timeout fixture failures after retry, and 1 skipped,
  `just fix -p codex-app-server` completing after unrelated fixer hunks were
  reverted, `just fmt` passing, `git diff --check` passing, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`.
- The latest focused validation for commit `bcdbdff` is
  `just test -p codex-app-server connection_rpc_gate saturated_external_controller`
  passing 10/10 focused tests, `just test -p codex-app-server controller`
  passing 93/93 controller tests, `just fix -p codex-app-server` completing
  after unrelated fixer hunks were reverted, `just fmt` passing, `git diff
  --check` and `git diff --cached --check` passing, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex` in 1.47s.
- The latest focused validation for commit `251372e` is
  `just test -p codex-app-server local_controller_initialize_suppresses_pre_participation_notifications in_process::tests::local_controller_socket_uses_main_thread_interface_and_tui_reclaim`
  passing 2/2 focused tests, `just test -p codex-app-server controller`
  passing 94/94 controller-filtered tests, `just fix -p codex-app-server`
  completing after unrelated fixer hunks were reverted, `just fmt` passing,
  `git diff --check` and `git diff --cached --check` passing, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex` in 21.75s.
- The latest focused validation for commit `ae608c3` is
  `just test -p codex-tui controller_reclaim` passing 4/4 focused tests,
  `just fix -p codex-tui` passing, `just fmt` passing, `git diff --check`
  passing, `pre-commit run --files codex-rs/tui/src/app_command.rs` failing
  only because `.pre-commit-config.yaml` is not present, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex` in 15.25s.
- The latest focused validation for commit `02d3d1c` is
  `just test -p codex-app-server controller_main_thread_close_marks_launch_closed`
  passing 1/1 focused test, `just test -p codex-app-server controller`
  passing 95/95 controller-filtered tests, `just test -p codex-app-server`
  ending 1230 passed, 2 flaky passed on retry, 3 failed, and 1 skipped due to
  unrelated hosted-login callback and zsh-fork fixture failures,
  `just fmt` passing, `just fix -p codex-app-server` completing after
  unrelated fixer hunks were reverted, `git diff --check` passing,
  `pre-commit run --all-files` failing only because
  `.pre-commit-config.yaml` is not present, and
  `cargo build -p codex-cli -j 4` rebuilding
  `codex-rs/target/debug/codex`. A separate
  `cargo build -p codex-code-mode-host -j 4` attempt failed before producing a
  fresh host because the `v8` build script could not download the
  `rusty_v8` sandbox archive after Python TLS certificate verification failed;
  the existing `codex-rs/target/debug/codex-code-mode-host` remains present.
- The latest focused validation for commit `3bf6c0d` is
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

## In Flight

- Codex-side selection of the next normal-interface parity slice: remaining
  implicit targets, egress transactionality, and long-lived subscription edges
  not covered by the committed cleanup, resume/turn override gating,
  cursor-binding, internally-sent resume cursor binding, owner-binding,
  controller notification work, serialized-request priority/dequeue reclaim,
  controller auto-subscribe filtering, and terminal sign-off/disconnect
  subscription fencing, plus generic broadcast filtering and targeted
  main-thread lifecycle and status delivery, plus thread-scoped global
  notification targeting, listener-command thread-goal egress targeting, and
  listener-warning targeting, plus extension no-listener goal/warning fallback
  controller targeting and app-server thread-goal fallback targeting, plus
  listener server-request resolution targeting, plus thread-scoped MCP OAuth
  completion targeting, plus controller-origin
  detached review fencing and realtime
  context/configuration override gating, plus realtime text role fencing and
  section-move implicit-target fencing, Guardian-denied TUI approval reclaim
  coverage, archive/delete spawned-descendant subtree fencing, delivered
  controller prompt replay fencing, and extension fallback goal/warning
  controller targeting, listener server-request resolution targeting, and
  embedded in-process transcript/item delivery preservation before the
  app-server-client lossless bridge, plus centralized lossless delivery
  classification shared by the embedded runtime writer and app-server-client,
  terminal local-controller acceptor failure handling, bounded
  external-controller ingress overload, and separate controller control-plane
  ingress, plus pre-participation initialize-notification suppression coverage,
  plus exhaustive TUI command reclaim classification, plus terminal
  main-thread-close launch handling, plus in-process lifecycle/state
  notification preservation.
  There is no known uncommitted Codex-side source diff in that source
  checkpoint.
- Downstream controller-host discovery and display behavior for all live Codex
  launches, including non-Herdr launches.

## Remaining

- Codex app-server should route and validate any remaining server-side binding
  for long-lived subscriptions, implicit targets, and egress transactionality.
  Exact-thread and collection-filtered pagination cursors are now
  connection-bound for controllers, including internally-sent `thread/resume`
  response cursors. Controller-origin `thread/resume` override fields and
  controller-origin turn context/configuration overrides now have pre-dispatch
  gates, owner-aware prompt/current-time replies are owner-epoch-bound, and
  sign-off plus authorization expiry now clean up the controller's main-thread
  subscription. Native TUI-unavailable launch state is now terminal for the
  launch and fences main-thread egress. Unexpected controller disconnect now
  revokes ownership, rebinds pre-delivery prompts, and removes the controller's
  main-thread subscription before RPC drain. Controller authorization and
  ownership notifications now emit from the coordinator transition boundary.
  Primary/TUI serialized requests now preempt queued controller work, and
  queued primary thread input reclaims ownership again at dequeue time.
  Automatic subscription attach now filters external controllers to the
  authorized main thread. Terminal sign-off/disconnect paths now remove
  subscriptions before terminal revocation events or notifications. Generic
  broadcasts now skip external controllers, and main-thread lifecycle
  notifications plus `thread/status/changed` notifications are retargeted only
  to authorized external subscribers. Thread-scoped global notifications now
  add targeted copies for authorized external-controller recipients while
  preserving primary/TUI broadcasts, including live listener-command thread
  goal update/clear/snapshot paths, running-thread resume goal snapshots, and
  listener warning notifications. Extension no-listener goal-update fallback now
  targets thread subscribers instead of relying on raw broadcast. App-server
  `thread/goal` update, clear, and snapshot fallbacks now use the
  controller-aware thread sender. Thread-scoped MCP OAuth completion
  notifications now use controller-aware global notification targeting so
  authorized main-thread controller subscribers receive targeted copies.
  Controller-origin `review/start` now rejects
  detached delivery so active controllers cannot create secondary review threads
  outside the authorized main-thread scope. Controller-origin
  `thread/realtime/start` now rejects context/configuration overrides while
  preserving realtime input/transport startup on the authorized main thread.
  Controller-origin `thread/realtime/appendText` now rejects non-user roles.
  Controller-origin `thread/section/move` now rejects `beforeThreadId`.
  Controller-origin `thread/archive` and `thread/delete` now reject
  spawned-descendant subtree targets before handler dispatch, preserving normal
  TUI subtree behavior while preventing controller implicit secondary-thread
  mutation.
  Running-thread resume replay now skips prompts that already crossed external
  delivery to a controller, so ownership changes or TUI resume do not duplicate
  delivered controller prompts to another connection.
  Embedded in-process app-server delivery now preserves transcript deltas,
  plan/reasoning deltas, item completion, terminal notifications,
  controller-relevant thread lifecycle/state notifications, and controller
  ownership/status notifications under saturation so the client-side lossless
  bridge is not bypassed before the TUI can reflect controller-originated work.
  The app-server-client bridge now delegates its delivery classifier to the
  embedded app-server classifier, avoiding future drift between the producer and
  bridge lossless sets.
  Terminal local-controller acceptor failure now stops the acceptor, reports the
  failure to the embedded runtime, drops socket/metadata guards, prevents
  `mainThreadId` republication through the closed handle, and closes existing
  external-controller connections through the normal revocation path. Late
  endpoint failure now also reports `embedded-unavailable` into the owning TUI.
  Initialized external-controller ingress is now bounded per connection before
  serialized app-server work is enqueued. Saturated controller ingress receives
  JSON-RPC `-32001` with typed `controller-overloaded` data and
  `sameConnection` retry guidance. Controller control-plane RPCs use a separate
  bounded per-connection reservation so saturated normal controller ingress does
  not prevent participation, control acquire/release, or sign-off.
  Pre-participation local-controller socket coverage now verifies an initialized
  controller does not receive startup config-warning notifications before native
  participation is approved.
  Closing or unloading the immutable main thread now marks the launch terminal
  for native external controllers and cancels pending controller-delivered
  prompts with typed `main-thread-closed`.
  Controller-relevant thread lifecycle/state notifications are now also in the
  lossless in-process delivery tier so controller-originated normal-interface
  actions cannot be dropped before the TUI bridge observes them.
- Downstream controller host should discover all live launches through
  local-controller metadata watching/rescanning.
- Downstream display should separate:
  - launch liveness,
  - participation/authorization state,
  - active input ownership, and
  - product slot assignment.
- Downstream auto-assignment should preserve explicit slots and fill free slots
  deterministically for newly discovered live launches.

## Risks or Follow-ups

- A controller product that gates visibility on Herdr `agent_session` can hide
  valid Codex launches.
- A product that maps "not approved" or "not active owner" to offline will
  misrepresent live Codex sessions.
- The implementation plan must continue to avoid durable enrollment or reusable
  controller credentials unless a future design explicitly changes that
  decision.
- Keep Codex native local-controller tests on the TUI approval path so stale
  credential-enrollment assumptions do not mask launch behavior regressions.
- Keep controller admission and target-extraction tables aligned when opening
  additional normal app-server methods to approved controllers.
- Continue validating long-lived subscription behavior; exact-thread and
  collection-filtered pagination cursors now have explicit
  connection/main-thread binding coverage, including internal `thread/resume`
  response sends, while terminal TUI-unavailable now suppresses main-thread
  notifications for existing subscribers, and unexpected disconnect now
  performs early revocation and subscription removal before RPC drain. Any
  remaining subscription work is outside the committed sign-off,
  authorization-expiry cleanup, terminal TUI-unavailable,
  disconnect-revocation, disconnect-subscription-cleanup, and queued
  primary-reclaim paths, plus automatic subscription attach filtering and
  terminal sign-off/disconnect subscription fencing, plus generic broadcast
  filtering and targeted main-thread lifecycle and status delivery, plus
  thread-scoped global notification targeting and listener-command thread-goal
  egress targeting, listener-warning targeting, and extension no-listener
  goal/warning fallback controller targeting, plus app-server thread-goal
  fallback targeting, listener server-request resolution targeting, and
  thread-scoped MCP OAuth completion targeting, plus bounded controller ingress
  overload, separate controller control-plane ingress, and pre-participation
  initialize-notification suppression coverage.
- For the next Codex-side slice, start from source inspection rather than
  assuming the previous interrupted exploration found a confirmed bug.
- Treat the current zsh-fork timeout failures as a separate fixture health issue
  unless a future controller change newly affects that cluster.
- Do not expect a local-controller launch to become available again after a
  native `TuiUnavailable` decision; a fresh Codex launch should publish fresh
  metadata and require a new controller participation request.
- Refreshing `codex-code-mode-host` still depends on resolving the
  `rusty_v8` sandbox archive download/source-build path when the archive is not
  already cached.
