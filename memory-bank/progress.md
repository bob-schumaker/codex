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
- The latest Codex-side source checkpoint is
  `09768dc` for launch metadata publication, native approval coverage, exact
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
  coverage, archive/delete spawned-descendant subtree fencing, delivered
  controller prompt replay fencing, and extension fallback goal/warning
  controller targeting.
- Focused app-server controller/current-time/cursor tests pass. The latest full
  `just test -p codex-app-server` run ended 1192 passed, 3 flaky passed on
  retry, 1 leaky, 2 zsh-fork failures after retry, and 1 skipped; the
  zsh-fork cluster remains a separate fixture health issue outside the
  controller slice.
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
  thread-scoped MCP OAuth completion targeting, plus controller-origin
  detached review fencing and realtime
  context/configuration override gating, plus realtime text role fencing and
  section-move implicit-target fencing, Guardian-denied TUI approval reclaim
  coverage, archive/delete spawned-descendant subtree fencing, delivered
  controller prompt replay fencing, and extension fallback goal/warning
  controller targeting. There is no known uncommitted Codex-side source diff in
  this checkpoint.
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
  fallback targeting and thread-scoped MCP OAuth completion targeting.
- For the next Codex-side slice, start from source inspection rather than
  assuming the previous interrupted exploration found a confirmed bug.
- Treat the current zsh-fork timeout failures as a separate fixture health issue
  unless a future controller change newly affects that cluster.
- Do not expect a local-controller launch to become available again after a
  native `TuiUnavailable` decision; a fresh Codex launch should publish fresh
  metadata and require a new controller participation request.
