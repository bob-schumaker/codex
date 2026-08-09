use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use codex_app_server_protocol::ClientRequestSerializationScope;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use futures::future::join_all;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::connection_rpc_gate::ConnectionRpcGate;
use crate::connection_rpc_gate::ConnectionRpcReservation;
use crate::outgoing_message::ConnectionId;

type BoxFutureUnit = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

static QUEUED_REQUESTS: Gauge = Gauge::new("app.requests.queued");
const PRIMARY_DEQUEUE_FAIRNESS_LIMIT: usize = 8;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RequestSerializationQueueKey {
    Global(&'static str),
    Thread {
        thread_id: String,
    },
    ThreadPath {
        path: PathBuf,
    },
    CommandExecProcess {
        connection_id: ConnectionId,
        process_id: String,
    },
    Process {
        connection_id: ConnectionId,
        process_handle: String,
    },
    FuzzyFileSearchSession {
        session_id: String,
    },
    FsWatch {
        connection_id: ConnectionId,
        watch_id: String,
    },
    McpOauth {
        server_name: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestSerializationAccess {
    Exclusive,
    SharedRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestSerializationPriority {
    Primary,
    ExternalController,
}

impl RequestSerializationQueueKey {
    pub(crate) fn from_scope(
        connection_id: ConnectionId,
        scope: ClientRequestSerializationScope,
    ) -> (Self, RequestSerializationAccess) {
        match scope {
            ClientRequestSerializationScope::Global(name) => {
                (Self::Global(name), RequestSerializationAccess::Exclusive)
            }
            ClientRequestSerializationScope::GlobalSharedRead(name) => {
                (Self::Global(name), RequestSerializationAccess::SharedRead)
            }
            ClientRequestSerializationScope::Thread { thread_id } => (
                Self::Thread { thread_id },
                RequestSerializationAccess::Exclusive,
            ),
            ClientRequestSerializationScope::ThreadPath { path } => (
                Self::ThreadPath { path },
                RequestSerializationAccess::Exclusive,
            ),
            ClientRequestSerializationScope::CommandExecProcess { process_id } => (
                Self::CommandExecProcess {
                    connection_id,
                    process_id,
                },
                RequestSerializationAccess::Exclusive,
            ),
            ClientRequestSerializationScope::Process { process_handle } => (
                Self::Process {
                    connection_id,
                    process_handle,
                },
                RequestSerializationAccess::Exclusive,
            ),
            ClientRequestSerializationScope::FuzzyFileSearchSession { session_id } => (
                Self::FuzzyFileSearchSession { session_id },
                RequestSerializationAccess::Exclusive,
            ),
            ClientRequestSerializationScope::FsWatch { watch_id } => (
                Self::FsWatch {
                    connection_id,
                    watch_id,
                },
                RequestSerializationAccess::Exclusive,
            ),
            ClientRequestSerializationScope::McpOauth { server_name } => (
                Self::McpOauth { server_name },
                RequestSerializationAccess::Exclusive,
            ),
        }
    }
}

pub(crate) struct QueuedInitializedRequest {
    gate: Option<Arc<ConnectionRpcGate>>,
    _reservation: Option<ConnectionRpcReservation>,
    future: BoxFutureUnit,
}

impl QueuedInitializedRequest {
    pub(crate) fn new(
        gate: Arc<ConnectionRpcGate>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Self {
            gate: Some(gate),
            _reservation: None,
            future: Box::pin(future),
        }
    }

    pub(crate) fn new_with_reservation(
        gate: Arc<ConnectionRpcGate>,
        reservation: ConnectionRpcReservation,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Self {
            gate: Some(gate),
            _reservation: Some(reservation),
            future: Box::pin(future),
        }
    }

    fn new_background(future: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            gate: None,
            _reservation: None,
            future: Box::pin(future),
        }
    }

    pub(crate) async fn run(self) {
        let Self {
            gate,
            _reservation,
            future,
        } = self;
        match gate {
            Some(gate) => gate.run(future).await,
            None => future.await,
        }
    }
}

struct QueuedSerializedRequest {
    access: RequestSerializationAccess,
    priority: RequestSerializationPriority,
    request: QueuedInitializedRequest,
    _diagnostics_guard: GaugeGuard,
}

#[derive(Default)]
struct ActiveSerializedQueue {
    requests: VecDeque<QueuedSerializedRequest>,
    consecutive_primary_dequeues: usize,
}

impl ActiveSerializedQueue {
    fn push_back(&mut self, request: QueuedSerializedRequest) {
        self.requests.push_back(request);
    }

    fn pop_next_batch(&mut self) -> Option<Vec<QueuedSerializedRequest>> {
        let first_index = self.next_request_index()?;
        let controller_waiting = self
            .requests
            .iter()
            .any(|request| request.priority == RequestSerializationPriority::ExternalController);
        let first = self.requests.remove(first_index)?;
        let priority = first.priority;
        let access = first.access;
        let mut requests = vec![first];

        if access == RequestSerializationAccess::SharedRead {
            while self.requests.get(first_index).is_some_and(|request| {
                request.priority == priority
                    && request.access == RequestSerializationAccess::SharedRead
            }) {
                let Some(request) = self.requests.remove(first_index) else {
                    break;
                };
                requests.push(request);
            }
        }

        match priority {
            RequestSerializationPriority::Primary if controller_waiting => {
                self.consecutive_primary_dequeues =
                    self.consecutive_primary_dequeues.saturating_add(1);
            }
            RequestSerializationPriority::Primary => {
                self.consecutive_primary_dequeues = 0;
            }
            RequestSerializationPriority::ExternalController => {
                self.consecutive_primary_dequeues = 0;
            }
        }

        Some(requests)
    }

    fn next_request_index(&self) -> Option<usize> {
        let primary_index = self
            .requests
            .iter()
            .position(|request| request.priority == RequestSerializationPriority::Primary);
        let controller_index = self.requests.iter().position(|request| {
            request.priority == RequestSerializationPriority::ExternalController
        });

        match (primary_index, controller_index) {
            (Some(_), Some(controller_index))
                if self.consecutive_primary_dequeues >= PRIMARY_DEQUEUE_FAIRNESS_LIMIT =>
            {
                Some(controller_index)
            }
            (Some(primary_index), Some(_)) => Some(primary_index),
            (Some(primary_index), None) => Some(primary_index),
            (None, Some(controller_index)) => Some(controller_index),
            (None, None) => None,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct RequestSerializationQueues {
    inner: Arc<Mutex<HashMap<RequestSerializationQueueKey, ActiveSerializedQueue>>>,
}

impl RequestSerializationQueues {
    /// Enqueue app-owned work alongside RPCs that mutate the same serialized resource.
    pub(crate) async fn enqueue_background(
        &self,
        key: RequestSerializationQueueKey,
        access: RequestSerializationAccess,
        future: impl Future<Output = ()> + Send + 'static,
    ) {
        self.enqueue(
            key,
            access,
            QueuedInitializedRequest::new_background(future),
        )
        .await;
    }

    pub(crate) async fn enqueue(
        &self,
        key: RequestSerializationQueueKey,
        access: RequestSerializationAccess,
        request: QueuedInitializedRequest,
    ) {
        self.enqueue_with_priority(key, access, RequestSerializationPriority::Primary, request)
            .await;
    }

    pub(crate) async fn enqueue_with_priority(
        &self,
        key: RequestSerializationQueueKey,
        access: RequestSerializationAccess,
        priority: RequestSerializationPriority,
        request: QueuedInitializedRequest,
    ) {
        let request = QueuedSerializedRequest {
            access,
            priority,
            request,
            _diagnostics_guard: QUEUED_REQUESTS.track(),
        };
        let should_spawn = {
            let mut queues = self.inner.lock().await;
            match queues.get_mut(&key) {
                Some(queue) => {
                    queue.push_back(request);
                    false
                }
                None => {
                    let mut queue = ActiveSerializedQueue::default();
                    queue.push_back(request);
                    queues.insert(key.clone(), queue);
                    true
                }
            }
        };

        if should_spawn {
            let queues = self.clone();
            let span = tracing::debug_span!("app_server.serialized_request_queue", ?key);
            tokio::spawn(async move { queues.drain(key).await }.instrument(span));
        }
    }

    async fn drain(self, key: RequestSerializationQueueKey) {
        loop {
            let requests = {
                let mut queues = self.inner.lock().await;
                let Some(queue) = queues.get_mut(&key) else {
                    return;
                };
                match queue.pop_next_batch() {
                    Some(requests) => requests,
                    None => {
                        queues.remove(&key);
                        return;
                    }
                }
            };

            join_all(requests.into_iter().map(|request| request.request.run())).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_rpc_gate::EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::time::Duration;
    use tokio::time::timeout;

    const FIRST_REQUEST_VALUE: i32 = 1;
    const SECOND_REQUEST_VALUE: i32 = 2;
    const THIRD_REQUEST_VALUE: i32 = 3;

    fn gate() -> Arc<ConnectionRpcGate> {
        Arc::new(ConnectionRpcGate::new())
    }

    fn queue_drain_timeout() -> Duration {
        Duration::from_secs(/*secs*/ 1)
    }

    fn shutdown_wait_timeout() -> Duration {
        Duration::from_millis(/*millis*/ 50)
    }

    #[tokio::test]
    async fn same_key_requests_run_fifo() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let gate = gate();
        let (tx, mut rx) = mpsc::unbounded_channel();

        for value in [
            FIRST_REQUEST_VALUE,
            SECOND_REQUEST_VALUE,
            THIRD_REQUEST_VALUE,
        ] {
            let tx = tx.clone();
            queues
                .enqueue(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    QueuedInitializedRequest::new(Arc::clone(&gate), async move {
                        tx.send(value).expect("receiver should be open");
                    }),
                )
                .await;
        }
        drop(tx);

        let mut values = Vec::new();
        while let Some(value) = timeout(queue_drain_timeout(), rx.recv())
            .await
            .expect("timed out waiting for queued request")
        {
            values.push(value);
        }

        assert_eq!(
            values,
            vec![
                FIRST_REQUEST_VALUE,
                SECOND_REQUEST_VALUE,
                THIRD_REQUEST_VALUE
            ]
        );
    }

    #[tokio::test]
    async fn primary_priority_runs_before_queued_controller_work() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel::<()>();
        let (blocker_release_tx, blocker_release_rx) = oneshot::channel::<()>();
        let (tx, mut rx) = mpsc::unbounded_channel();

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    blocker_started_tx
                        .send(())
                        .expect("receiver should be open");
                    let _ = blocker_release_rx.await;
                }),
            )
            .await;
        timeout(queue_drain_timeout(), blocker_started_rx)
            .await
            .expect("blocker should start")
            .expect("sender should be open");

        {
            let tx = tx.clone();
            queues
                .enqueue_with_priority(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    RequestSerializationPriority::ExternalController,
                    QueuedInitializedRequest::new(gate(), async move {
                        tx.send(SECOND_REQUEST_VALUE)
                            .expect("receiver should be open");
                    }),
                )
                .await;
        }
        {
            let tx = tx.clone();
            queues
                .enqueue_with_priority(
                    key,
                    RequestSerializationAccess::Exclusive,
                    RequestSerializationPriority::Primary,
                    QueuedInitializedRequest::new(gate(), async move {
                        tx.send(FIRST_REQUEST_VALUE)
                            .expect("receiver should be open");
                    }),
                )
                .await;
        }
        drop(tx);
        blocker_release_tx
            .send(())
            .expect("blocker should still be waiting");

        let mut values = Vec::new();
        while let Some(value) = timeout(queue_drain_timeout(), rx.recv())
            .await
            .expect("timed out waiting for queue to drain")
        {
            values.push(value);
        }

        assert_eq!(values, vec![FIRST_REQUEST_VALUE, SECOND_REQUEST_VALUE]);
    }

    #[tokio::test]
    async fn controller_work_runs_after_eight_primary_dequeues() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel::<()>();
        let (blocker_release_tx, blocker_release_rx) = oneshot::channel::<()>();
        let (tx, mut rx) = mpsc::unbounded_channel();

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    blocker_started_tx
                        .send(())
                        .expect("receiver should be open");
                    let _ = blocker_release_rx.await;
                }),
            )
            .await;
        timeout(queue_drain_timeout(), blocker_started_rx)
            .await
            .expect("blocker should start")
            .expect("sender should be open");

        {
            let tx = tx.clone();
            queues
                .enqueue_with_priority(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    RequestSerializationPriority::ExternalController,
                    QueuedInitializedRequest::new(gate(), async move {
                        tx.send(/*controller*/ 100)
                            .expect("receiver should be open");
                    }),
                )
                .await;
        }

        for value in 1..=9 {
            let tx = tx.clone();
            queues
                .enqueue_with_priority(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    RequestSerializationPriority::Primary,
                    QueuedInitializedRequest::new(gate(), async move {
                        tx.send(value).expect("receiver should be open");
                    }),
                )
                .await;
        }
        drop(tx);
        blocker_release_tx
            .send(())
            .expect("blocker should still be waiting");

        let mut values = Vec::new();
        while let Some(value) = timeout(queue_drain_timeout(), rx.recv())
            .await
            .expect("timed out waiting for queue to drain")
        {
            values.push(value);
        }

        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 100, 9]);
    }

    #[tokio::test]
    async fn different_keys_run_concurrently() {
        let queues = RequestSerializationQueues::default();
        let (blocked_tx, blocked_rx) = oneshot::channel::<()>();
        let (ran_tx, ran_rx) = oneshot::channel::<()>();

        queues
            .enqueue(
                RequestSerializationQueueKey::Global("blocked"),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    let _ = blocked_rx.await;
                }),
            )
            .await;
        queues
            .enqueue(
                RequestSerializationQueueKey::Global("other"),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    ran_tx.send(()).expect("receiver should be open");
                }),
            )
            .await;

        timeout(queue_drain_timeout(), ran_rx)
            .await
            .expect("other key should not be blocked")
            .expect("sender should be open");
        blocked_tx
            .send(())
            .expect("blocked request should be waiting");
    }

    #[tokio::test]
    async fn closed_gate_request_is_skipped_and_following_requests_continue() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let live_gate = gate();
        let closed_gate = gate();
        closed_gate.close().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (blocked_tx, blocked_rx) = oneshot::channel::<()>();

        {
            let tx = tx.clone();
            queues
                .enqueue(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    QueuedInitializedRequest::new(Arc::clone(&live_gate), async move {
                        tx.send(FIRST_REQUEST_VALUE)
                            .expect("receiver should be open");
                        let _ = blocked_rx.await;
                    }),
                )
                .await;
        }
        {
            let tx = tx.clone();
            queues
                .enqueue(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    QueuedInitializedRequest::new(closed_gate, async move {
                        tx.send(SECOND_REQUEST_VALUE)
                            .expect("receiver should be open");
                    }),
                )
                .await;
        }
        {
            let tx = tx.clone();
            queues
                .enqueue(
                    key,
                    RequestSerializationAccess::Exclusive,
                    QueuedInitializedRequest::new(live_gate, async move {
                        tx.send(THIRD_REQUEST_VALUE)
                            .expect("receiver should be open");
                    }),
                )
                .await;
        }
        drop(tx);

        assert_eq!(
            timeout(queue_drain_timeout(), rx.recv())
                .await
                .expect("timed out waiting for first request"),
            Some(FIRST_REQUEST_VALUE)
        );
        blocked_tx
            .send(())
            .expect("blocked request should be waiting");

        let mut values = Vec::new();
        while let Some(value) = timeout(queue_drain_timeout(), rx.recv())
            .await
            .expect("timed out waiting for queue to drain")
        {
            values.push(value);
        }

        assert_eq!(values, vec![THIRD_REQUEST_VALUE]);
    }

    #[tokio::test]
    async fn shutdown_of_live_gate_skips_already_queued_requests() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let live_gate = gate();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (blocked_tx, blocked_rx) = oneshot::channel::<()>();

        {
            let tx = tx.clone();
            queues
                .enqueue(
                    key.clone(),
                    RequestSerializationAccess::Exclusive,
                    QueuedInitializedRequest::new(Arc::clone(&live_gate), async move {
                        tx.send(FIRST_REQUEST_VALUE)
                            .expect("receiver should be open");
                        let _ = blocked_rx.await;
                    }),
                )
                .await;
        }
        {
            let tx = tx.clone();
            queues
                .enqueue(
                    key,
                    RequestSerializationAccess::Exclusive,
                    QueuedInitializedRequest::new(live_gate.clone(), async move {
                        tx.send(SECOND_REQUEST_VALUE)
                            .expect("receiver should be open");
                    }),
                )
                .await;
        }
        drop(tx);

        assert_eq!(
            timeout(queue_drain_timeout(), rx.recv())
                .await
                .expect("timed out waiting for first request"),
            Some(FIRST_REQUEST_VALUE)
        );

        let gate_for_shutdown = Arc::clone(&live_gate);
        let shutdown_task = tokio::spawn(async move {
            gate_for_shutdown.shutdown().await;
        });

        timeout(shutdown_wait_timeout(), shutdown_task)
            .await
            .expect_err("shutdown should wait for the running request");

        blocked_tx
            .send(())
            .expect("blocked request should still be waiting");

        assert_eq!(
            timeout(queue_drain_timeout(), rx.recv())
                .await
                .expect("timed out waiting for queue to drain"),
            None
        );
    }

    #[tokio::test]
    async fn external_controller_reservation_is_released_after_request_runs() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let gate = gate();
        let request_reservation = gate
            .try_reserve_external_controller_request()
            .expect("request reservation should be available");
        let mut held_reservations = Vec::new();
        for _ in 1..EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY {
            held_reservations.push(
                gate.try_reserve_external_controller_request()
                    .expect("filler reservation should be available"),
            );
        }
        assert!(gate.try_reserve_external_controller_request().is_none());
        let (ran_tx, ran_rx) = oneshot::channel::<()>();

        queues
            .enqueue(
                key,
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new_with_reservation(
                    Arc::clone(&gate),
                    request_reservation,
                    async move {
                        ran_tx.send(()).expect("receiver should be open");
                    },
                ),
            )
            .await;

        timeout(queue_drain_timeout(), ran_rx)
            .await
            .expect("queued request should run")
            .expect("sender should be open");
        assert!(gate.try_reserve_external_controller_request().is_some());
    }

    #[tokio::test]
    async fn same_key_shared_reads_run_concurrently() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel::<()>();
        let (blocker_release_tx, blocker_release_rx) = oneshot::channel::<()>();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (release_tx, _) = broadcast::channel::<()>(/*capacity*/ 1);

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    blocker_started_tx
                        .send(())
                        .expect("receiver should be open");
                    let _ = blocker_release_rx.await;
                }),
            )
            .await;
        timeout(queue_drain_timeout(), blocker_started_rx)
            .await
            .expect("blocker should start")
            .expect("sender should be open");

        for value in [FIRST_REQUEST_VALUE, SECOND_REQUEST_VALUE] {
            let started_tx = started_tx.clone();
            let mut release_rx = release_tx.subscribe();
            queues
                .enqueue(
                    key.clone(),
                    RequestSerializationAccess::SharedRead,
                    QueuedInitializedRequest::new(gate(), async move {
                        started_tx.send(value).expect("receiver should be open");
                        let _ = release_rx.recv().await;
                    }),
                )
                .await;
        }
        drop(started_tx);
        blocker_release_tx
            .send(())
            .expect("blocker should still be waiting");

        let mut started = Vec::new();
        for _ in 0..2 {
            started.push(
                timeout(queue_drain_timeout(), started_rx.recv())
                    .await
                    .expect("timed out waiting for shared read")
                    .expect("sender should be open"),
            );
        }
        assert_eq!(started, vec![FIRST_REQUEST_VALUE, SECOND_REQUEST_VALUE]);

        release_tx
            .send(())
            .expect("shared reads should still be waiting");
    }

    #[tokio::test]
    async fn exclusive_write_waits_for_running_shared_reads() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel::<()>();
        let (blocker_release_tx, blocker_release_rx) = oneshot::channel::<()>();
        let (read_started_tx, mut read_started_rx) = mpsc::unbounded_channel();
        let (read_release_tx, _) = broadcast::channel::<()>(/*capacity*/ 1);
        let (write_started_tx, write_started_rx) = oneshot::channel::<()>();

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    blocker_started_tx
                        .send(())
                        .expect("receiver should be open");
                    let _ = blocker_release_rx.await;
                }),
            )
            .await;
        timeout(queue_drain_timeout(), blocker_started_rx)
            .await
            .expect("blocker should start")
            .expect("sender should be open");

        for value in [FIRST_REQUEST_VALUE, SECOND_REQUEST_VALUE] {
            let read_started_tx = read_started_tx.clone();
            let mut read_release_rx = read_release_tx.subscribe();
            queues
                .enqueue(
                    key.clone(),
                    RequestSerializationAccess::SharedRead,
                    QueuedInitializedRequest::new(gate(), async move {
                        read_started_tx
                            .send(value)
                            .expect("receiver should be open");
                        let _ = read_release_rx.recv().await;
                    }),
                )
                .await;
        }
        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    write_started_tx.send(()).expect("receiver should be open");
                }),
            )
            .await;
        drop(read_started_tx);
        blocker_release_tx
            .send(())
            .expect("blocker should still be waiting");

        for _ in 0..2 {
            timeout(queue_drain_timeout(), read_started_rx.recv())
                .await
                .expect("timed out waiting for shared read")
                .expect("sender should be open");
        }
        let mut write_started_rx = Box::pin(write_started_rx);
        timeout(shutdown_wait_timeout(), &mut write_started_rx)
            .await
            .expect_err("write should wait for running shared reads");

        read_release_tx
            .send(())
            .expect("shared reads should still be waiting");
        timeout(queue_drain_timeout(), &mut write_started_rx)
            .await
            .expect("write should start after shared reads finish")
            .expect("sender should be open");
    }

    #[tokio::test]
    async fn later_shared_reads_do_not_jump_ahead_of_queued_write() {
        let queues = RequestSerializationQueues::default();
        let key = RequestSerializationQueueKey::Global("test");
        let (blocker_started_tx, blocker_started_rx) = oneshot::channel::<()>();
        let (blocker_release_tx, blocker_release_rx) = oneshot::channel::<()>();
        let (first_read_started_tx, first_read_started_rx) = oneshot::channel::<()>();
        let (first_read_release_tx, first_read_release_rx) = oneshot::channel::<()>();
        let (write_started_tx, write_started_rx) = oneshot::channel::<()>();
        let (write_release_tx, write_release_rx) = oneshot::channel::<()>();
        let (later_read_started_tx, later_read_started_rx) = oneshot::channel::<()>();

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    blocker_started_tx
                        .send(())
                        .expect("receiver should be open");
                    let _ = blocker_release_rx.await;
                }),
            )
            .await;
        timeout(queue_drain_timeout(), blocker_started_rx)
            .await
            .expect("blocker should start")
            .expect("sender should be open");

        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::SharedRead,
                QueuedInitializedRequest::new(gate(), async move {
                    first_read_started_tx
                        .send(())
                        .expect("receiver should be open");
                    let _ = first_read_release_rx.await;
                }),
            )
            .await;
        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::Exclusive,
                QueuedInitializedRequest::new(gate(), async move {
                    write_started_tx.send(()).expect("receiver should be open");
                    let _ = write_release_rx.await;
                }),
            )
            .await;
        queues
            .enqueue(
                key.clone(),
                RequestSerializationAccess::SharedRead,
                QueuedInitializedRequest::new(gate(), async move {
                    later_read_started_tx
                        .send(())
                        .expect("receiver should be open");
                }),
            )
            .await;
        blocker_release_tx
            .send(())
            .expect("blocker should still be waiting");

        timeout(queue_drain_timeout(), first_read_started_rx)
            .await
            .expect("first read should start")
            .expect("sender should be open");
        let mut write_started_rx = Box::pin(write_started_rx);
        timeout(shutdown_wait_timeout(), &mut write_started_rx)
            .await
            .expect_err("write should wait for the first read");
        let mut later_read_started_rx = Box::pin(later_read_started_rx);
        timeout(shutdown_wait_timeout(), &mut later_read_started_rx)
            .await
            .expect_err("later read should wait behind the queued write");

        first_read_release_tx
            .send(())
            .expect("first read should still be waiting");
        timeout(queue_drain_timeout(), &mut write_started_rx)
            .await
            .expect("write should start after the first read")
            .expect("sender should be open");
        timeout(shutdown_wait_timeout(), &mut later_read_started_rx)
            .await
            .expect_err("later read should still wait while the write is running");

        write_release_tx
            .send(())
            .expect("write should still be waiting");
        timeout(queue_drain_timeout(), &mut later_read_started_rx)
            .await
            .expect("later read should start after the write")
            .expect("sender should be open");
    }
}
