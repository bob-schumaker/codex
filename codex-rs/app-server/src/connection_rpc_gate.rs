use std::future::Future;

use tokio::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;

pub(crate) const EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY: usize = 128;
pub(crate) const EXTERNAL_CONTROLLER_CONTROL_RPC_QUEUE_CAPACITY: usize = 16;

/// Per-connection gate for initialized RPC handler execution.
///
/// Closing the gate prevents queued handlers from starting while allowing
/// handlers that already acquired a token to finish.
#[derive(Debug)]
pub(crate) struct ConnectionRpcGate {
    accepting: Mutex<bool>,
    tasks: TaskTracker,
    external_controller_rpc_permits: std::sync::Arc<Semaphore>,
    external_controller_control_rpc_permits: std::sync::Arc<Semaphore>,
}

#[derive(Debug)]
pub(crate) struct ConnectionRpcReservation {
    _permit: OwnedSemaphorePermit,
}

impl ConnectionRpcGate {
    pub(crate) fn new() -> Self {
        let accepting = true;
        Self {
            accepting: Mutex::new(accepting),
            tasks: TaskTracker::new(),
            external_controller_rpc_permits: std::sync::Arc::new(Semaphore::new(
                EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY,
            )),
            external_controller_control_rpc_permits: std::sync::Arc::new(Semaphore::new(
                EXTERNAL_CONTROLLER_CONTROL_RPC_QUEUE_CAPACITY,
            )),
        }
    }

    pub(crate) fn try_reserve_external_controller_request(
        &self,
    ) -> Option<ConnectionRpcReservation> {
        self.external_controller_rpc_permits
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| ConnectionRpcReservation { _permit: permit })
    }

    pub(crate) fn try_reserve_external_controller_control_request(
        &self,
    ) -> Option<ConnectionRpcReservation> {
        self.external_controller_control_rpc_permits
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| ConnectionRpcReservation { _permit: permit })
    }

    pub(crate) async fn run<F>(&self, future: F)
    where
        F: Future<Output = ()>,
    {
        let token = {
            let accepting = self.accepting.lock().await;
            if !*accepting {
                return;
            }
            self.tasks.token()
        };

        future.await;
        drop(token);
    }

    pub(crate) async fn close(&self) {
        let mut accepting = self.accepting.lock().await;
        *accepting = false;
        self.tasks.close();
    }

    pub(crate) async fn shutdown(&self) {
        self.close().await;
        self.tasks.wait().await;
    }

    #[cfg(test)]
    async fn is_accepting(&self) -> bool {
        *self.accepting.lock().await
    }

    #[cfg(test)]
    fn inflight_count(&self) -> usize {
        self.tasks.len()
    }

    #[cfg(test)]
    fn available_external_controller_request_permits(&self) -> usize {
        self.external_controller_rpc_permits.available_permits()
    }

    #[cfg(test)]
    fn available_external_controller_control_request_permits(&self) -> usize {
        self.external_controller_control_rpc_permits
            .available_permits()
    }
}

impl Default for ConnectionRpcGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use tokio::sync::oneshot;
    use tokio::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn run_executes_while_open() {
        let gate = ConnectionRpcGate::new();
        let ran = Arc::new(AtomicBool::new(/*v*/ false));
        let ran_clone = Arc::clone(&ran);

        gate.run(async move {
            ran_clone.store(/*val*/ true, Ordering::Release);
        })
        .await;

        assert!(ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn run_drops_future_without_polling_after_close() {
        let gate = ConnectionRpcGate::new();
        gate.close().await;
        let polled = Arc::new(AtomicBool::new(/*v*/ false));
        let polled_clone = Arc::clone(&polled);

        gate.run(async move {
            polled_clone.store(/*val*/ true, Ordering::Release);
        })
        .await;

        assert!(!polled.load(Ordering::Acquire));
        assert!(!gate.is_accepting().await);
    }

    #[tokio::test]
    async fn close_returns_while_started_run_remains_active() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    started_tx.send(()).expect("receiver should be open");
                    let _ = finish_rx.await;
                })
                .await;
        });

        started_rx.await.expect("run should start");
        gate.close().await;
        assert!(!gate.is_accepting().await);
        assert_eq!(gate.inflight_count(), 1);

        finish_tx
            .send(())
            .expect("running future should be waiting");
        run_task.await.expect("run task should complete");
        gate.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_started_run_to_finish() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    started_tx.send(()).expect("receiver should be open");
                    let _ = finish_rx.await;
                })
                .await;
        });

        started_rx.await.expect("run should start");
        assert_eq!(gate.inflight_count(), 1);

        let gate_for_shutdown = Arc::clone(&gate);
        let shutdown_task = tokio::spawn(async move {
            gate_for_shutdown.shutdown().await;
        });

        timeout(Duration::from_millis(/*millis*/ 50), shutdown_task)
            .await
            .expect_err("shutdown should wait for the running future");

        finish_tx
            .send(())
            .expect("running future should be waiting");
        run_task.await.expect("run task should complete");
        gate.shutdown().await;
        assert_eq!(gate.inflight_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_drops_late_runs_while_waiting_for_inflight_work() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    started_tx.send(()).expect("receiver should be open");
                    let _ = finish_rx.await;
                })
                .await;
        });

        started_rx.await.expect("run should start");
        let gate_for_shutdown = Arc::clone(&gate);
        let shutdown_task = tokio::spawn(async move {
            gate_for_shutdown.shutdown().await;
        });

        timeout(Duration::from_millis(/*millis*/ 50), shutdown_task)
            .await
            .expect_err("shutdown should wait for the running future");

        let late_polled = Arc::new(AtomicBool::new(/*v*/ false));
        let late_polled_clone = Arc::clone(&late_polled);
        gate.run(async move {
            late_polled_clone.store(/*val*/ true, Ordering::Release);
        })
        .await;

        assert!(!late_polled.load(Ordering::Acquire));

        finish_tx
            .send(())
            .expect("running future should still be waiting");
        run_task.await.expect("run task should complete");
        gate.shutdown().await;
        assert_eq!(gate.inflight_count(), 0);
    }

    #[tokio::test]
    async fn run_is_counted_before_handler_body_continues() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (entered_tx, entered_rx) = oneshot::channel();
        let (continue_tx, continue_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    entered_tx.send(()).expect("receiver should be open");
                    let _ = continue_rx.await;
                })
                .await;
        });

        entered_rx.await.expect("handler body should be entered");
        assert_eq!(gate.inflight_count(), 1);

        continue_tx
            .send(())
            .expect("handler body should still be waiting");
        run_task.await.expect("run task should complete");
        assert_eq!(gate.inflight_count(), 0);
    }

    #[test]
    fn external_controller_reservations_are_bounded_and_released() {
        let gate = ConnectionRpcGate::new();
        let mut reservations = Vec::new();

        for _ in 0..EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY {
            reservations.push(
                gate.try_reserve_external_controller_request()
                    .expect("permit should be available"),
            );
        }

        assert!(gate.try_reserve_external_controller_request().is_none());
        assert_eq!(gate.available_external_controller_request_permits(), 0);

        reservations.pop();

        assert_eq!(gate.available_external_controller_request_permits(), 1);
        assert!(gate.try_reserve_external_controller_request().is_some());
    }

    #[test]
    fn external_controller_control_reservations_are_independent() {
        let gate = ConnectionRpcGate::new();
        let mut normal_reservations = Vec::new();
        for _ in 0..EXTERNAL_CONTROLLER_RPC_QUEUE_CAPACITY {
            normal_reservations.push(
                gate.try_reserve_external_controller_request()
                    .expect("normal permit should be available"),
            );
        }
        assert!(gate.try_reserve_external_controller_request().is_none());

        let mut control_reservations = Vec::new();
        for _ in 0..EXTERNAL_CONTROLLER_CONTROL_RPC_QUEUE_CAPACITY {
            control_reservations.push(
                gate.try_reserve_external_controller_control_request()
                    .expect("control permit should be available"),
            );
        }

        assert!(
            gate.try_reserve_external_controller_control_request()
                .is_none()
        );
        assert_eq!(gate.available_external_controller_request_permits(), 0);
        assert_eq!(
            gate.available_external_controller_control_request_permits(),
            0
        );

        control_reservations.pop();

        assert_eq!(
            gate.available_external_controller_control_request_permits(),
            1
        );
        assert!(
            gate.try_reserve_external_controller_control_request()
                .is_some()
        );
    }
}
