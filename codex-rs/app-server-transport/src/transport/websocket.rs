use super::CHANNEL_CAPACITY;
use super::ConnectionOrigin;
use super::TransportEvent;
use super::auth::WebsocketAuthPolicy;
use super::auth::authorize_upgrade;
use super::auth::is_unauthenticated_non_loopback_listener;
use super::forward_incoming_message;
use super::next_connection_id;
use super::serialize_outgoing_message;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::QueuedOutgoingMessage;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::ConnectInfo;
use axum::extract::State;
use axum::extract::ws::Message as AxumWebSocketMessage;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::ORIGIN;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::any;
use axum::routing::get;
use futures::SinkExt;
use futures::StreamExt;
use owo_colors::OwoColorize;
use owo_colors::Stream;
use owo_colors::Style;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "test-support")]
use std::sync::atomic::AtomicBool;
#[cfg(feature = "test-support")]
use std::sync::atomic::Ordering;
use tokio::net::TcpListener;
#[cfg(feature = "test-support")]
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as TungsteniteWebSocketMessage;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

/// WebSocket clients can briefly lag behind normal turn output bursts while the
/// writer task is healthy, so give them more headroom than internal channels.
const WEBSOCKET_OUTBOUND_CHANNEL_CAPACITY: usize = 32 * 1024;
const _: () = assert!(WEBSOCKET_OUTBOUND_CHANNEL_CAPACITY > CHANNEL_CAPACITY);

/// Test-only control for one local-controller writer failure after it has
/// consumed an external prompt's write permit.
#[cfg(feature = "test-support")]
#[derive(Clone, Debug)]
pub struct LocalControllerWriterTestGate {
    state: Arc<LocalControllerWriterTestGateState>,
}

#[cfg(feature = "test-support")]
#[derive(Debug)]
struct LocalControllerWriterTestGateState {
    armed: AtomicBool,
    write_started: AtomicBool,
    fail_next_write: AtomicBool,
    released: AtomicBool,
    pause_before_next_queue_receive: AtomicBool,
    writer_paused: AtomicBool,
    write_started_notify: Notify,
    writer_paused_notify: Notify,
    queue_pause_notify: Notify,
    release_notify: Notify,
    outbound_channel_capacity: usize,
}

#[cfg(feature = "test-support")]
impl LocalControllerWriterTestGate {
    pub fn pause_after_write_started() -> Self {
        Self {
            state: Arc::new(LocalControllerWriterTestGateState {
                armed: AtomicBool::new(false),
                write_started: AtomicBool::new(false),
                fail_next_write: AtomicBool::new(false),
                released: AtomicBool::new(false),
                pause_before_next_queue_receive: AtomicBool::new(false),
                writer_paused: AtomicBool::new(false),
                write_started_notify: Notify::new(),
                writer_paused_notify: Notify::new(),
                queue_pause_notify: Notify::new(),
                release_notify: Notify::new(),
                outbound_channel_capacity: WEBSOCKET_OUTBOUND_CHANNEL_CAPACITY,
            }),
        }
    }

    pub fn pause_before_queue_receive() -> Self {
        Self {
            state: Arc::new(LocalControllerWriterTestGateState {
                armed: AtomicBool::new(false),
                write_started: AtomicBool::new(false),
                fail_next_write: AtomicBool::new(false),
                released: AtomicBool::new(false),
                pause_before_next_queue_receive: AtomicBool::new(false),
                writer_paused: AtomicBool::new(false),
                write_started_notify: Notify::new(),
                writer_paused_notify: Notify::new(),
                queue_pause_notify: Notify::new(),
                release_notify: Notify::new(),
                outbound_channel_capacity: 4,
            }),
        }
    }

    pub fn arm_next_write_failure(&self) {
        self.state.armed.store(true, Ordering::Release);
    }

    pub fn arm_queue_pause(&self) {
        self.state
            .pause_before_next_queue_receive
            .store(true, Ordering::Release);
        self.state.queue_pause_notify.notify_waiters();
    }

    pub async fn wait_until_writer_paused(&self) {
        while !self.state.writer_paused.load(Ordering::Acquire) {
            self.state.writer_paused_notify.notified().await;
        }
    }

    pub async fn wait_until_write_started(&self) {
        while !self.state.write_started.load(Ordering::Acquire) {
            self.state.write_started_notify.notified().await;
        }
    }

    pub fn fail_next_write_and_release(&self) {
        self.state.fail_next_write.store(true, Ordering::Release);
        self.state.released.store(true, Ordering::Release);
        self.state.release_notify.notify_waiters();
    }

    async fn pause_after_write_started_if_armed(&self) -> bool {
        if !self.state.armed.swap(false, Ordering::AcqRel) {
            return false;
        }
        self.state.write_started.store(true, Ordering::Release);
        self.state.write_started_notify.notify_waiters();
        while !self.state.released.load(Ordering::Acquire) {
            self.state.release_notify.notified().await;
        }
        self.state.fail_next_write.swap(false, Ordering::AcqRel)
    }

    fn outbound_channel_capacity(&self) -> usize {
        self.state.outbound_channel_capacity
    }

    async fn pause_before_queue_receive_if_armed(&self, disconnect_token: &CancellationToken) {
        if !self
            .state
            .pause_before_next_queue_receive
            .swap(false, Ordering::AcqRel)
        {
            return;
        }
        self.state.writer_paused.store(true, Ordering::Release);
        self.state.writer_paused_notify.notify_waiters();
        loop {
            if self.state.released.load(Ordering::Acquire) {
                return;
            }
            tokio::select! {
                _ = self.state.release_notify.notified() => {}
                _ = disconnect_token.cancelled() => return,
            }
        }
    }

    async fn wait_for_queue_pause_arm(&self) {
        while !self
            .state
            .pause_before_next_queue_receive
            .load(Ordering::Acquire)
        {
            self.state.queue_pause_notify.notified().await;
        }
    }
}

fn colorize(text: &str, style: Style) -> String {
    text.if_supports_color(Stream::Stderr, |value| value.style(style))
        .to_string()
}

#[allow(clippy::print_stderr)]
fn print_websocket_startup_banner(addr: SocketAddr) {
    let title = colorize("codex app-server (WebSockets)", Style::new().bold().cyan());
    let listening_label = colorize("listening on:", Style::new().dimmed());
    let listen_url = colorize(&format!("ws://{addr}"), Style::new().green());
    let ready_label = colorize("readyz:", Style::new().dimmed());
    let ready_url = colorize(&format!("http://{addr}/readyz"), Style::new().green());
    let health_label = colorize("healthz:", Style::new().dimmed());
    let health_url = colorize(&format!("http://{addr}/healthz"), Style::new().green());
    let note_label = colorize("note:", Style::new().dimmed());
    eprintln!("{title}");
    eprintln!("  {listening_label} {listen_url}");
    eprintln!("  {ready_label} {ready_url}");
    eprintln!("  {health_label} {health_url}");
    if addr.ip().is_loopback() {
        eprintln!(
            "  {note_label} binds localhost only (use SSH port-forwarding for remote access)"
        );
    } else {
        eprintln!("  {note_label} websocket auth is required for non-localhost listeners");
    }
}

#[derive(Clone)]
struct WebSocketListenerState {
    transport_event_tx: mpsc::Sender<TransportEvent>,
    auth_policy: Arc<WebsocketAuthPolicy>,
}

async fn health_check_handler() -> StatusCode {
    StatusCode::OK
}

async fn reject_requests_with_origin_header(
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if request.headers().contains_key(ORIGIN) {
        warn!(
            method = %request.method(),
            uri = %request.uri(),
            "rejecting websocket listener request with Origin header"
        );
        Err(StatusCode::FORBIDDEN)
    } else {
        Ok(next.run(request).await)
    }
}

async fn websocket_upgrade_handler(
    websocket: WebSocketUpgrade,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    State(state): State<WebSocketListenerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(err) = authorize_upgrade(&headers, state.auth_policy.as_ref()) {
        warn!(
            %peer_addr,
            message = err.message(),
            "rejecting websocket client during upgrade"
        );
        return (err.status_code(), err.message()).into_response();
    }
    info!(%peer_addr, "websocket client connected");
    websocket
        .on_upgrade(move |stream| async move {
            let (websocket_writer, websocket_reader) = stream.split();
            run_websocket_connection(
                websocket_writer,
                websocket_reader,
                state.transport_event_tx,
                ConnectionOrigin::WebSocket,
                #[cfg(feature = "test-support")]
                None,
            )
            .await;
        })
        .into_response()
}

pub async fn start_websocket_acceptor(
    bind_address: SocketAddr,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
    auth_policy: WebsocketAuthPolicy,
) -> IoResult<JoinHandle<()>> {
    if is_unauthenticated_non_loopback_listener(bind_address, &auth_policy) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to start non-loopback websocket listener {bind_address} without auth; configure `--ws-auth capability-token` or `--ws-auth signed-bearer-token`"
            ),
        ));
    }
    let listener = TcpListener::bind(bind_address).await?;
    let local_addr = listener.local_addr()?;
    print_websocket_startup_banner(local_addr);
    info!("app-server websocket listening on ws://{local_addr}");

    let router = Router::new()
        .route("/readyz", get(health_check_handler))
        .route("/healthz", get(health_check_handler))
        .fallback(any(websocket_upgrade_handler))
        .layer(middleware::from_fn(reject_requests_with_origin_header))
        .with_state(WebSocketListenerState {
            transport_event_tx,
            auth_policy: Arc::new(auth_policy),
        });
    let server = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_token.cancelled().await;
    });
    Ok(tokio::spawn(async move {
        if let Err(err) = server.await {
            error!("websocket acceptor failed: {err}");
        }
        info!("websocket acceptor shutting down");
    }))
}

pub(crate) async fn run_websocket_connection<M, SinkError, StreamError>(
    websocket_writer: impl futures::sink::Sink<M, Error = SinkError> + Send + 'static,
    websocket_reader: impl futures::stream::Stream<Item = Result<M, StreamError>> + Send + 'static,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    origin: ConnectionOrigin,
    #[cfg(feature = "test-support")] writer_test_gate: Option<LocalControllerWriterTestGate>,
) where
    M: AppServerWebSocketMessage + Send + 'static,
    SinkError: Send + 'static,
    StreamError: std::fmt::Display + Send + 'static,
{
    let connection_id = next_connection_id();
    #[cfg(feature = "test-support")]
    let outbound_channel_capacity = writer_test_gate.as_ref().map_or(
        WEBSOCKET_OUTBOUND_CHANNEL_CAPACITY,
        LocalControllerWriterTestGate::outbound_channel_capacity,
    );
    #[cfg(not(feature = "test-support"))]
    let outbound_channel_capacity = WEBSOCKET_OUTBOUND_CHANNEL_CAPACITY;
    let (writer_tx, writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(outbound_channel_capacity);
    let writer_tx_for_reader = writer_tx.clone();
    let disconnect_token = CancellationToken::new();
    if transport_event_tx
        .send(TransportEvent::ConnectionOpened {
            connection_id,
            origin,
            writer: writer_tx,
            disconnect_sender: Some(disconnect_token.clone()),
        })
        .await
        .is_err()
    {
        return;
    }

    let (writer_control_tx, writer_control_rx) = mpsc::channel::<M>(CHANNEL_CAPACITY);
    let mut outbound_task = tokio::spawn(run_websocket_outbound_loop(
        websocket_writer,
        writer_rx,
        writer_control_rx,
        disconnect_token.clone(),
        #[cfg(feature = "test-support")]
        writer_test_gate,
    ));
    let mut inbound_task = tokio::spawn(run_websocket_inbound_loop(
        websocket_reader,
        transport_event_tx.clone(),
        writer_tx_for_reader,
        writer_control_tx,
        connection_id,
        disconnect_token.clone(),
    ));

    tokio::select! {
        _ = &mut outbound_task => {
            disconnect_token.cancel();
            inbound_task.abort();
        }
        _ = &mut inbound_task => {
            disconnect_token.cancel();
            outbound_task.abort();
        }
    }

    let _ = transport_event_tx
        .send(TransportEvent::ConnectionClosed { connection_id })
        .await;
}

pub(crate) enum IncomingWebSocketMessage {
    Text(String),
    Binary,
    Ping(Bytes),
    Pong,
    Close,
}

/// Converts concrete WebSocket message types into the small message surface the
/// app-server transport needs, and constructs the only outbound frames it
/// sends directly.
pub(crate) trait AppServerWebSocketMessage: Sized {
    fn text(text: String) -> Self;
    fn pong(payload: Bytes) -> Self;
    fn into_incoming(self) -> Option<IncomingWebSocketMessage>;
}

impl AppServerWebSocketMessage for AxumWebSocketMessage {
    fn text(text: String) -> Self {
        Self::Text(text.into())
    }

    fn pong(payload: Bytes) -> Self {
        Self::Pong(payload)
    }

    fn into_incoming(self) -> Option<IncomingWebSocketMessage> {
        Some(match self {
            Self::Text(text) => IncomingWebSocketMessage::Text(text.to_string()),
            Self::Binary(_) => IncomingWebSocketMessage::Binary,
            Self::Ping(payload) => IncomingWebSocketMessage::Ping(payload),
            Self::Pong(_) => IncomingWebSocketMessage::Pong,
            Self::Close(_) => IncomingWebSocketMessage::Close,
        })
    }
}

impl AppServerWebSocketMessage for TungsteniteWebSocketMessage {
    fn text(text: String) -> Self {
        Self::Text(text.into())
    }

    fn pong(payload: Bytes) -> Self {
        Self::Pong(payload)
    }

    fn into_incoming(self) -> Option<IncomingWebSocketMessage> {
        Some(match self {
            Self::Text(text) => IncomingWebSocketMessage::Text(text.to_string()),
            Self::Binary(_) => IncomingWebSocketMessage::Binary,
            Self::Ping(payload) => IncomingWebSocketMessage::Ping(payload),
            Self::Pong(_) => IncomingWebSocketMessage::Pong,
            Self::Close(_) => IncomingWebSocketMessage::Close,
            Self::Frame(_) => return None,
        })
    }
}

async fn run_websocket_outbound_loop<M, SinkError>(
    websocket_writer: impl futures::sink::Sink<M, Error = SinkError> + Send + 'static,
    mut writer_rx: mpsc::Receiver<QueuedOutgoingMessage>,
    mut writer_control_rx: mpsc::Receiver<M>,
    disconnect_token: CancellationToken,
    #[cfg(feature = "test-support")] writer_test_gate: Option<LocalControllerWriterTestGate>,
) where
    M: AppServerWebSocketMessage + Send + 'static,
    SinkError: Send + 'static,
{
    tokio::pin!(websocket_writer);
    loop {
        tokio::select! {
            _ = wait_for_queue_pause_arm(
                #[cfg(feature = "test-support")]
                writer_test_gate.as_ref(),
            ) => {
                #[cfg(feature = "test-support")]
                writer_test_gate
                    .as_ref()
                    .expect("queue pause branch requires a writer test gate")
                    .pause_before_queue_receive_if_armed(&disconnect_token)
                    .await;
            }
            _ = disconnect_token.cancelled() => {
                break;
            }
            message = writer_control_rx.recv() => {
                let Some(message) = message else {
                    break;
                };
                if websocket_writer.send(message).await.is_err() {
                    break;
                }
            }
            queued_message = writer_rx.recv() => {
                let Some(queued_message) = queued_message else {
                    break;
                };
                if !queued_message.begin_write() {
                    continue;
                }
                #[cfg(feature = "test-support")]
                if queued_message.write_complete_tx.is_some()
                    && let Some(writer_test_gate) = &writer_test_gate
                    && writer_test_gate.pause_after_write_started_if_armed().await
                {
                    break;
                }
                let Some(json) = serialize_outgoing_message(queued_message.message) else {
                    continue;
                };
                if websocket_writer.send(M::text(json)).await.is_err() {
                    break;
                }
                if let Some(write_complete_tx) = queued_message.write_complete_tx {
                    write_complete_tx.complete();
                }
            }
        }
    }
}

async fn wait_for_queue_pause_arm(
    #[cfg(feature = "test-support")] writer_test_gate: Option<&LocalControllerWriterTestGate>,
) {
    #[cfg(feature = "test-support")]
    {
        match writer_test_gate {
            Some(writer_test_gate) => writer_test_gate.wait_for_queue_pause_arm().await,
            None => std::future::pending::<()>().await,
        }
    }
    #[cfg(not(feature = "test-support"))]
    std::future::pending::<()>().await;
}

async fn run_websocket_inbound_loop<M, StreamError>(
    websocket_reader: impl futures::stream::Stream<Item = Result<M, StreamError>> + Send + 'static,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    writer_tx_for_reader: mpsc::Sender<QueuedOutgoingMessage>,
    writer_control_tx: mpsc::Sender<M>,
    connection_id: ConnectionId,
    disconnect_token: CancellationToken,
) where
    M: AppServerWebSocketMessage + Send + 'static,
    StreamError: std::fmt::Display + Send + 'static,
{
    tokio::pin!(websocket_reader);
    loop {
        tokio::select! {
            _ = disconnect_token.cancelled() => {
                break;
            }
            incoming_message = websocket_reader.next() => {
                match incoming_message {
                    Some(Ok(message)) => match message.into_incoming() {
                        Some(IncomingWebSocketMessage::Text(text))
                            if !forward_incoming_message(
                                &transport_event_tx,
                                &writer_tx_for_reader,
                                connection_id,
                                &text,
                            )
                            .await
                        => {
                            break;
                        }
                        Some(IncomingWebSocketMessage::Text(_)) => {}
                        Some(IncomingWebSocketMessage::Ping(payload)) => {
                            match writer_control_tx.try_send(M::pong(payload)) {
                                Ok(()) => {}
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    warn!("websocket control queue full while replying to ping; closing connection");
                                    break;
                                }
                            }
                        }
                        Some(IncomingWebSocketMessage::Pong) => {}
                        Some(IncomingWebSocketMessage::Close) => break,
                        Some(IncomingWebSocketMessage::Binary) => {
                            warn!("dropping unsupported binary websocket message");
                        }
                        None => {}
                    },
                    None => break,
                    Some(Err(err)) => {
                        warn!("websocket receive error: {err}");
                        break;
                    }
                }
            }
        }
    }
}
