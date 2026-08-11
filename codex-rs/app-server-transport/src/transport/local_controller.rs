use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use codex_utils_absolute_path::AbsolutePathBuf;
use constant_time_eq::constant_time_eq;
use futures::StreamExt;
use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::ErrorResponse;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::handshake::server::Response;
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::ConnectionOrigin;
use super::TransportEvent;
use crate::transport::websocket::run_websocket_connection;
use codex_uds::PeerCredentials;
use codex_uds::UnixListener;
use codex_uds::UnixStream;

const LOCAL_CONTROLLER_DIR_NAME: &str = "local-controllers";
const LOCAL_CONTROLLER_METADATA_PREFIX: &str = "launch-";
const LOCAL_CONTROLLER_METADATA_SUFFIX: &str = ".json";
const LOCAL_CONTROLLER_SOCKET_PREFIX: &str = "codex-";
const LOCAL_CONTROLLER_SOCKET_SUFFIX: &str = ".sock";
const LOCAL_CONTROLLER_METADATA_MODE: u32 = 0o600;
#[cfg(unix)]
const LOCAL_CONTROLLER_SOCKET_MODE: u32 = 0o600;

pub const LOCAL_CONTROLLER_METADATA_VERSION: u32 = 1;
pub const LOCAL_CONTROLLER_PROTOCOL_VERSION: u32 = 1;
pub const LOCAL_CONTROLLER_LAUNCH_NONCE_HEADER: &str = "X-Codex-Launch-Nonce";

/// Whether the current platform can expose a secure local-controller endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalControllerEndpointSupport {
    Available,
    Unavailable {
        reason: LocalControllerUnavailableReason,
    },
}

/// Why a local-controller endpoint cannot be exposed on this platform.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalControllerUnavailableReason {
    PeerCredentialsUnavailable,
}

impl LocalControllerUnavailableReason {
    fn message(self) -> &'static str {
        match self {
            Self::PeerCredentialsUnavailable => {
                "local-controller endpoint is unavailable because peer credential verification is unsupported on this platform"
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalControllerEndpointPaths {
    pub directory: AbsolutePathBuf,
    pub metadata_path: AbsolutePathBuf,
    pub socket_path: AbsolutePathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalControllerEndpointMetadata {
    pub version: u32,
    pub launch_id: String,
    pub launch_nonce: String,
    pub endpoint_uri: String,
    pub process_id: u32,
    pub created_at: i64,
    pub protocol_version: u32,
    pub main_thread_id: Option<String>,
}

impl LocalControllerEndpointMetadata {
    pub fn new(codex_home: &Path, main_thread_id: Option<String>) -> io::Result<Self> {
        let launch_id = Uuid::now_v7().to_string();
        let paths = local_controller_endpoint_paths(codex_home, &launch_id)?;
        let mut launch_nonce_bytes = [0_u8; 32];
        rand::rng().fill_bytes(&mut launch_nonce_bytes);
        Ok(Self {
            version: LOCAL_CONTROLLER_METADATA_VERSION,
            launch_id,
            launch_nonce: URL_SAFE_NO_PAD.encode(launch_nonce_bytes),
            endpoint_uri: format!("unix://{}", paths.socket_path.display()),
            process_id: std::process::id(),
            created_at: now_unix_seconds(),
            protocol_version: LOCAL_CONTROLLER_PROTOCOL_VERSION,
            main_thread_id,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct LocalControllerEndpointFailure {
    pub reason: String,
}

#[derive(Debug)]
pub struct LocalControllerEndpointGuard {
    metadata_path: AbsolutePathBuf,
    launch_id: String,
    launch_nonce: String,
}

impl LocalControllerEndpointGuard {
    pub fn metadata_path(&self) -> &AbsolutePathBuf {
        &self.metadata_path
    }
}

impl Drop for LocalControllerEndpointGuard {
    fn drop(&mut self) {
        match remove_metadata_if_owned(
            self.metadata_path.as_path(),
            &self.launch_id,
            &self.launch_nonce,
        ) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    metadata_path = %self.metadata_path.display(),
                    %err,
                    "failed to remove local-controller metadata"
                );
            }
        }
    }
}

#[derive(Debug)]
pub struct LocalControllerEndpointHandle {
    codex_home: AbsolutePathBuf,
    metadata: LocalControllerEndpointMetadata,
    socket_path: AbsolutePathBuf,
    shutdown_token: CancellationToken,
    accept_handle: Option<JoinHandle<()>>,
    failure_rx: Option<oneshot::Receiver<LocalControllerEndpointFailure>>,
}

impl LocalControllerEndpointHandle {
    pub fn metadata(&self) -> &LocalControllerEndpointMetadata {
        &self.metadata
    }

    pub fn socket_path(&self) -> &AbsolutePathBuf {
        &self.socket_path
    }

    pub async fn publish_main_thread_id(&mut self, main_thread_id: String) -> io::Result<()> {
        if self.metadata.main_thread_id.is_some() {
            return Ok(());
        }
        if self
            .accept_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "local-controller endpoint acceptor is closed",
            ));
        }

        let mut metadata = self.metadata.clone();
        metadata.main_thread_id = Some(main_thread_id);
        write_local_controller_metadata(self.codex_home.as_path(), &metadata).await?;
        self.metadata = metadata;
        Ok(())
    }

    pub fn take_failure_receiver(
        &mut self,
    ) -> Option<oneshot::Receiver<LocalControllerEndpointFailure>> {
        self.failure_rx.take()
    }

    pub async fn shutdown(mut self) -> Result<(), JoinError> {
        self.shutdown_token.cancel();
        match self.accept_handle.take() {
            Some(accept_handle) => accept_handle.await,
            None => Ok(()),
        }
    }
}

impl Drop for LocalControllerEndpointHandle {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
        if let Some(accept_handle) = self.accept_handle.take() {
            accept_handle.abort();
        }
    }
}

pub fn local_controller_endpoint_paths(
    codex_home: &Path,
    launch_id: &str,
) -> io::Result<LocalControllerEndpointPaths> {
    validate_launch_id(launch_id)?;
    let directory = absolute_path(codex_home.join(LOCAL_CONTROLLER_DIR_NAME))?;
    let metadata_path = absolute_path(codex_home.join(LOCAL_CONTROLLER_DIR_NAME).join(format!(
        "{LOCAL_CONTROLLER_METADATA_PREFIX}{launch_id}{LOCAL_CONTROLLER_METADATA_SUFFIX}"
    )))?;
    let socket_path = absolute_path(codex_home.join(LOCAL_CONTROLLER_DIR_NAME).join(format!(
        "{LOCAL_CONTROLLER_SOCKET_PREFIX}{launch_id}{LOCAL_CONTROLLER_SOCKET_SUFFIX}"
    )))?;
    Ok(LocalControllerEndpointPaths {
        directory,
        metadata_path,
        socket_path,
    })
}

pub fn local_controller_endpoint_support() -> LocalControllerEndpointSupport {
    if cfg!(unix) {
        LocalControllerEndpointSupport::Available
    } else {
        LocalControllerEndpointSupport::Unavailable {
            reason: LocalControllerUnavailableReason::PeerCredentialsUnavailable,
        }
    }
}

pub async fn start_local_controller_acceptor(
    codex_home: &Path,
    main_thread_id: Option<String>,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
) -> io::Result<LocalControllerEndpointHandle> {
    ensure_local_controller_endpoint_available(local_controller_endpoint_support())?;
    prune_stale_local_controller_endpoints(codex_home).await;

    let metadata = LocalControllerEndpointMetadata::new(codex_home, main_thread_id)?;
    let paths = local_controller_endpoint_paths(codex_home, &metadata.launch_id)?;
    prepare_local_controller_socket_path(paths.socket_path.as_path()).await?;
    let listener = UnixListener::bind(paths.socket_path.as_path()).await?;
    let socket_guard = LocalControllerSocketFileGuard {
        socket_path: paths.socket_path.clone(),
    };
    set_socket_permissions(socket_guard.socket_path.as_path()).await?;

    let metadata_guard = match publish_local_controller_metadata(codex_home, &metadata).await {
        Ok(metadata_guard) => metadata_guard,
        Err(err) => {
            drop(socket_guard);
            return Err(err);
        }
    };
    let metadata_path = metadata_guard.metadata_path().clone();
    let endpoint_shutdown_token = shutdown_token.child_token();
    let (failure_tx, failure_rx) = oneshot::channel();
    let accept_handle = tokio::spawn(run_local_controller_acceptor(
        listener,
        transport_event_tx,
        endpoint_shutdown_token.clone(),
        socket_guard,
        metadata_guard,
        metadata.launch_nonce.clone(),
        failure_tx,
    ));
    tracing::info!(
        socket_path = %paths.socket_path.display(),
        metadata_path = %metadata_path.display(),
        "local-controller endpoint listening"
    );

    Ok(LocalControllerEndpointHandle {
        codex_home: absolute_path(codex_home.to_path_buf())?,
        metadata,
        socket_path: paths.socket_path,
        shutdown_token: endpoint_shutdown_token,
        accept_handle: Some(accept_handle),
        failure_rx: Some(failure_rx),
    })
}

fn ensure_local_controller_endpoint_available(
    support: LocalControllerEndpointSupport,
) -> io::Result<()> {
    match support {
        LocalControllerEndpointSupport::Available => Ok(()),
        LocalControllerEndpointSupport::Unavailable { reason } => {
            Err(local_controller_unavailable_error(reason))
        }
    }
}

fn local_controller_unavailable_error(reason: LocalControllerUnavailableReason) -> io::Error {
    io::Error::new(ErrorKind::Unsupported, reason.message())
}

pub async fn publish_local_controller_metadata(
    codex_home: &Path,
    metadata: &LocalControllerEndpointMetadata,
) -> io::Result<LocalControllerEndpointGuard> {
    let metadata_path = write_local_controller_metadata(codex_home, metadata).await?;

    Ok(LocalControllerEndpointGuard {
        metadata_path,
        launch_id: metadata.launch_id.clone(),
        launch_nonce: metadata.launch_nonce.clone(),
    })
}

async fn write_local_controller_metadata(
    codex_home: &Path,
    metadata: &LocalControllerEndpointMetadata,
) -> io::Result<AbsolutePathBuf> {
    let paths = local_controller_endpoint_paths(codex_home, &metadata.launch_id)?;
    codex_uds::prepare_private_socket_directory(paths.directory.as_path()).await?;
    remove_stale_metadata_if_owned(paths.metadata_path.as_path(), metadata).await?;

    let temp_path = paths
        .directory
        .as_path()
        .join(format!(".{}.tmp", Uuid::now_v7()));
    let mut payload = serde_json::to_vec_pretty(metadata).map_err(io::Error::other)?;
    payload.push(b'\n');
    tokio::fs::write(&temp_path, payload).await?;
    set_metadata_permissions(&temp_path).await?;
    if let Err(err) = tokio::fs::hard_link(&temp_path, paths.metadata_path.as_path()).await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(err);
    }
    if let Err(err) = tokio::fs::remove_file(&temp_path).await {
        tracing::warn!(
            temp_path = %temp_path.display(),
            %err,
            "failed to remove local-controller metadata temp file"
        );
    }

    Ok(paths.metadata_path)
}

async fn prune_stale_local_controller_endpoints(codex_home: &Path) {
    let directory = match absolute_path(codex_home.join(LOCAL_CONTROLLER_DIR_NAME)) {
        Ok(directory) => directory,
        Err(err) => {
            tracing::warn!(%err, "failed to resolve local-controller directory for stale cleanup");
            return;
        }
    };
    let mut entries = match tokio::fs::read_dir(directory.as_path()).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return,
        Err(err) => {
            tracing::warn!(
                directory = %directory.display(),
                %err,
                "failed to read local-controller directory for stale cleanup"
            );
            return;
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => return,
            Err(err) => {
                tracing::warn!(
                    directory = %directory.display(),
                    %err,
                    "failed to iterate local-controller directory for stale cleanup"
                );
                return;
            }
        };
        prune_stale_local_controller_entry(codex_home, entry.path()).await;
    }
}

async fn prune_stale_local_controller_entry(codex_home: &Path, metadata_path: PathBuf) {
    let Some(file_name) = metadata_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
    else {
        return;
    };
    let Some(launch_id) = file_name
        .strip_prefix(LOCAL_CONTROLLER_METADATA_PREFIX)
        .and_then(|suffix| suffix.strip_suffix(LOCAL_CONTROLLER_METADATA_SUFFIX))
    else {
        return;
    };
    let paths = match local_controller_endpoint_paths(codex_home, launch_id) {
        Ok(paths) => paths,
        Err(_) => return,
    };
    if metadata_path != paths.metadata_path.as_path() {
        return;
    }

    let metadata = match tokio::fs::read(paths.metadata_path.as_path())
        .await
        .and_then(|bytes| {
            serde_json::from_slice::<LocalControllerEndpointMetadata>(&bytes)
                .map_err(io::Error::other)
        }) {
        Ok(metadata) => metadata,
        Err(_) => return,
    };
    if metadata.launch_id != launch_id {
        return;
    }
    if process_may_be_running(metadata.process_id) {
        return;
    }

    match remove_stale_metadata_if_owned(paths.metadata_path.as_path(), &metadata).await {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => return,
        Err(err) => {
            tracing::warn!(
                metadata_path = %paths.metadata_path.display(),
                %err,
                "failed to remove stale local-controller metadata"
            );
            return;
        }
    }
    match remove_socket_file_if_owned(paths.socket_path.as_path()) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(
                socket_path = %paths.socket_path.display(),
                %err,
                "failed to remove stale local-controller socket"
            );
        }
    }
}

async fn prepare_local_controller_socket_path(socket_path: &Path) -> io::Result<()> {
    if let Some(parent) = socket_path.parent() {
        codex_uds::prepare_private_socket_directory(parent).await?;
    }

    match UnixStream::connect(socket_path).await {
        Ok(_stream) => {
            return Err(io::Error::new(
                ErrorKind::AddrInUse,
                format!(
                    "local-controller socket is already in use at {}",
                    socket_path.display()
                ),
            ));
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) if err.kind() == ErrorKind::ConnectionRefused => {}
        Err(err) => {
            if !socket_path.exists() {
                return Ok(());
            }
            return Err(err);
        }
    }

    if !socket_path.try_exists()? {
        return Ok(());
    }

    if !codex_uds::is_stale_socket_path(socket_path).await? {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "local-controller socket path exists and is not a socket: {}",
                socket_path.display()
            ),
        ));
    }
    tokio::fs::remove_file(socket_path).await
}

async fn run_local_controller_acceptor(
    mut listener: UnixListener,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    shutdown_token: CancellationToken,
    socket_guard: LocalControllerSocketFileGuard,
    metadata_guard: LocalControllerEndpointGuard,
    launch_nonce: String,
    failure_tx: oneshot::Sender<LocalControllerEndpointFailure>,
) {
    let _socket_guard = socket_guard;
    let _metadata_guard = metadata_guard;
    let mut failure_tx = Some(failure_tx);
    loop {
        let stream = tokio::select! {
            _ = shutdown_token.cancelled() => {
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok(stream) => stream,
                    Err(err) => {
                        if matches!(
                            err.kind(),
                            ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset | ErrorKind::Interrupted
                        ) {
                            tracing::warn!("recoverable local-controller socket accept error: {err}");
                            continue;
                        }
                        report_local_controller_acceptor_failure(&mut failure_tx, err);
                        break;
                    }
                }
            }
        };

        let transport_event_tx = transport_event_tx.clone();
        let launch_nonce = launch_nonce.clone();
        tokio::spawn(async move {
            if let Err(err) = verify_same_user_peer(&stream) {
                tracing::warn!(%err, "rejecting local-controller peer");
                return;
            }
            let websocket_stream = match accept_hdr_async(
                stream,
                move |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
                    authorize_local_controller_upgrade(request, response, &launch_nonce)
                },
            )
            .await
            {
                Ok(websocket_stream) => websocket_stream,
                Err(err) => {
                    tracing::warn!(
                        "failed to upgrade local-controller websocket connection: {err}"
                    );
                    return;
                }
            };
            let (websocket_writer, websocket_reader) = websocket_stream.split();
            run_websocket_connection(
                websocket_writer,
                websocket_reader,
                transport_event_tx,
                ConnectionOrigin::ExternalController,
            )
            .await;
        });
    }
    tracing::info!("local-controller acceptor shutting down");
}

fn report_local_controller_acceptor_failure(
    failure_tx: &mut Option<oneshot::Sender<LocalControllerEndpointFailure>>,
    err: io::Error,
) {
    let reason = format!("local-controller socket accept error: {err}");
    tracing::error!("{reason}");
    if let Some(failure_tx) = failure_tx.take() {
        let _ = failure_tx.send(LocalControllerEndpointFailure { reason });
    }
}

fn verify_same_user_peer(stream: &UnixStream) -> io::Result<()> {
    verify_peer_credentials(stream.peer_credentials())
}

fn verify_peer_credentials(credentials: io::Result<PeerCredentials>) -> io::Result<()> {
    let credentials = credentials.map_err(|err| {
        io::Error::new(
            ErrorKind::PermissionDenied,
            format!("missing local-controller peer credentials: {err}"),
        )
    })?;
    if !credentials.belongs_to_current_user() {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "local-controller peer belongs to a different user",
        ));
    }
    Ok(())
}

fn authorize_local_controller_upgrade(
    request: &Request,
    response: Response,
    launch_nonce: &str,
) -> Result<Response, ErrorResponse> {
    let Some(header_value) = request.headers().get(LOCAL_CONTROLLER_LAUNCH_NONCE_HEADER) else {
        return Err(local_controller_upgrade_error(
            StatusCode::UNAUTHORIZED,
            "missing launch nonce",
        ));
    };
    let Ok(header_value) = header_value.to_str() else {
        return Err(local_controller_upgrade_error(
            StatusCode::UNAUTHORIZED,
            "invalid launch nonce",
        ));
    };
    if !constant_time_eq(header_value.as_bytes(), launch_nonce.as_bytes()) {
        return Err(local_controller_upgrade_error(
            StatusCode::UNAUTHORIZED,
            "invalid launch nonce",
        ));
    }
    Ok(response)
}

fn local_controller_upgrade_error(status: StatusCode, message: &str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(message.to_string()));
    *response.status_mut() = status;
    response
}

async fn remove_stale_metadata_if_owned(
    metadata_path: &Path,
    metadata: &LocalControllerEndpointMetadata,
) -> io::Result<()> {
    let existing = match tokio::fs::symlink_metadata(metadata_path).await {
        Ok(existing) => existing,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if !existing.is_file() {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "local-controller metadata path exists and is not a regular file: {}",
                metadata_path.display()
            ),
        ));
    }

    let bytes = tokio::fs::read(metadata_path).await?;
    if !metadata_bytes_are_owned_by(&bytes, &metadata.launch_id, &metadata.launch_nonce) {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "local-controller metadata path belongs to a different launch: {}",
                metadata_path.display()
            ),
        ));
    }
    tokio::fs::remove_file(metadata_path).await
}

fn remove_metadata_if_owned(
    metadata_path: &Path,
    launch_id: &str,
    launch_nonce: &str,
) -> io::Result<()> {
    let existing = std::fs::symlink_metadata(metadata_path)?;
    if !existing.is_file() {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "local-controller metadata path exists and is not a regular file: {}",
                metadata_path.display()
            ),
        ));
    }

    let bytes = std::fs::read(metadata_path)?;
    if metadata_bytes_are_owned_by(&bytes, launch_id, launch_nonce) {
        std::fs::remove_file(metadata_path)?;
    }
    Ok(())
}

fn metadata_bytes_are_owned_by(bytes: &[u8], launch_id: &str, launch_nonce: &str) -> bool {
    serde_json::from_slice::<LocalControllerEndpointMetadata>(bytes).is_ok_and(|metadata| {
        metadata.launch_id == launch_id && metadata.launch_nonce == launch_nonce
    })
}

fn validate_launch_id(launch_id: &str) -> io::Result<()> {
    if launch_id.is_empty()
        || !launch_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "local-controller launch ID must contain only ASCII alphanumeric, '-' or '_' characters",
        ));
    }
    Ok(())
}

fn absolute_path(path: PathBuf) -> io::Result<AbsolutePathBuf> {
    AbsolutePathBuf::from_absolute_path(path).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("local-controller path must be absolute: {err}"),
        )
    })
}

fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn process_may_be_running(process_id: u32) -> bool {
    const SIGNAL_EXISTS_CHECK: i32 = 0;
    const ESRCH: i32 = 3;

    if process_id == 0 || process_id > i32::MAX as u32 {
        return false;
    }
    if unsafe { kill(process_id as i32, SIGNAL_EXISTS_CHECK) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(ESRCH)
}

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

#[cfg(not(unix))]
fn process_may_be_running(_process_id: u32) -> bool {
    true
}

#[cfg(unix)]
async fn set_socket_permissions(socket_path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(
        socket_path,
        std::fs::Permissions::from_mode(LOCAL_CONTROLLER_SOCKET_MODE),
    )
    .await
}

#[cfg(not(unix))]
async fn set_socket_permissions(_socket_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
async fn set_metadata_permissions(metadata_path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(
        metadata_path,
        std::fs::Permissions::from_mode(LOCAL_CONTROLLER_METADATA_MODE),
    )
    .await
}

#[cfg(not(unix))]
async fn set_metadata_permissions(_metadata_path: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Debug)]
struct LocalControllerSocketFileGuard {
    socket_path: AbsolutePathBuf,
}

impl Drop for LocalControllerSocketFileGuard {
    fn drop(&mut self) {
        match remove_socket_file_if_owned(self.socket_path.as_path()) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    socket_path = %self.socket_path.display(),
                    %err,
                    "failed to remove local-controller socket file"
                );
            }
        }
    }
}

#[cfg(unix)]
fn remove_socket_file_if_owned(socket_path: &Path) -> io::Result<()> {
    use std::os::unix::fs::FileTypeExt;

    let metadata = std::fs::symlink_metadata(socket_path)?;
    if metadata.file_type().is_socket() {
        std::fs::remove_file(socket_path)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn remove_socket_file_if_owned(socket_path: &Path) -> io::Result<()> {
    match std::fs::remove_file(socket_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
#[path = "local_controller_tests.rs"]
mod tests;
