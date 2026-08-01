use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use codex_utils_absolute_path::AbsolutePathBuf;
use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

const LOCAL_CONTROLLER_DIR_NAME: &str = "local-controllers";
const LOCAL_CONTROLLER_METADATA_PREFIX: &str = "launch-";
const LOCAL_CONTROLLER_METADATA_SUFFIX: &str = ".json";
const LOCAL_CONTROLLER_SOCKET_PREFIX: &str = "codex-";
const LOCAL_CONTROLLER_SOCKET_SUFFIX: &str = ".sock";
const LOCAL_CONTROLLER_METADATA_MODE: u32 = 0o600;

pub const LOCAL_CONTROLLER_METADATA_VERSION: u32 = 1;
pub const LOCAL_CONTROLLER_PROTOCOL_VERSION: u32 = 1;

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
    pub fn new(endpoint_uri: String, main_thread_id: Option<String>) -> Self {
        Self {
            version: LOCAL_CONTROLLER_METADATA_VERSION,
            launch_id: new_local_controller_launch_id(),
            launch_nonce: new_local_controller_launch_nonce(),
            endpoint_uri,
            process_id: std::process::id(),
            created_at: now_unix_seconds(),
            protocol_version: LOCAL_CONTROLLER_PROTOCOL_VERSION,
            main_thread_id,
        }
    }
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

pub fn new_local_controller_launch_id() -> String {
    Uuid::now_v7().to_string()
}

pub fn new_local_controller_launch_nonce() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
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

pub async fn publish_local_controller_metadata(
    codex_home: &Path,
    metadata: &LocalControllerEndpointMetadata,
) -> io::Result<LocalControllerEndpointGuard> {
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

    Ok(LocalControllerEndpointGuard {
        metadata_path: paths.metadata_path,
        launch_id: metadata.launch_id.clone(),
        launch_nonce: metadata.launch_nonce.clone(),
    })
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

#[cfg(test)]
#[path = "local_controller_tests.rs"]
mod tests;
