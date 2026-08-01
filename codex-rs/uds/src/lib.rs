//! Cross-platform async Unix domain socket helpers.

use std::io::Result as IoResult;
use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

/// Native peer credentials for a connected local socket.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PeerCredentials {
    /// Effective user ID of the peer process.
    pub user_id: u32,
    /// Effective group ID of the peer process.
    pub group_id: u32,
    /// Peer process ID when the platform exposes it.
    pub process_id: Option<i32>,
}

impl PeerCredentials {
    /// Returns whether these credentials belong to the current effective user.
    pub fn belongs_to_current_user(&self) -> bool {
        Some(self.user_id) == platform::current_effective_user_id()
    }
}

/// Creates `socket_dir` if needed and restricts it to the current user where
/// the platform exposes Unix permissions.
pub async fn prepare_private_socket_directory(socket_dir: impl AsRef<Path>) -> IoResult<()> {
    platform::prepare_private_socket_directory(socket_dir.as_ref()).await
}

/// Returns whether `socket_path` points at a stale Unix socket rendezvous path.
///
/// On Unix this checks the file type. On Windows, `uds_windows` represents the
/// rendezvous as a regular path, so existence is the only useful stale-path
/// signal available.
pub async fn is_stale_socket_path(socket_path: impl AsRef<Path>) -> IoResult<bool> {
    platform::is_stale_socket_path(socket_path.as_ref()).await
}

/// Async Unix domain socket listener.
pub struct UnixListener {
    inner: platform::Listener,
}

impl UnixListener {
    /// Binds a new listener at `socket_path`.
    pub async fn bind(socket_path: impl AsRef<Path>) -> IoResult<Self> {
        platform::bind_listener(socket_path.as_ref())
            .await
            .map(|inner| Self { inner })
    }

    /// Accepts the next incoming stream.
    pub async fn accept(&mut self) -> IoResult<UnixStream> {
        self.inner.accept().await.map(|inner| UnixStream { inner })
    }
}

/// Async Unix domain socket stream.
pub struct UnixStream {
    inner: platform::Stream,
}

impl UnixStream {
    /// Connects to `socket_path`.
    pub async fn connect(socket_path: impl AsRef<Path>) -> IoResult<Self> {
        platform::connect_stream(socket_path.as_ref())
            .await
            .map(|inner| Self { inner })
    }

    /// Returns native credentials for the peer on the other side of this stream.
    pub fn peer_credentials(&self) -> IoResult<PeerCredentials> {
        platform::peer_credentials(&self.inner)
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(unix)]
mod platform {
    use std::io;
    use std::io::ErrorKind;
    use std::io::Result as IoResult;
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    use super::PeerCredentials;
    use tokio::fs;
    use tokio::net::UnixListener;
    use tokio::net::UnixStream;

    /// Owner-only access keeps the control socket directory private while
    /// preserving owner traversal and socket path creation.
    const SOCKET_DIR_MODE: u32 = 0o700;
    const SOCKET_DIR_PERMISSION_BITS: u32 = 0o777;

    pub(super) type Stream = UnixStream;

    pub(super) struct Listener(UnixListener);

    pub(super) async fn prepare_private_socket_directory(socket_dir: &Path) -> IoResult<()> {
        let mut dir_builder = fs::DirBuilder::new();
        dir_builder.mode(SOCKET_DIR_MODE);
        match dir_builder.create(socket_dir).await {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }

        let metadata = fs::symlink_metadata(socket_dir).await?;
        if !metadata.is_dir() {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!(
                    "socket directory path exists and is not a directory: {}",
                    socket_dir.display()
                ),
            ));
        }

        let permissions = metadata.permissions();
        // The SSH-over-UDS control socket is reachable by path, so the
        // rendezvous directory must be owner-traversable while denying
        // group/other access; exact 0700 fixes insecure modes and unusable
        // owner-only modes like 0600.
        if permissions.mode() & SOCKET_DIR_PERMISSION_BITS != SOCKET_DIR_MODE {
            fs::set_permissions(socket_dir, std::fs::Permissions::from_mode(SOCKET_DIR_MODE))
                .await?;
        }

        Ok(())
    }

    pub(super) async fn bind_listener(socket_path: &Path) -> IoResult<Listener> {
        UnixListener::bind(socket_path).map(Listener)
    }

    impl Listener {
        pub(super) async fn accept(&mut self) -> IoResult<Stream> {
            self.0.accept().await.map(|(stream, _addr)| stream)
        }
    }

    pub(super) async fn connect_stream(socket_path: &Path) -> IoResult<Stream> {
        UnixStream::connect(socket_path).await
    }

    pub(super) async fn is_stale_socket_path(socket_path: &Path) -> IoResult<bool> {
        Ok(fs::symlink_metadata(socket_path)
            .await?
            .file_type()
            .is_socket())
    }

    pub(super) fn current_effective_user_id() -> Option<u32> {
        Some(unsafe { libc::geteuid() })
    }

    #[cfg(any(target_os = "android", target_os = "linux", target_os = "cygwin"))]
    pub(super) fn peer_credentials(stream: &Stream) -> IoResult<PeerCredentials> {
        use libc::SO_PEERCRED;
        use libc::SOL_SOCKET;
        use libc::c_void;
        use libc::getsockopt;
        use libc::socklen_t;
        use libc::ucred;

        let mut ucred_size = size_of::<ucred>() as socklen_t;
        let mut ucred = ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let result = unsafe {
            getsockopt(
                stream.as_raw_fd(),
                SOL_SOCKET,
                SO_PEERCRED,
                (&raw mut ucred).cast::<c_void>(),
                &mut ucred_size,
            )
        };
        if result == 0 && ucred_size as usize == size_of::<ucred>() {
            Ok(PeerCredentials {
                user_id: ucred.uid,
                group_id: ucred.gid,
                process_id: Some(ucred.pid),
            })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_vendor = "apple")]
    pub(super) fn peer_credentials(stream: &Stream) -> IoResult<PeerCredentials> {
        use libc::LOCAL_PEERPID;
        use libc::SOL_LOCAL;
        use libc::c_void;
        use libc::getpeereid;
        use libc::getsockopt;
        use libc::gid_t;
        use libc::pid_t;
        use libc::socklen_t;
        use libc::uid_t;

        let mut user_id: uid_t = 0;
        let mut group_id: gid_t = 0;
        let result = unsafe { getpeereid(stream.as_raw_fd(), &mut user_id, &mut group_id) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }

        let mut process_id: pid_t = 0;
        let mut process_id_size = size_of::<pid_t>() as socklen_t;
        let result = unsafe {
            getsockopt(
                stream.as_raw_fd(),
                SOL_LOCAL,
                LOCAL_PEERPID,
                (&raw mut process_id).cast::<c_void>(),
                &mut process_id_size,
            )
        };
        if result == 0 && process_id_size as usize == size_of::<pid_t>() {
            Ok(PeerCredentials {
                user_id,
                group_id,
                process_id: Some(process_id),
            })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(all(
        not(any(target_os = "android", target_os = "linux", target_os = "cygwin")),
        not(target_vendor = "apple")
    ))]
    pub(super) fn peer_credentials(_stream: &Stream) -> IoResult<PeerCredentials> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "peer credential lookup is unavailable on this Unix platform",
        ))
    }
}

#[cfg(windows)]
mod platform {
    use std::io;
    use std::io::Result as IoResult;
    use std::net::Shutdown;
    use std::ops::Deref;
    use std::os::windows::io::AsRawSocket;
    use std::os::windows::io::AsSocket;
    use std::os::windows::io::BorrowedSocket;
    use std::path::Path;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::ready;

    use async_io::Async;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncWrite;
    use tokio::io::ReadBuf;
    use tokio::task;
    use tokio_util::compat::Compat;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    use super::PeerCredentials;

    pub(super) struct Stream(Compat<Async<WindowsUnixStream>>);

    pub(super) async fn prepare_private_socket_directory(socket_dir: &Path) -> IoResult<()> {
        tokio::fs::create_dir_all(socket_dir).await
    }

    pub(super) struct Listener(Async<WindowsUnixListener>);

    pub(super) async fn bind_listener(socket_path: &Path) -> IoResult<Listener> {
        let socket_path = socket_path.to_path_buf();
        let listener =
            spawn_blocking_io(move || uds_windows::UnixListener::bind(socket_path)).await?;
        Async::new(WindowsUnixListener::from(listener)).map(Listener)
    }

    impl Listener {
        pub(super) async fn accept(&mut self) -> IoResult<Stream> {
            let (stream, _addr) = self.0.read_with(|listener| listener.accept()).await?;
            Async::new(WindowsUnixStream::from(stream))
                .map(FuturesAsyncReadCompatExt::compat)
                .map(Stream)
        }
    }

    pub(super) async fn connect_stream(socket_path: &Path) -> IoResult<Stream> {
        let socket_path = socket_path.to_path_buf();
        let stream =
            spawn_blocking_io(move || uds_windows::UnixStream::connect(socket_path)).await?;
        Async::new(WindowsUnixStream::from(stream))
            .map(FuturesAsyncReadCompatExt::compat)
            .map(Stream)
    }

    pub(super) async fn is_stale_socket_path(socket_path: &Path) -> IoResult<bool> {
        tokio::fs::try_exists(socket_path).await
    }

    pub(super) fn current_effective_user_id() -> Option<u32> {
        None
    }

    pub(super) fn peer_credentials(_stream: &Stream) -> IoResult<PeerCredentials> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "peer credential lookup is unavailable for Windows UDS",
        ))
    }

    async fn spawn_blocking_io<T>(
        operation: impl FnOnce() -> IoResult<T> + Send + 'static,
    ) -> IoResult<T>
    where
        T: Send + 'static,
    {
        task::spawn_blocking(operation)
            .await
            .map_err(|err| io::Error::other(format!("blocking socket task failed: {err}")))?
    }

    pub(super) struct WindowsUnixListener(uds_windows::UnixListener);

    impl From<uds_windows::UnixListener> for WindowsUnixListener {
        fn from(listener: uds_windows::UnixListener) -> Self {
            Self(listener)
        }
    }

    impl Deref for WindowsUnixListener {
        type Target = uds_windows::UnixListener;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl AsSocket for WindowsUnixListener {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            unsafe { BorrowedSocket::borrow_raw(self.as_raw_socket()) }
        }
    }

    pub(super) struct WindowsUnixStream(uds_windows::UnixStream);

    impl From<uds_windows::UnixStream> for WindowsUnixStream {
        fn from(stream: uds_windows::UnixStream) -> Self {
            Self(stream)
        }
    }

    impl Deref for WindowsUnixStream {
        type Target = uds_windows::UnixStream;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl AsSocket for WindowsUnixStream {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            unsafe { BorrowedSocket::borrow_raw(self.as_raw_socket()) }
        }
    }

    impl io::Read for WindowsUnixStream {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            io::Read::read(&mut self.0, buf)
        }
    }

    impl io::Write for WindowsUnixStream {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            io::Write::write(&mut self.0, buf)
        }

        fn flush(&mut self) -> IoResult<()> {
            io::Write::flush(&mut self.0)
        }
    }

    impl AsyncRead for Stream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<IoResult<()>> {
            Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Stream {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<IoResult<usize>> {
            Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
            Pin::new(&mut self.get_mut().0).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
            let stream = &mut self.get_mut().0;
            ready!(Pin::new(&mut *stream).poll_flush(cx))?;
            // `Compat<Async<_>>` maps shutdown to `poll_close()`, which only
            // flushes for `async_io::Async`; call the socket shutdown directly.
            stream.get_ref().get_ref().shutdown(Shutdown::Write)?;
            Poll::Ready(Ok(()))
        }
    }

    unsafe impl async_io::IoSafe for WindowsUnixListener {}
    unsafe impl async_io::IoSafe for WindowsUnixStream {}
}

#[cfg(test)]
mod lib_tests;
