//! The worker's admin socket: how `cli` reaches an app that is already
//! booted.
//!
//! ```text
//!   cli   Remote::call(Request) ─▶ frame ─▶ unix socket (0600) ─▶ AdminSocket ─▶ Handler
//!              Response              ◀─ frame ◀────────────────────────────────────────┘
//! ```
//!
//! One connection carries one call: a frame is a `u32` big-endian length and
//! that many bytes of postcard. The request frame is an [`Envelope`] (the
//! app's name, its wire fingerprint, the body); the answer is the body of the
//! app's response, or an error's words. The runtime knows nothing of an app's
//! calls: each app declares its own `Request`/`Response`.
//!
//! There is no compatibility between versions, only detection: the CLI and
//! the worker ship together, so a fingerprint (`App::WIRE`) that differs means
//! "update", and nothing is decoded or called. The filesystem is the boundary:
//! whoever can open the socket is the operator. Bodies carry passwords and
//! keys, so nothing here logs one.

use crate::app::Shutdown;
use crate::{Error, Stopped};
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::{UnixListener, UnixStream};

/// The largest frame either side sends or accepts.
pub const MAX_FRAME: usize = 64 << 20;

/// What a mismatched fingerprint answers.
const DIFFERENT_VERSIONS: &str = "the CLI and the process that works were built from different versions: update one or the other";

/// FNV-1a of `text`, at compile time: an app's `WIRE` is this of the file
/// that holds every type it sends, so any edit to that file changes it.
#[must_use]
#[allow(
    clippy::as_conversions,
    reason = "u8 to u64 is lossless, and `From` is not const"
)]
pub const fn fingerprint(text: &str) -> u64 {
    let mut rest = text.as_bytes();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    while let [byte, tail @ ..] = rest {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        rest = tail;
    }
    hash
}

/// The request frame. Never changes: it is what tells two versions apart.
#[derive(Serialize, Deserialize)]
struct Envelope {
    app: String,
    wire: u64,
    body: Vec<u8>,
}

/// The answer frame: the response's body, or an error's words.
type Reply = Result<Vec<u8>, String>;

/// An app's calls, answered in the worker. Cloneable, so the app can still be
/// consumed by `background`.
pub type Handler<Req, Resp> =
    Arc<dyn Fn(Req) -> BoxFuture<'static, Result<Resp, Error>> + Send + Sync>;

/// No calls: the `Request`/`Response` of an app without any.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NoCalls {}

/// An app's handler with its types erased: body bytes in, reply out.
pub struct Served {
    app: &'static str,
    wire: u64,
    call: Arc<dyn Fn(Vec<u8>) -> BoxFuture<'static, Reply> + Send + Sync>,
}

impl Served {
    pub(crate) fn new<Req, Resp>(app: &'static str, wire: u64, handler: Handler<Req, Resp>) -> Self
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
    {
        let call = Arc::new(move |body: Vec<u8>| -> BoxFuture<'static, Reply> {
            let request = postcard::from_bytes::<Req>(&body);
            let handler = handler.clone();
            Box::pin(async move {
                let request = request.map_err(|e| format!("the request did not decode: {e}"))?;
                let response = handler(request).await.map_err(|e| e.to_string())?;
                postcard::to_stdvec(&response)
                    .map_err(|e| format!("the response did not encode: {e}"))
            })
        });
        Self { app, wire, call }
    }
}

/// How a command reaches its app: over the socket (`cli`), or straight
/// to a handler (tests of the commands).
pub enum Remote<Req, Resp> {
    Socket {
        path: PathBuf,
        app: &'static str,
        wire: u64,
        types: PhantomData<fn(Req) -> Resp>,
    },
    Direct(Handler<Req, Resp>),
}

impl<Req, Resp> Remote<Req, Resp>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    #[must_use]
    pub const fn socket(path: PathBuf, app: &'static str, wire: u64) -> Self {
        Self::Socket {
            path,
            app,
            wire,
            types: PhantomData,
        }
    }

    /// One call: connect, send, read the answer.
    pub async fn call(&self, request: Req) -> Result<Resp, Error> {
        let (path, app, wire) = match self {
            Self::Direct(handler) => return handler(request).await,
            Self::Socket {
                path, app, wire, ..
            } => (path, *app, *wire),
        };
        let mut stream = UnixStream::connect(path).await.map_err(|e| {
            Error::new(format!(
                "the worker is not reachable at {} ({e}): is it running, and booted, in this container?",
                path.display()
            ))
        })?;
        let envelope = Envelope {
            app: app.to_owned(),
            wire,
            body: postcard::to_stdvec(&request).map_err(Error::new)?,
        };
        write_frame(
            &mut stream,
            &postcard::to_stdvec(&envelope).map_err(Error::new)?,
        )
        .await?;
        let reply: Reply = postcard::from_bytes(&read_frame(&mut stream).await?)
            .map_err(|e| Error::new(format!("the worker's answer did not decode: {e}")))?;
        let body = reply.map_err(Error::new)?;
        postcard::from_bytes(&body)
            .map_err(|e| Error::new(format!("the worker's answer did not decode: {e}")))
    }
}

async fn write_frame(w: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<(), Error> {
    if bytes.len() > MAX_FRAME {
        return Err(Error::new(format!(
            "a frame of {} bytes is over the limit of {MAX_FRAME}",
            bytes.len()
        )));
    }
    let len = u32::try_from(bytes.len()).map_err(Error::new)?;
    w.write_u32(len).await.map_err(Error::new)?;
    w.write_all(bytes).await.map_err(Error::new)?;
    w.flush().await.map_err(Error::new)
}

async fn read_frame(r: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>, Error> {
    let len = usize::try_from(r.read_u32().await.map_err(Error::new)?).map_err(Error::new)?;
    if len > MAX_FRAME {
        return Err(Error::new(format!(
            "a frame of {len} bytes is over the limit of {MAX_FRAME}"
        )));
    }
    let mut bytes = vec![0; len];
    r.read_exact(&mut bytes).await.map_err(Error::new)?;
    Ok(bytes)
}

/// The worker's end: bound at boot, served until shutdown.
pub struct AdminSocket {
    listener: std::os::unix::net::UnixListener,
    path: PathBuf,
}

impl AdminSocket {
    /// Bind `path`, mode 0600 from the start. A socket left there by a worker
    /// that crashed is replaced; one that answers is another worker's, and
    /// anything that is not a socket is someone's file: both refuse the boot.
    pub fn bind(path: &Path) -> Result<Self, Error> {
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        if !parent.is_dir() {
            return Err(Error::new(format!(
                "the CLI socket's directory {} does not exist",
                parent.display()
            )));
        }
        clear_stale(path)?;
        // Bound in a directory only we can enter, made 0600 there, then moved
        // into place: no moment where another user could connect.
        let private = tempfile::Builder::new()
            .prefix(".btrb-cli-")
            .tempdir_in(parent)
            .map_err(Error::new)?;
        let staged = private.path().join("s");
        let listener = std::os::unix::net::UnixListener::bind(&staged).map_err(Error::new)?;
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
            Error::new(format!(
                "could not make the CLI socket 0600 in {} ({e}): put it on a filesystem that keeps \
                 a socket's mode (a shared mount such as virtiofs does not)",
                parent.display()
            ))
        })?;
        std::fs::rename(&staged, path).map_err(|e| {
            Error::new(format!(
                "could not put the CLI socket at {}: {e}",
                path.display()
            ))
        })?;
        listener.set_nonblocking(true).map_err(Error::new)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Answer calls until `stop`; calls in flight finish, then the socket file
    /// goes.
    pub async fn serve(self, apps: Vec<Served>, stop: Shutdown) -> Result<Stopped, Error> {
        let listener = UnixListener::from_std(self.listener).map_err(Error::new)?;
        let apps: Arc<HashMap<&'static str, Served>> =
            Arc::new(apps.into_iter().map(|s| (s.app, s)).collect());
        let calls = tokio_util::task::TaskTracker::new();
        tracing::info!(path = %self.path.display(), "the CLI socket is open");
        let token = stop.token();
        loop {
            tokio::select! {
                () = token.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        calls.spawn(answer(stream, apps.clone()));
                    }
                    Err(e) => tracing::warn!(%e, "the CLI socket could not accept a connection"),
                },
            }
        }
        drop(listener);
        calls.close();
        calls.wait().await;
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::debug!(%e, "the CLI socket's file was already gone");
        }
        Ok(stop.wait().await)
    }
}

/// Make room at `path`: nothing there, or a socket nobody answers on.
fn clear_stale(path: &Path) -> Result<(), Error> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::new(format!("{}: {e}", path.display()))),
    };
    if !meta.file_type().is_socket() {
        return Err(Error::new(format!(
            "{} exists and is not a socket: not replacing it",
            path.display()
        )));
    }
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => Err(Error::new(format!(
            "another worker answers on {}: refusing to take its socket",
            path.display()
        ))),
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
            tracing::info!(path = %path.display(), "replacing a stale CLI socket");
            std::fs::remove_file(path).map_err(Error::new)
        }
        Err(e) => Err(Error::new(format!("{}: {e}", path.display()))),
    }
}

/// One connection: one request, one answer.
async fn answer(mut stream: UnixStream, apps: Arc<HashMap<&'static str, Served>>) {
    let frame = match read_frame(&mut stream).await {
        Ok(frame) => frame,
        Err(e) => {
            tracing::debug!(%e, "a CLI connection sent no request");
            return;
        }
    };
    let reply = dispatch(&apps, &frame).await;
    let encoded = match postcard::to_stdvec(&reply) {
        Ok(encoded) => encoded,
        Err(e) => {
            tracing::warn!(%e, "a CLI answer did not encode");
            return;
        }
    };
    if let Err(e) = write_frame(&mut stream, &encoded).await {
        tracing::debug!(%e, "a CLI answer could not be sent");
    }
}

async fn dispatch(apps: &HashMap<&'static str, Served>, frame: &[u8]) -> Reply {
    // A frame that is not an envelope comes from another version too: the
    // envelope never changes, so this is the same "update".
    let Ok(envelope) = postcard::from_bytes::<Envelope>(frame) else {
        return Err(DIFFERENT_VERSIONS.to_owned());
    };
    let Some(served) = apps.get(envelope.app.as_str()) else {
        return Err(format!("this worker has no app named {:?}", envelope.app));
    };
    if envelope.wire != served.wire {
        return Err(DIFFERENT_VERSIONS.to_owned());
    }
    let reply = (served.call)(envelope.body).await;
    match &reply {
        Ok(_) => tracing::info!(app = served.app, "CLI call done"),
        Err(e) => tracing::info!(app = served.app, error = %e, "CLI call failed"),
    }
    reply
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Ask {
        Echo(String),
        Fail,
    }

    fn echo() -> Handler<Ask, String> {
        Arc::new(|ask| {
            Box::pin(async move {
                match ask {
                    Ask::Echo(s) => Ok(s),
                    Ask::Fail => Err(Error::new("refused")),
                }
            })
        })
    }

    struct Running {
        _dir: tempfile::TempDir,
        path: PathBuf,
        stop: Shutdown,
        served: tokio::task::JoinHandle<Result<Stopped, Error>>,
    }

    fn start(wire: u64) -> Running {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cli.sock");
        let socket = AdminSocket::bind(&path).unwrap();
        let stop = Shutdown::new();
        let served =
            tokio::spawn(socket.serve(vec![Served::new("app", wire, echo())], stop.clone()));
        Running {
            _dir: dir,
            path,
            stop,
            served,
        }
    }

    #[test]
    fn the_fingerprint_follows_every_byte() {
        assert_eq!(fingerprint(""), 0xcbf2_9ce4_8422_2325);
        assert_ne!(fingerprint("enum A { B }"), fingerprint("enum A { C }"));
    }

    #[tokio::test]
    async fn a_call_answers_and_an_error_keeps_its_words() {
        let run = start(7);
        let remote = Remote::<Ask, String>::socket(run.path.clone(), "app", 7);
        assert_eq!(remote.call(Ask::Echo("hi".into())).await.unwrap(), "hi");
        assert_eq!(
            remote.call(Ask::Fail).await.unwrap_err().to_string(),
            "refused"
        );
        run.stop.cancel();
        run.served.await.unwrap().unwrap();
        assert!(!run.path.exists(), "the socket file stays after shutdown");
    }

    #[tokio::test]
    async fn another_version_is_told_to_update() {
        let run = start(7);
        let remote = Remote::<Ask, String>::socket(run.path.clone(), "app", 8);
        let e = remote.call(Ask::Echo("hi".into())).await.unwrap_err();
        assert_eq!(e.to_string(), DIFFERENT_VERSIONS);
        let other = Remote::<Ask, String>::socket(run.path.clone(), "nope", 7);
        assert!(
            other
                .call(Ask::Fail)
                .await
                .unwrap_err()
                .to_string()
                .contains("no app")
        );
        run.stop.cancel();
    }

    #[tokio::test]
    async fn the_socket_is_the_owners_alone() {
        let run = start(1);
        let mode = std::fs::metadata(&run.path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        run.stop.cancel();
    }

    #[tokio::test]
    async fn a_live_socket_refuses_a_second_worker_and_a_stale_one_is_replaced() {
        let run = start(1);
        let e = AdminSocket::bind(&run.path).err().unwrap();
        assert!(e.to_string().contains("another worker"), "{e}");
        run.stop.cancel();
        run.served.await.unwrap().unwrap();

        // A crash leaves the file and nobody listening.
        let stale = std::os::unix::net::UnixListener::bind(&run.path).unwrap();
        drop(stale);
        assert!(run.path.exists());
        AdminSocket::bind(&run.path).unwrap();
    }

    #[tokio::test]
    async fn a_file_that_is_not_a_socket_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cli.sock");
        std::fs::write(&path, "mine").unwrap();
        let e = AdminSocket::bind(&path).err().unwrap();
        assert!(e.to_string().contains("not a socket"), "{e}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "mine");
    }

    #[tokio::test]
    async fn no_worker_says_where_it_looked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cli.sock");
        let remote = Remote::<Ask, String>::socket(path, "app", 1);
        let e = remote.call(Ask::Fail).await.unwrap_err().to_string();
        assert!(e.contains("not reachable") && e.contains("cli.sock"), "{e}");
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused() {
        let (mut a, mut b) = tokio::io::duplex(64);
        tokio::spawn(async move {
            a.write_u32(u32::try_from(MAX_FRAME + 1).unwrap())
                .await
                .unwrap();
        });
        assert!(read_frame(&mut b).await.is_err());
    }
}
