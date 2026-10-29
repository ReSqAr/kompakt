//! Read-after-write: an app's interactive handle on its live view, and the
//! request layer that carries a person's last write from server to server.
//!
//! ```text
//!   W  handler ─▶ Live::write(live events) ─▶ quorum ─▶ wait until in W's view (≤ cap)
//!      response (written, page) ──────────────────────▶ Set-Cookie rw_<app>=<d>
//!   P  next request with rw_<app>=<d> ─▶ layer: view.wait_for(d, ≤ cap) ─▶ handler
//! ```
//!
//! Best effort throughout: a wait that reaches the cap ends, the request goes
//! on, and `btrb_read_after_write_wait_seconds` counts it. The write itself is
//! durable once the quorum acked it. Nothing is kept on the server: the
//! position is the browser's cookie, and a stale one costs one comparison.
//!
//! [`Live::write`] takes the app's owned live subset only (`App::Live`), so a
//! bulk event does not compile here: bulk and system writes go through the
//! plain [`EventWriter`]. Use `Live::write` for a write made on behalf of a
//! person whose next read should see it.
//!
//! Design: nlnr `docs/logs/2026-10-28_design_read_after_write.md`.

use crate::CookiePolicy;
use crate::base_path::BasePath;
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::http::header::{COOKIE, SET_COOKIE};
use axum::middleware::Next;
use axum::response::{IntoResponseParts, Response, ResponseParts};
use futures::FutureExt as _;
use futures::future::BoxFuture;
use nvzn::{EventSet, EventWriter, Wait, WithWireFormat};
use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use zslg::DataId;

/// The default for `BTRB_READ_AFTER_WRITE_WAIT`: how long either wait may
/// take before the request goes on.
pub const DEFAULT_WAIT: Duration = Duration::from_millis(200);

/// The read-after-write wait, by app, site (`write` or `read`) and outcome.
pub const WAIT_SECONDS: &str = "btrb_read_after_write_wait_seconds";

/// Seconds ladder for [`WAIT_SECONDS`]: a held entry in microseconds, the cap
/// at a fraction of a second.
pub(crate) const WAIT_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0,
];

/// What an app's [`Live`] is built with: the cap and the cookie.
#[derive(Debug, Clone)]
pub struct LiveSettings {
    cap: Duration,
    cookie: Arc<Cookie>,
}

impl LiveSettings {
    pub(crate) fn new(
        app: &'static str,
        base: &BasePath,
        cookies: CookiePolicy,
        cap: Duration,
    ) -> Self {
        Self {
            cap,
            cookie: Arc::new(Cookie {
                app,
                name: format!("rw_{app}"),
                path: base.path(),
                secure: cookies.secure_attribute(),
            }),
        }
    }

    /// For a test rig: the default cap, cookie `rw_test` at `/`.
    #[must_use]
    pub fn for_test() -> Self {
        Self::new(
            "test",
            &BasePath::ROOT,
            CookiePolicy::AllowPlainHttp,
            DEFAULT_WAIT,
        )
    }
}

/// `rw_<app>`: per app, because two apps can share a path (one mounted at `/`).
#[derive(Debug)]
struct Cookie {
    app: &'static str,
    name: String,
    path: String,
    secure: bool,
}

impl Cookie {
    fn set(&self, d: DataId) -> String {
        let secure = if self.secure { "; Secure" } else { "" };
        format!(
            "{}={}; Path={}; HttpOnly; SameSite=Lax; Max-Age=10{secure}",
            self.name,
            hex(&d.into_bytes()),
            self.path
        )
    }

    /// This cookie's `DataId` in `request`, if it carries one that parses.
    fn find(&self, request: &Request) -> Option<DataId> {
        request
            .headers()
            .get_all(COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .filter_map(|pair| pair.trim().split_once('='))
            .find(|(name, _)| *name == self.name)
            .and_then(|(_, value)| unhex(value))
            .map(DataId::from_bytes)
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    let mut out = [0u8; 32];
    let digits = s.as_bytes();
    if digits.len() != 64 {
        return None;
    }
    let (pairs, _) = digits.as_chunks::<2>();
    for (byte, pair) in out.iter_mut().zip(pairs) {
        let pair = std::str::from_utf8(pair).ok()?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

#[derive(Clone, Copy)]
enum Site {
    Write,
    Read,
}

fn record(cookie: &Cookie, site: Site, wait: Wait, elapsed: Duration) {
    let site = match site {
        Site::Write => "write",
        Site::Read => "read",
    };
    metrics::histogram!(
        WAIT_SECONDS,
        "app" => cookie.app,
        "site" => site,
        "outcome" => wait.name()
    )
    .record(elapsed);
}

/// An app's interactive handle: its live view, and the write a person's
/// next read will see. One per app per process; clone it to share it.
pub struct Live<Ev, L> {
    live: nvzn::Live<Ev>,
    writer: EventWriter<Ev>,
    settings: LiveSettings,
    _live_set: PhantomData<fn() -> L>,
}

impl<Ev, L> Clone for Live<Ev, L> {
    fn clone(&self) -> Self {
        Self {
            live: self.live.clone(),
            writer: self.writer.clone(),
            settings: self.settings.clone(),
            _live_set: PhantomData,
        }
    }
}

impl<Ev: WithWireFormat, L: EventSet + Into<Ev>> Live<Ev, L> {
    pub(crate) fn new(
        live: nvzn::Live<Ev>,
        writer: EventWriter<Ev>,
        settings: LiveSettings,
    ) -> Self {
        Self {
            live,
            writer,
            settings,
            _live_set: PhantomData,
        }
    }

    /// The live state; see [`nvzn::Live::snapshot`]. Blocking: call it from
    /// blocking code.
    pub fn snapshot(&self) -> Result<nvzn::Snapshot<'_>, nvzn::Error> {
        self.live.snapshot()
    }

    /// Moves when a live event reaches this node; see [`nvzn::Live::changed`].
    #[must_use]
    pub fn changed(&self) -> tokio::sync::watch::Receiver<u64> {
        self.live.changed()
    }

    /// Write `events`, then wait (up to the cap) until this server's live view
    /// holds them. The returned [`Written`] goes into the response: it is
    /// the cookie that lets the next request, on any server, wait for it too.
    pub async fn write(&self, events: impl IntoIterator<Item = L>) -> Result<Written, nvzn::Error> {
        let events: Vec<Ev> = events.into_iter().map(Into::into).collect();
        let d = self.writer.write(&events).await?;
        let started = Instant::now();
        let wait = self.live.wait_for(d, self.settings.cap).await;
        record(&self.settings.cookie, Site::Write, wait, started.elapsed());
        Ok(Written {
            data_id: d,
            cookie: Arc::clone(&self.settings.cookie),
        })
    }

    /// The layer's handle on this view: `wait_for` and the cookie.
    pub(crate) fn read_after_write(&self) -> ReadAfterWrite
    where
        Ev: Send + Sync + 'static,
    {
        ReadAfterWrite {
            waits: Arc::new(self.live.clone()),
            cookie: Arc::clone(&self.settings.cookie),
            cap: self.settings.cap,
        }
    }
}

/// A live write: put it in the response, where it sets the person's
/// read-after-write cookie. A path with no response (the CLI) drops it,
/// saying why.
#[must_use = "put it in the response: it is the person's read-after-write cookie"]
pub struct Written {
    data_id: DataId,
    cookie: Arc<Cookie>,
}

impl Written {
    #[must_use]
    pub const fn data_id(&self) -> DataId {
        self.data_id
    }
}

impl IntoResponseParts for Written {
    type Error = Infallible;

    fn into_response_parts(self, mut res: ResponseParts) -> Result<ResponseParts, Self::Error> {
        // The value is the cookie's own name, a hex id and a path from the
        // mount table: always a valid header value.
        if let Ok(value) = HeaderValue::from_str(&self.cookie.set(self.data_id)) {
            res.headers_mut().append(SET_COOKIE, value);
        }
        Ok(res)
    }
}

/// `wait_for`, with the event type erased: boot holds apps as trait objects
/// by the time it builds their routes.
trait Waits: Send + Sync {
    fn wait_for(&self, d: DataId, cap: Duration) -> BoxFuture<'_, Wait>;
}

impl<Ev: WithWireFormat + Send + Sync + 'static> Waits for nvzn::Live<Ev> {
    fn wait_for(&self, d: DataId, cap: Duration) -> BoxFuture<'_, Wait> {
        nvzn::Live::wait_for(self, d, cap).boxed()
    }
}

/// One app's read side of read-after-write, for its router's layer.
#[derive(Clone)]
pub struct ReadAfterWrite {
    waits: Arc<dyn Waits>,
    cookie: Arc<Cookie>,
    cap: Duration,
}

impl ReadAfterWrite {
    /// `router` with the wait in front of every route.
    pub(crate) fn layer(self, router: axum::Router) -> axum::Router {
        router.layer(axum::middleware::from_fn_with_state(self, wait_before))
    }
}

/// The person's last write, if their cookie names one: wait (up to the cap)
/// until this server's live view has it. Never fails a request.
async fn wait_before(State(raw): State<ReadAfterWrite>, request: Request, next: Next) -> Response {
    if let Some(d) = raw.cookie.find(&request) {
        let started = Instant::now();
        let wait = raw.waits.wait_for(d, raw.cap).await;
        record(&raw.cookie, Site::Read, wait, started.elapsed());
    }
    next.run(request).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::response::IntoResponse;
    use std::sync::Mutex;
    use tower::ServiceExt as _;

    fn d(n: u8) -> DataId {
        DataId::from_bytes([n; 32])
    }

    fn cookie() -> Arc<Cookie> {
        LiveSettings::new(
            "robin",
            &BasePath::under("robin"),
            CookiePolicy::RequireHttps,
            DEFAULT_WAIT,
        )
        .cookie
    }

    #[test]
    fn a_written_sets_the_apps_cookie_with_its_attributes() {
        let written = Written {
            data_id: d(0xab),
            cookie: cookie(),
        };
        let res = (written, "ok").into_response();
        let set = res.headers().get(SET_COOKIE).unwrap().to_str().unwrap();
        assert_eq!(
            set,
            format!(
                "rw_robin={}; Path=/robin/; HttpOnly; SameSite=Lax; Max-Age=10; Secure",
                "ab".repeat(32)
            )
        );
    }

    #[test]
    fn the_cookie_round_trips_and_ignores_the_rest() {
        let c = cookie();
        let header = format!(
            "other=1; rw_palmtree={}; rw_robin={}",
            "00".repeat(32),
            "ab".repeat(32)
        );
        let request = Request::builder()
            .header(COOKIE, header)
            .body(Body::empty())
            .unwrap();
        assert_eq!(c.find(&request), Some(d(0xab)));

        let garbled = Request::builder()
            .header(COOKIE, "rw_robin=zz")
            .body(Body::empty())
            .unwrap();
        assert_eq!(c.find(&garbled), None, "an unparsable cookie is ignored");
        let none = Request::builder().body(Body::empty()).unwrap();
        assert_eq!(c.find(&none), None);
    }

    /// Records the ids it was asked about; answers `Seen`, or sleeps to the cap.
    struct Fake {
        asked: Mutex<Vec<DataId>>,
        seen: bool,
    }

    impl Waits for Fake {
        fn wait_for(&self, d: DataId, cap: Duration) -> BoxFuture<'_, Wait> {
            self.asked.lock().unwrap().push(d);
            let seen = self.seen;
            async move {
                if seen {
                    Wait::Seen
                } else {
                    tokio::time::sleep(cap).await;
                    Wait::TimedOut
                }
            }
            .boxed()
        }
    }

    fn app(fake: Arc<Fake>, cap: Duration) -> axum::Router {
        let raw = ReadAfterWrite {
            waits: fake,
            cookie: cookie(),
            cap,
        };
        raw.layer(axum::Router::new().route("/", axum::routing::get(|| async { "page" })))
    }

    #[tokio::test]
    async fn the_layer_waits_only_for_a_parsable_cookie() {
        let fake = Arc::new(Fake {
            asked: Mutex::new(Vec::new()),
            seen: true,
        });
        let router = app(Arc::clone(&fake), DEFAULT_WAIT);
        let with = Request::builder()
            .uri("/")
            .header(COOKIE, format!("rw_robin={}", "ab".repeat(32)))
            .body(Body::empty())
            .unwrap();
        assert!(
            router
                .clone()
                .oneshot(with)
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let without = Request::builder().uri("/").body(Body::empty()).unwrap();
        assert!(router.oneshot(without).await.unwrap().status().is_success());
        assert_eq!(*fake.asked.lock().unwrap(), vec![d(0xab)]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_layer_gives_up_at_the_cap_and_serves_the_page() {
        let fake = Arc::new(Fake {
            asked: Mutex::new(Vec::new()),
            seen: false,
        });
        let cap = Duration::from_millis(200);
        let router = app(fake, cap);
        let request = Request::builder()
            .uri("/")
            .header(COOKIE, format!("rw_robin={}", "ab".repeat(32)))
            .body(Body::empty())
            .unwrap();
        let started = tokio::time::Instant::now();
        let res = router.oneshot(request).await.unwrap();
        assert!(res.status().is_success(), "a wait never fails the request");
        assert_eq!(started.elapsed(), cap);
    }
}
