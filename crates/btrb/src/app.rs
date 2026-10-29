//! The seam between the runtime and an app: what an author implements, what the
//! runtime makes of it.
//!
//! ```text
//!   Entry::of::<A>(base)       one line of a binary's list
//!        │ configure_all       A::config reads the app's env slice (Env::finish then
//!        ▼                     rejects whatever nobody read, before any store opens)
//!      Ready ── boot ──▶ A::new(Nvzn, AppCache) ──▶ Booted { base, Box<dyn Mounted>,
//!                                                           read_after_write }
//!
//!   Mounted::router      server      mount at its BasePath
//!   Mounted::background  worker      jobs that end only when told to
//!   Mounted::standalone  standalone  both, from one instance
//!   Mounted::admin       worker      the calls the CLI makes over the socket
//!   Entry::command       cli         one subcommand, through a Console and a
//!                                    Remote; nothing is booted
//! ```
//!
//! `Event` and `Config` differ per app, so the list is not `Vec<dyn App>`: an
//! app is erased after it was configured and built. An app never sees the
//! dispatcher, a second tenant, or the fold's start (see [`Nvzn`]), and `NAME`
//! is permanent: it is the env prefix and, with version 0, the tenant that
//! hashes into every key the app writes.

use crate::admin::{Handler, Remote, Served};
use crate::base_path::BasePath;
use crate::config::{Env, Scope};
use crate::console::Console;
use crate::live::{Live, LiveSettings, ReadAfterWrite};
use crate::metrics::Histogram;
use crate::{CookiePolicy, Error};
use axum::Router;
use clap::{ArgMatches, FromArgMatches, Subcommand};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use nvzn::{
    AssociatedProjectorManager, Dispatcher, EventSet, EventWriter, Projection, Reader, SubEventOf,
    Tenant, WithWireFormat,
};
use objectstorage_cache::{AppCache, Builder as CacheBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Deployment facts every app is given, by value.
#[derive(Debug, Clone, Copy)]
pub struct Global {
    pub cookies: CookiePolicy,
    /// `BTRB_READ_AFTER_WRITE_WAIT`: the cap on either read-after-write wait.
    pub read_after_write_wait: Duration,
}

/// The runtime's request to stop: SIGTERM, or the fold stopping.
#[derive(Clone, Default)]
pub struct Shutdown(CancellationToken);

/// Proof that shutdown was requested. A background job returns it, so a job
/// cannot end with `Ok` before shutdown: there is no way to build one.
#[derive(Debug)]
pub struct Stopped(());

impl Shutdown {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Shares `token`'s state: cancelling either cancels both.
    #[must_use]
    pub const fn from_token(token: CancellationToken) -> Self {
        Self(token)
    }

    /// The underlying token, for code that waits on a `CancellationToken`.
    #[must_use]
    pub fn token(&self) -> CancellationToken {
        self.0.clone()
    }

    pub fn cancel(&self) {
        self.0.cancel();
    }

    #[must_use]
    pub fn is_stopping(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Resolves once shutdown was requested.
    pub async fn wait(&self) -> Stopped {
        self.0.cancelled().await;
        Stopped(())
    }
}

/// What [`Nvzn::finish`] hands an app: its writer, reader and live view.
pub type Handles<Ev, L> = (EventWriter<Ev>, Reader<Ev>, Live<Ev, L>);

/// What an app may do with nvzn: register its projections, then take its
/// writer, reader and live view. The manager borrows the dispatcher, so
/// registration is sequential per app and the handle cannot outlive `App::new`.
///
/// Generic over the event type and the live set `L` (an owned subset), not
/// over the `App`: a test rig below the app's crate builds one too.
pub struct Nvzn<'d, Ev, L> {
    manager: AssociatedProjectorManager<'d, Ev>,
    settings: LiveSettings,
    /// Boot's slot for the request layer; a rig's `Nvzn` has none.
    read_after_write: Option<&'d mut Option<ReadAfterWrite>>,
    _live_set: PhantomData<fn() -> L>,
}

impl<'d, Ev, L> Nvzn<'d, Ev, L> {
    /// A rig's way in (`LiveSettings::for_test()`): no request layer. Apps are
    /// handed an `Nvzn` by the runtime, they never make one.
    #[must_use]
    pub fn new(manager: AssociatedProjectorManager<'d, Ev>, settings: LiveSettings) -> Self {
        Self {
            manager,
            settings,
            read_after_write: None,
            _live_set: PhantomData,
        }
    }

    fn booted(
        manager: AssociatedProjectorManager<'d, Ev>,
        settings: LiveSettings,
        slot: &'d mut Option<ReadAfterWrite>,
    ) -> Self {
        Self {
            manager,
            settings,
            read_after_write: Some(slot),
            _live_set: PhantomData,
        }
    }
}

impl<Ev, L> Nvzn<'_, Ev, L>
where
    Ev: WithWireFormat + Send + Sync + 'static,
    L: EventSet + Into<Ev>,
{
    /// Registers `P` and every index of it.
    pub fn register<P>(&mut self) -> Result<(), nvzn::Error>
    where
        P: Projection,
        for<'e> P::Event<'e>: SubEventOf<'e, Ev>,
    {
        self.manager.register::<P>()
    }

    /// Ends registration: the writer, the reader, and the app's live view
    /// (it starts once the fold has replayed the log).
    pub fn finish(self) -> Result<Handles<Ev, L>, nvzn::Error> {
        let (writer, reader) = self.manager.finalise()?;
        let view = reader.live::<L>()?;
        let live = Live::new(view, writer.clone(), self.settings);
        if let Some(slot) = self.read_after_write
            && !L::EVENTS.is_empty()
        {
            *slot = Some(live.read_after_write());
        }
        Ok((writer, reader, live))
    }
}

/// The events of an app that keeps nothing in the log: there are none.
#[derive(Debug, Clone, Copy)]
pub enum NoEvents {}

/// `NoEvents` on the wire: also empty.
#[derive(Debug, Serialize, Deserialize)]
pub enum NoWire {}

impl From<NoWire> for NoEvents {
    fn from(wire: NoWire) -> Self {
        match wire {}
    }
}

impl From<NoEvents> for NoWire {
    fn from(event: NoEvents) -> Self {
        match event {}
    }
}

impl From<&NoEvents> for NoWire {
    fn from(event: &NoEvents) -> Self {
        match *event {}
    }
}

/// An app with no events has no live set either.
impl EventSet for NoEvents {
    const EVENTS: &'static [u32] = &[];
}

impl WithWireFormat for NoEvents {
    type WireFormat = NoWire;
    type WireFormatRef<'a> = NoWire;

    fn number(&self) -> u32 {
        match *self {}
    }
}

/// No subcommands: for an app without CLI commands.
#[derive(Debug, Clone, Copy)]
pub enum Nothing {}

impl FromArgMatches for Nothing {
    fn from_arg_matches(_: &ArgMatches) -> Result<Self, clap::Error> {
        Err(clap::Error::new(clap::error::ErrorKind::InvalidSubcommand))
    }

    fn update_from_arg_matches(&mut self, _: &ArgMatches) -> Result<(), clap::Error> {
        match *self {}
    }
}

impl Subcommand for Nothing {
    fn augment_subcommands(cmd: clap::Command) -> clap::Command {
        cmd
    }

    fn augment_subcommands_for_update(cmd: clap::Command) -> clap::Command {
        cmd
    }

    fn has_subcommand(_: &str) -> bool {
        false
    }
}

/// A background job: ends only when told to (or fails).
pub type Job = BoxFuture<'static, Result<Stopped, Error>>;

/// Run every app's background jobs together. The first job to fail cancels
/// `stop`; the rest drain, and that first error is returned. With no jobs this
/// waits for shutdown, so a worker of job-less apps is still a worker.
pub async fn run_background(stop: &Shutdown, jobs: Vec<Job>) -> Result<(), Error> {
    if jobs.is_empty() {
        stop.wait().await;
        return Ok(());
    }
    let mut running: FuturesUnordered<_> = jobs.into_iter().collect();
    let mut first = None;
    while let Some(done) = running.next().await {
        if let Err(e) = done {
            stop.cancel();
            first.get_or_insert(e);
        }
    }
    first.map_or(Ok(()), Err)
}

/// An app: a tenant of the log, a router, background work, CLI commands.
///
/// `router`, `background` and `standalone` consume `self`: a process runs
/// exactly one of them — the server, the worker, or both roles in one process.
/// Anything role-specific is built inside the method of its role; `new` is
/// what they share. The CLI builds no app: `command` reaches the worker's
/// through `Request`/`Response` (see [`crate::admin`]).
pub trait App: Sized + Send + 'static {
    /// `[a-z]+`: the env prefix (`APP_`) and the tenant name. Permanent.
    const NAME: &'static str;
    type Event: WithWireFormat + Send + Sync + 'static;
    /// The live set: an owned subset of `Event` (`define_events!`'s `owned`
    /// block), what [`Live::write`] takes. `NoEvents` for an app without events.
    type Live: EventSet + Into<Self::Event> + Send + 'static;
    /// What `config` reads. A zero-sized `Config` claims no env prefix.
    type Config: Send + 'static;
    type Command: Subcommand + FromArgMatches + Send + 'static;
    /// The calls `command` makes, answered by `admin` in the worker.
    type Request: Serialize + DeserializeOwned + Send + 'static;
    type Response: Serialize + DeserializeOwned + Send + 'static;
    /// The fingerprint of the file holding every type `Request` and `Response`
    /// are made of: `admin::fingerprint(include_str!(…))`.
    const WIRE: u64;

    fn config(env: &mut Scope<'_>, global: &Global) -> Result<Self::Config, Error>;
    /// Build the app: register projections and cache kinds. `cache` is this
    /// app's part of the process's cache; its kinds are registered here, and
    /// reads start after the boot.
    fn new(
        nvzn: Nvzn<'_, Self::Event, Self::Live>,
        cache: AppCache,
        config: Self::Config,
    ) -> Result<Self, Error>;

    fn describe_metrics() {}

    /// The bounds of the histograms this app records; any it does not list
    /// get the exporter's defaults.
    #[must_use]
    fn histograms() -> &'static [Histogram] {
        &[]
    }

    /// Routes relative to the app; `base` only for what must be absolute.
    fn router(self, base: &BasePath) -> Result<Router, Error>;

    /// Jobs that end only when `stop` fires (or fail).
    fn background(self, _stop: Shutdown) -> Vec<Job> {
        Vec::new()
    }

    /// Both roles from one instance: what `router` and `background` build,
    /// sharing what they share, in a process that serves and works.
    ///
    /// Without a default: "the router, no jobs" would silently drop the jobs
    /// of an app that overrides `background`.
    fn standalone(self, base: &BasePath, stop: Shutdown) -> Result<(Router, Vec<Job>), Error>;

    /// The worker's answers to `Request`s; asked once, before `background`.
    /// `None`: this app takes no calls.
    fn admin(&self) -> Option<Handler<Self::Request, Self::Response>> {
        None
    }

    /// One subcommand, in the CLI: prompts and printing through `console`,
    /// the work through `remote`.
    fn command(
        cmd: Self::Command,
        console: &mut dyn Console,
        remote: &Remote<Self::Request, Self::Response>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// An app after `boot`: configured, built, its types erased.
pub trait Mounted: Send {
    fn name(&self) -> &'static str;
    fn describe_metrics(&self);
    fn router(self: Box<Self>, base: &BasePath) -> Result<Router, Error>;
    fn background(self: Box<Self>, stop: Shutdown) -> Vec<Job>;
    fn standalone(
        self: Box<Self>,
        base: &BasePath,
        stop: Shutdown,
    ) -> Result<(Router, Vec<Job>), Error>;
    /// The app's calls with their types erased, for the worker's socket.
    fn admin(&self) -> Option<Served>;
}

struct Wrapped<A: App>(A);

impl<A: App> Mounted for Wrapped<A> {
    fn name(&self) -> &'static str {
        A::NAME
    }

    fn describe_metrics(&self) {
        A::describe_metrics();
    }

    fn router(self: Box<Self>, base: &BasePath) -> Result<Router, Error> {
        self.0.router(base)
    }

    fn background(self: Box<Self>, stop: Shutdown) -> Vec<Job> {
        self.0.background(stop)
    }

    fn standalone(
        self: Box<Self>,
        base: &BasePath,
        stop: Shutdown,
    ) -> Result<(Router, Vec<Job>), Error> {
        self.0.standalone(base, stop)
    }

    fn admin(&self) -> Option<Served> {
        self.0
            .admin()
            .map(|handler| Served::new(A::NAME, A::WIRE, handler))
    }
}

/// `A`'s subcommand, run against the worker listening on `socket`.
fn command<'a, A: App>(
    matches: &'a ArgMatches,
    console: &'a mut dyn Console,
    socket: &'a Path,
) -> BoxFuture<'a, Result<(), Error>> {
    Box::pin(async move {
        let cmd = A::Command::from_arg_matches(matches).map_err(Error::new)?;
        let remote = Remote::socket(socket.to_owned(), A::NAME, A::WIRE);
        A::command(cmd, console, &remote).await
    })
}

/// An app after `configure`: its settings are read (and consumed from the
/// environment); nothing is registered with nvzn yet.
trait Pending: Send {
    fn boot(
        self: Box<Self>,
        dispatcher: &mut Dispatcher,
        cache: AppCache,
        settings: LiveSettings,
    ) -> Result<(Box<dyn Mounted>, Option<ReadAfterWrite>), Error>;
}

struct Configured<A: App>(A::Config);

impl<A: App> Pending for Configured<A> {
    /// Registers the tenant (`Tenant::new(NAME, 0)`) and builds the app.
    fn boot(
        self: Box<Self>,
        dispatcher: &mut Dispatcher,
        cache: AppCache,
        settings: LiveSettings,
    ) -> Result<(Box<dyn Mounted>, Option<ReadAfterWrite>), Error> {
        let manager = dispatcher.new_handler::<A::Event>(Tenant::new(A::NAME, 0));
        let mut read_after_write = None;
        let app = A::new(
            Nvzn::booted(manager, settings, &mut read_after_write),
            cache,
            self.0,
        )?;
        Ok((Box::new(Wrapped(app)), read_after_write))
    }
}

fn configure<A: App>(env: &mut Env, global: &Global) -> Result<Box<dyn Pending>, Error> {
    let mut scope = env.scope(A::NAME);
    Ok(Box::new(Configured::<A>(A::config(&mut scope, global)?)))
}

fn check_name(name: &str) -> Result<(), Error> {
    if !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase()) {
        Ok(())
    } else {
        Err(Error::new(format!(
            "app name {name:?} is not [a-z]+ (it is the env prefix and the tenant)"
        )))
    }
}

/// One line of a binary's app list: the app and where it is mounted.
#[derive(Clone, Copy)]
pub struct Entry {
    name: &'static str,
    base: BasePath,
    claims_env: bool,
    configure: Configure,
    augment: fn(clap::Command) -> clap::Command,
    command: CommandFn,
    describe_metrics: fn(),
    histograms: fn() -> &'static [Histogram],
}

type CommandFn =
    for<'a> fn(&'a ArgMatches, &'a mut dyn Console, &'a Path) -> BoxFuture<'a, Result<(), Error>>;

type Configure = fn(&mut Env, &Global) -> Result<Box<dyn Pending>, Error>;

impl Entry {
    #[must_use]
    pub const fn of<A: App>(base: BasePath) -> Self {
        Self {
            name: A::NAME,
            base,
            claims_env: std::mem::size_of::<A::Config>() != 0,
            configure: configure::<A>,
            augment: <A::Command as Subcommand>::augment_subcommands,
            command: command::<A>,
            describe_metrics: A::describe_metrics,
            histograms: A::histograms,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn base(&self) -> BasePath {
        self.base
    }

    /// Registers the help text of this app's series.
    pub fn describe_metrics(&self) {
        (self.describe_metrics)();
    }

    /// The bounds of this app's histograms.
    #[must_use]
    pub fn histograms(&self) -> &'static [Histogram] {
        (self.histograms)()
    }

    /// Adds this app's subcommands to `cli` (before anything is connected).
    #[must_use]
    pub fn augment(&self, cli: clap::Command) -> clap::Command {
        (self.augment)(cli)
    }

    /// Run this app's subcommand (`matches` under its name) against the
    /// worker on `socket`. Nothing is configured or booted here.
    pub fn command<'a>(
        &self,
        matches: &'a ArgMatches,
        console: &'a mut dyn Console,
        socket: &'a Path,
    ) -> BoxFuture<'a, Result<(), Error>> {
        (self.command)(matches, console, socket)
    }
}

/// The env prefixes the list claims (`APP_`): what `Env::from_process` snapshots.
#[must_use]
pub fn env_prefixes(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter(|e| e.claims_env)
        .map(|e| format!("{}_", e.name.to_ascii_uppercase()))
        .collect()
}

/// An app whose settings are read, waiting for the dispatcher.
pub struct Ready {
    base: BasePath,
    name: &'static str,
    pending: Box<dyn Pending>,
    global: Global,
}

/// An app booted, with where it is mounted.
pub struct Booted {
    pub base: BasePath,
    pub app: Box<dyn Mounted>,
    /// The read side of read-after-write, for its routes; none for an app
    /// without a live set.
    pub read_after_write: Option<ReadAfterWrite>,
}

impl Booted {
    /// `routes` with the read-after-write wait in front, if this app has one.
    pub fn wrap(read_after_write: Option<ReadAfterWrite>, routes: Router) -> Router {
        match read_after_write {
            Some(raw) => raw.layer(routes),
            None => routes,
        }
    }
}

impl Ready {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Phase 1 of nvzn: register the tenant and build the app, which gets
    /// its part of the process's cache.
    pub fn boot(
        self,
        dispatcher: &mut Dispatcher,
        cache: &mut CacheBuilder,
    ) -> Result<Booted, Error> {
        let app_cache = cache.app(self.name);
        let settings = LiveSettings::new(
            self.name,
            &self.base,
            self.global.cookies,
            self.global.read_after_write_wait,
        );
        let (app, read_after_write) = self.pending.boot(dispatcher, app_cache, settings)?;
        Ok(Booted {
            base: self.base,
            app,
            read_after_write,
        })
    }
}

/// Read every app's settings, in list order. Names and mounts are checked
/// first; the caller then asks the environment to account for the rest
/// (`Env::finish`) before anything is opened.
pub fn configure_all(
    env: &mut Env,
    global: &Global,
    entries: &[Entry],
) -> Result<Vec<Ready>, Error> {
    let mut names = BTreeSet::new();
    let mut mounts = BTreeSet::new();
    for e in entries {
        check_name(e.name)?;
        if !names.insert(e.name) {
            return Err(Error::new(format!("two apps are named {:?}", e.name)));
        }
        if !mounts.insert(e.base.path()) {
            return Err(Error::new(format!(
                "two apps are mounted at {}",
                e.base.path()
            )));
        }
    }
    entries
        .iter()
        .map(|e| {
            Ok(Ready {
                base: e.base,
                name: e.name,
                pending: (e.configure)(env, global)?,
                global: *global,
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    /// An app with a route and one job, for the shapes of a process.
    struct Probe;

    impl App for Probe {
        const NAME: &'static str = "probe";
        type Event = NoEvents;
        type Live = NoEvents;
        type Config = ();
        type Command = Nothing;
        type Request = crate::NoCalls;
        type Response = crate::NoCalls;
        const WIRE: u64 = 0;

        fn config(_: &mut Scope<'_>, _: &Global) -> Result<(), Error> {
            Ok(())
        }

        fn new(_: Nvzn<'_, NoEvents, NoEvents>, _: AppCache, (): ()) -> Result<Self, Error> {
            Ok(Self)
        }

        fn router(self, _: &BasePath) -> Result<Router, Error> {
            Ok(Router::new().route("/", axum::routing::get(|| async { "probe" })))
        }

        fn standalone(self, base: &BasePath, stop: Shutdown) -> Result<(Router, Vec<Job>), Error> {
            let job: Job = Box::pin(async move { Ok(stop.wait().await) });
            Ok((self.router(base)?, vec![job]))
        }

        async fn command(
            cmd: Nothing,
            _: &mut dyn Console,
            _: &Remote<crate::NoCalls, crate::NoCalls>,
        ) -> Result<(), Error> {
            match cmd {}
        }
    }

    #[tokio::test]
    async fn standalone_gives_the_router_and_the_jobs_of_one_instance() {
        use tower::ServiceExt;
        let stop = Shutdown::new();
        let mounted: Box<dyn Mounted> = Box::new(Wrapped(Probe));
        let (router, jobs) = mounted.standalone(&BasePath::ROOT, stop.clone()).unwrap();

        let answer = router
            .oneshot(
                axum::http::Request::get("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(answer.status(), axum::http::StatusCode::OK);

        assert_eq!(jobs.len(), 1);
        let running = tokio::spawn({
            let stop = stop.clone();
            async move { run_background(&stop, jobs).await }
        });
        tokio::task::yield_now().await;
        assert!(!running.is_finished(), "the job ends only when told to");
        stop.cancel();
        running.await.unwrap().unwrap();
    }

    #[test]
    fn a_name_is_lowercase_letters_only() {
        assert!(check_name("app").is_ok());
        for bad in ["", "Invalid", "other-tree", "app_cache", "app2"] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn the_first_failure_stops_the_others_and_is_the_result() {
        let stop = Shutdown::new();
        let patient: BoxFuture<'static, Result<Stopped, Error>> = {
            let stop = stop.clone();
            Box::pin(async move { Ok(stop.wait().await) })
        };
        let failing: BoxFuture<'static, Result<Stopped, Error>> =
            Box::pin(async { Err(Error::new("boom")) });
        let result = run_background(&stop, vec![patient, failing]).await;
        assert_eq!(result.unwrap_err().to_string(), "boom");
        assert!(stop.is_stopping());
    }

    #[tokio::test]
    async fn no_jobs_waits_for_shutdown() {
        let stop = Shutdown::new();
        let waiting = tokio::spawn({
            let stop = stop.clone();
            async move { run_background(&stop, Vec::new()).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        stop.cancel();
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_job_ends_only_when_told() {
        let stop = Shutdown::new();
        let waiting = tokio::spawn({
            let stop = stop.clone();
            async move { stop.wait().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        stop.cancel();
        waiting.await.unwrap();
        assert!(stop.is_stopping());
    }
}
