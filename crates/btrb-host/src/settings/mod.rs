//! Every zslg and nvzn knob this binary reads, plus its own, from the
//! environment.
//!
//! Two entry points, for two processes:
//!
//! ```text
//!   from_env            the server — identity, listeners, durable stores, the
//!                       app's signing key, every zslg/nvzn tunable:
//!     ZSLG_SERVER_NAME=node-a            ZSLG_LISTEN_ADDR=0.0.0.0:9000
//!     ZSLG_PEERS=node-a@a.svc:9000,node-b@b.svc:9000
//!     ZSLG_BOOTSTRAP_SERVER_NAME=node-a
//!     ZSLG_DB_PATH=/var/lib/zslg         NVZN_DB_PATH=/var/lib/nvzn
//!     ZSLG_OBJECT_STORE=:memory:
//!
//!   worker_from_env     the worker — a follower on persistent stores, and
//!                       BTRB_CLI_SOCKET, where the CLI reaches it
//!   cli_socket_from_env the CLI — BTRB_CLI_SOCKET and nothing else
//! ```
//!
//! Optional settings leave the crate default in place; defaults are never
//! restated here because zslg owns them, and the constructor fixes which six
//! are mandatory: they are simply its arguments. `BTRB_` names what this binary
//! owns instead of a crate, so its one default lives here.
//!
//! The CLI never asks `Env::finish`: a CLI sharing a shell with a server must
//! ignore the server's variables, not reject them.
//!
//! Three sentinels carry what a path cannot spell:
//!
//! | `:memory:` | the in-process object store |
//! | `:temp:` | a directory that lives exactly as long as the process |
//! | `off` / `none` | pin an optional field back to the crate's derivation |
//!
//! A `:temp:` guard travels out in [`Settings`] /
//! [`WorkerSettings`]: dropping it deletes the directory under a running
//! RocksDB.
//!
//! Names track the zslg field, except the bare transport ones take
//! `ZSLG_NETWORK_` and the identity four drop it: those get typed by hand.
//!
//! | [`parse`] | the value grammar — units, booleans, sentinels |
//! | [`env`] | the environment as a map consumed by reading, so a typo is caught |

use std::net::SocketAddr;
use std::path::PathBuf;

use btrb_web::CookiePolicy;
use tempfile::TempDir;
use zslg::{FollowerNodeConfig, ReplicaNodeConfig};

pub(crate) use btrb_config::ConfigError;
use btrb_config::{Env, parse};

pub(crate) use btrb_db::settings::CacheSetup;

/// Variables under these prefixes are ours: read them, or reject them. The
/// apps' own prefixes (`OTHER_`) come from the app list.
const PREFIXES: [&str; 3] = ["ZSLG_", "NVZN_", "BTRB_"];

fn prefixes(apps: &[btrb_app::Entry]) -> Vec<String> {
    PREFIXES
        .iter()
        .map(|p| (*p).to_owned())
        .chain(btrb_app::env_prefixes(apps))
        .collect()
}

fn snapshot(apps: &[btrb_app::Entry]) -> Result<Env, ConfigError> {
    let prefixes = prefixes(apps);
    let prefixes: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    Env::from_process(&prefixes)
}

/// Every app in the binary's list reads its own slice of the environment.
fn configure_apps(
    env: &mut Env,
    cookies: CookiePolicy,
    apps: &[btrb_app::Entry],
) -> Result<Vec<btrb_app::Ready>, ConfigError> {
    let mut read_after_write_wait = btrb_live::DEFAULT_WAIT;
    env.set(
        &mut read_after_write_wait,
        "BTRB_READ_AFTER_WRITE_WAIT",
        parse::duration,
    )?;
    let global = btrb_app::Global {
        cookies,
        read_after_write_wait,
    };
    btrb_app::configure_all(env, &global, apps).map_err(|e| ConfigError::Other(e.to_string()))
}

/// host's defaults for both RocksDB budgets (`ZSLG_STORAGE_MEMORY`,
/// `NVZN_MEMORY`) — restated here on purpose. The libraries default to
/// 256 MiB each, which a node fills during an import. The server: 32 MiB
/// each — 134 MB after a 16k-photo import (218 MB at 64 MiB), latency
/// unchanged (e2e log, 2026-09-30). The worker: 64 MiB each, ~200 MB at the
/// peak of an import (app MVP log, future-model design §6.3).
const SERVER_STORAGE_MEMORY: zslg::ByteSize = zslg::ByteSize::from_mib(32);

const WORKER_STORAGE_MEMORY: zslg::ByteSize = zslg::ByteSize::from_mib(64);

/// A standalone process is small on purpose: one node doing both roles, to see
/// how it fares. The log's store 16 MiB, the projections' 32 MiB.
const STANDALONE_ZSLG_MEMORY: zslg::ByteSize = zslg::ByteSize::from_mib(16);

const STANDALONE_NVZN_MEMORY: zslg::ByteSize = zslg::ByteSize::from_mib(32);

/// Both node configurations and this binary's own, plus the guards keeping any
/// `:temp:` directory alive. Hold `temp` for as long as the stores are open.
pub(crate) struct Settings {
    pub(crate) zslg: ReplicaNodeConfig,
    pub(crate) nvzn: nvzn::StoreConfig,
    /// Whether cookies may carry `Secure`: handed to every app, and decides HSTS.
    pub(crate) cookies: CookiePolicy,
    /// Where the HTTP server binds. Unlike zslg's, port 0 is allowed: nothing
    /// advertises this address to a peer, and `main` logs the bound port.
    pub(crate) http_listen_addr: SocketAddr,
    pub(crate) metrics_listen_addr: Option<SocketAddr>,
    /// The apps of the binary's list, configured, waiting for the dispatcher.
    pub(crate) apps: Vec<btrb_app::Ready>,
    /// The cache this process opens once its apps are built.
    pub(crate) cache: Option<CacheSetup>,
    pub(crate) _temp: Vec<TempDir>,
}

/// Read every setting, then reject any `ZSLG_*` / `NVZN_*` / `BTRB_*` variable
/// nothing claimed.
pub(crate) fn from_env(entries: &[btrb_app::Entry]) -> Result<Settings, ConfigError> {
    read_server(entries, false).map(|(settings, _)| settings)
}

/// The server's settings and the CLI socket of a process that also works.
pub(crate) struct StandaloneSettings {
    pub(crate) server: Settings,
    /// Where the CLI reaches this process (`BTRB_CLI_SOCKET`, required).
    pub(crate) cli_socket: PathBuf,
}

/// The server's settings, plus the socket, for a process that serves and
/// works on one node: the budgets are the server's, and the node is alone.
pub(crate) fn standalone_from_env(
    entries: &[btrb_app::Entry],
) -> Result<StandaloneSettings, ConfigError> {
    let (server, cli_socket) = read_server(entries, true)?;
    let cli_socket = cli_socket.ok_or_else(|| {
        ConfigError::Other("a standalone process needs BTRB_CLI_SOCKET".to_owned())
    })?;
    Ok(StandaloneSettings { server, cli_socket })
}

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot startup: the frame is the sum of every builder's config and is gone before serving"
)]
fn read_server(
    entries: &[btrb_app::Entry],
    standalone: bool,
) -> Result<(Settings, Option<PathBuf>), ConfigError> {
    let mut env = snapshot(entries)?;
    let mut temp = Vec::new();

    let (zslg_memory, nvzn_memory) = if standalone {
        (STANDALONE_ZSLG_MEMORY, STANDALONE_NVZN_MEMORY)
    } else {
        (SERVER_STORAGE_MEMORY, SERVER_STORAGE_MEMORY)
    };
    let zslg = btrb_db::settings::zslg_config(&mut env, &mut temp, zslg_memory)?;
    if standalone {
        btrb_db::settings::alone(&zslg)?;
    }
    let nvzn = btrb_db::settings::nvzn_config(&mut env, &mut temp, nvzn_memory)?;

    let http_listen_addr = btrb_web::settings::http_listen_addr(&mut env)?;

    let cookies = btrb_web::settings::cookie_policy(&mut env)?;
    let cache = Some(btrb_db::settings::read_cache(&mut env)?);
    let apps = configure_apps(&mut env, cookies, entries)?;
    let cli_socket = standalone
        .then(|| btrb_cli::settings::cli_socket(&mut env))
        .transpose()?;

    let metrics_listen_addr = btrb_metrics::settings::listen_addr(
        &mut env,
        Some(btrb_metrics::settings::DEFAULT_LISTEN_ADDR),
    )?;

    temp.extend(env.take_temp());
    env.finish()?;

    tracing::info!(
        settings = ?btrb_config::env::present_names(
            &prefixes(entries).iter().map(String::as_str).collect::<Vec<_>>()
        ),
        db_path = ?zslg.db_path,
        server_name = %zslg.network_server_name,
        peers = zslg.peers.len(),
        "configuration read from the environment"
    );

    let settings = Settings {
        zslg,
        nvzn,
        cookies,
        http_listen_addr,
        metrics_listen_addr,
        apps,
        cache,
        _temp: temp,
    };
    Ok((settings, cli_socket))
}

// ── worker ───────────────────────────────────────────────────────────────────

/// The general background worker: a follower node on **persistent** stores (a
/// restart must not re-sync the log or refold every projection), its metrics
/// listener, and each app's own section.
pub(crate) struct WorkerSettings {
    pub(crate) zslg: FollowerNodeConfig,
    pub(crate) nvzn: nvzn::StoreConfig,
    pub(crate) metrics_listen_addr: Option<SocketAddr>,
    /// Where the CLI reaches this worker (`BTRB_CLI_SOCKET`, required).
    pub(crate) cli_socket: PathBuf,
    pub(crate) apps: Vec<btrb_app::Ready>,
    pub(crate) cache: CacheSetup,
    pub(crate) _temp: Vec<TempDir>,
}

/// Read the worker's settings and reject anything unclaimed: the worker is a
/// long-running process with its own environment.
pub(crate) fn worker_from_env(apps: &[btrb_app::Entry]) -> Result<WorkerSettings, ConfigError> {
    let mut env = snapshot(apps)?;
    let settings = worker_config(&mut env, apps)?;
    env.finish()?;
    tracing::info!(
        settings = ?btrb_config::env::present_names(
            &prefixes(apps).iter().map(String::as_str).collect::<Vec<_>>()
        ),
        "worker configuration read from the environment"
    );
    Ok(settings)
}

fn worker_config(
    env: &mut Env,
    entries: &[btrb_app::Entry],
) -> Result<WorkerSettings, ConfigError> {
    let mut temp = Vec::new();
    let zslg = btrb_db::settings::follower_config(env, &mut temp, WORKER_STORAGE_MEMORY)?;

    let nvzn = btrb_db::settings::nvzn_config(env, &mut temp, WORKER_STORAGE_MEMORY)?;

    let metrics_listen_addr = btrb_metrics::settings::listen_addr(env, None)?;

    let cli_socket = btrb_cli::settings::cli_socket(env)?;
    let cookies = btrb_web::settings::cookie_policy(env)?;
    let cache = btrb_db::settings::read_cache(env)?;
    let apps = configure_apps(env, cookies, entries)?;
    temp.extend(env.take_temp());

    Ok(WorkerSettings {
        zslg,
        nvzn,
        metrics_listen_addr,
        cli_socket,
        apps,
        cache,
        _temp: temp,
    })
}

// ── the CLI ──────────────────────────────────────────────────────────────────

/// The one variable the CLI reads: where the worker listens. The rest of the
/// shell is left alone (it may be the worker's own).
pub(crate) fn cli_socket_from_env() -> Result<PathBuf, ConfigError> {
    let mut env = Env::from_process(&["BTRB_"])?;
    btrb_cli::settings::cli_socket(&mut env)
}

// ── apps ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests;
