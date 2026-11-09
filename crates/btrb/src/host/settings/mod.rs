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

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use crate::CookiePolicy;
use tempfile::TempDir;
use zslg::{FollowerNodeConfig, ReplicaNodeConfig};

pub(crate) use crate::config::ConfigError;
use crate::config::{Env, parse};

mod cache;
mod store;

pub(crate) use cache::CacheSetup;

mod fields;
mod replica_node;

pub(crate) use self::fields::{
    compression, count, directory, jobs, listen_addr, object_store, open_files, peers, socket_addr,
    spare_selection, tiers,
};
pub(crate) use self::replica_node::zslg_config;

/// Variables under these prefixes are ours: read them, or reject them. The
/// apps' own prefixes (`OTHER_`) come from the app list.
const PREFIXES: [&str; 3] = ["ZSLG_", "NVZN_", "BTRB_"];

fn prefixes(apps: &[crate::Entry]) -> Vec<String> {
    PREFIXES
        .iter()
        .map(|p| (*p).to_owned())
        .chain(crate::env_prefixes(apps))
        .collect()
}

fn snapshot(apps: &[crate::Entry]) -> Result<Env, ConfigError> {
    let prefixes = prefixes(apps);
    let prefixes: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    Env::from_process(&prefixes)
}

/// Every app in the binary's list reads its own slice of the environment.
fn configure_apps(
    env: &mut Env,
    cookies: CookiePolicy,
    apps: &[crate::Entry],
) -> Result<Vec<crate::Ready>, ConfigError> {
    let mut read_after_write_wait = crate::live::DEFAULT_WAIT;
    env.set(
        &mut read_after_write_wait,
        "BTRB_READ_AFTER_WRITE_WAIT",
        parse::duration,
    )?;
    let global = crate::Global {
        cookies,
        read_after_write_wait,
    };
    crate::configure_all(env, &global, apps).map_err(|e| ConfigError::Other(e.to_string()))
}

/// The sentinel path standing for "a fresh directory, gone when the process is".
const TEMP: &str = ":temp:";

/// The sentinel object-store backend standing for "in this process's heap".
const MEMORY: &str = ":memory:";

const DEFAULT_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
const DEFAULT_METRICS_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 9100);

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
    pub(crate) apps: Vec<crate::Ready>,
    /// The cache this process opens once its apps are built.
    pub(crate) cache: Option<CacheSetup>,
    pub(crate) _temp: Vec<TempDir>,
}

/// Read every setting, then reject any `ZSLG_*` / `NVZN_*` / `BTRB_*` variable
/// nothing claimed.
pub(crate) fn from_env(entries: &[crate::Entry]) -> Result<Settings, ConfigError> {
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
    entries: &[crate::Entry],
) -> Result<StandaloneSettings, ConfigError> {
    let (server, cli_socket) = read_server(entries, true)?;
    let cli_socket = cli_socket.ok_or_else(|| {
        ConfigError::Other("a standalone process needs BTRB_CLI_SOCKET".to_owned())
    })?;
    Ok(StandaloneSettings { server, cli_socket })
}

/// A standalone process is one replica: its peers are itself.
fn alone(zslg: &ReplicaNodeConfig) -> Result<(), ConfigError> {
    let others = zslg
        .peers
        .iter()
        .filter(|p| p.server_name != zslg.network_server_name)
        .count();
    if others == 0 {
        return Ok(());
    }
    Err(ConfigError::Other(format!(
        "ZSLG_PEERS names {others} other node(s): a standalone process is one node \
         (use `serve` and `work` for more)"
    )))
}

#[allow(
    clippy::large_stack_frames,
    reason = "one-shot startup: the frame is the sum of every builder's config and is gone before serving"
)]
fn read_server(
    entries: &[crate::Entry],
    standalone: bool,
) -> Result<(Settings, Option<PathBuf>), ConfigError> {
    let mut env = snapshot(entries)?;
    let mut temp = Vec::new();

    let (zslg_memory, nvzn_memory) = if standalone {
        (STANDALONE_ZSLG_MEMORY, STANDALONE_NVZN_MEMORY)
    } else {
        (SERVER_STORAGE_MEMORY, SERVER_STORAGE_MEMORY)
    };
    let zslg = zslg_config(&mut env, &mut temp, zslg_memory)?;
    if standalone {
        alone(&zslg)?;
    }
    let nvzn = nvzn_config(&mut env, &mut temp, nvzn_memory)?;

    let mut http_listen_addr = DEFAULT_LISTEN_ADDR;
    env.set(&mut http_listen_addr, "BTRB_LISTEN_ADDR", socket_addr)?;

    let cookies = cookie_policy(&mut env)?;
    let cache = Some(cache::read(&mut env)?);
    let apps = configure_apps(&mut env, cookies, entries)?;
    let cli_socket = standalone.then(|| cli_socket(&mut env)).transpose()?;

    let mut metrics_listen_addr = Some(DEFAULT_METRICS_LISTEN_ADDR);
    env.set_optional(
        &mut metrics_listen_addr,
        "BTRB_METRICS_LISTEN_ADDR",
        socket_addr,
    )?;

    temp.extend(env.take_temp());
    env.finish()?;

    tracing::info!(
        settings = ?crate::config::env::present_names(
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
    pub(crate) apps: Vec<crate::Ready>,
    pub(crate) cache: CacheSetup,
    pub(crate) _temp: Vec<TempDir>,
}

/// Read the worker's settings and reject anything unclaimed: the worker is a
/// long-running process with its own environment.
pub(crate) fn worker_from_env(apps: &[crate::Entry]) -> Result<WorkerSettings, ConfigError> {
    let mut env = snapshot(apps)?;
    let settings = worker_config(&mut env, apps)?;
    env.finish()?;
    tracing::info!(
        settings = ?crate::config::env::present_names(
            &prefixes(apps).iter().map(String::as_str).collect::<Vec<_>>()
        ),
        "worker configuration read from the environment"
    );
    Ok(settings)
}

fn worker_config(env: &mut Env, entries: &[crate::Entry]) -> Result<WorkerSettings, ConfigError> {
    let mut temp = Vec::new();
    let peers_raw = env.required("ZSLG_PEERS")?;
    let peers = peers("ZSLG_PEERS", &peers_raw)?;
    let object_store_raw = env.required("ZSLG_OBJECT_STORE")?;
    let object_store = object_store("ZSLG_OBJECT_STORE", &object_store_raw, &mut temp)?;
    let db_path_raw = env.required("ZSLG_DB_PATH")?;
    let db_path = directory("ZSLG_DB_PATH", &db_path_raw, &mut temp)?;

    let mut zslg = FollowerNodeConfig::new(peers, db_path, object_store).map_err(|source| {
        ConfigError::Rejected {
            var: "ZSLG_OBJECT_STORE",
            source,
        }
    })?;
    zslg.storage_memory = WORKER_STORAGE_MEMORY;
    env.set(
        &mut zslg.storage_memory,
        "ZSLG_STORAGE_MEMORY",
        parse::bytes,
    )?;

    let nvzn = nvzn_config(env, &mut temp, WORKER_STORAGE_MEMORY)?;

    let mut metrics_listen_addr = None;
    env.set_optional(
        &mut metrics_listen_addr,
        "BTRB_METRICS_LISTEN_ADDR",
        socket_addr,
    )?;

    let cli_socket = cli_socket(env)?;
    let cookies = cookie_policy(env)?;
    let cache = cache::read(env)?;
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
    cli_socket(&mut env)
}

fn cli_socket(env: &mut Env) -> Result<PathBuf, ConfigError> {
    let raw = env.required("BTRB_CLI_SOCKET")?;
    if raw.trim().is_empty() {
        return Err(ConfigError::Other(
            "BTRB_CLI_SOCKET is empty: it is the path of the worker's socket".to_owned(),
        ));
    }
    Ok(PathBuf::from(raw))
}

// ── apps ─────────────────────────────────────────────────────────────────────

/// Read the settings every mounted app is handed.
///
/// `BTRB_INSECURE` turns off the `Secure` cookie attribute so a plain-HTTP
/// `localhost` run can hold a session. It defaults to off, and it is loud when
/// on: a deployment that reaches this branch by accident is serving session
/// cookies a plain-HTTP hop can read, and the warning is the only thing that
/// will say so.
///
/// It is read here rather than in the app so the `BTRB_` prefix brings it under
/// `Env::finish`'s unknown-variable check. Without that a typo reads as "not
/// set" — safe for this variable, and not for the next one.
fn cookie_policy(env: &mut Env) -> Result<CookiePolicy, ConfigError> {
    let mut insecure = false;
    env.set(&mut insecure, "BTRB_INSECURE", parse::boolean)?;

    if insecure {
        tracing::warn!(
            "BTRB_INSECURE is set: cookies will not carry the Secure attribute, so any \
             plain-HTTP hop can read a session. Development only."
        );
        Ok(CookiePolicy::AllowPlainHttp)
    } else {
        Ok(CookiePolicy::RequireHttps)
    }
}

// ── nvzn ─────────────────────────────────────────────────────────────────────

fn nvzn_config(
    env: &mut Env,
    temp: &mut Vec<TempDir>,
    memory: zslg::ByteSize,
) -> Result<nvzn::StoreConfig, ConfigError> {
    let path_raw = env.required("NVZN_DB_PATH")?;
    let mut c = nvzn::StoreConfig::new(directory("NVZN_DB_PATH", &path_raw, temp)?);

    c.memory = memory;
    env.set(&mut c.memory, "NVZN_MEMORY", parse::bytes)?;
    env.set(
        &mut c.flush_interval,
        "NVZN_FLUSH_INTERVAL",
        parse::duration,
    )?;
    env.set_optional(&mut c.write_buffer, "NVZN_WRITE_BUFFER", parse::bytes)?;
    env.set_optional(
        &mut c.target_file_size,
        "NVZN_TARGET_FILE_SIZE",
        parse::bytes,
    )?;
    env.set_optional(&mut c.max_total_wal, "NVZN_MAX_TOTAL_WAL", parse::bytes)?;
    env.set_optional(&mut c.max_background_jobs, "NVZN_MAX_BACKGROUND_JOBS", jobs)?;
    env.set_optional(&mut c.max_open_files, "NVZN_MAX_OPEN_FILES", open_files)?;
    env.set_optional(&mut c.retire_sweep, "NVZN_RETIRE_SWEEP", parse::duration)?;
    env.set(&mut c.fold_commit, "NVZN_FOLD_COMMIT", parse::bytes)?;

    Ok(c)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests;
