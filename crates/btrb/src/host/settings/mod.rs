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
use std::sync::Arc;

use crate::CookiePolicy;
use tempfile::TempDir;
use zslg::{
    FollowerNodeConfig, ObjectStoreConfig, PeerAddr, ReplicaNodeConfig, ServerName,
    SpareSelectionStrategy,
};

pub(crate) use crate::config::ConfigError;
use crate::config::{Env, parse};

mod cache;
mod store;

pub(crate) use cache::CacheSetup;

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

// ── zslg ─────────────────────────────────────────────────────────────────────

fn zslg_config(
    env: &mut Env,
    temp: &mut Vec<TempDir>,
    memory: zslg::ByteSize,
) -> Result<ReplicaNodeConfig, ConfigError> {
    let server_name = ServerName::from(env.required("ZSLG_SERVER_NAME")?.trim());
    let peers_raw = env.required("ZSLG_PEERS")?;
    let peers = peers("ZSLG_PEERS", &peers_raw)?;
    let bootstrap = ServerName::from(env.required("ZSLG_BOOTSTRAP_SERVER_NAME")?.trim());
    let listen_raw = env.required("ZSLG_LISTEN_ADDR")?;
    let listen = listen_addr("ZSLG_LISTEN_ADDR", &listen_raw)?;
    let db_path_raw = env.required("ZSLG_DB_PATH")?;
    let db_path = directory("ZSLG_DB_PATH", &db_path_raw, temp)?;
    let object_store_raw = env.required("ZSLG_OBJECT_STORE")?;
    let object_store = object_store("ZSLG_OBJECT_STORE", &object_store_raw, temp)?;

    let mut c =
        ReplicaNodeConfig::new(server_name, peers, bootstrap, listen, db_path, object_store)
            .map_err(|source| ConfigError::Rejected {
                var: "ZSLG_OBJECT_STORE",
                source,
            })?;

    raft(env, &mut c)?;
    snapshots(env, &mut c)?;
    storage(env, &mut c, memory)?;
    replica_log(env, &mut c)?;
    learner(env, &mut c)?;
    stable_log(env, &mut c)?;
    cluster(env, &mut c)?;
    transport(env, &mut c)?;
    writes(env, &mut c)?;

    Ok(c)
}

/// Consensus timing and the log-compaction triggers.
fn raft(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.raft_heartbeat_interval,
        "ZSLG_RAFT_HEARTBEAT_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.raft_election_timeout_min,
        "ZSLG_RAFT_ELECTION_TIMEOUT_MIN",
        parse::duration,
    )?;
    env.set(
        &mut c.raft_election_timeout_max,
        "ZSLG_RAFT_ELECTION_TIMEOUT_MAX",
        parse::duration,
    )?;
    env.set(
        &mut c.raft_startup_phase_timeout,
        "ZSLG_RAFT_STARTUP_PHASE_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.raft_client_write_timeout,
        "ZSLG_RAFT_CLIENT_WRITE_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.raft_snapshot_logs_since_last,
        "ZSLG_RAFT_SNAPSHOT_LOGS_SINCE_LAST",
        count,
    )?;
    env.set(
        &mut c.raft_max_in_snapshot_log_to_keep,
        "ZSLG_RAFT_MAX_IN_SNAPSHOT_LOG_TO_KEEP",
        count,
    )?;
    Ok(())
}

/// Snapshot cadence, the compaction tier ladder, and object cleanup.
fn snapshots(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.snapshot_interval,
        "ZSLG_SNAPSHOT_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.snapshot_cutoff_timeout,
        "ZSLG_SNAPSHOT_CUTOFF_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.snapshot_compaction_tiers,
        "ZSLG_SNAPSHOT_COMPACTION_TIERS",
        tiers,
    )?;
    env.set(
        &mut c.snapshot_compaction_interval,
        "ZSLG_SNAPSHOT_COMPACTION_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.snapshot_cleanup_retention,
        "ZSLG_SNAPSHOT_CLEANUP_RETENTION",
        parse::duration,
    )?;
    env.set(
        &mut c.snapshot_cleanup_interval,
        "ZSLG_SNAPSHOT_CLEANUP_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.snapshot_compression,
        "ZSLG_SNAPSHOT_COMPRESSION",
        compression,
    )?;
    env.set(
        &mut c.snapshot_max_entries,
        "ZSLG_SNAPSHOT_MAX_ENTRIES",
        parse::nonzero,
    )?;
    Ok(())
}

/// The node's one RocksDB. `storage_memory` is the budget the rest derive from,
/// which is why they are `Option`.
fn storage(
    env: &mut Env,
    c: &mut ReplicaNodeConfig,
    memory: zslg::ByteSize,
) -> Result<(), ConfigError> {
    c.storage_memory = memory;
    env.set(&mut c.storage_memory, "ZSLG_STORAGE_MEMORY", parse::bytes)?;
    env.set_optional(
        &mut c.storage_write_buffer,
        "ZSLG_STORAGE_WRITE_BUFFER",
        parse::bytes,
    )?;
    env.set_optional(
        &mut c.storage_target_file_size,
        "ZSLG_STORAGE_TARGET_FILE_SIZE",
        parse::bytes,
    )?;
    env.set_optional(
        &mut c.storage_max_total_wal,
        "ZSLG_STORAGE_MAX_TOTAL_WAL",
        parse::bytes,
    )?;
    env.set_optional(
        &mut c.storage_max_background_jobs,
        "ZSLG_STORAGE_MAX_BACKGROUND_JOBS",
        jobs,
    )?;
    env.set_optional(
        &mut c.storage_max_open_files,
        "ZSLG_STORAGE_MAX_OPEN_FILES",
        open_files,
    )?;
    env.set_optional(
        &mut c.storage_retire_sweep,
        "ZSLG_STORAGE_RETIRE_SWEEP",
        parse::duration,
    )?;
    Ok(())
}

/// The replica log's retention and its two payload-carrying queues.
fn replica_log(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.replica_log_purge_grace_period,
        "ZSLG_REPLICA_LOG_PURGE_GRACE_PERIOD",
        parse::duration,
    )?;
    env.set(
        &mut c.replica_log_fetch_sweep_interval,
        "ZSLG_REPLICA_LOG_FETCH_SWEEP_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.replica_log_compression,
        "ZSLG_REPLICA_LOG_COMPRESSION",
        parse::boolean,
    )?;
    env.set(
        &mut c.replica_log_subscribe_channel_capacity,
        "ZSLG_REPLICA_LOG_SUBSCRIBE_CHANNEL_CAPACITY",
        parse::nonzero,
    )?;
    env.set(
        &mut c.replica_log_write_channel_capacity,
        "ZSLG_REPLICA_LOG_WRITE_CHANNEL_CAPACITY",
        parse::nonzero,
    )?;
    env.set(
        &mut c.replica_log_write_out_capacity,
        "ZSLG_REPLICA_LOG_WRITE_OUT_CAPACITY",
        parse::nonzero,
    )?;
    env.set(
        &mut c.replica_log_max_flush_batch,
        "ZSLG_REPLICA_LOG_MAX_FLUSH_BATCH",
        parse::nonzero,
    )?;
    Ok(())
}

/// Ingest, the local store, and the quorum-read timings.
fn learner(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.learner_fetch_sweep_interval,
        "ZSLG_LEARNER_FETCH_SWEEP_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.learner_purge_grace_period,
        "ZSLG_LEARNER_PURGE_GRACE_PERIOD",
        parse::duration,
    )?;
    env.set(
        &mut c.learner_local_store_flush_interval,
        "ZSLG_LEARNER_LOCAL_STORE_FLUSH_INTERVAL",
        parse::duration,
    )?;
    env.set_optional(
        &mut c.learner_local_store_dedup_ring,
        "ZSLG_LEARNER_LOCAL_STORE_DEDUP_RING",
        parse::nonzero,
    )?;
    env.set(
        &mut c.learner_local_store_compression,
        "ZSLG_LEARNER_LOCAL_STORE_COMPRESSION",
        parse::boolean,
    )?;
    env.set(
        &mut c.learner_local_store_integrate_channel_capacity,
        "ZSLG_LEARNER_LOCAL_STORE_INTEGRATE_CHANNEL_CAPACITY",
        parse::nonzero,
    )?;
    env.set(
        &mut c.learner_quorum_read_deadtime,
        "ZSLG_LEARNER_QUORUM_READ_DEADTIME",
        parse::duration,
    )?;
    env.set(
        &mut c.learner_quorum_read_timeout,
        "ZSLG_LEARNER_QUORUM_READ_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.learner_quorum_wait_timeout,
        "ZSLG_LEARNER_QUORUM_WAIT_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.learner_quorum_poll_interval,
        "ZSLG_LEARNER_QUORUM_POLL_INTERVAL",
        parse::duration,
    )?;
    env.set_optional(
        &mut c.learner_stable_purge_ttl,
        "ZSLG_LEARNER_STABLE_PURGE_TTL",
        parse::duration,
    )?;
    Ok(())
}

/// The CQRS replay source: per-subscriber queue depth and footprint.
fn stable_log(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.stable_log_subscriber_queue_capacity,
        "ZSLG_STABLE_LOG_SUBSCRIBER_QUEUE_CAPACITY",
        parse::nonzero,
    )?;
    env.set(
        &mut c.stable_log_compression,
        "ZSLG_STABLE_LOG_COMPRESSION",
        parse::boolean,
    )?;
    Ok(())
}

/// Clock skew, the membership target, and liveness pappg.
fn cluster(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.hlc_max_clock_skew,
        "ZSLG_HLC_MAX_CLOCK_SKEW",
        parse::duration,
    )?;
    env.set(
        &mut c.membership_replica_node_count,
        "ZSLG_MEMBERSHIP_REPLICA_NODE_COUNT",
        count,
    )?;
    env.set(
        &mut c.membership_replica_node_max_failures,
        "ZSLG_MEMBERSHIP_REPLICA_NODE_MAX_FAILURES",
        count,
    )?;
    env.set(
        &mut c.membership_forced_node_removal_timeout,
        "ZSLG_MEMBERSHIP_FORCED_NODE_REMOVAL_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.membership_poll_interval,
        "ZSLG_MEMBERSHIP_POLL_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.membership_spare_selection,
        "ZSLG_MEMBERSHIP_SPARE_SELECTION",
        spare_selection,
    )?;
    env.set(
        &mut c.liveness_probe_interval,
        "ZSLG_LIVENESS_PROBE_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.liveness_probe_timeout,
        "ZSLG_LIVENESS_PROBE_TIMEOUT",
        parse::duration,
    )?;
    Ok(())
}

/// The TCP knobs, whose zslg field names are bare — hence `ZSLG_NETWORK_`.
fn transport(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(
        &mut c.keepalive_interval,
        "ZSLG_NETWORK_KEEPALIVE_INTERVAL",
        parse::duration,
    )?;
    env.set(
        &mut c.keepalive_retries,
        "ZSLG_NETWORK_KEEPALIVE_RETRIES",
        count,
    )?;
    env.set(
        &mut c.user_timeout,
        "ZSLG_NETWORK_USER_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.socket_buffer,
        "ZSLG_NETWORK_SOCKET_BUFFER",
        parse::bytes,
    )?;
    env.set(
        &mut c.connect_timeout,
        "ZSLG_NETWORK_CONNECT_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.unary_timeout,
        "ZSLG_NETWORK_UNARY_TIMEOUT",
        parse::duration,
    )?;
    env.set(
        &mut c.max_unary_conns_per_peer,
        "ZSLG_NETWORK_MAX_UNARY_CONNS_PER_PEER",
        count,
    )?;
    env.set(
        &mut c.max_idle_unary_conns_per_peer,
        "ZSLG_NETWORK_MAX_IDLE_UNARY_CONNS_PER_PEER",
        count,
    )?;
    env.set(
        &mut c.pool_channel_capacity,
        "ZSLG_NETWORK_POOL_CHANNEL_CAPACITY",
        count,
    )?;
    env.set(
        &mut c.subscribe_channel_capacity,
        "ZSLG_NETWORK_SUBSCRIBE_CHANNEL_CAPACITY",
        parse::nonzero,
    )?;
    Ok(())
}

/// The write deadline and the shutdown drain.
fn writes(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(&mut c.write_timeout, "ZSLG_WRITE_TIMEOUT", parse::duration)?;
    env.set(&mut c.drain_timeout, "ZSLG_DRAIN_TIMEOUT", parse::duration)?;
    Ok(())
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

// ── Field parsers ────────────────────────────────────────────────────────────

fn count<T: std::str::FromStr>(var: &'static str, raw: &str) -> Result<T, ConfigError> {
    parse::integer(var, raw, "a non-negative integer")
}

fn jobs(var: &'static str, raw: &str) -> Result<i32, ConfigError> {
    parse::integer(var, raw, "a thread count")
}

fn open_files(var: &'static str, raw: &str) -> Result<i32, ConfigError> {
    parse::integer(var, raw, "a file-descriptor cap, or -1 for unlimited")
}

fn compression(var: &'static str, raw: &str) -> Result<zslg::CompressionLevel, ConfigError> {
    let level: u8 = parse::integer(var, raw, "a zstd level, 0–22")?;
    zslg::CompressionLevel::new(level).map_err(|source| ConfigError::Rejected { var, source })
}

fn spare_selection(var: &'static str, raw: &str) -> Result<SpareSelectionStrategy, ConfigError> {
    match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "random" => Ok(SpareSelectionStrategy::Random),
        "lowest-latency" => Ok(SpareSelectionStrategy::LowestLatency),
        _ => Err(ConfigError::bad_value(
            var,
            raw,
            "one of: random, lowest-latency",
        )),
    }
}

/// Compaction tiers, coarsest last. zslg checks the ordering and the floor
/// against `snapshot_interval` itself, so this only has to produce the list.
fn tiers(var: &'static str, raw: &str) -> Result<Vec<zslg::CompactionTier>, ConfigError> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| parse::duration(var, entry).map(Into::into))
        .collect()
}

/// `name@host:port`, or `host:port` when the logical name and the DNS name are
/// the same string. The two are separate because the name is the cluster's
/// identity for the peer — it is what membership admits on — while `host:port`
/// is re-resolved at every reconnect.
fn peers(var: &'static str, raw: &str) -> Result<Vec<PeerAddr>, ConfigError> {
    const EXPECTED: &str = "a comma-separated list of `name@host:port` (or `host:port`), e.g. `node-a@node-a.svc:9000,node-b@node-b.svc:9000`";

    let peers: Vec<PeerAddr> = raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let (name, addr) = match entry.split_once('@') {
                Some((name, addr)) => (name.trim(), addr.trim()),
                // No explicit name: everything before the final colon is both.
                // `rsplit_once` so a bracketed IPv6 literal keeps its colons.
                None => (
                    entry
                        .rsplit_once(':')
                        .ok_or_else(|| ConfigError::bad_value(var, raw, EXPECTED))?
                        .0,
                    entry,
                ),
            };
            let port = addr
                .rsplit_once(':')
                .ok_or_else(|| ConfigError::bad_value(var, raw, EXPECTED))?
                .1;
            match port.parse::<u16>() {
                Ok(0) | Err(_) => {
                    return Err(ConfigError::bad_value(var, raw, EXPECTED));
                }
                Ok(_) => {}
            }
            if name.is_empty() {
                return Err(ConfigError::bad_value(var, raw, EXPECTED));
            }
            Ok(PeerAddr {
                addr: zslg::HostPort::from(addr),
                server_name: ServerName::from(name),
            })
        })
        .collect::<Result<_, _>>()?;

    if peers.is_empty() {
        return Err(ConfigError::bad_value(var, raw, EXPECTED));
    }
    Ok(peers)
}

fn socket_addr(var: &'static str, raw: &str) -> Result<SocketAddr, ConfigError> {
    raw.trim()
        .parse()
        .map_err(|_| ConfigError::bad_value(var, raw, "a socket address, e.g. `0.0.0.0:9000`"))
}

/// The node's own bind address. Port 0 is refused here and allowed for the HTTP
/// listener, because this node appears in its own peer list and a peer entry
/// cannot name a port the kernel has not chosen yet.
fn listen_addr(var: &'static str, raw: &str) -> Result<SocketAddr, ConfigError> {
    let addr = socket_addr(var, raw)?;
    if addr.port() == 0 {
        return Err(ConfigError::bad_value(
            var,
            raw,
            "a socket address with a fixed port, e.g. `0.0.0.0:9000` (port 0 cannot be advertised to peers)",
        ));
    }
    Ok(addr)
}

/// A directory that must exist by the time a store opens into it. RocksDB
/// creates its own root but not the path to it, so the whole path is created
/// here — where a permissions problem can still name the variable that caused it.
fn directory(
    var: &'static str,
    raw: &str,
    temp: &mut Vec<TempDir>,
) -> Result<PathBuf, ConfigError> {
    let raw = raw.trim();
    if raw == TEMP {
        return temp_directory(var, temp);
    }
    let path = PathBuf::from(raw);
    std::fs::create_dir_all(&path).map_err(|source| ConfigError::Directory {
        var,
        path: path.clone(),
        source,
    })?;
    Ok(path)
}

/// The `:temp:` sentinel: a fresh `TempDir` and the guard that keeps it alive.
///
/// `owner` names the store in a failure. It is a variable for an
/// environment-driven path, or the store itself where the CLI fixes the path.
fn temp_directory(owner: &'static str, temp: &mut Vec<TempDir>) -> Result<PathBuf, ConfigError> {
    let dir = tempfile::tempdir().map_err(|source| ConfigError::Directory {
        var: owner,
        path: std::env::temp_dir(),
        source,
    })?;
    let path = dir.path().to_path_buf();
    // The guard, not the path, is what keeps the directory alive; it goes to
    // the caller because dropping it deletes the store out from under RocksDB.
    temp.push(dir);
    Ok(path)
}

/// `:memory:` for the in-process store, `:temp:` for a directory that dies with
/// the process, `s3://bucket[/prefix]?endpoint=…&access_key=…&secret_key=…` for
/// S3 (the key pair is part of the value: see `store`), anything else for a
/// directory on disk.
///
/// zslg's own config cannot carry a key prefix, so the store is built here and
/// a non-empty prefix wraps it (`PrefixStore`). zslg takes the built
/// `Arc<dyn ObjectStore>` all the same.
fn object_store(
    var: &'static str,
    raw: &str,
    temp: &mut Vec<TempDir>,
) -> Result<Arc<dyn object_store::ObjectStore>, ConfigError> {
    let (config, prefix) = object_store_config(var, raw, temp)?;
    let store = config
        .build()
        .map_err(|source| ConfigError::Rejected { var, source })?;
    Ok(with_prefix(store, &prefix))
}

/// A prefix is not part of zslg's backend config, so it is applied over the
/// built store: every key zslg reads or writes gains the prefix.
fn with_prefix(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Arc<dyn object_store::ObjectStore> {
    if prefix.is_empty() {
        return store;
    }
    Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
}

/// The backend and, for S3, the key prefix below which this store lives.
fn object_store_config(
    var: &'static str,
    raw: &str,
    temp: &mut Vec<TempDir>,
) -> Result<(ObjectStoreConfig, String), ConfigError> {
    if raw.trim() == MEMORY {
        return Ok((ObjectStoreConfig::InMemory, String::new()));
    }
    if raw.trim().starts_with("s3://") {
        return store::s3(var, raw);
    }
    Ok((
        ObjectStoreConfig::LocalFs {
            root: directory(var, raw, temp)?,
        },
        String::new(),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;

    const V: &str = "ZSLG_TEST";

    #[test]
    fn a_peer_may_name_itself_separately_from_its_dial_target() {
        let peers = peers(V, "node-a@node-a.svc.cluster.local:9000").unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(&*peers[0].server_name, "node-a");
        assert_eq!(
            peers[0].addr.to_string(),
            "node-a.svc.cluster.local:9000",
            "the dial target stays unresolved so DNS is re-read on reconnect"
        );
    }

    #[test]
    fn a_peer_without_an_at_takes_its_name_from_the_host() {
        let peers = peers(V, "node-b:9000").unwrap();
        assert_eq!(&*peers[0].server_name, "node-b");
        assert_eq!(peers[0].addr.to_string(), "node-b:9000");
    }

    #[test]
    fn peers_split_on_commas_and_tolerate_whitespace() {
        let peers = peers(V, " node-a@a:9000 , node-b@b:9000 ,").unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(&*peers[1].server_name, "node-b");
    }

    #[test]
    fn an_ipv6_literal_keeps_its_colons() {
        let peers = peers(V, "node-c@[::1]:9000").unwrap();
        assert_eq!(peers[0].addr.to_string(), "[::1]:9000");
    }

    fn node(peers: &str) -> ReplicaNodeConfig {
        let mut env = Env::from_pairs(&[
            ("ZSLG_SERVER_NAME", "a"),
            ("ZSLG_PEERS", peers),
            ("ZSLG_BOOTSTRAP_SERVER_NAME", "a"),
            ("ZSLG_LISTEN_ADDR", "127.0.0.1:9000"),
            ("ZSLG_DB_PATH", ":temp:"),
            ("ZSLG_OBJECT_STORE", ":memory:"),
        ]);
        zslg_config(&mut env, &mut Vec::new(), SERVER_STORAGE_MEMORY).unwrap()
    }

    #[test]
    fn a_standalone_log_store_is_small_on_purpose() {
        let mut env = Env::from_pairs(&[
            ("ZSLG_SERVER_NAME", "a"),
            ("ZSLG_PEERS", "a@127.0.0.1:9000"),
            ("ZSLG_BOOTSTRAP_SERVER_NAME", "a"),
            ("ZSLG_LISTEN_ADDR", "127.0.0.1:9000"),
            ("ZSLG_DB_PATH", ":temp:"),
            ("ZSLG_OBJECT_STORE", ":memory:"),
        ]);
        let c = zslg_config(&mut env, &mut Vec::new(), STANDALONE_ZSLG_MEMORY).unwrap();
        assert_eq!(c.storage_memory, zslg::ByteSize::from_mib(16));
        assert_eq!(STANDALONE_NVZN_MEMORY, zslg::ByteSize::from_mib(32));
        assert_eq!(
            node("a@127.0.0.1:9000").storage_memory,
            SERVER_STORAGE_MEMORY
        );
    }

    #[test]
    fn a_standalone_process_is_one_node() {
        assert!(alone(&node("a@127.0.0.1:9000")).is_ok());
        let refused = alone(&node("a@127.0.0.1:9000,b@127.0.0.1:9001")).unwrap_err();
        assert!(refused.to_string().contains("one node"), "{refused}");
    }

    #[test]
    fn a_peer_needs_a_real_port() {
        assert!(peers(V, "node-a").is_err());
        assert!(peers(V, "node-a@node-a").is_err());
        assert!(peers(V, "node-a@a:0").is_err(), "port 0 cannot be dialled");
        assert!(peers(V, "node-a@a:99999").is_err());
        assert!(peers(V, "@a:9000").is_err());
        assert!(peers(V, "").is_err(), "an empty peer list is not a cluster");
    }

    /// Metrics must not be exported to the world because nobody said not to.
    #[test]
    fn metrics_default_to_loopback() {
        assert!(
            DEFAULT_METRICS_LISTEN_ADDR.ip().is_loopback(),
            "the default metrics listener is reachable off-box"
        );
    }

    /// The two listeners must not collide, or one silently fails to bind.
    #[test]
    fn the_two_listeners_have_different_defaults() {
        assert_ne!(DEFAULT_LISTEN_ADDR, DEFAULT_METRICS_LISTEN_ADDR);
    }

    #[test]
    fn the_http_listener_may_take_an_ephemeral_port() {
        assert_eq!(socket_addr(V, "127.0.0.1:0").unwrap().port(), 0);
        assert!(socket_addr(V, "not-an-address").is_err());
    }

    #[test]
    fn the_listen_port_may_not_be_ephemeral() {
        assert_eq!(
            listen_addr(V, "0.0.0.0:9000").unwrap(),
            "0.0.0.0:9000".parse::<SocketAddr>().unwrap()
        );
        assert!(listen_addr(V, "127.0.0.1:0").is_err());
        assert!(listen_addr(V, "not-an-address").is_err());
    }

    #[test]
    fn compaction_tiers_are_a_duration_list() {
        let parsed = tiers(V, "1min, 15min, 1h, 1d").unwrap();
        assert_eq!(parsed.len(), 4);
        assert!(tiers(V, "1min,nonsense").is_err());
    }

    #[test]
    fn spare_selection_accepts_either_word_separator() {
        assert!(matches!(
            spare_selection(V, "random").unwrap(),
            SpareSelectionStrategy::Random
        ));
        for spelling in ["lowest-latency", "lowest_latency", "LOWEST-LATENCY"] {
            assert!(matches!(
                spare_selection(V, spelling).unwrap(),
                SpareSelectionStrategy::LowestLatency
            ));
        }
        assert!(spare_selection(V, "fastest").is_err());
    }

    #[test]
    fn compression_is_bounded_by_zstds_own_range() {
        assert!(compression(V, "0").is_ok());
        assert!(compression(V, "22").is_ok());
        assert!(compression(V, "23").is_err());
        assert!(compression(V, "-1").is_err());
    }

    /// Spelled out rather than routed through [`MEMORY`], so the literal an
    /// operator types is what this pins.
    #[test]
    fn memory_selects_the_in_process_object_store() {
        let mut temp = Vec::new();
        let (config, prefix) = object_store_config(V, ":memory:", &mut temp).unwrap();
        assert!(matches!(config, ObjectStoreConfig::InMemory));
        assert!(prefix.is_empty(), "the heap store has no prefix");
        assert!(temp.is_empty(), "the heap store needs no directory");
    }

    /// No fuzzy matching: a directory named `memory:` is a thing someone may want.
    #[test]
    fn a_sentinel_without_both_colons_is_just_a_path() {
        let mut temp = Vec::new();
        let root = directory(V, TEMP, &mut temp).unwrap();
        let near_miss = root.join("memory:");
        assert!(matches!(
            object_store_config(V, &near_miss.to_string_lossy(), &mut temp)
                .unwrap()
                .0,
            ObjectStoreConfig::LocalFs { .. }
        ));
        assert!(near_miss.is_dir());
    }

    /// The deploy keeps every store in one bucket, so `ZSLG_OBJECT_STORE`
    /// carries `bucket/zslg` as a prefix. zslg's config cannot hold one; the
    /// platform applies it over the built store instead of rejecting it.
    #[test]
    fn an_s3_prefix_is_kept_for_the_store_to_wrap() {
        let mut temp = Vec::new();
        let (config, prefix) = object_store_config(
            V,
            "s3://bucket/bucket/zslg?endpoint=http://s3:9000&region=nbg1&access_key=k&secret_key=s",
            &mut temp,
        )
        .unwrap();

        assert!(matches!(config, ObjectStoreConfig::S3 { .. }));
        assert_eq!(prefix, "bucket/zslg");
        assert!(temp.is_empty(), "an S3 location needs no directory");
    }

    #[test]
    fn a_store_without_a_prefix_is_left_unwrapped() {
        let mut temp = Vec::new();
        let (_, prefix) = object_store_config(
            V,
            "s3://zslg?endpoint=http://s3:9000&region=nbg1&access_key=k&secret_key=s",
            &mut temp,
        )
        .unwrap();

        assert!(prefix.is_empty());
    }

    /// Wrapping is real, not a marker: a key put through the wrapped store
    /// lands under the prefix in the backend.
    #[test]
    fn a_prefix_moves_every_key_below_it() {
        use futures::StreamExt;
        use object_store::ObjectStoreExt;
        use object_store::PutPayload;
        use object_store::path::Path;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let store = with_prefix(inner.clone(), "bucket/zslg");
        futures::executor::block_on(async {
            store
                .put(&Path::from("snapshot"), PutPayload::from_static(b"bytes"))
                .await
                .unwrap();
            let keys: Vec<String> = inner
                .list(None)
                .map(|entry| entry.unwrap().location.to_string())
                .collect()
                .await;
            assert_eq!(keys, ["bucket/zslg/snapshot"]);
        });
    }

    #[test]
    fn temp_yields_a_live_directory_and_its_guard() {
        let mut temp = Vec::new();
        let path = directory(V, TEMP, &mut temp).unwrap();
        assert!(path.is_dir());
        assert_eq!(temp.len(), 1);

        temp.clear();
        assert!(
            !path.exists(),
            "dropping the guard must remove the directory"
        );
    }

    #[test]
    fn a_path_is_created_if_it_does_not_exist() {
        let mut temp = Vec::new();
        let root = directory(V, TEMP, &mut temp).unwrap();
        let nested = root.join("a").join("b");
        let made = directory(V, &nested.to_string_lossy(), &mut temp).unwrap();
        assert!(made.is_dir(), "intermediate components are created too");
        assert_eq!(temp.len(), 1, "a real path allocates no guard");
    }

    #[test]
    fn the_cli_socket_is_required_and_not_empty() {
        let mut env = Env::from_pairs(&[]);
        assert!(cli_socket(&mut env).is_err());
        let mut env = Env::from_pairs(&[("BTRB_CLI_SOCKET", " ")]);
        assert!(cli_socket(&mut env).is_err());
        let mut env = Env::from_pairs(&[("BTRB_CLI_SOCKET", "/run/host/cli.sock")]);
        assert_eq!(
            cli_socket(&mut env).unwrap(),
            PathBuf::from("/run/host/cli.sock")
        );
    }
}
