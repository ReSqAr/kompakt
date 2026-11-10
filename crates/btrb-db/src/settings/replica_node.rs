//! Reading the zslg replica-node knobs from the environment.

use super::fields::{
    compression, count, directory, jobs, listen_addr, object_store, open_files, peers,
    spare_selection, tiers,
};
use btrb_config::{ConfigError, Env, parse};
use tempfile::TempDir;
use zslg::{FollowerNodeConfig, ReplicaNodeConfig, ServerName};

// ── the follower ─────────────────────────────────────────────────────────────

/// The worker's follower node: the peers, the object store and a persistent path, and the one budget it has.
pub fn follower_config(
    env: &mut Env,
    temp: &mut Vec<TempDir>,
    memory: zslg::ByteSize,
) -> Result<FollowerNodeConfig, ConfigError> {
    let peers_raw = env.required("ZSLG_PEERS")?;
    let peers = peers("ZSLG_PEERS", &peers_raw)?;
    let object_store_raw = env.required("ZSLG_OBJECT_STORE")?;
    let object_store = object_store("ZSLG_OBJECT_STORE", &object_store_raw, temp)?;
    let db_path_raw = env.required("ZSLG_DB_PATH")?;
    let db_path = directory("ZSLG_DB_PATH", &db_path_raw, temp)?;

    let mut zslg = FollowerNodeConfig::new(peers, db_path, object_store).map_err(|source| {
        ConfigError::Rejected {
            var: "ZSLG_OBJECT_STORE",
            source,
        }
    })?;
    zslg.storage_memory = memory;
    env.set(
        &mut zslg.storage_memory,
        "ZSLG_STORAGE_MEMORY",
        parse::bytes,
    )?;
    Ok(zslg)
}

// ── zslg ─────────────────────────────────────────────────────────────────────

pub fn zslg_config(
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
pub(super) fn raft(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn snapshots(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn storage(
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
pub(super) fn replica_log(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn learner(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn stable_log(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn cluster(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn transport(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
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
pub(super) fn writes(env: &mut Env, c: &mut ReplicaNodeConfig) -> Result<(), ConfigError> {
    env.set(&mut c.write_timeout, "ZSLG_WRITE_TIMEOUT", parse::duration)?;
    env.set(&mut c.drain_timeout, "ZSLG_DRAIN_TIMEOUT", parse::duration)?;
    Ok(())
}
