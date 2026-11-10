//! The db capability's slice of the environment: the zslg node, the nvzn store and the cache.
//!
//! ```text
//!   ZSLG_*   replica_node   the node's identity, listeners, durable stores, raft, snapshots, learner, writes
//!   NVZN_*   nvzn_config    the projection store: path, memory, flush, compaction
//!   BTRB_CACHE_*  cache     the object cache's directory and budgets
//! ```
//!
//! The role chooses the memory budgets (server, worker, standalone), so the readers take them as
//! arguments; a `:temp:` directory's guard is pushed onto the caller's list and lives as long as the stores.

mod cache;
mod fields;
mod replica_node;
mod store;

pub use cache::{CacheSetup, read as read_cache};
pub use fields::{directory, object_store, peers};
use fields::{jobs, open_files};
pub use replica_node::{follower_config, zslg_config};

use btrb_config::{ConfigError, Env, parse};
use tempfile::TempDir;
use zslg::ReplicaNodeConfig;

/// The sentinel path standing for "a fresh directory, gone when the process is".
const TEMP: &str = ":temp:";

/// The sentinel object-store backend standing for "in this process's heap".
const MEMORY: &str = ":memory:";

/// A standalone process is one replica: its peers are itself.
pub fn alone(zslg: &ReplicaNodeConfig) -> Result<(), ConfigError> {
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

pub fn nvzn_config(
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
