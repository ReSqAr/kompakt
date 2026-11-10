//! Field parsers: counts, sizes, peer lists, paths and object-store specs.

use super::{MEMORY, TEMP, store};
use crate::config::{ConfigError, parse};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zslg::{ObjectStoreConfig, PeerAddr, ServerName, SpareSelectionStrategy};

// ── Field parsers ────────────────────────────────────────────────────────────

pub(crate) fn count<T: std::str::FromStr>(var: &'static str, raw: &str) -> Result<T, ConfigError> {
    parse::integer(var, raw, "a non-negative integer")
}

pub(crate) fn jobs(var: &'static str, raw: &str) -> Result<i32, ConfigError> {
    parse::integer(var, raw, "a thread count")
}

pub(crate) fn open_files(var: &'static str, raw: &str) -> Result<i32, ConfigError> {
    parse::integer(var, raw, "a file-descriptor cap, or -1 for unlimited")
}

pub(crate) fn compression(
    var: &'static str,
    raw: &str,
) -> Result<zslg::CompressionLevel, ConfigError> {
    let level: u8 = parse::integer(var, raw, "a zstd level, 0–22")?;
    zslg::CompressionLevel::new(level).map_err(|source| ConfigError::Rejected {
        var,
        source: source.into(),
    })
}

pub(crate) fn spare_selection(
    var: &'static str,
    raw: &str,
) -> Result<SpareSelectionStrategy, ConfigError> {
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
pub(crate) fn tiers(
    var: &'static str,
    raw: &str,
) -> Result<Vec<zslg::CompactionTier>, ConfigError> {
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
pub(crate) fn peers(var: &'static str, raw: &str) -> Result<Vec<PeerAddr>, ConfigError> {
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

pub(crate) use btrb_config::parse::socket_addr;

/// The node's own bind address. Port 0 is refused here and allowed for the HTTP
/// listener, because this node appears in its own peer list and a peer entry
/// cannot name a port the kernel has not chosen yet.
pub(crate) fn listen_addr(var: &'static str, raw: &str) -> Result<SocketAddr, ConfigError> {
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
pub(crate) fn directory(
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
pub(crate) fn temp_directory(
    owner: &'static str,
    temp: &mut Vec<TempDir>,
) -> Result<PathBuf, ConfigError> {
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
pub(crate) fn object_store(
    var: &'static str,
    raw: &str,
    temp: &mut Vec<TempDir>,
) -> Result<Arc<dyn object_store::ObjectStore>, ConfigError> {
    let (config, prefix) = object_store_config(var, raw, temp)?;
    let store = config.build().map_err(|source| ConfigError::Rejected {
        var,
        source: source.into(),
    })?;
    Ok(with_prefix(store, &prefix))
}

/// A prefix is not part of zslg's backend config, so it is applied over the
/// built store: every key zslg reads or writes gains the prefix.
pub(crate) fn with_prefix(
    store: Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Arc<dyn object_store::ObjectStore> {
    if prefix.is_empty() {
        return store;
    }
    Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
}

/// The backend and, for S3, the key prefix below which this store lives.
pub(crate) fn object_store_config(
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
