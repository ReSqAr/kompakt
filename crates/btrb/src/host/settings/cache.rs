//! The process's disk cache, from the environment.
//!
//! ```text
//!   BTRB_CACHE_DIR      this process's cache directory          (required)
//!   BTRB_CACHE_SOFT     kept after use: the retention budget    (required)
//!   BTRB_CACHE_HARD     a download waits above this: everything
//!                       on disk, in use or in flight            (required)
//!   BTRB_CACHE_WEIGHTS  app=90,other=10                    (unset: equal)
//! ```
//!
//! No defaults for the limits: they belong to the deployment — its disk and
//! its shape. A serving process (`serve`, `standalone`) keeps the viewers'
//! working set (soft of a few GiB, hard above it by the downloads in
//! flight); the split worker keeps nothing past use (soft 0, hard what its
//! lanes hold at once).
//!
//! A directory is not shared between processes (the cache locks it): the
//! server and the worker each get their own, by their own `.env`. The CLI
//! never opens the configured one: its cache is a throwaway, like its log.
//! The weights must name exactly the apps that use the cache — the cache
//! checks that at the boot, with every app's kinds registered.

use crate::config::{ConfigError, Env, parse};
use crate::{CacheConfig, CacheLimits};
use std::path::PathBuf;

/// A throwaway cache: a command may fetch a handful of files.
const THROWAWAY: CacheLimits = CacheLimits {
    soft: 1 << 30,
    hard: 2 << 30,
};

/// Which cache a process opens.
pub(crate) enum CacheSetup {
    /// The configured directory (server, worker).
    Directory(CacheConfig),
    /// A temporary directory, removed at exit (the CLI).
    Throwaway,
}

impl CacheSetup {
    /// Claim the directory (or make the temporary one): fails the boot when
    /// the directory is not a cache directory or is in use.
    ///
    /// # Errors
    /// The cache's configuration error, naming the directory.
    pub(crate) fn builder(self) -> Result<crate::CacheBuilder, crate::Error> {
        let builder = match self {
            CacheSetup::Directory(config) => crate::Cache::builder(config),
            CacheSetup::Throwaway => crate::Cache::temporary(THROWAWAY),
        };
        builder.map_err(crate::Error::new)
    }
}

/// The server's and the worker's cache.
pub(super) fn read(env: &mut Env) -> Result<CacheSetup, ConfigError> {
    let raw = env.required("BTRB_CACHE_DIR")?;
    let dir: PathBuf = env.directory("BTRB_CACHE_DIR", &raw)?;
    let soft = parse::bytes("BTRB_CACHE_SOFT", &env.required("BTRB_CACHE_SOFT")?)?;
    let hard = parse::bytes("BTRB_CACHE_HARD", &env.required("BTRB_CACHE_HARD")?)?;
    if hard.as_u64() < soft.as_u64() {
        return Err(ConfigError::bad_value(
            "BTRB_CACHE_HARD",
            &hard.as_u64().to_string(),
            "at least BTRB_CACHE_SOFT",
        ));
    }
    let mut weights = None;
    if let Some(raw) = env.take("BTRB_CACHE_WEIGHTS") {
        weights = Some(weights_of("BTRB_CACHE_WEIGHTS", &raw)?);
    }
    Ok(CacheSetup::Directory(CacheConfig {
        dir,
        limits: CacheLimits {
            soft: soft.as_u64(),
            hard: hard.as_u64(),
        },
        weights,
    }))
}

/// `app=90,other=10`: every entry `name=number`, names once.
fn weights_of(var: &'static str, raw: &str) -> Result<Vec<(String, u32)>, ConfigError> {
    const EXPECTED: &str = "name=weight pairs such as app=90,other=10";
    let mut out: Vec<(String, u32)> = Vec::new();
    for pair in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (name, weight) = pair
            .split_once('=')
            .ok_or_else(|| ConfigError::bad_value(var, raw, EXPECTED))?;
        let weight: u32 = weight
            .trim()
            .parse()
            .map_err(|_| ConfigError::bad_value(var, raw, EXPECTED))?;
        let name = name.trim();
        if name.is_empty() || out.iter().any(|(n, _)| n == name) {
            return Err(ConfigError::bad_value(var, raw, EXPECTED));
        }
        out.push((name.to_owned(), weight));
    }
    if out.is_empty() {
        return Err(ConfigError::bad_value(var, raw, EXPECTED));
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;

    fn read_from(pairs: &[(&str, &str)]) -> Result<CacheSetup, ConfigError> {
        read(&mut Env::from_pairs(pairs))
    }

    #[test]
    fn a_process_names_its_directory_and_both_limits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().into_owned();
        let Ok(CacheSetup::Directory(c)) = read_from(&[
            ("BTRB_CACHE_DIR", &path),
            ("BTRB_CACHE_SOFT", "0"),
            ("BTRB_CACHE_HARD", "4gib"),
        ]) else {
            panic!("a directory is configured");
        };
        assert_eq!((c.limits.soft, c.limits.hard), (0, 4 << 30));
        assert!(c.weights.is_none());
        let Ok(CacheSetup::Directory(c)) = read_from(&[
            ("BTRB_CACHE_DIR", &path),
            ("BTRB_CACHE_SOFT", "1gib"),
            ("BTRB_CACHE_HARD", "2gib"),
            ("BTRB_CACHE_WEIGHTS", "app=90,other=10"),
        ]) else {
            panic!("a directory is configured");
        };
        assert_eq!((c.limits.soft, c.limits.hard), (1 << 30, 2 << 30));
        assert_eq!(c.weights.unwrap().len(), 2);
    }

    #[test]
    fn a_missing_directory_or_limit_and_hard_below_soft_stop_the_boot() {
        assert!(matches!(
            read_from(&[]),
            Err(ConfigError::Missing {
                var: "BTRB_CACHE_DIR"
            })
        ));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().into_owned();
        assert!(matches!(
            read_from(&[("BTRB_CACHE_DIR", &path), ("BTRB_CACHE_HARD", "1gib")]),
            Err(ConfigError::Missing {
                var: "BTRB_CACHE_SOFT"
            })
        ));
        assert!(matches!(
            read_from(&[("BTRB_CACHE_DIR", &path), ("BTRB_CACHE_SOFT", "1gib")]),
            Err(ConfigError::Missing {
                var: "BTRB_CACHE_HARD"
            })
        ));
        assert!(
            read_from(&[
                ("BTRB_CACHE_DIR", &path),
                ("BTRB_CACHE_SOFT", "2gib"),
                ("BTRB_CACHE_HARD", "1gib"),
            ])
            .is_err()
        );
    }

    #[test]
    fn weights_are_name_equals_number_pairs() {
        let w = weights_of("V", "app=90, other=10").unwrap();
        assert_eq!(w, vec![("app".to_owned(), 90), ("other".to_owned(), 10)]);
        for bad in ["", "app", "app=x", "app=1,app=2", "=1", "app=-1"] {
            assert!(weights_of("V", bad).is_err(), "{bad:?}");
        }
    }
}
