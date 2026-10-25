//! The environment as a consumable map.
//!
//! Reading config from `std::env::var` at each use site cannot answer the one
//! question that matters after a bad deploy: *did anything I set go nowhere?*
//! So the slice of the environment under the claimed prefixes is snapshotted up
//! front and each variable is **removed as it is read**. Whatever is still in
//! the map at [`Env::finish`] was set by someone who believed it meant
//! something, and is an error — a misspelled tunable that silently does nothing
//! is the failure mode this module exists to prevent.
//!
//! ```text
//!   snapshot ──> take("ZSLG_SNAPSHOT_INTERVAL") ──> …  ──> finish()
//!   ZSLG_*         ├── removed from `unread`              │
//!   NVZN_*         └── recorded in `vocabulary`           └── `unread` empty?
//!                      (present or not)                       else: typo,
//!                                                              nearest
//!                                                              vocabulary word
//! ```
//!
//! `vocabulary` is why the suggestion needs no hand-maintained list of valid
//! names: every builder asks for every name it knows, so the set of names asked
//! for *is* the set of names that exist.

use std::collections::BTreeMap;
use std::path::PathBuf;

use tempfile::TempDir;

/// The sentinel path standing for "a fresh directory, gone when the process is".
pub const TEMP: &str = ":temp:";

use super::ConfigError;

/// A pair of values sharing a name: `{prefix}{NAME}{a}` and `{prefix}{NAME}{b}`.
/// Both halves of a name are present, or it is an error.
pub type Pairs = Vec<(String, (String, String))>;

pub struct Env {
    unread: BTreeMap<String, String>,
    vocabulary: Vec<&'static str>,
    pairs: BTreeMap<String, Pairs>,
    /// Composed `{PREFIX}_{NAME}` names, leaked once each: boot-time only, and
    /// bounded by the number of settings.
    interned: BTreeMap<String, &'static str>,
    /// `:temp:` directories handed out: dropping a guard deletes its directory,
    /// so whoever opened a store in it must keep these alive (`take_temp`).
    temp: Vec<TempDir>,
}

impl Env {
    /// Snapshot the prefixed slice of the process environment.
    ///
    /// Iterates `vars_os` rather than `vars`, which panics on a non-UTF-8
    /// entry: a stray binary variable belonging to something else must not take
    /// the process down. A non-UTF-8 *value* on one of **our** names is a
    /// different matter and is reported.
    pub fn from_process(prefixes: &[&str]) -> Result<Self, ConfigError> {
        let mut unread = BTreeMap::new();
        for (key, value) in std::env::vars_os() {
            let Some(key) = key.to_str() else { continue };
            if !prefixes.iter().any(|p| key.starts_with(p)) {
                continue;
            }
            let value = value
                .to_str()
                .ok_or_else(|| ConfigError::NonUtf8 {
                    var: key.to_owned(),
                })?
                .to_owned();
            unread.insert(key.to_owned(), value);
        }
        Ok(Self {
            unread,
            vocabulary: Vec::new(),
            pairs: BTreeMap::new(),
            interned: BTreeMap::new(),
            temp: Vec::new(),
        })
    }

    /// Every `{prefix}{NAME}{a}` / `{prefix}{NAME}{b}` pair, consumed. Read
    /// once: asking again returns the same answer (the halves are gone from the
    /// map by then). A name with only one half set is an error: that is a typo,
    /// not a choice.
    pub fn pairs(
        &mut self,
        prefix: &'static str,
        a: &'static str,
        b: &'static str,
    ) -> Result<Pairs, ConfigError> {
        if let Some(done) = self.pairs.get(prefix) {
            return Ok(done.clone());
        }
        // Each name shows up twice (its two halves): a set.
        let names: std::collections::BTreeSet<String> = self
            .unread
            .keys()
            .filter_map(|k| k.strip_prefix(prefix))
            .filter_map(|rest| rest.strip_suffix(a).or_else(|| rest.strip_suffix(b)))
            .map(str::to_owned)
            .collect();
        let mut found = Vec::new();
        for name in names {
            let first = format!("{prefix}{name}{a}");
            let second = format!("{prefix}{name}{b}");
            let (Some(x), Some(y)) = (self.unread.remove(&first), self.unread.remove(&second))
            else {
                return Err(ConfigError::HalfPair {
                    prefix,
                    name,
                    halves: (a, b),
                });
            };
            found.push((name, (x, y)));
        }
        self.pairs.insert(prefix.to_owned(), found.clone());
        Ok(found)
    }

    /// The raw value, consuming it. Records `var` as a known name whether or not
    /// it was set — that is what makes the did-you-mean complete.
    pub fn take(&mut self, var: &'static str) -> Option<String> {
        self.vocabulary.push(var);
        self.unread.remove(var)
    }

    /// A variable that must be set, to something other than whitespace.
    pub fn required(&mut self, var: &'static str) -> Result<String, ConfigError> {
        self.take(var)
            .ok_or(ConfigError::Missing { var })
            .and_then(|raw| {
                if raw.trim().is_empty() {
                    Err(ConfigError::bad_value(var, &raw, "a non-empty value"))
                } else {
                    Ok(raw)
                }
            })
    }

    /// Overwrite `slot` only if the variable is set, so an unset variable leaves
    /// the underlying crate's own default in place rather than restating it here.
    pub fn set<T, F>(
        &mut self,
        slot: &mut T,
        var: &'static str,
        parse: F,
    ) -> Result<(), ConfigError>
    where
        F: FnOnce(&'static str, &str) -> Result<T, ConfigError>,
    {
        if let Some(raw) = self.take(var) {
            *slot = parse(var, &raw)?;
        }
        Ok(())
    }

    /// [`Self::set`] for an `Option` field, where `off` / `none` is a value in
    /// its own right: it pins the field back to the crate's derivation (or turns
    /// the feature off) rather than leaving whatever was there.
    pub fn set_optional<T, F>(
        &mut self,
        slot: &mut Option<T>,
        var: &'static str,
        parse: F,
    ) -> Result<(), ConfigError>
    where
        F: FnOnce(&'static str, &str) -> Result<T, ConfigError>,
    {
        if let Some(raw) = self.take(var) {
            *slot = if super::parse::is_absent(&raw) {
                None
            } else {
                Some(parse(var, &raw)?)
            };
        }
        Ok(())
    }

    /// Reject anything left over. Called once, after every builder has run.
    pub fn finish(self) -> Result<(), ConfigError> {
        let Some((var, _)) = self.unread.into_iter().next() else {
            return Ok(());
        };
        let suggestion = nearest(&var, &self.vocabulary);
        Err(ConfigError::Unknown { var, suggestion })
    }
}

impl Env {
    /// A directory that must exist by the time a store opens into it: the whole
    /// path is created here, where a permissions problem can still name the
    /// variable that caused it. [`TEMP`] is a fresh directory that lives as long
    /// as the guard in [`Self::take_temp`].
    pub fn directory(&mut self, var: &'static str, raw: &str) -> Result<PathBuf, ConfigError> {
        let raw = raw.trim();
        if raw == TEMP {
            return self.temp_directory(var);
        }
        let path = PathBuf::from(raw);
        std::fs::create_dir_all(&path).map_err(|source| ConfigError::Directory {
            var,
            path: path.clone(),
            source,
        })?;
        Ok(path)
    }

    /// A fresh directory, deleted with its guard (see [`Self::take_temp`]).
    pub fn temp_directory(&mut self, owner: &'static str) -> Result<PathBuf, ConfigError> {
        let dir = tempfile::tempdir().map_err(|source| ConfigError::Directory {
            var: owner,
            path: std::env::temp_dir(),
            source,
        })?;
        let path = dir.path().to_path_buf();
        self.temp.push(dir);
        Ok(path)
    }

    /// Forget every unread variable under `prefix`: for a binary that reads
    /// only part of a namespace (the CLI reads two `ZSLG_*`, and may share an
    /// environment with a server) and must not call the rest a typo.
    pub fn discard(&mut self, prefix: &str) {
        self.unread.retain(|k, _| !k.starts_with(prefix));
    }

    /// The guards of every `:temp:` directory handed out so far.
    pub fn take_temp(&mut self) -> Vec<TempDir> {
        std::mem::take(&mut self.temp)
    }

    /// The slice of the environment an app may name: `NAME_*`, with the prefix
    /// implied. An app holding a `Scope` cannot read another prefix.
    pub fn scope(&mut self, name: &'static str) -> Scope<'_> {
        Scope {
            prefix: format!("{}_", name.to_ascii_uppercase()),
            env: self,
        }
    }

    fn intern(&mut self, full: String) -> &'static str {
        if let Some(known) = self.interned.get(&full) {
            return known;
        }
        let leaked: &'static str = Box::leak(full.clone().into_boxed_str());
        self.interned.insert(full, leaked);
        leaked
    }
}

/// A value that must not reach a log line, an error message or `Debug`.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// One app's view of the environment: `pop("CACHE_DIR")` reads
/// `APP_CACHE_DIR`. Consumption is recorded exactly as on [`Env`].
pub struct Scope<'e> {
    env: &'e mut Env,
    prefix: String,
}

impl Scope<'_> {
    /// The full name of `var` (`APP_` + `CACHE_DIR`), for error messages.
    pub fn name_of(&mut self, var: &str) -> &'static str {
        self.full(var)
    }

    fn full(&mut self, var: &str) -> &'static str {
        let full = format!("{}{var}", self.prefix);
        self.env.intern(full)
    }

    /// The raw value, consuming it; records the name as known whether or not it
    /// was set.
    pub fn pop(&mut self, var: &str) -> Option<String> {
        let full = self.full(var);
        self.env.take(full)
    }

    /// As [`Self::pop`], for a value that is never printed.
    pub fn pop_secret(&mut self, var: &str) -> Option<Secret> {
        self.pop(var).map(Secret)
    }

    /// Every variable `{PREFIX}{each}*`, consumed, as (what follows `each`, value).
    pub fn pop_each(&mut self, each: &str) -> Vec<(String, String)> {
        let start = format!("{}{each}", self.prefix);
        let keys: Vec<String> = self
            .env
            .unread
            .keys()
            .filter(|k| k.starts_with(&start))
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|k| {
                let value = self.env.unread.remove(&k)?;
                let rest = k.strip_prefix(&start)?;
                Some((rest.to_owned(), value))
            })
            .collect()
    }

    pub fn required(&mut self, var: &str) -> Result<String, ConfigError> {
        let full = self.full(var);
        self.env.required(full)
    }

    /// A required directory (created; `:temp:` is a throwaway one).
    pub fn directory(&mut self, var: &str) -> Result<PathBuf, ConfigError> {
        let full = self.full(var);
        let raw = self.env.required(full)?;
        self.env.directory(full, &raw)
    }

    /// See [`Env::directory`], for a value already read.
    pub fn directory_of(&mut self, var: &str, raw: &str) -> Result<PathBuf, ConfigError> {
        let full = self.full(var);
        self.env.directory(full, raw)
    }

    /// See [`Env::pairs`]: `{PREFIX}{each}{NAME}{a}` / `…{b}`.
    pub fn pairs(
        &mut self,
        each: &str,
        a: &'static str,
        b: &'static str,
    ) -> Result<Pairs, ConfigError> {
        let prefix = self.full(each);
        self.env.pairs(prefix, a, b)
    }

    /// See [`Env::set`]; the parser receives the full variable name.
    pub fn set<T, F>(&mut self, slot: &mut T, var: &str, parse: F) -> Result<(), ConfigError>
    where
        F: FnOnce(&'static str, &str) -> Result<T, ConfigError>,
    {
        let full = self.full(var);
        self.env.set(slot, full, parse)
    }

    /// See [`Env::set_optional`].
    pub fn set_optional<T, F>(
        &mut self,
        slot: &mut Option<T>,
        var: &str,
        parse: F,
    ) -> Result<(), ConfigError>
    where
        F: FnOnce(&'static str, &str) -> Result<T, ConfigError>,
    {
        let full = self.full(var);
        self.env.set_optional(slot, full, parse)
    }
}

/// The vocabulary word closest to `var`, if one is close enough to be worth
/// offering. Four edits is roughly a doubled letter plus a transposition — past
/// that a "did you mean" is noise pointing at an unrelated variable.
fn nearest(var: &str, vocabulary: &[&'static str]) -> Option<&'static str> {
    vocabulary
        .iter()
        .map(|known| (edit_distance(var, known), *known))
        .filter(|(distance, _)| *distance <= 4)
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, known)| known)
}

/// Levenshtein distance, two rows at a time.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur: Vec<usize> = vec![0; prev.len()];
    for (i, ca) in a.chars().enumerate() {
        cur[0] = i.saturating_add(1);
        for (j, cb) in b.iter().enumerate() {
            let next = j.saturating_add(1);
            let substitute = prev[j].saturating_add(usize::from(ca != *cb));
            let delete = prev[next].saturating_add(1);
            let insert = cur[j].saturating_add(1);
            cur[next] = substitute.min(delete).min(insert);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// The prefixed names actually present, for the startup log line. Re-reads the
/// environment rather than the snapshot, which the builders have consumed by the
/// time anyone wants to log it.
#[must_use]
pub fn present_names(prefixes: &[&str]) -> Vec<String> {
    let mut names: Vec<String> = std::env::vars_os()
        .filter_map(|(key, _)| key.into_string().ok())
        .filter(|key| prefixes.iter().any(|p| key.starts_with(p)))
        .collect();
    names.sort();
    names
}

impl Env {
    /// A fixed environment, for tests. `set_var` is `unsafe` in edition 2024 and
    /// the workspace forbids `unsafe_code`, so the consume-and-check machinery is
    /// exercised against a supplied map rather than the process's own.
    #[must_use]
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self {
            unread: pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            vocabulary: Vec::new(),
            pairs: BTreeMap::new(),
            interned: BTreeMap::new(),
            temp: Vec::new(),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]
mod tests {
    use super::*;

    #[test]
    fn pairs_come_in_halves_and_are_consumed() {
        let mut env = Env::from_pairs(&[
            ("BTRB_CREDENTIAL_BENCH_ACCESS_KEY", "a"),
            ("BTRB_CREDENTIAL_BENCH_SECRET_KEY", "s"),
        ]);
        let p = env
            .pairs("BTRB_CREDENTIAL_", "_ACCESS_KEY", "_SECRET_KEY")
            .unwrap();
        assert_eq!(
            p,
            vec![("BENCH".to_owned(), ("a".to_owned(), "s".to_owned()))]
        );
        let again = env
            .pairs("BTRB_CREDENTIAL_", "_ACCESS_KEY", "_SECRET_KEY")
            .unwrap();
        assert_eq!(again.len(), 1, "read once, cached");
        env.finish().unwrap();

        let mut half = Env::from_pairs(&[("BTRB_CREDENTIAL_X_ACCESS_KEY", "a")]);
        assert!(matches!(
            half.pairs("BTRB_CREDENTIAL_", "_ACCESS_KEY", "_SECRET_KEY"),
            Err(ConfigError::HalfPair { name, .. }) if name == "X"
        ));
    }

    #[test]
    fn a_scope_reads_only_its_own_prefix() {
        let mut env = Env::from_pairs(&[
            ("APP_CACHE_DIR", "/c"),
            ("OTHER_CACHE_DIR", "/p"),
            ("APP_CREDENTIAL_A", "1"),
            ("APP_CREDENTIAL_B", "2"),
        ]);
        let mut app = env.scope("app");
        assert_eq!(app.pop("CACHE_DIR").as_deref(), Some("/c"));
        assert_eq!(
            app.pop_each("CREDENTIAL_"),
            vec![
                ("A".to_owned(), "1".to_owned()),
                ("B".to_owned(), "2".to_owned())
            ]
        );
        assert!(matches!(
            env.finish(),
            Err(ConfigError::Unknown { var, .. }) if var == "OTHER_CACHE_DIR"
        ));
    }

    #[test]
    fn a_secret_prints_nothing() {
        let mut env = Env::from_pairs(&[("APP_SIGNING_KEY", "hunter2")]);
        let s = env.scope("app").pop_secret("SIGNING_KEY").unwrap();
        assert_eq!(format!("{s:?}"), "Secret(<redacted>)");
        assert_eq!(s.expose(), "hunter2");
    }

    #[test]
    fn an_unset_variable_leaves_the_crate_default_alone() {
        let mut env = Env::from_pairs(&[]);
        let mut slot = 7u32;
        env.set(&mut slot, "ZSLG_MEMBERSHIP_REPLICA_NODE_COUNT", |_, raw| {
            Ok(raw.parse().unwrap())
        })
        .unwrap();
        assert_eq!(slot, 7, "unset must not overwrite");
        env.finish().unwrap();
    }

    #[test]
    fn off_pins_an_option_field_back_to_none() {
        let mut env = Env::from_pairs(&[("ZSLG_LEARNER_STABLE_PURGE_TTL", "off")]);
        let mut slot = Some(1u64);
        env.set_optional(&mut slot, "ZSLG_LEARNER_STABLE_PURGE_TTL", |_, raw| {
            Ok(raw.parse().unwrap())
        })
        .unwrap();
        assert_eq!(slot, None);
    }

    #[test]
    fn a_typo_is_rejected_and_names_the_variable_it_meant() {
        let mut env = Env::from_pairs(&[("ZSLG_SNAPSHOT_INTERVALL", "5s")]);
        let mut slot = std::time::Duration::ZERO;
        env.set(&mut slot, "ZSLG_SNAPSHOT_INTERVAL", |_, _| {
            Ok(std::time::Duration::ZERO)
        })
        .unwrap();

        match env.finish() {
            Err(ConfigError::Unknown { var, suggestion }) => {
                assert_eq!(var, "ZSLG_SNAPSHOT_INTERVALL");
                assert_eq!(suggestion, Some("ZSLG_SNAPSHOT_INTERVAL"));
            }
            other => panic!("expected an unknown-variable error, got {other:?}"),
        }
    }

    #[test]
    fn the_vocabulary_covers_names_that_were_not_set() {
        let mut env = Env::from_pairs(&[("ZSLG_DRAIN_TIMEOUTT", "30s")]);
        let mut slot = std::time::Duration::ZERO;
        env.set(&mut slot, "ZSLG_DRAIN_TIMEOUT", |_, _| {
            Ok(std::time::Duration::ZERO)
        })
        .unwrap();
        assert!(matches!(
            env.finish(),
            Err(ConfigError::Unknown {
                suggestion: Some("ZSLG_DRAIN_TIMEOUT"),
                ..
            })
        ));
    }

    #[test]
    fn a_variable_unlike_anything_gets_no_suggestion() {
        let mut env = Env::from_pairs(&[("ZSLG_COMPLETELY_UNRELATED_THING", "1")]);
        let mut slot = 0u32;
        env.set(&mut slot, "ZSLG_MEMBERSHIP_POLL_INTERVAL", |_, _| Ok(0))
            .unwrap();
        assert!(matches!(
            env.finish(),
            Err(ConfigError::Unknown {
                suggestion: None,
                ..
            })
        ));
    }

    #[test]
    fn edit_distance_counts_edits() {
        assert_eq!(edit_distance("abc", "abc"), 0);
        assert_eq!(edit_distance("abc", "abcc"), 1);
        assert_eq!(edit_distance("abc", ""), 3);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
    }
}
