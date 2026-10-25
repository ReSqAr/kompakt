//! L0: one directory the process owns. Marked, locked, laid out; no policy.
//!
//! ```text
//!   {root}/.objcache                 marker: this directory is a cache
//!   {root}/.objcache.lock            held (advisory) for the life of `Disk`
//!   {root}/{app}/{kind}/{h[0..2]}/{h}          a complete file
//!   {root}/{app}/{kind}/{h[0..2]}/{h}.part     a download in progress
//! ```
//!
//! `claim` is the only way a directory becomes ours: an empty one gets the
//! marker, a marked one is taken over, anything else is refused — the scan
//! deletes, so it must never run in a directory somebody else's files live
//! in. App and kind are readable in the path, so the scan needs no registry:
//! it lists the layout and is told which (app, kind) pairs to keep. What is
//! to be evicted is not decided here: this layer only does.

use crate::Error;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const MARKER: &str = ".objcache";
const LOCK: &str = ".objcache.lock";
const MARKER_CONTENT: &str = "objcache layout 1\n";

/// An app's kind: the two directories above a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Group {
    pub(crate) app: &'static str,
    pub(crate) kind: &'static str,
}

/// A cached file: its group and the hash of what identifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Id {
    pub(crate) group: Group,
    pub(crate) hash: [u8; 32],
}

/// What the start-up scan found: the files to account for, and what it took
/// out of the way.
#[derive(Debug, Default)]
pub(crate) struct Scan {
    /// `(id, size, modified)`: the modification time is recency.
    pub(crate) files: Vec<(Id, u64, Option<SystemTime>)>,
    pub(crate) removed: Removed,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Removed {
    pub(crate) files: u64,
    pub(crate) bytes: u64,
}

impl Removed {
    fn add(&mut self, other: Removed) {
        self.files = self.files.saturating_add(other.files);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

pub(crate) struct Disk {
    root: PathBuf,
    /// Dropping it releases the lock (so does the process ending).
    _lock: File,
}

impl Disk {
    /// An empty directory gets the marker; a marked one is taken over;
    /// another process holding it, or a directory that is neither, is an
    /// [`Error::Config`] that names the directory and what to do.
    pub(crate) fn claim(dir: &Path) -> Result<Disk, Error> {
        std::fs::create_dir_all(dir)?;
        let marker = dir.join(MARKER);
        match std::fs::read_to_string(&marker) {
            Ok(content) if content == MARKER_CONTENT => {}
            Ok(_) => {
                return Err(Error::Config(format!(
                    "{} has a cache marker of another layout; empty the directory or name another",
                    dir.display()
                )));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let foreign = std::fs::read_dir(dir)?
                    .filter_map(Result::ok)
                    .any(|e| e.file_name() != LOCK);
                if foreign {
                    return Err(Error::Config(format!(
                        "{} is not empty and is not a cache directory (no {MARKER} marker): \
                         the cache deletes what it did not write, so name an empty directory \
                         or empty this one",
                        dir.display()
                    )));
                }
                std::fs::write(&marker, MARKER_CONTENT)?;
            }
            Err(e) => return Err(e.into()),
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(LOCK))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Config(format!(
                    "cache directory {} is in use by another process",
                    dir.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }
        Ok(Disk {
            root: dir.to_owned(),
            _lock: lock,
        })
    }

    pub(crate) fn path(&self, id: &Id) -> PathBuf {
        let hex = hex(&id.hash);
        let fan = hex.get(..2).unwrap_or("00");
        self.root
            .join(id.group.app)
            .join(id.group.kind)
            .join(fan)
            .join(hex)
    }

    pub(crate) fn part_path(&self, id: &Id) -> PathBuf {
        let mut p = self.path(id).into_os_string();
        p.push(".part");
        PathBuf::from(p)
    }

    /// Make the file's directory (the first file of a fan-out bucket).
    pub(crate) async fn prepare(&self, id: &Id) -> io::Result<()> {
        match self.path(id).parent() {
            Some(parent) => tokio::fs::create_dir_all(parent).await,
            None => Ok(()),
        }
    }

    pub(crate) fn remove(&self, id: &Id) -> io::Result<()> {
        std::fs::remove_file(self.path(id))
    }

    /// Set the file's modification time: its recency across a restart.
    pub(crate) fn touch(&self, id: &Id, at: SystemTime) -> io::Result<()> {
        std::fs::OpenOptions::new()
            .write(true)
            .open(self.path(id))?
            .set_modified(at)
    }

    /// Walk the layout. Whatever is not a complete file of a `registered`
    /// group — `.part` leftovers, directories of an app or kind nobody
    /// registered, anything off the layout — is removed and counted.
    pub(crate) fn scan(&self, registered: &[Group]) -> io::Result<Scan> {
        let mut scan = Scan::default();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                scan.removed.add(remove_all(&path));
                continue;
            };
            if name == MARKER || name == LOCK {
                continue;
            }
            let app = registered.iter().find(|g| g.app == name).map(|g| g.app);
            match app {
                Some(app) if path.is_dir() => self.scan_app(&path, app, registered, &mut scan)?,
                _ => scan.removed.add(remove_all(&path)),
            }
        }
        Ok(scan)
    }

    fn scan_app(
        &self,
        dir: &Path,
        app: &'static str,
        registered: &[Group],
        scan: &mut Scan,
    ) -> io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let group = entry.file_name().to_str().and_then(|name| {
                registered
                    .iter()
                    .find(|g| g.app == app && g.kind == name)
                    .copied()
            });
            match group {
                Some(group) if path.is_dir() => Self::scan_kind(&path, group, scan)?,
                _ => scan.removed.add(remove_all(&path)),
            }
        }
        Ok(())
    }

    fn scan_kind(dir: &Path, group: Group, scan: &mut Scan) -> io::Result<()> {
        for fan in std::fs::read_dir(dir)? {
            let fan = fan?;
            let fan_path = fan.path();
            let fan_name = fan.file_name().to_string_lossy().into_owned();
            if fan_name.len() != 2 || !fan_path.is_dir() {
                scan.removed.add(remove_all(&fan_path));
                continue;
            }
            for file in std::fs::read_dir(&fan_path)? {
                let file = file?;
                let path = file.path();
                let name = file.file_name().to_string_lossy().into_owned();
                let hash = parse_hash(&name).filter(|_| name.starts_with(&fan_name));
                let meta = file.metadata()?;
                match hash {
                    Some(hash) if meta.is_file() => {
                        scan.files
                            .push((Id { group, hash }, meta.len(), meta.modified().ok()));
                    }
                    _ => scan.removed.add(remove_all(&path)),
                }
            }
        }
        Ok(())
    }
}

/// Remove a file or a tree; count what went.
fn remove_all(path: &Path) -> Removed {
    let mut removed = Removed::default();
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                removed.add(remove_all(&entry.path()));
            }
        }
        std::fs::remove_dir(path).ok();
    } else {
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if std::fs::remove_file(path).is_ok() {
            removed.files = 1;
            removed.bytes = size;
        }
    }
    removed
}

pub(crate) fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn parse_hash(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let at = i.checked_mul(2)?;
        *byte = u8::from_str_radix(s.get(at..at.checked_add(2)?)?, 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]
mod tests {
    use super::*;

    const THUMB: Group = Group {
        app: "app",
        kind: "thumbnail",
    };

    fn id(group: Group, b: u8) -> Id {
        Id {
            group,
            hash: [b; 32],
        }
    }

    async fn write(disk: &Disk, id: &Id, bytes: &[u8]) {
        disk.prepare(id).await.unwrap();
        std::fs::write(disk.path(id), bytes).unwrap();
    }

    #[test]
    fn an_empty_directory_is_claimed_and_a_marked_one_is_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        drop(Disk::claim(dir.path()).unwrap());
        assert!(dir.path().join(MARKER).exists());
        drop(Disk::claim(dir.path()).unwrap());
    }

    #[test]
    fn a_directory_with_other_files_is_refused_and_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("zslg.db"), b"precious").unwrap();
        let Err(Error::Config(msg)) = Disk::claim(dir.path()) else {
            panic!("a foreign directory must be refused");
        };
        assert!(msg.contains("not a cache directory"), "{msg}");
        assert!(dir.path().join("zslg.db").exists());
        assert!(
            !dir.path().join(LOCK).exists() && !dir.path().join(MARKER).exists(),
            "a refused directory is not touched"
        );
    }

    #[test]
    fn a_second_claim_while_the_first_lives_fails() {
        let dir = tempfile::tempdir().unwrap();
        let first = Disk::claim(dir.path()).unwrap();
        let Err(Error::Config(msg)) = Disk::claim(dir.path()) else {
            panic!("a second claim must fail");
        };
        assert!(msg.contains("in use"), "{msg}");
        drop(first);
        drop(Disk::claim(dir.path()).unwrap());
    }

    #[tokio::test]
    async fn the_scan_keeps_registered_files_and_removes_the_rest_with_a_count() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Disk::claim(dir.path()).unwrap();
        let keep = id(THUMB, 0xab);
        write(&disk, &keep, b"12345").await;
        let other_kind = id(
            Group {
                app: "app",
                kind: "preview",
            },
            0xcd,
        );
        write(&disk, &other_kind, b"1234").await;
        let other_app = id(
            Group {
                app: "gone",
                kind: "thumbnail",
            },
            0xef,
        );
        write(&disk, &other_app, b"123").await;
        std::fs::write(disk.part_path(&keep), b"half").unwrap();
        std::fs::write(dir.path().join("flat-old-file"), b"12").unwrap();

        let scan = disk.scan(&[THUMB]).unwrap();
        assert_eq!(scan.files.len(), 1);
        assert_eq!((scan.files[0].0, scan.files[0].1), (keep, 5));
        assert_eq!(
            scan.removed,
            Removed {
                files: 4,
                bytes: 4 + 3 + 4 + 2
            }
        );
        assert!(disk.path(&keep).exists());
        assert!(!disk.path(&other_kind).exists());
        assert!(!dir.path().join("gone").exists(), "the whole app directory");
        assert!(dir.path().join(MARKER).exists());
    }

    #[tokio::test]
    async fn touch_sets_the_recency_the_scan_reports() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Disk::claim(dir.path()).unwrap();
        let a = id(THUMB, 1);
        write(&disk, &a, b"x").await;
        let at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        disk.touch(&a, at).unwrap();
        let scan = disk.scan(&[THUMB]).unwrap();
        assert_eq!(scan.files[0].2, Some(at));
    }
}
