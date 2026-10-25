//! Which bytes under a key: part of a cached file's identity.

use sha2::Digest;

/// The version of what a key names. A new version is another file: the old
/// one stays until the LRU evicts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    /// The bytes under this key never change (the key carries what they were
    /// made from).
    Immutable,
    /// The object as its listing described it: a file replaced in place has
    /// another size or modification time.
    Stat {
        size: u64,
        modified_seconds: i64,
        modified_nanos: u32,
    },
}

impl Version {
    pub(crate) fn feed(self, h: &mut sha2::Sha256) {
        match self {
            Version::Immutable => h.update([0u8]),
            Version::Stat {
                size,
                modified_seconds,
                modified_nanos,
            } => {
                h.update([1u8]);
                h.update(size.to_be_bytes());
                h.update(modified_seconds.to_be_bytes());
                h.update(modified_nanos.to_be_bytes());
            }
        }
    }
}
