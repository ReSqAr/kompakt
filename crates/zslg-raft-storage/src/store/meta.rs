//! Reading and writing the raft-meta singleton slots.

use crate::Error;
use rocksdb_kit::Storage;
use rocksdb_kit::{Batch, Cf};
use serde::Serialize;
use serde::de::DeserializeOwned;
use zslg_log::LogError;
use zslg_log::ToCodec;

use super::{RAFT_META_CF, RaftMetaSlot, encode_meta_key};
// SUPER

pub(super) fn read_meta<T: DeserializeOwned>(
    storage: &Storage,
    slot: RaftMetaSlot,
) -> Result<Option<T>, Error> {
    let cf = storage.cf(RAFT_META_CF)?;
    let key = encode_meta_key(slot);
    let Some(pinned) = storage.get_pinned(&cf, key).map_err(|e| {
        Error::Log(LogError::Storage(format!(
            "read raft meta slot {slot:?}: {e}"
        )))
    })?
    else {
        return Ok(None);
    };
    // PINNING RULE: `pinned` holds its block in the block cache for as long as
    // it lives. It must not cross an `.await` or a batch commit — decode here
    // and let it drop at the end of this expression.
    Ok(Some(
        postcard::from_bytes(&pinned).to_codec("read raft meta")?,
    ))
}

/// Stage one meta slot's overwrite into `batch` (LWW — the slot byte is the
/// whole key, so a later write simply replaces it). The caller owns the batch
/// because a multi-slot batch has to be one atomic write.
pub(super) fn put_meta<T: Serialize>(
    batch: &mut Batch,
    cf: &Cf<'_>,
    slot: RaftMetaSlot,
    value: &T,
) -> Result<(), Error> {
    let bytes = postcard::to_allocvec(value).to_codec("encode raft meta")?;
    batch.put(cf, encode_meta_key(slot), &bytes);
    Ok(())
}

/// Write one meta slot and fsync — the ACK fence for a single-slot update
/// (vote / committed).
pub(super) fn write_meta_durable<T: Serialize>(
    storage: &Storage,
    slot: RaftMetaSlot,
    value: &T,
) -> Result<(), Error> {
    let cf = storage.cf(RAFT_META_CF)?;
    let mut batch = Batch::new();
    put_meta(&mut batch, &cf, slot, value)?;
    storage.write(batch)?;
    storage.fence().map_err(Into::into)
}
