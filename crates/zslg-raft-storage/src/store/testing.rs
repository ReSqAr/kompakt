//! Entry points for the tests of the crates that run a raft node on this store.

use super::{
    Arc, Batch, Error, LogId, NodeId, NodeInfo, RAFT_META_CF, RaftMetaSlot, RaftStateHandle,
    RocksRaftStorage, Storage, StoredMembership, TypeConfig, Vote, put_meta,
};
use zslg_consensus::ToRaft as _;

/// Builds a `Raft` instance backed by RocksDB storage in `storage`, generic over
/// the network factory so a test harness can supply its in-memory router. The
/// caller opens `storage` because a root's column-family list must name every
/// family the node's other stores use too.
///
/// # Errors
/// If the store cannot be opened or the engine refuses to start.
pub async fn build_raft_for_test<NF>(
    node_id: NodeId,
    config: Arc<raft_kit::Config>,
    network: NF,
    storage: Arc<Storage>,
    raft_state: RaftStateHandle,
) -> Result<zslg_raft::Raft, Error>
where
    NF: raft_kit::RaftNetworkFactory<TypeConfig>,
{
    let (log_store, sm) = RocksRaftStorage::open(raft_state, storage)
        .await?
        .into_adaptor();
    zslg_raft::Raft::new(node_id, config, network, log_store, sm)
        .await
        .to_raft("Raft::new (test)")
        .map_err(Into::into)
}

/// Overwrite the persisted membership + vote with a FOREIGN node id (simulates
/// on-disk raft state written by a different node — the hard-crash rejoin
/// scenario). Writes straight into the raft-meta family.
///
/// # Errors
/// If the write or its fence fails.
pub async fn seed_foreign_membership_for_test(storage: &Storage) -> Result<(), Error> {
    use raft_kit::Membership;
    let other = NodeId::new_v4();
    let voters: std::collections::BTreeSet<NodeId> = [other].into_iter().collect();
    let membership: Membership<NodeId, NodeInfo> =
        Membership::new(vec![voters.clone()], Some(voters));
    let stored = StoredMembership::new(None::<LogId<NodeId>>, membership);
    let vote = Vote::<NodeId>::new(1, other);
    let cf = storage.cf(RAFT_META_CF)?;
    let mut batch = Batch::new();
    put_meta(&mut batch, &cf, RaftMetaSlot::LastMembership, &stored)?;
    put_meta(&mut batch, &cf, RaftMetaSlot::Vote, &vote)?;
    storage.write(batch)?;
    storage.fence().map_err(Into::into)
}
