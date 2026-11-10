//! A one-write helper for tests that talk to a `Replica`.

use crossbeam_combinators::Channel;
use zslg_consensus::RaftState;
use zslg_log::{Data, Error, Replica, StampedWrite, WriteOutcome};
use zslg_net::NetError;

/// Write one `Data` to a `Replica` and await its outcome: `Accepted` is
/// `Ok(raft_state)`, `Rejected` and `Failed` are `Err(error)`. Opens a
/// single-write stream, for tests that hold one write per call; production
/// callers hold one connection per peer.
///
/// # Errors
/// The replica's rejection or failure, or a transport error when the replica
/// drops the outcome.
pub async fn replica_write<R: Replica + ?Sized>(
    replica: &R,
    data: Data,
) -> Result<RaftState, Error> {
    let mut outcomes = replica.write(Channel::once(StampedWrite::now(data)));
    // The channel blocks on the replica's pipeline, so it must not be pulled on
    // the reactor.
    let first = tokio::task::spawn_blocking(move || outcomes.recv())
        .await
        .map_err(|e| Error::Net(NetError::Rpc(format!("write stream join: {e}"))))?;
    match first {
        Ok(Ok(WriteOutcome::Accepted { raft_state, .. })) => Ok(raft_state),
        Ok(Ok(WriteOutcome::Rejected { error, .. } | WriteOutcome::Failed { error, .. }))
        | Ok(Err(error)) => Err(error),
        Err(_) => Err(Error::Net(NetError::Rpc(
            "replica dropped write outcome".into(),
        ))),
    }
}
