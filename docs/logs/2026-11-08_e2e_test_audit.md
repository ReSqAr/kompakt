# zslg end-to-end tests: overlap audit

After the move into `zslg-cluster-tests` (32 tests, ~108 s; 22 and ~81 s after the merges below). What each test adds, and
what is already covered elsewhere. Read from the test bodies, not the names.

## Done

- `multi_node_writes::three_node_tcp_write_round_trip` removed. It brought up three nodes, wrote
  through the leader and read the entry back from the unstable tail — a strict subset of
  `cluster_bringup::three_node_cluster_brought_up_via_public_api` (bring-up + write + invariants)
  and of `read_paths` (now `read_paths`, which reads back through `unstable_tail`).
- `full_restart::rpc_server_accepts_connections_while_raft_startup_is_blocked` and
  `::cancelling_the_shutdown_token_stops_a_node_that_never_becomes_ready` drive **one** node with an
  unreachable peer and never touched the cluster harness. They now live in `zslg-replica-node`
  (`node_tests/blocked_startup.rs`) beside the other single-node startup tests.

## Candidates, done

All four were taken (22 tests, ~81 s, down from 32 / ~108 s). Each changes what is asserted where, not
what is asserted:

- **Replace-a-node tests.** `tcp_failure_detection` (restart a follower on the same address, then
  pooled writes survive it) is folded into `rolling_replacement`: after every one of the three
  rotations it now writes a batch through the leader and checks the *replaced* node sees it in its own
  tail. The proptest `full_replacement_transfers_membership_and_data` stays as it was.
- **Eviction.** `reconciliation` and `decommission::evicted_node_survives_a_denied_rejoin` shared the
  arrangement (trim the victim's name from every other node's peers, relaunch them with fast election
  timers). They are one test now, `eviction::evicted_member_leaves_the_set_is_refused_reads_and_stays_up`:
  the replicated set shrinks, the victim's reads are refused with `NotReady`, and its supervision tree
  stays up for 30 s. It runs on four nodes (the reconciliation shape). `decommission` keeps the
  demote-to-learner test and `unwanted_leader_evicts_itself`.
- **Read paths.** `writer_integration`, both `learner_integration` tests and
  `stable_read::writes_appear_in_stable_log_across_all_nodes` are `read_paths::every_read_path_sees_the_writes`:
  external writer -> replica tail, replica writer -> learner tail (the learner's clean start/stop
  included), stable log on every node, durability chain. `stable_read` keeps the teeth check for the chain.
- **`leader_failover::follower_loss_does_not_block_writes`** is dropped; the proptest's degraded windows
  and `rolling_replacement` cover it.

Cost: the merged tests are longer and a failure names a step, not a test. They are written as ordered
sections with a comment per claim for that reason.

## One thing the audit surfaced

None of the replace-a-node tests catches the bug fixed in the writer coordinator (a surviving
writer kept a write stream open to a *killed* node's dropped service, which answered `Shutdown`
forever). They replace nodes gracefully, so the old stream ends cleanly. Only the proptest's
`Kill` op (a hard kill with the sockets left behind) exercised it, and only under I/O pressure.
The regression test is at the coordinator now
(`zslg-writer`, `a_stream_to_a_dropped_instance_is_reopened`): a fake replica answers `Failed(Shutdown)`
on the first stream and `Accepted` on the next. It fails without the reconnect and passes with it.
