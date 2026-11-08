# zslg end-to-end tests: overlap audit

After the move into `zslg-cluster-tests` (32 tests, ~108 s). What each test adds, and
what is already covered elsewhere. Read from the test bodies, not the names.

## Done

- `multi_node_writes::three_node_tcp_write_round_trip` removed. It brought up three nodes, wrote
  through the leader and read the entry back from the unstable tail — a strict subset of
  `cluster_bringup::three_node_cluster_brought_up_via_public_api` (bring-up + write + invariants)
  and of `writer_integration` (which reads back through `unstable_tail`).
- `full_restart::rpc_server_accepts_connections_while_raft_startup_is_blocked` and
  `::cancelling_the_shutdown_token_stops_a_node_that_never_becomes_ready` drive **one** node with an
  unreachable peer and never touched the cluster harness. They now live in `zslg-replica-node`
  (`node_tests/blocked_startup.rs`) beside the other single-node startup tests.

## Candidates (not done; each changes what is asserted, so it is your call)

| tests | overlap | suggestion |
|---|---|---|
| `tcp_failure_detection` ×2, `rolling_replacement`, proptest `full_replacement_transfers_membership_and_data` | all replace nodes on the same address while writes continue and then check the cluster still commits | keep the proptest script and `rolling_replacement`; fold the "sustained writes survive a follower replacement" assertion into `rolling_replacement` and drop `tcp_failure_detection` |
| `reconciliation` ×1, `decommission::evicted_node_survives_a_denied_rejoin` | same arrangement: trim a server name from every other node's `config.peers` so the leader evicts it for real; `decommission` says so in its own doc comment | one file, one setup helper, both assertions |
| `writer_integration`, `learner_integration`, `stable_read::writes_appear_in_stable_log_across_all_nodes` | each boots three nodes, writes, and reads back through a different reader (writer node, learner node, stable log) | share one cluster in a `read_paths` file: saves two or three boots |
| `leader_failover::follower_loss_does_not_block_writes` | the proptest's degraded windows cover a follower down while writes continue | drop; keep `leader_failover_writes_recover` (it asserts the cutoff observer) |

Estimated effect: 32 → ~24 tests, ~108 s → ~70 s.

## One thing the audit surfaced

None of the replace-a-node tests catches the bug fixed in the writer coordinator (a surviving
writer kept a write stream open to a *killed* node's dropped service, which answered `Shutdown`
forever). They replace nodes gracefully, so the old stream ends cleanly. Only the proptest's
`Kill` op (a hard kill with the sockets left behind) exercised it, and only under I/O pressure.
A deterministic regression test belongs at the coordinator, with a fake replica that answers
`Failed(Shutdown)` on the first stream and `Accepted` on the second; that is not written yet.
