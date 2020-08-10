Experiment with transaction protocol.

Similar to how CRDB handles transactions: <https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html#overview>

Also inspired by TAPIR - let's see if we need consensus in the data shards.

Transaction proceeds as follows:

Assume a set of _transaction registry_ nodes.

1. Generate a unique, monotonically increasing transaction ID `txid`. (TODO: magic)

2. Send a `CreateTransaction(txid, leader_id)` to all txreg nodes. `leader_id` is the node id of the _transaction leader_. Choose the tx leader to have low latency to the client.

3. txreg nodes create an empty tx record in memory. They don't want to persist it before responding. In fact they need not persist it at all.
  New tx record has state `InProgress` and ballot number `(1, leader_id)`.

4. Wait for the majority of txreg nodes to respond.

5. At this point we can use `txid` in write records.

6. To write a value to a data range, send a `Write(txid, range_id, key, value)` to replicas of the range. Wait for majority to respond.

7. Storage nodes write the value keyed by `(key, txid)` and start persisting it, but don't wait for disk flush before responding.

7. If writing to a new range, send a `AddRange(txid, range_id)` to the txreg nodes. No need to wait for response right now.

8. To commit, send a `FlushWrites(txid)` to all ranges we've written to. Wait for majority of each range to respond.

9. Then send `RequestCommit(txid)` to the tx leader.

10. tx leader sends `Commit(txid, ballot)` to txreg nodes, and waits for response from the majority.
