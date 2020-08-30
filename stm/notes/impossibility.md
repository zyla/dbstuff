# Impossibility of non-abortible interactive read transactions with strict serializability without central coordination

(informal "proof")

Suppose we want to implement a distributed transactional KV store.

We want to provide strict serializability, that is:
- transactions observe outcomes as if they happened in a serial order, and
- the order is consistent with real time order. For example, if T1 committed before T2 started, then T2 must observe the effects of T1.

We also want the following constraints:
1. Not relying on real-time clocks for correctness. (Why? Because they can't be perfectly synchronized.)
2. Not using a Timestamp Oracle either. (Why? Because it is a central resource which all transactions must contact. Also: it means an additional roundtrip at the start of a transaction.)
3. Read-only transactions don't abort. (Why? Because they may be long running, and if they could be aborted by any write, then starvation would be almost guaranteed.)
4. Writes don't block on read transactions. (Why? Because again - read transactions may be long running.). Writes may be, however, required to synchronously _notify_ a read transaction that a write has happened to ensure serializability.
5. Keys may be stored on different servers. (Why? Because without that the database is not distributed, and we're limited to a single node.)
6. Reads and writes only contact the servers which store the accessed keys, and possibly the servers involved in concurrently running transactions which could be affected by them. (Why? It's trivial to implement the wanted semantics by notifying every server about every transaction, but it's not scalable.)

## The counterexample

We will now show a scenario where a system satisfying the above constraints is unable to give the correct answer.

Assume, for simplicity, that servers don't fail, and that each key is stored in only one copy on one server.

Assume that we have to two keys, `x` and `y`, initially having value 0, and stored on two different servers (the "`x`-server" and the "`y`-server").

Consider the following scenario (steps are listed in sequential order):

```
T1: begin(); read(x)
T2: begin(); write(x, 1); commit()
T3: begin(); write(y, 1); commit()
T1: read(y); commit()
```

T2 happens before T3 in real time, so the legal serialization orders are:
- T1 T2 T3 (T1 reads x=0, y=0)
- T2 T1 T3 (T1 reads x=1, y=0)
- T2 T3 T1 (T1 reads x=1, y=1)

Note that it is not allowed for T1 to read x=0, y=1. That would mean that it observed T3, but failed to observe T2, which would create a causality cycle:
- T1 depends on T3
- T2 anti-depends on T1
- T3 started after T2 committed

What values could `T1` observe given the implementation constraints?

- When T1 reads `x`, the writer transactions haven't even started. So it must read x=0.
- When T2 writes `x`, it can even notice that there is a read transaction in progress which read x, and notify it before committing.
- When T3 writes `y`, it can't know about the existence of T1. Why? Because T1 hasn't yet read `y`, which means it hasn't contacted the server which stores `y` yet.
- When T1 reads `y`, it must decide whether it should observe T3. (i.e. whether to read the value T3 just wrote, or the previous one). It must satisfy the following constraints:
  - If T3 committed before T1 started, then T1 should observe T3 (due to real-time constraints).
  - But if T3 started after T2 committed, then T1 should not observe T3 (because it didn't observe T2).

T1 doesn't have enough information to decide. Consider the following scenario:

```
T3: begin(); write(y, 1); commit()
T1: begin(); read(x)
T2: begin(); write(x, 1); commit()
T1: read(y); commit()
```

It is indistinguishable from the first scenario from the point of view of T1, because whatever happens at the `y`-server happens independently from T1 and T2, until T1 reads `y`. But in this case T1 must observe x=0, y=1 - an outcome not legal in the first scenario.

To implement the correct semantics, we would have to obtain information about the relative real-time ordering between T2 and T3. That would require writes which don't see any concurrently running read transaction to communicate with _some_ server such that any transaction which may read the same value in the future knows about the order of this write relative to others. Since when writing we don't know which transaction may read the value in the future, that would mean global coordination (communicating with either one central server, or the majority of all servers). This violates our constraint #6.

## What _can_ be done

Serializability without real time constraints is still useful - this is what Postgres officially provides, for example.

In the example above, observing x=0 and y=1 would be legal - that would correspond to the serialization order T1 T3 T2. This behavior could be achieved by arbitrarily (e.g. lexicographically) ordering transactions when they are not causally related from the point of view of the database.

We could even formalize a consistency level between serializability and strict serializability, which requires being consistent with real time ordering only between transaction with overlapping sets of accessed objects, and not providing such guarantee in case of independent transactions (such as T2 and T3 in the example).

## Curiosities

CockroachDB [appears to have the same problem](https://jepsen.io/analyses/cockroachdb-beta-20160829#comments) (also called "causal reverse" - see [CockroachDB’s Consistency Model](https://www.cockroachlabs.com/blog/consistency-model/), section "CockroachDB does not offer strict serializability"). This suggests that formalizing the not-quite-strict-serializability guarantees would be beneficial. ~~Maybe someone has already done it?~~ There's [an article by Daniel Abadi](https://fauna.com/blog/demystifying-database-systems-correctness-anomalies-under-serializable-isolation) which calls this "strong partitioned serializability".

FaunaDB [claims to support strict serializability](https://fauna.com/blog/serializability-vs-strict-serializability-the-dirty-secret-of-database-isolation-levels), and in the article they acknowledge the causal reverse problem. They also claim not to rely on clock synchronization. I wonder how they do this.
