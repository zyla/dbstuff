## Isolation and consistency levels

- [Linearizability: A Correctness Condition for Concurrent Objects](http://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf)
- [A Critique of ANSI SQL isolation levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf)
- [Highly Available Transactions: Virtues and Limitations](https://amplab.cs.berkeley.edu/wp-content/uploads/2013/10/hat-vldb2014.pdf)
- [Weak Consistency: A Generalized Theory and OptimisticImplementations for Distributed Transactions](http://pmg.csail.mit.edu/papers/adya-phd.pdf) (Atul Adya's PhD thesis)
- [Lazy Database Replication with Snapshot Isolation](http://www.vldb.org/conf/2006/p715-daudjee.pdf) - definitions of SI and Strong SI
- [Lazy Database Replication with Ordering Guarantees](https://cs.uwaterloo.ca/~kmsalem/pubs/DaudjeeICDE04.pdf) - Session Serializability
- [An explanation of the difference between Isolation levels vs. Consistency levels](https://dbmsmusings.blogspot.com/2019/08/an-explanation-of-difference-between.html)
- [Demystifying Database Systems, Part 2: Correctness Anomalies Under Serializable Isolation](https://fauna.com/blog/demystifying-database-systems-correctness-anomalies-under-serializable-isolation)

## Transaction protocols

- [Building Consistent Transactions with Inconsistent Replication](https://syslab.cs.washington.edu/papers/tapir-tr-v2.pdf)
- [Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36726.pdf)
- [Spanner: Google’s Globally-Distributed Database](https://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf)
- [Introducing SLOG: Cheating the low-latency vs. strict serializability tradeoff](https://dbmsmusings.blogspot.com/2019/10/introducing-slog-cheating-low-latency.html)
- [Minimizing Commit Latency of Transactions inGeo-Replicated Data Stores](http://www.nawab.me/Uploads/Nawab_Helios_SIGMOD2015.pdf) (cited by the above post as "If fact, there exist proofs in the literature that show that there is a fundamental tradeoff between serializability and latency in geographically distributed systems.")
