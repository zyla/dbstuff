[![build](https://github.com/zyla/dbstuff/workflows/build/badge.svg)](https://github.com/zyla/dbstuff/actions?query=workflow:build)

Random experiments in the area of databases. Written in Rust, using async.

Components
- Page cache / buffer pool ([buffer-pool](./buffer-pool))
  - Naive when it comes to locking. In particular, keeps writer lock while doing IO on pages, which seems really really bad.
  - Probably buggy
- A buggy (I mean really buggy, doesn't work at all) lock free hash table ([buffer-pool/src/hashtable.rs](./buffer-pool/src/hashtable.rs))
- Work in progress: Table heap ([table](./table))
- Beginnings of a Raft implementation (leader election) ([raft](./raft))
  - using [stateright](https://docs.rs/stateright/0.13.0/stateright/)
