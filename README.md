[![build](https://github.com/zyla/dbstuff/workflows/build/badge.svg)](https://github.com/zyla/dbstuff/actions?query=workflow:build)

Random experiments in the area of databases. Written in Rust, using async.

Components
- Implemented: Page cache / buffer pool ([buffer-pool](./buffer-pool))
  - Naive when it comes to locking. In particular, keeps writer lock while doing IO on pages, which seems really really bad.
  - Probably buggy
- Work in progress: Table heap ([table](./table))
