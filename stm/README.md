Implementation of transactional memory with various isolation levels.

Constraint: don't use global timestamps, and don't take long-lived locks - each object and each transaction are assumed to be independent.
The idea is to derive transaction ordering if necessary based only on their dependencies.

- **Read Committed**
