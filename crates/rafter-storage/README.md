# rafter-storage

Durable storage traits and implementations for Rafter.

This crate defines the hard-state store, log segment, and snapshot store used
by `rafter-runtime`. It includes in-memory implementations for tests and
file-backed implementations for local durable nodes.

The on-disk formats are versioned and checksummed. Checksums are for accidental
corruption detection, not adversarial integrity; production deployments should
authenticate storage or validate application snapshots above this layer when
that threat model matters.

Use this crate with `rafter-runtime` when opening durable Raft nodes.

Application state is not stored here. Pair these stores with an application
applied-floor policy so Raft recovery never replays commands the application
has already durably applied.
