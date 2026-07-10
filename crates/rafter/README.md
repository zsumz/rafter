# rafter

Deterministic, sans-IO Raft protocol core.

`rafter` contains the protocol kernel only: elections, replication,
configuration changes, read barriers, snapshots, leadership transfer, and
explicit input/output handling. It does not open sockets, touch files, spawn
tasks, or read wall-clock time.

Use this crate when you want to drive Raft from your own runtime or test
harness:

```rust
use rafter::{Input, Node, NodeConfig, NodeId};

let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 10)
    .expect("valid config");
let mut node = Node::new(config);
let outputs = node.step(Input::Tick);
```

Most production embeddings should pair this crate with `rafter-runtime` and
`rafter-storage` so persistence happens before peer or application outputs are
released.

## Public message buffers

`AppendEntries.entries` is a `SharedEntries` value. It behaves like an immutable
slice for normal consumers: use `entries.iter()`, `entries.as_slice()`, or
`&entries` when inspecting a received message. Code that must take ownership can
call `entries.to_vec()`. Code constructing messages can continue to use
`vec![entry].into()`.

This is an in-process allocation-sharing API, not a protocol change. Each peer
still receives ordinary Raft `AppendEntries` entries on the wire; a leader only
avoids rebuilding the same bounded log slice for every follower in one broadcast
round.
