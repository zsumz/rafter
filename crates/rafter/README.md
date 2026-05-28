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
