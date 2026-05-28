# rafter-maelstrom

Maelstrom test node for Rafter.

`rafter-maelstrom` is a publish-disabled binary crate that adapts Rafter to
Maelstrom's JSON protocol for linearizable key/value workloads. It is used to
exercise real process behavior, peer messaging, leader changes, durable state,
and read/write semantics under Maelstrom fault injection.

Run it through the repository scripts rather than depending on it as a library:

```sh
scripts/maelstrom-lin-kv
scripts/maelstrom-lin-kv-leader-restart
```
