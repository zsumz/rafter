# rafter-multiraft

Many-group host layer for Rafter.

`rafter-multiraft` manages multiple caller-defined Raft groups in one process.
It provides untyped and typed host APIs so sharded systems can route messages,
step proposals, collect metrics, and keep group identity separate from node
identity.

Use `MultiRaftHost` when groups are dynamic or heterogeneous. Use
`TypedMultiRaftHost` when groups share one command type and one apply result
type.
