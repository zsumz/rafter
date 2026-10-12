# Standalone Rafter counter

Three separate server processes, file-backed Raft and application state, and
mutually authenticated TLS peer connections. The
[getting-started walkthrough](../../getting-started.md) explains how the crates
connect, what Rafter handles, and what your application implements. Run commands
and a complete source reference follow the explanations.

This folder is its own Cargo project. Copy it anywhere; its Rafter dependencies
come from a pinned Git revision, with no dependency on the surrounding checkout.
The pin includes the ciphertext-level TLS handshake deadline and nested snapshot
directory synchronization fixes qualified in candidate `7151197c`.
Generated certificates and data stay under `certs/` and `data/` in the directory
you run it from. The client HTTP listeners bind to loopback only.

The HTTP acceptor is a teaching front end, separate from Raft admission. Its
`tiny_http` connection workers and request queue are not bounded by the owner's
eight read/write slots. Serial handling limits concurrent managed waits; it does
not cap accepted connections, queued HTTP requests or their memory. Keep this
listener for trusted local use. A deployed service needs a front end with explicit
connection, queue, request-size and timeout limits before managed admission.
The five-second operation wait starts after HTTP parsing and dispatch; it is not
an end-to-end network deadline.

Run the additional checks with Rust 1.88, OpenSSL, and Python 3 installed:

```sh
cargo test --locked
cargo build --locked
python3 smoke.py target/debug/rafter-counter
```

Application tests also qualify `src/application.rs` with the bounded worker:
protocol-only prefixes, result correlation, retained credits, and failed durable
publication. The main thread owns `DurableRaftDriver`; HTTP callers use bounded
client handles. Shutdown waits for storage release while that owner keeps driving.
The optional `rafter-service/durable` feature is enabled explicitly.

This example supports only the original three voters. Its checkpoint preserves
the existing retirement facts across restart, and membership changes, contradictory
records, or lost checkpoint history refuse startup. It does not implement dynamic
membership administration or exactly-once increment retries. A write error
reports `write_may_have_committed: false` only for a proven pre-append refusal;
timeouts and unresolved outcomes remain uncertain.

For explicit peer placement, set the same `COUNTER_PEER_ADDRS` on all three
nodes, with numeric socket addresses in node 1, 2, 3 order, for example:

```sh
export COUNTER_PEER_ADDRS='192.0.2.11:7001,192.0.2.12:7002,192.0.2.13:7003'
```

Replace those documentation addresses with the hosts' actual interface addresses.
Each node binds its own address and dials the other two directly using the existing
TLS transport. Certificate names remain `node-1`, `node-2`, and `node-3`; changing
placement does not change authentication or authorize new members. Use brackets
for IPv6, such as `[2001:db8::11]:7001`. The setting overrides `COUNTER_PEER_BASE`;
an absent setting keeps the default loopback/base-port behavior. Partial lists,
duplicate endpoints, unspecified or multicast IPs, and zero ports are refused.

The HTTP API and redirects still use local loopback ports. A multi-host load runner
must run locally on each test host and select the leader explicitly instead of
following another host's loopback redirect. Provision each node's private key and
data only on its owning host; shared CA and public peer certificates are needed on
all three. This address option prepares a network test fixture; local smoke tests
do not establish multi-host latency, throughput, or resource bounds.

The smoke test uses temporary directories and its own local ports. It checks
TLS client authentication, writes and reads, leader loss, full-cluster crash
recovery, missing/corrupt or unsafe checkpoint state, quorum loss, failed
application publication with committed replay after restart, and clean shutdown. It only
stops processes it started. Add `--explicit-peers` to repeat the checks with
explicit, reordered peer ports. The guide checker runs both address modes.

`GET /metrics` observes the owner without borrowing the application. Progress
indexes and pending counts are last published and can stay stale during storage
I/O. `retained_writes`, `retained_reads` and their `_bytes` fields read current
admission credits; `write_refusals` and `read_refusals` count budget refusals since
owner construction and reset on restart. Credits include canceled work still
owned by the driver and results awaiting collection. The counters remain available
while storage is blocked, but do not form an atomic snapshot, reserve capacity or
bound whole-host memory. `GET /status` also reads the local value and can wait for
that store. Metrics do not prove a quorum read. Commit and applied indexes include
different kinds of work, so their difference is not an application backlog count.

On POSIX hosts, `python3 pressure.py target/debug/rafter-counter` deliberately
blocks the leader's application publication on a FIFO. It takes about a minute:
eight client timeouts must retain their write slots while followers keep applying,
a ninth write must refuse promptly, and a read must wait for application durability.
It then requests shutdown, releases the blocked operation into an I/O error, and
checks replay and fresh admission after restart. This exercises blocked publication
and failure cleanup, not physical disk latency or power-loss behavior.

From the Rafter repository root, use Python 3.11 or newer for
`python3 scripts/check-counter-guide`. It checks that the teaching excerpts
match this application, extracts the complete source into a fresh folder, and
qualifies it against the current checkout with temporary dependency overrides.
`python3 scripts/check-counter-guide --mode pinned` separately builds that copy
using its canonical lock and exact Git revision, without checkout dependencies.
Both modes verify the resolved graph before building and run the process smoke
test. Add `--pressure` to run the separate blocked-publication qualification; CI
and `zcheck run counter-pressure` require it against the checkout. Only the temporary checkout-mode lock may change. After editing
application files, update the explanations and excerpts as needed;
`python3 scripts/check-counter-guide --update` refreshes the complete source
reference and checks the excerpts without running Cargo.


## Capture a sustained workload

With Python 3.11 or newer, run one local client on each test host, using that host's
node ID. For example, on node 1's host:

```sh
python3 load.py --node 1 --seconds 1800 --output /tmp/counter-node-1-run
```

The output directory must be new and its parent must exist. `--http-base` selects
the same base port used by the local server. The client checks the node identity,
keeps one active client exchange, increments by one, and makes every fifth
operation a quorum read. `--read-every`, `--pause-ms`, and `--timeout` control that
mix, the pause after completion, and the whole request deadline. `--max-operations`
can additionally cap the number of dispatched operations. The duration bounds new
dispatches; an already dispatched operation can finish within its request timeout.

The client always contacts loopback, ignores HTTP proxy settings, and never follows
redirects or retries writes. A follower's redirect is recorded as a refusal. A
timeout, malformed acknowledgement, or uncertain server error remains uncertain;
only an explicit pre-append refusal clears that uncertainty. Later operations have
new IDs and are not retries of an earlier increment. Run clients on all replicas
to continue supplying work when leadership moves.

`history.jsonl` records each invocation before sending and each completion with its
outcome and observed latency. An interrupted invocation without a completion stays
unresolved. Histories stream to disk with bounded response buffers; their disk size
grows with the workload. `summary.json` binds the exact history bytes by SHA-256,
records outcome counts and latency range, and distinguishes complete from incomplete
capture. `invocation.json` records the client source hash, host, parameters and
endpoint; `initial-metrics.json` preserves the initial local node observation.
Existing output is never overwritten. A missing or incomplete summary requires
inspection, and a completed capture may contain no successful operations.

These files are workload observations, not a qualification verdict. Capture the
initial and final quorum value, server source/binary identities, topology, fault
timeline, resource samples and all client histories when qualifying a cluster.
Monotonic timestamps from different hosts cannot establish a shared ordering.
HTTP round-trip latency includes the front end, consensus and storage; serial
clients and history writes also affect load, so this is not a peak-throughput
benchmark or a whole-host resource bound. Multi-host qualification remains separate
from the local smoke suite. `python3 -m unittest test_load.py` exercises deadlines,
cancellation, redirects, response bounds and uncertain outcomes; both guide modes
also verify captured histories against real leader and follower processes.
