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
python3 load.py --run-id tls-soak-001 --node 1 --seconds 1800 --output /tmp/counter-node-1-run
```

Choose a fresh `--run-id` for each test attempt and use that same identifier on
all three clients. It appears in every history event and capture summary so results
from different attempts cannot be accidentally combined. The output directory must
be new and its parent must exist. `--http-base` selects
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


After stopping all clients, collect their three output directories and check them
against quorum reads taken before any client starts and after all clients finish:

```sh
python3 check_load.py --run-id tls-soak-001 --baseline 0 --final 123 node-1-run node-2-run node-3-run
```

Replace `0` and `123` with those actual observations. This check requires that no
other clients write during the measured interval. It accepts only complete,
nonempty histories with the expected run identifier covering nodes 1, 2, and 3,
generated by the adjacent `load.py`
source. It recomputes byte hashes, operation pairing, counts and latency summaries;
rejects duplicate acknowledged values and observations that regress within one
client; and requires successful write/read progress. The final counter must include
every acknowledged increment and can additionally include uncertain increments.
It cannot include unexplained writes. `--minimum-seconds` requires each history's
first-invocation-to-last-completion span to reach the requested duration; allow
extra client runtime for the trailing pause when enforcing a duration floor.

The JSON report identifies these accounting checks and hashes every input file.
Its boundary values are caller-supplied; the surrounding test must retain their
quorum-read evidence, server identities and fault timeline. This is not a complete
linearizability decision across unsynchronized hosts. Incomplete captures fail
this check and must remain available for diagnosis; they must not be edited into
complete captures. Run `python3 -m unittest test_load.py test_check_load.py` to
exercise the recorder and the verifier's rehashed counterexamples. The guide smoke
suite also checks three real node histories against before/after quorum reads.


### Check ordering when clients share a clock

Add `--shared-monotonic-clock` only when the caller has established that every
client uses the same monotonic clock, such as the processes started by `soak.py`.
A matching host label is not proof of this condition. The default remains the
clock-independent accounting check for histories collected on separate hosts.

This mode decides whether the recorded increment-by-one and quorum-read intervals
admit a sequential counter execution. It rejects a read older than another
client's already completed write, reads of increments not yet invoked, and
acknowledgements in an impossible order. It retains a witness identifying each
selected operation, its counter value and a permissible monotonic timestamp.
Equal clock samples may overlap; the check cannot resolve ordering below the
clock's resolution. The before/after quorum values and absence of other writers
remain required external facts.

A timed-out write is a pending operation whose eventual effect is unknown. It may
linearize after the client timeout, or remain unapplied through the final quorum
observation. The witness chooses only as many uncertain increments as the final
value requires; it does not relabel their client outcomes as acknowledgements.
This follows the pending-operation distinction in
[Herlihy and Wing's linearizability model](https://cs.brown.edu/people/mph/HerlihyW90/p463-herlihy.pdf)
and its [clarification for pending invocations](https://arxiv.org/abs/2105.06737).

The solver uses the counter's ranks: each acknowledged increment fixes one rank;
a read of value `v` bounds increment `v` above by its completion and increment
`v+1` below by its invocation. Missing ranks consume the earliest-invoked unused
uncertain writes. These are identical increments with no observed completion
bound, so selecting an earlier invocation cannot remove a valid placement.
Choosing each rank's earliest permitted time leaves maximal room for later ranks.
Tests compare this solver with an independent enumeration of pending subsets and
all sequential orders for 3,000 deterministic small histories, then replay every
accepted witness. This model covers this counter workload; it is not a verifier
for arbitrary application commands or a multi-host clock protocol.


## Retain a coordinated local fault run

`soak.py` starts three owned server processes and three simultaneous load clients,
then kills the current leader with `SIGKILL`. It preserves that node's files before
reopening them and requires the surviving majority to acknowledge new client work.
After the clients finish, it checks their histories with the shared-clock solver,
shuts down the servers with
backend release, preserves their state, and performs a cold restart. The recovered
quorum value must match the measured final value, and a new increment must work.

```sh
python3 soak.py --binary target/debug/rafter-counter --output /tmp/counter-soak-001
```

The default workload is 60 seconds; `--seconds` accepts a finite value of at least
30. Output must be a new directory beneath an existing parent. The fixture copies
and hashes the executed binary and retains the run ID, events, all client histories,
server logs, history report, and pre-recovery file hashes and contents. Its receipt
requires source/binary preservation and successful owned-process cleanup. Failure
keeps the available evidence. It only signals processes it started.

The all-server cold restart follows graceful shutdown; the earlier leader loss is
an actual abrupt kill under load. This is a local operational fixture using the
existing TLS transport. It does not qualify multiple hosts, physical power loss,
cross-host real-time ordering or whole-host resource bounds. Binary hashes alone
do not establish compiler or library-source provenance.

From the repository root, bind the run to the checked build with:

```sh
python3 scripts/check-counter-guide --mode checkout --soak-artifacts /tmp/counter-qualified-001
```

This retains the copied source hashes, dependency metadata and resolved lock beside
the 60-second run. The Linux I/O lane uses this option and preserves the complete
checkout inventory before and after build and workload execution. The isolated
standalone Git pin can be checked with `--mode pinned` and another new output path.
`python3 -m unittest test_load.py test_check_load.py test_soak.py` also covers the
recorder, accounting checks, process cleanup and recovery-input preservation.
