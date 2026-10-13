# Linux I/O stall qualification

The `Linux I/O stall qualification` workflow first qualifies the fault mechanism:
a child must remain pending in its file-sync protocol while a private mapping is
suspended, complete after resume, and leave the expected file contents. This is a
prerequisite for exercising the durable counter under the same fault. The probe
alone does not qualify Rafter, physical power loss, disk corruption, multi-host
availability, or any OS/filesystem support policy.

The job runs in a fresh `ubuntu-24.04` hosted VM. It creates a 128 MiB regular image,
formats that image as ext4, and derives a loop device, uniquely named/identified
linear device-mapper mapping, and private mount. No existing block device is
accepted as input. The resource owner verifies the exact backing file, mapping
UUID/table and mounted device before suspending or releasing resources. All
executables and evidence remain outside the suspended filesystem.

The child first writes and syncs a baseline, then reports its file identity. The
controller suspends the owned mapping with `dmsetup suspend --nolockfs --noflush`,
allows a second write, and requires the sync completion to remain pending for
three seconds. After resume it requires a successful child exit, the matching
content hash, and no extra protocol events. A portable negative control runs the
real child on an ordinary filesystem and requires rejection of that unstalled
execution. Portable positive controls exercise only the protocol using a gate;
they are not kernel-stall evidence.

Cleanup attempts resume before killing or waiting for a child. It then unmounts,
removes the owned mapping, and detaches the owned loop, in that order. An ambiguous
loop allocation is recovered only through its exact private backing file. An
ownership mismatch or cleanup failure fails qualification and prevents releasing
lower resources. There is no forced unmount, global device cleanup, or successful
skip on unsupported hosts. A process stuck in the kernel may remain unreapable
until storage resumes; such a timeout is retained as failure. Discard the VM if
cleanup cannot complete.

Run the portable controls on a development host with:

```sh
zcheck run io-stall-tests
```

Run the real probe only in a disposable Linux VM with Python 3, ext4/device-mapper
and loop tools, and passwordless sudo:

```sh
python3 scripts/check-io-stall --disposable-linux-vm \
  --artifacts /tmp/rafter-io-stall-evidence
```

The artifacts directory must be new and the source checkout clean. Receipts retain
source SHA and every tracked file hash before/after, host and tool versions,
command arguments/results, child events, and cleanup outcomes. The fresh backing
image remains under the recorded temporary root; it is not uploaded. GitHub
uploads the evidence even when qualification fails and retains it for 30 days.
The focused workflow runs on relevant pushes and pull requests; manual dispatch
becomes available once the workflow exists on the default branch. It has a
separate concurrency group from the archive and full Nightly profiles.

Device-mapper's [suspend/resume semantics](https://github.com/lvmteam/lvm2/blob/main/man/dmsetup.8_main)
are the injected fault. The
[hosted-runner VM isolation](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
limits the experiment to disposable resources. Actual runner execution is still
required: portable command fixtures cannot establish kernel/module availability
or a successful stall.

## Existing TLS counter workload

Before building the counter, its Linux job exactly selects both transport
handshake-deadline regressions. They cover raw TLS trickling and a known
certificate holder trickling a complete encrypted Rafter hello. The build
artifact retains `handshake-deadline.log`; a failed or missing case stops the
qualification before fault injection.

The probe-only [Linux run at `60a496dd`](https://github.com/zsumz/rafter/actions/runs/38186801268)
passed with a 3.03-second pending sync, a successful child exit, all owned
resources released and all 2,818 source hashes preserved. The subsequent
`tls-counter` job builds and smoke-tests the existing guide with every Rafter
dependency resolved to the checkout, then passes that exact binary to:

```sh
python3 scripts/check-io-stall --disposable-linux-vm \
  --counter-binary /path/to/checkout-built/rafter-counter \
  --artifacts /tmp/rafter-counter-stall-evidence
```

The runner initializes all three nodes' data directly on the private volume.
Their working directories, TLS certificates, executable and logs stay outside it.
After an acknowledged baseline and convergence, the same independent sync probe
proves the mapping is suspended. While it remains held, nine sequential write attempts
must return uncertain timeouts or prompt, typed full-admission refusals. No write
may acknowledge success. Queued cancellations can safely release capacity before
append even while the persistence worker is blocked; the oracle allows this and
counts uncertain attempts separately from requests known not to have appended.
At least one write must reach the uncertain timeout path. Passive metrics remain reachable and a quorum
read must time out in this workload. Progress metrics are last published and
can stay stale during synchronous consensus I/O. Separate live retained-credit
counters must stay within the configured count and byte budgets; write refusal
counters must match the requests known not to have appended. Read budget refusal
counters must remain unchanged for the timed-out quorum read.

After resume, the aggregate unknown increments must remain within zero through
the recorded number of uncertain attempts (at most nine). The runner never retries an uncertain increment. It requires a fresh
acknowledged write, convergence and drained live write/read credits, then orderly
shutdown and cold recovery of that acknowledged value. Another write must succeed
following recovery. The exact binary hash is checked before and after execution.

Each node has a resource sample at baseline, every stalled write observation,
the held quorum-read observation, resumed progress and cold recovery before
shutdown. The isolated-leader case samples baseline, majority acknowledgement
while held, resume and cold recovery. Samples retain PID/start ticks, CPU ticks
and clock rate, RSS/high-water/swap bytes, thread count and the actual descriptor
count, with the raw procfs inputs and sampling interval. Exited children, changed
identities, missing fields and unreadable inputs fail qualification; they cannot
be reported as zero usage. A collection failure follows the same resume/cleanup
path as a workload failure.

These are sequential, non-atomic process observations. The Linux kernel documents
that [procfs RSS counters are approximate](https://docs.kernel.org/filesystems/proc.html).
Summing node RSS can double-count shared pages and excludes kernel buffers, page
cache and the runner. High-water values belong to each process lifetime; restarted
nodes have new identities. Sparse samples can miss transient thread/descriptor
peaks. No fixed resource ceiling, physical-host memory bound, or performance
threshold is established by this instrumentation.

The [Linux resource run at `fa6533b0`](https://github.com/zsumz/rafter/actions/runs/38265739912)
passes all 63 portable/native controls, both handshake cases, four I/O scenarios
and the 36-case snapshot matrix. Readback verifies all 2,827 source hashes and
90 individual process samples across the three counter scenarios. Sampled RSS
maxima are 10.5–10.7 MiB per node, with at most 14 threads and 14 descriptors at
the selected points. Each node uses less than 0.5 CPU seconds from baseline to
the held observation in either 53-second whole-cluster stall. Those observations
belong to this fixed workload and process identities; they are not release limits.

The tutorial HTTP acceptor needs a separate resource policy. An isolated macOS
loopback probe using its exact `tiny_http 0.12.0` dependency grows from six to 67
process threads with 64 idle connections; threads release after disconnect. This
is a dependency-boundary reproduction, not a full Rafter or Linux fault run.
The counter's managed admission credits do not govern the HTTP library's workers
or queued requests. The [counter guide](examples/counter/README.md) explicitly
limits this listener to trusted local use and identifies front-end limits needed
by deployed applications. Neither serial managed waits nor loopback binding is
a whole-process resource bound.

Cleanup resumes storage before trying every child, even if one cannot be reaped.
Failed child cleanup retains the volume and fails the receipt. Node logs and
per-request outcomes are uploaded with source, binary and resource receipts.
The [Linux run at `90a5de9b`](https://github.com/zsumz/rafter/actions/runs/38187400606)
passes a 53.13-second stall: nine uncertain write timeouts, a pending quorum read,
resumed value 6 after baseline 5, then acknowledged value 106 preserved through
cold recovery and a following write to 107. Both jobs release every owned
resource, and downloaded evidence verifies all 2,820 tracked source hashes.
Portable controls and a real unstalled three-process negative control check the
oracles.
The existing counter pressure test separately qualifies all eight retained credits
and prompt full-queue refusal while application publication is blocked.
This single-host, shared-volume case does not establish whole-host memory bounds,
independent multi-host failures, device corruption or physical power-loss safety.

## Process loss during the stall

`--crash-while-stalled` requires `--counter-binary` and adds a separate process-loss
case after the ordinary stall workload. It sends `SIGKILL` to all three live
counter processes while the independent sync probe still proves I/O is held.
The controller resumes storage before waiting for those processes, and requires
actual `SIGKILL` exits before starting recovery.

Every regular node-data file is copied and hashed into `pre-recovery-data/` and
`pre-recovery-data.json` before any reopen. Symbolic links, special files, an empty
inventory, changed inputs or a mismatching copy fail qualification. Recovery uses
the counter's unchanged strict store-opening policy. A refusal remains a failed
receipt with preserved inputs and logs for diagnosis; the harness never silently
substitutes uncommitted-tail repair. Successful recovery must satisfy the same
bounded unknown-write result, fresh acknowledgement and cold-recovery checks.

Portable controls cover ordering, real disposable-process signal exits, input
preservation and failure handling. The [Linux run at `f5a9a3c8`](https://github.com/zsumz/rafter/actions/runs/38188063513)
passes the crash case with a 53.06-second hold and three verified `SIGKILL` exits.
Downloaded evidence verifies all 24 pre-recovery files (including hidden storage
locks), all 2,821 source hashes and complete resource cleanup. Acknowledged value
106 survives cold recovery; the next write reaches 107. The [live-credit run at `3ccf9d3e`](https://github.com/zsumz/rafter/actions/runs/38188937241)
also passes both 53-second counter cases. Every stalled write observation reports
one retained write/eight bytes while published pending writes remain zero; all
live credits drain after resume or recovery. It verifies the same 24 crash inputs
and all 2,821 source hashes. This tests OS process loss with the host kernel and
page cache still present; it does not simulate physical power loss.

## One stalled leader with a healthy majority

`--leader-only` requires `--counter-binary` and is separate from crash mode. The
runner creates three independently owned loop/ext4 mappings and initializes fresh
bootstrap data on each. Each live replica opens only its own node directory on
its assigned filesystem; the runner checks that their device identities differ.
The other bootstrap directories are never opened by a replica.

After an acknowledged baseline of 5 and convergence, the observed leader selects
which mapping to suspend. The same independent probe proves that filesystem stays
blocked. One increment submitted to the old leader must time out uncertainly;
it is never retried. The two healthy replicas must elect a higher-term leader,
complete a quorum read, acknowledge a fresh increment of 100 and read it back
before the mapping resumes. The blocked replica's published role may be stale,
so it is excluded from leader discovery during the hold.

After resume, all replicas must converge. The aggregate uncertain increment stays
bounded to zero or one, and the majority's acknowledgement cannot disappear. Live
credits must drain, a new write must succeed, and orderly cold recovery must
preserve that value and accept another write. Source and binary hashes, per-node
filesystem identities, the selected fault mapping, operation outcomes and every
resource cleanup are retained. All mappings are resumed before any child is
reaped. Failed child cleanup retains all three volumes; independent volume cleanup
is attempted even if another owned volume fails to close.

Portable controls qualify these failure paths and reject three directories on
one filesystem as isolation. A failed cleanup diagnostic must not prevent cleanup
of the other independently owned volumes; its failure remains in the receipt.

The [Linux run at `a07ae8e0`](https://github.com/zsumz/rafter/actions/runs/38189432313)
passes an 8.07-second isolated hold. Node 1's mapping remains suspended while node
3 becomes leader in term 2, reads value 6 and acknowledges/read-checks value 106.
The uncertain increment reached the healthy majority: the pipelined owner allows
dependency-safe replication while local persistence is pending, but a timeout
still cannot promise whether the write committed. After resume, all replicas
converge, live credits drain, value 107 survives cold recovery, and the following
write reaches 108. Downloaded evidence verifies all three device identities,
all 2,823 source hashes, the acknowledgement before resume and complete cleanup.
The shared-volume normal/crash cases also pass in that run. These are separate
virtual block devices on one host; they do not establish multi-host availability
or physical disk/power-loss behavior.

## Snapshot publication process loss

The workflow also runs `snapshot-publication` independently of the suspended-volume
jobs. It invokes one exactly selected, ignored storage test containing 36 cases:
nine existing publication/cleanup boundaries, with and without an old current
snapshot, through streamed file-source publication and inbound-transfer promotion.
The payload crosses three of the store's 256 KiB publication chunks.

A test-only hook stops the writer child at the named boundary. The parent waits
for that marker, requires an actual `SIGKILL` exit, and copies every regular target
file, including hidden temporary files, before strict reopen. Copies are checked
byte-for-byte and retained with a length/CRC32 inventory; non-regular inputs and a
changed inventory fail the test. Child startup and reaping have deadlines, and a
failed case retains its workspace for diagnosis.

Recovery must select the old snapshot (or none) before current-manifest rename,
and the complete new snapshot afterward. It checks the full descriptor and every
payload byte, the exact pending-transfer cleanup state, then publishes and reopens
another snapshot. A complete transfer remains resumable before cleanup; removing
its manifest makes a remaining body abandoned rather than authoritative.

Run locally on Linux or macOS with:

```sh
zcheck run snapshot-process
```

To retain successful cases too, set `RAFTER_SNAPSHOT_PROCESS_ARTIFACTS` to a new
directory whose parent already exists. The ordinary CI `test` job selects this
parent explicitly; `zcheck run check` and `full` include it. The dedicated Linux
job additionally retains compiler/source identity, tracked-file SHA-256 inventories
before and after, case logs, outcomes and pre-recovery copies, including hidden
files. Its uploads retain evidence for 30 days even on failure.

The initial local matrix passes all 36 cases, and readback verifies all 138
pre-recovery file copies. The [Linux run at `c591bfff`](https://github.com/zsumz/rafter/actions/runs/38193176690)
also passes all 36 cases; downloaded evidence verifies the same 138 copies and
all 2,825 tracked source hashes before and after execution. These are
snapshot-store process-loss checks with the kernel and page cache present;
they do not model physical power loss, the application database's publication,
or an entire Raft node's recovery across all stores.

## Ledger application publication process loss

The separate `ledger-publication` job tests the reference ledger's application
journal. Its one exactly selected, ignored Unix test contains 13 cases: three
journal-creation boundaries, four append/seal boundaries, and three replacement
boundaries each for snapshot-state replacement and in-place compaction. Pause
hooks exist only in the library test binary. Every case requires a child to reach
its named boundary and exit from `SIGKILL`; startup and reaping have deadlines.

Before recovery, the parent copies every regular database file, including the
staging journal, and verifies its bytes against a length/CRC32 inventory. Recovery
compares the full ledger view and applied index together: balances, deposited
supply, session state, cached requests and cached results. It checks the exact
committed-frame count, replays the last request without another mutation, commits
a fresh request, and reopens that result.

The two complete-but-unsealed append cases must fail ordinary open with
`UnsealedCompleteFrame`, without changing any journal byte. Only a separate,
explicit repair after input preservation may discard that unacknowledged frame;
the repair must report its exact length and recover the previous state. A sealed
append is recovered as the complete new state. Replacement before rename selects
the old journal; replacement after rename selects the new one. Compaction keeps
the state and reduces the committed frame count only after rename.

Run on Linux or macOS with `zcheck run reference-ledger-publication`, or use
`scripts/reference-ledger-publication --list-only` to check the exact selection.
`zcheck run full` includes the matrix. Set `RAFTER_LEDGER_PUBLICATION_ARTIFACTS`
to a new directory whose parent exists to retain successful cases too; failures
always retain their workspace. The Linux job preserves source/compiler identity,
native filesystem observations, the resolved reference dependency lock, all case
outcomes and pre-recovery files for 30 days. The initial local matrix passes all 13 verified process exits.

This qualifies process loss in this reference application's journal with the
kernel and page cache intact. It does not establish physical power-loss behavior,
the durability of an arbitrary embedding database, or an entire Raft node's
recovery across all stores. The existing injected-error and torn-byte tests
remain separate coverage.
