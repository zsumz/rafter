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
read must time out in this workload. Metrics are the last published owner
observation, which can be stale during synchronous consensus I/O; they are not
used as current admission counters.

After resume, the aggregate unknown increments must remain within zero through
the recorded number of uncertain attempts (at most nine). The runner never retries an uncertain increment. It requires a fresh
acknowledged write, convergence and drained observed proposal queues, then orderly
shutdown and cold recovery of that acknowledged value. Another write must succeed
following recovery. The exact binary hash is checked before and after execution.

Cleanup resumes storage before trying every child, even if one cannot be reaped.
Failed child cleanup retains the volume and fails the receipt. Node logs and
per-request outcomes are uploaded with source, binary and resource receipts.
Portable controls and a real unstalled three-process negative control qualify the
oracles; hosted execution must still pass before claiming the counter stall case.
The existing counter pressure test separately qualifies all eight retained credits
and prompt full-queue refusal while application publication is blocked.
This single-host, shared-volume case does not establish whole-host memory bounds,
independent multi-host failures, device corruption or physical power-loss safety.
