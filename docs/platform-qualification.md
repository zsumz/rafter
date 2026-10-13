# Platform qualification

This matrix records the candidate's executed coverage. Stable release support
commitments, minimum operating-system versions, and supported storage stacks
remain to be selected. The [release checklist](../RELEASE.md) and
[work-completion map](work-completion.md) define the remaining release gates.

## Native targets

| Target | Recurring qualification | Filesystem evidence |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 24.04 CI runs workspace tests, strict lint, published API checks, exact-package/MSRV consumers, and the invariant producers. Dedicated Linux jobs run TLS process failures, I/O stalls, and snapshot process loss; Nightly runs the longer owner soak and invariant profiles. | The I/O-stall fixtures explicitly create private loop-backed ext4 filesystems. The native test job now records filesystem type and mount options for the workspace and temporary directories before execution. |
| `aarch64-apple-darwin` | The macOS 15 CI lane runs strict workspace and optional durable-service lint, the complete storage suite, and the portable launcher checks. The integrated local checkpoint also passed the workspace build, tests, strict lint, and rustdoc with the durable feature. | Local durability/performance observations use APFS. The hosted storage lane now records its containing volumes and native mount options before execution. |
| Other architectures, Linux libc variants, and Windows targets | No native execution evidence in this qualification matrix. Cross-platform APIs or compilation on another target do not establish recovery or transport behavior on that target. | No filesystem qualification recorded here. |

The strict build and exact-package baseline is Rust 1.88. The additional
`current-stable` job records its resolved compiler; it does not change the MSRV.
The full invariant evidence executor requires Linux. Its macOS tests exercise
portable process handling and the required refusal of Linux-only execution.

At candidate `0ef3a1a0`, the completed native jobs in
[CI run 38377077377](https://github.com/zsumz/rafter/actions/runs/38377077377)
record Ubuntu 24.04.5 on x86-64 and macOS 15.7.9 on Apple Silicon, both using
Rust 1.88. Linux passes 3,255 workspace tests (53 ignored) and the separate
440-test durable selection. These counts overlap. The macOS storage command
passes 321 tests with one process-loss case intentionally ignored; that case has
a separate Linux selection. Its strict workspace/durable lint and reviewed
launcher selections also pass.

The checkout is merge commit `55219c61`, whose tree matches the candidate. The
retained logs verify that checkout, compiler and exact test outcomes; the native
receipts below additionally verify a clean checkout at capture time. They do not
supply a before/after source inventory for these two jobs or replace
maintainer architecture approval. The current PR invariant jobs remain in
progress; the completed aggregates at `5d1cc025` retain their own source identity.
The earlier complete local workspace result is recorded in the
[release-readiness checkpoint](release-readiness.md#current-code-checkpoint).

## Native environment receipts

`scripts/record-storage-environment --output NEW_FILE.json` captures the current
OS, compiler, source head/status, and filesystem identity of the workspace,
`std::env::temp_dir()` selected by that exact Rust toolchain, and `RUNNER_TEMP`
when present. `--path DIRECTORY` adds a known existing test directory. The output
file must be new; failures retain a failed receipt and available observations.

Linux capture uses `findmnt` and checks the returned device against the directory.
macOS capture correlates POSIX `df` with the native mount table and verifies the
volume's device. This handles APFS firmlinks without assuming that the textual
path is beneath the reported mountpoint. Raw commands and their outputs accompany
the structured filesystem and mount-option fields.

CI keeps `linux-storage-environment.json` and `macos-storage-environment.json`
inside the existing native-test diagnostics artifacts. The Linux I/O lane also
keeps a receipt with its counter build and snapshot process artifacts; its private
loop/ext4 fault volumes retain their own separate mount evidence. Older candidate
jobs, including the `7151197c` and `bc6d1677` native runs, predate these receipts.
Their filesystem details must not be inferred from a newer run.

Capture is an observation at one point in time. It does not prove that mounts
remain unchanged throughout a test or that a filesystem/controller/device stack
honors synchronization after power loss. Support decisions still require the
recorded test results and the storage contract below.

## Observed candidate test storage

The `0ef3a1a0` [Linux test job](https://github.com/zsumz/rafter/actions/runs/38377077377/job/115228075000)
and [macOS storage/launcher job](https://github.com/zsumz/rafter/actions/runs/38377077377/job/115228075424)
retain their own native receipts. Independent readback checks the downloaded
artifact hashes, recorded Git/compiler identity, recorder source hash, raw mount
queries and selection of all three directories: workspace, Rust temporary path
and runner temporary path. Both receipts are under
`target/rafter-invariants/ci-diagnostics/` within their respective artifacts.

| Job | Receipt in the diagnostics artifact | Observed filesystem and volume | Mountpoint and options |
| --- | --- | --- | --- |
| Linux test | `linux-storage-environment.json` | ext4 on `/dev/sda1` | `/`; `rw,relatime,discard,journal_async_commit,nobarrier,errors=remount-ro,commit=30,data=writeback` |
| macOS storage/launcher | `macos-storage-environment.json` | APFS on `/dev/disk2s5` | `/System/Volumes/Data`; `local,journaled,nobrowse,root data` |

Each job's three directories resolve to its listed volume. On macOS, the
workspace under `/Users` and resolved Rust temporary path under `/private/var`
share that Data volume through APFS firmlinks; textual path-prefix matching would
not establish that. The source-bound native recorder checks device equality at
capture time. Readback verifies its queries and retained selections without
claiming a new stat of the remote host. These observations do not qualify other
runner volumes, mount stability during the subsequent tests or hardware power
loss.

## Observed hosted Linux storage

The first completed capture at source `9296d80d` is retained in
[Linux I/O run 38312228874](https://github.com/zsumz/rafter/actions/runs/38312228874).
Its counter and snapshot jobs each record Ubuntu 24.04, x86-64,
Linux `6.17.0-1022-azure`, and Rust 1.88. Their workspace, Rust temporary directory
(`/tmp`), and runner temporary directory all resolve to the job's root ext4 mount:

| Job | Receipt within its artifact | Root filesystem source | Device major:minor |
| --- | --- | --- | --- |
| TLS counter | `io-stall-counter-build/storage-environment.json` | `/dev/sda1` | `8:1` |
| Snapshot process loss | `storage-environment.json` | `/dev/nvme0n1p1` | `259:1` |

Both receipts report
`rw,relatime,discard,journal_async_commit,nobarrier,errors=remount-ro,commit=30,data=writeback`.
These observations describe those two jobs; they do not establish the configuration
of earlier jobs or other hosted runners. The private loop/ext4 I/O-stall volumes
are separate mounts and retain separate ownership evidence.

The [Linux 6.17 ext4 documentation](https://www.kernel.org/doc/html/v6.17/admin-guide/ext4.html#options)
explains that `nobarrier` disables journal write barriers and `data=writeback`
allows file data to reach the filesystem after the associated metadata journal
commit. The receipts do not establish the virtual disk's cache or power-loss
guarantees. Passing process-loss tests under these options therefore supplies no
physical-power-loss qualification, and performance observed here cannot establish
performance under different mount and cache policies. Deployment qualification
must record and exercise the intended storage stack against the contract below.

## Storage contract and qualification boundary

File-backed Raft stores and durable TLS session state depend on successful file
synchronization, atomic same-directory replacement, parent-directory
synchronization, and exclusive ownership. The selected filesystem, mount
configuration, virtual disk, controller, and device must honor those operations.
The [storage durability protocol](../crates/rafter-storage/DURABILITY_PROTOCOL.md)
defines the ordering, error fencing, recovery, and directory-provisioning rules.
Application state and its applied floor need their own atomic durable publication.

The [Linux I/O-stall fixtures](io-stall-qualification.md) exercise blocked I/O,
quorum progress, process termination during a stall, and cold recovery on private
ext4 volumes. Those results, the separate snapshot process-loss matrix, and local
APFS observations are limited to their recorded environments. These executions
do not simulate loss of volatile device caches, hypervisor storage faults, or
physical power removal. Network filesystems, other filesystems, filesystem
options, and hardware stacks require their own qualification against the same
contract.

Before declaring or expanding support, retain native toolchain/OS identity,
filesystem and mount configuration, workspace and public-consumer results,
ownership/recovery tests, TLS process behavior, and the applicable durability
fault evidence. A release support decision must identify which of those results
backs each target and storage stack; the current candidate still has open
physical-power-loss and sustained multi-host qualification work.
