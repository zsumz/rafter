# Platform qualification

This matrix records the candidate's executed coverage. Stable release support
commitments, minimum operating-system versions, and supported storage stacks
remain to be selected. The [release checklist](../RELEASE.md) and
[work-completion map](work-completion.md) define the remaining release gates.

## Native targets

| Target | Recurring qualification | Filesystem evidence |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 24.04 CI runs workspace tests, strict lint, published API checks, exact-package/MSRV consumers, and the invariant producers. Dedicated Linux jobs run TLS process failures, I/O stalls, and snapshot process loss; Nightly runs the longer owner soak and invariant profiles. | The I/O-stall fixtures explicitly create private loop-backed ext4 filesystems. Ordinary hosted storage tests do not retain filesystem type or mount options. |
| `aarch64-apple-darwin` | The macOS 15 CI lane runs strict workspace and optional durable-service lint, the complete storage suite, and the portable launcher checks. The integrated local checkpoint also passed the workspace build, tests, strict lint, and rustdoc with the durable feature. | Local durability/performance observations use APFS. The hosted storage lane does not retain filesystem type or mount options. |
| Other architectures, Linux libc variants, and Windows targets | No native execution evidence in this qualification matrix. Cross-platform APIs or compilation on another target do not establish recovery or transport behavior on that target. | No filesystem qualification recorded here. |

The strict build and exact-package baseline is Rust 1.88. The additional
`current-stable` job records its resolved compiler; it does not change the MSRV.
The full invariant evidence executor requires Linux. Its macOS tests exercise
portable process handling and the required refusal of Linux-only execution.

At candidate `7151197c`, the completed native jobs in
[CI run 38264606712](https://github.com/zsumz/rafter/actions/runs/38264606712)
record Ubuntu 24.04.5 on x86-64 and macOS 15.7.9 on Apple Silicon, both using
Rust 1.88. The macOS storage command passes 321 tests with one process-loss
case intentionally ignored; that case has a separate Linux selection. The
checkout is merge commit `4d0f1850`, whose tree matches the candidate. These
individual job results do not replace the candidate's final invariant aggregate
or architecture review. The earlier complete local workspace result is recorded
in the [release-readiness checkpoint](release-readiness.md#current-code-checkpoint).

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
