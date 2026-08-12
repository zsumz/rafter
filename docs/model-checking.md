# Model Checking

Two independent engines carry this name. The bounded simulator
(`rafter-model-check-fast`) explores deterministic Raft schedules over the real
implementation; the TLA+ tier ladder runs TLC over `specs/tla/raft/Raft.tla`, a
design model that does not import rafter. Everything below the
[TLA+ tier ladder](#tla-tier-ladder) section describes the simulator.

`rafter-model-check-fast` explores bounded, deterministic Raft schedules. The
profiles differ in bounds and scheduling breadth; all exhaustive checks must
end with `frontier_exhausted`. A state, time, or memory budget ending the run
is incomplete coverage, not a pass.

The invariant gate's ownership, dependency rules, and trust boundaries live in
[Invariant tooling architecture](invariant-tooling-architecture.md).

## TLA+ Tier Ladder

Three tiers are wired, each pinned to one primary config by
`verification/raft-invariant-profiles.json` and by the profile contract in
`crates/rafter-invariants`, and each carrying a list of [focused proof
obligations](#how-an-obligation-is-wired).

| Tier | Primary config | Nodes | Values | MaxTerm | MaxLogLen | ReadRequests | Symmetry | Primary continuation |
| --- | --- | --- | --- | ---: | ---: | --- | --- | --- |
| PR | `RaftCi.cfg` | `{n1,n2}` | `{v1,v2}` | 2 | 2 | `{r1}` | yes | gating |
| Nightly | `RaftNightly.cfg` | `{n1,n2,n3}` | `{v1,v2}` | 3 | 3 | `{r1,r2}` | yes | reporting |
| Weekly | `Raft.cfg` | `{n1,n2,n3}` | `{v1,v2}` | 3 | 3 | `{r1,r2}` | no | reporting |

### What a green tier means

The two answers differ, and the difference is pinned in the contract as
`primary_completion` rather than left to a reader's assumption.

**PR is gating.** `RaftCi.cfg` genuinely drains its queue, so the PR tier
passes only when TLC reports `states_left = 0` and clears the calibrated
floors of 255,177,640 generated and 36,058,645 distinct states — the exact
counts of a measured post-reduction exhaustion (93 minutes at `-workers 4` on
a 14-core host), pinned the way obligation floors are pinned. A timeout is
incomplete coverage, not a pass. Nothing about the PR gate's semantics
changed.

**Nightly and weekly are reporting.** A green scheduled tier means: every wired
proof obligation exhausted its frontier at its own calibrated floors, the
trace-sample and negative-detector qualification passed, and the monolith
continuation ran its full budget, checkpointed, recovered, and reported a
healthy expanding frontier. It does **not** mean the monolith completed. The
monolith has never completed, not once, and at a measured frontier fanout near
2.85 with no inflection it is not going to; `scripts/tla-continuation-telemetry`
classifies each run's trajectory and the nightly lineage currently reads
`incomplete-expanding`.

That is the honest statement of what was always true. Gating the scheduled
lanes on an event that cannot occur produced a permanently red result that said
nothing about the protocol, while the evidence those lanes do produce -- drained
obligations over sound sub-relations, plus accumulated monolith coverage -- went
unreported. The continuation is research and coverage accumulation. The
obligations are the proof.

Reporting relaxes the budget and nothing else. A counterexample found by a
reporting continuation still fails the layer red; so does a malformed, missing,
or unreadable artifact set, an incompatible checkpoint, a failed qualification
probe, and an undischarged obligation. The pinned 120M/16M minimums remain in
the contract for the scheduled profiles as the accumulation bar the lineage
reports progress against, published beside the observed counters rather than
enforced as a terminal condition. Every TLA+ receipt carries a
`tla_continuation` binding naming its pinned policy and the continuation's
actual ending -- `frontier-exhausted`, `counterexample`, or
`budget-elapsed-frontier-open` -- so a green scheduled receipt cannot be read as
a completed monolith. The policy is contract state: the verifier refuses a
receipt whose declared policy disagrees with the profile it claims to come
from, and refuses outright any PR receipt claiming a reporting continuation.

A profile may only demote its primary when something else still exhausts: the
contract rejects a reporting profile that declares no obligations.

### Pinning the TLC tool

Upstream `tlaplus/tlaplus` tag `v1.8.0` is a rolling nightly channel, not an
immutable release: the tag stays fixed while its assets are rebuilt, uploaded
under fresh IDs, and their predecessors deleted. Three distinct `tla2tools.jar`
digests were observed in five weeks, and the asset ID pinned before this
contract now 404s. `tools/tla/ASSET_ID` is therefore a liveness pin only.

The identity pin is the SHA-256 in `tools/tla/SHA256SUMS`, which
`scripts/tla-model-check` verifies before every run and which the profile
contract repeats independently as `tool_sha256`, so a silently swapped upstream
asset fails closed rather than being accepted. `tools/tla/VERSION` records the
channel and the TLC build string it reported. A repo-controlled mirror of the
reviewed jar is the durable fix for the liveness half; it is a maintainer
decision and is not made here.

#### TLA+ contract migration: v15 to v16

`verification/model-check-contract-migrations.json` is the simulator's ledger,
not this one: its entries pin `rafter-model-check-fast` profiles and require at
least one monotone `configured_depth` increase measured from that binary's
telemetry, so a TLA+ entry cannot be written there without breaking the
simulator's overhead gate. The TLA+ runner contract records its migrations as a
profile-manifest schema bump plus a producer identity bump, guarded by negative
fixtures in `crates/rafter-invariants/src/contract/profile/tests.rs`.

The v9/`rafter-invariants-tla-v15` to v10/`rafter-invariants-tla-v16` migration
introduces the obligation vocabulary and re-pins the TLC tool. Because the
checkpoint contract digests the runner's `configuration` map, and the tool pin
lives in that map, this migration resets the accumulated TLC checkpoint lineage
for every profile once. The affected `runner_contract_sha256` values are:

| Profile | From | To |
| --- | --- | --- |
| PR | `84f4980f5963064f…` | `4ec4e394e5afbc62…` |
| Nightly | `4ca7833d8f558e44…` | `9de14d88b983720d…` |
| Weekly | `8d09c27585de34fa…` | `580d06b52b886fd3…` |

Scheduled continuations restart from an empty queue at the next run and
reaccumulate. Obligations themselves are outside that map by design, so future
obligation edits do not repeat this reset.

TLC's `-coverage` is deliberately not part of the contract. Measured on this
repository's own trace-sample model, one coverage report costs about 790 KB of
additional framed stdout (3,849 bytes without, 793,379 with). At the PR tier's
325-minute budget, `-coverage 5` would emit roughly 65 reports — on the order
of 50 MB against the producer's 64 MiB per-process stdout cap, all of it inside
the receipt-bound, hashed, uploaded, and re-parsed `tla-log` artifact. It
destabilizes more than it informs at these run lengths.

### What the ladder does not prove

Nightly and weekly have **identical constants**. Only `SYMMETRY
ModelPermutations` differs, so the deepest tier is a soundness check on the
symmetry quotient, not a deeper exploration. No tier exceeds three nodes or log
length three.

The table below is an exhaustive evaluation of the spec's own quorum predicate
(`StableQuorum`/`MembershipQuorum`) over every configuration in
`ConfigurationSet` for each node set. The *quorum core* of a configuration is
the set of nodes belonging to every one of its quorums. An empty core is
exactly the condition under which majority overlap does work: no single node
can decide alone.

| Nodes | Configurations | Empty core | Empty core and joint | Exactly one minimal quorum |
| --- | ---: | ---: | ---: | --- |
| `{n1,n2}` (PR) | 7 | 0 | 0 | 7 of 7 |
| `{n1,n2,n3}` (nightly, weekly) | 25 | 1 | 0 | 24 of 25 |
| `{n1,n2,n3,n4}` | 71 | 13 | 8 | 58 of 71 |

Read in the direction the ladder uses it:

- **At the PR tier no majority-overlap argument is exercised at all.** Every
  one of the seven reachable configurations has exactly one minimal quorum, so
  a quorum rule demanding unanimity would pass `RaftCi.cfg` identically.
- **Joint-consensus quorum intersection is degenerate at every wired tier,
  including weekly.** With `|Nodes| <= 3` and `OneVoterChange`, all 18 joint
  configurations retain a non-empty core, so the two-half conjunction never
  constrains more than a fixed set of nodes does. The single empty-core
  configuration any wired tier reaches is `Stable({n1,n2,n3})`, so stable
  majority overlap is exercised from three nodes up and joint-quorum
  intersection is exercised nowhere.
- The scripted `RaftMembershipTraceSample` does not close this gap. Its two
  joint configurations are `Joint({n1,n2,n3},{n1,n2})` and its inverse, whose
  core is `{n1,n2}`.

### What the ladder does prove about membership

Two claims about membership coverage are easy to state too strongly; both were
measured rather than assumed.

- `MaxLogLen = 2` does mean **no committed command can coexist with a completed
  configuration change** at the PR tier. `EnterJoint` and `LeaveJoint` append
  one entry each, so a completed change plus a command needs three log slots.
  This is arithmetic and holds for any two-slot model.
- A command committed **while a joint configuration is the commit authority**
  is nevertheless reachable at *every* tier, PR included: it needs only two
  slots, one joint configuration entry and one command. `ClientAppend` carries
  no membership guard. TLC witnesses it at `{n1,n2}`/`MaxTerm=2`/`MaxLogLen=2`
  with `[Joint({n1,n2},{n2})@1, Command(v1)@2]`, both committed.
- A committed command coexisting with a completed change is reachable at
  nightly and weekly bounds, witnessed at `{n1,n2,n3}`/`MaxLogLen=3` with
  `[Joint({n1,n2,n3},{n1,n2})@1, Stable({n1,n2})@2, Command(v1)@3]`.

### The four-voter joint-quorum model

`specs/tla/raft/RaftJointQuorum.cfg` is the smallest model that reaches a joint
configuration with an empty quorum core. Four voters is the smallest node set
where one exists at all, and `EnterJoint` from `Stable(Nodes)` reaches four of
them, `Joint({n1,n2,n3,n4},{n1,n2,n3})` and its three siblings. The spec needs
no change to get there: the two-voter change one might expect to be required is
neither required nor supported, because `EnterJoint` guards on
`OneVoterChange` and `ConfigurationSet` admits only one-voter joint pairs.
`MaxTerm = 1` and `MaxLogLen = 2` are the smallest bounds that commit that
configuration and then complete the change with a following stable entry. Run
it with `scripts/tla-model-check --joint-quorum`.

It is a manual-run artifact and not a wired obligation, for one reason that
used to be two. The contract objection is gone: when this section was first
written the profile contract admitted exactly three configs at one shared
floor pair, and no fourth model could register at any budget. The obligations
vocabulary removed that wall — an obligation registers any non-primary config
at its own calibrated floors and budget, which is how eight of them are wired
today. What survives is the empirical objection, and it is sufficient on its
own:

**It does not exhaust in a tier's budget.** Measured on a 14-core machine with
`-workers 4 -Xmx8g`, the same flags `--joint-quorum` uses. State counts are
independent of machine load; the wall times below were taken while the host was
heavily contended and should be read as upper bounds.

These rows were measured before the state-space reductions described in
"Exact state-space reductions" below, and are left as they were taken. The
two-voter row in particular no longer reproduces: the same model now exhausts
at 127,112 generated and 24,995 distinct. The three- and four-voter rows are
readings from runs that never exhausted, so the reductions do not give them a
corrected value either — they would still be readings, just different ones.

| Model | Generated | Distinct | Queue | Depth | Wall | Peak RSS | Exhausted |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2 voters, `MaxTerm=1`, `MaxLogLen=1` | 137,480 | 35,363 | 0 | 28 | 44 s | 1.71 GiB | yes |
| 3 voters, `MaxTerm=1`, `MaxLogLen=1` | 2,183,849 | 639,627 | 315,832 | 19 | 10 min | — | no, still expanding |
| 4 voters, `MaxTerm=1`, `MaxLogLen=2` (this config) | 10,073,549 | 2,551,776 | 1,397,208 | 20 | 45 min | — | no, still expanding |

The two unexhausted rows are readings taken while the runs were still going;
they were stopped shortly afterwards, at 12.8 and 47.0 minutes, without ever
reaching a shrinking queue, which is why they report no peak RSS.

Each added voter costs more than an order of magnitude, and the frontier of the
four-voter model was still growing when measurement stopped. A wired tier
passes only on `states_left = 0`, so this model would have to drain a queue
that was still getting longer after ten million generated states.

The honest summary: joint-quorum intersection is checkable, cheaply witnessed,
and not currently checked by anything that runs on a schedule. Restricting the
transition relation was tried against exactly this problem and did not change
that; the measurements are in the next section.

### Focused proof obligations

`Spec` checks all nine invariants against all of `Next`. `Raft.tla` also defines
four narrower transition relations, each a strict disjunct-subset of `Next`
composed from the same action operators under unmodified guards, so every
behavior of a focused spec is a behavior of `Spec`:

| Relation | Spec operator | Action families |
| --- | --- | --- |
| `CoreNext` | `CoreSpec` | `Timeout`, `SendRequestVote`, `DeliverRequestVote`, `BecomeLeader`, `ClientAppend`, `SendAppend`, `DeliverAppend`, `Commit`, `Apply` |
| `MembershipNext` | `MembershipSpec` | core minus `ClientAppend`, plus `EnterJoint`, `LeaveJoint` |
| `SnapshotNext` | `SnapshotSpec` | core plus `ApplicationStateLoss`, `Restart`, `CreateSnapshot`, `TransferSnapshot`, `InstallSnapshot` |
| `ReadNext` | `ReadSpec` | core plus `RegisterRead`, `GrantRead` |

All four are plain disjunct-subsets. `SnapshotNext` used to need an IF wrapper
reproducing `Next`'s compaction-first branch, because `CompactSnapshot` was
reachable only through it; folding creation and compaction into one action
removed the branch, the wrapper, and the separate `ProtocolNext` name that
existed to denote the disjunction without the branch.

`JointQuorumInit` is a focused initial state for the joint-quorum obligation: the
exact post-state of `Timeout(L)`, three `SendRequestVote`/`DeliverRequestVote`
pairs, and `BecomeLeader(L)` from `Init`, with every witness and monitor variable
derived from the action that last writes it along that prefix rather than
guessed. It is therefore a reachable state of `Spec`, and TLC reports no
invariant violation on it. Because it names a leader, a config using it cannot
declare `SYMMETRY ModelPermutations`; `RaftJointQuorumFocusedInit.cfg` omits
symmetry, and `JointQuorumPermutations` is the sound reduced set (permutations
fixing the distinguished leader) available if that trade is revisited.

Two configs carry `RaftJointQuorum.cfg`'s constants and all nine invariants and
change only the transition relation and initial state:

| Config | Spec | Init | Symmetry |
| --- | --- | --- | --- |
| `RaftJointQuorumFocusedNext.cfg` | `MembershipSpec` | standard | yes |
| `RaftJointQuorumFocusedInit.cfg` | `JointQuorumFocusedSpec` | `JointQuorumInit` | no |

#### Measurements

Same host and flags as the table above: 14 cores, `-workers 4 -Xmx8g`. Neither
run reached a shrinking queue, so neither reports peak RSS, and both wall times
should be read as upper bounds — the host was running an unrelated build
throughout.

| Model | Generated | Distinct | Queue | Depth | Wall | Exhausted |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 4 voters, full `Spec` (`--joint-quorum`, from the table above) | 10,073,549 | 2,551,776 | 1,397,208 | 20 | 45 min | no, still expanding |
| 4 voters, `MembershipSpec` (`--joint-quorum-focused-next`) | 25,934,867 | 6,201,924 | 3,185,806 | 24 | 34 min | no, still expanding |
| 4 voters, `JointQuorumFocusedSpec` (`--joint-quorum-focused-init`) | 8,109,085 | 2,040,484 | 1,004,645 | 20 | 13 min | no, still expanding |

Sampled queue trajectories, both monotonically increasing from the first
progress line to the last:

| `--joint-quorum-focused-next` | Generated | Distinct | Queue | Depth |
| ---: | ---: | ---: | ---: | ---: |
| 0 min | 39,396 | 11,157 | 5,724 | 15 |
| 6 min | 5,067,921 | 1,363,929 | 730,788 | 22 |
| 14 min | 10,352,918 | 2,663,185 | 1,407,124 | 23 |
| 23 min | 15,695,471 | 3,923,771 | 2,042,022 | 24 |
| 34 min | 25,934,867 | 6,201,924 | 3,185,806 | 24 |

| `--joint-quorum-focused-init` | Generated | Distinct | Queue | Depth |
| ---: | ---: | ---: | ---: | ---: |
| 0 min | 34,717 | 14,175 | 8,131 | 13 |
| 2 min | 2,018,205 | 563,884 | 282,247 | 18 |
| 7 min | 4,622,630 | 1,216,625 | 601,466 | 19 |
| 11 min | 6,839,230 | 1,758,878 | 864,955 | 19 |
| 13 min | 8,109,085 | 2,040,484 | 1,004,645 | 20 |

Neither run was stopped by exhaustion. The focused-next run was stopped at a
wall cap. The focused-init run ended earlier because the host filesystem filled
and TLC's disk state queue failed to write; that is a host limit, not a result
about the model, and its numbers are a truncated reading rather than a budget.
No invariant was violated in either run.

#### What this means

**Restricting `Next` did not make the four-voter model exhaust.** The focused
relations are cheaper per state — `--joint-quorum-focused-next` passed the full
model's 45-minute counts in about 14 minutes — but cheaper per state is not the
constraint. The frontier still grew monotonically for the whole of every run, and
`--joint-quorum-focused-next` reached depth 24 against the full model's 20, so
the extra throughput bought depth rather than closure. Dropping `ClientAppend`,
reads, snapshots, restart, and application-state loss removes about eighteen
action families down to ten and still leaves a state space that four voters, two
log slots, and a set-valued `messages` variable blow past.

**The focused initial state did not change that either**, and it costs a
symmetry quotient to have. `ModelPermutations` was already collapsing the
symmetric copies of the election that `JointQuorumInit` skips, so naming a
leader trades a quotient the model had for a prefix it did not need. At equal
wall time the two runs are within a small factor of each other, and neither
shows a shrinking queue.

So the first phase of this redesign answers its question in the negative:
**focused proof obligations are not on their own sufficient to make the
four-voter joint-quorum model a wired tier.** What they are is real and
narrower: each relation states an obligation that is sound by construction and
that TLC can discharge at smaller bounds — `CoreSpec` and `ReadSpec` both
exhaust at two voters in seconds — so they are usable as tiers wherever the
bounds are already tractable. Making four voters tractable needs a different
lever than action-family selection: a smaller `messages` representation, a state
constraint bounding in-flight messages, or a symmetry-preserving focused initial
state. None of those is implemented here.

### Exact state-space reductions

Two changes to `Raft.tla` remove state the model was carrying without using.
Both are exact: they change no reachable behavior, and each has an argument for
why, stated at its definition in the spec and repeated here.

#### Snapshot creation and compaction are one action

`CreateSnapshot(n)` used to set `compactionPending[n]`, `Next` carried a
compaction-first branch that disabled every protocol action while any flag was
set, and `CompactSnapshot(n)` cleared the flag and wrote nothing else.

**Why it is sound.** The intermediate state was unobservable. No protocol action
was enabled in it; the only action that was, `CompactSnapshot`, agreed with its
own successor on every variable except the flag; and no predicate outside the
flag's own `TypeOK` conjunct read the flag. So for any old behavior, deleting
the flag from every state turns the intermediate state into a stuttering step of
the folded spec, which `[][Next]_vars` already admits, and leaves every other
state unchanged. The two specs are therefore stuttering-equivalent for every
property over the remaining variables, and no invariant loses a state it used to
be checked on: the intermediate state and its successor agreed everywhere an
invariant could look.

This one reduces the state count, because the intermediate state was a distinct
state.

#### The snapshot prefix is derived, not stored

`snapshotPrefix[n]` used to be a variable holding a copy of the log up to the
snapshot floor. It is now `SnapshotPrefix(n) == Prefix(log[n], snapshotIndex[n])`.

**Why it is sound.** `SnapshotIdentitySoundFor` asserted exactly that equality,
and `LogMatching` checked it on every reachable state, so on every state the
model reaches the derived value and the stored value were already the same
sequence. The step that needs more than "the invariant held" is a recorder
called with a primed log against an unprimed snapshot floor —
`RecordLogicalPrefixes(log', snapshotIndex, ...)` in `ClientAppend` and
`DeliverAppend` — where the derived form reads the successor log while the
stored form still held the predecessor's prefix. These agree because no action
rewrites a log at or below its own snapshot floor. `ClientAppend`, `EnterJoint`
and `LeaveJoint` only append. `DeliverAppend` replaces the receiver's log
wholesale, but only under `CanAdoptLog`, which requires every index up to
`commitIndex[n]` to match what the receiver already has, and
`snapshotIndex[n] <= commitIndex[n]` holds throughout: `CreateSnapshot` snapshots
at `AppliedThrough(n)`, which `TypeOK` bounds by `commitIndex[n]`, and
`InstallSnapshot` raises `commitIndex` to the transfer index in the same step.
That bound is now a `TypeOK` conjunct, so the argument is machine-checked rather
than asserted.

The exception is deliberate and stayed materialized: a snapshot in flight
carries `snapshotTransfer.prefix`, a copy frozen at send time. The sender's log
may change before the receiver installs, so that field is a value and not a
view, and it remains a stored field of the transfer record.

**This one does not reduce the state count, and is not claimed to.** The removed
variable was a function of two others, so no two reachable states ever differed
in it alone; the distinct-state counts below are identical across it, which is
the point rather than a disappointment — a component that cannot distinguish two
states was not carrying verification. Peak RSS was also unchanged within noise
(2.90–2.97 GiB across four runs of the `SnapshotSpec` model at `-Xmx8g`, with
the two variants' wall times interleaving). What it buys is an obligation
retired: three of the four conjuncts `SnapshotIdentitySoundFor` used to check
are now definitional, and the way to violate them — writing the wrong prefix —
no longer exists.

#### Measurements

Same host and flags as above: 14 cores, `-workers 4 -Xmx8g -seed 2026081101
-fp 0 -fpmem 0.45`. Every run below exhausted on both sides. "Old" is the spec
before both reductions; "new" is after both. Reported search depth is not
deterministic under `-workers 4` — the `MembershipSpec` row read 23 and 24 on
different runs of the *same* spec — so only the state counts are load-bearing.

| Model | Old generated | Old distinct | New generated | New distinct | Distinct change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 voters, full `Spec`, `MaxTerm=1`, `MaxLogLen=1` | 137,480 | 35,363 | 127,112 | 24,995 | −29.3% |
| 2 voters, `SnapshotSpec`, `MaxLogLen=2` | 1,407,838 | 305,787 | 1,312,094 | 210,043 | −31.3% |
| 2 voters, `SnapshotSpec`, `MaxLogLen=1` | 13,966 | 3,963 | 12,814 | 2,811 | −29.1% |
| 2 voters, `CoreSpec` | 387 | 124 | 387 | 124 | none |
| 3 voters, `CoreSpec` | 357,423 | 52,247 | 357,423 | 52,247 | none |
| 2 voters, `ReadSpec` | 1,405 | 336 | 1,405 | 336 | none |
| 3 voters, `ReadSpec` | 1,213,423 | 154,409 | 1,213,423 | 154,409 | none |
| 2 voters, `MembershipSpec`, `MaxLogLen=2` | 7,215 | 1,708 | 7,215 | 1,708 | none |

The four unchanged relations exclude the snapshot lifecycle entirely, so a
reduction to snapshot machinery must leave them alone to the state. It does, on
models up to 1.2 million generated states. That identity is the evidence the
reductions touched only what they claim.

Two smaller fixtures move by construction rather than by measurement.
`RaftTraceSample` is unchanged at 5 generated and 4 distinct, and the negative
detector still exits 12 naming `ElectionSafety` at the same 6 and 4. The
detector's snapshot lifecycle fixture goes from 7 distinct states at depth 7 to
6 at depth 6 — exactly the one intermediate state the fold removes, which is the
fold's claim stated as a number. `RaftMembershipTraceSample` drops from 46
states to 45 because it executed `CompactSnapshot` as a step and that step no
longer exists; it still executes every transition the contract requires, and the
producer's trace floors moved from 46 to 45 with it.

#### How an obligation is wired

The TLA+ runner contract is one primary configuration plus a list of proof
obligations. Each profile's `tla` runner in
`verification/raft-invariant-profiles.json` carries an `obligations` array whose
entries pin an `id`, a `config` under `specs/tla/raft/`, a `completion` (only
`frontier-exhausted` is legal), per-obligation `minimum_generated_states` and
`minimum_distinct_states` floors, a whole-minute `soft_timeout`, and a `seed`.
Workers, heap, and fingerprint memory are inherited from the parent
configuration; an obligation is a different model on the same machine, not a
different machine budget.

Three rules make the vocabulary mean something.

On the scheduled tiers the obligations are not an addition to the gate, they
**are** the gate: the primary continuation there is reporting-only, so a green
nightly or weekly lane rests on these exhausted frontiers and on qualification.
The contract enforces that by refusing a reporting profile with an empty
obligation list.

**Obligations run before the primary configuration**, after the trace and
detector qualification probes. A broken theorem is then red in minutes rather
than after a five-hour continuation, and the primary run inherits whatever
remains of the shared execution window. The contract refuses an obligation set
whose budgets plus the primary `soft_timeout` exceed `total_timeout` less
`finalization_reserve`, so the ordering cannot silently truncate the monolith.

**Obligations never checkpoint.** Each runs from scratch into an ephemeral
state directory, writes no checkpoint, recovers none, and contributes to no
cache key. An obligation that cannot exhaust its frontier in one bounded run is
not an obligation but a second monolith, and belongs in the primary
configuration's continuation instead. Keeping obligations out of the serialized
`configuration` map is what enforces this: the checkpoint contract digests only
that map, so adding, retuning, or removing an obligation cannot invalidate the
primary configuration's accumulated TLC state.

**An obligation passes only on frontier exhaustion** with zero invariant
violations and both floors met. Any other outcome fails the layer red. The
floors are per-obligation ratchets calibrated against the obligation's own
measured state space; they are unrelated to the primary configuration's
120M/16M monolith floors. Each obligation binds the same nine invariants as the
primary configuration and is held to the same safety-only boundary, so a
configuration that checks nothing cannot discharge by exiting cleanly.

The negative-detector qualification stays bound once per layer, to the primary
configuration. Obligations strengthen the layer; they do not add registry
evidence rows, and a refutation inside one is reported as a red harness-level
failure for a human to read rather than attached to a predicate the primary run
never falsified.

#### Calibrated obligations

Calibration runs use the pinned jar and the wired flags (`-workers 4 -Xmx8g
-seed 2026081101 -fp 0 -fpmem 0.45`, symmetric) on the same 14-core host as
every table above. State counts are machine-independent; wall times are not,
and the host ran an unrelated toolchain build during several of these runs, so
read every wall figure as an upper bound.

What exhausts, at which bounds:

| Obligation config | Bounds | Generated | Distinct | Wall | Exhausted |
| --- | --- | ---: | ---: | ---: | --- |
| `RaftCoreObligation.cfg` | 2v, V2, T2, L2, R1 | 113,201 | 20,282 | 15 s | yes |
| `RaftReadObligation.cfg` | 2v, V2, T2, L2, R1 | 592,279 | 98,948 | 30 s | yes |
| `RaftCoreObligationDeep.cfg` | 2v, V2, T3, L3, R2 | 14,734,799 | 2,004,053 | 8.5 min | yes |
| `RaftSnapshotObligation.cfg` (folded spec) | 2v, V2, T2, L2, R1 | 14,119,884 | 2,002,205 | 6.7 min | yes |
| `RaftCoreObligationUnsymmetrized.cfg` | 2v, V2, T2, L2, R1, no symmetry | 452,327 | 80,977 | 15 s | yes |
| `RaftReadObligationUnsymmetrized.cfg` | 2v, V2, T2, L2, R1, no symmetry | 2,368,355 | 395,573 | 60 s | yes |
| `RaftIntegrationUnsymmetrized.cfg` | full `Spec`, 2v, V1, T1, L1, R1, no symmetry | 254,211 | 49,985 | 15 s | yes |
| `RaftSnapshotObligationUnsymmetrized.cfg` | 2v, V2, T2, L2, R1, no symmetry | 56,476,413 | 8,008,105 | 17 min | yes |

The unsymmetrized family doubles as a standing symmetry audit, and the audit
is a pair of numbers per config, not a round ratio. Each measured distinct
count falls slightly short of the group order times its symmetric sibling's
count — 80,977 against 4 x 20,282 = 81,128; 395,573 against 395,792; 49,985
against 2 x 24,995 = 49,990; 8,008,105 against 4 x 2,002,205 = 8,008,820 —
deficits of 151, 219, 5, and 715 states. The deficit is structural: a state
fixed by some permutation has an orbit smaller than the full group, so the
unquotiented count is the group order times the quotient count minus exactly
the symmetric states' missing orbit mass. A future change in either number of
any pair is evidence about symmetry soundness, which is what the weekly tier
exists to supply.

What does not, and how it fails to:

| Model | Bounds | Generated | Distinct | Queue at kill | Verdict |
| --- | --- | ---: | ---: | ---: | --- |
| `CoreSpec`, 3 voters | T3, L3 | 12,975,580 | 4,210,497 | 2,699,386 ↑ | diverges, fanout ≈ 2.9 |
| `CoreSpec`, 3 voters | T2, L2 | 24,590,487 | 5,443,386 | 2,557,643 ↑ | diverges, depth pinned at 21 |
| `MembershipSpec`, 3 voters | T3, L3 | 52,410,403 | 9,935,434 | 3,959,098 ↑ | diverges, depth pinned at 24 |
| `MembershipSpec`, 3 voters (folded spec) | T2, L2 | 46,023,230 | 7,526,805 | 2,416,633 ↑ | diverges, depth pinned at 26 |
| `SnapshotSpec`, 2 voters (pre-fold spec) | T2, L3 | 50,137,394 | 13,356,326 | 4,992,419 ↑ | diverges, depth pinned at 26 |

The pattern in the second table is one wall seen five ways: at three voters
the set-valued `messages` variable dominates every relation that contains the
core send/deliver actions, and at `MaxLogLen 3` the snapshot lifecycle
recreated the same explosion at two. Dropping per-node bounds does not move
the three-voter wall — core and membership both diverge at `MaxTerm 2 /
MaxLogLen 2`, membership even on the folded spec — so the node count binds,
not the bounds, and `ReadNext ⊇ CoreNext` settles read at three voters by
containment. Every candidate membership bound has now been measured and every
one diverges; its header carries the `DO NOT WIRE` marker and the conclusion
that exhausting it is a message-dimension redesign, not a bounds hunt.

The snapshot row moved between the tables, and that movement is the fold's
payoff stated as a verdict rather than a percentage: the pre-fold spec
diverged at two voters and `MaxLogLen 3`, and the folded spec at `MaxLogLen 2`
drains in under seven minutes, which turned the snapshot lifecycle — atomic
create-and-compact, transfer, install, restart, application-state loss — into
a wired exhaustive theorem.

The wired manifest, after calibration:

| Profile | Obligation | Config | Floors (generated / distinct) | Budget |
| --- | --- | --- | --- | --- |
| pr | `core-replication` | `RaftCoreObligation.cfg` | 113,201 / 20,282 | 4m |
| pr | `read-fencing` | `RaftReadObligation.cfg` | 592,279 / 98,948 | 6m |
| nightly, weekly | `core-replication-deep` | `RaftCoreObligationDeep.cfg` | 14,734,799 / 2,004,053 | 25m |
| nightly, weekly | `read-fencing` | `RaftReadObligation.cfg` | 592,279 / 98,948 | 6m |
| nightly, weekly | `snapshot-lifecycle` | `RaftSnapshotObligation.cfg` | 14,119,884 / 2,002,205 | 25m |
| weekly | `core-replication-unsymmetrized` | `RaftCoreObligationUnsymmetrized.cfg` | 452,327 / 80,977 | 4m |
| weekly | `integration-unsymmetrized` | `RaftIntegrationUnsymmetrized.cfg` | 254,211 / 49,985 | 4m |
| weekly | `read-fencing-unsymmetrized` | `RaftReadObligationUnsymmetrized.cfg` | 2,368,355 / 395,573 | 6m |
| weekly | `snapshot-lifecycle-unsymmetrized` | `RaftSnapshotObligationUnsymmetrized.cfg` | 56,476,413 / 8,008,105 | 45m |

Budgets are set against measured CI wall time, not local wall time. The first
nightly dispatch put CI runners at almost exactly twice the local calibration
wall on the multi-minute models — the deep core obligation ran 16m26s against
8.5 local minutes, the snapshot obligation 13m24s against 6.7 — and the
snapshot obligation's original 12-minute budget was killed eighty seconds
short of a drained queue, which is why every multi-minute budget now carries
roughly 2x-of-CI-projection headroom. Heap is part of the calibration
conditions too: the first weekly dispatch ran the 8M-distinct unsymmetrized
snapshot obligation on the tier's old 4g heap — half the heap every
calibration ran at — and could not drain it inside any budget, so weekly now
runs the same 8g as nightly. The same dispatch discharged its other six
obligations with distinct counts matching local calibration to the state,
which is the cross-machine determinism the exact floors rely on, observed in
production.

Weekly affords its unsymmetrized family by trading continuation time for it:
its reporting primary runs 190 minutes against nightly's 250, and the
recovered hour funds the symmetry audit applied to every theorem that
actually gates — all four gating obligation families now run both quotiented
and unquotiented on the weekly tier, 115 obligation minutes inside the
120-minute budget the trade opened.

Floors are the exact measured counts: TLC's breadth-first counts are
deterministic for a fixed spec, config, and symmetry, so any deviation is a
spec change and should be re-calibrated deliberately, not absorbed. The
two-voter core and read counts were re-measured on the final spec, after the
reductions, and match the pre-reduction calibration to the state — the
reductions' identity guarantee observed end to end. The deep core floor was
measured before the reductions; `CoreSpec` contains no snapshot action, the
identity was verified exactly at four other core/read bound-sets, and the
re-verified two-voter runs above confirm it on the wired configs themselves.

The PR primary's floors are exact for the same reason. The post-reduction
`RaftCi.cfg` exhaustion completed at 255,177,640 generated and 36,058,645
distinct states with the queue drained, in 93 minutes at `-workers 4` on a
14-core host, and those counts replaced the round pre-reduction floors of
120,000,000 / 16,000,000 in the pinned contract. The search runs materially
past its old slack bar — the frontier deepened from 28 to 39 in the final
third — which is why a measured exhaustion, not an extrapolated one, is the
only admissible calibration source.

### Correspondence to the implementation

`Raft.tla` is a design model, not a refinement of rafter, and its header states
each place the two deliberately differ. Two are worth repeating here because
they change how a reader should read a green TLA+ tier.

**No no-op entry kind.** `EntrySet` is `Command \cup Configuration`. The
on-election no-op that rafter appends (`LogEntryKind::Noop`) is unmodelled, and
that is a deliberate abstraction rather than a missing safety case: the rule the
no-op exists to make reachable is Raft's current-term commit restriction, and
`Commit` enforces that rule directly by requiring
`LogicalEntry(n, i).term = currentTerm[n]`. No leader in the model can count
replicas of a prior-term entry, so the device that earns that right in the
implementation is not needed to state the property. The no-op is a progress
mechanism, and progress belongs to the simulator. The refinement consequence,
stated in the direction the code uses it: a rafter log carries one extra entry
per leader term that a model log does not, so index equality between the two is
never the refinement mapping.

**One monitor no tier asserts.** `frozenAppendAuthorityFailed` stays `FALSE`
under the spec as written, and no wired tier config names it — `TypeOK` only
gives it a type. The algebra is three definitions wide and needs no model: the
latch needs `senderPendingSelfRemoval` together with
`receiverWouldAccept /\ ~accepted`, and `accept` differs from
`receiverWouldAccept` only by `AppendSenderAuthorized(m)`, which holds whenever
`senderPendingSelfRemoval` holds.

That resting state is the monitor's specification, not a defect. The latch is
unsatisfiable for exactly as long as `DeliverAppend` judges sender authority by
the membership frozen into the message at send time, and it exists to fail if
that ever changes. `RafterInvariantDetectorNegative` does assert it, through
`FrozenAppendAuthorityInvariant`, and
`delayed_append_uses_frozen_sender_authority_after_self_removal` in
`crates/rafter-invariants` runs that fixture twice: the unmutated spec passes,
and a mutation that re-derives sender authority from the sender's live
membership exits 12 naming that invariant. It is the only predicate in the
fixture that catches the mutation — with the `~frozenAppendAuthorityFailed`
conjunct removed, `TypeOK`, `ElectionSafety`, `LogMatching`,
`LeaderCompleteness` and `CommittedPrefixStability` all pass on the mutated
spec. So the reading a green tier supports is narrow but real: the wired tiers
do not check this property, and the mutation suite does.

## State Counts

Each exhaustive check reports two distinct cardinalities:

- **Protocol states** hash the simulated protocol and scheduler state while
  excluding retained verifier history. Scheduler counters remain included, so
  this is the model state used by the explorer, not a count of abstract Raft
  paper states.
- **Verifier states** hash the complete exploration state, including retained
  evidence needed to detect temporal violations. This is the deduplication and
  unique-state-budget key.

Profile totals add each check's independently explored cardinality. They are
not a globally deduplicated union. The scheduled `raft-nightly` and
`raft-weekly` gates enforce reviewed lower bounds on both totals: 13 million
and 250 million states respectively. The floors are coverage ratchets; they do
not control the configured exploration depth or workloads.

## Retained Logical Prefixes

The verifier retains logical-prefix witnesses across transitions so log
matching, leader append-only, committed-prefix stability, and leader
completeness remain temporal properties. A valid observed logical log is
resolved into an immutable persistent spine. Each unique extension adds one
node; shorter witnesses reuse ancestors, and each retained witness clone copies
only a constant-size handle. Cloning the full verifier state still copies its
maps and logical views. Snapshot creation, transfer,
installation, compaction, and restart preserve the visible logical-prefix
identity.

The implementation keeps those responsibilities explicit under
`state/logical_log`: `observation.rs` owns canonical reconciliation,
`snapshot.rs` owns transfer provenance, and `types/{prefix,view,violation}.rs`
separate persistent storage from protocol views and detector output.

Sharing is an allocation detail, never an identity shortcut. Equality,
ordering decisions, debug output, and the verifier-state `Hash` stream use the
exact visible entries through the witness boundary, not pointer identity,
backing suffixes, an insertion-order handle, or a probabilistic digest. A
malformed boundary remains distinguishable from valid evidence and cannot
qualify a snapshot witness. Focused tests assert shared backing across every
prefix of an observed log and across state clones, equal exact hashes for
equal visible prefixes on different allocations, and unchanged negative
detector behavior for divergent and malformed prefixes.

This changes retained prefix storage under sequential log growth from a
triangular number of copied entries to one node per unique extension. Exact
canonical state hashing still scans the complete visible witness structure;
the source-bound cost comparison below measures its runtime and peak-RSS effect
rather than treating structural sharing as proof of an end-to-end performance
result.

## Cost Evidence

Run a source-bound comparison with:

```sh
MODEL_CHECK_BASE_REF=main \
MODEL_CHECK_PROFILES=fast \
MODEL_CHECK_RUNS=6 \
scripts/model-check-profile-compare
```

The harness builds measured commits in release mode with `--locked`, then
alternates base/current execution order across six paired runs, balancing each
revision in each process-order position. Multiple profiles also run in
alternating order. It consumes structured `RAFTER_EVENT` records, requires
independent protocol and verifier counts, requires every exhaustive check to
pass with an exhausted frontier, and rejects shape drift between repeated
samples of one revision. Human-readable summary lines and legacy compatibility
counts are never accepted as calibration data. Profiles used for cost
comparison must contain at least one exhaustive check; the soak-only profile
remains liveness evidence rather than state-space cost evidence.

Comparisons that do not cross a reviewed profile-contract change remain strict
like-for-like runs: profile headers, check IDs, and configured depths must be
identical. A mismatch without a matching source-controlled migration is a
harness error and therefore red.

### Pinned Contract Migrations

`verification/model-check-contract-migrations.json` is the only migration
input. It pins the migration commit and its sole parent, the exact changed-path
set, canonical old/new contract digests for every affected profile, and every
configured-depth increase. The planner verifies those identities against Git
and requires the requested baseline to be an ancestor of the current commit.
There is no runtime flag that permits arbitrary bound drift.

When a comparison crosses a pinned migration, the harness emits three evidence
segments:

1. requested baseline to the pivot parent, under the old contract;
2. pivot to current `HEAD`, under the new contract; and
3. pivot parent to pivot, as a two-run contract and coverage delta.

The unchanged 2.25x wall and 1.75x peak-RSS ceilings apply independently to
both non-empty like-for-like segments. The migration delta is not a performance
comparison. It must reproduce both pinned contract digests, preserve profile
semantics and the exact check set, and match only the reviewed monotone depth
increases. Every increased bound must reach a deeper frontier with nondecreasing
protocol-state, verifier-state, and explored-action counts. A segment whose
endpoints are the same commit is explicitly marked not required. Missing,
malformed, failed, or source-mismatched segment evidence makes the aggregate red.

Schema-v3 `compare.json` preserves source trees, lockfile and binary digests,
toolchain and host metadata, every raw sample, additive state totals, wall time,
peak RSS, the validated migration identity when applicable, complete segment
reports, and per-check coverage deltas. `compare.md` summarizes the aggregate.
CI uploads the report, raw events, timing logs, build logs, and a SHA-256
manifest even when validation or report construction fails. Main pushes use the
pre-push commit as baseline; scheduled runs use `HEAD^`; manual runs require an
explicit baseline input.

The first comparison against a revision that predates structured events uses
the source-recorded evidence-format baseline `9770d1a` and records both the
requested and effective baseline. It never parses legacy human output as
equivalent evidence. Every like-for-like segment requires an unchanged
protocol-state shape plus paired median current/base ceilings of 2.25x wall
time and 1.75x peak RSS. Verifier-state growth is reported separately and is
expected when sound history is added; protocol-state drift or a cost ceiling
breach fails the job after the JSON and Markdown reports have been written.

The default requires a clean checkout so a commit names the measured source.
`MODEL_CHECK_ALLOW_DIRTY=1` exists only for directional local experiments; such
a run records `clean: false` and is not release or threshold evidence.

## Producer Provenance Threat Model

Invariant producers run on a trusted CI host. Before `run` or `run-all` executes
evidence checks, the CLI publishes its bytes as a regular, non-symlink,
read-only executable at
`target/rafter-invariants/producer-images/<sha256>/rafter-invariants` and
re-executes that image. Schema-v14 receipts bind the exact path, digest, and
preserved executable artifact. This prevents nested Cargo builds, stale target
paths, partial publication, symlinked artifact paths, and later deletion of the
bootstrap executable from changing which producer image the aggregate accepts.

Schema-v14 source receipts also carry a `git-head-worktree-raw-v1`
materialization. The producer enumerates the immutable `HEAD` tree with Git
replacement objects disabled, rejects tracked symlinks, and reads each tracked
regular file as raw bytes. It checks every Git blob ID and the exact owner
executable bit, then SHA-256 binds the ordered mode, path, and content inventory.
This catches index flags such as
`assume-unchanged` and `skip-worktree` that can make porcelain status appear
clean after bytes or modes change. Ignored paths are permitted only in reviewed
generated-output roots (including the invariant harness's own nested Cargo
target), and ignored symlinks fail closed. Rust input validation starts from
each exact resolved workspace and path-package Cargo target root, treats those
roots as Rust regardless of filename extension, and follows only actual tracked
module, `include!`, and literal `#[path]` edges transitively with the same rule;
unreferenced source files do not create inputs. Raw include and path identifiers
are normalized, and direct, qualified, transitively included, and multi-hop use
aliases are resolved to a fixed point before validation. Macro-generated,
dynamically selected, or target-conditional compiler inputs fail closed.
Workspace and path-package build scripts are prohibited. Registry
build scripts are admitted only from
the full locked metadata graph when their crate archive has a Cargo.lock
checksum; the lockfile binds their source archive and the preserved producer
executable digest binds their compiled effects. Gitlinks, noncanonical paths,
filesystem aliases, and platform materializations that cannot preserve the
reviewed raw-byte and mode semantics fail closed; symlinks and submodules are
not part of the contract.

Source and process environments are deliberately distinct. The source receipt
binds only platform compiler-selection inputs (`DEVELOPER_DIR`, `SDKROOT`, and
`SYSTEMROOT` when present). The execution invocation binds the complete safe
base environment, including isolated cache and workspace paths, and every child
process log must match that base plus its reviewed command-specific additions.
Changing a compiler-selection input invalidates source identity; moving the same
authenticated source into a fresh aggregate Cargo home does not.

The registry checksum is a source-identity proof, not a hermetic-build proof.
A registry build script can observe host files, clocks, kernel behavior, or
other runner state that Cargo.lock does not describe. Effects that reach the
producer executable are nevertheless frozen by the independently preserved
executable digest, and aggregation executes that exact artifact rather than
rebuilding it. Effects that depend on external state without being reflected in
the executable, malicious build scripts, and compromised build runners remain
outside this portable contract. Proving the stronger source-to-binary claim
would require a hermetic build sandbox and external build attestation; the
invariant report does not claim that property.

This is deterministic repository provenance, not hostile-host attestation. It
does not defend against a malicious producer binary, compromised kernel or CI
runner, SHA-256 compromise, or a hostile same-UID process that can replace files
between verification and `exec`. Those threats require an external build
attestation system or OS-specific sealed execution and are outside the
repository provenance contract. Production evidence execution is narrower:
descriptor-bound target launch is Linux-only and fails closed elsewhere. The
macOS lane exercises launcher mechanics under test-only fallback; it does not
produce accepted invariant evidence.

Source capture also fails closed on Cargo configuration that can alter the
compiled dependency graph without identifying the replacement source. In
particular, every top-level `[patch]` configuration is forbidden, including a
patch whose path currently points inside the checkout. The receipt binds the
configuration bytes and path string, but it does not recursively bind an
arbitrary replacement source tree. Reviewed overrides therefore belong in the
tracked workspace manifests and lockfile, not in ancestor or Cargo-home
configuration.

Receipt `duration_ms` and `peak_rss_kib` fields are execution metrics derived
only from the hashed child-process logs attached to that receipt. They measure
the compiled tests, simulator/model checker, TLC, or Maelstrom process groups;
they do not claim to measure parent-producer planning, source capture, artifact
hashing, or report rendering. Model-check performance comparisons use the
simulator process group's wall time and peak RSS together with separately
reported protocol-state and verifier-state counts.

Simulator detector fixtures have two independent execution checks. During the
producer run, the parent creates a fresh challenge on a connected anonymous Unix
socket pair and passes only the child endpoint through an inherited descriptor.
The trusted detector wrapper requests and retains that challenge, then shuts
down and closes the descriptor before fixture code runs; no pathname, listener,
reconnectable endpoint, or proof capability remains available to fixture
helpers. The wrapper emits challenge-bound proofs only after an invocation-bound
rejecting witness exists. The final verifier requires the ordinary witness
inventory and the challenge-bound proof inventory to match exactly, so an early
return still cannot qualify. The proof channel is covered by the same
trusted-host boundary described above.

The aggregate independently qualifies every direct simulator detector fixture.
Its source analyzer resolves local calls by exact crate-module identity across
the tracked Cargo target graph and recursively inspects every plausible reachable
helper. Untracked, symlinked, out-of-tree, or item-macro-generated source outside
the bound test context fails closed. The analyzer binds `test`, the host target,
and disabled package features to the exact host-targeted
`--no-default-features` detector compile contract; custom and profile-sensitive
`cfg` predicates without an execution binding remain red. Profile schema v9
requires exactly 256 locked registry packages, 77 unique fixtures, 79
invariant/evidence bindings, and two test targets, and binds their complete
identities, transitive target source graphs, and associations to one reviewed
digest. The verifier snapshots the
clean checkout, authenticates registry archives against `Cargo.lock`,
reconstructs a read-only Cargo directory source, and compiles only the reviewed
targets under a private Cargo home with
`--locked --offline --no-default-features`. Strict Cargo JSON admission rejects
ambient workspace roots, source escapes, unknown sources, cached executable
claims, duplicate completion records, metadata target substitution, executable
byte drift, and inventory drift. Profile-owned aggregate byte and entry limits
bound registry extraction, and the replay deadline starts before preparation.

Every replay emits a strict schema-v4 machine-readable report plus exact
length-framed v2 stdout and stderr artifacts. The report retains the
authenticated commit, tree, materialization, environment, actual Cargo and rustc executable
paths and digests, reviewed inventory digest, every fixture source binding,
unique execution identities, and the detector token and pre-body challenge.
Those content-addressed files are published into a
never-reused invocation directory, remain under held filesystem identities,
lose write permission at sealing, and are rehashed against an exact complete
tree inventory before verdict reduction and after report publication. Replay
work stops before the outer deadline to retain a verifier-owned publication
reserve. CI seals the exact set into a deterministic read-only tar, downloads it
to a fresh path, and repeats digest, canonical metadata, schema, and semantic
validation without extracting it. Sealing and readback compare the report with
a separately loaded canonical profile manifest, captured checkout, actual Rust
toolchain, and independently materialized registry receipt; reconstruct the
fixture plan and inventory digest; require an exact bijection between
execution-bound process logs and content-addressed archive members; and rerun
detector transcript qualification over the archived bytes.

Hosted CI jobs name explicit Ubuntu and macOS releases, and TLA+ jobs select the
exact reviewed Temurin build. Scheduled invariant jobs use fresh run-, job-, and
attempt-specific Cargo homes and target directories, restore neither compiled
targets nor Cargo binaries, and reject stale Maelstrom extraction roots. Every
external Action is locked to one reviewed full commit, and every aggregate step
has a timeout inside a mechanically checked job budget. Nightly and weekly
invariant jobs run on fresh `ubuntu-24.04` GitHub-hosted VMs. Their TLA+ layer
uses a 320-minute internal deadline inside a 330-minute step and six-hour job,
leaving 30 minutes for setup, exact-compatible checkpoint handling, and evidence
upload. The Maelstrom setup installs Graphviz and gnuplot into the ephemeral VM,
then preflights both tools before execution.
A fixture failure or incomplete qualification turns only its bound evidence
into a harness error; an existing invariant violation remains an invariant
violation. Missing or mismatched replay coverage fails closed for every required
binding. PR, nightly, and weekly aggregate jobs prefetch into isolated
run-specific Cargo homes, then perform this compilation and replay fully
offline on descriptor-bound Linux executables.
