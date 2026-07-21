# Invariant tooling architecture

Rafter's invariant tooling turns a reviewed verification contract into
source-bound evidence and exactly one deterministic verdict per invariant. The
tooling is security-sensitive build and verification code: its structure should
make trust boundaries, ownership, and fail-closed decisions obvious to a reader.

This document covers the three workspace crates introduced for that contract:

- `rafter-invariant-test-macros` defines the trusted detector-test attribute;
- `rafter-invariant-test` defines typed oracle markers and detector proof
  lifecycle;
- `rafter-invariants` loads the contract, executes evidence producers,
  independently verifies their artifacts, and emits the 44-row report.

The design follows the protocol core's architectural style without copying its
sans-I/O boundary. Invariant tooling legitimately owns files, processes,
parsers, schemas, and command-line orchestration.

This cleanup uses ordinary Rust modules and subfolders, not Git submodules or a
new layer of workspace crates. The existing three-crate split is sound: syntax
expansion, detector runtime, and deterministic aggregation have different
dependency and trust boundaries.

## Design goals

1. A reader can follow the system from contract to plan, evidence, verification,
   verdict, and report without first learning implementation filenames.
2. Every load-bearing decision has one named owning module.
3. Producers and verifiers are visibly separate. They may share serialized
   vocabulary and neutral syntax decoders, but never pass/fail reducers.
4. Crate roots and domain facades are maps, not implementation containers.
5. Tests mirror stable domains and read as evidence or adversarial stories.
6. Refactors preserve CLI, public API, schema, artifact, and fail-closed
   behavior unless a milestone explicitly changes a reviewed contract.
7. Architecture tests prevent the flat layout and broad dependency edges from
   returning.

## Original pressure baseline

The protocol core demonstrates the desired presentation shape: 169 Rust source
files, only eight over its 300-line implementation target, and none over 500
lines. At the start of this cleanup, the new invariant tooling had 147 Rust
files, 82 over 300 lines, 42 over 500 lines, and 20 over 800 lines. Only a small
fraction of its production modules began with an ownership contract. These
numbers are the ratchet baseline, not a claim about the current tree.

The problem was not line count by itself. Several baseline files mixed
independent vocabularies or lifecycle phases:

- `types.rs` owns plan, execution, evidence, and verdict models;
- `catalog.rs` owns registry descriptors, profile models, simulator contracts,
  and liveness policy;
- `producer/` owns shared filesystem, process, source, and target mechanics as
  well as four evidence families;
- artifact verifiers import producer internals, obscuring the independence
  boundary;
- receipt validation is spread across flat `receipt_*.rs` files;
- large root-level test files do not mirror the domains they exercise;
- `main.rs` owns command vocabulary and command execution.

Splitting files is useful only when the resulting modules name these ownership
boundaries.

## Architectural vocabulary

Use these terms consistently:

- **registry**: the reviewed authoring document containing invariant, clause,
  and evidence declarations;
- **catalog**: the normalized executable view derived from the registry;
- **profile**: the selected evidence layers, runner bounds, and coverage policy;
- **plan**: the immutable, source-bound contract selected for one invocation;
- **producer**: code that executes one evidence layer and writes a receipt;
- **evidence**: immutable serialized observations, artifacts, and results;
- **receipt**: the structured account of one plan or producer execution;
- **provenance**: identities that bind source, executable, target, tool, and
  artifact bytes;
- **verification**: independent reconstruction and validation of a receipt and
  its artifacts;
- **verdict**: the fail-closed reduction of verified evidence for one clause or
  invariant;
- **gate**: orchestration that requires the selected profile's complete set of
  verdicts;
- **report**: a deterministic rendering of verdicts, never a second verdict
  engine.

Avoid generic homes such as `utils`, repository-wide `support`, or `common`.
Shared mechanics belong to the narrowest named domain that owns their contract.

## Dependency direction

The internal dependency graph is intentionally one-way:

```text
contract     -> evidence, plan, producer, verification, verdict, gate, cli
evidence     -> plan, producer, verification, verdict, gate
execution    -> provenance, producer, verification
provenance   -> plan, producer, verification
plan         -> producer, gate
producer     -> gate
verification -> verdict, gate
verdict      -> gate
gate         -> cli
```

Each arrow runs from dependency to consumer. The reviewed target graph is the
import rule for a source tree once that tree enters the migrated-source
manifest. That manifest is executable and deliberately separate from the
target graph: an unmigrated `plan`, `gate`, or `cli` path is not presented as
already conforming. In particular, `execution` feeds the neutral provenance
observer, producer, and verifier-owned replay, not `plan`; verification
consumes contract, evidence, neutral execution, and provenance without
importing producer implementation.

- `contract` owns reviewed declarations and profile policy. It does not execute
  processes or inspect produced artifacts.
- `evidence` owns serialized vocabulary. It may embed the exact reviewed
  profile contract selected for a run, so it depends on `contract`; it does not
  decide whether its own values are trustworthy.
- `provenance` owns reusable identity derivation and byte-level checks. It may
  use neutral execution mechanics to bound source and toolchain observation.
- `execution` owns confined filesystem and managed-process mechanics. It does
  not know invariant IDs or verdict policy.
- `plan` resolves a contract and binds invocation inputs.
- `producer` may depend on the lower domains, but never on `verification` or
  `verdict`.
- `verification` may depend on contract, evidence, provenance, and neutral
  execution mechanics, but never on producer implementation. Neutral format
  parsers may be shared through the evidence domain; semantic acceptance logic
  may not.
- `verification` returns a typed `EvidenceIntake`; raw bundles cannot cross that
  boundary by convention alone.
- `verdict` consumes `EvidenceIntake`. It does not spawn tools, discover tests,
  or read unverified runner files directly.
- `gate` orchestrates the lifecycle. It does not duplicate producer,
  verification, or verdict policy.
- `cli` parses user intent and delegates to the gate.

Architecture tests should reject forbidden imports across these boundaries.

Profile policy is domain vocabulary, not open text. Evidence selection, clause
selection, required clause strength, evidence layers, and evidence strengths
are exhaustive Rust enums whose serde names preserve the reviewed JSON wire
contract. Unknown values therefore fail while the profile is decoded; later
validation checks only cross-field and profile-specific relationships.

## Target crate layouts

The trees below name the intended homes. Leaf names may be refined during an
extraction when the code reveals a more precise concept, but ownership and
dependency direction should remain stable.

### `rafter-invariant-test-macros`

```text
src/
  lib.rs                 proc-macro entry point only
  detector_test.rs       parse, validate, and expand `#[detector_test]`
  detector_test/tests.rs parser and expansion contracts
```

The attribute's accepted signature and generated hidden calls are its public
contract. Keep the entry point tiny and preserve the exact expansion protocol.
Compiler-facing cases live with `rafter-invariant-test`, where they exercise
the proc macro against its real runtime ABI; parser validation also has a
focused unit matrix in this crate. This split is about a declarative crate root
and explicit compile-time contracts, not chasing file count.

### `rafter-invariant-test`

```text
src/
  lib.rs                 flat public macro and outcome facade
  detector/
    mod.rs               detector lifecycle vocabulary facade
    session.rs           thread-local session ownership
    witness.rs           invocation-bound witness inventory
    proof.rs             parent challenge channel and encoding
    wire.rs              versioned marker and challenge wire vocabulary
    outcome.rs           libtest `Termination` contract
  oracle/
    mod.rs               hidden oracle support facade
    call.rs              compiler-resolved function invocation adapters
    marker.rs            observed and violation markers
    macros.rs            exported assertion and recorder macros
  tests.rs               stable externally named subprocess fixtures
  tests/
    support.rs           forged transcript and subprocess helpers
tests/
  api.rs                 exported assertion and recorder behavior
  lifecycle.rs           ordinary and gate-bound lifecycle stories
  ui/                    compile-time macro and invocation contracts
```

All existing exported macro names, hidden helper names, marker strings,
environment names, and `DetectorTestOutcome` behavior remain compatible.
Hidden does not mean unimportant: the producer and macro expansion consume
these symbols as an internal ABI.

The protected source analyzer binds the exported oracle definitions exactly to
`oracle/macros.rs`, the invocation-adapter expansions exactly to
`oracle/call.rs`, and the thread-local session declaration exactly to
`detector/session.rs`. Copies in the crate root or another module are rejected;
changing any of these homes requires changing the analyzer and its positive and
negative source-location contracts in the same commit.

Internally, model the lifecycle with named states rather than overlapping flags
and strings: standalone execution, proof-bound execution, and setup failure;
expected-rejection and recorder-invocation witness kinds; and an opaque detector
identity. Serialize those types back to the existing marker text only at the
wire boundary.

### `rafter-invariants`

```text
src/
  lib.rs                 flat library facade only
  main.rs                parse and delegate only
  cli/
    mod.rs               clap command vocabulary
    check.rs             check command adaptation
    run.rs               producer command adaptation
    document.rs          registry-document command adaptation
  contract/
    mod.rs               contract facade
    identity.rs          test and simulator identities
    registry/
      mod.rs             registry authoring model
      parse/             strict YAML parser by syntax domain
      render.rs          canonical Markdown rendering
    catalog/
      mod.rs             normalized executable descriptors
      resolve.rs         registry-to-catalog conversion
    profile/
      mod.rs             profile and runner vocabulary
      load.rs            strict profile-manifest loading
      model.rs           profile, runner, and simulator-check wire models
      replay.rs          typed aggregate replay policy and resource bounds
      validate.rs        cross-profile policy validation
      runner_contract/   typed per-layer configuration validation
      simulator/         simulator floors and check contracts
      liveness/          bounded-liveness obligations and execution policy
    schema/
      mod.rs             schema facade
      json.rs            generic checked-in JSON-schema validator
  evidence/
    mod.rs               serialized evidence facade
    artifact.rs          immutable artifact references
    bundle.rs            one layer's complete result bundle
    detector_proof.rs    neutral detector-proof framing and transcript decoding
    execution.rs         execution, invocation, and check receipts
    result.rs            evidence status and failure classification
    schema.rs            result-bundle shape validation
    receipt/
      mod.rs             receipt vocabulary facade
      source.rs          source and materialization receipts
      simulator.rs       simulator-specific bindings
      tool.rs            tool and producer-image receipts
    liveness/
      mod.rs             liveness evidence-binding facade
      binding.rs         typed contract/report binding
      digest.rs          canonical report and contract identities
    format/
      mod.rs             neutral wire-format facade
      libtest.rs         canonical libtest and oracle-marker decoding
      process/           versioned process-log vocabulary and decoding
  provenance/
    mod.rs               provenance facade
    source/
      mod.rs             checkout and Cargo-input identity
      materialization.rs immutable tracked-tree materialization
      rust/              Rust target input discovery
    target/
      mod.rs             Cargo target identity
      cfg.rs             bound cfg evaluation
      compiler.rs        protected compiler identity
    image.rs             immutable producer image publication
    artifact.rs          byte hashing and confined artifact capture
  execution/
    mod.rs               execution mechanics facade
    filesystem/          confined paths, traversal, cleanup, and publication
    process/             bounded process mechanics facade
      anchor.rs          releasable direct-child target-group anchor
      artifacts.rs       held process artifacts and replay retention
      diagnostics.rs     retained lifecycle and telemetry failures
      direct_child.rs    unreaped child and signal-identity capability
      environment.rs     minimal deterministic inherited environment
      finalization.rs    deadline- and size-bounded receipt finalization
      launch.rs          descriptor mapping and wrapper launch facade
      launch/
        program.rs       launcher protocol, programs, and target environment
      lease.rs           inherited process-lineage lifetime evidence
      process_group.rs   two-phase publication and target-group ownership
      internal_command.rs bounded observer execution and output draining
      internal_command/
        test_support.rs  deterministic observer fault and boundary controls
      internal_process.rs trusted direct-child observer ownership
      telemetry.rs       resource parsing and process-group observation
      managed.rs         measured-process topology facade
      managed/
        target.rs        placement transitions and quiescence proofs
        cleanup.rs       emergency cleanup and quarantine transfer
      model.rs           requests, observations, completions, and policies
      output.rs          bounded process collection orchestration
      reaper.rs          observation-only quarantine facade
      reaper/
        request.rs       child, leased-child, and anchored-group ownership
        worker.rs        no-signal polling and reaping
      reaping.rs         bounded wrapper and process-group reaping
      signal.rs          process-group probes and signal delivery
      termination.rs     timeout escalation and signal policy
  plan/
    mod.rs               `ExecutionPlan` facade
    model.rs             plan receipt and bound input vocabulary
    capture.rs           invocation and plan-input capture
    validate.rs          immutable plan validation
  producer/
    mod.rs               layer dispatch and atomic receipt publication
    process/             profile budgets, invocation binding, and evidence adaptation
      runtime.rs         descriptor-bound process-runtime inventory
      runtime/           fail-closed script-interpreter binding
    test_compile.rs       test-target compilation compatibility facade
    test_compile/
      target.rs           Cargo target identity and selector vocabulary
      scratch.rs          held compilation scratch directory
      compilation.rs      bounded Cargo invocation and artifact capture
      cargo_output.rs     strict compiler-message qualification
      executable.rs       workspace, package, and executable identity
      protected.rs        protected oracle compiler-artifact policy
      tests.rs            compiler-artifact identity scenarios
    test_exec.rs          exact libtest execution compatibility facade
    test_exec/
      discovery.rs        ordinary and ignored exact-test inventory
      execution.rs        bounded process and proof qualification
      evaluation.rs       compile-to-outcome orchestration
      outcome.rs          typed evidence reduction
      artifact_log.rs     incremental durable transcript publication
      detector_policy.rs  producer-owned detector transcript policy
      detector_proof.rs   challenge-channel process adaptation
      discovery/tests.rs  exact inventory scenarios
      execution/tests.rs  exact transcript classification scenarios
    simulator/           model execution, event binding, and detectors
    tla/                 command, contract, output, mutation, and checkpoint
    maelstrom/           tooling, scenarios, trials, EDN, and lease markers
  verification/
    mod.rs               `EvidenceIntake` and verification facade
    detector.rs          fixture-analysis lifecycle and public source binding
    detector/
      source.rs          declarative invocation-bound analysis facade
      source/analysis.rs exact fixture-to-detector orchestration
      source/contract.rs derived detector invocation contract
      source/function_collection.rs authenticated target function discovery
      source/function_body/ call, scope, flow, macro, and visitor analysis
      source/function_index/ function catalog and local resolver model
      source/reachability/ call-graph expansion and witness policy
      transcript.rs      independent detector transcript acceptance
      source_tests.rs    detector-source reachability scenarios
    detector_replay/
      plan.rs            reviewed fixture, target, and evidence inventory
      workspace.rs       isolated private Cargo home and build workspace
      metadata/          authenticated Cargo graph reconstruction
      compiler/          strict compiler-message and executable admission
      execution/         deadline-bound replay compilation orchestration
      fixture.rs         challenge-bound fixture execution and qualification
      process.rs         reviewed raw-process capability adapter
      artifact/          report coordination, strict semantics, exact logs, and integrity guard
      assessment.rs      evidence-local replay qualifications
      result.rs          compilation and fixture attempt vocabulary
    source/
      receipt.rs         authenticated checkout and actual toolchain receipts
      snapshot/          clean tracked-tree capture and integrity validation
      registry_snapshot/ locked archive admission and directory-source materialization
      sealed.rs          immutable file identity and permission ledger
      permissions.rs     portable reviewed mode policy
    intake/
      mod.rs             verified evidence intake facade
      paths.rs           aggregate and layer path verification
      replay.rs          aggregate-only replay orchestration and overlay
      model.rs           accepted evidence, defects, and retained artifact guard
    publication/         exact directory intake, deterministic tar sealing, and no-extract readback
    error.rs             fail-closed verification error vocabulary
    process_receipt.rs   process invocation and launcher-chain acceptance
    bundle/              common receipt, provenance, and integrity checks
    simulator/           event, schedule, provenance, and metrics checks
      liveness/          independent bounded-report semantic validation
    tla/                 invocation, tool pin, checkpoint, and mutation checks
    maelstrom/           history, scenario, durability, and lease checks
  artifact_verify/
    test_logs.rs         legacy physical facade, logically verification-owned
    test_logs/
      detector.rs        detector-log adaptation into verifier transcript policy
      environment.rs     deterministic exact-test environment reconstruction
      invocation.rs      discovery plan and executable provenance
      outcome.rs         independent verifier outcome acceptance
      policy.rs          fail-closed exact-execution policy
      registry.rs        registry-to-libtest identity binding
      runner.rs          receipt and transcript orchestration
      tests.rs           detector and outcome-policy adversarial scenarios
  verdict/
    mod.rs               verdict vocabulary facade
    model.rs             report, summary, clause, issue, and status types
    validate.rs          verdict shape and semantic validation
    aggregate.rs         clause and invariant reduction
    report/
      mod.rs             deterministic report facade
      markdown.rs        Markdown rendering
      junit.rs           JUnit rendering
  gate/
    mod.rs               public gate facade
    check.rs             consume existing evidence and report
    run.rs               execute one source-bound producer
    run_all.rs           execute all required producers then aggregate
```

The most important structural change is extracting `provenance` and
`execution` from `producer`. A verifier must not import `crate::producer::*` to
validate producer output. Shared wire decoding, including canonical libtest and
process transcripts, belongs under `evidence::format`; producer and verifier
policy remains separate.

`artifact_verify/test_logs.rs` and its child directory retain their physical
paths while test verification moves behind the reviewed domain vocabulary.
They are logically part of `verification`, and the migrated-source manifest
models both the facade file and child directory so neither can import producer
implementation. This is a compatibility mount, not a second verification
domain.

Detector proof handling has the same neutral-format boundary. The evidence
module decodes marker records without deciding whether a transcript is
acceptable. Producer acceptance lives in
`producer/test_exec/detector_policy.rs`; verifier acceptance lives in
`verification/detector/transcript.rs`. The two policies consume the same
decoded vocabulary but must independently check execution tokens, challenges,
witness inventories, and role-specific obligations. A shared
`verify_transcript` reducer is forbidden.

Verifier-owned detector replay is an aggregate-only second execution layer. It
does not trust a producer's claim that a negative fixture ran. The current
profile schema is v9. The top-level `verifiers` map deliberately separates
aggregate-owned execution policy from producer `runners`; every profile must
have exactly one contract in each map. This additive public model change is an
intentional schema migration, not an internal module-move side effect. The
reviewed replay boundary binds 247 locked registry packages, 77
unique detector fixtures, 79
invariant/evidence bindings, and two Cargo test targets to one canonical
inventory digest. The digest commits every target, fixture, detector, complete
repository-relative target source graph, expected witness, and evidence
association, so direct-file or count-preserving substitution is red.
Planning rejects any drift before execution. The verifier captures a
clean tracked source snapshot, authenticates every registry archive against
`Cargo.lock`, materializes a read-only directory source, and reconstructs the
Cargo graph under a private Cargo home. Aggregate archive, expanded-byte, and
entry limits are profile-owned, and the total replay deadline begins before
inventory analysis or registry materialization. Compilation is `--locked`,
`--offline`, and `--no-default-features`; strict Cargo JSON parsing matches
target name, kind, and source to authenticated metadata and admits only fresh
executables whose compiler-time bytes remain unchanged.

Each fixture then executes through the descriptor-only detector challenge
protocol. A passing subprocess is insufficient: source, executable, transcript,
challenge, and evidence identities must all qualify. Failures affect only the
bound evidence records unless the verifier itself loses structural integrity;
an existing invariant violation is never overwritten by a harness error.
Coverage mismatch fails every required replay binding and retains the report
and logs that explain the mismatch. Schema v9 binds the pre-body secret protocol:
the child consumes and closes its inherited proof descriptor before fixture
code runs, then emits a proof only after an invocation-bound rejection.
Reachable unsafe blocks, unsafe functions,
foreign functions, and foreign ABI functions are rejected because they could
inspect or divert the inherited proof capability outside the reviewed Rust
call graph.

Replay artifacts are verifier-owned, not producer receipts. Exact stdout and
stderr byte streams use a length-framed v2 envelope that binds the process
role, unique execution identity, stream, and arbitrary payload bytes. The
strict schema-v4 JSON report records the authenticated commit, tree,
materialization and environment,
the canonical paths and hashes of the actual Cargo and rustc executables, the
registry receipt, reviewed inventory digest, complete fixture source bindings,
compilation, typed completed or lifecycle-error process observations, and every
evidence result. Receipt digests make each source and toolchain field
load-bearing. Archive sealing and downloaded-archive readback independently
load the canonical profile manifest, capture the active checkout and actual
Cargo and rustc executables, and rematerialize the authenticated registry
receipt; a self-consistent stale or substituted report therefore remains red.
Readback reconstructs the logical replay plan from its fixture rows, recomputes
the canonical inventory digest, requires the
exact compilation roles, and cross-checks every referenced length-framed log
against one content-addressed archive member. It reruns detector transcript
qualification over the archived bytes and recorded token, challenge, and
witness contract. Reportless archives, omitted logs, reused logs, malformed
transcripts, and orphaned payloads are rejected. Capture and readback share a
512-file, 256 MiB exact archive budget, including the manifest, so a green
replay cannot cross a stricter sealing boundary. Publication is
content-addressed beneath a never-reused, invocation-specific
`target/rafter-invariants/verifier-evidence/<workflow-run>/run-<source>-<invocation>`
directory in CI, with a stable local default when no publication root is
selected. A content-addressed manifest covers every replay report and process
log. CI carries that manifest's digest through the aggregate step, verifies the
exact read-only inventory and every listed digest, seals the set into a
deterministic read-only tar with canonical headers, and uploads only that tar.
The aggregate then downloads the same artifact into a fresh path and validates
the captured archive digest, captured manifest digest, exact inventory, entry
metadata, trusted context, reconstructed inventory, report semantics,
process-log envelopes, and canonical report bytes without extracting the
archive.
The aggregate holds identity-bound file capabilities, hashes every artifact at
publication and sealing, requires the complete directory inventory to equal
the published inventory, removes write permission from files and directory,
and revalidates them before verdict reduction and again after report
publication. Replay work has a fixed publication reserve inside the profile's
absolute deadline. Concurrent runs for the same profile and source use advisory
ownership locks and cannot replace one another's workspace or artifact tree.
Replay execution is admitted only on
Linux, where the executable is launched through its held descriptor. PR,
nightly, and weekly aggregate jobs use fresh run-specific Cargo homes and target
directories, never restore compiled targets or Cargo binaries, prefetch the
locked archives, execute the aggregate offline, require exactly one replay
report tree, and upload verifier evidence with missing files treated as errors,
independently from optional process telemetry. Before publication, the
repository-owned report-set verifier strictly decodes the JSON verdict report,
revalidates all profile and 44-row semantics, deterministically rerenders JUnit
and Markdown, and byte-compares both representations. Missing, malformed, or
status-divergent report files are red.

Every external GitHub Action reference is a full reviewed commit recorded in
`verification/github-actions.lock`; the repository contract scans every
workflow and composite-action YAML file, rejects inventory drift, and the
networked `scripts/verify-action-pins` audit proves each locked revision exists.
Every aggregate step has an explicit timeout, and the aggregate job budget must
exceed the sum of all step budgets by at least five minutes.

These controls assume the aggregate process owns an isolated runner account.
Read-only modes and before/after hashes detect accidental or in-process drift;
they are not claimed as an immutable boundary against a hostile concurrent
process with the same Unix identity. PR evidence producers and the aggregate
run on explicit GitHub-hosted Ubuntu releases, with a separate explicit macOS
launcher producer; their provider-managed ephemeral accounts and workspaces are
the trust boundary. Nightly and weekly invariant evidence producers and their
aggregate run in the same reviewed self-hosted Linux runtime class under an
isolated runner account and do not share their verifier workspace with
unrelated workloads.

`verification/detector.rs` owns the public fixture binding and reusable analysis
session. Its `source/` subtree owns parsing, target-graph binding, call
resolution, reachability, and oracle policy. Artifact verification consumes
that domain API; producers remain independent and their claims qualify only
after verifier-owned source analysis. No detector-source compatibility edge
back into `artifact_verify` remains. The architecture guard rejects the retired
physical mount and any producer/verifier dependency in either direction.

Process timing has an explicit policy/mechanics boundary. `producer/process`
owns profile and layer budget allocation because it consumes `RunnerContract`
and reserves evidence-finalization time. It emits one absolute lifecycle
deadline plus explicit execution and receipt-finalization boundaries;
publication, observation, target execution, escalation, reaping, and final
receipt reads derive their phase deadlines from that clock. `execution/process`
owns the descriptor-bound launch, elapsed-time observation, output collection,
process-group cleanup,
receipt retention, and termination mechanics; it does not interpret profiles
or layers. Each stdout, stderr, resource, process-group, and reservation file
is created as a held capability before launch. Child writes use inherited
descriptors and reads use those same file identities. Target-group publication
uses two distinct direct children. A dedicated helper anchors target group `A`;
the `/usr/bin/time` wrapper anchors wrapper group `W` and remains outside `A` so
SIGKILL of a timed-out target cannot destroy its authoritative resource
telemetry. A one-way lifetime-pipe writer is inherited through `W`, the target
launcher, and every target descendant; the dedicated anchor never holds it. A
no-signal reaper is ready before either child is spawned. Lifetime-lease setup
completes before anchor spawn, so a lease preflight failure cannot create
process ownership that would require rollback.

Target publication is a two-acknowledgement ownership handshake. The launcher
first publishes its PID while it is still inside `W`. After the parent validates
that membership, it records the uncertain `W`-to-`A` transition before sending
`G`. The launcher joins `A`, publishes `ready`, and remains blocked before target
`exec`. Only after the parent validates membership in `A`, records the anchored
placement, and sends `R` may user code run. Cleanup therefore knows whether it
must terminate `W`, `A`, or both even if publication fails between `G` and
`ready`.

Every signalable process-group ID is backed by an unreaped direct `Child`; a
published launcher PID is validation metadata and is never a signal target.
TERM and KILL attempts are monotonic per owned group. Process observation
brackets one bounded PID/PGID/RSS/state inventory with lifetime-lease probes,
excludes the helper from target RSS, and rejects a snapshot that omits an
anchor still live according to non-reaping `waitid`. Lease EOF is authoritative
target-lineage lifetime evidence. A stable EOF plus an empty target inventory
after `W` exits mints the placement-bound quiescence proof consumed by anchor
release or post-SIGKILL reap. A held lease plus an empty target inventory after
`W` exits, or EOF plus a live target row, is a harness error. A held-to-EOF
transition across the inventory is retried as an ordinary exit race.
`GroupAbsent` while any direct child remains unreaped is an ownership error,
not successful cleanup.

`ManagedProcess` stores absolute cleanup and confirmation deadlines and retains
fallback failures in an execution-scoped sink. At deadline expiry, every
unreaped wrapper is transferred to the already-running observation-only reaper.
The target anchor, its control channel, and the lifetime reader transfer as one
aggregate; the worker retains the unreaped anchor until lease EOF, then closes
the control channel and reaps it. Trusted internal observer commands inherit a
separate process-lineage lease. Their direct child remains unreaped until both
its exit and lease EOF are observed before the absolute cleanup deadline. If
that proof is late, the child and lease transfer together instead of probing a
numeric process-group identity after reap. Three typed adoption channels keep
ordinary children, leased children, and anchored groups structurally distinct.
The reaper has no signal capability and can only observe leases, close anchor
release control, and reap owned child handles. Relative grace and confirmation
intervals are capped by their precomputed absolute phase boundaries; deadline
checks precede each new completion observation, and no interval can refresh an
expired boundary or consume the reserved receipt-finalization phase.

The raw capability set is retained as non-verdict diagnostic material. The
versioned process log serialized into an `ArtifactRef` is the replayable,
content-hashed evidence. CI therefore uploads telemetry in uniquely named
diagnostic artifacts, never in a layer evidence artifact, and downloads each
layer's machine-readable evidence into a distinct directory. The aggregate
job does not merge telemetry trees or allow diagnostics to influence a verdict.

The launcher control plane and target environment are separate contracts.
Schema v14 source receipts bind a process-runtime inventory; every process log
records an ordered launcher chain and verifiers require each digest to match
that inventory. Their source-environment digest covers only compiler-selection
inputs (`DEVELOPER_DIR`, `SDKROOT`, and `SYSTEMROOT` when present). Cache,
workspace, home, and executable-search paths belong to execution instead: the
top-level invocation receipt binds the complete deterministic base environment,
and each child log must match that base plus only its reviewed command-specific
additions. This lets aggregate jobs use isolated Cargo homes without weakening
compiler-source identity or accepting environment drift. Perl, `/usr/bin/time`,
and the platform-pinned `ps` observer
execute through the descriptors whose bytes were hashed. Reviewed Bash scripts
are launched through a separately bound Bash descriptor rather than a later
kernel shebang lookup. `/usr/bin/env bash` selects the source receipt's
PATH-bound Bash identity; an absolute Bash shebang is accepted only when it
resolves to that same path and digest. Unknown or alternate interpreters and
shebang options fail closed. The top-level producer invocation has no launcher
chain; launcher receipts belong only to the subprocess logs they describe.
The launchers receive only a minimal deterministic environment plus reserved
descriptor controls. The exact receipt-bound target map is carried as data,
installed only immediately before target `exec`, and rejected if it uses a
reserved control key. Parent capabilities are `CLOEXEC` by default; the
launcher closes every inherited descriptor outside the exact mapped inventory
before starting `time`. Internal observer pipes are nonblocking and drained in
the deadline loop, so a descendant retaining a writer cannot turn cleanup into
an unbounded thread join.

## Reading path

A reader should be able to understand the verifier in this order:

1. `contract/registry/`: the reviewed invariant, clause, and evidence language;
2. `contract/catalog/` and `contract/profile/`: its executable normalized view
   and selected runner obligations;
3. `evidence/`: the serialized observations and receipts crossing trust
   boundaries;
4. `plan/`: the immutable contract and invocation selected for one run;
5. `provenance/` and `execution/`: source, target, artifact, filesystem, and
   process mechanics;
6. `producer/<layer>/`: how each required evidence family is executed;
7. `verification/<layer>/`: how artifacts are independently reconstructed and
   checked;
8. `verdict/`: the only clause and invariant reduction path;
9. `gate/` and `cli/`: orchestration and user intent.

## Trust boundaries

Repository code and the documented CI host boundary are trusted to run the
contract. Produced receipts, logs, source paths, counters, and artifact claims
are untrusted until verification reopens, confines, hashes, parses, and
rederives them.

- A receipt type may express a claim; it may not validate itself as true.
- JSON schema validation establishes shape, not semantic acceptance.
- Producer status is never a substitute for artifact verification.
- Verifiers share only stable vocabulary and neutral syntax decoding with
  producers. They independently own semantic acceptance and failure
  classification.
- Missing, malformed, partial, stale, or multiply bound evidence remains red.
- Artifact paths are confined before reads, and preserved hashes are checked
  against bytes rather than trusted metadata.
- Process receipts are read from the capabilities created before launch, not
  by reopening mutable names. A path replacement invalidates final binding but
  cannot substitute bytes or turn finalization into a FIFO/device wait.
- Byte and elapsed-time limits bound user-space receipt work. The execution
  contract assumes the repository and telemetry directory are on a responsive
  local filesystem; a kernel metadata syscall stalled by a failed remote mount
  requires an outer job or container deadline.
- Reports render an existing verdict and cannot improve or reinterpret it.
- Source-bound evidence programs and their descendants must not call `setsid`
  or `setpgid` to escape the managed process group. They must preserve the
  inherited lifetime writer across fork and exec: closing it, setting
  `FD_CLOEXEC`, applying `closefrom` or `close_range` across it, passing it to
  unrelated processes, or writing to it violates the execution contract. A
  runner that executes adversarial child code requires a stronger container or
  cgroup boundary; process-group and lease evidence alone are not that
  boundary.

`EvidenceIntake` contains normalized results accepted from verified bundles,
their profile and source binding, and typed intake defects for missing,
malformed, stale, or unverifiable inputs. Raw `ResultBundle` values are
consumed and discarded at that boundary. An accepted result may still carry an
invariant violation, coverage miss, or harness error; verification means the
result is authentically and coherently represented, not that it passed.
Verdict reduction therefore retains enough information to emit all 44 rows
even when no bundle is acceptable.

Defect kinds remain verifier-internal vocabulary. Version-2 verdicts preserve
the reviewed `aggregate/harness` issue ID and public structural-error messages;
changing that projection requires an explicit verdict-schema migration.

Registry parsing owns authoring syntax and registry-specific parse errors. It
does not construct catalog types. Catalog normalization owns the explicit
`TryFrom<RegistryDocument>` conversion. Likewise, profile contracts own
liveness obligations; serialized evidence only records bindings to them. These
conversion boundaries remove the current registry/catalog and catalog/evidence
cycles without introducing adapter traits or callback frameworks.

## Facades and visibility

- `lib.rs` preserves the existing flat `rafter_invariants::{...}` API through
  declarations and re-exports only. Public implementation lives in its owning
  domain; internal folders are not public API.
- Domain `mod.rs` files contain module declarations, vocabulary declarations,
  and narrow re-exports only. They do not contain execution functions or broad
  `impl` blocks.
- Prefer `pub(super)` or `pub(in crate::<domain>)` for machinery owned by one
  domain. Use `pub(crate)` only for an intentional cross-domain contract.
- External callers use the flat crate facade. Internal modules import explicit
  owning domains such as `crate::contract::...` or `crate::evidence::...`, so
  dependency direction remains visible to architecture tests. Imports from
  `super` are for immediate implementation collaborators. Avoid deep chains of
  `super::super::super`.
- A helper should name a verification phase such as `classify`, `bind`,
  `validate`, `rederive`, `publish`, or `reduce`; it should not merely move
  lines out of sight.

## Test architecture

Tests are executable verification stories:

1. arrange a contract, source image, process result, or artifact set;
2. perform one named execution or adversarial mutation;
3. assert a typed pass, invariant violation, coverage miss, or harness error.

Production modules do not embed test bodies. Small private-unit checks use a
sibling `*_test.rs`; larger stories live under the owning domain's `tests/`
tree. Mature test facades contain declarations and local imports only. Every
test module begins with a `//!` scenario contract.

Fixture builders stay beside the domain they model:

- source and Cargo fixtures under `provenance/source/tests/`;
- process lifecycle fixtures under `execution/process/tests/`;
- detector call-graph fixtures under `verification/detector/source_tests/`;
- simulator schedule fixtures under `verification/simulator/tests/`;
- TLA mutation and checkpoint fixtures under their TLA producer or verifier;
- Maelstrom history and lease fixtures under their Maelstrom producer or
  verifier.

Do not create a global fixture grab bag. Detector-level negative fixtures must
continue to invoke the real detector or recorder path.

## Compatibility and trust constraints

This refactor is source-visible even when behavior is unchanged. The gate is
designed to reject stale evidence, so every milestone must produce fresh
receipts for its own source ref.

The following contracts remain stable during architecture-only milestones:

- the 44 reviewed invariant IDs, clause IDs, and evidence IDs;
- registry, result, plan, and verdict schema versions and serialized shapes;
- CLI command names, arguments, exit behavior, and output locations;
- JSON, JUnit, and Markdown report semantics;
- failure classification as invariant violation, coverage not reached, or
  harness error;
- detector macro expansion protocol, marker prefixes, and parent challenge
  handshake, except in an explicit reviewed protocol-version milestone;
- Cargo target roots such as `rafter-invariant-test/src/lib.rs`;
- evidence paths, symbols, test names, and derived module identities declared in
  `verification/raft-invariants.yaml`;
- artifact names and replayable counterexample/log retention.

A module move alone does not justify a schema-version bump. Schema versions
change only when serialized readers need an explicit compatibility boundary.
Profile schema v8 is such a boundary: it adds a canonical replay-inventory
digest and aggregate registry resource limits to the exact package, fixture,
binding, and target counts plus the authenticated offline directory-source
build policy. A v7 reader cannot enforce those obligations and must not accept
the manifest as equivalent. Profile schema v9 is a second boundary: it adds the
separately selected aggregate `verifiers` contract and requires the pre-body
detector secret protocol, under which the inherited descriptor is closed before
fixture code can run. A v8 reader cannot select that aggregate policy or treat
the changed execution capability contract as equivalent.

Before moving producer files, replace workflow checkpoint hashes that enumerate
specific producer subdirectories with a reviewed recursive source input. Update
the CI contract test in the same milestone. A conservative cache miss is
acceptable; accepting a checkpoint built by omitted implementation code is not.

Keep the protected library and macro target roots at `src/lib.rs`. Existing
subprocess fixture functions must retain their exact `tests::...` identities in
`src/tests.rs`; move only their helper machinery into child modules. The source
verifier admits the reviewed `impl_oracle_call!` and `thread_local!` support
invocations only from their exact domain sources named above. Move either only
in a coordinated verifier change with source-graph and forged-fixture tests.
This avoids weakening the protected-target contract in pursuit of a declarative
facade.

Proof-bound detector execution is a Unix transport contract and relies on an
exact, single-threaded subprocess plus process-global environment. Linux and
macOS must behave identically; unsupported transports fail closed. Standalone
ordinary tests may remain portable, but must never imply that a non-Unix host
produced a challenge-bound detector proof. The invocation adapter's supported
arity and the canonical `rafter_invariant_test` crate-name requirement are also
part of the compile-time contract and need explicit tests.

The v3 detector transport is descriptor-only. The parent owns an anonymous
connected Unix socket pair, passes one endpoint through the managed process
launcher, and retains the other endpoint behind a one-shot challenge gate. The
child validates the inherited descriptor as a connected Unix socket, removes
its environment capability, obtains the secret, and closes the descriptor
before fixture code runs. It uses safe syscall wrappers under the workspace-wide
`unsafe_code = "forbid"` policy. Pathname
listeners, reconnectable sockets, and shared challenge files are forbidden.

Some internal module paths are serialized or invoked as protocol identities.
For example, TLA mutation validation currently binds
`producer::tla_exec::mutation_tests`, and detector binding derives module
segments from registered test names. Keep compatibility facades at these paths
while implementation moves behind them. Retire or rename an identity only in a
reviewed protocol migration with negative compatibility fixtures; a cleaner
folder name is not sufficient reason.

Do not let producer and verifier read one mutable compatibility allowlist. A
review inventory may document both sides, but independent code and tests must
continue to enforce their respective contracts.

## Architecture ratchets

Extend the repository's existing readability guards to the three invariant
crates.

- Every production module starts with a concise `//!` ownership contract.
- Every test module starts with a concise `//!` scenario contract.
- Production and mature test facades are declarative.
- Production files target 300 lines; coherent test stories target 400 lines;
  facades use the tighter existing facade budget.
- Size remains a graduated signal, not a reason to fracture one concept. New or
  moved files may not increase the count above a ratchet threshold.
- Modeled `contract`, `evidence`, `execution`, `verification`, `verdict`,
  `gate`, and extracted producer paths have no legacy documentation allowance: every Rust
  file in those trees must carry a module or scenario contract even while older
  domains retain aggregate debt.
- No production module embeds a test body.
- The target dependency graph and the exact migrated-source manifest are both
  executable. Adding a source tree to the manifest is an explicit ratchet;
  target domains not yet listed remain migration work, not implied coverage.
- A migrated source may be a directory or one Rust facade file. Compatibility
  facades outside their child directory must be modeled explicitly.
- Dependency checks normalize `crate`, `self`, and `super` paths and inspect
  both imports and expression paths. Exact raw-process callsites cannot be
  widened through aliases, relative paths, globs, or test-like filenames.
- `producer` and `verification` may not import one another.
- Neutral detector transcript decoding and independent producer/verifier
  acceptance policies are source-location ratchets; shared transcript verdict
  reducers cannot return.
- The flat root-level files retired by the migration may not reappear.
- Exceptions require an exact owner, source, import, narrow reason, and tracking
  label. Missing, duplicated, broadened, or stale exceptions fail.

## Migration plan

Each milestone is behavior-preserving, independently reviewable, and committed
only while the repository is green.

1. **Stabilize architecture inputs.** Replace brittle workflow source globs,
   require exact nonzero test inventories for every filtered CI invocation,
   add the invariant-crate facade/dependency manifests, and record the current
   size ratchet without weakening any existing threshold. This includes the
   macOS launcher filters and the complete TLA mutation suite.
2. **Clean the detector-test crates.** Split macro expansion and detector/oracle
   lifecycles, preserve the hidden ABI, and add compile-time and adversarial
   tests.
3. **Establish contract and evidence vocabulary.** Move registry, catalog,
   profile, schema, and serialized models behind declarative facades while
   preserving all public re-exports and wire shapes. Give registry parsing its
   own error type and make catalog normalization an explicit
   `TryFrom<RegistryDocument>` conversion.
4. **Extract provenance and execution mechanics.** Move source, target, image,
   filesystem, and process ownership out of `producer`; remove every verifier
   dependency on producer internals.
5. **Refactor one evidence family at a time.** For tests, simulator, TLA, then
   Maelstrom, move producer and verifier code into mirrored domain trees. Move
   tests with the code and run each family's negative fixtures before the next
   family.
6. **Separate verdict, reporting, gate, and CLI.** Make aggregation the sole
   verdict reducer over `EvidenceIntake`, make rendering pure, and reduce
   `main.rs` to parsing and delegation.
7. **Close the ratchets.** Require module contracts everywhere, delete retired
   flat paths, remove temporary exceptions, and run independent architecture,
   false-positive, and false-negative reviews.

## Verification for every milestone

Use the narrowest relevant tests first, then the repository gates:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --locked -p rafter-invariant-test-macros
cargo test --locked -p rafter-invariant-test
cargo test --locked -p rafter-invariants --lib
cargo test --locked -p rafter --test dependency_boundary
cargo test --locked -p rafter --test file_size_guard
cargo test --locked -p rafter --test test_location_guard
cargo test --locked -p rafter --test invariant_ci_contract
```

Run source-provenance, detector, ignored TLA validation, and process-lifecycle
tests whenever their owning domains move. At each completed domain milestone,
produce fresh evidence and require the deterministic PR profile to return
exactly `44/44 green`. The final branch must pass the complete GitHub Actions
matrix and preserve its JSON, JUnit, Markdown, logs, and replay artifacts.

For source-identity changes, exercise the compatibility matrix explicitly:

- old artifact with its old checkout remains green;
- old artifact against the new checkout is stale and red;
- mixed old and new layer bundles are red;
- fresh artifacts against the new checkout can become green.

Because producers require a clean checkout, commit each reviewed design or
code milestone before generating its fresh evidence. An untracked architecture
document is correctly treated as an unbound source input rather than ignored.

Refactoring success is not fewer lines or more folders. It is a verifier whose
trust boundaries and fail-closed decisions can be understood, tested, and
changed one domain at a time.
