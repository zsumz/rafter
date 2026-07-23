# Invariant tooling architecture

Status: accepted and maintained with the implementation.

Rafter's invariant tooling turns a reviewed verification contract into
source-bound evidence and exactly one deterministic verdict per invariant. It
is security-sensitive build and verification code: module ownership, trust
boundaries, and fail-closed decisions must remain obvious to a reader.

The tooling spans three workspace crates:

- `rafter-invariant-test-macros` defines the trusted detector-test attribute;
- `rafter-invariant-test` defines typed oracle markers and detector-proof
  lifecycle; and
- `rafter-invariants` loads contracts, executes evidence producers,
  independently verifies artifacts, and emits the 44-row report.

The split separates syntax expansion, detector runtime, and deterministic
aggregation. Within `rafter-invariants`, ordinary Rust modules separate stable
domains; there are no Git submodules or additional workspace-layer crates.

## Reading path

Read one gate execution in this order:

1. `contract/` loads the invariant registry and profile policy.
2. `plan/` binds those inputs to the invocation and source identity.
3. `producer/` executes the selected tests, simulator, TLA+, or Maelstrom
   layer and writes a receipt.
4. `verification/` reconstructs the source, process, and artifact claims
   without trusting producer acceptance decisions.
5. `verdict/` reduces verified evidence into clause and invariant verdicts.
6. `gate/` orchestrates complete execution, report publication, and semantic
   readback.
7. `gate/command/` adapts binary command inputs to gate operations.
8. `cli/` owns only Clap vocabulary, dispatch, and terminal emission.

`evidence/` owns serialized vocabulary used across those phases.
`execution/` owns neutral filesystem and managed-process mechanics.
`provenance/` owns reusable source, executable, target, and tool identities.

## Vocabulary

- **registry**: the reviewed authoring document containing invariant, clause,
  and evidence declarations;
- **catalog**: the normalized executable view derived from the registry;
- **profile**: selected evidence layers, runner bounds, and coverage policy;
- **plan**: the immutable, source-bound contract selected for one invocation;
- **producer**: code that executes one evidence layer and writes a receipt;
- **evidence**: immutable serialized observations, artifacts, and results;
- **receipt**: a structured account of plan or producer execution;
- **provenance**: identities binding source, executable, target, tool, and
  artifact bytes;
- **verification**: independent reconstruction and validation of receipts and
  artifacts;
- **verdict**: fail-closed reduction of verified evidence for one clause or
  invariant;
- **gate**: orchestration requiring the selected profile's complete verdict
  set; and
- **report**: a deterministic verdict projection, never a second verdict
  engine.

Avoid generic ownership homes such as `utils`, repository-wide `support`, or
`common`. Shared mechanics belong to the narrowest named domain that owns the
contract.

## Dependency direction

The reviewed dependency graph is one-way:

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

Each arrow runs from dependency to consumer.

- `contract` owns declarations and profile policy. It does not execute
  processes or inspect produced artifacts.
- `evidence` owns wire vocabulary. It does not decide whether its own values
  are trustworthy.
- `execution` owns confined filesystem and managed-process mechanics. It does
  not know invariant IDs or verdict policy.
- `provenance` derives reusable byte and identity observations without
  accepting evidence.
- `plan` resolves a contract and binds invocation inputs.
- `producer` may depend on the lower domains, but never on `verification` or
  `verdict`.
- `verification` may depend on contract, evidence, neutral execution, and
  provenance, but never on producer implementation.
- `verdict` consumes typed verified intake. It does not spawn tools, discover
  tests, or read unverified runner files.
- `gate` owns cross-domain orchestration and command adapters.
- `cli` depends only on `contract` and `gate`; the current binary reaches the
  library exclusively through `gate::command`.

The binary and companion library are distinct Rust crates in one package.
Architecture analysis therefore treats `rafter_invariants::...` exactly like
`crate::...` and rejects crate-root aliases that could hide a domain edge.
Gate command adapters own producer-image bootstrap, plan loading, layer
execution, archive publication/readback, and report-set verification so the
CLI cannot assemble those preconditions itself.

## Producer and verifier separation

Producer and verifier may share serialized types and neutral syntax decoders,
but they may not share pass/fail reducers. Detector transcripts, TLA+ mutation
qualification, checkpoint policy, Maelstrom history, and source receipts each
retain independent producer and verifier acceptance logic.

Verification returns typed evidence intake. Raw result bundles cannot cross
the verdict boundary. Aggregate intake revalidates artifacts before reduction
and after report publication; newly observed drift causes the report to be
reduced and published again as red.

The full production `producer/` source tree is in the enforced architecture
inventory. Production modules import receipt, contract, and evidence types
through their owning domains rather than through crate-root compatibility
facades. Test-only compatibility mounts remain outside production dependency
claims and are checked by their own identity guards.

## Source, process, and publication trust boundaries

Producers run on a trusted CI host. Source receipts bind a clean Git checkout,
raw tracked-file materialization, lockfile, compiler selection, toolchain, and
producer executable. This is deterministic repository provenance, not
hostile-host attestation or a hermetic source-to-binary proof.

External evidence processes use bounded lifecycle management, held executable
and working-directory identities, process-group ownership, deadline-aware
termination, and content-addressed logs. Production evidence subprocess
execution requires Linux descriptor-bound executable launch and fails closed
on other operating systems. The macOS CI lane exercises launcher mechanics
under test-only fallback; it does not produce accepted invariant evidence.

The verifier independently authenticates source and registry inputs, compiles
detector fixtures in a fresh private target, executes bounded replay, and
publishes content-addressed process logs plus a replay report. CI seals that
inventory into a deterministic archive, downloads it to a fresh path, and
repeats digest, metadata, schema, and semantic readback.

The threat model does not cover a malicious producer binary, compromised
kernel or CI runner, hostile same-UID process, or SHA-256 compromise. Those
claims require external build and host attestation.

## Compatibility identities

Some internal paths are protocol identities. TLA+ mutation validation retains
the reviewed `producer::tla_exec::mutation_tests` identity, and detector
binding derives module segments from registered test names. Compatibility
facades at such paths remain declarative and test-only where applicable.

Rename or retire a serialized path, public root export, schema field, artifact
name, CLI command, or test identity only through an explicit compatibility
migration with negative fixtures. A cleaner folder name is not sufficient.

## Executable architecture ratchets

The repository guards enforce the architecture rather than relying on this
document alone:

- production and test modules begin with ownership/scenario contracts;
- mature facades remain declarative;
- the reviewed domain graph applies to every completed source root;
- the complete production producer tree is fail-closed;
- both `crate`-relative and binary companion-library paths are normalized;
- crate-root, relative-path, grouped-import, expression-path, and macro
  indirection cannot hide forbidden dependencies;
- producer and verifier cannot import one another;
- raw process execution has exact, non-aliasable call sites;
- producer/verifier transcript policies remain independently owned;
- retired flat files and root-owned implementation paths cannot return;
- exceptions require an exact owner, source, import, reason, and tracking
  label; and
- presentation and dependency debt baselines may only shrink.

Changes to source identity require fresh evidence. The deterministic PR gate
must still emit exactly 44 unique green verdicts, and compatibility changes
must explicitly exercise old/old, old/new, mixed-source, and fresh/new
artifact cases as applicable.
