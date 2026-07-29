# Shared process-test boundary

This inventory records the duplication that existed between the ledger and
fenced-lock process suites before extraction. The harness may own only
mechanisms already present in both consumers.

## Proven common mechanisms

- Poll a predicate until it returns a value or a named deadline expires.
- Connect to a TCP address and exchange one newline-delimited request and
  response with bounded reads and writes.
- Reuse a connection, reconnect once after an exchange on a cached connection
  fails, and do not retry an initial connection failure.
- Spawn a caller-configured child with captured stdout, retain every lifecycle
  line, and search retained lines without consuming them.
- Report the child identity, awaited condition, timeout, exit state, and
  retained output when a lifecycle wait fails.
- Stop a child cleanly or forcefully and always reap it, including during
  unwinding.
- Create a uniquely named scratch directory and remove it when its owner drops.

## Consumer-owned responsibilities

The ledger and fenced-lock suites continue to own:

- binary discovery, command-line arguments, ports, and replica configuration;
- lifecycle prefixes and their meanings;
- request rendering, response parsing, and all protocol vocabulary;
- history events, outcomes, operation identifiers, and diagnostic rendering;
- leadership, readiness, replay, and recovery policy;
- repair or recovery-mode decisions and escalation records;
- durable artifact paths, corruption helpers, and storage assertions;
- fenced-lock load generators, queued requests, and control-plane faults.

The shared crate is not a generic server runtime or transport abstraction. It
does not interpret a lifecycle line, choose whether to reconnect a process,
decide how recovery proceeds, or know what any request means.
