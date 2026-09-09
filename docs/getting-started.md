# Getting started

Rafter helps multiple copies of your app agree on changes. You decide what a
change does; Rafter makes sure the copies agree on the order.

Let's start with a tiny app that saves and reads a value. Then we'll choose
which parts of Rafter to use and try an app with disk storage and TLS.

## 1. Run your first example

You'll need Rust 1.88 and a copy of this repository. Run this command from the
repository's root folder.

```sh
cargo run -p rafter-service --example replicated_kv_service
```

This starts three copies of a small key-value app inside one process. It saves
`alpha = one`, reads the value back, and shuts down.

**Look for this line:**

```text
linearizable read returned alpha=Some("one")
```

> This first example keeps everything in memory. It is a quick way to learn the
> API. Step 4 uses a separate app with disk storage and real TLS connections.

## 2. Follow a write and a read

These are the two calls used by the example, shown inside an async function
after creating its `raft` handle:

```rust
// Save a value.
let write = raft.write(("alpha".to_owned(), "one".to_owned())).await?;

// Read it back.
let read = raft
    .read("alpha".to_owned(), rafter_service::ReadConsistency::Linearizable)
    .await?;

assert_eq!(read.result, Some("one".to_owned()));
```

For the write, Rafter gets the group to agree on the command. Your app then
applies it: in this case, storing `one` under the key `alpha`.

`Linearizable` asks for a read that accounts for writes completed before the
read began, even though the app has multiple copies.

Your data and the code that changes it are called a **state machine**. In this
example, that's a Rust map and the code that inserts and looks up values.

**Open the [example source](../crates/rafter-service/examples/replicated_kv_service.rs)**
to see the complete setup and the small KV state machine.

## 3. Choose how much Rafter to use

Rafter is a set of libraries. You can use just the core, use the supplied
components together, or mix them with your own.

### Minimal: use just the core

Choose this when you want to plug Raft into storage and networking you already
have. Your app depends on **`rafter`** and connects the pieces itself.

Try the smaller example:

```sh
cargo run -p rafter --example pure_raft
```

**What you'll see:** node 1 becomes the leader, and all three nodes report
applying `set account:7 balance=42`.

The [source](../crates/rafter/examples/pure_raft.rs) shows the basic loop: give
Rafter a timer tick or a message, deliver the messages it produces, and apply
commands when Rafter says they are agreed. This example uses a message queue
and has no disk storage.

### Full stack: use the supplied components

Choose this when you want Rafter to provide more of the pieces. This is a good
starting point if you don't already have storage and networking to integrate.

| Piece | What it does |
| --- | --- |
| `rafter` | Gets the copies of your app to agree on commands. |
| `rafter-storage` + `rafter-runtime` | Save Raft's own state to disk and recover it after a restart. |
| `rafter-app` | Connects those commands to your state machine. |
| `rafter-service` | Gives your app the write and read API from step 2. |
| `rafter-transport-tls` | Encrypts connections between servers and checks who is connecting. |

You still write the application: what its commands do, how it saves its data,
and how clients talk to it. You also provide the server addresses and TLS
credentials. Rafter's libraries run inside that application.

<details>
<summary><strong>Adding the crates to your own project</strong></summary>

For a local project beside this `rafter` checkout, add the pieces you want to
its `Cargo.toml`. The minimal path needs only the first line under
`[dependencies]`; the full stack uses all six.

```toml
[dependencies]
rafter = { path = "../rafter/crates/rafter" }
rafter-storage = { path = "../rafter/crates/rafter-storage" }
rafter-runtime = { path = "../rafter/crates/rafter-runtime" }
rafter-app = { path = "../rafter/crates/rafter-app" }
rafter-service = { path = "../rafter/crates/rafter-service" }
rafter-transport-tls = { path = "../rafter/crates/rafter-transport-tls" }
```

These paths use the same code as the examples. Adjust them if your folders are
arranged differently.

</details>

## 4. Try disk storage and TLS

The repository includes a complete example app that manages locks: it decides
which client can use a shared resource. It uses Rafter's supplied layers,
including TLS between servers, plus its own code for saving application data.

Run the app's test suite:

```sh
scripts/reference-source-check -- \
  -p rafter-reference-fenced-lock --test process_production \
  -- --ignored --test-threads=1
```

The tests start real server processes, perform writes and reads, check recovery
after a restart, and clean up when finished. They also check who can connect and
how servers join or leave the group.

**Look for five passing tests, followed by:**

```text
reference source mode passed
```

The command also checks formatting, lints, and documentation, so its first run
takes longer than the earlier examples. Use this script to test the libraries
from your checkout.

> This is a local test run, using test certificates. TLS protects the connections
> between servers. Your application's client-facing API needs its own setup.

<details>
<summary><strong>What this command needs</strong></summary>

Install the Rust tools used by the check if you don't have them:

```sh
rustup component add rustfmt clippy
```

The TLS dependency also needs a native C compiler. The tests need to create
temporary files, start child processes, and open local network ports.

</details>

## 5. Make it your own

Start small and build up:

1. **Change the app's behavior.** Use the [KV example](../crates/rafter-service/examples/replicated_kv_service.rs)
   as a starting point. Define your commands and how each one changes your data.
2. **Save your app's data.** Save both the data and a record of which commands
   are already included, so a restart can continue from the right place. The
   [KV state file example](../crates/rafter-runtime/examples/replicated_kv/app_state.rs)
   shows one way to do that.
3. **Connect your servers.** Follow the [complete app's setup](../reference/fenced-lock/src/bin/lock-production-node/main.rs)
   to combine storage, TLS, and the loop that keeps Rafter processing work.
   Start with one group of servers before adding more groups.

You can replace individual pieces as your app grows. Use your own storage or
transport, skip the service layer if you prefer direct calls, or add
`rafter-multiraft` when one process needs to manage many independent groups.

For the details you'll need as you build:

| Next question | Read this |
| --- | --- |
| How do the pieces fit together? | [Architecture](./architecture.md) |
| How do I recover safely after a restart? | [Recovery and snapshots](./architecture.md#recovery-and-snapshots) |
| How do I configure TLS and peer identities? | [TLS transport](../crates/rafter-transport-tls/README.md) |
| How does the complete example work? | [Reference applications](./reference-consumers.md#production-composition) |
