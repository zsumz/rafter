//! Replicated KV lifecycle over three durable Raft nodes.
//!
//! This example is intentionally small and complete: it opens three
//! file-backed runtimes, elects a leader, applies `set` commands into a KV
//! state machine, serves a linearizable read through `ReadIndex`, restarts a
//! follower with an applied floor, installs a snapshot into a lagging follower,
//! and transfers leadership during load.
//!
//! It demonstrates the production embedding contract shape summarized in the
//! repository README: persist application state with its applied floor,
//! consume recovery outputs before serving traffic, and treat the reference TCP
//! transport as unauthenticated demo plumbing rather than a production
//! peer-identity layer.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-runtime --example replicated_kv
//! cargo run -p rafter-runtime --example replicated_kv -- --process-cluster
//! ```

#[path = "replicated_kv/app_state.rs"]
mod app_state;
#[path = "replicated_kv/cli.rs"]
mod cli;
#[path = "replicated_kv/codec.rs"]
mod codec;
#[path = "replicated_kv/in_process.rs"]
mod in_process;
#[path = "replicated_kv/process.rs"]
mod process;
#[path = "replicated_kv/storage.rs"]
mod storage;
#[path = "replicated_kv/types.rs"]
mod types;

pub use in_process::run_in_process_demo;
pub use process::{run_process_demo_with_spawn, run_process_node_from_env, ProcessSpawn};
pub use types::{ScenarioOptions, ScenarioReport};

#[cfg(test)]
pub(crate) use app_state::{load_app_state, persist_app_state};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    cli::run(&args);
}
