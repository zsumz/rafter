//! A supervisor that restarts the Maelstrom node underneath a live run.
//!
//! It sits between Maelstrom and one `rafter-maelstrom` child and forwards
//! both directions untouched except where a configured fault requires
//! otherwise. It is a fault injector, not a participant: it speaks no Raft,
//! holds no application state, and decides nothing the checker will judge.

use std::error::Error;

#[path = "rafter-maelstrom-leader-restart-proxy/config.rs"]
mod config;
#[path = "rafter-maelstrom-leader-restart-proxy/lease_isolation.rs"]
mod lease_isolation;
#[path = "rafter-maelstrom-leader-restart-proxy/protocol.rs"]
mod protocol;
#[path = "rafter-maelstrom-leader-restart-proxy/supervisor.rs"]
mod supervisor;

fn main() {
    if let Err(error) = run() {
        eprintln!("rafter-maelstrom leader restart proxy failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    supervisor::run()
}
