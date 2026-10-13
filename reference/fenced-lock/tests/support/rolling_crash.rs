//! Orderly replacement or observed SIGKILL with inputs retained before recovery.

use std::{path::PathBuf, process::ExitStatus};

use rafter::NodeId;

use crate::{copy_state, production_process::ProductionCluster};

pub struct Replacements {
    crash_inputs: Option<PathBuf>,
}

impl Replacements {
    pub fn from_env() -> Self {
        let crash_inputs = std::env::var_os("RAFTER_UPGRADE_CRASH_STATE").map(PathBuf::from);
        let mode = if let Some(path) = &crash_inputs {
            assert!(
                path.is_absolute(),
                "crash evidence must use an absolute path"
            );
            std::fs::create_dir(path).expect("crash evidence directory must be fresh");
            "sigkill"
        } else {
            "graceful"
        };
        println!("UPGRADE_REPLACEMENTS mode={mode}");
        Self { crash_inputs }
    }

    pub fn stop(&self, cluster: &mut ProductionCluster, node: NodeId, stage: &str) {
        let Some(root) = &self.crash_inputs else {
            cluster.stop_checked(node);
            return;
        };
        let (pid, status) = cluster.crash_checked(node);
        require_sigkill(status);
        // The killed child has been reaped, and no replacement is started until
        // this copy completes. Other live replicas own different directories.
        copy_state(
            &cluster.root().join(format!("node-{}", node.0)),
            &root.join(format!("{stage}-node-{}", node.0)),
        );
        println!(
            "UPGRADE_CRASH stage={stage} node={} pid={pid} signal=9",
            node.0
        );
    }
}

#[cfg(unix)]
fn require_sigkill(status: ExitStatus) {
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(9),
        "replacement requires actual SIGKILL: {status}"
    );
}

#[cfg(not(unix))]
fn require_sigkill(status: ExitStatus) {
    panic!("SIGKILL replacement qualification requires Unix: {status}");
}
