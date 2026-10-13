//! Orderly replacement or observed SIGKILL with inputs retained before recovery.

use std::{path::PathBuf, process::ExitStatus};

use rafter::NodeId;

use crate::{
    copy_state,
    production_process::{wait_until, ProductionCluster},
};

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
        // Strict reopen is qualified after the submitted workload is applied.
        // Killing a lagging application during its publication can correctly
        // require an explicit repair decision; that is a separate contract.
        // No new client command is submitted between this barrier and SIGKILL.
        let leader = cluster.wait_for_leader();
        let observed = cluster.observe(leader);
        let committed = observed.number("commit_index");
        // Application indices intentionally skip Raft no-ops and configuration
        // entries, so they need not equal the committed Raft index.
        let application = observed.number("applied_index");
        assert!(application > 0 && application <= committed);
        let (observed_commit, applied) =
            wait_until("replacement victim to apply the committed workload", || {
                let observed = cluster.observe(node);
                let observed_commit = observed.number("commit_index");
                let applied = observed.number("applied_index");
                (observed_commit >= committed && applied >= application)
                    .then_some((observed_commit, applied))
            });
        let (pid, status) = cluster.crash_checked(node);
        require_sigkill(status);
        // The killed child has been reaped, and no replacement is started until
        // this copy completes. Other live replicas own different directories.
        copy_state(
            &cluster.root().join(format!("node-{}", node.0)),
            &root.join(format!("{stage}-node-{}", node.0)),
        );
        println!(
            "UPGRADE_CRASH stage={stage} node={} pid={pid} signal=9 \
             committed={committed} application={application} observed_commit={observed_commit} applied={applied}",
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
