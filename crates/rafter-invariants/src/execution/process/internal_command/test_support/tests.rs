//! The late-completion fixture waits for lineage, not only the direct child.

use std::{os::unix::process::CommandExt, process::Command, time::Instant};

use super::{
    await_completion_boundary_if_requested, await_next_internal_completion_after_deadline,
    Duration, INTERNAL_CLEANUP_ALLOWANCE,
};
use crate::execution::process::{
    clear_signal_attempts, hold_lease_writer, spawn_child, take_signal_attempts, CleanupFailures,
    ManagedInternalProcess, NoSignalReaper, ProcessLeaseState,
};

#[test]
fn late_completion_boundary_requires_the_exited_childs_lineage_to_release() {
    let reaper = NoSignalReaper::start().expect("start fixture reaper");
    let mut command = Command::new("/bin/sh");
    command.args(["-c", "exit 0"]).process_group(0);
    let child = spawn_child(&mut command).expect("spawn fixture before holding the lease");
    let (lease, held_writer) = hold_lease_writer().expect("hold a separate inherited writer");
    let cleanup_deadline = Instant::now() + INTERNAL_CLEANUP_ALLOWANCE;
    let mut process = ManagedInternalProcess::new(
        child,
        cleanup_deadline,
        CleanupFailures::default(),
        reaper,
        lease,
    );
    while !process.exit_observed().expect("observe direct child") {
        assert!(
            Instant::now() < cleanup_deadline,
            "fixture child did not exit"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(process.lifetime_state().unwrap(), ProcessLeaseState::Held);
    clear_signal_attempts();
    let expired = Instant::now();
    await_next_internal_completion_after_deadline();
    let held_result = await_completion_boundary_if_requested(&mut process, expired, expired);
    drop(held_writer);
    await_next_internal_completion_after_deadline();
    let released_result = await_completion_boundary_if_requested(&mut process, expired, expired);
    assert!(process.try_wait().expect("reap exited fixture").is_some());
    process.disarm().expect("release fully completed fixture");
    assert!(
        held_result.is_err(),
        "an exited child with a held lineage must not satisfy the completion fixture"
    );
    assert!(
        released_result.is_ok(),
        "released lineage completes the fixture"
    );
    assert!(take_signal_attempts().is_empty());
}
