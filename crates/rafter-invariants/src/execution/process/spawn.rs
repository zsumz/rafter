//! The only process creation in this domain, and the only lease-writer window.
//!
//! A child that has forked but not yet exec'd still carries its parent's image,
//! including a copy of every descriptor the parent held at fork time.
//! `FD_CLOEXEC` closes those copies at exec, not at fork, so a lease writer that
//! is open while any other thread forks is held by that thread's child until it
//! reaches exec — and until then the lease reads `Held` with every intended
//! holder gone. Under load that window is wide enough to sample with `ps`.
//!
//! This module closes that by construction rather than by timing: it is the only
//! place a lease writer can be opened, and [`PROCESS_CREATION`] admits one
//! process creation at a time, so no fork in this domain ever runs while a
//! writer exists. The exclusion spans `Command::spawn`, which is the whole
//! window: `command_fds` installs a `pre_exec` hook on every command that
//! carries descriptors, and a command with such a hook does not return from
//! `spawn` until its child has reached exec or failed.
//!
//! Excluding writer-free spawns from each other is not incidental. `spawn`
//! itself hands the child a `FD_CLOEXEC` pipe and blocks reading it until exec
//! closes it, so two overlapping spawns inherit each other's pipe and each waits
//! for the other's child — the same inheritance, on the descriptor the standard
//! library uses to report exec failure. One at a time removes that too.
//!
//! The exclusion is scoped to this domain in both directions. Nothing outside
//! it creates a lease, and process creation outside it is not covered — other
//! modules of this binary run `git`, `cargo`, and a compiler under the same
//! test harness, and each of those forks can still capture a writer for the
//! length of its own pre-exec window.

mod lease;

use std::{
    error::Error,
    io::PipeWriter,
    process::{Child, Command},
    sync::{Mutex, PoisonError},
};

#[cfg(test)]
use std::sync::MutexGuard;

#[cfg(test)]
pub(crate) use lease::fail_next_process_lifetime_lease_creation;
pub(crate) use lease::{
    ProcessLeaseState, ProcessLifetimeLease, TargetLeaseState, TargetLifetimeLease,
};

/// Admits one process creation at a time, and holds off all of them for the
/// whole life of a lease writer — from the pipe to the last copy the spawned
/// command kept.
static PROCESS_CREATION: Mutex<()> = Mutex::new(());

/// Create a process that carries no process-lifetime lease writer.
pub(super) fn spawn_child(command: &mut Command) -> std::io::Result<Child> {
    let _creating = PROCESS_CREATION
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    command.spawn()
}

/// Create a process together with the lease that observes its lineage.
///
/// `build` receives the writer so it can place it in the child's descriptor
/// table. The writer, and every copy `build` left in the command it returned,
/// are closed before the exclusion is released.
pub(super) fn spawn_leased_child<Build>(
    build: Build,
) -> Result<(Child, ProcessLifetimeLease), Box<dyn Error>>
where
    Build: FnOnce(&PipeWriter) -> Result<Command, Box<dyn Error>>,
{
    let _creating = PROCESS_CREATION
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let (lease, writer) = ProcessLifetimeLease::create()?;
    // `command` is declared after `writer`, so it drops first: the descriptors
    // `build` cloned into it are gone before the writer, and both are gone
    // before the exclusion.
    let mut command = build(&writer)?;
    let child = command.spawn()?;
    Ok((child, lease))
}

/// A lease writer this process keeps itself, so a fixture can choose when the
/// lease releases instead of tying it to a child's exit.
///
/// The hold keeps the exclusion for as long as the writer exists, which is what
/// puts it under the same rule as a spawned one. Creating a process in this
/// domain while a hold is alive would deadlock; fixtures spawn first.
#[cfg(test)]
pub(super) struct LeaseWriterHold {
    // Declaration order is drop order: the writer closes, then the exclusion
    // opens, so no fork can be between the two.
    _writer: PipeWriter,
    _creating: MutexGuard<'static, ()>,
}

#[cfg(test)]
pub(super) fn hold_lease_writer() -> Result<(ProcessLifetimeLease, LeaseWriterHold), Box<dyn Error>>
{
    let creating = PROCESS_CREATION
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let (lease, writer) = ProcessLifetimeLease::create()?;
    Ok((
        lease,
        LeaseWriterHold {
            _writer: writer,
            _creating: creating,
        },
    ))
}
