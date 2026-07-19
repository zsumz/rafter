//! Process-group probing, signal delivery, and absence confirmation.

use std::error::Error;

#[cfg(test)]
use std::time::Duration;

#[cfg(all(test, unix))]
use rustix::process::test_kill_process_group;
#[cfg(unix)]
use rustix::{
    io::Errno,
    process::{kill_process_group, Pid, Signal},
};

#[cfg(test)]
use super::ProcessGroupState;
use super::{model::ProcessSignal, SignalDelivery};

#[cfg(test)]
thread_local! {
    static SIGNAL_ATTEMPTS: std::cell::RefCell<Vec<(u32, ProcessSignal)>> = const {
        std::cell::RefCell::new(Vec::new())
    };
    static FORCE_NEXT_GROUP_ABSENT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(unix)]
fn process_group_pid(pid: u32) -> Result<Pid, Box<dyn Error>> {
    let pid = i32::try_from(pid).map_err(|_| format!("process group ID exceeds i32: {pid}"))?;
    Pid::from_raw(pid).ok_or_else(|| format!("process group ID must be positive: {pid}").into())
}

#[cfg(unix)]
pub(crate) fn classify_signal_delivery(result: Result<(), Errno>) -> Result<SignalDelivery, Errno> {
    match result {
        Ok(()) => Ok(SignalDelivery::Sent),
        Err(Errno::SRCH) => Ok(SignalDelivery::GroupAbsent),
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
pub(crate) fn classify_signal_delivery(
    _result: Result<(), std::convert::Infallible>,
) -> Result<SignalDelivery, std::convert::Infallible> {
    unreachable!("signal delivery classification requires Unix")
}

#[cfg(test)]
pub(crate) fn process_group_state(pid: u32) -> Result<ProcessGroupState, Box<dyn Error>> {
    #[cfg(unix)]
    {
        return classify_process_group_probe(test_kill_process_group(process_group_pid(pid)?))
            .map_err(|error| format!("probe process group {pid}: {error}").into());
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err("process-group probing requires Unix".into())
    }
}

#[cfg(all(test, unix))]
pub(crate) fn classify_process_group_probe(
    result: Result<(), Errno>,
) -> Result<ProcessGroupState, Errno> {
    match result {
        Ok(()) | Err(Errno::PERM) => Ok(ProcessGroupState::Alive),
        Err(Errno::SRCH) => Ok(ProcessGroupState::Absent),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
pub(super) fn signal_process_group(
    pid: u32,
    signal: ProcessSignal,
) -> Result<SignalDelivery, Box<dyn Error>> {
    let process_group = process_group_pid(pid)?;
    let unix_signal = match signal {
        ProcessSignal::Term => Signal::TERM,
        ProcessSignal::Kill => Signal::KILL,
    };
    let signal_name = match signal {
        ProcessSignal::Term => "SIGTERM",
        ProcessSignal::Kill => "SIGKILL",
    };
    #[cfg(test)]
    SIGNAL_ATTEMPTS.with(|attempts| attempts.borrow_mut().push((pid, signal)));
    #[cfg(test)]
    let delivery = if FORCE_NEXT_GROUP_ABSENT.with(|force| force.replace(false)) {
        Err(Errno::SRCH)
    } else {
        kill_process_group(process_group, unix_signal)
    };
    #[cfg(not(test))]
    let delivery = kill_process_group(process_group, unix_signal);
    classify_signal_delivery(delivery)
        .map_err(|error| format!("send {signal_name} to process group {pid}: {error}").into())
}

#[cfg(test)]
pub(crate) fn force_next_signal_group_absent() {
    FORCE_NEXT_GROUP_ABSENT.with(|force| force.set(true));
}

#[cfg(test)]
pub(crate) fn clear_signal_attempts() {
    SIGNAL_ATTEMPTS.with(|attempts| attempts.borrow_mut().clear());
}

#[cfg(test)]
pub(crate) fn take_signal_attempts() -> Vec<(u32, ProcessSignal)> {
    SIGNAL_ATTEMPTS.with(|attempts| std::mem::take(&mut *attempts.borrow_mut()))
}

#[cfg(not(unix))]
pub(super) fn signal_process_group(
    _pid: u32,
    _signal: ProcessSignal,
) -> Result<SignalDelivery, Box<dyn Error>> {
    Err("process-group cleanup requires Unix".into())
}

#[cfg(test)]
pub(crate) fn confirm_process_group_absent_with(
    timeout: Duration,
    mut probe: impl FnMut() -> Result<ProcessGroupState, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() >= timeout {
            return Err(format!(
                "process group absence was not observed within {} ms",
                super::duration_ms(timeout)
            )
            .into());
        }
        match probe()? {
            ProcessGroupState::Absent => return Ok(()),
            ProcessGroupState::Alive => std::thread::sleep(super::PROCESS_POLL_INTERVAL),
        }
    }
}
