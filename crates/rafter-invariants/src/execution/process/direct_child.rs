//! Direct-child ownership that keeps a numeric process-group identity signal-safe.

use std::{
    os::unix::net::UnixStream,
    process::{Child, ChildStderr, ChildStdout, ExitStatus},
    time::Instant,
};

use super::{
    signal::signal_process_group, NoSignalReaper, ProcessLifetimeLease, ProcessSignal,
    SignalDelivery, TargetLifetimeLease, PROCESS_POLL_INTERVAL,
};

#[derive(Debug)]
enum ChildState {
    Owned(Child),
    Reaped(ExitStatus),
    Quarantined,
}

/// Owns the direct child whose unreaped PID anchors its process-group identity.
#[derive(Debug)]
pub(crate) struct DirectChild {
    id: u32,
    state: ChildState,
    term_sent: bool,
    kill_sent: bool,
    reaper: NoSignalReaper,
}

impl DirectChild {
    pub(crate) fn new(child: Child, reaper: NoSignalReaper) -> Self {
        Self {
            id: child.id(),
            state: ChildState::Owned(child),
            term_sent: false,
            kill_sent: false,
            reaper,
        }
    }

    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    pub(crate) fn is_owned(&self) -> bool {
        matches!(self.state, ChildState::Owned(_))
    }

    pub(crate) fn is_reaped(&self) -> bool {
        matches!(self.state, ChildState::Reaped(_))
    }

    pub(crate) fn is_quarantined(&self) -> bool {
        matches!(self.state, ChildState::Quarantined)
    }

    pub(crate) fn signal_was_sent(&self, signal: ProcessSignal) -> bool {
        match signal {
            ProcessSignal::Term => self.term_sent,
            ProcessSignal::Kill => self.kill_sent,
        }
    }

    pub(crate) fn signal_group(
        &mut self,
        signal: ProcessSignal,
    ) -> Result<SignalDelivery, Box<dyn std::error::Error>> {
        if !self.is_owned() {
            return Ok(SignalDelivery::GroupAbsent);
        }
        if self.kill_sent || signal == ProcessSignal::Term && self.term_sent {
            return Ok(SignalDelivery::AlreadySent);
        }
        let delivery = signal_process_group(self.id, signal)?;
        if delivery == SignalDelivery::GroupAbsent {
            return Err(format!(
                "owned direct-child process group {} became unsignalable before reap",
                self.id
            )
            .into());
        }
        if delivery == SignalDelivery::Sent {
            match signal {
                ProcessSignal::Term => self.term_sent = true,
                ProcessSignal::Kill => self.kill_sent = true,
            }
        }
        Ok(delivery)
    }

    #[cfg(test)]
    pub(crate) fn record_signal_for_test(&mut self, signal: ProcessSignal) {
        match signal {
            ProcessSignal::Term => self.term_sent = true,
            ProcessSignal::Kill => self.kill_sent = true,
        }
    }

    #[cfg(test)]
    pub(crate) fn replace_numeric_identity_for_test(&mut self, id: u32) {
        self.id = id;
    }

    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        match &mut self.state {
            ChildState::Owned(child) => child.stdout.take(),
            ChildState::Reaped(_) | ChildState::Quarantined => None,
        }
    }

    pub(crate) fn take_stderr(&mut self) -> Option<ChildStderr> {
        match &mut self.state {
            ChildState::Owned(child) => child.stderr.take(),
            ChildState::Reaped(_) | ChildState::Quarantined => None,
        }
    }

    pub(crate) fn exit_observed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let ChildState::Owned(_) = self.state else {
            return Ok(self.is_reaped());
        };
        let raw = i32::try_from(self.id)?;
        let pid = rustix::process::Pid::from_raw(raw)
            .ok_or("direct child process ID must be positive")?;
        Ok(rustix::process::waitid(
            rustix::process::WaitId::Pid(pid),
            rustix::process::WaitIdOptions::EXITED
                | rustix::process::WaitIdOptions::NOHANG
                | rustix::process::WaitIdOptions::NOWAIT,
        )
        .map_err(|error| {
            format!(
                "observe direct child {} exit without reaping: {error}",
                self.id
            )
        })?
        .is_some())
    }

    pub(crate) fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        match &mut self.state {
            ChildState::Owned(child) => {
                let status = child.try_wait()?;
                if let Some(status) = status {
                    self.state = ChildState::Reaped(status);
                }
                Ok(status)
            }
            ChildState::Reaped(status) => Ok(Some(*status)),
            ChildState::Quarantined => Ok(None),
        }
    }

    pub(crate) fn wait_until(&mut self, deadline: Instant) -> std::io::Result<Option<ExitStatus>> {
        loop {
            if self.is_quarantined() {
                return Ok(None);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            std::thread::sleep(PROCESS_POLL_INTERVAL.min(deadline.duration_since(now)));
        }
    }

    pub(crate) fn quarantine(
        &mut self,
        role: &'static str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let state = std::mem::replace(&mut self.state, ChildState::Quarantined);
        match state {
            ChildState::Owned(child) => match self.reaper.adopt(child, role) {
                Ok(()) => Ok(true),
                Err((child, error)) => {
                    self.state = ChildState::Owned(child);
                    Err(error.into())
                }
            },
            ChildState::Reaped(status) => {
                self.state = ChildState::Reaped(status);
                Ok(false)
            }
            ChildState::Quarantined => Ok(false),
        }
    }

    pub(crate) fn quarantine_anchored_group(
        &mut self,
        control: UnixStream,
        lifetime: TargetLifetimeLease,
    ) -> Result<bool, (UnixStream, TargetLifetimeLease, Box<dyn std::error::Error>)> {
        let state = std::mem::replace(&mut self.state, ChildState::Quarantined);
        match state {
            ChildState::Owned(child) => {
                match self.reaper.adopt_anchored_group(child, control, lifetime) {
                    Ok(()) => Ok(true),
                    Err((child, control, lifetime, error)) => {
                        self.state = ChildState::Owned(child);
                        Err((control, lifetime, error.into()))
                    }
                }
            }
            ChildState::Reaped(status) => {
                self.state = ChildState::Reaped(status);
                Ok(false)
            }
            ChildState::Quarantined => Ok(false),
        }
    }

    pub(crate) fn quarantine_leased(
        &mut self,
        lifetime: ProcessLifetimeLease,
    ) -> Result<bool, (ProcessLifetimeLease, Box<dyn std::error::Error>)> {
        let state = std::mem::replace(&mut self.state, ChildState::Quarantined);
        match state {
            ChildState::Owned(child) => match self.reaper.adopt_leased(child, lifetime) {
                Ok(()) => Ok(true),
                Err((child, lifetime, error)) => {
                    self.state = ChildState::Owned(child);
                    Err((lifetime, error.into()))
                }
            },
            ChildState::Reaped(status) => {
                self.state = ChildState::Reaped(status);
                Ok(false)
            }
            ChildState::Quarantined => Ok(false),
        }
    }
}
