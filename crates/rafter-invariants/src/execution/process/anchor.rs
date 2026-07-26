//! Parent-owned process-group anchor with explicit readiness and release.

use std::{
    error::Error,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::{Command, ExitStatus, Stdio},
    time::Instant,
};

#[cfg(unix)]
use command_fds::{CommandFdExt, FdMapping};
#[cfg(unix)]
use std::os::{
    fd::{AsFd, AsRawFd},
    unix::process::CommandExt,
};

use super::{
    base_environment, spawn_child, DirectChild, NoSignalReaper, ProcessSignal, RuntimeExecutable,
    SignalDelivery, TargetLifetimeLease,
};

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_STARTUP: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static LAST_FAILED_STARTUP_ID: std::cell::Cell<Option<u32>> = const {
        std::cell::Cell::new(None)
    };
    static LAST_SPAWNED_ID: std::cell::Cell<Option<u32>> = const {
        std::cell::Cell::new(None)
    };
    static EXPIRE_NEXT_READINESS_CLASSIFICATION: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

const ANCHOR_CONTROL_FD_ENV: &str = "RAFTER_INVARIANT_ANCHOR_CONTROL_FD";
const ANCHOR_PROGRAM: &str = r#"
my $control_fd = delete $ENV{'RAFTER_INVARIANT_ANCHOR_CONTROL_FD'};
open(my $control, '+<&=', $control_fd) or die "open anchor control: $!";
$SIG{'TERM'} = 'IGNORE';
syswrite($control, 'A', 1) == 1 or die "publish anchor readiness: $!";
my $release = '';
sysread($control, $release, 1) == 1 && $release eq 'F'
    or die "read anchor release: $!";
close($control) or die "close anchor control: $!";
exit 0;
"#;

#[derive(Debug)]
pub(crate) struct ProcessGroupAnchor {
    child: DirectChild,
    control: Option<UnixStream>,
}

impl ProcessGroupAnchor {
    pub(crate) fn spawn(
        runtime: RuntimeExecutable<'_>,
        readiness_deadline: Instant,
        reaper: NoSignalReaper,
        stderr: Stdio,
    ) -> Result<Self, Box<dyn Error>> {
        let (control, child_control) = UnixStream::pair()?;
        let mut command = Command::new(runtime.path);
        command
            .arg("-e")
            .arg(anchor_program())
            .env_clear()
            .envs(base_environment())
            .env(ANCHOR_CONTROL_FD_ENV, child_control.as_raw_fd().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr);
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(unix)]
        command.fd_mappings(vec![
            FdMapping {
                parent_fd: runtime.descriptor.try_clone_to_owned()?,
                child_fd: runtime.descriptor.as_raw_fd(),
            },
            FdMapping {
                parent_fd: child_control.as_fd().try_clone_to_owned()?,
                child_fd: child_control.as_raw_fd(),
            },
        ])?;
        let child = spawn_child(&mut command)?;
        drop(child_control);
        let mut anchor = Self {
            child: DirectChild::new(child, reaper),
            control: Some(control),
        };
        #[cfg(test)]
        LAST_SPAWNED_ID.with(|last| last.set(Some(anchor.id())));
        if let Err(readiness) = anchor.await_ready(readiness_deadline) {
            #[cfg(test)]
            LAST_FAILED_STARTUP_ID.with(|last| last.set(Some(anchor.id())));
            let detail = match anchor.child.quarantine("target-group anchor") {
                Ok(_) => format!("await process-group anchor readiness: {readiness}"),
                Err(cleanup) => format!(
                    "await process-group anchor readiness: {readiness}; quarantine failed anchor startup: {cleanup}"
                ),
            };
            return Err(detail.into());
        }
        Ok(anchor)
    }

    pub(crate) fn id(&self) -> u32 {
        self.child.id()
    }

    pub(crate) fn is_owned(&self) -> bool {
        self.child.is_owned()
    }

    pub(crate) fn signal(
        &mut self,
        signal: ProcessSignal,
    ) -> Result<SignalDelivery, Box<dyn Error>> {
        self.child.signal_group(signal)
    }

    pub(crate) fn signal_was_sent(&self, signal: ProcessSignal) -> bool {
        self.child.signal_was_sent(signal)
    }

    #[cfg(test)]
    pub(crate) fn record_signal_for_test(&mut self, signal: ProcessSignal) {
        self.child.record_signal_for_test(signal);
    }

    pub(crate) fn release(&mut self, deadline: Instant) -> Result<ExitStatus, Box<dyn Error>> {
        self.control
            .as_mut()
            .ok_or("process-group anchor control was already transferred")?
            .write_all(b"F")?;
        self.wait_until(deadline)?
            .ok_or_else(|| "process-group anchor did not exit after release".into())
    }

    pub(crate) fn exit_observed(&self) -> Result<bool, Box<dyn Error>> {
        self.child.exit_observed()
    }

    pub(crate) fn wait_until(&mut self, deadline: Instant) -> std::io::Result<Option<ExitStatus>> {
        self.child.wait_until(deadline)
    }

    pub(crate) fn quarantine_until_target_exit(
        &mut self,
        lifetime: TargetLifetimeLease,
    ) -> Result<bool, (TargetLifetimeLease, Box<dyn Error>)> {
        let Some(control) = self.control.take() else {
            return Err((
                lifetime,
                "process-group anchor control was already transferred".into(),
            ));
        };
        match self.child.quarantine_anchored_group(control, lifetime) {
            Ok(adopted) => Ok(adopted),
            Err((control, lifetime, error)) => {
                self.control = Some(control);
                Err((lifetime, error))
            }
        }
    }

    fn await_ready(&mut self, deadline: Instant) -> Result<(), Box<dyn Error>> {
        let started = Instant::now();
        if started >= deadline {
            return Err("process-group anchor readiness deadline expired".into());
        }
        let remaining = deadline.duration_since(started);
        let control = self
            .control
            .as_mut()
            .ok_or("process-group anchor control was already transferred")?;
        control.set_read_timeout(Some(remaining))?;
        let mut ready = [0_u8; 1];
        control.read_exact(&mut ready)?;
        control.set_read_timeout(None)?;
        if readiness_classification_time(deadline) >= deadline {
            return Err("process-group anchor readiness deadline expired".into());
        }
        if ready != *b"A" {
            return Err("process-group anchor published malformed readiness".into());
        }
        Ok(())
    }
}

fn readiness_classification_time(deadline: Instant) -> Instant {
    #[cfg(test)]
    if EXPIRE_NEXT_READINESS_CLASSIFICATION.with(|expire| expire.replace(false)) {
        return deadline;
    }
    #[cfg(not(test))]
    let _ = deadline;
    Instant::now()
}

fn anchor_program() -> &'static str {
    #[cfg(test)]
    if FAIL_NEXT_STARTUP.with(|fail| fail.replace(false)) {
        return r#"print STDERR "injected anchor startup failure\n"; exit 72;"#;
    }
    ANCHOR_PROGRAM
}

#[cfg(test)]
pub(crate) fn fail_next_anchor_startup() {
    FAIL_NEXT_STARTUP.with(|fail| fail.set(true));
}

#[cfg(test)]
pub(crate) fn take_last_failed_anchor_startup_id() -> Option<u32> {
    LAST_FAILED_STARTUP_ID.with(std::cell::Cell::take)
}

#[cfg(test)]
pub(crate) fn expire_next_anchor_readiness_classification() {
    EXPIRE_NEXT_READINESS_CLASSIFICATION.with(|expire| expire.set(true));
}

#[cfg(test)]
pub(crate) fn take_last_spawned_anchor_id() -> Option<u32> {
    LAST_SPAWNED_ID.with(std::cell::Cell::take)
}

impl Drop for ProcessGroupAnchor {
    fn drop(&mut self) {
        let _ = self.child.quarantine("target-group anchor");
    }
}
