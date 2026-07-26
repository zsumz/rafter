//! Inherited process-lineage lifetime evidence.

use std::io::{PipeReader, PipeWriter, Read};

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_CREATION: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessLeaseState {
    Held,
    Released,
}

/// The read end of a lease inherited by a process and all of its descendants.
///
/// EOF proves that every inherited writer description has closed. Launched code
/// is required to preserve the writer unchanged across fork and exec.
///
/// A writer is only reachable through [`super::spawn_leased_child`], which is
/// what keeps unrelated forks from inheriting one: `create` is private to the
/// spawn module, so there is no way to open a lease outside the exclusion that
/// module holds.
#[derive(Debug)]
pub(crate) struct ProcessLifetimeLease {
    reader: PipeReader,
}

impl ProcessLifetimeLease {
    pub(super) fn create() -> Result<(Self, PipeWriter), Box<dyn std::error::Error>> {
        #[cfg(test)]
        if FAIL_NEXT_CREATION.with(|fail| fail.replace(false)) {
            return Err("injected process lifetime lease creation failure".into());
        }
        let (reader, writer) = std::io::pipe()?;
        let flags = rustix::fs::fcntl_getfl(&reader)?;
        rustix::fs::fcntl_setfl(&reader, flags | rustix::fs::OFlags::NONBLOCK)?;
        Ok((Self { reader }, writer))
    }

    pub(crate) fn observe(&self) -> Result<ProcessLeaseState, Box<dyn std::error::Error>> {
        let mut byte = [0_u8; 1];
        loop {
            match (&self.reader).read(&mut byte) {
                Ok(0) => return Ok(ProcessLeaseState::Released),
                Ok(_) => {
                    return Err(
                        "process lifetime lease carried data instead of remaining write-idle"
                            .into(),
                    )
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(ProcessLeaseState::Held)
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    return Err(format!("observe inherited process lifetime lease: {error}").into())
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn fail_next_process_lifetime_lease_creation() {
    FAIL_NEXT_CREATION.with(|fail| fail.set(true));
}

pub(crate) type TargetLeaseState = ProcessLeaseState;
pub(crate) type TargetLifetimeLease = ProcessLifetimeLease;
