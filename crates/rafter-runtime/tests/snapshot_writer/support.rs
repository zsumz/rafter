//! Observable staging sink with partial writes, retryable errors, and fatal faults.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_runtime::snapshot_io::SnapshotCancellation;
use std::{cell::Cell, io, rc::Rc};

#[derive(Clone, Copy, Debug, Default)]
pub enum Fault {
    #[default]
    None,
    Zero,
    Overreported,
    Error,
    Interrupted,
    Cancel,
    CancelInterrupted,
    Panic,
}

#[derive(Debug)]
pub struct Sink {
    pub bytes: Vec<u8>,
    pub writes: usize,
    pub flushes: usize,
    pub largest: usize,
    pub partial: usize,
    pub write_fault: Rc<Cell<Fault>>,
    pub flush_fault: Rc<Cell<Fault>>,
    pub cancel: SnapshotCancellation,
}

impl Default for Sink {
    fn default() -> Self {
        Self {
            bytes: Vec::new(),
            writes: 0,
            flushes: 0,
            largest: 0,
            partial: 3,
            write_fault: Rc::default(),
            flush_fault: Rc::default(),
            cancel: SnapshotCancellation::new(),
        }
    }
}

impl Sink {
    fn fault(&self, fault: &Cell<Fault>) -> io::Result<()> {
        match fault.replace(Fault::None) {
            Fault::Error => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sink denied",
            )),
            Fault::Interrupted => Err(io::Error::from(io::ErrorKind::Interrupted)),
            Fault::Cancel => {
                self.cancel.cancel();
                Ok(())
            }
            Fault::CancelInterrupted => {
                self.cancel.cancel();
                Err(io::Error::from(io::ErrorKind::Interrupted))
            }
            Fault::Panic => panic!("injected sink panic"),
            _ => Ok(()),
        }
    }
}

impl io::Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        self.largest = self.largest.max(bytes.len());
        match self.write_fault.get() {
            Fault::Zero => return Ok(0),
            Fault::Overreported => return Ok(bytes.len() + 1),
            _ => self.fault(&self.write_fault)?,
        }
        let count = bytes.len().min(self.partial);
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        self.fault(&self.flush_fault)
    }
}

pub fn metadata() -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("writer-tests").unwrap(),
        NodeId(1),
        LogIndex(42),
        Term(3),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("opaque").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap()
}
