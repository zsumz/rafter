//! Bounded staging preserves the existing rename publication and fault boundaries.

use std::fs::{self, OpenOptions};

use crate::adapter::codec::SnapshotEncodingError;
use crate::store::{
    fault::WriteFaultSite,
    format::HEADER_LEN,
    frame_output::FramePlan,
    journal_file::{staged_path, sync_directory},
    LedgerStore, LedgerStoreError,
};

use super::output::{write_staged, FileSink};

impl LedgerStore {
    pub(super) fn rewrite(
        &mut self,
        frame: &FramePlan<'_>,
        plan: u64,
    ) -> Result<u64, LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }
        let path = staged_path(&self.directory);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .map_err(|source| LedgerStoreError::Io {
                operation: "open the staged ledger journal",
                path: path.clone(),
                source,
            })?;
        let mut sink = FileSink::new(
            &mut file,
            self.faults.fault_for(plan),
            "write the staged ledger journal",
        );
        if let Err(error) = write_staged(&mut sink, self.config, frame) {
            let error = if sink.injected {
                self.take_fault(plan, WriteFaultSite::AfterBytes)
                    .expect("only an armed byte boundary can interrupt staging")
            } else {
                match error {
                    SnapshotEncodingError::Codec(error) => LedgerStoreError::Image(error),
                    SnapshotEncodingError::Sink(source) => LedgerStoreError::Io {
                        operation: sink.operation,
                        path: path.clone(),
                        source,
                    },
                }
            };
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtFileSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the staged ledger journal",
                path: path.clone(),
                source,
            })
        })?;
        drop(file);

        #[cfg(all(test, unix))]
        super::super::process_test::pause(super::super::process_test::Point::RewriteAfterTempSync);

        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeRename) {
            return Err(self.publication_failed(error));
        }
        fs::rename(&path, &self.journal_path).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "publish the staged ledger journal",
                path: self.journal_path.clone(),
                source,
            })
        })?;

        #[cfg(all(test, unix))]
        super::super::process_test::pause(super::super::process_test::Point::RewriteAfterRename);

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AfterRename) {
            return Err(self.publication_failed(error));
        }
        sync_directory(&self.directory).map_err(|error| self.publication_failed(error))?;

        #[cfg(all(test, unix))]
        super::super::process_test::pause(
            super::super::process_test::Point::RewriteAfterDirectorySync,
        );

        Ok(HEADER_LEN as u64 + frame.len())
    }
}
