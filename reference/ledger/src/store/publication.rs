//! The two write paths.
//!
//! An append extends the journal and a rewrite replaces it, because the new
//! content of a snapshot install or a compaction does not extend the old. Both
//! end at a durability barrier whose return is the commit point, and the byte
//! order inside the append is load bearing twice over — the unsealed mark goes
//! out first, and the one byte that seals the frame goes out after the barrier
//! that made every other byte durable. The `# Crash contract` section of the
//! [module documentation](super) is what that order buys.

use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use rafter::LogIndex;

use crate::Ledger;

use super::{
    domination::{deduplication_progress_of, verify_deduplication_dominates},
    error::LedgerStoreError,
    fault::{WriteFault, WriteFaultSite},
    format::{SEALED_FRAME_MARK, UNSEALED_FRAME_MARK},
    frame::{encode_frame, encode_header},
    journal_file::{staged_path, sync_directory},
    Health, LedgerStore,
};

use std::fs;

impl LedgerStore {
    /// Returns the byte length one commit of `ledger` at `applied_index` would
    /// append.
    ///
    /// Crash tests sweep every boundary inside that length, so they need it
    /// before they arm the fault that stops inside it.
    ///
    /// # Errors
    ///
    /// Returns an error when the image cannot be encoded or does not fit the
    /// frame's length field.
    pub fn planned_frame_len(
        ledger: &Ledger,
        applied_index: LogIndex,
    ) -> Result<u64, LedgerStoreError> {
        Ok(encode_frame(ledger, applied_index)?.len() as u64)
    }

    /// Commits one transaction, appending it to the journal.
    ///
    /// The transaction carries the whole application state — account balances,
    /// sessions, the deduplication cache with its cached command results, the
    /// deposit total — and `applied_index` together. `Ok` means every one of
    /// them is durable; nothing partial is ever recoverable.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` does
    /// not advance, when the image cannot be encoded, or when the append or its
    /// durability barrier fails. After any of the latter the handle is
    /// poisoned and the caller must reopen to learn what committed.
    pub fn commit(
        &mut self,
        ledger: &Ledger,
        applied_index: LogIndex,
    ) -> Result<(), LedgerStoreError> {
        self.check_health()?;
        // An append must advance the floor. A batch that applied nothing never
        // reaches here, so an index that does not advance is a caller error
        // rather than a no-op, and committing it would leave two frames the
        // same age with no rule for choosing between them.
        if applied_index <= self.applied_index {
            return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        let frame = encode_frame(ledger, applied_index)?;
        let plan = self.begin_plan();
        self.append(&frame, plan)?;

        self.journal_len += frame.len() as u64;
        self.adopt(ledger.clone(), applied_index);
        Ok(())
    }

    /// Replaces the journal with one frame holding `ledger` at `applied_index`.
    ///
    /// This is the publication a snapshot install and a compaction share: the
    /// new content does not extend the old, so it is staged beside the journal
    /// and renamed into place rather than appended. Unlike [`LedgerStore::commit`]
    /// it accepts an `applied_index` equal to the current one, because
    /// compacting in place must not require inventing a new index.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` would
    /// move the applied floor backwards, when republishing an unchanged applied
    /// index would move a client slot's deduplication state backwards, when the
    /// image cannot be encoded, or when staging, renaming, or a durability
    /// barrier fails.
    pub fn replace(
        &mut self,
        ledger: &Ledger,
        applied_index: LogIndex,
    ) -> Result<(), LedgerStoreError> {
        self.check_health()?;
        if applied_index < self.applied_index {
            return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        if applied_index == self.applied_index {
            // The one publication the applied floor cannot judge. Two images at
            // one index can still disagree about which requests have completed,
            // and the poorer one makes an acknowledged mutation executable a
            // second time — exactly what the deduplication cache exists to
            // prevent, reached through the store rather than through a replay.
            // Above this index the model has legitimately advanced and is the
            // authority on the sessions it retired, so the check is scoped to
            // here.
            verify_deduplication_dominates(
                &deduplication_progress_of(&self.ledger),
                &deduplication_progress_of(ledger),
            )?;
        }

        let mut contents = encode_header(self.config);
        contents.extend_from_slice(&encode_frame(ledger, applied_index)?);
        let plan = self.begin_plan();
        self.rewrite(&contents, plan)?;

        self.journal_len = contents.len() as u64;
        self.adopt(ledger.clone(), applied_index);
        Ok(())
    }

    /// Rewrites the journal down to its current state in one frame.
    ///
    /// The journal grows by a whole image per transaction, so a caller bounds
    /// it by compacting. Doing that at an application snapshot point is the
    /// natural pairing: the application has already declared that everything
    /// below its applied index is reconstructible from state rather than from
    /// history.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LedgerStore::replace`].
    pub fn compact(&mut self) -> Result<(), LedgerStoreError> {
        let ledger = self.ledger.clone();
        self.replace(&ledger, self.applied_index)
    }

    fn adopt(&mut self, ledger: Ledger, applied_index: LogIndex) {
        self.ledger = ledger;
        self.applied_index = applied_index;
    }

    fn check_health(&self) -> Result<(), LedgerStoreError> {
        if self.requires_reopen() {
            return Err(LedgerStoreError::StoreRequiresReopen);
        }
        Ok(())
    }

    /// Allocates the next write-plan ordinal.
    fn begin_plan(&mut self) -> u64 {
        self.write_plans += 1;
        self.write_plans
    }

    /// Records that a publication failed, poisoning the handle.
    fn publication_failed(&mut self, error: LedgerStoreError) -> LedgerStoreError {
        self.health = Health::ReopenRequired;
        error
    }

    /// Takes the fault armed for `plan`, if any, remembering that it fired.
    fn take_fault(&mut self, plan: u64, at: WriteFaultSite) -> Option<LedgerStoreError> {
        let fault = self.faults.fault_for(plan)?;
        if !at.matches(fault) {
            return None;
        }
        self.fired_fault = Some(fault);
        Some(LedgerStoreError::InjectedFault { fault, plan })
    }

    /// Appends `frame` to the journal unsealed, makes it durable, and only then
    /// seals it.
    ///
    /// The journal's directory entry was made durable when the file was
    /// created, so an append touches only the file.
    ///
    /// Two things about the order are load bearing, and both exist to make
    /// recovery's truncation rule provable rather than plausible:
    ///
    /// 1. **The frame's first byte goes out first, and goes out unsealed.** A
    ///    crash leaves a prefix of what was written, so every interrupted
    ///    append leaves `UNSEALED_FRAME_MARK` where the next frame begins.
    ///    That is the half of recovery's proof the write path is responsible
    ///    for; the opener supplies the other half by re-reading the tail with
    ///    the mark restored.
    /// 2. **The seal is one byte, written after the barrier below returned.**
    ///    Nothing that follows the barrier can reach the medium before the
    ///    frame it seals, and a single byte cannot be half written, so a frame
    ///    is either committed or not.
    ///
    /// The file is opened for writing at an explicit offset rather than in
    /// append mode, because the seal goes back to the frame's own first byte.
    /// Recovery leaves the journal exactly its committed length, so that offset
    /// is the end of the file.
    fn append(&mut self, frame: &[u8], plan: u64) -> Result<(), LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }

        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.journal_path)
            .map_err(|source| LedgerStoreError::Io {
                operation: "open the ledger journal for append",
                path: self.journal_path.clone(),
                source,
            })?;

        let journal_path = self.journal_path.clone();
        let offset = self.journal_len;
        self.seek(&mut file, offset, &journal_path)?;

        let mut unsealed = frame.to_vec();
        unsealed[0] = UNSEALED_FRAME_MARK;
        let emitted = self.emit(&mut file, &unsealed, plan, &journal_path)?;
        if emitted < frame.len() {
            let error = self
                .take_fault(plan, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtFileSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the appended ledger transaction",
                path: journal_path.clone(),
                source,
            })
        })?;

        self.seal_frame(&mut file, offset, plan, &journal_path)
    }

    /// Replaces an appended frame's unsealed mark with the sealed one.
    ///
    /// This is the commit point. Everything before it is a tail a later opener
    /// may truncate; everything after it is a frame a later opener must be able
    /// to read or refuse over.
    fn seal_frame(
        &mut self,
        file: &mut File,
        offset: u64,
        plan: u64,
        path: &Path,
    ) -> Result<(), LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeSeal) {
            return Err(self.publication_failed(error));
        }
        self.seek(file, offset, path)?;
        file.write_all(&[SEALED_FRAME_MARK]).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "seal the appended ledger transaction",
                path: path.to_path_buf(),
                source,
            })
        })?;

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtSealSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the sealed ledger transaction",
                path: path.to_path_buf(),
                source,
            })
        })
    }

    fn seek(&mut self, file: &mut File, offset: u64, path: &Path) -> Result<(), LedgerStoreError> {
        file.seek(SeekFrom::Start(offset))
            .map(|_| ())
            .map_err(|source| {
                self.publication_failed(LedgerStoreError::Io {
                    operation: "seek within the ledger journal",
                    path: path.to_path_buf(),
                    source,
                })
            })
    }

    /// Stages `contents`, syncs it, renames it over the journal, and syncs the
    /// directory.
    fn rewrite(&mut self, contents: &[u8], plan: u64) -> Result<(), LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }

        let staged_path = staged_path(&self.directory);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staged_path)
            .map_err(|source| LedgerStoreError::Io {
                operation: "open the staged ledger journal",
                path: staged_path.clone(),
                source,
            })?;

        let emitted = self.emit(&mut file, contents, plan, &staged_path)?;
        if emitted < contents.len() {
            let error = self
                .take_fault(plan, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtFileSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the staged ledger journal",
                path: staged_path.clone(),
                source,
            })
        })?;
        drop(file);

        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeRename) {
            return Err(self.publication_failed(error));
        }
        fs::rename(&staged_path, &self.journal_path).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "publish the staged ledger journal",
                path: self.journal_path.clone(),
                source,
            })
        })?;

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AfterRename) {
            return Err(self.publication_failed(error));
        }
        sync_directory(&self.directory).map_err(|error| self.publication_failed(error))
    }

    /// Writes `bytes` to `file`, honoring a byte-boundary fault.
    ///
    /// Returns how many bytes were emitted; a short return means a fault
    /// stopped the plan, and the prefix was synced so recovery meets the worst
    /// case where it reached the medium.
    fn emit(
        &mut self,
        file: &mut File,
        bytes: &[u8],
        plan: u64,
        path: &Path,
    ) -> Result<usize, LedgerStoreError> {
        let limit = match self.faults.fault_for(plan) {
            Some(WriteFault::AfterBytes(stop)) => {
                usize::try_from(stop).unwrap_or(usize::MAX).min(bytes.len())
            }
            _ => bytes.len(),
        };

        file.write_all(&bytes[..limit]).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "write the ledger journal",
                path: path.to_path_buf(),
                source,
            })
        })?;
        if limit < bytes.len() {
            file.sync_data().map_err(|source| {
                self.publication_failed(LedgerStoreError::Io {
                    operation: "sync an interrupted ledger write",
                    path: path.to_path_buf(),
                    source,
                })
            })?;
        }
        Ok(limit)
    }
}
