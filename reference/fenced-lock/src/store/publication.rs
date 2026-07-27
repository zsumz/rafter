//! The one write path.
//!
//! Applying a batch and installing a snapshot are the same publication with
//! different applied-index rules, so there is one crash argument to audit
//! instead of two. The byte order inside it is load bearing twice over — the
//! unsealed mark goes out first, and the one byte that seals the image goes out
//! after the barrier that made every other byte durable — and the
//! `# Crash contract` section of the [module documentation](super) is what that
//! order buys.

use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use rafter::LogIndex;

use crate::{LockConfig, LockService};

use super::{
    domination::{
        marks_of, session_progress_of, verify_marks_dominate, verify_session_cache_dominates,
    },
    error::LockStoreError,
    fault::{WriteFault, WriteFaultSite},
    format::{as_u64, SlotIndex, SEALED_MARK, UNSEALED_MARK},
    image::encode_image,
    slot_file::slot_path,
    Health, LockStore,
};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::damage::SlotDamage;

impl LockStore {
    /// Returns the byte length one publication of `service` at `applied_index`
    /// would write.
    ///
    /// Crash tests sweep every boundary inside that length, so they need it
    /// before they arm the fault that stops inside it.
    ///
    /// The generation is a fixed-width header field, so the length does not
    /// depend on which one a publication would assign and this does not need to
    /// know. That is why it can be answered without a store.
    ///
    /// # Errors
    ///
    /// Returns an error when the image cannot be encoded or does not fit the
    /// slot header's length field.
    pub fn planned_image_len(
        config: LockConfig,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<u64, LockStoreError> {
        Ok(as_u64(
            encode_image(config, service, applied_index, 1)?.len(),
        ))
    }

    /// Commits one transaction, publishing it into the stale slot.
    ///
    /// The transaction carries the whole application state — the lock table,
    /// every high-water mark, sessions with their cached operations,
    /// fingerprints, and results, and the replicated logical time — together
    /// with `applied_index`. `Ok` means every one of them is durable; nothing
    /// partial is ever recoverable.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` does
    /// not advance, when the state would lower a fencing high-water mark, when
    /// the image cannot be encoded, or when the write or its durability barrier
    /// fails. After any of the latter the handle is poisoned and the caller must
    /// reopen to learn what committed.
    pub fn commit(
        &mut self,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<(), LockStoreError> {
        self.check_health()?;
        // A commit must advance the floor. A batch that applied nothing never
        // reaches here, so an index that does not advance is a caller error
        // rather than a no-op.
        if applied_index <= self.applied_index {
            return Err(LockStoreError::AppliedIndexRegression {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        self.publish(service, applied_index)
    }

    /// Publishes an installed snapshot's state into the stale slot.
    ///
    /// Unlike [`LockStore::commit`] this accepts an `applied_index` equal to
    /// the current one, because installing the state a replica already holds
    /// must not require inventing a new index. It is otherwise the same
    /// publication, byte for byte and crash window for crash window.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` would
    /// move the applied floor backwards, when the state would lower a fencing
    /// high-water mark, when republishing an unchanged applied index would move
    /// a session cache backwards, when the image cannot be encoded, or when the
    /// write or its durability barrier fails.
    pub fn install(
        &mut self,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<(), LockStoreError> {
        self.check_health()?;
        if applied_index < self.applied_index {
            return Err(LockStoreError::AppliedIndexRegression {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        if applied_index == self.applied_index {
            // The one publication the applied floor cannot judge. Two images at
            // one index can still disagree about which requests have completed,
            // and the poorer one makes an acknowledged acquisition executable
            // again — a second fencing token for one tenure. Above this index
            // the model is the authority on the sessions it retired along the
            // way, so the check is scoped to exactly here.
            verify_session_cache_dominates(
                &session_progress_of(&self.service),
                &session_progress_of(service),
            )?;
        }
        self.publish(service, applied_index)
    }

    /// The one write path: check, encode, write the stale slot, sync, adopt.
    fn publish(
        &mut self,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<(), LockStoreError> {
        let proposed = marks_of(service);
        // Refused before a byte is written: a state that loses a mark is not
        // one this store will make durable, whatever produced it.
        verify_marks_dominate(&self.acknowledged_marks, &proposed)?;

        let generation = self.generation + 1;
        let slot = self.next_slot();
        let image = encode_image(self.config, service, applied_index, generation)?;
        let publication = self.begin_publication();
        self.write_slot(slot, &image, publication)?;

        self.service = service.clone();
        self.applied_index = applied_index;
        self.generation = generation;
        self.live_slot = Some(slot);
        self.acknowledged_marks = proposed;
        Ok(())
    }

    fn check_health(&self) -> Result<(), LockStoreError> {
        if self.requires_reopen() {
            return Err(LockStoreError::StoreRequiresReopen);
        }
        Ok(())
    }

    /// Allocates the next publication ordinal.
    fn begin_publication(&mut self) -> u64 {
        self.publications += 1;
        self.publications
    }

    /// Records that a publication failed, poisoning the handle.
    fn publication_failed(&mut self, error: LockStoreError) -> LockStoreError {
        self.health = Health::ReopenRequired;
        error
    }

    /// Takes the fault armed for `publication`, if any, remembering that it
    /// fired.
    fn take_fault(&mut self, publication: u64, at: WriteFaultSite) -> Option<LockStoreError> {
        let fault = self.faults.fault_for(publication)?;
        if !at.matches(fault) {
            return None;
        }
        self.fired_fault = Some(fault);
        Some(LockStoreError::InjectedFault { fault, publication })
    }

    /// Writes `image` into the stale slot unsealed, makes it durable, and only
    /// then seals it.
    ///
    /// The slot being written is never the authoritative one, so every failure
    /// below leaves the live image whole. The directory entry was made durable
    /// at open, so nothing here touches the directory.
    ///
    /// Two things about the order are load bearing, and both exist to make
    /// recovery's skip rule provable rather than plausible:
    ///
    /// 1. **Byte zero goes out first, and goes out unsealed.** A crash leaves a
    ///    prefix of what was written, so every interrupted publication leaves
    ///    `UNSEALED_MARK` in the slot's first byte. That is the half of
    ///    recovery's proof the write path is responsible for; the opener
    ///    supplies the other half by re-reading the slot with the mark
    ///    restored.
    /// 2. **The seal is one byte, written after the barrier below returned.**
    ///    Nothing that follows the barrier can reach the medium before the
    ///    image it seals, and a single byte cannot be half written, so a slot
    ///    is either sealed or not.
    ///
    /// The slot is cut back to the new image's length before the barrier rather
    /// than truncated to nothing before the first byte. Truncating first would
    /// leave an empty file in the crash window, and an empty file is the one
    /// artifact this store must be able to call damage — see
    /// [`SlotDamage::SlotEmptied`].
    fn write_slot(
        &mut self,
        slot: SlotIndex,
        image: &[u8],
        publication: u64,
    ) -> Result<(), LockStoreError> {
        if let Some(error) = self.take_fault(publication, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }

        let path = slot_path(&self.directory, slot);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "open a lock store slot for publication",
                    path: path.clone(),
                    source,
                })
            })?;

        let mut unsealed = image.to_vec();
        unsealed[0] = UNSEALED_MARK;
        let emitted = self.emit(&mut file, &unsealed, publication, &path)?;
        if emitted < image.len() {
            let error = self
                .take_fault(publication, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        // Cutting back is what keeps a shorter image from inheriting a longer
        // one's tail. It is safe precisely because this slot is the stale one,
        // and it happens while the slot is still marked unsealed.
        file.set_len(as_u64(image.len())).map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "resize a lock store slot for publication",
                path: path.clone(),
                source,
            })
        })?;

        if let Some(error) = self.take_fault(publication, WriteFaultSite::AtSlotSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "sync a published lock store slot",
                path: path.clone(),
                source,
            })
        })?;

        self.seal_slot(&mut file, publication, &path)
    }

    /// Replaces a written slot's unsealed mark with the sealed one.
    ///
    /// This is the commit point. Everything before it is a slot a later opener
    /// may skip; everything after it is a slot a later opener must be able to
    /// read or refuse over.
    fn seal_slot(
        &mut self,
        file: &mut File,
        publication: u64,
        path: &Path,
    ) -> Result<(), LockStoreError> {
        if let Some(error) = self.take_fault(publication, WriteFaultSite::BeforeSeal) {
            return Err(self.publication_failed(error));
        }
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&[SEALED_MARK]))
            .map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "seal a published lock store slot",
                    path: path.to_path_buf(),
                    source,
                })
            })?;

        if let Some(error) = self.take_fault(publication, WriteFaultSite::AtSealSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "sync a sealed lock store slot",
                path: path.to_path_buf(),
                source,
            })
        })
    }

    /// Writes `bytes` to `file`, honoring a byte-boundary fault.
    ///
    /// Returns how many bytes were emitted; a short return means a fault
    /// stopped the publication, and the prefix was synced so recovery meets the
    /// worst case where it reached the medium.
    fn emit(
        &mut self,
        file: &mut File,
        bytes: &[u8],
        publication: u64,
        path: &Path,
    ) -> Result<usize, LockStoreError> {
        let limit = match self.faults.fault_for(publication) {
            Some(WriteFault::AfterBytes(stop)) => {
                usize::try_from(stop).unwrap_or(usize::MAX).min(bytes.len())
            }
            _ => bytes.len(),
        };

        file.write_all(&bytes[..limit]).map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "write a lock store slot",
                path: path.to_path_buf(),
                source,
            })
        })?;
        if limit < bytes.len() {
            file.sync_data().map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "sync an interrupted lock store write",
                    path: path.to_path_buf(),
                    source,
                })
            })?;
        }
        Ok(limit)
    }
}
