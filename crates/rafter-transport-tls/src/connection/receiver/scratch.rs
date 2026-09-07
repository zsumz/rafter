//! Connection-lifetime accounting for canonical group scratch storage.

use std::collections::TryReserveError;

use crate::queue::{ReceiveMemoryBudget, ReceiveMemoryPermit};
use crate::{GroupIdCodec, PeerFrameCodec, PeerFrameScratch};

pub(super) struct ReceiverScratch {
    frame: PeerFrameScratch,
    _declared_memory: ReceiveMemoryPermit,
    _allocator_excess_memory: Option<ReceiveMemoryPermit>,
}

#[derive(Debug)]
pub(super) enum ReceiverScratchError {
    MemoryFull,
    Allocation(TryReserveError),
}

impl ReceiverScratch {
    pub(super) fn acquire<G, C>(
        memory: &ReceiveMemoryBudget,
        codec: &PeerFrameCodec<G, C>,
    ) -> Result<Self, ReceiverScratchError>
    where
        C: GroupIdCodec<G>,
    {
        let declared = codec.max_encoded_group_bytes();
        let declared_memory = memory
            .try_acquire_scratch(declared)
            .map_err(|_| ReceiverScratchError::MemoryFull)?;
        let frame = PeerFrameScratch::try_with_group_id_capacity(declared)
            .map_err(ReceiverScratchError::Allocation)?;
        let allocator_excess = frame.group_id_capacity().saturating_sub(declared);
        let allocator_excess_memory = if allocator_excess == 0 {
            None
        } else {
            Some(
                memory
                    .try_acquire_scratch(allocator_excess)
                    .map_err(|_| ReceiverScratchError::MemoryFull)?,
            )
        };
        Ok(Self {
            frame,
            _declared_memory: declared_memory,
            _allocator_excess_memory: allocator_excess_memory,
        })
    }

    pub(super) const fn frame_mut(&mut self) -> &mut PeerFrameScratch {
        &mut self.frame
    }
}

#[cfg(test)]
#[path = "scratch_test.rs"]
mod tests;
