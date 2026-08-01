//! Exact frame and byte accounting shared by bounded queues.

/// Current count-and-byte use.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct QueueUsage {
    pub(crate) frames: usize,
    pub(crate) bytes: usize,
}

impl QueueUsage {
    pub(crate) fn can_add(self, bytes: usize, max_frames: usize, max_bytes: usize) -> bool {
        self.frames < max_frames
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|next| next <= max_bytes)
    }

    pub(crate) fn added(self, bytes: usize) -> Option<Self> {
        Some(Self {
            frames: self.frames.checked_add(1)?,
            bytes: self.bytes.checked_add(bytes)?,
        })
    }

    pub(crate) fn removed(self, bytes: usize) -> Option<Self> {
        Some(Self {
            frames: self.frames.checked_sub(1)?,
            bytes: self.bytes.checked_sub(bytes)?,
        })
    }
}
