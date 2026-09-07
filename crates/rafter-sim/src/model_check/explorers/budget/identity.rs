//! Hashing that turns a model state into its exact canonical identity.
//!
//! `StateKey` is a compact FNV bucket index; equality is always decided by the
//! lossless zero-run encoding of the same structural byte stream, so two
//! states merge exactly when their canonical encodings are identical.

use std::hash::{Hash, Hasher};

use super::super::super::state::ExplorationState;
use super::{
    CanonicalStateIdentity, ExactStateIdentity, ExactStateIdentityHasher, StateIdentity, StateKey,
    StateKeyHasher,
};

impl ExactStateIdentity {
    pub(super) fn from_hash(state: &impl Hash) -> Self {
        let mut hasher = ExactStateIdentityHasher::new();
        state.hash(&mut hasher);
        hasher.finish_identity()
    }

    pub(super) fn from_protocol_state(state: &impl StateIdentity) -> Self {
        let mut hasher = ExactStateIdentityHasher::new();
        state.hash_protocol_state(&mut hasher);
        hasher.finish_identity()
    }
}

impl StateKey {
    #[cfg(test)]
    pub(in crate::model_check::explorers) fn from_hash(state: &impl Hash) -> Self {
        let mut hasher = StateKeyHasher::new();
        state.hash(&mut hasher);
        hasher.finish_key()
    }

    pub(in crate::model_check::explorers) fn from_protocol_state(
        state: &impl StateIdentity,
    ) -> Self {
        let mut hasher = StateKeyHasher::new();
        state.hash_protocol_state(&mut hasher);
        hasher.finish_key()
    }
}

pub(in crate::model_check) fn protocol_state_fingerprint(
    state: &ExplorationState,
) -> (u64, u64, u64) {
    let key = StateKey::from_protocol_state(state);
    (key.len, key.hash_a, key.hash_b)
}

impl ExactStateIdentityHasher {
    pub(super) const fn new() -> Self {
        Self {
            key: StateKeyHasher::new(),
            canonical: Vec::new(),
            pending_zeros: 0,
        }
    }

    pub(super) fn finish_identity(mut self) -> ExactStateIdentity {
        self.flush_zeros();
        ExactStateIdentity {
            key: self.key.finish_key(),
            canonical: CanonicalStateIdentity(self.canonical.into_boxed_slice()),
        }
    }

    fn flush_zeros(&mut self) {
        if self.pending_zeros == 0 {
            return;
        }

        self.canonical.push(0);
        let mut remaining = self.pending_zeros;
        while remaining >= 0x80 {
            let chunk = (remaining & 0x7f).to_le_bytes()[0];
            self.canonical.push(chunk | 0x80);
            remaining >>= 7;
        }
        self.canonical.push(remaining.to_le_bytes()[0]);
        self.pending_zeros = 0;
    }
}

impl Hasher for ExactStateIdentityHasher {
    fn finish(&self) -> u64 {
        self.key.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.key.write_bytes(bytes);
        for byte in bytes {
            if *byte == 0 {
                if self.pending_zeros == usize::MAX {
                    self.flush_zeros();
                }
                self.pending_zeros += 1;
            } else {
                self.flush_zeros();
                self.canonical.push(*byte);
            }
        }
    }
}

impl StateKeyHasher {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self {
            len: 0,
            hash_a: Self::FNV_OFFSET,
            hash_b: Self::FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    const fn finish_key(self) -> StateKey {
        StateKey {
            len: self.len,
            hash_a: self.hash_a,
            hash_b: self.hash_b,
        }
    }

    pub(in crate::model_check::explorers) fn write_bytes(&mut self, bytes: &[u8]) {
        self.len = self.len.saturating_add(bytes.len() as u64);
        for byte in bytes {
            let byte = u64::from(*byte);
            self.hash_a ^= byte;
            self.hash_a = self.hash_a.wrapping_mul(Self::FNV_PRIME);
            self.hash_b ^= byte.wrapping_add(0x517c_c1b7_2722_0a95);
            self.hash_b = self.hash_b.wrapping_mul(Self::FNV_PRIME).rotate_left(13);
        }
    }
}

impl Hasher for StateKeyHasher {
    fn finish(&self) -> u64 {
        self.hash_a ^ self.hash_b.rotate_left(17) ^ self.len.rotate_left(31)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.write_bytes(bytes);
    }
}
