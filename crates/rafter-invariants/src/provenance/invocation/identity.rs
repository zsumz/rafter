//! Stable deterministic identities derived from invocation inputs.

use sha2::{Digest, Sha256};

pub(crate) fn deterministic_u64(namespace: &str, value: &str) -> u64 {
    let digest = Sha256::digest(format!("{namespace}\0{value}"));
    let mut prefix = [0; 8];
    prefix.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(prefix)
}
