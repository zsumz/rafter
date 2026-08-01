//! Fuzzes the bounded, canonical TLS transport data-frame decoder.
//!
//! The tiny fixed-width group codec keeps caller code deterministic while the
//! target explores outer framing, inner `rafter-codec` messages, sender
//! agreement, canonical routing, and all declared-length boundaries.

#![no_main]

use std::{error::Error, fmt};

use libfuzzer_sys::fuzz_target;
use rafter_transport_tls::{GroupIdCodec, PeerFrameCodec, PeerFrameScratch, WireLimits};

#[derive(Clone, Copy, Debug, Default)]
struct FixedGroupCodec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FixedGroupDecodeError;

impl fmt::Display for FixedGroupDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("group route is not exactly eight bytes")
    }
}

impl Error for FixedGroupDecodeError {}

impl GroupIdCodec<u64> for FixedGroupCodec {
    type Error = FixedGroupDecodeError;

    fn max_encoded_len(&self) -> usize {
        8
    }

    fn encode(&self, group_id: &u64, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        output.extend_from_slice(&group_id.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<u64, Self::Error> {
        input
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| FixedGroupDecodeError)
    }
}

fuzz_target!(|data: &[u8]| {
    let codec = PeerFrameCodec::new(FixedGroupCodec, WireLimits::default())
        .expect("fixed group codec fits the default wire contract");
    let _ = codec.decode(data, &mut PeerFrameScratch::new());
});
