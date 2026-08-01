//! Fuzzes both version-1 TLS transport hello decoders on raw bytes.
//!
//! The decoder contract is total over byte slices: every input produces a
//! typed value or typed refusal without panicking or reading past its bounds.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = rafter_transport_tls::decode_client_hello(data);
    let _ = rafter_transport_tls::decode_server_hello(data);
});
