//! Compiler contracts supplement API lints that miss handwritten trait removal.

use rafter_crc32::RunningCrc32;

#[test]
fn published_default_constructs_and_resets_an_empty_checksum() {
    let mut running = <RunningCrc32 as Default>::default();
    assert_eq!(running.value(), 0);

    running.update(b"1234");
    let mut prefix = std::mem::take(&mut running);
    assert_eq!(running.value(), 0, "generic take must reset to an empty CRC");

    // Independent IEEE check value: constructor/reset must use the correct
    // initial state, not merely retain the Default trait's type signature.
    prefix.update(b"56789");
    running.update(b"123456789");
    assert_eq!(prefix.value(), 0xCBF4_3926);
    assert_eq!(running.value(), 0xCBF4_3926);
}
