use std::{error::Error, fs, path::Path};

use rafter_codec::{
    decode_message, encode_message, DecodePeerMessageError, EncodePeerMessageError,
};

pub(super) fn check(directory: &Path) -> Result<(), Box<dyn Error>> {
    for name in [
        "future-version",
        "reserved-tag",
        "invalid-magic",
        "checksum",
        "truncated",
        "trailing",
        "invalid-boolean",
    ] {
        let bytes = fs::read(directory.join(format!("{name}.bin")))?;
        let error = decode_message(&bytes).expect_err("malformed peer frame must be refused");
        let expected = matches!(
            (name, &error),
            (
                "future-version",
                DecodePeerMessageError::UnsupportedVersion(99)
            ) | (
                "reserved-tag",
                DecodePeerMessageError::UnknownMessageType(5)
            ) | ("invalid-magic", DecodePeerMessageError::InvalidMagic(_))
                | (
                    "checksum",
                    DecodePeerMessageError::FrameChecksumMismatch { .. }
                )
                | ("truncated", DecodePeerMessageError::UnexpectedEof { .. })
                | ("trailing", DecodePeerMessageError::TrailingBytes(1))
                | ("invalid-boolean", DecodePeerMessageError::InvalidBoolean(2))
        );
        assert!(expected, "{name} produced the wrong refusal: {error:?}");
    }
    assert!(matches!(
        encode_message(&crate::messages::whole_snapshot()),
        Err(EncodePeerMessageError::UnsupportedMessage {
            message: "InstallSnapshot",
            ..
        })
    ));
    Ok(())
}
