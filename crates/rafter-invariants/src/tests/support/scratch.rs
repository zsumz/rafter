//! Fixture scratch roots identify themselves as generated cache data.

use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Once,
};

use crate::execution::filesystem::HeldDirectory;

/// Preserves repository-relative fixture paths while marking their cache root.
/// Cargo runs unit tests from the package directory, so this `target` can be
/// separate from Cargo's workspace target and has not necessarily been tagged.
pub(crate) fn scratch_path(relative: &str) -> PathBuf {
    static INITIALIZED: Once = Once::new();
    INITIALIZED.call_once(|| {
        const SIGNATURE: &[u8; 43] = b"Signature: 8a477f597d28d172789f06886806bc55";
        let target = HeldDirectory::create_all(Path::new("target"))
            .expect("create confined test cache root");
        let marker = Path::new("CACHEDIR.TAG");
        if target.path_exists(marker).expect("inspect test cache marker") {
            let mut signature = [0; SIGNATURE.len()];
            target
                .open_file(marker)
                .expect("open regular cache marker without following symlinks")
                .read_exact(&mut signature)
                .expect("read complete cache signature");
            assert_eq!(&signature, SIGNATURE, "preserve and reject invalid cache markers");
        } else {
            target
                .write_atomic(marker, b"Signature: 8a477f597d28d172789f06886806bc55\n# Generated invariant-test scratch; safe to discard between runs.\n")
                .expect("publish complete test cache marker");
        }
    });
    Path::new("target/rafter-invariants").join(relative)
}
