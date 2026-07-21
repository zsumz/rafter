//! Shared identity for exact test-execution storage and environment plans.

use crate::evidence::ResultBundle;

pub(super) fn profile(bundle: &ResultBundle) -> String {
    if bundle.runner == "simulator" {
        format!("{}-simulator-detectors", bundle.profile)
    } else {
        bundle.profile.clone()
    }
}
