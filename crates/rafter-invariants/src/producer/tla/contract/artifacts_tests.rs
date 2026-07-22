//! Source-artifact namespace scenarios.

use super::input_namespace;
use std::path::Path;

#[test]
fn source_inputs_are_self_contained_in_the_uploaded_tla_artifact_tree() {
    assert_eq!(
        input_namespace("pr", "0123456789abcdef"),
        Path::new("pr-tla/0123456789ab/inputs")
    );
}
