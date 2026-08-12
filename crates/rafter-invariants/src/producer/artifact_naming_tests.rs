//! Scenarios: every published artifact filename can actually be uploaded.

use super::{portable_filename, UNPORTABLE_FILENAME_CHARACTERS};

/// Every artifact kind any invariant producer can emit, drawn from the live
/// registries rather than a hand-copied list so a newly wired kind is covered
/// the moment it exists.
fn every_emitted_artifact_kind() -> std::collections::BTreeSet<String> {
    let (_, manifest) = crate::tests::loaded();
    let mut kinds = vec![
        "producer-binary".to_owned(),
        "summary".to_owned(),
        "test-log".to_owned(),
        "test-binary".to_owned(),
        "compile-log".to_owned(),
        "simulator-log".to_owned(),
        "simulator-binary".to_owned(),
        "tla-log".to_owned(),
        "tla-trace-log".to_owned(),
        "tla-tool".to_owned(),
        "tla-spec".to_owned(),
        "tla-trace-spec".to_owned(),
        "tla-detector-spec".to_owned(),
        "tla-runner".to_owned(),
        "tla-tool-asset-id".to_owned(),
        "tla-tool-checksums".to_owned(),
        "tla-config".to_owned(),
        "tla-trace-config".to_owned(),
        "tla-detector-config".to_owned(),
        crate::producer::tla_output::MUTATION_SUITE_ARTIFACT_KIND.to_owned(),
    ];
    for probe in crate::producer::tla_output::DETECTOR_PROBES {
        kinds.extend(crate::producer::tla_output::detector_log_kind(probe));
        kinds.extend(crate::producer::tla_output::detector_config_kind(probe));
    }
    for profile in ["pr", "nightly", "weekly"] {
        for obligation in &manifest.profiles[profile].runners["tla"].obligations {
            kinds.push(crate::producer::tla_output::obligation_log_kind(
                &obligation.id,
            ));
            kinds.push(crate::producer::tla_output::obligation_config_kind(
                &obligation.id,
            ));
        }
    }
    // Profiles share obligation identities, so the inventory is a set: the
    // collision guard below is about distinct kinds, not repeated ones.
    kinds.into_iter().collect()
}

/// `actions/upload-artifact` rejects the whole upload if any single path holds
/// a Windows-reserved character, so an un-portable filename does not degrade --
/// it discards a completed layer's entire evidence tree after the work is
/// already paid for. Structured kinds carry colons by design, so the mapping
/// from kind to filename is what has to stay portable.
#[test]
fn every_emitted_artifact_kind_maps_to_an_uploadable_filename() {
    let kinds = every_emitted_artifact_kind();
    assert!(
        kinds.iter().any(|kind| kind.contains(':')),
        "the inventory must include structured kinds or this guard proves nothing"
    );
    for kind in &kinds {
        let filename = portable_filename(kind);
        assert!(
            !filename.is_empty(),
            "artifact kind {kind} produced an empty filename"
        );
        if let Some(rejected) = filename
            .chars()
            .find(|character| UNPORTABLE_FILENAME_CHARACTERS.contains(character))
        {
            panic!("artifact kind {kind} produces un-uploadable filename {filename}: {rejected:?}");
        }
        assert!(
            !filename.contains('/'),
            "artifact kind {kind} produces a filename spanning directories: {filename}"
        );
    }
}

/// Distinct kinds must stay distinct after normalization, or two artifacts
/// would content-address into one another's filename.
#[test]
fn portable_filenames_do_not_collide_across_emitted_kinds() {
    let kinds = every_emitted_artifact_kind();
    let mapped = kinds
        .iter()
        .map(|kind| portable_filename(kind))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        kinds.len(),
        mapped.len(),
        "two distinct artifact kinds share one filename"
    );
}
