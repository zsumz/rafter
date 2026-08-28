//! Reviewed evidence-artifact names the producer treats as generated output.
//!
//! Detector artifacts are recognized from the registered probe vocabulary and
//! obligation artifacts from their normalized kind prefix, so the ignored-path
//! policy stays closed against everything else.

pub(super) fn reviewed_tla_evidence_artifact(name: &str) -> bool {
    matches!(
        name,
        "tla-log"
            | "tla.log"
            | "tla-trace-log"
            | "tla-tool"
            | "tla-spec"
            | "tla-trace-spec"
            | "tla-detector-spec"
            | "tla-runner"
            | "tla-tool-asset-id"
            | "tla-tool-checksums"
            | "tla-config"
            | "tla-trace-config"
            | "tla-detector-config"
            | "tla-mutation-log"
            | "tla-producer"
            | "tla-checkpoint-contract"
            | "tla-checkpoint-inventory"
            | "tla-checkpoint-recovered-contract"
            | "tla-checkpoint-recovered-inventory"
            | "tla-checkpoint-recovery-report"
    ) || crate::producer::tla_output::DETECTOR_PROBES
        .into_iter()
        .any(|probe| {
            crate::producer::tla_output::detector_log_kind(probe)
                .is_some_and(|kind| normalize_fixture_artifact_name(&kind) == name)
                || crate::producer::tla_output::detector_config_kind(probe)
                    .is_some_and(|kind| normalize_fixture_artifact_name(&kind) == name)
        })
        || reviewed_obligation_evidence_artifact(name)
}

/// Proof-obligation evidence names are open-ended: the identity is profile
/// data, so the reviewed set cannot be a literal list the way the detector
/// probes are. Recognizing them by their normalized kind prefix keeps the
/// source-identity policy closed against everything else while still admitting
/// whatever obligations the manifest declares.
fn reviewed_obligation_evidence_artifact(name: &str) -> bool {
    ["tla-obligation-log-", "tla-obligation-config-"]
        .into_iter()
        .any(|prefix| {
            name.strip_prefix(prefix)
                .is_some_and(|id| !id.is_empty() && id.bytes().all(is_obligation_identity_byte))
        })
}

fn is_obligation_identity_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
}

fn normalize_fixture_artifact_name(kind: &str) -> String {
    crate::producer::artifact::portable_filename(kind)
}
