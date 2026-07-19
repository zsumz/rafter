//! Reviewed names and layer inventories shared by CI scenarios.

pub(crate) const CANONICAL_INVARIANT_IDS: [&str; 44] = [
    "ST-01", "EL-01", "EL-02", "EL-03", "EL-04", "EL-05", "EL-06", "EL-07", "EL-08", "LG-01",
    "LG-02", "LG-03", "LG-04", "LG-05", "CM-01", "CM-02", "CM-03", "AP-01", "AP-02", "MB-01",
    "MB-02", "MB-03", "MB-04", "MB-05", "MB-06", "MB-07", "RD-01", "RD-02", "RD-03", "RD-04",
    "RD-05", "RD-06", "PS-01", "PS-02", "PS-03", "PS-04", "SS-01", "SS-02", "SS-03", "SS-04",
    "SS-05", "LV-01", "LV-02", "LV-03",
];

#[derive(Clone, Copy)]
pub(crate) struct ArtifactProducerContract {
    pub(crate) job: &'static str,
    pub(crate) layer: &'static str,
    pub(crate) upload_step: &'static str,
    pub(crate) diagnostics_step: &'static str,
    pub(crate) download_step: &'static str,
}

pub(crate) const PR_EVIDENCE_PRODUCERS: [ArtifactProducerContract; 3] = [
    ArtifactProducerContract {
        job: "invariants-tests",
        layer: "tests",
        upload_step: "Upload test evidence",
        diagnostics_step: "Upload test evidence process diagnostics",
        download_step: "Download available test evidence",
    },
    ArtifactProducerContract {
        job: "invariants-simulator",
        layer: "simulator",
        upload_step: "Upload simulator evidence",
        diagnostics_step: "Upload simulator evidence process diagnostics",
        download_step: "Download available simulator evidence",
    },
    ArtifactProducerContract {
        job: "invariants-tla",
        layer: "tla",
        upload_step: "Upload TLA+ evidence",
        diagnostics_step: "Upload TLA+ evidence process diagnostics",
        download_step: "Download available TLA+ evidence",
    },
];

pub(crate) const PR_LAYERS: [&str; 3] = ["tests", "simulator", "tla"];
pub(crate) const SCHEDULED_LAYERS: [&str; 4] = ["tests", "simulator", "tla", "maelstrom"];

#[derive(Clone, Copy)]
pub(crate) struct AggregateWorkflowContract {
    pub(crate) workflow: &'static str,
    pub(crate) profile: &'static str,
    pub(crate) job: &'static str,
    pub(crate) validate_step: &'static str,
    pub(crate) summary_step: &'static str,
    pub(crate) report_upload_step: &'static str,
    pub(crate) evidence_upload_step: &'static str,
    pub(crate) diagnostics_upload_step: &'static str,
    pub(crate) gate_step: &'static str,
}

pub(crate) fn scheduled_upload_step(profile: &str, layer: &str) -> String {
    match layer {
        "tests" => format!("Upload {profile} test evidence"),
        "simulator" => format!("Upload {profile} simulator evidence and replay logs"),
        "tla" => format!("Upload {profile} TLA+ evidence and counterexamples"),
        "maelstrom" => "Upload source-bound Maelstrom receipts and replay artifacts".to_owned(),
        _ => panic!("unknown scheduled evidence layer {layer}"),
    }
}

pub(crate) fn scheduled_diagnostics_step(profile: &str, layer: &str) -> String {
    let layer = display_layer(layer);
    format!("Upload {profile} {layer} process diagnostics")
}

pub(crate) fn display_layer(layer: &str) -> &str {
    match layer {
        "tests" => "test",
        "tla" => "TLA+",
        "maelstrom" => "Maelstrom",
        layer => layer,
    }
}
