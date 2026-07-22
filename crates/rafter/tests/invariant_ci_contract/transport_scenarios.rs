//! Transport scenarios: only complete, isolated, content-addressed evidence stages.

use super::support::*;

#[test]
fn aggregate_transport_staging_requires_a_fresh_complete_noncolliding_shape() {
    let root = workspace_root();
    for (workflow, profile, job, stage_name, layers) in [
        (
            ".github/workflows/ci.yml",
            "pr",
            "invariants-pr",
            "Stage current-run PR evidence artifacts",
            PR_LAYERS.as_slice(),
        ),
        (
            ".github/workflows/nightly.yml",
            "nightly",
            "invariants-nightly",
            "Stage current-run nightly evidence artifacts",
            SCHEDULED_LAYERS.as_slice(),
        ),
        (
            ".github/workflows/weekly.yml",
            "weekly",
            "invariants-weekly",
            "Stage current-run weekly evidence artifacts",
            SCHEDULED_LAYERS.as_slice(),
        ),
    ] {
        let source = read(&root.join(workflow));
        let stage = workflow_step(job_block(&source, job), stage_name);

        let complete = EvidenceTransportFixture::new(&root, profile, layers);
        let output = run_workflow_script(stage, &complete.workspace, &complete.environment());
        assert_success(&output, &format!("stage complete {profile} evidence"));
        complete
            .verify_staged_bundles(layers)
            .unwrap_or_else(|error| panic!("{error}"));

        let omitted = EvidenceTransportFixture::new(&root, profile, layers);
        omitted.remove_artifact_directory(layers[0]);
        let output = run_workflow_script(stage, &omitted.workspace, &omitted.environment());
        assert_failure(&output, &format!("stage incomplete {profile} evidence"));

        let missing_result = EvidenceTransportFixture::new(&root, profile, layers);
        missing_result.remove_result(layers[0]);
        let output = run_workflow_script(
            stage,
            &missing_result.workspace,
            &missing_result.environment(),
        );
        assert_failure(&output, &format!("stage missing {profile} result"));

        let missing_detector_artifacts = EvidenceTransportFixture::new(&root, profile, layers);
        missing_detector_artifacts.remove_simulator_detector_artifacts();
        let output = run_workflow_script(
            stage,
            &missing_detector_artifacts.workspace,
            &missing_detector_artifacts.environment(),
        );
        assert_failure(
            &output,
            &format!("stage missing {profile} simulator detector artifacts"),
        );

        let missing_reference = EvidenceTransportFixture::new(&root, profile, layers);
        missing_reference.remove_referenced_artifact(layers[0]);
        let output = run_workflow_script(
            stage,
            &missing_reference.workspace,
            &missing_reference.environment(),
        );
        assert_success(
            &output,
            &format!("stage structurally complete {profile} evidence"),
        );
        let error = missing_reference
            .verify_staged_bundles(layers)
            .expect_err("missing ArtifactRef target must fail post-stage verification");
        assert!(error.contains("ArtifactRef does not resolve after staging"));

        let contaminated = EvidenceTransportFixture::new(&root, profile, layers);
        contaminated.contaminate_with_diagnostics(layers[0]);
        let output =
            run_workflow_script(stage, &contaminated.workspace, &contaminated.environment());
        assert_success(
            &output,
            &format!("stage structurally complete contaminated {profile} evidence"),
        );
        let error = contaminated
            .verify_staged_bundles(layers)
            .expect_err("diagnostics contamination must fail post-stage verification");
        assert!(error.contains("diagnostics contaminated invariant evidence"));

        let colliding = EvidenceTransportFixture::new(&root, profile, layers);
        colliding.occupy_canonical_target(layers[0]);
        let output = run_workflow_script(stage, &colliding.workspace, &colliding.environment());
        assert_failure(&output, &format!("stage colliding {profile} evidence"));
    }
}
