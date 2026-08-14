//! Evidence-upload scenarios: archived invariant evidence stays byte-complete.

use super::support::*;

const UPLOAD_ACTION: &str = "uses: actions/upload-artifact@";

/// Workflows that archive invariant evidence, and the exact number of upload
/// steps in each whose path is a receipt-bound evidence tree. The count is
/// reviewed; the *set* is derived, so a new evidence upload joins the property
/// by existing rather than by being remembered.
const EVIDENCE_UPLOAD_WORKFLOWS: [(&str, usize); 3] = [
    (".github/workflows/ci.yml", 4),
    (".github/workflows/nightly.yml", 5),
    (".github/workflows/weekly.yml", 5),
];

/// Evidence receipts bind their artifacts by path, so an archive that is
/// missing a bound path is not a weaker copy of the evidence -- it is a bundle
/// that fails re-verification after every layer already passed.
/// `upload-artifact` excludes hidden files unless told otherwise, and the
/// Maelstrom durable stores carry deliberate dotfile markers
/// (`.app-persist-crashpoint-fired`) at exactly such a bound path. Which trees
/// happen to contain a dotfile today is a property of a filename generator, not
/// something an archive step can depend on, so require completeness of every
/// receipt-bound evidence upload rather than of the ones already known to need
/// it. The aggregate re-upload is the one this was written for: it re-archives
/// all four downloaded layers verbatim, so it inherits every layer's dotfiles
/// and had none of the flags.
#[test]
fn receipt_bound_evidence_uploads_are_byte_complete() {
    let root = workspace_root();
    for (workflow, expected) in EVIDENCE_UPLOAD_WORKFLOWS {
        let source = read(&root.join(workflow));
        let uploads = workflow_steps(&source)
            .into_iter()
            .filter(|step| step.contains(UPLOAD_ACTION))
            .collect::<Vec<_>>();
        assert_eq!(
            uploads.len(),
            source.matches(UPLOAD_ACTION).count(),
            "{workflow} has an upload step this guard cannot see; every step must open with `- `"
        );

        let mut bound = 0;
        for step in uploads {
            let paths = workflow_step_paths(step);
            assert!(
                !paths.is_empty(),
                "{workflow} upload step declares no path:\n{step}"
            );
            if !paths.iter().copied().any(is_receipt_bound_evidence) {
                continue;
            }
            bound += 1;
            assert!(
                step.contains("\n          include-hidden-files: true\n"),
                "{workflow} archives receipt-bound evidence without include-hidden-files, \
                 so any dotfile a receipt binds is stripped from the archive:\n{step}"
            );
        }
        assert_eq!(
            bound, expected,
            "{workflow} receipt-bound evidence upload inventory changed"
        );
    }
}

/// A path carries receipt-bound evidence when it names the canonical evidence
/// tree receipts bind artifacts into, or the transport copy of that same tree
/// the aggregate downloads into and re-uploads. Process diagnostics, rendered
/// reports, and the sealed verifier archive are none of those: nothing binds
/// them by path.
fn is_receipt_bound_evidence(path: &str) -> bool {
    path.starts_with("artifacts/invariants/") || path.contains("env.INVARIANT_EVIDENCE_DIR")
}
