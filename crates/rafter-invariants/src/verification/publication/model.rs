//! Bounded exact artifact-set model shared by directories and archives.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::evidence::limits::{MAX_VERIFIER_ARCHIVE_BYTES, MAX_VERIFIER_ARCHIVE_FILES};

use super::{manifest, VerifierArchiveExpectation};

pub(super) const MAX_FILES: usize = MAX_VERIFIER_ARCHIVE_FILES;
pub(super) const MAX_FILE_BYTES: usize = MAX_VERIFIER_ARCHIVE_BYTES;
pub(super) const MAX_TOTAL_BYTES: usize = MAX_VERIFIER_ARCHIVE_BYTES;

pub(super) struct ArtifactSet {
    files: BTreeMap<String, Vec<u8>>,
}

impl ArtifactSet {
    pub(super) fn verify(
        files: BTreeMap<String, Vec<u8>>,
        manifest_name: &str,
        expected_manifest_sha256: &str,
        expectation: &VerifierArchiveExpectation,
    ) -> Result<Self, String> {
        if files.is_empty() || files.len() > MAX_FILES {
            return Err(format!(
                "verifier artifact set contains {} files; expected 1..={MAX_FILES}",
                files.len()
            ));
        }
        let total = files.values().try_fold(0_usize, |total, bytes| {
            if bytes.len() > MAX_FILE_BYTES {
                return Err(format!(
                    "verifier artifact exceeds the {MAX_FILE_BYTES}-byte limit"
                ));
            }
            total
                .checked_add(bytes.len())
                .ok_or_else(|| "verifier artifact byte count overflow".to_owned())
        })?;
        if total > MAX_TOTAL_BYTES {
            return Err(format!(
                "verifier artifact set exceeds the {MAX_TOTAL_BYTES}-byte limit"
            ));
        }
        let manifest_bytes = files
            .get(manifest_name)
            .ok_or_else(|| "verifier artifact manifest is absent".to_owned())?;
        let manifest_sha256 = sha256(manifest_bytes);
        if manifest_sha256 != expected_manifest_sha256 {
            return Err("verifier artifact manifest digest changed".to_owned());
        }
        if manifest_name != format!("verifier-artifact-manifest-{manifest_sha256}") {
            return Err("verifier artifact manifest filename is not content-addressed".to_owned());
        }
        let expected = manifest::parse(manifest_bytes)?;
        if expected.contains_key(manifest_name) {
            return Err("verifier artifact manifest lists itself".to_owned());
        }
        let mut expected_names = expected.keys().cloned().collect::<BTreeSet<_>>();
        expected_names.insert(manifest_name.to_owned());
        let observed_names = files.keys().cloned().collect::<BTreeSet<_>>();
        if observed_names != expected_names {
            return Err("verifier artifact inventory is not exact".to_owned());
        }
        for (name, expected_sha256) in expected {
            if sha256(&files[&name]) != expected_sha256 {
                return Err(format!("verifier artifact digest changed for {name}"));
            }
        }
        let reports = files
            .iter()
            .filter(|(name, _)| name.starts_with("verifier-replay-report-"))
            .collect::<Vec<_>>();
        let [(report_name, report_bytes)] = reports.as_slice() else {
            return Err(
                "verifier artifact set does not contain exactly one replay report".to_owned(),
            );
        };
        let report_name = report_name.as_str();
        if report_name != format!("verifier-replay-report-{}", sha256(report_bytes)) {
            return Err("verifier replay report filename is not content-addressed".to_owned());
        }
        for name in files.keys() {
            if name != manifest_name
                && name != report_name
                && !name.starts_with("verifier-replay-process-log-")
            {
                return Err(format!(
                    "verifier artifact set contains unrecognized payload {name}"
                ));
            }
        }
        crate::verification::detector_replay::validate_report_bundle(
            report_bytes,
            expectation.replay(),
            &files,
        )
        .map_err(|error| format!("verifier replay report {report_name} is invalid: {error}"))?;
        Ok(Self { files })
    }

    pub(super) fn files(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.files
    }
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
