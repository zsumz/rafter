//! Scenarios for deterministic archive sealing and no-extract readback.

use std::{fs, io::Write};

use super::{publish_verifier_archive, support::*, verify_verifier_archive};

#[test]
#[cfg(unix)]
fn exact_read_only_set_round_trips_through_a_downloaded_archive() {
    let fixture = fixture();
    let archive = fixture.temp.path().join("verifier.tar");
    let archive_sha256 = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &archive,
        &fixture.expectation,
    )
    .expect("publish exact verifier archive");

    verify_verifier_archive(
        &archive,
        &archive_sha256,
        &fixture.manifest_sha256,
        &fixture.expectation,
    )
    .expect("verify downloaded archive");
}

#[test]
#[cfg(unix)]
fn completed_replay_round_trips_through_sealing_and_downloaded_readback() {
    let CompletedReport {
        bytes,
        expectation,
        artifacts,
    } = completed_report(false);
    let fixture = fixture_from_report("completed", &bytes, expectation, artifacts);
    let archive = fixture.temp.path().join("completed.tar");
    let archive_sha256 = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &archive,
        &fixture.expectation,
    )
    .expect("publish completed verifier archive");

    verify_verifier_archive(
        &archive,
        &archive_sha256,
        &fixture.manifest_sha256,
        &fixture.expectation,
    )
    .expect("verify completed downloaded archive");
}

#[test]
#[cfg(unix)]
fn appended_detector_execution_fails_during_sealing_and_downloaded_readback() {
    let CompletedReport {
        bytes,
        expectation,
        artifacts,
    } = completed_report(true);
    let fixture = fixture_from_report("appended-execution", &bytes, expectation, artifacts);
    let sealing = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join("rejected.tar"),
        &fixture.expectation,
    )
    .expect_err("appended detector execution must fail sealing")
    .to_string();
    assert!(sealing.contains("archived transcript"), "{sealing}");

    let archive = fixture.temp.path().join("downloaded.tar");
    let mut entries = fs::read_dir(&fixture.root)
        .expect("read verifier fixture")
        .map(|entry| entry.expect("verifier fixture entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    let mut file = fs::File::create(&archive).expect("create downloaded archive");
    {
        let mut builder = tar::Builder::new(&mut file);
        for path in entries {
            let name = path.file_name().unwrap().to_str().unwrap();
            append_canonical_entry(
                &mut builder,
                name,
                &fs::read(&path).expect("read verifier fixture entry"),
            );
        }
        builder.finish().expect("finish downloaded archive");
    }
    file.flush().expect("flush downloaded archive");
    let archive_sha256 = sha256(&fs::read(&archive).expect("read downloaded archive"));
    let readback = verify_verifier_archive(
        &archive,
        &archive_sha256,
        &fixture.manifest_sha256,
        &fixture.expectation,
    )
    .expect_err("appended detector execution must fail downloaded readback")
    .to_string();
    assert!(readback.contains("archived transcript"), "{readback}");
}

#[test]
#[cfg(unix)]
fn nested_payload_is_rejected_even_when_the_manifested_files_are_valid() {
    let fixture = fixture();
    make_writable(&fixture.root);
    fs::create_dir(fixture.root.join("nested")).expect("create unmanifested directory");
    make_read_only(&fixture.root);
    let error = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join("nested.tar"),
        &fixture.expectation,
    )
    .expect_err("nested upload payload must fail closed")
    .to_string();
    assert!(error.contains("not an exact regular file"));
}

#[test]
#[cfg(unix)]
fn writable_payload_and_changed_manifest_digest_are_rejected() {
    let fixture = fixture();
    make_writable(&fixture.payload);
    let writable = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join("writable.tar"),
        &fixture.expectation,
    )
    .expect_err("writable payload must fail closed")
    .to_string();
    assert!(writable.contains("is writable"));
    make_read_only(&fixture.payload);

    let digest = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &"0".repeat(64),
        &fixture.temp.path().join("digest.tar"),
        &fixture.expectation,
    )
    .expect_err("manifest digest mismatch must fail closed")
    .to_string();
    assert!(digest.contains("manifest digest changed"));
}

#[test]
#[cfg(unix)]
fn downloaded_archive_requires_both_captured_digests() {
    let fixture = fixture();
    let archive = fixture.temp.path().join("verifier.tar");
    let archive_sha256 = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &archive,
        &fixture.expectation,
    )
    .expect("publish verifier archive");

    assert!(verify_verifier_archive(
        &archive,
        &"0".repeat(64),
        &fixture.manifest_sha256,
        &fixture.expectation,
    )
    .is_err());
    assert!(verify_verifier_archive(
        &archive,
        &archive_sha256,
        &"0".repeat(64),
        &fixture.expectation,
    )
    .is_err());
}

#[test]
#[cfg(unix)]
fn downloaded_archive_rejects_noncanonical_entry_metadata() {
    let fixture = fixture();
    let archive = fixture.temp.path().join("noncanonical.tar");
    let mut file = fs::File::create(&archive).expect("create archive");
    {
        let mut builder = tar::Builder::new(&mut file);
        let report = fs::read(&fixture.payload).expect("read fixture report");
        append_entry(
            &mut builder,
            fixture.payload.file_name().unwrap().to_str().unwrap(),
            &report,
        );
        let manifest = fs::read(&fixture.manifest).expect("read fixture manifest");
        append_entry(
            &mut builder,
            fixture.manifest.file_name().unwrap().to_str().unwrap(),
            &manifest,
        );
        builder.finish().expect("finish archive");
    }
    file.flush().expect("flush archive");
    let archive_sha256 = sha256(&fs::read(&archive).expect("read archive"));

    let error = verify_verifier_archive(
        &archive,
        &archive_sha256,
        &fixture.manifest_sha256,
        &fixture.expectation,
    )
    .expect_err("writable archive entries must fail closed")
    .to_string();
    assert!(error.contains("metadata is noncanonical"));
}

#[test]
#[cfg(unix)]
fn malformed_replay_reports_fail_during_sealing_and_readback() {
    let fixture = semantic_failure_fixture();
    let sealing = publish_verifier_archive(
        &fixture.root,
        &fixture.manifest,
        &fixture.manifest_sha256,
        &fixture.temp.path().join("invalid-report.tar"),
        &fixture.expectation,
    )
    .expect_err("schema-invalid replay report must fail during sealing")
    .to_string();
    assert!(sealing.contains("replay report"), "{sealing}");

    let temp = tempfile::tempdir().expect("temporary archive root");
    let archive = temp.path().join("invalid-report.tar");
    let report_name = "verifier-replay-report-invalid";
    let report = b"{}\n";
    let manifest = format!("{}  {report_name}\n", sha256(report));
    let manifest_name = format!("verifier-artifact-manifest-{}", sha256(manifest.as_bytes()));
    let mut file = fs::File::create(&archive).expect("create archive");
    {
        let mut builder = tar::Builder::new(&mut file);
        append_canonical_entry(&mut builder, &manifest_name, manifest.as_bytes());
        append_canonical_entry(&mut builder, report_name, report);
        builder.finish().expect("finish archive");
    }
    file.flush().expect("flush archive");
    let archive_sha256 = sha256(&fs::read(&archive).expect("read archive"));
    let expectation = synthetic_report().1;
    let readback = verify_verifier_archive(
        &archive,
        &archive_sha256,
        &sha256(manifest.as_bytes()),
        &expectation,
    )
    .expect_err("schema-invalid replay report must fail after download")
    .to_string();
    assert!(readback.contains("replay report"), "{readback}");
}
