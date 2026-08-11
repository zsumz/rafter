//! TLA+ checkpoint compatibility, traversal, and finalization scenarios.

use super::{
    expected_contract, hash_reader, inventory, inventory_with_limits, prepare, prune_to_latest,
    read_candidate_json, read_file_with_deadline, read_sorted_entries,
    sanitize_cache_root_with_limits, validate_candidate, CheckpointContract, CheckpointInventory,
    RecoveryStatus, TraversalLimits, CACHE_VALID_FILE, HASH_BUFFER_BYTES, INPUT_KINDS,
    MAX_CHECKPOINT_METADATA_BYTES, RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND,
    RECOVERY_REPORT_KIND, TRAVERSAL_LIMITS,
};
use crate::ArtifactRef;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn checkpoint_inputs() -> (BTreeMap<String, String>, Vec<ArtifactRef>) {
    let configuration = BTreeMap::from([
        ("config".to_owned(), "Raft.cfg".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
    ]);
    let source_artifacts = INPUT_KINDS
        .into_iter()
        .map(|kind| ArtifactRef {
            kind: kind.to_owned(),
            path: format!("test-inputs/{kind}"),
            sha256: format!("{:0>64}", kind.len()),
            size_bytes: 1,
        })
        .collect();
    (configuration, source_artifacts)
}

fn test_root(label: &str) -> PathBuf {
    Path::new("target/rafter-invariants/checkpoint-fs-tests")
        .join(format!("{label}-{}", std::process::id()))
}

fn traversal_limits(directory_entries: usize, files: usize) -> TraversalLimits {
    TRAVERSAL_LIMITS
        .with_directory_entries(directory_entries)
        .with_files(files)
        .with_directories(64)
        .with_nodes(128)
        .with_depth(32)
}

#[test]
fn checkpoint_flat_directory_limit_covers_the_global_file_budget() {
    assert_eq!(TRAVERSAL_LIMITS.directory_entries(), 64 * 1024);
    assert_eq!(
        TRAVERSAL_LIMITS.directory_entries(),
        TRAVERSAL_LIMITS.files(),
        "a valid flat TLC state store must fit whenever its files fit the global checkpoint budget"
    );
}

#[test]
fn inventory_detects_changed_checkpoint_bytes() {
    let root = test_root("inventory");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("26-07-12-00-00-00.000")).expect("create checkpoint");
    fs::write(root.join("26-07-12-00-00-00.000/states_0.chkpt"), b"first")
        .expect("write checkpoint");
    let first = inventory(&root, &"1".repeat(64), deadline()).expect("inventory checkpoint");
    fs::write(root.join("26-07-12-00-00-00.000/states_0.chkpt"), b"second")
        .expect("mutate checkpoint");
    let second = inventory(&root, &"1".repeat(64), deadline()).expect("inventory checkpoint");
    assert_ne!(first, second);
    assert_eq!(
        first.latest_checkpoint.as_deref(),
        Some("26-07-12-00-00-00.000")
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn incomplete_new_checkpoint_is_discarded_before_inventory() {
    let root = test_root("partial");
    let _ = fs::remove_dir_all(&root);
    let complete = root.join("26-07-12-00-00-00.000");
    let partial = root.join("26-07-12-01-00-00.000");
    fs::create_dir_all(&complete).expect("create complete checkpoint");
    fs::create_dir_all(&partial).expect("create partial checkpoint");
    fs::write(complete.join("queue.chkpt"), b"complete").expect("write checkpoint");
    fs::write(partial.join("queue.chkpt"), b"old").expect("write old checkpoint");
    fs::write(partial.join("queue.tmp"), b"partial").expect("write partial checkpoint");

    assert!(inventory(&root, &"1".repeat(64), deadline()).is_err());
    prune_to_latest(&root, deadline()).expect("prune partial checkpoint");
    let retained =
        inventory(&root, &"1".repeat(64), deadline()).expect("inventory retained checkpoint");
    assert_eq!(
        retained.latest_checkpoint.as_deref(),
        Some("26-07-12-00-00-00.000")
    );
    assert!(!partial.exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn checkpoint_json_rejects_unknown_fields() {
    let source = r#"{
          "schema_version": 1,
          "contract_sha256": "digest",
          "latest_checkpoint": null,
          "files": [],
          "trusted": true
        }"#;
    assert!(serde_json::from_str::<CheckpointInventory>(source).is_err());

    let contract = CheckpointContract {
        schema_version: 1,
        profile: "weekly".to_owned(),
        config: "Raft.cfg".to_owned(),
        runner_contract_sha256: "2".repeat(64),
        input_sha256: BTreeMap::new(),
    };
    assert_eq!(contract.sha256().expect("digest").len(), 64);
}

#[test]
fn stale_candidate_is_rejected_without_deleting_usable_state() {
    let root = test_root("stale");
    let _ = fs::remove_dir_all(&root);
    let states = root.join("states");
    let run = states.join("26-07-12-00-00-00.000");
    fs::create_dir_all(&run).expect("create checkpoint");
    let state = run.join("states_0.chkpt");
    fs::write(&state, b"usable").expect("write checkpoint state");
    let expected = CheckpointContract {
        schema_version: 1,
        profile: "weekly".to_owned(),
        config: "Raft.cfg".to_owned(),
        runner_contract_sha256: "1".repeat(64),
        input_sha256: BTreeMap::new(),
    };
    let mut stale_contract = expected.clone();
    stale_contract.runner_contract_sha256 = "2".repeat(64);
    fs::write(
        root.join("checkpoint-contract.json"),
        serde_json::to_vec_pretty(&stale_contract).expect("serialize contract"),
    )
    .expect("write contract");
    let stale_inventory = inventory(
        &states,
        &stale_contract.sha256().expect("digest"),
        deadline(),
    )
    .expect("inventory state");
    fs::write(
        root.join("checkpoint-inventory.json"),
        serde_json::to_vec_pretty(&stale_inventory).expect("serialize inventory"),
    )
    .expect("write inventory");

    assert!(validate_candidate(
        &root.join("checkpoint-contract.json"),
        &root.join("checkpoint-inventory.json"),
        &states,
        &expected,
        deadline(),
    )
    .expect("candidate validation completes")
    .is_err());
    assert_eq!(fs::read(&state).expect("state remains"), b"usable");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn incompatible_candidate_is_sanitized_before_the_next_prepare() {
    let profile = format!("checkpoint-self-heal-{}", std::process::id());
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output_dir);

    let configuration = BTreeMap::from([
        ("config".to_owned(), "Raft.cfg".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
    ]);
    let source_artifacts = INPUT_KINDS
        .into_iter()
        .map(|kind| ArtifactRef {
            kind: kind.to_owned(),
            path: format!("test-inputs/{kind}"),
            sha256: format!("{:0>64}", kind.len()),
            size_bytes: 1,
        })
        .collect::<Vec<_>>();
    let expected = expected_contract(&profile, &configuration, &source_artifacts)
        .expect("derive expected contract");
    let mut stale = expected.clone();
    stale.runner_contract_sha256 = "f".repeat(64);
    let states = root.join("states");
    let run = states.join("26-07-12-00-00-00.000");
    fs::create_dir_all(&run).expect("create stale checkpoint");
    fs::write(run.join("states_0.chkpt"), b"poison").expect("write stale checkpoint");
    fs::write(
        root.join("checkpoint-contract.json"),
        serde_json::to_vec_pretty(&stale).expect("serialize stale contract"),
    )
    .expect("write stale contract");
    let stale_inventory = inventory(
        &states,
        &stale.sha256().expect("digest stale contract"),
        deadline(),
    )
    .expect("inventory stale checkpoint");
    fs::write(
        root.join("checkpoint-inventory.json"),
        serde_json::to_vec_pretty(&stale_inventory).expect("serialize stale inventory"),
    )
    .expect("write stale inventory");
    fs::write(root.join(CACHE_VALID_FILE), b"stale marker").expect("write stale validity marker");

    let first = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    )
    .expect("reject and sanitize stale checkpoint");
    assert_eq!(first.report.status, RecoveryStatus::Incompatible);
    assert!(first.error.is_some());
    assert!(!states.exists());
    assert!(!root.join("checkpoint-contract.json").exists());
    assert!(!root.join("checkpoint-inventory.json").exists());
    assert!(root.join(CACHE_VALID_FILE).is_file());
    let diagnostic_kinds = first
        .finish(&output_dir, deadline())
        .expect("finish incompatible preparation")
        .into_iter()
        .map(|artifact| artifact.kind)
        .collect::<Vec<_>>();
    assert!(diagnostic_kinds.contains(&RECOVERED_CONTRACT_KIND.to_owned()));
    assert!(diagnostic_kinds.contains(&RECOVERED_INVENTORY_KIND.to_owned()));
    assert!(diagnostic_kinds.contains(&RECOVERY_REPORT_KIND.to_owned()));

    let second = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    )
    .expect("prepare clean replacement checkpoint");
    assert_eq!(second.report.status, RecoveryStatus::Fresh);
    assert!(!second.report.candidate_present);
    assert!(second.error.is_none());
    second
        .finish(&output_dir, deadline())
        .expect("finish clean preparation");
    assert!(root.join(CACHE_VALID_FILE).is_file());

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(output_dir);
}

#[cfg(unix)]
#[test]
fn restored_metadata_symlink_is_rejected_without_copying_its_target() {
    use std::os::unix::fs::symlink;

    let profile = format!("checkpoint-metadata-symlink-{}", std::process::id());
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let external = std::env::temp_dir().join(format!("rafter-checkpoint-external-{profile}"));
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output_dir);
    let _ = fs::remove_file(&external);

    let external_bytes = b"external checkpoint metadata must not be captured";
    fs::create_dir_all(&root).expect("create checkpoint cache");
    fs::write(&external, external_bytes).expect("write external metadata target");
    symlink(&external, root.join("checkpoint-contract.json"))
        .expect("restore symlinked checkpoint metadata");
    let (configuration, source_artifacts) = checkpoint_inputs();

    let preparation = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    )
    .expect("reject symlinked checkpoint metadata");
    assert_eq!(preparation.report.status, RecoveryStatus::Incompatible);
    assert!(preparation
        .report
        .error
        .as_deref()
        .is_some_and(|error| error.contains("not a regular file")));
    assert!(!root.join("checkpoint-contract.json").exists());
    assert!(root.join(CACHE_VALID_FILE).is_file());

    let artifacts = preparation
        .finish(&output_dir, deadline())
        .expect("finish symlink rejection");
    assert!(artifacts
        .iter()
        .all(|artifact| artifact.kind != RECOVERED_CONTRACT_KIND));
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.kind == RECOVERY_REPORT_KIND));
    assert_eq!(
        fs::read(&external).expect("external target remains"),
        external_bytes
    );

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(output_dir);
    let _ = fs::remove_file(external);
}

#[cfg(unix)]
#[test]
fn restored_checkpoint_root_symlink_does_not_touch_external_sentinel() {
    use std::os::unix::fs::symlink;

    let profile = format!("checkpoint-root-symlink-{}", std::process::id());
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let external = std::env::temp_dir().join(format!("rafter-checkpoint-root-target-{profile}"));
    let sentinel = external.join("sentinel.txt");
    let external_marker = external.join(CACHE_VALID_FILE);
    let _ = fs::remove_file(&root);
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output_dir);
    let _ = fs::remove_dir_all(&external);

    fs::create_dir_all(root.parent().expect("checkpoint root parent"))
        .expect("create checkpoint root parent");
    fs::create_dir_all(&external).expect("create external root target");
    fs::write(&sentinel, b"external root sentinel").expect("write external root sentinel");
    fs::write(&external_marker, b"external marker sentinel")
        .expect("write external marker sentinel");
    symlink(&external, &root).expect("restore symlinked checkpoint root");
    let (configuration, source_artifacts) = checkpoint_inputs();

    let preparation = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    )
    .expect("classify and sanitize symlinked checkpoint root");
    assert_eq!(preparation.report.status, RecoveryStatus::Incompatible);
    assert!(preparation
        .report
        .error
        .as_deref()
        .is_some_and(|error| error.contains("root is a symlink")));
    assert!(root.is_dir());
    assert!(!fs::symlink_metadata(&root)
        .expect("inspect replacement root")
        .file_type()
        .is_symlink());
    assert_eq!(
        fs::read(&sentinel).expect("read external sentinel"),
        b"external root sentinel"
    );
    assert_eq!(
        fs::read(&external_marker).expect("read external marker"),
        b"external marker sentinel"
    );

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(output_dir);
    let _ = fs::remove_dir_all(external);
}

#[cfg(unix)]
#[test]
fn restored_states_symlink_does_not_touch_external_sentinel() {
    use std::os::unix::fs::symlink;

    let profile = format!("checkpoint-states-symlink-{}", std::process::id());
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let states = root.join("states");
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let external = std::env::temp_dir().join(format!("rafter-checkpoint-states-target-{profile}"));
    let sentinel = external.join("sentinel.txt");
    let external_checkpoint = external.join("states_0.chkpt");
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output_dir);
    let _ = fs::remove_dir_all(&external);

    fs::create_dir_all(&root).expect("create checkpoint root");
    fs::create_dir_all(&external).expect("create external states target");
    fs::write(&sentinel, b"external states sentinel").expect("write external states sentinel");
    fs::write(&external_checkpoint, b"external checkpoint sentinel")
        .expect("write external checkpoint sentinel");
    symlink(&external, &states).expect("restore symlinked states directory");
    let (configuration, source_artifacts) = checkpoint_inputs();

    let preparation = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    )
    .expect("classify and sanitize symlinked states directory");
    assert_eq!(preparation.report.status, RecoveryStatus::Incompatible);
    assert!(preparation
        .report
        .error
        .as_deref()
        .is_some_and(|error| error.contains("states directory is a symlink")));
    assert!(!states.exists());
    assert_eq!(
        fs::read(&sentinel).expect("read external sentinel"),
        b"external states sentinel"
    );
    assert_eq!(
        fs::read(&external_checkpoint).expect("read external checkpoint"),
        b"external checkpoint sentinel"
    );

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(output_dir);
    let _ = fs::remove_dir_all(external);
}

#[test]
fn oversized_metadata_is_reported_sanitized_and_not_preserved() {
    let profile = format!("checkpoint-oversized-metadata-{}", std::process::id());
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output_dir);

    fs::create_dir_all(&root).expect("create checkpoint cache");
    let oversized = root.join("checkpoint-contract.json");
    let file = fs::File::create(&oversized).expect("create oversized checkpoint metadata");
    file.set_len(MAX_CHECKPOINT_METADATA_BYTES + 1)
        .expect("extend sparse checkpoint metadata");
    drop(file);
    let (configuration, source_artifacts) = checkpoint_inputs();

    let preparation = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    )
    .expect("classify oversized metadata as incompatible");
    assert_eq!(preparation.report.status, RecoveryStatus::Incompatible);
    assert!(preparation
        .report
        .error
        .as_deref()
        .is_some_and(|error| error.contains("metadata size limit")));
    assert!(!oversized.exists());
    assert!(root.join(CACHE_VALID_FILE).is_file());

    let artifacts = preparation
        .finish(&output_dir, deadline())
        .expect("finish oversized metadata rejection");
    assert!(artifacts
        .iter()
        .all(|artifact| artifact.kind != RECOVERED_CONTRACT_KIND));
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.kind == RECOVERY_REPORT_KIND));
    assert!(artifacts
        .iter()
        .all(|artifact| artifact.size_bytes <= MAX_CHECKPOINT_METADATA_BYTES));

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(output_dir);
}

#[test]
fn checkpoint_directory_entry_limit_bounds_collection_before_sorting() {
    let root = test_root("directory-limit");
    let _ = fs::remove_dir_all(&root);
    let run = root.join("26-07-12-00-00-00.000");
    fs::create_dir_all(&run).expect("create checkpoint run");
    for index in 0..5 {
        fs::write(run.join(format!("state-{index}.chkpt")), b"state")
            .expect("write directory-limit fixture");
    }

    let error = inventory_with_limits(&root, &"1".repeat(64), deadline(), traversal_limits(4, 16))
        .expect_err("oversized checkpoint directory must be rejected");
    assert!(error.to_string().contains("entry limit of 4"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn checkpoint_total_file_limit_bounds_recursive_inventory() {
    let root = test_root("file-limit");
    let _ = fs::remove_dir_all(&root);
    let run = root.join("26-07-12-00-00-00.000");
    for partition in ["a", "b"] {
        let directory = run.join(partition);
        fs::create_dir_all(&directory).expect("create checkpoint partition");
        for index in 0..3 {
            fs::write(directory.join(format!("state-{index}.chkpt")), b"state")
                .expect("write file-limit fixture");
        }
    }

    let error = inventory_with_limits(&root, &"1".repeat(64), deadline(), traversal_limits(4, 5))
        .expect_err("excess checkpoint files must be rejected");
    assert!(error.to_string().contains("total file limit of 5"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn checkpoint_depth_limit_rejects_a_deep_tree_iteratively() {
    let root = test_root("depth-limit");
    let _ = fs::remove_dir_all(&root);
    let mut directory = root.join("26-07-12-00-00-00.000");
    for index in 0..12 {
        directory = directory.join(format!("level-{index}"));
    }
    fs::create_dir_all(&directory).expect("create deep checkpoint tree");
    fs::write(directory.join("states_0.chkpt"), b"state").expect("write deep checkpoint state");
    let limits = traversal_limits(4, 4).with_depth(8);

    let error = inventory_with_limits(&root, &"1".repeat(64), deadline(), limits)
        .expect_err("deep checkpoint tree must be rejected");
    assert!(error.to_string().contains("depth limit of 8"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn checkpoint_many_empty_directories_exhaust_global_budgets() {
    let root = test_root("empty-directory-limit");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create empty checkpoint root");
    for index in 0..12 {
        fs::create_dir(root.join(format!("empty-{index:02}")))
            .expect("create empty checkpoint directory");
    }

    let directory_limits = traversal_limits(16, 4).with_directories(5);
    let directory_error =
        inventory_with_limits(&root, &"1".repeat(64), deadline(), directory_limits)
            .expect_err("too many empty checkpoint directories must be rejected");
    assert!(directory_error
        .to_string()
        .contains("global directory limit of 5"));

    let node_limits = traversal_limits(16, 4).with_nodes(5);
    let node_error = inventory_with_limits(&root, &"1".repeat(64), deadline(), node_limits)
        .expect_err("too many empty checkpoint nodes must be rejected");
    assert!(node_error.to_string().contains("global node limit of 5"));

    let cleanup_error = sanitize_cache_root_with_limits(&root, deadline(), directory_limits)
        .expect_err("cleanup must reject too many empty checkpoint directories");
    assert!(cleanup_error
        .to_string()
        .contains("global directory limit of 5"));
    for index in 0..12 {
        assert!(root.join(format!("empty-{index:02}")).is_dir());
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn transient_candidate_io_error_preserves_recovery_state() {
    let profile = format!("checkpoint-io-error-{}", std::process::id());
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output_dir);

    let configuration = BTreeMap::from([
        ("config".to_owned(), "Raft.cfg".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
    ]);
    let source_artifacts = INPUT_KINDS
        .into_iter()
        .map(|kind| ArtifactRef {
            kind: kind.to_owned(),
            path: format!("test-inputs/{kind}"),
            sha256: format!("{:0>64}", kind.len()),
            size_bytes: 1,
        })
        .collect::<Vec<_>>();
    let contract = expected_contract(&profile, &configuration, &source_artifacts)
        .expect("derive checkpoint contract");
    fs::create_dir_all(&root).expect("create checkpoint cache");
    fs::write(
        root.join("checkpoint-contract.json"),
        serde_json::to_vec_pretty(&contract).expect("serialize contract"),
    )
    .expect("write contract");
    fs::write(
        root.join("checkpoint-inventory.json"),
        serde_json::to_vec_pretty(&CheckpointInventory {
            schema_version: 1,
            contract_sha256: contract.sha256().expect("digest contract"),
            latest_checkpoint: None,
            files: Vec::new(),
        })
        .expect("serialize inventory"),
    )
    .expect("write inventory");
    fs::write(root.join("states"), b"temporarily unreadable state root")
        .expect("write invalid state root");

    let Err(error) = prepare(
        &profile,
        "1c642bc4fe001234567890123456789012345678",
        &configuration,
        &source_artifacts,
        &output_dir,
        deadline(),
    ) else {
        panic!("candidate I/O failure must remain a harness error")
    };
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    assert_eq!(
        fs::read(root.join("states")).expect("state root remains"),
        b"temporarily unreadable state root"
    );
    assert!(root.join("checkpoint-contract.json").is_file());
    assert!(root.join("checkpoint-inventory.json").is_file());

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(output_dir);
}

#[test]
fn checkpoint_inventory_rejects_an_expired_deadline_before_hashing() {
    let root = test_root("expired-deadline");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("26-07-12-00-00-00.000")).expect("create checkpoint");
    fs::write(root.join("26-07-12-00-00-00.000/states_0.chkpt"), b"state")
        .expect("write checkpoint");

    let error = inventory(&root, &"1".repeat(64), Instant::now())
        .expect_err("expired checkpoint inventory must fail");
    assert!(error.to_string().contains("deadline expired"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn checkpoint_hashing_checks_its_deadline_between_chunks() {
    let bytes = vec![7_u8; HASH_BUFFER_BYTES * 2];
    let mut checks = 0_u64;
    let error = hash_reader(std::io::Cursor::new(bytes), || {
        checks += 1;
        if checks >= 3 {
            Err("injected checkpoint hashing deadline".into())
        } else {
            Ok(())
        }
    })
    .expect_err("mid-stream deadline must interrupt hashing");
    assert!(error
        .to_string()
        .contains("injected checkpoint hashing deadline"));
    assert_eq!(checks, 3);
}

#[test]
fn checkpoint_metadata_and_directory_reads_obey_deadlines_and_size_bounds() {
    let root = test_root("bounded-metadata");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create metadata fixture");
    let metadata = root.join("inventory.json");
    fs::write(&metadata, vec![1_u8; HASH_BUFFER_BYTES * 2]).expect("write metadata fixture");

    assert!(read_file_with_deadline(&metadata, Instant::now(), "test metadata read").is_err());
    assert!(read_sorted_entries(&root, Instant::now(), "test directory read").is_err());

    let oversized = root.join("oversized.json");
    let file = fs::File::create(&oversized).expect("create oversized metadata fixture");
    file.set_len(MAX_CHECKPOINT_METADATA_BYTES + 1)
        .expect("extend sparse metadata fixture");
    let rejected = read_candidate_json::<CheckpointInventory>(&oversized, "inventory", deadline())
        .expect("oversized metadata is a candidate defect");
    assert!(rejected
        .expect_err("oversized metadata must be rejected")
        .contains("size limit"));
    let _ = fs::remove_dir_all(root);
}
