//! Thirteen actual process exits, including strict refusal of an unsealed frame.

use std::{fs, path::Path};

use rafter::LogIndex;

use crate::store::{raw_journal, LedgerStore, LedgerStoreError, TornTail};

use super::{arm, model, process, Point};

const TEST: &str = "store::process_test::cases::ledger_publication_survives_process_loss";
const CHILD: &str = "RAFTER_LEDGER_PUBLICATION_CHILD";
const POINTS: [Point; 10] = [
    Point::CreateAfterTempSync,
    Point::CreateAfterRename,
    Point::CreateAfterDirectorySync,
    Point::AppendAfterWrite,
    Point::AppendAfterFrameSync,
    Point::AppendAfterSealWrite,
    Point::AppendAfterSealSync,
    Point::RewriteAfterTempSync,
    Point::RewriteAfterRename,
    Point::RewriteAfterDirectorySync,
];

#[test]
#[ignore = "spawns and kills ledger writers at publication boundaries"]
fn ledger_publication_survives_process_loss() {
    if let Some(root) = std::env::var_os(CHILD) {
        run_child(Path::new(&root));
        return;
    }
    let workspace = process::Workspace::new();
    let mut completed = 0;
    for (index, point) in POINTS.into_iter().enumerate() {
        run_case(workspace.path(), index, point, false);
        completed += 1;
        if index >= 7 {
            run_case(workspace.path(), index, point, true);
            completed += 1;
        }
    }
    assert_eq!(completed, 13);
    fs::write(workspace.path().join("completed.txt"), "13 cases passed\n")
        .expect("completion receipt writes");
    println!("ledger publication matrix: {completed} verified SIGKILL/recovery cases");
}

fn run_case(root: &Path, index: usize, point: Point, compact: bool) {
    let case = root.join(format!("compact-{compact}-{point:?}"));
    fs::create_dir(&case).expect("exclusive case directory");
    let database = case.join("database");
    fs::create_dir(&database).expect("database directory creates");
    if index >= 3 {
        let mut store = LedgerStore::open(&database, model::config()).expect("seed opens");
        model::seed(&mut store);
    }
    fs::write(case.join("operation.txt"), format!("{index}\n{compact}\n"))
        .expect("child operation records");
    let pid = process::kill_at_boundary(&case, TEST, CHILD, point);
    process::preserve_inputs(&database, &case.join("pre-recovery"));

    let ambiguous = matches!(point, Point::AppendAfterWrite | Point::AppendAfterFrameSync);
    let mut store = if ambiguous {
        let before = raw_journal::read(&database).expect("unsealed journal reads");
        let expected_len = LedgerStore::planned_frame_len(&model::image(5), LogIndex(5))
            .expect("planned append encodes");
        let refused = LedgerStore::open(&database, model::config()).expect_err("strict refusal");
        assert!(matches!(refused, LedgerStoreError::UnreadableFrame {
            corruption: TornTail::UnsealedCompleteFrame { len }, ..
        } if len == expected_len));
        assert_eq!(
            raw_journal::read(&database).expect("refused journal rereads"),
            before
        );
        // Repair is explicit, after copying the original inputs; ordinary open never repairs.
        let repaired = LedgerStore::open_and_repair(&database, model::config())
            .expect("explicit repair resolves only the unacknowledged append");
        let report = repaired
            .recovery()
            .repair()
            .expect("repair reports discarded frame");
        assert_eq!(report.discarded_bytes(), expected_len);
        assert_eq!(
            report.corruption(),
            TornTail::UnsealedCompleteFrame { len: expected_len }
        );
        repaired
    } else {
        LedgerStore::open(&database, model::config()).expect("strict recovery opens")
    };
    let count = match point {
        Point::CreateAfterTempSync | Point::CreateAfterRename | Point::CreateAfterDirectorySync => {
            0
        }
        Point::AppendAfterWrite | Point::AppendAfterFrameSync | Point::RewriteAfterTempSync => 4,
        _ if compact => 4,
        _ => 5,
    };
    model::assert_state(&store, count);
    let frames = match point {
        Point::RewriteAfterRename | Point::RewriteAfterDirectorySync => 1,
        _ => count,
    };
    assert_eq!(store.recovery().committed_frames(), frames as u64);
    model::continue_after_recovery(&mut store, count);
    let expected = store.ledger().view();
    let next_index = store.applied_index();
    drop(store);
    let reopened = LedgerStore::open(&database, model::config()).expect("next commit reopens");
    assert_eq!(reopened.applied_index(), next_index);
    assert_eq!(reopened.ledger().view(), expected);
    fs::write(case.join("outcome.txt"), format!(
        "point={point:?}\ncompact={compact}\npid={pid}\nsignal=9\nexplicit_repair={ambiguous}\nrecovered_index={count}\nnext_index={}\n", next_index.0))
        .expect("case receipt writes");
    println!("ledger process case: compact={compact} point={point:?} passed");
}

fn run_child(root: &Path) {
    let operation = fs::read_to_string(root.join("operation.txt")).expect("child operation reads");
    let mut lines = operation.lines();
    let index: usize = lines.next().expect("index").parse().expect("numeric index");
    let compact: bool = lines.next().expect("kind").parse().expect("boolean kind");
    assert!(lines.next().is_none());
    let database = root.join("database");
    if index < 3 {
        arm(POINTS[index], root.join("boundary.txt"));
        LedgerStore::open(&database, model::config()).expect("creation reaches boundary");
    } else {
        let mut store = LedgerStore::open(&database, model::config()).expect("child opens seed");
        model::assert_state(&store, 4);
        arm(POINTS[index], root.join("boundary.txt"));
        if index < 7 {
            store
                .commit(&model::image(5), LogIndex(5))
                .expect("append reaches boundary");
        } else if compact {
            store.compact().expect("compaction reaches boundary");
        } else {
            store
                .replace(&model::image(5), LogIndex(5))
                .expect("replacement reaches boundary");
        }
    }
    panic!("publication returned without reaching the armed process boundary");
}
