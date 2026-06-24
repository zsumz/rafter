use std::{fs, path::Path};

use rafter_storage::FileRaftHardStateStore;

#[test]
fn errors_propagate_with_question_mark() {
    fn open_corrupt_store(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        FileRaftHardStateStore::open(path)?;
        Ok(())
    }

    let path = std::env::temp_dir().join(format!(
        "rafter-storage-errors-propagate-{}.rafthard",
        std::process::id()
    ));
    fs::write(&path, b"bad").expect("corrupt store is written");

    let error = open_corrupt_store(&path).expect_err("corrupt store must not open");

    assert_eq!(
        error.to_string(),
        "stored Raft hard state is corrupt: \
         Raft hard-state envelope needs 4 bytes but only 3 remain"
    );
    let source = error.source().expect("open error wraps the decode error");
    assert_eq!(
        source.to_string(),
        "Raft hard-state envelope needs 4 bytes but only 3 remain"
    );
    let _ = fs::remove_file(&path);
}
