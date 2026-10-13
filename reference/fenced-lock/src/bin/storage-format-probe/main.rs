//! Public-API fixture built independently against released and candidate archives.

mod fixture;
mod stores;

use std::{error::Error, path::Path};

use rafter_storage::{
    DecodeRaftHardStateError, FileRaftNodeStores, OpenFileRaftNodeStoresError,
    OpenRaftHardStateStoreError,
};
use stores::Stores;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        return Err("expected: OPERATION log|snapshot|pending DIRECTORY".into());
    }
    let mode = args[1].as_str();
    let case = args[2].as_str();
    if !matches!(case, "log" | "snapshot" | "pending") {
        return Err("unknown storage case".into());
    }
    let directory = Path::new(&args[3]);
    if mode == "refuse-version" {
        let result = FileRaftNodeStores::open(directory);
        assert!(
            matches!(
                result,
                Err(OpenFileRaftNodeStoresError::HardState(
                    OpenRaftHardStateStoreError::Decode(
                        DecodeRaftHardStateError::UnsupportedVersion(99)
                    )
                ))
            ),
            "expected unsupported hard-state version: {result:?}"
        );
    } else {
        if !matches!(mode, "write" | "check" | "extend" | "check-extended") {
            return Err("unknown storage operation".into());
        }
        if mode == "write" {
            std::fs::create_dir(directory)?;
        }
        let mut stores = Stores::open(directory)?;
        match mode {
            "write" => stores.write(case)?,
            "check" => stores.check(case),
            "extend" => {
                stores.check(case);
                stores.extend(case)?;
            }
            "check-extended" => stores.check_extended(case),
            _ => unreachable!(),
        }
    }
    println!("passed {mode} {case}");
    Ok(())
}
