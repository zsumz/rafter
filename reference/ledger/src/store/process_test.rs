//! Process-loss hooks compiled only into the Unix library test binary.

use std::{cell::RefCell, fs, path::PathBuf, thread};

mod cases;
mod model;
mod process;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Point {
    CreateAfterTempSync,
    CreateAfterRename,
    CreateAfterDirectorySync,
    AppendAfterWrite,
    AppendAfterFrameSync,
    AppendAfterSealWrite,
    AppendAfterSealSync,
    RewriteAfterTempSync,
    RewriteAfterRename,
    RewriteAfterDirectorySync,
}

thread_local! {
    static PAUSE: RefCell<Option<(Point, PathBuf)>> = const { RefCell::new(None) };
}

fn arm(point: Point, marker: PathBuf) {
    PAUSE.with(|slot| {
        assert!(slot.replace(Some((point, marker))).is_none());
    });
}

pub(super) fn pause(point: Point) {
    let marker = PAUSE.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|(selected, _)| *selected == point)
            .map(|(_, path)| path.clone())
    });
    if let Some(marker) = marker {
        fs::write(marker, format!("{point:?}\n")).expect("publication boundary records");
        // Only the parent may release this process, by verified SIGKILL.
        loop {
            thread::park();
        }
    }
}
