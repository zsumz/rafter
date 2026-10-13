//! Unit checks over the store's own bytes, beside what each one is about.
//!
//! Everything here is a property of the journal format. The behaviour of a
//! store on a filesystem is checked by the crate's test binaries, which reach
//! it the way a consumer does. The sibling `process_test` module additionally
//! kills a child at the actual journal publication boundaries.

mod damage;
mod format;
mod frame;
mod support;
