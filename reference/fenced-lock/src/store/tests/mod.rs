//! Unit checks over the store's own bytes, beside what each one is about.
//!
//! Everything here is a property of the slot format or of one refusal's
//! rendering. The behaviour of a store on a filesystem is checked by the
//! crate's test binaries, which reach it the way a consumer does.

mod damage;
mod error;
mod format;
mod image;
mod support;
