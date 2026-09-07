//! Every way a durable runtime is opened over durable storage.
//!
//! Constructors that fill in in-memory stores, the one hydration path they all
//! reach, and the recovering pair that hand back committed replay outputs each
//! live in a focused child module. All of them end at the same durable truth:
//! hard state, a retained log suffix, and a snapshot store.

mod defaults;
mod hydrate;
mod recover;
