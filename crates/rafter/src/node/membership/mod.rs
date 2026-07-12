//! Membership views and safe dynamic-membership transitions.
//!
//! The view path names static, effective, committed, and snapshot membership.
//! The change path validates and appends stable or joint configuration entries.

mod change;
mod validate;
mod view;
