//! Blocking persistent sender, listener, and authenticated receiver adapters.

mod acceptor;
mod deadline;
mod dial;
mod io;
mod receiver;
mod receiver_registry;
mod sender;
mod snapshot;

pub(crate) use acceptor::{accept_loop, AcceptorContext};
pub(crate) use receiver::ReceiverTemplate;
pub(crate) use receiver_registry::ReceiverRegistry;
pub(crate) use sender::{sender_loop, SenderContext};
