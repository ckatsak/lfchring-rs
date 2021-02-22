//! TODO: Crate documentation
//!
//! - In multi-threaded context, `HashRing` should be explicitly wrapped in `Arc`. This is
//! deliberate, to expose the hidden cost of atomic reference counting to the callers and also give
//! a chance to single-threaded contexts to opt out of it.

//#![deny(missing_docs)]
//#![deny(missing_doc_code_examples)]
#![allow(dead_code, unused_variables, unused_imports)]

mod iter;
mod ring;
mod state;
mod types;
mod vnode;

#[cfg(test)]
mod tests;

pub use iter::Iter;
pub use ring::HashRing;
pub use types::{HashRingError, Hasher, Node, Result, Vnid};
pub use vnode::VirtualNode;

pub use crossbeam_epoch::{pin, Guard};
