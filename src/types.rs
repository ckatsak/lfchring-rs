use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher as StdHasher;

use thiserror::Error;

pub type Vnid = u16;

pub type Result<T> = std::result::Result<T, HashRingError>;

/// Node represents a single distinct node in the ring.
pub trait Node {
    /// Retrieve a name that uniquely identifies the particular Node.
    fn hashring_node_id(&self) -> Cow<'_, [u8]>;
}

// TODO: Reporting `VirtualNode`s as `String`s is probably useless. Is there any case where
// exposing actual information (e.g., some struct) to the caller through `HashRingError` could turn
// out to be useful?
#[derive(Debug, Error)]
pub enum HashRingError {
    #[error("Invalid configuration: replication factor of {0} and {1} virtual nodes per node")]
    InvalidConfiguration(u8, Vnid),
    #[error("Hash collision; Virtual node {0:?} may already exist in the ring")]
    VirtualNodeAlreadyExists(String),
    #[error("Virtual node {0:?} does not exist in the ring")]
    VirtualNodeDoesNotExist(String),
    #[error("Concurrent compare-and-swap modification detected")]
    ConcurrentModification,
    #[error("HashRing is empty")]
    EmptyRing,
    #[error("HashRing has only one distinct node")]
    SingleDistinctNodeRing,
}

pub(crate) enum Update {
    Insert,
    Remove,
}

pub(crate) enum Adjacency {
    Predecessor,
    Successor,
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//
// Hasher
//
//
///////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: The `Hasher` must also be `Default` as a means of instantiating it anew. Alternatively,
// maybe there could be an aditional requirement for a `reset()` function on `Hasher`, to allow
// implementations to reset `Hasher`'s internal state without actually instantiating a new struct.
pub trait Hasher: Default {
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8>;
}

#[derive(Debug, Default)]
pub(crate) struct DefaultStdHasher;

impl Hasher for DefaultStdHasher {
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut h = DefaultHasher::default();
        h.write(bytes);
        h.finish().to_ne_bytes().to_vec()
    }
}

//impl std::fmt::Debug for DefaultStdHasher {
//    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//        //write!(f, "std::collections::hash_map::DefaultHasher")
//        write!(f, "DefaultStdHasher{{}}")
//    }
//}
