// This file is part of lfchring-rs.
//
// Copyright 2021 Christos Katsakioris
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher as StdHasher;

use thiserror::Error;

pub(crate) enum Update {
    Insert,
    Remove,
}

pub(crate) enum Adjacency {
    Predecessor,
    Successor,
}

/// A type for the internal ID of each virtual node of every distinct consistent hashing ring node.
///
/// This is merely a type alias for `u16` for now.
/// Therefore, each distinct [`Node`] in the [`HashRing<N, H>`] can be mapped at least once and at
/// most [`u16::MAX`] times on the consistent hashing ring.
///
///
///  [`HashRing<N, H>`]: ../struct.HashRing.html
pub type Vnid = u16;

/// A custom `Result` type for this crate, combining a return value with a [`HashRingError`].
///
/// It is used all over the crate and also returned by many functions and method in its external
/// API.
pub type Result<T> = std::result::Result<T, HashRingError>;

/// A trait to be implemented by any type that needs to act as a distinct node in the consistent
/// hashing ring.
pub trait Node {
    /// Returns a byte slice that uniquely identifies the particular [`Node`] from the rest of its
    /// kind.
    fn hashring_node_id(&self) -> Cow<'_, [u8]>;
}

impl Node for String {
    #[inline]
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.as_bytes())
    }
}

impl Node for str {
    #[inline]
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
}

impl Node for Vec<u8> {
    #[inline]
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.as_slice())
    }
}

impl Node for &[u8] {
    #[inline]
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self)
    }
}

impl Node for [u8] {
    #[inline]
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self)
    }
}

/// An error type returned by calls to the API exposed by this crate.
#[derive(Debug, Error)]
pub enum HashRingError {
    /// The configuration parameters for the construction of the consistent hashing ring appears to
    /// be invalid.
    ///
    /// The error contains the invalid values for the *replication factor* and the *number of
    /// virtual nodes* per ring node, in this order.
    // TODO: Better make it a struct variant.
    #[error("Invalid configuration: replication factor of {0} and {1} virtual nodes per node")]
    InvalidConfiguration(u8, Vnid),

    /// A hash collision has been detected; one of the virtual nodes probably already exists in the
    /// consistent hashing ring.
    // TODO: Reporting `VirtualNode`s as `String`s is probably useless. Is there any case where
    // exposing actual information (e.g., some struct) to the caller through a `HashRingError`
    // could turn out to be useful?
    #[error("Hash collision; Virtual node {0:?} may already exist in the ring")]
    VirtualNodeAlreadyExists(String),

    /// One of the virtual nodes does not exist in the consistent hashing ring.
    // TODO: Reporting `VirtualNode`s as `String`s is probably useless. Is there any case where
    // exposing actual information (e.g., some struct) to the caller through a `HashRingError`
    // could turn out to be useful?
    #[error("Virtual node {0:?} does not exist in the ring")]
    VirtualNodeDoesNotExist(String),

    /// The *write operation* on the consistent hashing ring failed due to some other write
    /// operation occurring concurrently on it.
    #[error("Concurrent compare-and-swap modification detected")]
    ConcurrentModification,

    /// The consistent hashing ring is currently empty.
    #[error("HashRing is empty")]
    EmptyRing,

    /// Only one [`Node`] currently populates the consistent hashing ring.
    #[error("HashRing has only one distinct node")]
    SingleDistinctNodeRing,
}

/// A trait to be implemented by any type that needs to act as a hash algorithm implementation.
///
/// For general information about why this is required or how to use the built-in [`Hasher`],
/// please refer to the crate-level documentation and the documentation of the constructor methods
/// of the [`HashRing<N, H>`].
///
///
///  [`HashRing<N, H>`]: ../struct.HashRing.html
// NOTE: The `Hasher` must also be `Default` as a means of instantiating it anew. Alternatively,
// maybe there could be an aditional requirement for a `reset()` function on `Hasher`, to allow
// implementations to reset `Hasher`'s internal state without actually instantiating a new struct.
pub trait Hasher: Default {
    /// Given a byte slice, returns a hash digest as an owned [`Vec`] of `u8`.
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8>;
}

/// A [`Hasher`] implementation for standard library's [`DefaultHasher`].
#[derive(Debug, Default)]
pub struct DefaultStdHasher;

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

/// A [`Hasher`] implementation based on the [BLAKE3][BLAKE3.io] cryptographic hash function, as
/// implemented in the [blake3][blake3] crate.
///
/// To use this `Hasher` implementation in `lfchring-rs`, the `blake3-hash` crate feature must be
/// enabled.
///
/// # Examples
///
/// Assuming the `blake3-hash` feature is enabled, a [`HashRing<N, H>`] that uses it can be
/// initialized as shown below (for more initialization options, refer to the documentation of
/// [`HashRing<N, H>`]):
///
/// ```rust
/// use lfchring::{Blake3Hasher, HashRing, Result, Vnid};
///
/// const VIRTUAL_NODES_PER_NODE: Vnid = 2;
/// const REPLICATION_FACTOR: u8 = 3;
///
/// # fn main() -> Result<()> {
/// let ring: HashRing<str, Blake3Hasher> = HashRing::with_hasher(
///     Blake3Hasher::default(),
///     VIRTUAL_NODES_PER_NODE,
///     REPLICATION_FACTOR
/// )?;
/// # Ok(())
/// # }
/// ```
///
///  [BLAKE3.io]: https://blake3.io/
///  [blake3]: https://docs.rs/blake3/0.3/blake3/
///  [`HashRing<N, H>`]: struct.HashRing.html
#[cfg(any(feature = "blake3-hash", doc))]
#[derive(Debug, Default)]
pub struct Blake3Hasher;

#[cfg(feature = "blake3-hash")]
impl Hasher for Blake3Hasher {
    #[inline]
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8> {
        blake3::hash(bytes).as_bytes().to_vec()
    }
}

/// A [`Hasher`] implementation based on the [BLAKE2b][BLAKE2b] cryptographic hash function, as
/// implemented in the [blake2b_simd][blake2b_simd] crate.
///
/// To use this `Hasher` implementation in `lfchring-rs`, the `blake2b-hash` crate feature must be
/// enabled.
///
/// # Examples
///
/// Assuming the `blake2b-hash` feature is enabled, a [`HashRing<N, H>`] that uses it can be
/// initialized as shown below (for more initialization options, refer to the documentation of
/// [`HashRing<N, H>`]):
///
/// ```rust
/// use lfchring::{Blake2bHasher, HashRing, Result, Vnid};
///
/// const VIRTUAL_NODES_PER_NODE: Vnid = 2;
/// const REPLICATION_FACTOR: u8 = 3;
///
/// # fn main() -> Result<()> {
/// let ring: HashRing<str, Blake2bHasher> = HashRing::with_hasher(
///     Blake2bHasher::default(),
///     VIRTUAL_NODES_PER_NODE,
///     REPLICATION_FACTOR
/// )?;
/// # Ok(())
/// # }
/// ```
///
///  [BLAKE2b]: https://www.blake2.net/
///  [blake2b_simd]: https://docs.rs/blake2b_simd/0.5/blake2b_simd/
///  [`HashRing<N, H>`]: struct.HashRing.html
#[cfg(any(feature = "blake2b-hash", doc))]
#[derive(Debug, Default)]
pub struct Blake2bHasher;

#[cfg(feature = "blake2b-hash")]
impl Hasher for Blake2bHasher {
    #[inline]
    fn digest(&mut self, bytes: &[u8]) -> Vec<u8> {
        blake2b_simd::blake2b(bytes).as_bytes().to_vec()
    }
}
