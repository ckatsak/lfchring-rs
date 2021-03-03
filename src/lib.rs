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

//! This crate implements a concurrent, lock-free [consistent hashing ring] data structure.
//!
//! # Features
//!
//! This section documents the features of the data structure implementation.
//!
//! Information about the supported crate `features` can be found throughout this documentation
//! page.
//!
//! ## Virtual Nodes
//!
//! The crate includes support for a configurable number of *virtual nodes* per ring node (i.e.,
//! each node can be mapped and placed multiple times on the ring, and the number of these times is
//! configurable) to allow for improved efficiency in load balancing scenarios.
//!
//! The number of virtual nodes per ring node is configurable only at the time of the creation of
//! the data structure.
//!
//! ## Replication
//!
//! Moreover, the crate includes support for keeping track of key replication.
//! This means that, provided with a replication factor of the caller's choice, looking up keys in
//! the consistent hashing ring data structure reports all the distinct ring nodes on which
//! replicas of the key at hand are assigned.
//! Note that consecutive virtual nodes on a consistent hashing may belong to the same distinct
//! ring node; this crate makes sure that keys are assigned to a number of *distinct* ring nodes
//! equal to the (configurable) replication factor (as long as they are available), by skipping
//! some virtual nodes if required.
//!
//! The replication factor is configurable only at the time of the creation of the data structure.
//!
//! ## Original Purpose
//!
//! The primary purpose of this crate has been to be work correctly in multi-threaded contexts,
//! i.e., in cases where multiple threads have access to the consistent hashing ring data
//! structure and operate on it at the same time.
//! More specifically, it has been designed and implemented mainly for use cases where a large
//! number of reader threads may need to access it, whereas write operations are sparse
//! (if you are unsure whether you know what *read* and *write* operations may refer to, do not
//! worry, please read on).
//! Nevertheless, the data structure can be used in single-threaded environments equally well.
//!
//! ---
//!
//! The ring data structure is represented by the exported [`HashRing<N, H>`] type, which is
//! generic over the [`Node`] and the [`Hasher`].
//! Virtual nodes are represented by the exported [`VirtualNode<N>`] type.
//!
//! ## Node
//!
//! Any type can be used as a node of the consistent hashing ring, as long as it implements the
//! [`Node`] trait.
//! The implementation of this trait requires a method ([`Node::hashring_node_id`]) which allows to
//! uniquely represent the type as a byte slice.
//! Implementing the [`Node`] trait can be as trivial as:
//!
//! ```rust
//! # use std::borrow::Cow;
//! # use lfchring::Node;
//! struct ExampleNode {
//!     various: u64,
//!     fields: Vec<i32>,
//!     // . . .
//!     unique_name: String,
//! }
//!
//! impl Node for ExampleNode {
//!     fn hashring_node_id(&self) -> Cow<'_, [u8]> {
//!         Cow::Borrowed(&self.unique_name.as_bytes())
//!     }
//! }
//! ```
//!
//! or:
//!
//! ```rust
//! # use std::borrow::Cow;
//! # use lfchring::Node;
//! struct StrNode(str);
//!
//! impl Node for StrNode {
//!     fn hashring_node_id(&self) -> Cow<'_, [u8]> {
//!         Cow::Borrowed(self.0.as_bytes())
//!     }
//! }
//! ```
//!
//! Note that [`Node`] can be unsized and that the crate already provides implementations for the
//! following types:
//!  - `String`
//!  - `str`
//!  - `Vec<u8>`
//!  - `&[u8]`
//!  - `[u8]`
//!
//! ## Hasher
//!
//! For a `Node` to be inserted in (or removed from) the consistent hashing ring, it needs to be
//! hashed one or more times (depending on the configured number of virtual nodes per `Node`).
//! The produced virtual nodes are then appropriately "placed" on the ring based on their hash
//! digests.
//!
//! Multiple hash algorithms can be used.
//! The crate includes an implementation of [`HashRing<N, H>`] that employs standard library's
//! [`DefaultHasher`][DefaultHasher] (constructed via [`HashRing::new`] or
//! [`HashRing::with_nodes`]).
//! However, you may also bring your own hash algorithm implementation by implementing the
//! [`Hasher`] trait.
//! This trait defines the [`Hasher::digest`] method which, provided an input byte slice, produces
//! a hash digest as an owned `Vec<u8>`, to be used for placing the `Node`s on the consistent
//! hashing ring in the correct order.
//!
//! Not relying on standard library's machinery for hashing is a design choice that allows using
//! [`HashRing<N, H>`] with hash functions that produce digests longer than `u64`.
//!
//! ### Feature `blake3-hash`
//!
//! By enabling the `blake3-hash` crate feature, a [`Hasher`] that employs the [BLAKE3][BLAKE3.io]
//! cryptographic hash function as implemented in the [blake3 crate][blake3] is made available.
//!
//! For more information, refer to the documentation of the [`Blake3Hasher`] type.
//!
//! ### Feature `blake2b-hash`
//!
//! A [`Hasher`] implementation based on the [BLAKE2b][BLAKE2b] cryptographic hash function and the
//! [blake2b_simd crate][blake2b_simd] is available by enabling the `blake2b-hash` crate feature.
//!
//! For more information, refer to the documentation of the [`Blake2bHasher`] type.
//!
//! ## The Two Main Kinds of Operations
//!
//! All supported operations on the ring fall into two main categories: "read-like" and
//! "write-like".
//! - *Read operations* require access to the information stored in the ring, but they do *not*
//!   mutate the ring itself.
//!   The most common example of such an operation is looking up where a key should be assigned to
//!   according to the consistent hashing algorithm (e.g., see [`HashRing::nodes_for_key`]).
//!   Another example is to loop through all the virtual nodes that are contained in the consistent
//!   hashing ring via an iterator (e.g., see [`HashRing::iter`] and [`Iter`]).
//! - *Write operations* are considered those that effectively **mutate** the ring itself.
//!   Common examples are the [`HashRing::insert`] and [`HashRing::remove`] methods, which insert
//!   new nodes to the consistent hashing ring, or remove existing ones, respectively.
//!
//! ## Example
//!
//! What follows is a basic, single-threaded example to showcase its use:
//!
//! ```rust
//! use std::sync::Arc;
//! use hex_literal::hex;
//! use lfchring::{HashRing, Node, VirtualNode, Vnid};
//!
//! const VIRTUAL_NODES_PER_NODE: Vnid = 2;
//! const REPLICATION_FACTOR: u8 = 3;
//!
//! // Create an empty ring (see the docs for further options on constructing a ring).
//! let ring = HashRing::new(VIRTUAL_NODES_PER_NODE, REPLICATION_FACTOR).unwrap();
//!
//! // Insert three new Nodes.
//! let nodes: Vec<Arc<str>> = vec![Arc::from("Node1"), Arc::from("Node2"), Arc::from("Node3")];
//! ring.insert(&nodes).expect("hash collision when inserting 3 new Nodes");
//!
//! assert_eq!(ring.len_nodes(), 3);
//! assert_eq!(ring.len_virtual_nodes(), 3 * VIRTUAL_NODES_PER_NODE as usize);
//!
//! // Look up a key in the ring.
//! let key = hex!("232a8a941ee901c1");
//! let virtual_node_clone = ring.virtual_node_for_key(&key).unwrap();
//!
//! // The Nodes that should own a replica for the particular key can also be found via:
//! let replica_owning_nodes = ring.nodes_for_key(&key).unwrap();
//! assert_eq!(replica_owning_nodes, virtual_node_clone.replica_owners());
//!
//! // In this example, since the ring is populated with 3 Nodes and the replication factor is also
//! // equal to 3, all Nodes should be assigned with a replica of the key.
//! // However, the following assertion would fail, because of the order in which the replica
//! // owning Nodes are reported: they are reported according to the order in which their virtual
//! // nodes have been placed on the ring, rather than the order of the Nodes were inserted.
//! assert_ne!(nodes, replica_owning_nodes); // `ne` due to the order in which Nodes are reported!
//! ```
//!
//! # Implementation Notes
//!
//! ## `Arc`s
//!
//! Mind that in multi-threaded contexts, users of [`HashRing<N, H>`] should explicitly wrap it in
//! an [`Arc`][Arc] to share it among the threads.
//! Single-threaded contexts may opt out of this to avoid the overhead of atomic reference
//! counting; however, [`Arc`][Arc]s are being used internally all over the crate anyway.
//!
//! ## RCU-like Synchronization
//!
//! The soundness of the implementation of the crate is undergirded by a
//! [Read-Copy-Update][RCU]-like technique for synchronizing concurrent accesses to the consistent
//! hashing ring data structure by multiple threads.
//! This means that the implementation of the data structure does not rely on locks, but on
//! atomically reading a pointer internal to [`HashRing<N, H>`], copying the data, updating the
//! local copy and then atomically storing the new address to the pointer.
//! The primitives upon which the implementation is based belong to the [`crossbeam_epoch`] crate.
//!
//! An alternative to this would be to use a [`Mutex`][Mutex] or a [`RwLock`][RwLock], but this is
//! not how this crate has been implemented.
//!
//! ## Garbage Collection
//!
//! Concurrent read and write operations on a [`HashRing<N, H>`] using a [RCU][RCU]-like technique
//! introduce a major issue:
//! when a mutation occurs, the underlying data structure pointed to internally by the
//! [`HashRing<N, H>`] is no longer needed, and is therefore dropped and immediately freed.
//! However, a number of concurrent reader threads may still require access to it.
//! Garbage-collected languages, like Java and Go, trivially solve this issue by relying on their
//! runtime's garbage collector to clean it up only when no threads can access it.
//! Rust's lightweight runtime does not include a garbage collector; it rather relies on its unique
//! RAII-based system.
//!
//! To address this issue, this crate is based on the [`crossbeam_epoch`] crate, which enables
//! epoch-based memory reclamation.
//! This is also where the [`Guard`] type and the [`pin`] function are re-exported from, for ease
//! of use.
//! It is highly recommended for anyone that considers using this crate to go through the
//! documentation of the [`crossbeam_epoch`] crate as well.
//!
//! ## Performance
//!
//! The crate has not been properly benchmarked and evaluated yet.
//!
//! Informally, it certainly looks and feels fast, especially in the aforementioned cases of
//! multiple concurrent reader threads and rare write operations.
//!
//!
//!  [consistent hashing ring]: https://en.wikipedia.org/wiki/Consistent_hashing
//!  [DefaultHasher]: https://doc.rust-lang.org/std/collections/hash_map/struct.DefaultHasher.html
//!  [BLAKE3.io]: https://blake3.io/
//!  [blake3]: https://docs.rs/blake3/0.3/blake3/
//!  [BLAKE2b]: https://www.blake2.net/
//!  [blake2b_simd]: https://docs.rs/blake2b_simd/0.5/blake2b_simd/
//!  [Arc]: https://doc.rust-lang.org/std/sync/struct.Arc.html
//!  [RCU]: https://en.wikipedia.org/wiki/Read-copy-update
//!  [Mutex]: https://doc.rust-lang.org/std/sync/struct.Mutex.html
//!  [RwLock]: https://doc.rust-lang.org/std/sync/struct.RwLock.html
//!  [`crossbeam_epoch`]: https://docs.rs/crossbeam-epoch/0.9/crossbeam_epoch/index.html
// Also see: https://morestina.net/blog/742/exploring-lock-free-rust-1-locks

#![doc(html_root_url = "https://docs.rs/lfchring-rs/0.1.2")]
#![warn(rust_2018_idioms)]
#![deny(
    missing_docs,
    //missing_doc_code_examples, // nightly-only?
    unreachable_pub,
    broken_intra_doc_links,
)]
//#![allow(dead_code, unused_variables, unused_imports)]

mod iter;
mod ring;
mod state;
mod types;
mod vnode;

#[cfg(test)]
mod tests;

pub use iter::Iter;
pub use ring::HashRing;
pub use types::HashRingError;
pub use types::Hasher;
pub use types::Node;
pub use types::Result;
pub use types::Vnid;
pub use vnode::VirtualNode;

#[cfg(feature = "blake2b-hash")]
pub use types::Blake2bHasher;
#[cfg(feature = "blake3-hash")]
pub use types::Blake3Hasher;

pub use crossbeam_epoch::{pin, Guard};
