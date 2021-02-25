<!-- this file uses https://github.com/livioribeiro/cargo-readme -->
<!-- do not manually edit README.md, instead edit README.tpl or src/lib.rs -->

# lfchring-rs

[![Crates.io](https://img.shields.io/crates/v/cargo-readme.svg)](https://crates.io/crates/lfchring-rs)
[![docs.rs](https://docs.rs/lfchring-rs/badge.svg)](https://docs.rs/lfchring-rs)
[![GitHub](https://img.shields.io/github/license/ckatsak/lfchring-rs?style=flat)](#license)
[![deps.rs](https://deps.rs/repo/github/ckatsak/lfchring-rs/status.svg)](https://deps.rs/repo/github/ckatsak/lfchring-rs)
[![GitHub Workflow Status](https://github.com/ckatsak/lfchring-rs/actions/workflows/basic.yml/badge.svg?branch=main)](https://github.com/ckatsak/lfchring-rs/actions/workflows/basic.yml)
<!--[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/ckatsak/lfchring-rs/basic.yml)](https://github.com/ckatsak/lfchring-rs/actions/workflows/basic.yml)-->

This crate implements a concurrent, lock-free [consistent hashing ring] data structure.

## Features

### Virtual Nodes

The crate includes support for a configurable number of *virtual nodes* per ring node (i.e.,
each node can be mapped and placed multiple times on the ring, and the number of these times is
configurable) to allow for improved efficiency in load balancing scenarios.

The number of virtual nodes per ring node is configurable only at the time of the creation of
the data structure.

### Replication

Moreover, the crate includes support for keeping track of key replication.
This means that, provided with a replication factor of the caller's choice, looking up keys in
the consistent hashing ring data structure reports all the distinct ring nodes on which
replicas of the key at hand are assigned.
Note that consecutive virtual nodes on a consistent hashing may belong to the same distinct
ring node; this crate makes sure that keys are assigned to a number of *distinct* ring nodes
equal to the (configurable) replication factor (as long as they are available), by skipping
some virtual nodes if required.

The replication factor is configurable only at the time of the creation of the data structure.

### Original Purpose

The primary purpose of this crate has been to be work correctly in multi-threaded contexts,
i.e., in cases where multiple threads have access to the consistent hashing ring data
structure and operate on it at the same time.
More specifically, it has been designed and implemented mainly for use cases where a large
number of reader threads may need to access it, whereas write operations are sparse
(if you are unsure whether you know what *read* and *write* operations may refer to, do not
worry, please read on).
Nevertheless, the data structure can be used in single-threaded environments equally well.

---

The ring data structure is represented by the exported [`HashRing<N, H>`] type, which is
generic over the [`Node`] and the [`Hasher`].
Virtual nodes are represented by the exported [`VirtualNode<N>`] type.

### Node

Any type can be used as a node of the consistent hashing ring, as long as it implements the
[`Node`] trait.
The implementation of this trait requires a method ([`Node::hashring_node_id`]) which allows to
uniquely represent the type as a byte slice.
Implementing the [`Node`] trait can be as trivial as:

```rust
impl Node for String {
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.as_bytes())
    }
}

// Node can be unsized
impl Node for str {
    fn hashring_node_id(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
}
```

### Hasher

For a `Node` to be inserted in (or removed from) the consistent hashing ring, it needs to be
hashed one or more times (depending on the configured number of virtual nodes per `Node`).
The produced virtual nodes are then appropriately "placed" on the ring based on their hash
digests.

Multiple hash algorithms can be used.
The crate includes an implementation of [`HashRing<N, H>`] that employs standard library's
[`DefaultHasher`][DefaultHasher] (constructed via [`HashRing::new`] or
[`HashRing::with_nodes`]).
However, you may also bring your own hash algorithm implementation by implementing the
[`Hasher`] trait.
This trait defines the [`Hasher::digest`] method which, provided an input byte slice, produces
a hash digest as an owned `Vec<u8>`, to be used for placing the `Node`s on the consistent
hashing ring in the correct order.

Not relying on standard library's machinery for hashing is a design choice that allows using
[`HashRing<N, H>`] with hash functions that produce digests longer than `u64`.

### The Two Main Kinds of Operations

All supported operations on the ring fall into two main categories: "read-like" and
"write-like".
- *Read operations* require access to the information stored in the ring, but they do *not*
  mutate the ring itself.
  The most common example of such an operation is looking up where a key should be assigned to
  according to the consistent hashing algorithm (e.g., see [`HashRing::nodes_for_key`]).
  Another example is to loop through all the virtual nodes that are contained in the consistent
  hashing ring via an iterator (e.g., see [`HashRing::iter`] and [`Iter`]).
- *Write operations* are considered those that effectively **mutate** the ring itself.
  Common examples are the [`HashRing::insert`] and [`HashRing::remove`] methods, which insert
  new nodes to the consistent hashing ring, or remove existing ones, respectively.

## Implementation Notes

### `Arc`s

Mind that in multi-threaded contexts, users of [`HashRing<N, H>`] should explicitly wrap it in
an [`Arc`][Arc] to share it among the threads.
Single-threaded contexts may opt out of this to avoid the overhead of atomic reference
counting; however, [`Arc`][Arc]s are being used internally all over the crate anyway.

### RCU-like Synchronization

The soundness of the implementation of the crate is undergirded by a
[Read-Copy-Update][RCU]-like technique for synchronizing concurrent accesses to the consistent
hashing ring data structure by multiple threads.
This means that the implementation of the data structure does not rely on locks, but on
atomically reading a pointer internal to [`HashRing<N, H>`], copying the data, updating the
local copy and then atomically storing the new address to the pointer.
The primitives upon which the implementation is based belong to the [`crossbeam_epoch`] crate.

An alternative to this would be to use a [`Mutex`][Mutex] or a [`RwLock`][RwLock], but this is
not how this crate has been implemented.

### Garbage Collection

Concurrent read and write operations on a [`HashRing<N, H>`] using a [RCU][RCU]-like technique
introduce a major issue:
when a mutation occurs, the underlying data structure pointed to internally by the
[`HashRing<N, H>`] is no longer needed, and is therefore dropped and immediately freed.
However, a number of concurrent reader threads may still require access to it.
Garbage-collected languages, like Java and Go, trivially solve this issue by relying on their
runtime's garbage collector to clean it up only when no threads can access it.
Rust's lightweight runtime does not include a garbage collector; it rather relies on its unique
RAII-based system.

To address this issue, this crate is based on the [`crossbeam_epoch`] crate, which enables
epoch-based memory reclamation.
This is also where the [`Guard`] type and the [`pin`] function are re-exported from, for ease
of use.
It is highly recommended for anyone that considers using this crate to go through the
documentation of the [`crossbeam_epoch`] crate as well.

### Performance

The crate has not been properly benchmarked and evaluated yet.

Informally, it certainly looks and feels fast, especially in the aforementioned cases of
multiple concurrent reader threads and rare write operations.


 [consistent hashing ring]: https://en.wikipedia.org/wiki/Consistent_hashing
 [DefaultHasher]: https://doc.rust-lang.org/std/collections/hash_map/struct.DefaultHasher.html
 [Arc]: https://doc.rust-lang.org/std/sync/struct.Arc.html
 [RCU]: https://en.wikipedia.org/wiki/Read-copy-update
 [Mutex]: https://doc.rust-lang.org/std/sync/struct.Mutex.html
 [RwLock]: https://doc.rust-lang.org/std/sync/struct.RwLock.html
 [`crossbeam_epoch`]: https://docs.rs/crossbeam-epoch/0.9/crossbeam_epoch/index.html

## Running the tests

```console
$ RUST_LOG=debug cargo t -- --nocapture
```

## Generating the docs

```console
$ cargo doc --no-deps
```

## License

Distributed under the terms of Apache License, Version 2.0.

For further details consult the included [LICENSE file](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0.